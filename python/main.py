"""
RDH Item Master — PyFlink job on Amazon Managed Service for Apache Flink.
Automatically toggles between MSK CDC streaming and Dummy Sales Generation.
"""
import json
import os
import time
import random
import sys
import traceback
from datetime import datetime, timezone

from pyflink.common import Configuration, Row, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import Sink
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.java_gateway import get_gateway

APP_PROPERTIES_FILE = "/etc/flink/application_properties.json"
IS_LOCAL = not os.path.isfile(APP_PROPERTIES_FILE)

# --------------------------------------------------------------------------- config
def load_properties() -> dict:
    path = APP_PROPERTIES_FILE if not IS_LOCAL else os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "application_properties.json")
    if not os.path.isfile(path):
        return {}
    with open(path) as f:
        return {g["PropertyGroupId"]: g["PropertyMap"] for g in json.load(f)}

# --------------------------------------------------------------------------- mapping
TABLE_TO_SECTION = {
    "INVMST": "core",
    "INVPRC": "pricing",
    "INVUPC": "barcodes",
}

MUTATION_TYPE = Types.ROW_NAMED(
    ["op", "doc_key", "section", "payload", "event_ts"],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.LONG()],
)

def to_epoch_ms(ts: str) -> int:
    return int(datetime.fromisoformat(ts).replace(tzinfo=timezone.utc).timestamp() * 1000)

def cdc_to_mutation(raw: str):
    try:
        msg = json.loads(raw)
        body = msg.get("message", msg)
        headers = body["headers"]
        table = body.get("table") or headers.get("tableName")
        section = TABLE_TO_SECTION.get(table)
        if section is None:
            return

        operation = headers["operation"].upper()
        data = body["beforeData"] if operation == "DELETE" else body["data"]
        sku = str(data["INUMBR"]).strip()

        yield Row(
            "DELETE" if operation == "DELETE" else "UPSERT",
            f"ITEM::{sku}",
            section,
            json.dumps(data, separators=(",", ":")),
            to_epoch_ms(headers["timestamp"]),
        )
    except Exception:
        return

def generate_dummy_sales(seq_num: int):
    """Generates dummy sales data if Kafka is not configured."""
    time.sleep(0.5) 
    skus = ["SKU-101", "SKU-205", "SKU-999", "SKU-404"]
    sku = random.choice(skus)
    
    sales_data = {
        "order_id": f"ORD-{seq_num}",
        "store_id": random.randint(1, 50),
        "amount": round(random.uniform(15.0, 350.0), 2),
        "status": "COMPLETED",
        "currency": "INR"
    }
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    yield Row("UPSERT", f"ITEM::{sku}", "sales", json.dumps(sales_data, separators=(",", ":")), now_ms)

# --------------------------------------------------------------------------- sink
def couchbase_sink(p: dict) -> Sink:
    jvm = get_gateway().jvm
    j_sink = (
        jvm.com.smri.rdh.flink.couchbase.CouchbaseGuardedSink.builder()
        .connectionString(p.get("connection.string", "couchbases://cb.xxxxxxxx.cloud.couchbase.com"))
        .username(p.get("username", "Administrator"))
        .password(p.get("password", "password"))
        .bucket(p.get("bucket", "rdh_ods"))
        .scope(p.get("scope", "item"))
        .collection(p.get("collection", "item_master"))
        .batchSize(int(p.get("batch.size", "500")))
        .lingerMs(int(p.get("linger.ms", "200")))
        .maxConcurrency(int(p.get("max.concurrency", "128")))
        .kvTimeoutMs(int(p.get("kv.timeout.ms", "2500")))
        .build()
    )
    return Sink(j_sink)

# --------------------------------------------------------------------------- job
def main():
    try:
        props = load_properties()
        kafka_p = props.get("KafkaSource")
        cb_p = props.get("CouchbaseSink", {})
        job_p = props.get("Job", {})

        conf = Configuration()
        conf.set_string("python.fn-execution.bundle.time", job_p.get("bundle.time.ms", "100"))
        conf.set_string("python.fn-execution.bundle.size", job_p.get("bundle.size", "1000"))
        env = StreamExecutionEnvironment.get_execution_environment(conf)

        if IS_LOCAL:
            jar = os.path.join(os.path.dirname(os.path.realpath(__file__)), "lib", "rdh-couchbase-sink.jar")
            env.add_jars(f"file://{jar}")
            env.enable_checkpointing(10_000)

        # Toggle between MSK and Dummy Data
        if kafka_p:
            builder = (
                KafkaSource.builder()
                .set_bootstrap_servers(kafka_p["bootstrap.servers"])
                .set_topics(*kafka_p["topics"].split(","))
                .set_group_id(kafka_p.get("group.id", "rdh-item-master"))
                .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets())
                .set_value_only_deserializer(SimpleStringSchema())
            )
            if kafka_p.get("security.protocol") == "SASL_SSL":
                builder = (
                    builder.set_property("security.protocol", "SASL_SSL")
                    .set_property("sasl.mechanism", "AWS_MSK_IAM")
                    .set_property("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;")
                    .set_property("sasl.client.callback.handler.class",
                                  "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
                )
            cdc = env.from_source(builder.build(), WatermarkStrategy.no_watermarks(), "msk-qlik-cdc")
            mutations = cdc.flat_map(cdc_to_mutation, output_type=MUTATION_TYPE).name("map-to-sections")
        else:
            # Safe, built-in Flink collection generator that bypasses AWS sandbox restrictions
            dummy_sequence = env.from_collection(list(range(1, 10000)), type_info=Types.INT())
            mutations = dummy_sequence.flat_map(generate_dummy_sales, output_type=MUTATION_TYPE).name("dummy-sales-generator")

        # AUDIT FIX: row[1] extracts the doc_key securely in PyFlink
        mutations.key_by(lambda row: row[1]) \
                 .sink_to(couchbase_sink(cb_p)) \
                 .name("couchbase-guarded-sink")

        env.execute("rdh-item-master")
        
    except Exception as e:
        # AUDIT FIX: Force hidden AWS Flink startup errors to dump directly into CloudWatch
        print(f"\n--- CRITICAL PYTHON INITIALIZATION ERROR ---\n{e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        raise

if __name__ == "__main__":
    main()