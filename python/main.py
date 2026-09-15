"""
RDH Item Master — PyFlink job on Amazon Managed Service for Apache Flink.

  MSK (Qlik CDC JSON)  ->  Python: parse + map to (op, doc_key, section, payload, ts)
                       ->  Java:   CouchbaseGuardedSink (Couchbase Java SDK)

Python holds the business mapping; Java does the heavy, latency-sensitive writes.
No native Python packages are needed, so nothing has to be compiled for the runtime.
"""
import json
import os
from datetime import datetime, timezone

from pyflink.common import Configuration, Row, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import Sink
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.java_gateway import get_gateway

APP_PROPERTIES_FILE = "/etc/flink/application_properties.json"  # present on Managed Flink
IS_LOCAL = not os.path.isfile(APP_PROPERTIES_FILE)


# --------------------------------------------------------------------------- config
def load_properties() -> dict:
    """Managed Flink: runtime properties from the console. Local: a JSON file next to this script."""
    path = APP_PROPERTIES_FILE if not IS_LOCAL else os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "application_properties.json")
    with open(path) as f:
        return {g["PropertyGroupId"]: g["PropertyMap"] for g in json.load(f)}


# --------------------------------------------------------------------------- mapping
# Which MMS table feeds which section of the canonical ITEM document.
# Extend to all 14 tables; the Java sink does not change when this does.
TABLE_TO_SECTION = {
    "INVMST": "core",
    "INVPRC": "pricing",
    "INVUPC": "barcodes",
    # ...
}

# This is the exact Row shape the Java sink expects (positions matter).
MUTATION_TYPE = Types.ROW_NAMED(
    ["op", "doc_key", "section", "payload", "event_ts"],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.LONG()],
)


def to_epoch_ms(ts: str) -> int:
    # Qlik header timestamp, e.g. "2026-09-15T10:21:04.123" (UTC). Adjust to your envelope.
    return int(datetime.fromisoformat(ts).replace(tzinfo=timezone.utc).timestamp() * 1000)


def cdc_to_mutation(raw: str):
    """Qlik Replicate JSON -> one Row per section change. Bad records are dropped (send to a DLQ in prod)."""
    try:
        msg = json.loads(raw)
        body = msg.get("message", msg)
        headers = body["headers"]
        table = body.get("table") or headers.get("tableName")  # depends on your Qlik topic/envelope setup
        section = TABLE_TO_SECTION.get(table)
        if section is None:
            return

        operation = headers["operation"].upper()           # INSERT / UPDATE / DELETE / REFRESH
        data = body["beforeData"] if operation == "DELETE" else body["data"]
        sku = str(data["INUMBR"]).strip()                   # MMS item number

        yield Row(
            "DELETE" if operation == "DELETE" else "UPSERT",
            f"ITEM::{sku}",
            section,
            json.dumps(data, separators=(",", ":")),
            to_epoch_ms(headers["timestamp"]),
        )
    except Exception:  # noqa: BLE001 — never let one bad message kill the job
        return


# --------------------------------------------------------------------------- sink
def couchbase_sink(p: dict) -> Sink:
    """Build the Java sink through the Py4J gateway and wrap it for PyFlink."""
    jvm = get_gateway().jvm
    j_sink = (
        jvm.com.smri.rdh.flink.couchbase.CouchbaseGuardedSink.builder()
        .connectionString(p["connection.string"])
        .username(p["username"])
        .password(p["password"])          # production: read from Secrets Manager
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
    props = load_properties()
    kafka_p, cb_p, job_p = props["KafkaSource"], props["CouchbaseSink"], props.get("Job", {})

    conf = Configuration()
    # Python operators hand records to Java in bundles; defaults can add ~1s latency.
    conf.set_string("python.fn-execution.bundle.time", job_p.get("bundle.time.ms", "100"))
    conf.set_string("python.fn-execution.bundle.size", job_p.get("bundle.size", "1000"))
    env = StreamExecutionEnvironment.get_execution_environment(conf)

    if IS_LOCAL:
        # On Managed Flink the JAR is attached via the 'jarfile' runtime property instead.
        jar = os.path.join(os.path.dirname(os.path.realpath(__file__)), "lib", "rdh-couchbase-sink.jar")
        env.add_jars(f"file://{jar}")
        env.enable_checkpointing(10_000)  # on Managed Flink, checkpointing is set in the app config

    builder = (
        KafkaSource.builder()
        .set_bootstrap_servers(kafka_p["bootstrap.servers"])
        .set_topics(*kafka_p["topics"].split(","))
        .set_group_id(kafka_p.get("group.id", "rdh-item-master"))
        .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets())
        .set_value_only_deserializer(SimpleStringSchema())
    )
    if kafka_p.get("security.protocol") == "SASL_SSL":  # MSK IAM
        builder = (
            builder.set_property("security.protocol", "SASL_SSL")
            .set_property("sasl.mechanism", "AWS_MSK_IAM")
            .set_property("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;")
            .set_property("sasl.client.callback.handler.class",
                          "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
        )

    cdc = env.from_source(builder.build(), WatermarkStrategy.no_watermarks(), "msk-qlik-cdc")

    mutations = cdc.flat_map(cdc_to_mutation, output_type=MUTATION_TYPE).name("map-to-sections")

    # PERFECTION FIX: Route identical doc_keys to the exact same sink subtask
    mutations.key_by(lambda row: row.getField(1)) \
             .sink_to(couchbase_sink(cb_p)) \
             .name("couchbase-guarded-sink")

    env.execute("rdh-item-master")


if __name__ == "__main__":
    main()