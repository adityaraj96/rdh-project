"""
RDH Item Master — PyFlink job (DUMMY STREAM MODE).

Generates dummy sales data continuously and sinks it to Couchbase Capella 
using the Java Uber JAR. Kafka/MSK is temporarily bypassed for testing.
"""
import json
import os
import time
import random
from datetime import datetime, timezone

from pyflink.common import Configuration, Row, Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import Sink
from pyflink.java_gateway import get_gateway

APP_PROPERTIES_FILE = "/etc/flink/application_properties.json"
IS_LOCAL = not os.path.isfile(APP_PROPERTIES_FILE)

# --------------------------------------------------------------------------- config
def load_properties() -> dict:
    path = APP_PROPERTIES_FILE if not IS_LOCAL else os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "application_properties.json")
    with open(path) as f:
        return {g["PropertyGroupId"]: g["PropertyMap"] for g in json.load(f)}

# --------------------------------------------------------------------------- mapping
# The exact Row shape the Java sink expects
MUTATION_TYPE = Types.ROW_NAMED(
    ["op", "doc_key", "section", "payload", "event_ts"],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.LONG()],
)

def generate_dummy_sales(seq_num: int):
    """
    Acts as a dummy CDC stream. Receives a sequence number and yields a Couchbase mutation.
    """
    time.sleep(0.5)  # Throttle generation to ~2 records per second per Flink worker
    
    # Pick a random SKU to update
    skus = ["SKU-101", "SKU-205", "SKU-999", "SKU-404"]
    sku = random.choice(skus)
    
    # Generate dummy sales payload
    sales_data = {
        "order_id": f"ORD-{seq_num}",
        "store_id": random.randint(1, 50),
        "amount": round(random.uniform(15.0, 350.0), 2),
        "status": "COMPLETED",
        "currency": "INR"
    }
    
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

    # Yield the Row exactly as the Java Sink expects it
    yield Row(
        "UPSERT", 
        f"ITEM::{sku}", 
        "sales",                                         # This will become the "sales" section in Couchbase
        json.dumps(sales_data, separators=(",", ":")), 
        now_ms
    )

# --------------------------------------------------------------------------- sink
def couchbase_sink(p: dict) -> Sink:
    jvm = get_gateway().jvm
    j_sink = (
        jvm.com.smri.rdh.flink.couchbase.CouchbaseGuardedSink.builder()
        .connectionString(p["connection.string"])
        .username(p["username"])
        .password(p["password"])
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
    cb_p = props["CouchbaseSink"]
    job_p = props.get("Job", {})

    conf = Configuration()
    conf.set_string("python.fn-execution.bundle.time", job_p.get("bundle.time.ms", "100"))
    conf.set_string("python.fn-execution.bundle.size", job_p.get("bundle.size", "1000"))
    env = StreamExecutionEnvironment.get_execution_environment(conf)

    if IS_LOCAL:
        jar = os.path.join(os.path.dirname(os.path.realpath(__file__)), "lib", "rdh-couchbase-sink.jar")
        env.add_jars(f"file://{jar}")
        env.enable_checkpointing(10_000)

    # 1. Create a dummy infinite stream (generating numbers 1 to 1,000,000)
    dummy_sequence = env.from_sequence(1, 1_000_000)

    # 2. Map the sequence numbers into Dummy Sales rows
    mutations = dummy_sequence.flat_map(generate_dummy_sales, output_type=MUTATION_TYPE).name("dummy-sales-generator")

    # 3. Sink to Couchbase (using the same perfected key_by logic)
    mutations.key_by(lambda row: row.getField(1)) \
             .sink_to(couchbase_sink(cb_p)) \
             .name("couchbase-guarded-sink")

    env.execute("rdh-dummy-sales")

if __name__ == "__main__":
    main()