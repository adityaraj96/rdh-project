import json
import random
import time
from datetime import datetime, timezone

from pyflink.common import Types, Row
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import Sink
from pyflink.java_gateway import get_gateway

# --------------------------------------------------------------------------- config
def load_properties() -> dict:
    """Safely loads AWS Runtime Properties without triggering local laptop overrides."""
    try:
        with open("/etc/flink/application_properties.json") as f:
            return {g["PropertyGroupId"]: g["PropertyMap"] for g in json.load(f)}
    except Exception as e:
        print(f"Warning: Could not read properties file: {e}")
        return {}

# --------------------------------------------------------------------------- mapping
MUTATION_TYPE = Types.ROW_NAMED(
    ["op", "doc_key", "section", "payload", "event_ts"],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.LONG()],
)

def generate_dummy_sales(seq_num: int):
    time.sleep(0.5) 
    sku = random.choice(["SKU-101", "SKU-205", "SKU-999", "SKU-404"])
    
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
def couchbase_sink(cb_p: dict) -> Sink:
    jvm = get_gateway().jvm
    j_sink = (
        jvm.com.smri.rdh.flink.couchbase.CouchbaseGuardedSink.builder()
        .connectionString(cb_p.get("connection.string", "couchbases://cb.xxxxxxxx.cloud.couchbase.com"))
        .username(cb_p.get("username", "Administrator"))
        .password(cb_p.get("password", "password"))
        .bucket(cb_p.get("bucket", "rdh_ods"))
        .scope(cb_p.get("scope", "item"))
        .collection(cb_p.get("collection", "item_master"))
        .batchSize(int(cb_p.get("batch.size", "500")))
        .lingerMs(int(cb_p.get("linger.ms", "200")))
        .maxConcurrency(int(cb_p.get("max.concurrency", "128")))
        .kvTimeoutMs(int(cb_p.get("kv.timeout.ms", "2500")))
        .build()
    )
    return Sink(j_sink)

# --------------------------------------------------------------------------- job
def main():
    # 1. Initialize safely (AWS injects the JAR automatically based on console settings)
    props = load_properties()
    cb_p = props.get("CouchbaseSink", {})
    
    env = StreamExecutionEnvironment.get_execution_environment()

    # 2. Generate dummy data safely using built-in Python collections
    dummy_sequence = env.from_collection(list(range(1, 100000)), type_info=Types.INT())
    mutations = dummy_sequence.flat_map(generate_dummy_sales, output_type=MUTATION_TYPE).name("dummy-sales-generator")

    # 3. Sink to Couchbase using the perfected Java connector
    mutations.key_by(lambda row: row[1]).sink_to(couchbase_sink(cb_p)).name("couchbase-guarded-sink")

    env.execute("rdh-dummy-sales")

if __name__ == "__main__":
    main()