"""
RDH Item Master — PyFlink job (DUMMY STREAM MODE).
Hardened for AWS Managed Service for Apache Flink with Py4J and Logging Fixes.
"""
import json
import random
import time
import os
import sys
import logging
from datetime import datetime, timezone

from pyflink.common import Types, Row
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import Sink
from pyflink.java_gateway import get_gateway

# Force logging to standard out so AWS CloudWatch captures it before any crash
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- config
def load_properties() -> dict:
    try:
        with open("/etc/flink/application_properties.json") as f:
            return {g["PropertyGroupId"]: g["PropertyMap"] for g in json.load(f)}
    except Exception as e:
        logger.warning(f"Could not read properties file: {e}")
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
    try:
        logger.info("Starting PyFlink Initialization...")
        props = load_properties()
        cb_p = props.get("CouchbaseSink", {})
        
        env = StreamExecutionEnvironment.get_execution_environment()
        
        # FIX 1: Dynamically find the JAR in the AWS temp directory and inject it into the Py4J Gateway
        current_dir = os.path.dirname(os.path.abspath(__file__))
        jar_path = f"file://{current_dir}/lib/rdh-couchbase-sink.jar"
        env.add_jars(jar_path)
        logger.info(f"Successfully added JAR to classpath: {jar_path}")

        # FIX 2: Reduce the array size to 1,000 to prevent Akka RPC Frame payload crashes
        dummy_sequence = env.from_collection(list(range(1, 1000)), type_info=Types.INT())
        mutations = dummy_sequence.flat_map(generate_dummy_sales, output_type=MUTATION_TYPE).name("dummy-sales-generator")

        mutations.key_by(lambda row: row[1]).sink_to(couchbase_sink(cb_p)).name("couchbase-guarded-sink")

        logger.info("Executing Flink Job...")
        env.execute("rdh-dummy-sales")
        
    except Exception as e:
        logger.error(f"FATAL PYTHON CRASH: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main() 