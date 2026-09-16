import json
import random
import time
import os
import sys
import logging
from datetime import datetime, timezone

from pyflink.common import Types, Row
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.base import Sink
from pyflink.java_gateway import get_gateway

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

def generate_continuous_sales(_):
    seq_num = 1
    while True:
        time.sleep(1) 
        sku = random.choice(["SKU-101", "SKU-205", "SKU-999"])
        sales_data = {
            "order_id": f"ORD-{seq_num}",
            "amount": round(random.uniform(15.0, 350.0), 2)
        }
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        payload = json.dumps(sales_data, separators=(",", ":"))
        
        yield Row("UPSERT", f"ITEM::{sku}", "sales", payload, now_ms)
        seq_num += 1

def couchbase_sink() -> Sink:
    jvm = get_gateway().jvm
    j_sink = (
        jvm.com.smri.rdh.flink.couchbase.CouchbaseGuardedSink.builder()
        .connectionString("couchbases://cb.q8i2gha75y5jqvq6.cloud.couchbase.com") # INSERT ACTUAL URL
        .username("aws_flnk_notebook")                               # INSERT ACTUAL USER
        .password("Password@202610")                               # INSERT ACTUAL PASSWORD
        .bucket("sales")    
        .scope("item")
        .collection("item_master")
        .batchSize(10)
        .lingerMs(200)
        .maxConcurrency(128)
        .kvTimeoutMs(2500)
        .build()
    )
    return Sink(j_sink)

def main():
    try:
        env = StreamExecutionEnvironment.get_execution_environment()
        
        # THE FIX: Dynamically locate the extracted JAR in the AWS sandbox 
        # and force it into the Py4J Gateway classpath.
        jar_path = f"file://{os.getcwd()}/lib/rdh-couchbase-sink.jar"
        env.add_jars(jar_path)
        
        trigger = env.from_collection([1], type_info=Types.INT())
        
        mutations = trigger.flat_map(
            generate_continuous_sales, 
            output_type=Types.ROW([
                Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.LONG()
            ])
        ).name("infinite-sales-generator")
        
        mutations.sink_to(couchbase_sink()).name("couchbase-sink")
        
        env.execute("rdh-couchbase-integration")

    except Exception as e:
        logger.error(f"CRITICAL ERROR: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
