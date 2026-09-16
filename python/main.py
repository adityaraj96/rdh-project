import sys
import traceback
import logging

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

def deploy_dummy_error_job(error_details):
    """If the main graph fails to build, we deploy this dummy job to keep the cluster alive and log the exact error."""
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.common import Types
    import time
    
    env = StreamExecutionEnvironment.get_execution_environment()
    
    def log_and_wait(x):
        logger.error(f"===== PIPELINE FATAL ERROR =====")
        logger.error(error_details)
        time.sleep(10)
        return x
        
    trigger = env.from_collection([1], type_info=Types.INT())
    trigger.map(log_and_wait, output_type=Types.INT()).name("crash-reporter")
    
    env.execute("Crash-Report-Job")

try:
    # --- REAL PIPELINE CODE ---
    import json
    import random
    import time
    import os
    from datetime import datetime, timezone

    from pyflink.common import Types, Row
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.java_gateway import get_gateway
    
    # We deliberately DO NOT import pyflink.datastream.connectors.base.Sink to prevent 
    # top-level ImportErrors if AWS Flink 1.20 repackaged the Python wheel.

    def generate_continuous_sales(_):
        seq_num = 1
        while True:
            time.sleep(1) 
            sku = random.choice(["SKU-101", "SKU-205", "SKU-999", "SKU-404"])
            sales_data = {
                "order_id": f"ORD-{seq_num}",
                "amount": round(random.uniform(15.0, 350.0), 2),
                "currency": "INR"
            }
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            payload = json.dumps(sales_data, separators=(",", ":"))
            
            yield Row("UPSERT", f"ITEM::{sku}", "sales", payload, now_ms)
            seq_num += 1

    def couchbase_sink():
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
        return j_sink

    def main():
        env = StreamExecutionEnvironment.get_execution_environment()
        
        trigger = env.from_collection([1], type_info=Types.INT())
        
        mutations = trigger.flat_map(
            generate_continuous_sales, 
            output_type=Types.ROW([
                Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(), Types.LONG()
            ])
        ).name("infinite-sales-generator")
        
        # We fetch the raw Java sink and attach it natively to the Java DataStream.
        # This completely bypasses the Python PyFlink Sink wrapper module!
        j_sink = couchbase_sink()
        mutations._j_data_stream.sinkTo(j_sink)
        
        env.execute("rdh-couchbase-integration")

    if __name__ == "__main__":
        main()

except Exception as e:
    # If anything crashes during initialization, we catch the exact stack trace
    error_details = traceback.format_exc()
    logger.error(f"CAUGHT FATAL ERROR: {error_details}")
    try:
        # And deploy the dummy job so the error survives and prints to CloudWatch
        deploy_dummy_error_job(error_details)
    except Exception as secondary_e:
        logger.error(f"DUMMY JOB FAILED TO DEPLOY: {secondary_e}")
        sys.exit(1)