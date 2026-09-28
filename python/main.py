import json
import logging
import os
import random
import sys
import time
import traceback
from datetime import datetime, timezone
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment

# 1. AWS CloudWatch Routing
# Flink automatically pipes sys.stdout to CloudWatch taskmanager/jobmanager logs.
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# 2. Global Exception Catcher
# If an error somehow bypasses all try/except blocks, this intercepts the fatal crash
# and forces the stack trace into CloudWatch before the Python worker dies.
def global_exception_handler(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    logger.critical("FATAL_UNHANDLED_CRASH::", exc_info=(exc_type, exc_value, exc_traceback))

sys.excepthook = global_exception_handler

# REPLACE WITH YOUR ACTUAL COUCHBASE CAPELLA DETAILS
CB_CONN_STR = "couchbases://cb.q8i2gha75y5jqvq6.cloud.couchbase.com"
CB_USERNAME = "aws_flnk_notebook"
CB_PASSWORD = "Password@202610"
CB_BUCKET = "sales"
CB_SCOPE = "_default"
CB_COLLECTION = "orders"

def couchbase_writer_stream(_):
    from couchbase.auth import PasswordAuthenticator
    from couchbase.cluster import Cluster
    from couchbase.options import ClusterOptions
    from datetime import timedelta
    
    connected = False
    collection = None
    
    # 3. Connection-Level Error Handling
    try:
        auth = PasswordAuthenticator(CB_USERNAME, CB_PASSWORD)
        cluster = Cluster(CB_CONN_STR, ClusterOptions(auth))
        cluster.wait_until_ready(timedelta(seconds=15))
        
        bucket = cluster.bucket(CB_BUCKET)
        scope = bucket.scope(CB_SCOPE)
        collection = scope.collection(CB_COLLECTION)
        logger.info(f"Connected to Couchbase collection: {CB_BUCKET}.{CB_SCOPE}.{CB_COLLECTION}")
        connected = True
    except Exception as e:
        logger.error(f"NETWORK_HANDSHAKE_ERROR:: Failed to connect to Capella. Error: {str(e)}", exc_info=True)

    seq_num = 1
    while True:
        time.sleep(5)
        
        if not connected:
            msg = "NETWORK_BLOCKED:: Waiting for Capella IP Whitelist or AWS VPC NAT setup..."
            logger.warning(msg)
            yield msg
            continue
            
        order_id = f"ORD-{seq_num}"
        doc_key = f"order::{order_id}"
        sales_payload = {
            "order_id": order_id,
            "sku": random.choice(["SKU-101", "SKU-205", "SKU-999", "SKU-404"]),
            "amount": round(random.uniform(15.0, 350.0), 2),
            "currency": "INR",
            "created_at": datetime.now(timezone.utc).isoformat()
        }

        # 4. Record-Level Error Handling
        try:
            collection.upsert(doc_key, sales_payload)
            msg = f"UPSERT_SUCCESS:: Key={doc_key}"
            logger.info(msg)
            yield msg
        except Exception as e:
            err_msg = f"UPSERT_FAILED:: Key={doc_key} Error={str(e)}"
            logger.error(err_msg, exc_info=True)
            yield err_msg

        seq_num += 1

def main():
    # 5. Graph Initialization Error Handling
    try:
        env = StreamExecutionEnvironment.get_execution_environment()
        trigger = env.from_collection([1], type_info=Types.INT())
        
        writer = trigger.flat_map(
            couchbase_writer_stream, output_type=Types.STRING()
        ).name("couchbase-invincible-writer")
        
        writer.print()
        env.execute("rdh-couchbase-pure-python-writer")
        
    except Exception as e:
        logger.critical(f"JOB_INITIALIZATION_ERROR:: Flink failed to build or execute the graph. Error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()