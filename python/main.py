import json
import random
import time
import os
import sys
import logging
from datetime import datetime, timezone

from pyflink.common import Types, Row
from pyflink.datastream import StreamExecutionEnvironment

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

IS_LOCAL = os.environ.get("FLINK_ENV") == "local"
APP_PROPERTIES_FILE = "/etc/flink/application_properties.json"

def generate_dummy_sales(seq_num: int):
    time.sleep(0.5) 
    sku = random.choice(["SKU-101", "SKU-205", "SKU-999", "SKU-404"])
    sales_data = {
        "order_id": f"ORD-{seq_num}",
        "amount": round(random.uniform(15.0, 350.0), 2),
        "currency": "INR"
    }
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    
    payload = json.dumps(sales_data, separators=(",", ":"))
    logger.info(f"GENERATED_RECORD:: SKU={sku} PAYLOAD={payload}")
    
    yield Row(sku, payload, now_ms)

def main():
    try:
        env = StreamExecutionEnvironment.get_execution_environment()
        
        if IS_LOCAL:
            env.enable_checkpointing(10000)
        
        dummy_sequence = env.from_collection(list(range(1, 100)), type_info=Types.INT())
        
        mutations = dummy_sequence.flat_map(
            generate_dummy_sales, 
            output_type=Types.ROW([Types.STRING(), Types.STRING(), Types.LONG()])
        ).name("dummy-sales-generator")
        
        mutations.print().name("print-sink")
        
        env.execute("rdh-pure-dummy-test")

    except Exception as e:
        logger.error(f"CRITICAL INITIALIZATION ERROR: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
