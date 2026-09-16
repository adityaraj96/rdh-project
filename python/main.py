import json
import random
import time
import os
import sys
from datetime import datetime, timezone

from pyflink.common import Types, Row
from pyflink.datastream import StreamExecutionEnvironment

# Tech Lead Fix: Explicit opt-in for local mode. 
# Prevents AWS from accidentally triggering local logic due to slow volume mounts.
IS_LOCAL = os.environ.get("FLINK_ENV") == "local"
APP_PROPERTIES_FILE = "/etc/flink/application_properties.json"

def load_properties() -> dict:
    if IS_LOCAL or not os.path.isfile(APP_PROPERTIES_FILE):
        return {}
    with open(APP_PROPERTIES_FILE) as f:
        return {g["PropertyGroupId"]: g["PropertyMap"] for g in json.load(f)}

def generate_dummy_sales(seq_num: int):
    time.sleep(0.5) 
    sku = random.choice(["SKU-101", "SKU-205", "SKU-999", "SKU-404"])
    sales_data = {
        "order_id": f"ORD-{seq_num}",
        "amount": round(random.uniform(15.0, 350.0), 2),
        "currency": "INR"
    }
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    yield Row(sku, json.dumps(sales_data, separators=(",", ":")), now_ms)

def main():
    try:
        env = StreamExecutionEnvironment.get_execution_environment()
        
        if IS_LOCAL:
            # Tech Lead Fix: Checkpointing is ONLY enabled locally. 
            # On AWS, the orchestrator handles this natively.
            env.enable_checkpointing(10000)
        
        # 1. Generate 100 records
        dummy_sequence = env.from_collection(list(range(1, 100)), type_info=Types.INT())
        
        # 2. Map to Dummy Sales
        mutations = dummy_sequence.flat_map(
            generate_dummy_sales, 
            output_type=Types.ROW([Types.STRING(), Types.STRING(), Types.LONG()])
        ).name("dummy-sales-generator")
        
        # 3. Print Sink (Outputs to CloudWatch TaskManager logs)
        mutations.print().name("print-sink")
        
        env.execute("rdh-pure-dummy-test")

    except Exception as e:
        print(f"\n--- CRITICAL INITIALIZATION ERROR ---\n{e}", file=sys.stderr)
        raise

if __name__ == "__main__":
    main()