import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

# REPLACE WITH YOUR ACTUAL COUCHBASE CAPELLA DETAILS
CB_CONN_STR = "couchbases://cb.q8i2gha75y5jqvq6.cloud.couchbase.com"
CB_USERNAME = "aws_flnk_notebook"
CB_PASSWORD = "Password@202610"
CB_BUCKET = "sales"
CB_SCOPE = "_default"
CB_COLLECTION = "orders"


def couchbase_writer_stream(_):
  """Initializes the Couchbase connection inside the TaskManager worker and streams upserts continuously."""
  from couchbase.auth import PasswordAuthenticator
  from couchbase.cluster import Cluster
  from couchbase.options import ClusterOptions

  auth = PasswordAuthenticator(CB_USERNAME, CB_PASSWORD)
  cluster = Cluster(CB_CONN_STR, ClusterOptions(auth))
  bucket = cluster.bucket(CB_BUCKET)
  scope = bucket.scope(CB_SCOPE)
  collection = scope.collection(CB_COLLECTION)

  logger.info(
      f"Connected to Couchbase collection: {CB_BUCKET}.{CB_SCOPE}.{CB_COLLECTION}"
  )

  seq_num = 1
  while True:
    time.sleep(5)  # Write 1 document every 5 seconds
    order_id = f"ORD-{seq_num}"
    doc_key = f"order::{order_id}"

    sales_payload = {
        "order_id": order_id,
        "sku": random.choice(["SKU-101", "SKU-205", "SKU-999", "SKU-404"]),
        "amount": round(random.uniform(15.0, 350.0), 2),
        "currency": "INR",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    try:
      collection.upsert(doc_key, sales_payload)
      msg = f"UPSERT_SUCCESS:: Key={doc_key} Body={sales_payload}"
      logger.info(msg)
      yield msg
    except Exception as e:
      err_msg = f"UPSERT_FAILED:: Key={doc_key} Error={str(e)}"
      logger.error(err_msg)
      yield err_msg

    seq_num += 1


def main():
  env = StreamExecutionEnvironment.get_execution_environment()

  trigger = env.from_collection([1], type_info=Types.INT())

  writer = trigger.flat_map(
      couchbase_writer_stream, output_type=Types.STRING()
  ).name("couchbase-continuous-writer")

  writer.print()

  env.execute("rdh-couchbase-pure-python-writer")


if __name__ == "__main__":
  main()