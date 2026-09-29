"""
main.py - PyFlink reference job: Amazon MSK (Kafka) -> transform -> Couchbase Capella.

Shape of the job
  KafkaSource  ->  parse / normalise (your Flink logic goes here)  ->  CouchbaseUpsert

CouchbaseUpsert is a MapFunction with open()/close() lifecycle hooks. One Cluster object is created per Python
worker in open() and reused for every record; writes are key-value upserts
(sub-millisecond, no query service involved). Failures raise, so Flink's
restart strategy and Kafka offsets give at-least-once delivery; upserts are
idempotent so replays are safe.

Runtime properties (Managed Flink console -> Runtime properties)

  Group "kinesis.analytics.flink.run.options"
    python     app/main.py
    jarfile    app/lib/pyflink-dependencies.jar
    pyFiles    app/lib/python-deps.zip

  Group "KafkaSource"
    bootstrap.servers   b-1.msk...:9098,b-2.msk...:9098
    topic               qlik.db2.item_master
    group.id            flink-couchbase-ods
    security.protocol   SASL_SSL                     (MSK IAM: also sasl.mechanism=AWS_MSK_IAM,
    ...                                              sasl.jaas.config / sasl.client.callback.handler.class)

  Group "CouchbaseSink"
    connection_string   couchbases://cb.xxxx.cloud.couchbase.com
    username            <cluster access credential>
    password            <cluster access credential>
    bucket              ods
    scope               retail
    collection          item_master
    id_field            item_number             (JSON field used as the document key)
    durability          none | majority | majority_and_persist_to_active | persist_to_majority
"""
import json
import logging
import os
from datetime import timedelta

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import RuntimeContext, StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (KafkaOffsetResetStrategy, KafkaOffsetsInitializer,
                                                 KafkaSource)
from pyflink.datastream.functions import MapFunction

log = logging.getLogger("couchbase-sink")

APPLICATION_PROPERTIES = "/etc/flink/application_properties.json"


def load_properties():
    """Managed Flink writes runtime properties to a JSON file; locally use a copy."""
    path = APPLICATION_PROPERTIES if os.path.exists(APPLICATION_PROPERTIES) \
        else os.path.join(os.path.dirname(__file__), "application_properties.json")
    with open(path) as f:
        groups = json.load(f)
    return {g["PropertyGroupId"]: g["PropertyMap"] for g in groups}


class CouchbaseUpsert(MapFunction):
    """Upserts each JSON record into a Capella collection. Returns the document key."""

    def __init__(self, cfg):
        self.cfg = cfg
        self.collection = None
        self.upsert_opts = None

    def open(self, runtime_context: RuntimeContext):
        # Imported here, inside the worker, so an SDK problem surfaces as a
        # readable stack trace in the TaskManager log rather than a bare exit.
        from couchbase.auth import PasswordAuthenticator
        from couchbase.cluster import Cluster
        from couchbase.durability import DurabilityLevel, ServerDurability
        from couchbase.options import ClusterOptions, ClusterTimeoutOptions, UpsertOptions

        cfg = self.cfg
        opts = ClusterOptions(
            PasswordAuthenticator(cfg["username"], cfg["password"]),
            timeout_options=ClusterTimeoutOptions(
                bootstrap_timeout=timedelta(seconds=30),
                kv_timeout=timedelta(seconds=5),
            ),
        )
        cluster = Cluster(cfg["connection_string"], opts)
        cluster.wait_until_ready(timedelta(seconds=30))
        bucket = cluster.bucket(cfg["bucket"])
        self.collection = bucket.scope(cfg.get("scope", "_default")) \
                                .collection(cfg.get("collection", "_default"))

        level = {
            "majority": DurabilityLevel.MAJORITY,
            "majority_and_persist_to_active": DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE,
            "persist_to_majority": DurabilityLevel.PERSIST_TO_MAJORITY,
        }.get(cfg.get("durability", "none").lower())
        self.upsert_opts = UpsertOptions(durability=ServerDurability(level)) if level else UpsertOptions()
        log.info("Couchbase sink ready: %s/%s/%s", cfg["bucket"], cfg.get("scope"), cfg.get("collection"))

    def map(self, record: str) -> str:
        doc = json.loads(record)
        key = str(doc[self.cfg["id_field"]])
        self.collection.upsert(key, doc, self.upsert_opts)
        return key

    def close(self):
        self.collection = None


def transform(record: str) -> str:
    """Placeholder for the normalisation / mapping logic agreed on 16 Sep."""
    doc = json.loads(record)
    doc.setdefault("type", "item_master")
    return json.dumps(doc)


def main():
    props = load_properties()
    kafka = props["KafkaSource"]
    cb = props["CouchbaseSink"]

    env = StreamExecutionEnvironment.get_execution_environment()

    source = KafkaSource.builder() \
        .set_bootstrap_servers(kafka["bootstrap.servers"]) \
        .set_topics(kafka["topic"]) \
        .set_group_id(kafka.get("group.id", "flink-couchbase-ods")) \
        .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets(KafkaOffsetResetStrategy.EARLIEST)) \
        .set_value_only_deserializer(SimpleStringSchema())
    for k, v in kafka.items():                       # pass through any extra Kafka client settings
        if k not in ("bootstrap.servers", "topic", "group.id"):
            source = source.set_property(k, v)

    stream = env.from_source(source.build(), WatermarkStrategy.no_watermarks(), "msk-source")

    stream.map(transform, output_type=Types.STRING()) \
          .map(CouchbaseUpsert(cb), output_type=Types.STRING()) \
          .name("couchbase-upsert")

    env.execute("msk-to-couchbase-ods")


if __name__ == "__main__":
    main()
