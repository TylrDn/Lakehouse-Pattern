"""Kafka source with continuous processing + watermarked stateful aggregation.

Compared to ``ingestion/streaming_ingest.py`` (file source + ``AvailableNow``)
this module demonstrates the "always-on" side of Databricks streaming:

* **Kafka source** — ``spark.readStream.format("kafka")``. Same code path
  Databricks uses; only the bootstrap-servers change between local Redpanda
  and Confluent Cloud.
* **Watermark + windowed aggregation** — 1-minute tumbling revenue-per-country
  with a 10-minute watermark. This is stateful; state is checkpointed.
* **Trigger.Continuous** — sub-second latency mode. Ships as an alternative
  path; ``processingTime`` remains the default because Continuous only
  supports at-least-once and a subset of operators (`map`, `filter`,
  aggregations to Kafka sinks).
* **foreachBatch idempotency** — the sink uses ``txnAppId`` + ``txnVersion``
  properties on the Delta writer so a replay of the same batch is a no-op.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    window,
    sum as ssum,
)
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

from lakehouse import paths
from lakehouse.env import get_logger
from lakehouse.spark import get_spark

_log = get_logger("ingestion.streaming.kafka")

_KAFKA_SCHEMA = (
    StructType()
    .add("transaction_id", StringType())
    .add("customer_id", StringType())
    .add("product_id", StringType())
    .add("quantity", IntegerType())
    .add("unit_price", DoubleType())
    .add("currency", StringType())
    .add("event_ts", StringType())
    .add("country", StringType())
)

CHECKPOINT = paths.CHECKPOINT_DIR / "kafka_windowed_revenue"


def build_windowed_revenue(spark: SparkSession, bootstrap: str, topic: str) -> DataFrame:
    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )
    parsed = (
        kafka.select(from_json(col("value").cast("string"), _KAFKA_SCHEMA).alias("j"))
        .select("j.*")
        .withColumn("event_ts", col("event_ts").cast("timestamp"))
        .withColumn("revenue", col("quantity") * col("unit_price"))
    )
    return (
        parsed.withWatermark("event_ts", "10 minutes")
        .groupBy(window(col("event_ts"), "1 minute"), col("country"))
        .agg(ssum("revenue").alias("gross_revenue"))
    )


def _foreach_batch(df: DataFrame, batch_id: int) -> None:
    # Idempotent write: (txnAppId, txnVersion) pair — Delta's dedupe key for
    # streaming sinks. Replays of the same batch produce no additional rows.
    (
        df.write.format("delta")
        .mode("append")
        .option("txnAppId", "lakehouse-kafka-revenue")
        .option("txnVersion", str(batch_id))
        .save(str(paths.GOLD_DIR / "revenue_1min"))
    )
    _log.info("batch %d wrote %d rows", batch_id, df.count())


def start(bootstrap: str, topic: str, continuous: bool = False) -> None:
    spark = get_spark("kafka-continuous-revenue")
    windowed = build_windowed_revenue(spark, bootstrap, topic)
    writer = (
        windowed.writeStream.foreachBatch(_foreach_batch)
        .option("checkpointLocation", str(CHECKPOINT))
    )
    trigger = {"continuous": "1 second"} if continuous else {"processingTime": "30 seconds"}
    q = writer.trigger(**trigger).start()
    q.awaitTermination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="transactions")
    parser.add_argument("--continuous", action="store_true")
    args = parser.parse_args()
    start(args.bootstrap, args.topic, continuous=args.continuous)
