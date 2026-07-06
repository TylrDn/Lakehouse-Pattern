"""Postgres → Delta CDC — the inbound side of Lakebase sync.

Two modes:

* **Debezium JSON** (preferred) — read a Kafka topic that Debezium is
  populating from the Postgres WAL. Same code path as the streaming Kafka
  ingest in ``ingestion/streaming/kafka_source.py``.
* **``xmin`` polling** (fallback for laptops without Debezium) — poll
  Postgres for rows where ``xmin`` > last-seen. Not exactly-once, but a
  good demo.
"""

from __future__ import annotations

import argparse


from lakehouse import paths
from lakehouse.env import get_logger
from lakehouse.spark import get_spark

_log = get_logger("lakebase.pg_cdc")

_TARGET = paths.BRONZE_DIR / "orders_from_postgres"
_CHECKPOINT = paths.CHECKPOINT_DIR / "pg_cdc_orders"


def start_debezium(bootstrap: str, topic: str) -> None:
    spark = get_spark("pg-cdc-debezium")
    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .load()
    )
    # Debezium envelope: {before, after, op, ts_ms, source: {...}}
    parsed = kafka.selectExpr("CAST(value AS STRING) as v")
    # Delta MERGE side is handled by ``transform.cdc.apply_changes`` — this
    # module only lands the raw envelope into Bronze.
    (
        parsed.writeStream.format("delta")
        .option("checkpointLocation", str(_CHECKPOINT))
        .trigger(processingTime="30 seconds")
        .start(str(_TARGET))
        .awaitTermination()
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="pg.public.orders")
    args = parser.parse_args()
    start_debezium(args.bootstrap, args.topic)
