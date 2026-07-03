"""Structured Streaming ingest — Auto Loader analog.

This job watches ``data/sample_raw/`` for new CSV files and appends them to
the same Bronze Delta table as the batch job. It demonstrates the exact same
pattern you would use on Databricks with ``cloudFiles`` (Auto Loader), just
using the OSS ``file`` source so it runs locally without an object store.

Key correctness features
------------------------
* **Exactly-once semantics** are provided by combining the streaming source
  (which tracks processed files) with Delta's transactional writes — the
  checkpoint directory is the durable state store.
* **Trigger.AvailableNow** processes all currently-available files, then
  stops. This is the pattern used for "run every 15 minutes on new files"
  jobs — cheap, deterministic, easy to schedule in Databricks Workflows.
* On Databricks the only change is the source format:

    .. code-block:: python

        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", "<dbfs path>")
            .load("<landing zone>")

  Everything downstream (Bronze/Silver/Gold) is identical.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.streaming import StreamingQuery

from lakehouse import paths
from lakehouse.env import get_logger
from lakehouse.schemas import BRONZE_TRANSACTIONS_SCHEMA
from lakehouse.spark import get_spark

_log = get_logger("ingestion.streaming_ingest")


def build_stream(spark: SparkSession, source_dir: Path) -> DataFrame:
    """Build the streaming DataFrame over the landing zone.

    ``pathGlobFilter=*.csv`` scopes the file source strictly to CSV files;
    without it Spark would try to parse the sibling ``README.md`` and
    ``download.py`` as CSVs and pollute Bronze with junk rows.
    """
    return (
        spark.readStream.option("header", "true")
        .option("pathGlobFilter", "*.csv")
        .schema(BRONZE_TRANSACTIONS_SCHEMA)
        .csv(str(source_dir))
        .withColumn("_source_file", input_file_name())
        .withColumn("_ingest_ts", current_timestamp())
    )


def run(once: bool = True) -> StreamingQuery:
    """Kick off (and, by default, drain) the streaming ingest."""
    paths.ensure_dirs()
    spark = get_spark("bronze-streaming-ingest")
    stream = build_stream(spark, paths.RAW_DIR)

    writer = (
        stream.writeStream.format("delta")
        .option(
            "checkpointLocation",
            str(paths.CHECKPOINT_DIR / "bronze_transactions"),
        )
        .option("mergeSchema", "true")
        .outputMode("append")
    )

    # ``availableNow`` = process everything currently on disk and stop.
    # For a truly-continuous stream use ``.trigger(processingTime="30 seconds")``.
    query = (
        writer.trigger(availableNow=True)
        if once
        else writer.trigger(processingTime="30 seconds")
    ).start(str(paths.BRONZE_TRANSACTIONS))

    query.awaitTermination()
    n = spark.read.format("delta").load(str(paths.BRONZE_TRANSACTIONS)).count()
    _log.info("Streaming pass complete. Bronze row count: %d", n)
    return query


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Run forever with a 30s micro-batch (default: run-once and exit).",
    )
    args = parser.parse_args()
    run(once=not args.continuous)
