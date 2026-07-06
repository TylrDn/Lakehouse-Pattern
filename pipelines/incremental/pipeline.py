"""Incremental Delta Live Tables analog — streaming + expectations metrics.

The parent ``pipelines/declarative_pipeline.py`` is *structural*: it captures
the DLT DSL shape but overwrites tables on every run. This module completes
the picture with:

* **Incremental refresh** via ``foreachBatch`` — each microbatch runs the
  same declarative step and merges (not overwrites).
* **Checkpoint state** — Spark's structured-streaming checkpoint provides
  exactly-once guarantees across restarts (DLT's ``pipelines.reset()``
  equivalent is deleting the checkpoint directory).
* **Expectations metrics** — every microbatch appends counts (`kept`,
  `dropped`, `quarantined`, `failed`) to ``expectations_metrics.jsonl`, which
  the Streamlit dashboard renders as a mini DLT UI.
* **Backfill mode** — passing ``--backfill`` sets the trigger to
  ``availableNow`` and drops the checkpoint first, mirroring DLT's
  ``FULL REFRESH``.

Databricks-native
-----------------
On DLT this whole file is a decorator stack:

.. code-block:: python

    @dlt.table(comment="Silver transactions", partition_cols=["event_date"])
    @dlt.expect_or_drop("positive_qty", "quantity > 0")
    @dlt.expect_or_fail("non_null_id", "transaction_id IS NOT NULL")
    def silver_transactions():
        return dlt.read_stream("bronze_transactions").transform(_clean)

DLT then does incremental refresh, backfill, and metrics for free.
"""

from __future__ import annotations

import argparse
import json
import shutil
import time
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import DataStreamWriter

from lakehouse import paths
from lakehouse.env import get_logger
from lakehouse.spark import get_spark
from pipelines.declarative_pipeline import Expectation

_log = get_logger("pipelines.incremental")

_METRICS_PATH = Path(__file__).resolve().parent / "expectations_metrics.jsonl"
_CHECKPOINT = paths.CHECKPOINT_DIR / "incremental_silver"


_EXPECTATIONS = [
    Expectation("non_null_id", "transaction_id IS NOT NULL", action="fail"),
    Expectation("positive_qty", "quantity > 0", action="quarantine"),
    Expectation("non_negative_price", "unit_price >= 0", action="quarantine"),
]


def _write_metrics(batch_id: int, name: str, kept: int, rejected: int) -> None:
    _METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _METRICS_PATH.open("a") as f:
        f.write(
            json.dumps(
                {
                    "ts": time.time(),
                    "batch_id": batch_id,
                    "expectation": name,
                    "kept": kept,
                    "rejected": rejected,
                }
            )
            + "\n"
        )


def _foreach_batch(df: DataFrame, batch_id: int) -> None:
    """Per-microbatch handler: apply expectations, MERGE into Silver."""
    spark = df.sparkSession
    for exp in _EXPECTATIONS:
        kept_df, rejects = exp.split(df)
        rejected_n = rejects.count() if rejects is not None else 0
        kept_n = kept_df.count()
        _write_metrics(batch_id, exp.name, kept_n, rejected_n)
        df = kept_df
        if rejects is not None and rejected_n > 0:
            (
                rejects.withColumn("_batch_id", df.sparkSession.range(1).selectExpr(f"{batch_id} as v").first().v * 1 + 0)
                .write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .save(str(paths.SILVER_DIR / "transactions_rejects"))
            )

    if not DeltaTable.isDeltaTable(spark, str(paths.SILVER_TRANSACTIONS)):
        (
            df.write.format("delta")
            .partitionBy("event_date")
            .option("delta.enableChangeDataFeed", "true")
            .save(str(paths.SILVER_TRANSACTIONS))
        )
        return

    tgt = DeltaTable.forPath(spark, str(paths.SILVER_TRANSACTIONS))
    (
        tgt.alias("t")
        .merge(df.alias("s"), "t.transaction_id = s.transaction_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def _stream_source(spark: SparkSession) -> DataFrame:
    """Streaming read of Bronze — emits every new microbatch of appends."""
    return (
        spark.readStream.format("delta")
        .load(str(paths.BRONZE_TRANSACTIONS))
    )


def start(backfill: bool = False, once: bool = False) -> None:
    if backfill and _CHECKPOINT.exists():
        _log.warning("FULL REFRESH: dropping checkpoint %s", _CHECKPOINT)
        shutil.rmtree(_CHECKPOINT)

    spark = get_spark("incremental-dlt-analog")
    stream = _stream_source(spark)
    writer: DataStreamWriter = (
        stream.writeStream.foreachBatch(_foreach_batch)
        .option("checkpointLocation", str(_CHECKPOINT))
    )
    trigger = {"availableNow": True} if (backfill or once) else {"processingTime": "30 seconds"}
    query = writer.trigger(**trigger).start()
    _log.info(
        "incremental pipeline running (backfill=%s, once=%s, checkpoint=%s)",
        backfill,
        once,
        _CHECKPOINT,
    )
    query.awaitTermination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backfill", action="store_true")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    start(backfill=args.backfill, once=args.once)
