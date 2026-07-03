"""Bronze -> Silver: cleanse, enforce, MERGE.

Silver is the *first* place we trust the data. It's therefore the layer where
the majority of Delta Lake's differentiators earn their keep:

* **Schema enforcement** — the target table's schema rejects malformed writes.
* **MERGE upserts** — idempotent re-runs; if we reprocess Bronze we don't get
  duplicates in Silver.
* **Quality gates** — bad rows are quarantined into a ``_rejects`` Delta table
  rather than silently dropped, so we can audit them.
* **Partitioning + Z-ORDER** — we partition by ``event_date`` (low cardinality,
  predictable filter column) and Z-ORDER by ``customer_id`` (high-cardinality,
  frequently filtered column). This is the canonical Databricks tuning
  recipe.
* **OPTIMIZE + VACUUM** — file compaction and retention are demonstrated at
  the end of the job.

Every step below is commented with the *why*.
"""

from __future__ import annotations

import argparse

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    lower,
    to_date,
    to_timestamp,
    trim,
)

from lakehouse import paths
from lakehouse.env import get_logger
from lakehouse.schemas import SILVER_TRANSACTIONS_SCHEMA
from lakehouse.spark import get_spark

_log = get_logger("transform.silver_clean")


def _rejects_table() -> str:
    """Return the current rejects-table path (resolved lazily for testability)."""
    return str(paths.SILVER_DIR / "transactions_rejects")


# ---------------------------------------------------------------------------
# Cleansing rules
# ---------------------------------------------------------------------------
def _cast_and_normalize(df: DataFrame) -> DataFrame:
    """Cast string columns to their real types + normalize categoricals.

    We use ``to_timestamp`` / ``to_date`` in a permissive mode: malformed
    values become NULL, which we then route to the rejects table below. This
    is safer than throwing at cast time because Spark's failure mode on cast
    errors is "kill the whole task", which would take down the whole batch.
    """
    return df.select(
        col("transaction_id"),
        col("customer_id"),
        col("product_id"),
        col("quantity").cast("int").alias("quantity"),
        col("unit_price").cast("double").alias("unit_price"),
        trim(col("currency")).alias("currency"),
        to_timestamp(col("event_ts")).alias("event_ts"),
        trim(lower(col("country"))).alias("country"),
    )


def _apply_quality_gates(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split into (clean, rejects) based on business rules.

    Rules — each intentionally simple so the logic is auditable:
      * ``transaction_id`` must be non-null.
      * ``event_ts`` must be a real timestamp (i.e. cast succeeded).
      * ``quantity`` must be > 0 (returns/refunds live in a separate feed;
        for this demo we quarantine them).
      * ``unit_price`` must be >= 0.
      * ``customer_id`` must be non-null and non-empty.
    """
    is_valid = (
        col("transaction_id").isNotNull()
        & col("event_ts").isNotNull()
        & col("quantity").isNotNull()
        & (col("quantity") > 0)
        & col("unit_price").isNotNull()
        & (col("unit_price") >= 0)
        & col("customer_id").isNotNull()
        & (col("customer_id") != "")
    )

    clean = df.filter(is_valid)
    rejects = df.filter(~is_valid).withColumn("_rejected_at", current_timestamp())
    return clean, rejects


def _enrich(df: DataFrame) -> DataFrame:
    """Add derived columns needed by every downstream mart."""
    return (
        df.dropDuplicates(["transaction_id"])  # transaction_id is the natural key
        .withColumn("revenue", col("quantity") * col("unit_price"))
        .withColumn("event_date", to_date(col("event_ts")))
        .withColumn("ingest_ts", current_timestamp())
    )


# ---------------------------------------------------------------------------
# Delta MERGE — idempotent upsert into Silver
# ---------------------------------------------------------------------------
def _initial_write(spark: SparkSession, df: DataFrame) -> None:
    """Bootstrap the Silver table with partitioning + explicit schema.

    We create the table with the ``PARTITIONED BY (event_date)`` clause so the
    partition columns are set at DDL time; changing partitioning later would
    require a rewrite.
    """
    # Ensure schema alignment. If the incoming df is missing a column we
    # deliberately want the write to fail loudly. ``event_date`` is the
    # partition column; the remaining columns must appear in schema order.
    aligned = df.select(
        *[c.name for c in SILVER_TRANSACTIONS_SCHEMA.fields], "event_date"
    )
    (
        aligned.write.format("delta")
        .mode("overwrite")
        .partitionBy("event_date")
        .option("overwriteSchema", "true")
        .save(str(paths.SILVER_TRANSACTIONS))
    )


def _merge_upsert(spark: SparkSession, updates: DataFrame) -> None:
    """MERGE ``updates`` into Silver on the transaction_id natural key.

    This is what makes the pipeline replay-safe: if we re-ingest a Bronze
    partition, MERGE will match on ``transaction_id`` and update-in-place
    instead of appending duplicates.
    """
    target = DeltaTable.forPath(spark, str(paths.SILVER_TRANSACTIONS))
    (
        target.alias("t")
        .merge(
            updates.alias("s"),
            "t.transaction_id = s.transaction_id",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def _write_rejects(rejects: DataFrame) -> None:
    """Append quality-gate failures to a dedicated Delta table."""
    (
        rejects.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(_rejects_table())
    )


# ---------------------------------------------------------------------------
# OPTIMIZE / Z-ORDER / VACUUM
# ---------------------------------------------------------------------------
def _optimize_and_vacuum(spark: SparkSession) -> None:
    """Compact small files + physically colocate frequent filter columns.

    - ``OPTIMIZE`` merges many small files into ~1 GB target files, dropping
      task overhead and metadata bloat.
    - ``ZORDER BY (customer_id)`` clusters data on a high-cardinality column
      so point-lookups + range scans on ``customer_id`` prune far fewer
      files.
    - ``VACUUM`` reclaims storage for files no longer referenced by any
      snapshot older than the retention threshold. We use 168h (7d) here to
      demonstrate the API without hurting time-travel demos.
    """
    target = paths.SILVER_TRANSACTIONS
    spark.sql(f"OPTIMIZE delta.`{target}` ZORDER BY (customer_id)")
    # Retention shorter than 168h requires disabling a safety check — we keep
    # the default here so time-travel-to-yesterday demos still work.
    spark.sql(f"VACUUM delta.`{target}` RETAIN 168 HOURS")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------
def run() -> None:
    paths.ensure_dirs()
    spark = get_spark("silver-clean")

    bronze = spark.read.format("delta").load(str(paths.BRONZE_TRANSACTIONS))
    typed = _cast_and_normalize(bronze)
    clean, rejects = _apply_quality_gates(typed)
    enriched = _enrich(clean)

    if DeltaTable.isDeltaTable(spark, str(paths.SILVER_TRANSACTIONS)):
        _merge_upsert(spark, enriched)
    else:
        _initial_write(spark, enriched)

    _write_rejects(
        rejects.withColumn("_bronze_source", lit(str(paths.BRONZE_TRANSACTIONS)))
    )
    _optimize_and_vacuum(spark)

    n_silver = spark.read.format("delta").load(str(paths.SILVER_TRANSACTIONS)).count()
    rejects_path = _rejects_table()
    n_rejects = (
        spark.read.format("delta").load(rejects_path).count()
        if DeltaTable.isDeltaTable(spark, rejects_path)
        else 0
    )
    _log.info("Silver rows: %d. Rejects rows (cumulative): %d.", n_silver, n_rejects)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    run()
