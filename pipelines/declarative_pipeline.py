"""Declarative-pipeline analog for Delta Live Tables (DLT).

DLT is closed-source and only runs on Databricks. Rather than fake a DLT run,
this file demonstrates the *shape* of a DLT pipeline in OSS PySpark:

* Each layer is a pure function returning a DataFrame.
* Data-quality expectations are declared alongside the transform, with a
  policy — mirroring DLT's ``@dlt.expect_or_drop`` / ``@dlt.expect_or_fail``
  decorators. Three policies are supported:
    - ``drop``: silently filter out violating rows (DLT ``expect_or_drop``).
    - ``fail``: raise if any row violates (DLT ``expect_or_fail``).
    - ``quarantine``: keep clean rows AND append violating rows to a
      ``<target>_rejects`` Delta table for audit — mirroring the Silver
      rejects pattern in ``transform/silver_clean.py`` (DLT's rescued-data
      idea). Unlike ``drop``, no data is lost.
  An unknown policy is rejected at construction time (fail-closed) so a typo
  can never silently degrade to ``drop``.
* The runner materializes each step into a Delta table in dependency order.

Databricks-native version
-------------------------
The equivalent DLT file would import ``dlt`` and decorate each function:

    .. code-block:: python

        import dlt
        from pyspark.sql.functions import col

        @dlt.table(comment="Cleaned transactions")
        @dlt.expect_or_drop("valid_qty", "quantity > 0")
        @dlt.expect_or_drop("valid_price", "unit_price >= 0")
        def silver_transactions():
            return (
                dlt.read_stream("bronze_transactions")
                   .filter(col("transaction_id").isNotNull())
            )

DLT then handles orchestration, incremental refresh, retries, backfills, and
lineage automatically. The value we get from DLT on Databricks: expectations
become first-class metrics in the UI and lineage is auto-tracked in Unity
Catalog. In OSS we approximate this with the ``Expectation`` dataclass below.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from functools import reduce
from typing import Callable, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    to_date,
    to_timestamp,
    trim,
    lower,
)

from lakehouse import paths
from lakehouse.env import get_logger
from lakehouse.spark import get_spark

_log = get_logger("pipelines.declarative")


# ---------------------------------------------------------------------------
# Expectation DSL (mimicking DLT decorators)
# ---------------------------------------------------------------------------
VALID_ACTIONS = frozenset({"drop", "fail", "quarantine"})


@dataclass
class Expectation:
    """A DLT-style data-quality rule.

    ``action`` is validated at construction: an unknown value raises
    ``ValueError`` immediately rather than silently falling through to a
    permissive default — a typo like ``"quarentine"`` must never cost data.
    """

    name: str
    predicate: str  # a SQL predicate string, e.g. "quantity > 0"
    action: str = "drop"  # one of VALID_ACTIONS

    def __post_init__(self) -> None:
        if self.action not in VALID_ACTIONS:
            raise ValueError(
                f"Expectation '{self.name}': unknown action {self.action!r}. "
                f"Must be one of {sorted(VALID_ACTIONS)}."
            )

    def split(self, df: DataFrame) -> tuple[DataFrame, Optional[DataFrame]]:
        """Split ``df`` into (kept, rejected) according to the policy.

        - ``fail``: raise if any row violates; otherwise return (df, None).
        - ``drop``: return (clean rows, None) — violating rows are discarded.
        - ``quarantine``: return (clean rows, violating rows) so the caller can
          persist the rejects for audit.

        The three branches are exhaustive because ``__post_init__`` rejects any
        other action; there is no fall-through that could map an unknown action
        to ``drop``.
        """
        violating = df.filter(f"NOT ({self.predicate})")

        if self.action == "fail":
            n = violating.count()
            if n:
                raise ValueError(
                    f"Expectation '{self.name}' failed on {n} rows: {self.predicate}"
                )
            return df, None

        kept = df.filter(self.predicate)

        if self.action == "drop":
            dropped = violating.count()
            if dropped:
                _log.info(
                    "expectation %s dropped %d violating rows", self.name, dropped
                )
            return kept, None

        # action == "quarantine" (only remaining valid value)
        return kept, violating

    def apply(self, df: DataFrame) -> DataFrame:
        """Return only the rows kept by this expectation (rejects discarded).

        Thin wrapper over :meth:`split` for callers that don't need the
        quarantined rows (e.g. drop/fail semantics or unit tests).
        """
        return self.split(df)[0]


@dataclass
class Step:
    name: str
    transform: Callable[[SparkSession], DataFrame]
    target_path: str
    partition_by: list[str] = field(default_factory=list)
    expectations: list[Expectation] = field(default_factory=list)
    rejects_path: Optional[str] = None

    def resolved_rejects_path(self) -> str:
        """Where quarantined rows land; defaults to ``<target_path>_rejects``."""
        return self.rejects_path or f"{self.target_path.rstrip('/')}_rejects"


# ---------------------------------------------------------------------------
# Transforms — expressed as pure functions of the SparkSession
# ---------------------------------------------------------------------------
def _silver(spark: SparkSession) -> DataFrame:
    bronze = spark.read.format("delta").load(str(paths.BRONZE_TRANSACTIONS))
    return (
        bronze.select(
            col("transaction_id"),
            col("customer_id"),
            col("product_id"),
            col("quantity").cast("int").alias("quantity"),
            col("unit_price").cast("double").alias("unit_price"),
            trim(col("currency")).alias("currency"),
            to_timestamp(col("event_ts")).alias("event_ts"),
            trim(lower(col("country"))).alias("country"),
        )
        .dropDuplicates(["transaction_id"])
        .withColumn("revenue", col("quantity") * col("unit_price"))
        .withColumn("event_date", to_date(col("event_ts")))
        .withColumn("ingest_ts", current_timestamp())
    )


def _gold_daily(spark: SparkSession) -> DataFrame:
    silver = spark.read.format("delta").load(str(paths.SILVER_TRANSACTIONS))
    return (
        silver.groupBy("event_date", "country")
        .sum("revenue")
        .withColumnRenamed("sum(revenue)", "gross_revenue")
    )


def _pipeline() -> list[Step]:
    """Build the pipeline definition. Function-scoped so paths are resolved lazily."""
    return [
        Step(
            name="silver_transactions",
            transform=_silver,
            target_path=str(paths.SILVER_TRANSACTIONS),
            partition_by=["event_date"],
            expectations=[
                # A missing natural key is unrecoverable — fail the run loudly.
                Expectation("non_null_id", "transaction_id IS NOT NULL", action="fail"),
                # Business-rule violations are quarantined (kept for audit in the
                # rejects table) rather than silently dropped.
                Expectation("positive_qty", "quantity > 0", action="quarantine"),
                Expectation(
                    "non_negative_price", "unit_price >= 0", action="quarantine"
                ),
                Expectation("real_ts", "event_ts IS NOT NULL", action="quarantine"),
            ],
        ),
        Step(
            name="gold_daily_revenue",
            transform=_gold_daily,
            target_path=str(paths.GOLD_DAILY_REVENUE),
        ),
    ]


def _write_rejects(step: Step, rejects: list[DataFrame]) -> None:
    """Append quarantined rows to the step's rejects Delta table.

    Mirrors ``transform/silver_clean.py::_write_rejects``: append-only with
    ``mergeSchema`` so the audit table tolerates schema drift. Each row is
    tagged with the expectation that caught it and a rejection timestamp.
    """
    if not rejects:
        return
    combined = reduce(lambda a, b: a.unionByName(b), rejects)
    tagged = combined.withColumn("_rejected_at", current_timestamp())
    path = step.resolved_rejects_path()
    (
        tagged.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(path)
    )
    _log.info("quarantined %d rows -> %s", tagged.count(), path)


def run() -> None:
    paths.ensure_dirs()
    spark = get_spark("declarative-pipeline")

    for step in _pipeline():
        _log.info("running step: %s", step.name)
        df = step.transform(spark)
        rejects: list[DataFrame] = []
        for exp in step.expectations:
            df, rejected = exp.split(df)
            if rejected is not None:
                rejects.append(rejected.withColumn("_expectation", lit(exp.name)))
        writer = (
            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        )
        if step.partition_by:
            writer = writer.partitionBy(*step.partition_by)
        writer.save(step.target_path)
        _log.info("wrote %s -> %s", step.name, step.target_path)
        _write_rejects(step, rejects)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    run()
