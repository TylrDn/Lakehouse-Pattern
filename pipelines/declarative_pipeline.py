"""Declarative-pipeline analog for Delta Live Tables (DLT).

DLT is closed-source and only runs on Databricks. Rather than fake a DLT run,
this file demonstrates the *shape* of a DLT pipeline in OSS PySpark:

* Each layer is a pure function returning a DataFrame.
* Data-quality expectations are declared alongside the transform, with a
  policy (``drop``, ``quarantine``, ``fail``) — mirroring DLT's
  ``@dlt.expect_or_drop`` / ``@dlt.expect_or_fail`` decorators.
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
from typing import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date, to_timestamp, trim, lower

from lakehouse import paths
from lakehouse.spark import get_spark


# ---------------------------------------------------------------------------
# Expectation DSL (mimicking DLT decorators)
# ---------------------------------------------------------------------------
@dataclass
class Expectation:
    """A DLT-style data-quality rule."""

    name: str
    predicate: str  # a SQL predicate string, e.g. "quantity > 0"
    action: str = "drop"  # one of {"drop", "fail"}

    def apply(self, df: DataFrame) -> DataFrame:
        # Count violations; either drop them or raise.
        violations = df.filter(f"NOT ({self.predicate})").count()
        if violations == 0:
            return df
        if self.action == "fail":
            raise ValueError(
                f"Expectation '{self.name}' failed on {violations} rows: {self.predicate}"
            )
        print(f"  [expectation:{self.name}] dropping {violations} violating rows")
        return df.filter(self.predicate)


@dataclass
class Step:
    name: str
    transform: Callable[[SparkSession], DataFrame]
    target_path: str
    partition_by: list[str] = field(default_factory=list)
    expectations: list[Expectation] = field(default_factory=list)


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
    return silver.groupBy("event_date", "country").sum("revenue").withColumnRenamed(
        "sum(revenue)", "gross_revenue"
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
                Expectation("non_null_id", "transaction_id IS NOT NULL", action="fail"),
                Expectation("positive_qty", "quantity > 0"),
                Expectation("non_negative_price", "unit_price >= 0"),
                Expectation("real_ts", "event_ts IS NOT NULL"),
            ],
        ),
        Step(
            name="gold_daily_revenue",
            transform=_gold_daily,
            target_path=str(paths.GOLD_DAILY_REVENUE),
        ),
    ]


def run() -> None:
    paths.ensure_dirs()
    spark = get_spark("declarative-pipeline")

    for step in _pipeline():
        print(f"[pipeline] running step: {step.name}")
        df = step.transform(spark)
        for exp in step.expectations:
            df = exp.apply(df)
        writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if step.partition_by:
            writer = writer.partitionBy(*step.partition_by)
        writer.save(step.target_path)
        print(f"[pipeline] wrote {step.name} -> {step.target_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    run()
