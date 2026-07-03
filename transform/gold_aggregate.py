"""Silver -> Gold: business marts.

Gold tables are the "product" — everything above this line exists to make
these tables trustworthy and cheap to query. Two marts are built:

1. ``daily_revenue`` — grain: (event_date, country). Powers the Streamlit
   dashboard and is the training source for the ML model.
2. ``customer_ltv`` — grain: (customer_id). Powers the RAG customer-facing
   demo.

Design notes
------------
* Marts are overwritten each run — they are deterministic functions of Silver
  so idempotency is trivial. If Silver retention grew large we would switch
  to incremental MERGE on the aggregation key.
* We register the marts as SQL views on the Spark session at the end of the
  job so downstream notebooks / dashboards can query them by name without
  needing to remember paths. On Databricks these would be Unity Catalog
  tables under ``main.lakehouse_pattern.gold_*``.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    max as smax,
    min as smin,
    sum as ssum,
    to_date,
)

from lakehouse import paths
from lakehouse.env import get_logger
from lakehouse.spark import get_spark

_log = get_logger("transform.gold_aggregate")


def build_daily_revenue(silver: DataFrame) -> DataFrame:
    """Per-day, per-country revenue + order counts."""
    return (
        silver.groupBy(to_date(col("event_ts")).alias("event_date"), col("country"))
        .agg(
            ssum("revenue").alias("gross_revenue"),
            count("transaction_id").alias("order_count"),
            countDistinct("customer_id").alias("unique_customers"),
        )
        .orderBy("event_date", "country")
    )


def build_customer_ltv(silver: DataFrame) -> DataFrame:
    """Lifetime-to-date revenue + activity window per customer."""
    return (
        silver.groupBy("customer_id")
        .agg(
            ssum("revenue").alias("lifetime_revenue"),
            count("transaction_id").alias("orders"),
            smin("event_ts").alias("first_seen"),
            smax("event_ts").alias("last_seen"),
        )
        .orderBy(col("lifetime_revenue").desc())
    )


def _write(df: DataFrame, path) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(path))
    )


def _register_views(spark: SparkSession) -> None:
    spark.sql(
        f"CREATE OR REPLACE TEMP VIEW gold_daily_revenue AS "
        f"SELECT * FROM delta.`{paths.GOLD_DAILY_REVENUE}`"
    )
    spark.sql(
        f"CREATE OR REPLACE TEMP VIEW gold_customer_ltv AS "
        f"SELECT * FROM delta.`{paths.GOLD_CUSTOMER_LTV}`"
    )


def run() -> None:
    paths.ensure_dirs()
    spark = get_spark("gold-aggregate")
    silver = spark.read.format("delta").load(str(paths.SILVER_TRANSACTIONS))

    _write(build_daily_revenue(silver), paths.GOLD_DAILY_REVENUE)
    _write(build_customer_ltv(silver), paths.GOLD_CUSTOMER_LTV)
    _register_views(spark)

    _log.info(
        "Gold marts written: daily_revenue=%s customer_ltv=%s",
        paths.GOLD_DAILY_REVENUE,
        paths.GOLD_CUSTOMER_LTV,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    run()
