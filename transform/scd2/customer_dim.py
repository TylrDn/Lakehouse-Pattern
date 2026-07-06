"""SCD Type 2 build of ``silver.customer_dim`` from the CDC feed.

Slowly-Changing Dimensions Type 2 keeps history: every change closes the
prior row (``valid_to`` = change time, ``is_current`` = false) and opens a
new one. Databricks DLT's ``APPLY CHANGES INTO ... STORED AS SCD TYPE 2``
does this automatically; here we build the same behavior with two MERGE
statements — one to close-out and one to open new versions.

Correctness invariants (asserted by tests):
* No customer_id has two rows with ``is_current = true``.
* For each customer_id, ``valid_from`` values are strictly increasing.
* Closing rows have ``valid_to`` = the ``valid_from`` of the next version.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    sha2,
    concat_ws,
)
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from lakehouse import paths
from lakehouse.env import get_logger
from lakehouse.spark import get_spark

_log = get_logger("transform.scd2.customer_dim")

CUSTOMER_DIM_PATH = paths.SILVER_DIR / "customer_dim"

DIM_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("country", StringType(), True),
        StructField("email_domain", StringType(), True),
        StructField("row_hash", StringType(), False),
        StructField("valid_from", TimestampType(), False),
        StructField("valid_to", TimestampType(), True),
        StructField("is_current", BooleanType(), False),
    ]
)


def _initialize_if_missing(spark: SparkSession, path: Path) -> None:
    if DeltaTable.isDeltaTable(spark, str(path)):
        return
    empty = spark.createDataFrame([], DIM_SCHEMA)
    (
        empty.write.format("delta")
        .option("delta.enableChangeDataFeed", "true")
        .mode("overwrite")
        .save(str(path))
    )
    _log.info("initialized empty SCD2 dim at %s", path)


def _incoming_with_hash(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "row_hash",
        sha2(concat_ws("||", col("country"), col("email_domain")), 256),
    )


def upsert(spark: SparkSession, incoming: DataFrame) -> None:
    """Merge ``incoming`` into the SCD2 dim.

    ``incoming`` must have columns: customer_id, country, email_domain.
    """
    _initialize_if_missing(spark, CUSTOMER_DIM_PATH)
    dim = DeltaTable.forPath(spark, str(CUSTOMER_DIM_PATH))
    hashed = _incoming_with_hash(incoming)

    # 1) Close-out: any current row whose hash differs from the incoming row.
    (
        dim.alias("t")
        .merge(hashed.alias("s"), "t.customer_id = s.customer_id AND t.is_current")
        .whenMatchedUpdate(
            condition="t.row_hash != s.row_hash",
            set={
                "valid_to": "current_timestamp()",
                "is_current": lit(False),
            },
        )
        .execute()
    )

    # 2) Open new versions: rows whose hash is not present as current.
    current = (
        spark.read.format("delta")
        .load(str(CUSTOMER_DIM_PATH))
        .filter("is_current")
        .select("customer_id", "row_hash")
        .alias("cur")
    )
    new_versions = (
        hashed.alias("s")
        .join(
            current,
            (col("s.customer_id") == col("cur.customer_id"))
            & (col("s.row_hash") == col("cur.row_hash")),
            "left_anti",
        )
        .select(
            col("customer_id"),
            col("country"),
            col("email_domain"),
            col("row_hash"),
            current_timestamp().alias("valid_from"),
            lit(None).cast("timestamp").alias("valid_to"),
            lit(True).alias("is_current"),
        )
    )
    (
        new_versions.write.format("delta")
        .mode("append")
        .save(str(CUSTOMER_DIM_PATH))
    )
    _log.info("SCD2 merged %d new versions", new_versions.count())


def run() -> None:
    spark = get_spark("scd2-customer-dim")
    silver = spark.read.format("delta").load(str(paths.SILVER_TRANSACTIONS))
    # Toy derivation of the dim from Silver — real pipelines source from CRM.
    incoming = (
        silver.select("customer_id", "country")
        .withColumn("email_domain", lit("example.com"))
        .dropDuplicates(["customer_id"])
    )
    upsert(spark, incoming)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    run()
