"""APPLY CHANGES INTO analog — CDC merge from Bronze CDF into Silver.

Databricks Delta Live Tables provides ``APPLY CHANGES INTO`` which handles
inserts, updates, and deletes based on a change-feed operation column. On
OSS Delta Lake 3.x we get the same behavior from:

* ``delta.enableChangeDataFeed = true`` on the source table (Bronze).
* ``spark.read.format("delta").option("readChangeData", "true")`` to pull
  the change feed.
* ``DeltaTable.merge(...)`` with ``whenMatchedDelete``, ``whenMatchedUpdate``,
  and ``whenNotMatchedInsert`` clauses.

Ordering guarantee: we sort the change batch by ``_commit_version`` so an
update-then-delete for the same key applies as delete (Databricks does this
implicitly).
"""

from __future__ import annotations

import argparse
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

from lakehouse import paths
from lakehouse.env import get_logger
from lakehouse.spark import get_spark

_log = get_logger("transform.cdc")


def read_change_feed(
    spark: SparkSession,
    source_path: str,
    starting_version: Optional[int] = None,
) -> DataFrame:
    """Read the Change Data Feed since ``starting_version`` (default: earliest)."""
    reader = spark.read.format("delta").option("readChangeData", "true")
    if starting_version is not None:
        reader = reader.option("startingVersion", starting_version)
    else:
        reader = reader.option("startingVersion", 0)
    return reader.load(source_path)


def _dedupe_by_key(cdf: DataFrame, key_col: str) -> DataFrame:
    """Keep only the latest change per key by (_commit_version, _commit_timestamp)."""
    w = Window.partitionBy(key_col).orderBy(
        col("_commit_version").desc(), col("_commit_timestamp").desc()
    )
    return cdf.withColumn("_rn", row_number().over(w)).filter("_rn = 1").drop("_rn")


def apply_changes_into(
    target_path: str,
    changes: DataFrame,
    key_col: str = "transaction_id",
) -> None:
    """Merge a CDF batch into the target Delta table (upsert + delete).

    ``changes`` must contain a ``_change_type`` column with values in
    ``{insert, update_postimage, delete}``. Row-level values for updates are
    taken from the post-image; pre-images are ignored.
    """
    latest = _dedupe_by_key(
        changes.filter("_change_type != 'update_preimage'"), key_col
    )

    target = DeltaTable.forPath(latest.sparkSession, target_path)
    (
        target.alias("t")
        .merge(latest.alias("s"), f"t.{key_col} = s.{key_col}")
        .whenMatchedDelete(condition="s._change_type = 'delete'")
        .whenMatchedUpdateAll(condition="s._change_type = 'update_postimage'")
        .whenNotMatchedInsertAll(
            condition="s._change_type IN ('insert', 'update_postimage')"
        )
        .execute()
    )
    _log.info("APPLY CHANGES INTO %s: %d rows merged", target_path, latest.count())


def enable_cdf_on_bronze(spark: SparkSession) -> None:
    """Idempotently turn on Change Data Feed for the Bronze table."""
    spark.sql(
        f"ALTER TABLE delta.`{paths.BRONZE_TRANSACTIONS}` "
        "SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
    )


def run(starting_version: Optional[int] = None) -> None:
    spark = get_spark("cdc-apply-changes")
    enable_cdf_on_bronze(spark)
    cdf = read_change_feed(spark, str(paths.BRONZE_TRANSACTIONS), starting_version)
    apply_changes_into(str(paths.SILVER_TRANSACTIONS), cdf)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-version", type=int, default=None)
    args = parser.parse_args()
    run(starting_version=args.from_version)
