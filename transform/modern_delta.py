"""Modern Delta features — liquid clustering, deletion vectors, MERGE tuning.

This module is a thin retrofit for the existing Silver table. It is safe to
run once (or in every job) and demonstrates the *current* Databricks-
recommended tuning recipe, superseding OPTIMIZE + ZORDER.

References
----------
* Liquid clustering: Delta Lake 3.0+, ``CLUSTER BY``. Replaces partitioning
  + ZORDER with a single write-side hint that Databricks (and OSS Delta)
  physically re-organizes to match. No partition explosion, better than
  ZORDER on multi-column filters, and re-clusterable without a rewrite.
* Deletion vectors: Delta Lake 3.0+, ``delta.enableDeletionVectors``.
  MERGE/UPDATE/DELETE mark rows via bitmap side-files instead of rewriting
  data files. Dramatically cheaper on wide tables — but reads must be
  compatible (Photon, delta-rs ≥ 0.16, delta-spark ≥ 3.0).
* MERGE tuning: ``delta.autoOptimize.optimizeWrite`` and
  ``delta.autoOptimize.autoCompact`` — enabled here as table properties.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

from lakehouse import paths
from lakehouse.env import get_logger
from lakehouse.spark import get_spark

_log = get_logger("transform.modern_delta")


_CLUSTER_KEYS = ("event_date", "customer_id")


def enable_modern_features(spark: SparkSession, table_path: str) -> None:
    """Turn on deletion vectors + auto-optimize on an existing Delta table."""
    spark.sql(
        f"""
        ALTER TABLE delta.`{table_path}` SET TBLPROPERTIES (
          'delta.enableDeletionVectors' = 'true',
          'delta.autoOptimize.optimizeWrite' = 'true',
          'delta.autoOptimize.autoCompact' = 'true',
          'delta.enableChangeDataFeed' = 'true'
        )
        """
    )
    _log.info("modern Delta table properties applied to %s", table_path)


def cluster_by(spark: SparkSession, table_path: str, keys=_CLUSTER_KEYS) -> None:
    """Apply liquid clustering. Idempotent — re-declaring keys is a no-op."""
    key_list = ", ".join(keys)
    spark.sql(
        f"ALTER TABLE delta.`{table_path}` CLUSTER BY ({key_list})"
    )
    _log.info("CLUSTER BY (%s) declared on %s", key_list, table_path)


def optimize(spark: SparkSession, table_path: str) -> None:
    """Trigger a liquid-clustering rebalance. Cheap on already-clustered tables."""
    spark.sql(f"OPTIMIZE delta.`{table_path}`")
    _log.info("OPTIMIZE ran on %s", table_path)


def run() -> None:
    spark = get_spark("modern-delta")
    for path in (paths.BRONZE_TRANSACTIONS, paths.SILVER_TRANSACTIONS):
        enable_modern_features(spark, str(path))
        if path == paths.SILVER_TRANSACTIONS:
            cluster_by(spark, str(path))
            optimize(spark, str(path))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    run()
