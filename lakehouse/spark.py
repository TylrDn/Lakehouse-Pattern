"""Spark + Delta bootstrap.

We build the SparkSession in ONE place so every layer gets the exact same
Delta Lake configuration. This matters because Delta features like MERGE,
schema evolution, and ``_delta_log`` are only enabled when the Delta SQL
extensions and catalog are registered on the session — a missed config here is
a class of silent bug you learn to avoid the hard way.

On Databricks you would NOT call this — the runtime already provides a Spark
session with Delta configured. We simply mirror that config locally so the
same PySpark code runs in both environments.
"""

from __future__ import annotations

import os
from typing import Optional

from pyspark.sql import SparkSession

from lakehouse.env import check_prerequisites, get_logger

_log = get_logger("lakehouse.spark")


def get_spark(
    app_name: str = "lakehouse-pattern", shuffle_partitions: int = 4
) -> SparkSession:
    """Return a Delta-enabled SparkSession, creating it on first call.

    Parameters
    ----------
    app_name:
        Shows up in the Spark UI; make it specific per job so multi-app runs
        are easy to distinguish.
    shuffle_partitions:
        On a laptop 4 is faster than the default 200 (fewer tiny tasks).
        On a real cluster set this to ~2-3x total cores.
    """
    # Detect whether we are already running inside Databricks / a Spark shell
    # that provides a pre-configured session — reuse it if so.
    active: Optional[SparkSession] = SparkSession.getActiveSession()
    if active is not None:
        return active

    # Preflight: fail fast with an actionable message if Java is missing / old.
    # Skipped inside Databricks (where an active session exists) and can be
    # bypassed with LAKEHOUSE_SKIP_PREFLIGHT=1 in unusual setups.
    if os.environ.get("LAKEHOUSE_SKIP_PREFLIGHT") != "1":
        check_prerequisites()

    builder = (
        SparkSession.builder.appName(app_name)
        # Delta Lake extension + catalog — required for MERGE, time travel, etc.
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Cheaper local execution.
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        # Enable schema autoMerge for MERGE INTO (opt-in for safety in prod).
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        # Vectorized reader helps small local runs.
        .config("spark.sql.parquet.enableVectorizedReader", "true")
    )

    # Only add the Delta package coordinate when the JAR isn't already on the
    # classpath (i.e. plain ``pip install delta-spark`` on a laptop).
    if "DELTA_PACKAGE_ADDED" not in os.environ:
        builder = builder.config(
            "spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0"
        )
        os.environ["DELTA_PACKAGE_ADDED"] = "1"

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    _log.info(
        "Spark %s ready (app=%s, shuffle=%d)",
        spark.version,
        app_name,
        shuffle_partitions,
    )
    return spark


if __name__ == "__main__":
    s = get_spark("smoke-test")
    print(f"Spark {s.version} started. Delta configured.")
    s.stop()
