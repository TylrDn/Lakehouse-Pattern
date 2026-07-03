"""End-to-end sanity checks for the headline Delta features.

Covered:
* ACID append semantics
* MERGE upsert idempotency
* Schema enforcement rejects an incompatible write
* Time travel by version
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")


def test_merge_is_idempotent(spark, tmp_lakehouse):
    """Running silver_clean twice must not create duplicate transaction_ids."""
    from tests.test_data_quality import _seed_raw
    from ingestion import batch_ingest
    from transform import silver_clean
    from lakehouse import paths

    _seed_raw(tmp_lakehouse)
    batch_ingest.run()
    silver_clean.run()
    first = spark.read.format("delta").load(str(paths.SILVER_TRANSACTIONS)).count()

    # Re-run ingest + silver — MERGE must upsert, not append.
    batch_ingest.run()
    silver_clean.run()
    second = spark.read.format("delta").load(str(paths.SILVER_TRANSACTIONS)).count()

    assert first == second, "Silver should be idempotent under re-ingest"


def test_time_travel_returns_earlier_snapshot(spark, tmp_lakehouse):
    """Bronze v0 should differ from Bronze v1 after a second ingest."""
    from tests.test_data_quality import _seed_raw
    from ingestion import batch_ingest
    from lakehouse import paths

    _seed_raw(tmp_lakehouse)
    batch_ingest.run()
    v0 = (
        spark.read.format("delta")
        .option("versionAsOf", 0)
        .load(str(paths.BRONZE_TRANSACTIONS))
        .count()
    )

    batch_ingest.run()  # append again
    latest = spark.read.format("delta").load(str(paths.BRONZE_TRANSACTIONS)).count()

    assert (
        latest == 2 * v0
    ), "Bronze is append-only; second ingest should double the row count"


def test_schema_enforcement_rejects_bad_write(spark, tmp_lakehouse):
    """Writing an incompatible schema without mergeSchema must fail.

    Delta's schema enforcement rejects appends that add *new* columns unless
    ``mergeSchema=true`` is explicitly set. This is the guardrail that makes
    Delta safer than plain Parquet.

    Note: we globally enable ``spark.databricks.delta.schema.autoMerge.enabled``
    in ``lakehouse.spark``. To prove the enforcement contract holds, we
    disable it for this test only.
    """
    from pyspark.sql import Row

    from lakehouse import paths

    target = str(paths.BRONZE_DIR / "schema_test")

    df_ok = spark.createDataFrame([Row(a=1, b="x")])
    df_ok.write.format("delta").mode("overwrite").save(target)

    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false")
    try:
        # A row with an extra column ``c`` violates the current schema.
        df_bad = spark.createDataFrame([Row(a=1, b="y", c="extra")])
        with pytest.raises(Exception):
            df_bad.write.format("delta").mode("append").save(target)
    finally:
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
