"""Test for the Structured Streaming (Auto Loader analog) ingest (P1-4).

CI previously only exercised the batch ingest path. This drives
``streaming_ingest.run(once=True)`` (Trigger.AvailableNow) over a seeded
landing zone and asserts:
* all seeded CSV rows land in Bronze,
* the ``pathGlobFilter=*.csv`` scoping excludes non-CSV siblings, and
* the streaming checkpoint directory is created (durable exactly-once state).
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")


def test_streaming_ingest_appends_csv_and_excludes_non_csv(spark, tmp_lakehouse):
    from ingestion import streaming_ingest
    from lakehouse import paths

    raw = paths.RAW_DIR
    raw.mkdir(parents=True, exist_ok=True)
    (raw / "transactions.csv").write_text(
        "\n".join(
            [
                "transaction_id,customer_id,product_id,quantity,unit_price,currency,event_ts,country",
                "T-1,C-1,SKU-1,2,10.00,USD,2025-01-01 10:00:00,us",
                "T-2,C-2,SKU-1,1,5.00,USD,2025-01-01 11:00:00,us",
                "T-3,C-3,SKU-2,3,2.50,USD,2025-01-02 09:00:00,gb",
            ]
        )
        + "\n"
    )
    # A non-CSV sibling that must NOT be ingested (pathGlobFilter=*.csv). If the
    # glob filter regressed, Spark would try to parse this as a row.
    (raw / "README.md").write_text("# not data\n")

    streaming_ingest.run(once=True)

    bronze = spark.read.format("delta").load(str(paths.BRONZE_TRANSACTIONS))
    ids = {r["transaction_id"] for r in bronze.collect()}
    assert ids == {"T-1", "T-2", "T-3"}, "only the CSV rows should be ingested"

    checkpoint = paths.CHECKPOINT_DIR / "bronze_transactions"
    assert checkpoint.exists(), "streaming checkpoint dir must be created"
