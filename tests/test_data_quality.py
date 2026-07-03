"""Data-quality assertions on the Silver + Gold tables.

These are the kinds of checks you would gate a production release on:
* Silver has zero rows that violate the schema / business rules.
* Gold aggregations sum back to Silver revenue (no accidental double-count).

The tests run an end-to-end mini pipeline over ~50 rows in a temp lakehouse.
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")


def _seed_raw(tmp_lakehouse):
    """Write a tiny CSV mimicking the raw feed."""
    src = tmp_lakehouse / "sample_raw"
    src.mkdir(parents=True, exist_ok=True)
    p = src / "transactions.csv"
    rows = [
        "transaction_id,customer_id,product_id,quantity,unit_price,currency,event_ts,country",
        "T-1,C-1,SKU-1,2,10.00,USD,2025-01-01 10:00:00,us",
        "T-2,C-2,SKU-1,1,5.00,USD,2025-01-01 11:00:00,us",
        "T-1,C-1,SKU-1,2,10.00,USD,2025-01-01 10:00:00,us",  # dup
        "T-3,,SKU-2,1,9.99,USD,2025-01-02 09:00:00,us",  # null cust
        "T-4,C-3,SKU-3,-1,9.99,USD,2025-01-02 09:00:00,us",  # neg qty
        "T-5,C-4,SKU-4,1,1.00,USD,not-a-date,us",  # bad ts
    ]
    p.write_text("\n".join(rows) + "\n")
    return p


def test_end_to_end_quality(spark, tmp_lakehouse):
    _seed_raw(tmp_lakehouse)

    # Re-import after fixture reloaded the paths module.
    from ingestion import batch_ingest
    from transform import gold_aggregate, silver_clean
    from lakehouse import paths

    batch_ingest.run()
    silver_clean.run()
    gold_aggregate.run()

    silver = spark.read.format("delta").load(str(paths.SILVER_TRANSACTIONS))
    daily = spark.read.format("delta").load(str(paths.GOLD_DAILY_REVENUE))

    # (1) No null keys in Silver.
    assert silver.filter("transaction_id IS NULL OR customer_id IS NULL").count() == 0

    # (2) No non-positive quantities in Silver.
    assert silver.filter("quantity <= 0").count() == 0

    # (3) Dedup worked — T-1 appears once, and T-3/T-4/T-5 dropped.
    ids = {r["transaction_id"] for r in silver.collect()}
    assert ids == {"T-1", "T-2"}

    # (4) Gold reconciles with Silver revenue (sum invariant).
    silver_total = silver.selectExpr("sum(revenue)").first()[0]
    gold_total = daily.selectExpr("sum(gross_revenue)").first()[0]
    assert silver_total == pytest.approx(gold_total)
