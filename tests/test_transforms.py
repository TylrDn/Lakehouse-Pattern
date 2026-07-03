"""Unit tests for the Silver / Gold transform logic.

We keep these tests small + fast — they exercise the *logic*, not the file IO
plumbing, by constructing tiny in-memory DataFrames and asserting on the
outputs.
"""

from __future__ import annotations

from datetime import datetime

import pytest

pytest.importorskip("pyspark")


def _make_bronze(spark):
    rows = [
        ("T-1", "C-1", "SKU-1", "2", "10.00", "USD", "2025-01-01 10:00:00", "us"),
        ("T-2", "C-2", "SKU-1", "1", "5.00", "USD", "2025-01-01 11:00:00", " US "),
        # duplicate T-1 (should be deduped)
        ("T-1", "C-1", "SKU-1", "2", "10.00", "USD", "2025-01-01 10:00:00", "us"),
        # bad row: null customer_id
        ("T-3", "",    "SKU-2", "1", "9.99",  "USD", "2025-01-02 09:00:00", "us"),
        # bad row: negative qty
        ("T-4", "C-3", "SKU-3", "-1", "9.99", "USD", "2025-01-02 09:00:00", "us"),
        # bad row: malformed ts
        ("T-5", "C-4", "SKU-4", "1", "1.00", "USD", "not-a-date", "us"),
    ]
    cols = [
        "transaction_id",
        "customer_id",
        "product_id",
        "quantity",
        "unit_price",
        "currency",
        "event_ts",
        "country",
    ]
    return spark.createDataFrame(rows, cols)


def test_cast_and_normalize_lowercases_country(spark):
    from transform.silver_clean import _cast_and_normalize

    bronze = _make_bronze(spark)
    result = _cast_and_normalize(bronze).collect()
    countries = {r.country for r in result}
    assert countries == {"us"}, "Countries should be trimmed + lowercased"


def test_quality_gates_route_bad_rows(spark):
    from transform.silver_clean import _apply_quality_gates, _cast_and_normalize

    typed = _cast_and_normalize(_make_bronze(spark))
    clean, rejects = _apply_quality_gates(typed)

    clean_ids = {r.transaction_id for r in clean.collect()}
    reject_ids = {r.transaction_id for r in rejects.collect()}

    # T-1 (x2) and T-2 are clean; T-3/T-4/T-5 are rejected.
    assert clean_ids == {"T-1", "T-2"}
    assert reject_ids == {"T-3", "T-4", "T-5"}


def test_enrich_dedupes_and_computes_revenue(spark):
    from transform.silver_clean import _apply_quality_gates, _cast_and_normalize, _enrich

    typed = _cast_and_normalize(_make_bronze(spark))
    clean, _ = _apply_quality_gates(typed)
    enriched = _enrich(clean).collect()

    ids = [r.transaction_id for r in enriched]
    assert len(ids) == len(set(ids)), "dedup must remove duplicate transaction_id"

    by_id = {r.transaction_id: r for r in enriched}
    assert by_id["T-1"].revenue == pytest.approx(20.0)
    assert by_id["T-2"].revenue == pytest.approx(5.0)


def test_gold_daily_revenue_aggregation(spark):
    from pyspark.sql import Row

    from transform.gold_aggregate import build_daily_revenue

    silver = spark.createDataFrame(
        [
            Row(transaction_id="T-1", customer_id="C-1", event_ts=datetime(2025, 1, 1, 10),
                country="us", revenue=20.0),
            Row(transaction_id="T-2", customer_id="C-2", event_ts=datetime(2025, 1, 1, 11),
                country="us", revenue=5.0),
            Row(transaction_id="T-3", customer_id="C-1", event_ts=datetime(2025, 1, 2, 12),
                country="gb", revenue=8.0),
        ]
    )
    result = {(str(r["event_date"]), r["country"]): r for r in build_daily_revenue(silver).collect()}
    assert result[("2025-01-01", "us")].gross_revenue == pytest.approx(25.0)
    assert result[("2025-01-01", "us")].order_count == 2
    assert result[("2025-01-01", "us")].unique_customers == 2
    assert result[("2025-01-02", "gb")].gross_revenue == pytest.approx(8.0)
