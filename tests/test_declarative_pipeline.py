"""Tests for the DLT-analog declarative pipeline's Expectation DSL (P0-2/P1-3).

The critical behavior under test is that ``quarantine`` retains violating rows
in a rejects table (no silent data loss) and that an unknown action is rejected
at construction time rather than silently degrading to ``drop``.
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")

from pipelines.declarative_pipeline import Expectation, Step  # noqa: E402


def _make_df(spark):
    return spark.createDataFrame(
        [(1, 5), (2, -1), (3, 10)],
        ["id", "val"],
    )


# ---------------------------------------------------------------------------
# Construction-time validation (fail-closed)
# ---------------------------------------------------------------------------
def test_unknown_action_raises_at_construction():
    with pytest.raises(ValueError, match="unknown action"):
        Expectation("typo", "val > 0", action="quarentine")


@pytest.mark.parametrize("action", ["drop", "fail", "quarantine"])
def test_valid_actions_construct(action):
    exp = Expectation("ok", "val > 0", action=action)
    assert exp.action == action


# ---------------------------------------------------------------------------
# split() / apply() semantics
# ---------------------------------------------------------------------------
def test_drop_discards_violating_rows(spark):
    exp = Expectation("positive", "val > 0", action="drop")
    kept, rejected = exp.split(_make_df(spark))
    assert {r["id"] for r in kept.collect()} == {1, 3}
    assert rejected is None
    # apply() is the thin wrapper returning only kept rows.
    assert {r["id"] for r in exp.apply(_make_df(spark)).collect()} == {1, 3}


def test_quarantine_keeps_clean_and_returns_rejects(spark):
    exp = Expectation("positive", "val > 0", action="quarantine")
    kept, rejected = exp.split(_make_df(spark))
    assert {r["id"] for r in kept.collect()} == {1, 3}
    assert rejected is not None
    assert {r["id"] for r in rejected.collect()} == {2}


def test_fail_raises_on_violation(spark):
    exp = Expectation("positive", "val > 0", action="fail")
    with pytest.raises(ValueError, match="failed on 1 rows"):
        exp.split(_make_df(spark))


def test_fail_passes_when_all_clean(spark):
    clean = spark.createDataFrame([(1, 5), (2, 10)], ["id", "val"])
    exp = Expectation("positive", "val > 0", action="fail")
    kept, rejected = exp.split(clean)
    assert {r["id"] for r in kept.collect()} == {1, 2}
    assert rejected is None


def test_step_rejects_path_defaults_to_target_suffix():
    step = Step(name="s", transform=lambda _s: None, target_path="/tmp/silver/tx")
    assert step.resolved_rejects_path() == "/tmp/silver/tx_rejects"
    step2 = Step(
        name="s",
        transform=lambda _s: None,
        target_path="/tmp/silver/tx",
        rejects_path="/custom/rej",
    )
    assert step2.resolved_rejects_path() == "/custom/rej"


# ---------------------------------------------------------------------------
# End-to-end run(): quarantined rows land in the rejects table
# ---------------------------------------------------------------------------
def _seed_raw(tmp_lakehouse):
    src = tmp_lakehouse / "sample_raw"
    src.mkdir(parents=True, exist_ok=True)
    p = src / "transactions.csv"
    rows = [
        "transaction_id,customer_id,product_id,quantity,unit_price,currency,event_ts,country",
        "T-1,C-1,SKU-1,2,10.00,USD,2025-01-01 10:00:00,us",
        "T-2,C-2,SKU-1,1,5.00,USD,2025-01-01 11:00:00,us",
        "T-1,C-1,SKU-1,2,10.00,USD,2025-01-01 10:00:00,us",  # dup
        "T-3,C-3,SKU-2,1,9.99,USD,2025-01-02 09:00:00,us",  # clean
        "T-4,C-4,SKU-3,-1,9.99,USD,2025-01-02 09:00:00,us",  # neg qty -> quarantine
        "T-5,C-5,SKU-4,1,1.00,USD,not-a-date,us",  # bad ts -> quarantine
    ]
    p.write_text("\n".join(rows) + "\n")


def test_run_quarantines_bad_rows_to_rejects_table(spark, tmp_lakehouse):
    _seed_raw(tmp_lakehouse)

    from ingestion import batch_ingest
    from lakehouse import paths
    from pipelines import declarative_pipeline as dp

    batch_ingest.run()
    dp.run()

    silver = spark.read.format("delta").load(str(paths.SILVER_TRANSACTIONS))
    silver_ids = {r["transaction_id"] for r in silver.collect()}
    # Clean rows survive (dup T-1 collapsed); quarantined rows are excluded.
    assert silver_ids == {"T-1", "T-2", "T-3"}

    rejects = spark.read.format("delta").load(
        str(paths.SILVER_TRANSACTIONS) + "_rejects"
    )
    reject_ids = {r["transaction_id"] for r in rejects.collect()}
    assert {"T-4", "T-5"} <= reject_ids
    # Audit columns are present.
    assert "_expectation" in rejects.columns
    assert "_rejected_at" in rejects.columns
