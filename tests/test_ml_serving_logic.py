"""Pure-logic unit tests for ML + serving helpers (P1-5).

These deliberately avoid Spark and the heavy ML stack (mlflow, torch, chromadb)
by testing functions whose modules now import those lazily. The file/function
names avoid "rag"/"mlflow" so they run in the fast CI lane rather than being
excluded by ``-k "not rag and not mlflow"``.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd

from ml.register_model import _best_run
from ml.train_model import engineer_features
from ml.rag_demo.rag_pipeline import compose_answer
from serving.metrics import compute_kpis, revenue_by_country, top_customers


# ---------------------------------------------------------------------------
# RAG answer composition (template renderer; no embeddings / LLM)
# ---------------------------------------------------------------------------
def test_compose_answer_grounds_in_top_hit():
    hits = [
        {
            "text": "Customer C-1 ...",
            "distance": 0.12,
            "metadata": {"customer_id": "C-1", "orders": 9, "lifetime_revenue": 1234.5},
        },
        {
            "text": "Customer C-2 ...",
            "distance": 0.34,
            "metadata": {"customer_id": "C-2", "orders": 3, "lifetime_revenue": 200.0},
        },
    ]
    answer = compose_answer("Who is most valuable?", hits)
    assert "Who is most valuable?" in answer
    assert "customer C-1" in answer  # grounded in the closest (first) hit
    assert "1234.50" in answer


def test_compose_answer_handles_no_hits():
    assert compose_answer("anything", []) == (
        "I couldn't find any matching customers in Gold."
    )


# ---------------------------------------------------------------------------
# Model-registry best-run selection (MlflowClient mocked)
# ---------------------------------------------------------------------------
def test_best_run_selects_by_lowest_mae():
    client = MagicMock()
    experiment = MagicMock()
    experiment.experiment_id = "42"
    client.get_experiment_by_name.return_value = experiment
    best = MagicMock()
    client.search_runs.return_value = [best]

    assert _best_run(client) is best
    _, kwargs = client.search_runs.call_args
    assert kwargs["order_by"] == ["metrics.mae ASC"]
    assert kwargs["max_results"] == 1


def test_best_run_returns_none_without_experiment():
    client = MagicMock()
    client.get_experiment_by_name.return_value = None
    assert _best_run(client) is None
    client.search_runs.assert_not_called()


def test_best_run_returns_none_when_no_runs():
    client = MagicMock()
    experiment = MagicMock()
    experiment.experiment_id = "42"
    client.get_experiment_by_name.return_value = experiment
    client.search_runs.return_value = []
    assert _best_run(client) is None


# ---------------------------------------------------------------------------
# Feature engineering (pure pandas)
# ---------------------------------------------------------------------------
def test_engineer_features_lags_and_rolling():
    days = pd.date_range("2025-01-01", periods=10, freq="D")
    df = pd.DataFrame(
        {
            "event_date": days.astype(str),
            "country": ["us"] * 10,
            "gross_revenue": [float((i + 1) * 10) for i in range(10)],  # 10..100
            "order_count": list(range(1, 11)),
            "unique_customers": list(range(1, 11)),
        }
    )
    out = engineer_features(df)

    # lag_7 is the binding constraint: only indices >= 7 survive dropna.
    assert set(out["gross_revenue"]) == {80.0, 90.0, 100.0}
    for col in ("lag_1", "lag_7", "rolling_3", "day_of_week"):
        assert col in out.columns

    row = out[out["gross_revenue"] == 80.0].iloc[0]
    assert row["lag_1"] == 70.0  # previous day
    assert row["lag_7"] == 10.0  # seven days earlier
    assert row["rolling_3"] == 60.0  # mean of shifted 50,60,70
    assert row["day_of_week"] == pd.Timestamp("2025-01-08").dayofweek


# ---------------------------------------------------------------------------
# Serving KPI math (pure pandas)
# ---------------------------------------------------------------------------
def _serving_frames():
    daily = pd.DataFrame(
        {
            "event_date": ["2025-01-01", "2025-01-01", "2025-01-02"],
            "country": ["us", "gb", "us"],
            "gross_revenue": [100.0, 50.0, 25.0],
            "order_count": [3, 2, 1],
        }
    )
    ltv = pd.DataFrame(
        {
            "customer_id": ["c1", "c2", "c3"],
            "lifetime_revenue": [500.0, 300.0, 100.0],
            "orders": [5, 3, 1],
        }
    )
    return daily, ltv


def test_compute_kpis():
    daily, ltv = _serving_frames()
    kpis = compute_kpis(daily, ltv)
    assert kpis["gross_revenue"] == 175.0
    assert kpis["orders"] == 6
    assert kpis["unique_customers"] == 3


def test_revenue_by_country_pivot():
    daily, _ = _serving_frames()
    piv = revenue_by_country(daily)
    assert sorted(piv.columns) == ["gb", "us"]
    assert piv.loc[pd.Timestamp("2025-01-01"), "us"] == 100.0
    assert piv.loc[pd.Timestamp("2025-01-02"), "gb"] == 0.0  # filled


def test_top_customers_orders_desc():
    _, ltv = _serving_frames()
    top = top_customers(ltv, 2)
    assert list(top["customer_id"]) == ["c1", "c2"]
    assert len(top) == 2
