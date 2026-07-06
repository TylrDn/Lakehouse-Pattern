"""Sanity test — Dagster asset graph loads and has the expected shape."""

from __future__ import annotations

import pytest


def test_dagster_graph_shape():
    pytest.importorskip("dagster")
    from orchestration.dagster_project.definitions import defs

    asset_names = {a.key.to_user_string() for a in defs.assets}
    assert "bronze_transactions" in asset_names
    assert "silver_transactions" in asset_names
    assert "gold_daily_revenue" in asset_names
    assert "registered_model" in asset_names
