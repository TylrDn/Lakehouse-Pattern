"""Shape tests for the Feature Store analog — dataclass API compatibility."""

from __future__ import annotations

from ml.feature_store.store import FeatureLookup, FeatureTable


def test_feature_table_defaults():
    ft = FeatureTable(
        name="customer_features",
        primary_keys=["customer_id"],
        timestamp_key="event_ts",
    )
    assert ft.owner == "data-platform"
    assert ft.description == ""


def test_feature_lookup_matches_databricks_signature():
    lu = FeatureLookup(
        table_name="customer_features",
        lookup_key="customer_id",
        feature_names=["lifetime_revenue", "orders"],
    )
    # The Databricks SDK's FeatureLookup has these exact positional args;
    # importing this dataclass into a UC notebook should just work.
    assert lu.timestamp_lookup_key == "event_ts"
    assert lu.feature_names == ["lifetime_revenue", "orders"]
