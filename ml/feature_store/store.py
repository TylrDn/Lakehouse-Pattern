"""Feature Store analog — Delta tables + point-in-time joins + FeatureLookup.

Databricks Feature Store (now "Feature Engineering in UC") solves three
things that a raw Delta table does not:

1. **Point-in-time correctness** — training joins pull the feature value as
   of the label's timestamp, never later. Prevents leakage.
2. **Feature registry** — each feature has a name, owner, description,
   and version. Consumed by the model's ``signature`` and the online store.
3. **Online store** — low-latency lookup for inference (Redis, Cosmos, …).

This module ships (1) and (2). (3) is documented — swap the ``lookup``
implementation to hit Redis and you're done.

Wire-format compatibility
-------------------------
Feature tables here are plain Delta tables with two required columns:
``primary_key`` (str) and ``event_ts`` (timestamp). The ``FeatureLookup``
dataclass matches the ``databricks.feature_engineering.FeatureLookup``
signature so migration is a one-import swap.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Sequence

from lakehouse import paths
from lakehouse.env import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

_log = get_logger("ml.feature_store")

_REGISTRY = Path(__file__).resolve().parent / "registry.json"
FEATURES_DIR = paths.LAKEHOUSE_ROOT / "features"


@dataclass
class FeatureTable:
    name: str
    primary_keys: list[str]
    timestamp_key: str
    description: str = ""
    owner: str = "data-platform"


@dataclass
class FeatureLookup:
    table_name: str
    lookup_key: str
    feature_names: list[str] = field(default_factory=list)
    timestamp_lookup_key: str = "event_ts"


def register(table: FeatureTable) -> None:
    _REGISTRY.parent.mkdir(parents=True, exist_ok=True)
    existing = json.loads(_REGISTRY.read_text()) if _REGISTRY.exists() else {}
    existing[table.name] = asdict(table)
    _REGISTRY.write_text(json.dumps(existing, indent=2))
    _log.info("registered feature table %s", table.name)


def write_features(spark: "SparkSession", table: FeatureTable, df: "DataFrame") -> None:
    path = FEATURES_DIR / table.name
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("delta.enableChangeDataFeed", "true")
        .save(str(path))
    )
    _log.info("wrote features %s -> %s (%d rows)", table.name, path, df.count())


def _asof_join(
    labels: "DataFrame",
    features: "DataFrame",
    lookup: FeatureLookup,
) -> "DataFrame":
    """Left as-of join: latest feature row whose ``event_ts <= label.event_ts``."""
    from pyspark.sql.functions import col, row_number
    from pyspark.sql.window import Window

    joined = labels.alias("l").join(
        features.alias("f"),
        (col(f"l.{lookup.lookup_key}") == col(f"f.{lookup.lookup_key}"))
        & (col(f"f.{lookup.timestamp_lookup_key}") <= col(f"l.{lookup.timestamp_lookup_key}")),
        "left",
    )
    w = Window.partitionBy(
        *[col(f"l.{c}") for c in labels.columns]
    ).orderBy(col(f"f.{lookup.timestamp_lookup_key}").desc())
    ranked = joined.withColumn("_rn", row_number().over(w)).filter("_rn = 1").drop("_rn")
    keep = ["l." + c for c in labels.columns] + [
        f"f.{n}" for n in lookup.feature_names
    ]
    return ranked.selectExpr(*keep)


def create_training_set(
    spark: "SparkSession",
    labels: "DataFrame",
    lookups: Sequence[FeatureLookup],
) -> "DataFrame":
    """Return ``labels`` enriched with point-in-time-correct feature columns."""
    result = labels
    for lu in lookups:
        table_path = FEATURES_DIR / lu.table_name
        features = spark.read.format("delta").load(str(table_path))
        result = _asof_join(result, features, lu)
    return result


def lookup(spark: "SparkSession", table_name: str, keys: list[str]) -> "DataFrame":
    """Batch online lookup — Delta scan filtered to the current values."""
    from pyspark.sql.functions import col, row_number
    from pyspark.sql.window import Window

    df = spark.read.format("delta").load(str(FEATURES_DIR / table_name))
    w = Window.partitionBy("customer_id").orderBy(col("event_ts").desc())
    return (
        df.filter(col("customer_id").isin(keys))
        .withColumn("_rn", row_number().over(w))
        .filter("_rn = 1")
        .drop("_rn")
    )
