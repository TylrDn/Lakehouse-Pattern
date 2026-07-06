"""Train a daily-revenue forecasting model with MLflow tracking.

Why this model
--------------
We train a small ``RandomForestRegressor`` to predict tomorrow's revenue per
country from a few lag features derived from the Gold ``daily_revenue`` mart.
Model quality is not the point — the point is a complete, reproducible
tracking + registry loop that mirrors how you'd operationalize any model on
Databricks.

MLflow demonstrates
-------------------
* ``mlflow.start_run`` — one run per training attempt.
* ``mlflow.log_param`` / ``log_metric`` — hyperparameters + evaluation metrics.
* ``mlflow.sklearn.log_model`` — logs an MLmodel artifact + conda / pip env.
* Autologging via ``mlflow.sklearn.autolog()`` for parameters we forget.
* Signature inference so the registered model has typed inputs / outputs.

On Databricks this file runs unchanged; you just point ``MLFLOW_TRACKING_URI``
at the workspace tracking server (or leave unset — the Databricks runtime
sets it automatically).
"""

from __future__ import annotations

import argparse
import os
import tempfile
from pathlib import Path

import pandas as pd

from lakehouse import paths
from lakehouse.env import get_logger

# mlflow + scikit-learn (and the Spark session) are imported lazily inside the
# functions that need them so this module can be imported — and its pure
# feature-engineering logic unit-tested — in the fast CI lane without the heavy
# ML stack or a Spark bootstrap.

_log = get_logger("ml.train_model")

EXPERIMENT_NAME = "lakehouse-pattern-daily-revenue"
MODEL_ARTIFACT_PATH = "model"


def _default_tracking_uri() -> str:
    """Anchor MLflow at ``<repo-root>/mlruns`` unless overridden.

    We resolve to the *parent* of ``LAKEHOUSE_ROOT`` (which defaults to
    ``./data``) so the mlruns directory sits next to ``data/`` at the repo
    root, not inside it. Overridable via ``MLFLOW_TRACKING_URI``.
    """
    return os.environ.get(
        "MLFLOW_TRACKING_URI", f"file://{paths.LAKEHOUSE_ROOT.parent}/mlruns"
    )


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add lag / rolling / day-of-week features to a daily_revenue frame.

    Pure function of the input DataFrame (no IO), so it is unit-testable
    without Spark or the ML stack. Behavior is identical to the original
    inline logic: sort, derive per-country lag/rolling features, add
    day-of-week, then drop rows with NaNs introduced by the lags.
    """
    df = df.sort_values(["country", "event_date"]).reset_index(drop=True)
    df["lag_1"] = df.groupby("country")["gross_revenue"].shift(1)
    df["lag_7"] = df.groupby("country")["gross_revenue"].shift(7)
    df["rolling_3"] = (
        df.groupby("country")["gross_revenue"]
        .shift(1)
        .rolling(3)
        .mean()
        .reset_index(drop=True)
    )
    df["day_of_week"] = pd.to_datetime(df["event_date"]).dt.dayofweek
    return df.dropna().reset_index(drop=True)


def load_features() -> pd.DataFrame:
    """Read Gold daily_revenue and engineer lag features."""
    from lakehouse.spark import get_spark

    spark = get_spark("ml-train")
    df = spark.read.format("delta").load(str(paths.GOLD_DAILY_REVENUE)).toPandas()
    return engineer_features(df)


def train(n_estimators: int = 200, max_depth: int | None = 8) -> str:
    """Train + log a run. Returns the MLflow run_id."""
    import mlflow
    import mlflow.sklearn
    from mlflow.models.signature import infer_signature
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.metrics import mean_absolute_error, r2_score
    from sklearn.model_selection import train_test_split

    mlflow.set_tracking_uri(_default_tracking_uri())
    mlflow.set_experiment(EXPERIMENT_NAME)
    mlflow.sklearn.autolog(log_models=False, disable=False)

    features = load_features()
    if features.empty:
        raise RuntimeError(
            "No training rows after feature engineering. Gold daily_revenue is "
            "likely empty or too short to derive lag features \u2014 run the ETL first."
        )
    feature_cols = [
        "lag_1",
        "lag_7",
        "rolling_3",
        "day_of_week",
        "order_count",
        "unique_customers",
    ]
    # Cast integer columns to float64 so the MLflow-inferred signature can
    # represent missing values at inference time. Integer columns in pandas
    # cannot hold NaN, which trips MLflow's schema enforcement in serving.
    x = features[feature_cols].astype("float64")
    y = features["gross_revenue"].astype("float64")

    x_train, x_test, y_train, y_test = train_test_split(
        x, y, test_size=0.2, random_state=42
    )

    with mlflow.start_run() as run:
        mlflow.log_params(
            {
                "n_estimators": n_estimators,
                "max_depth": max_depth,
                "n_features": len(feature_cols),
                "train_rows": len(x_train),
            }
        )
        model = RandomForestRegressor(
            n_estimators=n_estimators, max_depth=max_depth, random_state=42, n_jobs=-1
        )
        model.fit(x_train, y_train)

        preds = model.predict(x_test)
        mae = float(mean_absolute_error(y_test, preds))
        r2 = float(r2_score(y_test, preds))
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("r2", r2)

        signature = infer_signature(x_train, preds)
        # MLflow 3 renamed the ``artifact_path`` kwarg to ``name``; keep the same
        # subdirectory (``model``) for backward compatibility with runs:/... URIs.
        mlflow.sklearn.log_model(
            model,
            name=MODEL_ARTIFACT_PATH,
            signature=signature,
            input_example=x_train.head(3),
        )

        # Log the feature list as a text artifact so downstream consumers can
        # reconstruct the model interface without magic constants. We use a
        # temp file so nothing lands in the working directory.
        with tempfile.TemporaryDirectory() as td:
            features_file = Path(td) / "features.txt"
            features_file.write_text("\n".join(feature_cols))
            mlflow.log_artifact(str(features_file))

        _log.info("MLflow run %s: MAE=%.2f R2=%.3f", run.info.run_id, mae, r2)
        return run.info.run_id


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n-estimators", type=int, default=200)
    parser.add_argument("--max-depth", type=int, default=8)
    args = parser.parse_args()
    train(args.n_estimators, args.max_depth)
