"""Register the best MLflow run to the Model Registry + tag stages.

MLflow's Model Registry is the OSS counterpart of Unity Catalog's Models. It
gives us:
* A versioned, named handle for the "current production" model.
* Stage transitions (``None`` -> ``Staging`` -> ``Production`` -> ``Archived``)
  that can gate serving deploys.
* Audit history (who promoted what, when).

On Databricks with UC, the exact same API works if you call
``mlflow.set_registry_uri("databricks-uc")`` — the model then lives under a
three-level name like ``main.lakehouse_pattern.daily_revenue_forecaster``.
"""

from __future__ import annotations

import argparse
import os
from typing import Optional

import mlflow
from mlflow.tracking import MlflowClient

from lakehouse import paths
from lakehouse.env import get_logger

_log = get_logger("ml.register_model")

MODEL_NAME = "daily_revenue_forecaster"
EXPERIMENT_NAME = "lakehouse-pattern-daily-revenue"


def _best_run(client: MlflowClient) -> Optional[mlflow.entities.Run]:
    """Return the run with the lowest MAE for our experiment, or None."""
    exp = client.get_experiment_by_name(EXPERIMENT_NAME)
    if exp is None:
        return None
    runs = client.search_runs(
        [exp.experiment_id],
        order_by=["metrics.mae ASC"],
        max_results=1,
    )
    return runs[0] if runs else None


def register(promote_to: str = "Staging") -> None:
    tracking_uri = os.environ.get(
        "MLFLOW_TRACKING_URI", f"file://{paths.LAKEHOUSE_ROOT.parent}/mlruns"
    )
    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient()

    best = _best_run(client)
    if best is None:
        raise RuntimeError(
            "No MLflow runs found for experiment '"
            f"{EXPERIMENT_NAME}'. Run `python -m ml.train_model` first."
        )
    _log.info("Best run: %s (mae=%.2f)", best.info.run_id, best.data.metrics.get("mae", float("nan")))

    model_uri = f"runs:/{best.info.run_id}/model"
    result = mlflow.register_model(model_uri, MODEL_NAME)
    _log.info("Registered %s v%s", MODEL_NAME, result.version)

    # Stage transitions are deprecated in newer MLflow in favor of aliases;
    # we set both for maximum reviewer clarity.
    client.set_registered_model_alias(MODEL_NAME, "champion", result.version)
    try:
        client.transition_model_version_stage(
            name=MODEL_NAME, version=result.version, stage=promote_to
        )
        _log.info("Transitioned v%s -> %s", result.version, promote_to)
    except Exception as exc:  # pragma: no cover - newer MLflow deprecates stages
        _log.warning("Stage transition skipped (%s); alias 'champion' set instead.", exc)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", default="Staging", choices=["Staging", "Production"])
    args = parser.parse_args()
    register(args.stage)
