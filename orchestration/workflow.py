"""Local DAG orchestrator — the Databricks Workflows analog.

The task graph:

    download_data
        └── bronze_batch_ingest
                └── silver_clean
                        └── gold_aggregate
                                ├── ml_train
                                │       └── ml_register
                                └── (downstream Streamlit reads Gold directly)

Failure semantics
-----------------
* Tasks fail fast: an exception in any task aborts the DAG.
* Retries: each task retries ``max_attempts`` times with exponential backoff
  (matches the default Databricks Workflows retry policy).
* Idempotency: every task is idempotent by design (see the individual module
  docstrings — MERGE, overwrite, deterministic embedders).

Databricks-native mapping
-------------------------
Each ``Task`` here maps to one **Task** in a Databricks **Job**. The DAG's
dependencies map 1:1 to ``depends_on`` in the Jobs YAML/API. On Databricks
you would additionally get: cluster reuse, ADF-style parameters, Git-source
tasks, alerting integrations, and cost attribution — all things you don't
get from a hand-rolled runner.
"""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass, field
from typing import Callable

from lakehouse.env import get_logger

_log = get_logger("orchestration")


@dataclass
class Task:
    name: str
    run: Callable[[], None]
    depends_on: list[str] = field(default_factory=list)
    max_attempts: int = 2


def _import_and_call(module: str, fn: str = "run") -> Callable[[], None]:
    """Return a zero-arg callable that lazily imports the target module.

    Lazy imports keep the DAG definition free of heavy Spark bootstrap cost
    until the task actually runs.
    """

    def _call() -> None:
        mod = __import__(module, fromlist=[fn])
        getattr(mod, fn)()

    _call.__name__ = f"{module}:{fn}"
    return _call


def _download() -> None:
    from data.sample_raw.download import main as dl

    dl()


TASKS: list[Task] = [
    Task("download_data", _download),
    Task(
        "bronze_batch_ingest",
        _import_and_call("ingestion.batch_ingest"),
        depends_on=["download_data"],
    ),
    Task(
        "silver_clean",
        _import_and_call("transform.silver_clean"),
        depends_on=["bronze_batch_ingest"],
    ),
    Task(
        "gold_aggregate",
        _import_and_call("transform.gold_aggregate"),
        depends_on=["silver_clean"],
    ),
    Task(
        "ml_train",
        _import_and_call("ml.train_model", "train"),
        depends_on=["gold_aggregate"],
    ),
    Task(
        "ml_register",
        _import_and_call("ml.register_model", "register"),
        depends_on=["ml_train"],
    ),
]


def _topo_order(tasks: list[Task]) -> list[Task]:
    by_name = {t.name: t for t in tasks}
    ordered: list[Task] = []
    seen: set[str] = set()

    def visit(t: Task) -> None:
        if t.name in seen:
            return
        for dep in t.depends_on:
            if dep not in by_name:
                raise ValueError(f"Task '{t.name}' depends on unknown '{dep}'")
            visit(by_name[dep])
        seen.add(t.name)
        ordered.append(t)

    for t in tasks:
        visit(t)
    return ordered


def run(skip_ml: bool = False) -> None:
    tasks = [t for t in TASKS if not (skip_ml and t.name.startswith("ml_"))]
    ordered = _topo_order(tasks)
    _log.info(
        "DAG start (%d tasks%s)", len(ordered), "; skip_ml=True" if skip_ml else ""
    )
    started = time.monotonic()
    for task in ordered:
        for attempt in range(1, task.max_attempts + 1):
            _log.info("%s (attempt %d/%d)", task.name, attempt, task.max_attempts)
            try:
                task.run()
                _log.info("%s OK", task.name)
                break
            except Exception as exc:  # pragma: no cover - retry path
                _log.error("%s FAILED: %s", task.name, exc)
                if attempt == task.max_attempts:
                    _log.error("DAG aborted after %.1fs", time.monotonic() - started)
                    raise
                sleep = 2**attempt
                _log.warning("retrying %s in %ds", task.name, sleep)
                time.sleep(sleep)
    _log.info("DAG complete in %.1fs", time.monotonic() - started)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--skip-ml",
        action="store_true",
        help="Run the ETL portion only (useful in CI where PyTorch install is heavy).",
    )
    args = parser.parse_args()
    run(skip_ml=args.skip_ml)
