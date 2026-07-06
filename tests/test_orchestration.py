"""Unit tests for the DAG orchestrator (P1-2).

These use fake zero-arg tasks and never touch Spark, so they run in the fast
lane. They cover topological ordering, unknown-dependency errors, retry/backoff
(with sleep monkeypatched), fail-fast abort, and the --skip-ml filter.
"""

from __future__ import annotations

import pytest

from orchestration import workflow
from orchestration.workflow import Task, _topo_order, run


def test_topo_order_respects_dependencies():
    a = Task("a", lambda: None)
    b = Task("b", lambda: None, depends_on=["a"])
    c = Task("c", lambda: None, depends_on=["b"])
    # Pass in a deliberately unsorted order.
    ordered = [t.name for t in _topo_order([c, a, b])]
    assert ordered.index("a") < ordered.index("b") < ordered.index("c")


def test_topo_order_unknown_dependency_raises():
    bad = Task("bad", lambda: None, depends_on=["does_not_exist"])
    with pytest.raises(ValueError, match="unknown 'does_not_exist'"):
        _topo_order([bad])


def test_run_retries_then_succeeds(monkeypatch):
    monkeypatch.setattr(workflow.time, "sleep", lambda _s: None)
    attempts = {"n": 0}

    def flaky() -> None:
        attempts["n"] += 1
        if attempts["n"] < 2:
            raise RuntimeError("transient")

    monkeypatch.setattr(workflow, "TASKS", [Task("flaky", flaky, max_attempts=3)])
    run()
    assert attempts["n"] == 2  # failed once, succeeded on the second attempt


def test_run_aborts_after_max_attempts(monkeypatch):
    monkeypatch.setattr(workflow.time, "sleep", lambda _s: None)
    attempts = {"n": 0}

    def always_fail() -> None:
        attempts["n"] += 1
        raise RuntimeError("boom")

    monkeypatch.setattr(
        workflow, "TASKS", [Task("always_fail", always_fail, max_attempts=2)]
    )
    with pytest.raises(RuntimeError, match="boom"):
        run()
    assert attempts["n"] == 2  # exhausted all attempts, then re-raised


def test_run_fail_fast_skips_downstream(monkeypatch):
    monkeypatch.setattr(workflow.time, "sleep", lambda _s: None)
    ran: list[str] = []

    def fail() -> None:
        ran.append("first")
        raise RuntimeError("stop")

    def never() -> None:
        ran.append("second")

    monkeypatch.setattr(
        workflow,
        "TASKS",
        [
            Task("first", fail, max_attempts=1),
            Task("second", never, depends_on=["first"], max_attempts=1),
        ],
    )
    with pytest.raises(RuntimeError, match="stop"):
        run()
    assert ran == ["first"]  # downstream task never executed


def test_skip_ml_excludes_ml_tasks(monkeypatch):
    ran: list[str] = []
    monkeypatch.setattr(
        workflow,
        "TASKS",
        [
            Task("etl", lambda: ran.append("etl")),
            Task("ml_train", lambda: ran.append("ml_train"), depends_on=["etl"]),
            Task("ml_register", lambda: ran.append("ml_register"), depends_on=["ml_train"]),
        ],
    )
    run(skip_ml=True)
    assert ran == ["etl"]  # ml_* tasks filtered out
