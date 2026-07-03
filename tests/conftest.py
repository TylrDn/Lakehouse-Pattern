"""Shared pytest fixtures.

We stand up a single SparkSession for the whole test module and point the
lakehouse root at a temp dir per test so tests don't collide.
"""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def spark():
    # Import here so that ``pytest --collect-only`` doesn't force a Spark import.
    from lakehouse.spark import get_spark

    session = get_spark("pytest-lakehouse", shuffle_partitions=2)
    yield session
    session.stop()


@pytest.fixture()
def tmp_lakehouse(tmp_path, monkeypatch) -> Path:
    """Give each test its own fresh lakehouse root."""
    root = tmp_path / "lakehouse"
    root.mkdir()
    monkeypatch.setenv("LAKEHOUSE_ROOT", str(root))
    # Reload path constants so they see the new env var.
    import importlib

    from lakehouse import paths as paths_mod

    importlib.reload(paths_mod)
    paths_mod.ensure_dirs()
    return root
