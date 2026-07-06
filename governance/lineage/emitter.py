"""OpenLineage emitter — a thin producer that writes events to JSONL.

Unity Catalog captures lineage transparently. In OSS we call
:func:`emit_read` and :func:`emit_write` from each pipeline step; the events
are consumed by ``governance/local_uc/system_tables.sql`` to reconstruct the
``system.access.table_lineage`` view.

Wire-format compatibility
-------------------------
The event schema is intentionally a subset of the OpenLineage v2 spec — the
same JSON that Marquez, DataHub, and OpenMetadata can ingest. Point their
producers at ``events.jsonl`` (or replace this file with the official
``openlineage-python`` client) to graduate off the toy emitter.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from lakehouse.env import get_logger

_log = get_logger("governance.lineage")

_EVENTS = Path(__file__).resolve().parent / "events.jsonl"


@dataclass
class Dataset:
    namespace: str  # e.g. "delta"
    name: str  # e.g. "data/silver/transactions"


def _write(event: dict) -> None:
    _EVENTS.parent.mkdir(parents=True, exist_ok=True)
    with _EVENTS.open("a") as f:
        f.write(json.dumps(event) + "\n")


def _base(job: str, event_type: str) -> dict:
    return {
        "eventTime": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "eventType": event_type,
        "producer": "https://github.com/TylrDn/Lakehouse-Pattern",
        "job": {"namespace": os.environ.get("LAKEHOUSE_NAMESPACE", "local"), "name": job},
        "run": {"runId": os.environ.get("LAKEHOUSE_RUN_ID", str(uuid.uuid4()))},
    }


def emit_read(job: str, source: Dataset) -> None:
    ev = _base(job, "START")
    ev["inputs"] = [{"namespace": source.namespace, "name": source.name}]
    _write(ev)
    _log.debug("lineage read: %s <- %s", job, source.name)


def emit_write(job: str, source: Dataset | None, target: Dataset, rows: int) -> None:
    ev = _base(job, "COMPLETE")
    if source is not None:
        ev["inputs"] = [{"namespace": source.namespace, "name": source.name}]
    ev["outputs"] = [
        {
            "namespace": target.namespace,
            "name": target.name,
            "facets": {"outputStatistics": {"rowCount": rows}},
        }
    ]
    _write(ev)
    _log.debug("lineage write: %s -> %s (%d rows)", job, target.name, rows)
