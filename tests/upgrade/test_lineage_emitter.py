"""Unit tests for the OpenLineage emitter."""

from __future__ import annotations

import json

from governance.lineage import emitter


def test_emit_write_records_row_count(monkeypatch, tmp_path):
    events = tmp_path / "events.jsonl"
    monkeypatch.setattr(emitter, "_EVENTS", events)

    ds_in = emitter.Dataset("delta", "bronze/transactions")
    ds_out = emitter.Dataset("delta", "silver/transactions")
    emitter.emit_write("silver_clean", ds_in, ds_out, rows=42)

    payload = json.loads(events.read_text().strip())
    assert payload["job"]["name"] == "silver_clean"
    assert payload["outputs"][0]["facets"]["outputStatistics"]["rowCount"] == 42
    assert payload["inputs"][0]["name"] == "bronze/transactions"
