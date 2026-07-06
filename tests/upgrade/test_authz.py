"""Unit tests for the local UC authz shim — grants + audit + policy."""

from __future__ import annotations

import json

import pytest

from governance.local_uc import authz


def test_admin_can_do_anything(monkeypatch, tmp_path):
    monkeypatch.setattr(authz, "_AUDIT", tmp_path / "audit.jsonl")
    p = authz.Principal.from_scim("dan_admin")
    authz.check("merge", p, "main.silver.transactions")


def test_analyst_denied_silver(monkeypatch, tmp_path):
    monkeypatch.setattr(authz, "_AUDIT", tmp_path / "audit.jsonl")
    p = authz.Principal.from_scim("alice_analyst")
    with pytest.raises(PermissionError):
        authz.check("select", p, "main.silver.transactions")


def test_analyst_allowed_gold_select(monkeypatch, tmp_path):
    monkeypatch.setattr(authz, "_AUDIT", tmp_path / "audit.jsonl")
    p = authz.Principal.from_scim("alice_analyst")
    authz.check("select", p, "main.gold.daily_revenue")


def test_engineer_can_merge_silver(monkeypatch, tmp_path):
    monkeypatch.setattr(authz, "_AUDIT", tmp_path / "audit.jsonl")
    p = authz.Principal.from_scim("bob_engineer")
    authz.check("merge", p, "main.silver.transactions")


def test_external_user_denied_everything(monkeypatch, tmp_path):
    monkeypatch.setattr(authz, "_AUDIT", tmp_path / "audit.jsonl")
    p = authz.Principal.from_scim("eve_external")
    with pytest.raises(PermissionError):
        authz.check("select", p, "main.gold.daily_revenue")


def test_audit_row_written(monkeypatch, tmp_path):
    audit = tmp_path / "audit.jsonl"
    monkeypatch.setattr(authz, "_AUDIT", audit)
    p = authz.Principal.from_scim("bob_engineer")
    authz.check("merge", p, "main.silver.transactions")
    rows = [json.loads(line) for line in audit.read_text().splitlines()]
    assert len(rows) == 1
    assert rows[0]["allowed"] is True
    assert rows[0]["user"] == "bob_engineer"
