"""Tests for the secrets shim — env / user / repo lookup ordering."""

from __future__ import annotations

import json

import pytest

from governance.local_uc import secrets


def test_env_var_takes_priority(monkeypatch):
    secrets._load.cache_clear()
    monkeypatch.setenv("LAKEHOUSE_SECRET_AI_OPENAI_API_KEY", "from-env")
    assert secrets.get("ai", "openai_api_key") == "from-env"


def test_file_fallback(monkeypatch, tmp_path):
    secrets._load.cache_clear()
    store = tmp_path / "secrets.local.json"
    store.write_text(json.dumps({"ai": {"openai_api_key": "from-file"}}))
    monkeypatch.setattr(secrets, "_LOCAL_STORE", store)
    monkeypatch.delenv("LAKEHOUSE_SECRET_AI_OPENAI_API_KEY", raising=False)
    assert secrets.get("ai", "openai_api_key") == "from-file"


def test_missing_secret_raises(monkeypatch, tmp_path):
    secrets._load.cache_clear()
    monkeypatch.setattr(secrets, "_LOCAL_STORE", tmp_path / "nope.json")
    monkeypatch.setattr(secrets, "_USER_STORE", tmp_path / "nope2.json")
    monkeypatch.delenv("LAKEHOUSE_SECRET_MISSING_KEY", raising=False)
    with pytest.raises(KeyError):
        secrets.get("missing", "key")
