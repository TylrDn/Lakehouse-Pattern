"""Secrets analog — mimics ``dbutils.secrets`` semantics in OSS.

Lookup order:
1. Environment variable ``LAKEHOUSE_SECRET_<SCOPE>_<KEY>``  (for CI).
2. ``~/.config/lakehouse-pattern/secrets.json`` — user-scoped file.
3. ``governance/local_uc/secrets.local.json`` — repo-scoped, git-ignored.

Never log secret *values*. The audit event records only the scope + key.
"""

from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path

from lakehouse.env import get_logger

_log = get_logger("governance.local_uc.secrets")

_LOCAL_STORE = Path(__file__).resolve().parent / "secrets.local.json"
_USER_STORE = Path.home() / ".config" / "lakehouse-pattern" / "secrets.json"


@lru_cache
def _load(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def get(scope: str, key: str) -> str:
    env_key = f"LAKEHOUSE_SECRET_{scope.upper()}_{key.upper()}"
    if env_key in os.environ:
        _log.info("secret hit (env) scope=%s key=%s", scope, key)
        return os.environ[env_key]

    for path in (_USER_STORE, _LOCAL_STORE):
        data = _load(path)
        if scope in data and key in data[scope]:
            _log.info("secret hit (%s) scope=%s key=%s", path.name, scope, key)
            return data[scope][key]

    raise KeyError(f"secret not found: scope={scope!r} key={key!r}")


def list_scopes() -> list[str]:
    scopes: set[str] = set()
    for path in (_USER_STORE, _LOCAL_STORE):
        scopes.update(_load(path).keys())
    return sorted(scopes)
