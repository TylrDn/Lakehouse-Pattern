"""Authorization shim — Unity Catalog grants analog, backed by OPA.

Every read / write on a governed table goes through :func:`check` which:

1. Loads the caller's identity from ``scim_users.json`` (mock SCIM).
2. Consults ``../opa/policies.rego`` via Open Policy Agent (or a local
   evaluator when OPA is not running — CI mode).
3. Emits an audit record to ``audit_log.jsonl`` so ``system.access.audit``
   is queryable after the fact.

Databricks-native mapping
-------------------------
On UC you would never write this code — grants are enforced transparently by
the metastore and audit rows appear in ``system.access.audit`` automatically.
This module is here so the *concept* is runnable locally and so the CI
pipeline can prove that a masking / RLS regression fails a test.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from lakehouse.env import get_logger

_log = get_logger("governance.local_uc.authz")

_HERE = Path(__file__).resolve().parent
_SCIM = _HERE / "scim_users.json"
_AUDIT = _HERE / "audit_log.jsonl"
_POLICY = _HERE.parent / "opa" / "policies.rego"


@dataclass
class Principal:
    user: str
    groups: list[str] = field(default_factory=list)

    @classmethod
    def from_scim(cls, user: str) -> "Principal":
        payload = json.loads(_SCIM.read_text()) if _SCIM.exists() else {}
        record = payload.get(user, {"user": user, "groups": []})
        return cls(user=record["user"], groups=record.get("groups", []))


def _audit(event: dict[str, Any]) -> None:
    event.setdefault("ts", time.time())
    _AUDIT.parent.mkdir(parents=True, exist_ok=True)
    with _AUDIT.open("a") as f:
        f.write(json.dumps(event) + "\n")


def _opa_eval(action: str, principal: Principal, resource: str) -> bool:
    """Evaluate via OPA if available; else fall back to a stub matcher.

    The fallback matches the shape of the rego policy so unit tests can run
    without a running OPA sidecar in CI. Production runs (or `make governance`
    with `docker compose up opa`) hit the real evaluator.
    """
    opa_url = os.environ.get("OPA_URL")
    if opa_url:
        import urllib.request

        payload = json.dumps(
            {
                "input": {
                    "action": action,
                    "user": principal.user,
                    "groups": principal.groups,
                    "resource": resource,
                }
            }
        ).encode()
        req = urllib.request.Request(
            f"{opa_url}/v1/data/lakehouse/allow",
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=2) as resp:  # noqa: S310
            body = json.loads(resp.read())
        return bool(body.get("result", False))

    # Local fallback — mirrors the rego rules in ../opa/policies.rego.
    if "admins" in principal.groups:
        return True
    layer = resource.split(".")[1] if "." in resource else ""
    if action == "select" and layer == "gold" and "analysts" in principal.groups:
        return True
    if (
        action in {"select", "insert", "update", "merge"}
        and layer in {"bronze", "silver"}
        and "data-engineers" in principal.groups
    ):
        return True
    if (
        action == "select"
        and layer in {"silver", "gold"}
        and "ml-engineers" in principal.groups
    ):
        return True
    return False


def check(action: str, principal: Principal, resource: str) -> None:
    """Raise ``PermissionError`` if ``principal`` may not perform ``action``."""
    allowed = _opa_eval(action, principal, resource)
    _audit(
        {
            "action": action,
            "user": principal.user,
            "groups": principal.groups,
            "resource": resource,
            "allowed": allowed,
        }
    )
    if not allowed:
        raise PermissionError(
            f"{principal.user} cannot {action} {resource} "
            f"(groups={principal.groups})"
        )
    _log.debug("authorized %s %s on %s", principal.user, action, resource)


def start_opa_sidecar(port: int = 8181) -> Optional[subprocess.Popen[bytes]]:
    """Start OPA in the background pointing at policies.rego. For local dev only."""
    if not _POLICY.exists():
        return None
    return subprocess.Popen(  # noqa: S603
        ["opa", "run", "--server", f"--addr=:{port}", str(_POLICY)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


if __name__ == "__main__":
    # Smoke test: each identity attempting one representative operation.
    for user in ("alice_analyst", "bob_engineer", "carol_ml", "dan_admin"):
        p = Principal.from_scim(user)
        for action, resource in [
            ("select", "main.gold.daily_revenue"),
            ("merge", "main.silver.transactions"),
        ]:
            try:
                check(action, p, resource)
                print(f"OK   {user} {action} {resource}")
            except PermissionError as exc:
                print(f"DENY {exc}")
