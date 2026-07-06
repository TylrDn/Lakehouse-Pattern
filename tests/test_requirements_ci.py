"""Guard against CI/runtime dependency drift (the P0-1 regression).

CI installs from ``requirements-ci.txt`` to keep the PR-gating lane fast, but
that only tests the shipped stack if every pin there is identical to the pin
in ``requirements.txt``. This test fails loudly if the two ever diverge, so we
can't silently recreate the drift we just fixed.
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_PIN_RE = re.compile(r"^\s*([A-Za-z0-9_.-]+)\s*==\s*([^\s#]+)")


def _parse_pins(path: Path) -> dict[str, str]:
    pins: dict[str, str] = {}
    for line in path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        match = _PIN_RE.match(stripped)
        if match:
            pins[match.group(1).lower()] = match.group(2)
    return pins


def test_ci_requirements_are_strict_subset_of_requirements():
    full = _parse_pins(_REPO_ROOT / "requirements.txt")
    ci = _parse_pins(_REPO_ROOT / "requirements-ci.txt")

    assert ci, "requirements-ci.txt parsed no pinned packages"

    mismatches = {
        pkg: (version, full.get(pkg))
        for pkg, version in ci.items()
        if full.get(pkg) != version
    }
    assert not mismatches, (
        "requirements-ci.txt pins must exactly match requirements.txt. "
        f"Offending packages (ci_version, requirements_version): {mismatches}"
    )
