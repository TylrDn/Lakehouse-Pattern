"""Copy root-level markdown into docs/ so mkdocs can nest them under nav.

Run before `mkdocs build`:

    python docs/prebuild.py

We keep the source of truth at the repo root (TROUBLESHOOTING.md,
CHANGELOG.md, etc.) so they render properly on GitHub, and we mirror
them into the docs/ tree for the MkDocs site.
"""

from __future__ import annotations

import shutil
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DOCS = REPO_ROOT / "docs"

MIRRORED = {
    "TROUBLESHOOTING.md": "troubleshooting.md",
    "CHANGELOG.md": "changelog.md",
    "CONTRIBUTING.md": "contributing.md",
    "SECURITY.md": "security.md",
}


def main() -> None:
    for src_name, dst_name in MIRRORED.items():
        src = REPO_ROOT / src_name
        dst = DOCS / dst_name
        if not src.exists():
            print(f"skip: {src_name} not found")
            continue
        shutil.copy2(src, dst)
        print(f"copied {src_name} -> docs/{dst_name}")


if __name__ == "__main__":
    main()
