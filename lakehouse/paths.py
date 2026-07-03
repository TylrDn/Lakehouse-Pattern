"""Canonical filesystem layout for the lakehouse.

Why centralize paths?
- Every job reads/writes the same physical locations. Drift here silently
  creates duplicate tables and is one of the most common lakehouse bugs.
- On Databricks these paths become ``dbfs:/`` or ``abfss://`` URIs and the rest
  of the code is unchanged; we simply override ``LAKEHOUSE_ROOT`` via env var.
"""

from __future__ import annotations

import os
from pathlib import Path

# ``LAKEHOUSE_ROOT`` lets the same code target local FS in CI and DBFS in prod.
LAKEHOUSE_ROOT = Path(os.environ.get("LAKEHOUSE_ROOT", "data")).resolve()

RAW_DIR = LAKEHOUSE_ROOT / "sample_raw"
BRONZE_DIR = LAKEHOUSE_ROOT / "bronze"
SILVER_DIR = LAKEHOUSE_ROOT / "silver"
GOLD_DIR = LAKEHOUSE_ROOT / "gold"
CHECKPOINT_DIR = LAKEHOUSE_ROOT / "checkpoints"

# Table paths (each is its own Delta table directory).
BRONZE_TRANSACTIONS = BRONZE_DIR / "transactions"
SILVER_TRANSACTIONS = SILVER_DIR / "transactions"
GOLD_DAILY_REVENUE = GOLD_DIR / "daily_revenue"
GOLD_CUSTOMER_LTV = GOLD_DIR / "customer_ltv"


def ensure_dirs() -> None:
    """Create the base directories if they do not already exist."""
    for p in (RAW_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR, CHECKPOINT_DIR):
        p.mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    ensure_dirs()
    print(f"LAKEHOUSE_ROOT = {LAKEHOUSE_ROOT}")
    for name, p in {
        "raw": RAW_DIR,
        "bronze": BRONZE_DIR,
        "silver": SILVER_DIR,
        "gold": GOLD_DIR,
        "checkpoints": CHECKPOINT_DIR,
    }.items():
        print(f"  {name:12s} -> {p}")
