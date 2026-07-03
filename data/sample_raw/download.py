"""Generate a small, reproducible sample dataset for the lakehouse demo.

Dataset choice
--------------
We considered three publicly available options:

1. **NYC Taxi trips** (TLC): great volume, but the CSVs are large (hundreds of MB
   per month) and the schema has shifted several times — awkward for a small
   reproducible portfolio repo.
2. **UCI Online Retail II**: real retail transactions, ~1M rows, has actual
   dirty data (nulls, negatives, cancellations). Excellent didactic value but
   requires downloading a ~45 MB Excel file — brittle in CI.
3. **Synthetic retail transactions** (this file) — generated deterministically
   from a fixed random seed. Small, no network dependency, lets us guarantee
   the "dirty data" cases we want Silver to clean.

We chose option 3 because CI reproducibility is non-negotiable for a
portfolio project and we can inject exactly the data-quality issues we want to
demonstrate. Real customers won't have this luxury — see the
`extension_point_real_dataset` note in the README for how to swap this out.
"""

from __future__ import annotations

import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

from lakehouse.paths import RAW_DIR, ensure_dirs

# Deterministic seed so every reviewer gets identical data.
_SEED = 42
_COUNTRIES = ["US", "GB", "DE", "FR", "JP", "CA", "AU"]
_CURRENCIES = {
    "US": "USD",
    "GB": "GBP",
    "DE": "EUR",
    "FR": "EUR",
    "JP": "JPY",
    "CA": "CAD",
    "AU": "AUD",
}
_PRODUCTS = [f"SKU-{i:04d}" for i in range(1, 41)]


def _rand_customer_id(rng: random.Random) -> str:
    # ~200 unique customers -> repeats guarantee interesting aggregates.
    return f"C-{rng.randint(1, 200):05d}"


def _generate_rows(n: int, start: datetime, rng: random.Random) -> list[dict]:
    """Generate ``n`` transactions with intentional dirty data mixed in.

    Injected quality issues (Silver must handle each):
      * duplicate transaction_ids
      * negative quantities (returns encoded as negatives — should be filtered
        from revenue but kept for auditing)
      * null customer_id
      * malformed timestamps
      * whitespace / mixed-case country codes
    """
    rows: list[dict] = []
    for i in range(n):
        country = rng.choice(_COUNTRIES)
        ts = start + timedelta(minutes=rng.randint(0, 60 * 24 * 30))
        qty = rng.randint(1, 6)
        price = round(rng.uniform(2.5, 199.0), 2)

        row = {
            "transaction_id": f"T-{i:07d}",
            "customer_id": _rand_customer_id(rng),
            "product_id": rng.choice(_PRODUCTS),
            "quantity": str(qty),
            "unit_price": f"{price:.2f}",
            "currency": _CURRENCIES[country],
            "event_ts": ts.isoformat(sep=" "),
            "country": country,
        }
        rows.append(row)

    # --- Inject dirty-data cases (~2% of rows) --------------------------------
    dirty = max(4, n // 50)
    for _ in range(dirty // 4):
        # Duplicate a random row (same transaction_id).
        rows.append(dict(rng.choice(rows)))
    for _ in range(dirty // 4):
        # Null customer_id.
        rows[rng.randrange(len(rows))]["customer_id"] = ""
    for _ in range(dirty // 4):
        # Negative quantity (return).
        idx = rng.randrange(len(rows))
        rows[idx]["quantity"] = f"-{rng.randint(1, 3)}"
    for _ in range(dirty // 4):
        # Malformed timestamp.
        rows[rng.randrange(len(rows))]["event_ts"] = "not-a-date"
    # Mixed-case + whitespace country.
    for _ in range(dirty // 4):
        idx = rng.randrange(len(rows))
        rows[idx]["country"] = f" {rows[idx]['country'].lower()} "

    rng.shuffle(rows)
    return rows


def write_csv(rows: list[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main(n_rows: int = 5000, out_dir: Path = RAW_DIR) -> Path:
    ensure_dirs()
    rng = random.Random(_SEED)
    rows = _generate_rows(n_rows, datetime(2025, 1, 1), rng)
    out = out_dir / "transactions.csv"
    write_csv(rows, out)
    print(f"Wrote {len(rows)} rows to {out}")
    return out


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rows", type=int, default=5000, help="Number of clean rows to generate"
    )
    args = parser.parse_args()
    main(args.rows)
