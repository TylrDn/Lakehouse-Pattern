"""Download UCI Online Retail II — the canonical real retail dataset.

Why this dataset
----------------
* ~1M transactions across 5,000+ customers and 40+ countries.
* Real customer IDs and invoice numbers — meaningful PII/masking demo.
* Well-known: many blog posts, so learners can compare to other tutorials.

Source: UCI Machine Learning Repository, dataset ID 502.
Direct URL: https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip
"""

from __future__ import annotations

import argparse
import hashlib
import io
import shutil
import urllib.request
import zipfile
from pathlib import Path

from lakehouse.env import get_logger

_log = get_logger("data.uci_online_retail")

URL = "https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip"
EXPECTED_SHA256 = None  # UCI does not publish a stable hash; skip verification
DEST = Path(__file__).resolve().parent / "raw"


def main(force: bool = False) -> Path:
    DEST.mkdir(parents=True, exist_ok=True)
    csv = DEST / "online_retail_II.csv"
    if csv.exists() and not force:
        _log.info("UCI dataset already present at %s", csv)
        return csv

    _log.info("downloading %s", URL)
    with urllib.request.urlopen(URL, timeout=120) as resp:  # noqa: S310
        buf = io.BytesIO(resp.read())
    with zipfile.ZipFile(buf) as zf:
        xlsx_name = next(n for n in zf.namelist() if n.endswith(".xlsx"))
        with zf.open(xlsx_name) as f, (DEST / xlsx_name).open("wb") as out:
            shutil.copyfileobj(f, out)
    _convert_to_csv(DEST / xlsx_name, csv)
    _log.info("wrote %s", csv)
    return csv


def _convert_to_csv(xlsx: Path, csv: Path) -> None:
    import openpyxl

    wb = openpyxl.load_workbook(xlsx, read_only=True)
    with csv.open("w") as f:
        first = True
        for sheet in wb.worksheets:
            for row in sheet.iter_rows(values_only=True):
                if first:
                    f.write(",".join(str(c or "") for c in row) + "\n")
                    first = False
                else:
                    f.write(",".join(str(c or "") for c in row) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    main(force=args.force)
