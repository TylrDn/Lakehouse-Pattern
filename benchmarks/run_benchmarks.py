"""Reproducible pipeline benchmarks.

Runs the pipeline in a clean workspace under /tmp and captures wall-clock
elapsed time for each stage plus row counts. Writes a Markdown table to
``benchmarks/latest.md`` including the machine profile and commit SHA so
future runs can be compared.

Usage:

    python benchmarks/run_benchmarks.py            # runs ETL only
    python benchmarks/run_benchmarks.py --with-ml  # also runs MLflow training

The output is **not** a comparative claim against Databricks. It is a
reproducibility receipt: on this hardware, with this commit, at this scale,
the pipeline takes roughly this long. If you re-run it and get very
different numbers, either your hardware or your environment differs — not
the code.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
OUT_MD = REPO / "benchmarks" / "latest.md"
OUT_JSON = REPO / "benchmarks" / "latest.json"


@dataclass
class StageResult:
    stage: str
    seconds: float
    rows: int | None = None


def _git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=REPO, text=True
        ).strip()
    except Exception:
        return "unknown"


def _machine_profile() -> dict[str, str]:
    return {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cpu_count": str(os.cpu_count() or "unknown"),
        "machine": platform.machine(),
    }


def _row_count(table: Path) -> int | None:
    try:
        from lakehouse.spark import get_spark

        spark = get_spark("benchmark-count")
        return spark.read.format("delta").load(str(table)).count()
    except Exception:  # pragma: no cover
        return None


def _time_stage(name: str, args: list[str]) -> StageResult:
    print(f"→ {name}: {' '.join(args)}", flush=True)
    t0 = time.perf_counter()
    subprocess.run(args, cwd=REPO, check=True)
    elapsed = time.perf_counter() - t0
    print(f"  ✓ {name} in {elapsed:.1f}s", flush=True)
    return StageResult(stage=name, seconds=round(elapsed, 2))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--with-ml",
        action="store_true",
        help="Also run MLflow training + registry stages.",
    )
    args = parser.parse_args()

    # Isolate the run under a scratch LAKEHOUSE_ROOT so benchmarks are
    # deterministic and don't pollute the developer's data directory.
    scratch = Path(tempfile.mkdtemp(prefix="lakehouse-bench-"))
    os.environ["LAKEHOUSE_ROOT"] = str(scratch)
    print(f"scratch root: {scratch}")

    try:
        results: list[StageResult] = []
        py = sys.executable

        results.append(_time_stage("sample-data", [py, "-m", "data.sample_raw.download"]))
        results.append(_time_stage("bronze", [py, "-m", "ingestion.batch_ingest"]))
        results.append(_time_stage("silver", [py, "-m", "transform.silver_clean"]))
        results.append(_time_stage("gold", [py, "-m", "transform.gold_aggregate"]))

        if args.with_ml:
            results.append(_time_stage("ml-train", [py, "-m", "ml.train_model"]))
            results.append(_time_stage("ml-register", [py, "-m", "ml.register_model"]))

        # Attach row counts to the layered tables
        from lakehouse import paths as _paths  # noqa: E402

        counts = {
            "bronze": _row_count(_paths.BRONZE_TRANSACTIONS),
            "silver": _row_count(_paths.SILVER_TRANSACTIONS),
            "gold-daily-revenue": _row_count(_paths.GOLD_DAILY_REVENUE),
            "gold-customer-ltv": _row_count(_paths.GOLD_CUSTOMER_LTV),
        }

        profile = _machine_profile()
        sha = _git_sha()
        total = round(sum(r.seconds for r in results), 2)

        payload = {
            "commit": sha,
            "profile": profile,
            "stages": [asdict(r) for r in results],
            "row_counts": counts,
            "total_seconds": total,
            "with_ml": args.with_ml,
        }

        OUT_JSON.write_text(json.dumps(payload, indent=2) + "\n")

        lines = [
            "# Benchmark run",
            "",
            "**⚠ Reproducibility receipt — not a comparative claim.**",
            "These numbers describe how long the pipeline takes on the machine",
            "and commit listed below. Re-running on different hardware or with",
            "a different sample size will yield different numbers.",
            "",
            f"- **Commit:** `{sha}`",
            f"- **Platform:** {profile['platform']}",
            f"- **Python:** {profile['python']}",
            f"- **CPU count:** {profile['cpu_count']}",
            f"- **With ML:** {args.with_ml}",
            f"- **Total wall-clock:** **{total}s**",
            "",
            "## Stage timings",
            "",
            "| Stage | Seconds |",
            "| --- | --- |",
        ]
        for r in results:
            lines.append(f"| `{r.stage}` | {r.seconds} |")
        lines += [
            "",
            "## Row counts",
            "",
            "| Table | Rows |",
            "| --- | --- |",
        ]
        for name, n in counts.items():
            lines.append(f"| `{name}` | {'-' if n is None else f'{n:,}'} |")
        lines.append("")
        OUT_MD.write_text("\n".join(lines))

        print(f"\nwrote {OUT_MD.relative_to(REPO)} and {OUT_JSON.relative_to(REPO)}")
        print(f"total: {total}s")
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


if __name__ == "__main__":
    main()
