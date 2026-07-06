# Agent / AI context

## Project

- **Purpose**: Reference implementation of the Databricks lakehouse pattern (Bronze/Silver/Gold medallion, Delta Lake, MLflow, OSS RAG, Streamlit serving) that runs entirely on open-source tooling and maps 1:1 to Databricks.
- **Stack**: Python 3.11+ (3.12 also tested), Java 17 (required by PySpark), PySpark 3.5.8 + delta-spark 3.2.1, MLflow 3.11, scikit-learn, chromadb + sentence-transformers (RAG), Streamlit (serving). Pins live in `requirements.txt` (single source of truth).
- **How to run**: `make preflight && make pipeline` (Bronze→Silver→Gold). Then `make ml` (train + register), `make rag` (build index + query), `make serve` (Streamlit at http://localhost:8501). `make dag` runs the orchestrated DAG.
- **How to test**: `make test` (fast) or `make test-cov` (with coverage). `make ci` runs lint + tests (what CI gates on).
- **How to lint / typecheck**: `make lint` (ruff). `make format` applies `ruff format`. Pre-commit hooks in `.pre-commit-config.yaml`.

## Environment notes

- **Java 17 is mandatory** for PySpark 3.5. `make preflight` (`python -m lakehouse.env`) fails fast with install hints if Java is missing/too old. Set `JAVA_HOME` to a JDK 17.
- The full Spark/ML stack requires network + Java 17. Pure-logic tests (orchestration, `compose_answer`, `_best_run`, `engineer_features`, serving KPI math) need only pandas and run without Spark.
- Editable install: `pip install -e .` (packaging in `pyproject.toml`) makes every module importable from any CWD.

## CI layout

- **`ci` workflow** (`.github/workflows/ci.yml`): PR-gating, runs on pushes to any branch + PRs to main. Installs `requirements-ci.txt` (a strict subset of `requirements.txt`, pins enforced identical by `tests/test_requirements_ci.py`), then ruff + editable-install import smoke + `pytest -k "not rag and not mlflow"` with coverage + an ETL DAG smoke. Matrix: Python 3.11 + 3.12.
- **`nightly` workflow** (`.github/workflows/nightly.yml`): schedule + `workflow_dispatch`. Installs the full `requirements.txt` and smoke-tests `make ml` + `make rag` plus `@slow` tests. Use this to verify ML/RAG paths the fast lane skips. (Note: `workflow_dispatch` only works once the workflow is on the default branch.)

## Conventions for AI changes

- Prefer the smallest diff that solves the task; avoid drive-by refactors.
- Follow existing naming, formatting, and test patterns; keep synthetic-data determinism (`_SEED = 42` in `data/sample_raw/download.py`) intact — README KPIs and tests depend on it.
- Keep heavy imports (mlflow, chromadb, sentence-transformers, streamlit, Spark) lazy inside functions so pure-logic stays unit-testable in the fast lane.
- Do not introduce network calls into the PR-gating test lane (CI reproducibility is non-negotiable).
- Never commit secrets; use env vars documented in the README and `.gitignore`.
- After substantive edits, run `make ci` (or the closest subset the environment allows) before considering work done.

## Repo map

- `lakehouse/` — Spark/Delta bootstrap (`spark.py`), path constants (`paths.py`), schemas (`schemas.py`), env/preflight + logger (`env.py`).
- `data/sample_raw/` — deterministic synthetic dataset loader.
- `ingestion/` — batch (`batch_ingest.py`) and Structured Streaming (`streaming_ingest.py`) writes into Bronze.
- `transform/` — Silver cleansing + MERGE + quality gates + OPTIMIZE/Z-ORDER/VACUUM (`silver_clean.py`); Gold marts (`gold_aggregate.py`).
- `pipelines/` — DLT-analog declarative pipeline with the `Expectation` DSL (drop/fail/quarantine).
- `orchestration/` — DAG runner with topo-sort + retry (`workflow.py`).
- `ml/` — MLflow tracking (`train_model.py`), registry (`register_model.py`), OSS RAG (`rag_demo/rag_pipeline.py`).
- `serving/` — Streamlit gold explorer (`app.py`) + pure KPI helpers (`metrics.py`).
- `governance/` — Unity Catalog SQL (runnable on paid tiers).
- `tests/` — unit + end-to-end + data-quality tests; fixtures in `conftest.py`.
- `.github/workflows/` — `ci.yml`, `nightly.yml`, `docs.yml`, `release.yml`.
- `Makefile` — canonical entry points (source of truth for run/test/lint commands).
