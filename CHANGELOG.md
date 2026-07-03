# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Streamlit gold-explorer hero screenshot in the README.
- Terminal-demo SVG showing `make preflight` → `make dag` → `make serve`.
- Social preview PNG (1280×640) at `docs/images/social-preview.png`.
- `docs/lineage.md` — table-level Mermaid + column-level lineage tables.
- `release`, `code style: ruff`, `pre-commit`, and `docs` badges on the README.
- MkDocs Material site auto-deployed to GitHub Pages on every push to main
  ([workflow](.github/workflows/docs.yml)).
- `docs/index.md` landing page and `docs/prebuild.py` that mirrors
  root-level markdown into the docs tree at build time.
- Concept-coverage matrix rewritten with GitHub links to the exact file
  (and where possible, the exact function or test) that implements each row.
- `notebooks/01_lakehouse_walkthrough.ipynb` — scrollable Bronze → Silver
  → Gold walkthrough with time-travel demo and optional MLflow cell.
- `benchmarks/run_benchmarks.py` — reproducible pipeline timings in a
  scratch workspace; writes `benchmarks/latest.md` + `latest.json` with
  commit SHA and machine profile. Explicitly framed as a reproducibility
  receipt, not a comparative claim.
- `.devcontainer/devcontainer.json` — one-click GitHub Codespaces / VS Code
  Dev Container with Python 3.11 + Java 17, auto-runs `make setup && make
  preflight`, forwards ports 8501 / 5000 / 8000.
- `Dockerfile` and `docker-compose.yml` — three services (`app`, `mlflow`,
  `streamlit`) sharing a repo bind-mount and a named `mlruns` volume so the
  full stack comes up with `docker compose up`.
- README Quickstart restructured into three parallel options: Codespaces,
  Docker Compose, and local venv.

### Fixed
- `serving/app.py` now works when `streamlit run` is invoked from a
  directory other than the repo root (self-adds the repo root to
  `sys.path`). Previously failed with `ModuleNotFoundError: lakehouse`.

### Security
- Bumped MLflow 2.14.1 -> 3.11.1, pyspark 3.5.1 -> 3.5.8, delta-spark 3.2.0 -> 3.2.1,
  pyarrow 16.1.0 -> 23.0.1, requests 2.32.3 -> 2.34.2, streamlit 1.36.0 -> 1.54.0,
  pytest 8.2.2 -> 9.0.3. Closes the 42 Dependabot alerts flagged on the
  0.1.0 pin (6 critical / 24 high / 10 moderate / 2 low). Full suite still
  17/17 green.

### Changed
- `ml/train_model.py` now uses `mlflow.sklearn.log_model(..., name=...)`
  (MLflow 3 API) instead of the deprecated `artifact_path=` kwarg.
- `ml/register_model.py` promotes via the alias API by default; the deprecated
  `transition_model_version_stage` call is gated behind `--legacy-stage`.

## [0.1.0] — 2026-07-03

Initial public release. Complete Bronze/Silver/Gold lakehouse walkthrough with
streaming ingest, declarative expectations, MLflow tracking + registry, an OSS
RAG demo, a Streamlit gold explorer, and a reproducible end-to-end DAG.

### Added
- Shared Spark bootstrap (`lakehouse/spark.py`) with delta-spark configuration
  and reproducible sample-data generation.
- Batch ingest (`ingestion.batch_ingest`) and structured streaming ingest
  (`ingestion.streaming_ingest`) into Bronze Delta tables.
- Silver transformations with MERGE upserts, plus OPTIMIZE / Z-ORDER / VACUUM
  maintenance (`transform.silver_clean`).
- Gold aggregations for daily-revenue and customer-lifetime marts
  (`transform.gold_aggregate`).
- Declarative pipeline with lightweight in-house expectations
  (`pipelines.declarative_pipeline`) and a Unity-Catalog-style SQL setup file.
- Orchestration DAG runner (`orchestration.workflow`) with `--skip-ml` support.
- MLflow training + model registry integration for a daily revenue forecaster
  (`ml.train_model`, `ml.register_model`).
- OSS RAG demo powered by ChromaDB + sentence-transformers
  (`ml.rag_demo.rag_pipeline`).
- Streamlit gold-explorer serving app (`serving/app.py`).
- Java 17+ preflight and shared structured logger (`lakehouse.env`).
- `TROUBLESHOOTING.md` covering the top-10 setup pitfalls.
- CI (`.github/workflows/ci.yml`) with lint + preflight smoke test + pytest,
  pinned to Python 3.11 and Java 17.
- Makefile targets: `preflight`, `sample-data`, `pipeline`, `dag`, `stream`,
  `declarative`, `test`, `lint`, `format`, `clean`.
- 17 pytest tests covering ingest schema enforcement, silver upserts, gold
  math, declarative expectations, environment preflight, and DAG smoke tests.
- Architecture and concept-coverage docs under `docs/`.

### Notes
- Delta Lake 3.2 requires Java 17 or newer. Run `make preflight` if unsure.
- MLflow artifacts default to `<repo>/mlruns/`. Override with
  `MLFLOW_TRACKING_URI` if needed.

[Unreleased]: https://github.com/TylrDn/Lakehouse-Pattern/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/TylrDn/Lakehouse-Pattern/releases/tag/v0.1.0
