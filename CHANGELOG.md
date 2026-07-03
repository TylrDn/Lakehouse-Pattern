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
- `release`, `code style: ruff`, and `pre-commit` badges on the README.

### Fixed
- `serving/app.py` now works when `streamlit run` is invoked from a
  directory other than the repo root (self-adds the repo root to
  `sys.path`). Previously failed with `ModuleNotFoundError: lakehouse`.

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
