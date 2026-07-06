# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] — 2026-07-06

Launch release. Hardens the 0.1.0 reference implementation for public use:
reconciles CI with the shipped dependency stack, closes a silent data-loss
trap in the declarative pipeline, adds deterministic packaging, raises test
coverage on previously-untested modules, and reconciles the docs/roadmap for
an honest launch story. Verified end-to-end by the fast CI lane (3.11 + 3.12)
plus a full-stack nightly job that exercises `make ml` and `make rag`.

### Added
- Expectation `quarantine` action: violating rows are written to a
  `<target>_rejects` Delta table (append, `mergeSchema`) instead of being
  silently dropped, mirroring the Silver `_write_rejects` pattern; unknown
  actions now raise `ValueError` at construction time (fail-closed). (P0-2)
- `pyproject.toml` (PEP 621) so the project is installable with
  `pip install -e .` and its modules import deterministically from any working
  directory; pytest config migrated from `pytest.ini`. (P0-3)
- `requirements-ci.txt` (a strict subset of `requirements.txt`) and
  `tests/test_requirements_ci.py`, which fails if the two ever drift, so CI
  can only ever test the shipped stack. (P0-1)
- `.github/workflows/nightly.yml` — scheduled + `workflow_dispatch` job that
  installs the full stack and smoke-tests `make ml` + `make rag` plus
  `@slow` tests, closing the gap left by the Spark-only fast lane. (P1-6)
- Coverage reporting: `pytest-cov`, a `make test-cov` target, and coverage in
  CI (baseline ~75%). (P1-1)
- Unit tests for the orchestration DAG runner (topological order, unknown
  dependency, retry/backoff, fail-fast abort, `skip_ml`). (P1-2)
- Unit tests for the Expectation DSL (`drop`/`fail`/`quarantine`/invalid) and
  an end-to-end quarantine run. (P1-3)
- Structured Streaming ingest test (`Trigger.AvailableNow`, `pathGlobFilter`
  scoping, checkpoint creation). (P1-4)
- `serving/metrics.py` with pure KPI/pivot/top-N helpers, an extracted
  `engineer_features` in `ml/train_model.py`, and unit tests for
  `compose_answer`, `_best_run`, `engineer_features`, and the serving KPI
  math — all runnable in the fast lane without the heavy ML stack. (P1-5)
- `AGENTS.md` documenting environment prerequisites, canonical `make`
  commands, the CI vs nightly layout, and the repo map. (P1-7)
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
- The `data` Makefile target now runs `python -m data.sample_raw.download`
  (consistent with every other target) instead of a direct script path, so
  `make data`/`make ml`/`make bronze` resolve first-party imports on a clean
  `make setup` and in the nightly job. Previously failed with
  `ModuleNotFoundError: No module named 'lakehouse'`.
- CI now installs from `requirements-ci.txt` (identical pins to
  `requirements.txt`) instead of a hand-maintained inline list that had
  drifted, so a green check reflects the stack users actually install. Bumped
  the Delta package coordinate in `lakehouse/spark.py` to `3.2.1` and fixed
  the stale `Dockerfile` version comment. (P0-1)

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
- CI runs on Python 3.11 and 3.12 (both green), on pushes to any branch and
  PRs to `main`, with a concurrency group and an editable-install import
  smoke step. (P1-6, P0-3)
- Heavy dependencies (mlflow, scikit-learn, chromadb, sentence-transformers)
  and the Spark bootstrap are imported lazily inside the functions that need
  them, so pure-logic helpers stay importable and unit-testable in the fast
  lane. No runtime behavior change. (P1-5)
- README and `docs/concept-coverage.md` reconciled with the shipped code; all
  six deferred roadmap items are explicitly labeled extension points "out of
  scope for v1.0" (documented-only, not implemented). (P1-8, P2-Doc)

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

[Unreleased]: https://github.com/TylrDn/Lakehouse-Pattern/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/TylrDn/Lakehouse-Pattern/compare/v0.1.0...v1.0.0
[0.1.0]: https://github.com/TylrDn/Lakehouse-Pattern/releases/tag/v0.1.0
