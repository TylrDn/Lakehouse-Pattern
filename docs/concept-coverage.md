# Databricks concept coverage

Every concept below is either **fully implemented** in runnable OSS code
(✅), a **runnable OSS analog** for a closed-source Databricks feature
(🧩), **documented and runnable on a paid Databricks tier** (📄), or a
**clearly-scoped extension point** with a design sketch in the roadmap
(🚧).

Every row links to the exact file — and where possible, the exact
function or test — that implements or documents it. Click through to
walk the codebase feature by feature.

## Storage & Delta Lake

| Concept | Status | Proof |
| --- | --- | --- |
| Medallion architecture (Bronze → Silver → Gold) | ✅ | [`ingestion/`](https://github.com/TylrDn/Lakehouse-Pattern/tree/main/ingestion), [`transform/`](https://github.com/TylrDn/Lakehouse-Pattern/tree/main/transform) |
| Delta Lake ACID writes | ✅ | Every write; [`tests/test_delta_features.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/tests/test_delta_features.py) |
| Delta schema enforcement | ✅ | [`lakehouse/schemas.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/lakehouse/schemas.py), [`test_schema_enforcement_rejects_bad_write`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/tests/test_delta_features.py#L60) |
| Delta schema evolution (`mergeSchema`) | ✅ | [`ingestion/batch_ingest.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/ingestion/batch_ingest.py) |
| Delta MERGE upsert | ✅ | [`transform/silver_clean.py::_merge_upsert`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/transform/silver_clean.py#L135) |
| Delta time travel (`versionAsOf`) | ✅ | [`test_time_travel_returns_earlier_snapshot`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/tests/test_delta_features.py#L37) |
| OPTIMIZE + Z-ORDER | ✅ | [`transform/silver_clean.py::_optimize_and_vacuum`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/transform/silver_clean.py#L168) |
| VACUUM (retention) | ✅ | Same file (`VACUUM … RETAIN 168 HOURS`) |
| Partitioning + tuning | ✅ | Silver partitioned by `event_date`; `spark.sql.shuffle.partitions` in [`lakehouse/spark.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/lakehouse/spark.py) |

## Ingestion & pipelines

| Concept | Status | Proof |
| --- | --- | --- |
| Batch ingest | ✅ | [`ingestion/batch_ingest.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/ingestion/batch_ingest.py) |
| Structured Streaming | ✅ | [`ingestion/streaming_ingest.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/ingestion/streaming_ingest.py) |
| Auto Loader (`cloudFiles`) | 📄 | Docstring in [`streaming_ingest.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/ingestion/streaming_ingest.py) — identical code shape |
| Delta Live Tables (DLT) | 🧩 | [`pipelines/declarative_pipeline.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/pipelines/declarative_pipeline.py) — declarative pattern in OSS |
| DLT expectations DSL | 🧩 | [`Expectation` class](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/pipelines/declarative_pipeline.py#L63) |
| Quarantine (rejects table) | ✅ | Same file — `silver.transactions_rejects` append-only Delta |
| Databricks Workflows (orchestration) | 🧩 | [`orchestration/workflow.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/orchestration/workflow.py) with retry semantics |

## ML & AI

| Concept | Status | Proof |
| --- | --- | --- |
| MLflow tracking | ✅ | [`ml/train_model.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/ml/train_model.py) |
| MLflow Model Registry | ✅ | [`ml/register_model.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/ml/register_model.py) |
| Databricks Model Serving | 📄 | Docstring in [`register_model.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/ml/register_model.py) — swap file URI for the endpoint |
| Vector Search / RAG | 🧩 | [`ml/rag_demo/rag_pipeline.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/ml/rag_demo/rag_pipeline.py) using chromadb + MiniLM |
| Feature store | 🚧 | Extension point — MLflow tracks features today; drop-in swap for FS |

## Governance

| Concept | Status | Proof |
| --- | --- | --- |
| Unity Catalog (catalogs/schemas/grants) | 📄 | [`governance/unity_catalog_setup.sql`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/governance/unity_catalog_setup.sql) — runnable on paid tier |
| UC tags | 📄 | Same file, §3 |
| Row-level security + column masking | 📄 | Same file, §5 |
| Table + column lineage | 📄 | Same file, §6; UC captures automatically |
| Documented lineage (repo-side) | ✅ | [Lineage doc](lineage.md) — hand-curated Mermaid + column-level tables |

## Serving

| Concept | Status | Proof |
| --- | --- | --- |
| Databricks Apps | 🧩 | [`serving/app.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/serving/app.py) — Streamlit; same code deploys to Databricks Apps |
| Serverless SQL / DBSQL | 📄 | [Architecture cost model](architecture.md) |

## Testing, CI, DX

| Concept | Status | Proof |
| --- | --- | --- |
| CI/CD | ✅ | [`.github/workflows/ci.yml`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/.github/workflows/ci.yml) — ruff + editable-install import smoke + pytest (with coverage) + ETL smoke, on Python 3.11 & 3.12 |
| Nightly heavy job (ML/RAG smoke) | ✅ | [`.github/workflows/nightly.yml`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/.github/workflows/nightly.yml) — full stack + `make ml` / `make rag` |
| Coverage reporting | ✅ | `make test-cov`; `pytest-cov` in CI (non-blocking baseline) |
| Docs site auto-deploy | ✅ | [`.github/workflows/docs.yml`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/.github/workflows/docs.yml) — MkDocs Material → GitHub Pages |
| Release automation | ✅ | [`.github/workflows/release.yml`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/.github/workflows/release.yml) — tags → GitHub Releases |
| Dependabot | ✅ | [`.github/dependabot.yml`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/.github/dependabot.yml) — weekly pip + actions bumps |
| Pre-commit hooks | ✅ | [`.pre-commit-config.yaml`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/.pre-commit-config.yaml) |
| Unit + integration tests | ✅ | [`tests/`](https://github.com/TylrDn/Lakehouse-Pattern/tree/main/tests) — unit + end-to-end coverage of transforms, Delta features, orchestration, declarative pipeline, streaming, and pure ML/serving logic |
| Data-quality tests | ✅ | [`tests/test_data_quality.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/tests/test_data_quality.py) |
| Java 17 preflight | ✅ | [`lakehouse/env.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/lakehouse/env.py) |
| Structured logger | ✅ | Same file — used across every module |
| Reproducible benchmarks | ✅ | [`benchmarks/run_benchmarks.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/benchmarks/run_benchmarks.py) |

## Roadmap / extension points

| Concept | Status | Notes |
| --- | --- | --- |
| Databricks Lakebase (managed Postgres OLTP) | 🚧 | See [README roadmap §1](https://github.com/TylrDn/Lakehouse-Pattern#roadmap--extension-points) |
| AI/BI Genie (NL-to-SQL) | 🚧 | See [README roadmap §2](https://github.com/TylrDn/Lakehouse-Pattern#roadmap--extension-points) |
| Delta Sharing | 🚧 | See [README roadmap §3](https://github.com/TylrDn/Lakehouse-Pattern#roadmap--extension-points) |
| Query federation (foreign catalogs) | 🚧 | See [README roadmap §4](https://github.com/TylrDn/Lakehouse-Pattern#roadmap--extension-points) |
| Model Serving endpoint (deployed) | 🚧 | See [README roadmap §5](https://github.com/TylrDn/Lakehouse-Pattern#roadmap--extension-points) |
| Real dataset ingestion (UCI Online Retail II) | 🚧 | See [README roadmap §6](https://github.com/TylrDn/Lakehouse-Pattern#roadmap--extension-points) |

**Legend.** ✅ implemented • 🧩 OSS analog for a closed-source Databricks
feature • 📄 documented and runnable on paid Databricks • 🚧 clearly-scoped
extension point.
