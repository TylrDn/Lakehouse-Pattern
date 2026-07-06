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
| Liquid clustering (`CLUSTER BY`) | ✅ | [`transform/modern_delta.py::cluster_by`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/transform/modern_delta.py) |
| Deletion vectors | ✅ | [`transform/modern_delta.py::enable_modern_features`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/transform/modern_delta.py) |
| Change Data Feed | ✅ | [`transform/cdc/apply_changes.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/transform/cdc/apply_changes.py) |
| APPLY CHANGES INTO (CDC MERGE) | ✅ | Same file — [`apply_changes_into`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/transform/cdc/apply_changes.py) |
| SCD Type 2 dimension | ✅ | [`transform/scd2/customer_dim.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/transform/scd2/customer_dim.py) |

## Ingestion & pipelines

| Concept | Status | Proof |
| --- | --- | --- |
| Batch ingest | ✅ | [`ingestion/batch_ingest.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/ingestion/batch_ingest.py) |
| Structured Streaming | ✅ | [`ingestion/streaming_ingest.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/ingestion/streaming_ingest.py) |
| Auto Loader (`cloudFiles`) | 📄 | Docstring in [`streaming_ingest.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/ingestion/streaming_ingest.py) — identical code shape |
| Kafka source + continuous processing | 🧩 | [`ingestion/streaming/kafka_source.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/ingestion/streaming/kafka_source.py) |
| Watermarks + stateful aggregation | ✅ | Same file — 1-minute tumbling revenue window |
| foreachBatch idempotency (`txnAppId`) | ✅ | Same file |
| Delta Live Tables (DLT) — declarative shape | 🧩 | [`pipelines/declarative_pipeline.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/pipelines/declarative_pipeline.py) |
| DLT incremental refresh + backfill | 🧩 | [`pipelines/incremental/pipeline.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/pipelines/incremental/pipeline.py) |
| DLT expectations DSL + metrics | 🧩 | Same file — metrics land in `expectations_metrics.jsonl` |
| Quarantine (rejects table) | ✅ | `silver.transactions_rejects` append-only Delta |
| Databricks Workflows (basic) | 🧩 | [`orchestration/workflow.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/orchestration/workflow.py) |
| Databricks Workflows (schedules, sensors, alerts) | 🧩 | [`orchestration/dagster_project/`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/orchestration/dagster_project/) |
| Databricks Workflows (task-based alt) | 🧩 | [`orchestration/airflow_dags/lakehouse_dag.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/orchestration/airflow_dags/lakehouse_dag.py) |

## ML & AI

| Concept | Status | Proof |
| --- | --- | --- |
| MLflow tracking | ✅ | [`ml/train_model.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/ml/train_model.py) |
| MLflow Model Registry | ✅ | [`ml/register_model.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/ml/register_model.py) |
| Databricks Model Serving | 📄 | Docstring in [`register_model.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/ml/register_model.py) — swap file URI for the endpoint |
| Vector Search (sidecar) | 🧩 | [`ml/rag_demo/rag_pipeline.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/ml/rag_demo/rag_pipeline.py) using chromadb + MiniLM |
| Vector Search (Delta-native) | 🧩 | [`ml/vector_search/index.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/ml/vector_search/index.py) — embedding column + hnswlib ANN |
| Feature Store / Feature Engineering in UC | 🧩 | [`ml/feature_store/store.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/ml/feature_store/store.py) — Delta + point-in-time joins |
| AI Functions (`ai_query`, `ai_classify`, `ai_extract`) | 🧩 | [`ml/ai_functions/udfs.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/ml/ai_functions/udfs.py) — Spark SQL UDFs |
| AI/BI Genie (NL-to-SQL) | 🧩 | [`serving/genie/app.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/serving/genie/app.py) |

## Governance

| Concept | Status | Proof |
| --- | --- | --- |
| Unity Catalog (paid) | 📄 | [`governance/unity_catalog_setup.sql`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/governance/unity_catalog_setup.sql) |
| UC local analog (Polaris + OPA) | 🧩 | [`governance/local_uc/`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/governance/local_uc/) — policies.sql, authz.py, secrets.py |
| Grants (`GRANT SELECT ON …`) | 🧩 | [`governance/opa/policies.rego`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/governance/opa/policies.rego) enforced by [`authz.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/governance/local_uc/authz.py) |
| UC tags | 📄 | `governance/unity_catalog_setup.sql`, §3 |
| Row-level security + column masking | 🧩 | [`governance/local_uc/policies.sql`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/governance/local_uc/policies.sql) — secure views |
| Table + column lineage (auto) | 📄 | `unity_catalog_setup.sql`, §6 |
| Lineage collector (OpenLineage) | 🧩 | [`governance/lineage/emitter.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/governance/lineage/emitter.py) |
| Secrets (`dbutils.secrets`) | 🧩 | [`governance/local_uc/secrets.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/governance/local_uc/secrets.py) |
| SCIM identity federation | 🧩 | [`governance/local_uc/scim_users.json`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/governance/local_uc/scim_users.json) |
| Audit logs (`system.access.audit`) | 🧩 | `authz.py` writes to `audit_log.jsonl` |
| System tables (`system.*`) | 🧩 | [`governance/local_uc/system_tables.sql`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/governance/local_uc/system_tables.sql) |
| Documented lineage (repo-side) | ✅ | [Lineage doc](lineage.md) |

## Serving

| Concept | Status | Proof |
| --- | --- | --- |
| Databricks Apps | 🧩 | [`serving/app.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/serving/app.py) — Streamlit; same code deploys to Databricks Apps |
| Serverless SQL / DBSQL | 📄 | [Architecture cost model](architecture.md) + `compute/sql_warehouse.json` |
| Delta Sharing | 🧩 | [`sharing/`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/sharing/) — OSS server config + client demo |
| Lakehouse Federation (JDBC foreign catalog) | 🧩 | [`federation/postgres_catalog.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/federation/postgres_catalog.py) |
| Lakebase / OLTP + reverse ETL | 🧩 | [`lakebase/`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/lakebase/) — reverse_etl.py + pg_cdc.py |
| Compute tiers (all-purpose / jobs / SQL WH / serverless) | 📄 | [`compute/`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/compute/) — config JSON per tier |
| Databricks Asset Bundles (DABs) | 📄 | [`infra/bundle/databricks.yml`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/infra/bundle/databricks.yml) |
| Terraform (databricks provider) | 📄 | [`infra/terraform/main.tf`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/infra/terraform/main.tf) |
| Notebook-native workflow (`%run`, `%sql`, widgets) | ✅ | [`notebooks/databricks_style/`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/notebooks/databricks_style/) |
| Real dataset ingest (UCI Online Retail II) | ✅ | [`data/uci_online_retail/download.py`](https://github.com/TylrDn/Lakehouse-Pattern/blob/main/data/uci_online_retail/download.py) |

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
| Model Serving endpoint (deployed) | 🚧 | Requires a paid Databricks workspace or a self-hosted MLflow serving container |
| Delta Lake UniForm (Iceberg-visible Delta) | 🚧 | Config-only; Delta 3.2 supports it via TBLPROPERTIES |
| Predictive optimization / auto liquid clustering | 🚧 | Databricks-exclusive today |
| Photon runtime | 🚧 | Databricks-exclusive |
| Cross-region Delta Sharing over C2C | 🚧 | Requires managed sharing on both sides |

**Legend.** ✅ implemented • 🧩 OSS analog for a closed-source Databricks
feature • 📄 documented and runnable on paid Databricks • 🚧 clearly-scoped
extension point.
