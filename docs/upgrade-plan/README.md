# Concept-Coverage v2 — Upgrade Plan

This directory documents the second wave of Databricks-concept coverage for
the Lakehouse-Pattern reference implementation. It closes the 19 gaps
identified in the concept-coverage audit and lands them on `main` behind a
single feature branch (`feat/concept-coverage-v2`).

The parent doc is [`docs/concept-coverage.md`](../concept-coverage.md) — that
file remains the source of truth for status per concept. This directory
contains the *design notes and Databricks-native mappings* for each new
module.

## Ranked task list

| # | Concept | Path | Status |
| --- | --- | --- | --- |
| 1 | Local Unity Catalog analog (Polaris + REST + grants) | `governance/local_uc/` | 🧩 spike |
| 1 | RLS + column masking via secure views | `governance/local_uc/policies.sql` | 🧩 spike |
| 1 | Lineage collector (OpenLineage → JSON) | `governance/lineage/` | 🧩 spike |
| 2 | Incremental DLT analog (foreachBatch + checkpoints) | `pipelines/incremental/` | 🧩 spike |
| 4 | Change Data Feed + APPLY CHANGES INTO | `transform/cdc/` | ✅ |
| 4 | SCD Type 2 in Silver | `transform/scd2/` | ✅ |
| 5 | Liquid clustering (`CLUSTER BY`) | `transform/silver_clean.py` | ✅ |
| 6 | Deletion vectors + MERGE tuning | `transform/silver_clean.py` | ✅ |
| 3 | Dagster orchestration | `orchestration/dagster_project/` | 🧩 spike |
| 3 | Airflow DAG | `orchestration/airflow_dags/` | 🧩 spike |
| 7 | Feature Store analog (Delta + point-in-time) | `ml/feature_store/` | 🧩 spike |
| 8 | Delta-native Vector Search (embedding column + ANN) | `ml/vector_search/` | 🧩 spike |
| 9 | AI Functions (`ai_query`, `ai_classify`, `ai_extract` UDFs) | `ml/ai_functions/` | 🧩 spike |
| 10 | Genie NL-to-SQL over Gold | `serving/genie/` | 🧩 spike |
| 11 | Delta Sharing OSS server config | `sharing/` | 🧩 spike |
| 12 | Lakehouse Federation (JDBC foreign catalog) | `federation/` | 🧩 spike |
| 13 | Lakebase / OLTP + reverse ETL | `lakebase/` | 🧩 spike |
| 14 | Compute-tier concepts (all-purpose vs jobs vs SQL vs serverless) | `compute/` | 📄 |
| 15 | Databricks Asset Bundles + Terraform | `infra/` | 📄 |
| 16 | Secrets / SCIM / audit / system tables | `governance/local_uc/` | 📄 |
| 17 | UCI Online Retail II ingest | `data/uci_online_retail/` | ✅ |
| 18 | Continuous streaming + Kafka + watermarks | `ingestion/streaming/` | 🧩 spike |
| 19 | Notebook-native workflow (`%run`, widgets, `%sql`) | `notebooks/databricks_style/` | ✅ |

**Legend.** ✅ real working code with tests · 🧩 runnable spike (OSS analog
for a closed-source Databricks feature) · 📄 documented + config, runnable
on paid Databricks.

## Non-goals for this branch

* Replacing the existing Bronze/Silver/Gold pipeline. All new modules are
  additive — the `make dag` target still runs the original flow end-to-end.
* Bumping PySpark/NumPy/PyArrow/sentence-transformers to major versions.
  That coordinated migration is tracked in issue #29 and lands on a
  separate branch.
* Writing a truly production-grade Polaris/OpenLineage stack. The point of
  the OSS analog is to make the *concept* runnable locally and show the
  exact swap path to Databricks Unity Catalog / DLT / Vector Search.

## How to run each new spike

Every module below has its own README with a one-line command. A summary is
also in the top-level `Makefile` under new targets: `make governance`,
`make cdc`, `make dagster`, `make airflow`, `make features`, `make vec`,
`make ai`, `make genie`, `make share`, `make federate`, `make lakebase`,
`make bundle`, `make uci`, `make stream-continuous`.
