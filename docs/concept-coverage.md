# Databricks concept coverage

Every concept below is either **fully implemented** in runnable OSS code, or
**documented as an extension point** with a paragraph describing how it would
be built in a paid Databricks tier.

| Concept | Status | Where |
| --- | --- | --- |
| Medallion architecture (Bronze/Silver/Gold) | Implemented | `ingestion/`, `transform/`, `data/bronze,silver,gold` |
| Delta Lake ACID writes | Implemented | Every write in `ingestion/` and `transform/` |
| Delta schema enforcement | Implemented | `lakehouse/schemas.py`, silver_clean writes |
| Delta schema evolution (`mergeSchema`) | Implemented | `ingestion/batch_ingest.py` |
| Delta MERGE upsert | Implemented | `transform/silver_clean.py::_merge_upsert` |
| Delta time travel (`versionAsOf`) | Implemented | `tests/test_delta_features.py::test_time_travel_returns_earlier_snapshot` |
| OPTIMIZE + Z-ORDER | Implemented | `transform/silver_clean.py::_optimize_and_vacuum` |
| VACUUM (retention) | Implemented | Same file, `VACUUM ... RETAIN 168 HOURS` |
| Partitioning | Implemented | Silver partitioned by `event_date` |
| Structured Streaming | Implemented | `ingestion/streaming_ingest.py` |
| Auto Loader (`cloudFiles`) | Documented | `streaming_ingest.py` docstring |
| Delta Live Tables (declarative pipelines) | OSS analog + doc | `pipelines/declarative_pipeline.py` |
| Data-quality expectations | Implemented (OSS analog) | `pipelines/declarative_pipeline.py::Expectation` |
| Spark DataFrames + Spark SQL | Implemented | Throughout `transform/` |
| Databricks Workflows (orchestration) | OSS analog + doc | `orchestration/workflow.py` |
| MLflow tracking | Implemented | `ml/train_model.py` |
| MLflow model registry | Implemented | `ml/register_model.py` |
| Model serving | Documented | `ml/register_model.py` docstring; swap for Model Serving endpoint |
| Vector Search / RAG | Implemented (OSS) | `ml/rag_demo/rag_pipeline.py` |
| Databricks Apps | OSS analog | `serving/app.py` (Streamlit) |
| Unity Catalog (catalogs/schemas/grants) | Documented (SQL runnable on paid tier) | `governance/unity_catalog_setup.sql` |
| Unity Catalog tags | Documented | Same file, §3 |
| Row-level security + column masking | Documented | Same file, §5 |
| Lineage | Documented | Same file, §6 (UC captures automatically) |
| Databricks Lakebase (Postgres OLTP) | Extension point | See README roadmap |
| AI/BI Genie (natural-language BI) | Extension point | See README roadmap |
| Delta Sharing | Extension point | See README roadmap |
| Query federation (foreign catalogs) | Extension point | See README roadmap |
| Serverless SQL / DBSQL | Documented cost impact | `docs/architecture.md#cost-model` |
| CI/CD | Implemented | `.github/workflows/ci.yml` |
| Data-quality testing | Implemented | `tests/test_data_quality.py` |
