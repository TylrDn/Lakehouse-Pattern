# Compute tiers

Databricks separates compute into four tiers with different pricing,
scaling, and use cases. This directory documents the mapping so the
same job artifact can target any of them.

| Tier | When to use | Local analog | Prod config |
| --- | --- | --- | --- |
| **All-purpose cluster** | Interactive dev, notebooks, ad-hoc | Local Spark session (`get_spark`) | `compute/all_purpose.json` |
| **Jobs cluster** | Scheduled batch, ephemeral | Docker `app` service | `compute/jobs_cluster.json` |
| **SQL warehouse** | Serverless BI queries, DBSQL, Genie | DuckDB (`serving/duckdb_dbsql.py`) | `compute/sql_warehouse.json` |
| **Serverless** | Pay-per-query, sub-second start | Not modeled locally | `compute/serverless.json` |

## Cost model

* **All-purpose**: expensive per DBU; 20–30 min idle timeout typical.
* **Jobs**: 50–70% cheaper per DBU; created + torn down per run.
* **SQL warehouse (classic)**: separate cluster type optimized for Photon SQL.
* **SQL warehouse (serverless)**: no cluster to manage; sub-10-second start;
  premium DBU rate but often net cheaper for spiky BI.
* **Serverless jobs**: same shape as serverless SQL but for Python/Scala jobs.

## Choosing

* Streamlit app → jobs cluster or serverless.
* Genie NL-to-SQL → serverless SQL warehouse.
* ML training → jobs cluster with GPU pool.
* Ad-hoc notebook → all-purpose.
