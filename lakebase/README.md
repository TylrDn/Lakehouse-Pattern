# Lakebase — OLTP tier + reverse ETL

Databricks Lakebase is a managed Postgres branch of the lakehouse with
Delta ↔ Postgres sync in both directions. OLTP apps (checkout, user
profile, notifications) get sub-10ms reads against the same data the lake
governs.

This directory models both halves:

* **Delta → Postgres (reverse ETL)** — `reverse_etl.py` reads
  `gold.customer_ltv` and upserts into a Postgres `customer_scores` table
  via `foreachBatch` streaming. This is what feeds a marketing app.
* **Postgres → Delta (CDC)** — `pg_cdc.py` uses Debezium's JSON envelope
  (or a plain `xmin`-poll fallback) to bring Postgres row changes into a
  Bronze Delta table. This is what backs "operational analytics".

## Databricks-native mapping

| Databricks | This repo |
| --- | --- |
| Lakebase database (managed Postgres) | Postgres 16 in `docker-compose.override.yml` |
| Automatic bidirectional sync | `reverse_etl.py` + `pg_cdc.py` — same code, no magic |
| Serving-layer feature lookups | The reverse-ETL target IS the online store |
| Delta Lake Uniform (Iceberg-visible) | Left as roadmap; requires Delta 3.2 UniForm config |
