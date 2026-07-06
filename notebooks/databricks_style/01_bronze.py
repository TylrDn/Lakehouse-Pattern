# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze
# MAGIC Batch ingest of raw CSVs into an append-only Delta table.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from ingestion.batch_ingest import run

run()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS bronze_rows FROM ${catalog}.${bronze_schema}.transactions;
