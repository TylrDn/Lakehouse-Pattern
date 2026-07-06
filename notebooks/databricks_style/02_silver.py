# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver
# MAGIC MERGE + expectations + liquid clustering + CDF enabled.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from transform.silver_clean import run as silver_run
from transform.modern_delta import run as modern_run

silver_run()
modern_run()

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL ${catalog}.${silver_schema}.transactions;
# MAGIC -- Verify: clusteringColumns, TBLPROPERTIES include enableDeletionVectors + enableChangeDataFeed.
