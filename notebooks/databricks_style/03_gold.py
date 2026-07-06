# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold marts

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from transform.gold_aggregate import run

run()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT country, ROUND(SUM(gross_revenue), 2) AS revenue
# MAGIC FROM   ${catalog}.${gold_schema}.daily_revenue
# MAGIC GROUP  BY country
# MAGIC ORDER  BY revenue DESC
# MAGIC LIMIT  10;
