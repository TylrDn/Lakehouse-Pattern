# Databricks notebook source
# MAGIC %md
# MAGIC Shared helpers loaded by other notebooks via `%run ./_helpers`.

# COMMAND ----------

def qtable(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"
