# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup
# MAGIC
# MAGIC Widgets, imports, and shared configuration.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "UC catalog")
dbutils.widgets.text("bronze_schema", "lakehouse_pattern_bronze", "Bronze schema")
dbutils.widgets.text("silver_schema", "lakehouse_pattern_silver", "Silver schema")
dbutils.widgets.text("gold_schema", "lakehouse_pattern_gold", "Gold schema")

CATALOG = dbutils.widgets.get("catalog")
BRONZE = f"{CATALOG}.{dbutils.widgets.get('bronze_schema')}"
SILVER = f"{CATALOG}.{dbutils.widgets.get('silver_schema')}"
GOLD = f"{CATALOG}.{dbutils.widgets.get('gold_schema')}"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS ${catalog};
# MAGIC CREATE SCHEMA  IF NOT EXISTS ${catalog}.${bronze_schema};
# MAGIC CREATE SCHEMA  IF NOT EXISTS ${catalog}.${silver_schema};
# MAGIC CREATE SCHEMA  IF NOT EXISTS ${catalog}.${gold_schema};

# COMMAND ----------

# MAGIC %run ./_helpers
