# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — MLflow train + register

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from ml.train_model import train
from ml.register_model import register

train()
register()
