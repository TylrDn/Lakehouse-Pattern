# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Vector Search + RAG

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from ml.vector_search.index import build_index, add_embeddings_to_delta, search
from lakehouse.spark import get_spark

spark = get_spark("nb-rag")
add_embeddings_to_delta(spark)
build_index(spark)

# COMMAND ----------

for cid, score in search("top-spending customer in the UK", k=5):
    print(cid, round(score, 4))
