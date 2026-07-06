# Databricks-style notebooks

The primary Databricks UX is a notebook with `%run`, `%sql`, and widget
parameters. This directory ships a notebook-native version of the pipeline
so a Databricks user can lift-and-shift.

Each file is a plain `.py` with Databricks notebook cell markers
(`# COMMAND ----------` and `# MAGIC %sql`), which both the Databricks
Notebook UI and `databricks-connect` render as cells.

| File | Contents |
| --- | --- |
| `00_setup.py` | widgets, `dbutils.widgets.get`, imports |
| `01_bronze.py` | batch ingest |
| `02_silver.py` | cleansing + quality gates |
| `03_gold.py` | marts |
| `04_ml.py` | MLflow train + register |
| `05_rag.py` | Vector Search + RAG |
| `sql/*.sql` | `%sql` cells for BI review |

## Convert to `.ipynb`

::

    jupytext --to ipynb notebooks/databricks_style/*.py
