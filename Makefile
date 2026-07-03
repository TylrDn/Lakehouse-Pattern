# Lakehouse-Pattern — developer entry points.
# Every target is idempotent and safe to re-run.

PYTHON ?= python
PIP    ?= pip
DATA_DIR ?= data

.PHONY: help setup lint test data bronze silver gold pipeline ml rag serve orchestrate clean ci

help:
	@echo "Targets:"
	@echo "  setup        Install Python dependencies"
	@echo "  data         Download the sample dataset into data/sample_raw/"
	@echo "  bronze       Run raw -> bronze batch ingest"
	@echo "  silver       Run bronze -> silver cleansing + MERGE"
	@echo "  gold        Run silver -> gold business aggregates"
	@echo "  pipeline     End-to-end: data -> bronze -> silver -> gold"
	@echo "  ml           Train + log MLflow experiment on gold"
	@echo "  rag          Build vector index and answer a demo question"
	@echo "  serve        Launch the Streamlit gold explorer"
	@echo "  orchestrate  Run the DAG orchestrator (local Workflows analog)"
	@echo "  test         Run pytest suite"
	@echo "  lint         Run ruff"
	@echo "  ci           lint + test (what CI runs)"
	@echo "  clean        Delete regenerable lakehouse artifacts"

setup:
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

data:
	$(PYTHON) data/sample_raw/download.py

bronze: data
	$(PYTHON) -m ingestion.batch_ingest

silver: bronze
	$(PYTHON) -m transform.silver_clean

gold: silver
	$(PYTHON) -m transform.gold_aggregate

pipeline:
	$(PYTHON) -m orchestration.workflow

ml: gold
	$(PYTHON) -m ml.train_model
	$(PYTHON) -m ml.register_model

rag:
	$(PYTHON) -m ml.rag_demo.rag_pipeline

serve:
	streamlit run serving/app.py

orchestrate:
	$(PYTHON) -m orchestration.workflow

test:
	pytest -q

lint:
	ruff check .

ci: lint test

clean:
	rm -rf data/bronze data/silver data/gold data/checkpoints data/downloads
	rm -rf mlruns mlartifacts spark-warehouse metastore_db derby.log
	rm -rf ml/rag_demo/chroma_store
