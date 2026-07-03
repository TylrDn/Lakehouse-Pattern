# Lakehouse-Pattern — developer entry points.
# Every target is idempotent and safe to re-run.

PYTHON ?= python
PIP    ?= pip

.PHONY: help setup preflight lint format test data bronze stream silver gold pipeline \
        declarative ml rag serve orchestrate dag clean ci docs docs-serve bench bench-ml

help:
	@echo "Targets:"
	@echo "  setup        Install Python dependencies"
	@echo "  preflight    Check Java 17+ / JAVA_HOME are available"
	@echo "  data         Generate the deterministic sample dataset"
	@echo "  bronze       Raw -> Bronze (batch)"
	@echo "  stream       Raw -> Bronze (Structured Streaming, availableNow)"
	@echo "  silver       Bronze -> Silver (MERGE, quality gates, OPTIMIZE)"
	@echo "  gold         Silver -> Gold marts (daily_revenue, customer_ltv)"
	@echo "  pipeline     End-to-end: data -> bronze -> silver -> gold"
	@echo "  declarative  Run the DLT-analog declarative pipeline"
	@echo "  ml           Train + register the MLflow model"
	@echo "  rag          Build the RAG vector index + demo query"
	@echo "  serve        Launch the Streamlit gold explorer"
	@echo "  orchestrate  Run the DAG orchestrator (all steps)"
	@echo "  dag          Run the DAG orchestrator, skipping ML tasks"
	@echo "  test         Run pytest suite"
	@echo "  lint         Run ruff"
	@echo "  ci           lint + test (what CI runs)"
	@echo "  docs         Build the MkDocs site into site/ (open site/index.html)"
	@echo "  docs-serve   Serve the docs at http://localhost:8000 with live reload"
	@echo "  bench        Reproducibility benchmark (ETL only)"
	@echo "  bench-ml     Reproducibility benchmark including MLflow training"
	@echo "  clean        Delete all regenerable lakehouse artifacts"

setup:
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

preflight:
	$(PYTHON) -m lakehouse.env

data:
	$(PYTHON) data/sample_raw/download.py

bronze: data
	$(PYTHON) -m ingestion.batch_ingest

stream: data
	$(PYTHON) -m ingestion.streaming_ingest

silver: bronze
	$(PYTHON) -m transform.silver_clean

gold: silver
	$(PYTHON) -m transform.gold_aggregate

pipeline:
	$(PYTHON) -m orchestration.workflow --skip-ml

declarative:
	$(PYTHON) -m pipelines.declarative_pipeline

ml: gold
	$(PYTHON) -m ml.train_model
	$(PYTHON) -m ml.register_model

rag:
	$(PYTHON) -m ml.rag_demo.rag_pipeline

serve:
	streamlit run serving/app.py

orchestrate:
	$(PYTHON) -m orchestration.workflow

dag:
	$(PYTHON) -m orchestration.workflow --skip-ml

test:
	pytest -q

lint:
	ruff check .

ci: lint test

format:
	ruff format .

docs:
	$(PYTHON) docs/prebuild.py
	mkdocs build

docs-serve:
	$(PYTHON) docs/prebuild.py
	mkdocs serve

bench:
	$(PYTHON) benchmarks/run_benchmarks.py

bench-ml:
	$(PYTHON) benchmarks/run_benchmarks.py --with-ml

clean:
	rm -rf data/bronze data/silver data/gold data/checkpoints data/downloads
	rm -rf mlruns mlartifacts spark-warehouse metastore_db derby.log
	rm -rf ml/rag_demo/chroma_store
	rm -rf site benchmarks/latest.md benchmarks/latest.json
