# Lakehouse-Pattern — developer entry points.
# Every target is idempotent and safe to re-run.

PYTHON ?= python
PIP    ?= pip

.PHONY: help setup preflight lint format test test-cov data bronze stream silver gold pipeline \
        declarative ml rag serve orchestrate dag clean ci docs docs-serve bench bench-ml \
        governance cdc scd2 modern-delta incremental dagster airflow features vec ai genie \
        share federate lakebase bundle uci stream-continuous

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
	@echo "  test-cov     Run pytest with coverage (term-missing + coverage.xml)"
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
	$(PYTHON) -m data.sample_raw.download

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

test-cov:
	pytest --cov=. --cov-report=term-missing --cov-report=xml

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
	rm -rf data/bronze data/silver data/gold data/checkpoints data/downloads data/features
	rm -rf mlruns mlartifacts spark-warehouse metastore_db derby.log
	rm -rf ml/rag_demo/chroma_store ml/vector_search/hnsw.bin ml/vector_search/id_map.pkl
	rm -rf governance/local_uc/audit_log.jsonl governance/lineage/events.jsonl
	rm -rf pipelines/incremental/expectations_metrics.jsonl
	rm -rf site benchmarks/latest.md benchmarks/latest.json

# ---------------------------------------------------------------------------
# Concept-coverage v2 targets (see docs/upgrade-plan/README.md).
# ---------------------------------------------------------------------------
governance:
	$(PYTHON) -m governance.local_uc.authz

cdc:
	$(PYTHON) -m transform.cdc.apply_changes

scd2:
	$(PYTHON) -m transform.scd2.customer_dim

modern-delta:
	$(PYTHON) -m transform.modern_delta

incremental:
	$(PYTHON) -m pipelines.incremental.pipeline --once

dagster:
	dagster dev -f orchestration/dagster_project/definitions.py

airflow:
	@echo 'Drop orchestration/airflow_dags/lakehouse_dag.py into $$AIRFLOW_HOME/dags/ then start Airflow.'

features:
	@echo 'Feature Store is a library; use ml.feature_store.store from a pipeline.'

vec:
	$(PYTHON) -m ml.vector_search.index build

vec-query:
	$(PYTHON) -m ml.vector_search.index query 'top-spending UK customers'

ai:
	@echo 'Register AI Functions in a Spark session: from ml.ai_functions.udfs import register_all; register_all(spark).'

genie:
	streamlit run serving/genie/app.py

share:
	@echo 'Delta Sharing server is Java-based. See sharing/README.md.'

federate:
	@echo 'See federation/README.md - requires a running Postgres.'

lakebase:
	$(PYTHON) -m lakebase.reverse_etl

bundle:
	@echo 'databricks bundle deploy -t prod (from infra/bundle/).'

uci:
	$(PYTHON) -m data.uci_online_retail.download

stream-continuous:
	$(PYTHON) -m ingestion.streaming.kafka_source --continuous
