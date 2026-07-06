"""Airflow DAG — an alternative OSS analog to Databricks Workflows.

Ship both Airflow and Dagster to demonstrate the mapping: Airflow is closer
to the *task-based* Workflows model, Dagster is closer to the *asset-based*
Delta Live Tables + Workflows combined model.

Deploy
------
Drop this file in ``$AIRFLOW_HOME/dags/`` (or the Astronomer/Composer
equivalent). It uses only PythonOperator so no extra providers are needed.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def _download() -> None:
    from data.sample_raw.download import main as dl

    dl()


def _bronze() -> None:
    from ingestion.batch_ingest import run

    run()


def _silver() -> None:
    from transform.silver_clean import run

    run()


def _gold() -> None:
    from transform.gold_aggregate import run

    run()


def _ml_train() -> None:
    from ml.train_model import train

    train()


def _ml_register() -> None:
    from ml.register_model import register

    register()


default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "email_on_failure": True,
    "email": ["data-platform@example.com"],
}


with DAG(
    dag_id="lakehouse_pattern_daily",
    description="Bronze -> Silver -> Gold -> MLflow",
    schedule="0 2 * * *",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["lakehouse", "delta", "medallion"],
) as dag:
    download = PythonOperator(task_id="download", python_callable=_download)
    bronze = PythonOperator(task_id="bronze", python_callable=_bronze)
    silver = PythonOperator(task_id="silver", python_callable=_silver)
    gold = PythonOperator(task_id="gold", python_callable=_gold)
    ml_train = PythonOperator(task_id="ml_train", python_callable=_ml_train)
    ml_register = PythonOperator(task_id="ml_register", python_callable=_ml_register)

    download >> bronze >> silver >> gold >> ml_train >> ml_register
