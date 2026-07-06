"""Dagster asset graph for the lakehouse.

Why Dagster over the hand-rolled runner in ``orchestration/workflow.py``?

* **Schedules + sensors** — Dagster's ``ScheduleDefinition`` and
  ``SensorDefinition`` cover cron + event triggers; Databricks Workflows
  gives you the same via ``schedule`` and ``file_arrival`` triggers.
* **Parameters** — asset configs flow between assets like ``depends_on``
  parameters flow between Databricks tasks.
* **Retries + backoff** — ``RetryPolicy`` on each op mirrors Workflows'
  ``max_retries`` + ``min_retry_interval_millis``.
* **Repair runs** — Dagster's *reexecution* covers Workflows' *repair run*
  (rerun only the failed downstream slice).
* **Alerting** — the ``run_failure_sensor`` hooks Slack/PagerDuty webhooks
  the same way Workflows' ``webhook_notifications`` does.
* **Continuous mode** — ``AutoMaterializePolicy.eager()`` on the streaming
  asset gives the same effect as a Workflows "continuous" job.

Run
---
::

    pip install "dagster[webserver]" dagster-shell
    dagster dev -f orchestration/dagster_project/definitions.py

Then open http://localhost:3000, click **Materialize all**.
"""

from __future__ import annotations

from dagster import (
    AssetIn,
    AutoMaterializePolicy,
    Definitions,
    RetryPolicy,
    ScheduleDefinition,
    asset,
    define_asset_job,
    run_failure_sensor,
    RunFailureSensorContext,
)


_RETRY = RetryPolicy(max_retries=2, delay=2.0, backoff=None)


@asset(retry_policy=_RETRY, group_name="bronze")
def raw_sample() -> None:
    from data.sample_raw.download import main as dl

    dl()


@asset(retry_policy=_RETRY, group_name="bronze", ins={"raw_sample": AssetIn()})
def bronze_transactions(raw_sample) -> None:
    from ingestion.batch_ingest import run

    run()


@asset(
    retry_policy=_RETRY,
    group_name="silver",
    ins={"bronze_transactions": AssetIn()},
)
def silver_transactions(bronze_transactions) -> None:
    from transform.silver_clean import run

    run()


@asset(
    retry_policy=_RETRY,
    group_name="silver",
    ins={"silver_transactions": AssetIn()},
)
def customer_dim(silver_transactions) -> None:
    from transform.scd2.customer_dim import run

    run()


@asset(
    retry_policy=_RETRY,
    group_name="gold",
    ins={"silver_transactions": AssetIn()},
)
def gold_daily_revenue(silver_transactions) -> None:
    from transform.gold_aggregate import run

    run()


@asset(
    retry_policy=_RETRY,
    group_name="ml",
    ins={"gold_daily_revenue": AssetIn()},
    auto_materialize_policy=AutoMaterializePolicy.eager(),
)
def registered_model(gold_daily_revenue) -> None:
    from ml.train_model import train
    from ml.register_model import register

    train()
    register()


daily_job = define_asset_job(name="daily_lakehouse_refresh")

daily_schedule = ScheduleDefinition(
    name="daily_2am_utc",
    cron_schedule="0 2 * * *",
    job=daily_job,
)


@run_failure_sensor
def notify_on_failure(context: RunFailureSensorContext) -> None:
    """Post to Slack on any run failure — Workflows' webhook_notifications analog."""
    from governance.local_uc.secrets import get

    try:
        webhook = get("orchestration", "slack_webhook")
    except KeyError:
        return
    import urllib.request

    payload = f'{{"text": "Lakehouse run FAILED: {context.failure_event.message}"}}'
    urllib.request.urlopen(  # noqa: S310
        urllib.request.Request(
            webhook, data=payload.encode(), headers={"Content-Type": "application/json"}
        )
    )


defs = Definitions(
    assets=[
        raw_sample,
        bronze_transactions,
        silver_transactions,
        customer_dim,
        gold_daily_revenue,
        registered_model,
    ],
    schedules=[daily_schedule],
    sensors=[notify_on_failure],
    jobs=[daily_job],
)
