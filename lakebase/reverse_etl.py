"""Reverse-ETL: Delta ``gold.customer_ltv`` → Postgres ``customer_scores``.

Databricks Lakebase gives you bidirectional Delta ↔ Postgres sync for free.
On OSS we implement the same pattern with structured streaming +
``foreachBatch`` writing through JDBC.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame

from governance.local_uc.secrets import get as get_secret
from lakehouse import paths
from lakehouse.env import get_logger
from lakehouse.spark import get_spark

_log = get_logger("lakebase.reverse_etl")

_TABLE = "customer_scores"
_CHECKPOINT = paths.CHECKPOINT_DIR / "reverse_etl_customer_scores"


def _jdbc_props() -> dict[str, str]:
    return {
        "user": get_secret("lakebase", "pg_user"),
        "password": get_secret("lakebase", "pg_password"),
        "driver": "org.postgresql.Driver",
    }


def _write_batch(df: DataFrame, batch_id: int) -> None:
    url = get_secret("lakebase", "pg_url")
    # Postgres UPSERT via a staging table + INSERT ... ON CONFLICT.
    stage = f"{_TABLE}_stage_{batch_id}"
    (
        df.write.jdbc(
            url=url,
            table=stage,
            mode="overwrite",
            properties=_jdbc_props(),
        )
    )
    # Merge stage into target via SQL. We use pgjdbc to run a single MERGE
    # statement rather than sending it row-by-row.
    import psycopg2

    with psycopg2.connect(url.replace("jdbc:postgresql", "postgresql"), **_jdbc_props()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {_TABLE} (customer_id, lifetime_revenue, orders, last_seen)
                SELECT customer_id, lifetime_revenue, orders, last_seen FROM {stage}
                ON CONFLICT (customer_id) DO UPDATE
                SET lifetime_revenue = EXCLUDED.lifetime_revenue,
                    orders           = EXCLUDED.orders,
                    last_seen        = EXCLUDED.last_seen;
                DROP TABLE {stage};
                """
            )
    _log.info("reverse-ETL batch %d merged %d rows", batch_id, df.count())


def start() -> None:
    spark = get_spark("reverse-etl")
    stream = spark.readStream.format("delta").load(str(paths.GOLD_CUSTOMER_LTV))
    (
        stream.writeStream.foreachBatch(_write_batch)
        .option("checkpointLocation", str(_CHECKPOINT))
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    start()
