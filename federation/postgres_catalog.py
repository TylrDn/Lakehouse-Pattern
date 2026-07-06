"""JDBC foreign catalog — Lakehouse Federation analog.

Register a Postgres schema as a first-class Spark catalog so users can
write cross-catalog queries like:

    SELECT g.event_date, g.gross_revenue, o.open_orders
    FROM   gold_daily_revenue g
    JOIN   postgres_ops.orders_summary o USING (event_date)

Pushdown: aggregations and filters are pushed to Postgres by Spark's JDBC
V2 catalog. Verify with ``EXPLAIN FORMATTED``.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from governance.local_uc.secrets import get as get_secret
from lakehouse.env import get_logger

_log = get_logger("federation.postgres")


def register_catalog(spark: SparkSession, catalog_name: str = "postgres_ops") -> None:
    """Register a Postgres foreign catalog via Spark's JDBC V2 catalog plugin."""
    url = get_secret("federation", "postgres_url")
    user = get_secret("federation", "postgres_user")
    password = get_secret("federation", "postgres_password")

    conf = spark.conf.set  # sugar
    conf(f"spark.sql.catalog.{catalog_name}", "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog")
    conf(f"spark.sql.catalog.{catalog_name}.url", url)
    conf(f"spark.sql.catalog.{catalog_name}.user", user)
    conf(f"spark.sql.catalog.{catalog_name}.password", password)
    conf(f"spark.sql.catalog.{catalog_name}.driver", "org.postgresql.Driver")
    _log.info("registered foreign catalog %s -> %s", catalog_name, url)
