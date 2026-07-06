# Lakehouse Federation — foreign catalog demo

Databricks Lakehouse Federation lets you `CREATE FOREIGN CATALOG` on top of
Postgres / MySQL / Snowflake and query them with Unity Catalog governance.

The OSS analog: **Spark's JDBC data source with foreign-catalog-style
registration**. This directory ships a Postgres foreign catalog that
exposes an operational transactions table alongside our lake catalog.

## Diagram

```
     analysts run SQL
           │
           ▼
   Spark SQL / Genie  ──► gold.daily_revenue           (Delta lake catalog)
           │
           └──►  postgres_ops.orders                    (JDBC foreign catalog)
```

## Files

* `postgres_catalog.py` — registers the JDBC source as a Spark catalog and
  proxies pushdown-eligible predicates.
* `docker-compose.override.yml` — spins up a Postgres with sample orders.
* `queries.sql` — a cross-catalog JOIN between lake and Postgres.
