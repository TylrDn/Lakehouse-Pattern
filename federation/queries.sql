-- Cross-catalog JOIN: lake ↔ Postgres foreign catalog.
-- After `register_catalog(spark)` the ``postgres_ops`` namespace is usable
-- exactly like a native catalog. Spark's JDBC V2 catalog will push down
-- aggregations and filters to Postgres — check with EXPLAIN FORMATTED.

SELECT
    g.event_date,
    g.country,
    g.gross_revenue,
    o.open_orders
FROM   delta.`${LAKEHOUSE_ROOT}/gold/daily_revenue` g
JOIN   postgres_ops.public.orders_summary          o
    ON g.event_date = o.event_date
   AND g.country    = o.country
WHERE  g.event_date >= current_date() - INTERVAL 7 DAYS
ORDER  BY g.event_date DESC, g.gross_revenue DESC;
