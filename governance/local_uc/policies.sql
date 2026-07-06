-- Secure-view analog of Unity Catalog row filters + column masks.
--
-- On Databricks we'd use ALTER TABLE ... SET ROW FILTER / SET MASK. In OSS
-- Spark SQL we build the same behavior with views on top of Delta paths.
--
-- Consumers (Streamlit, RAG, ML) query the *views*, never the base Delta
-- tables — the OPA / authz layer enforces that discipline.
--
-- These statements are idempotent; safe to re-run every time the pipeline
-- refreshes the base tables.

CREATE OR REPLACE TEMP VIEW silver_transactions_masked AS
SELECT
    transaction_id,
    -- Column mask: hash customer_id for anyone not on the ml-engineers list.
    -- Session role is set by governance.local_uc.spark_hooks.set_role().
    CASE
        WHEN current_user_group() = 'ml-engineers' THEN customer_id
        ELSE sha2(customer_id, 256)
    END AS customer_id,
    product_id,
    quantity,
    unit_price,
    currency,
    event_ts,
    country,
    revenue,
    event_date,
    ingest_ts
FROM delta.`${LAKEHOUSE_ROOT}/silver/transactions`;

CREATE OR REPLACE TEMP VIEW gold_daily_revenue_scoped AS
SELECT g.*
FROM delta.`${LAKEHOUSE_ROOT}/gold/daily_revenue` g
WHERE
    -- Row filter: analysts only see their own country scope.
    current_user_group() = 'admins'
    OR g.country IN (
        SELECT country
        FROM json.`${LAKEHOUSE_ROOT}/../governance/local_uc/analyst_scope.json`
        WHERE analyst = current_user()
    );
