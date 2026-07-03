-- Unity Catalog setup for the Lakehouse-Pattern demo.
--
-- REQUIREMENT: This file targets a workspace with Unity Catalog enabled
-- (Premium / Enterprise Databricks — NOT Community Edition). Run it from a
-- SQL warehouse or a notebook attached to a UC-enabled cluster.
--
-- Local equivalent: We do not have UC in OSS. The closest OSS approximation
-- is Delta table paths + Hive Metastore (also OSS). Everywhere this file
-- writes ``main.lakehouse_pattern.<table>``, the local pipeline instead uses
-- ``delta.`data/{layer}/<table>``` — see ``lakehouse/paths.py``.
--
-- ------------------------------------------------------------------
-- 1. Catalog + schemas (bronze / silver / gold) — separation of trust
-- ------------------------------------------------------------------
CREATE CATALOG IF NOT EXISTS main
  MANAGED LOCATION 's3://<your-bucket>/uc-managed/main';

USE CATALOG main;

CREATE SCHEMA IF NOT EXISTS lakehouse_pattern_bronze
  COMMENT 'Raw immutable landing zone. Append-only.';
CREATE SCHEMA IF NOT EXISTS lakehouse_pattern_silver
  COMMENT 'Cleansed, deduped, quality-gated data.';
CREATE SCHEMA IF NOT EXISTS lakehouse_pattern_gold
  COMMENT 'Business marts consumed by BI / ML / apps.';

-- ------------------------------------------------------------------
-- 2. Table registrations (managed tables — UC owns the storage lifecycle)
-- ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS main.lakehouse_pattern_bronze.transactions (
    transaction_id STRING,
    customer_id    STRING,
    product_id     STRING,
    quantity       STRING,
    unit_price     STRING,
    currency       STRING,
    event_ts       STRING,
    country        STRING,
    _source_file   STRING,
    _ingest_ts     TIMESTAMP
) USING DELTA
COMMENT 'Raw retail transactions, append-only.'
TBLPROPERTIES ('delta.appendOnly' = 'true');

CREATE TABLE IF NOT EXISTS main.lakehouse_pattern_silver.transactions (
    transaction_id STRING NOT NULL,
    customer_id    STRING NOT NULL,
    product_id     STRING NOT NULL,
    quantity       INT    NOT NULL,
    unit_price     DOUBLE NOT NULL,
    currency       STRING NOT NULL,
    event_ts       TIMESTAMP NOT NULL,
    country        STRING NOT NULL,
    revenue        DOUBLE NOT NULL,
    event_date     DATE   NOT NULL,
    ingest_ts      TIMESTAMP NOT NULL
) USING DELTA
PARTITIONED BY (event_date)
COMMENT 'Cleaned, deduped transactions with quality gates applied.'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);

-- ------------------------------------------------------------------
-- 3. Tags for governance, lineage filtering, cost attribution
-- ------------------------------------------------------------------
ALTER SCHEMA main.lakehouse_pattern_bronze SET TAGS ('layer' = 'bronze', 'owner' = 'data-platform');
ALTER SCHEMA main.lakehouse_pattern_silver SET TAGS ('layer' = 'silver', 'owner' = 'data-platform');
ALTER SCHEMA main.lakehouse_pattern_gold   SET TAGS ('layer' = 'gold',   'owner' = 'analytics');

ALTER TABLE main.lakehouse_pattern_silver.transactions
  SET TAGS ('pii' = 'true', 'domain' = 'retail');

-- Column-level tag (used by Lineage + AI-generated privacy policies).
ALTER TABLE main.lakehouse_pattern_silver.transactions
  ALTER COLUMN customer_id SET TAGS ('pii' = 'true', 'classification' = 'restricted');

-- ------------------------------------------------------------------
-- 4. Grants — least-privilege by role
-- ------------------------------------------------------------------
-- Data engineers: own bronze + silver.
GRANT USAGE ON CATALOG main TO `data-engineers`;
GRANT ALL PRIVILEGES ON SCHEMA main.lakehouse_pattern_bronze TO `data-engineers`;
GRANT ALL PRIVILEGES ON SCHEMA main.lakehouse_pattern_silver TO `data-engineers`;

-- Analysts: read-only on gold; NO access to bronze/silver (PII).
GRANT USAGE   ON CATALOG main TO `analysts`;
GRANT USAGE   ON SCHEMA  main.lakehouse_pattern_gold TO `analysts`;
GRANT SELECT  ON SCHEMA  main.lakehouse_pattern_gold TO `analysts`;

-- ML engineers: read silver (features) + full gold (labels).
GRANT USAGE   ON SCHEMA main.lakehouse_pattern_silver TO `ml-engineers`;
GRANT SELECT  ON SCHEMA main.lakehouse_pattern_silver TO `ml-engineers`;
GRANT USAGE   ON SCHEMA main.lakehouse_pattern_gold   TO `ml-engineers`;
GRANT SELECT  ON SCHEMA main.lakehouse_pattern_gold   TO `ml-engineers`;

-- ------------------------------------------------------------------
-- 5. Row-level security + column masking (UC feature preview)
-- ------------------------------------------------------------------
-- Row filter: analysts can only see rows from countries they own.
CREATE OR REPLACE FUNCTION main.lakehouse_pattern_gold.country_filter(country STRING)
RETURN IS_ACCOUNT_GROUP_MEMBER('admins')
    OR country IN (
        SELECT country FROM main.lakehouse_pattern_gold.analyst_country_scope
        WHERE analyst = CURRENT_USER()
    );

ALTER TABLE main.lakehouse_pattern_gold.daily_revenue
  SET ROW FILTER main.lakehouse_pattern_gold.country_filter ON (country);

-- Column mask: hash customer_id for anyone not on the ml-engineers list.
CREATE OR REPLACE FUNCTION main.lakehouse_pattern_silver.mask_customer_id(cid STRING)
RETURN CASE
  WHEN IS_ACCOUNT_GROUP_MEMBER('ml-engineers') THEN cid
  ELSE sha2(cid, 256)
END;

ALTER TABLE main.lakehouse_pattern_silver.transactions
  ALTER COLUMN customer_id SET MASK main.lakehouse_pattern_silver.mask_customer_id;

-- ------------------------------------------------------------------
-- 6. Lineage — automatic in UC
-- ------------------------------------------------------------------
-- Nothing to declare. Unity Catalog automatically captures lineage between
-- bronze/silver/gold tables based on the SQL / DataFrame operations that
-- read from and write to them. The lineage graph is queryable via
-- ``system.access.table_lineage`` and viewable in the Catalog Explorer.
