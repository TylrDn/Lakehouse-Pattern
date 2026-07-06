-- Polaris (Apache) catalog bootstrap — the OSS metastore that most closely
-- mirrors Unity Catalog's three-level namespace (catalog.schema.table).
--
-- Polaris speaks the Iceberg REST protocol. For a Delta-only demo we still
-- register the tables here so principals, tags, and grants have a single
-- source of truth. In a mixed Iceberg+Delta shop Polaris governs both.

CREATE CATALOG IF NOT EXISTS main
  PROPERTIES (
    'storage.location' = 's3a://lakehouse-pattern/main/',
    'default-file-format' = 'DELTA'
  );

CREATE NAMESPACE IF NOT EXISTS main.bronze
  PROPERTIES ('layer' = 'bronze', 'owner' = 'data-platform');
CREATE NAMESPACE IF NOT EXISTS main.silver
  PROPERTIES ('layer' = 'silver', 'owner' = 'data-platform', 'pii' = 'true');
CREATE NAMESPACE IF NOT EXISTS main.gold
  PROPERTIES ('layer' = 'gold', 'owner' = 'analytics');

-- Table registrations are handled by the pipeline at write time — Polaris
-- exposes a REST endpoint that `pipelines.declarative_pipeline` and
-- `orchestration.workflow` post to after each successful write.
