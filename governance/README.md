# Governance

`unity_catalog_setup.sql` is the Databricks-native governance layer for this
project. It requires a **Unity Catalog-enabled workspace** (Premium or above);
it will NOT run on Databricks Community Edition or in the local OSS demo.

## What it demonstrates

| Concept | Where |
| --- | --- |
| Catalog + schema hierarchy per medallion layer | Section 1 |
| Managed Delta tables with constraints | Section 2 |
| Tags on schemas, tables, and columns (for cost/PII/lineage) | Section 3 |
| Least-privilege GRANTs per persona | Section 4 |
| Row-level security + column masking | Section 5 |
| Automatic table + column lineage via UC | Section 6 |

## OSS equivalent

The runnable pipeline uses Delta paths on the local filesystem
(`data/bronze/`, `data/silver/`, `data/gold/`). We approximate governance with:

- File-system permissions (owner/group).
- The `transactions_rejects` Delta table (quarantine) as an audit trail.
- Explicit schemas in `lakehouse/schemas.py`.

To run the SQL against a real UC workspace: paste it into a Databricks SQL
editor or execute it via `databricks sql`.
