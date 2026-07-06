# Local Unity Catalog analog

Unity Catalog is closed-source and only runs on Databricks Premium/Enterprise.
This directory ships an OSS analog that models the same *concepts* — a
metastore, catalogs/schemas/tables, grants, tags, row-level security, column
masking, secrets, audit logs, and lineage — using open-source components:

| UC concept | OSS analog here |
| --- | --- |
| Metastore + catalogs/schemas/tables | **Apache Polaris** (REST-based Iceberg catalog) — see `polaris_bootstrap.sql` for the equivalent CREATE statements |
| Grants (`GRANT SELECT ON …`) | **Open Policy Agent (OPA)** policies in `../opa/policies.rego` + a Python authz shim in `authz.py` |
| Row-level security + column masking | **Secure views** in `policies.sql` — same behavior, different mechanism (views instead of RLS UDF + `SET MASK`) |
| Tags (`ALTER … SET TAGS`) | JSON side-cars in `tags.json` — Polaris supports tags natively in the newer REST API |
| Lineage (`system.access.table_lineage`) | **OpenLineage** events emitted to `../lineage/events.jsonl` |
| Secrets (`dbutils.secrets`) | `secrets.py` — reads from local file or `SOPS`-encrypted store; env-var passthrough for CI |
| SCIM identity federation | `scim_users.json` — mock identity provider file consumed by `authz.py` |
| Audit logs (`system.access.audit`) | Every mutating call in `authz.py` writes an event to `audit_log.jsonl` |
| System tables (`system.*`) | `system_tables.sql` — SQL views over `audit_log.jsonl`, `lineage/events.jsonl`, and Polaris metadata |

## Why the split from `../unity_catalog_setup.sql`

The parent SQL file remains the *paid-tier truth*: it is exactly the SQL you
would run on a Premium Databricks workspace. This directory is the local
mirror — same intent, executable on a laptop with `docker compose up`.

## Running locally

```bash
docker compose up polaris opa           # start metastore + policy engine
python -m governance.local_uc.bootstrap # create catalog/schemas/tables
python -m governance.local_uc.authz     # smoke test grants + masking
```

## What still requires paid Databricks

* Automatic column-level lineage capture across Photon SQL. We collect it
  manually via OpenLineage events; Photon does it transparently.
* SCIM push from Okta/Entra. We mock the identity file.
* Predictive optimization / auto-liquid-clustering. Doc-only.
