# Architecture

## Data flow

```
                  ┌───────────────────────┐
raw CSV/JSON ──▶  │  Bronze (Delta)       │  append-only, schema-on-read
                  │  data/bronze/…        │  lineage cols: _source_file, _ingest_ts
                  └──────────┬────────────┘
                             ▼                  MERGE upsert on transaction_id
                  ┌───────────────────────┐    partition by event_date
                  │  Silver (Delta)       │    Z-ORDER by customer_id
                  │  data/silver/…        │    quality-gated;
                  └──────────┬────────────┘    rejects → transactions_rejects
                             ▼
              ┌──────────────┴──────────────┐
              ▼                             ▼
┌─────────────────────┐         ┌─────────────────────┐
│ gold_daily_revenue  │         │ gold_customer_ltv   │
└──────────┬──────────┘         └──────────┬──────────┘
           ▼                               ▼
     Streamlit App                   MLflow training +
     (BI / Databricks Apps)          RAG grounding
```

## Layer contracts

### Bronze — `data/bronze/transactions`
- **Format**: Delta, append-only.
- **Schema**: `lakehouse.schemas.BRONZE_TRANSACTIONS_SCHEMA` (all string
  except lineage cols). This is intentional: raw data may violate the "real"
  types, and we want to capture that in Bronze rather than reject it.
- **Partitioning**: none (small volume; partitioning at Bronze prematurely is
  a common anti-pattern that creates the small-files problem).
- **Retention**: driven by cost/compliance; VACUUM policy set at deploy time.

### Silver — `data/silver/transactions`
- **Format**: Delta.
- **Schema**: `lakehouse.schemas.SILVER_TRANSACTIONS_SCHEMA` (typed, non-null
  where the contract allows).
- **Partitioning**: `event_date` (low cardinality, always in filters).
- **Clustering**: `ZORDER BY customer_id` (high cardinality, frequently
  filtered by point + range queries).
- **Idempotency**: MERGE upsert on `transaction_id`.
- **Rejects**: bad rows land in `data/silver/transactions_rejects` with a
  `_rejected_at` timestamp — never silently dropped.

### Gold — `data/gold/daily_revenue`, `data/gold/customer_ltv`
- **Format**: Delta.
- **Refresh**: full overwrite each run (deterministic function of Silver).
  Would swap to MERGE if Silver retention grew large enough that a full scan
  became expensive.
- **Consumers**: Streamlit app (`serving/app.py`), MLflow training
  (`ml/train_model.py`), RAG grounding (`ml/rag_demo/rag_pipeline.py`).

## Failure and retry

- Every job is idempotent and can be re-run without duplicating data
  (append + MERGE + overwrite semantics chosen accordingly).
- `orchestration/workflow.py` retries each task twice with exponential
  backoff. On Databricks this is replaced by the platform-native retry policy
  in the Job definition.
- Delta transactions are atomic — a job that fails mid-write leaves the table
  in its previous consistent state. There is no "corrupted table" recovery
  procedure to worry about.

## Mapping to a Databricks production deployment

| Local artifact | Databricks equivalent |
| --- | --- |
| `data/bronze/…` Delta paths | Managed tables in `main.lakehouse_pattern_bronze` (Unity Catalog) |
| `lakehouse/spark.py` | The workspace runtime (nothing to configure) |
| `ingestion/streaming_ingest.py` | Auto Loader (`cloudFiles`) task in a Job |
| `pipelines/declarative_pipeline.py` | A Delta Live Tables pipeline |
| `governance/unity_catalog_setup.sql` | Applied as-is via SQL warehouse |
| `orchestration/workflow.py` | Databricks Workflows job with one task per step |
| `ml/train_model.py` + `register_model.py` | Same code; tracking URI provided by the workspace |
| `ml/rag_demo/` | Vector Search + Foundation Model APIs + Model Serving |
| `serving/app.py` | Deployed as a Databricks App |

The Python is unchanged. Only the storage URIs, tracking URIs, and
orchestration wrapper differ.

## Cost model (why this shape is cheap)

- **Decoupled storage/compute**: Bronze/Silver/Gold sit on object storage;
  compute (cluster / SQL warehouse) is spun up per job and paid per second.
  Compare to a warehouse where storage and compute are coupled and paid 24/7.
- **Serverless SQL** for the Streamlit-analog reads only Gold — a tiny slice
  of total data. Query cost is a rounding error vs. training or ETL.
- **OPTIMIZE + Z-ORDER** on Silver cuts scan cost for BI + ML feature
  lookups; the tradeoff is the OPTIMIZE run itself, which we schedule daily
  during off-peak.
- **VACUUM** trims old files. Retention window balances cost vs. the
  time-travel window analysts need.
