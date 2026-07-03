# Benchmarks

**These are reproducibility receipts, not comparative claims.**

We do **not** benchmark this repo against Databricks, other lakehouse
implementations, or any specific runtime. The numbers here describe how
long the pipeline takes on a specific machine at a specific commit — so
that anyone can re-run the same script and either match those numbers
(same hardware) or explain the delta (different hardware, different
scale, different Spark configuration).

## Running

From the repo root:

```bash
# ETL only (Bronze → Silver → Gold + row counts)
python benchmarks/run_benchmarks.py

# With MLflow training + registry
python benchmarks/run_benchmarks.py --with-ml
```

The script:

1. Creates a scratch `LAKEHOUSE_ROOT` under `/tmp/` so it does not
   pollute your existing `data/` directory.
2. Times each stage (`sample-data`, `bronze`, `silver`, `gold`, and
   optionally `ml-train` + `ml-register`).
3. Records row counts for each layered table via Delta reads.
4. Writes `benchmarks/latest.md` (human-readable) and
   `benchmarks/latest.json` (machine-readable). Both files include the
   commit SHA, platform string, Python version, and CPU count so future
   runs can be compared.
5. Cleans up the scratch directory.

## What we deliberately do not measure

- **Latency.** This is a batch pipeline demo. Streaming latency should
  be measured on realistic hardware with a real message broker, not on
  a laptop reading from local disk.
- **Cost per query.** Serverless SQL pricing is workload- and
  cloud-specific; the README's cost narrative is qualitative.
- **Comparison against a warehouse.** We do not run a matched workload
  against Snowflake / BigQuery / Redshift. The architecture story
  covers the tradeoffs; the numbers you'd get there depend entirely on
  cluster size and pricing tier at run time.

## Interpreting the numbers

If your run is:

- **~2× slower than the reference:** likely fewer CPU cores or an older
  JVM. `make preflight` confirms Java 17+; `nproc` shows core count.
- **Wildly slower (10×+):** first-time delta-spark JAR download, cold
  page cache, or the Ivy cache is behind a proxy. Re-run once warm.
- **Faster:** great — no action needed. Consider opening a PR to
  update `latest.md` if the machine profile is meaningfully different
  and you want to add it as a reference.
