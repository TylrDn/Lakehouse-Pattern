# Troubleshooting

Common issues and how to fix them, in order of how often they trip people up.

## Contents
- [Java / JAVA_HOME](#java--java_home)
- [`ModuleNotFoundError: pyspark`](#modulenotfounderror-pyspark)
- [Delta JAR download stalls on first Spark start](#delta-jar-download-stalls-on-first-spark-start)
- [`AnalysisException: Table or view not found`](#analysisexception-table-or-view-not-found)
- [Streaming ingest picks up junk files](#streaming-ingest-picks-up-junk-files)
- [MLflow says "no runs found"](#mlflow-says-no-runs-found)
- [Streamlit shows a red error box](#streamlit-shows-a-red-error-box)
- [Tests hang or PySpark can't start on macOS Sonoma](#tests-hang-or-pyspark-cant-start-on-macos-sonoma)
- [`vacuum failed: retentionDurationCheck` warning](#vacuum-failed-retentiondurationcheck-warning)
- [Reset everything and start over](#reset-everything-and-start-over)

---

## Java / JAVA_HOME

**Symptom.** Any Spark command fails with a long Py4J stacktrace, or with:

```
PrerequisiteError: Java 17+ is required for PySpark 3.5.
  Detected: Java 11
```

**Fix.**

```bash
# macOS
brew install openjdk@17
export JAVA_HOME=$(/usr/libexec/java_home -v17)

# Debian / Ubuntu
sudo apt install -y openjdk-17-jre
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Verify
make preflight
```

Put the `export JAVA_HOME=...` line in your shell rc file so it survives new
terminals.

---

## `ModuleNotFoundError: pyspark`

**Symptom.** Running `make bronze` (or `python -m ingestion.batch_ingest`)
raises `ModuleNotFoundError`.

**Fix.** Your virtualenv is not activated, or you forgot `make setup`:

```bash
python -m venv .venv && source .venv/bin/activate
make setup
```

If that still fails, confirm you're on Python 3.11 (recommended) or 3.12:

```bash
python --version
```

---

## Delta JAR download stalls on first Spark start

**Symptom.** The very first Spark call after install hangs at:

```
Ivy Default Cache set to: ...
```

**Cause.** Spark is downloading `io.delta:delta-spark_2.12:3.2.0` from Maven
Central. It's ~30 MB but on a slow network can look stuck.

**Fix.** Wait it out once. Subsequent runs read from the local Ivy cache
(`~/.ivy2/`) and are instant. If your CI is repeatedly slow, cache
`~/.ivy2` between runs.

---

## `AnalysisException: Table or view not found`

**Symptom.** Silver or Gold job errors with something like
`AnalysisException: Path does not exist: data/bronze/transactions`.

**Cause.** You skipped a stage. The pipeline is layered — every task assumes
its upstream produced a Delta table.

**Fix.** Rerun from the top or use the orchestrator:

```bash
make pipeline       # runs data -> bronze -> silver -> gold
```

---

## Streaming ingest picks up junk files

**Symptom.** Row counts in Bronze look wrong after `make stream`; some rows
show `_source_file` ending in `.md` or `.py`.

**Fix.** This is already handled — both `ingestion/batch_ingest.py` and
`ingestion/streaming_ingest.py` set `pathGlobFilter="*.csv"`. If you see
this on a customized fork, add that option or move non-CSV files out of
`data/sample_raw/`.

---

## MLflow says "no runs found"

**Symptom.** `python -m ml.register_model` raises:

```
RuntimeError: No MLflow runs found for experiment 'lakehouse-pattern-daily-revenue'.
```

**Cause.** You skipped `python -m ml.train_model`, or you're pointing at a
different tracking URI.

**Fix.**

```bash
# Default: mlruns at repo root
python -m ml.train_model
python -m ml.register_model

# Or explicitly:
export MLFLOW_TRACKING_URI="file://$PWD/mlruns"
```

---

## Streamlit shows a red error box

**Symptom.** The app renders but shows `Failed to load Gold tables.`

**Fix.** Run the pipeline first:

```bash
make pipeline
```

Then hit **Rerun** in the Streamlit UI. The app caches for 60 s so the
tables will show up on the next request.

---

## Tests hang or PySpark can't start on macOS Sonoma

**Symptom.** Tests hang at the first `SparkSession` build; you see nothing
in the console for minutes.

**Cause.** On macOS, PySpark occasionally hits a `fork` safety issue.

**Fix.** Set the workaround before running tests:

```bash
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
pytest -q
```

You can also add this to your `~/.zshrc` if you hit it repeatedly.

---

## `vacuum failed: retentionDurationCheck` warning

**Symptom.** `silver_clean.py` prints a warning about VACUUM retention.

**Cause.** Delta enforces a 168 hr (7 d) minimum retention by default to
protect concurrent readers. Our code respects that. If you want to VACUUM
more aggressively for a demo, temporarily disable the check:

```python
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.sql("VACUUM delta.`data/silver/transactions` RETAIN 0 HOURS")
```

Do NOT ship this to production — you'll strand any concurrent reader
holding a snapshot older than the new retention window.

---

## Reset everything and start over

```bash
make clean          # wipes bronze/silver/gold/checkpoints/mlruns/rag store
make preflight      # sanity-check Java
make pipeline       # rebuild ETL
make ml             # rebuild model
make rag            # rebuild RAG index
```
