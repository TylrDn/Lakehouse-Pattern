"""Raw CSV -> Bronze Delta table (batch ingest).

Bronze design principles used here
----------------------------------
* **Schema-on-read**: we assert the raw column layout, but keep the columns as
  strings so bad values (malformed timestamps, negatives, etc.) survive the
  ingest and can be inspected later. Bronze is our append-only source of
  truth; validation happens at Silver.
* **Append-only, immutable**: we never UPDATE or DELETE Bronze rows. If
  reprocessing is needed we re-ingest new files; Delta time travel and
  ``VACUUM`` give us a reliable retention policy.
* **Ingest metadata**: we tag every row with a source filename and an
  ``ingest_ts`` so lineage and reprocessing are trivial.
* **ACID guarantee**: writing via Delta means partially-completed jobs never
  leave half-written files visible to readers, unlike plain Parquet writes.

Databricks-native equivalent: use **Auto Loader** (``cloudFiles``) for
incremental file discovery + schema inference; on Databricks this file would
be almost identical, just replacing ``spark.read.csv`` with
``spark.readStream.format("cloudFiles")`` (see ``streaming_ingest.py``).
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit

from lakehouse import paths
from lakehouse.schemas import BRONZE_TRANSACTIONS_SCHEMA
from lakehouse.spark import get_spark


def read_raw_csv(spark: SparkSession, src: Path) -> DataFrame:
    """Read the raw CSV with an explicit schema.

    We explicitly do NOT enable ``inferSchema``: on a real ingest that would
    require a scan of every file just to guess types, and inference guesses
    are the #1 cause of "the schema changed overnight" incidents.

    If ``src`` is a directory we also apply ``pathGlobFilter=*.csv`` so
    sibling docs (README, loaders) are not picked up as data.
    """
    reader = spark.read.option("header", "true").schema(BRONZE_TRANSACTIONS_SCHEMA)
    if src.is_dir():
        reader = reader.option("pathGlobFilter", "*.csv")
    return reader.csv(str(src))


def write_bronze(df: DataFrame, target: Path, source_hint: str) -> None:
    """Write to Bronze as an append-only Delta table with lineage columns."""
    enriched = (
        df.withColumn("_source_file", input_file_name())
        .withColumn("_source_hint", lit(source_hint))
        .withColumn("_ingest_ts", current_timestamp())
    )

    # ``mergeSchema`` handles the case where an upstream feed adds a column
    # between runs — Delta will evolve the table's schema atomically and
    # readers keep working. This is a headline Delta feature; without it we
    # would need a manual ALTER TABLE.
    (
        enriched.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(str(target))
    )


def run(source: Path | None = None) -> None:
    """Ingest ``source`` (or the default sample file) into Bronze."""
    paths.ensure_dirs()
    spark = get_spark("bronze-batch-ingest")
    src = source or (paths.RAW_DIR / "transactions.csv")
    if not src.exists():
        raise FileNotFoundError(
            f"Raw file not found at {src}. Run `make data` first to generate it."
        )

    df = read_raw_csv(spark, src)
    write_bronze(df, paths.BRONZE_TRANSACTIONS, source_hint=src.name)

    n = spark.read.format("delta").load(str(paths.BRONZE_TRANSACTIONS)).count()
    print(f"Bronze append complete. Table now has {n} rows at {paths.BRONZE_TRANSACTIONS}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source",
        type=Path,
        default=None,
        help="Optional path to a raw CSV. Defaults to data/sample_raw/transactions.csv",
    )
    args = parser.parse_args()
    run(args.source)
