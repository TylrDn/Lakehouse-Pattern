"""Explicit schemas for every layer.

Schema-on-read at Bronze + schema enforcement at Silver is the whole point of
Delta over vanilla Parquet-on-a-lake. Declaring schemas here keeps them
version-controlled and easy to diff in code review.
"""

from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Bronze: raw transactions as they arrive. We keep everything as strings where
# feasible so bad inputs are captured for later inspection instead of being
# rejected on read (a common lakehouse anti-pattern is being too strict at
# Bronze — you lose your "raw" copy).
# ---------------------------------------------------------------------------
BRONZE_TRANSACTIONS_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("quantity", StringType(), nullable=True),
        StructField("unit_price", StringType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
        StructField("event_ts", StringType(), nullable=True),
        StructField("country", StringType(), nullable=True),
    ]
)

# ---------------------------------------------------------------------------
# Silver: cleaned + typed. Column names are canonical, timestamps are
# timezone-normalized, and numeric columns are proper numeric types.
# ---------------------------------------------------------------------------
SILVER_TRANSACTIONS_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("product_id", StringType(), nullable=False),
        StructField("quantity", IntegerType(), nullable=False),
        StructField("unit_price", DoubleType(), nullable=False),
        StructField("currency", StringType(), nullable=False),
        StructField("event_ts", TimestampType(), nullable=False),
        StructField("country", StringType(), nullable=False),
        StructField("revenue", DoubleType(), nullable=False),
        StructField("ingest_ts", TimestampType(), nullable=False),
    ]
)

# Gold schemas are implicitly defined by the aggregation SQL. Keeping them
# implicit is intentional: business marts should evolve with product needs,
# and Delta will enforce them once the first write lands.
