"""Genie NL-to-SQL — Streamlit app that queries Gold in plain English.

Databricks AI/BI Genie is a chat-style interface that translates natural
language into SQL against a governed workspace. This module is the OSS
analog: a Streamlit UI + a two-stage LLM prompt that (1) picks a schema-
constrained query plan and (2) executes it against Gold via Spark SQL.

Safety rails
------------
* The LLM is only ever shown the schema of Gold, never row samples (avoids
  data-in-prompt exfiltration).
* The generated SQL is parsed by ``sqlglot`` and any statement other than
  ``SELECT`` is rejected.
* The result set is capped at 10k rows to prevent runaway costs.

Run
---
::

    streamlit run serving/genie/app.py
"""

from __future__ import annotations

from pathlib import Path

import streamlit as st

from lakehouse import paths


_GOLD_SCHEMA = """
Table: gold_daily_revenue (event_date DATE, country STRING, gross_revenue DOUBLE, order_count BIGINT, unique_customers BIGINT)
Table: gold_customer_ltv (customer_id STRING, lifetime_revenue DOUBLE, orders BIGINT, first_seen TIMESTAMP, last_seen TIMESTAMP)
""".strip()


def _guard_sql(sql: str) -> str:
    """Reject anything that isn't a single SELECT."""
    import sqlglot

    parsed = sqlglot.parse(sql, read="spark")
    if len(parsed) != 1 or not parsed[0] or parsed[0].key != "select":
        raise ValueError(f"only single SELECT statements are allowed; got: {sql[:80]}")
    return sql


def _nl_to_sql(question: str) -> str:
    from ml.ai_functions.udfs import _chat

    prompt = (
        "You translate natural-language business questions into Spark SQL.\n"
        "The available schema is:\n"
        f"{_GOLD_SCHEMA}\n\n"
        "Rules: single SELECT, no DDL/DML, LIMIT 10000 unless the question already caps rows.\n"
        f"Question: {question}\n\nReturn only the SQL."
    )
    return _chat("gpt-4o-mini", prompt).strip().strip("`").removeprefix("sql").strip()


def _run(sql: str):
    from lakehouse.spark import get_spark

    spark = get_spark("genie")
    spark.sql(
        f"CREATE OR REPLACE TEMP VIEW gold_daily_revenue AS "
        f"SELECT * FROM delta.`{paths.GOLD_DAILY_REVENUE}`"
    )
    spark.sql(
        f"CREATE OR REPLACE TEMP VIEW gold_customer_ltv AS "
        f"SELECT * FROM delta.`{paths.GOLD_CUSTOMER_LTV}`"
    )
    return spark.sql(sql).limit(10_000).toPandas()


def main() -> None:
    st.title("Genie — ask Gold in English")
    st.caption("Databricks AI/BI Genie analog. Read-only against Gold marts.")
    q = st.text_input("Question", "What are the top 5 countries by revenue?")
    if not q:
        return
    try:
        sql = _guard_sql(_nl_to_sql(q))
    except Exception as exc:  # noqa: BLE001
        st.error(f"SQL generation blocked: {exc}")
        return
    st.code(sql, language="sql")
    try:
        df = _run(sql)
    except Exception as exc:  # noqa: BLE001
        st.error(f"Query failed: {exc}")
        return
    st.dataframe(df)


if __name__ == "__main__":
    main()
