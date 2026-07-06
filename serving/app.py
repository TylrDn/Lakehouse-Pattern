"""Streamlit gold-layer explorer — a Databricks Apps analog.

Databricks Apps is a hosted-app product on the platform; the closest OSS
equivalent is a Streamlit app that queries the same Gold tables. Same code,
same tables — you deploy the app.py to Databricks Apps unchanged (aside from
using the workspace SparkSession instead of building one).

Run locally with:

    make serve

The app renders three panels:
1. KPIs from ``gold_daily_revenue``
2. Time-series chart of gross revenue by country
3. Top-N customers from ``gold_customer_ltv``
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure the repo root is on sys.path so ``streamlit run serving/app.py``
# works regardless of the caller's current working directory.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402

from lakehouse import paths  # noqa: E402
from lakehouse.spark import get_spark  # noqa: E402
from serving.metrics import compute_kpis, revenue_by_country, top_customers  # noqa: E402


@st.cache_data(ttl=60)
def load_daily_revenue() -> pd.DataFrame:
    spark = get_spark("serving-app")
    return spark.read.format("delta").load(str(paths.GOLD_DAILY_REVENUE)).toPandas()


@st.cache_data(ttl=60)
def load_customer_ltv() -> pd.DataFrame:
    spark = get_spark("serving-app")
    return spark.read.format("delta").load(str(paths.GOLD_CUSTOMER_LTV)).toPandas()


def main() -> None:
    st.set_page_config(page_title="Lakehouse-Pattern — Gold Explorer", layout="wide")
    st.title("Lakehouse-Pattern — Gold Explorer")
    st.caption(
        "Reads directly from the Gold Delta tables materialized by the pipeline."
    )

    try:
        daily = load_daily_revenue()
        ltv = load_customer_ltv()
    except Exception as exc:
        st.error(
            "Failed to load Gold tables. Have you run the pipeline yet?\n\n"
            "Try: `make pipeline`\n\n"
            f"Error: {exc}"
        )
        return

    kpis = compute_kpis(daily, ltv)
    col1, col2, col3 = st.columns(3)
    col1.metric("Total gross revenue", f"{kpis['gross_revenue']:,.2f}")
    col2.metric("Orders", f"{kpis['orders']:,}")
    col3.metric("Unique customers", f"{kpis['unique_customers']:,}")

    st.subheader("Revenue over time")
    st.line_chart(revenue_by_country(daily))

    st.subheader("Top 20 customers by lifetime revenue")
    st.dataframe(top_customers(ltv, 20), use_container_width=True)


if __name__ == "__main__":
    main()
