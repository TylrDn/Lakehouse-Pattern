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

import pandas as pd
import streamlit as st

from lakehouse import paths
from lakehouse.spark import get_spark


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

    col1, col2, col3 = st.columns(3)
    col1.metric("Total gross revenue", f"{daily['gross_revenue'].sum():,.2f}")
    col2.metric("Orders", f"{int(daily['order_count'].sum()):,}")
    col3.metric("Unique customers", f"{ltv.shape[0]:,}")

    st.subheader("Revenue over time")
    chart = (
        daily.assign(event_date=pd.to_datetime(daily["event_date"]))
        .pivot_table(
            index="event_date", columns="country", values="gross_revenue", aggfunc="sum"
        )
        .fillna(0)
    )
    st.line_chart(chart)

    st.subheader("Top 20 customers by lifetime revenue")
    st.dataframe(
        ltv.sort_values("lifetime_revenue", ascending=False)
        .head(20)
        .reset_index(drop=True),
        use_container_width=True,
    )


if __name__ == "__main__":
    main()
