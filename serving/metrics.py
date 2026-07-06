"""Pure aggregation helpers for the serving layer.

Kept free of Streamlit / Spark imports so the KPI math can be unit-tested in
the fast CI lane. ``serving/app.py`` renders the results of these functions.
"""

from __future__ import annotations

import pandas as pd


def compute_kpis(daily: pd.DataFrame, ltv: pd.DataFrame) -> dict[str, float]:
    """Headline KPIs shown at the top of the dashboard.

    - ``gross_revenue``: total revenue across all days/countries.
    - ``orders``: total order count.
    - ``unique_customers``: number of distinct customers (rows in the LTV mart).
    """
    return {
        "gross_revenue": float(daily["gross_revenue"].sum()),
        "orders": int(daily["order_count"].sum()),
        "unique_customers": int(ltv.shape[0]),
    }


def revenue_by_country(daily: pd.DataFrame) -> pd.DataFrame:
    """Pivot daily revenue into a date x country matrix for the time series."""
    return (
        daily.assign(event_date=pd.to_datetime(daily["event_date"]))
        .pivot_table(
            index="event_date",
            columns="country",
            values="gross_revenue",
            aggfunc="sum",
        )
        .fillna(0)
    )


def top_customers(ltv: pd.DataFrame, n: int = 20) -> pd.DataFrame:
    """Top-N customers by lifetime revenue, index reset for display."""
    return (
        ltv.sort_values("lifetime_revenue", ascending=False)
        .head(n)
        .reset_index(drop=True)
    )
