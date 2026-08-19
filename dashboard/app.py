"""Customer 360 dashboard - Streamlit app over the gold layer.

Queries Trino live (catalog ``iceberg``) against the gold-layer marts
materialized by the Dagster pipeline - no local data copies, no database
of its own.

Run:
    make dashboard
    # or: .venv/bin/streamlit run dashboard/app.py
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
import trino.dbapi
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")

from marts import MARTS  # noqa: E402  (script-dir import; streamlit adds it to sys.path)

TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
TRINO_USER = os.environ.get("TRINO_USER", "admin")

st.set_page_config(
    page_title="Customer 360",
    page_icon=":material/dataset:",
    layout="wide",
)


# ---------------------------------------------------------------------------
# Data access
# ---------------------------------------------------------------------------
@st.cache_data(ttl=300, show_spinner=False)
def run_mart(name: str) -> pd.DataFrame:
    """Run one mart query from marts.py and return a DataFrame."""
    con = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER)
    try:
        cur = con.cursor()
        cur.execute(MARTS[name])
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        con.close()


def mart_or_die(name: str) -> pd.DataFrame:
    """run_mart() with a friendly error if the lakehouse is not up."""
    try:
        return run_mart(name)
    except Exception as exc:  # noqa: BLE001 - surface any Trino failure cleanly
        st.error(
            f"Could not query `{name}` on Trino ({TRINO_HOST}:{TRINO_PORT}).\n\n"
            f"Is the stack running? Start it with `make up` (or `make run`).\n\n"
            f"<small>{exc}</small>"
        )
        st.stop()


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------
st.title("Customer 360")
st.caption(
    "E-commerce customer analytics over the **gold layer** "
    "(`iceberg.gold`, MinIO + Iceberg + Trino). "
    "Synthetic data, seed 42: 5,000 customers / 50,000 orders, Jan-Dec 2024."
)

with st.sidebar:
    st.header("Controls")
    if st.button(":material/refresh: Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.divider()
    st.markdown(
        "**Pipeline**: Dagster job `lakehouse_refresh` (bronze -> silver -> gold, "
        "85 DQ checks).\n\n"
        "**Stack**: MinIO - Iceberg - Hive Metastore - Trino - Dagster - Streamlit."
    )

# --- KPI row -----------------------------------------------------------------
kpi_customers = mart_or_die("kpi_customers")
kpi_orders = mart_or_die("kpi_orders")
kpi_gmv = mart_or_die("kpi_gmv")
kpi_aov = mart_or_die("kpi_aov")

col_a, col_b, col_c, col_d = st.columns(4)
col_a.metric("Total customers", f"{int(kpi_customers['value'][0]):,}")
col_b.metric("Total orders", f"{int(kpi_orders['value'][0]):,}")
col_c.metric("Gross revenue", f"${kpi_gmv['value'][0]:,.2f}")
col_d.metric("Avg order value (net)", f"${float(kpi_aov['value'][0]):,.2f}")

st.divider()

# --- Revenue trend -------------------------------------------------------------
trend = mart_or_die("revenue_trend")
trend_long = trend.melt(
    id_vars="month", value_vars=["gross_revenue", "net_revenue"],
    var_name="series", value_name="revenue",
)
trend_long["series"] = trend_long["series"].str.capitalize()
trend_long = trend_long.sort_values("month")
fig_trend = px.line(
    trend_long, x="month", y="revenue", color="series",
    markers=True,
    title="Revenue trend (monthly, gross vs net)",
)
fig_trend.update_traces(mode="lines+markers")
fig_trend.update_layout(height=380, margin=dict(t=50, b=10, l=10, r=10))
st.plotly_chart(fig_trend, use_container_width=True)

# --- LTV distribution + churn risk ---------------------------------------------
ltv = mart_or_die("ltv_distribution").sort_values("ltv_bucket")
churn = mart_or_die("churn_risk_mix")

row1_a, row1_b = st.columns([3, 2])
fig_ltv = px.bar(
    ltv, x="ltv_bucket", y="customers",
    title="LTV distribution (net revenue per customer)",
)
fig_ltv.update_layout(height=360, margin=dict(t=50, b=10, l=10, r=10))
row1_a.plotly_chart(fig_ltv, use_container_width=True)

fig_churn = px.pie(
    churn, names="churn_risk", values="customers", hole=0.45,
    title="Churn risk mix",
)
fig_churn.update_layout(height=360, margin=dict(t=50, b=10, l=10, r=10))
row1_b.plotly_chart(fig_churn, use_container_width=True)

# --- RFM segments + channel mix ---------------------------------------------------
rfm = mart_or_die("rfm_segment_mix").sort_values("rfm_segment")
channel = mart_or_die("channel_mix")

row2_a, row2_b = st.columns(2)
fig_rfm = px.bar(
    rfm, x="rfm_segment", y="customers",
    title="RFM segments (customers per segment)",
)
fig_rfm.update_layout(height=360, margin=dict(t=50, b=10, l=10, r=10))
row2_a.plotly_chart(fig_rfm, use_container_width=True)

fig_channel = px.bar(
    channel, x="channel", y="net_revenue",
    title="Net revenue by channel",
)
fig_channel.update_layout(height=360, margin=dict(t=50, b=10, l=10, r=10))
row2_b.plotly_chart(fig_channel, use_container_width=True)

# --- Category mix -----------------------------------------------------------------
category = mart_or_die("category_mix")
fig_cat = px.bar(
    category, x="category", y="gross_revenue",
    title="Gross revenue by product category",
)
fig_cat.update_layout(height=380, margin=dict(t=50, b=10, l=10, r=10))
st.plotly_chart(fig_cat, use_container_width=True)

# --- Customer 360 sample ---------------------------------------------------------
st.divider()
st.subheader("Customer profiles (top 25 by realized LTV)")
sample = mart_or_die("customer_sample")
display = sample.rename(
    columns={
        "customer_id": "Customer",
        "first_name": "First name",
        "last_name": "Last name",
        "marketing_channel": "Channel",
        "signup_date": "Signup",
        "total_orders": "Orders",
        "valid_orders": "Valid orders",
        "gross_revenue": "Gross rev.",
        "net_revenue": "Net rev. (LTV)",
        "total_tickets": "Tickets",
        "open_tickets": "Open tickets",
        "total_web_events": "Web events",
    }
)
st.dataframe(
    display, use_container_width=True, hide_index=True, height=420,
    column_config={
        "Gross rev.": st.column_config.NumberColumn(format="%.2f"),
        "Net rev. (LTV)": st.column_config.NumberColumn(format="%.2f"),
    },
)
st.caption(
    "Source: `iceberg.gold.customer_360` (one row per customer: demographics, "
    "order/LTV metrics, RFM scores + segment, churn risk, support & web engagement)."
)
