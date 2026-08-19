"""Customer 360 dashboard - interactive Streamlit app over the gold layer.

Queries Trino live (catalog ``iceberg``) against the gold-layer marts
materialized by the Dagster pipeline - no local data copies, no database
of its own. Sidebar filters (channel, churn risk, month range, customer
search) are pushed down into the mart SQL.

Run:
    make dashboard
    # or: .venv/bin/streamlit run dashboard/app.py
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
import trino.dbapi
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")

from marts import MARTS, build_sql  # noqa: E402  (script-dir import; streamlit adds it to sys.path)

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
def _query(sql: str) -> pd.DataFrame:
    con = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER)
    try:
        cur = con.cursor()
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        con.close()


def mart_or_die(name: str, f: dict) -> pd.DataFrame:
    """Run one (optionally filtered) mart with a friendly error if the
    lakehouse is not up."""
    try:
        return _query(build_sql(name, f))
    except Exception as exc:  # noqa: BLE001 - surface any Trino failure cleanly
        st.error(
            f"Could not query `{name}` on Trino ({TRINO_HOST}:{TRINO_PORT}).\n\n"
            f"Is the stack running? Start it with `make up` (or `make run`).\n\n"
            f"<small>{exc}</small>"
        )
        st.stop()


# ---------------------------------------------------------------------------
# Sidebar: filters
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("Controls")
    if st.button(":material/refresh: Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.header("Filters")
    try:
        channel_opts = _query(
            "SELECT DISTINCT channel FROM iceberg.gold.revenue_by_channel ORDER BY 1"
        )["channel"].tolist()
        acq_opts = _query(
            "SELECT DISTINCT marketing_channel FROM iceberg.gold.customer_360 ORDER BY 1"
        )["marketing_channel"].tolist()
        risk_opts = _query(
            "SELECT DISTINCT churn_risk FROM iceberg.gold.customer_360 ORDER BY 1"
        )["churn_risk"].tolist()
        mrange = _query(
            "SELECT min(month) AS mn, max(month) AS mx "
            "FROM iceberg.gold.revenue_by_channel"
        ).iloc[0]
    except Exception:
        st.error("Trino not reachable - start the stack with `make up`.")
        st.stop()

    channels = st.multiselect("Order channels (revenue views)", channel_opts, default=channel_opts, key="f_channels")
    acq = st.multiselect("Acquisition channel (customer views)", acq_opts, default=acq_opts, key="f_acq")
    risks = st.multiselect("Churn risk", risk_opts, default=risk_opts, key="f_risks")

    def _d(v) -> date:
        return v if isinstance(v, date) else date.fromisoformat(str(v)[:10])

    full_range = (_d(mrange["mn"]), _d(mrange["mx"]))
    months = st.date_input(
        "Order months",
        value=full_range,
        min_value=full_range[0],
        max_value=full_range[1],
        key="f_months",
    )
    month_range = None
    if isinstance(months, tuple) and len(months) == 2 and months[0] and months[1]:
        month_range = (months[0].isoformat(), months[1].isoformat())

    f = {
        "channels": channels or None,
        "acq_channels": acq or None,
        "churn_risks": risks or None,
        "months": month_range,
        "search": None,
    }

    filtered = (
        (channels and channels != channel_opts)
        or (acq and acq != acq_opts)
        or (risks and risks != risk_opts)
        or (month_range and month_range != (full_range[0].isoformat(), full_range[1].isoformat()))
    )
    if filtered:
        st.info("Filters active - all views below are re-queried on Trino.")
        if st.button(":material/filter_alt_off: Clear filters", use_container_width=True):
            st.session_state["f_channels"] = channel_opts
            st.session_state["f_acq"] = acq_opts
            st.session_state["f_risks"] = risk_opts
            st.session_state["f_months"] = full_range
            st.rerun()
    st.divider()
    st.markdown(
        "**Pipeline**: Dagster job `lakehouse_refresh` (bronze → silver → gold, "
        "85 DQ checks).\n\n"
        "**Data**: synthetic, seeded (`DATA_SEED`). Re-seed with "
        "`make reseed SEED=<n>` to see the whole pipeline react to new data.\n\n"
        "**Stack**: MinIO · Iceberg · Trino · Dagster · Streamlit."
    )


st.title("Customer 360")
st.caption(
    "E-commerce customer analytics over the **gold layer** "
    "(`iceberg.gold`, MinIO + Iceberg + Trino). Live queries - every filter "
    "change is executed on Trino."
)

# ---------------------------------------------------------------------------
# KPI row
# ---------------------------------------------------------------------------
kpi_customers = mart_or_die("kpi_customers", f)
kpi_orders = mart_or_die("kpi_orders", f)
kpi_gmv = mart_or_die("kpi_gmv", f)
kpi_aov = mart_or_die("kpi_aov", f)

col_a, col_b, col_c, col_d = st.columns(4)
col_a.metric("Total customers", f"{int(kpi_customers['value'][0]):,}")
col_b.metric("Total orders", f"{int(kpi_orders['value'][0]):,}")
col_c.metric("Gross revenue", f"${kpi_gmv['value'][0]:,.2f}")
col_d.metric("Avg order value (net)", f"${float(kpi_aov['value'][0]):,.2f}")

st.divider()

# ---------------------------------------------------------------------------
# Revenue trend
# ---------------------------------------------------------------------------
trend = mart_or_die("revenue_trend", f)
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

# ---------------------------------------------------------------------------
# LTV distribution + churn risk
# ---------------------------------------------------------------------------
ltv = mart_or_die("ltv_distribution", f).sort_values("ltv_bucket")
churn = mart_or_die("churn_risk_mix", f)

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

# ---------------------------------------------------------------------------
# RFM segments + channel mix
# ---------------------------------------------------------------------------
rfm = mart_or_die("rfm_segment_mix", f).sort_values("rfm_segment")
channel = mart_or_die("channel_mix", f)

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

# ---------------------------------------------------------------------------
# Category mix
# ---------------------------------------------------------------------------
category = mart_or_die("category_mix", f)
fig_cat = px.bar(
    category, x="category", y="gross_revenue",
    title="Gross revenue by product category",
)
fig_cat.update_layout(height=380, margin=dict(t=50, b=10, l=10, r=10))
st.plotly_chart(fig_cat, use_container_width=True)

# ---------------------------------------------------------------------------
# Customer profiles (searchable)
# ---------------------------------------------------------------------------
st.divider()
st.subheader("Customer profiles (top 25 by realized LTV)")
search = st.text_input(
    "Search customers (name, email, customer_id)",
    key="customer_search",
    placeholder="e.g. 'smith' or C-00042",
)
f_search = dict(f)
f_search["search"] = search or None
sample = mart_or_die("customer_sample", f_search)

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
