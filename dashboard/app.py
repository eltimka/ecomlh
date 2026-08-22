"""Customer 360 dashboard - interactive Streamlit app over the gold layer.

Queries Trino live (catalog ``iceberg``) against the gold-layer marts
materialized by the Dagster pipeline - no local data copies, no database
of its own. Sidebar filters (channel, churn risk, month range, customer
search) are pushed down into the mart SQL.

Two views (sidebar toggle):
  * Customer 360 (gold) - the mart views above.
  * Live stream - Kafka -> Flink -> Iceberg web event path: job state,
    committed rows vs Kafka end offset (lag), source row counts, event
    rate, latest events. Auto-refreshes every 15s (st.fragment).

Run:
    make dashboard
    # or: .venv/bin/streamlit run dashboard/app.py
"""

from __future__ import annotations

import os
import time
from datetime import date
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
import trino.dbapi
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")

from live import (  # noqa: E402  (script-dir import; streamlit adds it to sys.path)
    flink_job_status,
    kafka_topic_end_offset,
    latest_events,
    latest_orders,
    money_stream_summary,
    stream_counts,
)
from marts import MARTS, build_sql  # noqa: E402

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
# Sidebar: view toggle + filters
# ---------------------------------------------------------------------------
GOLD_VIEW = "Customer 360 (gold)"
LIVE_VIEW = "Live stream (web events)"

with st.sidebar:
    st.header("Controls")
    view = st.radio("View", [GOLD_VIEW, LIVE_VIEW], key="view")

    if view == GOLD_VIEW:
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
    else:
        st.caption(
            "The live view auto-refreshes every 15s. Feed it events with "
            "`make stream-up` (replays history only if the topic is empty, "
            "then streams live at ~20 events/s); stop with `make stream-down`."
        )
    st.divider()
    st.markdown(
        "**Pipeline**: Dagster job `lakehouse_refresh` (bronze → silver → gold, "
        "109 DQ checks).\n\n"
        "**Data**: synthetic, seeded (`DATA_SEED`). Re-seed with "
        "`make reseed SEED=<n>` to see the whole pipeline react to new data.\n\n"
        "**Stack**: MinIO · Iceberg · Trino · Dagster · Streamlit."
    )


# ---------------------------------------------------------------------------
# Live stream view (auto-refreshing fragment)
# ---------------------------------------------------------------------------
@st.fragment(run_every="15s")
def _live_panel() -> None:
    job = flink_job_status()
    kafka = kafka_topic_end_offset()
    counts = stream_counts()

    if not job.get("found"):
        st.error(
            f"**Flink stream job** is {job['state']}.\n\n"
            f"<small>{job.get('error', 'no job with the stream name is running')}\n\n"
            f"Start it with `make flink-up` (or `make run`).</small>"
        )
    elif not counts.get("ok"):
        st.error(
            "**Stream table missing or unreadable.**\n\n"
            f"<small>{counts.get('error', '')}\n\n"
            f"The Flink job writes `iceberg.bronze.stream_web_events` - start it with "
            f"`make flink-up`.</small>"
        )

    if job.get("found") and job.get("state") == "RUNNING":
        last_ms = job.get("last_checkpoint_ms")
        st.success(
            f"**Flink job RUNNING** - {job.get('checkpoints_completed', 0)} checkpoints completed"
            + (f", last took {last_ms / 1000:.1f}s" if last_ms else "")
        )
    elif job.get("state") not in (None, "unreachable", "not running"):
        st.warning(f"**Flink job state: {job['state']}**")

    # committed rows + event rate (self-measured between refreshes)
    now = time.time()
    samples: list[tuple[float, int]] = st.session_state.setdefault("live_samples", [])
    rate_per_min = None
    if counts.get("ok"):
        samples.append((now, counts["stream_rows"]))
        samples[:] = [s for s in samples if now - s[0] <= 120]
        if len(samples) >= 2:
            (t0, r0), (t1, r1) = samples[0], samples[-1]
            if t1 - t0 >= 5:
                rate_per_min = (r1 - r0) / (t1 - t0) * 60

    c1, c2, c3, c4 = st.columns(4)
    c1.metric(
        "Committed rows (stream bronze)",
        f"{counts['stream_rows']:,}" if counts.get("ok") else "-",
    )
    if kafka.get("ok"):
        c2.metric(
            "Kafka topic end offset",
            f"{kafka['end_offset']:,}" if counts.get("ok") else "-",
            delta=(
                f"{kafka['end_offset'] - counts['stream_rows']:,} uncommitted"
                if counts.get("ok") and kafka["end_offset"] != counts["stream_rows"]
                else "caught up"
            )
            if counts.get("ok")
            else None,
        )
    else:
        c2.metric("Kafka topic end offset", "-", delta="broker unreachable" if not counts.get("ok") else None)
    c3.metric(
        "Event rate (measured)",
        f"~{rate_per_min:,.0f}/min" if rate_per_min is not None else "waiting...",
    )
    c4.metric(
        "Last event",
        counts.get("last_event_id") or "-",
        delta=counts.get("last_event_date"),
    )

    if not kafka.get("ok") and counts.get("ok"):
        st.warning(f"Kafka not reachable - lag unknown. <small>{kafka.get('error', '')}</small>")

    if counts.get("ok"):
        s1, s2, s3 = st.columns(3)
        s1.metric("Batch bronze (`web_events`)", f"{counts['batch_rows']:,}")
        s2.metric("Stream bronze (`stream_web_events`)", f"{counts['stream_rows']:,}")
        s3.metric(
            "Merged silver (`fct_web_events`)",
            f"{counts['merged_rows']:,}",
            delta=f"{counts['batch_rows'] + counts['stream_rows'] - counts['merged_rows']:,} deduped on event_id",
        )

    latest = None
    if counts.get("ok") and counts["stream_rows"] > 0:
        try:
            latest = latest_events()
        except Exception:  # noqa: BLE001
            pass
    if latest is not None:
        st.markdown("**Latest events (stream bronze, newest first)**")
        st.dataframe(
            latest,
            use_container_width=True,
            hide_index=True,
            height=300,
            column_config={
                "customer_id": st.column_config.TextColumn("Customer (null = anonymous)"),
            },
        )

    # ------------------------------------------------------------------ money
    st.markdown(
        "**Money path (orders / items / payments)** - Phase 15: the order "
        "stream flows Kafka → Flink → Iceberg → silver merge → gold."
    )
    try:
        money = money_stream_summary()
        mrows = []
        for key, e in money["streams"].items():
            rows, kend = e.get("stream_rows"), e.get("kafka_end")
            mrows.append({
                "stream": key,
                "flink job": e.get("job_state") or "n/a",
                "kafka end offset": kend if kend is not None else "-",
                "committed rows": rows if rows is not None else "-",
                "uncommitted": (kend - rows) if (rows is not None and kend is not None) else "-",
            })
        st.dataframe(pd.DataFrame(mrows), use_container_width=True, hide_index=True, height=160)
        if money["streams"].get("orders", {}).get("stream_rows"):
            odf = latest_orders()
            if not odf.empty:
                st.markdown("**Latest orders (stream bronze, newest first)**")
                st.dataframe(
                    odf, use_container_width=True, hide_index=True, height=220,
                    column_config={
                        "order_id": st.column_config.TextColumn("Order"),
                        "customer_id": st.column_config.TextColumn("Customer"),
                        "channel": st.column_config.TextColumn("Channel"),
                        "total_amount": st.column_config.NumberColumn("Total", format="%.2f"),
                    },
                )
    except Exception as exc:  # noqa: BLE001 - money stream may not exist yet
        st.warning(f"Money stream not readable yet. <small>{str(exc)[:160]}</small>")

    st.caption(f"Last updated {time.strftime('%H:%M:%S')} - auto-refreshes every 15s.")


if view == LIVE_VIEW:
    st.title("Live stream (web events + money path)")
    st.caption(
        "Kafka (`raw.web_events`, `raw.orders`, `raw.order_items`, "
        "`raw.payments`) → Flink (append) → Iceberg (`iceberg.bronze.stream_*`) "
        "→ silver dedup merge → gold. This panel polls the running stack; "
        "`make stream-up` feeds it."
    )
    _live_panel()
    st.stop()

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
