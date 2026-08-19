"""Gold-layer mart queries for the Customer 360 dashboard.

Single source of truth for the dashboard SQL. Every query reads from
``iceberg.gold`` (the gold layer materialized by the Dagster pipeline -
see ``dagster_project/ecommerce_lakehouse/assets/gold/``).

``build_sql(name, filters)`` renders a mart with optional filters:

    filters = {
        "channels":       ["web", ...]   # ORDER channel (revenue marts)
        "acq_channels":   ["email", ...] # ACQUISITION channel (customer marts)
        "churn_risks":    ["high", ...]  # customer marts only
        "months":         ("2024-03-01", "2024-09-01")  # revenue marts only
        "search":         "name fragment"                # customer marts only
    }

Note: order channels (mobile/pos/web) and customer acquisition channels
(direct/email/organic_search/...) are different dimensions - they are
filtered separately.

Pass ``filters=None`` (or omit keys) for the unfiltered mart - that is what
``scripts/verify_dashboard.py`` uses.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Filter helpers
# ---------------------------------------------------------------------------
def _q(v: str) -> str:
    """Single-quote an SQL string literal (escape embedded quotes)."""
    return "'" + v.replace("'", "''") + "'"


def _in_list(values: list[str]) -> str:
    return ", ".join(_q(v) for v in values)


def _customer_where(f: dict) -> str:
    """WHERE clause for customer_360-based marts."""
    clauses = []
    if f.get("acq_channels"):
        clauses.append(f"marketing_channel IN ({_in_list(f['acq_channels'])})")
    if f.get("churn_risks"):
        clauses.append(f"churn_risk IN ({_in_list(f['churn_risks'])})")
    if f.get("search"):
        s = f["search"].lower().replace("'", "''")
        clauses.append(
            f"(lower(first_name) || ' ' || lower(last_name) || ' ' || lower(email) "
            f"|| ' ' || lower(customer_id)) LIKE '%{s}%'"
        )
    return (" WHERE " + " AND ".join(clauses)) if clauses else ""


def _revenue_where(f: dict, column: str = "channel") -> str:
    """WHERE clause for channel/category revenue marts."""
    clauses = []
    if f.get("channels") and column == "channel":
        clauses.append(f"channel IN ({_in_list(f['channels'])})")
    if f.get("months"):
        clauses.append(
            f"month BETWEEN DATE '{f['months'][0]}' AND DATE '{f['months'][1]}'"
        )
    return (" WHERE " + " AND ".join(clauses)) if clauses else ""


# ---------------------------------------------------------------------------
# Mart builders - one per dashboard view
# ---------------------------------------------------------------------------
def kpi_customers(f: dict) -> str:
    return (
        f"SELECT COUNT(*) AS value FROM iceberg.gold.customer_360"
        f"{_customer_where(f)}"
    )


def kpi_orders(f: dict) -> str:
    # revenue_by_channel (not monthly_kpis) so the channel/month filters apply
    return (
        "SELECT SUM(orders) AS value FROM iceberg.gold.revenue_by_channel"
        f"{_revenue_where(f)}"
    )


def kpi_gmv(f: dict) -> str:
    return (
        "SELECT SUM(gross_revenue) AS value FROM iceberg.gold.revenue_by_channel"
        f"{_revenue_where(f)}"
    )


def kpi_aov(f: dict) -> str:
    return (
        "SELECT SUM(net_revenue) / SUM(valid_orders) AS value "
        "FROM iceberg.gold.revenue_by_channel"
        f"{_revenue_where(f)}"
    )


def ltv_distribution(f: dict) -> str:
    return f"""
        SELECT CASE
                   WHEN net_revenue <= 0   THEN '0. no revenue'
                   WHEN net_revenue < 50   THEN '1. < 50'
                   WHEN net_revenue < 250  THEN '2. 50-250'
                   WHEN net_revenue < 500  THEN '3. 250-500'
                   WHEN net_revenue < 1000 THEN '4. 500-1k'
                   WHEN net_revenue < 2500 THEN '5. 1k-2.5k'
                   WHEN net_revenue < 5000 THEN '6. 2.5k-5k'
                   ELSE '7. 5k+'
               END AS ltv_bucket,
               COUNT(*) AS customers
        FROM iceberg.gold.customer_360
        {_customer_where(f)}
        GROUP BY 1
    """


def churn_risk_mix(f: dict) -> str:
    return (
        "SELECT churn_risk, COUNT(*) AS customers FROM iceberg.gold.customer_360"
        f"{_customer_where(f)} GROUP BY 1"
    )


def rfm_segment_mix(f: dict) -> str:
    return (
        "SELECT rfm_segment, COUNT(*) AS customers FROM iceberg.gold.customer_360"
        f"{_customer_where(f)} GROUP BY 1"
    )


def revenue_trend(f: dict) -> str:
    return (
        "SELECT month, SUM(gross_revenue) AS gross_revenue, "
        "       SUM(net_revenue) AS net_revenue "
        "FROM iceberg.gold.revenue_by_channel"
        f"{_revenue_where(f)} GROUP BY 1"
    )


def channel_mix(f: dict) -> str:
    return (
        "SELECT channel, SUM(net_revenue) AS net_revenue "
        "FROM iceberg.gold.revenue_by_channel"
        f"{_revenue_where(f)} GROUP BY 1"
    )


def category_mix(f: dict) -> str:
    return (
        "SELECT category, SUM(gross_revenue) AS gross_revenue "
        "FROM iceberg.gold.revenue_by_category"
        f"{_revenue_where(f, column='category')} GROUP BY 1"
    )


def customer_sample(f: dict) -> str:
    return f"""
        SELECT customer_id, first_name, last_name, country, city, age,
               marketing_channel, signup_date,
               total_orders, frequency AS valid_orders,
               gross_revenue, net_revenue,
               CASE
                   WHEN net_revenue <= 0   THEN '0. no revenue'
                   WHEN net_revenue < 50   THEN '1. < 50'
                   WHEN net_revenue < 250  THEN '2. 50-250'
                   WHEN net_revenue < 500  THEN '3. 250-500'
                   WHEN net_revenue < 1000 THEN '4. 500-1k'
                   WHEN net_revenue < 2500 THEN '5. 1k-2.5k'
                   WHEN net_revenue < 5000 THEN '6. 2.5k-5k'
                   ELSE '7. 5k+'
               END AS ltv_bucket,
               rfm_segment, churn_risk,
               total_tickets, open_tickets, total_web_events
        FROM iceberg.gold.customer_360
        {_customer_where(f)}
        ORDER BY net_revenue DESC
        LIMIT 25
    """


MARTS: dict[str, callable] = {
    "kpi_customers": kpi_customers,
    "kpi_orders": kpi_orders,
    "kpi_gmv": kpi_gmv,
    "kpi_aov": kpi_aov,
    "ltv_distribution": ltv_distribution,
    "churn_risk_mix": churn_risk_mix,
    "rfm_segment_mix": rfm_segment_mix,
    "revenue_trend": revenue_trend,
    "channel_mix": channel_mix,
    "category_mix": category_mix,
    "customer_sample": customer_sample,
}


def build_sql(name: str, filters: dict | None = None) -> str:
    """Render the SQL for one mart, optionally filtered."""
    if name not in MARTS:
        raise KeyError(f"unknown mart: {name}")
    return MARTS[name](filters or {})
