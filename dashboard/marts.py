"""Gold-layer mart queries for the Customer 360 dashboard.

Single source of truth for the dashboard SQL. Every query reads from
``iceberg.gold`` (the gold layer materialized by the Dagster pipeline -
see ``dagster_project/ecommerce_lakehouse/assets/gold/``).

The dashboard (``dashboard/app.py``) renders one view per mart; the
verification script (``scripts/verify_dashboard.py``) executes all of
them and checks invariants.
"""

from __future__ import annotations

MARTS: dict[str, str] = {
    # --- KPI marts (scalar values) -------------------------------------------
    "kpi_customers": (
        "SELECT COUNT(*) AS value FROM iceberg.gold.customer_360"
    ),
    "kpi_orders": (
        "SELECT SUM(orders) AS value FROM iceberg.gold.monthly_kpis"
    ),
    "kpi_gmv": (
        "SELECT SUM(gross_revenue) AS value FROM iceberg.gold.revenue_by_channel"
    ),
    "kpi_aov": (
        "SELECT SUM(net_revenue) / SUM(valid_orders) AS value "
        "FROM iceberg.gold.revenue_by_channel"
    ),
    # --- Distribution / mix marts ---------------------------------------------
    "ltv_distribution": """
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
        GROUP BY 1
    """,
    "churn_risk_mix": (
        "SELECT churn_risk, COUNT(*) AS customers "
        "FROM iceberg.gold.customer_360 GROUP BY 1"
    ),
    "rfm_segment_mix": (
        "SELECT rfm_segment, COUNT(*) AS customers "
        "FROM iceberg.gold.customer_360 GROUP BY 1"
    ),
    "revenue_trend": (
        "SELECT month, SUM(gross_revenue) AS gross_revenue, "
        "       SUM(net_revenue) AS net_revenue "
        "FROM iceberg.gold.revenue_by_channel GROUP BY 1"
    ),
    "channel_mix": (
        "SELECT channel, SUM(net_revenue) AS net_revenue "
        "FROM iceberg.gold.revenue_by_channel GROUP BY 1"
    ),
    "category_mix": (
        "SELECT category, SUM(gross_revenue) AS gross_revenue "
        "FROM iceberg.gold.revenue_by_category GROUP BY 1"
    ),
    # --- Customer 360 sample (wide profile, top by realized LTV) --------------
    "customer_sample": """
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
        ORDER BY net_revenue DESC
        LIMIT 25
    """,
}
