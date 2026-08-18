#!/usr/bin/env python3
"""Verify the Phase 7 gold layer (Customer 360 marts).

Checks (stack up, silver materialized):
  1. All 4 gold tables exist.
  2. customer_360 is one row per customer and consistent with silver:
     order/refund totals reconcile, score ranges, segment & churn
     domains, no-order customers accounted for.
  3. Revenue invariant: gross revenue sums agree across
     silver.fct_orders, gold.customer_360, gold.revenue_by_channel,
     gold.revenue_by_category and gold.monthly_kpis.
  4. monthly_kpis covers the full month range and reconciles volumes.
  5. Money columns are DECIMAL.

Exit code 0 + "RESULT: PASS" when everything is green.
"""

from __future__ import annotations

import sys

from trino.dbapi import connect

GOLD_TABLES = [
    "customer_360",
    "revenue_by_channel",
    "revenue_by_category",
    "monthly_kpis",
]

MONEY_COLUMNS = [
    ("customer_360", "gross_revenue"),
    ("customer_360", "total_refunded"),
    ("customer_360", "net_revenue"),
    ("customer_360", "aov"),
    ("revenue_by_channel", "gross_revenue"),
    ("revenue_by_channel", "refunds"),
    ("revenue_by_channel", "net_revenue"),
    ("revenue_by_channel", "aov"),
    ("revenue_by_category", "gross_revenue"),
    ("monthly_kpis", "gross_revenue"),
    ("monthly_kpis", "refunds"),
    ("monthly_kpis", "net_revenue"),
    ("monthly_kpis", "aov"),
]


def main() -> int:
    failures: list[str] = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f" ({detail})" if detail else ""))
        if not ok:
            failures.append(name)

    cur = connect(host="localhost", port=8080, user="admin").cursor()

    def scalar(sql: str):
        cur.execute(sql)
        return cur.fetchone()[0]

    print("Gold layer verification")
    print("=" * 60)

    # 1. Tables exist
    cur.execute("SHOW TABLES FROM iceberg.gold")
    present = {row[0] for row in cur.fetchall()}
    check("all 4 gold tables exist", set(GOLD_TABLES) <= present, f"present: {len(present)}")

    silver_customers = scalar("SELECT count(*) FROM iceberg.silver.dim_customers")
    orders_total = scalar("SELECT count(*) FROM iceberg.silver.fct_orders")
    customers_in_orders = scalar(
        "SELECT count(DISTINCT customer_id) FROM iceberg.silver.fct_orders")
    gross_valid = float(scalar(
        "SELECT coalesce(sum(total_amount) FILTER (WHERE is_cancelled = false), 0) "
        "FROM iceberg.silver.fct_orders"))
    refunds_total = float(scalar(
        "SELECT coalesce(sum(refund_amount), 0) FROM iceberg.silver.fct_refunds"))

    # 2. customer_360 structure & reconciliation
    c360_rows = scalar("SELECT count(*) FROM iceberg.gold.customer_360")
    check("customer_360: one row per customer", c360_rows == silver_customers,
          f"{c360_rows:,} vs {silver_customers:,}")

    total_orders_sum = scalar("SELECT sum(total_orders) FROM iceberg.gold.customer_360")
    check("customer_360: sum(total_orders) = silver orders",
          total_orders_sum == orders_total,
          f"{total_orders_sum:,} vs {orders_total:,}")

    gross_c360 = float(scalar("SELECT sum(gross_revenue) FROM iceberg.gold.customer_360"))
    check("customer_360: sum(gross_revenue) = silver valid-order gross",
          abs(gross_c360 - gross_valid) < 0.05,
          f"{gross_c360:,.2f} vs {gross_valid:,.2f}")

    refunds_c360 = float(scalar("SELECT sum(total_refunded) FROM iceberg.gold.customer_360"))
    check("customer_360: sum(total_refunded) = silver refunds",
          abs(refunds_c360 - refunds_total) < 0.05,
          f"{refunds_c360:,.2f} vs {refunds_total:,.2f}")

    no_orders = scalar(
        "SELECT count(*) FROM iceberg.gold.customer_360 WHERE total_orders = 0")
    check("customer_360: no-order customers accounted for",
          no_orders == silver_customers - customers_in_orders,
          f"{no_orders:,} vs {silver_customers - customers_in_orders:,}")

    bad_scores = scalar(
        "SELECT count(*) FROM iceberg.gold.customer_360 "
        "WHERE total_orders > 0 "
        "  AND (r_score NOT BETWEEN 1 AND 5 OR f_score NOT BETWEEN 1 AND 5 "
        "       OR m_score NOT BETWEEN 1 AND 5)")
    null_scores = scalar(
        "SELECT count(*) FROM iceberg.gold.customer_360 "
        "WHERE total_orders > 0 AND (r_score IS NULL OR f_score IS NULL "
        "                            OR m_score IS NULL)")
    check("customer_360: RFM scores in 1..5 for ordering customers",
          bad_scores == 0 and null_scores == 0,
          f"out_of_range={bad_scores}, null={null_scores}")

    bad_segments = scalar(
        "SELECT count(*) FROM iceberg.gold.customer_360 "
        "WHERE rfm_segment NOT IN ('no_orders', 'champion', 'loyal', "
        "  'new_or_returning', 'lost', 'hibernating', 'potential_loyalist', "
        "  'needs_attention')")
    seg_no_orders = scalar(
        "SELECT count(*) FROM iceberg.gold.customer_360 WHERE rfm_segment = 'no_orders'")
    check("customer_360: rfm_segment domain", bad_segments == 0,
          f"invalid={bad_segments}")
    check("customer_360: 'no_orders' segment matches zero-order customers",
          seg_no_orders == no_orders, f"{seg_no_orders:,} vs {no_orders:,}")

    bad_churn = scalar(
        "SELECT count(*) FROM iceberg.gold.customer_360 "
        "WHERE churn_risk NOT IN ('none', 'low', 'medium', 'high')")
    churn_none = scalar(
        "SELECT count(*) FROM iceberg.gold.customer_360 WHERE churn_risk = 'none'")
    check("customer_360: churn_risk domain", bad_churn == 0, f"invalid={bad_churn}")
    check("customer_360: churn 'none' matches zero-order customers",
          churn_none == no_orders, f"{churn_none:,} vs {no_orders:,}")

    neg_recency = scalar(
        "SELECT count(*) FROM iceberg.gold.customer_360 "
        "WHERE total_orders > 0 AND recency_days < 0")
    null_recency = scalar(
        "SELECT count(*) FROM iceberg.gold.customer_360 "
        "WHERE total_orders > 0 AND recency_days IS NULL")
    check("customer_360: recency_days >= 0 for ordering customers",
          neg_recency == 0 and null_recency == 0)

    # 3. Revenue invariant across all five views
    gmv_views = {
        "silver.fct_orders": gross_valid,
        "gold.customer_360": gross_c360,
        "gold.revenue_by_channel": float(
            scalar("SELECT sum(gross_revenue) FROM iceberg.gold.revenue_by_channel")),
        "gold.revenue_by_category": float(
            scalar("SELECT sum(gross_revenue) FROM iceberg.gold.revenue_by_category")),
        "gold.monthly_kpis": float(
            scalar("SELECT sum(gross_revenue) FROM iceberg.gold.monthly_kpis")),
    }
    spread = max(gmv_views.values()) - min(gmv_views.values())
    check("gross revenue consistent across all 5 views", spread < 0.05,
          "spread " + f"{spread:.2f} in " +
          ", ".join(f"{k}={v:,.2f}" for k, v in gmv_views.items()))

    # 4. monthly_kpis volume reconciliation
    kpi_months = scalar("SELECT count(DISTINCT month) FROM iceberg.gold.monthly_kpis")
    check("monthly_kpis: covers 12 months", kpi_months == 12, f"{kpi_months} months")
    kpi_orders = scalar("SELECT sum(orders) FROM iceberg.gold.monthly_kpis")
    check("monthly_kpis: sum(orders) = silver orders",
          kpi_orders == orders_total, f"{kpi_orders:,} vs {orders_total:,}")
    kpi_new = scalar("SELECT sum(new_customers) FROM iceberg.gold.monthly_kpis")
    check("monthly_kpis: sum(new_customers) = distinct ordering customers",
          kpi_new == customers_in_orders,
          f"{kpi_new:,} vs {customers_in_orders:,}")

    # 5. Money columns are decimal
    cur.execute(
        "SELECT table_name, column_name, data_type FROM iceberg.information_schema.columns "
        "WHERE table_catalog = 'iceberg' AND table_schema = 'gold'"
    )
    types = {(r[0], r[1]): r[2] for r in cur.fetchall()}
    bad = [(t, c) for t, c in MONEY_COLUMNS
           if "decimal" not in types.get((t, c), "").lower()]
    check("all money columns are DECIMAL", not bad,
          "bad: " + ", ".join(f"{t}.{c}={types[(t, c)]}" for t, c in bad) if bad else "")

    # Informational (reported, not asserted)
    cur.execute(
        "SELECT rfm_segment, count(*) FROM iceberg.gold.customer_360 "
        "GROUP BY 1 ORDER BY 2 DESC")
    print("        info: segments: " +
          ", ".join(f"{s}={n:,}" for s, n in cur.fetchall()))
    cur.execute(
        "SELECT churn_risk, count(*) FROM iceberg.gold.customer_360 "
        "GROUP BY 1 ORDER BY 1")
    print("        info: churn:    " +
          ", ".join(f"{s}={n:,}" for s, n in cur.fetchall()))

    print("=" * 60)
    if failures:
        print(f"RESULT: FAIL - {len(failures)} check(s) failed: {failures}")
        return 1
    print("RESULT: PASS - gold layer verified")
    return 0


if __name__ == "__main__":
    sys.exit(main())
