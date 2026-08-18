#!/usr/bin/env python3
"""Verify the Phase 6 silver layer.

Checks (stack up, bronze + silver materialized):
  1. All 9 silver tables exist (3 dimensions + 6 facts).
  2. Row-count conformance vs bronze (dedup/filter effects explained).
  3. Quality assertions that must hold (violations counted, expect 0):
     email uniqueness, enum normalization, payment consistency,
     line-total arithmetic, refund lag >= 0, linked-ticket validity.
  4. Money columns are DECIMAL (via information_schema).
  5. Cross-layer invariant: GMV computed from silver == GMV from bronze.

Exit code 0 + "RESULT: PASS" when everything is green.
"""

from __future__ import annotations

import sys

from trino.dbapi import connect

SILVER_TABLES = [
    "dim_customers",
    "dim_products",
    "dim_order_dates",
    "fct_orders",
    "fct_order_items",
    "fct_payments",
    "fct_refunds",
    "fct_web_events",
    "fct_support_tickets",
]

# (table, money column) pairs that must be decimal-typed in silver
MONEY_COLUMNS = [
    ("dim_products", "unit_price"),
    ("fct_orders", "total_amount"),
    ("fct_order_items", "unit_price"),
    ("fct_order_items", "line_total"),
    ("fct_payments", "amount"),
    ("fct_refunds", "refund_amount"),
    ("fct_refunds", "order_total"),
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

    print("Silver layer verification")
    print("=" * 60)

    # 1. Tables exist
    cur.execute("SHOW TABLES FROM iceberg.silver")
    present = {row[0] for row in cur.fetchall()}
    check("all 9 silver tables exist", set(SILVER_TABLES) <= present, f"present: {len(present)}")

    # 2. Row-count conformance: silver == bronze - documented drops
    counts = {t: scalar(f"SELECT count(*) FROM iceberg.silver.{t}") for t in SILVER_TABLES}
    bronze = {
        "customers": scalar("SELECT count(*) FROM iceberg.bronze.customers"),
        "products": scalar("SELECT count(*) FROM iceberg.bronze.products"),
        "orders": scalar("SELECT count(*) FROM iceberg.bronze.orders"),
        "order_items": scalar("SELECT count(*) FROM iceberg.bronze.order_items"),
        "payments": scalar("SELECT count(*) FROM iceberg.bronze.payments"),
        "refunds": scalar("SELECT count(*) FROM iceberg.bronze.refunds"),
        "web_events": scalar("SELECT count(*) FROM iceberg.bronze.web_events"),
        "support_tickets": scalar("SELECT count(*) FROM iceberg.bronze.support_tickets"),
    }
    drops = {
        "dup_customers": bronze["customers"]
        - scalar("SELECT count(DISTINCT lower(email)) FROM iceberg.bronze.customers"),
        "bad_prices": scalar(
            "SELECT count(*) FROM iceberg.bronze.products WHERE unit_price <= 0"),
        "orphan_orders": scalar(
            "SELECT count(*) FROM iceberg.bronze.orders o "
            "LEFT JOIN iceberg.bronze.customers c ON c.customer_id = o.customer_id "
            "WHERE c.customer_id IS NULL"),
        "orphan_items": scalar(
            "SELECT count(*) FROM iceberg.bronze.order_items i "
            "WHERE NOT EXISTS (SELECT 1 FROM iceberg.bronze.orders o "
            "                       WHERE o.order_id = i.order_id) "
            "   OR NOT EXISTS (SELECT 1 FROM iceberg.bronze.products p "
            "                       WHERE p.product_id = i.product_id)"),
        "dup_payments": scalar(
            "SELECT count(*) - count(DISTINCT order_id) FROM iceberg.bronze.payments"),
        "orphan_payments": scalar(
            "SELECT count(*) FROM iceberg.bronze.payments p "
            "LEFT JOIN iceberg.bronze.orders o ON o.order_id = p.order_id "
            "WHERE o.order_id IS NULL"),
        "orphan_refunds": scalar(
            "SELECT count(*) FROM iceberg.bronze.refunds r "
            "LEFT JOIN iceberg.bronze.orders o ON o.order_id = r.order_id "
            "WHERE o.order_id IS NULL"),
    }
    expected = {
        "dim_customers": bronze["customers"] - drops["dup_customers"],
        "dim_products": bronze["products"] - drops["bad_prices"],
        "fct_orders": bronze["orders"] - drops["orphan_orders"],
        "fct_order_items": bronze["order_items"] - drops["orphan_items"],
        "fct_payments": bronze["payments"] - drops["dup_payments"] - drops["orphan_payments"],
        "fct_refunds": bronze["refunds"] - drops["orphan_refunds"],
        "fct_web_events": bronze["web_events"],
        "fct_support_tickets": bronze["support_tickets"],
    }
    for table, exp in expected.items():
        check(f"rows: {table} = bronze - drops",
              counts[table] == exp,
              f"{counts[table]:,} vs expected {exp:,}")
    expected_days = scalar(
        "SELECT date_diff('day', (SELECT min(order_date) FROM iceberg.bronze.orders), "
        "(SELECT max(order_date) FROM iceberg.bronze.orders)) + 1"
    )
    check("rows: dim_order_dates = full date span (leap-aware)",
          counts["dim_order_dates"] == expected_days,
          f"{counts['dim_order_dates']:,} vs {expected_days:,}")
    print(f"        info: documented drops: {drops}")

    # 3. Quality assertions (all must be zero)
    zero_checks = [
        ("dim_customers: email collisions after dedup",
         "SELECT count(*) - count(DISTINCT email) FROM iceberg.silver.dim_customers"),
        ("dim_customers: null emails",
         "SELECT count(*) FROM iceberg.silver.dim_customers WHERE email IS NULL"),
        ("dim_customers: unnormalized emails (not lowercase/trimmed)",
         "SELECT count(*) FROM iceberg.silver.dim_customers "
         "WHERE email <> lower(trim(email))"),
        ("dim_customers: unnormalized marketing_channel",
         "SELECT count(*) FROM iceberg.silver.dim_customers "
         "WHERE marketing_channel <> lower(trim(marketing_channel))"),
        ("fct_orders: null customer_id",
         "SELECT count(*) FROM iceberg.silver.fct_orders WHERE customer_id IS NULL"),
        ("fct_orders: payment status conflicts (cancelled <=> failed)",
         "SELECT count(*) FROM iceberg.silver.fct_orders WHERE payment_status_conflict"),
        ("fct_orders: unnormalized status/channel",
         "SELECT count(*) FROM iceberg.silver.fct_orders "
         "WHERE order_status <> lower(trim(order_status)) "
         "OR channel <> lower(trim(channel))"),
        ("fct_order_items: line_total <> quantity * unit_price",
         "SELECT count(*) FROM iceberg.silver.fct_order_items WHERE line_total_mismatch"),
        ("fct_payments: amount <> order total",
         "SELECT count(*) FROM iceberg.silver.fct_payments WHERE amount_mismatch"),
        ("fct_refunds: negative refund lag",
         "SELECT count(*) FROM iceberg.silver.fct_refunds WHERE refund_lag_days < 0"),
        ("fct_support_tickets: linked order not owned by customer",
         "SELECT count(*) FROM iceberg.silver.fct_support_tickets "
         "WHERE has_linked_order AND NOT linked_order_valid"),
    ]
    for name, sql in zero_checks:
        n = scalar(sql)
        check(name + " == 0", n == 0, f"count: {n:,}")

    # 4. Money columns are decimal
    cur.execute(
        "SELECT table_name, column_name, data_type FROM iceberg.information_schema.columns "
        "WHERE table_catalog = 'iceberg' AND table_schema = 'silver'"
    )
    types = {(r[0], r[1]): r[2] for r in cur.fetchall()}
    bad = [
        (t, c)
        for t, c in MONEY_COLUMNS
        if "decimal" not in types.get((t, c), "").lower()
    ]
    check("all money columns are DECIMAL", not bad,
          "bad: " + ", ".join(f"{t}.{c}={types[(t, c)]}" for t, c in bad) if bad else "")

    # 5. Cross-layer invariant: GMV silver == bronze
    gmv_bronze = scalar(
        "SELECT sum(line_total) FROM iceberg.bronze.order_items"
    )
    gmv_silver = scalar(
        "SELECT sum(line_total) FROM iceberg.silver.fct_order_items"
    )
    check("GMV consistent bronze vs silver",
          abs(float(gmv_bronze) - float(gmv_silver)) < 0.01,
          f"bronze {float(gmv_bronze):,.2f} vs silver {float(gmv_silver):,.2f}")

    # Informational (reported, not asserted)
    full_ref = scalar("SELECT count(*) FROM iceberg.silver.fct_refunds WHERE is_full_refund")
    anon = scalar("SELECT count(*) FROM iceberg.silver.fct_web_events WHERE is_anonymous")
    linked = scalar("SELECT count(*) FROM iceberg.silver.fct_support_tickets "
                    "WHERE has_linked_order")
    print(f"        info: full refunds {full_ref:,}/{counts['fct_refunds']:,}, "
          f"anonymous events {anon:,}/{counts['fct_web_events']:,}, "
          f"linked tickets {linked:,}/{counts['fct_support_tickets']:,}")

    print("=" * 60)
    if failures:
        print(f"RESULT: FAIL - {len(failures)} check(s) failed: {failures}")
        return 1
    print("RESULT: PASS - silver layer verified")
    return 0


if __name__ == "__main__":
    sys.exit(main())
