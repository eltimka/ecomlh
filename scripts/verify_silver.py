#!/usr/bin/env python3
"""Verify the Phase 6 silver layer.

Checks (stack up, bronze + silver materialized):
  1. All 9 silver tables exist (3 dimensions + 6 facts).
  2. Row-count conformance vs bronze (dedup/filter effects explained;
     fct_web_events = distinct event_ids across batch + stream bronze,
     duplicates deduped; with a live producer running the check is
     bound-checked, since the fact was materialized at the last refresh).
  3. Quality assertions that must hold (violations counted, expect 0):
     email uniqueness, enum normalization, payment consistency,
     line-total arithmetic, refund lag >= 0, linked-ticket validity.
  4. Money columns are DECIMAL (via information_schema).
  5. Cross-layer invariant: GMV computed from silver == GMV from bronze.

Exit code 0 + "RESULT: PASS" when everything is green.
"""

from __future__ import annotations

import sys
import time

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

    # 2. Row-count conformance.
    #    Merged facts (web_events, orders, order_items, payments) = distinct
    #    PK across (batch + stream bronze) - documented drops. Dims and the
    #    other facts = bronze - drops. With a live producer running, the
    #    merged facts are bound-checked (batch distinct .. current union):
    #    the fact was materialized at the last refresh, which can predate
    #    live rows committed meanwhile.
    BZ = "iceberg.bronze"
    counts = {t: scalar(f"SELECT count(*) FROM iceberg.silver.{t}") for t in SILVER_TABLES}
    bronze = {
        "customers": scalar(f"SELECT count(*) FROM {BZ}.customers"),
        "products": scalar(f"SELECT count(*) FROM {BZ}.products"),
        "orders": scalar(f"SELECT count(*) FROM {BZ}.orders"),
        "order_items": scalar(f"SELECT count(*) FROM {BZ}.order_items"),
        "payments": scalar(f"SELECT count(*) FROM {BZ}.payments"),
        "refunds": scalar(f"SELECT count(*) FROM {BZ}.refunds"),
        "web_events": scalar(f"SELECT count(*) FROM {BZ}.web_events"),
        "support_tickets": scalar(f"SELECT count(*) FROM {BZ}.support_tickets"),
    }

    def union_distinct(col: str, batch_t: str, stream_t: str) -> int:
        return scalar(
            f"SELECT count(DISTINCT {col}) FROM "
            f"(SELECT {col} FROM {BZ}.{batch_t} UNION ALL SELECT {col} FROM {BZ}.{stream_t}) u"
        )

    # detect a running live producer (any stream table growing over 3s)
    stream_a = {k: scalar(f"SELECT count(*) FROM {BZ}.stream_{k}")
                for k in ("web_events", "orders", "order_items", "payments")}
    time.sleep(3)
    stream_b = {k: scalar(f"SELECT count(*) FROM {BZ}.stream_{k}")
                for k in stream_a}
    live = {k: stream_b[k] != stream_a[k] for k in stream_a}
    any_live = any(live.values())

    web_union = union_distinct("event_id", "web_events", "stream_web_events")
    orders_union = union_distinct("order_id", "orders", "stream_orders")
    items_union = union_distinct("order_item_id", "order_items", "stream_order_items")
    # fct_payments dedups to one row per order that exists in the merged orders
    pay_union = scalar(
        "SELECT count(DISTINCT order_id) FROM "
        "(SELECT order_id FROM iceberg.bronze.payments "
        "UNION ALL SELECT order_id FROM iceberg.bronze.stream_payments) pp "
        "WHERE order_id IN (SELECT order_id FROM iceberg.bronze.orders "
        "UNION SELECT order_id FROM iceberg.bronze.stream_orders)"
    )
    # drops computed over the MERGED (batch + stream) sources
    drops = {
        "dup_customers": bronze["customers"]
        - scalar(f"SELECT count(DISTINCT lower(email)) FROM {BZ}.customers"),
        "bad_prices": scalar(f"SELECT count(*) FROM {BZ}.products WHERE unit_price <= 0"),
        "orphan_orders": scalar(
            "SELECT count(DISTINCT order_id) FROM "
            "(SELECT order_id, customer_id FROM iceberg.bronze.orders "
            "UNION ALL SELECT order_id, customer_id FROM iceberg.bronze.stream_orders) o "
            "LEFT JOIN iceberg.bronze.customers c ON c.customer_id = o.customer_id "
            "WHERE c.customer_id IS NULL"),
        "orphan_items": scalar(
            "SELECT count(DISTINCT order_item_id) FROM "
            "(SELECT order_item_id, order_id, product_id FROM iceberg.bronze.order_items "
            "UNION ALL SELECT order_item_id, order_id, product_id "
            "FROM iceberg.bronze.stream_order_items) i "
            "WHERE NOT EXISTS (SELECT 1 FROM (SELECT order_id FROM iceberg.bronze.orders "
            "UNION ALL SELECT order_id FROM iceberg.bronze.stream_orders) oo "
            "WHERE oo.order_id = i.order_id) "
            "OR NOT EXISTS (SELECT 1 FROM iceberg.bronze.products p "
            "WHERE p.product_id = i.product_id)"),
        "orphan_refunds": scalar(
            "SELECT count(*) FROM iceberg.bronze.refunds r "
            "LEFT JOIN iceberg.bronze.orders o ON o.order_id = r.order_id "
            "WHERE o.order_id IS NULL"),
    }
    expected = {
        "dim_customers": bronze["customers"] - drops["dup_customers"],
        "dim_products": bronze["products"] - drops["bad_prices"],
        "fct_orders": orders_union - drops["orphan_orders"],
        "fct_order_items": items_union - drops["orphan_items"],
        "fct_payments": pay_union,
        "fct_refunds": bronze["refunds"] - drops["orphan_refunds"],
        "fct_web_events": web_union,
        "fct_support_tickets": bronze["support_tickets"],
    }
    # merged facts: (fact, live-key, batch-distinct floor, union-ceiling key)
    merged = {
        "fct_web_events": ("web_events", bronze["web_events"]),
        "fct_orders": ("orders", bronze["orders"]),
        "fct_order_items": ("order_items", bronze["order_items"]),
        "fct_payments": ("payments", bronze["payments"]),
    }
    for table, exp in expected.items():
        detail = f"{counts[table]:,} vs expected {exp:,}"
        ok = counts[table] == exp
        if table in merged:
            key, floor = merged[table]
            if live[key]:
                ok = floor <= counts[table] <= exp
                detail += (f" (live {key} active: bound between batch {floor:,} "
                            f"and current union {exp:,})")
        check(f"rows: {table} = bronze - drops", ok, detail)
    for table, (key, floor) in merged.items():
        check(f"rows: {table} is a superset of the batch source",
              counts[table] >= floor,
              f"{counts[table]:,} vs batch {floor:,}")
    expected_days = scalar(
        "SELECT date_diff('day', (SELECT min(order_date) FROM iceberg.bronze.orders), "
        "(SELECT max(order_date) FROM iceberg.bronze.orders)) + 1"
    )
    check("rows: dim_order_dates = full date span (leap-aware)",
          counts["dim_order_dates"] == expected_days,
          f"{counts['dim_order_dates']:,} vs {expected_days:,}")
    print(f"        info: documented drops: {drops}")
    for key in ("web_events", "orders", "order_items", "payments"):
        batch = bronze[key]
        now = stream_b[key]
        union = {"web_events": web_union, "orders": orders_union,
                 "order_items": items_union, "payments": pay_union}[key]
        deduped = batch + now - union
        print(f"        info: {key} merge: batch {batch:,} + stream {now:,} "
              f"-> {union:,} distinct ({deduped:,} deduped)"
              + (" [live]" if live[key] else ""))
    if any_live:
        print("        info: LIVE producer detected on:",
              [k for k, v in live.items() if v])

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

    # 5. Cross-layer invariant: GMV silver == merged bronze (batch + stream,
    #    deduped on order_item_id exactly as silver does - batch wins)
    gmv_bronze = scalar(
        "SELECT coalesce(sum(line_total), 0) FROM ("
        "SELECT line_total, row_number() OVER (PARTITION BY order_item_id "
        "ORDER BY src_rank) AS rn FROM ("
        "SELECT order_item_id, line_total, 1 AS src_rank FROM iceberg.bronze.order_items "
        "UNION ALL "
        "SELECT order_item_id, line_total, 2 AS src_rank FROM iceberg.bronze.stream_order_items"
        ") u) r WHERE rn = 1"
    )
    gmv_silver = scalar(
        "SELECT sum(line_total) FROM iceberg.silver.fct_order_items"
    )
    check("GMV consistent merged-bronze vs silver",
          abs(float(gmv_bronze) - float(gmv_silver)) < 0.01,
          f"merged bronze {float(gmv_bronze):,.2f} vs silver {float(gmv_silver):,.2f}")

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
