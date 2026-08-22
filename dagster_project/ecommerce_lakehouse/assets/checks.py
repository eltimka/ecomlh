"""Data quality & observability checks (Phase 8).

Declarative, SQL-based Dagster **asset checks** attached to the bronze,
silver and gold assets. Each spec is (asset, name, description, SQL,
condition, blocking); the factory below turns them into check functions
that run automatically after the asset's materialization.

Checks that read more than one table (referential integrity, the
5-view revenue invariant) are attached to the per-layer barrier asset
(``<layer>/__layer_gate__``), which depends on every table of the layer:
they then run only after all tables have committed, so they can never
read a sibling table inside its full-refresh drop window (a race the
parallel executor would otherwise schedule).

Check families (per PROJECT.md Phase 8):
- not-empty / uniqueness (PK per table)
- completeness: bronze row counts vs the Phase 4 manifest
- freshness: date columns cover the expected DATA_DATE_START..END range
- referential integrity: child FKs exist in the parent table
- business rules: silver violation flags are zero; gold score/segment
  domains; 5-view revenue invariant
- simple anomaly detection: |z| > 3 on monthly order counts
- stream merge (Phase 13): unbounded date range on the stream table and the
  merged fact, within-stream duplicates (at-least-once after a hard kill),
  cross-source duplicate event_ids (informational), stream FK validity,
  merged-count reconciliation
- stream health (Phase 15): consumer_lag - committed stream rows vs the
  Kafka topic end offset (detects a stopped/stuck Flink consumer)

``blocking=True`` on the critical checks (uniqueness, referential
integrity, manifest completeness): a failed blocking check stops
downstream assets from materializing on top of bad data.
"""

import json
import os
from datetime import date
from pathlib import Path

import dagster as dg
from dagster import AssetCheckExecutionContext

from ..resources.trino import TrinoResource
from .bronze import BRONZE_ASSETS_BY_NAME
from .factory import GATE_NAME
from .gold import GOLD_ASSETS_BY_NAME
from .silver import SILVER_ASSETS_BY_NAME

REPO_ROOT = Path(__file__).resolve().parents[3]
SYNTHETIC_DATA_DIR = Path(
    os.environ.get("SYNTHETIC_DATA_DIR", str(REPO_ROOT / "data" / "synthetic"))
)
DATA_DATE_START = date.fromisoformat(os.environ.get("DATA_DATE_START", "2024-01-01"))
DATA_DATE_END = date.fromisoformat(os.environ.get("DATA_DATE_END", "2024-12-31"))
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "raw.web_events")
KAFKA_TOPIC_ORDERS = os.environ.get("KAFKA_TOPIC_ORDERS", "raw.orders")
KAFKA_TOPIC_ORDER_ITEMS = os.environ.get("KAFKA_TOPIC_ORDER_ITEMS", "raw.order_items")
KAFKA_TOPIC_PAYMENTS = os.environ.get("KAFKA_TOPIC_PAYMENTS", "raw.payments")
# consumer_lag tolerance: a healthy Flink job commits within its 10s
# checkpoint interval (a few hundred rows at the default 20 events/s live
# rate); a stopped consumer drifts far past this within a minute.
LAG_TOLERANCE = int(os.environ.get("STREAM_LAG_TOLERANCE", "1000"))

ASSETS_BY_LAYER = {
    "bronze": BRONZE_ASSETS_BY_NAME,
    "silver": SILVER_ASSETS_BY_NAME,
    "gold": GOLD_ASSETS_BY_NAME,
}

B, S, G = "iceberg.bronze", "iceberg.silver", "iceberg.gold"


# --------------------------------------------------------------------------
# SQL builders
# --------------------------------------------------------------------------


def _count_sql(table: str) -> str:
    return f"SELECT count(*) FROM {table}"


def _unique_sql(table: str, col: str) -> str:
    return (
        f"SELECT count(*) FROM (SELECT {col} FROM {table} "
        f"GROUP BY {col} HAVING count(*) > 1) d"
    )


def _unique_composite_sql(table: str, cols: list[str]) -> str:
    cols_sql = ", ".join(cols)
    return f"SELECT count(*) FROM (SELECT {cols_sql} FROM {table} " \
           f"GROUP BY {cols_sql} HAVING count(*) > 1) d"


def _not_null_sql(table: str, col: str) -> str:
    return f"SELECT count(*) FROM {table} WHERE {col} IS NULL"


def _ref_sql(child: str, col: str, parent: str) -> str:
    """Count child rows whose FK has no matching parent PK (same-named col)."""
    return (
        f"SELECT count(*) FROM {child} c "
        f"LEFT JOIN {parent} p ON p.{col} = c.{col} "
        f"WHERE p.{col} IS NULL"
    )


def _range_sql(table: str, col: str) -> str:
    return f"SELECT min({col}), max({col}) FROM {table}"


# --------------------------------------------------------------------------
# Spec list
# --------------------------------------------------------------------------

_BZ_PK = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "payments": "payment_id",
    "refunds": "refund_id",
    "web_events": "event_id",
    "support_tickets": "ticket_id",
}
_SV_UNIQUE = {
    "dim_customers": "email",
    "dim_products": "product_id",
    "dim_order_dates": "order_date",
    "fct_orders": "order_id",
    "fct_order_items": "order_item_id",
    "fct_payments": "order_id",  # one payment per order
    "fct_refunds": "refund_id",
    "fct_web_events": "event_id",
    "fct_support_tickets": "ticket_id",
}
_SV_NOT_NULL = [
    ("dim_customers", "email"),
    ("dim_products", "unit_price"),
    ("fct_orders", "customer_id"),
    ("fct_orders", "total_amount"),
    ("fct_order_items", "line_total"),
    ("fct_refunds", "refund_amount"),
]
_SV_REFS = [
    ("fct_orders", "customer_id", f"{S}.dim_customers"),
    ("fct_order_items", "order_id", f"{S}.fct_orders"),
    ("fct_order_items", "product_id", f"{S}.dim_products"),
    ("fct_payments", "order_id", f"{S}.fct_orders"),
    ("fct_refunds", "order_id", f"{S}.fct_orders"),
]
_BZ_REFS = [
    ("order_items", "order_id", f"{B}.orders"),
    ("order_items", "product_id", f"{B}.products"),
    ("orders", "customer_id", f"{B}.customers"),
    ("payments", "order_id", f"{B}.orders"),
    ("refunds", "order_id", f"{B}.orders"),
]
_BZ_DATE_COLS = {
    # (table, col, range_kind, fresh_days)
    #   full        : min <= start AND max >= end   (dense tables, cover every day)
    #   refunds     : min >= start AND max >= end   (no upper bound - refunds
    #               legitimately lag the data range by up to 45 days)
    #   fresh       : max >= end - fresh_days       (sparse tables; only the
    #               recency of the newest rows matters)
    "orders": ("order_date", "full", 0),
    "payments": ("payment_date", "full", 0),
    "refunds": ("refund_date", "refunds", 0),
    "web_events": ("event_date", "full", 0),
    "support_tickets": ("ticket_date", "fresh", 7),
}

SPECS: list[dict] = []

# ---- bronze ---------------------------------------------------------------
for _t, _pk in _BZ_PK.items():
    SPECS.append({
        "layer": "bronze", "table": _t, "name": "not_empty",
        "description": f"bronze.{_t} is not empty",
        "sql": _count_sql(f"{B}.{_t}"), "condition": "positive", "blocking": False,
    })
    SPECS.append({
        "layer": "bronze", "table": _t, "name": "pk_unique",
        "description": f"bronze.{_t}: {_pk} is unique",
        "sql": _unique_sql(f"{B}.{_t}", _pk), "condition": "zero", "blocking": True,
    })
    SPECS.append({
        "layer": "bronze", "table": _t, "name": "manifest_rows",
        "description": f"bronze.{_t}: row count matches the Phase 4 manifest",
        "sql": _count_sql(f"{B}.{_t}"), "condition": "manifest", "blocking": True,
    })

for _t, (_col, _kind, _fresh) in _BZ_DATE_COLS.items():
    _kind_desc = {
        "full": f"covers the expected data range ({DATA_DATE_START} .. {DATA_DATE_END})",
        "refunds": (
            f"starts within the data range and is fresh up to {DATA_DATE_END} "
            "(no upper bound: refunds lag orders by up to 45 days)"
        ),
        "fresh": f"newest row within {_fresh} days of {DATA_DATE_END} (sparse table)",
    }[_kind]
    SPECS.append({
        "layer": "bronze", "table": _t, "name": "date_range",
        "description": f"bronze.{_t}: {_col} {_kind_desc}",
        "sql": _range_sql(f"{B}.{_t}", _col), "condition": "range_cover",
        "range_kind": _kind, "fresh_days": _fresh, "blocking": False,
    })

for _child, _col, _parent in _BZ_REFS:
    SPECS.append({
        "layer": "bronze", "table": GATE_NAME, "name": f"{_child}_refs_{_parent.rsplit('.', 1)[-1]}",
        "description": f"bronze.{_child}: every {_col} exists in {_parent}",
        "sql": _ref_sql(f"{B}.{_child}", _col, _parent),
        "condition": "zero", "blocking": True,
    })

# ---- Phase 13: stream bronze (bronze.stream_web_events, written by Flink) --
SPECS.append({
    "layer": "bronze", "table": "stream_web_events", "name": "date_range",
    "description": (
        f"bronze.stream_web_events: event_date starts within the history "
        f"window (>= {DATA_DATE_START}) and reaches at least {DATA_DATE_END} "
        "(live events may extend it)"
    ),
    "sql": _range_sql(f"{B}.stream_web_events", "event_date"),
    "condition": "range_cover", "range_kind": "stream", "blocking": False,
})
SPECS.append({
    "layer": "bronze", "table": "stream_web_events", "name": "event_id_unique",
    "description": (
        "bronze.stream_web_events: event_id is unique (with Phase 15 durable "
        "restarts, within-stream duplicates can only appear after an "
        "UNGRACEFUL kill mid-checkpoint; silver's dedup absorbs them - this "
        "check surfaces them)"
    ),
    "sql": _unique_sql(f"{B}.stream_web_events", "event_id"),
    "condition": "zero", "blocking": False,
})
SPECS.append({
    "layer": "bronze", "table": "stream_web_events", "name": "consumer_lag",
    "description": (
        f"bronze.stream_web_events vs Kafka topic '{KAFKA_TOPIC}': committed "
        f"rows are within +/-{LAG_TOLERANCE:,} of the topic end offset "
        "(a stopped or stuck Flink consumer drifts past the tolerance; "
        "transient checkpoint lag of a healthy job stays well inside it)"
    ),
    "sql": _count_sql(f"{B}.stream_web_events"),
    "condition": "kafka_lag", "topic": KAFKA_TOPIC,
    "lag_tolerance": LAG_TOLERANCE, "blocking": False,
})
SPECS.append({
    "layer": "bronze", "table": GATE_NAME, "name": "web_events_cross_source_dups",
    "description": (
        "bronze.web_events vs bronze.stream_web_events: event_ids arriving via "
        "BOTH sources (replay mode: the full history; live mode: the replayed "
        "prefix). Informational - silver dedups them."
    ),
    "sql": (
        "SELECT count(*) FROM (SELECT event_id FROM iceberg.bronze.web_events "
        "INTERSECT SELECT event_id FROM iceberg.bronze.stream_web_events) d"
    ),
    "condition": "info", "blocking": False,
})

# ---- Phase 15 item 4: money-path stream bronze (written by Flink) ----------
_MONEY_STREAMS = [
    # (table, pk, date_col or None, kafka topic)
    ("stream_orders", "order_id", "order_date", KAFKA_TOPIC_ORDERS),
    ("stream_order_items", "order_item_id", None, KAFKA_TOPIC_ORDER_ITEMS),
    ("stream_payments", "payment_id", "payment_date", KAFKA_TOPIC_PAYMENTS),
]
for _tbl, _pk, _dcol, _topic in _MONEY_STREAMS:
    SPECS.append({
        "layer": "bronze", "table": _tbl, "name": f"{_pk}_unique",
        "description": (
            f"bronze.{_tbl}: {_pk} is unique (with Phase 15 durable restarts, "
            "within-stream duplicates can only appear after an UNGRACEFUL kill "
            "mid-checkpoint; silver's dedup absorbs them - surfaced here)"
        ),
        "sql": _unique_sql(f"{B}.{_tbl}", _pk), "condition": "zero", "blocking": False,
    })
    SPECS.append({
        "layer": "bronze", "table": _tbl, "name": "consumer_lag",
        "description": (
            f"bronze.{_tbl} vs Kafka topic '{_topic}': committed rows are "
            f"within +/-{LAG_TOLERANCE:,} of the topic end offset (a stopped "
            "or stuck Flink consumer drifts past the tolerance)"
        ),
        "sql": _count_sql(f"{B}.{_tbl}"), "condition": "kafka_lag",
        "topic": _topic, "lag_tolerance": LAG_TOLERANCE, "blocking": False,
    })
for _tbl, _dcol in (("stream_orders", "order_date"), ("stream_payments", "payment_date")):
    SPECS.append({
        "layer": "bronze", "table": _tbl, "name": "date_range",
        "description": (
            f"bronze.{_tbl}: {_dcol} starts within the history window "
            f"(>= {DATA_DATE_START}) and reaches at least {DATA_DATE_END} "
            "(live rows may extend it)"
        ),
        "sql": _range_sql(f"{B}.{_tbl}", _dcol),
        "condition": "range_cover", "range_kind": "stream", "blocking": False,
    })
for _bt, _st, _col in (
    ("orders", "stream_orders", "order_id"),
    ("order_items", "stream_order_items", "order_item_id"),
    ("payments", "stream_payments", "payment_id"),
):
    SPECS.append({
        "layer": "bronze", "table": GATE_NAME, "name": f"{_bt}_cross_source_dups",
        "description": (
            f"bronze.{_bt} vs bronze.{_st}: {_col}s arriving via BOTH sources "
            "(replay mode: the full history; live mode: the replayed prefix). "
            "Informational - silver dedups them."
        ),
        "sql": (
            f"SELECT count(*) FROM (SELECT {_col} FROM {B}.{_bt} "
            f"INTERSECT SELECT {_col} FROM {B}.{_st}) d"
        ),
        "condition": "info", "blocking": False,
    })

# ---- silver ---------------------------------------------------------------
for _t in _SV_UNIQUE:
    SPECS.append({
        "layer": "silver", "table": _t, "name": "not_empty",
        "description": f"silver.{_t} is not empty",
        "sql": _count_sql(f"{S}.{_t}"), "condition": "positive", "blocking": True,
    })
    SPECS.append({
        "layer": "silver", "table": _t, "name": f"unique_{_SV_UNIQUE[_t]}",
        "description": f"silver.{_t}: {_SV_UNIQUE[_t]} is unique",
        "sql": _unique_sql(f"{S}.{_t}", _SV_UNIQUE[_t]),
        "condition": "zero", "blocking": True,
    })

for _t, _col in _SV_NOT_NULL:
    SPECS.append({
        "layer": "silver", "table": _t, "name": f"not_null_{_col}",
        "description": f"silver.{_t}: {_col} is never null",
        "sql": _not_null_sql(f"{S}.{_t}", _col), "condition": "zero",
        "blocking": False,
    })

for _child, _col, _parent in _SV_REFS:
    SPECS.append({
        "layer": "silver", "table": GATE_NAME, "name": f"{_child}_refs_{_parent.rsplit('.', 1)[-1]}",
        "description": f"silver.{_child}: every {_col} exists in {_parent}",
        "sql": _ref_sql(f"{S}.{_child}", _col, _parent),
        "condition": "zero", "blocking": True,
    })

_SV_RULES = [
    ("fct_orders", "payment_status_conflict",
     "silver.fct_orders: cancelled<=>failed payment rule holds",
     "SELECT count(*) FROM iceberg.silver.fct_orders WHERE payment_status_conflict"),
    ("fct_order_items", "line_total_integrity",
     "silver.fct_order_items: line_total = quantity * unit_price everywhere",
     "SELECT count(*) FROM iceberg.silver.fct_order_items WHERE line_total_mismatch"),
    ("fct_payments", "amount_matches_order",
     "silver.fct_payments: payment amount equals order total",
     "SELECT count(*) FROM iceberg.silver.fct_payments WHERE amount_mismatch"),
    ("fct_refunds", "non_negative_lag",
     "silver.fct_refunds: refund never precedes its order",
     "SELECT count(*) FROM iceberg.silver.fct_refunds WHERE refund_lag_days < 0"),
    ("fct_support_tickets", "linked_order_owned_by_customer",
     "silver.fct_support_tickets: linked orders belong to the ticket's customer",
     "SELECT count(*) FROM iceberg.silver.fct_support_tickets "
     "WHERE has_linked_order AND NOT linked_order_valid"),
]
for _t, _n, _d, _sql in _SV_RULES:
    SPECS.append({
        "layer": "silver", "table": _t, "name": f"rule_{_n}",
        "description": _d, "sql": _sql, "condition": "zero", "blocking": False,
    })

SPECS.append({
    "layer": "silver", "table": "fct_orders", "name": "date_range",
    "description": (
        f"silver.fct_orders: order_date (merged batch + stream) starts within "
        f"the history window (>= {DATA_DATE_START}) and reaches at least "
        f"{DATA_DATE_END} (live orders may extend it)"
    ),
    "sql": _range_sql(f"{S}.fct_orders", "order_date"),
    "condition": "range_cover", "range_kind": "stream", "blocking": False,
})

# ---- Phase 13: silver merge of batch + stream web events ------------------
SPECS.append({
    "layer": "silver", "table": "fct_web_events", "name": "date_range",
    "description": (
        f"silver.fct_web_events: event_date (merged batch + stream) starts "
        f"within the history window (>= {DATA_DATE_START}) and reaches at "
        f"least {DATA_DATE_END} (live events may extend it)"
    ),
    "sql": _range_sql(f"{S}.fct_web_events", "event_date"),
    "condition": "range_cover", "range_kind": "stream", "blocking": False,
})
SPECS.append({
    "layer": "silver", "table": GATE_NAME, "name": "stream_customers_fk",
    "description": (
        "bronze.stream_web_events: every non-null customer_id exists in "
        "silver.dim_customers (anonymous events are null by design)"
    ),
    "sql": (
        "SELECT count(*) FROM iceberg.bronze.stream_web_events c "
        "LEFT JOIN iceberg.silver.dim_customers p "
        "ON p.customer_id = c.customer_id "
        "WHERE c.customer_id IS NOT NULL AND p.customer_id IS NULL"
    ),
    "condition": "zero", "blocking": False,
})
SPECS.append({
    "layer": "silver", "table": GATE_NAME, "name": "web_events_merge_reconcile",
    "description": (
        "silver.fct_web_events: row count equals the distinct event_ids "
        "across both bronze sources. Exact while the topic is stable; with a "
        "live producer, rows can land between the CTAS and this check"
    ),
    "sql": (
        "SELECT (SELECT count(*) FROM iceberg.silver.fct_web_events) "
        "= (SELECT count(DISTINCT event_id) FROM ("
        "SELECT event_id FROM iceberg.bronze.web_events "
        "UNION ALL "
        "SELECT event_id FROM iceberg.bronze.stream_web_events) u)"
    ),
    "condition": "boolean_true", "blocking": False,
})

# ---- Phase 15 item 4: silver merge of batch + stream money path -----------
SPECS.append({
    "layer": "silver", "table": "fct_payments", "name": "date_range",
    "description": (
        f"silver.fct_payments: payment_date (merged batch + stream) starts "
        f"within the history window (>= {DATA_DATE_START}) and reaches at "
        f"least {DATA_DATE_END} (live rows may extend it)"
    ),
    "sql": _range_sql(f"{S}.fct_payments", "payment_date"),
    "condition": "range_cover", "range_kind": "stream", "blocking": False,
})
SPECS.append({
    "layer": "silver", "table": GATE_NAME, "name": "stream_orders_customers_fk",
    "description": (
        "bronze.stream_orders: every customer_id exists in silver.dim_customers"
    ),
    "sql": (
        "SELECT count(*) FROM iceberg.bronze.stream_orders c "
        "LEFT JOIN iceberg.silver.dim_customers p "
        "ON p.customer_id = c.customer_id WHERE p.customer_id IS NULL"
    ),
    "condition": "zero", "blocking": False,
})
SPECS.append({
    "layer": "silver", "table": GATE_NAME, "name": "stream_order_items_products_fk",
    "description": (
        "bronze.stream_order_items: every product_id exists in silver.dim_products"
    ),
    "sql": (
        "SELECT count(*) FROM iceberg.bronze.stream_order_items c "
        "LEFT JOIN iceberg.silver.dim_products p "
        "ON p.product_id = c.product_id WHERE p.product_id IS NULL"
    ),
    "condition": "zero", "blocking": False,
})
for _silver, _batch, _stream, _col in (
    ("fct_orders", "orders", "stream_orders", "order_id"),
    ("fct_order_items", "order_items", "stream_order_items", "order_item_id"),
    ("fct_payments", "payments", "stream_payments", "payment_id"),
):
    SPECS.append({
        "layer": "silver", "table": GATE_NAME, "name": f"{_silver}_merge_reconcile",
        "description": (
            f"silver.{_silver}: row count equals the distinct {_col} across "
            "both bronze sources. Exact while the topic is stable; with a "
            "live producer rows can land between the CTAS and this check."
        ),
        "sql": (
            f"SELECT (SELECT count(*) FROM {S}.{_silver}) "
            f"= (SELECT count(DISTINCT {_col}) FROM "
            f"(SELECT {_col} FROM {B}.{_batch} "
            f"UNION ALL SELECT {_col} FROM {B}.{_stream}) u)"
        ),
        "condition": "boolean_true", "blocking": False,
    })

# ---- gold -----------------------------------------------------------------
for _t in ("customer_360", "revenue_by_channel", "revenue_by_category", "monthly_kpis"):
    SPECS.append({
        "layer": "gold", "table": _t, "name": "not_empty",
        "description": f"gold.{_t} is not empty",
        "sql": _count_sql(f"{G}.{_t}"), "condition": "positive", "blocking": True,
    })

SPECS += [
    {
        "layer": "gold", "table": "customer_360", "name": "customer_id_unique",
        "description": "gold.customer_360: one row per customer (customer_id unique)",
        "sql": _unique_sql(f"{G}.customer_360", "customer_id"),
        "condition": "zero", "blocking": True,
    },
    {
        "layer": "gold", "table": GATE_NAME, "name": "one_row_per_customer",
        "description": "gold.customer_360: row count equals silver.dim_customers",
        "sql": (
            "SELECT (SELECT count(*) FROM iceberg.gold.customer_360) "
            "= (SELECT count(*) FROM iceberg.silver.dim_customers)"
        ),
        "condition": "boolean_true", "blocking": True,
    },
    {
        "layer": "gold", "table": "customer_360", "name": "rfm_score_range",
        "description": "gold.customer_360: RFM scores are 1..5 for ordering customers",
        "sql": (
            "SELECT count(*) FROM iceberg.gold.customer_360 WHERE total_orders > 0 "
            "AND (r_score IS NULL OR f_score IS NULL OR m_score IS NULL "
            "  OR r_score NOT BETWEEN 1 AND 5 OR f_score NOT BETWEEN 1 AND 5 "
            "  OR m_score NOT BETWEEN 1 AND 5)"
        ),
        "condition": "zero", "blocking": False,
    },
    {
        "layer": "gold", "table": "customer_360", "name": "segment_domain",
        "description": "gold.customer_360: rfm_segment is a known label",
        "sql": (
            "SELECT count(*) FROM iceberg.gold.customer_360 WHERE rfm_segment NOT IN "
            "('no_orders', 'champion', 'loyal', 'new_or_returning', 'lost', "
            "'hibernating', 'potential_loyalist', 'needs_attention')"
        ),
        "condition": "zero", "blocking": False,
    },
    {
        "layer": "gold", "table": "customer_360", "name": "churn_domain",
        "description": "gold.customer_360: churn_risk is a known level",
        "sql": (
            "SELECT count(*) FROM iceberg.gold.customer_360 "
            "WHERE churn_risk NOT IN ('none', 'low', 'medium', 'high')"
        ),
        "condition": "zero", "blocking": False,
    },
    {
        "layer": "gold", "table": GATE_NAME, "name": "revenue_invariant",
        "description": (
            "gross revenue agrees across silver.fct_orders, customer_360, "
            "revenue_by_channel, revenue_by_category and monthly_kpis (spread < $0.05)"
        ),
        "sql": (
            "SELECT (SELECT coalesce(sum(total_amount) FILTER (WHERE is_cancelled = false), 0) "
            "         FROM iceberg.silver.fct_orders), "
            "       (SELECT coalesce(sum(gross_revenue), 0) FROM iceberg.gold.customer_360), "
            "       (SELECT coalesce(sum(gross_revenue), 0) FROM iceberg.gold.revenue_by_channel), "
            "       (SELECT coalesce(sum(gross_revenue), 0) FROM iceberg.gold.revenue_by_category), "
            "       (SELECT coalesce(sum(gross_revenue), 0) FROM iceberg.gold.monthly_kpis)"
        ),
        "condition": "gmv_spread", "blocking": True,
    },
    {
        "layer": "gold", "table": "monthly_kpis", "name": "freshness",
        "description": (
            f"gold.monthly_kpis: latest month is at least the data range end "
            f"month ({DATA_DATE_END.year}-{DATA_DATE_END.month:02d}); a live "
            "producer may push it further out"
        ),
        "sql": f"SELECT max(month) FROM {G}.monthly_kpis",
        "condition": "max_month", "blocking": False,
    },
    {
        "layer": "gold", "table": "monthly_kpis", "name": "orders_reconcile",
        "description": "gold.monthly_kpis: sum(orders) equals silver.fct_orders row count",
        "sql": (
            "SELECT (SELECT sum(orders) FROM iceberg.gold.monthly_kpis) "
            "= (SELECT count(*) FROM iceberg.silver.fct_orders)"
        ),
        "condition": "boolean_true", "blocking": False,
    },
    {
        "layer": "gold", "table": "monthly_kpis", "name": "new_customers_reconcile",
        "description": (
            "gold.monthly_kpis: sum(new_customers) equals distinct ordering customers"
        ),
        "sql": (
            "SELECT (SELECT sum(new_customers) FROM iceberg.gold.monthly_kpis) "
            "= (SELECT count(DISTINCT customer_id) FROM iceberg.silver.fct_orders)"
        ),
        "condition": "boolean_true", "blocking": False,
    },
    {
        "layer": "gold", "table": "monthly_kpis", "name": "monthly_anomaly",
        "description": (
            "simple anomaly detection over the historical months only "
            f"(<= {DATA_DATE_END.year}-{DATA_DATE_END.month:02d}): no month's "
            "order count deviates > 3 sigma from the mean. Any live tail month "
            "is excluded - it is partial and would always look anomalous."
        ),
        "sql": (
            "SELECT coalesce(max(abs(z)), 0) FROM ("
            "SELECT (orders - avg(orders) OVER ()) / stddev(orders) OVER () AS z "
            f"FROM iceberg.gold.monthly_kpis "
            f"WHERE month <= date '{date(DATA_DATE_END.year, DATA_DATE_END.month, 1).isoformat()}') t"
        ),
        "condition": "z_lt_3", "blocking": False,
    },
    {
        "layer": "gold", "table": "revenue_by_channel", "name": "month_channel_unique",
        "description": "gold.revenue_by_channel: (month, channel) is unique",
        "sql": _unique_composite_sql(f"{G}.revenue_by_channel", ["month", "channel"]),
        "condition": "zero", "blocking": False,
    },
    {
        "layer": "gold", "table": "revenue_by_category", "name": "month_category_unique",
        "description": "gold.revenue_by_category: (month, category) is unique",
        "sql": _unique_composite_sql(f"{G}.revenue_by_category", ["month", "category"]),
        "condition": "zero", "blocking": False,
    },
]


# --------------------------------------------------------------------------
# Condition evaluators
# --------------------------------------------------------------------------


def _manifest_rows(table: str) -> int:
    manifest = json.loads((SYNTHETIC_DATA_DIR / "manifest.json").read_text())
    return int(manifest["tables"][table]["rows"])


def _kafka_end_offset(topic: str) -> int | None:
    """Total end offset across all partitions of the topic (None if the
    broker is unreachable or the topic is missing)."""
    try:
        from kafka import KafkaConsumer
        from kafka.structs import TopicPartition

        consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP,
                                 api_version=(3, 0, 0), request_timeout_ms=8000)
        try:
            parts = consumer.partitions_for_topic(topic) or set()
            if not parts:
                return None
            tps = [TopicPartition(topic, p) for p in parts]
            return sum(consumer.end_offsets(tps).values())
        finally:
            consumer.close()
    except Exception:  # noqa: BLE001 - broker down / topic missing
        return None


def _evaluate(spec: dict, row: tuple) -> tuple[bool, str]:
    condition = spec["condition"]
    if condition == "zero":
        n = int(row[0])
        return n == 0, f"violations: {n:,}"
    if condition == "positive":
        n = int(row[0])
        return n > 0, f"rows: {n:,}"
    if condition == "manifest":
        n, expected = int(row[0]), _manifest_rows(spec["table"])
        return n == expected, f"actual {n:,} vs manifest {expected:,}"
    if condition == "range_cover":
        mn, mx = row
        if mn is None or mx is None:
            return False, "table is empty (no committed rows yet)"
        kind = spec.get("range_kind", "full")
        if kind == "full":
            ok = mn <= DATA_DATE_START and mx >= DATA_DATE_END
        elif kind == "refunds":
            ok = mn >= DATA_DATE_START and mx >= DATA_DATE_END
        elif kind == "stream":
            # like refunds, but named for its purpose: the lower bound keeps
            # the history window, and there is NO upper bound because live
            # events legitimately extend the range beyond DATA_DATE_END
            ok = mn >= DATA_DATE_START and mx >= DATA_DATE_END
        elif kind == "fresh":
            from datetime import timedelta

            ok = mx >= DATA_DATE_END - timedelta(days=spec.get("fresh_days", 7))
        else:
            raise ValueError(f"Unknown range_kind: {kind}")
        return ok, f"range {mn} .. {mx} (kind={kind}, expected {DATA_DATE_START} .. {DATA_DATE_END})"
    if condition == "info":
        # informational: always passes, the count is reported in the detail
        n = int(row[0])
        return True, f"count: {n:,}"
    if condition == "kafka_lag":
        # row[0] = rows committed to the stream bronze; compare against the
        # Kafka topic end offset. lag = end_offset - committed; a healthy,
        # caught-up job sits at 0 (checkpoint interval 10s), a stopped
        # consumer drifts positive, an over-appended table drifts negative.
        committed = int(row[0])
        end = _kafka_end_offset(spec.get("topic", KAFKA_TOPIC))
        if end is None:
            return False, f"kafka unreachable or topic '{spec.get('topic', KAFKA_TOPIC)}' missing (cannot measure lag)"
        lag = end - committed
        tol = int(spec.get("lag_tolerance", LAG_TOLERANCE))
        return abs(lag) <= tol, (
            f"lag {lag:+,} (topic end_offset {end:,}, committed {committed:,}, "
            f"tolerance +/-{tol:,})"
        )
    if condition == "boolean_true":
        return bool(row[0]), "reconciled" if row[0] else "mismatch"
    if condition == "gmv_spread":
        values = [float(v) for v in row]
        spread = max(values) - min(values)
        return spread < 0.05, f"spread {spread:.4f} across 5 views"
    if condition == "z_lt_3":
        z = float(row[0])
        return z < 3, f"max |z| = {z:.2f}"
    if condition == "max_month":
        mx = row[0]
        expected = date(DATA_DATE_END.year, DATA_DATE_END.month, 1)
        return mx >= expected, f"latest month {mx} >= {expected} (live-aware)"
    raise ValueError(f"Unknown check condition: {condition}")


# --------------------------------------------------------------------------
# Check factory
# --------------------------------------------------------------------------


def _make_check(spec: dict):
    asset = ASSETS_BY_LAYER[spec["layer"]][spec["table"]]

    @dg.asset_check(
        asset=asset,
        name=spec["name"],
        description=spec["description"],
        blocking=spec["blocking"],
    )
    def check(context: AssetCheckExecutionContext, trino: TrinoResource) -> dg.AssetCheckResult:
        (row,) = trino.fetch(spec["sql"])
        passed, detail = _evaluate(spec, row)
        return dg.AssetCheckResult(passed=passed, description=detail)

    return check


asset_checks = [_make_check(spec) for spec in SPECS]
