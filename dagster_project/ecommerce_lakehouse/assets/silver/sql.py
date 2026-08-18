"""Silver layer transformation specs (Phase 6).

Each spec drives one silver asset (see __init__.py factory):

- ``deps`` / ``silver_deps``: upstream asset names (bronze / silver) for
  Dagster lineage.
- ``ctas``: full-refresh ``CREATE TABLE iceberg.silver.<name> AS ...``
- ``pre_stats`` / ``post_stats``: (label, scalar SQL) pairs reported as
  run metadata, so every transformation is *measurable* (rows in/out,
  duplicates removed, orphans dropped, business-rule violations).

Design rules:
- Dimensions: deduplicated on their natural key, standardized enums
  (lower(trim(...))), proper nouns trimmed, money as DECIMAL(12,2).
- Facts: inner-joined to dimensions where referential integrity is a
  business guarantee (orphans are dropped and counted), enriched with
  derived flags / metrics. Full-refresh, idempotent.
- No data is fabricated: standardizations that are no-ops on the current
  dataset still guard against upstream drift.
"""

SILVER_SPECS: list[dict] = [
    # ------------------------------------------------------------------ dims
    {
        "name": "dim_customers",
        "table": "iceberg.silver.dim_customers",
        "deps": ["customers"],
        "description": (
            "Clean customer dimension: standardized names/email/channel, "
            "deduplicated on email (earliest signup wins)."
        ),
        "ctas": """
            CREATE TABLE iceberg.silver.dim_customers {location_props} AS
            SELECT customer_id, first_name, last_name, email, signup_date,
                   country, city, marketing_channel, age
            FROM (
                SELECT customer_id,
                       trim(first_name)          AS first_name,
                       trim(last_name)           AS last_name,
                       lower(trim(email))        AS email,
                       signup_date,
                       trim(country)             AS country,
                       trim(city)                AS city,
                       lower(trim(marketing_channel)) AS marketing_channel,
                       age,
                       row_number() OVER (
                           PARTITION BY lower(trim(email))
                           ORDER BY signup_date, customer_id
                       ) AS rn
                FROM iceberg.bronze.customers
            ) t
            WHERE rn = 1
        """,
        "pre_stats": [
            ("bronze_rows", "SELECT count(*) FROM iceberg.bronze.customers"),
            (
                "duplicate_emails",
                "SELECT count(*) - count(DISTINCT lower(trim(email))) "
                "FROM iceberg.bronze.customers",
            ),
        ],
        "post_stats": [
            (
                "email_collisions_after",
                "SELECT count(*) - count(DISTINCT email) "
                "FROM iceberg.silver.dim_customers",
            ),
            (
                "null_emails",
                "SELECT count(*) FROM iceberg.silver.dim_customers "
                "WHERE email IS NULL",
            ),
        ],
    },
    {
        "name": "dim_products",
        "table": "iceberg.silver.dim_products",
        "deps": ["products"],
        "description": (
            "Clean product dimension: standardized names, unit price as "
            "DECIMAL(12,2), non-positive prices dropped."
        ),
        "ctas": """
            CREATE TABLE iceberg.silver.dim_products {location_props} AS
            SELECT product_id,
                   trim(product_name)            AS product_name,
                   trim(category)                AS category,
                   trim(brand)                   AS brand,
                   CAST(unit_price AS DECIMAL(12, 2)) AS unit_price
            FROM iceberg.bronze.products
            WHERE unit_price > 0
        """,
        "pre_stats": [
            ("bronze_rows", "SELECT count(*) FROM iceberg.bronze.products"),
            (
                "non_positive_prices",
                "SELECT count(*) FROM iceberg.bronze.products "
                "WHERE unit_price <= 0",
            ),
        ],
        "post_stats": [],
    },
    {
        "name": "dim_order_dates",
        "table": "iceberg.silver.dim_order_dates",
        "deps": ["orders"],
        "description": (
            "Calendar dimension spanning the observed order date range, with "
            "year/month/quarter/weekday and a holiday-season flag."
        ),
        "ctas": """
            CREATE TABLE iceberg.silver.dim_order_dates {location_props} AS
            SELECT d                                       AS order_date,
                   year(d)                                 AS year,
                   month(d)                                AS month,
                   quarter(d)                              AS quarter,
                   day_of_week(d)                          AS day_of_week,
                   ELEMENT_AT(ARRAY['Sunday', 'Monday', 'Tuesday', 'Wednesday',
                                     'Thursday', 'Friday', 'Saturday'],
                              day_of_week(d))               AS day_of_week_name,
                   month(d) IN (11, 12)                     AS is_holiday_season
            FROM UNNEST(SEQUENCE(
                (SELECT min(order_date) FROM iceberg.bronze.orders),
                (SELECT max(order_date) FROM iceberg.bronze.orders)
            )) AS t (d)
        """,
        "pre_stats": [
            (
                "expected_days",
                "SELECT date_diff('day', "
                "(SELECT min(order_date) FROM iceberg.bronze.orders), "
                "(SELECT max(order_date) FROM iceberg.bronze.orders)) + 1",
            ),
        ],
        "post_stats": [],
    },
    # ----------------------------------------------------------------- facts
    {
        "name": "fct_orders",
        "table": "iceberg.silver.fct_orders",
        "deps": ["orders", "customers", "order_items", "payments"],
        "description": (
            "Order fact: one row per order, validated against the customer "
            "dimension (orphans dropped), enriched with item counts, status "
            "flags and a payment-consistency flag (cancelled <=> failed)."
        ),
        "ctas": """
            CREATE TABLE iceberg.silver.fct_orders {location_props} AS
            SELECT o.order_id,
                   o.customer_id,
                   o.order_date,
                   lower(trim(o.order_status))   AS order_status,
                   lower(trim(o.channel))        AS channel,
                   CAST(o.total_amount AS DECIMAL(12, 2)) AS total_amount,
                   COALESCE(it.item_count, 0)    AS item_count,
                   COALESCE(it.distinct_products, 0) AS distinct_product_count,
                   lower(trim(o.order_status)) = 'completed' AS is_completed,
                   lower(trim(o.order_status)) = 'returned'  AS is_returned,
                   lower(trim(o.order_status)) = 'cancelled' AS is_cancelled,
                   p.payment_status IS NOT NULL            AS has_payment,
                   p.payment_status = 'succeeded'          AS is_payment_succeeded,
                   (lower(trim(o.order_status)) = 'cancelled')
                       <> (p.payment_status = 'failed')    AS payment_status_conflict
            FROM iceberg.bronze.orders o
            JOIN iceberg.bronze.customers c
                 ON c.customer_id = o.customer_id
            LEFT JOIN (
                SELECT order_id,
                       count(*)                   AS item_count,
                       count(DISTINCT product_id) AS distinct_products
                FROM iceberg.bronze.order_items
                GROUP BY order_id
            ) it ON it.order_id = o.order_id
            LEFT JOIN (
                SELECT order_id, lower(trim(payment_status)) AS payment_status
                FROM (
                    SELECT payment_id, order_id, payment_status,
                           row_number() OVER (
                               PARTITION BY order_id ORDER BY payment_id
                           ) AS rn
                    FROM iceberg.bronze.payments
                ) p
                WHERE rn = 1
            ) p ON p.order_id = o.order_id
        """,
        "pre_stats": [
            ("bronze_rows", "SELECT count(*) FROM iceberg.bronze.orders"),
            (
                "orphan_orders",
                "SELECT count(*) FROM iceberg.bronze.orders o "
                "LEFT JOIN iceberg.bronze.customers c "
                "       ON c.customer_id = o.customer_id "
                "WHERE c.customer_id IS NULL",
            ),
        ],
        "post_stats": [
            (
                "payment_conflicts",
                "SELECT count(*) FROM iceberg.silver.fct_orders "
                "WHERE payment_status_conflict",
            ),
            (
                "orders_without_payment",
                "SELECT count(*) FROM iceberg.silver.fct_orders "
                "WHERE has_payment = false",
            ),
        ],
    },
    {
        "name": "fct_order_items",
        "table": "iceberg.silver.fct_order_items",
        "deps": ["order_items", "orders"],
        "silver_deps": ["dim_products"],
        "description": (
            "Order-item fact: validated against orders and dim_products "
            "(orphans dropped), DECIMAL money, denormalized product "
            "attributes, and a qty*price vs line_total integrity flag."
        ),
        "ctas": """
            CREATE TABLE iceberg.silver.fct_order_items {location_props} AS
            SELECT i.order_item_id,
                   i.order_id,
                   i.product_id,
                   p.product_name,
                   p.category,
                   p.brand,
                   i.quantity,
                   CAST(i.unit_price AS DECIMAL(12, 2)) AS unit_price,
                   CAST(i.line_total AS DECIMAL(12, 2)) AS line_total,
                   CAST(i.quantity * i.unit_price AS DECIMAL(12, 2))
                       <> CAST(i.line_total AS DECIMAL(12, 2))
                                                                  AS line_total_mismatch
            FROM iceberg.bronze.order_items i
            JOIN iceberg.bronze.orders o
                 ON o.order_id = i.order_id
            JOIN iceberg.silver.dim_products p
                 ON p.product_id = i.product_id
        """,
        "pre_stats": [
            ("bronze_rows", "SELECT count(*) FROM iceberg.bronze.order_items"),
            (
                "orphan_items",
                "SELECT count(*) FROM iceberg.bronze.order_items i "
                "WHERE NOT EXISTS (SELECT 1 FROM iceberg.bronze.orders o "
                "                   WHERE o.order_id = i.order_id) "
                "   OR NOT EXISTS (SELECT 1 FROM iceberg.bronze.products p "
                "                   WHERE p.product_id = i.product_id)",
            ),
        ],
        "post_stats": [
            (
                "line_total_mismatches",
                "SELECT count(*) FROM iceberg.silver.fct_order_items "
                "WHERE line_total_mismatch",
            ),
        ],
    },
    {
        "name": "fct_payments",
        "table": "iceberg.silver.fct_payments",
        "deps": ["payments", "orders"],
        "description": (
            "Payment fact: deduplicated to one row per order (latest payment "
            "id), validated against orders, DECIMAL amounts, status flags and "
            "an amount-vs-order-total mismatch flag."
        ),
        "ctas": """
            CREATE TABLE iceberg.silver.fct_payments {location_props} AS
            SELECT p.payment_id,
                   p.order_id,
                   o.customer_id,
                   lower(trim(p.payment_method))   AS payment_method,
                   lower(trim(p.payment_status))   AS payment_status,
                   p.payment_date,
                   CAST(p.amount AS DECIMAL(12, 2)) AS amount,
                   lower(trim(p.payment_status)) = 'succeeded' AS is_succeeded,
                   lower(trim(p.payment_status)) = 'failed'    AS is_failed,
                   lower(trim(p.payment_status)) = 'pending'   AS is_pending,
                   CAST(p.amount AS DECIMAL(12, 2))
                       <> CAST(o.total_amount AS DECIMAL(12, 2))
                                                                  AS amount_mismatch
            FROM (
                SELECT payment_id, order_id, payment_method, payment_status,
                       payment_date, amount,
                       row_number() OVER (
                           PARTITION BY order_id ORDER BY payment_id
                       ) AS rn
                FROM iceberg.bronze.payments
            ) p
            JOIN iceberg.bronze.orders o
                 ON o.order_id = p.order_id
            WHERE p.rn = 1
        """,
        "pre_stats": [
            ("bronze_rows", "SELECT count(*) FROM iceberg.bronze.payments"),
            (
                "duplicate_payments",
                "SELECT count(*) - count(DISTINCT order_id) "
                "FROM iceberg.bronze.payments",
            ),
        ],
        "post_stats": [
            (
                "amount_mismatches",
                "SELECT count(*) FROM iceberg.silver.fct_payments "
                "WHERE amount_mismatch",
            ),
            (
                "succeeded",
                "SELECT count(*) FROM iceberg.silver.fct_payments "
                "WHERE is_succeeded",
            ),
            (
                "failed",
                "SELECT count(*) FROM iceberg.silver.fct_payments "
                "WHERE is_failed",
            ),
            (
                "pending",
                "SELECT count(*) FROM iceberg.silver.fct_payments "
                "WHERE is_pending",
            ),
        ],
    },
    {
        "name": "fct_refunds",
        "table": "iceberg.silver.fct_refunds",
        "deps": ["refunds", "orders"],
        "description": (
            "Refund fact: validated against orders, DECIMAL amounts, refund "
            "lag in days (refund_date - order_date), and a full-vs-partial "
            "flag derived from the order total."
        ),
        "ctas": """
            CREATE TABLE iceberg.silver.fct_refunds {location_props} AS
            SELECT r.refund_id,
                   r.order_id,
                   r.customer_id,
                   o.order_date,
                   r.refund_date,
                   date_diff('day', o.order_date, r.refund_date)
                                                               AS refund_lag_days,
                   CAST(r.refund_amount AS DECIMAL(12, 2)) AS refund_amount,
                   CAST(o.total_amount AS DECIMAL(12, 2))  AS order_total,
                   lower(trim(r.refund_reason))          AS refund_reason,
                   CAST(r.refund_amount AS DECIMAL(12, 2))
                       >= CAST(o.total_amount AS DECIMAL(12, 2))
                                                               AS is_full_refund
            FROM iceberg.bronze.refunds r
            JOIN iceberg.bronze.orders o
                 ON o.order_id = r.order_id
        """,
        "pre_stats": [
            ("bronze_rows", "SELECT count(*) FROM iceberg.bronze.refunds"),
        ],
        "post_stats": [
            (
                "full_refunds",
                "SELECT count(*) FROM iceberg.silver.fct_refunds "
                "WHERE is_full_refund",
            ),
            (
                "min_refund_lag_days",
                "SELECT coalesce(min(refund_lag_days), 0) "
                "FROM iceberg.silver.fct_refunds",
            ),
            (
                "max_refund_lag_days",
                "SELECT coalesce(max(refund_lag_days), 0) "
                "FROM iceberg.silver.fct_refunds",
            ),
        ],
    },
    {
        "name": "fct_web_events",
        "table": "iceberg.silver.fct_web_events",
        "deps": ["web_events"],
        "description": (
            "Web-event fact: standardized enums, anonymous sessions kept and "
            "flagged (customer_id stays null by design)."
        ),
        "ctas": """
            CREATE TABLE iceberg.silver.fct_web_events {location_props} AS
            SELECT e.event_id,
                   e.customer_id,
                   lower(trim(e.event_type))     AS event_type,
                   trim(e.category)              AS category,
                   lower(trim(e.device))         AS device,
                   e.event_date,
                   e.customer_id IS NULL         AS is_anonymous
            FROM iceberg.bronze.web_events e
        """,
        "pre_stats": [
            ("bronze_rows", "SELECT count(*) FROM iceberg.bronze.web_events"),
            (
                "anonymous_rows",
                "SELECT count(*) FROM iceberg.bronze.web_events "
                "WHERE customer_id IS NULL",
            ),
        ],
        "post_stats": [
            (
                "anonymous_after",
                "SELECT count(*) FROM iceberg.silver.fct_web_events "
                "WHERE is_anonymous",
            ),
        ],
    },
    {
        "name": "fct_support_tickets",
        "table": "iceberg.silver.fct_support_tickets",
        "deps": ["support_tickets", "orders"],
        "description": (
            "Support-ticket fact: standardized enums; order links validated "
            "against the customer's own orders (linked_order_valid)."
        ),
        "ctas": """
            CREATE TABLE iceberg.silver.fct_support_tickets {location_props} AS
            SELECT t.ticket_id,
                   t.customer_id,
                   t.order_id,
                   t.order_id IS NOT NULL          AS has_linked_order,
                   o.order_id IS NOT NULL          AS linked_order_valid,
                   lower(trim(t.issue_type))       AS issue_type,
                   lower(trim(t.priority))         AS priority,
                   t.ticket_date,
                   lower(trim(t.status))           AS status
            FROM iceberg.bronze.support_tickets t
            LEFT JOIN iceberg.bronze.orders o
                 ON o.order_id = t.order_id
                AND o.customer_id = t.customer_id
        """,
        "pre_stats": [
            ("bronze_rows", "SELECT count(*) FROM iceberg.bronze.support_tickets"),
            (
                "linked_rows",
                "SELECT count(*) FROM iceberg.bronze.support_tickets "
                "WHERE order_id IS NOT NULL",
            ),
        ],
        "post_stats": [
            (
                "invalid_linked_orders",
                "SELECT count(*) FROM iceberg.silver.fct_support_tickets "
                "WHERE has_linked_order AND NOT linked_order_valid",
            ),
        ],
    },
]
