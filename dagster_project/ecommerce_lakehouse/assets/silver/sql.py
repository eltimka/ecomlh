"""Silver layer transformation specs (Phase 6).

Each spec drives one silver asset (see __init__.py factory):

- ``deps`` / ``local_deps``: upstream asset names (bronze / silver) for
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
        "deps": [
            "orders", "customers", "order_items", "payments",
            "stream_orders", "stream_order_items", "stream_payments",
        ],
        "description": (
            "Order fact: batch + stream sources UNIONed and deduped on "
            "order_id (batch wins - identical payload per order_id, no "
            "arrival timestamp, so the source rank only makes the dedup "
            "deterministic). One row per order, validated against the "
            "customer dimension (orphans dropped), enriched with item "
            "counts (across both item sources), status flags and a "
            "payment-consistency flag (cancelled <=> failed)."
        ),
        "ctas": """
            CREATE TABLE iceberg.silver.fct_orders {location_props} AS
            WITH orders_merged AS (
                SELECT order_id, customer_id, order_date, order_status, channel, total_amount
                FROM (
                    SELECT order_id, customer_id, order_date, order_status, channel, total_amount,
                           row_number() OVER (PARTITION BY order_id ORDER BY src_rank) AS rn
                    FROM (
                        SELECT order_id, customer_id, order_date, order_status, channel,
                               total_amount, 1 AS src_rank
                        FROM iceberg.bronze.orders
                        UNION ALL
                        SELECT order_id, customer_id, order_date, order_status, channel,
                               total_amount, 2 AS src_rank
                        FROM iceberg.bronze.stream_orders
                    ) sources
                ) ranked
                WHERE rn = 1
            ),
            items_agg AS (
                SELECT order_id,
                       count(*)                   AS item_count,
                       count(DISTINCT product_id) AS distinct_products
                FROM (
                    SELECT order_id, product_id FROM iceberg.bronze.order_items
                    UNION ALL
                    SELECT order_id, product_id FROM iceberg.bronze.stream_order_items
                ) ui
                GROUP BY order_id
            ),
            pay_merged AS (
                SELECT order_id, payment_status
                FROM (
                    SELECT order_id, lower(trim(payment_status)) AS payment_status,
                           row_number() OVER (PARTITION BY order_id
                                              ORDER BY src_rank, payment_id) AS rn
                    FROM (
                        SELECT order_id, payment_id, payment_status, 1 AS src_rank
                        FROM iceberg.bronze.payments
                        UNION ALL
                        SELECT order_id, payment_id, payment_status, 2 AS src_rank
                        FROM iceberg.bronze.stream_payments
                    ) sources
                ) ranked
                WHERE rn = 1
            )
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
            FROM orders_merged o
            JOIN iceberg.bronze.customers c
                 ON c.customer_id = o.customer_id
            LEFT JOIN items_agg it ON it.order_id = o.order_id
            LEFT JOIN pay_merged p ON p.order_id = o.order_id
        """,
        "pre_stats": [
            ("bronze_rows", "SELECT count(*) FROM iceberg.bronze.orders"),
            ("stream_rows", "SELECT count(*) FROM iceberg.bronze.stream_orders"),
            (
                "orphan_orders",
                "SELECT count(*) FROM (SELECT customer_id FROM iceberg.bronze.orders "
                "UNION ALL SELECT customer_id FROM iceberg.bronze.stream_orders) o "
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
            (
                "duplicates_dropped",
                "SELECT (SELECT count(*) FROM iceberg.bronze.orders) "
                "+ (SELECT count(*) FROM iceberg.bronze.stream_orders) "
                "- (SELECT count(*) FROM iceberg.silver.fct_orders)",
            ),
        ],
    },
    {
        "name": "fct_order_items",
        "table": "iceberg.silver.fct_order_items",
        "deps": ["order_items", "orders", "stream_order_items", "stream_orders"],
        "local_deps": ["dim_products"],
        "description": (
            "Order-item fact: batch + stream sources UNIONed and deduped on "
            "order_item_id (batch wins), validated against the merged "
            "orders and dim_products (orphans dropped), DECIMAL money, "
            "denormalized product attributes, and a qty*price vs line_total "
            "integrity flag."
        ),
        "ctas": """
            CREATE TABLE iceberg.silver.fct_order_items {location_props} AS
            WITH items_merged AS (
                SELECT order_item_id, order_id, product_id, quantity, unit_price, line_total
                FROM (
                    SELECT order_item_id, order_id, product_id, quantity, unit_price,
                           line_total,
                           row_number() OVER (PARTITION BY order_item_id
                                              ORDER BY src_rank) AS rn
                    FROM (
                        SELECT order_item_id, order_id, product_id, quantity, unit_price,
                               line_total, 1 AS src_rank
                        FROM iceberg.bronze.order_items
                        UNION ALL
                        SELECT order_item_id, order_id, product_id, quantity, unit_price,
                               line_total, 2 AS src_rank
                        FROM iceberg.bronze.stream_order_items
                    ) sources
                ) ranked
                WHERE rn = 1
            ),
            orders_merged AS (
                SELECT order_id
                FROM (
                    SELECT order_id, 1 AS src_rank FROM iceberg.bronze.orders
                    UNION ALL
                    SELECT order_id, 2 AS src_rank FROM iceberg.bronze.stream_orders
                ) sources
                GROUP BY order_id
            )
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
            FROM items_merged i
            JOIN orders_merged o
                 ON o.order_id = i.order_id
            JOIN iceberg.silver.dim_products p
                 ON p.product_id = i.product_id
        """,
        "pre_stats": [
            ("bronze_rows", "SELECT count(*) FROM iceberg.bronze.order_items"),
            ("stream_rows", "SELECT count(*) FROM iceberg.bronze.stream_order_items"),
            (
                "orphan_items",
                "SELECT count(*) FROM (SELECT order_id, product_id "
                "FROM iceberg.bronze.order_items "
                "UNION ALL SELECT order_id, product_id "
                "FROM iceberg.bronze.stream_order_items) i "
                "WHERE NOT EXISTS (SELECT 1 FROM (SELECT order_id FROM iceberg.bronze.orders "
                "                              UNION ALL SELECT order_id FROM iceberg.bronze.stream_orders) o "
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
            (
                "duplicates_dropped",
                "SELECT (SELECT count(*) FROM iceberg.bronze.order_items) "
                "+ (SELECT count(*) FROM iceberg.bronze.stream_order_items) "
                "- (SELECT count(*) FROM iceberg.silver.fct_order_items)",
            ),
        ],
    },
    {
        "name": "fct_payments",
        "table": "iceberg.silver.fct_payments",
        "deps": ["payments", "orders", "stream_payments", "stream_orders"],
        "description": (
            "Payment fact: batch + stream sources UNIONed, deduplicated to "
            "one row per order (batch wins, then lowest payment_id), "
            "validated against the merged orders, DECIMAL amounts, status "
            "flags and an amount-vs-order-total mismatch flag."
        ),
        "ctas": """
            CREATE TABLE iceberg.silver.fct_payments {location_props} AS
            WITH payments_merged AS (
                SELECT payment_id, order_id, payment_method, payment_status,
                       payment_date, amount
                FROM (
                    SELECT payment_id, order_id, payment_method, payment_status,
                           payment_date, amount,
                           row_number() OVER (PARTITION BY order_id
                                              ORDER BY src_rank, payment_id) AS rn
                    FROM (
                        SELECT payment_id, order_id, payment_method, payment_status,
                               payment_date, amount, 1 AS src_rank
                        FROM iceberg.bronze.payments
                        UNION ALL
                        SELECT payment_id, order_id, payment_method, payment_status,
                               payment_date, amount, 2 AS src_rank
                        FROM iceberg.bronze.stream_payments
                    ) sources
                ) ranked
                WHERE rn = 1
            ),
            orders_merged AS (
                SELECT order_id, customer_id, total_amount
                FROM (
                    SELECT order_id, customer_id, total_amount,
                           row_number() OVER (PARTITION BY order_id
                                              ORDER BY src_rank) AS rn
                    FROM (
                        SELECT order_id, customer_id, total_amount, 1 AS src_rank
                        FROM iceberg.bronze.orders
                        UNION ALL
                        SELECT order_id, customer_id, total_amount, 2 AS src_rank
                        FROM iceberg.bronze.stream_orders
                    ) sources
                ) ranked
                WHERE rn = 1
            )
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
            FROM payments_merged p
            JOIN orders_merged o
                 ON o.order_id = p.order_id
        """,
        "pre_stats": [
            ("bronze_rows", "SELECT count(*) FROM iceberg.bronze.payments"),
            ("stream_rows", "SELECT count(*) FROM iceberg.bronze.stream_payments"),
            (
                "duplicate_payments",
                "SELECT count(*) - count(DISTINCT order_id) FROM (SELECT order_id "
                "FROM iceberg.bronze.payments "
                "UNION ALL SELECT order_id FROM iceberg.bronze.stream_payments) pp",
            ),
        ],
        "post_stats": [
            (
                "duplicates_dropped",
                "SELECT (SELECT count(*) FROM iceberg.bronze.payments) "
                "+ (SELECT count(*) FROM iceberg.bronze.stream_payments) "
                "- (SELECT count(*) FROM iceberg.silver.fct_payments)",
            ),
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
        "deps": ["web_events", "stream_web_events"],
        "description": (
            "Web-event fact: batch + stream sources UNIONed and deduped on "
            "event_id (on a duplicate the batch row wins - both sources "
            "carry the identical payload per event_id and there is no "
            "arrival timestamp, so the source rank exists only to make the "
            "dedup deterministic). Standardized enums, anonymous sessions "
            "kept and flagged (customer_id stays null by design)."
        ),
        "ctas": """
            CREATE TABLE iceberg.silver.fct_web_events {location_props} AS
            SELECT r.event_id,
                   r.customer_id,
                   r.event_type,
                   r.category,
                   r.device,
                   r.event_date,
                   r.customer_id IS NULL         AS is_anonymous
            FROM (
                SELECT event_id,
                       customer_id,
                       lower(trim(event_type))   AS event_type,
                       trim(category)             AS category,
                       lower(trim(device))       AS device,
                       event_date,
                       row_number() OVER (PARTITION BY event_id
                                          ORDER BY src_rank) AS rn
                FROM (
                    SELECT event_id, customer_id, event_type, category,
                           device, event_date, 1 AS src_rank
                    FROM iceberg.bronze.web_events
                    UNION ALL
                    SELECT event_id, customer_id, event_type, category,
                           device, event_date, 2 AS src_rank
                    FROM iceberg.bronze.stream_web_events
                ) sources
            ) r
            WHERE r.rn = 1
        """,
        "pre_stats": [
            ("bronze_rows", "SELECT count(*) FROM iceberg.bronze.web_events"),
            ("stream_rows", "SELECT count(*) FROM iceberg.bronze.stream_web_events"),
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
            (
                "duplicates_dropped",
                "SELECT (SELECT count(*) FROM iceberg.bronze.web_events) "
                "+ (SELECT count(*) FROM iceberg.bronze.stream_web_events) "
                "- (SELECT count(*) FROM iceberg.silver.fct_web_events)",
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
