"""Gold layer (Customer 360 marts) specs (Phase 7).

Four assets, all reading ``iceberg.silver.*``:

- ``customer_360``: one row per customer - demographics + LTV metrics +
  RFM scores/segment + churn risk + support/engagement.
- ``revenue_by_channel``: month x channel revenue mart.
- ``revenue_by_category``: month x product-category revenue mart.
- ``monthly_kpis``: monthly order-summary KPIs (the "order summary
  metrics").

Modeling conventions (kept deterministic and data-driven):
- "As of" date for recency = max(order_date) in silver.fct_orders, so
  scores are stable for a given dataset (no wall-clock dependency).
- Revenue = non-cancelled orders only (cancelled orders never generated
  money); realized LTV = net_revenue = gross - refunds.
- RFM: quintile scores 1-5 (5 = best) over customers with orders only;
  R scored on recency_days ascending, F on valid_orders ascending,
  M on net_revenue ascending. Segment is a rule map over the scores.
- Churn risk: recency divided by the customer's own average inter-order
  gap (fallback: global average gap) - < 1.5 "low", < 2.5 "medium",
  else "high"; customers without orders are "none".
"""

GOLD_SPECS: list[dict] = [
    {
        "name": "customer_360",
        "table": "iceberg.gold.customer_360",
        "deps": [
            "dim_customers",
            "fct_orders",
            "fct_refunds",
            "fct_support_tickets",
            "fct_web_events",
        ],
        "description": (
            "Customer 360 wide table (one row per customer): demographics, "
            "order/LTV metrics, RFM scores + segment, churn risk, support "
            "and web engagement."
        ),
        "ctas": """
            CREATE TABLE iceberg.gold.customer_360 {location_props} AS
            WITH asof AS (
                SELECT max(order_date) AS asof_date
                FROM iceberg.silver.fct_orders
            ),
            order_agg AS (
                SELECT customer_id,
                       count(*)                               AS total_orders,
                       count(*) FILTER (WHERE is_completed)   AS completed_orders,
                       count(*) FILTER (WHERE is_returned)    AS returned_orders,
                       count(*) FILTER (WHERE is_cancelled)   AS cancelled_orders,
                       count(*) FILTER (WHERE is_cancelled = false) AS valid_orders,
                       min(order_date)                        AS first_order_date,
                       max(order_date)                        AS last_order_date,
                       coalesce(sum(total_amount)
                                  FILTER (WHERE is_cancelled = false), 0) AS gross_revenue,
                       coalesce(sum(item_count)
                                  FILTER (WHERE is_cancelled = false), 0) AS items_purchased
                FROM iceberg.silver.fct_orders
                GROUP BY customer_id
            ),
            gaps AS (
                SELECT customer_id,
                       avg(date_diff('day', prev_date, order_date))
                                       AS avg_order_gap_days
                FROM (
                    SELECT customer_id, order_date,
                           lag(order_date) OVER (
                               PARTITION BY customer_id ORDER BY order_date
                           ) AS prev_date
                    FROM (
                        SELECT DISTINCT customer_id, order_date
                        FROM iceberg.silver.fct_orders
                    ) d
                ) t
                WHERE prev_date IS NOT NULL
                GROUP BY customer_id
            ),
            global_gap AS (
                SELECT coalesce(avg(avg_order_gap_days), 1) AS global_avg_gap_days
                FROM gaps
            ),
            refund_agg AS (
                SELECT customer_id,
                       coalesce(sum(refund_amount), 0) AS total_refunded,
                       count(*)                       AS total_refunds
                FROM iceberg.silver.fct_refunds
                GROUP BY customer_id
            ),
            ticket_agg AS (
                SELECT customer_id,
                       count(*)                            AS total_tickets,
                       count(*) FILTER (WHERE status = 'open') AS open_tickets
                FROM iceberg.silver.fct_support_tickets
                GROUP BY customer_id
            ),
            event_agg AS (
                SELECT customer_id,
                       count(*)                                            AS total_web_events,
                       count(*) FILTER (WHERE event_type = 'add_to_cart')  AS cart_adds,
                       max(event_date)                                     AS last_event_date
                FROM iceberg.silver.fct_web_events
                WHERE customer_id IS NOT NULL
                GROUP BY customer_id
            ),
            base AS (
                SELECT c.customer_id, c.first_name, c.last_name, c.email,
                       c.country, c.city, c.marketing_channel, c.age,
                       c.signup_date,
                       coalesce(o.total_orders, 0)     AS total_orders,
                       coalesce(o.completed_orders, 0) AS completed_orders,
                       coalesce(o.returned_orders, 0)  AS returned_orders,
                       coalesce(o.cancelled_orders, 0) AS cancelled_orders,
                       coalesce(o.valid_orders, 0)     AS valid_orders,
                       o.first_order_date,
                       o.last_order_date,
                       CAST(coalesce(o.gross_revenue, 0)
                            AS DECIMAL(14, 2))         AS gross_revenue,
                       CAST(coalesce(r.total_refunded, 0)
                            AS DECIMAL(14, 2))         AS total_refunded,
                       CAST(coalesce(o.gross_revenue, 0)
                            - coalesce(r.total_refunded, 0)
                            AS DECIMAL(14, 2))         AS net_revenue,
                       coalesce(o.items_purchased, 0)  AS items_purchased,
                       CASE WHEN o.last_order_date IS NOT NULL
                            THEN date_diff('day', o.last_order_date, a.asof_date)
                       END                             AS recency_days,
                       g.avg_order_gap_days,
                       gg.global_avg_gap_days,
                       coalesce(tk.total_tickets, 0)   AS total_tickets,
                       coalesce(tk.open_tickets, 0)    AS open_tickets,
                       coalesce(ev.total_web_events, 0) AS total_web_events,
                       coalesce(ev.cart_adds, 0)       AS cart_adds,
                       ev.last_event_date
                FROM iceberg.silver.dim_customers c
                CROSS JOIN asof a
                LEFT JOIN order_agg  o  ON o.customer_id  = c.customer_id
                LEFT JOIN refund_agg r  ON r.customer_id  = c.customer_id
                LEFT JOIN gaps       g  ON g.customer_id  = c.customer_id
                CROSS JOIN global_gap gg
                LEFT JOIN ticket_agg tk ON tk.customer_id = c.customer_id
                LEFT JOIN event_agg  ev ON ev.customer_id = c.customer_id
            ),
            scored AS (
                SELECT b.*,
                       6 - ntile(5) OVER (
                           PARTITION BY b.valid_orders > 0
                           ORDER BY b.recency_days ASC)  AS r_score,
                       ntile(5) OVER (
                           PARTITION BY b.valid_orders > 0
                           ORDER BY b.valid_orders ASC)  AS f_score,
                       ntile(5) OVER (
                           PARTITION BY b.valid_orders > 0
                           ORDER BY b.net_revenue ASC)   AS m_score
                FROM base b
            )
            SELECT s.customer_id, s.first_name, s.last_name, s.email,
                   s.country, s.city, s.marketing_channel, s.age, s.signup_date,
                   s.total_orders, s.completed_orders, s.returned_orders,
                   s.cancelled_orders,
                   s.first_order_date, s.last_order_date,
                   s.gross_revenue, s.total_refunded, s.net_revenue,
                   CASE WHEN s.valid_orders > 0
                        THEN CAST(s.gross_revenue / s.valid_orders
                                 AS DECIMAL(14, 2)) END   AS aov,
                   s.items_purchased,
                   s.recency_days,
                   s.valid_orders                          AS frequency,
                   s.net_revenue                           AS monetary,
                   CASE WHEN s.total_orders > 0 THEN s.r_score END AS r_score,
                   CASE WHEN s.total_orders > 0 THEN s.f_score END AS f_score,
                   CASE WHEN s.total_orders > 0 THEN s.m_score END AS m_score,
                   CASE
                       WHEN s.total_orders = 0                  THEN 'no_orders'
                       WHEN s.r_score = 5 AND s.f_score >= 4    THEN 'champion'
                       WHEN s.f_score >= 4 AND s.r_score >= 3   THEN 'loyal'
                       WHEN s.r_score >= 4                      THEN 'new_or_returning'
                       WHEN s.r_score <= 2 AND s.f_score <= 2   THEN 'lost'
                       WHEN s.r_score <= 2                      THEN 'hibernating'
                       WHEN s.f_score >= 3                      THEN 'potential_loyalist'
                       ELSE 'needs_attention'
                   END                                     AS rfm_segment,
                   coalesce(s.avg_order_gap_days, s.global_avg_gap_days)
                                                           AS avg_order_gap_days,
                   CASE WHEN s.total_orders = 0 THEN NULL
                        ELSE s.recency_days
                             / coalesce(s.avg_order_gap_days, s.global_avg_gap_days)
                   END                                     AS churn_ratio,
                   CASE
                       WHEN s.total_orders = 0 THEN 'none'
                       WHEN s.recency_days
                              / coalesce(s.avg_order_gap_days, s.global_avg_gap_days)
                              < 1.5                        THEN 'low'
                       WHEN s.recency_days
                              / coalesce(s.avg_order_gap_days, s.global_avg_gap_days)
                              < 2.5                        THEN 'medium'
                       ELSE 'high'
                   END                                     AS churn_risk,
                   s.total_tickets, s.open_tickets,
                   s.total_web_events, s.cart_adds, s.last_event_date
            FROM scored s
        """,
        "pre_stats": [
            (
                "silver_customer_rows",
                "SELECT count(*) FROM iceberg.silver.dim_customers",
            ),
        ],
        "post_stats": [
            (
                "customers_with_orders",
                "SELECT count(*) FROM iceberg.gold.customer_360 "
                "WHERE total_orders > 0",
            ),
            (
                "champions",
                "SELECT count(*) FROM iceberg.gold.customer_360 "
                "WHERE rfm_segment = 'champion'",
            ),
            (
                "churn_high",
                "SELECT count(*) FROM iceberg.gold.customer_360 "
                "WHERE churn_risk = 'high'",
            ),
        ],
    },
    {
        "name": "revenue_by_channel",
        "table": "iceberg.gold.revenue_by_channel",
        "deps": ["fct_orders", "fct_refunds"],
        "description": (
            "Revenue mart by channel x month: order counts, gross/net "
            "revenue, AOV, return and cancel rates."
        ),
        "ctas": """
            CREATE TABLE iceberg.gold.revenue_by_channel {location_props} AS
            WITH order_month AS (
                SELECT date_trunc('month', order_date) AS month,
                       channel,
                       count(*)                            AS orders,
                       count(*) FILTER (WHERE is_completed)  AS completed_orders,
                       count(*) FILTER (WHERE is_returned)   AS returned_orders,
                       count(*) FILTER (WHERE is_cancelled)  AS cancelled_orders,
                       count(*) FILTER (WHERE is_cancelled = false) AS valid_orders,
                       coalesce(sum(total_amount)
                                  FILTER (WHERE is_cancelled = false), 0)
                                                          AS gross_revenue
                FROM iceberg.silver.fct_orders
                GROUP BY 1, 2
            ),
            refund_month AS (
                SELECT date_trunc('month', r.refund_date) AS month,
                       o.channel,
                       sum(r.refund_amount)               AS refunds
                FROM iceberg.silver.fct_refunds r
                JOIN iceberg.silver.fct_orders o
                     ON o.order_id = r.order_id
                GROUP BY 1, 2
            )
            SELECT m.month,
                   m.channel,
                   m.orders,
                   m.completed_orders,
                   m.returned_orders,
                   m.cancelled_orders,
                   m.valid_orders,
                   CAST(m.gross_revenue AS DECIMAL(14, 2))     AS gross_revenue,
                   CAST(coalesce(r.refunds, 0) AS DECIMAL(14, 2)) AS refunds,
                   CAST(m.gross_revenue - coalesce(r.refunds, 0)
                        AS DECIMAL(14, 2))                     AS net_revenue,
                   CASE WHEN m.valid_orders > 0
                        THEN CAST(m.gross_revenue / m.valid_orders
                                 AS DECIMAL(14, 2)) END        AS aov,
                   m.returned_orders * 1.0 / m.orders          AS return_rate,
                   m.cancelled_orders * 1.0 / m.orders         AS cancel_rate
            FROM order_month m
            LEFT JOIN refund_month r
                 ON r.month = m.month AND r.channel = m.channel
        """,
        "pre_stats": [],
        "post_stats": [
            (
                "channels",
                "SELECT count(DISTINCT channel) FROM iceberg.gold.revenue_by_channel",
            ),
            (
                "months",
                "SELECT count(DISTINCT month) FROM iceberg.gold.revenue_by_channel",
            ),
        ],
    },
    {
        "name": "revenue_by_category",
        "table": "iceberg.gold.revenue_by_category",
        "deps": ["fct_order_items", "fct_orders"],
        "description": (
            "Revenue mart by product category x month: orders, units sold, "
            "gross revenue and each category's monthly revenue share."
        ),
        "ctas": """
            CREATE TABLE iceberg.gold.revenue_by_category {location_props} AS
            SELECT date_trunc('month', o.order_date)       AS month,
                   i.category,
                   count(DISTINCT o.order_id)              AS orders,
                   sum(i.quantity)                         AS units,
                   CAST(sum(i.line_total) AS DECIMAL(14, 2)) AS gross_revenue,
                   100.0 * sum(i.line_total)
                       / sum(sum(i.line_total)) OVER (
                           PARTITION BY date_trunc('month', o.order_date))
                                                           AS revenue_share_pct
            FROM iceberg.silver.fct_order_items i
            JOIN iceberg.silver.fct_orders o
                 ON o.order_id = i.order_id
            WHERE o.is_cancelled = false
            GROUP BY 1, 2
        """,
        "pre_stats": [],
        "post_stats": [
            (
                "categories",
                "SELECT count(DISTINCT category) "
                "FROM iceberg.gold.revenue_by_category",
            ),
        ],
    },
    {
        "name": "monthly_kpis",
        "table": "iceberg.gold.monthly_kpis",
        "deps": ["fct_orders", "fct_refunds"],
        "description": (
            "Monthly order-summary KPIs: volumes, status mix, gross/net "
            "revenue, AOV, return/cancel rates, new vs returning customers."
        ),
        "ctas": """
            CREATE TABLE iceberg.gold.monthly_kpis {location_props} AS
            WITH order_month AS (
                SELECT date_trunc('month', order_date) AS month,
                       count(*)                          AS orders,
                       count(*) FILTER (WHERE is_completed) AS completed_orders,
                       count(*) FILTER (WHERE is_returned)  AS returned_orders,
                       count(*) FILTER (WHERE is_cancelled) AS cancelled_orders,
                       count(*) FILTER (WHERE is_cancelled = false) AS valid_orders,
                       count(DISTINCT customer_id)       AS unique_customers,
                       coalesce(sum(total_amount)
                                  FILTER (WHERE is_cancelled = false), 0)
                                                         AS gross_revenue
                FROM iceberg.silver.fct_orders
                GROUP BY 1
            ),
            new_customers AS (
                SELECT date_trunc('month', first_order_date) AS month,
                       count(*)                              AS new_customers
                FROM (
                    SELECT customer_id, min(order_date) AS first_order_date
                    FROM iceberg.silver.fct_orders
                    GROUP BY customer_id
                ) t
                GROUP BY 1
            ),
            refund_month AS (
                SELECT date_trunc('month', refund_date) AS month,
                       sum(refund_amount)               AS refunds
                FROM iceberg.silver.fct_refunds
                GROUP BY 1
            )
            SELECT m.month,
                   m.orders,
                   m.completed_orders,
                   m.returned_orders,
                   m.cancelled_orders,
                   m.valid_orders,
                   CAST(m.gross_revenue AS DECIMAL(14, 2))     AS gross_revenue,
                   CAST(coalesce(r.refunds, 0) AS DECIMAL(14, 2)) AS refunds,
                   CAST(m.gross_revenue - coalesce(r.refunds, 0)
                        AS DECIMAL(14, 2))                     AS net_revenue,
                   CASE WHEN m.valid_orders > 0
                        THEN CAST(m.gross_revenue / m.valid_orders
                                 AS DECIMAL(14, 2)) END        AS aov,
                   m.returned_orders * 1.0 / m.orders          AS return_rate,
                   m.cancelled_orders * 1.0 / m.orders         AS cancel_rate,
                   m.unique_customers,
                   coalesce(nc.new_customers, 0)               AS new_customers,
                   m.unique_customers - coalesce(nc.new_customers, 0)
                                                               AS returning_customers
            FROM order_month m
            LEFT JOIN new_customers nc ON nc.month = m.month
            LEFT JOIN refund_month r   ON r.month = m.month
        """,
        "pre_stats": [],
        "post_stats": [
            (
                "months",
                "SELECT count(DISTINCT month) FROM iceberg.gold.monthly_kpis",
            ),
        ],
    },
]
