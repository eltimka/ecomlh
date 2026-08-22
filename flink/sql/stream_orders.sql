-- =============================================================================
-- Flink SQL (Phase 15 item 4): Kafka raw.orders -> Iceberg bronze.stream_orders
--
-- Submit:  make flink-up
-- Stop:    make flink-down
--
-- Same semantics as stream_web_events.sql: dumb append (silver is the merge
-- point), group-offsets startup + earliest fallback for a reset topic,
-- durable 10s checkpoints on s3a://flink-state/checkpoints. Money-path
-- sibling of the web-events job - see PROJECT.md, Phase 15 notes item 4.
-- =============================================================================

CREATE CATALOG IF NOT EXISTS iceberg WITH (
    'type' = 'iceberg',
    'catalog-type' = 'hive',
    'uri' = 'thrift://hive-metastore:9083'
);

CREATE TABLE IF NOT EXISTS iceberg.bronze.stream_orders (
    order_id     STRING,
    customer_id  STRING,
    order_date   DATE,
    order_status STRING,
    channel      STRING,
    total_amount DOUBLE
) PARTITIONED BY (order_date)
WITH (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'location' = 's3a://bronze/stream_orders'
);

CREATE TEMPORARY TABLE kafka_orders (
    order_id     STRING,
    customer_id  STRING,
    order_date   DATE,
    order_status STRING,
    channel      STRING,
    total_amount DOUBLE
) WITH (
    'connector' = 'kafka',
    'properties.bootstrap.servers' = 'kafka:9094',
    'topic' = 'raw.orders',
    'properties.group.id' = 'bronze.stream_orders',
    'scan.startup.mode' = 'group-offsets',
    'properties.auto.offset.reset' = 'earliest',
    'format' = 'json'
);

INSERT INTO iceberg.bronze.stream_orders
SELECT order_id, customer_id, order_date, order_status, channel, total_amount
FROM kafka_orders;
