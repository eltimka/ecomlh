-- =============================================================================
-- Flink SQL (Phase 15 item 4): Kafka raw.order_items ->
-- Iceberg bronze.stream_order_items
--
-- Same semantics as stream_web_events.sql (dumb append, group-offsets +
-- earliest fallback, durable 10s checkpoints on S3).
--
-- The item rows carry no date column, so the table is unpartitioned (a
-- single always-open write partition - cheap on checkpoint memory).
-- =============================================================================

CREATE CATALOG IF NOT EXISTS iceberg WITH (
    'type' = 'iceberg',
    'catalog-type' = 'hive',
    'uri' = 'thrift://hive-metastore:9083'
);

CREATE TABLE IF NOT EXISTS iceberg.bronze.stream_order_items (
    order_item_id STRING,
    order_id      STRING,
    product_id    STRING,
    quantity      INT,
    unit_price    DOUBLE,
    line_total    DOUBLE
) WITH (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'location' = 's3a://bronze/stream_order_items'
);

CREATE TEMPORARY TABLE kafka_order_items (
    order_item_id STRING,
    order_id      STRING,
    product_id    STRING,
    quantity      INT,
    unit_price    DOUBLE,
    line_total    DOUBLE
) WITH (
    'connector' = 'kafka',
    'properties.bootstrap.servers' = 'kafka:9094',
    'topic' = 'raw.order_items',
    'properties.group.id' = 'bronze.stream_order_items',
    'scan.startup.mode' = 'group-offsets',
    'properties.auto.offset.reset' = 'earliest',
    'format' = 'json'
);

INSERT INTO iceberg.bronze.stream_order_items
SELECT order_item_id, order_id, product_id, quantity, unit_price, line_total
FROM kafka_order_items;
