-- =============================================================================
-- Flink SQL (Phase 15 item 4): Kafka raw.payments -> Iceberg bronze.stream_payments
--
-- Same semantics as stream_web_events.sql: dumb append (silver is the merge
-- point), group-offsets startup + earliest fallback for a reset topic,
-- durable 10s checkpoints on s3a://flink-state/checkpoints.
-- =============================================================================

CREATE CATALOG IF NOT EXISTS iceberg WITH (
    'type' = 'iceberg',
    'catalog-type' = 'hive',
    'uri' = 'thrift://hive-metastore:9083'
);

CREATE TABLE IF NOT EXISTS iceberg.bronze.stream_payments (
    payment_id     STRING,
    order_id       STRING,
    payment_method STRING,
    payment_status STRING,
    payment_date   DATE,
    amount         DOUBLE
) PARTITIONED BY (payment_date)
WITH (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'location' = 's3a://bronze/stream_payments'
);

CREATE TEMPORARY TABLE kafka_payments (
    payment_id     STRING,
    order_id       STRING,
    payment_method STRING,
    payment_status STRING,
    payment_date   DATE,
    amount         DOUBLE
) WITH (
    'connector' = 'kafka',
    'properties.bootstrap.servers' = 'kafka:9094',
    'topic' = 'raw.payments',
    'properties.group.id' = 'bronze.stream_payments',
    'scan.startup.mode' = 'group-offsets',
    'properties.auto.offset.reset' = 'earliest',
    'format' = 'json'
);

INSERT INTO iceberg.bronze.stream_payments
SELECT payment_id, order_id, payment_method, payment_status, payment_date, amount
FROM kafka_payments;
