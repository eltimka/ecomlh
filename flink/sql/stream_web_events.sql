-- =============================================================================
-- Flink SQL (Phase 12): Kafka raw.web_events -> Iceberg bronze.stream_web_events
--
-- Submit:  make flink-up
--          (docker exec flink-jobmanager ./bin/sql-client.sh
--            -f /opt/flink/sql/stream_web_events.sql)
-- Stop:    make flink-down   (cancels the running job)
--
-- Semantics:
--   * Append-only Iceberg writes (the Flink job is a dumb append); dedup and
--     conformance happen in silver (Phase 13).
--   * scan.startup.mode = group-offsets: a (re)started job resumes from the
--     consumer group offsets committed on cancellation, so it does NOT
--     re-read the topic. A freshly reset topic (make reseed) has no group
--     offsets; properties.auto.offset.reset = earliest then applies and the
--     job replays the whole history - exactly what the rebuilt table needs.
--     (Without a reset policy the consumer fails with
--     NoOffsetForPartitionException on a group that has no committed
--     offsets.)
--   * Checkpoints are durable on S3 (s3a://flink-state/checkpoints, 10s
--     interval, retained on cancellation) - container recreation does not
--     lose committed Iceberg snapshots or Kafka offsets.
--   * Iceberg snapshots are committed at checkpoints (cluster interval 10s),
--     so new rows become visible in Trino within ~10-20s.
--   * The sink table uses an explicit s3a:// location (same rule as the
--     batch CTASes), so it never falls back to the HMS warehouse path.
-- =============================================================================

CREATE CATALOG IF NOT EXISTS iceberg WITH (
    'type' = 'iceberg',
    'catalog-type' = 'hive',
    'uri' = 'thrift://hive-metastore:9083'
);

CREATE TABLE IF NOT EXISTS iceberg.bronze.stream_web_events (
    event_id    STRING,
    customer_id STRING,
    event_type  STRING,
    category    STRING,
    device      STRING,
    event_date  DATE
) PARTITIONED BY (event_date)
-- (Flink's Iceberg catalog only supports identity partitioning in DDL - a
--  TODO in FlinkCatalog - but for a DATE column `day(x)` is physically
--  identical to identity(x) in Iceberg, so the layout matches the batch
--  bronze tables.)
WITH (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'location' = 's3a://bronze/stream_web_events'
);

CREATE TEMPORARY TABLE kafka_web_events (
    event_id    STRING,
    customer_id STRING,
    event_type  STRING,
    category    STRING,
    device      STRING,
    event_date  DATE
) WITH (
    'connector' = 'kafka',
    'properties.bootstrap.servers' = 'kafka:9094',
    'topic' = 'raw.web_events',
    'properties.group.id' = 'bronze.stream_web_events',
    'scan.startup.mode' = 'group-offsets',
    'properties.auto.offset.reset' = 'earliest',
    'format' = 'json'
);

INSERT INTO iceberg.bronze.stream_web_events
SELECT event_id, customer_id, event_type, category, device, event_date
FROM kafka_web_events;
