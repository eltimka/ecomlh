-- =============================================================================
-- Create medallion schemas in the Trino Iceberg catalog (Hive Metastore).
-- Idempotent: safe to re-run.
--
-- Run via the Trino UI (http://localhost:8080), or:
--   docker compose exec -T trino trino --execute "$(cat scripts/create_schemas.sql)"
-- (exec from the docker/ directory)
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS iceberg.bronze;
CREATE SCHEMA IF NOT EXISTS iceberg.silver;
CREATE SCHEMA IF NOT EXISTS iceberg.gold;
