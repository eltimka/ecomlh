#!/usr/bin/env python3
"""End-to-end Phase 2 verification: buckets, schemas, and a test Iceberg table.

Checks, in order:
  1. MinIO is reachable and the bronze/silver/gold buckets exist
  2. Trino is reachable (SELECT 1)
  3. Medallion schemas exist in the Iceberg catalog (creates them if missing)
  4. A test Iceberg table can be created, written, queried, and dropped
     (location lives on MinIO: s3a://bronze/bootstrap_test)

Usage:
    python scripts/verify_lakehouse.py

Exits 0 only if every check passes.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import boto3
from botocore.config import Config
from dotenv import load_dotenv
from trino.dbapi import connect as trino_connect

REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")

PASS = "\033[32m[ok]\033[0m"
FAIL = "\033[31m[FAIL]\033[0m"

failures: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> bool:
    """Record and print a single check result."""
    mark = PASS if ok else FAIL
    suffix = f" - {detail}" if detail else ""
    print(f"  {mark} {name}{suffix}")
    if not ok:
        failures.append(name)
    return ok


def minio_check() -> None:
    endpoint = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
    use_ssl = os.environ.get("MINIO_USE_SSL", "false").lower() == "true"
    s3 = boto3.client(
        "s3",
        endpoint_url=f"{'https' if use_ssl else 'http'}://{endpoint}",
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        region_name=os.environ.get("MINIO_REGION", "us-east-1"),
        config=Config(signature_version="s3v4"),
    )
    try:
        buckets = {b["Name"] for b in s3.list_buckets()["Buckets"]}
    except Exception as exc:  # noqa: BLE001 - any connectivity failure is a FAIL
        check("MinIO reachable", False, str(exc))
        return

    check("MinIO reachable", True)
    for layer in ("bronze", "silver", "gold"):
        name = os.environ.get(f"MINIO_BUCKET_{layer.upper()}", layer)
        check(f"bucket '{name}' exists", name in buckets)


def trino_client():
    return trino_connect(
        host=os.environ.get("TRINO_HOST", "localhost"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "admin"),
    )


def exec_sql(conn, sql: str) -> None:
    """Execute a single statement (no results expected), retrying while Trino starts up."""
    for attempt in range(12):
        try:
            cur = conn.cursor()
            try:
                cur.execute(sql)
                return
            finally:
                cur.close()
        except Exception as exc:  # noqa: BLE001
            if "SERVER_STARTING_UP" in str(exc) and attempt < 11:
                time.sleep(5)
                continue
            raise


def query_sql(conn, sql: str) -> list[tuple]:
    """Execute a query and return all rows, retrying while Trino starts up."""
    for attempt in range(12):
        try:
            cur = conn.cursor()
            try:
                cur.execute(sql)
                return cur.fetchall()
            finally:
                cur.close()
        except Exception as exc:  # noqa: BLE001
            if "SERVER_STARTING_UP" in str(exc) and attempt < 11:
                time.sleep(5)
                continue
            raise
    raise AssertionError("unreachable")


def trino_checks() -> None:
    try:
        conn = trino_client()
    except Exception as exc:  # noqa: BLE001
        check("Trino reachable", False, str(exc))
        return
    check("Trino reachable", True, "SELECT 1")

    catalog = os.environ.get("TRINO_CATALOG", "iceberg")

    # 3. Schemas ----------------------------------------------------------------
    for layer in ("bronze", "silver", "gold"):
        exec_sql(conn, f"CREATE SCHEMA IF NOT EXISTS {catalog}.{layer}")
    schemas = {r[0] for r in query_sql(conn, f"SHOW SCHEMAS FROM {catalog}")}
    for layer in ("bronze", "silver", "gold"):
        check(f"schema {catalog}.{layer}", layer in schemas)

    # 4. Test Iceberg table lifecycle --------------------------------------------
    table = f"{catalog}.bronze.bootstrap_test"
    location = "s3a://bronze/bootstrap_test"
    try:
        exec_sql(conn, f"DROP TABLE IF EXISTS {table}")
        exec_sql(
            conn,
            f"""
            CREATE TABLE {table} (
                id BIGINT,
                name VARCHAR,
                score DOUBLE
            ) WITH (
                format = 'PARQUET',
                location = '{location}'
            )
            """,
        )
        check(f"create test table {table}", True)

        exec_sql(
            conn,
            f"""
            INSERT INTO {table} VALUES
                (1, 'alice', 0.91),
                (2, 'bob', 0.55),
                (3, 'carol', 0.77)
            """,
        )

        rows = query_sql(conn, f"SELECT * FROM {table} ORDER BY id")
        check("insert + select from test table", len(rows) == 3, f"{rows}")

        # Round-trip through Iceberg metadata (schema via catalog, not cache).
        cols = {
            str(r[0]).lower(): str(r[1]).lower()
            for r in query_sql(conn, f"DESCRIBE {table}")
        }
        check(
            "table columns round-trip",
            cols.get("id") == "bigint" and cols.get("name") == "varchar" and cols.get("score") == "double",
            str(cols),
        )
    except Exception as exc:  # noqa: BLE001
        check("test Iceberg table lifecycle", False, str(exc))
        return

    # Cleanup: drop the table so the lake starts clean.
    exec_sql(conn, f"DROP TABLE IF EXISTS {table}")
    still_there = query_sql(conn, f"SHOW TABLES FROM {catalog}.bronze LIKE 'bootstrap_test'")
    check("drop test table", not still_there)


def main() -> int:
    print("Phase 2 verification: lakehouse bootstrap\n")
    minio_check()
    print()
    trino_checks()
    print()
    if failures:
        print(f"RESULT: FAIL ({len(failures)} check(s) failed: {', '.join(failures)})")
        return 1
    print("RESULT: PASS - all Phase 2 checks succeeded")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
