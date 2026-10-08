#!/usr/bin/env python3
"""Bootstrap object storage: create the lakehouse buckets + medallion schemas.

  - Garage buckets (bronze / silver / gold / flink-state) via the S3 API
  - Trino medallion schemas (iceberg.bronze / silver / gold) via the
    Iceberg catalog (scripts/create_schemas.sql has the same statements)

Idempotent - safe to re-run at any time. Existing buckets and schemas are
reported and skipped, so this script can be part of any one-command startup
flow.

Creating the SCHEMAS here (not in a verify script) is required for a cold
start: the Flink stream jobs run `CREATE TABLE IF NOT EXISTS
iceberg.bronze.stream_*`, which fails with InvalidObjectException when the
database does not exist in the Hive Metastore yet - and on a fresh stack
this bootstrap is the first thing to touch the Iceberg catalog.

Usage:
    python scripts/bootstrap_storage.py

Connection details are read from the repo-root .env file (see .env.example).
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import boto3
import trino.dbapi
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")

LAYERS = ("bronze", "silver", "gold")


def get_bucket_names() -> list[str]:
    """Medallion buckets + Flink state bucket, configurable via .env."""
    return [
        os.environ.get("GARAGE_BUCKET_BRONZE", "bronze"),
        os.environ.get("GARAGE_BUCKET_SILVER", "silver"),
        os.environ.get("GARAGE_BUCKET_GOLD", "gold"),
        os.environ.get("GARAGE_BUCKET_FLINK", "flink-state"),
    ]


def build_s3_client() -> boto3.client:
    """Build an S3 client pointed at the local Garage endpoint."""
    endpoint = os.environ.get("GARAGE_ENDPOINT", "localhost:3900")
    use_ssl = os.environ.get("GARAGE_USE_SSL", "false").lower() == "true"
    scheme = "https" if use_ssl else "http"
    return boto3.client(
        "s3",
        endpoint_url=f"{scheme}://{endpoint}",
        aws_access_key_id=os.environ.get("GARAGE_ACCESS_KEY", "garageadmin"),
        aws_secret_access_key=os.environ.get(
            "GARAGE_SECRET_KEY", "garageadmin-local-dev-secret"
        ),
        region_name=os.environ.get("GARAGE_REGION", "garage"),
        config=Config(signature_version="s3v4"),
    )


def bootstrap_buckets() -> bool:
    """Create the lakehouse buckets on Garage (idempotent)."""
    print("Bootstrapping Garage buckets...")
    try:
        s3 = build_s3_client()
        existing = {b["Name"] for b in s3.list_buckets()["Buckets"]}
    except (ClientError, ConnectionError) as exc:  # noqa: B014 - boto3 wraps socket errors
        print(f"[FAIL] Cannot reach Garage S3 API: {exc}", file=sys.stderr)
        return False

    ok = True
    for name in get_bucket_names():
        if name in existing:
            print(f"  [ok]   bucket '{name}' already exists")
            continue
        try:
            s3.create_bucket(Bucket=name)
            print(f"  [ok]   created bucket '{name}'")
        except ClientError as exc:
            print(f"[FAIL] could not create bucket '{name}': {exc}", file=sys.stderr)
            ok = False

    print(f"  Buckets ready: {', '.join(get_bucket_names())}")
    return ok


def exec_sql(conn, sql: str) -> None:
    """Execute a single statement, retrying while the Iceberg connector starts up."""
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


def bootstrap_schemas() -> bool:
    """Create the medallion schemas in the Trino Iceberg catalog (idempotent)."""
    print("Creating Trino medallion schemas (iceberg catalog)...")
    try:
        conn = trino.dbapi.connect(
            host=os.environ.get("TRINO_HOST", "localhost"),
            port=int(os.environ.get("TRINO_PORT", "8080")),
            user=os.environ.get("TRINO_USER", "admin"),
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[FAIL] Cannot reach Trino: {exc}", file=sys.stderr)
        return False

    try:
        for layer in LAYERS:
            exec_sql(conn, f"CREATE SCHEMA IF NOT EXISTS iceberg.{layer}")
            print(f"  [ok]   schema iceberg.{layer}")
    except Exception as exc:  # noqa: BLE001
        print(f"[FAIL] could not create medallion schemas: {exc}", file=sys.stderr)
        return False
    finally:
        conn.close()

    return True


def main() -> int:
    ok = bootstrap_buckets() and bootstrap_schemas()
    if not ok:
        print("Bootstrap finished with errors.", file=sys.stderr)
        return 1
    print("Done. Storage bootstrapped (buckets + schemas).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
