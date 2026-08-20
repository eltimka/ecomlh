#!/usr/bin/env python3
"""Bootstrap MinIO: create the lakehouse buckets (bronze / silver / gold).

Idempotent - safe to re-run at any time. Existing buckets are reported and
skipped, so this script can be part of any one-command startup flow.

Usage:
    python scripts/bootstrap_minio.py

Connection details are read from the repo-root .env file (see .env.example).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")


def get_bucket_names() -> list[str]:
    """Layer buckets + Flink state bucket, configurable via .env."""
    return [
        os.environ.get("MINIO_BUCKET_BRONZE", "bronze"),
        os.environ.get("MINIO_BUCKET_SILVER", "silver"),
        os.environ.get("MINIO_BUCKET_GOLD", "gold"),
        os.environ.get("MINIO_BUCKET_FLINK", "flink-state"),
    ]


def build_s3_client() -> boto3.client:
    """Build an S3 client pointed at the local MinIO endpoint."""
    endpoint = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
    use_ssl = os.environ.get("MINIO_USE_SSL", "false").lower() == "true"
    scheme = "https" if use_ssl else "http"
    return boto3.client(
        "s3",
        endpoint_url=f"{scheme}://{endpoint}",
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        region_name=os.environ.get("MINIO_REGION", "us-east-1"),
        config=Config(signature_version="s3v4"),
    )


def main() -> int:
    print("Bootstrapping MinIO buckets...")
    try:
        s3 = build_s3_client()
        existing = {b["Name"] for b in s3.list_buckets()["Buckets"]}
    except (ClientError, ConnectionError) as exc:  # noqa: B014 - boto3 wraps socket errors
        print(f"[FAIL] Cannot reach MinIO: {exc}", file=sys.stderr)
        return 1

    failed = False
    for name in get_bucket_names():
        if name in existing:
            print(f"  [ok]   bucket '{name}' already exists")
            continue
        try:
            s3.create_bucket(Bucket=name)
            print(f"  [ok]   created bucket '{name}'")
        except ClientError as exc:
            print(f"  [FAIL] could not create bucket '{name}': {exc}", file=sys.stderr)
            failed = True

    if failed:
        print("Bootstrap finished with errors.", file=sys.stderr)
        return 1

    print(f"Done. Buckets ready: {', '.join(get_bucket_names())}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
