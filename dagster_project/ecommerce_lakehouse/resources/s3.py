"""Garage (S3-compatible) resource: object-storage access to the lakehouse."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config as BotoConfig
from dagster import ConfigurableResource
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(REPO_ROOT / ".env")


class S3StorageResource(ConfigurableResource):
    """Dagster resource for the local Garage S3 API.

    Used for bucket management and object-level checks. Iceberg table
    *reads/writes* go through Trino (see TrinoResource) or pyiceberg in
    later phases; this resource is for infrastructure-level operations.

    Defaults come from the repo-root .env (GARAGE_* variables).
    """

    endpoint: str = os.environ.get("GARAGE_ENDPOINT", "localhost:3900")
    access_key: str = os.environ.get("GARAGE_ACCESS_KEY", "garageadmin")
    secret_key: str = os.environ.get(
        "GARAGE_SECRET_KEY", "garageadmin-local-dev-secret"
    )
    use_ssl: bool = os.environ.get("GARAGE_USE_SSL", "false").lower() == "true"
    region: str = os.environ.get("GARAGE_REGION", "garage")

    # Medallion buckets (see scripts/bootstrap_storage.py)
    bucket_bronze: str = os.environ.get("GARAGE_BUCKET_BRONZE", "bronze")
    bucket_silver: str = os.environ.get("GARAGE_BUCKET_SILVER", "silver")
    bucket_gold: str = os.environ.get("GARAGE_BUCKET_GOLD", "gold")

    def s3_client(self) -> "boto3.client":
        """Build an S3 client pointed at the local Garage endpoint."""
        scheme = "https" if self.use_ssl else "http"
        return boto3.client(
            "s3",
            endpoint_url=f"{scheme}://{self.endpoint}",
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region,
            config=BotoConfig(signature_version="s3v4"),
        )

    def list_buckets(self) -> list[str]:
        """Return the names of all buckets in the Garage instance."""
        return [b["Name"] for b in self.s3_client().list_buckets()["Buckets"]]

    def bucket_exists(self, name: str) -> bool:
        """Check whether a bucket exists."""
        return name in self.list_buckets()

    def list_prefix(self, bucket: str, prefix: str = "") -> list[str]:
        """List object keys under a prefix in a bucket."""
        paginator = self.s3_client().get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return keys

    def upload_file(self, local_path: str | Path, key: str, bucket: str | None = None) -> str:
        """Upload a local file to the bronze bucket (default) at ``key``.

        Returns the s3a:// URI of the uploaded object. Overwrites any
        existing object with the same key (bronze raw files are keyed by
        table name; regeneration is deterministic, see Phase 4).
        """
        target = bucket or self.bucket_bronze
        self.s3_client().upload_file(str(local_path), target, key)
        return f"s3a://{target}/{key}"
