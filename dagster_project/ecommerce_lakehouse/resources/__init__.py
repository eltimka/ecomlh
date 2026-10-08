"""Dagster resources for the e-commerce lakehouse (Trino, Garage S3)."""

from .s3 import S3StorageResource
from .trino import TrinoResource

__all__ = ["S3StorageResource", "TrinoResource"]
