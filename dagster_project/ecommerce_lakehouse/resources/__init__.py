"""Dagster resources for the e-commerce lakehouse (Trino, MinIO)."""

from .minio import MinioResource
from .trino import TrinoResource

__all__ = ["MinioResource", "TrinoResource"]
