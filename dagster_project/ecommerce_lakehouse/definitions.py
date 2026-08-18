"""Dagster Definitions for the e-commerce Customer 360 lakehouse.

Load with:
    dagster dev                      (from dagster_project/)
    dagster -w workspace.yaml ...    (CLI)
"""

from dagster import Definitions

from .assets import bronze_assets, gold_assets, silver_assets
from .resources import MinioResource, TrinoResource

definitions = Definitions(
    assets=[*bronze_assets, *silver_assets, *gold_assets],
    resources={
        "trino": TrinoResource(),
        "minio": MinioResource(),
    },
)
