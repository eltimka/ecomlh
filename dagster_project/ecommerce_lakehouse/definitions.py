"""Dagster Definitions for the e-commerce Customer 360 lakehouse.

Load with:
    dagster dev                      (from dagster_project/)
    dagster -w workspace.yaml ...    (CLI)

Jobs
----
Asset jobs materialize a layer (or the whole lakehouse) and - unlike the
in-process ``materialize()`` helper - also execute the Phase 8 asset
checks attached to each asset (uniqueness, freshness, referential
integrity, business rules, anomaly detection). Blocking checks failing
stops the affected asset's downstream chain.
"""

from dagster import AssetSelection, Definitions, define_asset_job

from .assets import bronze_assets, gold_assets, silver_assets
from .assets.checks import asset_checks
from .resources import MinioResource, TrinoResource

bronze_refresh = define_asset_job(
    "bronze_refresh",
    selection=AssetSelection.key_prefixes("bronze"),
    description="Full-refresh the bronze layer (raw ingest) + run its checks.",
)
silver_refresh = define_asset_job(
    "silver_refresh",
    selection=AssetSelection.key_prefixes("silver"),
    description="Rebuild the silver layer from bronze + run its checks.",
)
gold_refresh = define_asset_job(
    "gold_refresh",
    selection=AssetSelection.key_prefixes("gold"),
    description="Rebuild the gold Customer 360 marts from silver + run its checks.",
)
lakehouse_refresh = define_asset_job(
    "lakehouse_refresh",
    selection=AssetSelection.all(),
    description="Full lakehouse refresh: bronze -> silver -> gold + all 92 asset checks.",
)

definitions = Definitions(
    assets=[*bronze_assets, *silver_assets, *gold_assets],
    asset_checks=asset_checks,
    jobs=[bronze_refresh, silver_refresh, gold_refresh, lakehouse_refresh],
    resources={
        "trino": TrinoResource(),
        "minio": MinioResource(),
    },
)
