"""Silver layer (clean, standardized, conformed data) - asset group.

Phase 6: 3 dimension tables + 6 fact tables in ``iceberg.silver``.

Every asset is a full-refresh (drop + CTAS) transformation over bronze,
built from the declarative specs in :mod:`.sql`:

- standardization (enums lowercased/trimmed, proper nouns trimmed,
  money cast to DECIMAL(12,2))
- deduplication (dim_customers on email, fct_payments one row per order)
- referential-integrity enforcement (facts inner-joined to their
  dimensions; dropped orphans are counted in metadata)
- derived flags & metrics (status booleans, refund lag, holiday season,
  business-rule violation flags)

Each run reports measurable metadata: ``bronze_rows``, the effect of every
dedup/filter, ``rows_silver``, and post-condition checks (e.g.
``payment_conflicts``, ``email_collisions_after``) - so a no-op run and a
run that actually cleaned data are both auditable in the Dagster UI.
"""

import dagster as dg
from dagster import AssetsDefinition, AssetExecutionContext

from ...resources.trino import TrinoResource
from ..bronze import BRONZE_ASSETS_BY_NAME
from .sql import SILVER_SPECS


def _transform(context: AssetExecutionContext, spec: dict, trino: TrinoResource) -> None:
    """Apply one silver spec: stats -> drop + CTAS -> stats -> metadata."""
    table = spec["table"]
    meta: dict[str, int | str] = {"table": table}

    for label, sql in spec.get("pre_stats", []):
        (value,) = trino.fetch(sql)[0]
        meta[label] = int(value)

    # Explicit S3 location on every Iceberg table (HMS warehouse dir is not
    # usable by Trino's native filesystems - same rule as the bronze layer).
    location = f"s3a://silver/{spec['name']}"
    trino.execute(f"DROP TABLE IF EXISTS {table}")
    trino.execute(
        spec["ctas"].format(
            location_props=f"WITH (format='PARQUET', location='{location}') "
        )
    )

    (rows,) = trino.fetch(f"SELECT count(*) FROM {table}")[0]
    meta["rows_silver"] = int(rows)

    for label, sql in spec.get("post_stats", []):
        (value,) = trino.fetch(sql)[0]
        meta[label] = int(value)

    context.add_output_metadata(meta)
    context.log.info(f"silver.{spec['name']}: {rows} rows -> {table} {meta}")


def _silver_asset(spec: dict, deps: list[AssetsDefinition]) -> AssetsDefinition:
    """Build the Dagster asset for one silver table spec."""

    @dg.asset(
        key_prefix=("silver",),
        name=spec["name"],
        deps=deps,
        description=spec["description"],
    )
    def silver_asset(context: AssetExecutionContext, trino: TrinoResource) -> None:
        _transform(context, spec, trino)

    return silver_asset


def _build_silver_assets() -> list[AssetsDefinition]:
    """Materialize specs into assets, resolving bronze + intra-silver deps.

    Spec order matters: silver dims are listed before facts that depend on
    them (fct_order_items joins silver.dim_products).
    """
    assets: list[AssetsDefinition] = []
    silver_by_name: dict[str, AssetsDefinition] = {}
    for spec in SILVER_SPECS:
        deps = [BRONZE_ASSETS_BY_NAME[n] for n in spec.get("deps", [])]
        deps += [silver_by_name[n] for n in spec.get("silver_deps", [])]
        asset = _silver_asset(spec, deps)
        silver_by_name[spec["name"]] = asset
        assets.append(asset)
    return assets


silver_assets = _build_silver_assets()

#: table name -> asset definition, for wiring downstream (gold) `deps=[...]`
SILVER_ASSETS_BY_NAME = {a.key.path[-1]: a for a in silver_assets}
