"""Spec-driven medallion asset factory shared by the silver and gold layers.

A *layer spec* (see :mod:`silver.sql`, :mod:`gold.sql`) describes one table:

- ``name``: asset and table name (asset key: ``<layer>/<name>``)
- ``deps`` / ``local_deps``: upstream asset names (other layers / same
  layer) - resolved into Dagster ``deps`` for UI lineage
- ``description``: shown in the Dagster UI
- ``ctas``: ``CREATE TABLE iceberg.<layer>.<name> {location_props} AS ...``
  (``{location_props}`` is filled with the explicit S3 location)
- ``pre_stats`` / ``post_stats``: (label, scalar SQL) pairs reported as run
  metadata, making every run auditable (rows in/out, rule effects)

Every asset is a full-refresh (drop + CTAS) - idempotent and trivially
re-runnable at this data volume.
"""

import dagster as dg
from dagster import AssetExecutionContext, AssetsDefinition

from ..resources.trino import TrinoResource

#: Name of the per-layer barrier asset that cross-table checks attach to.
GATE_NAME = "__layer_gate__"


def build_layer_gate(layer: str, table_assets: dict[str, AssetsDefinition]) -> AssetsDefinition:
    """No-op barrier asset depending on every table of the layer.

    Cross-table data-quality checks (referential integrity, cross-table
    invariants) attach to this asset instead of a single table: they only
    run once ALL tables of the layer have committed in the current run.
    Without the barrier, a check on table A could read table B while B is
    inside its full-refresh drop window (drop + CTAS) - a race the
    parallel executor would happily schedule.
    """

    @dg.asset(
        key_prefix=(layer,),
        name=GATE_NAME,
        deps=list(table_assets.values()),
        description=(
            f"Barrier: all {layer} tables committed in this run. "
            "Cross-table DQ checks (referential integrity, invariants) "
            "run here - see assets/checks.py."
        ),
    )
    def layer_gate() -> None:
        return None

    return layer_gate


def build_layer_assets(
    layer: str,
    specs: list[dict],
    external_deps: dict[str, AssetsDefinition],
) -> tuple[list[AssetsDefinition], dict[str, AssetsDefinition]]:
    """Build all assets of one medallion layer.

    ``layer`` doubles as the Trino schema (``iceberg.<layer>``) and the
    MinIO bucket for table locations (``s3a://<layer>/<name>``).
    ``external_deps`` maps asset names from upstream layers; ``local_deps``
    resolve against specs earlier in the list, so dimensions must precede
    the facts that join them.

    Returns (assets, by_name) where ``by_name`` is table name -> asset
    definition, for downstream layers' ``external_deps``.
    """
    assets: list[AssetsDefinition] = []
    local: dict[str, AssetsDefinition] = {}
    for spec in specs:
        deps = [external_deps[n] for n in spec.get("deps", [])]
        deps += [local[n] for n in spec.get("local_deps", [])]
        asset = _layer_asset(layer, spec, deps)
        assets.append(asset)
        local[spec["name"]] = asset
    gate = build_layer_gate(layer, local)
    assets.append(gate)
    local[GATE_NAME] = gate
    return assets, local


def _layer_asset(
    layer: str, spec: dict, deps: list[AssetsDefinition]
) -> AssetsDefinition:
    @dg.asset(
        key_prefix=(layer,),
        name=spec["name"],
        deps=deps,
        description=spec["description"],
    )
    def layer_asset(context: AssetExecutionContext, trino: TrinoResource) -> None:
        _transform(context, layer, spec, trino)

    return layer_asset


def _transform(
    context: AssetExecutionContext, layer: str, spec: dict, trino: TrinoResource
) -> None:
    """Apply one spec: pre stats -> drop + CTAS -> post stats -> metadata."""
    table = f"iceberg.{layer}.{spec['name']}"
    meta: dict[str, int | str] = {"table": table}

    for label, sql in spec.get("pre_stats", []):
        (value,) = trino.fetch(sql)[0]
        meta[label] = int(value)

    # Explicit S3 location on every Iceberg table: Trino's native
    # filesystems cannot reach the HMS warehouse (file://) fallback.
    location = f"s3a://{layer}/{spec['name']}"
    trino.execute(f"DROP TABLE IF EXISTS {table}")
    trino.execute(
        spec["ctas"].format(
            location_props=f"WITH (format='PARQUET', location='{location}') "
        )
    )

    (rows,) = trino.fetch(f"SELECT count(*) FROM {table}")[0]
    meta["rows"] = int(rows)

    for label, sql in spec.get("post_stats", []):
        (value,) = trino.fetch(sql)[0]
        meta[label] = int(value)

    context.add_output_metadata(meta)
    context.log.info(f"{layer}.{spec['name']}: {rows} rows -> {table} {meta}")
