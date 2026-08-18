"""Bronze layer (raw ingested data) - asset group.

Phase 3: placeholder asset only. Real ingestion assets
(customers, orders, order_items, payments, ...) land here in Phase 5
and write Iceberg tables to the `bronze` schema on MinIO.
"""

import dagster as dg


@dg.asset(
    key_prefix=("bronze",),
    description=(
        "Placeholder for the bronze layer. Will be replaced by real "
        "ingestion assets in Phase 5."
    ),
)
def bronze_placeholder() -> None:
    """Placeholder asset marking the bronze asset group (Phase 3)."""
    # No-op: nothing to materialize yet.


bronze_assets = [bronze_placeholder]
