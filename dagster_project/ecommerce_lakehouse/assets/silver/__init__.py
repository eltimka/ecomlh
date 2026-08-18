"""Silver layer (cleaned & standardized data) - asset group.

Phase 3: placeholder asset only. Real cleaning/join assets
(dedup, type casting, dimension & fact tables) land here in Phase 6.
"""

import dagster as dg


@dg.asset(
    key_prefix=("silver",),
    description=(
        "Placeholder for the silver layer. Will be replaced by real "
        "transformation assets in Phase 6."
    ),
)
def silver_placeholder() -> None:
    """Placeholder asset marking the silver asset group (Phase 3)."""
    # No-op: nothing to materialize yet.


silver_assets = [silver_placeholder]
