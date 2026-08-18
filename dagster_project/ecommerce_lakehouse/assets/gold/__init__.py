"""Gold layer (Customer 360 marts) - asset group.

Phase 3: placeholder asset only. Real Customer 360 models
(customer_360, LTV, RFM, churn risk, revenue marts) land here in Phase 7.
"""

import dagster as dg


@dg.asset(
    key_prefix=("gold",),
    description=(
        "Placeholder for the gold layer. Will be replaced by real "
        "Customer 360 assets in Phase 7."
    ),
)
def gold_placeholder() -> None:
    """Placeholder asset marking the gold asset group (Phase 3)."""
    # No-op: nothing to materialize yet.


gold_assets = [gold_placeholder]
