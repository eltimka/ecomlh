"""Silver layer (clean, standardized, conformed data) - asset group.

Phase 6: 3 dimension tables + 6 fact tables in ``iceberg.silver``.

The per-table specs (CTAS SQL + pre/post stat queries) live in
:mod:`.sql`; assets are built by the shared spec-driven factory
(:func:`ecommerce_lakehouse.assets.factory.build_layer_assets`).

Every run reports measurable metadata: ``bronze_rows``, the effect of
every dedup/filter, ``rows``, and post-condition checks (e.g.
``payment_conflicts``, ``email_collisions_after``) - so a no-op run and a
run that actually cleaned data are both auditable in the Dagster UI.
"""

from ..bronze import BRONZE_ASSETS_BY_NAME
from ..factory import build_layer_assets
from .sql import SILVER_SPECS

silver_assets, SILVER_ASSETS_BY_NAME = build_layer_assets(
    "silver", SILVER_SPECS, BRONZE_ASSETS_BY_NAME
)
