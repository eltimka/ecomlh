"""Gold layer (Customer 360 marts) - asset group.

Phase 7: 4 Customer 360 models in ``iceberg.gold``:

- ``customer_360``      - one row per customer: demographics + LTV
  metrics + RFM scores/segment + churn risk + support/web engagement
- ``revenue_by_channel`` - month x channel revenue mart
- ``revenue_by_category`` - month x category revenue mart
- ``monthly_kpis``       - monthly order-summary KPIs

Per-table specs (CTAS SQL + stats) live in :mod:`.sql`; modeling
conventions (as-of date, LTV definition, RFM quintiles, churn heuristic)
are documented there. Assets are built by the shared spec-driven factory.
"""

from ..factory import build_layer_assets
from ..silver import SILVER_ASSETS_BY_NAME
from .sql import GOLD_SPECS

gold_assets, GOLD_ASSETS_BY_NAME = build_layer_assets(
    "gold", GOLD_SPECS, SILVER_ASSETS_BY_NAME
)
