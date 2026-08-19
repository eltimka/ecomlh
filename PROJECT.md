# E-Commerce Customer 360 Local Lakehouse

**Fully local • Open-source • No cloud**

This project builds a production-style Customer 360 data pipeline using a modern lakehouse architecture that runs entirely on your machine.

## Goal

Create a complete, interview-ready portfolio project that demonstrates:

- Data ingestion
- Medallion architecture (Bronze → Silver → Gold)
- Lakehouse storage & table format
- Orchestration with software-defined assets
- SQL analytics engine
- Customer 360 modeling (LTV, RFM, churn risk, etc.)
- Data quality
- Local dashboard

## Core Principles

- 100% local and open-source
- No real AWS / GCP / Snowflake / managed services
- Use MinIO as S3-compatible storage
- Prefer Apache Iceberg tables (ACID, schema evolution, time travel)
- Build **component by component** and verify each one before moving forward
- Keep the project clean, well-documented, and easy to explain

## Final Architecture

```
Synthetic E-commerce data
        ↓
Dagster Assets (Python + Polars / PyArrow)
        ↓
Write Iceberg tables → MinIO (s3a://)
        ↓
Hive Metastore (metadata)
        ↓
Trino (query engine)
        ↓
Transformations (dbt-trino or Trino SQL in Dagster)
        ↓
Gold Customer 360 marts
        ↓
Superset dashboard
```

## Tech Stack (Locked)

| Layer              | Tool                          | Notes                                      |
|--------------------|-------------------------------|--------------------------------------------|
| Object Storage     | MinIO                         | S3-compatible, local                       |
| Metastore          | Apache Hive Metastore         | Postgres backend                           |
| Table Format       | Apache Iceberg                | Via Hive catalog in Trino                  |
| Query Engine       | Trino                         | Single coordinator for local use           |
| Orchestration      | Dagster                       | Asset-based, local                         |
| Data Processing    | Polars + PyArrow              | Fast local processing                      |
| Transformations    | dbt-trino (preferred) or pure Trino SQL |                                 |
| Data Quality       | dbt tests + Dagster asset checks |                                      |
| Dashboard          | Apache Superset             | Local BI on top of Trino, dashboards as JSON |
| Language           | Python 3.11+                  |                                            |

## Project Structure (Target)

```text
ecommerce-customer-360-lakehouse/
├── docker/
│   ├── docker-compose.yml
│   ├── trino/
│   │   └── catalog/
│   │       └── iceberg.properties
│   ├── hive/
│   │   └── conf/
│   └── minio/                  # optional init scripts
├── dagster_project/
│   ├── ecommerce_lakehouse/
│   │   ├── __init__.py
│   │   ├── assets/
│   │   │   ├── bronze/
│   │   │   ├── silver/
│   │   │   └── gold/
│   │   ├── resources/
│   │   ├── jobs.py
│   │   └── definitions.py
│   ├── setup.py / pyproject.toml
│   └── workspace.yaml
├── dbt/
│   ├── models/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   ├── dbt_project.yml
│   └── profiles.yml
├── data_generator/
│   └── generate_synthetic.py
├── dashboard/
│   └── dashboards/          # Superset dashboard JSON exports (loaded via REST API)
├── scripts/
│   ├── bootstrap_minio.py
│   └── create_schemas.sql
├── .env.example
├── requirements.txt
├── PROJECT.md                  ← this file
└── README.md
```

## Build Order (Component by Component)

Follow this exact sequence. **Do not skip ahead.** After each phase, verify that everything works before continuing.

### Phase 0 – Project Scaffold
- Create the directory structure above
- Create a clean `README.md`
- Create `.env.example` and `requirements.txt`
- Initialize git (optional)

### Phase 1 – Infrastructure (Docker Compose)
Create a working local lakehouse foundation:

Services required:
- MinIO (ports 9000 + 9001)
- Postgres (for Hive Metastore)
- Hive Metastore (port 9083)
- Trino (port 8080)

Deliverables:
- `docker/docker-compose.yml`
- Trino catalog file for Iceberg + MinIO (`iceberg.properties`)
- Hive Metastore configuration
- Ability to start the whole stack with one command: `docker compose up -d`
- MinIO console accessible at http://localhost:9001
- Trino UI at http://localhost:8080

Success criteria:
- All containers healthy
- Can create a bucket in MinIO
- Can connect to Trino and see the Iceberg catalog

### Phase 2 – Bootstrap Lakehouse
- Create MinIO buckets: `bronze`, `silver`, `gold` (or a single `lakehouse` bucket with prefixes)
- Create Trino schemas: `bronze`, `silver`, `gold`
- Create a simple test Iceberg table and query it successfully from Trino
- Provide helper scripts (`scripts/bootstrap_minio.py` and SQL)

### Phase 3 – Dagster Project Setup
- Initialize a proper Dagster project under `dagster_project/`
- Configure resources for:
  - MinIO / S3
  - Trino
  - Hive Metastore (if needed)
- Make Dagster able to run locally (`dagster dev`)
- Create the first empty asset groups: bronze / silver / gold

### Phase 4 – Synthetic Data Generator
- Create a high-quality synthetic e-commerce data generator
- Entities needed:
  - customers
  - products
  - orders
  - order_items
  - payments
  - refunds
  - web_events
  - support_tickets (optional)
- Output as Parquet or directly usable by ingestion assets
- Make it deterministic with a seed for reproducibility

### Phase 5 – Bronze Layer (Ingestion)
- Dagster assets that:
  - Generate or load synthetic data
  - Write Iceberg tables into the `bronze` schema on MinIO
- Tables: `bronze.customers`, `bronze.orders`, `bronze.order_items`, `bronze.payments`, etc.
- Use proper partitioning where it makes sense (e.g., by order date)

### Phase 6 – Silver Layer
- Clean, standardize, and join data
- Type casting, deduplication, basic business rules
- Create clean dimension and fact tables in `silver` schema

### Phase 7 – Gold Layer (Customer 360)
Build the core analytical models:
- `gold.customer_360` (main wide table)
- Lifetime value (LTV)
- RFM segments
- Churn risk indicators
- Revenue by channel / product category
- Order summary metrics

### Phase 8 – Data Quality & Observability
- dbt tests or Dagster asset checks
- Basic freshness, uniqueness, not-null, referential integrity
- Optional: simple anomaly detection

### Phase 9 – Dashboard (Apache Superset)
- Add an `apache/superset` service to the Docker Compose stack (UI on port 8088):
  - Reuse the existing Postgres container for Superset's metadb (separate
    `superset` database; no extra containers)
  - Local-only admin user (documented, local use only)
- Register **Trino as a Superset database** (SQLAlchemy URI
  `trino://<user>@localhost:8080/<catalog>`) so Superset queries the Iceberg
  marts directly - no copy into a warehouse
- Dashboards & charts are built **deterministically**: chart/dashboard JSON is
  committed under `dashboard/dashboards/` and loaded by a bootstrap script via
  the Superset REST API (idempotent, re-runnable):
  - Customer 360 overview (KPIs: customers, orders, GMV, AOV)
  - LTV distribution (from `gold.customer_360`)
  - Churn risk breakdown
  - Revenue trends (monthly, by channel)
  - RFM segment mix
- Success criteria:
  - `superset` container healthy, UI reachable at http://localhost:8088
  - Trino database registered and queryable inside Superset
  - Dashboards exist with charts rendering gold-layer data
  - `scripts/verify_dashboard.py` → RESULT: PASS

### Phase 10 – Polish
- Good README with architecture diagram and how to run
- Clear instructions for Pi Agent users
- Environment variables documented
- One-command startup experience as much as possible

## Coding & Style Guidelines for Pi Agent

- Prefer clear, readable Python over clever one-liners
- Use type hints
- Add docstrings to assets and key functions
- Keep secrets in `.env` (never hardcode credentials)
- Use environment variables for all connection details
- Name assets and tables consistently (`bronze_`, `silver_`, `gold_` prefixes when helpful)
- After creating any Docker or config file, always remind the user how to start/test it
- When a phase is complete, explicitly say:  
  **“Phase X complete. Please verify the following before we continue…”** and list verification steps

## Important Constraints

- Never introduce real cloud services (AWS S3, Snowflake, etc.)
- MinIO credentials should be simple for local use (e.g. `minioadmin` / `minioadmin`)
- Prefer Iceberg over plain Hive tables
- Keep resource usage reasonable for a laptop (single Trino coordinator is fine)
- All services must be startable with Docker Compose

## How to Work with Pi Agent

1. Feed this entire `PROJECT.md` to Pi at the beginning of the session.
2. Tell Pi: “We are following PROJECT.md. Start with Phase 0 / Phase 1.”
3. After each phase, verify the success criteria yourself (or ask Pi to help verify).
4. Only then say “Phase X is verified. Proceed to Phase Y.”

---

**Current status:** Phases 0-10 complete - project done (scaffold, Docker Compose
infrastructure, bootstrap lakehouse, Dagster project setup, synthetic data
generator, bronze layer ingestion, silver layer transformations, gold
Customer 360 marts, data quality & observability, Apache Superset dashboard,
polish & one-command startup).

Phase 9 notes (Apache Superset dashboard):
- apache/superset:4.1.1 service in the compose stack (UI port 8088; the
  in-container web server also listens on 8088 - healthcheck must use 8088,
  not 8888). Metadb = dedicated `superset` database in the shared Postgres
  container. Child image docker/superset/Dockerfile adds psycopg2-binary +
  sqlalchemy-trino (the 4.x base image ships no DB drivers; the Trino dialect
  PyPI project is `sqlalchemy-trino`, not `trino-sqlalchemy`).
- Dashboards-as-code: dashboard/dashboards/{datasets,charts,dashboard}.json
  are the source of truth; scripts/bootstrap_superset.py loads them via the
  REST API and is idempotent + self-healing (compares SQL/query_context and
  PUTs changed objects; re-lays the dashboard tiles on every run).
- Design: 10 small SQL mart datasets (virtual datasets over iceberg.gold,
  fully qualified table names) + 10 charts (4 KPI BIG_NUMBERs, LTV
  distribution, churn/RFM/channel/category mixes, monthly revenue trend)
  in one published "Customer 360" dashboard. Charts aggregate plain columns
  of the SQL marts with ad-hoc SQL metrics - this sidesteps 4.1's strict
  query-context schema (string metrics must be predefined; arbitrary SQL
  needs ad-hoc objects; CASE expressions cannot be groupby columns).
- 4.1 REST API gotchas (all hit live): login is a flat JSON body (no
  jsonrpc); token response is flat {access_token}; CSRF token (GET
  /api/v1/security/csrf_token/) required on writes as X-CSRFToken; list
  filter rison syntax unreliable - page_size:100 + client-side match
  instead; chart query_context/params are JSON *strings*; datasource.id is
  the plain integer; dashboard layout = json_metadata.positions dict with
  meta.chartId (no DASHBOARD_CLOUD_CHART_DATA, no chart-association
  endpoint - the PUT syncs dashboard_slices); sqllab execute needs
  database_id + sql and client_id <= 11 chars (metadb varchar(11)).
- superset init (not bare db upgrade) is required once: it seeds roles/
  permissions (without it every API call is 403 even as Admin).
- config.py: no SERVER_NAME (it silently 404s every route when requests
  carry a non-default port); no examples; local admin/admin user.
- Verified: scripts/verify_dashboard.py (RESULT: PASS) - container health,
  login, SELECT 1 through Superset->Trino, 10 datasets, all 10 charts
  render via GET /api/v1/chart/<id>/data/ with real rows (GMV tile =
  $15,221,141.27, matching the Phase 7 5-view invariant), dashboard tiles
  all reference existing charts. Fresh-metadb rebuild tested (drop DB ->
  full re-bootstrap from zero).

Next: Phase 10 (polish, docs, one-command startup).

Phase 10 notes (polish & one-command startup):
- Makefile: run / up / down / bootstrap / refresh / dev / verify /
  verify-dq / logs / clean. `make run` = scripts/start_all.py:
  compose up + wait healthy -> generate data if missing -> bootstrap
  MinIO -> bootstrap Superset -> lakehouse_refresh job (all checks) ->
  quick verification suite (5 scripts). Whole flow: ~3 min on a warm
  machine.
- CLI run of the refresh job: `dagster job execute` in Dagster 1.13 takes
  -m (module) not --definitions/-f (relative imports break file mode):
  `cd dagster_project && dagster job execute -m
  ecommerce_lakehouse.definitions -j lakehouse_refresh`.
- BUG FOUND & FIXED: the parallel executor scheduled cross-table checks
  (referential integrity, 5-view revenue invariant) concurrently with
  sibling tables' full-refresh drop windows -> intermittent
  TABLE_NOT_FOUND (failed ~50% of runs once triggered). Fix: per-layer
  no-op barrier assets <layer>/__layer_gate__ (factory.build_layer_gate)
  depending on every table of the layer; the 12 cross-table checks now
  attach to the gate, so they run only after the whole layer committed.
  No fake lineage edges, no executor tricks; asset count 21 -> 24, check
  count 85 unchanged (bronze 34 / silver 35 / gold 16). Verified with 3
  consecutive green full runs + verify_dq.
- README rewritten (architecture diagram, make targets, verification
  matrix, design decisions, 5-minute demo script). Removed leftover dbt
  scaffolding (dbt/ dir, dbt-core/dbt-trino requirements, .gitignore
  entries) - transformations are Trino SQL in Dagster; also dropped
  unused pyiceberg from requirements (all Iceberg I/O goes through Trino).
- verify_dq EXPECTED_ASSETS 21 -> 24 (tables + gates).

Project status: all phases complete; final state verified end-to-end via
`make run` + `make verify` + `make verify-dq` (all RESULT: PASS).

Phase 8 notes (data quality & observability):
- 85 declarative Dagster asset checks in assets/checks.py (same spec-driven
  pattern as the layers): SQL + condition + blocking flag per check.
  Families: not-empty, PK uniqueness (blocking), bronze row counts vs the
  Phase 4 manifest (blocking), freshness (date columns vs DATA_DATE_*),
  referential integrity (blocking, bronze + silver), silver business rules
  (payment conflicts, line-total arithmetic, amount mismatches, negative
  refund lag, ticket-link validity), gold invariants (score/segment/churn
  domains, 1:1 customer coverage, 5-view revenue invariant - blocking),
  and simple anomaly detection (|z| > 3 on monthly order counts).
- Freshness semantics are per-table: dense tables must cover the full data
  range; refunds only from-within-range and fresh-up-to-end (no upper bound
  - refund lag is realistic); sparse tables (support_tickets, 300 rows)
  only need the newest row within 7 days of the range end. (Both were
  discovered by the test suite itself on first run.)
- Checks run via asset JOBS (dagster_project/.../definitions.py adds
  bronze_refresh / silver_refresh / gold_refresh / lakehouse_refresh):
  the in-process materialize() helper skips check steps, but a resolved
  asset job includes them as ops; a failed BLOCKING check fails the run
  (DagsterAssetCheckFailedError) and stops downstream assets.
- 1.13 API notes: decorator is dg.asset_check (not asset_check_spec),
  result class is dg.AssetCheckResult; AssetSelection.key_prefixes (plural),
  AssetSelection.assets (keys is deprecated); RepositoryDefinition is built
  via Definitions.get_repository_def(); DagsterInstance has no
  events_for_run - use get_latest_asset_check_evaluation_record per check
  key (status SUCCEEDED/FAILED).
- Verify: python scripts/verify_dq.py (18 condition unit tests + live
  negative test + full lakehouse_refresh: 21 assets, 85/85 checks PASS).

Next: Phase 10 (polish - README/docs, one-command startup).

Phase 7 notes (gold layer - Customer 360):
- 4 assets in iceberg.gold, all reading silver:
  * customer_360 - one row per customer: demographics + LTV metrics
    (gross/net revenue, refunds, AOV, items), RFM quintile scores + segment
    (champion/loyal/new_or_returning/potential_loyalist/needs_attention/
    hibernating/lost/no_orders), churn risk (recency vs the customer's own
    avg inter-order gap, fallback global avg: <1.5 low, <2.5 medium, else
    high), support tickets + web engagement (cart adds, last event).
  * revenue_by_channel - month x channel (orders, gross/net, AOV, rates)
  * revenue_by_category - month x category (units, gross, revenue share)
  * monthly_kpis - monthly order-summary KPIs (status mix, new vs returning
    customers, return/cancel rates)
- Modeling conventions (documented in assets/gold/sql.py): as-of date =
  max(order_date) in the data (deterministic, no wall-clock); revenue =
  non-cancelled orders; realized LTV = net revenue.
- Refactor: the silver/gold spec-driven factory was extracted to
  assets/factory.py (build_layer_assets: CTAS + pre/post stat metadata +
  explicit s3a locations + dep resolution for lineage); silver re-tested
  green after the refactor.
- Verify: python scripts/verify_gold.py (17 checks, RESULT: PASS), incl.
  a 5-view gross-revenue invariant (fct_orders = customer_360 = by_channel
  = by_category = monthly_kpis, exact to the cent).

Next: Phase 8 (data quality & observability - Dagster asset checks) - done,
see Phase 8 notes above.

Phase 6 notes (silver layer):
- 9 assets: 3 dimensions (dim_customers, dim_products, dim_order_dates) +
  6 facts (fct_orders, fct_order_items, fct_payments, fct_refunds,
  fct_web_events, fct_support_tickets) in iceberg.silver, all explicit
  s3a://silver/<name> locations (same rule as bronze).
- Declarative spec-driven design: assets/silver/sql.py holds one spec per
  table (CTAS SQL + pre/post stat queries); the factory in
  assets/silver/__init__.py builds the assets, resolves bronze +
  intra-silver deps (fct_order_items depends on silver.dim_products) for
  UI lineage, and reports every drop/fix as run metadata (bronze_rows,
  duplicate_emails, orphan_orders, rows_silver, payment_conflicts, ...).
- Transformations: enum standardization (lower+trim), dedup on email
  (dim_customers) and one-payment-per-order (fct_payments), referential
  integrity enforcement (inner joins to dimensions; orphans dropped and
  counted), DECIMAL(12,2) money, derived flags/metrics (status booleans,
  refund_lag_days, is_full_refund, holiday-season calendar dim,
  payment-status conflict flag, ticket link validation).
- Trino 483 gotchas: no title() function, no bare SELECT UNNEST(...) -
  use FROM UNNEST(SEQUENCE(...)) AS t(d); no day_of_week_name - use
  ELEMENT_AT over an array indexed by day_of_week(); iceberg CTAS without
  location falls back to an unusable file:// HMS warehouse path.
- Verify: python scripts/verify_silver.py (24 checks, RESULT: PASS).

Next: Phase 7 (Gold layer - Customer 360 wide table, LTV, RFM, churn).

Phase 5 notes (bronze layer):
- Trino 4xx's hive connector does NOT use Hadoop: it has a native S3
  filesystem (trino-filesystem-s3, AWS SDK v2). Enable with catalog props
  `fs.s3.enabled=true` + `s3.endpoint`/`s3.aws-access-key`/... in
  docker/trino/catalog/hive.properties. Stock `trinodb/trino:483` image
  works - no custom image or hadoop-aws jars needed (an earlier custom
  image attempt was reverted after this was discovered).
- Flow per asset: upload data/synthetic/<t>.parquet -> s3a://bronze/raw/<t>/
  (immutable landing zone, directly queryable as hive.bronze_raw.<t>),
  then DROP + CTAS into iceberg.bronze.<t> (full refresh, idempotent).
  DDL column types inferred from the parquet schema via polars.
- 8 bronze assets (bronze.customers ... bronze.support_tickets) replace the
  Phase 3 placeholder; row counts match the Phase 4 manifest exactly.
- Dagster 1.13 gotchas hit and fixed: resources must be declared as typed
  function params (Config-based classes are NOT recognized as resource
  annotations - switched resources to ConfigurableResource, a Config subclass
  that is); `context.resource("x")` is gone (scoped `context.resources`);
  `AssetKey.to_string()` now returns JSON-like output and `has_prefix`
  requires a sequence; `materialize()` on bare asset defs needs explicit
  `resources={...}`; `dg.Failure(msg)` takes a positional message.
- Verify: python scripts/verify_bronze.py (12 checks, RESULT: PASS).

Next: Phase 6 (Silver layer - cleaning, conformance, dedup) - done, see
Phase 6 notes above.
