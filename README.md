# E-Commerce Customer 360 — Local Lakehouse

A production-style **Customer 360 data pipeline** that runs entirely on your
laptop. Synthetic e-commerce data flows through a **Medallion lakehouse**
(Bronze → Silver → Gold) on **Apache Iceberg** in **MinIO**, orchestrated by
**Dagster** (with 85 declarative data-quality checks), queried by **Trino**,
and visualized in a local **Streamlit** dashboard.

100% local and open-source — no AWS, no GCP, no Snowflake.

## What this project demonstrates

- **Medallion architecture** — Bronze (raw landing) → Silver (conformed
  dimensions + facts) → Gold (Customer 360 marts), each layer as real Iceberg
  tables with ACID commits and time travel
- **Software-defined assets** — every table is a Dagster asset with lineage;
  all transformations are versioned Trino SQL (spec-driven, declarative)
- **Data quality as code** — 85 asset checks (uniqueness, referential
  integrity, freshness, business rules, invariants, anomaly detection),
  blocking where it matters, running inside the refresh jobs
- **End-to-end verification** — one script per layer, each prints
  `RESULT: PASS`; the gold layer enforces a five-view revenue invariant
  ($15,221,141.27 for seed 42) that the dashboard renders back
- **Dashboard as code** — the entire dashboard is committed Python
  (`dashboard/marts.py` + `dashboard/app.py`): every chart is a versioned
  Trino query with push-down filters, no manual configuration, no extra
  service
- **Re-seedable data** — `make reseed SEED=<n>` regenerates the dataset and
  re-runs the whole pipeline; seed 42 is bit-reproducible end to end
- **Reproducibility** — deterministic synthetic data (seeded, per-entity RNG
  streams, Zipf skew, seasonality, realistic refund/churn behavior)

## Architecture

```
                        ┌────────────────────────────┐
   data_generator/      │         Dagster            │
   (seeded Parquet) ───▶ │  24 assets + 85 checks     │
                        │  jobs: bronze/silver/gold/ │
                        │  lakehouse_refresh         │
                        └─────────────┬──────────────┘
                                      │ Trino SQL (CTAS, idempotent)
                                      ▼
  ┌──────────────────────────  Trino 483  ──────────────────────────┐
  │   hive catalog ────────────┐        iceberg catalog             │
  └────────────────────────────┼────────────────────────────────────┘
                               ▼
        ┌─────────────────────────────────────────────┐
        │        Apache Iceberg on MinIO (S3)         │
        │  bronze/  silver/  gold/   (3 buckets)      │
        └─────────────────────────────────────────────┘
                               ▲
        metadata (schemas/tables)
                               │
        ┌─────────────────────────────────────────────┐
        │  Hive Metastore (Thrift) ← Postgres store   │
        └─────────────────────────────────────────────┘

  Gold marts ──▶ Trino ──▶ Streamlit (port 8501)
                     "Customer 360" dashboard (live Trino queries):
        KPIs (customers/orders/GMV/AOV) · LTV distribution ·
        churn-risk mix · RFM segments · monthly revenue trend ·
        channel & category mixes · customer profile table
```

| Layer   | Tables                                                        |
|---------|---------------------------------------------------------------|
| Bronze  | 8 raw landing tables (customers, orders, order_items, payments, refunds, web_events, products, support_tickets) |
| Silver  | 3 dimensions (customers, products, order_dates) + 6 conformed facts |
| Gold    | `customer_360` (LTV, RFM, churn), `revenue_by_channel`, `revenue_by_category`, `monthly_kpis` |

## Tech stack

| Component          | Tool                          |
|--------------------|-------------------------------|
| Orchestration      | Dagster (assets, checks, jobs) |
| Object storage     | MinIO (S3-compatible)          |
| Table format       | Apache Iceberg                 |
| Metastore          | Hive Metastore + Postgres      |
| Query engine       | Trino 483                      |
| Transformations    | Trino SQL (versioned, spec-driven) |
| Synthetic data     | Python + NumPy + Polars/Parquet |
| Dashboard          | Streamlit + Plotly (local venv, live Trino queries) |
| Infrastructure     | Docker Compose                 |

## Project structure

```text
Elvira_Project/
├── docker/                    # compose stack + Trino/Hive configs
│   ├── docker-compose.yml
│   ├── trino/                 # config + hive/iceberg catalogs (native S3 FS)
│   └── hive/                  # HMS config (Postgres backend)
├── dagster_project/           # Dagster definitions
│   └── ecommerce_lakehouse/
│       ├── definitions.py     # assets, checks, 4 refresh jobs
│       ├── assets/            # bronze / silver / gold + checks.py + factory
│       └── resources/         # TrinoResource, MinioResource
├── data_generator/            # seeded synthetic data generator
├── dashboard/                 # Streamlit dashboard (app.py + marts.py SQL)
├── scripts/                   # bootstrap + verification (one per layer)
├── data/synthetic/            # generated Parquet (gitignored)
├── Makefile                   # one-command entry points
├── PROJECT.md                 # phased build plan + phase-by-phase notes
└── README.md
```

## Quick start

**Prerequisites:** Docker + Compose, Python 3.11+ (3.14 tested), `make`.

```bash
# one-time
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp .env.example .env          # all defaults work out of the box

# that's it - infra + data + pipeline + dashboard, all idempotent
make run
```

`make run` brings up the stack, generates data if missing, bootstraps MinIO,
starts the dashboard, runs the full `lakehouse_refresh` job
(24 assets + 85 checks), and finishes with the quick verification suite.
Then:

| Open                              | What to see                                                    |
|-----------------------------------|----------------------------------------------------------------|
| http://localhost:8501               | "Customer 360" dashboard: KPIs, LTV distribution, churn/RFM mixes, revenue trend, channel/category, customer profiles. **Interactive**: order-channel / acquisition-channel / churn-risk / month-range filters + customer search - every change re-queries Trino |
| `make dev` → http://localhost:3000  | Dagster: asset graph, runs, 85 check results                   |
| http://localhost:9001 (minioadmin/minioadmin) | bronze/silver/gold buckets with Iceberg metadata/data |

### Manual steps (what `make run` does)

```bash
cd docker && docker compose up -d && cd ..
.venv/bin/python data_generator/generate_synthetic.py   # if data/ missing
.venv/bin/python scripts/bootstrap_minio.py              # buckets + schemas
cd dagster_project
../.venv/bin/dagster job execute -m ecommerce_lakehouse.definitions -j lakehouse_refresh
cd ..   # bronze→silver→gold + 85 checks
.venv/bin/streamlit run dashboard/app.py              # dashboard on :8501
.venv/bin/python scripts/verify_dashboard.py           # RESULT: PASS
```

## Everyday commands

| Command         | What it does                                                    |
|-----------------|-----------------------------------------------------------------|
| `make run`      | one-command end-to-end startup (idempotent)                     |
| `make up` / `down` | start / stop the Docker stack + dashboard (data kept)       |
| `make bootstrap`  | MinIO buckets + Trino schemas (idempotent)                     |
| `make refresh`    | re-run the whole pipeline with all checks                      |
| `make dev`        | Dagster UI on :3000                                            |
| `make dashboard`  | Streamlit Customer 360 dashboard on :8501 (foreground)         |
| `make verify`     | quick verification suite (all layers + dashboard)              |
| `make verify-dq`  | heavy DQ suite: check-unit tests + failure-path test + full re-run |
| `make reseed SEED=<n>` | regenerate synthetic data with a new seed, re-run the full pipeline + verification (42 = canonical dataset) |
| `make logs`       | tail all service logs                                          |
| `make clean`      | stack down **+ volume deletion** + generated data (full reset) |

## Changing the data (re-seeding)

The dataset is deterministic: `DATA_SEED` (default 42) drives a master PCG64
RNG with per-entity child streams, so the same seed always produces
bit-identical Parquet and therefore bit-identical gold marts. That is why
refreshing the pipeline never changes the numbers - refresh re-materializes,
it does not mutate.

To see the whole pipeline react to *new* data:

```bash
make reseed SEED=123     # regenerate + full pipeline (all 85 checks) + verify
# open http://localhost:8501 - every KPI, mix and trend now differs
make reseed SEED=42      # restore the canonical dataset (bit-identical)
```

`scripts/verify_dashboard.py` is seed-aware: expected row counts come from
`data/synthetic/manifest.json` and the GMV invariant is a cross-layer check
(gold revenue vs silver valid-order revenue), so any seed verifies.

## Verification

Each layer has a persistent verification script; all print `RESULT: PASS`:

| Script                    | Proves                                                              |
|---------------------------|---------------------------------------------------------------------|
| `verify_lakehouse.py`     | stack up: MinIO/Postgres/HMS/Trino reachable, catalogs configured   |
| `verify_bronze.py`        | 8 tables in `iceberg.bronze`, row counts = generator manifest, S3 objects present |
| `verify_silver.py`        | 9 conformed tables, row conformance (bronze − documented drops), key integrity |
| `verify_gold.py`          | 4 marts, 1:1 customer coverage, RFM/churn domains, **five-view revenue invariant** |
| `verify_dq.py`            | check logic unit-tested, a failing check fails the run, full green refresh |
| `verify_dashboard.py`     | all 11 marts execute on Trino, deterministic KPI invariants (5,000 / 50,000 / $15,221,141.27), app health → 200 |

## Key design decisions

- **Iceberg with explicit `s3a://` locations** — every CTAS pins
  `WITH (location='s3a://<bucket>/...')`; the HMS warehouse dir is never
  relied on, and `DROP TABLE` deletes S3 objects, making refreshes clean.
- **Trino as the transform + query engine** — the Trino *hive* connector
  speaks S3 natively (`trino-filesystem-s3`), so no Hadoop on the Trino side;
  the Hive *iceberg* connector is used for time travel.
- **Spec-driven assets** — each layer is a list of table specs (SQL +
  expected-stat queries) compiled into Dagster assets by a shared factory;
  adding a table is adding a spec, not code.
- **Checks are declarative too** — `(asset, name, SQL, condition, blocking)`
  tuples compiled into `@asset_check` functions; blocking checks fail the
  refresh job, non-blocking ones surface in the UI. Freshness semantics are
  per-table (dense / refund-lag / sparse-recency). Cross-table checks
  (referential integrity, 5-view invariant) attach to per-layer **gate**
  assets (`<layer>/__layer_gate__`) that depend on every table of the layer
  — otherwise a parallel check could read a sibling table inside its
  full-refresh drop window (a real race, found and fixed in Phase 10).
- **Silver conformance is self-consistent** — row counts are
  `bronze − documented drops`, so checks survive data drift.
- **Gold conventions** — as-of = max order date; revenue excludes cancelled
  orders; realized LTV = net revenue; RFM quintiles (5 = best); churn =
  recency ÷ own average inter-order gap.
- **Streamlit over Superset, no warehouse copy** — the dashboard queries
  `iceberg.gold` directly through the same Trino client the pipeline uses.
  Phase 9 was first built on Apache Superset (git history + PROJECT.md notes);
  API-created charts hit Superset 4.1's legacy-viz/frontend format
  incompatibilities, so the final dashboard is a thin Streamlit app: zero
  extra containers, full rendering control, 100% committed code.
- **Determinism** — one master seed, per-entity PCG64 child streams, so
  every regeneration is bit-identical and all documented invariants hold.

## 5-minute demo script

1. `make run` (or point at a running stack) — show the compose stack green.
2. Open the **dashboard** (http://localhost:8501): read the KPI row
   (5,000 customers, 50,000 orders, $15.2M GMV, $313 AOV), the
   November/December seasonal bump in the revenue trend, the Zipf-skewed LTV
   distribution, churn/RFM mixes, and a few customer profiles.
3. Open **Dagster** → show the asset graph (bronze → silver → gold) and a
   finished run with **85/85 checks green**; point at a blocking check.
4. `docker exec trino trino --execute "SELECT ... FOR SYSTEM_TIME AS OF
   ..."` or `SELECT * FROM gold.customer_360` — Iceberg time travel.
5. Run `make verify` — five `RESULT: PASS` lines, ending on the dashboard.

## Build history

Built and documented phase by phase in [PROJECT.md](./PROJECT.md)
(Phases 0–10: scaffold → infrastructure → Dagster → synthetic data →
bronze → silver → gold → data quality → Streamlit dashboard → polish).

## License

MIT
