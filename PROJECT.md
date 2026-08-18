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
Streamlit / Superset dashboard
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
| Dashboard          | Streamlit (primary)           | Optional: Superset later                   |
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
│   └── app.py
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

### Phase 9 – Dashboard
- Streamlit app that connects to Trino and visualizes:
  - Customer 360 overview
  - LTV distribution
  - Churn risk
  - Revenue trends
  - Key metrics

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

**Current status:** Ready to begin Phase 0.

Start by creating the project scaffold and then the Docker Compose infrastructure (Phase 1).
