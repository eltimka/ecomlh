# E-Commerce Customer 360 Local Lakehouse

A fully local, open-source **Customer 360 data pipeline** built as a modern lakehouse.

This project demonstrates a production-style data engineering stack that runs entirely on your laptop — no cloud services required.

## What This Project Shows

- End-to-end data pipeline (ingestion → transformation → analytics)
- Medallion architecture (Bronze → Silver → Gold)
- Lakehouse design with Apache Iceberg
- Software-defined assets with Dagster
- SQL analytics with Trino
- Customer 360 modeling (LTV, RFM, churn risk, etc.)
- Local S3-compatible storage with MinIO
- Clean, interview-ready portfolio project

## Architecture

```
Synthetic E-commerce Data
        ↓
Dagster Assets (Python + Polars)
        ↓
Iceberg Tables on MinIO (S3-compatible)
        ↓
Hive Metastore
        ↓
Trino Query Engine
        ↓
Gold Customer 360 Marts
        ↓
Streamlit Dashboard
```

## Tech Stack

| Component          | Tool                      |
|--------------------|---------------------------|
| Orchestration      | Dagster                   |
| Object Storage     | MinIO                     |
| Table Format       | Apache Iceberg            |
| Metastore          | Hive Metastore + Postgres |
| Query Engine       | Trino                     |
| Transformations    | dbt-trino / Trino SQL     |
| Processing         | Polars + PyArrow          |
| Dashboard          | Streamlit                 |
| Infrastructure     | Docker Compose            |

## Project Structure

```text
ecommerce-customer-360-lakehouse/
├── docker/                  # Docker Compose + Trino/Hive configs
├── dagster_project/         # Dagster assets & resources
├── dbt/                     # dbt models (optional)
├── data_generator/          # Synthetic data generator
├── dashboard/               # Streamlit app
├── scripts/                 # Bootstrap & helper scripts
├── PROJECT.md               # Detailed build plan (for AI agents)
└── README.md
```

## Prerequisites

- Docker & Docker Compose
- Python 3.11+
- Git

## Quick Start (once built)

```bash
# 1. Start the lakehouse infrastructure
cd docker
docker compose up -d

# 2. Bootstrap MinIO buckets and Trino schemas
python ../scripts/bootstrap_minio.py

# 3. Start Dagster
cd ../dagster_project
dagster dev

# 4. Materialize assets from the Dagster UI (http://localhost:3000)

# 5. Run the dashboard
cd ../dashboard
streamlit run app.py
```

## Key Features

- **Fully local** – MinIO replaces AWS S3
- **Modern table format** – Apache Iceberg with ACID transactions and time travel
- **Asset-based orchestration** – Dagster with clear lineage
- **Customer 360 models** – Lifetime Value, RFM segmentation, churn indicators
- **Reproducible** – Synthetic data generator with seed support

## Development Guide

For step-by-step construction of this project (especially when using AI coding agents like Pi), see:

**→ [PROJECT.md](./PROJECT.md)**

That file contains the exact phased build plan.

## License

MIT

---

Built as a portfolio project to demonstrate modern data engineering skills using only open-source tools.
