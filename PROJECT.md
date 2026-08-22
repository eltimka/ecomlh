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
Streamlit dashboard (live Trino queries, port 8501)

Streaming path (Phases 11-15 - DONE, see build order):
stream_producer.py (seeded, simulated clock)
        ↓
Kafka (topics: raw.web_events, raw.orders, raw.order_items, raw.payments)
        ↓
Flink (4 dumb-append jobs: Kafka consumer → Iceberg)
        ↓
bronze.stream_{web_events, orders, order_items, payments}
        ↓
silver.fct_{web_events, orders, order_items, payments} merges
          (batch ∪ stream, dedup on PK; replay = zero delta) ──▶ gold
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
| Dashboard          | Streamlit + Plotly        | Thin local app querying Trino live (v1 was Superset, see notes) |
| Streaming Bus      | Apache Kafka (KRaft, single node)    | Phase 11+15: 4 topics (web_events + money path) |
| Stream Processing  | Apache Flink + Iceberg Flink connector | 4 jobs (Kafka → Iceberg append, 8-slot TM) |
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
│   ├── generate_synthetic.py
 │   └── stream_producer.py          # Phase 11: Kafka producer (replay/live)
 ├── flink/                          # Phase 12
 │   ├── Dockerfile                  # flink + iceberg-flink + hive/hadoop clients + S3A
 │   ├── pom.xml                     # maven dependency closure for the image
  │   ├── hadoop/core-site.xml        # S3A/MinIO config (mounted, HADOOP_CONF_DIR)
  │   └── sql/                        # Flink SQL: 4 Kafka → Iceberg jobs
  │       ├── stream_web_events.sql
  │       ├── stream_orders.sql
  │       ├── stream_order_items.sql
  │       └── stream_payments.sql
├── dashboard/
│   ├── app.py               # Streamlit dashboard (KPIs + 6 charts + profiles)
│   └── marts.py             # the 11 gold-layer mart queries (single source of truth)
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

### Phase 9 – Dashboard (Streamlit; v1 was Apache Superset)
- v1 (superseded): Apache Superset in Docker, Trino as database, dashboard
  built deterministically via the REST API. The API-level checks all passed,
  but charts created via API could not be rendered by the 4.1 frontend
  (legacy-viz registry keys + form_data/query-context format mismatches).
- v2 (final): a thin **Streamlit** app in the local venv
  (`dashboard/app.py` + `dashboard/marts.py`):
  - queries the gold marts **live** through the same Trino client the
    pipeline uses - no extra container, no metadb, no REST bootstrap
  - 4 KPI cards (customers/orders/GMV/AOV), monthly revenue trend,
    LTV distribution, churn-risk pie, RFM segment mix, channel mix,
    category mix, top-25 customer profile table
  - 5-minute query cache + manual refresh button; friendly error if the
    stack is down
- Success criteria:
  - dashboard serving at http://localhost:8501 (started by `make run` / `make dashboard`)
  - KPI row shows the deterministic seed-42 values
  - `scripts/verify_dashboard.py` → RESULT: PASS (all 11 marts execute on
    Trino, KPI invariants hold, app health → 200)

### Phase 10 – Polish
- Good README with architecture diagram and how to run
- Clear instructions for Pi Agent users
- Environment variables documented
- One-command startup experience as much as possible

### Phase 11 – Streaming Bus: Kafka + Seeded Producer
Streaming scope is **web_events only** in v1 (orders/payments deferred -
Phase 15 backlog). Full Flink cluster, not Trino micro-batch - decision
recorded in "Streaming design decisions" below.
- Compose: + `kafka` (KRaft single node, port 9092; topic `raw.web_events`)
- `data_generator/stream_producer.py` - reuses the generator's seeded RNG:
  - `--replay`: full seeded web_events history at max speed; the emitted
    payload set is bit-identical to data/synthetic/web_events.parquet
  - `--live`: continues after max(event_date) on a simulated clock at a
    configurable rate - never wall clock (determinism story intact)
- Success criteria:
  - topic exists; a consumer reads the emitted messages
  - replay hash check passes (`scripts/verify_kafka.py`: hash of
    event_id+payload == generator dataset)
  - `make stream-up` / `make stream-down` manage the producer

### Phase 12 – Flink → Iceberg (stream bronze)
- Spike FIRST: the Flink Iceberg connector version must be compatible with
  the v2 metadata Trino 483 + HMS writes. Test both directions (Trino
  creates table → Flink appends → Trino reads back; Flink creates →
  Trino reads/appends) BEFORE building the rest.
- Custom Flink image (`flink/Dockerfile`): flink:1.2x-java17 +
  iceberg-flink-runtime + hadoop-s3 plugin (MinIO endpoint, path-style
  S3A, minioadmin creds)
- Compose: + flink jobmanager (:8081) + 1 taskmanager; starts after
  kafka + hive
- `flink/stream_web_events.sql` (Flink SQL): Kafka connector (JSON format)
  → APPEND into `iceberg.bronze.stream_web_events`, explicit
  s3a://bronze/stream_web_events location, partitioned by day(event_date).
  Append-only in Flink - dedup/conformance stays in silver.
- Success criteria:
  - Flink job RUNNING, checkpoints clean
  - rows in bronze.stream_web_events == messages emitted
    (`scripts/verify_stream.py`)
  - Trino can query the stream table (incl. time travel)

### Phase 13 – Silver Merge + DQ
- `fct_web_events` spec → UNION ALL of bronze.web_events +
  bronze.stream_web_events, dedup on event_id (ROW_NUMBER window); stat
  metadata gains stream_rows / duplicates_dropped. fct_orders /
  fct_payments unchanged in this scope.
- Gold unchanged: customer_360 web engagement (cart adds, last event)
  picks up merged events automatically; the 5-view revenue invariant is
  structurally untouched (no money path in v1).
- Dagster: virtual assets for the stream bronze table (ops query Trino for
  counts/metadata - no fake lineage edges); new check families:
  cross-source duplicate event_id, stream FK validity
  (customer_id in silver.dim_customers), event-time monotonicity + lag.
- Freshness: boundaries recomputed from all sources (stream rows carry
  newer event dates than DATA_DATE_*).
- Success: `make refresh` + `make verify` + `make verify-dq` green in
  replay mode (zero data delta - every replayed event dedups out) and in a
  short live window (conformance = bronze − drops + stream − dups).

### Phase 14 – Live Dashboard + Ops
- Dashboard "Live" panel: web event rate (events/min over last N minutes),
  latest-events table, short TTL cache (10-30s)
- Makefile: stream-up / stream-down / stream-live; `make run` orchestrates
  replay + Flink job start (idempotent); `make reseed` resets the topic
  (delete + recreate) then replays
- README + PROJECT.md: architecture diagram with the streaming path;
  demo script step "watch the live panel tick"
- Success criteria: `make run` end-to-end green + live demo works + all
  verify scripts PASS

### Phase 15 – Hardening + Money-path streaming (ALL COMPLETE)
- [x] Flink checkpoints to S3 (durability across restarts)
- [x] Consumer-lag check in Dagster (on the virtual stream asset)
- [x] expire_snapshots / partition maintenance for the stream tables
- [x] **Stream the money path** (Phase 15 item 4): topics
  `raw.orders` / `raw.order_items` / `raw.payments`, 3 new Flink jobs,
  3 new stream bronze tables, silver merges batch ∪ stream into
  `fct_orders` / `fct_order_items` / `fct_payments` (dedup on PK), live
  order groups on a simulated clock (order + items + one payment, FK-valid,
  cancelled ⇒ failed, no returned). Same pattern as Phases 11-14, applied
  to the higher-risk money path. 92 → 109 checks, 25 → 28 assets.

## Streaming Design Decisions (Phase 11+)

- **Full Flink, not Trino micro-batch.** Canonical Kafka → Flink → Iceberg
  real-time lakehouse; Trino's Kafka connector is batch-only and a
  scheduled INSERT...SELECT FROM kafka would be micro-batching, not
  streaming. Cost: +~3 GB RAM (Kafka + JM + TM) - document in README.
- **All 4 event types stream (Phase 15 item 4).** v1 was web_events only;
  the money path (orders + order_items + payments) was added later with the
  same pattern. order_items is REQUIRED, not optional: the blocking
  `revenue_invariant` compares 5 views including the item-based
  `revenue_by_category`, so live orders without items would break it and
  halt gold. Live orders are completed/cancelled only (no `returned` —
  refunds stay batch-only), cancelled ⇒ failed payment, total = Σ items.
- **Separate stream bronze tables.** The batch full-refresh DROP+CTAS must
  never race with live appends (same class of race as the Phase 10
  layer-gate fix). Flink writes bronze.stream_web_events; silver merges.
- **Silver is the merge point.** Dedup on event_id in silver SQL keeps the
  spec-driven layer the single source of truth for conformance; Flink
  stays a dumb append.
- **Determinism.** Replay mode is bit-identical to the batch dataset, so
  after replay + refresh every existing invariant holds with zero delta
  (all stream rows dedup out). Live mode = simulated clock continuing
  after max(event_date); no wall clock anywhere.
- **Virtual Dagster assets for stream bronze.** Flink materializes outside
  Dagster; virtual assets register counts/metadata via Trino queries to
  keep lineage in the UI, and the new stream DQ checks attach there.
- **Durable restarts (Phase 15).** Kafka source runs `scan.startup.mode=
  group-offsets` + `properties.auto.offset.reset=earliest`, checkpoints are
  durable at `s3a://flink-state/checkpoints` (10s interval, retained on
  cancellation). Net effect: a graceful stop/start resumes exactly where it
  left off (no re-append); a topic reset (reseed) has no group offsets and
  replays from earliest. Only an ungraceful kill can leave the stream bronze
  with at-least-once duplicates, which silver dedups and the
  `event_id_unique` check surfaces.

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

**Current status:** Phases 0-15 complete (scaffold, Docker Compose
infrastructure, bootstrap lakehouse, Dagster project setup, synthetic data
generator, bronze layer ingestion, silver layer transformations, gold
Customer 360 marts, data quality & observability, Streamlit dashboard
(v1 Superset, superseded), polish & one-command startup, Kafka streaming bus
+ seeded producer, Flink -> Iceberg stream bronze, silver merge + DQ,
live dashboard + ops, hardening + money-path streaming).

Phase 15 is fully complete, including item 4 (the money path): Flink
checkpoints to S3 / durable restarts, non-blocking consumer-lag Dagster
check, snapshot + orphan-file maintenance (`make maintain`), and streaming
orders + order_items + payments (4 topics, 4 Flink jobs, 4 stream bronze
tables, silver merges batch ∪ stream into the 3 money facts, live order
groups on a simulated clock). 28 assets, 109 checks. The five-view revenue
invariant holds with live money streaming (spread 0.0000). See the Phase 15
section, the "Phase 15 notes" entries and "Streaming design decisions"
above.

Phase 11 notes (streaming bus: Kafka + seeded producer) - complete:
- Compose: + `kafka` (apache/kafka:3.7.2, KRaft single node, 512M heap) with
  three listeners: PLAINTEXT -> localhost:9092 (HOST clients: producer,
  scripts), INTERNAL -> kafka:9094 (CONTAINER clients - Flink in Phase 12),
  CONTROLLER -> :9093 (KRaft quorum). The INTERNAL listener is required
  because the host listener's advertised address (localhost:9092) is not
  reachable from inside other containers. + one-shot `kafka-init` creates
  raw.web_events (3 partitions, RF 1, --if-not-exists) via the INTERNAL
  listener (same one-shot pattern as hive-jdbc-init).
  GOTCHA: the apache/kafka image does not put /opt/kafka/bin on PATH -
  healthcheck and kafka-init must call /opt/kafka/bin/kafka-topics.sh
  explicitly (healthcheck silently failed otherwise; broker was fine).
- data_generator/stream_producer.py (kafka-python 3.x in the venv):
  --mode replay emits data/synthetic/web_events.parquet as JSON payloads at
  max speed (~7k msg/s). Bit-identity is guaranteed by READING the Parquet,
  not by re-deriving the RNG. --mode live replays first (unless
  --skip-replay), then emits new events from a deterministic stream
  SeedSequence([DATA_SEED, 0x4C495645]): simulated clock continuing after
  max(event_date) (exponential inter-arrival, mean = history density),
  event_id continues after the history (EVT-00050001...), customer_id 70%
  sampled from the historical pool (FK-valid) / 30% null, --rate (default
  20/s) only paces wall-clock arrival. Content is a function of
  (seed, live-count) only - no wall clock anywhere. Flags: --reset-topic
  (delete + recreate; used by `make reseed`), --if-empty (idempotent replay;
  used by start_all.py so re-runs of `make run` don't double the topic).
  kafka-python 3.x API notes: KafkaAdminClient takes no `timeout=` config;
  TopicPartition imports from `kafka` (kafka.structs); Serializer/
  Deserializer are ABCs with serialize(topic, headers, data) /
  deserialize(topic, headers, data) - plain callables only warn.
- scripts/verify_kafka.py: broker/topic reachable, partition count == 3,
  full consume from offset 0, count == parquet rows, multiset sha256 over
  canonical payload fields == parquet hash (replay bit-identical to batch),
  event_ids exactly EVT-00000001..EVT-000NNNNN and unique. RESULT: PASS.
- Makefile: stream-up (live producer detached, .logs/stream_producer.{pid,log},
  refuses if already running), stream-down; `make down` also stops the
  producer; `make reseed` = reset-topic + replay before refresh; `make verify`
  includes verify_kafka. start_all.py: new step 4/7 (replay --if-empty),
  kafka in the healthy-service set, verify suite + final summary line.
- .env.example: KAFKA_BOOTSTRAP / KAFKA_TOPIC / STREAM_LIVE_RATE.
- Verified: full `make run` green (235s: all 6 verify scripts RESULT: PASS,
  lakehouse_refresh with all 85 checks green); `make verify` green; live mode
  tested (sequential ids after history, monotonic simulated clock, ~30%
  anonymous, exact 100 msg/s pacing, same-seed determinism, diff-seed
   divergence); reset + replay + re-verify green.

Phase 12 notes (Flink -> Iceberg stream bronze) - complete:
- Spike (both directions, per the phase plan) PASSED before building:
  Trino-created v2 table -> Flink INSERT -> Trino reads back; Flink-created
  table (explicit s3a:// location) -> Trino INSERT -> Trino reads back.
  Spike tables dropped + S3 dirs purged afterwards.
- flink/Dockerfile (3 stages, image ecommerce-flink:1.20.3):
  1. `apache/hive:3.1.3` - source of the Hive client jars. Iceberg 1.9.1's
     hive module references org.apache.hadoop.hive.metastore.* at runtime but
     declares NO hive dependency (verified in its poms). It is compiled
     against the Hive 3.x client API (HiveCatalog/CachedClientPool reference
     HiveConf.ConfVars.METASTOREURIS - a NoSuchFieldError with the Hive 4
     client), while the Thrift protocol talks fine to the Hive 4.0.1 HMS.
     The images thin hive-metastore-*.jar - the full client lives in
     hive-standalone-metastore-3.1.3.jar.
  2. maven: dependency closure of flink/pom.xml = iceberg-flink-1.20:1.9.1 +
     flink-sql-connector-kafka:3.4.0-1.20, plus:
     - flink-metrics-dropwizard:1.20.3 + metrics-core:4.2.30 -
       IcebergStreamWriterMetrics references the com.codahale.metrics 4.x
       API (Histogram/Reservoir/SlidingWindowReservoir) with no declared
       dependency; the 1.20.3 flink-dist ships only a zookeeper-shaded copy.
     - slf4j-api pinned to 1.7.36 - the closure otherwise pulls 2.0.x,
       which ignores the Flink image's 1.7-style log4j2 binding => all
       logs go to a NOP logger.
  3. flink:1.20.3-scala_2.12-java17 + closure + flink-s3-fs-hadoop-1.20.3
     (self-contained S3A; ships a TRIMMED hadoop-common) + Hadoop 3.3.6
     client jars (common, hdfs-client, mapreduce-client core/common,
     yarn-api - same Hadoop as the S3A jar) + the Hive 3.1.3 client set
     (standalone-metastore, common, storage-api, libthrift, libfb303, guava,
     commons-lang2).
- Compose: + flink-jobmanager (healthcheck on :8081, 1600m) +
  flink-taskmanager (2 slots). GOTCHA x3 found by tcpdump/thread-dumps:
  1. HADOOP_CONF_DIR=/opt/flink/conf env - Flink 1.20's classpath is lib/*.jar
     ONLY; conf/ is only reachable via HADOOP_CONF_DIR (appended by
     config.sh), otherwise Hadoop's Configuration never sees core-site.xml.
  2. fs.s3a.endpoint must carry the scheme: `http://minio:9000`. S3A
     defaults to HTTPS for scheme-less endpoints - a TLS ClientHello to the
     plain-HTTP MinIO port yields "400 Bad Request" and the AWS SDK retries
     FOREVER (silent hang; no exception surfaces in Flink).
  3. The default compose network was renamed to `lakehouse-net` (no
     underscores): the Hive 3.x client canonicalizes the HMS host to
     <container>.<network> and parses it as a java.net.URI, which rejects
     underscores (URISyntaxException). Affects Flink only - Trino's shaded
     client never canonicalizes.
- flink/sql/stream_web_events.sql: CREATE CATALOG (hive) + table
  (format-v2, explicit s3a:// location) + Kafka TEMPORARY TABLE
  ('properties.bootstrap.servers'=kafka:9094 - the INTERNAL listener;
  'properties.group.id'=bronze.stream_web_events - Flink 1.20 names it
  properties.group.id, not group.id; 'format'=json; earliest-offset) +
  INSERT. Mirrors the batch bronze tables (same schema + explicit s3a://
  location rule).
  PARTITIONED BY (event_date): Flink's Iceberg catalog supports only
  IDENTITY partitioning in DDL (FlinkCatalog.toPartitionSpec - a TODO for
  transforms), but day(x) == identity(x) physically for DATE columns.
- OOM lesson (costly to find): PartitionedDeltaWriter holds ONE OPEN
  parquet/zstd writer per active partition until a checkpoint closes them.
  The 50k-event replay spans ~340 distinct event_date partitions => ~340
  open writers before the first 10s checkpoint => OOM on the 537MB task
  heap (2048m process) => infinite earliest-offset restart loop. Fix:
  taskmanager.memory.process.size 4096m (~1.1GB task heap). Scales with
  distinct-partition-count, not data volume.
- Makefile: flink-up (REST /jobs/overview - job active? submit via
  sql-client.sh -f, idempotent), flink-down (flink cancel by job name).
  `make reseed` now: flink-down -> reset_stream.py -> topic reset + replay ->
  refresh -> flink-up -> verify. `make verify` += verify_stream.
- scripts/reset_stream.py: cancel job (REST DELETE), DROP TABLE via Trino
  (also removes the S3 location), best-effort MinIO prefix purge.
- scripts/verify_stream.py: job RUNNING (must pick the ACTIVE entry - the
  overview lists job history incl. cancelled jobs), checkpoints
  completed>=1 / failed==0, table row count catches up with the topic's
  end-offsets (polled), no duplicate event_ids (one clean replay), full
  history coverage (distinct >= parquet rows, min == EVT-00000001).
- start_all.py: 8 steps - new step 5 = submit stream job if not active,
  wait for RUNNING + 12s stability; flink-jobmanager in the healthy set;
  verify suite + banner line.
- Verified: production job ingested the full 50k replay (50,000 rows,
  50,000 distinct, 70 checkpoints, 0 failed); `make reseed SEED=42` fully
  green (stream reset + batch refresh + all verifies); `make run` fully
  green (325s, 7/7 verify scripts PASS).

Phase 13 notes (silver merge + DQ) - complete:
- Virtual asset bronze/stream_web_events (assets/bronze/__init__.py):
  read-only observer for the Flink-written table (no writes, no fake
  lineage). Op: exists? (iceberg.information_schema.tables) -> if not,
  dg.Failure with the actionable fix (`make flink-up`); if yes, rows /
  distinct event_id / min-max event_date as run metadata (0 rows = first
  checkpoint pending, reported not failed). Joins the bronze layer, so the
  bronze gate waits for it and silver's fct_web_events gets a REAL upstream
  dep (deps = ["web_events", "stream_web_events"]).
- silver fct_web_events spec: UNION ALL of bronze.web_events +
  bronze.stream_web_events (identical schemas), dedup on event_id via
  row_number() OVER (PARTITION BY event_id ORDER BY src_rank) where batch =
  1, stream = 2. On a duplicate the BATCH row wins: both sources carry the
  identical payload per event_id and there is no arrival timestamp, so the
  source rank exists only to make the dedup deterministic (documented in the
  spec description). Stats: + stream_rows (pre), + duplicates_dropped
  (post = in-batch + in-stream - out). Output schema unchanged, so gold is
  untouched (customer_360 web engagement picks up merged events
  automatically).
- Ordering constraint (the phase's main gotcha): silver now READS the stream
  table, so the stream must be committed AND caught up BEFORE any refresh.
  * `make reseed` reordered: flink-down -> reset_stream -> topic
    reset + replay -> flink-up -> verify_stream (catch-up gate, polls) ->
    refresh -> verify.
  * start_all.py: 9 steps - new step 6/9 runs verify_stream.py as the
    catch-up gate between Flink submit and refresh.
  * RACE FOUND: verify_stream.py asserted "checkpoint completed >= 1"
    BEFORE its catch-up wait - a freshly submitted job legitimately has
    zero completed checkpoints. Fixed by moving the checkpoint assertion
    AFTER the catch-up poll.
- 6 new asset checks (85 -> 91; verify_dq: 25 assets = 22 tables + 3 gates,
  22 unit tests):
  1. bronze gate web_events_cross_source_dups - event_ids in BOTH sources;
     new condition "info" (always passes, reports the count: 50,000 in
     replay, the replayed prefix in live).
  2. bronze/stream_web_events event_id_unique - zero, NON-blocking: Flink is
     at-least-once without persistent checkpoints in v1, so a job restart
     re-appends events; silver absorbs them, this check surfaces them.
  3. bronze/stream_web_events date_range - new range_kind "stream":
     mn >= DATA_DATE_START AND mx >= DATA_DATE_END, NO upper bound (live
     events extend the range); empty table (min/max NULL) -> fail
     ("no committed rows yet"), non-blocking.
  4. silver gate stream_customers_fk - every non-null stream customer_id in
     silver.dim_customers (nulls are anonymous by design - the generic
     _ref_sql would have counted them as orphans).
  5. silver gate web_events_merge_reconcile - fct count == distinct
     event_ids across both bronzes; boolean_true, NON-blocking: with a live
     producer, rows can land between the CTAS and the check (the blocking
     fct_web_events pk_unique check remains the correctness gate).
  6. silver/fct_web_events date_range - same "stream" kind on the merged
     table (freshness boundaries recomputed from all sources).
- verify_silver.py: fct_web_events conformance = distinct event_ids across
  batch + stream (not the batch row count anymore) + superset check
  (merged >= batch) + merge info line (batch 50,000 + stream 50,000 ->
  50,000 distinct, 50,000 deduped in replay).
- verify_kafka.py made LIVE-aware: strict equality (count == history, full
  multiset hash, exact id sequence) only in replay mode; with a live suffix
  it checks count >= history, the multiset hash of the REPLAY SUBSET (ids in
  the history set) vs the Parquet, and that live ids uniquely continue the
  sequence (EVT-000NNNN1..). A double replay or second live run fails.
- Verified:
  * replay (zero delta): `make reseed SEED=42` green - fct_web_events
    50,000 from 100,000 merged rows, 50,000 deduped, all gold invariants
    unchanged ($15,221,141.27).
  * live window: 300 live events (EVT-00050001..300, simulated clock
    2025-01-01/02) -> refresh green (91 checks), fct_web_events 50,300,
    gold.customer_360 last_event_date advanced to 2025-01-02 for 115
    customers, GMV invariants untouched.
  * `make verify-dq`: 22 unit tests + negative test + full run 91/91 checks,
    25/25 assets. `make run` green (280s, 9 steps, 7/7 verifies). Final
    `make reseed SEED=42` restored the canonical dataset.

Phase 14 notes (live dashboard + ops) - complete:
- Dashboard "Live stream (web events)" view: a sidebar radio toggle alongside
  the existing gold view. The gold filters are now only rendered/queried when
  the gold view is selected (they were unconditionally hitting Trino before).
  The live panel is a `@st.fragment(run_every="15s")` so it polls on its own
  without re-running the gold page.
- Data access in `dashboard/live.py` (new module, all read-only, each function
  degrades to an `error` field instead of raising so the panel renders a
  friendly failure and the gold view is unaffected):
  * `flink_job_status()` - Flink REST `/jobs/overview` + `/jobs/{jid}/checkpoints`
    (RUNNING state, checkpoints completed, last checkpoint duration).
  * `kafka_topic_end_offset()` - sum of end offsets over the topic's
    partitions (the producer-side high-water mark).
  * `stream_counts()` - batch/stream/merged row counts + newest event_id/date.
  * `latest_events()` - newest 15 stream-bronze rows (event_id = arrival order).
- Panel content: Flink job state + checkpoint progress; committed rows vs
  Kafka end offset (delta = "caught up" / "N uncommitted"); a self-measured
  event rate (row-count samples kept in `st.session_state`, ~events/min over
  the gap between refreshes); last event id + date; batch/stream/merged source
  counts (delta = "N deduped on event_id"); latest-events table; last-updated
  caption.
- Makefile: `dashboard` target now passes `--server.headless true` (it had
  been prompting for onboarding email and exiting non-zero when run from a
  TTY).
- `make stream-up` gained `--if-empty` on the live producer: it only replays
  the history preamble when the topic is actually empty, so starting a live
  feed on top of an already-seeded topic does not double the history.
- Live-window DQ: a running producer means the merged silver fact was
  materialized at the last refresh, i.e. BEFORE newer live rows committed, so
  strict `fct_web_events == union(batch, stream)` can be transiently false.
  * verify_silver.py: samples the stream row count 3s apart; if it grew
    ("live producer running") it re-samples the union and BOUND-checks
    `batch_distinct <= fct_web_events <= current_union` instead of equality.
    Static (no live producer) keeps the exact check.
  * verify_dashboard.py: samples committed count BEFORE the topic end offset
    (both monotonic, committed <= produced) so a stale end-offset sample can't
    break the comparison under a live producer.
- verify_dashboard.py gained a "live stream panel data sources" section
  (7 checks): Flink job RUNNING, Kafka end offset readable, source counts
  readable, merged >= batch (superset), merged <= batch+stream (dedup bound),
  committed <= end offset, latest-events query returns unique event_ids.
 - Ops note (SUPERSEDED by Phase 15 item 1): at this stage a stopped-and-
   restarted Flink job re-consumed from the topic start (the DDL was pinned to
   `scan.startup.mode=earliest-offset` and checkpoints lived on the container's
   ephemeral filesystem) and re-appended the history, so the stream table
   briefly held duplicates that silver deduped away. `make reseed SEED=42` is
   the canonical reset (resets job + table + topic, re-replays, re-verifies)
   and was re-run to leave the stack in the clean 50k state.
- Verified:
  * `make run` end-to-end green (9 steps, 7/7 verify scripts PASS) with the
    live panel data sources checked.
  * Live tick demo: `make stream-up` against a seeded topic (no double
    replay); `stream_counts().stream_rows` and `last_event_id` advance in
    real time (50,000 -> 50,348+), Flink job RUNNING with checkpoints
    completing; AppTest renders both views (gold default + live toggle) with
    no exceptions and the live metrics present.
  * Full live-window `make verify`: 7/7 PASS with a live producer running
    (26,000+ live events, silver bound-check, dashboard ordering check).
  * Final `make reseed SEED=42` restored the canonical 50k/50k/50k dataset,
    7/7 PASS.

Phase 15 notes - item 1 (Flink checkpoints to S3, durable restarts) - complete:
- Root cause of the re-consume-on-restart behavior: the Kafka source DDL was
  pinned to `scan.startup.mode=earliest-offset`, so EVERY (re)start re-read
  the whole topic, and checkpoints lived at `file:///opt/flink/checkpoints`
  (JM container filesystem - wiped on container recreation).
- Fix (flink/sql/stream_web_events.sql):
  * `scan.startup.mode=group-offsets` - a restarted job resumes from the
    consumer-group offsets that Flink commits on graceful cancellation, so no
    re-consumption.
  * `properties.auto.offset.reset=earliest` - REQUIRED companion: after
    `make reseed` the topic is deleted+recreated and the group has no
    committed offsets; without a reset policy the consumer crash-loops with
    NoOffsetForPartitionException (found during the reseed test). With it, a
    fresh topic falls back to earliest = full replay, exactly what the
    rebuilt table needs.
- Durable checkpoints (docker-compose.yml, flink-jobmanager):
  `state.checkpoints.dir: s3a://flink-state/checkpoints` (new `flink-state`
  MinIO bucket, created by bootstrap_minio.py / MINIO_BUCKET_FLINK),
  `execution.checkpointing.externalized-checkpoint-retention:
  RETAIN_ON_CANCELLATION` so cancelled jobs keep their checkpoint dirs.
  S3A goes through the flink-s3-fs-hadoop plugin (already in the image) + the
  mounted core-site.xml; TMs write checkpoint parts through the same config.
- Verified:
  * restart-resume: flink-down (graceful cancel commits group offsets) ->
    JM container recreated -> flink-up -> table stays exactly 50,000 rows
    (previously ballooned to 100,000 with duplicates). Proven twice,
    including on a freshly rebuilt dataset.
  * rebuild: `make reseed SEED=42` end-to-end green (topic reset -> earliest
    fallback -> clean 50,000 replay, 0 duplicates, all 7 verify scripts PASS).
  * checkpoints: 10+ completed at 10s interval under s3a://flink-state/
    checkpoints/<jobId>/, stale chk dirs auto-cleaned, retained after
    cancellation (both jobs observed).
- The Phase 13 bronze check `event_id_unique` (non-blocking) now acts as
  defense-in-depth: with durable restarts, within-stream duplicates should
  only appear after an UNGRACEFUL kill (SIGKILL/power loss mid-checkpoint).

Phase 15 notes - item 2 (consumer-lag Dagster check) - complete:
- New non-blocking asset check `consumer_lag` on `bronze.stream_web_events`
  (assets/checks.py): committed rows (existing count query) vs the Kafka
  topic end offset (`_kafka_end_offset()`, kafka-python
  KafkaConsumer.end_offsets over the topic's partitions; None when Kafka is
  unreachable -> the check reports unavailable instead of failing). Pass =
  `abs(lag) <= LAG_TOLERANCE` (default 1000, env STREAM_LAG_TOLERANCE).
  Non-blocking: a transient lag or Kafka outage must never take the batch
  pipeline down - it is a stream-health signal, not a correctness gate.
- Counts: 91 -> 92 checks (verify_dq.py EXPECTED_CHECKS, definitions.py,
  Makefile comment, README, dashboard footer). 5 new pure unit cases for the
  `kafka_lag` condition in verify_dq.py (patched `_kafka_end_offset`, no I/O):
  caught-up, within tolerance, beyond tolerance, end offset unavailable,
  committed count missing.
- Verified: `make verify-dq` green (92/92 checks, 25/25 assets, 27 unit
  tests); live check reported "lag +0 (topic end_offset 50,000, committed
  50,000, tolerance +/-1,000)".

Phase 15 notes - item 3 (snapshot + orphan-file maintenance) - complete:
- `scripts/maintain_iceberg.py` (+ Makefile `make maintain`): per Iceberg
  table, `ALTER TABLE ... EXECUTE expire_snapshots` (skipped below 3
  snapshots) then `remove_orphan_files`, with a MinIO size report
  (bronze+silver+gold) before/after. Defaults: snapshots < 1h, orphans < 2h
  (env MAINTAIN_SNAPSHOT_AGE / MAINTAIN_ORPHAN_AGE).
- Trino 483 syntax note: these run as `ALTER TABLE ... EXECUTE` commands -
  the older `CALL system.expire_snapshots(...)` procedure form is NOT
  registered in this version ("Procedure not registered"). The connector's
  default retention floor is 7d; the script lowers it per session
  (`SET SESSION iceberg.*_min_retention = '10m'`), so no global catalog
  config change is needed.
- Safety margin: retention thresholds must stay far above the Flink
  checkpoint interval (10s) - in-flight, not-yet-committed stream files are
  seconds old, so a 2h orphan retention can never touch a file the running
  job still needs.
- Empirical finding: in this stack DROP TABLE + CTAS leaves NO orphan files
  (verified by full MinIO inventory vs `table$files` across all tables: every
  parquet is referenced; the only extras are the `raw/*` landing files
  used by the bronze_raw Hive tables). Trino's DROP removes the table's
  data files. So the real maintenance problem is SNAPSHOT accumulation: the
  stream table gains a snapshot on every Flink checkpoint even while idle
  (observed 26 -> 28 in a few minutes with no traffic). Orphan removal is
  still kept as the cleanup for the one scenario that does produce orphans:
  an ungraceful kill of the job mid-checkpoint (files written, never
  committed).
- Non-Iceberg tables (bronze_raw.* Hive landing tables, which the iceberg
  catalog's information_schema still lists) are detected via the
  "Not an Iceberg table" / missing `$snapshots` error and skipped with a
  note; information_schema/system schemas are excluded from the table list.
- Verified: proof run with 10m/10m retention expired 22 stream snapshots
  (28 -> 6), MinIO 10.3 -> 10.2 MB, orphan scan classified all files
  (deleted 0 - none exist); afterwards the Flink job stayed RUNNING and the
  stream table still read exactly 50,000 rows; `make verify` 7/7 green.
- Bug found while testing: dashboard live.py `flink_job_status()` picked the
  FIRST name-matching job from /jobs/overview, and after several reseed
  cycles the list holds stale CANCELED runs before the RUNNING one - the
   live panel (and verify_dashboard) then reported state=CANCELED. Fixed to
   prefer a RUNNING match.

Phase 15 notes - item 4 (stream the money path) - complete:
- Scope: 3 new Kafka topics (raw.orders / raw.order_items / raw.payments),
  3 new Flink jobs, 3 new stream bronze tables (stream_orders /
  stream_order_items / stream_payments), silver merges batch ∪ stream into
  fct_orders / fct_order_items / fct_payments. order_items is REQUIRED: the
  blocking revenue_invariant compares 5 views including the item-based
  revenue_by_category, so live orders without items would break it and halt
  gold. Refunds stay batch-only (live orders are never 'returned').
- Producer (data_generator/stream_producer.py) generalized from 1 topic to
  4, driven by a STREAMS list: replay emits all 4 Parquet histories (each
  bit-identical to its file). Live mode interleaves the web-event stream
  (20/s) and the live ORDER GROUPS (2/s) on one wall-clock timeline; each
  group = 1 order + 1-5 items + exactly 1 payment sharing the same
  simulated order_date. Determinism: SeedSequence([seed, 0x4C4F5244]); ids
  continue the batch sequences (ORD-/OI-/PAY-), customer sampled from the
  historical order-customer pool, product from the historical product pool
  with the batch ±2% price jitter (FK-valid), total = Σ line_total,
  cancelled ⇒ failed payment (else 97% succeeded / 3% pending), no returned.
  --if-empty became per-topic. Bug found: live_order_stream needed the
  products frame but main() only loaded the 4 stream Parquet files ->
  KeyError 'products'; fixed by loading products.parquet in live mode.
- Flink: 3 new SQL files (same DDL pattern as stream_web_events.sql:
  group-offsets + earliest fallback, 10s S3 checkpoints). stream_order_items
  is UNPARTITIONED (item rows carry no date column - one always-open write
  partition, cheap on checkpoint memory); the other two partition by their
  date column. Compose TM: 2 slots/4GB -> 8 slots/11GB so all 4 jobs'
  (source + sink) tasks fit (each open parquet/zstd writer per date
  partition is the memory driver).
- kafka-init now loops over the 4 topics (GOTCHA: the shell var must be
  escaped as $$t in compose, or compose interpolates it to empty and
  kafka-topics.sh fails with "topic name '' cannot be represented").
- scripts/stream_spec.py: new single source of truth (topic / flink job
  name / stream table / history parquet / PK / first id per stream) shared
  by reset_stream.py, verify_stream.py and verify_kafka.py - all three
  generalized from 1 to 4 streams. Makefile flink-up/down loop over the 4
  job names (job name -> sql file via ${JOB##*.}.sql).
- Silver (assets/silver/sql.py): the 3 money facts now CTE-merge batch ∪
  stream (ROW_NUMBER PARTITION BY pk ORDER BY src_rank, batch wins) before
  the existing joins/enrichment; fct_orders aggregates item counts across
  BOTH item sources; fct_payments dedups batch-wins-then-lowest-payment_id.
  pre/post stats gained stream_rows / duplicates_dropped. fct_refunds stays
  batch-only.
- Dagster (assets/bronze/__init__.py): the hardcoded web_events virtual
  asset became a _stream_asset(name, pk, date_col) factory; 4 virtual
  stream assets registered (assets 25 -> 28).
- DQ (assets/checks.py): 92 -> 109 checks. New: per-stream <pk>_unique +
  consumer_lag (3) + date_range (2) + <t>_cross_source_dups (3) on bronze;
  per-fact <fct>_merge_reconcile (3) + stream FKs (2) + fct_payments
  date_range (1) on silver. Live-aware: fct_orders.date_range full ->
  stream kind; gold freshness max_month == -> >= (live tail months allowed);
  monthly_anomaly now computes z over historical months only (WHERE month
  <= end-month) - a partial live tail month would always look anomalous.
- Verify scripts made live/money-aware: verify_kafka (4 topics),
  verify_stream (4 jobs/tables), verify_silver (union-based expected for all
  4 merged facts + per-stream live detection + bounds; GMV check now
  merged-bronze vs silver - batch-only bronze vs merged silver was the
  exact live delta), verify_gold (12 -> >=12 months), verify_dashboard
  (orders == -> >= manifest; 12 -> >=12 trend months).
- Dashboard: live.py flink_job_status()/kafka_topic_end_offset() take a
  job/topic argument; new latest_orders() + money_stream_summary(); the Live
  view gains a money-path section (per-stream table: job state, Kafka end
  offset, committed, uncommitted; + latest-15 orders dataframe).
- Verified: `make reseed SEED=42` from clean state (reset 4 topics, 4
  rebuilt tables, 4 jobs caught up, canonical 50,000 orders, `make verify`
  7/7); live mode (8 live order groups -> Kafka -> Flink -> 4 stream tables
  -> silver fct_orders 50,008 -> gold, revenue_invariant spread 0.0000
  across all 5 views, `make verify` 7/7); 109/109 checks + 28 assets
  (verify_dq constants).

Phase 9 notes (v2: Streamlit - final dashboard):
- dashboard/marts.py = the 11 gold-layer mart queries (single source of
  truth for the dashboard SQL); dashboard/app.py renders 4 KPI cards +
  6 Plotly charts + a top-25 customer profile table, querying Trino live
  (same trino client as the Dagster TrinoResource). st.cache_data(ttl=300)
  + sidebar refresh button.
- Start: `make dashboard` (foreground) or via `make run` (start_all.py
  step 4 launches it detached on :8501, pid in .logs/dashboard.pid, log in
  .logs/dashboard.log; `make down` stops it). Health: /_stcore/health.
- verify_dashboard.py: executes all 11 marts on Trino, asserts the
  deterministic seed-42 invariants (customers 5,000; orders 50,000;
  GMV 15,221,141.27; 12 trend months; 3 channels; 8 categories), then
  checks the app health endpoint (auto-starts a throwaway headless
  instance if the dashboard is not running, and stops it afterwards).
- Interactivity (post-v2): the sidebar filters (order channel,
  acquisition channel, churn risk, month range) + customer search are
  pushed down into the mart SQL (dashboard/marts.py build_sql(name,
  filters) - the 11 marts became small SQL builder functions; the
  unfiltered form is what the verifier uses). Verified headlessly:
  removing the 'web' channel drops GMV 15,221,141.27 -> 6,889,883.44;
  search 'C-000001' returns exactly that customer.
  Trino gotchas found: no ILIKE (use lower(...) LIKE lower pattern via
  python-side .lower()), month is DATE (needs DATE '...' literals), and
  order channels (mobile/pos/web) vs customer acquisition channels
  (direct/email/organic_search/...) are different dimensions - filtered
  separately.
- Re-seeding: `make reseed SEED=<n>` = delete data/synthetic +
  generate_synthetic.py --seed <n> + make refresh + make verify.
  verify_dashboard.py is seed-aware: row counts come from the manifest,
  GMV is a cross-layer check (gold revenue_by_channel vs silver
  fct_orders valid-order sum) instead of a hardcoded seed-42 value.
  Tested: seed 123 -> GMV $18,101,425.63 / AOV $373.56 (vs $15.2M /
  $313.38 for seed 42); `make reseed SEED=42` restores bit-identical data.
- Why not Superset (see v1 notes below for the deep dive): API-created
  charts cannot be rendered by the 4.1 frontend - the dashboard grid
  rebuilds each chart's query from `form_data` and looks the viz up in a
  plugin registry keyed by legacy snake_case keys (`big_number`, `bar`,
  `pie`); the 4.x form_data contract (dimensions in `columns`, ad-hoc
  metric objects in `metrics`) is not documented and diverges from the
  backend query-context contract; the legacy `big_number` viz also forces
  `is_timeseries: true` (trendline) which requires a datetime column. The
  backend was fine all along (full query contexts returned correct data) -
  it was the 4.1 frontend's legacy-viz path that was unfriendly to
  API-created charts. For a 100% local portfolio project, a thin
  Streamlit app is simpler and fully under our control.

Phase 9 notes (v1: Apache Superset - superseded, kept for the lessons):
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
- Makefile: run / up / down / bootstrap / refresh / dev / dashboard /
  verify / verify-dq / logs / clean. `make run` = scripts/start_all.py:
  compose up + wait healthy -> generate data if missing -> bootstrap
  MinIO -> start Streamlit dashboard (:8501) -> lakehouse_refresh job
  (all checks) -> quick verification suite (5 scripts). Whole flow:
  ~3 min on a warm machine.
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
