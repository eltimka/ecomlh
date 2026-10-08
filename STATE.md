# Project state — handoff snapshot

_Captured 2026-10-08, after a full fresh-cold-start validation of every
component with **Garage** as the S3-compatible object store (MinIO removed);
same day the stack was brought down with `make down` (data volumes kept)._

## Where things stand

Everything rebuilt and re-validated from a truly clean slate (all
volumes/data wiped, then `make up` → bootstrap → seed → replay →
flink-up → refresh → verify, plus the ops demos). All gates green:

- `lakehouse_refresh` Dagster job: **109/109 asset checks PASS**
- `verify_lakehouse / kafka / stream / bronze / silver / gold / dashboard`
  — all **`RESULT: PASS`**
- Cold-start path proven: a fresh `make run` no longer dies with
  `InvalidObjectException(database hive.bronze)` (see "session fixes" —
  medallion schemas are now created by bootstrap)
- `make reseed SEED=123` validated end-to-end (Kafka topic reset,
  `reset_stream.py`'s S3 client against Garage, Flink down/up, full
  re-materialization: GMV $18,101,425.63 ≠ canonical, as expected),
  then `make reseed SEED=42` restored the canonical dataset:
  5,000 customers / 50,000 orders / GMV **$15,221,141.27**
- `make down`'s dashboard kill verified: process-group kill actually
  stops Streamlit (the stale-pid bug from the previous snapshot is fixed)

## Current system state (as left)

- **Stack down** (stopped with `make down`): containers and network
  removed, all data volumes kept (garage-meta/-data, postgres-data,
  kafka-data, hive-auxjars); dashboard and stream producer stopped
  (no pid files in `.logs/`).
- **Canonical seed-42 data loaded**: bronze/silver/gold re-materialized
  and verify suite green as of the last run before shutdown.
- Data lives in the `garage-data` volume (`/data` in the container);
  a full wipe is `docker compose down -v` + `rm -rf data/synthetic .logs`
  + Trino schema drop — i.e. start over from `make up && make bootstrap`.

## Resuming

```bash
# stack is stopped (data volumes intact):
make up          # compose up (7 services, garage healthgated)
make bootstrap   # idempotent: Garage buckets + Trino medallion schemas
make run         # full pipeline: seed 42, replay, flink-up, refresh, verify
make verify      # quick re-check without re-materializing
make reseed SEED=<n>   # full reset+replay+re-materialize (42 = canonical)
make stream-up   # live producer demo (stream-down to stop)
make down        # stop everything, incl. dashboard process group
```

## Session fixes (uncommitted)

1. **MinIO → Garage** (`dxflrs/garage:v2.4.1`, pinned):
   - Single-node mode via `docker/garage/garage.toml`
     (`s3_region = "garage"`) + `server --single-node --default-bucket`;
     cluster + access key auto-configured from `GARAGE_DEFAULT_*` env.
   - Ports **3900 (S3) / 3903 (admin)** replace 9000/9001.
   - Trino catalogs: native S3 (SDK v2) → `s3.endpoint=http://garage:3900`,
     `s3.region=garage`, path-style.
   - Hadoop S3A (Hive Metastore + Flink): endpoint/credentials updated in
     both `core-site.xml` files.
   - `bootstrap_minio.py` → `bootstrap_storage.py`; `resources/minio.py`
     → `resources/s3.py`; resource key `minio` → `s3`.
   - `reset_stream.py` keeps the `minio` **python package** as its S3
     client (generic S3 SDK; validated against Garage by the reseed).
2. **Cold-start crash**: medallion schemas (`iceberg.bronze/silver/gold`)
   were only created by `verify_lakehouse.py`, which runs *after* Flink
   job submission → `InvalidObjectException(database hive.bronze)`.
   `bootstrap_storage.py` now creates the three schemas via Trino after
   bucket creation, so `make run` works on a truly fresh stack.
3. **Garage SigV4 region scope** (the non-obvious one): Garage validates
   the region in the S3 `credential scope` against its `s3_region` and
   rejects mismatches with `400 ... unexpected scope '.../us-east-1/s3',
   expected '.../garage/s3'` — MinIO did not. Hadoop S3A signs
   `us-east-1` by default; the failing caller was **hive-metastore**
   validating table locations at `CREATE TABLE`. Fix:
   `fs.s3a.endpoint.region=garage` in both `core-site.xml` files
   (property exists in hadoop-aws 3.3.6, which the Hive 4.0.1 image
   bundles). Trino's native S3 connector already signed with `garage`
   via `s3.region` and never hit this.
4. **Stale dashboard pid on `make down`** (carried over from the
   2026-08-22 snapshot): `stop_dashboard()` now kills the **process
   group** (`os.killpg`), and the Makefile `down` target uses
   `kill -15 -$(cat .logs/dashboard.pid)` with a single-pid fallback.
   Verified both paths actually terminate Streamlit.

## Garage quirks (remember if storage changes again)

- **SigV4 scope region must match `s3_region`** (fix #3 above). If you
  ever switch `s3_region` to `us-east-1`, S3A needs no extra property;
  Trino needs `s3.region=us-east-1`.
- **Anonymous (unsigned) S3 requests hang indefinitely** (curl HEAD
  hung 120 s). All stack clients sign, so it's harmless — but never
  "test" Garage with an unsigned curl.
- **Distroless image**: only the `/garage` binary, no shell/curl.
  Healthcheck is `["CMD", "/garage", "status"]`; exec-based probing
  won't work.
- Fallback if S3A ever regresses: Iceberg `'io-impl'` → S3FileIO, or
  set `s3_region = "us-east-1"` everywhere.

## Known issues / open items

- The Flink startup checkpoint race (harmless first-trigger abort) can
  still occur on fresh submissions; `verify_stream.py` tolerates it,
  Flink UI may show lifetime `failed=1`. Cosmetic, no data impact.
- Remaining `minio` string refs are intentional: the compose comment
  documenting the swap, and the `minio` python-package import in
  `reset_stream.py` (see fix #1).

## Environment notes

- venv `.venv/` (Python 3.12.15 via uv); always call
  `.venv/bin/python` / `.venv/bin/streamlit`.
- All Docker images present locally, incl. built
  `ecommerce-flink:1.20.3` — no image builds needed on resume.
- Ports: 8501 dashboard, 3000 Dagster dev, 8080 Trino, 8081 Flink,
  **3900 Garage S3 / 3903 admin**, 9092 Kafka, 9083 HMS, 5432 Postgres.
- Compose-network container IPs: kafka .2, postgres .3, hive-metastore
  .4, garage .5, flink-taskmanager .6, trino .7, flink-jobmanager .8.
- After editing mounted JVM configs (`core-site.xml`, Trino catalog
  properties), containers must be restarted — configs load at startup.
