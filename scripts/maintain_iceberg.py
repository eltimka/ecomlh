#!/usr/bin/env python3
"""Iceberg maintenance (Phase 15 item 3): snapshot expiration + orphan files.

Two problems this solves:
  * The stream bronze table gets ONE Iceberg snapshot per Flink checkpoint
    (10s interval) - a continuously running live stream would accumulate
    ~8,640 snapshots/day of metadata.
  * Every batch table is materialized by DROP TABLE + CTAS (full refresh).
    Trino's DROP removes the catalog metadata but leaves the old data files
    in Garage, where they accumulate silently across refreshes/reseeds.

What it does (per table in the iceberg catalog):
  1. ALTER TABLE ... EXECUTE expire_snapshots (metadata + unreferenced files)
  2. ALTER TABLE ... EXECUTE remove_orphan_files (unreferenced data files)

Trino 483 syntax note: these run as ALTER TABLE EXECUTE commands with a
retention_threshold (Iceberg duration literal, e.g. '1h', '30m') - the older
CALL system.* procedure form is not registered in this version.

Safety margins: the retention thresholds must stay comfortably above the
Flink checkpoint interval (10s) - in-flight, not-yet-committed stream files
are only seconds old, so a 2h orphan retention can never touch a file the
running job still needs, and the stream table's newest snapshots are always
kept. The per-session floor this script sets (MIN_RETENTION_FLOOR) is also
far above the checkpoint interval for the same reason.

Tune via env: MAINTAIN_SNAPSHOT_AGE / MAINTAIN_ORPHAN_AGE.

The connector's default retention floor is 7d; this script lowers it per
session (iceberg.*_min_retention session properties) so sub-day retention
works. The floor it sets (MIN_RETENTION_FLOOR) stays far above the Flink
checkpoint interval (10s) for the same in-flight-safety reason.

Exit code 0 + "RESULT: PASS" when the pass completes.
"""

from __future__ import annotations

import os
import sys

import boto3
import trino.dbapi
import trino.exceptions
from dotenv import load_dotenv

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(REPO_ROOT, ".env"))

TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
TRINO_USER = os.environ.get("TRINO_USER", "admin")
GARAGE_ENDPOINT = os.environ.get("GARAGE_ENDPOINT", "localhost:3900")
GARAGE_ACCESS_KEY = os.environ.get("GARAGE_ACCESS_KEY", "garageadmin")
GARAGE_SECRET_KEY = os.environ.get("GARAGE_SECRET_KEY", "garageadmin-local-dev-secret")
GARAGE_BUCKETS = [
    os.environ.get("GARAGE_BUCKET_BRONZE", "bronze"),
    os.environ.get("GARAGE_BUCKET_SILVER", "silver"),
    os.environ.get("GARAGE_BUCKET_GOLD", "gold"),
]

SNAPSHOT_MAX_AGE = os.environ.get("MAINTAIN_SNAPSHOT_AGE", "1h")
ORPHAN_MAX_AGE = os.environ.get("MAINTAIN_ORPHAN_AGE", "2h")
# tables with fewer snapshots than this skip expire_snapshots (nothing to do)
MIN_SNAPSHOTS_TO_EXPIRE = 3
# per-session retention floor (connector default is 7d). Still far above the
# Flink checkpoint interval (10s), so in-flight stream files stay safe.
MIN_RETENTION_FLOOR = "10m"


def trino_tables(cur) -> list[tuple[str, str]]:
    cur.execute(
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_catalog = 'iceberg' "
        "AND table_schema NOT IN ('information_schema', 'system') "
        "ORDER BY 1, 2"
    )
    return [(s, t) for s, t in cur.fetchall()]


def snapshot_count(cur, schema: str, table: str) -> int:
    cur.execute(f"SELECT count(*) FROM iceberg.\"{schema}\".\"{table}$snapshots\"")
    return int(cur.fetchone()[0])


def garage_size(s3) -> int:
    total = 0
    for bucket in GARAGE_BUCKETS:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            total += sum(o.get("Size", 0) for o in page.get("Contents", []))
    return total


def main() -> int:
    print(
        f"Iceberg maintenance (expire snapshots < {SNAPSHOT_MAX_AGE}, "
        f"remove orphans < {ORPHAN_MAX_AGE})"
    )
    print("=" * 60)

    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{GARAGE_ENDPOINT}",
        aws_access_key_id=GARAGE_ACCESS_KEY,
        aws_secret_access_key=GARAGE_SECRET_KEY,
    )
    size_before = garage_size(s3)

    con = trino.dbapi.connect(
        host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER, catalog="iceberg"
    )
    cur = con.cursor()
    try:
        cur.execute(
            f"SET SESSION iceberg.expire_snapshots_min_retention = '{MIN_RETENTION_FLOOR}'"
        )
        cur.execute(
            f"SET SESSION iceberg.remove_orphan_files_min_retention = '{MIN_RETENTION_FLOOR}'"
        )
        tables = trino_tables(cur)
        if not tables:
            print("RESULT: FAIL - no tables found in the iceberg catalog")
            return 1

        snaps_before = 0
        snaps_after = 0
        orphans_removed = 0
        orphans_bytes = 0

        for schema, table in tables:
            qualified = f'"{schema}"."{table}"'
            try:
                n_before = snapshot_count(cur, schema, table)
            except trino.exceptions.TrinoUserError as exc:
                msg = str(exc)
                if "Not an Iceberg table" in msg or ("$snapshots" in msg and "does not exist" in msg):
                    # e.g. bronze_raw.*: plain Hive landing tables in the
                    # metastore - no snapshots, no orphans to maintain
                    print(f"  {f'{schema}.{table}':45s} skipped (not an Iceberg table)")
                    continue
                raise
            snaps_before += n_before

            if n_before >= MIN_SNAPSHOTS_TO_EXPIRE:
                cur.execute(
                    f"ALTER TABLE {qualified} EXECUTE expire_snapshots"
                    f"(retention_threshold => '{SNAPSHOT_MAX_AGE}')"
                )
                cur.fetchall()
            n_after = snapshot_count(cur, schema, table)
            snaps_after += n_after

            cur.execute(
                f"ALTER TABLE {qualified} EXECUTE remove_orphan_files"
                f"(retention_threshold => '{ORPHAN_MAX_AGE}')"
            )
            metrics = {name: value for name, value in cur.fetchall()}
            removed_here = int(metrics.get("deleted_files_count") or 0)
            bytes_here = int(metrics.get("deleted_bytes") or 0)
            orphans_removed += removed_here
            orphans_bytes += bytes_here

            print(
                f"  {f'{schema}.{table}':45s} snapshots {n_before:>4d} -> {n_after:>4d}, "
                f"orphans removed {removed_here:>4d} ({bytes_here / 1e6:.1f} MB)"
            )
    finally:
        con.close()

    size_after = garage_size(s3)
    print("=" * 60)
    print(
        f"  snapshots: {snaps_before:,} -> {snaps_after:,} "
        f"({snaps_before - snaps_after:,} expired)"
    )
    print(f"  orphan files removed: {orphans_removed:,} ({orphans_bytes / 1e6:.1f} MB)")
    print(f"  garage (bronze+silver+gold): {size_before / 1e6:.1f} MB -> {size_after / 1e6:.1f} MB")
    print("RESULT: PASS - iceberg maintenance complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
