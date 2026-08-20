#!/usr/bin/env python3
"""Reset the Phase 12 streaming path to a clean slate (used by `make reseed`).

Steps:
  1. Cancel the Flink stream job if it is running (REST :8081).
  2. Drop iceberg.bronze.stream_web_events via Trino (this also removes the
     table's data directory from MinIO).
  3. Best-effort: purge any leftover objects under bronze/stream_web_events/.

The Kafka topic is NOT touched here - `stream_producer.py --reset-topic`
(reset + replay) runs separately, and the job is resubmitted afterwards
(`make flink-up`), reading the fresh topic from earliest offset.

Exit code 0 + "RESULT: PASS" when the stream path is reset.
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.request
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")

FLINK_REST = "http://localhost:8081"
STREAM_JOB_NAME = "insert-into_iceberg.bronze.stream_web_events"
TABLE = "iceberg.bronze.stream_web_events"
BUCKET = "bronze"
TABLE_PREFIX = "stream_web_events/"
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_USER = os.environ.get("MINIO_USER", "minioadmin")
MINIO_PASSWORD = os.environ.get("MINIO_PASSWORD", "minioadmin")


def flink_rest(path: str) -> dict:
    with urllib.request.urlopen(f"{FLINK_REST}{path}", timeout=5) as r:
        return json.load(r)


def step_cancel_job() -> None:
    print("[1] cancel stream job")
    try:
        jobs = flink_rest("/jobs/overview").get("jobs", [])
    except Exception:  # noqa: BLE001
        print("  flink not reachable - nothing to cancel")
        return
    job = next((j for j in jobs if j["name"] == STREAM_JOB_NAME and j["state"] in ("RUNNING", "RESTARTING")), None)
    if job is None:
        print("  stream job not running")
        return
    jid = job["jid"]
    req = urllib.request.Request(f"{FLINK_REST}/jobs/{jid}", method="DELETE")
    urllib.request.urlopen(req, timeout=10)
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            jobs = flink_rest("/jobs/overview").get("jobs", [])
        except Exception:  # noqa: BLE001
            break
        still = next((j for j in jobs if j["name"] == STREAM_JOB_NAME and j["state"] in ("RUNNING", "RESTARTING")), None)
        if still is None:
            print(f"  job {jid[:12]} cancelled")
            return
        time.sleep(2)
    print("  WARN: job still not fully cancelled after 30s (continuing)")


def step_drop_table() -> None:
    print("[2] drop stream table")
    import trino.dbapi

    conn = trino.dbapi.connect(
        os.environ.get("TRINO_HOST", "localhost"),
        int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "admin"),
    )
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
    conn.close()
    print(f"  dropped {TABLE}")


def step_purge_s3() -> None:
    print("[3] purge leftover s3 objects (best effort)")
    try:
        from minio import Minio

        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_USER,
            secret_key=MINIO_PASSWORD,
            secure=MINIO_ENDPOINT.startswith("https"),
        )
        removed = 0
        for obj in client.list_objects(BUCKET, prefix=TABLE_PREFIX, recursive=True):
            client.remove_object(BUCKET, obj.object_name)
            removed += 1
        print(f"  removed {removed} object(s)")
    except Exception as exc:  # noqa: BLE001
        print(f"  skipped: {str(exc)[:120]}")


def main() -> int:
    print("Resetting the Flink -> Iceberg streaming path")
    print("=" * 60)
    step_cancel_job()
    try:
        step_drop_table()
    except Exception as exc:  # noqa: BLE001
        print(f"RESULT: FAIL - drop table failed: {str(exc)[:200]}")
        return 1
    step_purge_s3()
    print("=" * 60)
    print("RESULT: PASS - stream path reset (replay the topic, then: make flink-up)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
