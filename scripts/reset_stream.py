#!/usr/bin/env python3
"""Reset the Flink -> Iceberg streaming path to a clean slate (`make reseed`).

Steps (for every stream in stream_spec.STREAM_SPECS):
   1. Cancel the Flink stream job if it is running (REST :8081).
   2. Drop the iceberg.bronze.stream_* table via Trino (removes its data dir).
   3. Best-effort: purge any leftover objects under bronze/stream_*/.

The Kafka topics are NOT touched here - `stream_producer.py --reset-topic`
(reset + replay of ALL topics) runs separately, and the jobs are resubmitted
afterwards (`make flink-up`), reading the fresh topics from earliest offset.

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

from stream_spec import STREAM_SPECS

FLINK_REST = "http://localhost:8081"
BUCKET = "bronze"
GARAGE_ENDPOINT = os.environ.get("GARAGE_ENDPOINT", "localhost:3900")
GARAGE_ACCESS_KEY = os.environ.get("GARAGE_ACCESS_KEY", "garageadmin")
GARAGE_SECRET_KEY = os.environ.get("GARAGE_SECRET_KEY", "garageadmin-local-dev-secret")


def flink_rest(path: str) -> dict:
    with urllib.request.urlopen(f"{FLINK_REST}{path}", timeout=5) as r:
        return json.load(r)


def _job(jobs: list[dict], name: str) -> dict | None:
    return next((j for j in jobs if j["name"] == name and j["state"] in ("RUNNING", "RESTARTING")), None)


def step_cancel_jobs() -> None:
    print("[1] cancel stream jobs")
    try:
        jobs = flink_rest("/jobs/overview").get("jobs", [])
    except Exception:  # noqa: BLE001
        print("  flink not reachable - nothing to cancel")
        return
    for spec in STREAM_SPECS:
        job = _job(jobs, spec.job)
        if job is None:
            continue
        jid = job["jid"]
        req = urllib.request.Request(f"{FLINK_REST}/jobs/{jid}", method="DELETE")
        urllib.request.urlopen(req, timeout=10)
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                jobs = flink_rest("/jobs/overview").get("jobs", [])
            except Exception:  # noqa: BLE001
                break
            if _job(jobs, spec.job) is None:
                print(f"  {spec.key}: cancelled ({jid[:12]})")
                break
            time.sleep(2)
        else:
            print(f"  {spec.key}: WARN still not fully cancelled after 30s (continuing)")


def step_drop_tables() -> None:
    print("[2] drop stream tables")
    import trino.dbapi

    conn = trino.dbapi.connect(
        os.environ.get("TRINO_HOST", "localhost"),
        int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "admin"),
    )
    cur = conn.cursor()
    try:
        for spec in STREAM_SPECS:
            cur.execute(f"DROP TABLE IF EXISTS {spec.table}")
            print(f"  dropped {spec.table}")
    finally:
        conn.close()


def step_purge_s3() -> None:
    print("[3] purge leftover s3 objects (best effort)")
    try:
        from minio import Minio

        client = Minio(
            GARAGE_ENDPOINT,
            access_key=GARAGE_ACCESS_KEY,
            secret_key=GARAGE_SECRET_KEY,
            secure=GARAGE_ENDPOINT.startswith("https"),
        )
        for spec in STREAM_SPECS:
            removed = 0
            for obj in client.list_objects(BUCKET, prefix=f"stream_{spec.key}/", recursive=True):
                client.remove_object(BUCKET, obj.object_name)
                removed += 1
            print(f"  stream_{spec.key}: removed {removed} object(s)")
    except Exception as exc:  # noqa: BLE001
        print(f"  skipped: {str(exc)[:120]}")


def main() -> int:
    print(f"Resetting the Flink -> Iceberg streaming path ({len(STREAM_SPECS)} streams)")
    print("=" * 60)
    step_cancel_jobs()
    try:
        step_drop_tables()
    except Exception as exc:  # noqa: BLE001
        print(f"RESULT: FAIL - drop tables failed: {str(exc)[:200]}")
        return 1
    step_purge_s3()
    print("=" * 60)
    print("RESULT: PASS - stream path reset (replay the topics, then: make flink-up)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
