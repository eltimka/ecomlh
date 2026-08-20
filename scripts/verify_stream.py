#!/usr/bin/env python3
"""Verify the Phase 12 streaming path (Flink -> Iceberg).

Checks (Flink + Trino must be up; run after `make run` or `make reseed`):
  1. Flink REST is reachable and the stream job is RUNNING.
  2. The Iceberg table has caught up with the topic (row count >= total
     messages in raw.web_events). Polls for a while - the initial replay
     takes a minute or two.
  3. The job is committing: at least one checkpoint completed, none failed.
     (Asserted AFTER catch-up: a freshly submitted job legitimately has
     zero completed checkpoints until its first commit lands.)
  4. No duplicate event_ids in the table (the table holds exactly one
     replay; a stale job would show duplicates -> run `make reseed`).
  5. Full coverage of the batch history: every history event_id is present
     (distinct count >= history rows, first id == EVT-00000001).

Exit code 0 + "RESULT: PASS" when everything is green.
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.request
from pathlib import Path

import polars as pl
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "raw.web_events")
FLINK_REST = "http://localhost:8081"
STREAM_JOB_NAME = "insert-into_iceberg.bronze.stream_web_events"
TABLE = "iceberg.bronze.stream_web_events"
HISTORY = REPO_ROOT / "data" / "synthetic" / "web_events.parquet"
CATCHUP_DEADLINE_S = 180


def flink_rest(path: str) -> dict:
    with urllib.request.urlopen(f"{FLINK_REST}{path}", timeout=5) as r:
        return json.load(r)


def stream_job() -> dict | None:
    """The active stream job (RUNNING/RESTARTING), else the most recent entry."""
    jobs = [j for j in flink_rest("/jobs/overview").get("jobs", []) if j["name"] == STREAM_JOB_NAME]
    if not jobs:
        return None
    for job in jobs:
        if job["state"] in ("RUNNING", "RESTARTING"):
            return job
    return max(jobs, key=lambda j: j["start-time"])


def trino_query(sql: str):
    import trino.dbapi

    conn = trino.dbapi.connect(
        os.environ.get("TRINO_HOST", "localhost"),
        int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "admin"),
    )
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    conn.close()
    return rows


def topic_message_count() -> int:
    from kafka import KafkaConsumer
    from kafka import TopicPartition

    consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP)
    try:
        partitions = [TopicPartition(KAFKA_TOPIC, p) for p in sorted(consumer.partitions_for_topic(KAFKA_TOPIC) or set())]
        ends = consumer.end_offsets(partitions)
        return sum(ends.values())
    finally:
        consumer.close()


def main() -> int:
    failures: list[str] = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f" ({detail})" if detail else ""))
        if not ok:
            failures.append(name)

    print("Flink -> Iceberg streaming path verification (Phase 12)")
    print("=" * 60)

    if not HISTORY.exists():
        print(f"RESULT: FAIL - {HISTORY} missing (run generate_synthetic.py / make run first)")
        return 1
    n_hist = len(pl.read_parquet(HISTORY))

    # ------------------------------------------------------------------ 1.
    print("[1] flink stream job")
    try:
        job = stream_job()
    except Exception as exc:  # noqa: BLE001
        check("flink REST reachable", False, str(exc)[:120])
        print("=" * 60)
        print("RESULT: FAIL - flink not reachable")
        return 1
    check(
        "stream job is RUNNING",
        job is not None and job["state"] == "RUNNING",
        f"state={job['state'] if job else 'NOT SUBMITTED'}",
    )
    if job is None or job["state"] != "RUNNING":
        print("=" * 60)
        print("RESULT: FAIL - stream job not running (make flink-up)")
        return 1

    # ------------------------------------------------------------------ 2.
    print("[2] catch-up with the topic")
    try:
        n_topic = topic_message_count()
    except Exception as exc:  # noqa: BLE001
        check("kafka reachable", False, str(exc)[:120])
        n_topic = 0
    n_rows = -1
    t0 = time.time()
    while time.time() - t0 < CATCHUP_DEADLINE_S:
        try:
            n_rows = trino_query(f"SELECT count(*) FROM {TABLE}")[0][0]
        except Exception:  # noqa: BLE001
            n_rows = 0
        if n_rows >= n_topic:
            break
        time.sleep(5)
    check(
        f"table caught up with topic ({n_topic:,} messages)",
        n_rows >= n_topic > 0,
        f"table={n_rows:,} after {time.time() - t0:.0f}s",
    )

    # ------------------------------------------------------------------ 3.
    print("[3] checkpoints")
    counts = flink_rest(f"/jobs/{job['jid']}/checkpoints").get("counts", {})
    check(
        "at least one checkpoint completed",
        counts.get("completed", 0) >= 1,
        f"completed={counts.get('completed', 0)}",
    )
    check("no failed checkpoints", counts.get("failed", 0) == 0, f"failed={counts.get('failed', 0)}")

    # ------------------------------------------------------------------ 4.
    print("[4] no duplicates")
    total, distinct = trino_query(
        f"SELECT count(*), count(DISTINCT event_id) FROM {TABLE}"
    )[0]
    check(
        "no duplicate event_ids (one clean replay)",
        total == distinct,
        f"rows={total:,} distinct={distinct:,}",
    )

    # ------------------------------------------------------------------ 5.
    print("[5] history coverage")
    n_distinct = trino_query(f"SELECT count(DISTINCT event_id) FROM {TABLE}")[0][0]
    first = trino_query(f"SELECT min(event_id) FROM {TABLE}")[0][0]
    check(
        f"all {n_hist:,} history events landed",
        n_distinct >= n_hist,
        f"distinct={n_distinct:,}",
    )
    check("first event_id == EVT-00000001", first == "EVT-00000001", f"min={first}")

    print("=" * 60)
    if failures:
        print(f"RESULT: FAIL - {len(failures)} check(s) failed: {failures}")
        return 1
    print("RESULT: PASS - flink stream path verified (Kafka -> Iceberg, caught up, no dupes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
