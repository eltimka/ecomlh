#!/usr/bin/env python3
"""Verify the Flink -> Iceberg streaming path (all four stream bronze tables).

For every stream in stream_spec.STREAM_SPECS (web_events + the Phase 15 money
path), run after `make run` or `make reseed`:
   1. The Flink job is RUNNING.
   2. The table has caught up with its topic (row count >= topic messages);
      polls for a while - the initial replay takes a minute or two.
   3. The job is committing: >= 1 checkpoint completed, none failed
      (asserted AFTER catch-up: a fresh job legitimately has zero until its
      first commit lands).
   4. No duplicate primary keys in the table (one clean replay).
   5. Full history coverage: distinct pk >= history rows, min pk == first_id.

Exit code 0 + "RESULT: PASS" when every stream is green.
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

from stream_spec import STREAM_SPECS

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
FLINK_REST = "http://localhost:8081"
CATCHUP_DEADLINE_S = 180


def flink_rest(path: str) -> dict:
    with urllib.request.urlopen(f"{FLINK_REST}{path}", timeout=5) as r:
        return json.load(r)


def stream_job(name: str) -> dict | None:
    """The active job for `name` (RUNNING/RESTARTING), else the most recent."""
    jobs = [j for j in flink_rest("/jobs/overview").get("jobs", []) if j["name"] == name]
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


def topic_message_count(topic: str) -> int:
    from kafka import KafkaConsumer
    from kafka import TopicPartition

    consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP)
    try:
        partitions = [TopicPartition(topic, p) for p in sorted(consumer.partitions_for_topic(topic) or set())]
        return sum(consumer.end_offsets(partitions).values())
    finally:
        consumer.close()


def main() -> int:
    failures: list[str] = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f" ({detail})" if detail else ""))
        if not ok:
            failures.append(name)

    print(f"Flink -> Iceberg streaming path verification ({len(STREAM_SPECS)} streams)")
    print("=" * 60)

    n_hist: dict[str, int] = {}
    for spec in STREAM_SPECS:
        if not spec.history.exists():
            print(f"RESULT: FAIL - {spec.history} missing (run generate_synthetic.py / make run first)")
            return 1
        n_hist[spec.key] = len(pl.read_parquet(spec.history))

    # ------------------------------------------------------------------ 1.
    print("[1] flink stream jobs")
    try:
        job_by_key = {s.key: stream_job(s.job) for s in STREAM_SPECS}
    except Exception as exc:  # noqa: BLE001
        check("flink REST reachable", False, str(exc)[:120])
        print("=" * 60)
        print("RESULT: FAIL - flink not reachable")
        return 1
    for spec in STREAM_SPECS:
        job = job_by_key[spec.key]
        check(
            f"{spec.key}: job RUNNING",
            job is not None and job["state"] == "RUNNING",
            f"state={job['state'] if job else 'NOT SUBMITTED'}",
        )
    not_running = [s.key for s in STREAM_SPECS if not (job_by_key[s.key] and job_by_key[s.key]["state"] == "RUNNING")]
    if not_running:
        print("=" * 60)
        print(f"RESULT: FAIL - stream job(s) not running: {not_running} (make flink-up)")
        return 1

    # ------------------------------------------------------------------ 2.
    print("[2] catch-up with the topics")
    n_topic: dict[str, int] = {}
    n_rows: dict[str, int] = {}
    for spec in STREAM_SPECS:
        n_topic[spec.key] = topic_message_count(spec.topic)
    t0 = time.time()
    pending = [s.key for s in STREAM_SPECS]
    while pending and time.time() - t0 < CATCHUP_DEADLINE_S:
        still_pending = []
        for key in pending:
            spec = next(s for s in STREAM_SPECS if s.key == key)
            try:
                n_rows[key] = trino_query(f"SELECT count(*) FROM {spec.table}")[0][0]
            except Exception:  # noqa: BLE001
                n_rows[key] = 0
            if n_rows[key] < n_topic[key]:
                still_pending.append(key)
        pending = still_pending
        if pending:
            time.sleep(5)
    for spec in STREAM_SPECS:
        check(
            f"{spec.key}: caught up with topic ({n_topic[spec.key]:,} msgs)",
            n_rows.get(spec.key, 0) >= n_topic[spec.key] > 0,
            f"table={n_rows.get(spec.key, 0):,} after {time.time() - t0:.0f}s",
        )

    # ------------------------------------------------------------------ 3.
    print("[3] checkpoints")
    for spec in STREAM_SPECS:
        cp = flink_rest(f"/jobs/{job_by_key[spec.key]['jid']}/checkpoints")
        counts = cp.get("counts", {})
        check(
            f"{spec.key}: >=1 checkpoint completed",
            counts.get("completed", 0) >= 1,
            f"completed={counts.get('completed', 0)}",
        )
        # Flink's lifetime `failed` counter also counts checkpoints that are
        # aborted at trigger time while a task is still starting ("Not all
        # required tasks are currently running") - a benign startup race that
        # taints the counter for the job's whole lifetime. So instead of
        # asserting failed == 0, assert that every checkpoint AFTER the first
        # COMPLETED one is also clean: a failure before the first successful
        # commit is the known startup window, a failure after it is a real
        # regression (and would usually also show up as consumer lag).
        hist = cp.get("history", []) or []
        first_ok = next((c["id"] for c in hist if c.get("status") == "COMPLETED"), None)
        dirty = [
            f"#{c.get('id')}={c.get('status')}"
            for c in hist
            if c.get("status") in ("FAILED", "EXPIRED") and first_ok is not None and c.get("id", 0) > first_ok
        ]
        check(
            f"{spec.key}: checkpoints clean after first commit",
            first_ok is not None and not dirty,
            f"last {len(hist)} shown, lifetime failed={counts.get('failed', 0)}"
            + (f" (post-commit failures: {'; '.join(dirty)})" if dirty else ""),
        )

    # ------------------------------------------------------------------ 4.
    print("[4] no duplicates")
    for spec in STREAM_SPECS:
        total, distinct = trino_query(f"SELECT count(*), count(DISTINCT {spec.pk}) FROM {spec.table}")[0]
        check(
            f"{spec.key}: no duplicate {spec.pk} (one clean replay)",
            total == distinct,
            f"rows={total:,} distinct={distinct:,}",
        )

    # ------------------------------------------------------------------ 5.
    print("[5] history coverage")
    for spec in STREAM_SPECS:
        n_distinct = trino_query(f"SELECT count(DISTINCT {spec.pk}) FROM {spec.table}")[0][0]
        first = trino_query(f"SELECT min({spec.pk}) FROM {spec.table}")[0][0]
        check(
            f"{spec.key}: all {n_hist[spec.key]:,} history rows landed",
            n_distinct >= n_hist[spec.key],
            f"distinct={n_distinct:,}",
        )
        check(f"{spec.key}: first {spec.pk} == {spec.first_id}", first == spec.first_id, f"min={first}")

    print("=" * 60)
    if failures:
        print(f"RESULT: FAIL - {len(failures)} check(s) failed: {failures}")
        return 1
    print("RESULT: PASS - all flink stream paths verified (Kafka -> Iceberg, caught up, no dupes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
