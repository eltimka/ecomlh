#!/usr/bin/env python3
"""Verify the streaming bus (Kafka) for all four topics.

For every stream in stream_spec.STREAM_SPECS (web_events + the Phase 15 money
path), run after `make run` or `make reseed` - each topic holds one replay of
its batch history, optionally followed by live rows from `make stream-up`:
   1. The topic exists with the expected partition count.
   2. All messages are consumable from offset 0.
   3. Message count >= history rows (== in replay mode, > in live mode).
   4. Content hash of the replay subset (per-row sha256 over every payload
      column, multiset) == the same hash over the Parquet file - the replay
      is bit-identical to the batch dataset.
   5. Primary keys are unique.

Exit code 0 + "RESULT: PASS" when every topic is green.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from datetime import date, datetime
from pathlib import Path

import polars as pl
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")

from stream_spec import STREAM_SPECS

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
EXPECTED_PARTITIONS = 3
CONSUME_TIMEOUT_MS = 20_000
CONSUME_DEADLINE_S = 180


def _canonical(payload: dict, columns: list[str]) -> str:
    parts = []
    for c in columns:
        v = payload.get(c)
        if v is None:
            parts.append("NULL")
        elif isinstance(v, (date, datetime)):
            parts.append(v.isoformat())
        else:
            parts.append(str(v))
    return "|".join(parts)


def _row_digest(payload: dict, columns: list[str]) -> str:
    return hashlib.sha256(_canonical(payload, columns).encode("utf-8")).hexdigest()


def _multiset_hash(rows, columns: list[str]) -> str:
    digests = sorted(_row_digest(r, columns) for r in rows)
    return hashlib.sha256("\n".join(digests).encode("utf-8")).hexdigest()


def consume_all(topic: str) -> tuple[list[dict], int]:
    """Consume every message from offset 0; return (payloads, partition_count)."""
    from kafka import KafkaConsumer
    from kafka.serializer import Deserializer

    class JsonDeserializer(Deserializer):
        def deserialize(self, topic: str, headers, data: bytes):
            return json.loads(data)

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        value_deserializer=JsonDeserializer(),
        group_id=f"verify-{int(time.time() * 1000)}",
        consumer_timeout_ms=CONSUME_TIMEOUT_MS,
    )
    try:
        parts = sorted(consumer.partitions_for_topic(topic) or set())
        messages: list[dict] = []
        t0 = time.time()
        for msg in consumer:
            messages.append(msg.value)
            if time.time() - t0 > CONSUME_DEADLINE_S:
                break
        return messages, len(parts)
    finally:
        consumer.close()


def main() -> int:
    failures: list[str] = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f" ({detail})" if detail else ""))
        if not ok:
            failures.append(name)

    print(f"Kafka streaming bus verification ({len(STREAM_SPECS)} topics)")
    print("=" * 60)

    # ------------------------------------------------------------------ 1.
    print("[1] broker reachable, topics exist")
    try:
        from kafka.admin import KafkaAdminClient

        client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
        topics = client.list_topics()
        client.close()
        check("broker reachable", True, KAFKA_BOOTSTRAP)
        for spec in STREAM_SPECS:
            check(f"topic '{spec.topic}' exists", spec.topic in topics, f"{len(topics)} topic(s)")
    except Exception as exc:  # noqa: BLE001
        check("broker reachable", False, str(exc)[:120])
        print("=" * 60)
        print("RESULT: FAIL - kafka broker not reachable")
        return 1

    for spec in STREAM_SPECS:
        if not spec.history.exists():
            print(f"RESULT: FAIL - {spec.history} missing (run generate_synthetic.py / make run first)")
            return 1
        history = pl.read_parquet(spec.history)
        columns = history.columns
        n_hist = len(history)
        hist_rows = list(history.select(columns).iter_rows(named=True))
        hist_ids = {r[spec.pk] for r in hist_rows}
        h_hist = _multiset_hash(hist_rows, columns)

        # ------------------------------------------------------------------ 2+3.
        print(f"[2] {spec.key}: consume + partitions")
        try:
            messages, n_parts = consume_all(spec.topic)
            check(f"{spec.key}: partition count == {EXPECTED_PARTITIONS}", n_parts == EXPECTED_PARTITIONS, f"got {n_parts}")
        except Exception as exc:  # noqa: BLE001
            check(f"{spec.key}: consumption completed", False, str(exc)[:120])
            continue

        if not messages:
            check(f"{spec.key}: messages present", False, "topic empty (run the producer replay first)")
            continue
        check(f"{spec.key}: consumption completed", True, f"{len(messages):,} messages")

        # ------------------------------------------------------------------ 4.
        n_total = len(messages)
        n_live = n_total - n_hist
        live_mode = n_total > n_hist
        check(
            f"{spec.key}: message count {'>' if live_mode else '='} history rows ({n_hist:,})",
            n_total >= n_hist,
            f"topic has {n_total:,}" + (f" ({n_live:,} live rows)" if live_mode else ""),
        )

        # ------------------------------------------------------------------ 5.
        print(f"[3] {spec.key}: replay determinism")
        replay_subset = [m for m in messages if m[spec.pk] in hist_ids]
        h_topic = _multiset_hash(replay_subset, columns)
        check(
            f"{spec.key}: content hash matches {spec.history.name}" + (" (replay subset)" if live_mode else ""),
            h_topic == h_hist,
            f"topic={h_topic[:16]}... parquet={h_hist[:16]}...",
        )

        # ------------------------------------------------------------------ 6.
        ids = [m[spec.pk] for m in messages]
        check(
            f"{spec.key}: {spec.pk}s unique",
            len(set(ids)) == len(ids),
            f"{len(set(ids)):,} unique of {len(ids):,}",
        )
        if live_mode:
            check(
                f"{spec.key}: live suffix disjoint from history",
                len([i for i in ids if i not in hist_ids]) == n_live,
                f"{n_live:,} live ids",
            )

    print("=" * 60)
    if failures:
        print(f"RESULT: FAIL - {len(failures)} check(s) failed: {failures}")
        return 1
    print("RESULT: PASS - kafka streaming bus verified (all replays bit-identical to batch)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
