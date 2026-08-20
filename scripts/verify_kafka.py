#!/usr/bin/env python3
"""Verify the Phase 11 streaming bus (Kafka + seeded producer).

Checks (Kafka must be up; run after `make run` or `make reseed` - the topic
holds one replay of the batch web_events history, optionally followed by
live events from `make stream-up`):
  1. Kafka broker is reachable and the topic exists.
  2. The topic has the expected partition count.
  3. The topic's messages can all be consumed from offset 0.
  4. Message count == data/synthetic/web_events.parquet row count (replay
     mode) or >= it (live mode: replay + live suffix).
  5. Content hash of the replay subset (per-row sha256 over the canonical
     payload fields, multiset) == the same hash computed over the Parquet
     file - i.e. the replay is bit-identical to the batch dataset.
  6. event_ids are unique; in replay mode they form the exact sequence
     EVT-00000001..EVT-000NNNNN; in live mode the suffix continues the
     sequence (EVT-000NNNN1..).

Exit code 0 + "RESULT: PASS" when everything is green.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from pathlib import Path

import polars as pl
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "raw.web_events")
EXPECTED_PARTITIONS = 3
HISTORY = REPO_ROOT / "data" / "synthetic" / "web_events.parquet"
CONSUME_TIMEOUT_MS = 20_000
CONSUME_DEADLINE_S = 120


def _row_digest(payload: dict) -> str:
    """Order-independent per-row digest over the canonical payload fields."""
    raw = "|".join(
        [
            payload["event_id"],
            payload["customer_id"] or "NULL",
            payload["event_type"],
            payload["category"],
            payload["device"],
            payload["event_date"],
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _multiset_hash(rows) -> str:
    digests = sorted(_row_digest(r) for r in rows)
    return hashlib.sha256("\n".join(digests).encode("utf-8")).hexdigest()


def main() -> int:
    failures: list[str] = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f" ({detail})" if detail else ""))
        if not ok:
            failures.append(name)

    print("Kafka streaming bus verification (Phase 11)")
    print("=" * 60)

    if not HISTORY.exists():
        print(f"RESULT: FAIL - {HISTORY} missing (run generate_synthetic.py / make run first)")
        return 1
    history = pl.read_parquet(HISTORY)

    # ------------------------------------------------------------------ 1+2.
    print("[1] broker reachable, topic exists")
    try:
        from kafka.admin import KafkaAdminClient

        client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
        topics = client.list_topics()
        client.close()
        check("broker reachable", True, KAFKA_BOOTSTRAP)
        check(f"topic '{KAFKA_TOPIC}' exists", KAFKA_TOPIC in topics, f"{len(topics)} topic(s)")
    except Exception as exc:  # noqa: BLE001
        check("broker reachable", False, str(exc)[:120])
        print("=" * 60)
        print("RESULT: FAIL - kafka broker not reachable")
        return 1

    # ------------------------------------------------------------------ 3.
    print("[2] consume all messages from offset 0")
    messages: list[dict] = []
    try:
        from kafka import KafkaConsumer
        from kafka.serializer import Deserializer

        class JsonDeserializer(Deserializer):
            def deserialize(self, topic: str, headers, data: bytes):
                return json.loads(data)

        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            auto_offset_reset="earliest",
            value_deserializer=JsonDeserializer(),
            group_id=f"verify-{int(time.time() * 1000)}",
            consumer_timeout_ms=CONSUME_TIMEOUT_MS,
        )
        parts = sorted(consumer.partitions_for_topic(KAFKA_TOPIC) or set())
        check(
            f"partition count == {EXPECTED_PARTITIONS}",
            len(parts) == EXPECTED_PARTITIONS,
            f"got {len(parts)}",
        )
        t0 = time.time()
        for msg in consumer:
            messages.append(msg.value)
            if time.time() - t0 > CONSUME_DEADLINE_S:
                break
        consumer.close()
        check("consumption completed", True, f"{len(messages):,} messages in {time.time() - t0:.1f}s")
    except Exception as exc:  # noqa: BLE001
        check("consumption completed", False, str(exc)[:120])
        messages = []

    if not messages:
        print("=" * 60)
        print("RESULT: FAIL - no messages in the topic (run the producer replay first: make run / make reseed)")
        return 1

    # ------------------------------------------------------------------ 4.
    hist_rows = [
        {**row, "event_date": row["event_date"].isoformat()}
        for row in history.select(
            ["event_id", "customer_id", "event_type", "category", "device", "event_date"]
        ).iter_rows(named=True)
    ]
    n_hist = len(history)
    hist_ids = {r["event_id"] for r in hist_rows}
    h_hist = _multiset_hash(hist_rows)

    # ------------------------------------------------------------------ 5.
    print("[3] counts")
    n_total = len(messages)
    n_live = n_total - n_hist
    live_mode = n_total > n_hist
    check(
        f"message count {'>' if live_mode else '='} history rows ({n_hist:,})",
        n_total >= n_hist,
        f"topic has {n_total:,}" + (f" ({n_live:,} live events)" if live_mode else ""),
    )

    # ------------------------------------------------------------------ 6.
    print("[4] replay determinism (content hash vs Parquet)")
    # live mode: only the replay subset (history event_ids) is checked
    # against the Parquet hash; the live suffix is checked structurally
    replay_subset = [m for m in messages if m["event_id"] in hist_ids]
    h_topic = _multiset_hash(replay_subset)
    check(
        "content hash matches web_events.parquet" + (" (replay subset)" if live_mode else ""),
        h_topic == h_hist,
        f"topic={h_topic[:16]}... parquet={h_hist[:16]}...",
    )

    # ------------------------------------------------------------------ 7.
    print("[5] event_id sequence")
    ids = [m["event_id"] for m in messages]
    check("event_ids unique", len(set(ids)) == len(ids), f"{len(set(ids)):,} unique of {len(ids):,}")
    if live_mode:
        live_ids = [i for i in ids if i not in hist_ids]
        expected_live = {f"EVT-{n_hist + i + 1:08d}" for i in range(n_live)}
        check(
            f"live event_ids continue the sequence (EVT-{n_hist + 1:08d}..)",
            set(live_ids) == expected_live,
            f"{len(live_ids):,} live ids, first={min(live_ids) if live_ids else '-'}",
        )
    else:
        expected = {f"EVT-{i + 1:08d}" for i in range(n_hist)}
        check(
            "event_ids == EVT-00000001..EVT-000NNNNN",
            set(ids) == expected,
            f"first={ids[0]} last={ids[-1]}",
        )

    print("=" * 60)
    if failures:
        print(f"RESULT: FAIL - {len(failures)} check(s) failed: {failures}")
        return 1
    suffix = f" + {n_live:,} live events" if live_mode else ""
    print(f"RESULT: PASS - kafka streaming bus verified (replay bit-identical to batch{suffix})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
