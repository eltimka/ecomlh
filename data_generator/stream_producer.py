#!/usr/bin/env python3
"""Streaming producer for the Customer 360 lakehouse (Phase 11).

Streams web events into the Kafka topic `raw.web_events` (one JSON object
per message) so the Flink job (Phase 12) can append them to Iceberg. The
batch pipeline is untouched - Kafka is the only new side effect of Phase 11.

Modes
-----
--mode replay (default)
    Emit the full seeded web_events history (data/synthetic/web_events.parquet)
    at maximum speed. The emitted payload set is bit-identical to the batch
    dataset, which is what keeps `make run` deterministic and re-verifiable:
    after a replay, silver (Phase 13) dedups every streamed event out.

--mode live
    Replay the history first (unless --skip-replay), then emit NEW events on
    a simulated clock continuing after max(event_date). The content is fully
    determined by (seed, --live-count); --rate only paces the wall-clock
    arrival. No wall clock is used anywhere.

Payload schema (identical for replay and live):
    {"event_id": "EVT-00000001", "customer_id": "C-000001" | null,
     "event_type": "page_view", "category": "Electronics",
     "device": "desktop", "event_date": "2024-03-15"}

Usage
-----
    python data_generator/stream_producer.py --mode replay
    python data_generator/stream_producer.py --mode replay --if-empty
    python data_generator/stream_producer.py --mode live --rate 20
    python data_generator/stream_producer.py --mode live --skip-replay --live-count 10000
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import timedelta
from pathlib import Path

import numpy as np
import polars as pl
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")

from generate_synthetic import DEVICE_P, DEVICES, EVENT_TYPE_P, EVENT_TYPES, PRODUCT_CATALOG

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "raw.web_events")
TOPIC_PARTITIONS = 3
TOPIC_REPLICATION = 1
LIVE_SEED_TAG = 0x4C495645  # "LIVE" - keeps the live stream distinct from the 8 batch child streams
PAYLOAD_COLUMNS = ["event_id", "customer_id", "event_type", "category", "device", "event_date"]


def _admin(bootstrap: str):
    from kafka.admin import KafkaAdminClient

    return KafkaAdminClient(bootstrap_servers=bootstrap)


def _new_topic(topic: str):
    from kafka.admin import NewTopic

    return NewTopic(topic, num_partitions=TOPIC_PARTITIONS, replication_factor=TOPIC_REPLICATION)


def ensure_topic(bootstrap: str, topic: str) -> None:
    """Create the topic if it does not exist (kafka-init may not have run)."""
    client = _admin(bootstrap)
    try:
        if topic not in client.list_topics():
            client.create_topics([_new_topic(topic)])
            print(f"  created topic {topic} ({TOPIC_PARTITIONS} partitions)")
    finally:
        client.close()


def reset_topic(bootstrap: str, topic: str) -> None:
    """Delete and recreate the topic (used by `make reseed` for a clean slate)."""
    client = _admin(bootstrap)
    try:
        if topic in client.list_topics():
            client.delete_topics([topic])
            print(f"  deleted topic {topic}")
        client.create_topics([_new_topic(topic)])
        print(f"  created topic {topic} ({TOPIC_PARTITIONS} partitions)")
    finally:
        client.close()


def topic_message_count(bootstrap: str, topic: str) -> int:
    """Total messages across all partitions (0 if the topic is missing)."""
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(bootstrap_servers=bootstrap, group_id=f"count-{int(time.time() * 1000)}")
    try:
        partitions = sorted(consumer.partitions_for_topic(topic) or set())
        if not partitions:
            return 0
        consumer.assign([TopicPartition(topic, p) for p in partitions])
        consumer.seek_to_end()
        return sum(consumer.position(TopicPartition(topic, p)) or 0 for p in partitions)
    finally:
        consumer.close()


def history_payloads(history: pl.DataFrame):
    """Yield the historical web_events rows as JSON payloads (bit-identical
    to data/synthetic/web_events.parquet)."""
    for row in history.select(PAYLOAD_COLUMNS).iter_rows(named=True):
        row["event_date"] = row["event_date"].isoformat()
        yield row


def live_event_stream(seed: int, n: int, history: pl.DataFrame):
    """Yield NEW events on a simulated clock continuing after max(event_date).

    The content is fully determined by (seed, n) - the wall-clock rate never
    influences it. Customer ids are sampled from the historical pool so every
    streamed customer exists in the batch dimensions (FK validity).
    """
    # Deterministic stream derived from (DATA_SEED, "LIVE"), independent of
    # the 8 batch child streams in generate_synthetic._child_rngs.
    rng = np.random.default_rng(np.random.SeedSequence([seed, LIVE_SEED_TAG]))
    categories = [c[0] for c in PRODUCT_CATALOG]
    cust_pool = history["customer_id"].drop_nulls().unique().sort().to_list()

    min_d = history["event_date"].min()
    max_d = history["event_date"].max()
    # Keep the live event density close to the historical one.
    mean_interval = max(1.0, (max_d - min_d).days * 86400.0 / len(history))

    n_hist = len(history)
    t = 0.0
    i = 0
    while n == 0 or i < n:
        t += float(rng.exponential(mean_interval))
        known = rng.random() < 0.70
        yield {
            "event_id": f"EVT-{n_hist + i + 1:08d}",
            "customer_id": str(cust_pool[int(rng.integers(len(cust_pool)))]) if known else None,
            "event_type": str(rng.choice(EVENT_TYPES, p=EVENT_TYPE_P)),
            "category": str(rng.choice(categories)),
            "device": str(rng.choice(DEVICES, p=DEVICE_P)),
            "event_date": (max_d + timedelta(seconds=t)).isoformat(),
        }
        i += 1


def produce(bootstrap: str, topic: str, payloads, rate: float | None, label: str) -> int:
    """Send payloads to the topic. rate=None -> maximum speed. Returns count."""
    from kafka import KafkaProducer
    from kafka.serializer import Serializer

    class JsonSerializer(Serializer):
        def serialize(self, topic: str, headers, data: dict) -> bytes:
            return json.dumps(data).encode("utf-8")

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=JsonSerializer(),
        acks=1,
        linger_ms=20,
        batch_size=65536,
    )
    count = 0
    last = None
    interval = 1.0 / rate if rate and rate > 0 else 0.0
    next_t = time.monotonic()
    t0 = time.monotonic()
    try:
        for payload in payloads:
            producer.send(topic, payload)
            count += 1
            last = payload
            if interval:
                next_t += interval
                delay = next_t - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
            if count % 10_000 == 0:
                print(f"  {label}: {count:,} sent", flush=True)
        producer.flush()
    finally:
        producer.close()
    dt = time.monotonic() - t0
    print(f"  {label}: {count:,} messages in {dt:.1f}s ({count / max(dt, 1e-9):,.0f} msg/s)")
    if last and "event_date" in last:
        print(f"  last event_date: {last['event_date']}")
    return count


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--mode", choices=["replay", "live"], default="replay")
    parser.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    parser.add_argument("--topic", default=KAFKA_TOPIC)
    parser.add_argument("--out-dir", default=str(REPO_ROOT / "data" / "synthetic"))
    parser.add_argument("--seed", type=int, default=int(os.environ.get("DATA_SEED", "42")))
    parser.add_argument(
        "--rate",
        type=float,
        default=float(os.environ.get("STREAM_LIVE_RATE", "20")),
        help="live mode: events per second (default 20)",
    )
    parser.add_argument(
        "--live-count",
        type=int,
        default=100_000,
        help="live mode: number of new events to emit (0 = unlimited)",
    )
    parser.add_argument("--skip-replay", action="store_true", help="live mode: do not re-emit the history first")
    parser.add_argument(
        "--if-empty",
        action="store_true",
        help="skip the history replay if the topic already has messages "
        "(replay mode: also exits; live mode: continues straight to live events)",
    )
    parser.add_argument("--reset-topic", action="store_true", help="delete and recreate the topic before emitting")
    args = parser.parse_args(argv)

    out_dir = Path(args.out_dir)
    if not out_dir.is_absolute():
        out_dir = REPO_ROOT / out_dir
    history_path = out_dir / "web_events.parquet"
    if not history_path.exists():
        print(f"FATAL: {history_path} not found - run data_generator/generate_synthetic.py first")
        return 1
    history = pl.read_parquet(history_path)
    print(f"Streaming web events -> {args.bootstrap} / topic {args.topic}")
    print(f"  history: {len(history):,} events ({history_path})")

    if args.reset_topic:
        reset_topic(args.bootstrap, args.topic)
    else:
        ensure_topic(args.bootstrap, args.topic)

    if args.mode == "replay":
        if args.if_empty:
            existing = topic_message_count(args.bootstrap, args.topic)
            if existing > 0:
                print(f"  topic already has {existing:,} messages - skipping replay (--if-empty)")
                return 0
        produce(args.bootstrap, args.topic, history_payloads(history), rate=None, label="replay")
    else:
        if not args.skip_replay:
            if args.if_empty and topic_message_count(args.bootstrap, args.topic) > 0:
                print("  topic already seeded - skipping history preamble (--if-empty), going live")
            else:
                produce(args.bootstrap, args.topic, history_payloads(history), rate=None, label="replay")
        print(f"live: emitting {args.live_count or 'unlimited'} new events at {args.rate:g} events/s (simulated clock)")
        produce(
            args.bootstrap,
            args.topic,
            live_event_stream(args.seed, args.live_count, history),
            rate=args.rate,
            label="live",
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
