#!/usr/bin/env python3
"""Streaming producer for the Customer 360 lakehouse (Phase 11, Phase 15 item 4).

Streams the raw business events into Kafka so the Flink jobs can append them
to Iceberg. One JSON object per message, per topic:

    raw.web_events    <- data/synthetic/web_events.parquet    (Phase 11)
    raw.orders        <- data/synthetic/orders.parquet        (Phase 15)
    raw.order_items   <- data/synthetic/order_items.parquet   (Phase 15)
    raw.payments      <- data/synthetic/payments.parquet      (Phase 15)

Modes
-----
--mode replay (default)
    Emit the full seeded history of ALL FOUR tables at maximum speed. The
    emitted payload sets are bit-identical to the batch Parquet files, which
    is what keeps `make run` deterministic and re-verifiable: after a replay,
    silver dedups every streamed row out (zero delta on every invariant).

--mode live
    Replay the history first (unless --skip-replay / --if-empty), then emit
    NEW rows on simulated clocks continuing after each table's
    max(date). The content is fully determined by (seed, live counts); the
    wall-clock rates only pace arrival. No wall clock is used anywhere.

    Live rows are generated as consistent ORDER GROUPS: one order (new
    order_id, customer sampled from the historical customer pool so FKs stay
    valid, items sampled from the historical product pool with the batch
    price-jitter rule) plus exactly one payment (cancelled orders carry a
    failed payment, mirroring the batch rule). Total amount = sum of line
    totals, so the five-view revenue invariant holds for live orders too.
    Live orders are completed/cancelled only - 'returned' is excluded so no
    refund is implied (refunds remain batch-only by design).

Payload schemas (identical to the batch Parquet columns):
    web_events:    event_id, customer_id, event_type, category, device, event_date
    orders:        order_id, customer_id, order_date, order_status, channel, total_amount
    order_items:   order_item_id, order_id, product_id, quantity, unit_price, line_total
    payments:      payment_id, order_id, payment_method, payment_status, payment_date, amount

Usage
-----
    python data_generator/stream_producer.py --mode replay
    python data_generator/stream_producer.py --mode replay --if-empty
    python data_generator/stream_producer.py --mode live --rate 20 --order-rate 2
    python data_generator/stream_producer.py --mode live --skip-replay --live-orders 1000
"""

from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import polars as pl
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")

from generate_synthetic import (
    CHANNELS,
    CHANNEL_P,
    DEVICE_P,
    DEVICES,
    EVENT_TYPE_P,
    EVENT_TYPES,
    PAYMENT_METHODS,
    PAYMENT_METHOD_P,
    PRODUCT_CATALOG,
)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "raw.web_events")
KAFKA_TOPIC_ORDERS = os.environ.get("KAFKA_TOPIC_ORDERS", "raw.orders")
KAFKA_TOPIC_ORDER_ITEMS = os.environ.get("KAFKA_TOPIC_ORDER_ITEMS", "raw.order_items")
KAFKA_TOPIC_PAYMENTS = os.environ.get("KAFKA_TOPIC_PAYMENTS", "raw.payments")
TOPIC_PARTITIONS = 3
TOPIC_REPLICATION = 1
LIVE_SEED_TAG = 0x4C495645  # "LIVE" - keeps the live streams distinct from the 8 batch child streams
ORDER_LIVE_TAG = 0x4C4F5244  # "LORD" - separate live stream for the order family

PAYLOAD_COLUMNS = ["event_id", "customer_id", "event_type", "category", "device", "event_date"]
ORDER_COLUMNS = ["order_id", "customer_id", "order_date", "order_status", "channel", "total_amount"]
ITEM_COLUMNS = ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "line_total"]
PAYMENT_COLUMNS = ["payment_id", "order_id", "payment_method", "payment_status", "payment_date", "amount"]


@dataclass(frozen=True)
class Stream:
    """One topic + its source Parquet + payload schema."""

    key: str
    topic: str
    file: str
    columns: list[str]
    date_cols: frozenset[str]
    pk: str


STREAMS: list[Stream] = [
    Stream("web_events", KAFKA_TOPIC, "web_events.parquet", PAYLOAD_COLUMNS, frozenset({"event_date"}), "event_id"),
    Stream("orders", KAFKA_TOPIC_ORDERS, "orders.parquet", ORDER_COLUMNS, frozenset({"order_date"}), "order_id"),
    Stream("order_items", KAFKA_TOPIC_ORDER_ITEMS, "order_items.parquet", ITEM_COLUMNS, frozenset(), "order_item_id"),
    Stream("payments", KAFKA_TOPIC_PAYMENTS, "payments.parquet", PAYMENT_COLUMNS, frozenset({"payment_date"}), "payment_id"),
]


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


def _jsonify(value):
    """Convert a polars row value to a JSON-serializable python value."""
    if value is None:
        return None
    if isinstance(value, (date,)):
        return value.isoformat()
    if isinstance(value, (int, np.integer)):
        return int(value)
    if isinstance(value, (float, np.floating)):
        return float(value)
    return str(value)


def history_payloads(stream: Stream, df: pl.DataFrame):
    """Yield the history rows of one table as JSON payloads (bit-identical
    to the Parquet file, in file order)."""
    for row in df.select(stream.columns).iter_rows(named=True):
        yield {k: _jsonify(v) for k, v in row.items()}


# ---------------------------------------------------------------------------
# Live generation (deterministic simulated clocks)
# ---------------------------------------------------------------------------


def live_event_stream(seed: int, n: int, history: pl.DataFrame):
    """Yield NEW web events on a simulated clock continuing after
    max(event_date). Content is fully determined by (seed, n). Customer ids
    are sampled from the historical pool so every streamed customer exists
    in the batch dimensions (FK validity)."""
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


def live_order_stream(seed: int, n: int, orders: pl.DataFrame, items: pl.DataFrame, products: pl.DataFrame):
    """Yield NEW order groups (order, [items], payment) on a simulated clock
    continuing after max(order_date). Content is fully determined by
    (seed, n); wall-clock pacing never influences it.

    Consistency rules (mirroring the batch generator):
      * customer from the historical order-customer pool (FK-valid);
      * 1-5 items, product from the historical pool, batch price jitter;
      * total_amount = round(sum(line_total), 2);
      * one payment per order: cancelled -> failed, else 97% succeeded;
      * status: completed/cancelled only (no 'returned' -> no refund implied);
      * ids continue the batch sequences (ORD-/OI-/PAY-).
    """
    rng = np.random.default_rng(np.random.SeedSequence([seed, ORDER_LIVE_TAG]))
    cust_pool = orders["customer_id"].unique().sort().to_list()
    prod = products.select(["product_id", "unit_price"]).sort("product_id")
    prod_ids = prod["product_id"].to_list()
    base_prices = prod["unit_price"].to_numpy()

    max_d = orders["order_date"].max()
    # Historical order density (~137/day for seed 42).
    mean_interval = max(1.0, (max_d - orders["order_date"].min()).days * 86400.0 / len(orders))

    n_o = len(orders)
    n_i = len(items)
    item_counts = [1, 2, 3, 4, 5]
    item_count_p = [0.45, 0.30, 0.15, 0.07, 0.03]

    t = 0.0
    item_counter = 0
    i = 0
    while n == 0 or i < n:
        t += float(rng.exponential(mean_interval))
        order_date = max_d + timedelta(seconds=t)
        order_id = f"ORD-{n_o + i + 1:07d}"

        cancelled = rng.random() < (0.09 / 0.93)
        status = "cancelled" if cancelled else "completed"
        channel = str(rng.choice(CHANNELS, p=CHANNEL_P))

        k = int(rng.choice(item_counts, p=item_count_p))
        order_items: list[dict] = []
        total = 0.0
        for _ in range(k):
            pi = int(rng.integers(len(prod_ids)))
            qty = int(rng.integers(1, 6))  # 1..5
            unit_price = round(float(base_prices[pi]) * (1.0 + rng.uniform(-0.02, 0.02)), 2)
            line_total = round(qty * unit_price, 2)
            total = round(total + line_total, 2)
            item_counter += 1
            order_items.append(
                {
                    "order_item_id": f"OI-{n_i + item_counter:08d}",
                    "order_id": order_id,
                    "product_id": str(prod_ids[pi]),
                    "quantity": qty,
                    "unit_price": unit_price,
                    "line_total": line_total,
                }
            )

        order = {
            "order_id": order_id,
            "customer_id": str(cust_pool[int(rng.integers(len(cust_pool)))]),
            "order_date": order_date.isoformat(),
            "order_status": status,
            "channel": channel,
            "total_amount": total,
        }

        method = str(rng.choice(PAYMENT_METHODS, p=PAYMENT_METHOD_P))
        if cancelled:
            p_status = "failed"
        else:
            p_status = "pending" if rng.random() >= 0.97 else "succeeded"
        payment = {
            "payment_id": f"PAY-{n_o + i + 1:07d}",
            "order_id": order_id,
            "payment_method": method,
            "payment_status": p_status,
            "payment_date": order_date.isoformat(),
            "amount": total,
        }
        yield order, order_items, payment
        i += 1


# ---------------------------------------------------------------------------
# Emission
# ---------------------------------------------------------------------------


def _json_serializer():
    from kafka.serializer import Serializer

    class JsonSerializer(Serializer):
        def serialize(self, topic: str, headers, data: dict) -> bytes:
            return json.dumps(data).encode("utf-8")

    return JsonSerializer()


def produce(bootstrap: str, topic: str, payloads, rate: float | None, label: str) -> int:
    """Send payloads to one topic. rate=None -> maximum speed. Returns count."""
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=_json_serializer(),
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
    elif last and "order_date" in last:
        print(f"  last order_date: {last['order_date']}")
    return count


def produce_live_multi(
    bootstrap: str,
    topics: dict[str, str],
    event_iter,
    order_iter,
    rate: float,
    order_rate: float,
) -> tuple[int, int]:
    """Interleave the live web-event stream and the live order groups on a
    single wall-clock timeline (events at `rate`/s, order groups at
    `order_rate`/s; items + payment ride along with their order). Returns
    (n_events, n_orders)."""
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=_json_serializer(),
        acks=1,
        linger_ms=20,
        batch_size=65536,
    )
    ev = iter(event_iter)
    od = iter(order_iter)
    next_ev = next(ev, None)
    next_od = next(od, None)
    ev_period = 1.0 / rate if rate and rate > 0 else 0.0
    od_period = 1.0 / order_rate if order_rate and order_rate > 0 else 0.0
    ev_due = time.monotonic()
    od_due = time.monotonic()
    n_ev = 0
    n_od = 0
    t0 = time.monotonic()
    try:
        while next_ev is not None or next_od is not None:
            take_event = next_od is None or (next_ev is not None and ev_due <= od_due)
            if take_event:
                producer.send(topics["web_events"], next_ev)
                n_ev += 1
                next_ev = next(ev, None)
                ev_due += ev_period
                due = ev_due
            else:
                order, order_items, payment = next_od
                producer.send(topics["orders"], order)
                for item in order_items:
                    producer.send(topics["order_items"], item)
                producer.send(topics["payments"], payment)
                n_od += 1
                next_od = next(od, None)
                od_due += od_period
                due = od_due
            delay = due - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            if n_ev and n_ev % 10_000 == 0:
                print(f"  live: {n_ev:,} events, {n_od:,} orders", flush=True)
            if n_od and n_od % 1000 == 0:
                print(f"  live: {n_ev:,} events, {n_od:,} orders", flush=True)
        producer.flush()
    finally:
        producer.close()
    dt = time.monotonic() - t0
    print(f"  live: {n_ev:,} events + {n_od:,} order groups in {dt:.1f}s of wall time")
    return n_ev, n_od


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--mode", choices=["replay", "live"], default="replay")
    parser.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    parser.add_argument("--out-dir", default=str(REPO_ROOT / "data" / "synthetic"))
    parser.add_argument("--seed", type=int, default=int(os.environ.get("DATA_SEED", "42")))
    parser.add_argument(
        "--rate",
        type=float,
        default=float(os.environ.get("STREAM_LIVE_RATE", "20")),
        help="live mode: web events per second (default 20)",
    )
    parser.add_argument(
        "--order-rate",
        type=float,
        default=float(os.environ.get("STREAM_LIVE_ORDER_RATE", "2")),
        help="live mode: order groups per second (items + payment ride along)",
    )
    parser.add_argument(
        "--live-count",
        type=int,
        default=100_000,
        help="live mode: number of new web events to emit (0 = unlimited)",
    )
    parser.add_argument(
        "--live-orders",
        type=int,
        default=0,
        help="live mode: number of new order groups to emit (0 = unlimited)",
    )
    parser.add_argument("--skip-replay", action="store_true", help="live mode: do not re-emit the history first")
    parser.add_argument(
        "--if-empty",
        action="store_true",
        help="skip the history replay if the topics already have messages "
        "(replay mode: also exits; live mode: continues straight to live rows)",
    )
    parser.add_argument("--reset-topic", action="store_true", help="delete and recreate ALL topics before emitting")
    args = parser.parse_args(argv)

    out_dir = Path(args.out_dir)
    if not out_dir.is_absolute():
        out_dir = REPO_ROOT / out_dir
    print(f"Streaming {len(STREAMS)} streams -> {args.bootstrap}")
    frames: dict[str, pl.DataFrame] = {}
    for s in STREAMS:
        path = out_dir / s.file
        if not path.exists():
            print(f"FATAL: {path} not found - run data_generator/generate_synthetic.py first")
            return 1
        frames[s.key] = pl.read_parquet(path)
        print(f"  history {s.key}: {len(frames[s.key]):,} rows ({s.topic})")
    # live order generation samples from the historical product pool
    products_path = out_dir / "products.parquet"
    if args.mode == "live":
        if not products_path.exists():
            print(f"FATAL: {products_path} not found - run generate_synthetic.py first")
            return 1
        frames["products"] = pl.read_parquet(products_path)
        print(f"  history products: {len(frames['products']):,} rows (live order sampling pool)")

    topics = {s.key: s.topic for s in STREAMS}

    if args.reset_topic:
        for s in STREAMS:
            reset_topic(args.bootstrap, s.topic)
    else:
        for s in STREAMS:
            ensure_topic(args.bootstrap, s.topic)

    def seeded_topics() -> dict[str, bool]:
        if not args.if_empty:
            return {s.key: False for s in STREAMS}
        return {s.key: topic_message_count(args.bootstrap, s.topic) > 0 for s in STREAMS}

    def replay_preamble(seeded: dict[str, bool]) -> None:
        for s in STREAMS:
            if seeded[s.key]:
                print(f"  replay {s.key}: topic already seeded - skipping (--if-empty)")
            else:
                produce(
                    args.bootstrap,
                    s.topic,
                    history_payloads(s, frames[s.key]),
                    rate=None,
                    label=f"replay {s.key}",
                )

    if args.mode == "replay":
        replay_preamble(seeded_topics())
    else:
        if not args.skip_replay:
            replay_preamble(seeded_topics())
        print(
            f"live: emitting {args.live_count or 'unlimited'} events @ {args.rate:g}/s and "
            f"{args.live_orders or 'unlimited'} order groups @ {args.order_rate:g}/s (simulated clocks)"
        )
        produce_live_multi(
            args.bootstrap,
            topics,
            live_event_stream(args.seed, args.live_count, frames["web_events"]),
            live_order_stream(args.seed, args.live_orders, frames["orders"], frames["order_items"], frames["products"]),
            rate=args.rate,
            order_rate=args.order_rate,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
