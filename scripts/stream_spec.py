"""Shared spec of the four Kafka -> Flink -> Iceberg stream paths.

Single source of truth for reset_stream.py / verify_stream.py / verify_kafka.py
(Phase 11 web events + Phase 15 item 4 money path). Each entry describes one
topic, the Flink job that appends it to a stream bronze table, and the batch
history it replays. Add a new stream here and the verify/reset scripts pick it
up automatically.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SYNTH = REPO_ROOT / "data" / "synthetic"


@dataclass(frozen=True)
class StreamSpec:
    key: str      # short name
    topic: str    # kafka topic
    job: str      # flink job name (from the INSERT target in flink/sql/*.sql)
    table: str    # iceberg.bronze.stream_* table
    history: Path # batch parquet the replay is bit-identical to
    pk: str       # primary / sequence column
    first_id: str # expected min pk in the history


STREAM_SPECS: list[StreamSpec] = [
    StreamSpec(
        "web_events", "raw.web_events", "insert-into_iceberg.bronze.stream_web_events",
        "iceberg.bronze.stream_web_events", SYNTH / "web_events.parquet", "event_id", "EVT-00000001",
    ),
    StreamSpec(
        "orders", "raw.orders", "insert-into_iceberg.bronze.stream_orders",
        "iceberg.bronze.stream_orders", SYNTH / "orders.parquet", "order_id", "ORD-0000001",
    ),
    StreamSpec(
        "order_items", "raw.order_items", "insert-into_iceberg.bronze.stream_order_items",
        "iceberg.bronze.stream_order_items", SYNTH / "order_items.parquet", "order_item_id", "OI-00000001",
    ),
    StreamSpec(
        "payments", "raw.payments", "insert-into_iceberg.bronze.stream_payments",
        "iceberg.bronze.stream_payments", SYNTH / "payments.parquet", "payment_id", "PAY-0000001",
    ),
]
