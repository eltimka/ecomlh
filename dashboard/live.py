"""Data access for the dashboard's Live stream panel.

Every function degrades gracefully: a down Flink job, a down Kafka broker,
or a missing stream table (Flink not started yet) yields an ``error`` field
instead of raising - the panel renders the failure, the gold view is
unaffected.

Endpoints:
  * Flink REST  (FLINK_REST, default http://localhost:8081)
  * Kafka       (KAFKA_BOOTSTRAP, default localhost:9092 - the EXTERNAL
    listener of the compose stack; INTERNAL is kafka:9094 for containers)
  * Trino       (TRINO_HOST/TRINO_PORT, iceberg catalog)

The stream table ``iceberg.bronze.stream_web_events`` is written by the
Flink job (Phases 12-13); this module only reads it.
"""

from __future__ import annotations

import os

import pandas as pd
import trino.dbapi

FLINK_REST = os.environ.get("FLINK_REST", "http://localhost:8081")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "raw.web_events")
STREAM_JOB_PREFIX = "insert-into_iceberg.bronze.stream_web_events"

STREAM_TABLE = "iceberg.bronze.stream_web_events"
LATEST_LIMIT = 15

# (key, kafka topic, flink job name) for the Phase 15 money path
MONEY_STREAMS = [
    ("orders", "raw.orders", "insert-into_iceberg.bronze.stream_orders"),
    ("order_items", "raw.order_items", "insert-into_iceberg.bronze.stream_order_items"),
    ("payments", "raw.payments", "insert-into_iceberg.bronze.stream_payments"),
]


def _trino(sql: str) -> pd.DataFrame:
    con = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "localhost"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user=os.environ.get("TRINO_USER", "admin"),
    )
    try:
        cur = con.cursor()
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        con.close()


def flink_job_status(job_prefix: str = STREAM_JOB_PREFIX) -> dict:
    """State + checkpoint progress of one Kafka -> Iceberg Flink job."""
    try:
        import requests

        r = requests.get(f"{FLINK_REST}/jobs/overview", timeout=5)
        r.raise_for_status()
        jobs = [j for j in r.json().get("jobs", []) if j.get("name", "").startswith(job_prefix)]
        if not jobs:
            return {"found": False, "state": "not running"}
        # Flink lists jobs oldest-ish first; after reseeds the list holds
        # several CANCELED runs of the same job, so prefer the RUNNING one.
        job = next((j for j in jobs if j["state"] == "RUNNING"), jobs[0])
        state = job["state"]
        out: dict = {"found": True, "state": state, "name": job["name"]}
        if state == "RUNNING":
            c = requests.get(f"{FLINK_REST}/jobs/{job['jid']}/checkpoints", timeout=5).json()
            counts = c.get("counts", {})
            out["checkpoints_completed"] = counts.get("completed", 0)
            last = c.get("latest", {}).get("completed")
            out["last_checkpoint_ms"] = last.get("duration") if last else None
        return out
    except Exception as exc:  # noqa: BLE001 - Flink may simply be down
        return {"found": False, "state": "unreachable", "error": str(exc)[:160]}


def kafka_topic_end_offset(topic: str = KAFKA_TOPIC) -> dict:
    """Sum of end offsets over all partitions of a raw topic."""
    try:
        from kafka import KafkaConsumer
        from kafka.structs import TopicPartition

        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            api_version=(3, 0, 0),
            request_timeout_ms=8000,
        )
        try:
            parts = consumer.partitions_for_topic(topic) or set()
            if not parts:
                return {"ok": False, "error": f"topic '{topic}' not found"}
            tps = [TopicPartition(topic, p) for p in parts]
            end = consumer.end_offsets(tps)
            return {"ok": True, "end_offset": sum(end.values()), "partitions": len(parts)}
        finally:
            consumer.close()
    except Exception as exc:  # noqa: BLE001 - Kafka may simply be down
        return {"ok": False, "error": str(exc)[:160]}


def stream_counts() -> dict:
    """Row counts of the two bronze sources and the merged silver fact."""
    try:
        row = _trino(
            "SELECT (SELECT count(*) FROM iceberg.bronze.web_events) AS batch_rows, "
            f"(SELECT count(*) FROM {STREAM_TABLE}) AS stream_rows, "
            "(SELECT count(*) FROM iceberg.silver.fct_web_events) AS merged_rows"
        ).iloc[0]
        last = _trino(
            f"SELECT max(event_id) AS last_id, "
            f"max(event_date) FILTER (WHERE event_id = (SELECT max(event_id) FROM {STREAM_TABLE})) AS last_date "
            f"FROM {STREAM_TABLE}"
        ).iloc[0]
        return {
            "ok": True,
            "batch_rows": int(row["batch_rows"]),
            "stream_rows": int(row["stream_rows"]),
            "merged_rows": int(row["merged_rows"]),
            "last_event_id": str(last["last_id"]) if last["last_id"] is not None else None,
            "last_event_date": last["last_date"].isoformat() if last["last_date"] is not None else None,
        }
    except Exception as exc:  # noqa: BLE001 - stream table may not exist yet
        return {"ok": False, "error": str(exc)[:160]}


def latest_events(limit: int = LATEST_LIMIT) -> pd.DataFrame:
    """Newest events in the stream bronze (event_id sequence = arrival order)."""
    return _trino(f"SELECT * FROM {STREAM_TABLE} ORDER BY event_id DESC LIMIT {int(limit)}")


def latest_orders(limit: int = LATEST_LIMIT) -> pd.DataFrame:
    """Newest orders in the money stream bronze (order_id = arrival order)."""
    return _trino(
        f"SELECT order_id, customer_id, order_date, order_status, channel, total_amount "
        f"FROM iceberg.bronze.stream_orders ORDER BY order_id DESC LIMIT {int(limit)}"
    )


def money_stream_summary() -> dict:
    """Per-money-stream state: committed rows, kafka end offset, job state.

    Degrades gracefully per-stream - a missing table or down broker yields an
    ``error``/None for that stream without failing the whole summary.
    """
    out: dict = {"ok": True, "streams": {}}
    for key, topic, job in MONEY_STREAMS:
        table = f"iceberg.bronze.stream_{key}"
        entry: dict = {"key": key}
        try:
            (rows,) = _trino(f"SELECT count(*) FROM {table}").iloc[0].tolist()
            entry["stream_rows"] = int(rows)
        except Exception as exc:  # noqa: BLE001 - table may not exist yet
            entry["error"] = str(exc)[:120]
        off = kafka_topic_end_offset(topic)
        entry["kafka_end"] = off.get("end_offset") if off.get("ok") else None
        entry["job_state"] = flink_job_status(job).get("state")
        out["streams"][key] = entry
    return out
