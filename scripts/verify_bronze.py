#!/usr/bin/env python3
"""Verify the Phase 5 bronze layer.

Checks (Trino + Garage must be up, bronze assets materialized):
  1. All 8 bronze Iceberg tables exist in iceberg.bronze.
  2. Row counts match the Phase 4 manifest (data/synthetic/manifest.json).
  3. Raw Parquet files exist in the bronze landing zone (s3a://bronze/raw/).
  4. The raw files are directly queryable via the hive catalog (hive.bronze_raw).
  5. A sample join query over bronze works (top-5 customers by revenue).

Exit code 0 + "RESULT: PASS" when everything is green.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import boto3
from botocore.config import Config as BotoConfig
from dotenv import load_dotenv
from trino.dbapi import connect

REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")

MANIFEST = REPO_ROOT / "data" / "synthetic" / "manifest.json"
BRONZE_BUCKET = "bronze"
TABLES = [
    "customers", "products", "orders", "order_items",
    "payments", "refunds", "web_events", "support_tickets",
]


def main() -> int:
    failures: list[str] = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f" ({detail})" if detail else ""))
        if not ok:
            failures.append(name)

    manifest = json.loads(MANIFEST.read_text())
    expected = {name: info["rows"] for name, info in manifest["tables"].items()}

    cur = connect(
        host="localhost", port=8080, user="admin",
    ).cursor()

    print("Bronze layer verification")
    print("=" * 60)

    # 1. Iceberg tables exist
    cur.execute("SHOW TABLES FROM iceberg.bronze")
    present = {row[0] for row in cur.fetchall()}
    check("all 8 iceberg.bronze tables exist", set(TABLES) <= present,
          f"present: {len(present)}")

    # 2. Row counts match the Phase 4 manifest
    counts: dict[str, int] = {}
    for table in TABLES:
        if table in present:
            cur.execute(f"SELECT count(*) FROM iceberg.bronze.{table}")
            counts[table] = cur.fetchone()[0]
    for table in TABLES:
        ok = counts.get(table) == expected.get(table)
        check(f"rows match manifest: {table}", ok,
              f"{counts.get(table):,} vs expected {expected.get(table):,}" if table in expected else "not in manifest")

    # 3. Raw landing zone files on Garage
    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{os.environ.get('GARAGE_ENDPOINT', 'localhost:3900')}",
        aws_access_key_id=os.environ.get("GARAGE_ACCESS_KEY", "garageadmin"),
        aws_secret_access_key=os.environ.get("GARAGE_SECRET_KEY", "garageadmin-local-dev-secret"),
        config=BotoConfig(signature_version="s3v4"),
    )
    keys = {
        obj["Key"]
        for obj in s3.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix="raw/").get("Contents", [])
    }
    raw_ok = all(f"raw/{t}/{t}.parquet" in keys for t in TABLES)
    check("raw parquet files in bronze/raw/", raw_ok, f"{len(keys)} objects")

    # 4. Raw files queryable via the hive catalog
    cur.execute("SHOW TABLES FROM hive.bronze_raw")
    raw_present = {row[0] for row in cur.fetchall()}
    all_ok = set(TABLES) <= raw_present
    for table in TABLES:
        if all_ok:
            cur.execute(f"SELECT count(*) FROM hive.bronze_raw.{table}")
            raw_count = cur.fetchone()[0]
            if raw_count != counts.get(table):
                all_ok = False
    check("hive.bronze_raw readable, counts consistent", all_ok,
          f"{len(raw_present)} external tables")

    # 5. Sample analytical query over bronze (top-5 customers by revenue)
    try:
        cur.execute(
            """
            SELECT c.customer_id,
                   count(DISTINCT o.order_id)                 AS n_orders,
                   round(sum(i.line_total), 2)                AS revenue
            FROM   iceberg.bronze.orders o
            JOIN   iceberg.bronze.order_items i ON i.order_id = o.order_id
            JOIN   iceberg.bronze.customers c ON c.customer_id = o.customer_id
            GROUP  BY c.customer_id
            ORDER  BY revenue DESC
            LIMIT  5
            """
        )
        top5 = cur.fetchall()
        check("sample join query over bronze", len(top5) == 5)
        for cid, n, rev in top5:
            print(f"        {cid}: {n:,} orders, ${rev:,.2f}")
    except Exception as exc:  # noqa: BLE001
        check("sample join query over bronze", False, str(exc)[:120])

    print("=" * 60)
    if failures:
        print(f"RESULT: FAIL - {len(failures)} check(s) failed: {failures}")
        return 1
    print("RESULT: PASS - bronze layer verified")
    return 0


if __name__ == "__main__":
    sys.exit(main())
