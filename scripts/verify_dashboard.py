#!/usr/bin/env python3
"""Verify the Phase 9 Apache Superset dashboard.

Checks (run: .venv/bin/python scripts/verify_dashboard.py):
  1. superset container healthy, /health endpoint up
  2. admin login works
  3. Trino database registered + queryable through Superset (SELECT 1)
  4. all SQL mart datasets from dashboard/dashboards/datasets.json exist
  5. all charts from dashboard/dashboards/charts.json exist AND render
     (GET /api/v1/chart/<id>/data/ executes the stored query context and
     returns rows without error)
  6. the "Customer 360" dashboard exists, is published, and every tile
     references an existing chart

Exit code 0 + "RESULT: PASS" when everything is green.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

import requests
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")

COMPOSE_FILE = REPO_ROOT / "docker" / "docker-compose.yml"
DASH_DIR = REPO_ROOT / "dashboard" / "dashboards"

SUPERSET_HOST = os.environ.get("SUPERSET_HOST", "localhost")
SUPERSET_PORT = int(os.environ.get("SUPERSET_PORT", "8088"))
ADMIN_USER = os.environ.get("SUPERSET_ADMIN_USER", "admin")
ADMIN_PASSWORD = os.environ.get("SUPERSET_ADMIN_PASSWORD", "admin")

BASE_URL = f"http://{SUPERSET_HOST}:{SUPERSET_PORT}"


def main() -> int:
    failures: list[str] = []
    print("Superset dashboard verification")
    print("=" * 60)

    def check(name: str, ok: bool, detail: str = "") -> None:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f" ({detail})" if detail else ""))
        if not ok:
            failures.append(name)

    # ------------------------------------------------------------------ 1.
    proc = subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "ps", "superset",
         "--format", "{{.Status}}"],
        capture_output=True, text=True,
    )
    status = proc.stdout.strip()
    check("superset container healthy", status.endswith("(healthy)"), status)

    healthy = False
    for _ in range(30):
        try:
            r = requests.get(f"{BASE_URL}/health", timeout=5)
            if r.status_code == 200:
                healthy = True
                break
        except requests.RequestException:
            pass
        time.sleep(3)
    check("GET /health -> 200", healthy)
    if not healthy:
        print("RESULT: FAIL - superset not reachable")
        return 1

    # ------------------------------------------------------------------ 2.
    session = requests.Session()
    r = session.post(
        f"{BASE_URL}/api/v1/security/login",
        json={
            "username": ADMIN_USER,
            "password": ADMIN_PASSWORD,
            "provider": "db",
            "refresh": True,
        },
        timeout=30,
    )
    login_ok = r.status_code == 200 and "access_token" in r.json()
    check("admin login", login_ok)
    if not login_ok:
        print(f"RESULT: FAIL - login failed: {r.text[:300]}")
        return 1
    session.headers["Authorization"] = f"Bearer {r.json()['access_token']}"

    # ------------------------------------------------------------------ 3.
    r = session.get(
        f"{BASE_URL}/api/v1/security/csrf_token/", timeout=30
    )
    csrf = r.json() if r.ok else {}
    if "result" not in csrf:
        print(f"RESULT: FAIL - csrf token failed: {r.text[:300]}")
        return 1
    session.headers["X-CSRFToken"] = csrf["result"]

    r = session.get(f"{BASE_URL}/api/v1/database/?q=(page_size:100)", timeout=30)
    dbs = {d["database_name"]: d for d in r.json()["result"]}
    check("Trino database registered", "trino_iceberg" in dbs)
    db_id = dbs.get("trino_iceberg", {}).get("id")

    r = session.post(
        f"{BASE_URL}/api/v1/sqllab/execute/",
        json={
            "client_id": uuid.uuid4().hex[:11],  # metadb column is varchar(11)
            "database_id": db_id,
            "sql": "SELECT 1 AS one",
        },
        timeout=90,
    )
    ok = r.status_code == 200 and r.json().get("status") == "success"
    check("SELECT 1 via Superset -> Trino", ok)

    # ------------------------------------------------------------------ 4.
    dataset_specs = json.loads((DASH_DIR / "datasets.json").read_text())
    r = session.get(f"{BASE_URL}/api/v1/dataset/?q=(page_size:100)", timeout=30)
    datasets = {d["table_name"]: d for d in r.json()["result"]}
    missing = [d["name"] for d in dataset_specs if d["name"] not in datasets]
    check(
        f"all {len(dataset_specs)} SQL mart datasets exist",
        not missing,
        f"missing: {missing}" if missing else ", ".join(d['name'] for d in dataset_specs),
    )

    # ------------------------------------------------------------------ 5.
    chart_specs = json.loads((DASH_DIR / "charts.json").read_text())
    r = session.get(f"{BASE_URL}/api/v1/chart/?q=(page_size:100)", timeout=30)
    charts = {c["slice_name"]: c for c in r.json()["result"]}
    missing = [c["name"] for c in chart_specs if c["name"] not in charts]
    check(f"all {len(chart_specs)} charts exist", not missing,
          f"missing: {missing}" if missing else "")

    rendered = 0
    for spec in chart_specs:
        chart = charts.get(spec["name"])
        if not chart:
            continue
        r = session.get(f"{BASE_URL}/api/v1/chart/{chart['id']}/data/", timeout=120)
        if r.status_code != 200:
            print(f"        FAIL {spec['name']}: http {r.status_code} {r.text[:200]}")
            failures.append(f"chart renders: {spec['name']}")
            continue
        payload = r.json()
        res = payload["result"][0] if isinstance(payload, dict) else payload[0]
        if res.get("error"):
            print(f"        FAIL {spec['name']}: {str(res['error'])[:200]}")
            failures.append(f"chart renders: {spec['name']}")
            continue
        rows = res.get("data", [])
        rendered += 1
        print(f"        ok   {spec['name']:<18} rows={len(rows)}")
    check(f"all {len(chart_specs)} charts render without error",
          rendered == len(chart_specs), f"rendered: {rendered}/{len(chart_specs)}")

    # ------------------------------------------------------------------ 6.
    r = session.get(f"{BASE_URL}/api/v1/dashboard/?q=(page_size:100)", timeout=30)
    dashboards = {d["dashboard_title"]: d for d in r.json()["result"]}
    dash = dashboards.get("Customer 360")
    check("dashboard 'Customer 360' exists", dash is not None)
    if dash:
        check("dashboard is published", bool(dash.get("published")))
        detail = session.get(
            f"{BASE_URL}/api/v1/dashboard/{dash['id']}", timeout=30
        ).json()["result"]
        try:
            positions = json.loads(detail.get("position_json") or "{}")
        except json.JSONDecodeError:
            positions = {}
        tile_chart_ids = {
            v.get("meta", {}).get("chartId") for v in positions.values()
        }
        tile_chart_ids.discard(None)
        chart_ids = {c["id"] for c in charts.values()}
        dangling = tile_chart_ids - chart_ids
        check(
            f"dashboard has {len(chart_specs)} tiles, all referencing existing charts",
            len(tile_chart_ids) == len(chart_specs) and not dangling,
            f"tiles: {len(tile_chart_ids)}, dangling: {sorted(dangling)}" if dangling else "",
        )

    print("=" * 60)
    if failures:
        print(f"RESULT: FAIL - {len(failures)} check(s) failed")
        return 1
    print("RESULT: PASS - Superset dashboard verified")
    return 0


if __name__ == "__main__":
    sys.exit(main())
