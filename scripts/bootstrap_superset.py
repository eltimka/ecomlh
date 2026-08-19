#!/usr/bin/env python3
"""Bootstrap the Apache Superset dashboard (idempotent).

Steps:
  1. Ensure the ``superset`` database exists in the shared Postgres
  2. ``superset init`` (migrations + roles/permissions seeding, idempotent)
  3. Ensure the local admin user exists
  4. Wait for the REST API, log in
  5. Register the Trino database connection (iceberg catalog)
  6. Create datasets for the four gold tables
  7. Create charts from dashboard/dashboards/charts.json
  8. Create the "Customer 360" dashboard from
     dashboard/dashboards/dashboard.json and lay out its tiles
  9. Sanity check: run ``SELECT 1`` through Superset -> Trino

Run:  .venv/bin/python scripts/bootstrap_superset.py
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
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
DB_NAME = os.environ.get("SUPERSET_DB_NAME", "superset")

# Inside the compose network the Trino service is `trino:8080`.
TRINO_SQLALCHEMY_URI = "trino://admin@trino:8080/iceberg"
TRINO_DB_NAME = "trino_iceberg"

# Table datasets created in an earlier iteration (replaced by SQL marts);
# removed for a clean workspace.
LEGACY_TABLE_DATASETS = [
    "customer_360",
    "monthly_kpis",
    "revenue_by_channel",
    "revenue_by_category",
]

BASE_URL = f"http://{SUPERSET_HOST}:{SUPERSET_PORT}"


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def compose(*args: str) -> str:
    """Run a docker compose command against the superset service."""
    cmd = [
        "docker", "compose", "-f", str(COMPOSE_FILE), "exec", "-T",
        "superset", *args,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    out = (proc.stdout or "") + (proc.stderr or "")
    return out


def compose_run(*args: str) -> str:
    """One-shot `docker compose run --rm superset ...`.

    Used for `db upgrade` / `fab create-admin` because the web container
    crash-loops until the metadb exists (so plain `exec` is racy).
    """
    cmd = [
        "docker", "compose", "-f", str(COMPOSE_FILE), "run", "--rm",
        "superset", *args,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    out = (proc.stdout or "") + (proc.stderr or "")
    return out


def step(n: int, msg: str) -> None:
    print(f"[{n}/8] {msg}", flush=True)


def wait_health(timeout_s: int = 300) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            r = requests.get(f"{BASE_URL}/health", timeout=5)
            if r.status_code == 200:
                # /health returns plain text "OK" (not JSON)
                print(f"        health: {r.text.strip()}")
                return
        except requests.RequestException:
            pass
        time.sleep(3)
    raise SystemExit(f"superset /health did not come up within {timeout_s}s")


def login(session: requests.Session) -> None:
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
    r.raise_for_status()
    payload = r.json()
    if "error" in payload:
        raise SystemExit(f"login failed: {payload['error']}")
    token = payload["access_token"]
    session.headers["Authorization"] = f"Bearer {token}"
    # CSRF token required for POST/PUT/DELETE on the REST API
    r = session.get(f"{BASE_URL}/api/v1/security/csrf_token/", timeout=30)
    r.raise_for_status()
    session.headers["X-CSRFToken"] = r.json()["result"]
    print(f"        logged in as {ADMIN_USER}")


def _q(*clauses: str) -> str:
    """Build a Superset list-query q=(...) string (simple clauses only)."""
    return "(" + ",".join(clauses) + ")"


def get_one(
    session: requests.Session, endpoint: str, column: str, value: str
) -> dict | None:
    """Find an object by name. Uses a plain list query (page_size:100) and
    matches client-side - avoids the version-fragile rison filter syntax."""
    r = session.get(f"{BASE_URL}/api/v1/{endpoint}/?q=(page_size:100)", timeout=30)
    r.raise_for_status()
    for item in r.json().get("result", []):
        if item.get(column) == value:
            return item
    return None


def post(session: requests.Session, endpoint: str, body: dict) -> dict:
    r = session.post(f"{BASE_URL}/api/v1/{endpoint}", json=body, timeout=60)
    if not r.ok:
        raise SystemExit(
            f"POST /api/v1/{endpoint} -> {r.status_code}: {r.text[:500]}"
        )
    return r.json()


# --------------------------------------------------------------------------- #
# 1-3. metadb + migrations + admin user
# --------------------------------------------------------------------------- #
def ensure_metadb() -> None:
    step(1, f"ensuring Postgres database '{DB_NAME}'")
    check = subprocess.run(
        [
            "docker", "exec", "-e", "PGPASSWORD=hive", "lakehouse-postgres",
            "psql", "-U", "hive", "-d", "postgres", "-tAc",
            f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}'",
        ],
        capture_output=True, text=True,
    )
    if "1" in check.stdout:
        print("        exists")
        return
    create = subprocess.run(
        [
            "docker", "exec", "-e", "PGPASSWORD=hive", "lakehouse-postgres",
            "psql", "-U", "hive", "-d", "postgres", "-c",
            f"CREATE DATABASE {DB_NAME}",
        ],
        capture_output=True, text=True,
    )
    if create.returncode != 0:
        raise SystemExit(f"CREATE DATABASE failed: {create.stderr}")
    print("        created")


def db_upgrade() -> None:
    step(2, "superset init (migrations + role/permission seeding)")
    out = compose_run("superset", "init")
    if "Traceback" in out or "ERROR" in out:
        # A partially applied migration (e.g. after a web-container connection
        # was killed mid-flight) fails the app-level queries at the end of
        # init. A bare `db upgrade` finishes the migration, then init succeeds.
        print("        init failed - running db upgrade and retrying")
        compose_run("superset", "db", "upgrade")
        out = compose_run("superset", "init")
        if "Traceback" in out or "ERROR" in out:
            raise SystemExit(f"superset init failed:\n{out[-2000:]}")
    print("        done")


def ensure_admin() -> None:
    step(3, f"ensuring admin user '{ADMIN_USER}'")
    out = compose_run(
        "superset", "fab", "create-admin",
        "--username", ADMIN_USER, "--password", ADMIN_PASSWORD,
        "--firstname", "Local", "--lastname", "Dev",
        "--email", "admin@localhost",
    )
    if "already exists" in out.lower():
        print("        exists")
    elif "Aborted" in out or "Error" in out:
        raise SystemExit(f"create-admin failed:\n{out[-2000:]}")
    elif "created" in out.lower():
        print("        created")
    else:
        # create-admin is noisy; verify via the API login later instead.
        print("        (output ambiguous - will verify via login)")


# --------------------------------------------------------------------------- #
# 4-8. REST API objects
# --------------------------------------------------------------------------- #
def build_query_context(spec: dict, dataset_id: int) -> dict:
    """Materialize a Superset 4.x query_context from a chart spec.

    4.1 format (ChartDataQueryContextSchema): top level accepts only
    datasource / queries / result_type / result_format / form_data. Each
    query object uses `columns` (dimensions, must exist in the dataset) +
    ad-hoc `metrics` objects; `orderby` entries are [column, is_asc] pairs.
    """
    groupby = list(spec.get("groupby", []))
    metrics = [
        {
            "expressionType": "SQL",
            "sqlExpression": m["sql"],
            "label": m["label"],
            "hasCustomLabel": True,
        }
        for m in spec.get("metrics", [])
    ]
    orderby = spec.get("orderby")
    if not orderby:
        if groupby:
            orderby = [[groupby[0], True]]
    else:
        orderby = [[col, True if d == "asc" else False] for col, d in orderby]
    return {
        "datasource": {"id": dataset_id, "type": "table"},
        "queries": [
            {
                "columns": groupby,
                "metrics": metrics,
                "orderby": orderby,
                "row_limit": 10000,
                "time_range": "No filter",
                "is_timeseries": False,
                "extras": {},
            }
        ],
        "result_type": "full",
        "result_format": "json",
        "form_data": {"viz_type": spec["viz_type"]},
    }


def ensure_trino_db(session: requests.Session) -> int:
    step(4, f"registering Trino database '{TRINO_DB_NAME}'")
    existing = get_one(session, "database", "database_name", TRINO_DB_NAME)
    if existing:
        print(f"        exists (id={existing['id']})")
        return existing["id"]
    body = {
        "database_name": TRINO_DB_NAME,
        "sqlalchemy_uri": TRINO_SQLALCHEMY_URI,
        "expose_in_sqllab": True,
        "allow_run_async": False,
    }
    result = post(session, "database", body)
    print(f"        created (id={result['id']})")
    return result["id"]


def ensure_sql_dataset(
    session: requests.Session, spec: dict, db_id: int
) -> int:
    existing = get_one(session, "dataset", "table_name", spec["name"])
    if existing:
        detail = session.get(
            f"{BASE_URL}/api/v1/dataset/{existing['id']}", timeout=30
        ).json()["result"]
        if detail.get("sql") != spec["sql"]:
            r = session.put(
                f"{BASE_URL}/api/v1/dataset/{existing['id']}",
                json={
                    "table_name": spec["name"],
                    "database": db_id,
                    "sql": spec["sql"],
                },
                timeout=60,
            )
            r.raise_for_status()
            print(f"        dataset {spec['name']} sql updated")
        return existing["id"]
    body = {"table_name": spec["name"], "database": db_id, "sql": spec["sql"]}
    result = post(session, "dataset", body)
    print(f"        dataset {spec['name']} created (id={result['id']})")
    return result["id"]


def remove_legacy_table_datasets(session: requests.Session) -> None:
    """Drop the plain table datasets from the first iteration (now replaced
    by SQL mart datasets)."""
    for name in LEGACY_TABLE_DATASETS:
        existing = get_one(session, "dataset", "table_name", name)
        if not existing:
            continue
        detail = session.get(
            f"{BASE_URL}/api/v1/dataset/{existing['id']}", timeout=30
        ).json()["result"]
        if detail.get("sql") is None:
            r = session.delete(
                f"{BASE_URL}/api/v1/dataset/{existing['id']}", timeout=30
            )
            r.raise_for_status()
            print(f"        removed legacy table dataset '{name}'")


def ensure_chart(
    session: requests.Session, spec: dict, dataset_id: int
) -> int:
    context = json.dumps(build_query_context(spec, dataset_id))
    existing = get_one(session, "chart", "slice_name", spec["name"])
    if existing:
        detail = session.get(
            f"{BASE_URL}/api/v1/chart/{existing['id']}", timeout=30
        ).json()["result"]
        if detail.get("query_context") != context:
            r = session.put(
                f"{BASE_URL}/api/v1/chart/{existing['id']}",
                json={
                    "slice_name": spec["name"],
                    "viz_type": spec["viz_type"],
                    "datasource_id": dataset_id,
                    "datasource_type": "table",
                    "query_context": context,
                    "params": "{}",
                },
                timeout=60,
            )
            r.raise_for_status()
            print(f"        chart {spec['name']} context updated")
        return existing["id"]
    body = {
        "slice_name": spec["name"],
        "viz_type": spec["viz_type"],
        "datasource_id": dataset_id,
        "datasource_type": "table",
        # the chart API expects these two as serialized JSON strings
        "query_context": context,
        "params": "{}",
    }
    result = post(session, "chart", body)
    print(f"        chart {spec['name']} created (id={result['id']})")
    return result["id"]


def ensure_dashboard(session: requests.Session, chart_ids: dict[str, int]) -> int:
    dash_spec = json.loads((DASH_DIR / "dashboard.json").read_text())
    title = dash_spec["title"]
    existing = get_one(session, "dashboard", "dashboard_title", title)
    if existing:
        dash_id = existing["id"]
        print(f"        dashboard '{title}' exists (id={dash_id}) - relaying out")
    else:
        body = {
            "dashboard_title": title,
            "published": True,
            "json_metadata": "{}",
        }
        result = post(session, "dashboard", body)
        dash_id = result["id"]
        print(f"        dashboard '{title}' created (id={dash_id})")

    # 4.1 layout format: json_metadata.positions is a dict keyed by tile id;
    # each entry is {type, x, y, w, h, meta: {chartId}}. set_dash_metadata()
    # syncs dashboard.slices from meta.chartId and moves positions into the
    # dashboard's position_json column.
    positions = {
        f"chart-{chart_ids[tile['chart']]}": {
            "type": "CHART",
            "x": tile["x"],
            "y": tile["y"],
            "w": tile["w"],
            "h": tile["h"],
            "meta": {"chartId": chart_ids[tile["chart"]]},
        }
        for tile in dash_spec["layout"]
    }
    metadata = json.dumps({"positions": positions})
    r = session.put(
        f"{BASE_URL}/api/v1/dashboard/{dash_id}",
        json={
            "dashboard_title": title,
            "published": True,
            "json_metadata": metadata,
        },
        timeout=60,
    )
    r.raise_for_status()
    print(f"        {len(dash_spec['layout'])} tiles laid out")
    return dash_id


# --------------------------------------------------------------------------- #
# main
# --------------------------------------------------------------------------- #
def main() -> int:
    ensure_metadb()
    db_upgrade()
    ensure_admin()

    step(0, "waiting for superset health")
    wait_health()

    step(5, "logging in")
    session = requests.Session()
    login(session)

    db_id = ensure_trino_db(session)

    step(6, "creating gold SQL mart datasets")
    dataset_specs = json.loads((DASH_DIR / "datasets.json").read_text())
    dataset_ids = {
        d["name"]: ensure_sql_dataset(session, d, db_id) for d in dataset_specs
    }
    remove_legacy_table_datasets(session)

    step(7, "creating charts")
    chart_specs = json.loads((DASH_DIR / "charts.json").read_text())
    chart_ids = {}
    for spec in chart_specs:
        chart_ids[spec["name"]] = ensure_chart(
            session, spec, dataset_ids[spec["dataset"]]
        )

    step(8, "creating dashboard")
    ensure_dashboard(session, chart_ids)

    # Sanity: run SQL through Superset -> Trino
    print("[9/9] sanity: SELECT 1 via sqllab (Superset -> Trino)")
    r = session.post(
        f"{BASE_URL}/api/v1/sqllab/execute/",
        json={
            # the metadb query.client_id column is varchar(11)
            "client_id": uuid.uuid4().hex[:11],
            "database_id": db_id,
            "sql": "SELECT 1 AS one",
        },
        timeout=60,
    )
    r.raise_for_status()
    payload = r.json()
    rows = payload.get("data", [])
    print(f"        rows: {rows}")
    if payload.get("status") != "success" or not rows or rows[0].get("one") != 1:
        raise SystemExit(f"sanity query failed: {payload}")

    print()
    print(f"Superset ready: {BASE_URL}  (login: {ADMIN_USER} / {ADMIN_PASSWORD})")
    print(f"Dashboard: 'Customer 360' with {len(chart_specs)} charts")
    return 0


if __name__ == "__main__":
    sys.exit(main())
