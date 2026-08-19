#!/usr/bin/env python3
"""Verify the Phase 9 Streamlit Customer 360 dashboard.

Checks (run: .venv/bin/python scripts/verify_dashboard.py):
  1. every mart in dashboard/marts.py executes on Trino and returns rows
  2. data invariants - seed-aware (expected counts come from
     data/synthetic/manifest.json, so any DATA_SEED verifies):
       customers / orders = generator row counts
       GMV: gold.revenue_by_channel == silver.fct_orders (cross-layer)
       12 trend months, 3 channels, 8 categories
  3. the Streamlit app is serving (health endpoint -> 200); a throwaway
     headless instance is started and stopped automatically if the
     dashboard is not already running

Exit code 0 + "RESULT: PASS" when everything is green.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import trino.dbapi
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(REPO_ROOT / ".env")

sys.path.insert(0, str(REPO_ROOT / "dashboard"))
from marts import MARTS, build_sql  # noqa: E402

TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
TRINO_USER = os.environ.get("TRINO_USER", "admin")

DASHBOARD_PORT = int(os.environ.get("STREAMLIT_PORT", "8501"))
HEALTH_URL = f"http://localhost:{DASHBOARD_PORT}/_stcore/health"
MANIFEST = REPO_ROOT / "data" / "synthetic" / "manifest.json"


def main() -> int:
    failures: list[str] = []
    manifest = json.loads(MANIFEST.read_text())
    seed = manifest["seed"]
    n_customers = manifest["tables"]["customers"]["rows"]
    n_orders = manifest["tables"]["orders"]["rows"]

    print(f"Streamlit dashboard verification (dataset seed: {seed})")
    print("=" * 60)

    def check(name: str, ok: bool, detail: str = "") -> None:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f" ({detail})" if detail else ""))
        if not ok:
            failures.append(name)

    # ------------------------------------------------------------------ 1.
    con = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER)
    try:
        results: dict[str, list[tuple]] = {}
        cur = con.cursor()
        for name in MARTS:
            cur.execute(build_sql(name))
            results[name] = cur.fetchall()
        # cross-layer GMV reference (seed-independent): valid orders in silver
        cur.execute(
            "SELECT COALESCE(SUM(total_amount) FILTER (WHERE is_cancelled = false), 0) "
            "FROM iceberg.silver.fct_orders"
        )
        silver_gmv = float(cur.fetchone()[0])
        print(f"  ok    queried all {len(MARTS)} marts on {TRINO_HOST}:{TRINO_PORT}")
        check(f"all {len(MARTS)} marts execute and return rows",
              all(len(rows) > 0 for rows in results.values()))
    except Exception as exc:  # noqa: BLE001
        print(f"  Trino query failed: {exc}")
        print("RESULT: FAIL - could not query the gold layer")
        return 1
    finally:
        con.close()

    # ------------------------------------------------------------------ 2.
    customers = int(results["kpi_customers"][0][0])
    orders = int(results["kpi_orders"][0][0])
    gmv = float(results["kpi_gmv"][0][0])
    aov = float(results["kpi_aov"][0][0])

    check(f"KPI customers = manifest count", customers == n_customers,
          f"got {customers:,}, expected {n_customers:,}")
    check(f"KPI orders = manifest count", orders == n_orders,
          f"got {orders:,}, expected {n_orders:,}")
    check("KPI GMV = silver valid-order revenue (cross-layer)",
          abs(gmv - silver_gmv) < 0.01, f"gold {gmv:,.2f} vs silver {silver_gmv:,.2f}")
    check("KPI AOV in (0, 10,000)", 0 < aov < 10_000, f"got {aov:,.2f}")
    check("revenue trend has 12 months (Jan-Dec 2024)", len(results["revenue_trend"]) == 12,
          f"got {len(results['revenue_trend'])}")
    check("channel mix has 3 channels", len(results["channel_mix"]) == 3,
          f"got {len(results['channel_mix'])}")
    check("category mix has 8 categories", len(results["category_mix"]) == 8,
          f"got {len(results['category_mix'])}")
    check("churn risk mix has 1-4 segments", 1 <= len(results["churn_risk_mix"]) <= 4,
          f"got {len(results['churn_risk_mix'])}")
    check("ltv distribution has 1-8 buckets", 1 <= len(results["ltv_distribution"]) <= 8,
          f"got {len(results['ltv_distribution'])}")
    check("customer sample has 25 rows", len(results["customer_sample"]) == 25,
          f"got {len(results['customer_sample'])}")

    # ------------------------------------------------------------------ 3.
    proc = None
    try:
        try:
            with urllib.request.urlopen(HEALTH_URL, timeout=5) as r:
                already_running = r.status == 200
        except Exception:
            already_running = False

        if already_running:
            check(f"dashboard health endpoint -> 200 (port {DASHBOARD_PORT})", True, "already running")
        else:
            (REPO_ROOT / ".logs").mkdir(exist_ok=True)
            log = (REPO_ROOT / ".logs" / "dashboard_verify.log").open("a")
            proc = subprocess.Popen(
                [
                    str(REPO_ROOT / ".venv" / "bin" / "streamlit"), "run",
                    str(REPO_ROOT / "dashboard" / "app.py"),
                    "--server.port", str(DASHBOARD_PORT),
                    "--server.address", "localhost",
                    "--server.headless", "true",
                ],
                cwd=str(REPO_ROOT),
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            ok = False
            for _ in range(60):
                try:
                    with urllib.request.urlopen(HEALTH_URL, timeout=3) as r:
                        if r.status == 200:
                            ok = True
                            break
                except Exception:
                    pass
                if proc.poll() is not None:
                    break
                time.sleep(2)
            check(f"dashboard health endpoint -> 200 (port {DASHBOARD_PORT})", ok,
                  "throwaway headless instance" if ok else "see .logs/dashboard_verify.log")
    finally:
        if proc is not None:  # we started it - stop it again
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
            log.close()

    print("=" * 60)
    if failures:
        print(f"RESULT: FAIL - {len(failures)} check(s) failed")
        return 1
    print("RESULT: PASS - Streamlit dashboard verified")
    return 0


if __name__ == "__main__":
    sys.exit(main())
