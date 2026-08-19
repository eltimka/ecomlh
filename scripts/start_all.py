#!/usr/bin/env python3
"""One-command startup for the full Customer 360 lakehouse.

Run: .venv/bin/python scripts/start_all.py   (or: make run)

Steps (each idempotent - safe to re-run at any time):
  1. docker compose up -d + wait until the stack is healthy
  2. generate synthetic data if data/synthetic/ is missing
  3. bootstrap MinIO buckets + Trino schemas
  4. start the Streamlit Customer 360 dashboard (port 8501)
  5. run the lakehouse_refresh Dagster job (bronze -> silver -> gold
     with all 85 data-quality checks)
  6. run the quick verification suite
     (lakehouse, bronze, silver, gold, dashboard)

The heavy DQ verification (scripts/verify_dq.py - a second full refresh
run) is left out of the fast path; run `make verify-dq` for it.

At the end you get:
  - Customer 360 dashboard: http://localhost:8501  (Streamlit)
  - Dagster UI:    http://localhost:3000  (start with `make dev`)
  - Trino:         localhost:8080 (user: admin, catalog: iceberg)
  - MinIO console: http://localhost:9001 (minioadmin / minioadmin)
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ["docker", "compose", "-f", str(REPO_ROOT / "docker" / "docker-compose.yml")]
VENV = REPO_ROOT / ".venv" / "bin"
DAGSTER_PROJECT = REPO_ROOT / "dagster_project"
LOGS_DIR = REPO_ROOT / ".logs"
DASHBOARD_PORT = int(os.environ.get("STREAMLIT_PORT", "8501"))

# long-running services that must be (healthy); one-shot inits may be Exited (0)
LONG_RUNNING = {"minio", "lakehouse-postgres", "hive-metastore", "trino"}


def run(cmd: list[str], cwd: Path | None = None, **kw) -> subprocess.CompletedProcess:
    print(f"\n$ {' '.join(cmd)}")
    proc = subprocess.run(cmd, cwd=str(cwd or REPO_ROOT), **kw)
    return proc


def fail(msg: str) -> None:
    print(f"\nFATAL: {msg}")
    sys.exit(1)


def stack_status() -> list[tuple[str, str]]:
    proc = subprocess.run(
        COMPOSE + ["ps", "--format", "{{.Name}}\t{{.Status}}"],
        capture_output=True, text=True,
    )
    rows = []
    for line in proc.stdout.splitlines():
        if "\t" in line:
            name, status = line.split("\t", 1)
            rows.append((name, status.strip()))
    return rows


def step_up() -> None:
    print("=" * 62)
    print("[1/6] docker compose up + wait for healthy stack")
    print("=" * 62)
    proc = subprocess.run(COMPOSE + ["up", "-d"], cwd=str(REPO_ROOT))
    if proc.returncode != 0:
        fail("docker compose up failed")

    deadline = time.time() + 600
    last = ""
    while time.time() < deadline:
        rows = stack_status()
        by_name = {n: s for n, s in rows}
        problems = []
        for svc in sorted(LONG_RUNNING):
            status = by_name.get(svc, "MISSING")
            if "(unhealthy)" in status or "health: starting" in status:
                problems.append(f"{svc}={status}")
        if not problems and len(by_name) >= len(LONG_RUNNING):
            for name, status in rows:
                print(f"  {name:<20} {status}")
            print("  stack is up")
            return
        last = "; ".join(problems) if problems else f"waiting ({len(by_name)} containers)"
        print(f"  waiting... {last}")
        time.sleep(5)
    fail(f"stack did not become healthy in time (last: {last})")


def step_data() -> None:
    print("=" * 62)
    print("[2/6] synthetic data")
    print("=" * 62)
    manifest = REPO_ROOT / "data" / "synthetic" / "manifest.json"
    if manifest.exists():
        print(f"  {manifest.relative_to(REPO_ROOT)} exists - skipping generation")
        return
    proc = run([str(VENV / "python"), "data_generator/generate_synthetic.py"])
    if proc.returncode != 0:
        fail("synthetic data generation failed")


def step_bootstrap() -> None:
    print("=" * 62)
    print("[3/6] bootstrap MinIO buckets + Trino schemas")
    print("=" * 62)
    proc = run([str(VENV / "python"), "scripts/bootstrap_minio.py"])
    if proc.returncode != 0:
        fail("minio bootstrap failed")


def dashboard_health_ok() -> bool:
    url = f"http://localhost:{DASHBOARD_PORT}/_stcore/health"
    try:
        with urllib.request.urlopen(url, timeout=3) as r:
            return r.status == 200
    except Exception:
        return False


def stop_dashboard() -> None:
    """Stop a previously started dashboard process (pid file based)."""
    pid_file = LOGS_DIR / "dashboard.pid"
    if not pid_file.exists():
        return
    try:
        os.kill(int(pid_file.read_text().strip()), 15)
    except (ValueError, OSError):
        pass
    pid_file.unlink(missing_ok=True)


def step_dashboard() -> None:
    print("=" * 62)
    print(f"[4/6] Streamlit Customer 360 dashboard (port {DASHBOARD_PORT})")
    print("=" * 62)
    if dashboard_health_ok():
        print(f"  already running at http://localhost:{DASHBOARD_PORT} - skipping")
        return
    stop_dashboard()
    LOGS_DIR.mkdir(exist_ok=True)
    log = (LOGS_DIR / "dashboard.log").open("a")
    proc = subprocess.Popen(
        [
            str(VENV / "streamlit"), "run", str(REPO_ROOT / "dashboard" / "app.py"),
            "--server.port", str(DASHBOARD_PORT),
            "--server.address", "localhost",
            "--server.headless", "true",
        ],
        cwd=str(REPO_ROOT),
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    (LOGS_DIR / "dashboard.pid").write_text(str(proc.pid))
    deadline = time.time() + 120
    while time.time() < deadline:
        if dashboard_health_ok():
            print(f"  dashboard up at http://localhost:{DASHBOARD_PORT} (pid {proc.pid})")
            return
        if proc.poll() is not None:
            fail("streamlit exited during startup - see .logs/dashboard.log")
        time.sleep(2)
    fail(f"dashboard did not become healthy in time - see .logs/dashboard.log")


def step_refresh() -> None:
    print("=" * 62)
    print("[5/6] materialize the lakehouse (lakehouse_refresh job, all checks)")
    print("=" * 62)
    proc = run(
        [
            str(VENV / "dagster"), "job", "execute",
            "-m", "ecommerce_lakehouse.definitions",
            "-j", "lakehouse_refresh",
        ],
        cwd=REPO_ROOT / "dagster_project",
    )
    if proc.returncode != 0:
        fail("lakehouse_refresh job failed (asset checks may have caught something)")


def step_verify() -> None:
    print("=" * 62)
    print("[6/6] quick verification suite")
    print("=" * 62)
    for script in [
        "verify_lakehouse.py",
        "verify_bronze.py",
        "verify_silver.py",
        "verify_gold.py",
        "verify_dashboard.py",
    ]:
        proc = run([str(VENV / "python"), f"scripts/{script}"], capture_output=True, text=True)
        last = proc.stdout.strip().splitlines()[-1] if proc.stdout.strip() else ""
        print(f"  {script:<22} rc={proc.returncode}  {last}")
        if proc.returncode != 0:
            print(proc.stdout[-3000:])
            fail(f"{script} failed")


def main() -> None:
    t0 = time.time()
    step_up()
    step_data()
    step_bootstrap()
    step_dashboard()
    step_refresh()
    step_verify()
    print("\n" + "=" * 62)
    print("Lakehouse is ready. Open:")
    print(f"  Customer 360 dashboard : http://localhost:{DASHBOARD_PORT}  (Streamlit)")
    print("  Dagster UI             : .venv/bin/dagster dev  -> http://localhost:3000")
    print("  Trino                  : localhost:8080 (user: admin, catalog: iceberg)")
    print("  MinIO console          : http://localhost:9001 (minioadmin / minioadmin)")
    print(f"  Full DQ suite          : make verify-dq   (took {time.time() - t0:.0f}s total)")
    print("=" * 62)


if __name__ == "__main__":
    main()
