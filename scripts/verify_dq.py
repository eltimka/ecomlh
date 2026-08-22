#!/usr/bin/env python3
"""Verify the Phase 8 data-quality layer (Dagster asset checks).

Checks:
  1. Condition evaluator unit tests (zero / positive / range_cover /
     boolean_true / gmv_spread / z_lt_3 / max_month / info, pass and fail
     sides).
  2. Live negative test: a deliberately failing check is recorded as
     failed (the suite is not vacuously green).
  3. Full ``lakehouse_refresh`` job run in a fresh instance:
     all 25 assets (22 tables + 3 layer gates) materialize and all 91
     asset checks evaluate to
     passed.

Exit code 0 + "RESULT: PASS" when everything is green.
"""

from __future__ import annotations

import os
import sys
import tempfile
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "dagster_project"))

import dagster as dg  # noqa: E402
from dagster import DagsterInstance  # noqa: E402
from dagster._core.storage.asset_check_execution_record import (  # noqa: E402
    AssetCheckExecutionRecordStatus,
)

from ecommerce_lakehouse.assets import checks as checks_mod  # noqa: E402
from ecommerce_lakehouse.definitions import definitions  # noqa: E402

EXPECTED_ASSETS = 28  # 25 tables (21 marts/dims + 4 stream virtuals) + 3 per-layer gate (barrier) assets
EXPECTED_CHECKS = 109


def main() -> int:
    failures: list[str] = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}" + (f" ({detail})" if detail else ""))
        if not ok:
            failures.append(name)

    print("Data quality layer verification")
    print("=" * 60)

    # ------------------------------------------------------------------ 1.
    print("[1] Condition evaluator unit tests")
    ev = checks_mod._evaluate
    end = checks_mod.DATA_DATE_END

    cases = [
        ("zero: pass", {"condition": "zero"}, (0,), True),
        ("zero: fail", {"condition": "zero"}, (3,), False),
        ("positive: pass", {"condition": "positive"}, (5000,), True),
        ("positive: fail", {"condition": "positive"}, (0,), False),
        (
            "range_cover: pass",
            {"condition": "range_cover"},
            (date(2024, 1, 1), date(2024, 12, 31)),
            True,
        ),
        (
            "range_cover: fail (stale max)",
            {"condition": "range_cover"},
            (date(2024, 1, 1), date(2024, 6, 30)),
            False,
        ),
        (
            "range_refunds: pass (max may exceed end - refund lag)",
            {"condition": "range_cover", "range_kind": "refunds"},
            (date(2024, 1, 6), date(2025, 2, 14)),
            True,
        ),
        (
            "range_refunds: fail (starts before range)",
            {"condition": "range_cover", "range_kind": "refunds"},
            (date(2023, 12, 1), date(2025, 2, 14)),
            False,
        ),
        (
            "range_fresh: pass (max within 7d of end, sparse table)",
            {"condition": "range_cover", "range_kind": "fresh", "fresh_days": 7},
            (date(2024, 1, 1), date(2024, 12, 28)),
            True,
        ),
        (
            "range_fresh: fail (max too old)",
            {"condition": "range_cover", "range_kind": "fresh", "fresh_days": 7},
            (date(2024, 1, 1), date(2024, 12, 1)),
            False,
        ),
        ("boolean_true: pass", {"condition": "boolean_true"}, (True,), True),
        ("boolean_true: fail", {"condition": "boolean_true"}, (False,), False),
        (
            "gmv_spread: pass",
            {"condition": "gmv_spread"},
            (100.0, 100.0, 100.0, 100.0, 100.0),
            True,
        ),
        (
            "gmv_spread: fail",
            {"condition": "gmv_spread"},
            (100.0, 100.0, 99.5, 100.0, 100.0),
            False,
        ),
        ("z_lt_3: pass", {"condition": "z_lt_3"}, (1.7,), True),
        ("z_lt_3: fail", {"condition": "z_lt_3"}, (3.4,), False),
        (
            "max_month: pass",
            {"condition": "max_month"},
            (date(end.year, end.month, 1),),
            True,
        ),
        (
            "max_month: fail",
            {"condition": "max_month"},
            (date(end.year, 1, 1),),
            False,
        ),
        (
            "range_stream: pass (max may exceed end - live events)",
            {"condition": "range_cover", "range_kind": "stream"},
            (date(2024, 1, 1), date(2025, 2, 14)),
            True,
        ),
        (
            "range_stream: fail (stale max, stream not fresh)",
            {"condition": "range_cover", "range_kind": "stream"},
            (date(2024, 1, 1), date(2024, 6, 30)),
            False,
        ),
        (
            "range_stream: fail (empty table)",
            {"condition": "range_cover", "range_kind": "stream"},
            (None, None),
            False,
        ),
        (
            "info: pass (informational count, any value)",
            {"condition": "info"},
            (4999,),
            True,
        ),
    ]
    for name, spec, row, expected in cases:
        passed, _ = ev(spec, row)
        check(name, passed is expected)

    # kafka_lag: patch the Kafka accessor so these cases stay pure
    _lag_spec = {"condition": "kafka_lag", "topic": "raw.web_events", "lag_tolerance": 1000}
    real_end_offset = checks_mod._kafka_end_offset
    try:
        checks_mod._kafka_end_offset = lambda topic: 50_000
        lag_cases = [
            ("kafka_lag: pass (caught up)", (50000,), True),
            ("kafka_lag: pass (small checkpoint lag)", (49800,), True),
            ("kafka_lag: fail (consumer behind)", (48000,), False),
            ("kafka_lag: fail (over-appended table)", (52000,), False),
        ]
        for name, row, expected in lag_cases:
            passed, _ = ev(dict(_lag_spec), row)
            check(name, passed is expected)
        checks_mod._kafka_end_offset = lambda topic: None
        passed, _ = ev(dict(_lag_spec), (50000,))
        check("kafka_lag: fail (kafka unreachable)", passed is False)
    finally:
        checks_mod._kafka_end_offset = real_end_offset

    # ------------------------------------------------------------------ 2.
    print("[2] Live negative test (deliberately failing check)")
    bad_spec = {
        "layer": "gold",
        "table": "monthly_kpis",
        "name": "selftest_must_fail",
        "description": "self-test: a check that must fail",
        "sql": "SELECT 1",
        "condition": "zero",
        "blocking": True,
    }
    bad_check = checks_mod._make_check(bad_spec)

    @dg.asset(name="dq_selftest_asset")
    def dq_selftest_asset() -> None:  # pragma: no cover
        return None

    selftest_defs = dg.Definitions(
        assets=[*definitions.assets, dq_selftest_asset],
        asset_checks=[*definitions.asset_checks, bad_check],
        jobs=[
            dg.define_asset_job(
                "dq_selftest_job",
                selection=dg.AssetSelection.assets(
                    dg.AssetKey(["gold", "monthly_kpis"])
                ),
            )
        ],
        resources=definitions.resources,
    )
    selftest_repo = selftest_defs.get_repository_def()
    home = tempfile.mkdtemp(prefix="dq_neg_")
    storage = os.path.join(home, "storage")
    os.makedirs(storage)
    instance = DagsterInstance.from_config(storage)
    job = selftest_repo.get_job("dq_selftest_job")
    job.execute_in_process(instance=instance, raise_on_error=False)

    bad_key = next(
        k
        for c in [bad_check]
        for k in c.asset_and_check_keys
        if k.name == "selftest_must_fail"
    )
    rec = instance.get_latest_asset_check_evaluation_record(bad_key)
    check(
        "failing check is recorded as FAILED (run fails)",
        rec is not None
        and rec.status == AssetCheckExecutionRecordStatus.FAILED,
        f"status={getattr(rec, 'status', None)}",
    )

    # ------------------------------------------------------------------ 3.
    print(f"[3] Full lakehouse_refresh run ({EXPECTED_ASSETS} assets + all asset checks)")
    home2 = tempfile.mkdtemp(prefix="dq_full_")
    storage2 = os.path.join(home2, "storage")
    os.makedirs(storage2)
    full_instance = DagsterInstance.from_config(storage2)
    repo = definitions.get_repository_def()
    full_job = repo.get_job("lakehouse_refresh")

    result = full_job.execute_in_process(
        instance=full_instance, raise_on_error=False
    )
    check("lakehouse_refresh run succeeded", result.success)

    total = passed = 0
    by_layer: dict[str, list[int]] = {}
    for c in definitions.asset_checks:
        for ck in c.asset_and_check_keys:
            total += 1
            rec = full_instance.get_latest_asset_check_evaluation_record(ck)
            good = (
                rec is not None
                and rec.status == AssetCheckExecutionRecordStatus.SUCCEEDED
            )
            passed += good
            layer = ck.asset_key.path[0]
            counts = by_layer.setdefault(layer, [0, 0])
            counts[0 if good else 1] += 1

    check(
        f"all {EXPECTED_CHECKS} asset checks evaluated & passed",
        total == EXPECTED_CHECKS and passed == EXPECTED_CHECKS,
        f"evaluated {total}, passed {passed}",
    )

    asset_keys = sorted(
        {
            k
            for ad in definitions.assets
            for k in ad.asset_and_check_keys
            if len(k.path) == 2
        }
    )
    materialized = 0
    for k in asset_keys:
        me = full_instance.get_latest_materialization_event(k)
        if me is not None and me.run_id == result.run_id:
            materialized += 1
    check(
        f"all {EXPECTED_ASSETS} assets materialized in the run",
        materialized == EXPECTED_ASSETS,
        f"materialized: {materialized}",
    )

    for layer in ("bronze", "silver", "gold"):
        ok_n, bad_n = by_layer.get(layer, [0, 0])
        print(f"        info: {layer:<7} {ok_n}/{ok_n + bad_n} checks passed")

    print("=" * 60)
    if failures:
        print(f"RESULT: FAIL - {len(failures)} check(s) failed: {failures}")
        return 1
    print("RESULT: PASS - data quality layer verified")
    return 0


if __name__ == "__main__":
    sys.exit(main())
