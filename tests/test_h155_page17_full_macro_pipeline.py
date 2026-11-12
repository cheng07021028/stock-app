# -*- coding: utf-8 -*-
"""H155 deterministic offline regression: Page17 full Page01 parity and same-run gate."""
from __future__ import annotations

import ast
from pathlib import Path
from godpick_auto_scheduler import (
    H155_REQUIRED_REFRESH_JOBS,
    h155_verify_same_run_dependencies,
)

ROOT = Path(__file__).resolve().parents[1]


def _rows(statuses):
    return [{"job": k, "status": v} for k, v in statuses.items()]


def test_all_same_run_prerequisites_pass():
    statuses = {k: "SUCCESS" for k in H155_REQUIRED_REFRESH_JOBS}
    ok, msg = h155_verify_same_run_dependencies(_rows(statuses))
    assert ok and not msg


def test_missing_macro_blocks_prior_success_does_not_count():
    statuses = {k: "SUCCESS" for k in H155_REQUIRED_REFRESH_JOBS if k != "macro_full"}
    ok, msg = h155_verify_same_run_dependencies(_rows(statuses))
    assert not ok and "大盤" in msg and "NOT_RUN" in msg


def test_failed_macro_blocks():
    statuses = {k: "SUCCESS" for k in H155_REQUIRED_REFRESH_JOBS}
    statuses["macro_full"] = "FAILED"
    ok, msg = h155_verify_same_run_dependencies(_rows(statuses))
    assert not ok and "FAILED" in msg


def test_warning_official_blocks_until_verified_success():
    statuses = {k: "SUCCESS" for k in H155_REQUIRED_REFRESH_JOBS}
    statuses["official_factors"] = "WARNING"
    ok, msg = h155_verify_same_run_dependencies(_rows(statuses))
    assert not ok and "WARNING" in msg


def test_newest_status_controls_not_older_success():
    statuses = {k: "SUCCESS" for k in H155_REQUIRED_REFRESH_JOBS}
    rows = _rows(statuses) + [{"job": "macro_full", "status": "FAILED"}]
    ok, msg = h155_verify_same_run_dependencies(rows)
    assert not ok and "FAILED" in msg


def test_real_page01_canonical_runner_is_still_used():
    task_source = (ROOT / "godpick_auto_update_tasks.py").read_text(encoding="utf-8")
    assert 'ns.get("_v70_run_one_click_update_and_write")' in task_source
    assert '"all_required_updated"' in task_source
    assert '"all_required_written"' in task_source


def test_page17_dispatch_and_worker_paths_match():
    page = (ROOT / "pages" / "17_系統健康檢查.py").read_text(encoding="utf-8")
    runner = (ROOT / "tools" / "run_godpick_auto_scheduler_v191.py").read_text(encoding="utf-8")
    assert '_h156_dispatch_refresh("full_refresh_and_recommend")' in page
    assert 'manual_job in H156_MANUAL_BATCH_MODES' in runner
    assert 'strict_same_run=True' in runner
    assert 'temporary_enable_jobs=' in runner
    assert 'run_global_update(' in page  # the old fast path remains


def test_modified_python_files_parse():
    for rel in (
        "godpick_auto_scheduler.py",
        "tools/run_godpick_auto_scheduler_v191.py",
        "pages/17_系統健康檢查.py",
    ):
        ast.parse((ROOT / rel).read_text(encoding="utf-8"), filename=rel)
