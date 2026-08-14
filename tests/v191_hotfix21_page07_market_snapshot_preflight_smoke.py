# -*- coding: utf-8 -*-
"""V191-H21 Page07 same-run market snapshot preflight regression smoke.

H21 fixes the cross-runner split where ``macro_full`` succeeds in one GitHub
Actions wake-up, but the later recommendation runner checks out an old repository
``market_snapshot.json`` and therefore classifies every formal/A- candidate
through the same stale-market gate.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import godpick_recommendation_market_preflight as preflight  # noqa: E402

TAIPEI = timezone(timedelta(hours=8))


def _write_snapshot(root: Path, *, data_date: str, updated_at: str) -> None:
    (root / "market_snapshot.json").write_text(
        json.dumps(
            {
                "version": "test",
                "updated_at": updated_at,
                "data_date": data_date,
                "market_score": 55,
                "market_trend": "偏多",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def test_stale_checkout_is_rehydrated_in_same_runner(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_snapshot(
        tmp_path,
        data_date="2026-07-09",
        updated_at="2026-07-11 23:01:10",
    )

    now = datetime.now(TAIPEI)
    today = now.strftime("%Y-%m-%d")
    fake_macro = ModuleType("macro_startup_service")

    def fake_run_fast_update(*, sync_github: bool, max_runtime_seconds: int):
        assert sync_github is False
        assert 10 <= max_runtime_seconds <= 45
        _write_snapshot(
            tmp_path,
            data_date=today,
            updated_at=now.strftime("%Y-%m-%d %H:%M:%S"),
        )
        return {"ok": True, "message": "same-run refresh ok"}

    fake_macro._run_fast_update = fake_run_fast_update  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "macro_startup_service", fake_macro)

    report = preflight.ensure_page07_market_snapshot_current(
        base_dir=tmp_path,
        max_runtime_seconds=20,
    )
    assert report["ok"] is True, report
    assert report["mode"] == "same_run_market_refresh", report
    assert report["before_market_date"] == "2026-07-09", report
    assert report["market_date"] == today, report
    assert report["refreshed"] is True, report


def test_stale_checkout_fails_closed_instead_of_persisting_false_zero(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_snapshot(
        tmp_path,
        data_date="2026-07-09",
        updated_at="2026-07-11 23:01:10",
    )
    fake_macro = ModuleType("macro_startup_service")

    def fake_run_fast_update(*, sync_github: bool, max_runtime_seconds: int):
        return {"ok": False, "message": "network unavailable"}

    fake_macro._run_fast_update = fake_run_fast_update  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "macro_startup_service", fake_macro)

    try:
        preflight.ensure_page07_market_snapshot_current(base_dir=tmp_path, max_runtime_seconds=20)
    except RuntimeError as exc:
        text = str(exc)
        assert "拒絕執行股神推薦" in text
        assert "正式推薦/A-準主推薦" in text
        assert "2026-07-09" in text
    else:
        raise AssertionError("stale market snapshot must fail closed")


def test_fresh_same_runtime_snapshot_does_not_refetch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(TAIPEI)
    _write_snapshot(
        tmp_path,
        data_date=now.strftime("%Y-%m-%d"),
        updated_at=now.strftime("%Y-%m-%d %H:%M:%S"),
    )
    fake_macro = ModuleType("macro_startup_service")

    def forbidden_refresh(**kwargs):
        raise AssertionError("fresh same-run snapshot must not refetch")

    fake_macro._run_fast_update = forbidden_refresh  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "macro_startup_service", fake_macro)

    report = preflight.ensure_page07_market_snapshot_current(base_dir=tmp_path)
    assert report["ok"] is True, report
    assert report["mode"] == "reuse_same_runtime_snapshot", report
    assert report["refreshed"] is False, report


def test_headless_page07_wiring_runs_preflight_before_page_exec() -> None:
    src = (ROOT / "godpick_headless_page_loader.py").read_text(encoding="utf-8")
    fn_start = src.index("def load_page_namespace")
    guard_pos = src.index("ensure_page07_market_snapshot_current", fn_start)
    read_pos = src.index('source = path.read_text(encoding="utf-8-sig")', fn_start)
    exec_pos = src.index("exec(code, ns, ns)", fn_start)
    assert fn_start < guard_pos < read_pos < exec_pos
    assert 'path.name == "7_股神推薦.py"' in src
    assert 'st.session_state["v191_h21_page07_market_preflight"]' in src


def main() -> None:
    # Keep a direct script entry for the project's existing smoke-test style.
    from tempfile import TemporaryDirectory
    from _pytest.monkeypatch import MonkeyPatch

    mp = MonkeyPatch()
    try:
        with TemporaryDirectory() as td:
            test_stale_checkout_is_rehydrated_in_same_runner(Path(td), mp)
        mp.undo(); mp = MonkeyPatch()
        with TemporaryDirectory() as td:
            test_stale_checkout_fails_closed_instead_of_persisting_false_zero(Path(td), mp)
        mp.undo(); mp = MonkeyPatch()
        with TemporaryDirectory() as td:
            test_fresh_same_runtime_snapshot_does_not_refetch(Path(td), mp)
        mp.undo()
        test_headless_page07_wiring_runs_preflight_before_page_exec()
    finally:
        mp.undo()
    print("PASS V191-H21 Page07 same-run market snapshot preflight")


if __name__ == "__main__":
    main()
