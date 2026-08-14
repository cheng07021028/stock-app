# -*- coding: utf-8 -*-
"""V191-H22 regression: a reboot must never move macro authority back to July."""
from __future__ import annotations

import copy
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import macro_runtime_authority as h22  # noqa: E402


def _snapshot(date_text: str, updated: str, close: float) -> dict:
    return {
        "version": "test",
        "updated_at": updated,
        "data_date": date_text,
        "twse_data_date": date_text,
        "otc_data_date": date_text,
        "futures_data_date": date_text,
        "twse_index": close,
        "twse_change": 12.3,
        "twse_change_pct": 0.25,
        "otc_index": 400.0,
        "otc_change": 1.2,
        "otc_change_pct": 0.3,
        "futures_index": close - 100,
        "futures_change": 88,
        "market_score": 70,
        "market_trend": "偏多",
    }


def _cache(date_text: str, close: float) -> dict:
    return {
        date_text.replace("-", ""): {
            "ok": True,
            "date": date_text,
            "used_date": date_text,
            "close": close,
            "updated_at": date_text + " 18:00:00",
        }
    }


def test_business_date_beats_deployment_age() -> None:
    old = _snapshot("2026-07-09", "2026-08-14 23:59:59", 22000)
    new = _snapshot("2026-08-14", "2026-08-14 20:29:13", 45811.01)
    assert h22.authority_key(new, "runtime-data") > h22.authority_key(old, "local")


def test_runtime_snapshot_restores_old_checkout_and_rebuilds_missing_market_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    old_snapshot = _snapshot("2026-07-09", "2026-07-11 23:01:10", 22000)
    new_snapshot = _snapshot("2026-08-14", "2026-08-14 20:29:13", 45811.01)
    old_cache = _cache("2026-07-09", 22000)

    (tmp_path / "market_snapshot.json").write_text(json.dumps(old_snapshot), encoding="utf-8")
    (tmp_path / "macro_market_close_cache.json").write_text(json.dumps(old_cache), encoding="utf-8")
    monkeypatch.setattr(h22, "BASE_DIR", tmp_path)
    h22.reset_macro_authority_process_guard()

    remote = {"market_snapshot.json": new_snapshot}
    monkeypatch.setattr(
        h22,
        "_read_runtime_remote",
        lambda name: (copy.deepcopy(remote.get(name)), "mock runtime-data"),
    )

    writes: dict[str, object] = {}
    def fake_write(name: str, payload):
        writes[name] = copy.deepcopy(payload)
        (tmp_path / name).write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        return True, "mock write"
    monkeypatch.setattr(h22, "_write_local", fake_write)

    report = h22.ensure_macro_runtime_authority_current(force=True, queue_newer_local=False)
    assert report["snapshot_business_date"] == "2026-08-14", report
    assert report["market_cache_business_date"] == "2026-08-14", report
    assert h22.business_date(writes["market_snapshot.json"]) == "2026-08-14"
    assert h22.business_date(writes["macro_market_close_cache.json"]) == "2026-08-14"
    rebuilt = writes["macro_market_close_cache.json"]
    assert isinstance(rebuilt, dict)
    assert float(rebuilt["20260814"]["close"]) == 45811.01
    assert rebuilt["20260814"]["authority_rebuilt_from"] == "market_snapshot.json"


def test_old_runtime_cannot_overwrite_newer_local(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    local_new = _snapshot("2026-08-14", "2026-08-14 21:00:00", 45811.01)
    remote_old = _snapshot("2026-07-09", "2026-08-14 23:59:59", 22000)
    (tmp_path / "market_snapshot.json").write_text(json.dumps(local_new), encoding="utf-8")
    monkeypatch.setattr(h22, "BASE_DIR", tmp_path)
    h22.reset_macro_authority_process_guard()
    monkeypatch.setattr(
        h22,
        "_read_runtime_remote",
        lambda name: (copy.deepcopy(remote_old) if name == "market_snapshot.json" else None, "mock old runtime"),
    )
    writes: dict[str, object] = {}
    monkeypatch.setattr(h22, "_write_local", lambda name, payload: (writes.setdefault(name, copy.deepcopy(payload)) is not None, "mock"))

    report = h22.ensure_macro_runtime_authority_current(force=True, queue_newer_local=False)
    snapshot_row = next(row for row in report["rows"] if row.get("file") == "market_snapshot.json")
    assert snapshot_row["chosen_source"] == "local", report
    assert snapshot_row["chosen_business_date"] == "2026-08-14", report
    assert "market_snapshot.json" not in writes


def test_app_auth_restores_macro_before_auth_core() -> None:
    src = (ROOT / "app_auth.py").read_text(encoding="utf-8")
    guard_pos = src.index("install_runtime_branch_guard()")
    restore_pos = src.index("ensure_macro_runtime_authority_current()")
    auth_pos = src.index("import app_auth_core as _core")
    assert guard_pos < restore_pos < auth_pos
    assert "2026-07-09" in src


def test_page07_h22_wiring_is_present() -> None:
    src = (ROOT / "godpick_recommendation_market_preflight.py").read_text(encoding="utf-8")
    assert "_restore_h22_macro_authority(force=False, queue_newer_local=False)" in src
    assert "_restore_h22_macro_authority(force=True, queue_newer_local=True)" in src
    assert "拒絕執行股神推薦" in src


def main() -> None:
    # Static subset for direct smoke execution; pytest covers temp-dir cases.
    test_business_date_beats_deployment_age()
    test_app_auth_restores_macro_before_auth_core()
    test_page07_h22_wiring_is_present()
    print("PASS V191-H22 macro authority no July rollback")


if __name__ == "__main__":
    main()
