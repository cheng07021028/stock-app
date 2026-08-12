# -*- coding: utf-8 -*-
"""V180 offline regression/performance tests for page 7 recommendation pipeline."""
from __future__ import annotations

import ast
import sys
import tempfile
import time
import types
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

PAGE = ROOT / "pages" / "7_股神推薦.py"
SRC = PAGE.read_text(encoding="utf-8")
TREE = ast.parse(SRC)


def extract(name: str, ns: dict):
    node = next(n for n in TREE.body if isinstance(n, ast.FunctionDef) and n.name == name)
    mod = ast.Module(body=[node], type_ignores=[])
    ast.fix_missing_locations(mod)
    exec(compile(mod, str(PAGE), "exec"), ns)
    return ns[name]


def test_no_duplicate_full_history_retry():
    calls = {"n": 0}
    def slow_empty(**kwargs):
        calls["n"] += 1
        time.sleep(0.08)
        return pd.DataFrame()
    ns = {
        "pd": pd,
        "date": date,
        "time": time,
        "get_history_data": slow_empty,
        "get_history_data_debug": None,
        "HISTORY_DEBUG_ON_FAIL": False,
        "V180_DISABLE_DUPLICATE_HISTORY_RETRY": True,
        "_safe_str": lambda v: "" if v is None else str(v).strip(),
        "_prepare_history_df": lambda df: df if isinstance(df, pd.DataFrame) else pd.DataFrame(),
    }
    fn = extract("_get_history_smart", ns)
    t0 = time.perf_counter()
    out, market, dbg = fn("9999", "測試", "上市", date(2026, 1, 1), date(2026, 8, 8))
    elapsed = time.perf_counter() - t0
    assert calls["n"] == 1, calls
    assert out.empty
    assert any(a.get("source") == "v180_no_duplicate_full_pipeline_retry" for a in dbg.get("attempts", [])), dbg
    assert elapsed < 0.16, elapsed


def test_page7_wires_nonblocking_persistence_and_checkpoint_reuse():
    assert any(v in SRC for v in [
        'PAGE07_SPEED_FIX_VERSION = "page07_v180_nonblocking_full_market_pipeline_20260809"',
        'PAGE07_SPEED_FIX_VERSION = "page07_v181_single_pass_final_decision_20260809"',
        'PAGE07_SPEED_FIX_VERSION = "page07_v183_super_ai_durable_perf_20260811"',
        'PAGE07_SPEED_FIX_VERSION = "page07_v185_durable_latest_authority_20260811"',
        'PAGE07_SPEED_FIX_VERSION = "page07_v189_v188_final_cache_guard_20260812"',
    ])
    assert "reuse_finished_checkpoint=bool(submit_recommend and not submit_refresh and not resume_scan_btn)" in SRC
    assert "save_rotation_snapshot(rotation_source, background_remote=True)" in SRC
    assert "background_remote=True" in SRC and "pre_scored=True" in SRC
    assert "save_calibration_samples(\n                    auto_source, max_near=24, max_missed=20, background_remote=True" in SRC
    assert "V180_DISABLE_DUPLICATE_HISTORY_RETRY = True" in SRC


def _prescored_frame(n: int = 50) -> pd.DataFrame:
    return pd.DataFrame([
        {
            "股票代號": f"{1000+i}", "股票名稱": f"測試{i}", "市場別": "上市", "類別": f"類股{i%5}",
            "AI綜合決策分": 70 + (i % 20), "AI模型版本": "test", "AI發現母體": "FULL-MARKET",
            "AI推薦資格": "AI-R1X", "AI主要召回路徑": "跨市場新強股", "K線最後交易日": "2026-08-07",
            "股神推薦優先分": 70 + (i % 20), "正式推薦分區": "盤中雷達追蹤", "今日訊號新鮮分": 60,
        }
        for i in range(n)
    ])


def test_learning_remote_is_nonblocking():
    import godpick_learning_system as gls
    fake = types.ModuleType("godpick_persistence_service")
    def slow_save(*args, **kwargs):
        time.sleep(0.45)
        return types.SimpleNamespace(permanent_ok=True, firestore_message="ok", github_message="ok")
    fake.save_named_json_permanent = slow_save
    old = sys.modules.get("godpick_persistence_service")
    sys.modules["godpick_persistence_service"] = fake
    try:
        with tempfile.TemporaryDirectory(prefix="v180_learning_") as td:
            df = _prescored_frame(80)
            t0 = time.perf_counter()
            ok, msgs, state = gls.save_learning_run(
                df, df.head(8), base_dir=td, persist_remote=True,
                background_remote=True, pre_scored=True,
            )
            latency = time.perf_counter() - t0
            assert ok
            assert latency < 0.30, latency
            assert any("背景同步" in m for m in msgs), msgs
            assert Path(td, state["last_run_path"]).exists()
            time.sleep(0.55)  # allow mocked background remote worker to finish before temp cleanup
    finally:
        if old is None:
            sys.modules.pop("godpick_persistence_service", None)
        else:
            sys.modules["godpick_persistence_service"] = old


def test_rotation_remote_is_nonblocking():
    import godpick_recommendation_rotation as rot
    fake = types.ModuleType("godpick_persistence_service")
    def slow_save(*args, **kwargs):
        time.sleep(0.45)
        return types.SimpleNamespace(permanent_ok=True)
    fake.save_named_json_permanent = slow_save
    old = sys.modules.get("godpick_persistence_service")
    sys.modules["godpick_persistence_service"] = fake
    try:
        with tempfile.TemporaryDirectory(prefix="v180_rotation_") as td:
            df = _prescored_frame(12)
            t0 = time.perf_counter()
            ok, msg = rot.save_rotation_snapshot(df, base_dir=td, persist_remote=True, background_remote=True)
            latency = time.perf_counter() - t0
            assert ok, msg
            assert latency < 0.40, latency
            assert "背景同步" in msg
            assert Path(td, rot.ROTATION_HISTORY_FILE).exists()
            time.sleep(0.55)  # allow mocked background remote worker to finish before temp cleanup
    finally:
        if old is None:
            sys.modules.pop("godpick_persistence_service", None)
        else:
            sys.modules["godpick_persistence_service"] = old


def test_calibration_remote_is_nonblocking():
    import godpick_calibration_sample_service as cal
    old_build = cal.build_calibration_samples
    old_read = cal._read_github
    old_gh = cal._sync_github
    old_fs = cal._sync_firestore
    old_path = cal.DEFAULT_CALIBRATION_PATH
    try:
        with tempfile.TemporaryDirectory(prefix="v180_cal_") as td:
            cal.DEFAULT_CALIBRATION_PATH = str(Path(td) / "calibration.json")
            cal.build_calibration_samples = lambda *a, **k: pd.DataFrame([{
                "校正樣本鍵": "x1", "校正樣本類型": "C｜近門檻", "股票代號": "1234", "推薦日期": "2026-08-07"
            }])
            def slow_read():
                time.sleep(0.45); return [], "ok"
            def slow_sync(records):
                time.sleep(0.45); return True, "ok"
            cal._read_github = slow_read
            cal._sync_github = slow_sync
            cal._sync_firestore = slow_sync
            t0 = time.perf_counter()
            added, msgs, summary = cal.save_calibration_samples(pd.DataFrame([{"股票代號":"1234"}]), background_remote=True)
            latency = time.perf_counter() - t0
            assert added == 1
            assert latency < 0.40, latency
            assert any("背景" in m for m in msgs), msgs
            assert Path(cal.DEFAULT_CALIBRATION_PATH).exists()
            time.sleep(1.45)  # mocked read + GitHub + Firestore background chain
    finally:
        cal.build_calibration_samples = old_build
        cal._read_github = old_read
        cal._sync_github = old_gh
        cal._sync_firestore = old_fs
        cal.DEFAULT_CALIBRATION_PATH = old_path


def main():
    test_no_duplicate_full_history_retry()
    test_page7_wires_nonblocking_persistence_and_checkpoint_reuse()
    test_learning_remote_is_nonblocking()
    test_rotation_remote_is_nonblocking()
    test_calibration_remote_is_nonblocking()
    print("PASS V180 page7: no duplicate history pipeline, nonblocking post-scan persistence, prescored learning and checkpoint reuse wiring")


if __name__ == "__main__":
    main()
