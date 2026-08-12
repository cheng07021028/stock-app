# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from godpick_official_release_timing import evaluate_twse_t86_release_timing
import godpick_execution_governance as gov
import pandas as pd


def _now(h: int, m: int) -> datetime:
    return datetime(2026, 8, 12, h, m)


def _frame(n: int = 100) -> pd.DataFrame:
    rows = []
    for i in range(n):
        rows.append({
            "股票代號": f"{1100+i:04d}",
            "成交額百萬": 300,
            "20日均成交額百萬": 250,
            "最新成交量_張": 1000,
            "20日均量_張": 900,
            "官方資料完整度": 100,
            "官方因子資料狀態": "完整",
            "每日因子來源可信度": 100,
            "因子來源可信度": 100,
            "官方因子資料日期": "2026-08-11",
            "官方資料日期": "2026-08-11",
            "法人資料日期": "2026-08-11",
            "本輪市場最新交易日": "2026-08-12",
            "K線最後交易日": "2026-08-12",
        })
    return pd.DataFrame(rows)


def _summary(n: int = 100):
    return {"total_count": n, "analyzed_ok": n, "history_ok": n}


def test_1749_t1_is_normal_not_error():
    x = evaluate_twse_t86_release_timing(market_date="2026-08-12", official_date="2026-08-11", now=_now(17, 49))
    assert x["phase"] == "WAIT_FIRST_T86", x
    assert x["t1_is_normal_now"] is True, x
    assert x["level"] == "info", x
    assert "18:00" in x["next_milestone"], x
    assert "不是更新失敗" in x["detail"], x


def test_1830_waits_for_2000_final_file():
    x = evaluate_twse_t86_release_timing(market_date="2026-08-12", official_date="2026-08-11", now=_now(18, 30))
    assert x["phase"] == "WAIT_FINAL_T86", x
    assert x["t1_is_normal_now"] is True, x
    assert "20:00" in x["next_milestone"], x


def test_2010_grace_is_still_normal():
    x = evaluate_twse_t86_release_timing(market_date="2026-08-12", official_date="2026-08-11", now=_now(20, 10))
    assert x["phase"] == "FINAL_RELEASE_GRACE", x
    assert x["t1_is_normal_now"] is True, x
    assert x["same_day_final_expected"] is True, x


def test_2030_t1_becomes_update_warning_not_hard_data_error():
    x = evaluate_twse_t86_release_timing(market_date="2026-08-12", official_date="2026-08-11", now=_now(20, 30))
    assert x["phase"] == "T0_EXPECTED", x
    assert x["t1_is_normal_now"] is False, x
    assert x["level"] == "warning", x
    assert "重新更新" in x["detail"], x


def test_same_day_is_ready():
    x = evaluate_twse_t86_release_timing(market_date="2026-08-12", official_date="2026-08-12", now=_now(17, 49))
    assert x["phase"] == "T0_READY", x
    assert x["level"] == "success", x


def test_governance_1749_uses_normal_t1_wording_and_keeps_075():
    r = gov.build_scan_quality_report(
        _summary(), universe_size=100, candidate_count=100, final_count=8,
        candidate_frame=_frame(), now_taipei=_now(17, 49),
    )
    assert r["正式推薦可用"] is True, r
    assert abs(float(r["倉位折減係數"]) - 0.75) < 1e-9, r
    assert r["官方盤後T-1是否正常"] is True, r
    assert r["官方盤後產製階段"] == "WAIT_FIRST_T86", r
    assert "正常最新完整基準" in r["掃描品質狀態"], r


def test_page7_no_longer_calls_normal_t1_not_latest():
    src = (ROOT / "pages" / "7_股神推薦.py").read_text(encoding="utf-8")
    assert "ℹ️ 官方盤後資料時序正常" in src
    assert "股神推薦資料不是最新" not in src
    assert "18:00產製不含鉅額首版、20:00產製含鉅額完整版" in src


def test_page16_explains_official_production_times():
    src = (ROOT / "pages" / "16_官方因子快取中心.py").read_text(encoding="utf-8")
    assert "18:00 TWT86UC（不含鉅額）" in src
    assert "20:00 TWTAIUC（含鉅額）" in src
    assert "公開網站/OpenAPI同步可能稍晚" in src


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("PASS V190 TWSE release timing: 17:49 normal T-1, 18/20 milestones, grace, post-20 warning, governance/UI wiring")
