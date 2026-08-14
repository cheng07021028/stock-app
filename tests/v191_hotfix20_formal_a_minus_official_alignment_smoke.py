# -*- coding: utf-8 -*-
"""V191-H20 Formal/A- official-factor alignment regression smoke.

This test is intentionally network/Streamlit free. It reproduces the exact bug:
a row that qualifies for A- only after per-stock official dates are available was
classified before the official cache merge, then that stale partition was kept.
H20 must preserve risk/RR gates while fixing the ordering and cold-cache evidence.
"""
from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import godpick_execution_governance as gov  # noqa: E402
import godpick_formal_recommendation_engine as formal  # noqa: E402
from godpick_v188_cache_guard import repair_v188_decision_frame  # noqa: E402


def _a_minus_row(rr: float = 1.20) -> dict:
    return {
        "股票代號": "9999", "股票名稱": "H20測試A股", "市場別": "上市", "類別": "半導體",
        "最新價": 100, "推薦買點_拉回": 99, "實戰觸發價": 102, "觸發後守價": 100,
        "實戰停損距離%": 5.0, "實戰壓力空間%": 6.0,
        "實戰風險報酬比": rr, "風險報酬比": rr,
        "買進分數": 68, "Entry進場買點分": 75, "Risk風控安全分": 68, "股神實戰總分": 75,
        "技術結構分數": 75, "起漲前兆分數": 72, "交易可行分數": 70,
        "推薦總分": 78, "候選強度分": 78,
        "主流資金分": 65, "族群攻擊強度": 60, "族群輪動分": 60, "資金攻擊有效分": 65,
        "成交額百萬": 400, "20日均成交額百萬": 350, "最新成交量_張": 1500, "20日均量_張": 1400,
        "追價風險分": 45, "近5日漲幅%": 1.0, "近20日漲幅%": 8.0,
        "今日漲幅%": 1.0, "當日量比": 1.1, "當日收盤位置%": 70,
        "爆發雷達分": 60, "隔日爆發分": 60, "強勢動能分": 60, "強勢前兆分": 60,
        "推薦角色": "B｜等突破確認", "族群廣度分": 60,
        "K線最後交易日": "2026-08-14", "K線落後交易日": 0,
        "K線資料新鮮度": "最新", "本輪市場最新交易日": "2026-08-14",
        "大盤資料日期": "2026-08-14", "大盤資料新鮮度": "最新",
        "大盤風險燈號": "綠燈", "大盤策略模式": "多頭攻擊", "大盤橋接分數": 70,
    }


def _official_overlay(row: dict, date: str = "2026-08-14") -> dict:
    return {
        **row,
        "官方資料完整度": 100,
        "官方因子資料狀態": "完整",
        "每日因子來源可信度": 100,
        "因子來源可信度": 100,
        "來源可信度狀態": "官方高可信",
        "官方資料日期": date,
        "官方因子資料日期": date,
        "法人資料日期": date,
        "估值資料日期": date,
    }


def test_reproduce_zero_gate_and_h20_recovery() -> None:
    # Pre-H20 order: classify first, with no per-stock official date.
    pre = formal.apply_formal_recommendation_engine(pd.DataFrame([_a_minus_row()]))
    assert pre.iloc[0]["正式推薦分區"] not in {"正式下週主推薦", "A-｜準主推薦小量試單"}, pre.iloc[0].to_dict()
    assert "日期未驗證" in str(pre.iloc[0]["官方因子新鮮度"]), pre.iloc[0].to_dict()

    # H20 order: official evidence first, then the exact same Formal/A- formula.
    post = formal.apply_formal_recommendation_engine(pd.DataFrame([_official_overlay(_a_minus_row())]))
    assert post.iloc[0]["正式推薦分區"] == "A-｜準主推薦小量試單", post.iloc[0].to_dict()
    assert "最新" in str(post.iloc[0]["官方因子新鮮度"]), post.iloc[0].to_dict()
    assert str(post.iloc[0]["推薦升級判定路徑"]).startswith("A-｜"), post.iloc[0].to_dict()


def test_risk_rr_guard_was_not_relaxed() -> None:
    too_low_rr = formal.apply_formal_recommendation_engine(
        pd.DataFrame([_official_overlay(_a_minus_row(rr=0.95))])
    )
    assert too_low_rr.iloc[0]["正式推薦分區"] not in {
        "正式下週主推薦", "A-｜準主推薦小量試單"
    }, too_low_rr.iloc[0].to_dict()


def test_candidate_diagnosis_keeps_official_evidence() -> None:
    row = _official_overlay(_a_minus_row())
    row.update({
        "官方因子新鮮度": "最新/對齊",
        "官方因子落後交易日": 0,
        "V191_H20正式分區官方對齊": "是",
        "V191_H20正式分區版本": "v191_h20_official_before_formal_20260814",
    })
    decided = formal.apply_formal_recommendation_engine(pd.DataFrame([row]))
    decided["V191_H20正式分區官方對齊"] = "是"
    decided["V191_H20正式分區版本"] = "v191_h20_official_before_formal_20260814"
    diag = gov.build_candidate_diagnosis(decided)
    required = {
        "官方資料日期", "官方因子資料日期", "官方因子落後交易日", "官方因子新鮮度",
        "每日因子來源可信度", "因子來源可信度", "來源可信度狀態",
        "V191_H20正式分區官方對齊", "V191_H20正式分區版本",
    }
    assert required.issubset(diag.columns), sorted(required - set(diag.columns))
    assert str(diag.iloc[0]["官方因子新鮮度"]).strip() != ""


def test_v188_repair_orders_official_before_formal() -> None:
    calls: list[str] = []
    raw = pd.DataFrame([{"股票代號": "9999"}])

    def official(df: pd.DataFrame) -> pd.DataFrame:
        calls.append("official")
        out = df.copy()
        out["官方資料日期"] = "2026-08-14"
        return out

    def formal_after(df: pd.DataFrame) -> pd.DataFrame:
        calls.append("formal")
        assert "官方資料日期" in df.columns
        out = df.copy()
        out["V191_H20正式分區官方對齊"] = "是"
        return out

    def super_ai(df: pd.DataFrame) -> pd.DataFrame:
        calls.append("super")
        out = df.copy()
        out["V188版本"] = "test"
        out["SuperAI Alpha等級"] = "A"
        out["SuperAI Trade等級"] = "B"
        out["SuperAI最終作戰等級"] = "RADAR"
        out["V188交易許可"] = "WAIT"
        out["V188正式推薦資格"] = "否"
        out["V188股神作戰優先分"] = 70.0
        out["SuperAI Alpha分"] = 80.0
        out["SuperAI Trade分"] = 65.0
        return out

    repaired, report = repair_v188_decision_frame(
        raw,
        official_factor_callable=official,
        formal_recommendation_callable=formal_after,
        super_ai_callable=super_ai,
    )
    assert calls[:3] == ["official", "formal", "super"], calls
    assert report["complete"] is True, report
    assert repaired.iloc[0]["V191_H20正式分區官方對齊"] == "是"


def test_page07_wiring_is_official_then_formal() -> None:
    src = (ROOT / "pages" / "7_股神推薦.py").read_text(encoding="utf-8")
    assert "H20_FORMAL_ALIGNMENT_VERSION" in src
    helper_start = src.index("def _h20_rebuild_formal_partition_after_official_factors")
    helper_end = src.index("def _recalc_night_strategy_after_macro_v100", helper_start)
    helper = src[helper_start:helper_end]
    official_pos = helper.index("_apply_official_factor_cache_v109(work)")
    formal_pos = helper.index("apply_formal_recommendation_engine(work)")
    assert official_pos < formal_pos, (official_pos, formal_pos)

    build_start = src.index("def _build_recommend_df")
    main_call = src.index("governed_candidate_df = _h20_rebuild_formal_partition_after_official_factors(base_df)", build_start)
    later_scan = src.index("build_scan_quality_report(", main_call)
    assert build_start < main_call < later_scan
    assert '"V191_H20正式分區官方對齊"' in src
    assert "funnel_official" in src and "官方因子對齊" in src


def main() -> None:
    test_reproduce_zero_gate_and_h20_recovery()
    test_risk_rr_guard_was_not_relaxed()
    test_candidate_diagnosis_keeps_official_evidence()
    test_v188_repair_orders_official_before_formal()
    test_page07_wiring_is_official_then_formal()
    print("PASS V191-H20 formal/A- official alignment")


if __name__ == "__main__":
    main()
