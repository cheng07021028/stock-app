# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

import official_factor_service as ofs
import godpick_execution_governance as gov


def _official_row(i: int, *, good: bool = True, legacy_trust: int = 60) -> dict:
    if not good:
        return {
            "股票代號": f"{1100+i:04d}",
            "成交額百萬": 300,
            "20日均成交額百萬": 250,
            "最新成交量_張": 1000,
            "20日均量_張": 900,
            "官方資料完整度": 0,
            "官方因子資料狀態": "未取得官方資料",
            "本輪市場最新交易日": "2026-08-11",
            "K線最後交易日": "2026-08-11",
        }
    return {
        "股票代號": f"{1100+i:04d}",
        "成交額百萬": 300,
        "20日均成交額百萬": 250,
        "最新成交量_張": 1000,
        "20日均量_張": 900,
        "官方資料完整度": 90,
        "官方因子資料狀態": "完整",
        # Old V182-V186 bug: one previous-cache fill downgraded the whole row.
        "因子來源可信度": legacy_trust,
        "因子備援來源": "前次有效快取",
        # Actual daily provenance is official and T-1.
        "法人資料日期": "20260810",
        "法人資料源": "TWSE_T86",
        "估值資料日期": "20260810",
        "估值資料源": "TWSE_OPENAPI_BWIBBU_ALL",
        # Monthly revenue can be restored from prior cache; it must not poison daily trust.
        "營收資料日期": "20260701",
        "營收資料源": "前次有效快取",
        "外資近1日買賣超": 1200,
        "投信近1日買賣超": 100,
        "三大法人近1日合計": 1300,
        "法人籌碼官方分數": 75,
        "PER本益比": 18.5,
        "PBR股價淨值比": 2.2,
        "股利殖利率%": 2.0,
        "估算EPS": 8.5,
        "官方估值風險分數": 70,
        "當月營收": 100000,
        "月營收YoY%": 12.0,
        "營收成長官方分數": 68,
        "本輪市場最新交易日": "2026-08-11",
        "K線最後交易日": "2026-08-11",
    }


def test_legacy_60_is_rebuilt_from_actual_official_sources():
    row = _official_row(1)
    trust = ofs._derive_source_trust_v187(row)
    assert trust["每日因子來源可信度"] == 100, trust
    assert trust["因子來源可信度"] >= 80, trust
    assert trust["來源可信度狀態"] == "官方高可信", trust
    assert "法人:TWSE_T86=100" in trust["來源可信度說明"], trust
    assert "估值:TWSE_OPENAPI_BWIBBU_ALL=100" in trust["來源可信度說明"], trust


def test_838_effective_836_t1_no_longer_false_blocks():
    rows = [_official_row(i, good=(i < 84)) for i in range(100)]
    df = ofs._apply_source_trust_migration_v187(pd.DataFrame(rows))
    # Simulate the merged Page7 candidate frame after migration.
    for col in ["官方因子資料日期", "官方資料日期"]:
        if col not in df.columns:
            df[col] = ""
    derived = df.apply(ofs._row_daily_factor_date_v184, axis=1)
    df["官方因子資料日期"] = derived
    df["官方資料日期"] = derived
    report = gov.build_scan_quality_report(
        {"total_count": 100, "analyzed_ok": 100, "history_ok": 100},
        universe_size=100,
        candidate_count=100,
        final_count=8,
        candidate_frame=df,
    )
    assert report["官方有效因子覆蓋率%"] == 84.0, report
    assert report["官方來源可信覆蓋率%"] == 84.0, report
    assert report["官方日期T-1內覆蓋率%"] == 84.0, report
    assert report["官方最新可信覆蓋率%"] == 84.0, report
    assert report["正式推薦可用"] is True, report
    assert abs(float(report["倉位折減係數"]) - 0.75) < 1e-9, report
    assert "T-1" in report["掃描品質狀態"], report


def test_true_previous_cache_only_stays_blocked():
    rows = []
    for i in range(100):
        row = _official_row(i)
        row["法人資料源"] = "前次有效快取"
        row["估值資料源"] = "前次有效快取"
        row["營收資料源"] = "前次有效快取"
        rows.append(row)
    df = ofs._apply_source_trust_migration_v187(pd.DataFrame(rows))
    df["官方因子資料日期"] = "20260810"
    df["官方資料日期"] = "20260810"
    report = gov.build_scan_quality_report(
        {"total_count": 100, "analyzed_ok": 100, "history_ok": 100},
        universe_size=100,
        candidate_count=100,
        final_count=8,
        candidate_frame=df,
    )
    assert report["官方來源可信覆蓋率%"] == 0.0, report
    assert report["官方日期T-1內覆蓋率%"] == 100.0, report
    assert report["正式推薦可用"] is False, report
    assert "來源可信度不足" in report["掃描品質狀態"], report


def test_fallback_fill_does_not_downgrade_existing_official_trust():
    base = pd.DataFrame([{
        "股票代號": "2330",
        "因子來源可信度": 100,
        "法人資料源": "TWSE_T86",
        "估值資料源": "TWSE_OPENAPI_BWIBBU_ALL",
        "月營收YoY%": "",
    }])
    fallback = pd.DataFrame([{"股票代號": "2330", "月營收YoY%": 12.3, "FinMind資料日期": "20260801"}])
    merged, filled = ofs._coalesce_fallback(base, fallback, "FinMind", trust_score=82)
    assert filled == 1, merged.to_dict("records")
    assert float(merged.loc[0, "因子來源可信度"]) == 100.0, merged.loc[0].to_dict()


def test_v187_ui_wiring():
    page7 = (ROOT / "pages" / "7_股神推薦.py").read_text(encoding="utf-8-sig")
    page16 = (ROOT / "pages" / "16_官方因子快取中心.py").read_text(encoding="utf-8-sig")
    assert "官方來源可信覆蓋率%" in page7
    assert "官方日期T-1內覆蓋率%" in page7
    assert "V187 官方因子治理" in page7
    assert "V187 校正來源可信度並永久保存" in page16
    assert "每日因子來源可信度" in page16


def main():
    test_legacy_60_is_rebuilt_from_actual_official_sources()
    test_838_effective_836_t1_no_longer_false_blocks()
    test_true_previous_cache_only_stays_blocked()
    test_fallback_fill_does_not_downgrade_existing_official_trust()
    test_v187_ui_wiring()
    print("PASS V187 source trust governance｜legacy 60 false-block repaired, verified T-1 usable, true old-cache blocked, fallback no downgrade")


if __name__ == "__main__":
    main()
