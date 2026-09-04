# -*- coding: utf-8 -*-
from pathlib import Path
import sys
import json
import tempfile
import time
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import godpick_tdcc_holder_service as tdcc
from godpick_tdcc_holder_service import parse_tdcc_csv_bytes
from godpick_h60_compound_engine import VERSION as H60_ENGINE_VERSION, apply_h60_compound_engine
from godpick_human_master_engine import VERSION, apply_human_master_engine, build_h60_single_decision_truth_table
from godpick_t1_trade_truth import TRUTH_VERSION, build_h57_h60_learning_summary

EXPECTED = "v191_h60_mainrise_holder_snowball_truth_20260904"


def base_row(code="9999"):
    return {
        "股票代號": code, "股票名稱": "測試股", "類別": "測試族群",
        "是否正式推薦": "否｜準主推薦", "正式推薦分區": "A-｜準主推薦小量試單",
        "V188正式推薦資格": "是｜Formal/A-權威＋V188均通過", "V188交易許可": "WAIT-PULLBACK",
        "操作許可": "WAIT-PULLBACK｜只准回測守價", "最終操作結論": "A-｜條件推薦", "推薦升級判定路徑": "A-｜盤中確認",
        "H50族群生命週期": "A｜新主流", "H50波段機會階段": "N-EARLY",
        "H47主流領先狀態": "L-EARLY", "H50族群可買主流分": 82, "H45族群主流分": 82,
        "H50族群新鮮度分": 84, "H50族群回檔再攻分": 74, "H47個股相對強度分": 83,
        "H47族群內領先百分位%": 88, "H47起漲優先分": 84, "H45趨勢延續分": 82,
        "今日漲幅%": 2.0, "近5日漲幅%": 8.0, "近20日漲幅%": 18.0, "近60日漲幅%": 30.0,
        "距20日高點%": 1.5, "當日量比": 1.5, "當日收盤位置%": 82, "上影線比例%": 8,
        "成交額百萬": 1500, "20日均成交額百萬": 900, "主流資金分": 82,
        "EPS代理分數": 78, "營收動能代理分數": 82, "獲利代理分數": 76,
        "族群攻擊強度": 82, "族群廣度分": 80, "族群成交額分": 78, "族群資金流分數": 82,
        "同族群強勢比例": 0.70, "同族群平均量能分": 76, "H45族群5日上漲比例%": 70,
        "今日訊號新鮮分": 82, "H50重複推薦扣分": 0, "追價風險分": 35,
        "路徑風險報酬比": 1.9, "SuperAI Trade分": 80, "Risk風控安全分": 78,
        "Entry進場買點分": 76, "實戰停損距離%": 4.5, "最新價": 100,
        "實戰觸發價": 103, "觸發後守價": 101, "實戰停損參考": 96,
        "隔日可執行優先分": 80, "守價回測距離%": 1.0, "隔日耗竭風險分": 30,
        "隔夜風控分數": 50, "隔夜風險等級": "中性", "隔日大盤分數": 50,
        "隔日下跌機率%": 50, "隔日大盤預測加減分": 0,
        "NASDAQ漲跌%": 0, "S&P500漲跌%": 0, "費半漲跌%": 0, "台指夜盤漲跌": 0,
        "K線日期": "2026-09-03", "隔日大盤預測日期": "2026-09-04", "隔夜更新時間": "", "隔夜資料品質": "PENDING",
        "K線資料新鮮度": "最新交易日",
        "主流主升優先分": 86, "基本面成長分數": 82, "營收成長分數": 84, "EPS成長分數": 78,
        "技術趨勢分數": 82, "籌碼續航分": 76, "大戶鎖碼代理分數": 74, "大戶承接分": 76,
        "H51發動潛力分": 86, "H51個股領漲品質分": 84, "H51基本面資金分": 80,
        "H53族群共振分": 82, "H55主線延續路徑分": 84, "H57主流形成前兆分": 80, "H54耗竭風險分": 42,
    }


def main():
    assert VERSION == EXPECTED
    assert H60_ENGINE_VERSION == EXPECTED
    assert TRUTH_VERSION == "godpick_t1_trade_truth_v191_h60_mainrise_holder_snowball_truth_20260904"

    # A) TDCC official class-15 parser.
    fixture = ("資料日期,證券代號,持股分級,人數,股數,占集保庫存數比例%\n"
               "20260904,9999,15,88,12345678,56.2\n"
               "20260904,9999,14,120,3456789,12.3\n").encode("utf-8")
    parsed = parse_tdcc_csv_bytes(fixture)
    assert parsed["data_date"] == "20260904"
    assert abs(parsed["rows"]["9999"]["占集保庫存數比例%"] - 56.2) < 1e-9

    # A2) Cached prior snapshot produces a real week-over-week delta.
    with tempfile.TemporaryDirectory() as td:
        tdpath = Path(td)
        previous = {"version": tdcc.VERSION, "data_date": "20260828", "rows": {"9999": {"占集保庫存數比例%": 54.6}}}
        (tdpath / "previous.json").write_text(json.dumps(previous, ensure_ascii=False), encoding="utf-8")
        tdcc.PREVIOUS_FILE = tdpath / "previous.json"
        tdcc.LATEST_FILE = tdpath / "latest.json"
        tdcc.FETCH_META_FILE = tdpath / "fetch_meta.json"
        tdcc._MEM_CACHE = parsed
        tdcc._MEM_CACHE_AT = time.time()
        enriched = tdcc.enrich_tdcc_holder_truth(pd.DataFrame([{"股票代號": "9999"}]), allow_network=False)
        assert enriched.iloc[0]["TDCC大戶資料狀態"] == "ACTUAL"
        assert abs(float(enriched.iloc[0]["TDCC千張大戶週變化pp"]) - 1.6) < 1e-9

    # B) Proxy holder lock is explicitly labeled PROXY, never ACTUAL.
    proxy = apply_h60_compound_engine(pd.DataFrame([base_row("9998")]))
    assert str(proxy.iloc[0]["H60鎖碼來源"]).startswith("PROXY")
    assert pd.isna(proxy.iloc[0]["H60千張大戶持股比%"])

    # C) Actual TDCC ratio/delta drives official lock truth and T3-A when all three align.
    r = base_row("9999")
    r.update({"TDCC大戶資料狀態": "ACTUAL", "TDCC千張大戶持股比%": 62.0,
              "TDCC千張大戶週變化pp": 1.6, "TDCC大戶資料日期": "20260904"})
    actual = apply_h60_compound_engine(pd.DataFrame([r]))
    row = actual.iloc[0]
    assert str(row["H60鎖碼來源"]).startswith("ACTUAL")
    assert row["H60大戶鎖碼真相分"] > 75
    assert str(row["H60主升階段"]).startswith("MR1")
    assert str(row["H60雪球股層級"]).startswith("SB1")
    assert str(row["H60三因子層級"]).startswith("T3-A")

    # D) A- authority bug: generic qualifier containing "Formal/A-" must NOT become FORMAL.
    governed = apply_human_master_engine(pd.DataFrame([r]))
    assert governed.iloc[0]["H56上游權威層級"] == "A-MINUS", governed[["H56上游權威層級", "H56最終參考層級"]].to_dict("records")
    assert str(governed.iloc[0]["H56最終參考層級"]).startswith("P0｜AUTHORITY-CAPPED")

    # E) H60 single truth shows H60 research context but cannot upgrade A- to buy.
    truth = build_h60_single_decision_truth_table(governed, max_rows=10)
    assert not truth.empty
    assert str(truth.iloc[0]["H60是否可買"]).startswith("否"), truth.to_dict("records")
    assert str(truth.iloc[0]["H60三因子層級"]).startswith("T3")

    # F) T+1 H60 cohort metrics are Selection-only.
    learning = build_h57_h60_learning_summary([
        {"T1成熟": True, "H60主升階段": "MR1｜主升起漲", "H60雪球股層級": "SB1｜雪球複利核心", "H60三因子層級": "T3-A｜主升×真實鎖碼×雪球核心", "隔日候選漲跌%": 4.0, "Selection Alpha%": 2.5},
        {"T1成熟": True, "H60主升階段": "MR1｜主升起漲", "H60雪球股層級": "SB1｜雪球複利核心", "H60三因子層級": "T3-P｜主升×代理鎖碼×雪球核心", "隔日候選漲跌%": -1.0, "Selection Alpha%": -1.5},
    ])
    assert learning["H60_MR1成熟樣本"] == 2
    assert learning["H60_SB1成熟樣本"] == 2
    assert learning["H60_T3成熟樣本"] == 2
    assert learning["H60_T3正報酬率%"] == 50.0

    page = (ROOT / "pages" / "7_股神推薦.py").read_text(encoding="utf-8")
    assert 'H51_HUMAN_MASTER_EXPECTED_VERSION = "v191_h60_mainrise_holder_snowball_truth_20260904"' in page
    assert 'PAGE07_SPEED_FIX_VERSION = "page07_v191_h60_mainrise_holder_snowball_truth_20260904"' in page
    assert "build_h60_single_decision_truth_table" in page
    assert "enrich_tdcc_holder_truth" in page
    assert "H60千張大戶持股比%" in page
    print("PASS v191_h60_mainrise_holder_snowball_smoke")


if __name__ == "__main__":
    main()
