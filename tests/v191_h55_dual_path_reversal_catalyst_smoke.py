# -*- coding: utf-8 -*-
from pathlib import Path
import sys
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from godpick_human_master_engine import (
    VERSION,
    apply_human_master_engine,
    build_h51_mainstream_leader_table,
    build_h51_sector_table,
)


def base_row(code: str, name: str, sector: str) -> dict:
    return {
        "股票代號": code, "股票名稱": name, "類別": sector,
        "H50族群生命週期": "B2｜一般輪動", "H50波段機會階段": "N-RADAR｜觀察",
        "H47主流領先狀態": "L-RADAR｜觀察", "H50族群可買主流分": 58,
        "H45族群主流分": 58, "H50族群新鮮度分": 62, "H50族群回檔再攻分": 60,
        "H47個股相對強度分": 66, "H47族群內領先百分位%": 62, "H47起漲優先分": 60,
        "H45趨勢延續分": 58, "今日漲幅%": 0.8, "近5日漲幅%": 1.2, "近20日漲幅%": 4.0,
        "距20日高點%": 7.0, "當日量比": 1.20, "當日收盤位置%": 68, "上影線比例%": 18,
        "成交額百萬": 950, "20日均成交額百萬": 700, "主流資金分": 58,
        "EPS代理分數": 66, "營收動能代理分數": 66, "獲利代理分數": 66,
        "族群攻擊強度": 58, "族群廣度分": 56, "族群成交額分": 60, "族群資金流分數": 58,
        "同族群強勢比例": 0.46, "同族群平均量能分": 60, "H45族群5日上漲比例%": 50,
        "今日訊號新鮮分": 72, "H50重複推薦扣分": 0, "追價風險分": 36,
        "路徑風險報酬比": 1.35, "SuperAI Trade分": 58, "Risk風控安全分": 67,
        "Entry進場買點分": 59, "實戰停損距離%": 5.5, "最新價": 100,
        "隔日可執行優先分": 64, "守價回測距離%": 1.5, "隔日耗竭風險分": 32,
        "隔夜風控分數": 62, "隔夜風險等級": "中性", "隔日大盤分數": 56,
        "隔日下跌機率%": 45, "隔日大盤預測加減分": 0,
        "K線日期": "2026-08-31", "隔日大盤預測日期": "2026-09-01",
    }


def main():
    assert VERSION == "v191_h56_authority_preopen_two_stage_truth_20260902"

    # A fresh-event/reversal candidate: old H50 mainstream is only ordinary,
    # but upstream radar engines already detected reversal/precursor/theme/leader replay.
    fresh = base_row("2454", "新點火股", "IC設計")
    fresh.update({
        "紅燈逆勢反轉分": 90,
        "紅燈逆勢反轉判定": "強｜逆勢反轉核心",
        "強勢前兆分": 88,
        "強勢前兆判定": "強勢前兆",
        "起漲前兆分數": 86,
        "隔日爆發分": 91,
        "爆發雷達分": 89,
        "飆股攻擊分": 87,
        "飆股雷達角色": "S+｜爆發核心",
        "局部題材火種分": 90,
        "漲停族群相似度": 86,
        "主流領漲回補分": 92,
        "市場領漲相似分": 88,
        "漲停回放分": 90,
        "強勢股漏選風險分": 84,
        "領漲回補角色": "L+｜領漲回補",
        "H47個股相對強度分": 82,
        "當日收盤位置%": 82,
        "今日訊號新鮮分": 90,
        "族群攻擊強度": 72,
        "族群廣度分": 66,
    })

    # A conventional prior-mainstream candidate with decent H54 continuation but no catalyst evidence.
    old = base_row("9001", "昨日主流股", "昨日熱門")
    old.update({
        "H50族群生命週期": "A1｜新鮮主流", "H50波段機會階段": "N-EARLY｜主升起漲",
        "H47主流領先狀態": "L-LEADER｜領漲", "H50族群可買主流分": 82,
        "H45族群主流分": 80, "H50族群新鮮度分": 82, "H50族群回檔再攻分": 78,
        "H47個股相對強度分": 80, "H47族群內領先百分位%": 86, "H47起漲優先分": 82,
        "H45趨勢延續分": 78, "族群攻擊強度": 76, "族群廣度分": 74,
        "族群成交額分": 76, "族群資金流分數": 74,
        "同族群強勢比例": 0.68, "H45族群5日上漲比例%": 70,
        "今日訊號新鮮分": 78, "隔日可執行優先分": 72,
        "隔日耗竭風險分": 38,
    })

    scored = apply_human_master_engine(pd.DataFrame([fresh, old]))
    assert {"H55雙路徑隔日分", "H55反轉點火路徑分", "H55催化代理分", "H55參考層級"}.issubset(scored.columns)

    r_fresh = scored.loc[scored["股票代號"].eq("2454")].iloc[0]
    r_old = scored.loc[scored["股票代號"].eq("9001")].iloc[0]
    assert r_fresh["H55反轉點火路徑分"] >= 72, r_fresh[["H55反轉點火路徑分", "H55催化代理分"]].to_dict()
    assert r_fresh["H55催化代理分"] >= 75
    assert str(r_fresh["H55機會型態"]).startswith(("REVERSAL-CATALYST", "DUAL"))
    assert str(r_fresh["H55參考層級"]).startswith(("R2", "P1", "A1", "A2")), r_fresh["H55參考層級"]
    # Research elevation must not silently rewrite the H51 trade authority.
    if str(r_fresh["H55參考層級"]).startswith("R2"):
        assert not str(r_fresh["H51交易許可"]).startswith("BUY-READY")

    leaders = build_h51_mainstream_leader_table(scored, max_rows=10)
    assert "2454" in leaders["股票代號"].astype(str).tolist(), leaders.to_dict("records")
    assert "H55反轉點火路徑分" in leaders.columns

    sectors = build_h51_sector_table(scored, max_rows=10)
    assert "H55族群機會分" in sectors.columns
    assert "H55族群反轉點火分" in sectors.columns

    page = (ROOT / "pages" / "7_股神推薦.py").read_text(encoding="utf-8")
    assert 'H51_HUMAN_MASTER_EXPECTED_VERSION = "v191_h56_authority_preopen_two_stage_truth_20260902"' in page
    assert 'PAGE07_SPEED_FIX_VERSION = "page07_v191_h56_authority_preopen_two_stage_truth_20260902"' in page
    assert "H56權威＋盤前二階段真相" in page

    print("PASS v191_h55_dual_path_reversal_catalyst_smoke")


if __name__ == "__main__":
    main()
