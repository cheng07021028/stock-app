# -*- coding: utf-8 -*-
from pathlib import Path
import sys
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from godpick_human_master_engine import VERSION, apply_human_master_engine, build_h51_final_decision_table


def row(code: str, name: str, sector: str = "被動元件") -> dict:
    return {
        "股票代號": code, "股票名稱": name, "類別": sector,
        "H50族群生命週期": "A1｜新鮮主流", "H50波段機會階段": "N-EARLY｜主升起漲",
        "H47主流領先狀態": "L-LEADER｜領漲", "H50族群可買主流分": 84,
        "H45族群主流分": 82, "H50族群新鮮度分": 86, "H50族群回檔再攻分": 80,
        "H47個股相對強度分": 84, "H47族群內領先百分位%": 90, "H47起漲優先分": 86,
        "H45趨勢延續分": 82, "今日漲幅%": 3.2, "近5日漲幅%": 7.0, "近20日漲幅%": 18.0,
        "距20日高點%": 2.0, "當日量比": 1.45, "當日收盤位置%": 80, "上影線比例%": 12,
        "成交額百萬": 900, "20日均成交額百萬": 650, "主流資金分": 83,
        "EPS代理分數": 72, "營收動能代理分數": 74, "獲利代理分數": 72,
        "族群攻擊強度": 84, "族群廣度分": 82, "族群成交額分": 80, "族群資金流分數": 82,
        "同族群強勢比例": 0.72, "同族群平均量能分": 82, "H45族群5日上漲比例%": 76,
        "今日訊號新鮮分": 86, "H50重複推薦扣分": 0, "追價風險分": 40,
        "路徑風險報酬比": 1.45, "SuperAI Trade分": 64, "Risk風控安全分": 68,
        "Entry進場買點分": 64, "實戰停損距離%": 6.0, "最新價": 100,
        "隔日可執行優先分": 78, "守價回測距離%": 1.2, "隔日耗竭風險分": 28,
        "隔夜風控分數": 72, "隔夜風險等級": "低", "隔日大盤分數": 66, "隔日下跌機率%": 38,
        "隔日大盤預測加減分": 3, "NASDAQ漲跌%": 0.8, "S&P500漲跌%": 0.5, "費半漲跌%": 1.2,
        "K線日期": "2026-08-28", "隔日大盤預測日期": "2026-08-31",
    }


def main():
    assert VERSION == "v191_h55_dual_path_reversal_catalyst_truth_20260901"

    good = row("1001", "健康延續")
    # Monday target after Friday is a real information window, so it should not be blindly P1.
    weekend = apply_human_master_engine(pd.DataFrame([good]))
    assert weekend.iloc[0]["H54資訊空窗風險"] >= 20
    assert not str(weekend.iloc[0]["H54決策層級"]).startswith("P1")
    weekend_final = build_h51_final_decision_table(weekend, max_rows=6)
    assert "股票代號" not in weekend_final.columns, weekend_final.to_dict("records")
    assert "H55判定" in str(weekend_final.iloc[0].get("狀態", ""))

    normal = row("1002", "一般隔日")
    normal["K線日期"] = "2026-08-27"
    normal["隔日大盤預測日期"] = "2026-08-28"
    normal_scored = apply_human_master_engine(pd.DataFrame([normal]))
    assert normal_scored.iloc[0]["H54隔日真相分"] >= 65
    assert normal_scored.iloc[0]["H54耗竭風險分"] == 28

    hot = row("1003", "高擁擠噴出")
    hot.update({
        "今日漲幅%": 5.5, "近5日漲幅%": 10, "追價風險分": 70,
        "隔日耗竭風險分": 84, "當沖比率%": 72,
        "隔夜風控分數": 30, "隔夜風險等級": "高風險｜偏空", "隔日大盤分數": 34,
        "隔日下跌機率%": 68, "隔日大盤預測加減分": -8, "NASDAQ漲跌%": -2.2,
        "S&P500漲跌%": -1.7, "費半漲跌%": -3.4, "K線日期": "2026-08-28", "推薦日期": "2026-08-30",
    })
    hot_scored = apply_human_master_engine(pd.DataFrame([hot]))
    r = hot_scored.iloc[0]
    assert r["H54耗竭風險分"] >= 85
    assert r["H54隔夜風險扣分"] >= 35
    assert r["H54資訊空窗風險"] >= 20
    assert str(r["H54決策層級"]).startswith("P3"), r["H54決策層級"]
    assert r["H54隔日真相分"] < normal_scored.iloc[0]["H54隔日真相分"]

    # H54 is a reference/ranking layer only: it must not silently upgrade H51 trade authority.
    assert str(r["H51交易許可"]).startswith(("SETUP-PREP", "LEADER-WATCH", "NO-CHASE"))
    final = build_h51_final_decision_table(pd.concat([normal_scored, hot_scored], ignore_index=True), max_rows=6)
    if "H54隔日真相分" in final.columns and len(final) >= 2:
        assert final.iloc[0]["H54隔日真相分"] >= final.iloc[1]["H54隔日真相分"]

    page_text = (ROOT / "pages" / "7_股神推薦.py").read_text(encoding="utf-8")
    assert 'H51_HUMAN_MASTER_EXPECTED_VERSION = "v191_h55_dual_path_reversal_catalyst_truth_20260901"' in page_text
    assert 'PAGE07_SPEED_FIX_VERSION = "page07_v191_h55_dual_path_reversal_catalyst_truth_20260901"' in page_text
    assert "H55雙路徑真相" in page_text

    print("PASS v191_h54_exhaustion_overnight_truth_smoke")


if __name__ == "__main__":
    main()
