# -*- coding: utf-8 -*-
from pathlib import Path
import sys
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from godpick_human_master_engine import VERSION, apply_human_master_engine, build_h51_final_decision_table


def strong_row(code="3711"):
    return {
        "股票代號": code, "股票名稱": "H56測試股", "類別": "封測",
        "H50族群生命週期": "A1｜新鮮主流", "H50波段機會階段": "N-EARLY｜主升起漲",
        "H47主流領先狀態": "L-LEADER｜領漲", "H50族群可買主流分": 86,
        "H45族群主流分": 84, "H50族群新鮮度分": 88, "H50族群回檔再攻分": 82,
        "H47個股相對強度分": 86, "H47族群內領先百分位%": 92, "H47起漲優先分": 88,
        "H45趨勢延續分": 84, "今日漲幅%": 3.0, "近5日漲幅%": 5.0, "近20日漲幅%": 12.0,
        "距20日高點%": 2.0, "當日量比": 1.5, "當日收盤位置%": 82, "上影線比例%": 10,
        "成交額百萬": 1500, "20日均成交額百萬": 900, "主流資金分": 85,
        "EPS代理分數": 78, "營收動能代理分數": 80, "獲利代理分數": 78,
        "族群攻擊強度": 84, "族群廣度分": 82, "族群成交額分": 82, "族群資金流分數": 83,
        "同族群強勢比例": 0.72, "同族群平均量能分": 82, "H45族群5日上漲比例%": 76,
        "今日訊號新鮮分": 88, "H50重複推薦扣分": 0, "追價風險分": 28,
        "路徑風險報酬比": 1.8, "SuperAI Trade分": 76, "Risk風控安全分": 74,
        "Entry進場買點分": 72, "實戰停損距離%": 5.0, "最新價": 100,
        "隔日可執行優先分": 80, "守價回測距離%": 1.0, "隔日耗竭風險分": 20,
        "隔夜風控分數": 50, "隔夜風險等級": "中性", "隔日大盤分數": 50,
        "隔日下跌機率%": 50, "隔日大盤預測加減分": 0,
        "NASDAQ漲跌%": 0, "S&P500漲跌%": 0, "費半漲跌%": 0, "台指夜盤漲跌": 0,
        "K線日期": "2026-09-01", "隔日大盤預測日期": "2026-09-02",
        "隔夜催化需求": "盤前需重掃：隔夜美股/費半可能改變隔日方向。",
    }


def main():
    assert VERSION == "v191_h59_formal_recall_learning_truth_20260903"

    # 1) Explicit V188/Formal rejection is a hard authority ceiling.
    capped = strong_row("1001")
    capped.update({
        "是否正式推薦": False,
        "V188正式推薦資格": "否｜V188為降級治理，不越權升格",
        "V188交易許可": "WAIT-PULLBACK｜禁止突破追價，只准回測守價",
        "操作許可": "WAIT-PULLBACK｜禁止突破追價，只准回測守價",
        "最終操作結論": "A-｜準主推薦：盤中確認後只允許小量試單",
        "正式推薦分區": "盤中雷達追蹤",
        "正式推薦動作": "V188未通過交易品質治理；保留Alpha研究價值，但不得建立正式新倉。",
    })
    rc = apply_human_master_engine(pd.DataFrame([capped])).iloc[0]
    assert not str(rc["H51交易許可"]).startswith("BUY-READY"), rc["H51交易許可"]
    assert str(rc["H51交易許可"]).startswith("SETUP-PREP")
    assert rc["H56上游權威層級"] == "A-MINUS"
    assert str(rc["H56最終參考層級"]).startswith("P0｜AUTHORITY-CAPPED")

    # 2) Without upstream authority fields, standalone engine compatibility remains.
    unknown = strong_row("1002")
    ru = apply_human_master_engine(pd.DataFrame([unknown])).iloc[0]
    assert ru["H56上游權威層級"] == "UNKNOWN"
    assert str(ru["H51交易許可"]).startswith("BUY-READY"), ru["H51交易許可"]
    # Neutral 50/50/50 and no fresh timestamp must never become "overnight confirmed".
    assert str(ru["H56隔夜證據狀態"]).startswith("PENDING")
    assert str(ru["H56最終參考層級"]).startswith("A0｜PREOPEN-PENDING")
    assert ru["H56T1確認分"] <= 68

    # 3) Formal + fresh next-day overnight snapshot can confirm A1.
    verified = strong_row("1003")
    verified.update({
        "是否正式推薦": True,
        "正式推薦分區": "正式主推薦",
        "V188正式推薦資格": "是｜正式交易品質通過",
        "V188交易許可": "ALLOW｜正式可執行",
        "操作許可": "ALLOW｜正式可執行",
        "隔夜更新時間": "2026-09-02 08:10:00",
        "隔夜資料品質": "完整｜可用",
        "隔夜風控分數": 74, "隔日大盤分數": 68, "隔日下跌機率%": 34,
        "NASDAQ漲跌%": 0.7, "S&P500漲跌%": 0.5, "費半漲跌%": 1.1, "台指夜盤漲跌": 0.6,
    })
    rv = apply_human_master_engine(pd.DataFrame([verified])).iloc[0]
    assert rv["H56上游權威層級"] == "FORMAL"
    assert str(rv["H56隔夜證據狀態"]).startswith("VERIFIED｜")
    assert str(rv["H56最終參考層級"]).startswith("A1｜PREOPEN-CONFIRMED"), rv["H56最終參考層級"]

    # 4) Fresh but adverse overnight evidence suspends a formal candidate.
    risk = dict(verified)
    risk["股票代號"] = "1004"
    risk.update({
        "隔夜風控分數": 30, "隔日大盤分數": 34, "隔日下跌機率%": 68,
        "隔日大盤預測加減分": -8, "NASDAQ漲跌%": -2.2, "S&P500漲跌%": -1.7,
        "費半漲跌%": -3.2, "台指夜盤漲跌": -2.0,
    })
    rr = apply_human_master_engine(pd.DataFrame([risk])).iloc[0]
    assert str(rr["H56隔夜證據狀態"]).startswith("VERIFIED-RISK")
    assert str(rr["H56最終參考層級"]).startswith("X2｜OVERNIGHT-RISK-HOLD")
    final = build_h51_final_decision_table(pd.DataFrame([rr]), max_rows=6)
    assert "股票代號" not in final.columns, final.to_dict("records")

    page = (ROOT / "pages" / "7_股神推薦.py").read_text(encoding="utf-8")
    assert 'H51_HUMAN_MASTER_EXPECTED_VERSION = "v191_h59_formal_recall_learning_truth_20260903"' in page
    assert 'PAGE07_SPEED_FIX_VERSION = "page07_v191_h59_formal_recall_learning_truth_20260903"' in page
    assert 'x["隔夜更新時間"] = overnight_info.get("updated_at")' in page
    assert "超級AI唯一決策｜H59 Formal權威召回＋單一真相" in page

    print("PASS v191_h56_authority_preopen_truth_smoke")


if __name__ == "__main__":
    main()
