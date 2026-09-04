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
    build_h59_single_decision_truth_table,
)
from godpick_t1_trade_truth import TRUTH_VERSION, build_h57_h59_learning_summary


def weak_formal(code: str, name: str, fresh: bool = False) -> dict:
    # Intentionally weak H51 research shape. Upstream Formal must still be recalled.
    row = {
        "股票代號": code, "股票名稱": name, "類別": "同一族群",
        "是否正式推薦": True, "正式推薦分區": "正式下週主推薦",
        "V188正式推薦資格": "是｜Formal/A-權威＋V188均通過",
        "V188交易許可": "ALLOW｜正式可執行", "操作許可": "ALLOW｜正式可執行",
        "最終操作結論": "正式主推薦｜依觸發價執行",
        "H50族群生命週期": "D｜非主流", "H50波段機會階段": "N-NO",
        "H47主流領先狀態": "L-NO", "H50族群可買主流分": 45,
        "H45族群主流分": 45, "H50族群新鮮度分": 45, "H50族群回檔再攻分": 40,
        "H47個股相對強度分": 48, "H47族群內領先百分位%": 45, "H47起漲優先分": 42,
        "H45趨勢延續分": 48, "今日漲幅%": 0.2, "近5日漲幅%": 1.0, "近20日漲幅%": 2.0,
        "距20日高點%": 8.0, "當日量比": 1.0, "當日收盤位置%": 55, "上影線比例%": 15,
        "成交額百萬": 900, "20日均成交額百萬": 850, "主流資金分": 48,
        "EPS代理分數": 60, "營收動能代理分數": 60, "獲利代理分數": 60,
        "族群攻擊強度": 45, "族群廣度分": 45, "族群成交額分": 55, "族群資金流分數": 48,
        "同族群強勢比例": 0.35, "同族群平均量能分": 50, "H45族群5日上漲比例%": 42,
        "今日訊號新鮮分": 55, "H50重複推薦扣分": 0, "追價風險分": 35,
        "路徑風險報酬比": 1.8, "SuperAI Trade分": 76, "Risk風控安全分": 75,
        "Entry進場買點分": 72, "實戰停損距離%": 5.0, "最新價": 100,
        "實戰觸發價": 103, "觸發後守價": 101, "實戰停損參考": 96,
        "隔日可執行優先分": 78, "守價回測距離%": 1.0, "隔日耗竭風險分": 25,
        "隔夜風控分數": 50, "隔夜風險等級": "中性", "隔日大盤分數": 50,
        "隔日下跌機率%": 50, "隔日大盤預測加減分": 0,
        "NASDAQ漲跌%": 0, "S&P500漲跌%": 0, "費半漲跌%": 0, "台指夜盤漲跌": 0,
        "K線日期": "2026-09-02", "隔日大盤預測日期": "2026-09-03",
        "K線資料新鮮度": "最新交易日",
    }
    if fresh:
        row.update({
            "隔夜更新時間": "2026-09-03 08:10:00", "隔夜資料品質": "完整｜可用",
            "隔夜風控分數": 68, "隔日大盤分數": 64, "隔日下跌機率%": 40,
            "NASDAQ漲跌%": 0.4, "S&P500漲跌%": 0.3, "費半漲跌%": 0.6, "台指夜盤漲跌": 0.2,
        })
    return row


def main():
    assert VERSION == "v191_h60_mainrise_holder_snowball_truth_20260904"
    assert TRUTH_VERSION == "godpick_t1_trade_truth_v191_h60_mainrise_holder_snowball_truth_20260904"

    # A) Formal must become A0 even if H51 says NO-PRIORITY / research-only.
    pending = apply_human_master_engine(pd.DataFrame([weak_formal("9001", "Formal待盤前", fresh=False)]))
    r = pending.iloc[0]
    assert r["H56上游權威層級"] == "FORMAL"
    assert not str(r["H51交易許可"]).startswith("BUY-READY"), r["H51交易許可"]
    assert str(r["H56最終參考層級"]).startswith("A0｜PREOPEN-PENDING"), r["H56最終參考層級"]
    t = build_h59_single_decision_truth_table(pending, max_rows=2)
    assert len(t) == 1 and str(t.iloc[0]["H59唯一決策"]).startswith("A0｜Formal"), t.to_dict("records")
    assert t.iloc[0]["H56上游權威"] == "FORMAL"

    # B) Fresh non-adverse overnight confirms Formal A1 regardless of H51 research score.
    confirmed = apply_human_master_engine(pd.DataFrame([weak_formal("9002", "Formal已盤前", fresh=True)]))
    r2 = confirmed.iloc[0]
    assert str(r2["H56最終參考層級"]).startswith("A1｜PREOPEN-CONFIRMED"), r2["H56最終參考層級"]
    t2 = build_h59_single_decision_truth_table(confirmed, max_rows=2)
    assert str(t2.iloc[0]["H59唯一決策"]).startswith("A1｜可執行")
    assert str(t2.iloc[0]["H59是否可買"]).startswith("是｜")

    # C) A-minus can never become A1.
    capped = weak_formal("9003", "Aminus", fresh=True)
    capped.update({
        "是否正式推薦": False, "正式推薦分區": "A-｜準主推薦小量試單",
        "V188正式推薦資格": "否｜V188為降級治理，不越權升格",
        "V188交易許可": "WAIT-PULLBACK｜只准回測守價",
        "操作許可": "WAIT-PULLBACK｜只准回測守價",
        "最終操作結論": "A-｜準主推薦：盤中確認後只允許小量試單",
    })
    rc = apply_human_master_engine(pd.DataFrame([capped])).iloc[0]
    assert rc["H56上游權威層級"] == "A-MINUS"
    assert str(rc["H56最終參考層級"]).startswith("P0｜AUTHORITY-CAPPED")

    # D) Formal recall is not subject to sector cap or max_rows.
    many = apply_human_master_engine(pd.DataFrame([weak_formal(str(9100+i), f"Formal{i}", fresh=False) for i in range(4)]))
    truth = build_h59_single_decision_truth_table(many, max_rows=2)
    assert len(truth) == 4, truth[["股票代號", "H59唯一決策"]].to_dict("records")
    assert truth["H56上游權威"].eq("FORMAL").all()

    # E) H57/H59 T+1 learning is Selection-only and computes cohort metrics.
    learning = build_h57_h59_learning_summary([
        {"T1成熟": True, "H57精選雷達層級": "E1｜ELITE-PRE-IGNITION", "H57前兆階段": "PI3｜PRE-IGNITION", "H59唯一決策": "E1｜頂級發動前兆", "隔日候選漲跌%": 3.0, "Selection Alpha%": 2.0},
        {"T1成熟": True, "H57精選雷達層級": "E1｜ELITE-PRE-IGNITION", "H57前兆階段": "PI3｜PRE-IGNITION", "H59唯一決策": "E1｜頂級發動前兆", "隔日候選漲跌%": -1.0, "Selection Alpha%": -0.5},
        {"T1成熟": True, "H57精選雷達層級": "", "H57前兆階段": "", "H59唯一決策": "A1｜可執行", "隔日候選漲跌%": 2.0, "Selection Alpha%": 1.2},
    ])
    assert learning["H57_E1成熟樣本"] == 2
    assert learning["H57_E1正報酬率%"] == 50.0
    assert abs(learning["H57_E1平均1日報酬%"] - 1.0) < 1e-9
    assert abs(learning["H57_E1平均SelectionAlpha%"] - 0.75) < 1e-9
    assert learning["H59_A1成熟樣本"] == 1

    page = (ROOT / "pages" / "7_股神推薦.py").read_text(encoding="utf-8")
    assert 'H51_HUMAN_MASTER_EXPECTED_VERSION = "v191_h60_mainrise_holder_snowball_truth_20260904"' in page
    assert 'PAGE07_SPEED_FIX_VERSION = "page07_v191_h60_mainrise_holder_snowball_truth_20260904"' in page
    assert "超級AI唯一決策｜H60 主升×大戶鎖碼真相×雪球複利＋Formal單一真相" in page
    assert "H60唯一決策Formal召回" in page
    assert "H57_E1平均SelectionAlpha%" in page
    assert "build_h60_single_decision_truth_table(h51_source, max_rows=10)" in page

    print("PASS v191_h59_formal_recall_learning_smoke")


if __name__ == "__main__":
    main()
