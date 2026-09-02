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
    build_h58_single_decision_truth_table,
)


def base_row(code: str, name: str) -> dict:
    return {
        "股票代號": code, "股票名稱": name, "類別": "AI伺服器",
        "H50族群生命週期": "A1｜新鮮主流", "H50波段機會階段": "N-EARLY｜主升起漲",
        "H47主流領先狀態": "L-LEADER｜領漲", "H50族群可買主流分": 86,
        "H45族群主流分": 84, "H50族群新鮮度分": 88, "H50族群回檔再攻分": 82,
        "H47個股相對強度分": 86, "H47族群內領先百分位%": 92, "H47起漲優先分": 88,
        "H45趨勢延續分": 84, "今日漲幅%": 2.0, "近5日漲幅%": 5.0, "近20日漲幅%": 12.0,
        "距20日高點%": 2.0, "當日量比": 1.5, "當日收盤位置%": 82, "上影線比例%": 10,
        "成交額百萬": 1500, "20日均成交額百萬": 900, "主流資金分": 85,
        "EPS代理分數": 78, "營收動能代理分數": 80, "獲利代理分數": 78,
        "族群攻擊強度": 84, "族群廣度分": 82, "族群成交額分": 82, "族群資金流分數": 83,
        "同族群強勢比例": 0.72, "同族群平均量能分": 82, "H45族群5日上漲比例%": 76,
        "今日訊號新鮮分": 88, "H50重複推薦扣分": 0, "追價風險分": 28,
        "路徑風險報酬比": 1.8, "SuperAI Trade分": 76, "Risk風控安全分": 74,
        "Entry進場買點分": 72, "實戰停損距離%": 5.0, "最新價": 100,
        "實戰觸發價": 103, "觸發後守價": 101.5, "實戰停損參考": 96,
        "隔日可執行優先分": 80, "守價回測距離%": 1.0, "隔日耗竭風險分": 20,
        "隔夜風控分數": 50, "隔夜風險等級": "中性", "隔日大盤分數": 50,
        "隔日下跌機率%": 50, "隔日大盤預測加減分": 0,
        "NASDAQ漲跌%": 0, "S&P500漲跌%": 0, "費半漲跌%": 0, "台指夜盤漲跌": 0,
        "K線日期": "2026-09-01", "隔日大盤預測日期": "2026-09-02",
        "K線資料新鮮度": "最新交易日",
        "成交額3日加速度%": 55, "成交額5日加速度%": 42,
        "成交量3日加速度%": 44, "成交量5日加速度%": 31,
        "波動壓縮比": 0.62, "前5日波動壓縮比": 0.55, "當日區間擴張倍數": 1.2,
        "3日動能加速度百分點": 2.8, "3日平均收盤位置%": 78,
    }


def main():
    assert VERSION == "v191_h58_single_decision_truth_console_20260902"

    # A1: Formal + fresh verified overnight evidence.
    a1 = base_row("1001", "A1測試")
    a1.update({
        "是否正式推薦": True, "正式推薦分區": "正式主推薦",
        "V188正式推薦資格": "是｜正式交易品質通過", "V188交易許可": "ALLOW｜正式可執行",
        "操作許可": "ALLOW｜正式可執行", "隔夜更新時間": "2026-09-02 08:10:00",
        "隔夜資料品質": "完整｜可用", "隔夜風控分數": 74, "隔日大盤分數": 68,
        "隔日下跌機率%": 34, "NASDAQ漲跌%": 0.7, "S&P500漲跌%": 0.5,
        "費半漲跌%": 1.1, "台指夜盤漲跌": 0.6,
    })

    # A0: Formal/BUY candidate, but overnight has not happened yet.
    a0 = base_row("1002", "A0測試")
    a0.update({
        "是否正式推薦": True, "正式推薦分區": "正式主推薦",
        "V188正式推薦資格": "是｜正式交易品質通過", "V188交易許可": "ALLOW｜正式可執行",
        "操作許可": "ALLOW｜正式可執行",
    })

    # P0: Model likes it, but upstream authority caps it to A-/radar.
    p0 = base_row("1003", "P0測試")
    p0.update({
        "是否正式推薦": False, "正式推薦分區": "盤中雷達追蹤",
        "V188正式推薦資格": "否｜V188為降級治理，不越權升格",
        "V188交易許可": "WAIT-PULLBACK｜禁止突破追價，只准回測守價",
        "操作許可": "WAIT-PULLBACK｜禁止突破追價，只准回測守價",
        "最終操作結論": "A-｜準主推薦：盤中確認後只允許小量試單",
    })

    governed = apply_human_master_engine(pd.DataFrame([a1, a0, p0]))

    # E1-only research row: explicitly mark as research; it must appear below A/P tiers and never become buyable.
    e1 = governed.iloc[[2]].copy()
    e1["股票代號"] = "1004"
    e1["股票名稱"] = "E1測試"
    e1["類別"] = "光通訊"
    e1["H51交易許可"] = "LEADER-WATCH｜主線成立但交易品質不足"
    e1["H56最終參考層級"] = "W2｜RESEARCH"
    e1["H56上游權威層級"] = "RADAR"
    e1["H57精選雷達層級"] = "E1｜ELITE-PRE-IGNITION｜全市場頂級發動前兆"
    e1["H57前兆階段"] = "PI3｜PRE-IGNITION｜高品質1-3日發動前兆"
    e1["H57飆股發動前兆分"] = 92.0
    e1["H57全市場前兆百分位%"] = 99.2
    e1["H51版本"] = VERSION

    combined = pd.concat([governed, e1], ignore_index=True)
    out = build_h58_single_decision_truth_table(combined, max_rows=10)
    assert not out.empty, out
    tiers = out["H58唯一決策"].astype(str).tolist()
    assert tiers[0].startswith("A1｜可執行"), tiers
    assert any(x.startswith("A0") for x in tiers), tiers
    assert any(x.startswith("P0") for x in tiers), tiers
    assert any(x.startswith("E1") for x in tiers), tiers

    e1_row = out.loc[out["股票代號"].astype(str).eq("1004")].iloc[0]
    assert e1_row["H58是否可買"] == "否｜研究"
    assert "Formal/V188/H56" in e1_row["現在該做什麼"]
    a1_row = out.loc[out["股票代號"].astype(str).eq("1001")].iloc[0]
    assert a1_row["H58是否可買"].startswith("是｜")
    assert float(a1_row["實戰觸發價"]) > 0

    page = (ROOT / "pages" / "7_股神推薦.py").read_text(encoding="utf-8")
    assert 'H51_HUMAN_MASTER_EXPECTED_VERSION = "v191_h58_single_decision_truth_console_20260902"' in page
    assert 'PAGE07_SPEED_FIX_VERSION = "page07_v191_h58_single_decision_truth_console_20260902"' in page
    assert "超級AI唯一決策｜H58 單一真相控制台" in page
    assert "build_h58_single_decision_truth_table(h51_source, max_rows=10)" in page
    assert "_h58_focus = build_h58_single_decision_truth_table(_h51_source_ui, max_rows=10)" in page
    assert "本輪舊版作戰名單（非H58決策來源）" in page
    assert "舊版飆股補抓｜僅研究" in page
    assert "權威底層稽核｜Formal／A-／核心雷達（非主要決策）" in page

    print("PASS v191_h58_single_decision_truth_smoke")


if __name__ == "__main__":
    main()
