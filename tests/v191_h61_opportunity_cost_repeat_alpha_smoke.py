# -*- coding: utf-8 -*-
from pathlib import Path
import sys
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from godpick_h61_opportunity_cost_engine import VERSION, apply_h61_opportunity_cost_engine
from godpick_human_master_engine import build_h61_single_decision_truth_table

EXPECTED = "v191_h61_opportunity_cost_repeat_alpha_truth_20260906"
H60 = "v191_h60_mainrise_holder_snowball_truth_20260904"


def row(code, name, exp10, rr, h57, pct, h49, wave, near5=0, auth="A-MINUS", h56="P0｜AUTHORITY-CAPPED｜上游未正式授權，僅可等待/雷達"):
    return {
        "股票代號": code, "股票名稱": name, "類別": "測試族群",
        "H51版本": H60, "H60版本": H60,
        "H51交易許可": "SETUP-PREP｜值得盯，等Pivot/量價/路徑RR補齊",
        "H51市場地位": "HM-EARLY｜新主流起漲候選", "H51專業參考分": 78,
        "H56上游權威層級": auth, "H56最終參考層級": h56, "H56T1確認分": 62,
        "H57前兆階段": "PI1｜EARLY-SIGNAL", "H57精選雷達層級": "",
        "H57飆股發動前兆分": h57, "H57全市場前兆百分位%": pct, "H57主流形成前兆分": 75,
        "H60三因子共振分": 75, "H60主升段分": 78, "H60雪球複利分": 74,
        "H60三因子層級": "T2｜雙因子以上共振", "H60主升階段": "MR1｜主升起漲",
        "H60大戶鎖碼層級": "LK2｜鎖碼偏強", "H60雪球股層級": "SB2｜雪球成長候選",
        "H32_10日預估報酬%": exp10, "H51路徑RR": rr, "H49上漲潛力分": h49, "波段潛力分數": wave,
        "近5次入榜次數": near5, "連續入榜次數": max(0, near5 - 2),
        "最新價": 100, "實戰觸發價": 103, "觸發後守價": 101, "實戰停損參考": 96,
        "H56決策理由": "test",
    }


def main():
    assert VERSION == EXPECTED
    frame = pd.DataFrame([
        row("2376", "熟面孔", 2.2, 1.55, 80, 99, 76, 78, near5=4),
        row("9999", "新機會", 7.0, 2.5, 88, 99.5, 86, 84, near5=0),
        row("8888", "Formal保留", 0.5, 0.7, 55, 55, 55, 55, near5=4, auth="FORMAL", h56="A0｜PREOPEN-PENDING｜Formal候選成立，但隔夜尚未確認"),
    ])
    truth = [
        {"股票代號": "2376", "推薦日期": "2026-09-03", "T1成熟": True, "Selection Alpha%": -1.8},
        {"股票代號": "2376", "推薦日期": "2026-09-01", "T1成熟": True, "Selection Alpha%": -1.2},
        {"股票代號": "2376", "推薦日期": "2026-08-31", "T1成熟": True, "Selection Alpha%": -0.9},
        {"股票代號": "8888", "推薦日期": "2026-09-03", "T1成熟": True, "Selection Alpha%": -2.0},
    ]
    scored = apply_h61_opportunity_cost_engine(frame, truth_rows=truth)
    fav = scored.loc[scored["股票代號"].eq("2376")].iloc[0]
    fresh = scored.loc[scored["股票代號"].eq("9999")].iloc[0]
    formal = scored.loc[scored["股票代號"].eq("8888")].iloc[0]
    assert str(fav["H61機會層級"]).startswith("R0"), fav.to_dict()
    assert str(fav["H61前排資格"]).startswith("否"), fav.to_dict()
    assert fresh["H61機會價值分"] > fav["H61機會價值分"], scored[["股票代號", "H61機會價值分"]].to_dict("records")
    assert str(fresh["H61前排資格"]).startswith("是"), fresh.to_dict()
    assert str(formal["H61前排資格"]).startswith("是｜Formal"), formal.to_dict()

    console = build_h61_single_decision_truth_table(scored, max_rows=5)
    codes = console.get("股票代號", pd.Series([], dtype=str)).astype(str).tolist()
    assert "8888" in codes, console.to_dict("records")  # Formal may never disappear.
    assert "9999" in codes, console.to_dict("records")  # Better fresh opportunity rises.
    assert "2376" not in codes, console.to_dict("records")  # Repeated low-alpha favorite leaves scarce front screen.

    page = (ROOT / "pages" / "7_股神推薦.py").read_text(encoding="utf-8")
    assert 'PAGE07_SPEED_FIX_VERSION = "page07_v191_h61_opportunity_cost_repeat_alpha_truth_20260906"' in page
    assert 'EXCEL_COLUMN_LAYOUT_VERSION = "V191-H61-OPPORTUNITY-COST-REPEAT-ALPHA-TRUTH-20260906"' in page
    assert "build_h61_single_decision_truth_table" in page
    assert "H61機會價值分" in page
    print("PASS v191_h61_opportunity_cost_repeat_alpha_smoke")


if __name__ == "__main__":
    main()
