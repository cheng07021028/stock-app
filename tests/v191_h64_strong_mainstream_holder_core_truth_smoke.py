# -*- coding: utf-8 -*-
from pathlib import Path
import pandas as pd

from godpick_h64_core_truth_engine import (
    VERSION, apply_h64_core_truth,
    build_h64_single_decision_truth_table, build_h64_core_research_table,
)
from godpick_h60_compound_engine import apply_h60_compound_engine
from godpick_h62_incremental_opportunity_engine import apply_h62_incremental_opportunity_engine

ROOT = Path(__file__).resolve().parents[1]
EXPECTED = "v191_h64_strong_mainstream_holder_core_truth_20260908"


def row(code="9001", *, auth="RADAR", strong=True, mainstream=True, lock="confirmed", formal=False):
    d = {
        "股票代號": code, "股票名稱": f"測試{code}", "類別": "測試主流",
        "H56上游權威層級": "FORMAL" if formal else auth,
        "H62原始權威": "FORMAL" if formal else auth,
        "H62有效權威": "EFFECTIVE-FORMAL" if formal else auth,
        "正式推薦分區": "正式下週主推薦" if formal else "盤中雷達追蹤",
        "是否正式推薦": "是" if formal else "否",
        "H61機會價值分": 78.0, "H61近期SelectionAlpha%": 1.2, "H61近期成熟樣本": 3,
        "H61近期正Alpha率%": 66.7, "H61上漲空間分": 80.0, "H61RR品質分": 82.0,
        "H61機會層級": "O1｜高潛力新機會", "H61前排資格": "是",
        "H32_10日預估報酬%": 6.5, "H62增量機會分": 78.0,
        "H54耗竭風險分": 35.0,
        "H57前兆階段": "PI3｜PRE-IGNITION", "H57資金加速度分": 82.0,
        "H57相對強度轉折分": 76.0, "H57飆股發動前兆分": 84.0,
        "H57主流形成前兆分": 78.0, "當日收盤位置%": 82.0, "今日漲幅%": 2.2,
        "H60主升段分": 82.0, "H60主升階段": "MR1｜主升起漲",
        "H51路徑RR": 2.2,
        "H60鎖碼來源": "ACTUAL｜TDCC千張大戶真實持股",
        "H60千張大戶持股比%": 68.0,
    }
    if strong:
        d.update({"H42強勢分": 78.0, "H42強勢狀態": "S-READY｜當前強勢", "H47個股相對強度分": 76.0, "H51個股領漲品質分": 74.0})
    else:
        d.update({"H42強勢分": 47.0, "H42強勢狀態": "S-NO｜非主流領先優先", "H47個股相對強度分": 44.0, "H51個股領漲品質分": 40.0, "H57前兆階段": "PI0｜NORMAL", "H57資金加速度分": 48.0, "H57相對強度轉折分": 46.0})
    if mainstream:
        d.update({"H51族群主線分": 78.0, "H53族群共振分": 76.0, "H53族群廣度分": 72.0, "H53族群攻擊分": 74.0, "H51市場地位": "HM-EARLY｜新主流起漲"})
    else:
        d.update({"H51族群主線分": 38.0, "H53族群共振分": 42.0, "H53族群廣度分": 39.0, "H53族群攻擊分": 41.0, "H57主流形成前兆分": 40.0, "H60主升段分": 45.0, "H60主升階段": "MR0｜非主升優先", "H51市場地位": "HM-NO｜非真人主線優先"})
    if lock == "confirmed":
        d["H60千張大戶週變化pp"] = 0.65
    elif lock == "stable":
        d["H60千張大戶週變化pp"] = 0.05
    elif lock == "distribution":
        d["H60千張大戶週變化pp"] = -0.8
    elif lock == "missing":
        d["H60千張大戶週變化pp"] = None
    elif lock == "proxy":
        d["H60鎖碼來源"] = "PROXY｜量價/法人/承接代理"
        d["H60大戶鎖碼真相分"] = 88.0
        d["H60千張大戶週變化pp"] = None
        d["H60千張大戶持股比%"] = None
    return d


def main():
    assert VERSION == EXPECTED

    # A) H62 must not promote a non-strong / non-mainstream row merely because its
    # incremental score is high and it ranks high cross-sectionally.
    raw = pd.DataFrame([
        row("9001", strong=False, mainstream=False, lock="confirmed"),
        row("9002", strong=True, mainstream=True, lock="confirmed"),
    ])
    h62 = apply_h62_incremental_opportunity_engine(raw)
    b62 = h62.set_index("股票代號")
    assert str(b62.at["9001", "H62機會層級"]).startswith("D0"), b62.loc["9001"].to_dict()
    assert str(b62.at["9001", "H62前排資格"]).startswith("否")

    h64 = apply_h64_core_truth(h62)
    by = h64.set_index("股票代號")
    assert str(by.at["9001", "H64真強勢狀態"]).startswith("S0")
    assert str(by.at["9001", "H64主流真相狀態"]).startswith("M0")
    assert str(by.at["9001", "H64研究層級"]).startswith("D0")
    assert str(by.at["9002", "H64研究層級"]).startswith("C2")

    # B) First TDCC snapshot cannot be called locking. Strong+mainstream waits C3.
    missing = apply_h64_core_truth(pd.DataFrame([row("9003", lock="missing")])).iloc[0]
    assert str(missing["H64鎖碼趨勢狀態"]).startswith("LU"), missing.to_dict()
    assert str(missing["H64研究層級"]).startswith("C3"), missing.to_dict()

    # H60 itself must also stop calling a one-snapshot high ratio LK1/LK2.
    h60_input = row("9004", lock="missing")
    h60_input.update({"TDCC大戶資料狀態": "ACTUAL", "TDCC千張大戶持股比%": 72.0, "TDCC千張大戶週變化pp": None, "TDCC大戶資料日期": "20260907"})
    h60 = apply_h60_compound_engine(pd.DataFrame([h60_input])).iloc[0]
    assert str(h60["H60大戶鎖碼層級"]).startswith("LKU"), h60.to_dict()

    # C) Healthy formal + current strength/mainstream + verified accumulation stays formal.
    formal_ok = apply_h64_core_truth(pd.DataFrame([row("9005", formal=True, lock="confirmed")])).iloc[0]
    assert formal_ok["H64有效權威"] == "EFFECTIVE-FORMAL", formal_ok.to_dict()
    assert str(formal_ok["H64研究層級"]).startswith("C1")

    # D) A raw/effective formal with distribution is quality-held, never silently executed.
    formal_bad = apply_h64_core_truth(pd.DataFrame([row("9006", formal=True, lock="distribution")])).iloc[0]
    assert formal_bad["H64有效權威"] == "FORMAL-QUALITY-HOLD", formal_bad.to_dict()
    assert str(formal_bad["H64正式作戰資格"]).startswith("否")

    # E) Proxy lock never passes as actual locking.
    proxy = apply_h64_core_truth(pd.DataFrame([row("9007", lock="proxy")])).iloc[0]
    assert str(proxy["H64鎖碼趨勢狀態"]).startswith("LP")
    assert not str(proxy["H64研究層級"]).startswith("C2")

    # F) Console may be empty and must not pad weak rows.
    none = build_h64_single_decision_truth_table(pd.DataFrame([row("9008", strong=False, mainstream=False, lock="missing")]), max_rows=10)
    assert str(none.iloc[0]["H64唯一決策"]).startswith("NONE")
    core = build_h64_core_research_table(h64, max_rows=20)
    assert "9001" not in set(core.get("股票代號", pd.Series([], dtype=str)).astype(str))

    page=(ROOT/"pages"/"7_股神推薦.py").read_text(encoding="utf-8")
    assert 'PAGE07_SPEED_FIX_VERSION = "page07_v191_h66_adaptive_alpha_t1_timing_truth_20260909"' in page
    assert 'EXCEL_COLUMN_LAYOUT_VERSION = "V191-H66-ADAPTIVE-ALPHA-T1-TIMING-TRUTH-20260909"' in page
    assert "build_h64_single_decision_truth_table" in page
    assert "H64 強勢×主流×大戶鎖碼真相" in page
    print("PASS H64 strong-mainstream-holder core truth smoke")


if __name__ == "__main__":
    main()
