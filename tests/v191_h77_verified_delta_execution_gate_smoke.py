# -*- coding: utf-8 -*-
import pandas as pd

from godpick_h77_verified_delta_execution_gate import (
    VERSION,
    apply_h77_verified_delta_execution_gate,
    build_h77_evidence_table,
    build_h77_governance_summary,
    build_h77_performance_health_summary,
    build_h77_verified_alpha_table,
    build_h77_wait_core_table,
)


assert VERSION == "v191_h77_verified_delta_chase_entry_performance_brake_20260918"


def base_row(code: str, name: str) -> dict:
    return {
        "股票代號": code, "股票名稱": name, "市場別": "上市", "族群名稱": "測試族群", "類別": "測試族群",
        "H75版本": "v191_h75_executive_decision_export_20260917",
        "H74決策總分": 78, "H74強勢加速度分": 82, "H74主流新鮮度分": 68,
        "H74法人資金加速度分": 82, "H74成交資金加速度分": 84, "H74訊號新鮮分": 76,
        "H74大戶鎖碼狀態": "UNCONFIRMED_NO_PRIOR", "H74熟面孔慣性扣分": 0,
        "近5次入榜次數": 0, "連續入榜次數": 0, "H61重複慣性扣分": 0,
        "H72研究層級": "E2｜多模型優先觀察", "H73研究層級": "L2｜領先觀察",
        "H72風險調整分": 72, "H72品質獲利模型分": 70, "H72成長動能模型分": 72,
        "H72法人需求模型分": 78, "H64核心共振分": 68, "H61RR品質分": 55,
        "H62增量機會分": 72, "H64有效權威": "A-MINUS", "H68次日執行狀態": "NO-FORMAL｜僅研究觀察",
        "3日動能加速度百分點": 3.0, "成交額3日加速度%": 20.0, "成交量3日加速度%": 15.0,
        "三大法人近1日合計": 1200, "三大法人近3日合計": 3000,
        "TDCC千張大戶週變化pp": None, "TDCC大戶資料日期": None, "TDCC大戶前期日期": None,
        "今日漲幅%": 3.0, "近5日漲幅%": 6.0, "收盤距MA20%": 5.0, "當日收盤位置%": 85.0,
        "Entry進場買點分": 68,
    }


good = base_row("1001", "正常增量")
chase = base_row("1002", "漲停追價")
chase.update({"今日漲幅%": 9.8, "近5日漲幅%": 20.0, "收盤距MA20%": 21.0})
missing = base_row("1003", "只有絕對高分")
missing.update({
    "3日動能加速度百分點": None, "成交額3日加速度%": None, "成交量3日加速度%": None,
    "三大法人近1日合計": None, "三大法人近3日合計": None,
})
formal = base_row("1004", "Formal保留")
formal.update({"H64有效權威": "EFFECTIVE-FORMAL", "Entry進場買點分": 25, "今日漲幅%": 9.9})
holder = base_row("1005", "TDCC真增持")
holder.update({"TDCC千張大戶週變化pp": 0.8, "TDCC大戶資料日期": "2026-09-11", "TDCC大戶前期日期": "2026-09-04"})

frame = pd.DataFrame([good, chase, missing, formal, holder])
out = apply_h77_verified_delta_execution_gate(frame)

assert out.loc[out["股票代號"].eq("1001"), "H77研究層級"].iloc[0].startswith("A1")
assert out.loc[out["股票代號"].eq("1002"), "H77研究層級"].iloc[0].startswith("W1")
assert out.loc[out["股票代號"].eq("1003"), "H77研究層級"].iloc[0].startswith("W1")
assert out.loc[out["股票代號"].eq("1004"), "H77研究層級"].iloc[0].startswith("F0")
assert "TDCC真增持" in out.loc[out["股票代號"].eq("1005"), "H77增量證據摘要"].iloc[0]
assert int(out.loc[out["股票代號"].eq("1003"), "H77增量欄位覆蓋數"].iloc[0]) == 0

daily = build_h77_verified_alpha_table(frame, max_rows=8, max_per_sector=2)
assert "1002" not in set(daily["股票代號"].astype(str))
assert {"1001", "1004"}.issubset(set(daily["股票代號"].astype(str)))
wait = build_h77_wait_core_table(frame, max_rows=10)
assert {"1002", "1003"}.issubset(set(wait["股票代號"].astype(str)))
evidence = build_h77_evidence_table(frame, max_rows=10)
assert {"H77增量證據摘要", "H77追價風險", "H77進場品質分"}.issubset(evidence.columns)
governance = build_h77_governance_summary(frame)
assert int(governance.loc[governance["治理項目"].eq("追價/證據等待"), "目前狀態"].iloc[0]) >= 2

perf = pd.DataFrame({"績效指標": ["executable_samples", "executable_win_rate_pct", "avg_selection_alpha_pct"], "目前數值": [162, 40.74, -0.6761]})
summary = build_h77_performance_health_summary(perf, pd.DataFrame({"項目": ["掃描品質"], "數值": ["完整"]}))
assert str(summary.iloc[0]["數值"]).startswith("DEFENSIVE")
print("PASS H77 verified delta + chase/entry/performance brake")
