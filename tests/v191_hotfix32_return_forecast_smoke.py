# -*- coding: utf-8 -*-
from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from godpick_return_forecast_engine import apply_return_forecast, forecast_validation_summary


def _row() -> pd.DataFrame:
    return pd.DataFrame([{
        "股票代號": "9999",
        "股票名稱": "測試股",
        "正式推薦分區": "正式下週主推薦",
        "類別": "測試族群",
        "SuperAI校準後隔日上漲機率%": 64,
        "Entry進場分": 72,
        "Risk風控分": 76,
        "SuperAI Trade分": 78,
        "SuperAI 最終決策分": 70,
        "追價風險分": 28,
        "近5日漲幅%": 3,
        "近20日漲幅%": 7,
        "路徑風險報酬比": 2.4,
        "停損距離_隔日%": 6.0,
    }])


def _truth_rows(n: int = 120) -> list[dict]:
    rows = []
    for i in range(n):
        actual = 0.7 + ((i % 9) - 4) * 0.35
        rows.append({
            "T1成熟": True,
            "推薦角色": "正式下週主推薦",
            "類別": "測試族群",
            "SuperAI校準後上漲機率%": 62 + (i % 5),
            "隔日候選漲跌%": actual,
            "推薦後5日%": 2.5 + ((i % 11) - 5) * 0.7,
            "推薦後10日%": 4.2 + ((i % 13) - 6) * 0.9,
            "推薦後20日%": 6.5 + ((i % 15) - 7) * 1.1,
            "H32隔日預估漲跌幅%": 0.6,
            "H32隔日90%區間下緣%": -2.5,
            "H32隔日90%區間上緣%": 3.5,
            "H32_5日90%區間下緣%": -5.0,
            "H32_5日90%區間上緣%": 10.0,
            "H32_10日90%區間下緣%": -8.0,
            "H32_10日90%區間上緣%": 16.0,
            "H32_20日90%區間下緣%": -15.0,
            "H32_20日90%區間上緣%": 25.0,
        })
    return rows


def test_unverified_state_never_claims_90_accuracy() -> None:
    out = apply_return_forecast(_row(), truth_rows=[])
    text = str(out.iloc[0]["H32預測驗證狀態"])
    assert "禁止宣稱90%準確" in text, text
    assert out.iloc[0]["H32隔日90%區間下緣%"] <= out.iloc[0]["H32隔日預估漲跌幅%"] <= out.iloc[0]["H32隔日90%區間上緣%"]


def test_mature_truth_drives_empirical_forecast_and_validation() -> None:
    truth = _truth_rows()
    out = apply_return_forecast(_row(), truth_rows=truth)
    r = out.iloc[0]
    assert r["H32隔日校準樣本數"] >= 20
    assert "歷史同儕" in r["H32預測方法"]
    assert r["H32_10日90%區間下緣%"] < r["H32_10日預估報酬%"] < r["H32_10日90%區間上緣%"]
    summary = forecast_validation_summary(truth_rows=truth)
    assert summary["interval_samples"] == 120
    assert summary["interval_coverage_pct"] is not None
    assert "區間覆蓋" in summary["status"]


def test_h32_does_not_modify_formal_authority_columns() -> None:
    source = _row()
    out = apply_return_forecast(source, truth_rows=_truth_rows(40))
    for col in ["正式推薦分區", "Entry進場分", "Risk風控分", "路徑風險報酬比"]:
        assert out.iloc[0][col] == source.iloc[0][col]


def test_scheduler_passes_authority_rows_into_truth_service() -> None:
    src = (ROOT / "godpick_auto_update_tasks.py").read_text(encoding="utf-8")
    assert "refresh_t1_trade_truth(records=authority_rows" in src


def test_page07_applies_forecast_before_persistence() -> None:
    src = (ROOT / "pages" / "7_股神推薦.py").read_text(encoding="utf-8")
    apply_pos = src.index("rec_df = _h32_apply_return_forecast(rec_df)")
    save_pos = src.index("_save_recommend_result_to_state(rec_df, category_strength_df, hot_pick_df)", apply_pos)
    assert apply_pos < save_pos
    assert 'st.session_state[_k("candidate_diagnosis_store")] = _h32_candidate' in src


def test_t1_truth_persists_h32_forecast_for_future_oos_validation() -> None:
    src = (ROOT / "godpick_t1_trade_truth.py").read_text(encoding="utf-8")
    assert '"H32隔日預估漲跌幅%": _h32_pred1' in src
    assert '"H32實際10日報酬%": _h32_actual[10]' in src
    assert '"H32隔日90%區間命中"' in src


def test_excel_guide_surfaces_next_day_and_swing_forecasts() -> None:
    src = (ROOT / "godpick_super_ai_excel_guide.py").read_text(encoding="utf-8")
    assert 'selected["隔日預估漲跌幅%"]' in src
    assert 'selected["後續波段預估漲幅%"]' in src
    assert 'selected["報酬預測驗證"]' in src


if __name__ == "__main__":
    test_unverified_state_never_claims_90_accuracy()
    test_mature_truth_drives_empirical_forecast_and_validation()
    test_h32_does_not_modify_formal_authority_columns()
    test_scheduler_passes_authority_rows_into_truth_service()
    test_page07_applies_forecast_before_persistence()
    test_t1_truth_persists_h32_forecast_for_future_oos_validation()
    test_excel_guide_surfaces_next_day_and_swing_forecasts()
    print("PASS V191-H32 return forecast regression smoke")
