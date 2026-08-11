# -*- coding: utf-8 -*-
"""V176 latest-price/P&L regression tests without network or Streamlit runtime."""
from __future__ import annotations

import ast
import re
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PAGE = ROOT / "pages" / "8_股神推薦紀錄.py"
SOURCE = PAGE.read_text(encoding="utf-8")
TREE = ast.parse(SOURCE)

WANTED = {
    "_safe_str",
    "_safe_float",
    "_normalize_code",
    "_normalize_bool",
    "_now_text",
    "_k",
    "_resolve_recommendation_basis_v176",
    "_recalc_row",
    "_normalize_quote_date_v176",
    "_normalize_quote_time_v176",
    "_quote_price_from_info",
    "_quote_from_twse_mis",
    "_refresh_latest_prices",
}
selected = [node for node in TREE.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in WANTED]
module = ast.Module(body=selected, type_ignores=[])
ast.fix_missing_locations(module)

ns: dict[str, Any] = {
    "Any": Any,
    "pd": pd,
    "date": date,
    "datetime": datetime,
    "timedelta": timedelta,
    "re": re,
    "time": time,
    "PFX": "godpick_record_",
    "LATEST_PRICE_PNL_FIX_VERSION": "record_v176_verified_quote_pnl_v1_20260806",
    "st": SimpleNamespace(session_state={}),
    "_god_mode_decision": lambda src: {},
    "_ensure_godpick_record_columns": lambda df: df,
    "_tw_today": lambda: date.today(),
    "_tw_now": lambda: datetime.now(),
}
exec(compile(module, str(PAGE), "exec"), ns)


def test_prev_close_is_rejected() -> None:
    price, market, src, qdate, qtime = ns["_quote_price_from_info"]({
        "price": 100,
        "price_source": "prev_close",
        "market": "上市",
        "update_time": "20260805 13:30:00",
    })
    assert price is None
    assert src.startswith(("REFERENCE_ONLY", "INDICATIVE_ONLY"))
    assert qdate == "2026-08-05"
    assert qtime == "13:30:00"



def test_twse_mis_uses_only_final_trade_and_never_match_or_previous_close() -> None:
    ns["_quote_request_json"] = lambda *args, **kwargs: {
        "msgArray": [{"z": "-", "pz": "101.50", "y": "100.00", "d": "20260806", "t": "13:30:00"}]
    }
    price, market, src, qdate, qtime = ns["_quote_from_twse_mis"]("2330", "上市")
    # V178+ tightened the rule: pz/match is indicative, not a final transaction.
    assert price is None
    assert "INDICATIVE_ONLY" in src or "REFERENCE_ONLY" in src or "NO_ACTUAL_TRADE" in src
    assert qdate == "2026-08-06"

    ns["_quote_request_json"] = lambda *args, **kwargs: {
        "msgArray": [{"z": "-", "pz": "-", "y": "100.00", "d": "20260806", "t": "13:30:00"}]
    }
    price2, _, src2, _, _ = ns["_quote_from_twse_mis"]("2330", "上市")
    assert price2 is None
    assert "REFERENCE_ONLY" in src2 or "NO_ACTUAL_TRADE" in src2

def test_actual_trade_is_accepted_and_pnl_recalculated() -> None:
    row = ns["_recalc_row"]({
        "股票代號": "2330",
        "推薦日期": "2026-08-05",
        "推薦價格": 100,
        "推薦日價格": 100,
        "最新價": 105,
        "最新價資料日期": "2026-08-06",
        "最新價更新狀態": "已更新｜行情日期已驗證",
        "目前狀態": "觀察",
    })
    assert abs(row["損益金額"] - 5.0) < 1e-9
    assert abs(row["損益幅%"] - 5.0) < 1e-9
    assert row["推薦基準價來源"] == "推薦價格"
    assert "已計算" in row["損益計算狀態"]


def test_missing_recommendation_price_is_not_filled_from_latest() -> None:
    row = ns["_recalc_row"]({
        "股票代號": "2330",
        "推薦日期": "2026-08-05",
        "推薦價格": None,
        "推薦日價格": None,
        "最新價": 105,
        "最新價資料日期": "2026-08-06",
        "最新價更新狀態": "已更新｜行情日期已驗證",
        "目前狀態": "觀察",
    })
    assert row.get("推薦價格") is None
    assert row["損益金額"] is None
    assert row["損益幅%"] is None
    assert row["推薦基準價來源"] == "缺少推薦基準價"
    assert "未計算" in row["損益計算狀態"]


def test_refresh_accepts_newer_quote_and_calculates_pnl() -> None:
    ns["st"].session_state.clear()
    ns["st"].session_state[ns["_k"]("latest_price_batch_size")] = 20
    ns["_batch_latest_quotes"] = lambda payloads: {
        "2330": (105.0, "上市", "TWSE_MIS_tse_TRADE", "2026-08-06", "13:30:00")
    }
    df = pd.DataFrame([{
        "股票代號": "2330",
        "股票名稱": "台積電",
        "市場別": "上市",
        "推薦日期": "2026-08-05",
        "推薦價格": 100.0,
        "推薦日價格": 100.0,
        "最新價": 100.0,
        "目前狀態": "觀察",
    }])
    out = ns["_refresh_latest_prices"](df, only_active=True)
    row = out.iloc[0]
    summary = out.attrs["latest_refresh_summary"]
    assert float(row["最新價"]) == 105.0
    assert abs(float(row["損益幅%"]) - 5.0) < 1e-9
    assert row["最新價資料日期"] == "2026-08-06"
    assert summary["success"] == 1
    assert summary["pnl_calculated"] == 1


def test_refresh_does_not_claim_same_day_reference_as_new_update() -> None:
    ns["st"].session_state.clear()
    ns["st"].session_state[ns["_k"]("latest_price_batch_size")] = 20
    ns["_batch_latest_quotes"] = lambda payloads: {
        "2330": (100.0, "上市", "YAHOO_CHART:2330.TW", "2026-08-05", "")
    }
    df = pd.DataFrame([{
        "股票代號": "2330",
        "股票名稱": "台積電",
        "市場別": "上市",
        "推薦日期": "2026-08-05",
        "推薦價格": 100.0,
        "推薦日價格": 100.0,
        "最新價": 100.0,
        "目前狀態": "觀察",
    }])
    out = ns["_refresh_latest_prices"](df, only_active=True)
    row = out.iloc[0]
    summary = out.attrs["latest_refresh_summary"]
    # The test is intended for 2026-08-06+; if run on the recommendation date,
    # the same-day close is valid and this assertion is skipped.
    if date.today().isoformat() > "2026-08-05":
        assert summary["success"] == 0
        assert summary["waiting_new_trade"] == 1
        assert str(row["最新價更新狀態"]).startswith("等待新交易日")
        assert "沿用" in str(row["損益計算狀態"])


def main() -> None:
    test_prev_close_is_rejected()
    test_twse_mis_uses_only_final_trade_and_never_match_or_previous_close()
    test_actual_trade_is_accepted_and_pnl_recalculated()
    test_missing_recommendation_price_is_not_filled_from_latest()
    test_refresh_accepts_newer_quote_and_calculates_pnl()
    test_refresh_does_not_claim_same_day_reference_as_new_update()
    print("PASS page8 V176 verified quote and P/L smoke")


if __name__ == "__main__":
    main()
