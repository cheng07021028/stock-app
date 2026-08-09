# -*- coding: utf-8 -*-
"""V179 offline smoke: official full-market close snapshot + page8 weekend update path."""
from __future__ import annotations

import ast
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo
import re
import time
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# --- service parser tests -------------------------------------------------
import godpick_official_latest_price_service as svc


def test_twse_parser():
    payload = {
        "tables": [{
            "title": "每日收盤行情",
            "fields": ["證券代號", "證券名稱", "成交股數", "開盤價", "最高價", "最低價", "收盤價"],
            "data": [
                ["3036", "文曄", "1000000", "232.0", "240.0", "231.0", "238.5"],
                ["2317", "鴻海", "2000000", "264.5", "267.0", "260.0", "262.5"],
            ],
        }]
    }
    out = svc.parse_twse_daily_snapshot(payload, date(2026, 8, 7))
    assert out["3036"]["price"] == 238.5
    assert out["3036"]["date"] == "2026-08-07"
    assert out["3036"]["source"] == "TWSE_OFFICIAL_DAILY_CLOSE"
    assert out["2317"]["market"] == "上市"


def test_tpex_parser():
    payload = {
        "tables": [{
            "fields": ["代號", "名稱", "收盤", "漲跌", "開盤", "最高", "最低", "成交股數"],
            "data": [["8069", "元太", "198.0", "+2.5", "196.0", "200.0", "195.0", "3000000"]],
        }]
    }
    out = svc.parse_tpex_daily_snapshot(payload, date(2026, 8, 7))
    assert out["8069"]["price"] == 198.0
    assert out["8069"]["market"] == "上櫃"
    assert out["8069"]["source"] == "TPEX_OFFICIAL_DAILY_CLOSE"


def test_weekend_backtracks_to_friday():
    old_twse, old_tpex = svc.fetch_twse_daily_snapshot, svc.fetch_tpex_daily_snapshot
    calls = []
    try:
        def fake_twse(d, timeout=6.0):
            calls.append(("twse", d.isoformat()))
            if d == date(2026, 8, 7):
                return {"3036": {"code": "3036", "price": 238.5, "date": d.isoformat(), "market": "上市", "source": "TWSE_OFFICIAL_DAILY_CLOSE"}}, "ok"
            return {}, "empty"
        def fake_tpex(d, timeout=6.0):
            calls.append(("tpex", d.isoformat()))
            if d == date(2026, 8, 7):
                return {"8069": {"code": "8069", "price": 198.0, "date": d.isoformat(), "market": "上櫃", "source": "TPEX_OFFICIAL_DAILY_CLOSE"}}, "ok"
            return {}, "empty"
        svc.fetch_twse_daily_snapshot = fake_twse
        svc.fetch_tpex_daily_snapshot = fake_tpex
        out, diag = svc.fetch_latest_official_market_snapshot(as_of="2026-08-09", lookback_days=10)
        assert diag["twse_date"] == "2026-08-07", diag
        assert diag["tpex_date"] == "2026-08-07", diag
        assert len(out) == 2
        assert calls == [("twse", "2026-08-07"), ("tpex", "2026-08-07")], calls
    finally:
        svc.fetch_twse_daily_snapshot, svc.fetch_tpex_daily_snapshot = old_twse, old_tpex


# --- Page 8 extracted function simulation --------------------------------
PAGE = ROOT / "pages" / "8_股神推薦紀錄.py"
SRC = PAGE.read_text(encoding="utf-8")
TREE = ast.parse(SRC)
WANTED = {
    "_safe_str", "_safe_float", "_normalize_code", "_normalize_bool", "_k",
    "_tw_now", "_tw_today", "_now_text",
    "_resolve_recommendation_basis_v176", "_recalc_row",
    "_normalize_quote_date_v176", "_normalize_quote_time_v176", "_quote_price_from_info",
    "_market_candidates", "_batch_latest_quotes", "_refresh_latest_prices",
}
mod = ast.Module(body=[n for n in TREE.body if isinstance(n, ast.FunctionDef) and n.name in WANTED], type_ignores=[])
ast.fix_missing_locations(mod)
stub_st = SimpleNamespace(session_state={})
ns = {
    "Any": Any, "pd": pd, "date": date, "datetime": datetime, "timedelta": timedelta,
    "ZoneInfo": ZoneInfo, "re": re, "time": time,
    "ThreadPoolExecutor": ThreadPoolExecutor, "as_completed": as_completed,
    "PFX": "godpick_record_", "_TW_TZ": ZoneInfo("Asia/Taipei"), "st": stub_st,
    "LATEST_PRICE_PNL_FIX_VERSION": "record_v179_official_market_snapshot_price_fix_v1_20260809",
    "_god_mode_decision": lambda src: {},
    "_ensure_godpick_record_columns": lambda df: df,
}
exec(compile(mod, str(PAGE), "exec"), ns)


def test_official_snapshot_rescues_closed_market_without_alt_per_stock():
    stub_st.session_state.clear()
    stub_st.session_state[ns["_k"]("enable_alt_price_sources")] = True
    stub_st.session_state[ns["_k"]("enable_slow_price_fallback")] = False
    stub_st.session_state[ns["_k"]("alt_price_source_limit")] = 60
    stub_st.session_state[ns["_k"]("alt_price_workers")] = 4

    # Weekend-style realtime response: no actual trade; only bid reference -> must be rejected.
    ns["_rt_batch_fetch"] = lambda items, refresh_token="": {
        str(i["code"]): {"price": 233.5, "price_source": "bid", "market": i["market"], "raw": {"d": "20260807", "t": "13:30:00"}}
        for i in items
    }
    official_calls = []
    def official(as_of_text):
        official_calls.append(as_of_text)
        return {
            "3036": {"price": 238.5, "date": "2026-08-07", "time": "13:30:00", "market": "上市", "source": "TWSE_OFFICIAL_DAILY_CLOSE"},
            "8069": {"price": 198.0, "date": "2026-08-07", "time": "13:30:00", "market": "上櫃", "source": "TPEX_OFFICIAL_DAILY_CLOSE"},
        }, {"twse_date": "2026-08-07", "tpex_date": "2026-08-07", "twse_count": 1000, "tpex_count": 800}
    ns["_official_latest_market_snapshot_v179"] = official
    ns["_alternative_latest_quote"] = lambda *a, **k: (_ for _ in ()).throw(AssertionError("per-stock alt source should not run when official snapshot has code"))

    payloads = [
        {"股票代號": "3036", "股票名稱": "文曄", "市場別": "上市"},
        {"股票代號": "8069", "股票名稱": "元太", "市場別": "上櫃"},
    ]
    out = ns["_batch_latest_quotes"](payloads)
    assert out["3036"][:4] == (238.5, "上市", "TWSE_OFFICIAL_DAILY_CLOSE", "2026-08-07"), out["3036"]
    assert out["8069"][:4] == (198.0, "上櫃", "TPEX_OFFICIAL_DAILY_CLOSE", "2026-08-07"), out["8069"]
    assert len(official_calls) == 1


def test_recommendation_price_stays_immutable_and_latest_moves():
    stub_st.session_state.clear()
    stub_st.session_state[ns["_k"]("latest_price_batch_size")] = 20
    ns["_batch_latest_quotes"] = lambda payloads: {
        "3036": (238.5, "上市", "TWSE_OFFICIAL_DAILY_CLOSE", "2026-08-07", "13:30:00")
    }
    df = pd.DataFrame([{
        "股票代號": "3036", "股票名稱": "文曄", "市場別": "上市",
        "推薦日期": "2026-08-06", "推薦價格": 233.5, "推薦日價格": 233.5,
        "最新價": 233.5, "目前狀態": "觀察",
    }])
    out = ns["_refresh_latest_prices"](df, only_active=True)
    row = out.iloc[0]
    assert float(row["推薦價格"]) == 233.5
    assert float(row["推薦日價格"]) == 233.5
    assert float(row["最新價"]) == 238.5
    expected = (238.5 - 233.5) / 233.5 * 100.0
    assert abs(float(row["損益幅%"]) - expected) < 1e-9
    assert row["最新價資料日期"] == "2026-08-07"
    assert str(row["最新價來源"]).startswith("TWSE_OFFICIAL")
    assert out.attrs["latest_refresh_summary"]["success"] == 1



def test_screenshot_like_rows_refresh_from_0806_to_0807():
    stub_st.session_state.clear()
    stub_st.session_state[ns["_k"]("latest_price_batch_size")] = 20
    base = {
        "3036": 233.5, "3706": 92.0, "2317": 264.5, "2356": 65.5,
        "2376": 344.0, "3022": 100.5, "2027": 46.1,
    }
    # Synthetic official 8/7 closes intentionally differ from 8/6 recommendation prices.
    closes = {code: round(px * (1.01 + (int(code[-1]) % 3) * 0.002), 2) for code, px in base.items()}
    ns["_batch_latest_quotes"] = lambda payloads: {
        ns["_normalize_code"](p.get("股票代號")): (
            closes[ns["_normalize_code"](p.get("股票代號"))],
            str(p.get("市場別") or "上市"),
            "TWSE_OFFICIAL_DAILY_CLOSE", "2026-08-07", "13:30:00"
        ) for p in payloads
    }
    codes = ["3036", "3706", "2317", "2356", "2376", "3022", "2027", "2317", "2356", "2376"]
    rows = [{
        "股票代號": c, "股票名稱": c, "市場別": "上市", "推薦日期": "2026-08-06",
        "推薦價格": base[c], "推薦日價格": base[c], "最新價": base[c], "目前狀態": "觀察"
    } for c in codes]
    out = ns["_refresh_latest_prices"](pd.DataFrame(rows), only_active=True)
    assert len(out) == 10
    assert out.attrs["latest_refresh_summary"]["success"] == 10
    assert (pd.to_numeric(out["最新價"], errors="coerce") != pd.to_numeric(out["推薦價格"], errors="coerce")).all()
    assert (out["最新價資料日期"].astype(str) == "2026-08-07").all()
    assert pd.to_numeric(out["損益幅%"], errors="coerce").abs().gt(0).all()


def test_button_auto_persistence_is_present():
    # Regression guard: successful latest-price update must no longer be session-only.
    button_pos = SRC.find('if st.button("📈 更新最新價"')
    end_pos = SRC.find('with top_cols[2]:', button_pos)
    block = SRC[button_pos:end_pos]
    assert "_save_records_dual(df)" in block
    assert "已自動保存權威紀錄" in block


def main():
    test_twse_parser()
    test_tpex_parser()
    test_weekend_backtracks_to_friday()
    test_official_snapshot_rescues_closed_market_without_alt_per_stock()
    test_recommendation_price_stays_immutable_and_latest_moves()
    test_screenshot_like_rows_refresh_from_0806_to_0807()
    test_button_auto_persistence_is_present()
    print("PASS V179 official exchange snapshot, weekend backtrack, immutable recommendation price, P/L and auto-persistence")


if __name__ == "__main__":
    main()
