# -*- coding: utf-8 -*-
"""V183 Super-AI market/chip/ETF context service.

This module deliberately separates *data collection* from *trading inference*.
Missing/failed sources stay unknown; they are never converted to neutral zeroes.
Official sources are preferred:
- TWSE daily margin transactions (MI_MARGN)
- TPEx OpenAPI margin balance
- TAIFEX futures institutional positions and TXO Put/Call ratio
- Broad/technology ETF price breadth derived from normal market history service

The cache is persisted through the V183 durability coordinator.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Any
import math
import re

import pandas as pd
import requests

try:
    from godpick_durability_service import persist_json_async
except Exception:
    persist_json_async = None

CONTEXT_VERSION = "super_ai_market_context_v183_20260811"
CACHE_FILE = "super_ai_market_context.json"
BASE_DIR = Path(__file__).resolve().parent

TWSE_MARGIN_URL = "https://www.twse.com.tw/rwd/zh/marginTrading/MI_MARGN"
TPEX_MARGIN_URL = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_margin_balance"
TAIFEX_FUT_URL = "https://www.taifex.com.tw/cht/3/futContractsDateExcel"
TAIFEX_PC_URL = "https://www.taifex.com.tw/cht/3/pcRatioExcel"
ETF_CODES = ["0050", "006208", "0052", "00881"]


def _now() -> str:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Taipei")).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _today() -> datetime:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Taipei"))
    except Exception:
        return datetime.now()


def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def _num(v: Any, default: float | None = None) -> float | None:
    try:
        if v is None:
            return default
        s = str(v).strip().replace(",", "").replace("%", "")
        if not s or s.lower() in {"nan", "none", "null", "--", "-"}:
            return default
        x = float(s)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _find_key(row: dict[str, Any], aliases: list[str]) -> Any:
    for key in aliases:
        if key in row and _safe_str(row.get(key)):
            return row.get(key)
    norm = {re.sub(r"\s+", "", str(k)).lower(): v for k, v in row.items()}
    for alias in aliases:
        a = re.sub(r"\s+", "", alias).lower()
        for key, val in norm.items():
            if a == key or a in key:
                if _safe_str(val):
                    return val
    return None


def _last_completed_trade_date(now: datetime | None = None) -> datetime:
    dt = now or _today()
    d = dt
    # During a trading day use previous business day for end-of-day credit data;
    # after 21:30 allow today's margin publication to be attempted.
    if d.weekday() < 5 and (d.hour < 21 or (d.hour == 21 and d.minute < 30)):
        d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def _parse_twse_margin_payload(payload: dict[str, Any]) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    stock_map: dict[str, dict[str, Any]] = {}
    market: dict[str, Any] = {}
    # New/legacy TWSE JSONs may expose creditList plus tables; support both.
    credit_fields = payload.get("creditFields") or []
    credit_list = payload.get("creditList") or []
    if credit_fields and credit_list:
        for values in credit_list:
            row = dict(zip(credit_fields, values))
            item = _safe_str(row.get("項目"))
            if "融資金額" in item:
                prev = _num(row.get("前日餘額")); cur = _num(row.get("今日餘額"))
                market["上市融資金額前日仟元"] = prev
                market["上市融資金額今日仟元"] = cur
                market["上市融資金額增減仟元"] = (cur-prev) if cur is not None and prev is not None else None
            elif item.startswith("融資"):
                prev = _num(row.get("前日餘額")); cur = _num(row.get("今日餘額"))
                market["上市融資張數前日"] = prev; market["上市融資張數今日"] = cur
                market["上市融資張數增減"] = (cur-prev) if cur is not None and prev is not None else None
            elif item.startswith("融券"):
                prev = _num(row.get("前日餘額")); cur = _num(row.get("今日餘額"))
                market["上市融券張數前日"] = prev; market["上市融券張數今日"] = cur
                market["上市融券張數增減"] = (cur-prev) if cur is not None and prev is not None else None
    for table in payload.get("tables") or []:
        fields = table.get("fields") or []
        data = table.get("data") or []
        if not fields or not data:
            continue
        if not any("代號" in _safe_str(x) or "證券代號" in _safe_str(x) for x in fields):
            continue
        # Duplicate Chinese labels occur for margin and short blocks. Position is
        # more stable than dict(zip(...)) in this official report.
        for values in data:
            if not values:
                continue
            code = _safe_str(values[0])
            if not re.fullmatch(r"\d{4,6}", code):
                continue
            vals = [_num(x) for x in values]
            # Typical order: code/name + 6 margin + 6 short + offset/note.
            margin_prev = vals[5] if len(vals) > 6 else None
            margin_cur = vals[6] if len(vals) > 6 else None
            short_prev = vals[11] if len(vals) > 12 else None
            short_cur = vals[12] if len(vals) > 12 else None
            stock_map[code] = {
                "市場別": "上市",
                "融資前日餘額張": margin_prev,
                "融資今日餘額張": margin_cur,
                "融資增減張": (margin_cur-margin_prev) if margin_cur is not None and margin_prev is not None else None,
                "融券前日餘額張": short_prev,
                "融券今日餘額張": short_cur,
                "融券增減張": (short_cur-short_prev) if short_cur is not None and short_prev is not None else None,
                "來源": "TWSE_MI_MARGN",
            }
    return stock_map, market


def fetch_twse_margin(date: datetime | None = None, *, timeout: int = 12) -> tuple[dict[str, dict[str, Any]], dict[str, Any], str]:
    d = date or _last_completed_trade_date()
    try:
        r = requests.get(TWSE_MARGIN_URL, params={"date": d.strftime("%Y%m%d"), "selectType": "STOCK", "response": "json"}, timeout=(5, timeout), headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        payload = r.json()
        stock_map, market = _parse_twse_margin_payload(payload if isinstance(payload, dict) else {})
        market["TWSE融資券資料日期"] = d.strftime("%Y-%m-%d")
        return stock_map, market, f"TWSE margin {len(stock_map)}"
    except Exception as exc:
        return {}, {}, f"TWSE margin failed: {exc}"


def _parse_tpex_margin_rows(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        code = _safe_str(_find_key(row, ["SecuritiesCompanyCode", "股票代號", "證券代號", "代號", "Code"]))
        if not re.fullmatch(r"\d{4,6}", code):
            continue
        m_prev = _num(_find_key(row, ["MarginPurchaseYesterdayBalance", "融資前日餘額", "融資前日餘額張"]))
        m_cur = _num(_find_key(row, ["MarginPurchaseTodayBalance", "融資今日餘額", "融資餘額", "融資今日餘額張"]))
        s_prev = _num(_find_key(row, ["ShortSaleYesterdayBalance", "融券前日餘額", "融券前日餘額張"]))
        s_cur = _num(_find_key(row, ["ShortSaleTodayBalance", "融券今日餘額", "融券餘額", "融券今日餘額張"]))
        out[code] = {
            "市場別": "上櫃",
            "融資前日餘額張": m_prev,
            "融資今日餘額張": m_cur,
            "融資增減張": (m_cur-m_prev) if m_cur is not None and m_prev is not None else None,
            "融券前日餘額張": s_prev,
            "融券今日餘額張": s_cur,
            "融券增減張": (s_cur-s_prev) if s_cur is not None and s_prev is not None else None,
            "來源": "TPEX_OPENAPI_MARGIN",
        }
    return out


def fetch_tpex_margin(*, timeout: int = 12) -> tuple[dict[str, dict[str, Any]], str]:
    try:
        r = requests.get(TPEX_MARGIN_URL, timeout=(5, timeout), headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
        r.raise_for_status()
        rows = r.json()
        if isinstance(rows, dict):
            rows = rows.get("data") or rows.get("aaData") or []
        out = _parse_tpex_margin_rows(rows if isinstance(rows, list) else [])
        return out, f"TPEx margin {len(out)}"
    except Exception as exc:
        return {}, f"TPEx margin failed: {exc}"


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if isinstance(out.columns, pd.MultiIndex):
        out.columns = ["|".join(_safe_str(x) for x in tup if _safe_str(x)) for tup in out.columns]
    else:
        out.columns = [_safe_str(x) for x in out.columns]
    return out


def fetch_taifex_context(*, timeout: int = 12) -> tuple[dict[str, Any], list[str]]:
    result: dict[str, Any] = {}
    diag: list[str] = []
    try:
        r = requests.get(TAIFEX_PC_URL, timeout=(5, timeout), headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        tables = pd.read_html(StringIO(r.text))
        pc_df = next((t for t in tables if t.shape[1] >= 7 and any("日期" in _safe_str(c) for c in (t.columns if not isinstance(t.columns, pd.MultiIndex) else t.columns.get_level_values(-1)))), None)
        if pc_df is not None and not pc_df.empty:
            pc_df = _flatten_columns(pc_df)
            row = pc_df.iloc[0].to_dict()
            result["PCR成交量比%"] = _num(_find_key(row, ["買賣權成交量比率%", "成交量比率"]))
            result["PCR未平倉量比%"] = _num(_find_key(row, ["買賣權未平倉量比率%", "未平倉量比率"]))
            result["TAIFEX_PCR資料日期"] = _safe_str(_find_key(row, ["日期"]))
            diag.append("TAIFEX PCR OK")
    except Exception as exc:
        diag.append(f"TAIFEX PCR failed: {exc}")
    try:
        r = requests.get(TAIFEX_FUT_URL, timeout=(5, timeout), headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        tables = pd.read_html(StringIO(r.text))
        found = False
        for t in tables:
            t = _flatten_columns(t)
            if t.empty: continue
            for _, sr in t.iterrows():
                blob = "|".join(_safe_str(v) for v in sr.tolist())
                if "臺股期貨" in blob and "外資" in blob:
                    nums = [_num(v) for v in sr.tolist()]
                    nums = [x for x in nums if x is not None]
                    # TAIFEX table's last quantity-like net OI is the most useful
                    # robust field; exact column names vary with multiindex HTML.
                    if len(nums) >= 6:
                        result["外資臺指期未平倉淨口數"] = nums[-2] if len(nums) >= 2 else nums[-1]
                    result["TAIFEX期貨資料文字"] = blob[:300]
                    found = True; break
            if found: break
        diag.append("TAIFEX futures OK" if found else "TAIFEX futures row not found")
    except Exception as exc:
        diag.append(f"TAIFEX futures failed: {exc}")
    return result, diag


def _history_score(df: pd.DataFrame) -> dict[str, Any]:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return {"score": None, "ret5": None, "ret20": None, "last": None, "date": ""}
    work = df.copy()
    close_col = next((c for c in ["收盤價", "Close", "close"] if c in work.columns), None)
    date_col = next((c for c in ["日期", "Date", "date"] if c in work.columns), None)
    if not close_col:
        return {"score": None, "ret5": None, "ret20": None, "last": None, "date": ""}
    close = pd.to_numeric(work[close_col], errors="coerce").dropna()
    if close.empty:
        return {"score": None, "ret5": None, "ret20": None, "last": None, "date": ""}
    last = float(close.iloc[-1])
    def ret(n):
        return (last / float(close.iloc[-1-n]) - 1) * 100 if len(close) > n and float(close.iloc[-1-n]) > 0 else None
    r5, r20 = ret(5), ret(20)
    score = 50.0
    if r5 is not None: score += max(-20, min(20, r5 * 3.0))
    if r20 is not None: score += max(-20, min(20, r20 * 1.2))
    date_text = _safe_str(work[date_col].iloc[-1]) if date_col else ""
    return {"score": round(max(0,min(100,score)),1), "ret5": r5, "ret20": r20, "last": last, "date": date_text}


def fetch_etf_context() -> tuple[dict[str, Any], list[str]]:
    result: dict[str, Any] = {"ETFs": {}}
    diag: list[str] = []
    try:
        from utils import get_history_data
    except Exception as exc:
        return result, [f"ETF history service unavailable: {exc}"]
    now = _today(); start = (now - timedelta(days=80)).strftime("%Y-%m-%d"); end = now.strftime("%Y-%m-%d")
    scores=[]
    for code in ETF_CODES:
        try:
            df = get_history_data(code, code, "上市", start_date=start, end_date=end)
            info = _history_score(df)
            result["ETFs"][code] = info
            if info.get("score") is not None: scores.append(float(info["score"]))
        except Exception as exc:
            result["ETFs"][code] = {"score": None, "error": str(exc)}
    result["ETF市場確認分"] = round(sum(scores)/len(scores),1) if scores else None
    result["ETF有效檔數"] = len(scores)
    diag.append(f"ETF context {len(scores)}/{len(ETF_CODES)}")
    return result, diag


def refresh_super_ai_market_context(*, fetch_etf: bool = False) -> tuple[dict[str, Any], list[str]]:
    diagnostics: list[str] = []
    twse_map, twse_market, msg = fetch_twse_margin(); diagnostics.append(msg)
    tpex_map, msg = fetch_tpex_margin(); diagnostics.append(msg)
    taifex, msgs = fetch_taifex_context(); diagnostics.extend(msgs)
    etf, msgs = fetch_etf_context() if fetch_etf else ({"ETFs": {}, "ETF市場確認分": None, "ETF有效檔數": 0}, ["ETF live refresh skipped; use cached/context history"])
    diagnostics.extend(msgs)
    margin = dict(twse_map); margin.update(tpex_map)
    context = {
        "version": CONTEXT_VERSION,
        "updated_at": _now(),
        "margin_by_stock": margin,
        "market_margin": twse_market,
        "taifex": taifex,
        "etf": etf,
        "source_diagnostics": diagnostics,
        "provenance": {
            "TWSE融資融券": TWSE_MARGIN_URL,
            "TPEx融資融券": TPEX_MARGIN_URL,
            "TAIFEX期貨": TAIFEX_FUT_URL,
            "TAIFEX_PCR": TAIFEX_PC_URL,
            "ETF": "TWSE/TPEx price history via existing history service",
        },
    }
    if callable(persist_json_async):
        persist_json_async(CACHE_FILE, context, reason="V183 SuperAI market context refresh")
    else:
        import json
        (BASE_DIR/CACHE_FILE).write_text(json.dumps(context, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return context, diagnostics


def load_super_ai_market_context() -> dict[str, Any]:
    try:
        import json
        p=BASE_DIR/CACHE_FILE
        if p.exists():
            data=json.loads(p.read_text(encoding="utf-8-sig"))
            if isinstance(data, dict): return data
    except Exception:
        pass
    return {"version": CONTEXT_VERSION, "updated_at": "", "margin_by_stock": {}, "market_margin": {}, "taifex": {}, "etf": {}, "source_diagnostics": ["context cache missing"]}


__all__ = ["CONTEXT_VERSION", "CACHE_FILE", "load_super_ai_market_context", "refresh_super_ai_market_context", "fetch_twse_margin", "fetch_tpex_margin", "fetch_taifex_context", "fetch_etf_context"]
