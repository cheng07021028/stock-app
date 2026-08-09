# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any
import re

import requests

SERVICE_VERSION = "official_latest_snapshot_v179_20260809"
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
}


def _safe_text(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _norm_code(v: Any) -> str:
    s = _safe_text(v).split(".")[0]
    return "".join(ch for ch in s if ch.isdigit())[:6]


def _num(v: Any) -> float | None:
    if v is None:
        return None
    s = _safe_text(v).replace(",", "").replace("+", "").replace("--", "").replace("---", "")
    if not s or s in {"-", "X", "除權", "除息"}:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _field_key(v: Any) -> str:
    s = _safe_text(v).lower()
    s = re.sub(r"[\s\n\r\t()（）\[\]{}_/\\.-]+", "", s)
    return s


def _find_field_index(fields: list[Any], candidates: tuple[str, ...]) -> int | None:
    keys = [_field_key(x) for x in fields]
    wanted = tuple(_field_key(x) for x in candidates)
    for i, k in enumerate(keys):
        if k in wanted:
            return i
    for i, k in enumerate(keys):
        if any(w and (w in k or k in w) for w in wanted):
            return i
    return None


def _table_objects(payload: Any) -> list[dict[str, Any]]:
    """Recursively collect exchange table objects with fields/data arrays."""
    out: list[dict[str, Any]] = []
    seen: set[int] = set()

    def walk(obj: Any) -> None:
        oid = id(obj)
        if oid in seen:
            return
        seen.add(oid)
        if isinstance(obj, dict):
            fields = obj.get("fields") or obj.get("field") or obj.get("columns") or obj.get("titles")
            data = obj.get("data") or obj.get("aaData") or obj.get("rows")
            if isinstance(fields, list) and isinstance(data, list):
                out.append({"fields": fields, "data": data, "title": _safe_text(obj.get("title") or obj.get("name"))})
            for v in obj.values():
                if isinstance(v, (dict, list)):
                    walk(v)
        elif isinstance(obj, list):
            for v in obj:
                if isinstance(v, (dict, list)):
                    walk(v)

    walk(payload)
    return out


def _rows_from_flat_payload(payload: Any) -> list[list[Any]]:
    if not isinstance(payload, dict):
        return []
    for key in ("aaData", "data9", "data", "result"):
        v = payload.get(key)
        if isinstance(v, list) and v and all(isinstance(x, list) for x in v):
            return v
    return []


def _parse_snapshot(payload: Any, trade_date: date, market: str, source: str) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    tables = _table_objects(payload)

    for table in tables:
        fields = table.get("fields") or []
        rows = table.get("data") or []
        code_i = _find_field_index(fields, ("證券代號", "股票代號", "代號", "code", "stockno"))
        name_i = _find_field_index(fields, ("證券名稱", "股票名稱", "名稱", "name"))
        close_i = _find_field_index(fields, ("收盤價", "收盤", "close", "closingprice"))
        if code_i is None or close_i is None:
            continue
        for row in rows:
            if not isinstance(row, (list, tuple)):
                continue
            if max(code_i, close_i) >= len(row):
                continue
            code = _norm_code(row[code_i])
            price = _num(row[close_i])
            if not code or price is None or price <= 0:
                continue
            name = _safe_text(row[name_i]) if name_i is not None and name_i < len(row) else ""
            result[code] = {
                "code": code,
                "name": name,
                "price": float(price),
                "date": trade_date.isoformat(),
                "time": "13:30:00",
                "market": market,
                "source": source,
            }

    if result:
        return result

    # Legacy TPEx/TWSE responses may expose rows without fields.  Use conservative
    # fallback indices only when the row shape clearly matches common formats.
    for row in _rows_from_flat_payload(payload):
        if not isinstance(row, list) or len(row) < 3:
            continue
        code = _norm_code(row[0])
        if not code:
            continue
        price = _num(row[2])
        if market == "上市" and len(row) >= 9:
            # TWSE legacy MI_INDEX common layout: code,name,volume,...,open,high,low,close
            candidate = _num(row[8])
            if candidate is not None and candidate > 0:
                price = candidate
        if price is None or price <= 0:
            continue
        result[code] = {
            "code": code,
            "name": _safe_text(row[1]) if len(row) > 1 else "",
            "price": float(price),
            "date": trade_date.isoformat(),
            "time": "13:30:00",
            "market": market,
            "source": source,
        }
    return result


def parse_twse_daily_snapshot(payload: Any, trade_date: date) -> dict[str, dict[str, Any]]:
    return _parse_snapshot(payload, trade_date, "上市", "TWSE_OFFICIAL_DAILY_CLOSE")


def parse_tpex_daily_snapshot(payload: Any, trade_date: date) -> dict[str, dict[str, Any]]:
    return _parse_snapshot(payload, trade_date, "上櫃", "TPEX_OFFICIAL_DAILY_CLOSE")


def _get_json(url: str, params: dict[str, Any], timeout: float) -> Any:
    headers = dict(_HEADERS)
    if "twse.com.tw" in url:
        headers["Referer"] = "https://www.twse.com.tw/zh/trading/historical/mi-index.html"
    elif "tpex.org.tw" in url:
        headers["Referer"] = "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/quote.html"
    resp = requests.get(url, params=params, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def fetch_twse_daily_snapshot(trade_date: date, timeout: float = 6.0) -> tuple[dict[str, dict[str, Any]], str]:
    ymd = trade_date.strftime("%Y%m%d")
    endpoints = [
        "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX",
        "https://www.twse.com.tw/exchangeReport/MI_INDEX",
    ]
    errors: list[str] = []
    for url in endpoints:
        try:
            payload = _get_json(url, {"date": ymd, "type": "ALLBUT0999", "response": "json"}, timeout)
            rows = parse_twse_daily_snapshot(payload, trade_date)
            if rows:
                return rows, f"TWSE {trade_date.isoformat()} {len(rows)}檔"
            errors.append(f"TWSE_EMPTY:{url.split('/')[-1]}")
        except Exception as exc:
            errors.append(f"TWSE_ERR:{type(exc).__name__}:{str(exc)[:80]}")
    return {}, " | ".join(errors)[:400]


def fetch_tpex_daily_snapshot(trade_date: date, timeout: float = 6.0) -> tuple[dict[str, dict[str, Any]], str]:
    roc = f"{trade_date.year - 1911}/{trade_date.month:02d}/{trade_date.day:02d}"
    endpoints = [
        ("https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes", {"date": roc, "id": "", "response": "json"}),
        ("https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php", {"l": "zh-tw", "d": roc, "_": ""}),
    ]
    errors: list[str] = []
    for url, params in endpoints:
        try:
            payload = _get_json(url, params, timeout)
            rows = parse_tpex_daily_snapshot(payload, trade_date)
            if rows:
                return rows, f"TPEx {trade_date.isoformat()} {len(rows)}檔"
            errors.append(f"TPEX_EMPTY:{url.split('/')[-1]}")
        except Exception as exc:
            errors.append(f"TPEX_ERR:{type(exc).__name__}:{str(exc)[:80]}")
    return {}, " | ".join(errors)[:400]


def _coerce_date(v: date | datetime | str | None) -> date:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    if isinstance(v, str) and v.strip():
        try:
            return datetime.strptime(v.strip()[:10], "%Y-%m-%d").date()
        except Exception:
            pass
    return date.today()


def fetch_latest_official_market_snapshot(
    as_of: date | datetime | str | None = None,
    lookback_days: int = 10,
    timeout: float = 6.0,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Fetch latest completed TWSE + TPEx daily close maps with very few requests.

    The function probes recent weekdays backwards.  Once a market returns a valid
    full-market daily table, that market stops probing.  This makes weekend/holiday
    updates reliable without one HTTP request per stock.
    """
    end = _coerce_date(as_of)
    lookback_days = max(3, min(int(lookback_days or 10), 20))
    twse_rows: dict[str, dict[str, Any]] = {}
    tpex_rows: dict[str, dict[str, Any]] = {}
    twse_date = ""
    tpex_date = ""
    messages: list[str] = []
    attempts = 0

    for offset in range(0, lookback_days + 1):
        d = end - timedelta(days=offset)
        if d.weekday() >= 5:
            continue
        if not twse_rows:
            attempts += 1
            rows, msg = fetch_twse_daily_snapshot(d, timeout=timeout)
            messages.append(msg)
            if rows:
                twse_rows = rows
                twse_date = d.isoformat()
        if not tpex_rows:
            attempts += 1
            rows, msg = fetch_tpex_daily_snapshot(d, timeout=timeout)
            messages.append(msg)
            if rows:
                tpex_rows = rows
                tpex_date = d.isoformat()
        if twse_rows and tpex_rows:
            break

    merged = dict(twse_rows)
    merged.update(tpex_rows)
    diag = {
        "version": SERVICE_VERSION,
        "as_of": end.isoformat(),
        "twse_date": twse_date,
        "tpex_date": tpex_date,
        "twse_count": len(twse_rows),
        "tpex_count": len(tpex_rows),
        "total_count": len(merged),
        "http_attempts": attempts,
        "messages": messages[-12:],
    }
    return merged, diag
