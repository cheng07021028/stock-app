# -*- coding: utf-8 -*-
"""V108B 官方因子快取服務

目的：
- 把法人、營收、PER/EPS 類資料集中在獨立服務層，不直接塞進 07 股神推薦。
- 07 後續只需要讀 official_factors_cache.json，不需要即時連官方網站，避免拖慢推薦頁。
- 官方來源失敗時不拋例外中斷頁面，保留既有快取並回傳診斷訊息。

設計原則：
- best-effort 抓取；資料源格式異動時只影響本服務，不影響 07/10/8/14 主線。
- 不把缺資料當成 0 分，改用「資料完整度」與「官方因子資料狀態」標示。
"""
from __future__ import annotations

import base64
import datetime as dt
import json
import math
import os
import re
import time
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import requests

try:
    import certifi
except Exception:  # pragma: no cover
    certifi = None  # type: ignore

try:
    import urllib3
except Exception:  # pragma: no cover
    urllib3 = None  # type: ignore

try:
    import streamlit as st
except Exception:  # pragma: no cover
    st = None  # type: ignore

BASE_DIR = Path(__file__).resolve().parent
CACHE_FILE = BASE_DIR / "official_factors_cache.json"
LOG_FILE = BASE_DIR / "official_factors_update_log.json"
CACHE_VERSION = "v110_bounded_update_20260805"
REQUEST_TIMEOUT = 5
DEFAULT_RUN_TIMEOUT_SECONDS = 75
DEFAULT_RUN_REQUEST_BUDGET = 48

class OfficialFactorBudgetExceeded(RuntimeError):
    """Raised when the bounded one-click update reaches its time/request budget."""

_RUN_DEADLINE_MONOTONIC = 0.0
_RUN_REQUEST_BUDGET = 0
_RUN_REQUEST_COUNT = 0
_RUN_TIMED_OUT = False
_RUN_STARTED_MONOTONIC = 0.0

def _begin_run_budget(max_seconds: int | float | None, max_requests: int | None) -> None:
    global _RUN_DEADLINE_MONOTONIC, _RUN_REQUEST_BUDGET, _RUN_REQUEST_COUNT, _RUN_TIMED_OUT, _RUN_STARTED_MONOTONIC
    _RUN_STARTED_MONOTONIC = time.monotonic()
    seconds = float(max_seconds or 0)
    _RUN_DEADLINE_MONOTONIC = _RUN_STARTED_MONOTONIC + max(0.0, seconds) if seconds > 0 else 0.0
    _RUN_REQUEST_BUDGET = max(0, int(max_requests or 0))
    _RUN_REQUEST_COUNT = 0
    _RUN_TIMED_OUT = False

def _end_run_budget() -> dict[str, Any]:
    global _RUN_DEADLINE_MONOTONIC, _RUN_REQUEST_BUDGET, _RUN_REQUEST_COUNT, _RUN_TIMED_OUT, _RUN_STARTED_MONOTONIC
    elapsed = max(0.0, time.monotonic() - _RUN_STARTED_MONOTONIC) if _RUN_STARTED_MONOTONIC else 0.0
    status = {
        "elapsed_seconds": round(elapsed, 2),
        "request_count": int(_RUN_REQUEST_COUNT),
        "request_budget": int(_RUN_REQUEST_BUDGET),
        "timed_out": bool(_RUN_TIMED_OUT),
    }
    _RUN_DEADLINE_MONOTONIC = 0.0
    _RUN_REQUEST_BUDGET = 0
    _RUN_REQUEST_COUNT = 0
    _RUN_TIMED_OUT = False
    _RUN_STARTED_MONOTONIC = 0.0
    return status

def _remaining_run_seconds() -> float | None:
    if _RUN_DEADLINE_MONOTONIC <= 0:
        return None
    return max(0.0, _RUN_DEADLINE_MONOTONIC - time.monotonic())

def _budget_guard(label: str = "官方因子更新") -> None:
    global _RUN_TIMED_OUT
    remaining = _remaining_run_seconds()
    if remaining is not None and remaining <= 0:
        _RUN_TIMED_OUT = True
        raise OfficialFactorBudgetExceeded(f"{label}已達時間上限")
    if _RUN_REQUEST_BUDGET > 0 and _RUN_REQUEST_COUNT >= _RUN_REQUEST_BUDGET:
        _RUN_TIMED_OUT = True
        raise OfficialFactorBudgetExceeded(f"{label}已達請求上限 {_RUN_REQUEST_BUDGET}")

def _consume_request(label: str = "官方資料請求") -> float:
    global _RUN_REQUEST_COUNT
    _budget_guard(label)
    _RUN_REQUEST_COUNT += 1
    remaining = _remaining_run_seconds()
    if remaining is None:
        return float(REQUEST_TIMEOUT)
    return max(0.8, min(float(REQUEST_TIMEOUT), remaining))
USER_AGENT = "Mozilla/5.0 (SPT-Godpick-V109; official-factor-cache)"
FINMIND_API_URL = "https://api.finmindtrade.com/api/v4/data"

FACTOR_COLUMNS = [
    "股票代號",
    "股票名稱",
    "市場別",
    "正式產業別",
    "官方資料日期",
    "外資近1日買賣超",
    "外資近3日買賣超",
    "外資近5日買賣超",
    "投信近1日買賣超",
    "投信近3日買賣超",
    "投信近5日買賣超",
    "自營商近1日買賣超",
    "自營商近3日買賣超",
    "自營商近5日買賣超",
    "三大法人近1日合計",
    "三大法人近3日合計",
    "三大法人近5日合計",
    "法人連買天數",
    "法人籌碼官方分數",
    "當月營收",
    "月營收MoM%",
    "月營收YoY%",
    "累計營收YoY%",
    "營收年月",
    "營收成長官方分數",
    "PER本益比",
    "PBR股價淨值比",
    "股利殖利率%",
    "估算EPS",
    "官方估值風險分數",
    "官方基本面成長分數",
    "官方因子總分",
    "官方資料完整度",
    "官方因子資料狀態",
    "官方因子更新時間",
    "官方因子資料源",
    "因子主要來源",
    "因子備援來源",
    "因子來源可信度",
    "備援補值欄位數",
    "FinMind資料日期",
]

# TWSE official/public endpoints used as best-effort sources. Some datasets may be rate-limited
# or delayed. The service always falls back to existing cache.
TWSE_BWIBBU_ALL = "https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_ALL"
TWSE_MONTHLY_REVENUE_L = "https://openapi.twse.com.tw/v1/opendata/t187ap05_L"
TPEX_MONTHLY_REVENUE_O = "https://openapi.twse.com.tw/v1/opendata/t187ap05_O"
TWSE_T86 = "https://www.twse.com.tw/rwd/zh/fund/T86"
TWSE_T86_OLD = "https://www.twse.com.tw/fund/T86"
TPEX_3ITRADE = "https://www.tpex.org.tw/www/zh-tw/afterTrading/3itrade"
TPEX_PERATIO = "https://www.tpex.org.tw/www/zh-tw/afterTrading/peratio"
MOPS_REVENUE_HTML = "https://mops.twse.com.tw/nas/t21/{market}/t21sc03_{roc_year}_{month}_0.html"

# V108A: collect concise data-source diagnostics instead of printing repeated SSL tracebacks.
_REQUEST_NOTES: list[str] = []


def _note_once(msg: str) -> None:
    if msg and msg not in _REQUEST_NOTES:
        _REQUEST_NOTES.append(msg)


def _is_twse_public_url(url: str) -> bool:
    return any(host in str(url).lower() for host in [
        "openapi.twse.com.tw",
        "www.twse.com.tw",
        "www.tpex.org.tw",
        "mops.twse.com.tw",
    ])


def _now_text() -> str:
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _today_yyyymmdd() -> str:
    return dt.datetime.now().strftime("%Y%m%d")


def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    if isinstance(v, (list, tuple, set)):
        return "、".join(_safe_str(x) for x in v if _safe_str(x))
    if isinstance(v, dict):
        try:
            return json.dumps(v, ensure_ascii=False)
        except Exception:
            return str(v)
    return str(v).strip()


def _normalize_code(v: Any) -> str:
    s = _safe_str(v)
    m = re.search(r"(\d{4})", s)
    return m.group(1) if m else ""


def _to_float(v: Any, default: float | None = None) -> float | None:
    if v is None:
        return default
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        try:
            if math.isnan(float(v)):
                return default
        except Exception:
            pass
        return float(v)
    s = _safe_str(v)
    if not s or s in {"--", "-", "NA", "N/A", "nan", "None", "除權息"}:
        return default
    s = s.replace(",", "").replace("%", "").replace("＋", "+").replace("－", "-")
    s = re.sub(r"[^0-9.\-+]", "", s)
    try:
        return float(s)
    except Exception:
        return default


def _to_int(v: Any, default: int = 0) -> int:
    f = _to_float(v, None)
    if f is None:
        return default
    try:
        return int(round(f))
    except Exception:
        return default


def _json_safe(obj: Any) -> Any:
    if obj is None:
        return None
    try:
        if pd.isna(obj):
            return None
    except Exception:
        pass
    if isinstance(obj, (dt.datetime, dt.date)):
        return obj.isoformat()
    if isinstance(obj, pd.DataFrame):
        return [_json_safe(r) for r in obj.to_dict(orient="records")]
    if isinstance(obj, pd.Series):
        return {str(k): _json_safe(v) for k, v in obj.to_dict().items()}
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_json_safe(v) for v in obj]
    if hasattr(obj, "item"):
        try:
            return _json_safe(obj.item())
        except Exception:
            pass
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, (str, int, bool)):
        return obj
    return _safe_str(obj)


def _response_to_json(resp: requests.Response) -> Any:
    """Parse official response as JSON and fail with useful diagnostics.

    Some TWSE/TPEX endpoints can return HTTP 200 with an empty body, HTML gateway
    text, or text/plain JSON.  V108A counted the SSL fallback as success before
    parse, so users saw "SSL fallback success" followed by "Expecting value".
    V108B only treats a source as usable after the body is non-empty and JSON can
    actually be parsed.
    """
    text = (resp.text or "").strip("\ufeff \n\r\t")
    if not text:
        raise RuntimeError(f"官方回傳空內容 HTTP {getattr(resp, 'status_code', '')}")
    try:
        return resp.json()
    except Exception:
        pass
    if text.startswith("<"):
        title = re.search(r"<title[^>]*>(.*?)</title>", text, flags=re.I | re.S)
        brief = re.sub(r"\s+", " ", title.group(1)).strip() if title else text[:80]
        raise RuntimeError(f"官方回傳 HTML，非 JSON：{brief}")
    try:
        return json.loads(text)
    except Exception as exc:
        snippet = re.sub(r"\s+", " ", text[:120]).strip()
        raise RuntimeError(f"JSON 解析失敗：{exc}; 內容片段={snippet}")


def _response_to_text(resp: requests.Response) -> str:
    if not getattr(resp, "encoding", None):
        try:
            resp.encoding = resp.apparent_encoding or "utf-8"
        except Exception:
            resp.encoding = "utf-8"
    text = (resp.text or "").strip("\ufeff \n\r\t")
    if not text:
        raise RuntimeError(f"官方回傳空內容 HTTP {getattr(resp, 'status_code', '')}")
    return text


def _compact_error(exc: Exception) -> str:
    text = str(exc)
    text = re.sub(r"\s+", " ", text).strip()
    text = text.replace("HTTPSConnectionPool", "HTTPS")
    if len(text) > 240:
        text = text[:240] + "..."
    return text


def _request_with_fallback(url: str, params: dict[str, Any] | None = None) -> tuple[requests.Response, str]:
    """Bounded HTTP request. Certificate fallback is only used for SSL errors.

    The prior implementation retried every timeout up to three times. Combined with
    seven institutional dates and FinMind per-code fallback, one click could run for
    tens of minutes. This version consumes a shared time/request budget and avoids
    certificate retries for ordinary timeouts or connection failures.
    """
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/json,text/plain,text/html,*/*",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    last_exc: Exception | None = None
    attempts: list[tuple[str, Any]] = [("SSL正常", True)]
    index = 0
    while index < len(attempts):
        mode, verify_arg = attempts[index]
        index += 1
        try:
            if verify_arg is False and urllib3 is not None:
                try:
                    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                except Exception:
                    pass
            timeout_value = _consume_request(f"{url.split('?')[0]} 請求")
            r = requests.get(url, params=params or {}, headers=headers, timeout=timeout_value, verify=verify_arg)
            r.raise_for_status()
            return r, mode
        except OfficialFactorBudgetExceeded:
            raise
        except Exception as exc:
            last_exc = exc
            # Only certificate failures benefit from certifi / verify=False.
            if isinstance(exc, requests.exceptions.SSLError) and index == 1:
                if certifi is not None:
                    attempts.append(("certifi憑證", certifi.where()))
                if _is_twse_public_url(url):
                    attempts.append(("SSL備援", False))
            continue
    raise RuntimeError(_compact_error(last_exc or Exception("unknown request error")))


def _get_json(url: str, params: dict[str, Any] | None = None) -> Any:
    """Fetch JSON with certificate and body-parse fallback for TWSE public endpoints."""
    try:
        r, mode = _request_with_fallback(url, params=params)
        data = _response_to_json(r)
        if mode != "SSL正常":
            _note_once(f"{mode}成功且JSON可解析：{url.split('?')[0]}")
        return data
    except Exception as exc:
        raise RuntimeError(_compact_error(exc))


def _get_text(url: str, params: dict[str, Any] | None = None) -> str:
    try:
        r, mode = _request_with_fallback(url, params=params)
        text = _response_to_text(r)
        if mode != "SSL正常":
            _note_once(f"{mode}成功且內容可讀：{url.split('?')[0]}")
        return text
    except Exception as exc:
        raise RuntimeError(_compact_error(exc))


def _extract_first(row: dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if key in row:
            return row.get(key)
    lower_map = {str(k).strip().lower(): k for k in row.keys()}
    for key in keys:
        lk = key.strip().lower()
        if lk in lower_map:
            return row.get(lower_map[lk])
    return None


def _empty_factor_df() -> pd.DataFrame:
    return pd.DataFrame(columns=FACTOR_COLUMNS)


def load_factor_cache() -> dict[str, Any]:
    if not CACHE_FILE.exists():
        return {
            "version": CACHE_VERSION,
            "updated_at": "",
            "records": [],
            "diagnostics": ["尚未建立官方因子快取。"],
        }
    try:
        data = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return {"version": CACHE_VERSION, "updated_at": "", "records": data, "diagnostics": []}
        if isinstance(data, dict):
            data.setdefault("records", [])
            data.setdefault("diagnostics", [])
            return data
    except Exception as exc:
        return {
            "version": CACHE_VERSION,
            "updated_at": "",
            "records": [],
            "diagnostics": [f"讀取 official_factors_cache.json 失敗：{exc}"],
        }
    return {"version": CACHE_VERSION, "updated_at": "", "records": [], "diagnostics": ["快取格式不明。"]}


def _summarize_diagnostics(diagnostics: list[str] | None, max_items: int = 40) -> list[str]:
    out: list[str] = []
    for msg in diagnostics or []:
        text = re.sub(r"\s+", " ", _safe_str(msg)).strip()
        text = text.replace("HTTPSConnectionPool", "HTTPS")
        text = text.replace("Max retries exceeded with url:", "連線重試失敗:")
        if len(text) > 360:
            text = text[:360] + "..."
        if text and text not in out:
            out.append(text)
    return out[-max_items:]


def _complete_count_from_records(records: list[dict[str, Any]]) -> int:
    cnt = 0
    for r in records or []:
        try:
            if _to_float(r.get("官方資料完整度"), 0) >= 60:
                cnt += 1
        except Exception:
            pass
    return cnt


def _existing_complete_count() -> int:
    cache = load_factor_cache()
    records = cache.get("records", [])
    return _complete_count_from_records(records if isinstance(records, list) else [])


def save_factor_cache(records: list[dict[str, Any]], diagnostics: list[str] | None = None, meta: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = {
        "version": CACHE_VERSION,
        "updated_at": _now_text(),
        "record_count": len(records),
        "records": _json_safe(records),
        "diagnostics": _summarize_diagnostics(diagnostics or []),
        "meta": _json_safe(meta or {}),
    }
    CACHE_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _append_log("success", len(records), _summarize_diagnostics(diagnostics or []))
    return payload


def _append_log(status: str, row_count: int, diagnostics: list[str] | None = None) -> None:
    item = {"time": _now_text(), "status": status, "row_count": row_count, "diagnostics": diagnostics or []}
    old: list[dict[str, Any]] = []
    try:
        if LOG_FILE.exists():
            raw = json.loads(LOG_FILE.read_text(encoding="utf-8"))
            if isinstance(raw, list):
                old = raw
    except Exception:
        old = []
    old.insert(0, item)
    try:
        LOG_FILE.write_text(json.dumps(old[:200], ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def load_factor_frame() -> pd.DataFrame:
    cache = load_factor_cache()
    records = cache.get("records", [])
    if not isinstance(records, list) or not records:
        return _empty_factor_df()
    df = pd.DataFrame(records)
    for c in FACTOR_COLUMNS:
        if c not in df.columns:
            df[c] = ""
    return df[FACTOR_COLUMNS + [c for c in df.columns if c not in FACTOR_COLUMNS]].copy()


def load_update_logs() -> list[dict[str, Any]]:
    try:
        if LOG_FILE.exists():
            raw = json.loads(LOG_FILE.read_text(encoding="utf-8"))
            return raw if isinstance(raw, list) else []
    except Exception:
        pass
    return []


def cache_status() -> dict[str, Any]:
    cache = load_factor_cache()
    file_exists = CACHE_FILE.exists()
    size_kb = round(CACHE_FILE.stat().st_size / 1024, 1) if file_exists else 0.0
    df = load_factor_frame()
    complete = 0
    if not df.empty and "官方資料完整度" in df.columns:
        vals = pd.to_numeric(df["官方資料完整度"], errors="coerce")
        complete = int((vals >= 60).sum())
    return {
        "exists": file_exists,
        "path": str(CACHE_FILE),
        "size_kb": size_kb,
        "updated_at": _safe_str(cache.get("updated_at")),
        "record_count": int(len(df)) if df is not None else 0,
        "complete_count": complete,
        "diagnostics": cache.get("diagnostics", []),
    }


def _load_stock_master_fallback() -> pd.DataFrame:
    """Load stock master without requiring Streamlit, useful for tests and cache-only mode."""
    cache_path = BASE_DIR / "stock_master_cache.json"
    if not cache_path.exists():
        return pd.DataFrame()
    try:
        raw = json.loads(cache_path.read_text(encoding="utf-8"))
        if isinstance(raw, list):
            return pd.DataFrame(raw)
        if isinstance(raw, dict):
            for key in ["records", "data", "stocks", "items"]:
                val = raw.get(key)
                if isinstance(val, list):
                    return pd.DataFrame(val)
    except Exception:
        return pd.DataFrame()
    return pd.DataFrame()


def load_stock_universe(limit: int | None = None, market_filter: str = "全部") -> pd.DataFrame:
    try:
        from stock_master_service import load_stock_master
        master = load_stock_master()
    except Exception:
        master = _load_stock_master_fallback()
    if master is None or master.empty:
        master = _load_stock_master_fallback()
    if master is None or master.empty:
        return pd.DataFrame(columns=["股票代號", "股票名稱", "市場別", "正式產業別"])

    rename: dict[str, str] = {}
    if "code" in master.columns and "股票代號" not in master.columns:
        rename["code"] = "股票代號"
    if "name" in master.columns and "股票名稱" not in master.columns:
        rename["name"] = "股票名稱"
    if "market" in master.columns and "市場別" not in master.columns:
        rename["market"] = "市場別"
    if "正式產業別" not in master.columns:
        if "official_industry" in master.columns:
            rename["official_industry"] = "正式產業別"
        elif "category" in master.columns:
            rename["category"] = "正式產業別"
    df = master.rename(columns=rename).copy()
    for c in ["股票代號", "股票名稱", "市場別", "正式產業別"]:
        if c not in df.columns:
            df[c] = ""
    if "category" in df.columns:
        mask_empty_industry = df["正式產業別"].astype(str).str.strip().eq("")
        df.loc[mask_empty_industry, "正式產業別"] = df.loc[mask_empty_industry, "category"].astype(str)
    df["股票代號"] = df["股票代號"].map(_normalize_code)
    df = df[df["股票代號"].astype(str).str.len().eq(4)].drop_duplicates("股票代號")
    if market_filter and market_filter != "全部" and "市場別" in df.columns:
        df = df[df["市場別"].astype(str).str.contains(market_filter, na=False)]
    df = df.sort_values("股票代號")
    if limit and limit > 0:
        df = df.head(limit)
    return df[["股票代號", "股票名稱", "市場別", "正式產業別"]].reset_index(drop=True)


def fetch_twse_bwibbu_all() -> tuple[pd.DataFrame, str]:
    """上市 PER/PBR/殖利率。"""
    try:
        data = _get_json(TWSE_BWIBBU_ALL)
        if not isinstance(data, list):
            return pd.DataFrame(), "TWSE BWIBBU 回傳格式非 list。"
        rows = []
        for r in data:
            if not isinstance(r, dict):
                continue
            code = _normalize_code(_extract_first(r, ["Code", "證券代號", "股票代號", "代號"]))
            if not code:
                continue
            close = _to_float(_extract_first(r, ["ClosingPrice", "收盤價", "Close", "收盤價(元)"]))
            pe = _to_float(_extract_first(r, ["PEratio", "本益比", "P/E ratio", "PE Ratio"]))
            eps = None
            if close is not None and pe and pe > 0:
                eps = round(close / pe, 4)
            rows.append({
                "股票代號": code,
                "PER本益比": pe,
                "PBR股價淨值比": _to_float(_extract_first(r, ["PBratio", "股價淨值比", "P/B ratio", "PB Ratio"])),
                "股利殖利率%": _to_float(_extract_first(r, ["DividendYield", "殖利率(%)", "殖利率", "Dividend yield"])),
                "估算EPS": eps,
                "估值資料源": "TWSE_BWIBBU_ALL",
            })
        return pd.DataFrame(rows), f"TWSE PER/PBR/殖利率取得 {len(rows)} 筆。"
    except Exception as exc:
        return pd.DataFrame(), f"TWSE PER/PBR 取得失敗：{exc}"


def _recent_revenue_months(n: int = 4) -> list[tuple[int, int]]:
    today = dt.date.today().replace(day=1)
    out: list[tuple[int, int]] = []
    cur_year = today.year
    cur_month = today.month - 1
    if cur_month <= 0:
        cur_year -= 1
        cur_month = 12
    for _ in range(n):
        out.append((cur_year - 1911, cur_month))
        cur_month -= 1
        if cur_month <= 0:
            cur_year -= 1
            cur_month = 12
    return out


def _fetch_mops_monthly_revenue_html() -> tuple[pd.DataFrame, str]:
    """Fallback parser for MOPS monthly revenue HTML.

    OpenAPI may return an empty body on some Streamlit Cloud routes.  This fallback
    reads official MOPS HTML tables for the latest few months and extracts a
    conservative subset.
    """
    msgs: list[str] = []
    out: list[dict[str, Any]] = []
    market_map = [("上市", "sii"), ("上櫃", "otc")]
    for roc_year, month in _recent_revenue_months(4):
        got_any = False
        for market_name, market_key in market_map:
            url = MOPS_REVENUE_HTML.format(market=market_key, roc_year=roc_year, month=month)
            try:
                html = _get_text(url)
                tables = pd.read_html(html)
                cnt = 0
                for tb in tables:
                    if tb is None or tb.empty:
                        continue
                    flat_cols = []
                    for c in tb.columns:
                        if isinstance(c, tuple):
                            flat_cols.append("_".join(_safe_str(x) for x in c if _safe_str(x)))
                        else:
                            flat_cols.append(_safe_str(c))
                    tb = tb.copy()
                    tb.columns = flat_cols
                    code_col = next((c for c in tb.columns if "公司代號" in c or "代號" == c), "")
                    if not code_col:
                        continue
                    for _, r in tb.iterrows():
                        code = _normalize_code(r.get(code_col, ""))
                        if not code:
                            continue
                        row = {str(k): r.get(k) for k in tb.columns}
                        revenue = _extract_first(row, ["當月營收", "營業收入_當月營收", "營業收入-當月營收", "本月營收"])
                        mom = _extract_first(row, ["上月比較增減(%)", "上月比較增減％", "上月比較增減", "營收月增率"])
                        yoy = _extract_first(row, ["去年同月增減(%)", "去年同月增減％", "去年同月增減", "營收年增率"])
                        acc_yoy = _extract_first(row, ["前期比較增減(%)", "前期比較增減％", "累計營收年增率", "累計增減(%)"])
                        out.append({
                            "股票代號": code,
                            "當月營收": _to_float(revenue),
                            "月營收MoM%": _to_float(mom),
                            "月營收YoY%": _to_float(yoy),
                            "累計營收YoY%": _to_float(acc_yoy),
                            "營收年月": f"{roc_year + 1911}{month:02d}",
                            "營收資料源": f"MOPS_HTML_{market_name}",
                        })
                        cnt += 1
                if cnt:
                    got_any = True
                    msgs.append(f"MOPS HTML {market_name} {roc_year}/{month} 取得 {cnt} 筆。")
            except Exception as exc:
                msgs.append(f"MOPS HTML {market_name} {roc_year}/{month} 失敗：{exc}")
        if got_any:
            break
    if not out:
        return pd.DataFrame(), " / ".join(msgs[-8:])
    return pd.DataFrame(out).drop_duplicates("股票代號", keep="first"), " / ".join(msgs[-8:])


def fetch_monthly_revenue() -> tuple[pd.DataFrame, str]:
    """上市/上櫃月營收，使用 TWSE OpenAPI MOPS opendata 類資料。"""
    endpoints = [("上市", TWSE_MONTHLY_REVENUE_L), ("上櫃", TPEX_MONTHLY_REVENUE_O)]
    out: list[dict[str, Any]] = []
    msgs: list[str] = []
    for market, url in endpoints:
        try:
            data = _get_json(url)
            if not isinstance(data, list):
                msgs.append(f"{market}月營收回傳格式非 list。")
                continue
            cnt = 0
            for r in data:
                if not isinstance(r, dict):
                    continue
                code = _normalize_code(_extract_first(r, ["公司代號", "Code", "股票代號", "出表公司代號"]))
                if not code:
                    continue
                yoy = _to_float(_extract_first(r, ["去年同月增減(%)", "去年同月增減％", "去年同月增減百分比", "YoY", "營收年增率"]))
                mom = _to_float(_extract_first(r, ["上月比較增減(%)", "上月比較增減％", "MoM", "營收月增率"]))
                acc_yoy = _to_float(_extract_first(r, ["前期比較增減(%)", "前期比較增減％", "累計營收年增率", "累計增減(%)"]))
                year_month = _safe_str(_extract_first(r, ["資料年月", "出表日期", "營收年月", "年月", "YearMonth"]))
                out.append({
                    "股票代號": code,
                    "當月營收": _to_float(_extract_first(r, ["當月營收", "營業收入-當月營收", "本月營收"])),
                    "月營收MoM%": mom,
                    "月營收YoY%": yoy,
                    "累計營收YoY%": acc_yoy,
                    "營收年月": year_month,
                    "營收資料源": f"OpenAPI_{market}_monthly_revenue",
                })
                cnt += 1
            msgs.append(f"{market}月營收取得 {cnt} 筆。")
        except Exception as exc:
            msgs.append(f"{market}月營收取得失敗：{exc}")
    # A partial OpenAPI success must not suppress the MOPS fallback for the
    # failed market. Previously listed data succeeded, OTC failed, and the code
    # skipped fallback entirely; all OTC stocks therefore remained uncovered.
    failed_market = any("失敗" in msg or "0 筆" in msg or "格式非 list" in msg for msg in msgs)
    if not out or failed_market:
        fb_df, fb_msg = _fetch_mops_monthly_revenue_html()
        if fb_df is not None and not fb_df.empty:
            existing = pd.DataFrame(out) if out else pd.DataFrame()
            combined = pd.concat([existing, fb_df], ignore_index=True, sort=False)
            combined = combined.drop_duplicates("股票代號", keep="first")
            return combined, " / ".join(msgs + ["月營收缺漏市場改用 MOPS HTML 備援。", fb_msg])
        if not out:
            return pd.DataFrame(), " / ".join(msgs + ([fb_msg] if fb_msg else []))
        msgs.append("MOPS HTML 備援未取得額外資料。" + (fb_msg or ""))
    df = pd.DataFrame(out).drop_duplicates("股票代號", keep="first")
    return df, " / ".join(msgs)


def _recent_weekdays(days: int = 10) -> list[str]:
    today = dt.date.today()
    out = []
    i = 0
    while len(out) < days and i < days * 3:
        d = today - dt.timedelta(days=i)
        if d.weekday() < 5:
            out.append(d.strftime("%Y%m%d"))
        i += 1
    return out


def fetch_twse_institutional(days: int = 7) -> tuple[pd.DataFrame, str]:
    """上市三大法人買賣超。

    注意：T86 個股明細可能受官方資料政策、交易日、時間與格式影響。
    若取得失敗，本服務只回傳空表與診斷，不阻斷頁面。
    """
    records_by_code: dict[str, list[dict[str, Any]]] = {}
    msgs: list[str] = []
    for date_text in _recent_weekdays(max(days, 3)):
        try:
            _budget_guard("TWSE 法人更新")
            params = {"date": date_text, "selectType": "ALLBUT0999", "response": "json", "_": int(time.time() * 1000)}
            data = None
            last_t86_error = ""
            for endpoint in [TWSE_T86, TWSE_T86_OLD]:
                try:
                    data = _get_json(endpoint, params=params)
                    break
                except Exception as exc:
                    last_t86_error = _compact_error(exc)
                    data = None
            fields = data.get("fields") if isinstance(data, dict) else None
            rows = data.get("data") if isinstance(data, dict) else None
            if not fields or not rows:
                msgs.append(f"{date_text} T86 無資料或格式不可用。{last_t86_error}")
                continue
            field_map = {str(f).strip(): i for i, f in enumerate(fields)}

            def pick(row: list[Any], names: list[str]) -> Any:
                for n in names:
                    if n in field_map and field_map[n] < len(row):
                        return row[field_map[n]]
                return None

            cnt = 0
            for row in rows:
                if not isinstance(row, list):
                    continue
                code = _normalize_code(pick(row, ["證券代號", "股票代號", "代號"]))
                if not code:
                    continue
                foreign = _to_int(pick(row, ["外陸資買賣超股數(不含外資自營商)", "外資及陸資買賣超股數", "外資買賣超股數"]))
                trust = _to_int(pick(row, ["投信買賣超股數"]))
                dealer = _to_int(pick(row, ["自營商買賣超股數", "自營商買賣超股數(自行買賣)"]))
                total = _to_int(pick(row, ["三大法人買賣超股數", "合計買賣超股數"]))
                if total == 0:
                    total = foreign + trust + dealer
                records_by_code.setdefault(code, []).append({
                    "date": date_text,
                    "foreign": foreign,
                    "trust": trust,
                    "dealer": dealer,
                    "total": total,
                })
                cnt += 1
            msgs.append(f"{date_text} T86 取得 {cnt} 筆。")
            time.sleep(0.15)
        except OfficialFactorBudgetExceeded as exc:
            msgs.append(str(exc))
            break
        except Exception as exc:
            msgs.append(f"{date_text} T86 取得失敗：{exc}")

    out = []
    for code, items in records_by_code.items():
        items = sorted(items, key=lambda x: x.get("date", ""), reverse=True)
        one = items[:1]
        three = items[:3]
        five = items[:5]
        def s(key: str, arr: list[dict[str, Any]]) -> int:
            return int(sum(_to_int(x.get(key)) for x in arr))
        consecutive = 0
        for item in items:
            if _to_int(item.get("total")) > 0:
                consecutive += 1
            else:
                break
        out.append({
            "股票代號": code,
            "官方資料日期": items[0].get("date", "") if items else "",
            "外資近1日買賣超": s("foreign", one),
            "外資近3日買賣超": s("foreign", three),
            "外資近5日買賣超": s("foreign", five),
            "投信近1日買賣超": s("trust", one),
            "投信近3日買賣超": s("trust", three),
            "投信近5日買賣超": s("trust", five),
            "自營商近1日買賣超": s("dealer", one),
            "自營商近3日買賣超": s("dealer", three),
            "自營商近5日買賣超": s("dealer", five),
            "三大法人近1日合計": s("total", one),
            "三大法人近3日合計": s("total", three),
            "三大法人近5日合計": s("total", five),
            "法人連買天數": consecutive,
            "法人資料源": "TWSE_T86",
        })
    return pd.DataFrame(out), " / ".join(msgs)



def _tpex_tables(data: Any) -> list[tuple[list[str], list[list[Any]]]]:
    """Extract field/data tables from current and legacy TPEX JSON shapes."""
    tables: list[tuple[list[str], list[list[Any]]]] = []
    candidates: list[Any] = []
    if isinstance(data, dict):
        candidates.extend(data.get("tables", []) if isinstance(data.get("tables"), list) else [])
        candidates.append(data)
    elif isinstance(data, list):
        candidates.extend(data)
    for item in candidates:
        if not isinstance(item, dict):
            continue
        fields = item.get("fields") or item.get("columns") or item.get("titles")
        rows = item.get("data") or item.get("rows")
        if isinstance(fields, list) and isinstance(rows, list):
            tables.append(([str(x).strip() for x in fields], rows))
    return tables


def fetch_tpex_institutional(days: int = 7) -> tuple[pd.DataFrame, str]:
    """上櫃三大法人買賣超（櫃買中心官方 JSON，best effort）。"""
    records_by_code: dict[str, list[dict[str, Any]]] = {}
    msgs: list[str] = []
    for date_text in _recent_weekdays(max(days, 3)):
        try:
            _budget_guard("TPEx 法人更新")
        except OfficialFactorBudgetExceeded as exc:
            msgs.append(str(exc))
            break
        date_slash = f"{date_text[:4]}/{date_text[4:6]}/{date_text[6:8]}"
        try:
            data = _get_json(TPEX_3ITRADE, params={"date": date_slash, "type": "Daily", "response": "json"})
            cnt = 0
            for fields, rows in _tpex_tables(data):
                fmap = {str(f).replace(" ", "").strip(): i for i, f in enumerate(fields)}
                def pick(row: list[Any], names: list[str]) -> Any:
                    for n in names:
                        nn = n.replace(" ", "")
                        if nn in fmap and fmap[nn] < len(row):
                            return row[fmap[nn]]
                    return None
                for row in rows:
                    if not isinstance(row, list):
                        continue
                    code = _normalize_code(pick(row, ["代號", "證券代號", "股票代號"]))
                    if not code:
                        continue
                    foreign = _to_int(pick(row, ["外資及陸資(不含外資自營商)-買賣超股數", "外資及陸資買賣超股數", "外資買賣超股數"]))
                    trust = _to_int(pick(row, ["投信-買賣超股數", "投信買賣超股數"]))
                    dealer = _to_int(pick(row, ["自營商-買賣超股數", "自營商買賣超股數"]))
                    total = _to_int(pick(row, ["三大法人買賣超股數合計", "三大法人買賣超股數", "合計買賣超股數"]))
                    if total == 0:
                        total = foreign + trust + dealer
                    records_by_code.setdefault(code, []).append({
                        "date": date_text, "foreign": foreign, "trust": trust,
                        "dealer": dealer, "total": total,
                    })
                    cnt += 1
            msgs.append(f"{date_text} TPEX 法人取得 {cnt} 筆。")
            time.sleep(0.12)
        except Exception as exc:
            msgs.append(f"{date_text} TPEX 法人取得失敗：{exc}")

    out: list[dict[str, Any]] = []
    for code, items in records_by_code.items():
        items = sorted(items, key=lambda x: x.get("date", ""), reverse=True)
        def total(key: str, n: int) -> int:
            return int(sum(_to_int(x.get(key)) for x in items[:n]))
        consecutive = 0
        for item in items:
            if _to_int(item.get("total")) > 0:
                consecutive += 1
            else:
                break
        out.append({
            "股票代號": code,
            "官方資料日期": items[0].get("date", "") if items else "",
            "外資近1日買賣超": total("foreign", 1), "外資近3日買賣超": total("foreign", 3), "外資近5日買賣超": total("foreign", 5),
            "投信近1日買賣超": total("trust", 1), "投信近3日買賣超": total("trust", 3), "投信近5日買賣超": total("trust", 5),
            "自營商近1日買賣超": total("dealer", 1), "自營商近3日買賣超": total("dealer", 3), "自營商近5日買賣超": total("dealer", 5),
            "三大法人近1日合計": total("total", 1), "三大法人近3日合計": total("total", 3), "三大法人近5日合計": total("total", 5),
            "法人連買天數": consecutive, "法人資料源": "TPEX_3ITRADE",
        })
    return pd.DataFrame(out), " / ".join(msgs)


def fetch_tpex_valuation() -> tuple[pd.DataFrame, str]:
    """上櫃本益比／殖利率／股價淨值比（櫃買中心官方 JSON）。"""
    msgs: list[str] = []
    for date_text in _recent_weekdays(7):
        try:
            _budget_guard("TPEx 估值更新")
        except OfficialFactorBudgetExceeded as exc:
            msgs.append(str(exc))
            break
        date_slash = f"{date_text[:4]}/{date_text[4:6]}/{date_text[6:8]}"
        try:
            data = _get_json(TPEX_PERATIO, params={"date": date_slash, "id": "", "response": "json"})
            out: list[dict[str, Any]] = []
            for fields, rows in _tpex_tables(data):
                fmap = {str(f).replace(" ", "").strip(): i for i, f in enumerate(fields)}
                def pick(row: list[Any], names: list[str]) -> Any:
                    for n in names:
                        nn = n.replace(" ", "")
                        if nn in fmap and fmap[nn] < len(row):
                            return row[fmap[nn]]
                    return None
                for row in rows:
                    if not isinstance(row, list):
                        continue
                    code = _normalize_code(pick(row, ["股票代號", "證券代號", "代號"]))
                    if not code:
                        continue
                    out.append({
                        "股票代號": code,
                        "PER本益比": _to_float(pick(row, ["本益比", "本益比(倍)"])),
                        "PBR股價淨值比": _to_float(pick(row, ["股價淨值比", "股價淨值比(倍)"])),
                        "股利殖利率%": _to_float(pick(row, ["殖利率(%)", "殖利率％", "殖利率"])),
                        "估值資料源": "TPEX_PERATIO",
                    })
            if out:
                return pd.DataFrame(out).drop_duplicates("股票代號"), f"{date_text} TPEX PER/PBR/殖利率取得 {len(out)} 筆。"
            msgs.append(f"{date_text} TPEX 估值無資料。")
        except Exception as exc:
            msgs.append(f"{date_text} TPEX 估值取得失敗：{exc}")
    return pd.DataFrame(), " / ".join(msgs)


def _score_range(value: float | None, strong: float, mid: float, bad: float, reverse_bad: bool = False) -> float:
    if value is None:
        return 50.0
    try:
        v = float(value)
    except Exception:
        return 50.0
    if reverse_bad:
        if v <= strong:
            return 90.0
        if v <= mid:
            return 75.0
        if v <= bad:
            return 58.0
        return 38.0
    if v >= strong:
        return 90.0
    if v >= mid:
        return 75.0
    if v >= bad:
        return 58.0
    return 42.0


def _calc_scores(row: dict[str, Any]) -> dict[str, Any]:
    f5 = _to_float(row.get("外資近5日買賣超"), None)
    t5 = _to_float(row.get("投信近5日買賣超"), None)
    total5 = _to_float(row.get("三大法人近5日合計"), None)
    consec = _to_float(row.get("法人連買天數"), None)
    chip_parts = []
    for v, strong, mid, bad in [(f5, 3000, 500, -1000), (t5, 1000, 200, -300), (total5, 4000, 800, -1200), (consec, 3, 2, 0)]:
        if v is not None:
            chip_parts.append(_score_range(v, strong, mid, bad))
    chip_score = round(sum(chip_parts) / len(chip_parts), 2) if chip_parts else 50.0

    yoy = _to_float(row.get("月營收YoY%"), None)
    mom = _to_float(row.get("月營收MoM%"), None)
    acc_yoy = _to_float(row.get("累計營收YoY%"), None)
    rev_parts = []
    for v, w in [(yoy, 1.0), (mom, 0.6), (acc_yoy, 1.0)]:
        if v is not None:
            rev_parts.append(_score_range(v, 30, 10, -5) * w)
    rev_weight = sum([1.0 if yoy is not None else 0, 0.6 if mom is not None else 0, 1.0 if acc_yoy is not None else 0])
    rev_score = round(sum(rev_parts) / rev_weight, 2) if rev_weight else 50.0

    per = _to_float(row.get("PER本益比"), None)
    eps = _to_float(row.get("估算EPS"), None)
    # PER 合理，不是越低越好：虧損/無 EPS 保守；PER 過高扣分。
    if eps is not None and eps <= 0:
        val_score = 35.0
    elif per is None or per <= 0:
        val_score = 50.0
    elif per <= 12:
        val_score = 82.0
    elif per <= 22:
        val_score = 75.0
    elif per <= 35:
        val_score = 60.0
    elif per <= 60:
        val_score = 48.0
    else:
        val_score = 35.0

    basic_score = round((rev_score * 0.65) + (val_score * 0.35), 2)
    total_score = round((chip_score * 0.35) + (rev_score * 0.35) + (val_score * 0.30), 2)

    complete = 0
    if chip_parts:
        complete += 35
    if rev_weight:
        complete += 40
    if per is not None or eps is not None:
        complete += 25
    if complete >= 80:
        status = "完整"
    elif complete >= 45:
        status = "部分資料"
    elif complete > 0:
        status = "資料不足"
    else:
        status = "未取得官方資料"

    return {
        "法人籌碼官方分數": chip_score,
        "營收成長官方分數": rev_score,
        "官方估值風險分數": round(val_score, 2),
        "官方基本面成長分數": basic_score,
        "官方因子總分": total_score,
        "官方資料完整度": complete,
        "官方因子資料狀態": status,
    }




# =========================================================
# V109 FinMind trusted fallback
# =========================================================

def _finmind_token() -> str:
    """Read FinMind token without ever writing it into cache/log files."""
    token = _safe_str(os.getenv("FINMIND_TOKEN", ""))
    if token:
        return token
    if st is not None:
        try:
            return _safe_str(getattr(st, "secrets", {}).get("FINMIND_TOKEN", ""))
        except Exception:
            return ""
    return ""


def finmind_config_status() -> dict[str, Any]:
    token = _finmind_token()
    return {
        "enabled": bool(token),
        "token_configured": bool(token),
        "api_url": FINMIND_API_URL,
        "rate_limit_note": "有 token 建議每小時 600 次；無 token 約 300 次。",
    }


def _finmind_get(dataset: str, start_date: str, end_date: str, data_id: str = "") -> tuple[list[dict[str, Any]], str]:
    params: dict[str, Any] = {
        "dataset": dataset,
        "start_date": start_date,
        "end_date": end_date,
    }
    if data_id:
        params["data_id"] = data_id
    token = _finmind_token()
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
        params["token"] = token
    try:
        timeout_value = _consume_request(f"FinMind {dataset}")
        r = requests.get(FINMIND_API_URL, params=params, headers=headers, timeout=timeout_value)
        r.raise_for_status()
        payload = _response_to_json(r)
        if not isinstance(payload, dict):
            return [], f"FinMind {dataset} 回傳格式非 dict。"
        status = payload.get("status")
        data = payload.get("data", [])
        if status not in (None, 200) and not data:
            return [], f"FinMind {dataset} 失敗：{_safe_str(payload.get('msg') or payload.get('message') or status)}"
        if not isinstance(data, list):
            return [], f"FinMind {dataset} data 格式非 list。"
        return [x for x in data if isinstance(x, dict)], f"FinMind {dataset} 取得 {len(data)} 筆。"
    except OfficialFactorBudgetExceeded as exc:
        return [], f"FinMind {dataset} 已停止：{exc}"
    except Exception as exc:
        return [], f"FinMind {dataset} 取得失敗：{_compact_error(exc)}"


def _date_range(days: int = 12) -> tuple[str, str]:
    end = dt.date.today()
    start = end - dt.timedelta(days=max(1, int(days)))
    return start.isoformat(), end.isoformat()


def _latest_by_code(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        code = _normalize_code(row.get("stock_id") or row.get("股票代號") or row.get("data_id"))
        if not code:
            continue
        date_text = _safe_str(row.get("date") or row.get("日期") or row.get("create_time"))
        old = out.get(code)
        old_date = _safe_str((old or {}).get("date") or (old or {}).get("日期") or (old or {}).get("create_time"))
        if old is None or date_text >= old_date:
            out[code] = row
    return out


def _finmind_institutional_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        code = _normalize_code(row.get("stock_id"))
        if code:
            grouped.setdefault(code, []).append(row)
    out: list[dict[str, Any]] = []
    for code, items in grouped.items():
        items = sorted(items, key=lambda x: _safe_str(x.get("date")), reverse=True)
        def net(item: dict[str, Any], buy_keys: list[str], sell_keys: list[str]) -> int:
            buy = next((_to_int(item.get(k)) for k in buy_keys if k in item), 0)
            sell = next((_to_int(item.get(k)) for k in sell_keys if k in item), 0)
            return buy - sell
        normalized=[]
        for item in items:
            foreign = net(item, ["Foreign_Investor_buy", "Foreign_Investor_Buy"], ["Foreign_Investor_sell", "Foreign_Investor_Sell"])
            trust = net(item, ["Investment_Trust_buy", "Investment_Trust_Buy"], ["Investment_Trust_sell", "Investment_Trust_Sell"])
            dealer = net(item, ["Dealer_buy", "Dealer_self_buy", "Dealer_Hedging_buy"], ["Dealer_sell", "Dealer_self_sell", "Dealer_Hedging_sell"])
            normalized.append({"date": _safe_str(item.get("date")), "foreign": foreign, "trust": trust, "dealer": dealer, "total": foreign + trust + dealer})
        def total(key: str, n: int) -> int:
            return int(sum(_to_int(x.get(key)) for x in normalized[:n]))
        consecutive=0
        for item in normalized:
            if _to_int(item.get("total")) > 0:
                consecutive += 1
            else:
                break
        out.append({
            "股票代號": code, "FinMind資料日期": normalized[0]["date"] if normalized else "",
            "外資近1日買賣超": total("foreign",1), "外資近3日買賣超": total("foreign",3), "外資近5日買賣超": total("foreign",5),
            "投信近1日買賣超": total("trust",1), "投信近3日買賣超": total("trust",3), "投信近5日買賣超": total("trust",5),
            "自營商近1日買賣超": total("dealer",1), "自營商近3日買賣超": total("dealer",3), "自營商近5日買賣超": total("dealer",5),
            "三大法人近1日合計": total("total",1), "三大法人近3日合計": total("total",3), "三大法人近5日合計": total("total",5),
            "法人連買天數": consecutive, "FinMind法人資料源": "FinMind_TaiwanStockInstitutionalInvestorsBuySellWide",
        })
    return pd.DataFrame(out)


def _finmind_revenue_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    latest = _latest_by_code(rows)
    out=[]
    for code,row in latest.items():
        revenue = _to_float(_extract_first(row,["revenue","Revenue","當月營收"]), None)
        mom = _to_float(_extract_first(row,["revenue_month","revenue_mom","MoM","月營收MoM%"]), None)
        yoy = _to_float(_extract_first(row,["revenue_year","revenue_yoy","YoY","月營收YoY%"]), None)
        acc = _to_float(_extract_first(row,["accumulated_revenue_year","acc_revenue_yoy","累計營收YoY%"]), None)
        month = _safe_str(_extract_first(row,["revenue_month","month","營收年月"]))
        date_text = _safe_str(row.get("date") or row.get("create_time"))
        out.append({"股票代號":code,"當月營收":revenue,"月營收MoM%":mom,"月營收YoY%":yoy,"累計營收YoY%":acc,"營收年月":month,"FinMind資料日期":date_text,"FinMind營收資料源":"FinMind_TaiwanStockMonthRevenue"})
    return pd.DataFrame(out)


def _finmind_valuation_frame(rows: list[dict[str, Any]]) -> pd.DataFrame:
    latest = _latest_by_code(rows)
    out=[]
    for code,row in latest.items():
        per = _to_float(_extract_first(row,["PER","per","本益比"]), None)
        pbr = _to_float(_extract_first(row,["PBR","pbr","股價淨值比"]), None)
        yld = _to_float(_extract_first(row,["dividend_yield","DividendYield","殖利率"]), None)
        eps = _to_float(_extract_first(row,["EPS","eps","估算EPS"]), None)
        date_text = _safe_str(row.get("date"))
        out.append({"股票代號":code,"PER本益比":per,"PBR股價淨值比":pbr,"股利殖利率%":yld,"估算EPS":eps,"FinMind資料日期":date_text,"FinMind估值資料源":"FinMind_TaiwanStockPER"})
    return pd.DataFrame(out)


def fetch_finmind_fallback(
    universe: pd.DataFrame,
    max_stocks: int = 120,
    include_institutional: bool = True,
    include_revenue: bool = True,
    include_valuation: bool = True,
    *,
    bulk_only: bool = False,
    request_budget_override: int | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    """Fetch bounded FinMind fallback data. Bulk is attempted first; per-code calls are bounded.

    FinMind is never treated as exchange-official data. It only fills missing values and
    records source/trust metadata. A token is recommended; without it the function is
    disabled by default to avoid exhausting the anonymous quota.
    """
    diagnostics: list[str] = []
    if not _finmind_token():
        return pd.DataFrame(), ["FinMind 備援未啟用：請在 Streamlit Secrets 設定 FINMIND_TOKEN。"]
    codes = universe.get("股票代號", pd.Series([],dtype=str)).astype(str).map(_normalize_code).dropna().tolist()
    codes = [c for c in codes if c][:max(0,int(max_stocks))]
    if not codes:
        return pd.DataFrame(), ["FinMind 備援沒有可查詢股票代號。"]
    frames: list[pd.DataFrame] = []
    start12,end = _date_range(14)
    start550,_ = _date_range(550)
    datasets=[]
    if include_institutional:
        datasets.append(("TaiwanStockInstitutionalInvestorsBuySellWide",start12,end,_finmind_institutional_frame))
    if include_revenue:
        datasets.append(("TaiwanStockMonthRevenue",start550,end,_finmind_revenue_frame))
    if include_valuation:
        datasets.append(("TaiwanStockPER",start12,end,_finmind_valuation_frame))
    request_budget = int(request_budget_override or min(260, max(30, len(codes) * len(datasets))))
    request_budget = max(3, min(request_budget, 260))
    used=0
    for dataset,start,end_date,parser in datasets:
        bulk_rows,msg = _finmind_get(dataset,start,end_date)
        diagnostics.append(msg)
        used += 1
        parsed = parser(bulk_rows) if bulk_rows else pd.DataFrame()
        if parsed is not None and not parsed.empty:
            parsed = parsed[parsed["股票代號"].isin(codes)]
            if not parsed.empty:
                frames.append(parsed)
                continue
        if bulk_only:
            diagnostics.append(f"FinMind {dataset} 批次未取得可用資料；一鍵快速模式不逐檔查詢，留待排程增量補值。")
            continue
        per_rows=[]
        for code in codes:
            try:
                _budget_guard(f"FinMind {dataset}")
            except OfficialFactorBudgetExceeded as exc:
                diagnostics.append(str(exc))
                break
            if used >= request_budget:
                diagnostics.append(f"FinMind 已達本輪安全請求上限 {request_budget}，剩餘股票留待下次增量補值。")
                break
            rows,msg2 = _finmind_get(dataset,start,end_date,data_id=code)
            used += 1
            if rows:
                per_rows.extend(rows)
            if used % 20 == 0:
                time.sleep(0.15)
        parsed = parser(per_rows) if per_rows else pd.DataFrame()
        if parsed is not None and not parsed.empty:
            frames.append(parsed)
        diagnostics.append(f"FinMind {dataset} 本輪累計請求 {used} 次。")
    if not frames:
        return pd.DataFrame(), diagnostics
    out=frames[0]
    for frame in frames[1:]:
        out=out.merge(frame,on="股票代號",how="outer",suffixes=("","__fmdup"))
        for c in [x for x in out.columns if x.endswith("__fmdup")]:
            base=c[:-7]
            if base in out.columns:
                mask=_is_missing_factor_value(out[base],base)
                out.loc[mask,base]=out.loc[mask,c]
            else:
                out[base]=out[c]
            out=out.drop(columns=[c])
    return out.drop_duplicates("股票代號",keep="first"), diagnostics


def _coalesce_fallback(df: pd.DataFrame, fallback: pd.DataFrame, source_name: str, trust_score: int = 82) -> tuple[pd.DataFrame, int]:
    if fallback is None or fallback.empty or "股票代號" not in fallback.columns:
        return df, 0
    left=df.copy()
    right=fallback.copy()
    right["股票代號"]=right["股票代號"].map(_normalize_code)
    value_cols=[c for c in right.columns if c != "股票代號"]
    temp={c:f"__fallback__{i}" for i,c in enumerate(value_cols)}
    merged=left.merge(right[["股票代號"]+value_cols].rename(columns=temp),on="股票代號",how="left")
    filled_total=0
    filled_by_row=pd.Series(0,index=merged.index,dtype=int)
    data_cols=[c for c in value_cols if not c.endswith("資料源") and c not in {"FinMind資料日期"}]
    for c in data_cols:
        tc=temp[c]
        if c not in merged.columns:
            merged[c]=""
        mask=_is_missing_factor_value(merged[c],c) & ~_is_missing_factor_value(merged[tc],c)
        if mask.any():
            merged.loc[mask,c]=merged.loc[mask,tc]
            filled_by_row.loc[mask]+=1
            filled_total += int(mask.sum())
    if "備援補值欄位數" not in merged.columns:
        merged["備援補值欄位數"]=0
    merged["備援補值欄位數"]=pd.to_numeric(merged["備援補值欄位數"],errors="coerce").fillna(0).astype(int)+filled_by_row
    touched=filled_by_row.gt(0)
    if "因子備援來源" not in merged.columns:
        merged["因子備援來源"]=""
    merged.loc[touched,"因子備援來源"]=source_name
    if "因子來源可信度" not in merged.columns:
        merged["因子來源可信度"]=100
    merged.loc[touched,"因子來源可信度"]=trust_score
    if "FinMind資料日期" in temp:
        tc=temp["FinMind資料日期"]
        if "FinMind資料日期" not in merged.columns:
            merged["FinMind資料日期"]=""
        mask=touched & merged["FinMind資料日期"].astype(str).str.strip().eq("")
        merged.loc[mask,"FinMind資料日期"]=merged.loc[mask,tc]
    merged=merged.drop(columns=list(temp.values()),errors="ignore")
    return merged, filled_total


def build_official_factor_cache(
    limit: int | None = None,
    market_filter: str = "全部",
    include_institutional: bool = True,
    include_revenue: bool = True,
    include_valuation: bool = True,
    save: bool = True,
    enable_finmind_fallback: bool = True,
    finmind_max_stocks: int = 120,
    *,
    quick_mode: bool = False,
    max_runtime_seconds: int = DEFAULT_RUN_TIMEOUT_SECONDS,
    max_requests: int = DEFAULT_RUN_REQUEST_BUDGET,
    finmind_bulk_only: bool | None = None,
    progress_callback: Any = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Build official factor cache with a hard cooperative budget.

    Quick mode is used by page 17 one-click update. It prioritizes current
    exchange/MOPS bulk data, limits institutional lookback to three sessions, and
    never falls into hundreds of FinMind per-stock requests. Partial results are
    merged with the previous valid cache and saved only when quality is safe.
    """
    global _REQUEST_NOTES
    _REQUEST_NOTES = []
    _begin_run_budget(max_runtime_seconds, max_requests)
    diagnostics: list[str] = []
    progress_events: list[dict[str, Any]] = []

    def progress(stage: str, message: str) -> None:
        event = {"stage": stage, "message": message, "elapsed_seconds": round(time.monotonic() - _RUN_STARTED_MONOTONIC, 2)}
        progress_events.append(event)
        if callable(progress_callback):
            try:
                progress_callback(event)
            except Exception:
                pass

    try:
        universe = load_stock_universe(limit=limit, market_filter=market_filter)
        if universe.empty:
            diagnostics.append("股票主檔為空，請先更新 9_股票主檔更新。")
            return _empty_factor_df(), {"ok": False, "diagnostics": diagnostics, "progress_events": progress_events}

        df = universe.copy()
        institutional_days = 3 if quick_mode else 7
        if finmind_bulk_only is None:
            finmind_bulk_only = bool(quick_mode)
        if quick_mode:
            finmind_max_stocks = min(max(0, int(finmind_max_stocks)), 30)

        if include_valuation:
            progress("valuation", "更新上市與上櫃估值")
            try:
                _budget_guard("估值更新")
                val_df, msg = fetch_twse_bwibbu_all()
                diagnostics.append(msg)
                otc_val_df, otc_msg = fetch_tpex_valuation()
                diagnostics.append(otc_msg)
                frames = [x for x in [val_df, otc_val_df] if x is not None and not x.empty]
                if frames:
                    df = df.merge(pd.concat(frames, ignore_index=True, sort=False).drop_duplicates("股票代號", keep="first"), on="股票代號", how="left")
            except OfficialFactorBudgetExceeded as exc:
                diagnostics.append(str(exc))

        if include_revenue:
            progress("revenue", "更新月營收")
            try:
                _budget_guard("月營收更新")
                rev_df, msg = fetch_monthly_revenue()
                diagnostics.append(msg)
                if rev_df is not None and not rev_df.empty:
                    df = df.merge(rev_df, on="股票代號", how="left")
            except OfficialFactorBudgetExceeded as exc:
                diagnostics.append(str(exc))

        if include_institutional:
            progress("institutional", f"更新法人籌碼（近 {institutional_days} 個交易日）")
            try:
                _budget_guard("法人更新")
                inst_df, msg = fetch_twse_institutional(days=institutional_days)
                diagnostics.append(msg)
                otc_inst_df, otc_msg = fetch_tpex_institutional(days=institutional_days)
                diagnostics.append(otc_msg)
                frames = [x for x in [inst_df, otc_inst_df] if x is not None and not x.empty]
                if frames:
                    df = df.merge(pd.concat(frames, ignore_index=True, sort=False).drop_duplicates("股票代號", keep="first"), on="股票代號", how="left")
            except OfficialFactorBudgetExceeded as exc:
                diagnostics.append(str(exc))

        finmind_filled = 0
        if enable_finmind_fallback:
            progress("finmind", "FinMind 缺值備援（快速模式僅批次）")
            try:
                _budget_guard("FinMind 備援")
                fm_df, fm_diag = fetch_finmind_fallback(
                    universe, max_stocks=finmind_max_stocks,
                    include_institutional=include_institutional, include_revenue=include_revenue,
                    include_valuation=include_valuation, bulk_only=bool(finmind_bulk_only),
                    request_budget_override=6 if quick_mode else None,
                )
                diagnostics.extend(fm_diag)
                if fm_df is not None and not fm_df.empty:
                    df, finmind_filled = _coalesce_fallback(df, fm_df, "FinMind", trust_score=82)
                    diagnostics.append(f"FinMind 本輪補值 {finmind_filled} 個欄位；官方原值未被覆蓋。")
            except OfficialFactorBudgetExceeded as exc:
                diagnostics.append(str(exc))

        progress("cache", "合併前次有效快取並計算分數")
        old_df = load_factor_frame()
        old_filled = 0
        if old_df is not None and not old_df.empty:
            df, old_filled = _coalesce_fallback(df, old_df, "前次有效快取", trust_score=60)
            if old_filled:
                diagnostics.append(f"前次有效快取補回 {old_filled} 個仍缺欄位。")

        for c in FACTOR_COLUMNS:
            if c not in df.columns:
                df[c] = ""
        update_time = _now_text()
        sources = []
        if include_institutional:
            sources.append("TWSE_T86_TPEX_3ITRADE")
        if include_revenue:
            sources.append("TWSE_OpenAPI_MOPS_monthly_revenue")
        if include_valuation:
            sources.append("TWSE_BWIBBU_TPEX_PERATIO")

        score_rows = []
        for _, row in df.iterrows():
            item = {c: row.get(c, "") for c in df.columns}
            item.update(_calc_scores(item))
            item["官方因子更新時間"] = update_time
            item["官方因子資料源"] = ",".join(sources)
            item["因子主要來源"] = "TWSE/TPEx/MOPS"
            if not _safe_str(item.get("因子來源可信度")):
                item["因子來源可信度"] = 100
            score_rows.append(item)
        out = pd.DataFrame(score_rows)
        for c in FACTOR_COLUMNS:
            if c not in out.columns:
                out[c] = ""
        out = out[FACTOR_COLUMNS + [c for c in out.columns if c not in FACTOR_COLUMNS]].copy()

        if _REQUEST_NOTES:
            diagnostics = _REQUEST_NOTES + diagnostics
        complete_count = int((pd.to_numeric(out.get("官方資料完整度", pd.Series([], dtype=float)), errors="coerce") >= 60).sum()) if not out.empty else 0
        existing_complete = _existing_complete_count() if save else 0
        should_save = True
        if save and existing_complete > complete_count and complete_count < max(5, int(existing_complete * 0.5)):
            should_save = False
            diagnostics.append(f"本次完整度>=60 僅 {complete_count} 筆，低於既有快取 {existing_complete} 筆，已保留舊有效快取，不覆蓋。")

        budget_status = _end_run_budget()
        meta = {
            "ok": True, "updated_at": update_time, "record_count": int(len(out)),
            "complete_count": complete_count, "existing_complete_count": existing_complete,
            "saved": bool(should_save), "preserved_old_cache": bool(not should_save),
            "diagnostics": _summarize_diagnostics(diagnostics), "market_filter": market_filter,
            "limit": limit or 0, "quick_mode": bool(quick_mode),
            "max_runtime_seconds": int(max_runtime_seconds), "max_requests": int(max_requests),
            "finmind_bulk_only": bool(finmind_bulk_only), "progress_events": progress_events,
            "finmind_fallback_enabled": bool(enable_finmind_fallback),
            "finmind_token_configured": bool(_finmind_token()),
            "finmind_max_stocks": int(finmind_max_stocks),
            "finmind_filled_fields": int(finmind_filled),
            "old_cache_filled_fields": int(old_filled),
            "trusted_source_priority": ["TWSE/TPEx/MOPS", "FinMind", "前次有效快取"],
            **budget_status,
        }
        if save and should_save:
            save_factor_cache(out.to_dict(orient="records"), diagnostics=diagnostics, meta=meta)
        elif save and not should_save:
            _append_log("preserved_old_cache", int(len(out)), _summarize_diagnostics(diagnostics))
        return out, meta
    except Exception:
        _end_run_budget()
        raise


def _is_missing_factor_value(series: pd.Series, column: str) -> pd.Series:
    """Return rows whose existing factor value is only a placeholder.

    Recommendation frames are pre-created with zero/blank official-factor columns.
    A normal pandas merge preserves those placeholder columns and writes the real
    cache values into ``*_官方`` suffix columns.  Downstream coverage then sees the
    original zero columns and incorrectly reports 0% even though the cache is valid.
    """
    text = series.astype(str).str.strip()
    missing = series.isna() | text.isin({"", "nan", "None", "null", "--", "-"})
    if column in {
        "官方資料完整度", "法人籌碼官方分數", "營收成長官方分數",
        "官方估值風險分數", "官方基本面成長分數", "官方因子總分",
    }:
        numeric = pd.to_numeric(series, errors="coerce")
        missing = missing | numeric.fillna(0).le(0)
    return missing


def merge_official_factors(base_df: pd.DataFrame, factor_df: pd.DataFrame | None = None) -> pd.DataFrame:
    """Merge official factors and coalesce real cache values over placeholders.

    All cache columns are renamed to collision-proof temporary names before merge.
    This prevents recommendation frames that already contain ``*_官方`` helper
    columns from producing duplicate labels. Existing non-empty values are kept;
    blank/zero placeholders are replaced by the cache's authoritative values.
    """
    if base_df is None or base_df.empty:
        return base_df.copy() if isinstance(base_df, pd.DataFrame) else pd.DataFrame()
    df = base_df.copy()
    code_col = "股票代號" if "股票代號" in df.columns else ("code" if "code" in df.columns else "")
    if not code_col:
        return df
    df[code_col] = df[code_col].map(_normalize_code)
    fdf = factor_df.copy() if factor_df is not None else load_factor_frame()
    if fdf is None or fdf.empty or "股票代號" not in fdf.columns:
        return df
    fdf["股票代號"] = fdf["股票代號"].map(_normalize_code)
    fdf = fdf[fdf["股票代號"].astype(str).str.len().eq(4)].drop_duplicates("股票代號", keep="first")

    value_cols = [c for c in FACTOR_COLUMNS if c in fdf.columns and c not in {"股票代號", "股票名稱", "市場別", "正式產業別"}]
    temp_map = {c: f"__official_factor__{i}" for i, c in enumerate(value_cols)}
    right = fdf[["股票代號"] + value_cols].rename(columns=temp_map)
    merged = df.merge(right, left_on=code_col, right_on="股票代號", how="left")

    for column in value_cols:
        temp_col = temp_map[column]
        if column not in merged.columns:
            merged[column] = merged[temp_col]
        else:
            replace_mask = _is_missing_factor_value(merged[column], column)
            merged.loc[replace_mask, column] = merged.loc[replace_mask, temp_col]
        merged = merged.drop(columns=[temp_col])

    if code_col != "股票代號" and "股票代號" in merged.columns:
        merged = merged.drop(columns=["股票代號"])
    return merged


def export_cache_csv_bytes() -> bytes:
    df = load_factor_frame()
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")


# =========================================================
# GitHub optional sync helpers
# =========================================================

def _github_cfg() -> dict[str, str]:
    if st is None:
        return {"token": "", "owner": "", "repo": "", "branch": "main", "path": "official_factors_cache.json"}
    secrets = getattr(st, "secrets", {})
    return {
        "token": _safe_str(secrets.get("GITHUB_TOKEN", "")),
        "owner": _safe_str(secrets.get("GITHUB_REPO_OWNER", "cheng07021028")) or "cheng07021028",
        "repo": _safe_str(secrets.get("GITHUB_REPO_NAME", "stock-app")) or "stock-app",
        "branch": _safe_str(secrets.get("GITHUB_REPO_BRANCH", "main")) or "main",
        "path": _safe_str(secrets.get("OFFICIAL_FACTORS_GITHUB_PATH", "official_factors_cache.json")) or "official_factors_cache.json",
    }


def _github_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": USER_AGENT,
    }


def _github_url(owner: str, repo: str, path: str) -> str:
    return f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"


def push_cache_to_github() -> tuple[bool, str]:
    cfg = _github_cfg()
    token = cfg.get("token", "")
    if not token:
        return False, "未設定 GITHUB_TOKEN，略過 GitHub 同步。"
    if not CACHE_FILE.exists():
        return False, "official_factors_cache.json 尚未建立。"
    try:
        url = _github_url(cfg["owner"], cfg["repo"], cfg["path"])
        headers = _github_headers(token)
        sha = ""
        try:
            r = requests.get(url, headers=headers, params={"ref": cfg["branch"]}, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                sha = r.json().get("sha", "")
        except Exception:
            sha = ""
        content = base64.b64encode(CACHE_FILE.read_bytes()).decode("ascii")
        payload = {
            "message": f"Update official factors cache {CACHE_VERSION}",
            "content": content,
            "branch": cfg["branch"],
        }
        if sha:
            payload["sha"] = sha
        r = requests.put(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
        if r.status_code in (200, 201):
            return True, f"GitHub 同步成功：{cfg['path']}"
        return False, f"GitHub 同步失敗 HTTP {r.status_code}: {r.text[:200]}"
    except Exception as exc:
        return False, f"GitHub 同步失敗：{exc}"


def read_cache_from_github() -> tuple[bool, str]:
    cfg = _github_cfg()
    token = cfg.get("token", "")
    if not token:
        return False, "未設定 GITHUB_TOKEN，無法從 GitHub 讀取。"
    try:
        url = _github_url(cfg["owner"], cfg["repo"], cfg["path"])
        r = requests.get(url, headers=_github_headers(token), params={"ref": cfg["branch"]}, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            return False, f"GitHub 讀取失敗 HTTP {r.status_code}: {r.text[:200]}"
        data = r.json()
        content = base64.b64decode(data.get("content", "")).decode("utf-8")
        parsed = json.loads(content)
        if not isinstance(parsed, dict) or "records" not in parsed:
            return False, "GitHub 快取格式不正確。"
        CACHE_FILE.write_text(json.dumps(parsed, ensure_ascii=False, indent=2), encoding="utf-8")
        return True, f"已從 GitHub 讀取 official factors：{len(parsed.get('records', []))} 筆。"
    except Exception as exc:
        return False, f"GitHub 讀取失敗：{exc}"
