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
import threading
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

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
INSTITUTIONAL_HISTORY_FILE = BASE_DIR / "official_factor_institutional_history.json"
TAIPEI_TZ = ZoneInfo("Asia/Taipei")
CACHE_VERSION = "v187_official_factor_trust_governance_20260811"
REQUEST_TIMEOUT = 5
DEFAULT_RUN_TIMEOUT_SECONDS = 75
DEFAULT_RUN_REQUEST_BUDGET = 48
OFFICIAL_FACTOR_DURABLE_PATH = "official_factors_cache.json"
OFFICIAL_FACTOR_AUTHORITY_STATE_FILE = BASE_DIR / "official_factors_authority_state.json"
_AUTHORITY_RESTORE_LOCK = threading.Lock()
_AUTHORITY_RESTORE_DONE = False
_AUTHORITY_RESTORE_MESSAGE = "尚未執行永久權威恢復"
_AUTHORITY_RESTORE_SOURCE = "local"
_AUTHORITY_RESTORE_DATA_DATE = ""
_LAST_PERSIST_OK = False
_LAST_PERSIST_MESSAGE = "尚未執行V186永久化"

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
    "官方因子資料日期",
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
    "每日因子來源可信度",
    "來源可信度狀態",
    "來源可信度說明",
    "備援補值欄位數",
    "FinMind資料日期",
    "法人資料日期",
    "法人資料源",
    "營收資料日期",
    "營收資料源",
    "估值資料日期",
    "估值資料源",
]

# TWSE official/public endpoints used as best-effort sources. Some datasets may be rate-limited
# or delayed. The service always falls back to existing cache.
TWSE_BWIBBU_ALL = "https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_ALL"
TWSE_MONTHLY_REVENUE_L = "https://openapi.twse.com.tw/v1/opendata/t187ap05_L"
TWSE_DAILY_CLOSE_ALL = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
# V182: current official endpoints. TPEx legacy routes remain only as last-resort fallbacks.
TPEX_OPENAPI_BASE = "https://www.tpex.org.tw/openapi/v1"
TPEX_MONTHLY_REVENUE_O = f"{TPEX_OPENAPI_BASE}/mopsfin_t187ap05_O"
TPEX_MONTHLY_REVENUE_R = f"{TPEX_OPENAPI_BASE}/t187ap05_R"
TPEX_PERATIO_OPENAPI = f"{TPEX_OPENAPI_BASE}/tpex_mainboard_peratio_analysis"
TPEX_3INSTI_OPENAPI = f"{TPEX_OPENAPI_BASE}/tpex_3insti_daily_trading"
TPEX_DAILY_CLOSE_OPENAPI = f"{TPEX_OPENAPI_BASE}/tpex_mainboard_daily_close_quotes"
TWSE_T86 = "https://www.twse.com.tw/rwd/zh/fund/T86"
TWSE_T86_OLD = "https://www.twse.com.tw/fund/T86"
TPEX_3ITRADE_LEGACY = "https://www.tpex.org.tw/www/zh-tw/afterTrading/3itrade"
TPEX_PERATIO_LEGACY = "https://www.tpex.org.tw/www/zh-tw/afterTrading/peratio"
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


def _now_taipei() -> dt.datetime:
    return dt.datetime.now(TAIPEI_TZ)


def _now_text() -> str:
    return _now_taipei().strftime("%Y-%m-%d %H:%M:%S")


def _today_yyyymmdd() -> str:
    return _now_taipei().strftime("%Y%m%d")


def _legacy_endpoints_enabled() -> bool:
    """Obsolete 404-prone HTML/legacy routes are opt-in only in V182."""
    return _safe_str(os.getenv("OFFICIAL_FACTOR_ENABLE_LEGACY_ENDPOINTS", "")).lower() in {"1", "true", "yes", "on"}


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


def _redact_sensitive_text(value: Any) -> str:
    """Remove API tokens/authorization material from UI diagnostics and logs."""
    text = _safe_str(value)
    if not text:
        return ""
    token = _finmind_token() if "_finmind_token" in globals() else ""
    if token:
        text = text.replace(token, "***REDACTED***")
    text = re.sub(r"([?&](?:token|api[_-]?key|apikey|access[_-]?token)=)[^&\s]+", r"\1***REDACTED***", text, flags=re.I)
    text = re.sub(r"(Authorization[:=]\s*Bearer\s+)[A-Za-z0-9._~+\-/=]+", r"\1***REDACTED***", text, flags=re.I)
    return text


def _norm_key(value: Any) -> str:
    return re.sub(r"[^0-9a-zA-Z\u4e00-\u9fff]+", "", _safe_str(value)).lower()


def _pick_fuzzy(row: dict[str, Any], candidates: Iterable[str]) -> Any:
    """Pick a value using exact keys first, then normalized key equality/containment."""
    direct = _extract_first(row, candidates) if "_extract_first" in globals() else None
    if direct is not None and _safe_str(direct) != "":
        return direct
    norm_map = {_norm_key(k): k for k in row.keys()}
    cand_norms = [_norm_key(x) for x in candidates if _norm_key(x)]
    for cn in cand_norms:
        if cn in norm_map:
            return row.get(norm_map[cn])
    for cn in cand_norms:
        for nk, original in norm_map.items():
            if cn and (cn in nk or nk in cn):
                return row.get(original)
    return None


def _roc_or_iso_to_yyyymmdd(value: Any) -> str:
    s = re.sub(r"[^0-9]", "", _safe_str(value))
    if len(s) == 7:  # ROC yyyMMdd
        try:
            return f"{int(s[:3]) + 1911:04d}{s[3:]}"
        except Exception:
            return ""
    if len(s) >= 8:
        return s[:8]
    return ""


def _normalize_year_month(value: Any) -> str:
    s = re.sub(r"[^0-9]", "", _safe_str(value))
    if len(s) == 5:  # ROC yyyMM
        try:
            return f"{int(s[:3]) + 1911:04d}{s[3:]}"
        except Exception:
            return ""
    if len(s) >= 6:
        if len(s) == 7:  # ROC yyyMMdd accidentally provided; keep month only
            try:
                return f"{int(s[:3]) + 1911:04d}{s[3:5]}"
            except Exception:
                return ""
        return s[:6]
    return ""


def _pct_change(new_value: Any, old_value: Any) -> float | None:
    new = _to_float(new_value, None)
    old = _to_float(old_value, None)
    if new is None or old in (None, 0):
        return None
    return round((new - old) / abs(old) * 100.0, 4)


def _compact_error(exc: Exception) -> str:
    text = _redact_sensitive_text(exc)
    text = re.sub(r"\s+", " ", text).strip()
    text = text.replace("HTTPSConnectionPool", "HTTPS")
    # Do not render full query strings in diagnostics; dataset/source is enough.
    text = re.sub(r"(https?://[^?\s]+)\?[^\s]+", r"\1?[query-redacted]", text)
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


def _read_local_factor_cache_raw() -> dict[str, Any]:
    if not CACHE_FILE.exists():
        return {
            "version": CACHE_VERSION,
            "updated_at": "",
            "records": [],
            "diagnostics": ["尚未建立官方因子快取。"],
        }
    try:
        data = json.loads(CACHE_FILE.read_text(encoding="utf-8-sig"))
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


def _factor_payload_business_date(payload: Any) -> str:
    data = payload if isinstance(payload, dict) else {}
    direct = _roc_or_iso_to_yyyymmdd(data.get("data_date"))
    meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
    meta_date = _roc_or_iso_to_yyyymmdd(meta.get("data_date"))
    dates = [x for x in (direct, meta_date) if x]
    rows = data.get("records") if isinstance(data.get("records"), list) else []
    for row in rows:
        if not isinstance(row, dict):
            continue
        for field in (
            "官方因子資料日期", "官方資料日期", "三大法人資料日期",
            "法人資料日期", "估值資料日期", "FinMind資料日期",
        ):
            value = _roc_or_iso_to_yyyymmdd(row.get(field))
            if value:
                dates.append(value)
                break
    return max(dates) if dates else ""


def _write_factor_authority_state(payload: dict[str, Any], *, permanent_ok: bool, message: str, source: str) -> None:
    try:
        import hashlib
        compact = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
        state = {
            "version": "v186_official_factor_authority_state",
            "updated_at": _now_text(),
            "data_date": _factor_payload_business_date(payload),
            "record_count": len(payload.get("records", [])) if isinstance(payload.get("records"), list) else 0,
            "payload_hash": hashlib.sha256(compact.encode("utf-8")).hexdigest(),
            "source": source,
            "remote_permanent_confirmed": bool(permanent_ok),
            "message": str(message or "")[:1500],
        }
        tmp = OFFICIAL_FACTOR_AUTHORITY_STATE_FILE.with_suffix(".json.tmp_v186")
        tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(OFFICIAL_FACTOR_AUTHORITY_STATE_FILE)
    except Exception:
        pass


def _restore_official_factor_authority_once(*, force: bool = False) -> tuple[bool, str]:
    """Restore the newest business-date factor cache after Streamlit reboot.

    V184/V185 could recreate the repository's old packaged cache on every cold
    boot.  This guard runs once per Python process (or when explicitly forced),
    asks the durable persistence layer to elect Local/GitHub/Firestore by the
    *data date*, and writes the winner back to the local authority file.
    """
    global _AUTHORITY_RESTORE_DONE, _AUTHORITY_RESTORE_MESSAGE
    global _AUTHORITY_RESTORE_SOURCE, _AUTHORITY_RESTORE_DATA_DATE
    if _AUTHORITY_RESTORE_DONE and not force:
        return True, _AUTHORITY_RESTORE_MESSAGE
    with _AUTHORITY_RESTORE_LOCK:
        if _AUTHORITY_RESTORE_DONE and not force:
            return True, _AUTHORITY_RESTORE_MESSAGE
        before = _read_local_factor_cache_raw()
        before_date = _factor_payload_business_date(before)
        try:
            from godpick_persistence_service import load_named_json_permanent
            chosen, details = load_named_json_permanent(OFFICIAL_FACTOR_DURABLE_PATH, before)
            chosen = chosen if isinstance(chosen, dict) else before
            after_date = _factor_payload_business_date(chosen)
            detail_text = "｜".join(str(x) for x in (details or []))
            source = "local"
            if "權威來源：github" in detail_text:
                source = "github-runtime-data"
            elif "權威來源：firestore" in detail_text:
                source = "firestore"
            _AUTHORITY_RESTORE_DONE = True
            _AUTHORITY_RESTORE_SOURCE = source
            _AUTHORITY_RESTORE_DATA_DATE = after_date
            _AUTHORITY_RESTORE_MESSAGE = (
                f"V186官方因子權威恢復：{source}｜{before_date or '未驗證'}→{after_date or '未驗證'}"
            )
            _write_factor_authority_state(chosen, permanent_ok=(source != "local"), message=_AUTHORITY_RESTORE_MESSAGE, source=source)
            return True, _AUTHORITY_RESTORE_MESSAGE
        except Exception as exc:
            _AUTHORITY_RESTORE_DONE = True
            _AUTHORITY_RESTORE_SOURCE = "local-fallback"
            _AUTHORITY_RESTORE_DATA_DATE = before_date
            _AUTHORITY_RESTORE_MESSAGE = f"V186永久權威讀取失敗，暫用本機：{type(exc).__name__}: {exc}"
            return False, _AUTHORITY_RESTORE_MESSAGE


def get_factor_authority_status() -> dict[str, Any]:
    payload = _read_local_factor_cache_raw()
    state: dict[str, Any] = {}
    try:
        if OFFICIAL_FACTOR_AUTHORITY_STATE_FILE.exists():
            raw = json.loads(OFFICIAL_FACTOR_AUTHORITY_STATE_FILE.read_text(encoding="utf-8-sig"))
            if isinstance(raw, dict):
                state = raw
    except Exception:
        state = {}
    return {
        "version": CACHE_VERSION,
        "data_date": _factor_payload_business_date(payload),
        "record_count": len(payload.get("records", [])) if isinstance(payload.get("records"), list) else 0,
        "restore_done": bool(_AUTHORITY_RESTORE_DONE),
        "restore_source": _AUTHORITY_RESTORE_SOURCE,
        "restore_message": _AUTHORITY_RESTORE_MESSAGE,
        "last_persist_ok": bool(_LAST_PERSIST_OK),
        "last_persist_message": _LAST_PERSIST_MESSAGE,
        "state": state,
    }


def load_factor_cache(*, force_authority_restore: bool = False) -> dict[str, Any]:
    _restore_official_factor_authority_once(force=bool(force_authority_restore))
    return _read_local_factor_cache_raw()


def _summarize_diagnostics(diagnostics: list[str] | None, max_items: int = 40) -> list[str]:
    out: list[str] = []
    for msg in diagnostics or []:
        text = re.sub(r"\s+", " ", _redact_sensitive_text(msg)).strip()
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
    # V187 permanent migration: every newly saved cache carries evidence-based
    # source trust so Reboot/runtime-data restore cannot resurrect stale 60-point
    # whole-row trust flags from V182-V186.
    migrated_records: list[dict[str, Any]] = []
    for raw in records or []:
        item = dict(raw) if isinstance(raw, dict) else {}
        item.update(_derive_source_trust_v187(item))
        migrated_records.append(item)
    records = migrated_records
    meta_safe = _json_safe(meta or {})
    payload = {
        "version": CACHE_VERSION,
        "updated_at": _now_text(),
        "data_date": _safe_str((meta_safe or {}).get("data_date")),
        "record_count": len(records),
        "records": _json_safe(records),
        "diagnostics": _summarize_diagnostics(diagnostics or []),
        "meta": meta_safe,
    }

    # V186: this is critical business data.  Do not only queue an absolute-path
    # background job: a Streamlit reboot can terminate that thread and the next
    # process then falls back to the packaged July cache.  Persist the RELATIVE
    # authority path synchronously so GitHub runtime-data (or another configured
    # permanent layer) is confirmed before the update is reported complete.
    permanent_ok = False
    permanent_msg = ""
    try:
        from godpick_durability_service import persist_json_permanent
        permanent_ok, permanent_msg = persist_json_permanent(
            OFFICIAL_FACTOR_DURABLE_PATH, payload,
            reason="V186 official factor business-date authority",
        )
    except Exception as exc:
        permanent_msg = f"V186永久化服務例外：{type(exc).__name__}: {exc}"

    if not CACHE_FILE.exists():
        # Defensive local fallback if the persistence service was unavailable.
        try:
            tmp = CACHE_FILE.with_suffix(".json.tmp_v186")
            tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(CACHE_FILE)
        except Exception:
            pass

    _write_factor_authority_state(
        payload, permanent_ok=permanent_ok, message=permanent_msg,
        source="local+remote" if permanent_ok else "local-only",
    )
    persistence_note = (
        "V186：官方因子已完成遠端永久化確認。"
        if permanent_ok else
        "V186：本機已保存，但遠端永久化尚未確認；Reboot 前請至第17頁重試永久化。"
    )
    _append_log(
        "success" if permanent_ok else "local_only", len(records),
        _summarize_diagnostics(list(payload.get("diagnostics", [])) + [persistence_note]),
    )

    global _AUTHORITY_RESTORE_DONE, _AUTHORITY_RESTORE_MESSAGE, _AUTHORITY_RESTORE_SOURCE, _AUTHORITY_RESTORE_DATA_DATE
    global _LAST_PERSIST_OK, _LAST_PERSIST_MESSAGE
    _LAST_PERSIST_OK = bool(permanent_ok)
    _LAST_PERSIST_MESSAGE = str(permanent_msg or "")
    _AUTHORITY_RESTORE_DONE = True
    _AUTHORITY_RESTORE_SOURCE = "local+remote" if permanent_ok else "local-only"
    _AUTHORITY_RESTORE_DATA_DATE = _factor_payload_business_date(payload)
    _AUTHORITY_RESTORE_MESSAGE = (
        f"V186官方因子保存：data_date={_AUTHORITY_RESTORE_DATA_DATE or '未驗證'}｜"
        + ("遠端永久化已確認" if permanent_ok else "僅本機，遠端未確認")
    )
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


def _row_daily_factor_date_v184(row: Any) -> str:
    """Return a conservative daily official-factor date for one stock.

    Monthly revenue is intentionally excluded: comparing YYYYMM revenue with a
    trading-day K-line is semantically wrong.  When both valuation and
    institutional dates exist, use the OLDER daily date so one fresh domain
    cannot hide another stale daily domain.
    """
    values: list[str] = []
    getter = row.get if hasattr(row, "get") else (lambda _k, _d="": _d)
    for key in ["官方因子資料日期", "官方資料日期", "法人資料日期", "估值資料日期", "FinMind資料日期"]:
        d = _roc_or_iso_to_yyyymmdd(getter(key, ""))
        if d:
            values.append(d)
    return min(values) if values else ""


def _source_score_v187(source: Any) -> int:
    """Evidence-based trust score for one provenance string.

    This never upgrades a row merely because a value exists.  Official exchange
    and MOPS provenance is high trust, FinMind is a structured backup, and an
    unnamed/previous cache alone remains below the formal 70-point threshold.
    """
    text = _safe_str(source).upper()
    if not text:
        return 0
    if any(token in text for token in ["TWSE", "TPEX", "MOPS", "OPENAPI", "OFFICIAL_DAILY_HISTORY"]):
        return 100
    if "FINMIND" in text:
        return 82
    if any(token in text for token in ["前次有效快取", "PREVIOUS_CACHE", "LEGACY_CACHE", "CACHE"]):
        return 60
    return 50


def _has_domain_value_v187(row: Any, columns: list[str]) -> bool:
    getter = row.get if hasattr(row, "get") else (lambda _k, _d="": _d)
    for col in columns:
        value = getter(col, "")
        if _safe_str(value):
            number = _to_float(value, None)
            if number is not None or _safe_str(value) not in {"", "--", "-", "N/A", "NA"}:
                return True
    return False


def _derive_source_trust_v187(row: Any) -> dict[str, Any]:
    """Rebuild trust from actual per-domain provenance instead of stale row flags.

    Daily execution governance cares primarily about institution/valuation
    provenance; monthly revenue is useful for the overall factor score but does
    not define the trading-day freshness gate.  A previous-cache fill therefore
    cannot downgrade an otherwise official daily row.
    """
    getter = row.get if hasattr(row, "get") else (lambda _k, _d="": _d)
    generic_source = _safe_str(getter("官方因子資料源", "")) or _safe_str(getter("因子主要來源", ""))
    fallback_source = _safe_str(getter("因子備援來源", ""))

    domains = {
        "法人": {
            "source": _safe_str(getter("法人資料源", "")) or _safe_str(getter("FinMind法人資料源", "")),
            "date": _roc_or_iso_to_yyyymmdd(getter("法人資料日期", "")) or _roc_or_iso_to_yyyymmdd(getter("FinMind資料日期", "")),
            "has": _has_domain_value_v187(row, ["外資近1日買賣超", "投信近1日買賣超", "自營商近1日買賣超", "三大法人近1日合計", "法人籌碼官方分數"]),
            "weight": 0.45,
            "daily_weight": 0.60,
        },
        "估值": {
            "source": _safe_str(getter("估值資料源", "")) or _safe_str(getter("FinMind估值資料源", "")),
            "date": _roc_or_iso_to_yyyymmdd(getter("估值資料日期", "")) or _roc_or_iso_to_yyyymmdd(getter("FinMind資料日期", "")),
            "has": _has_domain_value_v187(row, ["PER本益比", "PBR股價淨值比", "股利殖利率%", "估算EPS", "官方估值風險分數"]),
            "weight": 0.30,
            "daily_weight": 0.40,
        },
        "營收": {
            "source": _safe_str(getter("營收資料源", "")) or _safe_str(getter("FinMind營收資料源", "")),
            "date": _roc_or_iso_to_yyyymmdd(getter("營收資料日期", "")) or _roc_or_iso_to_yyyymmdd(getter("FinMind資料日期", "")),
            "has": _has_domain_value_v187(row, ["當月營收", "月營收MoM%", "月營收YoY%", "累計營收YoY%", "營收成長官方分數"]),
            "weight": 0.25,
            "daily_weight": 0.0,
        },
    }

    detail: list[str] = []
    overall_num = overall_den = 0.0
    daily_num = daily_den = 0.0
    for name, info in domains.items():
        source = info["source"]
        if not source and (info["has"] or info["date"]):
            source = generic_source
        score = _source_score_v187(source)
        if score <= 0 and fallback_source and (info["has"] or info["date"]):
            score = _source_score_v187(fallback_source)
            source = source or fallback_source
        if info["has"] or info["date"] or source:
            detail.append(f"{name}:{source or '來源未驗證'}={score}")
            if score > 0:
                overall_num += score * float(info["weight"])
                overall_den += float(info["weight"])
                if float(info["daily_weight"]) > 0 and (info["date"] or info["has"]):
                    daily_num += score * float(info["daily_weight"])
                    daily_den += float(info["daily_weight"])

    generic_score = _source_score_v187(generic_source)
    legacy_score = int(_to_float(getter("因子來源可信度", ""), 0) or 0)
    overall = round(overall_num / overall_den) if overall_den > 0 else (generic_score or legacy_score)
    daily = round(daily_num / daily_den) if daily_den > 0 else (generic_score or legacy_score)

    # Never let an unrelated previous-cache field lower verified official daily
    # provenance. Conversely, do not magically turn unknown legacy rows into 100.
    overall = max(0, min(100, int(overall or 0)))
    daily = max(0, min(100, int(daily or 0)))
    status = "官方高可信" if daily >= 90 else "可信備援" if daily >= 70 else "低可信/舊快取" if daily > 0 else "來源未驗證"
    return {
        "因子來源可信度": overall,
        "每日因子來源可信度": daily,
        "來源可信度狀態": status,
        "來源可信度說明": "｜".join(detail)[:360],
    }


def _apply_source_trust_migration_v187(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    derived = [_derive_source_trust_v187(row) for row in out.to_dict("records")]
    if derived:
        ddf = pd.DataFrame(derived, index=out.index)
        for col in ["因子來源可信度", "每日因子來源可信度", "來源可信度狀態", "來源可信度說明"]:
            out[col] = ddf[col]
    return out


def load_factor_frame() -> pd.DataFrame:
    cache = load_factor_cache()
    records = cache.get("records", [])
    if not isinstance(records, list) or not records:
        return _empty_factor_df()
    df = pd.DataFrame(records)
    for c in FACTOR_COLUMNS:
        if c not in df.columns:
            df[c] = ""
    # V184 legacy migration: V182 already stored per-domain dates but many rows
    # left the generic official date blank.  Governance then reported
    # "有效83.8%／最新可信0%" even when the data was verifiable.
    derived = df.apply(_row_daily_factor_date_v184, axis=1)
    for col in ["官方資料日期", "官方因子資料日期"]:
        current = df[col].map(_roc_or_iso_to_yyyymmdd)
        df[col] = current.where(current.astype(str).str.len().eq(8), derived)
    # V187 legacy/live-cache migration: recompute provenance from actual domain
    # source fields every load.  This immediately repairs V182-V186 rows whose
    # whole-row trust was downgraded to 60/82 by a single fallback-filled field.
    df = _apply_source_trust_migration_v187(df)
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
    eligible_count = 0
    eligible_complete = 0
    market_stats: dict[str, Any] = {}
    if not df.empty:
        complete_vals = pd.to_numeric(df.get("官方資料完整度", pd.Series(index=df.index, dtype=float)), errors="coerce").fillna(0)
        complete = int((complete_vals >= 60).sum())
        markets = df.get("市場別", pd.Series(index=df.index, dtype=str)).astype(str)
        eligible = markets.str.contains("上市|上櫃", regex=True, na=False)
        eligible_count = int(eligible.sum())
        eligible_complete = int(((complete_vals >= 60) & eligible).sum())
        for market in ["上市", "上櫃", "興櫃"]:
            mask = markets.eq(market)
            if mask.any():
                market_stats[market] = {"rows": int(mask.sum()), "complete": int(((complete_vals >= 60) & mask).sum())}
    return {
        "exists": file_exists, "path": str(CACHE_FILE), "size_kb": size_kb,
        "updated_at": _safe_str(cache.get("updated_at")), "record_count": int(len(df)) if df is not None else 0,
        "complete_count": complete, "eligible_count": eligible_count, "eligible_complete_count": eligible_complete,
        "eligible_coverage": round(eligible_complete / eligible_count * 100.0, 2) if eligible_count else 0.0,
        "market_stats": market_stats, "diagnostics": cache.get("diagnostics", []),
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


def _official_close_map(url: str, market: str) -> tuple[dict[str, float], str]:
    """Fetch one latest official all-market close snapshot for EPS estimation.

    This is deliberately a single bulk request, never a per-stock loop. Failure does
    not make valuation unusable; it only leaves estimated EPS blank.
    """
    try:
        data = _get_json(url)
        if not isinstance(data, list):
            return {}, f"{market}官方收盤快照格式非 list"
        out: dict[str, float] = {}
        for row in data:
            if not isinstance(row, dict):
                continue
            code = _normalize_code(_pick_fuzzy(row, ["Code", "SecuritiesCompanyCode", "SecuritiesCode", "證券代號", "股票代號", "代號"]))
            close = _to_float(_pick_fuzzy(row, ["ClosingPrice", "Close", "收盤價", "最後成交價"]), None)
            if code and close is not None and close > 0:
                out[code] = float(close)
        return out, f"{market}官方收盤快照取得 {len(out)} 筆"
    except Exception as exc:
        return {}, f"{market}官方收盤快照失敗：{_compact_error(exc)}"


def fetch_twse_bwibbu_all() -> tuple[pd.DataFrame, str]:
    """上市 PER/PBR/殖利率（TWSE current OpenAPI）＋官方收盤估算 EPS。"""
    try:
        data = _get_json(TWSE_BWIBBU_ALL)
        if not isinstance(data, list):
            return pd.DataFrame(), "TWSE BWIBBU 回傳格式非 list。"
        rows: list[dict[str, Any]] = []
        dates: list[str] = []
        for r in data:
            if not isinstance(r, dict):
                continue
            code = _normalize_code(_pick_fuzzy(r, ["Code", "證券代號", "股票代號", "代號"]))
            if not code:
                continue
            date_text = _roc_or_iso_to_yyyymmdd(_pick_fuzzy(r, ["Date", "資料日期", "日期"]))
            if date_text:
                dates.append(date_text)
            pe = _to_float(_pick_fuzzy(r, ["PEratio", "PER", "PriceEarningRatio", "本益比"]), None)
            pbr = _to_float(_pick_fuzzy(r, ["PBratio", "PBR", "PriceBookRatio", "股價淨值比"]), None)
            yld = _to_float(_pick_fuzzy(r, ["DividendYield", "DividendYieldPercent", "殖利率(%)", "殖利率"]), None)
            rows.append({
                "股票代號": code, "PER本益比": pe, "PBR股價淨值比": pbr,
                "股利殖利率%": yld, "估算EPS": None,
                "估值資料日期": date_text, "估值資料源": "TWSE_OPENAPI_BWIBBU_ALL",
            })
        frame = pd.DataFrame(rows)
        eps_count = 0
        close_msg = ""
        if not frame.empty and pd.to_numeric(frame["PER本益比"], errors="coerce").notna().any():
            close_map, close_msg = _official_close_map(TWSE_DAILY_CLOSE_ALL, "TWSE")
            if close_map:
                eps_values=[]
                for _, row in frame.iterrows():
                    pe = _to_float(row.get("PER本益比"), None)
                    close = close_map.get(_normalize_code(row.get("股票代號")))
                    eps = round(close / pe, 4) if close is not None and pe not in (None, 0) else None
                    eps_values.append(eps)
                    if eps is not None:
                        eps_count += 1
                frame["估算EPS"] = eps_values
        latest = max(dates) if dates else ""
        suffix = f"｜資料日 {latest}" if latest else ""
        eps_suffix = f"｜估算EPS {eps_count} 筆" if eps_count else ""
        if close_msg and not eps_count:
            eps_suffix += f"｜{close_msg}"
        return frame, f"TWSE PER/PBR/殖利率取得 {len(frame)} 筆{suffix}{eps_suffix}。"
    except Exception as exc:
        return pd.DataFrame(), f"TWSE PER/PBR 取得失敗：{_compact_error(exc)}"

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


def _parse_monthly_revenue_rows(data: Any, market: str, source: str) -> pd.DataFrame:
    if not isinstance(data, list):
        return pd.DataFrame()
    out: list[dict[str, Any]] = []
    for r in data:
        if not isinstance(r, dict):
            continue
        code = _normalize_code(_pick_fuzzy(r, ["公司代號", "Code", "股票代號", "出表公司代號", "SecuritiesCompanyCode"]))
        if not code:
            continue
        current = _to_float(_pick_fuzzy(r, ["營業收入-當月營收", "當月營收", "本月營收", "CurrentMonthRevenue", "RevenueCurrentMonth"]), None)
        prev = _to_float(_pick_fuzzy(r, ["營業收入-上月營收", "上月營收", "PreviousMonthRevenue", "RevenuePreviousMonth"]), None)
        last_year = _to_float(_pick_fuzzy(r, ["營業收入-去年當月營收", "去年當月營收", "LastYearMonthRevenue", "RevenueLastYearMonth"]), None)
        current_acc = _to_float(_pick_fuzzy(r, ["累計營業收入-當月累計營收", "當月累計營收", "累計營收", "CurrentAccumulatedRevenue"]), None)
        last_acc = _to_float(_pick_fuzzy(r, ["累計營業收入-去年累計營收", "去年累計營收", "LastYearAccumulatedRevenue"]), None)
        mom = _to_float(_pick_fuzzy(r, ["上月比較增減(%)", "上月比較增減％", "上月比較增減百分比", "MoM", "營收月增率"]), None)
        yoy = _to_float(_pick_fuzzy(r, ["去年同月增減(%)", "去年同月增減％", "去年同月增減百分比", "YoY", "營收年增率"]), None)
        acc_yoy = _to_float(_pick_fuzzy(r, ["前期比較增減(%)", "前期比較增減％", "累計營收年增率", "累計增減(%)", "AccumulatedYoY"]), None)
        if mom is None:
            mom = _pct_change(current, prev)
        if yoy is None:
            yoy = _pct_change(current, last_year)
        if acc_yoy is None:
            acc_yoy = _pct_change(current_acc, last_acc)
        year_month = _normalize_year_month(_pick_fuzzy(r, ["資料年月", "營收年月", "年月", "YearMonth", "出表日期"]))
        out.append({
            "股票代號": code, "當月營收": current, "月營收MoM%": mom,
            "月營收YoY%": yoy, "累計營收YoY%": acc_yoy,
            "營收年月": year_month, "營收資料日期": year_month,
            "營收資料源": source, "市場別_營收": market,
        })
    if not out:
        return pd.DataFrame()
    return pd.DataFrame(out).drop_duplicates("股票代號", keep="first")


def fetch_monthly_revenue() -> tuple[pd.DataFrame, str]:
    """上市/上櫃月營收。V182 使用兩市場各自的 current OpenAPI。"""
    endpoints = [
        ("上市", TWSE_MONTHLY_REVENUE_L, "TWSE_OPENAPI_t187ap05_L"),
        ("上櫃", TPEX_MONTHLY_REVENUE_O, "TPEX_OPENAPI_mopsfin_t187ap05_O"),
    ]
    frames: list[pd.DataFrame] = []
    msgs: list[str] = []
    failed_markets: list[str] = []
    for market, url, source in endpoints:
        try:
            data = _get_json(url)
            frame = _parse_monthly_revenue_rows(data, market, source)
            if frame.empty:
                failed_markets.append(market)
                msgs.append(f"{market}月營收 OpenAPI 0 筆可解析資料。")
            else:
                frames.append(frame)
                ym = frame.get("營收年月", pd.Series([], dtype=str)).astype(str)
                latest = ym[ym.str.len().ge(6)].max() if not ym.empty else ""
                msgs.append(f"{market}月營收 OpenAPI 取得 {len(frame)} 筆" + (f"｜資料月 {latest}" if latest else "") + "。")
        except Exception as exc:
            failed_markets.append(market)
            msgs.append(f"{market}月營收 OpenAPI 失敗：{_compact_error(exc)}")

    # Last-resort MOPS HTML is used only for a market that current OpenAPI did not cover.
    # It is intentionally not queried when both current OpenAPIs succeed, avoiding the
    # obsolete NAS 404 storm seen in Streamlit Cloud.
    if failed_markets:
        if _legacy_endpoints_enabled():
            try:
                fb_df, fb_msg = _fetch_mops_monthly_revenue_html()
                if fb_df is not None and not fb_df.empty:
                    if "市場別_營收" not in fb_df.columns:
                        fb_df["市場別_營收"] = ""
                    frames.append(fb_df)
                    msgs.append("缺漏市場啟用 legacy MOPS HTML 最後備援：" + _redact_sensitive_text(fb_msg))
                else:
                    msgs.append("legacy MOPS HTML 最後備援未取得額外資料。")
            except Exception as exc:
                msgs.append(f"legacy MOPS HTML 最後備援失敗：{_compact_error(exc)}")
        else:
            msgs.append("缺漏市場未再呼叫已知 404 風險的 legacy MOPS NAS；直接交由 FinMind 缺值備援／前次有效快取。")

    if not frames:
        return pd.DataFrame(), " / ".join(msgs)
    combined = pd.concat(frames, ignore_index=True, sort=False)
    # Current OpenAPI frames are first and therefore authoritative over legacy fallback.
    combined = combined.drop_duplicates("股票代號", keep="first")
    return combined, " / ".join(msgs)

def _recent_weekdays(days: int = 10) -> list[str]:
    now = _now_taipei()
    today = now.date()
    # Before the exchange's post-close datasets are normally ready, start from
    # the prior calendar day. This avoids guaranteed 404/no-data noise at 00:xx~15:xx.
    if now.hour < 16:
        today = today - dt.timedelta(days=1)
    out: list[str] = []
    i = 0
    while len(out) < days and i < days * 4 + 7:
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
            "法人資料日期": items[0].get("date", "") if items else "",
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


def _load_institutional_history() -> dict[str, Any]:
    try:
        if INSTITUTIONAL_HISTORY_FILE.exists():
            raw = json.loads(INSTITUTIONAL_HISTORY_FILE.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                raw.setdefault("days", {})
                return raw
    except Exception:
        pass
    return {"version": CACHE_VERSION, "updated_at": "", "days": {}}


def _save_institutional_daily_snapshot(market: str, date_text: str, rows: list[dict[str, Any]]) -> None:
    if not date_text or not rows:
        return
    payload = _load_institutional_history()
    days = payload.setdefault("days", {})
    day = days.setdefault(date_text, {})
    compact = []
    for item in rows:
        code = _normalize_code(item.get("股票代號") or item.get("code"))
        if not code:
            continue
        compact.append({"股票代號": code, "foreign": _to_int(item.get("foreign")),
                        "trust": _to_int(item.get("trust")), "dealer": _to_int(item.get("dealer")),
                        "total": _to_int(item.get("total"))})
    if not compact:
        return
    day[market] = compact
    # retain only the newest 12 market days
    for old_day in sorted(days.keys(), reverse=True)[12:]:
        days.pop(old_day, None)
    payload["version"] = CACHE_VERSION
    payload["updated_at"] = _now_text()
    try:
        INSTITUTIONAL_HISTORY_FILE.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        try:
            from godpick_durability_service import persist_json_async
            persist_json_async(str(INSTITUTIONAL_HISTORY_FILE), payload, reason="V183 institutional history")
        except Exception:
            pass
    except Exception:
        pass


def _aggregate_institutional_history(market: str, latest_rows: list[dict[str, Any]] | None = None, latest_date: str = "", days: int = 7) -> pd.DataFrame:
    payload = _load_institutional_history()
    if latest_rows and latest_date:
        _save_institutional_daily_snapshot(market, latest_date, latest_rows)
        payload = _load_institutional_history()
    records_by_code: dict[str, list[dict[str, Any]]] = {}
    for date_text in sorted((payload.get("days") or {}).keys(), reverse=True):
        market_rows = ((payload.get("days") or {}).get(date_text) or {}).get(market, [])
        for row in market_rows if isinstance(market_rows, list) else []:
            code = _normalize_code(row.get("股票代號"))
            if not code:
                continue
            records_by_code.setdefault(code, []).append({"date": date_text, "foreign": _to_int(row.get("foreign")),
                                                          "trust": _to_int(row.get("trust")), "dealer": _to_int(row.get("dealer")),
                                                          "total": _to_int(row.get("total"))})
    out: list[dict[str, Any]] = []
    for code, items in records_by_code.items():
        items = sorted(items, key=lambda x: x["date"], reverse=True)[:max(days, 5)]
        def total(key: str, n: int) -> int:
            return int(sum(_to_int(x.get(key)) for x in items[:n]))
        consecutive = 0
        for item in items:
            if _to_int(item.get("total")) > 0:
                consecutive += 1
            else:
                break
        out.append({"股票代號": code, "官方資料日期": items[0]["date"] if items else "",
                    "法人資料日期": items[0]["date"] if items else "",
                    "外資近1日買賣超": total("foreign", 1), "外資近3日買賣超": total("foreign", 3), "外資近5日買賣超": total("foreign", 5),
                    "投信近1日買賣超": total("trust", 1), "投信近3日買賣超": total("trust", 3), "投信近5日買賣超": total("trust", 5),
                    "自營商近1日買賣超": total("dealer", 1), "自營商近3日買賣超": total("dealer", 3), "自營商近5日買賣超": total("dealer", 5),
                    "三大法人近1日合計": total("total", 1), "三大法人近3日合計": total("total", 3), "三大法人近5日合計": total("total", 5),
                    "法人連買天數": consecutive, "法人資料源": f"{market}_OFFICIAL_DAILY_HISTORY"})
    return pd.DataFrame(out)


def _parse_tpex_openapi_institutional(data: Any) -> tuple[list[dict[str, Any]], str]:
    if not isinstance(data, list):
        return [], ""
    out: list[dict[str, Any]] = []
    dates: list[str] = []
    for row in data:
        if not isinstance(row, dict):
            continue
        code = _normalize_code(_pick_fuzzy(row, ["SecuritiesCompanyCode", "SecuritiesCode", "Code", "股票代號", "證券代號", "代號"]))
        if not code:
            continue
        date_text = _roc_or_iso_to_yyyymmdd(_pick_fuzzy(row, ["Date", "資料日期", "日期"]))
        if date_text:
            dates.append(date_text)
        foreign = _to_int(_pick_fuzzy(row, [
            "ForeignInvestorsIncludeMainlandAreaInvestors-Difference",
            "Foreign Investors include Mainland Area Investors-Difference",
            "ForeignInvestorsDifference", "外資及陸資買賣超股數", "外資買賣超股數"
        ]))
        trust = _to_int(_pick_fuzzy(row, ["SecuritiesInvestmentTrustCompanies-Difference", "InvestmentTrustDifference", "投信買賣超股數"]))
        dealer = _to_int(_pick_fuzzy(row, ["Dealers-Difference", "DealerDifference", "自營商買賣超股數"]))
        total_raw = _pick_fuzzy(row, ["TotalDifference", "Total-Difference", "三大法人買賣超股數合計", "三大法人買賣超股數"])
        total = _to_int(total_raw)
        if total_raw is None or _safe_str(total_raw) == "":
            total = foreign + trust + dealer
        out.append({"股票代號": code, "foreign": foreign, "trust": trust, "dealer": dealer, "total": total})
    return out, (max(dates) if dates else "")


def fetch_tpex_institutional(days: int = 7) -> tuple[pd.DataFrame, str]:
    """上櫃三大法人。V182 先用 current TPEx OpenAPI，再以本地日快照累積 3/5 日。"""
    msgs: list[str] = []
    try:
        data = _get_json(TPEX_3INSTI_OPENAPI)
        latest_rows, data_date = _parse_tpex_openapi_institutional(data)
        if latest_rows:
            if not data_date:
                # OpenAPI may omit an explicit date; use last completed session candidate.
                data_date = (_recent_weekdays(1) or [""])[0]
            _save_institutional_daily_snapshot("上櫃", data_date, latest_rows)
            agg = _aggregate_institutional_history("上櫃", days=days)
            if not agg.empty:
                agg["法人資料源"] = "TPEX_OPENAPI_3INSTI+DAILY_HISTORY"
                return agg, f"TPEx OpenAPI 三大法人取得 {len(latest_rows)} 筆｜資料日 {data_date}｜歷史快照 {min(days, 5)} 日滾動。"
            # Even on first day, return 1-day values; revenue+valuation can still satisfy basic completeness.
            one = []
            for x in latest_rows:
                one.append({"股票代號": x["股票代號"], "官方資料日期": data_date, "法人資料日期": data_date,
                            "外資近1日買賣超": x["foreign"], "投信近1日買賣超": x["trust"], "自營商近1日買賣超": x["dealer"],
                            "三大法人近1日合計": x["total"], "法人資料源": "TPEX_OPENAPI_3INSTI"})
            return pd.DataFrame(one), f"TPEx OpenAPI 三大法人取得 {len(one)} 筆｜首日快照已建立。"
        msgs.append("TPEx OpenAPI 三大法人回傳 0 筆可解析資料。")
    except Exception as exc:
        msgs.append(f"TPEx OpenAPI 三大法人失敗：{_compact_error(exc)}")

    # Old /www/zh-tw/afterTrading route is known to 404 on the current site.
    # V182 does not hammer it by default; opt-in exists only for emergency rollback testing.
    if not _legacy_endpoints_enabled():
        agg = _aggregate_institutional_history("上櫃", days=days)
        if not agg.empty:
            return agg, " / ".join(msgs + ["TPEx current OpenAPI 暫時失敗；沿用既有官方日快照，跳過 obsolete legacy 404 route。"] )
        return pd.DataFrame(), " / ".join(msgs + ["TPEx current OpenAPI 暫時失敗；跳過 obsolete legacy 404 route，交由 FinMind/前次有效快取補值。"] )

    # Legacy historical endpoint is opt-in last-resort only.
    records_by_code: dict[str, list[dict[str, Any]]] = {}
    failures = 0
    for date_text in _recent_weekdays(max(days, 3)):
        date_slash = f"{date_text[:4]}/{date_text[4:6]}/{date_text[6:8]}"
        try:
            data = _get_json(TPEX_3ITRADE_LEGACY, params={"date": date_slash, "type": "Daily", "response": "json"})
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
                    total_raw = pick(row, ["三大法人買賣超股數合計", "三大法人買賣超股數", "合計買賣超股數"])
                    total = _to_int(total_raw) if total_raw is not None else foreign + trust + dealer
                    records_by_code.setdefault(code, []).append({"date": date_text, "foreign": foreign, "trust": trust, "dealer": dealer, "total": total})
                    cnt += 1
            if cnt:
                snapshot_rows=[{"股票代號":code, **items[-1]} for code,items in records_by_code.items() if items and items[-1].get("date")==date_text]
                _save_institutional_daily_snapshot("上櫃", date_text, snapshot_rows)
        except Exception:
            failures += 1
    agg = _aggregate_institutional_history("上櫃", days=days)
    if not agg.empty:
        return agg, " / ".join(msgs + [f"TPEx legacy/歷史快照補值成功；legacy失敗 {failures} 個日期。"])
    return pd.DataFrame(), " / ".join(msgs + [f"TPEx legacy 亦無可用資料；失敗 {failures} 個日期。"] )

def fetch_tpex_valuation() -> tuple[pd.DataFrame, str]:
    """上櫃本益比／殖利率／PBR。V182 以 TPEx OpenAPI 為主。"""
    msgs: list[str] = []
    try:
        data = _get_json(TPEX_PERATIO_OPENAPI)
        if isinstance(data, list):
            out: list[dict[str, Any]] = []
            dates: list[str] = []
            for row in data:
                if not isinstance(row, dict):
                    continue
                code = _normalize_code(_pick_fuzzy(row, ["SecuritiesCompanyCode", "SecuritiesCode", "Code", "股票代號", "證券代號", "代號"]))
                if not code:
                    continue
                date_text = _roc_or_iso_to_yyyymmdd(_pick_fuzzy(row, ["Date", "資料日期", "日期"]))
                if date_text:
                    dates.append(date_text)
                pe = _to_float(_pick_fuzzy(row, ["PriceEarningRatio", "PER", "PERatio", "本益比", "本益比(倍)"]), None)
                pbr = _to_float(_pick_fuzzy(row, ["PriceBookRatio", "PBR", "PBRatio", "股價淨值比", "股價淨值比(倍)"]), None)
                yld = _to_float(_pick_fuzzy(row, ["DividendYield", "DividendYieldPercent", "Yield", "殖利率(%)", "殖利率"]), None)
                close = _to_float(_pick_fuzzy(row, ["ClosingPrice", "Close", "收盤價"]), None)
                eps = round(close / pe, 4) if close is not None and pe not in (None, 0) else None
                out.append({"股票代號": code, "PER本益比": pe, "PBR股價淨值比": pbr,
                            "股利殖利率%": yld, "估算EPS": eps,
                            "估值資料日期": date_text, "估值資料源": "TPEX_OPENAPI_peratio_analysis"})
            if out:
                frame = pd.DataFrame(out).drop_duplicates("股票代號")
                eps_count = 0
                close_map, close_msg = _official_close_map(TPEX_DAILY_CLOSE_OPENAPI, "TPEx")
                if close_map:
                    eps_values = []
                    for _, item in frame.iterrows():
                        pe = _to_float(item.get("PER本益比"), None)
                        close = close_map.get(_normalize_code(item.get("股票代號")))
                        eps = round(close / pe, 4) if close is not None and pe not in (None, 0) else None
                        eps_values.append(eps)
                        if eps is not None:
                            eps_count += 1
                    frame["估算EPS"] = eps_values
                latest = max(dates) if dates else ""
                note = f"TPEx OpenAPI PER/PBR/殖利率取得 {len(frame)} 筆" + (f"｜資料日 {latest}" if latest else "")
                note += f"｜估算EPS {eps_count} 筆" if eps_count else f"｜{close_msg}"
                return frame, note + "。"
            msgs.append("TPEx OpenAPI 估值回傳 0 筆可解析資料。")
        else:
            msgs.append("TPEx OpenAPI 估值回傳格式非 list。")
    except Exception as exc:
        msgs.append(f"TPEx OpenAPI 估值失敗：{_compact_error(exc)}")

    # Old /www/zh-tw/afterTrading/peratio route is 404-prone on the current site.
    if not _legacy_endpoints_enabled():
        return pd.DataFrame(), " / ".join(msgs + ["跳過 obsolete TPEx legacy peratio；交由 FinMind/前次有效快取補值。"] )

    # Opt-in last resort only; do not loop seven dates.
    for date_text in _recent_weekdays(3):
        date_slash = f"{date_text[:4]}/{date_text[4:6]}/{date_text[6:8]}"
        try:
            data = _get_json(TPEX_PERATIO_LEGACY, params={"date": date_slash, "id": "", "response": "json"})
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
                    out.append({"股票代號": code,
                                "PER本益比": _to_float(pick(row, ["本益比", "本益比(倍)"])),
                                "PBR股價淨值比": _to_float(pick(row, ["股價淨值比", "股價淨值比(倍)"])),
                                "股利殖利率%": _to_float(pick(row, ["殖利率(%)", "殖利率％", "殖利率"])),
                                "估值資料日期": date_text, "估值資料源": "TPEX_LEGACY_PERATIO"})
            if out:
                return pd.DataFrame(out).drop_duplicates("股票代號"), " / ".join(msgs + [f"{date_text} TPEx legacy 估值取得 {len(out)} 筆。"])
        except Exception as exc:
            msgs.append(f"{date_text} TPEx legacy 估值失敗：{_compact_error(exc)}")
    return pd.DataFrame(), " / ".join(msgs[-5:])

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
    pbr = _to_float(row.get("PBR股價淨值比"), None)
    dividend_yield = _to_float(row.get("股利殖利率%"), None)
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
    if any(v is not None for v in (per, eps, pbr, dividend_yield)):
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
        "rate_limit_note": "FinMind 僅作最後備援；V182 不再將 token 放進 URL，也不在快速模式做無 data_id 的整批試探。",
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
        # Official FinMind docs support Bearer Authorization. Never duplicate the
        # token into query params, otherwise requests exceptions can leak it to UI logs.
        headers["Authorization"] = f"Bearer {token}"
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
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        code = _normalize_code(row.get("stock_id"))
        if code:
            grouped.setdefault(code, []).append(row)
    out: list[dict[str, Any]] = []
    for code, items in grouped.items():
        items = sorted(items, key=lambda x: _safe_str(x.get("date")))
        latest = items[-1] if items else {}
        current = _to_float(latest.get("revenue"), None)
        prev = _to_float(items[-2].get("revenue"), None) if len(items) >= 2 else None
        latest_month = int(_to_float(latest.get("revenue_month"), 0) or 0)
        latest_year = int(_to_float(latest.get("revenue_year"), 0) or 0)
        same_last_year = None
        for item in reversed(items[:-1]):
            if int(_to_float(item.get("revenue_month"), -1) or -1) == latest_month and int(_to_float(item.get("revenue_year"), -1) or -1) == latest_year - 1:
                same_last_year = _to_float(item.get("revenue"), None)
                break
        mom = _pct_change(current, prev)
        yoy = _pct_change(current, same_last_year)
        date_text = _safe_str(latest.get("date") or latest.get("create_time"))
        ym = f"{latest_year:04d}{latest_month:02d}" if latest_year and latest_month else _normalize_year_month(date_text)
        out.append({"股票代號": code, "當月營收": current, "月營收MoM%": mom, "月營收YoY%": yoy,
                    "營收年月": ym, "FinMind資料日期": date_text, "營收資料日期": ym,
                    "FinMind營收資料源": "FinMind_TaiwanStockMonthRevenue"})
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
    """Bounded FinMind final fallback.

    V182 deliberately does not issue speculative all-market requests. Current
    FinMind datasets are safest with data_id for institutional/PER and may reject
    large bulk ranges depending on dataset/plan. Quick mode therefore skips it;
    full mode queries only the already-filtered missing stock codes.
    """
    diagnostics: list[str] = []
    if not _finmind_token():
        return pd.DataFrame(), ["FinMind 備援未啟用：請在 Streamlit Secrets 設定 FINMIND_TOKEN。"]
    codes = universe.get("股票代號", pd.Series([], dtype=str)).astype(str).map(_normalize_code).tolist()
    codes = [c for c in codes if c][:max(0, int(max_stocks))]
    if not codes:
        return pd.DataFrame(), ["FinMind 備援沒有需要補值的股票。"]
    if bulk_only:
        return pd.DataFrame(), [f"V182 快速模式：官方 OpenAPI 優先；FinMind {len(codes)} 檔缺值留待完整增量逐檔補值，避免 400/額度浪費。"]

    frames: list[pd.DataFrame] = []
    start12, end = _date_range(14)
    start550, _ = _date_range(550)
    datasets: list[tuple[str, str, str, Any]] = []
    if include_institutional:
        datasets.append(("TaiwanStockInstitutionalInvestorsBuySellWide", start12, end, _finmind_institutional_frame))
    if include_revenue:
        datasets.append(("TaiwanStockMonthRevenue", start550, end, _finmind_revenue_frame))
    if include_valuation:
        datasets.append(("TaiwanStockPER", start12, end, _finmind_valuation_frame))
    request_budget = int(request_budget_override or min(180, max(12, len(codes) * max(1, len(datasets)))))
    used = 0
    for dataset, start, end_date, parser in datasets:
        per_rows: list[dict[str, Any]] = []
        dataset_used = 0
        for code in codes:
            try:
                _budget_guard(f"FinMind {dataset}")
            except OfficialFactorBudgetExceeded as exc:
                diagnostics.append(str(exc))
                break
            if used >= request_budget:
                diagnostics.append(f"FinMind 已達本輪安全請求上限 {request_budget}；其餘缺值保留前次有效快取。")
                break
            rows, _ = _finmind_get(dataset, start, end_date, data_id=code)
            used += 1
            dataset_used += 1
            if rows:
                per_rows.extend(rows)
        parsed = parser(per_rows) if per_rows else pd.DataFrame()
        if parsed is not None and not parsed.empty:
            frames.append(parsed)
        diagnostics.append(f"FinMind {dataset} 逐檔備援請求 {dataset_used} 次，取得 {0 if parsed is None else len(parsed)} 檔。")
        if used >= request_budget:
            break
    if not frames:
        return pd.DataFrame(), diagnostics
    out = frames[0]
    for frame in frames[1:]:
        out = out.merge(frame, on="股票代號", how="outer", suffixes=("", "__fmdup"))
        for c in [x for x in out.columns if x.endswith("__fmdup")]:
            base = c[:-7]
            if base in out.columns:
                mask = _is_missing_factor_value(out[base], base)
                out.loc[mask, base] = out.loc[mask, c]
            else:
                out[base] = out[c]
            out = out.drop(columns=[c])
    return out.drop_duplicates("股票代號", keep="first"), diagnostics

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
    # V187: a single fallback-filled field must NOT downgrade the whole row.
    # Example: TWSE/TPEX daily institution + valuation are official, while one
    # monthly/optional field is restored from the previous cache.  V186 used to
    # set the entire row to 60/82 here, which later caused 83% valid T-1 data to
    # be misclassified as untrusted and blocked all formal recommendations.
    if "因子來源可信度" not in merged.columns:
        merged["因子來源可信度"]=""
    existing_trust=pd.to_numeric(merged["因子來源可信度"],errors="coerce")
    only_missing=touched & (existing_trust.isna() | existing_trust.le(0))
    merged.loc[only_missing,"因子來源可信度"]=trust_score
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
            progress("finmind", "FinMind 最後缺值備援")
            try:
                _budget_guard("FinMind 備援")
                # Only query codes that still lack one or more requested domains.
                fm_need = df[["股票代號"]].copy()
                need_mask = pd.Series(False, index=df.index)
                if include_institutional:
                    inst_cols = [c for c in ["外資近1日買賣超", "三大法人近1日合計", "三大法人近5日合計"] if c in df.columns]
                    if inst_cols:
                        need_mask |= df[inst_cols].apply(lambda s: pd.to_numeric(s, errors="coerce")).isna().all(axis=1)
                    else:
                        need_mask |= True
                if include_revenue:
                    rev_cols = [c for c in ["當月營收", "月營收YoY%"] if c in df.columns]
                    if rev_cols:
                        need_mask |= df[rev_cols].apply(lambda s: pd.to_numeric(s, errors="coerce")).isna().all(axis=1)
                    else:
                        need_mask |= True
                if include_valuation:
                    val_cols = [c for c in ["PER本益比", "PBR股價淨值比", "股利殖利率%"] if c in df.columns]
                    if val_cols:
                        need_mask |= df[val_cols].apply(lambda s: pd.to_numeric(s, errors="coerce")).isna().all(axis=1)
                    else:
                        need_mask |= True
                fm_need = df.loc[need_mask, ["股票代號"]].head(max(0, int(finmind_max_stocks))).copy()
                fm_df, fm_diag = fetch_finmind_fallback(
                    fm_need, max_stocks=finmind_max_stocks,
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
            sources.append("TWSE_T86_TPEX_OPENAPI_3INSTI")
        if include_revenue:
            sources.append("TWSE_TPEX_OpenAPI_monthly_revenue")
        if include_valuation:
            sources.append("TWSE_BWIBBU_TPEX_OPENAPI_PERATIO")

        score_rows = []
        for _, row in df.iterrows():
            item = {c: row.get(c, "") for c in df.columns}
            item.update(_calc_scores(item))
            # V184：建立可被推薦治理層直接驗證的「逐股每日官方日期」。
            # 月營收是月頻資料，不可拿來與每日 K 線做交易日差。
            daily_factor_date = _row_daily_factor_date_v184(item)
            if daily_factor_date:
                item["官方資料日期"] = daily_factor_date
                item["官方因子資料日期"] = daily_factor_date
            item["官方因子更新時間"] = update_time
            item["官方因子資料源"] = ",".join(sources)
            item["因子主要來源"] = "TWSE/TPEx/MOPS"
            # V187: derive trust from each domain's actual source after all
            # official/fallback merges.  Do not use a single fallback touch as
            # the trust score for the entire row.
            item.update(_derive_source_trust_v187(item))
            score_rows.append(item)
        out = pd.DataFrame(score_rows)
        for c in FACTOR_COLUMNS:
            if c not in out.columns:
                out[c] = ""
        out = out[FACTOR_COLUMNS + [c for c in out.columns if c not in FACTOR_COLUMNS]].copy()

        if _REQUEST_NOTES:
            diagnostics = _REQUEST_NOTES + diagnostics
        complete_vals = pd.to_numeric(out.get("官方資料完整度", pd.Series(index=out.index, dtype=float)), errors="coerce").fillna(0) if not out.empty else pd.Series([], dtype=float)
        complete_count = int((complete_vals >= 60).sum()) if not out.empty else 0
        market_vals = out.get("市場別", pd.Series(index=out.index, dtype=str)).astype(str) if not out.empty else pd.Series([], dtype=str)
        eligible_mask = market_vals.isin(["上市", "上櫃"]) if not out.empty else pd.Series([], dtype=bool)
        eligible_count = int(eligible_mask.sum()) if not out.empty else 0
        eligible_complete_count = int(((complete_vals >= 60) & eligible_mask).sum()) if not out.empty else 0
        eligible_coverage = round(eligible_complete_count / eligible_count * 100.0, 2) if eligible_count else 0.0
        existing_complete = _existing_complete_count() if save else 0
        should_save = True
        if save and existing_complete > complete_count and complete_count < max(5, int(existing_complete * 0.5)):
            should_save = False
            diagnostics.append(f"本次完整度>=60 僅 {complete_count} 筆，低於既有快取 {existing_complete} 筆，已保留舊有效快取，不覆蓋。")

        # 快取層級日期取所有逐股「每日官方日期」的最新一日；這只用於
        # 頁面總體狀態，逐股交易許可仍以每列日期為準。
        daily_dates = [
            _roc_or_iso_to_yyyymmdd(v)
            for v in out.get("官方因子資料日期", pd.Series([], dtype=object)).tolist()
        ] if not out.empty else []
        daily_dates = [x for x in daily_dates if x]
        data_date = max(daily_dates) if daily_dates else ""

        budget_status = _end_run_budget()
        meta = {
            "ok": True, "updated_at": update_time, "data_date": data_date, "record_count": int(len(out)),
            "complete_count": complete_count, "eligible_count": eligible_count,
            "eligible_complete_count": eligible_complete_count, "eligible_coverage": eligible_coverage,
            "existing_complete_count": existing_complete,
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
            "trusted_source_priority": ["TWSE/TPEx current OpenAPI", "TWSE T86 / MOPS official", "FinMind", "前次有效快取"],
            **budget_status,
        }
        if save and should_save:
            save_factor_cache(out.to_dict(orient="records"), diagnostics=diagnostics, meta=meta)
            authority_status = get_factor_authority_status()
            meta["permanent_ok"] = bool(authority_status.get("last_persist_ok"))
            meta["permanent_message"] = _safe_str(authority_status.get("last_persist_message"))
            meta["authority_data_date"] = _safe_str(authority_status.get("data_date"))
        elif save and not should_save:
            meta["permanent_ok"] = False
            meta["permanent_message"] = "本輪品質保護未覆蓋既有快取，未建立新的永久版本。"
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
        return {"token": "", "owner": "", "repo": "", "branch": os.getenv("GITHUB_RUNTIME_DATA_BRANCH") or os.getenv("GITHUB_REPO_BRANCH") or "runtime-data", "path": "official_factors_cache.json", "history_path": "official_factor_institutional_history.json"}
    secrets = getattr(st, "secrets", {})
    return {
        "token": _safe_str(secrets.get("GITHUB_TOKEN", "")),
        "owner": _safe_str(secrets.get("GITHUB_REPO_OWNER", "cheng07021028")) or "cheng07021028",
        "repo": _safe_str(secrets.get("GITHUB_REPO_NAME", "stock-app")) or "stock-app",
        "branch": (_safe_str(os.getenv("GITHUB_RUNTIME_DATA_BRANCH", "")) or _safe_str(secrets.get("GITHUB_RUNTIME_DATA_BRANCH", "")) or _safe_str(os.getenv("GITHUB_REPO_BRANCH", "")) or _safe_str(secrets.get("GITHUB_REPO_BRANCH", "runtime-data")) or "runtime-data"),
        "path": _safe_str(secrets.get("OFFICIAL_FACTORS_GITHUB_PATH", "official_factors_cache.json")) or "official_factors_cache.json",
        "history_path": _safe_str(secrets.get("OFFICIAL_FACTORS_HISTORY_GITHUB_PATH", "official_factor_institutional_history.json")) or "official_factor_institutional_history.json",
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
    """V187 verified durable sync; migrate provenance before remote persistence."""
    payload = _read_local_factor_cache_raw()
    if not isinstance(payload, dict) or not isinstance(payload.get("records"), list) or not payload.get("records"):
        return False, "official_factors_cache.json 尚未建立有效資料。"

    # V187: manual sync must not perpetuate the obsolete whole-row 60/82 trust
    # flag. Rebuild provenance from the actual per-domain source fields first.
    try:
        migrated_df = _apply_source_trust_migration_v187(pd.DataFrame(payload.get("records", [])))
        payload = dict(payload)
        payload["version"] = CACHE_VERSION
        payload["records"] = _json_safe(migrated_df.to_dict("records"))
        payload["record_count"] = len(payload["records"])
    except Exception as exc:
        return False, f"V187來源可信度校正失敗，未同步舊可信度：{type(exc).__name__}: {exc}"

    # If the immediately preceding V186 save already confirmed the exact same
    # payload, do not upload the multi-MB cache twice from page17.
    try:
        status = get_factor_authority_status()
        state = status.get("state") if isinstance(status.get("state"), dict) else {}
        if state.get("remote_permanent_confirmed") and state.get("data_date") == _factor_payload_business_date(payload):
            return True, f"V187永久層已確認，無需重複上傳｜data_date={state.get('data_date') or '未驗證'}"
    except Exception:
        pass

    try:
        from godpick_durability_service import persist_json_permanent
        ok, msg = persist_json_permanent(
            OFFICIAL_FACTOR_DURABLE_PATH, payload, reason="V187 manual official-factor durable sync"
        )
        _write_factor_authority_state(
            payload, permanent_ok=bool(ok), message=msg,
            source="manual-local+remote" if ok else "manual-local-only",
        )
        return bool(ok), ("GitHub/runtime-data永久同步成功：" if ok else "永久同步未確認：") + str(msg)
    except Exception as exc:
        return False, f"永久同步例外：{type(exc).__name__}: {exc}"


def read_cache_from_github() -> tuple[bool, str]:
    """Restore by business-date authority instead of blindly overwriting local."""
    before = _factor_payload_business_date(_read_local_factor_cache_raw())
    ok, msg = _restore_official_factor_authority_once(force=True)
    after_payload = _read_local_factor_cache_raw()
    after = _factor_payload_business_date(after_payload)
    count = len(after_payload.get("records", [])) if isinstance(after_payload.get("records"), list) else 0
    # Even if a remote call failed, keeping a newer local business date is the
    # correct monotonic behavior.  Return warning in that case, but never regress.
    return bool(ok), f"V186權威選舉完成：{before or '未驗證'}→{after or '未驗證'}｜{count}筆｜{msg}"


