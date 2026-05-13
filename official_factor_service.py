# -*- coding: utf-8 -*-
"""V108 官方因子快取服務

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
import re
import time
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import requests

try:
    import streamlit as st
except Exception:  # pragma: no cover
    st = None  # type: ignore

BASE_DIR = Path(__file__).resolve().parent
CACHE_FILE = BASE_DIR / "official_factors_cache.json"
LOG_FILE = BASE_DIR / "official_factors_update_log.json"
CACHE_VERSION = "v108_official_factor_cache_center"
REQUEST_TIMEOUT = 12
USER_AGENT = "Mozilla/5.0 (SPT-Godpick-V108; official-factor-cache)"

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
]

# TWSE official/public endpoints used as best-effort sources. Some datasets may be rate-limited
# or delayed. The service always falls back to existing cache.
TWSE_BWIBBU_ALL = "https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_ALL"
TWSE_MONTHLY_REVENUE_L = "https://openapi.twse.com.tw/v1/opendata/t187ap05_L"
TPEX_MONTHLY_REVENUE_O = "https://openapi.twse.com.tw/v1/opendata/t187ap05_O"
TWSE_T86 = "https://www.twse.com.tw/rwd/zh/fund/T86"


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


def _get_json(url: str, params: dict[str, Any] | None = None) -> Any:
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json,text/plain,*/*"}
    r = requests.get(url, params=params or {}, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    try:
        return r.json()
    except Exception:
        text = r.text.strip("\ufeff \n\r\t")
        return json.loads(text)


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


def save_factor_cache(records: list[dict[str, Any]], diagnostics: list[str] | None = None, meta: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = {
        "version": CACHE_VERSION,
        "updated_at": _now_text(),
        "record_count": len(records),
        "records": _json_safe(records),
        "diagnostics": diagnostics or [],
        "meta": meta or {},
    }
    CACHE_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    _append_log("success", len(records), diagnostics or [])
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
    if not out:
        return pd.DataFrame(), " / ".join(msgs)
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
            params = {"date": date_text, "selectType": "ALLBUT0999", "response": "json"}
            data = _get_json(TWSE_T86, params=params)
            fields = data.get("fields") if isinstance(data, dict) else None
            rows = data.get("data") if isinstance(data, dict) else None
            if not fields or not rows:
                msgs.append(f"{date_text} T86 無資料。")
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


def build_official_factor_cache(
    limit: int | None = None,
    market_filter: str = "全部",
    include_institutional: bool = True,
    include_revenue: bool = True,
    include_valuation: bool = True,
    save: bool = True,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    diagnostics: list[str] = []
    universe = load_stock_universe(limit=limit, market_filter=market_filter)
    if universe.empty:
        diagnostics.append("股票主檔為空，請先更新 9_股票主檔更新。")
        df = _empty_factor_df()
        meta = {"ok": False, "diagnostics": diagnostics}
        return df, meta

    df = universe.copy()
    if include_valuation:
        val_df, msg = fetch_twse_bwibbu_all()
        diagnostics.append(msg)
        if not val_df.empty:
            df = df.merge(val_df, on="股票代號", how="left")
    if include_revenue:
        rev_df, msg = fetch_monthly_revenue()
        diagnostics.append(msg)
        if not rev_df.empty:
            df = df.merge(rev_df, on="股票代號", how="left")
    if include_institutional:
        inst_df, msg = fetch_twse_institutional(days=7)
        diagnostics.append(msg)
        if not inst_df.empty:
            df = df.merge(inst_df, on="股票代號", how="left")

    # Ensure all expected numeric/data columns exist before scoring.
    for c in FACTOR_COLUMNS:
        if c not in df.columns:
            df[c] = ""
    update_time = _now_text()
    sources = []
    if include_institutional:
        sources.append("TWSE_T86")
    if include_revenue:
        sources.append("TWSE_OpenAPI_MOPS_monthly_revenue")
    if include_valuation:
        sources.append("TWSE_BWIBBU_ALL")

    score_rows = []
    for _, row in df.iterrows():
        item = {c: row.get(c, "") for c in df.columns}
        item.update(_calc_scores(item))
        item["官方因子更新時間"] = update_time
        item["官方因子資料源"] = ",".join(sources)
        score_rows.append(item)
    out = pd.DataFrame(score_rows)
    for c in FACTOR_COLUMNS:
        if c not in out.columns:
            out[c] = ""
    out = out[FACTOR_COLUMNS + [c for c in out.columns if c not in FACTOR_COLUMNS]].copy()

    meta = {
        "ok": True,
        "updated_at": update_time,
        "record_count": int(len(out)),
        "diagnostics": diagnostics,
        "market_filter": market_filter,
        "limit": limit or 0,
    }
    if save:
        save_factor_cache(out.to_dict(orient="records"), diagnostics=diagnostics, meta=meta)
    return out, meta


def merge_official_factors(base_df: pd.DataFrame, factor_df: pd.DataFrame | None = None) -> pd.DataFrame:
    """供 07/10/8/14 後續使用：把官方因子以股票代號合併到任意表。"""
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
    use_cols = [c for c in FACTOR_COLUMNS if c in fdf.columns and c not in {"股票名稱", "市場別", "正式產業別"}]
    merged = df.merge(fdf[use_cols].drop_duplicates("股票代號"), left_on=code_col, right_on="股票代號", how="left", suffixes=("", "_官方"))
    if code_col != "股票代號" and "股票代號_官方" in merged.columns:
        merged = merged.drop(columns=["股票代號_官方"])
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
