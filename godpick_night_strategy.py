# -*- coding: utf-8 -*-
"""GodPick night strategy engine v1.0

Purpose
-------
This module is intentionally additive. It enriches the existing 7_股神推薦
result table with an after-market / next-trading-day decision layer without
rewriting the original recommendation pipeline.

Design goals
------------
- Fast by default: price/technical strategy is vectorized and local.
- External chips/fundamental data is optional and cached. If public endpoints
  fail or are unavailable, the system never blocks recommendation output.
- Honest data quality: real data and proxy data are clearly separated in the
  columns shown to the user.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
import json
import math
import os
import re
import time

import pandas as pd

try:
    import requests
except Exception:  # pragma: no cover
    requests = None  # type: ignore

CACHE_FILE = Path("godpick_night_strategy_cache.json")
CACHE_TTL_SECONDS = 18 * 3600
ENGINE_VERSION = "night_next_entry_v109_official_factor_cache_20260513"

NIGHT_COLUMNS = [
    "夜間股神版本",
    "夜間股神總分",
    "隔日進場分數",
    "波段潛力分數",
    "技術趨勢分數",
    "量價動能分數",
    "法人籌碼分數",
    "大戶鎖碼分數",
    "基本面成長分數",
    "營收成長分數",
    "EPS成長分數",
    "估值風險分數",
    "PER本益比",
    "估算EPS",
    "外資近1日買賣超",
    "投信近1日買賣超",
    "自營商近1日買賣超",
    "三大法人近1日合計",
    "外資近5日買賣超",
    "投信近5日買賣超",
    "三大法人近5日合計",
    "法人連買天數",
    "官方因子總分",
    "官方資料完整度",
    "官方因子資料狀態",
    "月營收YoY%",
    "月營收MoM%",
    "累計營收YoY%",
    "PBR股價淨值比",
    "股利殖利率%",
    "官方因子更新時間",
    "法人買超占量比%",
    "法人連買推估",
    "籌碼資料來源",
    "籌碼資料日期",
    "基本面資料來源",
    "基本面資料日期",
    "資料完整度",
    "進場型態_隔日",
    "隔日建議動作",
    "預估進場點",
    "回測承接價",
    "突破確認價_隔日",
    "停損價_隔日",
    "第一壓力價",
    "觀察週期",
    "夜間股神建議",
    "隔日作戰策略",
    "進場條件說明",
    "不追高條件",
    "夜間風險提醒",
]


def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def _safe_float(v: Any, default: float | None = None) -> float | None:
    try:
        if v is None:
            return default
        if isinstance(v, str):
            s = v.strip().replace(",", "").replace("%", "")
            if s in {"", "-", "--", "—", "nan", "None", "null"}:
                return default
            # TWSE sometimes returns '--' or Chinese text around numbers.
            m = re.search(r"-?\d+(?:\.\d+)?", s)
            if not m:
                return default
            return float(m.group(0))
        if pd.isna(v):
            return default
        return float(v)
    except Exception:
        return default


def _clip(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, float(v)))


def _fmt_price(v: Any) -> str:
    x = _safe_float(v)
    return "" if x is None else f"{x:.2f}"


def _code(v: Any) -> str:
    s = _safe_str(v).replace(".TW", "").replace(".TWO", "")
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits[:6] if len(digits) >= 4 else s


def _today_ymd() -> str:
    return datetime.now().strftime("%Y%m%d")


def _load_cache() -> dict[str, Any]:
    try:
        if CACHE_FILE.exists():
            return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {"version": ENGINE_VERSION, "payloads": {}, "institution_history": {}}


def _save_cache(cache: dict[str, Any]) -> None:
    try:
        cache["version"] = ENGINE_VERSION
        cache["saved_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    except Exception:
        pass


def _official_fetch_enabled(cache: dict[str, Any]) -> bool:
    """Official TWSE fetch is opt-in to protect 07 recommendation speed.

    Enable with environment variable GODPICK_ENABLE_OFFICIAL_NIGHT_DATA=1 or
    by adding {"enable_official_night_data": true} to godpick_user_settings.json.
    Existing cached official payloads are reused even when live fetch is off.
    """
    try:
        if str(os.environ.get("GODPICK_ENABLE_OFFICIAL_NIGHT_DATA", "")).lower() in {"1", "true", "yes", "y"}:
            return True
    except Exception:
        pass
    try:
        p = Path("godpick_user_settings.json")
        if p.exists():
            data = json.loads(p.read_text(encoding="utf-8"))
            if bool(data.get("enable_official_night_data", False)):
                return True
    except Exception:
        pass
    return False


def _cache_get(cache: dict[str, Any], key: str) -> Any | None:
    try:
        item = cache.get("payloads", {}).get(key)
        if not isinstance(item, dict):
            return None
        ts = float(item.get("ts", 0) or 0)
        if time.time() - ts > CACHE_TTL_SECONDS:
            return None
        return item.get("data")
    except Exception:
        return None


def _cache_set(cache: dict[str, Any], key: str, data: Any) -> None:
    cache.setdefault("payloads", {})[key] = {"ts": time.time(), "data": data}


def _request_json(url: str, params: dict[str, Any], timeout: float = 0.15) -> dict[str, Any] | None:
    if requests is None:
        return None
    try:
        r = requests.get(url, params=params, timeout=(0.05, timeout), headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


def _latest_trade_dates(max_days: int = 10) -> list[str]:
    out: list[str] = []
    d = date.today()
    for i in range(max_days):
        x = d - timedelta(days=i)
        if x.weekday() < 5:
            out.append(x.strftime("%Y%m%d"))
    return out


def _parse_twse_rows(payload: dict[str, Any] | None) -> tuple[list[str], list[list[Any]]]:
    if not isinstance(payload, dict):
        return [], []
    fields = payload.get("fields") or payload.get("titles") or []
    data = payload.get("data") or payload.get("tables", [{}])[0].get("data") if payload.get("tables") else payload.get("data")
    if not isinstance(data, list):
        data = []
    return [str(x) for x in fields], data


def _find_col(fields: list[str], candidates: list[str]) -> int | None:
    for cand in candidates:
        for idx, f in enumerate(fields):
            if cand in str(f):
                return idx
    return None


def _fetch_twse_institution(cache: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Fetch TWSE all-stock institution daily trading if available."""
    for ymd in _latest_trade_dates(2):
        cached = _cache_get(cache, f"twse_inst_{ymd}")
        if isinstance(cached, dict):
            return cached
    if not _official_fetch_enabled(cache):
        return {}
    for ymd in _latest_trade_dates(2):
        key = f"twse_inst_{ymd}"
        cached = _cache_get(cache, key)
        if isinstance(cached, dict):
            return cached
        payload = _request_json(
            "https://www.twse.com.tw/rwd/zh/fund/T86",
            {"date": ymd, "selectType": "ALLBUT0999", "response": "json"},
        )
        fields, rows = _parse_twse_rows(payload)
        if not fields or not rows:
            continue
        i_code = _find_col(fields, ["證券代號", "代號"])
        i_foreign = _find_col(fields, ["外陸資買賣超", "外資買賣超", "外資及陸資買賣超"])
        i_trust = _find_col(fields, ["投信買賣超"])
        i_dealer = _find_col(fields, ["自營商買賣超"])
        i_total = _find_col(fields, ["三大法人買賣超", "合計買賣超"])
        if i_code is None:
            continue
        out: dict[str, dict[str, Any]] = {}
        for row in rows:
            if not isinstance(row, (list, tuple)) or len(row) <= i_code:
                continue
            c = _code(row[i_code])
            foreign = _safe_float(row[i_foreign], 0) if i_foreign is not None and len(row) > i_foreign else 0
            trust = _safe_float(row[i_trust], 0) if i_trust is not None and len(row) > i_trust else 0
            dealer = _safe_float(row[i_dealer], 0) if i_dealer is not None and len(row) > i_dealer else 0
            total = _safe_float(row[i_total], None) if i_total is not None and len(row) > i_total else None
            if total is None:
                total = (foreign or 0) + (trust or 0) + (dealer or 0)
            out[c] = {
                "date": ymd,
                "foreign": foreign or 0,
                "trust": trust or 0,
                "dealer": dealer or 0,
                "total": total or 0,
                "source": "TWSE_T86",
            }
        if out:
            _cache_set(cache, key, out)
            return out
    return {}


def _fetch_twse_valuation(cache: dict[str, Any]) -> dict[str, dict[str, Any]]:
    for ymd in _latest_trade_dates(2):
        cached = _cache_get(cache, f"twse_valuation_{ymd}")
        if isinstance(cached, dict):
            return cached
    if not _official_fetch_enabled(cache):
        return {}
    for ymd in _latest_trade_dates(2):
        key = f"twse_valuation_{ymd}"
        cached = _cache_get(cache, key)
        if isinstance(cached, dict):
            return cached
        payload = _request_json(
            "https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d",
            {"date": ymd, "selectType": "ALL", "response": "json"},
        )
        fields, rows = _parse_twse_rows(payload)
        if not fields or not rows:
            continue
        i_code = _find_col(fields, ["證券代號", "代號"])
        i_pe = _find_col(fields, ["本益比"])
        i_pb = _find_col(fields, ["股價淨值比"])
        i_yield = _find_col(fields, ["殖利率"])
        if i_code is None:
            continue
        out: dict[str, dict[str, Any]] = {}
        for row in rows:
            if not isinstance(row, (list, tuple)) or len(row) <= i_code:
                continue
            c = _code(row[i_code])
            pe = _safe_float(row[i_pe], None) if i_pe is not None and len(row) > i_pe else None
            out[c] = {
                "date": ymd,
                "pe": pe,
                "pb": _safe_float(row[i_pb], None) if i_pb is not None and len(row) > i_pb else None,
                "yield": _safe_float(row[i_yield], None) if i_yield is not None and len(row) > i_yield else None,
                "source": "TWSE_BWIBBU",
            }
        if out:
            _cache_set(cache, key, out)
            return out
    return {}


def _institution_score(row: pd.Series, inst: dict[str, Any] | None) -> tuple[float, dict[str, Any]]:
    """法人籌碼分數。

    V109 優先使用 16_官方因子快取中心產生的官方快取欄位；若沒有，才使用
    godpick_night_strategy 的舊快取/代理資料。這樣 07 不會即時連官方網站。
    """
    proxy = _safe_float(row.get("法人連買代理分數"), 50) or 50
    volume_base = _safe_float(row.get("VOL20"), None) or _safe_float(row.get("成交量"), None)

    official_complete = _safe_float(row.get("官方資料完整度"), 0) or 0
    official_chip_score = _safe_float(row.get("法人籌碼官方分數"), None)
    official_total_5 = _safe_float(row.get("三大法人近5日合計"), None)
    official_foreign_5 = _safe_float(row.get("外資近5日買賣超"), None)
    official_trust_5 = _safe_float(row.get("投信近5日買賣超"), None)
    official_total_1 = _safe_float(row.get("三大法人近1日合計"), None)
    official_foreign_1 = _safe_float(row.get("外資近1日買賣超"), None)
    official_trust_1 = _safe_float(row.get("投信近1日買賣超"), None)
    official_dealer_1 = _safe_float(row.get("自營商近1日買賣超"), None)
    official_days = _safe_float(row.get("法人連買天數"), None)

    has_official = official_complete >= 35 or official_chip_score is not None or any(x is not None for x in [official_total_5, official_foreign_5, official_trust_5])
    if has_official and official_complete < 60:
        return proxy, {
            "外資近1日買賣超": official_foreign_1 if official_foreign_1 is not None else "",
            "投信近1日買賣超": official_trust_1 if official_trust_1 is not None else "",
            "自營商近1日買賣超": official_dealer_1 if official_dealer_1 is not None else "",
            "三大法人近1日合計": official_total_1 if official_total_1 is not None else "",
            "外資近5日買賣超": official_foreign_5 if official_foreign_5 is not None else "",
            "投信近5日買賣超": official_trust_5 if official_trust_5 is not None else "",
            "三大法人近5日合計": official_total_5 if official_total_5 is not None else "",
            "法人連買天數": official_days if official_days is not None else "",
            "官方因子總分": _safe_float(row.get("官方因子總分"), "") or "",
            "官方資料完整度": official_complete,
            "官方因子資料狀態": _safe_str(row.get("官方因子資料狀態")),
            "官方因子更新時間": _safe_str(row.get("官方因子更新時間")),
            "法人買超占量比%": "",
            "法人連買推估": "官方資料完整度<60，僅顯示不加分",
            "籌碼資料來源": "official_factors_cache_display_only",
            "籌碼資料日期": _safe_str(row.get("官方資料日期")),
        }
    if has_official:
        foreign = official_foreign_5 if official_foreign_5 is not None else (official_foreign_1 or 0)
        trust = official_trust_5 if official_trust_5 is not None else (official_trust_1 or 0)
        dealer = official_dealer_1 or 0
        total = official_total_5 if official_total_5 is not None else (official_total_1 if official_total_1 is not None else foreign + trust + dealer)
        buy_ratio = None
        if volume_base not in [None, 0]:
            buy_ratio = total / abs(volume_base) * 100
        if official_chip_score is not None and official_complete >= 45:
            score = official_chip_score
        else:
            score = 50.0
            score += max(min((total or 0) / 3000.0, 18), -18)
            score += max(min((foreign or 0) / 3500.0, 10), -10)
            score += max(min((trust or 0) / 1200.0, 16), -12)
            if official_days is not None:
                score += max(min(official_days * 3.0, 12), -6)
            if buy_ratio is not None:
                score += max(min(buy_ratio * 1.2, 12), -12)
        return _clip(score), {
            "外資近1日買賣超": official_foreign_1 if official_foreign_1 is not None else "",
            "投信近1日買賣超": official_trust_1 if official_trust_1 is not None else "",
            "自營商近1日買賣超": official_dealer_1 if official_dealer_1 is not None else "",
            "三大法人近1日合計": official_total_1 if official_total_1 is not None else "",
            "外資近5日買賣超": foreign,
            "投信近5日買賣超": trust,
            "三大法人近5日合計": total,
            "法人連買天數": official_days if official_days is not None else "",
            "官方因子總分": _safe_float(row.get("官方因子總分"), "") or "",
            "官方資料完整度": official_complete,
            "官方因子資料狀態": _safe_str(row.get("官方因子資料狀態")),
            "官方因子更新時間": _safe_str(row.get("官方因子更新時間")),
            "法人買超占量比%": round(buy_ratio, 2) if buy_ratio is not None else "",
            "法人連買推估": "官方法人偏買" if (total or 0) > 0 else ("官方法人偏賣" if (total or 0) < 0 else "官方中性"),
            "籌碼資料來源": "official_factors_cache",
            "籌碼資料日期": _safe_str(row.get("官方資料日期")),
        }

    if not inst:
        return proxy, {
            "外資近1日買賣超": "",
            "投信近1日買賣超": "",
            "自營商近1日買賣超": "",
            "三大法人近1日合計": "",
            "外資近5日買賣超": "",
            "投信近5日買賣超": "",
            "三大法人近5日合計": "",
            "法人連買天數": "",
            "官方因子總分": _safe_float(row.get("官方因子總分"), "") or "",
            "官方資料完整度": official_complete if official_complete else "",
            "官方因子資料狀態": _safe_str(row.get("官方因子資料狀態")),
            "官方因子更新時間": _safe_str(row.get("官方因子更新時間")),
            "法人買超占量比%": "",
            "法人連買推估": "代理",
            "籌碼資料來源": "技術量價代理",
            "籌碼資料日期": "",
        }
    foreign = _safe_float(inst.get("foreign"), 0) or 0
    trust = _safe_float(inst.get("trust"), 0) or 0
    dealer = _safe_float(inst.get("dealer"), 0) or 0
    total = _safe_float(inst.get("total"), foreign + trust + dealer) or 0
    buy_ratio = None
    if volume_base not in [None, 0]:
        buy_ratio = total / abs(volume_base) * 100

    score = 50.0
    score += max(min(total / 600.0, 18), -18)
    score += max(min(foreign / 800.0, 10), -10)
    score += max(min(trust / 250.0, 16), -12)
    if buy_ratio is not None:
        score += max(min(buy_ratio * 1.5, 12), -12)
    if trust > 0 and foreign > 0:
        score += 8
    if total > 0 and _safe_float(row.get("近5日漲幅%"), 0) and (_safe_float(row.get("近5日漲幅%"), 0) or 0) < 5:
        score += 4
    return _clip(score), {
        "外資近1日買賣超": foreign,
        "投信近1日買賣超": trust,
        "自營商近1日買賣超": dealer,
        "三大法人近1日合計": total,
        "外資近5日買賣超": "",
        "投信近5日買賣超": "",
        "三大法人近5日合計": "",
        "法人連買天數": "",
        "官方因子總分": _safe_float(row.get("官方因子總分"), "") or "",
        "官方資料完整度": official_complete if official_complete else "",
        "官方因子資料狀態": _safe_str(row.get("官方因子資料狀態")),
        "官方因子更新時間": _safe_str(row.get("官方因子更新時間")),
        "法人買超占量比%": round(buy_ratio, 2) if buy_ratio is not None else "",
        "法人連買推估": "法人偏買" if total > 0 else ("法人偏賣" if total < 0 else "中性"),
        "籌碼資料來源": inst.get("source") or "TWSE",
        "籌碼資料日期": inst.get("date") or "",
    }


def _valuation_score(row: pd.Series, val: dict[str, Any] | None) -> tuple[float, dict[str, Any]]:
    close = _safe_float(row.get("最新價") or row.get("推薦價格") or row.get("推薦日價格"))
    proxy_eps = _safe_float(row.get("EPS代理分數"), 50) or 50
    proxy_profit = _safe_float(row.get("獲利代理分數"), 50) or 50

    official_complete = _safe_float(row.get("官方資料完整度"), 0) or 0
    official_val_score = _safe_float(row.get("官方估值風險分數"), None)
    pe = _safe_float(row.get("PER本益比"), None)
    pb = _safe_float(row.get("PBR股價淨值比"), None)
    yld = _safe_float(row.get("股利殖利率%"), None)
    est_eps = _safe_float(row.get("估算EPS"), None)

    if pe is None and val:
        pe = _safe_float(val.get("pe"), None)
    if est_eps is None and pe not in [None, 0] and close not in [None, 0]:
        est_eps = close / pe

    if official_val_score is not None and official_complete >= 60:
        return _clip(official_val_score), {
            "PER本益比": round(pe, 2) if pe is not None else "",
            "PBR股價淨值比": round(pb, 2) if pb is not None else "",
            "股利殖利率%": round(yld, 2) if yld is not None else "",
            "估算EPS": round(est_eps, 2) if est_eps is not None else "",
            "基本面資料來源": "official_factors_cache",
            "基本面資料日期": _safe_str(row.get("官方資料日期")),
        }

    if pe is None:
        score = _clip(35 + proxy_eps * 0.35 + proxy_profit * 0.25)
        return score, {
            "PER本益比": "",
            "PBR股價淨值比": round(pb, 2) if pb is not None else "",
            "股利殖利率%": round(yld, 2) if yld is not None else "",
            "估算EPS": "",
            "基本面資料來源": "技術獲利代理",
            "基本面資料日期": "",
        }

    score = 50.0
    if 8 <= pe <= 18:
        score += 20
    elif 18 < pe <= 28:
        score += 10
    elif 28 < pe <= 45:
        score -= 2
    elif pe > 45:
        score -= 16
    elif pe > 0 and pe < 8:
        score += 8

    score += (proxy_eps - 50) * 0.20
    score += (proxy_profit - 50) * 0.16
    return _clip(score), {
        "PER本益比": round(pe, 2) if pe is not None else "",
        "PBR股價淨值比": round(pb, 2) if pb is not None else "",
        "股利殖利率%": round(yld, 2) if yld is not None else "",
        "估算EPS": round(est_eps, 2) if est_eps is not None else "",
        "基本面資料來源": (val.get("source") if val else "TWSE_BWIBBU") or "TWSE_BWIBBU",
        "基本面資料日期": (val.get("date") if val else "") or "",
    }


def _row_strategy(row: pd.Series, inst_map: dict[str, Any], val_map: dict[str, Any]) -> dict[str, Any]:
    code = _code(row.get("股票代號"))
    close = _safe_float(row.get("最新價") or row.get("推薦價格") or row.get("推薦日價格"))
    support = _safe_float(row.get("近端支撐"), _safe_float(row.get("主要支撐")))
    main_support = _safe_float(row.get("主要支撐"), support)
    resistance = _safe_float(row.get("近端壓力"), _safe_float(row.get("賣出目標1")))
    breakout = _safe_float(row.get("突破確認價"), _safe_float(row.get("推薦買點_突破"), resistance))
    pullback = _safe_float(row.get("推薦買點_拉回"), support)
    stop = _safe_float(row.get("停損價"), _safe_float(row.get("停損參考")))

    tech = _safe_float(row.get("技術結構分數"), 50) or 50
    volume = _safe_float(row.get("量能啟動分"), _safe_float(row.get("爆發力分數"), 50)) or 50
    prelaunch = _safe_float(row.get("起漲前兆分數"), 50) or 50
    trade = _safe_float(row.get("交易可行分數"), 50) or 50
    sector = _safe_float(row.get("族群資金流分數"), _safe_float(row.get("類股熱度分數"), 50)) or 50
    market = _safe_float(row.get("大盤橋接分數"), _safe_float(row.get("市場環境分數"), 50)) or 50
    score_now = _safe_float(row.get("推薦總分"), 0) or 0
    chase = _safe_float(row.get("追價風險分"), _safe_float(row.get("追高風險分數_決策"), 50)) or 50
    ret5 = _safe_float(row.get("近5日漲幅%"), _safe_float(row.get("區間漲跌幅%"), 0)) or 0
    opportunity = _safe_float(row.get("機會股分數"), 50) or 50
    lock_proxy = _safe_float(row.get("大戶鎖碼代理分數"), 50) or 50
    revenue_proxy = _safe_float(row.get("營收動能代理分數"), 50) or 50
    eps_proxy = _safe_float(row.get("EPS代理分數"), 50) or 50
    profit_proxy = _safe_float(row.get("獲利代理分數"), 50) or 50

    inst_score, inst_fields = _institution_score(row, inst_map.get(code))
    valuation_score, val_fields = _valuation_score(row, val_map.get(code))
    official_revenue_score = _safe_float(row.get("營收成長官方分數"), None)
    official_basic_score = _safe_float(row.get("官方基本面成長分數"), None)
    official_complete = _safe_float(row.get("官方資料完整度"), 0) or 0
    if official_revenue_score is not None and official_complete >= 60:
        revenue_score = _clip(official_revenue_score)
    else:
        revenue_score = _clip(35 + revenue_proxy * 0.55 + max(min((sector - 50) * 0.15, 8), -8))
    if official_basic_score is not None and official_complete >= 60:
        fundamental = _clip(official_basic_score)
        eps_score = _clip(fundamental * 0.55 + valuation_score * 0.20 + eps_proxy * 0.25)
    else:
        eps_score = _clip(30 + eps_proxy * 0.44 + profit_proxy * 0.32 + max(min((valuation_score - 50) * 0.12, 8), -8))
        fundamental = _clip(revenue_score * 0.42 + eps_score * 0.40 + valuation_score * 0.18)
    lock_score = _clip(lock_proxy * 0.70 + (100 - chase) * 0.15 + tech * 0.15)
    trend_score = _clip(tech * 0.50 + prelaunch * 0.28 + opportunity * 0.12 + sector * 0.10)
    momentum_score = _clip(volume * 0.45 + prelaunch * 0.28 + sector * 0.15 + max(min(ret5 * 1.8, 10), -10))

    support_dist = None
    pressure_space = None
    if close not in [None, 0] and support not in [None, 0]:
        support_dist = (close - support) / support * 100
    if close not in [None, 0] and resistance not in [None, 0]:
        pressure_space = (resistance - close) / close * 100

    night_total = _clip(
        trend_score * 0.25
        + momentum_score * 0.20
        + inst_score * 0.20
        + fundamental * 0.20
        + valuation_score * 0.10
        + market * 0.05
    )
    next_entry = _clip(
        trade * 0.22
        + opportunity * 0.20
        + prelaunch * 0.18
        + inst_score * 0.12
        + (100 - chase) * 0.16
        + (valuation_score * 0.05)
        + (market * 0.07)
    )
    if support_dist is not None:
        if -1.0 <= support_dist <= 4.0:
            next_entry += 8
        elif support_dist > 10:
            next_entry -= 8
    if pressure_space is not None:
        if pressure_space >= 8:
            next_entry += 5
        elif pressure_space < 3:
            next_entry -= 8
    if ret5 >= 12:
        next_entry -= 10
    next_entry = _clip(next_entry)

    swing = _clip(night_total * 0.45 + fundamental * 0.25 + inst_score * 0.15 + sector * 0.10 + lock_score * 0.05)

    # price plan
    if pullback in [None, 0]:
        pullback = support if support not in [None, 0] else close
    if breakout in [None, 0]:
        breakout = resistance if resistance not in [None, 0] else (close * 1.015 if close else None)
    if stop in [None, 0]:
        if main_support not in [None, 0]:
            stop = main_support * 0.975
        elif close not in [None, 0]:
            stop = close * 0.94
    if resistance in [None, 0] and close not in [None, 0]:
        resistance = close * 1.06

    overheated = chase >= 75 or ret5 >= 12 or (support_dist is not None and support_dist > 10)
    near_breakout = pressure_space is not None and -1.5 <= pressure_space <= 5.0
    near_support = support_dist is not None and -1.0 <= support_dist <= 5.0

    if overheated:
        pattern = "過熱等待型"
        action = "等拉回，不追高"
        observe = "3～5日"
    elif near_breakout and prelaunch >= 65 and inst_score >= 55:
        pattern = "隔日突破型"
        action = "放量站上突破價再小量"
        observe = "隔日～3日"
    elif near_support and trade >= 60:
        pattern = "回測承接型"
        action = "支撐不破分批觀察"
        observe = "1～3日"
    elif prelaunch >= 70 and ret5 < 8 and night_total >= 72:
        pattern = "剛起漲型"
        action = "不開高可小量分批"
        observe = "1～5日"
    elif night_total >= 76 and swing >= 72:
        pattern = "波段潛伏型"
        action = "列入隔日優先觀察"
        observe = "3～10日"
    else:
        pattern = "觀察確認型"
        action = "等突破或回測確認"
        observe = "1～5日"

    if pattern == "隔日突破型":
        entry_text = f"突破 { _fmt_price(breakout) } 後，量能確認再試單"
    elif pattern in {"回測承接型", "過熱等待型"}:
        entry_text = f"回測 { _fmt_price(pullback or support) }～{ _fmt_price(close) } 不破再分批"
    elif pattern == "剛起漲型":
        entry_text = f"{ _fmt_price(close) } 附近或小拉回至 { _fmt_price(pullback or support) }"
    else:
        entry_text = f"突破 { _fmt_price(breakout) } 或回測 { _fmt_price(pullback or support) } 確認"

    data_points = 0
    if inst_fields.get("籌碼資料來源") != "技術量價代理":
        data_points += 1
    if val_fields.get("基本面資料來源") != "技術獲利代理":
        data_points += 1
    completeness = "高" if data_points >= 2 else ("中" if data_points == 1 else "代理估算")

    reasons = [
        f"技術{trend_score:.1f}", f"量價{momentum_score:.1f}", f"法人{inst_score:.1f}",
        f"基本面{fundamental:.1f}", f"估值{valuation_score:.1f}", f"隔日進場{next_entry:.1f}",
    ]
    risk = []
    if overheated:
        risk.append("短線過熱，禁止無腦追高")
    if pressure_space is not None and pressure_space < 3:
        risk.append("距離壓力過近")
    if inst_score < 45:
        risk.append("法人籌碼偏弱或資料不足")
    if valuation_score < 45:
        risk.append("估值/獲利代理偏弱")
    if not risk:
        risk.append("依停損價控管，隔日仍需看開盤量價")

    strategy = (
        f"{pattern}｜{action}｜預估進場：{entry_text}｜"
        f"突破確認：{_fmt_price(breakout)}｜回測承接：{_fmt_price(pullback or support)}｜"
        f"停損：{_fmt_price(stop)}｜第一壓力：{_fmt_price(resistance)}"
    )

    return {
        "夜間股神版本": ENGINE_VERSION,
        "夜間股神總分": round(night_total, 2),
        "隔日進場分數": round(next_entry, 2),
        "波段潛力分數": round(swing, 2),
        "技術趨勢分數": round(trend_score, 2),
        "量價動能分數": round(momentum_score, 2),
        "法人籌碼分數": round(inst_score, 2),
        "大戶鎖碼分數": round(lock_score, 2),
        "基本面成長分數": round(fundamental, 2),
        "營收成長分數": round(revenue_score, 2),
        "EPS成長分數": round(eps_score, 2),
        "估值風險分數": round(valuation_score, 2),
        **inst_fields,
        **val_fields,
        "月營收YoY%": _safe_float(row.get("月營收YoY%"), "") or "",
        "月營收MoM%": _safe_float(row.get("月營收MoM%"), "") or "",
        "累計營收YoY%": _safe_float(row.get("累計營收YoY%"), "") or "",
        "資料完整度": ("官方快取" + str(int(official_complete)) + "%" if official_complete >= 60 else completeness),
        "進場型態_隔日": pattern,
        "隔日建議動作": action,
        "預估進場點": entry_text,
        "回測承接價": round((pullback or support), 2) if (pullback or support) not in [None, 0] else "",
        "突破確認價_隔日": round(breakout, 2) if breakout not in [None, 0] else "",
        "停損價_隔日": round(stop, 2) if stop not in [None, 0] else "",
        "第一壓力價": round(resistance, 2) if resistance not in [None, 0] else "",
        "觀察週期": observe,
        "夜間股神建議": strategy,
        "隔日作戰策略": strategy,
        "進場條件說明": "；".join(reasons),
        "不追高條件": "開高超過突破價太多、量能未放大、跌破支撐或大盤轉弱時不追。",
        "夜間風險提醒": "；".join(risk),
    }


def enrich_night_strategy(df: pd.DataFrame | None) -> pd.DataFrame:
    """Add night / next-entry strategy columns to recommendation DataFrame."""
    if df is None:
        return pd.DataFrame(columns=NIGHT_COLUMNS)
    if not isinstance(df, pd.DataFrame) or df.empty:
        out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
        for c in NIGHT_COLUMNS:
            if c not in out.columns:
                out[c] = ""
        return out

    out = df.copy()
    cache = _load_cache()
    inst_map = _fetch_twse_institution(cache)
    val_map = _fetch_twse_valuation(cache)
    _save_cache(cache)

    rows = []
    for _, r in out.iterrows():
        try:
            rows.append(_row_strategy(r, inst_map, val_map))
        except Exception as e:
            rows.append({
                "夜間股神版本": ENGINE_VERSION,
                "資料完整度": "計算失敗",
                "夜間風險提醒": f"夜間策略計算失敗：{e}",
            })
    enrich = pd.DataFrame(rows, index=out.index)
    for c in NIGHT_COLUMNS:
        if c not in enrich.columns:
            enrich[c] = ""
    for c in enrich.columns:
        out[c] = enrich[c]

    # Do not remove or overwrite the original score. Add a separate practical score for sort/export.
    try:
        original = pd.to_numeric(out.get("推薦總分"), errors="coerce").fillna(0)
        night = pd.to_numeric(out.get("夜間股神總分"), errors="coerce").fillna(0)
        entry = pd.to_numeric(out.get("隔日進場分數"), errors="coerce").fillna(0)
        out["隔日實戰排序分"] = (original * 0.45 + night * 0.35 + entry * 0.20).clip(lower=0, upper=100).round(2)
    except Exception:
        out["隔日實戰排序分"] = out.get("推薦總分", "")
    return out
