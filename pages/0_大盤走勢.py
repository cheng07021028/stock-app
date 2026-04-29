# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Any
import json
import threading
import time

import pandas as pd
import requests
import urllib3
import streamlit as st

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    from utils import inject_pro_theme, render_pro_hero, render_pro_kpi_row, render_pro_info_card
except Exception:
    def inject_pro_theme():
        pass

    def render_pro_hero(title: str, subtitle: str = ""):
        st.title(title)
        if subtitle:
            st.caption(subtitle)

    def render_pro_kpi_row(items):
        cols = st.columns(len(items) if items else 1)
        for col, item in zip(cols, items):
            with col:
                st.metric(item.get("label", ""), item.get("value", ""), item.get("delta", ""))

    def render_pro_info_card(title: str, pairs, chips=None):
        st.subheader(title)
        for a, b, *_ in pairs:
            st.write(f"**{a}**：{b}")

PAGE_TITLE = "大盤走勢"
PFX = "macro_safe_"
CACHE_FILE = "macro_market_close_cache.json"
INST_CACHE_FILE = "macro_institutional_cache.json"
US_CACHE_FILE = "macro_us_market_cache.json"
TAIFEX_CACHE_FILE = "macro_taifex_cache.json"
MACRO_BG_JOB_FILE = "macro_background_jobs.json"
MACRO_AUTO_REFRESH_SECONDS = 1800
MACRO_BG_MAX_RUNNING_SECONDS = 90


def _k(key: str) -> str:
    return f"{PFX}{key}"


def _tw_now() -> datetime:
    # Streamlit Cloud 常是 UTC，這裡轉台灣時間。
    return datetime.utcnow() + timedelta(hours=8)


def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def _safe_float(v: Any, default: float | None = None):
    try:
        if v is None:
            return default
        if isinstance(v, str):
            v = v.replace(",", "").replace("+", "").replace("%", "").strip()
            if not v or v in {"-", "--", "nan", "None"}:
                return default
        return float(v)
    except Exception:
        return default


def _read_cache() -> dict[str, Any]:
    p = Path(CACHE_FILE)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_cache(cache: dict[str, Any]) -> None:
    try:
        Path(CACHE_FILE).write_text(json.dumps(cache, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    except Exception:
        pass


def _read_inst_cache() -> dict[str, Any]:
    p = Path(INST_CACHE_FILE)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_inst_cache(cache: dict[str, Any]) -> None:
    try:
        Path(INST_CACHE_FILE).write_text(json.dumps(cache, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    except Exception:
        pass


def _extract_money_100m(v: Any):
    """TWSE 金額通常為元，轉成億元。"""
    s = _safe_str(v).replace(",", "").replace("+", "").replace(" ", "")
    if not s or s in {"-", "--", "None", "nan"}:
        return None
    try:
        return float(s) / 100000000
    except Exception:
        return None


def _read_us_cache() -> dict[str, Any]:
    p = Path(US_CACHE_FILE)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_us_cache(cache: dict[str, Any]) -> None:
    try:
        Path(US_CACHE_FILE).write_text(json.dumps(cache, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    except Exception:
        pass


def _fetch_yahoo_chart(symbol: str, target_date: date, timeout: float = 3.0) -> dict[str, Any]:
    try:
        target_dt = pd.to_datetime(target_date)
        start_dt = target_dt - pd.Timedelta(days=10)
        period1 = int(start_dt.timestamp())
        period2 = int((target_dt + pd.Timedelta(days=2)).timestamp())
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{requests.utils.quote(symbol, safe='^=')}"
        r = requests.get(
            url,
            params={"period1": period1, "period2": period2, "interval": "1d", "includePrePost": "false", "events": "div,splits"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=timeout,
            verify=False,
        )
        if r.status_code != 200:
            return {"ok": False, "symbol": symbol, "error": f"HTTP {r.status_code}"}
        data = r.json()
        result = (((data or {}).get("chart") or {}).get("result") or [{}])[0]
        timestamps = result.get("timestamp") or []
        quote = (((result.get("indicators") or {}).get("quote") or [{}])[0]) or {}
        if not timestamps or not quote:
            return {"ok": False, "symbol": symbol, "error": "no data"}

        df = pd.DataFrame({
            "date": pd.to_datetime(timestamps, unit="s", utc=True).tz_convert("Asia/Taipei").tz_localize(None),
            "open": quote.get("open", []),
            "high": quote.get("high", []),
            "low": quote.get("low", []),
            "close": quote.get("close", []),
            "volume": quote.get("volume", []),
        }).dropna(subset=["close"]).sort_values("date")

        if df.empty:
            return {"ok": False, "symbol": symbol, "error": "empty close"}
        df = df[df["date"] <= (target_dt + pd.Timedelta(days=1))]
        if df.empty:
            return {"ok": False, "symbol": symbol, "error": "no date before target"}

        row = df.iloc[-1]
        prev = df.iloc[-2]["close"] if len(df) >= 2 else None
        close = _safe_float(row.get("close"))
        pct = ((close - prev) / prev * 100) if close is not None and prev not in [None, 0] else None

        return {
            "ok": True,
            "symbol": symbol,
            "date": pd.to_datetime(row.get("date")).strftime("%Y-%m-%d"),
            "close": close,
            "pct": pct,
            "source": "Yahoo 手動外盤",
            "updated_at": _tw_now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    except Exception as e:
        return {"ok": False, "symbol": symbol, "error": str(e)}


def _fetch_us_market_manual(target_date: date) -> tuple[int, list[str]]:
    symbols = {
        "NASDAQ": "^IXIC",
        "SOX半導體": "^SOX",
        "S&P500": "^GSPC",
        "VIX": "^VIX",
        "台積電ADR": "TSM",
        "美元台幣": "TWD=X",
    }
    cache = _read_us_cache()
    ymd = pd.to_datetime(target_date).strftime("%Y%m%d")
    if ymd not in cache or not isinstance(cache.get(ymd), dict):
        cache[ymd] = {}

    added = 0
    msgs = []
    for name, symbol in symbols.items():
        row = _fetch_yahoo_chart(symbol, target_date, timeout=3.0)
        if row.get("ok"):
            cache[ymd][name] = row
            added += 1
            msgs.append(f"{name} {row.get('date')} 收盤 {row.get('close')} / {_safe_float(row.get('pct'), 0):+.2f}%")
        else:
            msgs.append(f"{name} 失敗：{row.get('error')}")
    _write_us_cache(cache)
    return added, msgs


def _default_us_market_row(target_date: date) -> dict[str, Any]:
    cache = _read_us_cache()
    ymd = pd.to_datetime(target_date).strftime("%Y%m%d")
    if isinstance(cache.get(ymd), dict):
        return cache[ymd]
    if cache:
        keys = sorted([k for k in cache.keys() if isinstance(cache.get(k), dict)], reverse=True)
        if keys:
            return cache[keys[0]]
    return {}


def _us_market_score(us_row: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(us_row, dict) or not us_row:
        return {"外盤分數": 50.0, "外盤狀態": "尚未更新", "外盤建議": "未納入外盤資料"}

    nas = _safe_float((us_row.get("NASDAQ") or {}).get("pct"), 0) or 0
    sox = _safe_float((us_row.get("SOX半導體") or {}).get("pct"), 0) or 0
    spx = _safe_float((us_row.get("S&P500") or {}).get("pct"), 0) or 0
    adr = _safe_float((us_row.get("台積電ADR") or {}).get("pct"), 0) or 0
    vix = _safe_float((us_row.get("VIX") or {}).get("pct"), 0) or 0

    score = 50
    score += max(min(nas * 4, 10), -10)
    score += max(min(sox * 5, 14), -14)
    score += max(min(spx * 3, 8), -8)
    score += max(min(adr * 4, 10), -10)
    score -= max(min(vix * 0.8, 8), -8)
    score = max(0, min(100, score))

    if score >= 70:
        label = "外盤順風"
        advice = "有利電子與半導體強勢股延續。"
    elif score >= 55:
        label = "外盤中性偏多"
        advice = "可正常觀察，但仍看台股量價。"
    elif score >= 45:
        label = "外盤中性"
        advice = "外盤未提供明確方向。"
    elif score >= 30:
        label = "外盤偏弱"
        advice = "降低隔日追價，優先等拉回。"
    else:
        label = "外盤逆風"
        advice = "隔日風險升高，保守控倉。"
    return {"外盤分數": round(score, 1), "外盤狀態": label, "外盤建議": advice}


def _us_cache_to_df() -> pd.DataFrame:
    cache = _read_us_cache()
    rows = []
    for ymd, payload in (cache or {}).items():
        if not isinstance(payload, dict):
            continue
        row = {"日期": ymd}
        for name, item in payload.items():
            if not isinstance(item, dict):
                continue
            row[f"{name}_收盤"] = item.get("close")
            row[f"{name}_漲跌幅%"] = item.get("pct")
            row[f"{name}_資料日"] = item.get("date")
        rows.append(row)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
    return df.dropna(subset=["日期"]).sort_values("日期").reset_index(drop=True)


def _fetch_twse_institutional_manual(target_date: date, timeout: float = 2.5) -> dict[str, Any]:
    """
    v28.3：法人籌碼多來源自動備援版。
    順序：
    1. TWSE BFI82U 三大法人買賣超金額。
    2. TWSE T86 個股法人買賣超加總代理。
    3. FinMind TaiwanStockInstitutionalInvestorsBuySell 代理。
    4. 指定日無資料，自動往前找最近 15 個工作日。
    5. 只在手動按鈕或背景 thread 中執行，不阻塞主畫面。
    """
    target_dt = pd.to_datetime(target_date).date()

    def _candidate_dates(end_date: date, max_days: int = 15) -> list[date]:
        out = []
        cur = end_date
        guard = 0
        while len(out) < max_days and guard < 35:
            if cur.weekday() < 5:
                out.append(cur)
            cur = cur - timedelta(days=1)
            guard += 1
        return out

    def _parse_money_payload(data: dict[str, Any], used_date: date, source_name: str) -> dict[str, Any]:
        rows = []
        if isinstance(data, dict):
            rows = data.get("data") or data.get("aaData") or []
            if not rows and isinstance(data.get("tables"), list):
                for t in data.get("tables") or []:
                    if isinstance(t, dict) and isinstance(t.get("data"), list):
                        rows.extend(t.get("data") or [])

        if not isinstance(rows, list) or not rows:
            return {}

        foreign = None
        invest = None
        dealer = None
        total = None
        raw_rows = []

        for row in rows:
            if not isinstance(row, list) or len(row) < 2:
                continue
            label = _safe_str(row[0])
            joined = " ".join(_safe_str(x) for x in row)
            nums = [_extract_money_100m(x) for x in row[1:]]
            nums = [x for x in nums if x is not None]
            if not nums:
                continue
            val = nums[-1]
            raw_rows.append({"項目": label or joined[:20], "買賣超億元": val, "原始列": row})

            if "外資" in label or "外資" in joined:
                foreign = val
            elif "投信" in label or "投信" in joined:
                invest = val
            elif "自營商" in label or "自營商" in joined:
                dealer = val
            elif "合計" in label or "總計" in label or "三大法人" in label or "合計" in joined:
                total = val

        calc_total = sum([x for x in [foreign, invest, dealer] if x is not None]) if any(x is not None for x in [foreign, invest, dealer]) else total
        if total is None:
            total = calc_total

        if any(x is not None for x in [foreign, invest, dealer, total]):
            return {
                "ok": True,
                "date": pd.to_datetime(used_date).strftime("%Y-%m-%d"),
                "source": source_name,
                "unit": "億元",
                "foreign_100m": foreign,
                "investment_trust_100m": invest,
                "dealer_100m": dealer,
                "total_100m": total,
                "raw_rows": raw_rows,
                "updated_at": _tw_now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        return {}

    def _to_float_plain(v):
        s = _safe_str(v).replace(",", "").replace("+", "")
        if not s or s in {"-", "--", "None", "nan"}:
            return None
        try:
            return float(s)
        except Exception:
            return None

    def _parse_t86_proxy(data: dict[str, Any], used_date: date, source_name: str = "TWSE T86 法人方向代理") -> dict[str, Any]:
        rows = []
        fields = []
        if isinstance(data, dict):
            rows = data.get("data") or data.get("aaData") or []
            fields = data.get("fields") or data.get("stat") or []
        if not isinstance(rows, list) or not rows:
            return {}

        foreign_shares = 0.0
        invest_shares = 0.0
        dealer_shares = 0.0
        used = 0

        field_names = [_safe_str(x) for x in fields] if isinstance(fields, list) else []
        for row in rows:
            if not isinstance(row, list):
                continue
            used += 1
            row_map = {field_names[i]: row[i] for i in range(min(len(field_names), len(row)))}

            f_val = None
            i_val = None
            d_val = None
            for k, v in row_map.items():
                kk = _safe_str(k)
                if f_val is None and ("外陸資買賣超" in kk or "外資買賣超" in kk):
                    f_val = _to_float_plain(v)
                if i_val is None and "投信買賣超" in kk:
                    i_val = _to_float_plain(v)
                if d_val is None and "自營商買賣超" in kk:
                    d_val = _to_float_plain(v)

            if f_val is None and len(row) > 4:
                f_val = _to_float_plain(row[4])
            if i_val is None and len(row) > 7:
                i_val = _to_float_plain(row[7])
            if d_val is None and len(row) > 10:
                d_val = _to_float_plain(row[10])

            foreign_shares += f_val or 0
            invest_shares += i_val or 0
            dealer_shares += d_val or 0

        if used <= 0:
            return {}

        foreign_100m_share = foreign_shares / 100000000
        invest_100m_share = invest_shares / 100000000
        dealer_100m_share = dealer_shares / 100000000
        total_proxy = foreign_100m_share + invest_100m_share + dealer_100m_share

        if abs(total_proxy) < 0.000001 and abs(foreign_100m_share) < 0.000001 and abs(invest_100m_share) < 0.000001 and abs(dealer_100m_share) < 0.000001:
            return {}

        return {
            "ok": True,
            "date": pd.to_datetime(used_date).strftime("%Y-%m-%d"),
            "source": source_name,
            "unit": "億股代理",
            "is_proxy": True,
            "foreign_100m": foreign_100m_share,
            "investment_trust_100m": invest_100m_share,
            "dealer_100m": dealer_100m_share,
            "total_100m": total_proxy,
            "raw_rows": [{"項目": "全市場加總", "樣本數": used, "單位": "億股代理"}],
            "updated_at": _tw_now().strftime("%Y-%m-%d %H:%M:%S"),
            "note": "三大法人金額不可用，已改用個股法人買賣超股數作方向代理。"
        }

    def _fetch_finmind_proxy(used_date: date) -> dict[str, Any]:
        """
        FinMind 備援：無 token 也可嘗試；若被限流則略過。
        以 buy-sell 股數加總，轉成億股代理。
        """
        day = pd.to_datetime(used_date).strftime("%Y-%m-%d")
        urls = [
            "https://api.finmindtrade.com/api/v4/data",
            "https://api.finmindtrade.com/api/v3/data",
        ]
        params = {
            "dataset": "TaiwanStockInstitutionalInvestorsBuySell",
            "start_date": day,
            "end_date": day,
        }
        for url in urls:
            try:
                r = requests.get(url, params=params, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout, verify=False)
                if r.status_code != 200:
                    continue
                data = r.json()
                rows = data.get("data") if isinstance(data, dict) else []
                if not isinstance(rows, list) or not rows:
                    continue

                foreign = 0.0
                invest = 0.0
                dealer = 0.0
                used = 0
                for item in rows:
                    if not isinstance(item, dict):
                        continue
                    name = _safe_str(item.get("name") or item.get("institutional_investors"))
                    buy = _safe_float(item.get("buy"), 0) or 0
                    sell = _safe_float(item.get("sell"), 0) or 0
                    net = buy - sell
                    used += 1
                    lname = name.lower()
                    if "foreign" in lname or "外資" in name:
                        foreign += net
                    elif "investment" in lname or "trust" in lname or "投信" in name:
                        invest += net
                    elif "dealer" in lname or "自營" in name:
                        dealer += net

                if used <= 0:
                    continue
                f = foreign / 100000000
                i = invest / 100000000
                d = dealer / 100000000
                total = f + i + d
                if abs(total) < 0.000001 and abs(f) < 0.000001 and abs(i) < 0.000001 and abs(d) < 0.000001:
                    continue
                return {
                    "ok": True,
                    "date": day,
                    "source": "FinMind 法人方向代理",
                    "unit": "億股代理",
                    "is_proxy": True,
                    "foreign_100m": f,
                    "investment_trust_100m": i,
                    "dealer_100m": d,
                    "total_100m": total,
                    "raw_rows": [{"項目": "FinMind全市場加總", "樣本數": used, "單位": "億股代理"}],
                    "updated_at": _tw_now().strftime("%Y-%m-%d %H:%M:%S"),
                    "note": "TWSE法人資料不可用，已改用FinMind法人買賣超股數作方向代理。",
                }
            except Exception:
                continue
        return {}

    tried = []
    for d in _candidate_dates(target_dt, max_days=15):
        ymd = pd.to_datetime(d).strftime("%Y%m%d")

        bfi_urls = [
            f"https://www.twse.com.tw/rwd/zh/fund/BFI82U?dayDate={ymd}&weekDate={ymd}&monthDate={ymd}&type=day&response=json",
            f"https://www.twse.com.tw/fund/BFI82U?dayDate={ymd}&weekDate={ymd}&monthDate={ymd}&type=day&response=json",
            f"https://www.twse.com.tw/rwd/zh/fund/BFI82U?date={ymd}&type=day&response=json",
        ]
        for url in bfi_urls:
            tried.append(f"{ymd} BFI82U")
            try:
                r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout, verify=False)
                if r.status_code != 200:
                    continue
                parsed = _parse_money_payload(r.json(), d, "TWSE 三大法人")
                if parsed:
                    if d != target_dt:
                        parsed["source"] = "TWSE 三大法人｜最近可用日"
                        parsed["note"] = f"指定日期 {pd.to_datetime(target_dt).strftime('%Y-%m-%d')} 尚無資料，已使用最近可用日 {pd.to_datetime(d).strftime('%Y-%m-%d')}"
                    return parsed
            except Exception:
                continue

        t86_urls = [
            f"https://www.twse.com.tw/rwd/zh/fund/T86?date={ymd}&selectType=ALLBUT0999&response=json",
            f"https://www.twse.com.tw/fund/T86?response=json&date={ymd}&selectType=ALLBUT0999",
            f"https://www.twse.com.tw/rwd/zh/fund/T86?date={ymd}&selectType=ALL&response=json",
        ]
        for url in t86_urls:
            tried.append(f"{ymd} T86代理")
            try:
                r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout, verify=False)
                if r.status_code != 200:
                    continue
                parsed = _parse_t86_proxy(r.json(), d)
                if parsed:
                    if d != target_dt:
                        parsed["source"] = "TWSE T86 法人方向代理｜最近可用日"
                        parsed["note"] = f"指定日期 {pd.to_datetime(target_dt).strftime('%Y-%m-%d')} 尚無資料，已使用最近可用日 {pd.to_datetime(d).strftime('%Y-%m-%d')}"
                    return parsed
            except Exception:
                continue

        tried.append(f"{ymd} FinMind代理")
        parsed = _fetch_finmind_proxy(d)
        if parsed:
            if d != target_dt:
                parsed["source"] = "FinMind 法人方向代理｜最近可用日"
                parsed["note"] = f"指定日期 {pd.to_datetime(target_dt).strftime('%Y-%m-%d')} 尚無資料，已使用最近可用日 {pd.to_datetime(d).strftime('%Y-%m-%d')}"
            return parsed

    return {
        "ok": False,
        "date": pd.to_datetime(target_date).strftime("%Y-%m-%d"),
        "source": "TWSE / FinMind 法人代理",
        "error": "法人資料尚未取得；BFI82U金額、T86代理、FinMind代理皆失敗。可能是TWSE/FinMind暫無資料、端點限制或連線失敗。",
        "tried": tried[-18:],
    }

def _save_inst_row(row: dict[str, Any]) -> None:
    if not isinstance(row, dict) or not row.get("ok"):
        return
    dt = pd.to_datetime(row.get("date") or _tw_now().date(), errors="coerce")
    if pd.isna(dt):
        dt = pd.Timestamp(_tw_now().date())
    ymd = dt.strftime("%Y%m%d")
    cache = _read_inst_cache()
    cache[ymd] = row
    _write_inst_cache(cache)


def _default_inst_row(target_date: date) -> dict[str, Any]:
    cache = _read_inst_cache()
    ymd = pd.to_datetime(target_date).strftime("%Y%m%d")
    if isinstance(cache.get(ymd), dict):
        return cache[ymd]
    if cache:
        keys = sorted([k for k in cache.keys() if isinstance(cache.get(k), dict)], reverse=True)
        if keys:
            row = dict(cache[keys[0]])
            row["source"] = _safe_str(row.get("source")) + "｜最近快取"
            return row
    return {"ok": False, "date": pd.to_datetime(target_date).strftime("%Y-%m-%d"), "source": "尚未更新", "total_100m": None}


def _institutional_score(inst: dict[str, Any]) -> dict[str, Any]:
    total = _safe_float(inst.get("total_100m"), 0) or 0
    foreign = _safe_float(inst.get("foreign_100m"), 0) or 0
    invest = _safe_float(inst.get("investment_trust_100m"), 0) or 0
    score = 50
    score += max(min(total * 0.25, 18), -18)
    score += max(min(foreign * 0.18, 12), -12)
    score += max(min(invest * 0.35, 10), -10)
    score = max(0, min(100, score))

    if score >= 70:
        label = "法人偏多"
        advice = "可提高順勢與回測承接權重。"
    elif score >= 55:
        label = "法人中性偏多"
        advice = "可正常觀察，但仍看技術確認。"
    elif score >= 45:
        label = "法人中性"
        advice = "法人沒有明顯方向，回到個股條件。"
    elif score >= 30:
        label = "法人偏空"
        advice = "降低追價，優先防守。"
    else:
        label = "法人明顯偏空"
        advice = "建議縮小部位或觀望。"

    return {"法人分數": round(score, 1), "法人狀態": label, "法人建議": advice}


def _inst_cache_to_df() -> pd.DataFrame:
    cache = _read_inst_cache()
    rows = []
    if isinstance(cache, dict):
        for k, v in cache.items():
            if not isinstance(v, dict):
                continue
            rows.append({
                "日期": v.get("date") or k,
                "外資": v.get("foreign_100m"),
                "單位": v.get("unit", "億元"),
                "投信": v.get("investment_trust_100m"),
                "自營商": v.get("dealer_100m"),
                "法人合計": v.get("total_100m"),
                "來源": v.get("source"),
                "更新時間": v.get("updated_at"),
            })
    if not rows:
        return pd.DataFrame(columns=["日期", "外資億元", "投信億元", "自營商億元", "三大法人合計億元", "來源", "更新時間"])
    df = pd.DataFrame(rows)
    df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
    return df.dropna(subset=["日期"]).sort_values("日期").reset_index(drop=True)


def _num_tw(v: Any):
    s = _safe_str(v).replace(",", "").replace("+", "").replace("%", "")
    if not s or s in {"-", "--", "None", "nan"}:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _fetch_twse_realtime(timeout: float = 1.8) -> dict[str, Any]:
    """手動按鈕才會呼叫。避免進頁卡住。"""
    url = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp"
    params = {
        "ex_ch": "tse_t00.tw",
        "json": "1",
        "delay": "0",
        "_": int(_tw_now().timestamp() * 1000),
    }
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://mis.twse.com.tw/stock/fibest.jsp?stock=t00",
    }
    try:
        r = requests.get(url, params=params, headers=headers, timeout=timeout, verify=False)
        if r.status_code != 200:
            return {"ok": False, "source": "TWSE MIS", "error": f"HTTP {r.status_code}"}
        data = r.json()
        arr = data.get("msgArray") or []
        if not arr:
            return {"ok": False, "source": "TWSE MIS", "error": "msgArray empty"}
        row = arr[0]
        current = _num_tw(row.get("z"))
        prev = _num_tw(row.get("y"))
        open_v = _num_tw(row.get("o"))
        high_v = _num_tw(row.get("h"))
        low_v = _num_tw(row.get("l"))
        if current is None:
            return {"ok": False, "source": "TWSE MIS", "error": "目前非盤中或即時值尚未提供", "prev_close": prev}
        pct = ((current - prev) / prev * 100) if prev not in [None, 0] else None
        d = _safe_str(row.get("d"))
        used_date = f"{d[:4]}-{d[4:6]}-{d[6:8]}" if len(d) == 8 else _tw_now().strftime("%Y-%m-%d")
        return {
            "ok": True,
            "source": "TWSE MIS 盤中即時",
            "date": used_date,
            "used_date": used_date,
            "time": _safe_str(row.get("t")),
            "close": current,
            "prev_close": prev,
            "open": open_v,
            "high": high_v,
            "low": low_v,
            "pct": pct,
            "is_realtime": True,
        }
    except Exception as e:
        return {"ok": False, "source": "TWSE MIS", "error": "連線失敗或 SSL 憑證被雲端環境攔截，請改按收盤紀錄或稍後再試。"}


def _fetch_twse_close(target_date: date, timeout: float = 2.5) -> dict[str, Any]:
    """手動按鈕才會呼叫。收盤後 / 晚上抓收盤資料。"""
    ymd = pd.to_datetime(target_date).strftime("%Y%m%d")
    urls = [
        f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={ymd}&type=IND&response=json",
        f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={ymd}&type=IND",
    ]

    def walk_rows(obj):
        if isinstance(obj, list):
            if obj and all(not isinstance(x, (list, dict)) for x in obj):
                yield obj
            for x in obj:
                yield from walk_rows(x)
        elif isinstance(obj, dict):
            for v in obj.values():
                yield from walk_rows(v)

    for url in urls:
        try:
            r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout, verify=False)
            if r.status_code != 200:
                continue
            data = r.json()
            for row in walk_rows(data):
                joined = " ".join(_safe_str(x) for x in row)
                if "發行量加權股價指數" not in joined and "TAIEX" not in joined.upper():
                    continue
                nums = [_num_tw(x) for x in row]
                nums = [x for x in nums if x is not None]
                if not nums:
                    continue
                close = nums[0]
                pct = None
                if len(nums) >= 3 and abs(nums[-1]) < 20:
                    pct = nums[-1]
                return {
                    "ok": True,
                    "source": "TWSE 收盤紀錄",
                    "date": pd.to_datetime(target_date).strftime("%Y-%m-%d"),
                    "used_date": pd.to_datetime(target_date).strftime("%Y-%m-%d"),
                    "close": close,
                    "pct": pct,
                    "is_realtime": False,
                }
        except Exception:
            continue
    return {"ok": False, "source": "TWSE 收盤紀錄", "error": "收盤資料尚未取得或交易所未發布"}



def _fetch_taiex_yahoo_backup(target_date: date, timeout: float = 2.5) -> dict[str, Any]:
    """
    v29.5：TWSE HTTP 502 / SSL / 無資料時的備援。
    使用 Yahoo ^TWII 日線，僅在手動/背景更新時呼叫，不會進頁同步等待。
    """
    try:
        # _fetch_yahoo_chart 在本檔後段已存在；若未存在則直接略過。
        if "_fetch_yahoo_chart" not in globals():
            return {"ok": False, "source": "Yahoo ^TWII 備援", "error": "yahoo helper not available"}
        row = _fetch_yahoo_chart("^TWII", target_date, timeout=timeout)
        if not isinstance(row, dict) or not row.get("ok"):
            return {"ok": False, "source": "Yahoo ^TWII 備援", "error": row.get("error") if isinstance(row, dict) else "no data"}
        return {
            "ok": True,
            "source": "Yahoo ^TWII 備援",
            "date": _safe_str(row.get("date")) or pd.to_datetime(target_date).strftime("%Y-%m-%d"),
            "used_date": _safe_str(row.get("date")) or pd.to_datetime(target_date).strftime("%Y-%m-%d"),
            "close": _safe_float(row.get("close")),
            "pct": _safe_float(row.get("pct")),
            "change_points": _calc_market_change_points({"close": _safe_float(row.get("close")), "pct": _safe_float(row.get("pct"))}),
            "is_realtime": False,
            "backup": True,
            "updated_at": _tw_now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    except Exception as e:
        return {"ok": False, "source": "Yahoo ^TWII 備援", "error": str(e)}


def _fetch_market_with_fallback(target_date: date, realtime: bool = False) -> dict[str, Any]:
    """
    v29.5：大盤自動備援。
    1. 盤中先 TWSE MIS。
    2. 非盤中/晚上先 TWSE 收盤。
    3. TWSE 失敗改 Yahoo ^TWII。
    4. 失敗只回傳錯誤，不卡住整頁。
    """
    primary = None
    if realtime:
        primary = _fetch_twse_realtime(timeout=1.5)
    else:
        primary = _fetch_twse_close(target_date, timeout=2.0)

    if isinstance(primary, dict) and primary.get("ok"):
        return primary

    backup = _fetch_taiex_yahoo_backup(target_date, timeout=2.0)
    if isinstance(backup, dict) and backup.get("ok"):
        backup["note"] = f"TWSE失敗，已改用Yahoo備援；TWSE原因：{(primary or {}).get('error', '')}"
        return backup

    return {
        "ok": False,
        "source": "大盤自動備援",
        "date": pd.to_datetime(target_date).strftime("%Y-%m-%d"),
        "error": f"TWSE與Yahoo備援皆失敗；TWSE:{(primary or {}).get('error', '')} / Yahoo:{(backup or {}).get('error', '')}",
    }


def _default_market_row(target_date: date) -> dict[str, Any]:
    cache = _read_cache()
    ymd = pd.to_datetime(target_date).strftime("%Y%m%d")
    if isinstance(cache.get(ymd), dict):
        return cache[ymd]

    # 找最近一筆快取
    if cache:
        keys = sorted([k for k in cache.keys() if isinstance(cache.get(k), dict)], reverse=True)
        if keys:
            row = dict(cache[keys[0]])
            row["source"] = _safe_str(row.get("source")) + "｜最近快取"
            return row

    return {
        "ok": False,
        "source": "尚未更新",
        "date": pd.to_datetime(target_date).strftime("%Y-%m-%d"),
        "used_date": pd.to_datetime(target_date).strftime("%Y-%m-%d"),
        "close": None,
        "pct": None,
        "is_realtime": False,
        "error": "尚未按更新，目前不呼叫外部資料源",
    }


def _save_market_row(row: dict[str, Any]) -> None:
    if not isinstance(row, dict) or row.get("close") is None:
        return
    dt = pd.to_datetime(row.get("date") or row.get("used_date") or _tw_now().date(), errors="coerce")
    if pd.isna(dt):
        dt = pd.Timestamp(_tw_now().date())
    ymd = dt.strftime("%Y%m%d")
    cache = _read_cache()
    cache[ymd] = row
    _write_cache(cache)


def _recent_business_dates(end_date: date, days: int = 20) -> list[date]:
    out = []
    cur = pd.to_datetime(end_date).date()
    guard = 0
    while len(out) < int(days) and guard < 60:
        if cur.weekday() < 5:
            out.append(cur)
        cur = cur - timedelta(days=1)
        guard += 1
    return list(reversed(out))


def _batch_fetch_close_cache(end_date: date, days: int = 20) -> tuple[int, list[str]]:
    """
    v27.1：手動補抓近 N 個交易日收盤紀錄。
    僅在使用者按鈕觸發，不會進頁自動跑，避免卡住。
    """
    cache = _read_cache()
    added = 0
    messages = []
    for d in _recent_business_dates(end_date, days):
        ymd = pd.to_datetime(d).strftime("%Y%m%d")
        if isinstance(cache.get(ymd), dict) and cache[ymd].get("close") is not None:
            messages.append(f"{ymd} 已有快取，略過")
            continue
        row = _fetch_twse_close(d, timeout=2.0)
        if row.get("ok"):
            cache[ymd] = row
            added += 1
            messages.append(f"{ymd} 收盤 {row.get('close')} 已加入")
        else:
            messages.append(f"{ymd} 未取得：{row.get('error')}")
    _write_cache(cache)
    return added, messages


def _cache_download_csv_bytes() -> bytes:
    df = _cache_to_market_df() if "_cache_to_market_df" in globals() else pd.DataFrame()
    if df is None or df.empty:
        return b""
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")


def _render_market_cache_chart():
    df = _cache_to_market_df()
    if df is None or df.empty or len(df) < 2:
        st.info("大盤快取資料不足，補抓近20日收盤後，這裡會顯示收盤趨勢。")
        return
    st.markdown("### 大盤快取趨勢")
    chart_df = df[["日期", "收盤"]].copy()
    chart_df["日期"] = pd.to_datetime(chart_df["日期"]).dt.strftime("%Y-%m-%d")
    st.line_chart(chart_df.set_index("日期"), use_container_width=True)


BRIDGE_FILE = "macro_mode_bridge.json"


def _build_macro_bridge_payload(row: dict[str, Any]) -> dict[str, Any]:
    factors = _calc_stable_market_factors(row)
    _bridge_date = pd.to_datetime(row.get("date") or row.get("used_date") or date.today(), errors="coerce")
    _bridge_date = _bridge_date.date() if pd.notna(_bridge_date) else date.today()
    inst = _default_inst_row(_bridge_date)
    inst_score = _institutional_score(inst)
    us_row = _default_us_market_row(_bridge_date)
    us_score = _us_market_score(us_row)
    tx_row = _default_taifex_row(_bridge_date)
    tx_score = _taifex_score(tx_row)
    close_val = _safe_float(row.get("close"))
    pct_val = _safe_float(row.get("pct"))
    payload = {
        "version": "v27.5_macro_bridge",
        "updated_at": _tw_now().strftime("%Y-%m-%d %H:%M:%S"),
        "market_date": _safe_str(row.get("used_date") or row.get("date")),
        "source": _safe_str(row.get("source")),
        "is_realtime": bool(row.get("is_realtime")),
        "close": close_val,
        "pct": pct_val,
        "change_points": _calc_market_change_points(row),
        "market_score": _safe_float(factors.get("大盤穩定分"), 50),
        "market_state": _safe_str(factors.get("大盤狀態")),
        "godpick_weight_advice": _safe_str(factors.get("股神推薦加權")),
        "strategy": _safe_str(factors.get("今日策略")),
        "ma5": _safe_float(factors.get("MA5")),
        "ma20": _safe_float(factors.get("MA20")),
        "dist_ma5_pct": _safe_float(factors.get("距MA5%")),
        "dist_ma20_pct": _safe_float(factors.get("距MA20%")),
        "position_20d_pct": _safe_float(factors.get("20日位置%")),
        "cache_count": int(factors.get("快取筆數") or 0),
        "institutional_date": _safe_str(inst.get("date")),
        "institutional_score": _safe_float(inst_score.get("法人分數"), 50),
        "institutional_state": _safe_str(inst_score.get("法人狀態")),
        "institutional_advice": _safe_str(inst_score.get("法人建議")),
        "foreign_100m": _safe_float(inst.get("foreign_100m")),
        "investment_trust_100m": _safe_float(inst.get("investment_trust_100m")),
        "dealer_100m": _safe_float(inst.get("dealer_100m")),
        "institutional_total_100m": _safe_float(inst.get("total_100m")),
        "us_market_score": _safe_float(us_score.get("外盤分數"), 50),
        "us_market_state": _safe_str(us_score.get("外盤狀態")),
        "us_market_advice": _safe_str(us_score.get("外盤建議")),
        "taifex_score": _safe_float(tx_score.get("期貨分數"), 50),
        "taifex_state": _safe_str(tx_score.get("期貨狀態")),
        "taifex_advice": _safe_str(tx_score.get("期貨建議")),
        "tx_close": _safe_float(tx_row.get("tx_close")),
        "tx_change": _safe_float(tx_row.get("tx_change")),
        "recommendation_bias": _macro_bias_from_score(
            (_safe_float(factors.get("大盤穩定分"), 50) * 0.52)
            + (_safe_float(inst_score.get("法人分數"), 50) * 0.23)
            + (_safe_float(us_score.get("外盤分數"), 50) * 0.15)
            + (_safe_float(tx_score.get("期貨分數"), 50) * 0.10)
        ),
    }
    return payload


def _macro_bias_from_score(score: float) -> dict[str, Any]:
    score = _safe_float(score, 50) or 50
    if score >= 75:
        return {
            "risk_filter": "放寬",
            "preferred_modes": ["強勢突破", "低檔轉強", "拉回承接"],
            "avoid_modes": ["純題材追高"],
            "position_advice": "可正常至偏積極，但仍須控單檔風險。",
        }
    if score >= 60:
        return {
            "risk_filter": "正常",
            "preferred_modes": ["低檔轉強", "拉回承接", "回測支撐"],
            "avoid_modes": ["高位爆量追高"],
            "position_advice": "可正常操作，優先挑選有支撐與族群轉強的標的。",
        }
    if score >= 45:
        return {
            "risk_filter": "中性",
            "preferred_modes": ["回測支撐", "低檔止跌"],
            "avoid_modes": ["追高突破", "無量反彈"],
            "position_advice": "降低追價，採分批與小部位。",
        }
    if score >= 30:
        return {
            "risk_filter": "偏嚴",
            "preferred_modes": ["低檔止跌", "防守型回測"],
            "avoid_modes": ["強勢追價", "高波動題材"],
            "position_advice": "保守操作，等待確認再進場。",
        }
    return {
        "risk_filter": "嚴格",
        "preferred_modes": ["觀望", "極低檔止跌"],
        "avoid_modes": ["追高", "突破", "高槓桿題材"],
        "position_advice": "以觀望與現金比重為主。",
    }


def _write_macro_bridge(row: dict[str, Any]) -> tuple[bool, str]:
    payload = _build_macro_bridge_payload(row)
    try:
        Path(BRIDGE_FILE).write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        return True, f"已寫入 {BRIDGE_FILE}，股神推薦可讀取大盤穩定因子。"
    except Exception as e:
        return False, f"寫入 {BRIDGE_FILE} 失敗：{e}"


def _read_macro_bridge() -> dict[str, Any]:
    p = Path(BRIDGE_FILE)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _render_macro_bridge_block(row: dict[str, Any]):
    st.markdown("### 股神推薦串聯")
    payload = _build_macro_bridge_payload(row)
    c1, c2, c3, c4 = st.columns([1.2, 1.2, 1.2, 2.4])
    with c1:
        if st.button("寫入股神大盤參考", use_container_width=True, type="primary"):
            ok, msg = _v294_write_bridge_with_quality(row)
            if ok:
                st.success(msg)
            else:
                st.warning(msg)
    with c2:
        st.metric("推薦風控", payload.get("recommendation_bias", {}).get("risk_filter", "中性"))
    with c3:
        st.metric("建議加權", payload.get("godpick_weight_advice", "0%"))
    with c4:
        st.caption("v27.5：將大盤穩定分、策略、風控建議寫入 macro_mode_bridge.json，後續可讓 7_股神推薦讀取。")

    with st.expander("股神大盤橋接檔內容", expanded=False):
        st.json(payload)
        old_bridge = _read_macro_bridge()
        if old_bridge:
            st.write("目前已存在橋接檔：")
            st.json(old_bridge)


def _macro_feature_status_df() -> pd.DataFrame:
    rows = [
        {
            "功能": "頁面先顯示",
            "目前狀態": "已恢復",
            "是否自動執行": "是",
            "說明": "進頁後直接顯示，不再等待外部API。",
        },
        {
            "功能": "盤中即時大盤",
            "目前狀態": "已恢復",
            "是否自動執行": "否，手動按鈕",
            "說明": "按「更新盤中即時大盤」才抓 TWSE MIS，避免卡住。",
        },
        {
            "功能": "晚上/收盤紀錄",
            "目前狀態": "已恢復",
            "是否自動執行": "否，手動按鈕",
            "說明": "按「更新收盤紀錄」或「補抓近20日收盤」。",
        },
        {
            "功能": "大盤穩定因子",
            "目前狀態": "已恢復",
            "是否自動執行": "是，讀本機快取",
            "說明": "使用 macro_market_close_cache.json 計算 MA5、MA20、20日位置與大盤穩定分。",
        },
        {
            "功能": "股神推薦橋接",
            "目前狀態": "已恢復",
            "是否自動執行": "否，手動寫入",
            "說明": "按「寫入股神大盤參考」產生 macro_mode_bridge.json。",
        },
        {
            "功能": "大盤趨勢圖",
            "目前狀態": "已恢復",
            "是否自動執行": "是，讀本機快取",
            "說明": "用本機快取畫收盤趨勢，不抓外部資料。",
        },
        {
            "功能": "大盤快取下載",
            "目前狀態": "已恢復",
            "是否自動執行": "否，下載按鈕",
            "說明": "可下載 macro_market_close_cache.csv。",
        },
        {
            "功能": "Google News 事件因子",
            "目前狀態": "暫停",
            "是否自動執行": "否",
            "說明": "先暫停，這是之前最容易造成頁面等待的來源之一。",
        },
        {
            "功能": "TAIFEX 期權因子",
            "目前狀態": "已恢復",
            "是否自動執行": "否，手動按鈕",
            "說明": "v29.5 改為背景自動更新；不進主畫面等待，避免卡住。",
        },
        {
            "功能": "完整法人籌碼",
            "目前狀態": "已恢復",
            "是否自動執行": "否，手動按鈕",
            "說明": "v27.6 已改為手動更新三大法人並寫入本機快取。",
        },
        {
            "功能": "Yahoo / Stooq 外盤模型",
            "目前狀態": "已恢復",
            "是否自動執行": "否，手動按鈕",
            "說明": "v27.8 已恢復 Yahoo 外盤手動更新；不進頁自動抓，避免卡住。",
        },
        {
            "功能": "完整大盤模式預估",
            "目前狀態": "暫停",
            "是否自動執行": "否",
            "說明": "舊版會自動跑完整模型，已改為先穩定顯示，再逐項回補。",
        },
    ]
    return pd.DataFrame(rows)


def _render_institutional_block(target_date: date):
    st.markdown("### 法人籌碼背景 / 快取")
    inst = _default_inst_row(target_date)
    score = _institutional_score(inst)

    c1, c2, c3, c4, c5 = st.columns([1.25, 1.1, 1.1, 1.1, 1.5])
    with c1:
        update_inst = st.button("更新三大法人", use_container_width=True)
    with c2:
        st.metric("法人分數", f"{_safe_float(score.get('法人分數'), 50):.1f}")
    with c3:
        st.metric("法人狀態", _safe_str(score.get("法人狀態")))
    with c4:
        st.metric("法人合計", f"{_safe_float(inst.get('total_100m'), 0):+.2f} " + (_safe_str(inst.get("unit")) or "億元"))
    with c5:
        st.caption("只在按下時抓 TWSE 法人資料，不會進頁自動執行。")

    if update_inst:
        with st.spinner("正在手動更新 TWSE 三大法人，最多等待約 3 秒..."):
            row = _fetch_twse_institutional_manual(target_date)
        if row.get("ok"):
            _save_inst_row(row)
            st.success(f"三大法人更新成功：合計 { _safe_float(row.get('total_100m'), 0):+.2f} 億元")
            st.rerun()
        else:
            st.warning(f"三大法人更新失敗：{row.get('error')}")
            if row.get("tried"):
                with st.expander("法人更新嘗試明細", expanded=False):
                    for item in row.get("tried", []):
                        st.write(f"- {item}")

    detail = [
        ("資料日期", _safe_str(inst.get("date")) or "尚未更新"),
        ("外資", f"{_safe_float(inst.get('foreign_100m'), 0):+.2f} " + (_safe_str(inst.get("unit")) or "億元")),
        ("投信", f"{_safe_float(inst.get('investment_trust_100m'), 0):+.2f} " + (_safe_str(inst.get("unit")) or "億元")),
        ("自營商", f"{_safe_float(inst.get('dealer_100m'), 0):+.2f} " + (_safe_str(inst.get("unit")) or "億元")),
        ("法人建議", _safe_str(score.get("法人建議"))),
    ]
    cols = st.columns(len(detail))
    for col, (title, value) in zip(cols, detail):
        with col:
            st.markdown(
                f"""
                <div style="border:1px solid #e2e8f0;border-radius:14px;padding:12px;background:#ffffff;min-height:84px;">
                    <div style="font-size:13px;color:#64748b;font-weight:800;">{title}</div>
                    <div style="font-size:15px;color:#0f172a;font-weight:900;margin-top:8px;">{value}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    with st.expander("法人籌碼快取明細", expanded=False):
        df = _inst_cache_to_df()
        st.dataframe(df, use_container_width=True, hide_index=True)
        if not df.empty:
            st.download_button(
                "下載法人快取CSV",
                data=df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"),
                file_name="macro_institutional_cache.csv",
                mime="text/csv",
                use_container_width=True,
            )


def _read_taifex_cache() -> dict[str, Any]:
    p = Path(TAIFEX_CACHE_FILE)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_taifex_cache(cache: dict[str, Any]) -> None:
    try:
        Path(TAIFEX_CACHE_FILE).write_text(json.dumps(cache, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    except Exception:
        pass


def _fetch_taifex_futures_manual(target_date: date, timeout: float = 3.0) -> dict[str, Any]:
    """
    v27.10：TAIFEX 期貨手動更新強化版。
    - 支援 GET / POST。
    - 若指定日期尚無資料，會往前找最近 10 個工作日。
    - 只在按鈕觸發時執行，不進頁自動抓，避免卡住。
    - 若仍失敗，可用頁面上的手動輸入保存。
    """
    target_dt = pd.to_datetime(target_date).date()

    def _candidate_dates(end_date: date, max_days: int = 10) -> list[date]:
        out = []
        cur = end_date
        guard = 0
        while len(out) < max_days and guard < 25:
            if cur.weekday() < 5:
                out.append(cur)
            cur = cur - timedelta(days=1)
            guard += 1
        return out

    def _parse_tables(html_text: str, used_date: date, source_name: str) -> dict[str, Any]:
        try:
            tables = pd.read_html(html_text)
        except Exception:
            tables = []

        for tb in tables:
            if tb is None or tb.empty:
                continue

            df = tb.copy()
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = ["_".join([str(x) for x in c if str(x) != "nan"]) for c in df.columns]
            else:
                df.columns = [str(c) for c in df.columns]

            full_text = " ".join(df.astype(str).fillna("").values.flatten().tolist()) + " " + " ".join(df.columns)
            if not any(k in full_text for k in ["TX", "臺股期貨", "台股期貨", "臺指", "台指", "契約"]):
                continue

            chosen = None
            for _, row in df.iterrows():
                row_text = " ".join(_safe_str(x) for x in row.values)
                if ("TX" in row_text or "臺股期貨" in row_text or "台股期貨" in row_text or "臺指" in row_text or "台指" in row_text):
                    chosen = row
                    break
            if chosen is None and len(df) > 0:
                chosen = df.iloc[0]

            row_dict = {str(k): chosen[k] for k in df.columns}
            close_val = None
            change_val = None
            volume_val = None

            for k, v in row_dict.items():
                kk = _safe_str(k)
                if close_val is None and any(x in kk for x in ["收盤", "最後成交", "最後"]):
                    close_val = _safe_float(v)
                if change_val is None and any(x in kk for x in ["漲跌", "價差"]):
                    change_val = _safe_float(v)
                if volume_val is None and any(x in kk for x in ["成交量", "交易量"]):
                    volume_val = _safe_float(v)

            nums = [_safe_float(x) for x in chosen.values]
            nums = [x for x in nums if x is not None]

            if close_val is None:
                candidates = [x for x in nums if 5000 <= abs(x) <= 50000]
                if candidates:
                    # 台指期點數通常是接近 2萬～3萬的數字
                    close_val = candidates[-1]

            if change_val is None:
                # 漲跌點通常絕對值小於 2000，避開月份/履約價
                change_candidates = [x for x in nums if -2000 <= x <= 2000 and x != 0]
                if change_candidates:
                    change_val = change_candidates[-1]

            if close_val is not None:
                return {
                    "ok": True,
                    "date": pd.to_datetime(used_date).strftime("%Y-%m-%d"),
                    "source": source_name,
                    "tx_close": close_val,
                    "tx_change": change_val,
                    "tx_volume": volume_val,
                    "raw": row_dict,
                    "updated_at": _tw_now().strftime("%Y-%m-%d %H:%M:%S"),
                }
        return {}

    tried = []
    endpoints = [
        "https://www.taifex.com.tw/cht/3/futDailyMarketReport",
        "https://www.taifex.com.tw/cht/3/futDailyMarketReportDown",
    ]

    for d in _candidate_dates(target_dt, max_days=10):
        qdate = pd.to_datetime(d).strftime("%Y/%m/%d")
        param_variants = [
            {"queryDate": qdate, "commodity_id": "TX"},
            {"queryDate": qdate, "commodity_id": "TX", "MarketCode": "0"},
            {"queryDate": qdate, "commodity_id": "TX", "queryType": "1"},
        ]

        for url in endpoints:
            for params in param_variants:
                tag = f"{pd.to_datetime(d).strftime('%Y-%m-%d')} {url.split('/')[-1]} {params}"
                tried.append(tag)

                # GET
                try:
                    r = requests.get(url, params=params, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout, verify=False)
                    if r.status_code == 200:
                        parsed = _parse_tables(r.text, d, "TAIFEX 台指期手動")
                        if parsed:
                            if d != target_dt:
                                parsed["source"] = "TAIFEX 台指期手動｜最近可用日"
                                parsed["note"] = f"指定日期 {pd.to_datetime(target_dt).strftime('%Y-%m-%d')} 尚無資料，已使用最近可用日 {pd.to_datetime(d).strftime('%Y-%m-%d')}"
                            return parsed
                except Exception:
                    pass

                # POST
                try:
                    r = requests.post(url, data=params, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout, verify=False)
                    if r.status_code == 200:
                        parsed = _parse_tables(r.text, d, "TAIFEX 台指期手動")
                        if parsed:
                            if d != target_dt:
                                parsed["source"] = "TAIFEX 台指期手動｜最近可用日"
                                parsed["note"] = f"指定日期 {pd.to_datetime(target_dt).strftime('%Y-%m-%d')} 尚無資料，已使用最近可用日 {pd.to_datetime(d).strftime('%Y-%m-%d')}"
                            return parsed
                except Exception:
                    pass

    return {
        "ok": False,
        "date": pd.to_datetime(target_date).strftime("%Y-%m-%d"),
        "source": "TAIFEX 台指期手動",
        "error": "TAIFEX 台指期資料尚未取得；可能是尚未收盤、非交易日、端點限制或雲端連線失敗。可用下方手動輸入保存。",
        "tried": tried[-12:],
    }


def _save_taifex_row(row: dict[str, Any]) -> None:
    if not isinstance(row, dict) or not row.get("ok"):
        return
    dt = pd.to_datetime(row.get("date") or _tw_now().date(), errors="coerce")
    if pd.isna(dt):
        dt = pd.Timestamp(_tw_now().date())
    ymd = dt.strftime("%Y%m%d")
    cache = _read_taifex_cache()
    cache[ymd] = row
    _write_taifex_cache(cache)
    try:
        _set_job_status("taifex_auto_bg", "finished", f"期貨快取已更新：{row.get('source')} / 收盤 {row.get('tx_close')} / 漲跌 {row.get('tx_change')}")
    except Exception:
        pass


def _save_taifex_manual_input(target_date: date, tx_close: float | None, tx_change: float | None, tx_volume: float | None = None) -> tuple[bool, str]:
    if tx_close is None or _safe_float(tx_close) is None or _safe_float(tx_close) <= 0:
        return False, "請輸入有效的期貨收盤/指數價。"

    row = {
        "ok": True,
        "date": pd.to_datetime(target_date).strftime("%Y-%m-%d"),
        "source": "手動輸入台指期",
        "tx_close": _safe_float(tx_close),
        "tx_change": _safe_float(tx_change, 0),
        "tx_volume": _safe_float(tx_volume),
        "raw": {"manual": True},
        "updated_at": _tw_now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    _save_taifex_row(row)
    return True, f"已手動保存台指期：收盤 {row['tx_close']} / 漲跌 {row['tx_change']}"


def _default_taifex_row(target_date: date) -> dict[str, Any]:
    cache = _read_taifex_cache()
    ymd = pd.to_datetime(target_date).strftime("%Y%m%d")
    if isinstance(cache.get(ymd), dict):
        return cache[ymd]
    if cache:
        keys = sorted([k for k in cache.keys() if isinstance(cache.get(k), dict)], reverse=True)
        if keys:
            row = dict(cache[keys[0]])
            row["source"] = _safe_str(row.get("source")) + "｜最近快取"
            return row
    return {"ok": False, "date": pd.to_datetime(target_date).strftime("%Y-%m-%d"), "source": "尚未更新", "tx_close": None, "tx_change": None}


def _taifex_score(row: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(row, dict) or not row:
        return {"期貨分數": 50.0, "期貨狀態": "期貨中性", "期貨建議": "期貨未提供明確方向。"}

    # Yahoo IX0126.TW 是備援參考，不是台指期近月合約；分數權重降低。
    is_ref = bool(row.get("is_reference_only"))
    chg = _safe_float(row.get("tx_change"), 0) or 0
    pct = _safe_float(row.get("tx_pct"), None)

    if is_ref and pct is not None:
        score = 50 + max(min(pct * 7, 10), -10)
    else:
        score = 50 + max(min(chg * 0.08, 18), -18)

    score = max(0, min(100, score))
    if score >= 65:
        label = "期貨偏多"
        advice = "期貨支撐偏多，可提高順勢觀察。"
    elif score >= 55:
        label = "期貨中性偏多"
        advice = "期貨略偏多，但仍需確認現貨量價。"
    elif score >= 45:
        label = "期貨中性"
        advice = "期貨未提供明確方向。"
    elif score >= 35:
        label = "期貨偏弱"
        advice = "隔日追高風險提高，等拉回確認。"
    else:
        label = "期貨偏空"
        advice = "期貨逆風，建議保守控倉。"

    if is_ref:
        advice += "｜目前使用Yahoo期貨指數備援，非近月台指期合約。"
    return {"期貨分數": round(score, 1), "期貨狀態": label, "期貨建議": advice}



def _taifex_cache_to_df() -> pd.DataFrame:
    cache = _read_taifex_cache()
    rows = []
    for ymd, v in (cache or {}).items():
        if not isinstance(v, dict):
            continue
        rows.append({
            "日期": v.get("date") or ymd,
            "期貨收盤/指數": v.get("tx_close"),
            "期貨漲跌": v.get("tx_change"),
            "成交量": v.get("tx_volume"),
            "來源": v.get("source"),
            "更新時間": v.get("updated_at"),
        })
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
    return df.dropna(subset=["日期"]).sort_values("日期").reset_index(drop=True)


def _background_taifex_worker(target_date_text: str) -> None:
    """
    v29：期貨自動背景更新。
    不在主畫面等待 TAIFEX；成功就寫 macro_taifex_cache.json，失敗只寫狀態。
    """
    try:
        d = pd.to_datetime(target_date_text, errors="coerce")
        if pd.isna(d):
            d = pd.Timestamp(_tw_now().date())
        d = d.date()

        row = _fetch_taifex_futures_manual(d, timeout=2.0)
        if isinstance(row, dict) and row.get("ok"):
            _save_taifex_row(row)
            _set_job_status("taifex_auto_bg", "finished", f"期貨更新成功：{row.get('source')} / 收盤 {row.get('tx_close')} / 漲跌 {row.get('tx_change')}")
        else:
            _set_job_status("taifex_auto_bg", "error", f"期貨更新失敗：{(row or {}).get('error', 'unknown')}")
    except Exception as e:
        _set_job_status("taifex_auto_bg", "error", f"期貨背景更新例外：{e}")


def _start_taifex_background_update(target_date: date, force: bool = False) -> None:
    if not force and _job_is_recent("taifex_auto_bg", MACRO_AUTO_REFRESH_SECONDS):
        return
    _set_job_status("taifex_auto_bg", "running", "期貨背景更新中")
    t = threading.Thread(
        target=_background_taifex_worker,
        args=(pd.to_datetime(target_date).strftime("%Y-%m-%d"),),
        daemon=True,
    )
    t.start()


def _sync_taifex_job_status_from_cache(target_date: date | None = None) -> None:
    """
    v29.5：期貨資料已經抓到但背景狀態仍顯示 running 時，自動同步成 finished。
    你畫面已出現收盤/漲跌/成交量，但狀態還 running，就是這個問題。
    """
    try:
        if target_date is None:
            target_date = date.today()
        row = _default_taifex_row(target_date)
        if not isinstance(row, dict) or row.get("tx_close") is None:
            return

        jobs = _read_bg_jobs()
        item = jobs.get("taifex_auto_bg", {}) if isinstance(jobs, dict) else {}
        status = _safe_str(item.get("status"))
        started = _safe_float(item.get("started_ts"), 0) or 0

        # 只要已有有效期貨快取，而狀態仍 running/空白，就改成 finished。
        if status in {"running", "", "尚未啟動"}:
            jobs["taifex_auto_bg"] = {
                "status": "finished",
                "message": f"期貨快取已更新：{row.get('source')} / 收盤 {row.get('tx_close')} / 漲跌 {row.get('tx_change')}",
                "started_ts": started or time.time(),
                "updated_at": _safe_str(row.get("updated_at")) or _tw_now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            _write_bg_jobs(jobs)
    except Exception:
        pass



def _render_taifex_bg_status():
    _sync_taifex_job_status_from_cache()
    _cleanup_stale_jobs()
    jobs = _read_bg_jobs()
    item = jobs.get("taifex_auto_bg", {}) if isinstance(jobs, dict) else {}
    status = _safe_str(item.get("status")) or "尚未啟動"
    msg = _safe_str(item.get("message"))
    updated = _safe_str(item.get("updated_at"))
    c1, c2, c3, c4 = st.columns([1.1, 3.0, 1.5, 1.1])
    with c1:
        st.metric("期貨背景狀態", status)
    with c2:
        st.caption(msg or "按下更新後會背景抓 TAIFEX；若逾時會自動熔斷。")
    with c3:
        st.caption(updated or "尚未更新")
    with c4:
        if st.button("重置期貨狀態", use_container_width=True, key=_k("reset_taifex_job_old")):
            jobs = _read_bg_jobs()
            if "taifex_auto_bg" in jobs:
                jobs.pop("taifex_auto_bg", None)
                _write_bg_jobs(jobs)
            st.success("已重置期貨背景狀態。")
            st.rerun()
    if status == "running":
        st.info(f"期貨背景更新中；若超過 {MACRO_BG_MAX_RUNNING_SECONDS} 秒會自動熔斷。")
    elif status == "timeout":
        st.warning("期貨背景更新逾時，已熔斷；保留舊快取。")





def _render_taifex_block(target_date: date):
    st.markdown("### 期貨自動背景更新")
    row = _default_taifex_row(target_date)
    score = _taifex_score(row)

    c1, c2, c3, c4, c5 = st.columns([1.3, 1.1, 1.1, 1.1, 2.1])
    with c1:
        update_taifex = st.button("背景更新台指期", use_container_width=True)
    with c2:
        st.metric("期貨分數", f"{_safe_float(score.get('期貨分數'), 50):.1f}")
    with c3:
        st.metric("期貨狀態", _safe_str(score.get("期貨狀態")))
    with c4:
        st.metric("期貨漲跌", f"{_safe_float(row.get('tx_change'), 0):+.0f}" + (f"｜{_safe_float(row.get('tx_pct'), 0):+.2f}%" if row.get("tx_pct") is not None else ""))
    with c5:
        st.caption("v29：期貨不再手動輸入，改成背景自動抓取；不等待、不卡頁。")

    if update_taifex:
        _start_taifex_background_update(target_date, force=True)
        st.success("已啟動期貨背景更新。頁面不會等待，稍後重新整理或切頁回來即可看到結果。")

    _render_taifex_bg_status()

    cols = st.columns(4)
    cards = [
        ("資料日期", _safe_str(row.get("date")) or "尚未更新"),
        ("期貨收盤/指數", f"{_safe_float(row.get('tx_close'), 0):,.0f}"),
        ("期貨漲跌", f"{_safe_float(row.get('tx_change'), 0):+.0f}" + (f"｜{_safe_float(row.get('tx_pct'), 0):+.2f}%" if row.get("tx_pct") is not None else "")),
        ("期貨建議", _safe_str(score.get("期貨建議"))),
    ]
    for col, (title, value) in zip(cols, cards):
        with col:
            st.markdown(
                f"""
                <div style="border:1px solid #e2e8f0;border-radius:14px;padding:12px;background:#ffffff;min-height:88px;">
                    <div style="font-size:13px;color:#64748b;font-weight:800;">{title}</div>
                    <div style="font-size:15px;color:#0f172a;font-weight:900;margin-top:8px;">{value}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    with st.expander("期貨快取明細", expanded=False):
        df = _taifex_cache_to_df()
        st.dataframe(df, use_container_width=True, hide_index=True)
        if not df.empty:
            st.download_button(
                "下載期貨快取CSV",
                data=df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"),
                file_name="macro_taifex_cache.csv",
                mime="text/csv",
                use_container_width=True,
            )



def _render_macro_feature_center():
    st.markdown("### 大盤功能管理中心")
    st.caption("v27.5：把 0_大盤趨勢的功能狀態攤開顯示，避免不知道哪些功能已恢復、哪些先暫停。")
    df = _macro_feature_status_df()

    active_count = int((df["目前狀態"] == "已恢復").sum())
    paused_count = int((df["目前狀態"] == "暫停").sum())

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("已恢復功能", active_count)
    with c2:
        st.metric("暫停功能", paused_count)
    with c3:
        st.metric("卡頓風險", "低")
    with c4:
        st.metric("外部API背景更新", "啟用")

    st.dataframe(df, use_container_width=True, hide_index=True)

    with st.expander("後續建議回補順序", expanded=False):
        st.write("1. 先回補：法人買賣超，但改成手動按鈕 + 本機快取。")
        st.write("2. 再回補：外盤 NASDAQ / SOX，但只抓收盤資料，不自動等待。")
        st.write("3. 再回補：TAIFEX 期權，同樣改成手動更新。")
        st.write("4. 最後才回補：Google News，因為最容易慢。")
        st.warning("重點：外部資料源改成背景更新，不在主畫面等待；即使外部端點慢，頁面也不會卡住。")



def _calc_market_change_points(row: dict[str, Any]) -> float | None:
    """
    v29.5：由目前大盤 close + pct 反推漲跌點數。
    pct = (close - prev_close) / prev_close * 100
    change = close - prev_close = close * pct / (100 + pct)
    若資料源已提供 change / change_points 則優先使用。
    """
    for key in ["change_points", "change", "diff", "漲跌點數"]:
        val = _safe_float(row.get(key))
        if val is not None:
            return val

    close = _safe_float(row.get("close"))
    pct = _safe_float(row.get("pct"))
    if close is None or pct is None:
        return None
    if abs(100 + pct) < 1e-9:
        return None
    try:
        return close * pct / (100 + pct)
    except Exception:
        return None


def _score_context(row: dict[str, Any]) -> dict[str, str]:
    pct = _safe_float(row.get("pct"), 0) or 0
    if pct >= 1.0:
        mood = "強多"
        advice = "可偏積極，但避免追高。"
    elif pct >= 0.3:
        mood = "偏多"
        advice = "可順勢觀察強勢族群。"
    elif pct <= -1.0:
        mood = "偏空"
        advice = "保守控倉，優先等止跌。"
    elif pct <= -0.3:
        mood = "偏弱"
        advice = "降低追價，等待支撐確認。"
    else:
        mood = "震盪"
        advice = "以低檔拉回、支撐回測為主。"
    return {"mood": mood, "advice": advice}


def _cache_to_market_df() -> pd.DataFrame:
    cache = _read_cache()
    rows = []
    if isinstance(cache, dict):
        for k, v in cache.items():
            if not isinstance(v, dict):
                continue
            dt = pd.to_datetime(v.get("date") or v.get("used_date") or k, errors="coerce")
            close = _safe_float(v.get("close"))
            pct = _safe_float(v.get("pct"))
            if pd.isna(dt) or close is None:
                continue
            rows.append(
                {
                    "日期": dt,
                    "收盤": close,
                    "漲跌幅%": pct,
                    "來源": _safe_str(v.get("source")),
                    "即時": bool(v.get("is_realtime")),
                }
            )
    if not rows:
        return pd.DataFrame(columns=["日期", "收盤", "漲跌幅%", "來源", "即時"])
    df = pd.DataFrame(rows).drop_duplicates(subset=["日期"], keep="last")
    return df.sort_values("日期").reset_index(drop=True)


def _calc_stable_market_factors(row: dict[str, Any]) -> dict[str, Any]:
    df = _cache_to_market_df()
    close = _safe_float(row.get("close"))
    pct = _safe_float(row.get("pct"), 0) or 0
    if close is None and not df.empty:
        close = _safe_float(df["收盤"].iloc[-1])
    if close is None:
        close = 0.0

    if not df.empty:
        closes = pd.to_numeric(df["收盤"], errors="coerce").dropna()
    else:
        closes = pd.Series(dtype=float)

    ma5 = float(closes.tail(5).mean()) if len(closes) >= 2 else close
    ma20 = float(closes.tail(20).mean()) if len(closes) >= 5 else close
    high20 = float(closes.tail(20).max()) if len(closes) >= 2 else close
    low20 = float(closes.tail(20).min()) if len(closes) >= 2 else close

    dist_ma5 = ((close / ma5 - 1) * 100) if ma5 not in [None, 0] else 0
    dist_ma20 = ((close / ma20 - 1) * 100) if ma20 not in [None, 0] else 0
    pos20 = ((close - low20) / (high20 - low20) * 100) if high20 != low20 else 50

    score = 50
    score += max(min(pct * 8, 12), -12)
    score += 8 if close >= ma5 else -8
    score += 10 if close >= ma20 else -10
    if pos20 >= 75:
        score += 6
    elif pos20 <= 25:
        score -= 6
    score = max(0, min(100, score))

    if score >= 75:
        market_state = "偏多可操作"
        godpick_weight = "+10%"
        strategy = "追強與回測支撐並行"
    elif score >= 60:
        market_state = "中性偏多"
        godpick_weight = "+5%"
        strategy = "優先挑選低檔轉強與拉回承接"
    elif score >= 45:
        market_state = "震盪觀望"
        godpick_weight = "0%"
        strategy = "降低追價，等支撐回測"
    elif score >= 30:
        market_state = "偏弱保守"
        godpick_weight = "-10%"
        strategy = "只做高勝率低檔止跌，不追高"
    else:
        market_state = "風險偏高"
        godpick_weight = "-20%"
        strategy = "以觀望與資金控管為主"

    return {
        "大盤穩定分": round(score, 1),
        "大盤狀態": market_state,
        "股神推薦加權": godpick_weight,
        "今日策略": strategy,
        "MA5": ma5,
        "MA20": ma20,
        "距MA5%": dist_ma5,
        "距MA20%": dist_ma20,
        "20日位置%": pos20,
        "快取筆數": len(df),
    }


def _render_stable_factor_block(row: dict[str, Any]):
    factors = _calc_stable_market_factors(row)
    st.markdown("### 大盤穩定因子")
    render_pro_kpi_row([
        {
            "label": "大盤穩定分",
            "value": f"{_safe_float(factors.get('大盤穩定分'), 0):.1f}",
            "delta": _safe_str(factors.get("大盤狀態")),
            "delta_class": "pro-kpi-delta-flat",
        },
        {
            "label": "股神推薦加權",
            "value": _safe_str(factors.get("股神推薦加權")),
            "delta": "僅作推薦頁參考",
            "delta_class": "pro-kpi-delta-flat",
        },
        {
            "label": "今日策略",
            "value": _safe_str(factors.get("今日策略")),
            "delta": f"快取 {factors.get('快取筆數')} 筆",
            "delta_class": "pro-kpi-delta-flat",
        },
        {
            "label": "20日位置",
            "value": f"{_safe_float(factors.get('20日位置%'), 0):.1f}%",
            "delta": f"距MA20 {_safe_float(factors.get('距MA20%'), 0):+.2f}%",
            "delta_class": "pro-kpi-delta-flat",
        },
    ])

    with st.expander("大盤穩定因子明細", expanded=False):
        st.dataframe(_cache_to_market_df(), use_container_width=True, hide_index=True)
        st.json(factors)


def _read_bg_jobs() -> dict[str, Any]:
    p = Path(MACRO_BG_JOB_FILE)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_bg_jobs(data: dict[str, Any]) -> None:
    try:
        Path(MACRO_BG_JOB_FILE).write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    except Exception:
        pass


def _cleanup_stale_jobs() -> None:
    """
    v29：背景工作熔斷。
    Streamlit Cloud / 外部端點有時會讓 thread 狀態停在 running。
    超過 MACRO_BG_MAX_RUNNING_SECONDS 就自動改成 timeout，避免畫面永遠 running。
    """
    jobs = _read_bg_jobs()
    if not isinstance(jobs, dict) or not jobs:
        return
    changed = False
    now_ts = time.time()
    for name, item in list(jobs.items()):
        if not isinstance(item, dict):
            continue
        status = _safe_str(item.get("status"))
        started = _safe_float(item.get("started_ts"), 0) or 0
        if status == "running" and started > 0 and (now_ts - started) > MACRO_BG_MAX_RUNNING_SECONDS:
            item["status"] = "timeout"
            item["message"] = "背景更新逾時，已自動熔斷；保留舊快取，不影響頁面。"
            item["updated_at"] = _tw_now().strftime("%Y-%m-%d %H:%M:%S")
            jobs[name] = item
            changed = True
    if changed:
        _write_bg_jobs(jobs)


def _reset_bg_jobs() -> None:
    _write_bg_jobs({})


def _job_is_recent(job_name: str, seconds: int = MACRO_AUTO_REFRESH_SECONDS) -> bool:
    _cleanup_stale_jobs()
    jobs = _read_bg_jobs()
    item = jobs.get(job_name, {})
    if not isinstance(item, dict):
        return False
    ts = _safe_float(item.get("started_ts"), 0) or 0
    status = _safe_str(item.get("status"))
    # timeout/error 不視為 recent，可重新啟動；running 只在未逾時時視為 recent。
    return (time.time() - ts) < seconds and status in {"running", "finished"}


def _set_job_status(job_name: str, status: str, message: str = "") -> None:
    jobs = _read_bg_jobs()
    old_ts = jobs.get(job_name, {}).get("started_ts", time.time()) if isinstance(jobs.get(job_name), dict) else time.time()
    jobs[job_name] = {
        "status": status,
        "message": message,
        "started_ts": time.time() if status == "running" else old_ts,
        "updated_at": _tw_now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    _write_bg_jobs(jobs)


def _background_update_worker(target_date_text: str) -> None:
    try:
        d = pd.to_datetime(target_date_text, errors="coerce")
        if pd.isna(d):
            d = pd.Timestamp(_tw_now().date())
        d = d.date()

        msgs = []

        try:
            mkt = _fetch_market_with_fallback(d, realtime=(9 <= _tw_now().hour < 14))
            if isinstance(mkt, dict) and mkt.get("ok"):
                _save_market_row(mkt)
                msgs.append(f"大盤更新成功:{mkt.get('source')}")
            else:
                msgs.append("大盤略過")
        except Exception:
            msgs.append("大盤例外略過")

        try:
            inst = _fetch_twse_institutional_manual(d, timeout=2.0)
            if isinstance(inst, dict) and inst.get("ok"):
                _save_inst_row(inst)
                msgs.append("法人更新成功")
            else:
                msgs.append("法人略過")
        except Exception:
            msgs.append("法人例外略過")

        try:
            added, _ = _fetch_us_market_manual(d)
            msgs.append(f"外盤成功 {added} 項")
        except Exception:
            msgs.append("外盤例外略過")

        try:
            tx = _fetch_taifex_futures_manual(d, timeout=2.0)
            if isinstance(tx, dict) and tx.get("ok"):
                _save_taifex_row(tx)
                msgs.append("期貨更新成功")
            else:
                msgs.append("期貨略過")
        except Exception:
            msgs.append("期貨例外略過")

        try:
            row = _default_market_row(d)
            ok, bridge_msg = _write_macro_bridge(row)
            msgs.append("橋接成功" if ok else f"橋接失敗:{bridge_msg}")
        except Exception:
            msgs.append("橋接例外略過")

        _set_job_status("macro_auto_bg", "finished", " / ".join(msgs))
    except Exception as e:
        _set_job_status("macro_auto_bg", "error", str(e))


def _maybe_start_background_update(target_date: date, enabled: bool = True) -> None:
    if not enabled:
        return
    if _job_is_recent("macro_auto_bg", MACRO_AUTO_REFRESH_SECONDS):
        return
    _set_job_status("macro_auto_bg", "running", "背景更新中")
    t = threading.Thread(
        target=_background_update_worker,
        args=(pd.to_datetime(target_date).strftime("%Y-%m-%d"),),
        daemon=True,
    )
    t.start()


def _render_background_update_status():
    _cleanup_stale_jobs()
    jobs = _read_bg_jobs()
    item = jobs.get("macro_auto_bg", {}) if isinstance(jobs, dict) else {}
    status = _safe_str(item.get("status")) or "尚未啟動"
    msg = _safe_str(item.get("message"))
    updated = _safe_str(item.get("updated_at"))
    st.markdown("### 自動背景更新狀態")
    c1, c2, c3, c4 = st.columns([1.1, 2.8, 1.5, 1.1])
    with c1:
        st.metric("背景狀態", status)
    with c2:
        st.caption(msg or "啟用後會背景更新大盤、法人、外盤與橋接檔；不等待、不卡頁。")
    with c3:
        st.caption(updated or "尚未更新")
    with c4:
        if st.button("重置背景狀態", use_container_width=True, key=_k("reset_bg_jobs")):
            _reset_bg_jobs()
            st.success("已重置背景狀態。")
            st.rerun()

    if status == "running":
        st.info(f"背景更新執行中；若超過 {MACRO_BG_MAX_RUNNING_SECONDS} 秒會自動熔斷，不會一直 running。")
    elif status == "timeout":
        st.warning("背景更新逾時，已熔斷。可按「重置背景狀態」後重新啟動，或保留舊快取。")
    elif status == "error":
        st.warning("背景更新失敗，頁面仍使用舊快取。")





# ===== v29.5 missing block safety patch =====
def _safe_us_cache_to_df_v285() -> pd.DataFrame:
    try:
        if "_us_cache_to_df" in globals():
            return _us_cache_to_df()
    except Exception:
        pass
    return pd.DataFrame()


def _safe_default_us_market_row_v285(target_date: date) -> dict[str, Any]:
    try:
        if "_default_us_market_row" in globals():
            row = _default_us_market_row(target_date)
            return row if isinstance(row, dict) else {}
    except Exception:
        pass
    return {}


def _safe_us_market_score_v285(us_row: dict[str, Any]) -> dict[str, Any]:
    try:
        if "_us_market_score" in globals():
            return _us_market_score(us_row)
    except Exception:
        pass
    return {"外盤分數": 50.0, "外盤狀態": "尚未啟用", "外盤建議": "外盤函式未載入，暫不納入。"}


def _render_us_market_block(target_date: date):
    """
    v29：外盤區塊安全版。
    避免舊包 main 呼叫 _render_us_market_block 但函式未定義造成 NameError。
    """
    st.markdown("### 外盤自動背景 / 快取")
    us_row = _safe_default_us_market_row_v285(target_date)
    us_score = _safe_us_market_score_v285(us_row)

    c1, c2, c3, c4 = st.columns([1.25, 1.1, 1.1, 2.2])
    with c1:
        update_us = st.button("背景更新外盤", use_container_width=True)
    with c2:
        st.metric("外盤分數", f"{_safe_float(us_score.get('外盤分數'), 50):.1f}")
    with c3:
        st.metric("外盤狀態", _safe_str(us_score.get("外盤狀態")))
    with c4:
        st.caption("外盤資料採背景/快取模式；不在主畫面等待，避免卡住。")

    if update_us:
        if "_fetch_us_market_manual" in globals():
            try:
                def _us_worker():
                    try:
                        added, _ = _fetch_us_market_manual(target_date)
                        _set_job_status("us_auto_bg", "finished", f"外盤更新完成，成功 {added} 項")
                    except Exception as e:
                        _set_job_status("us_auto_bg", "error", f"外盤背景更新失敗：{e}")
                _set_job_status("us_auto_bg", "running", "外盤背景更新中")
                threading.Thread(target=_us_worker, daemon=True).start()
                st.success("已啟動外盤背景更新，畫面不會等待。")
            except Exception as e:
                st.warning(f"外盤背景更新啟動失敗：{e}")
        else:
            st.warning("目前版本未載入外盤更新函式，請先保留快取資料或升級完整包。")

    jobs = _read_bg_jobs() if "_read_bg_jobs" in globals() else {}
    item = jobs.get("us_auto_bg", {}) if isinstance(jobs, dict) else {}
    if item:
        st.caption(f"外盤背景狀態：{item.get('status')}｜{item.get('message')}｜{item.get('updated_at')}")

    items = ["NASDAQ", "SOX半導體", "S&P500", "VIX", "台積電ADR", "美元台幣"]
    cols = st.columns(6)
    for col, name in zip(cols, items):
        item_data = us_row.get(name) if isinstance(us_row, dict) else {}
        with col:
            st.markdown(
                f"""
                <div style="border:1px solid #e2e8f0;border-radius:14px;padding:12px;background:#ffffff;min-height:88px;">
                    <div style="font-size:13px;color:#64748b;font-weight:800;">{name}</div>
                    <div style="font-size:15px;color:#0f172a;font-weight:900;margin-top:8px;">{_safe_float((item_data or {}).get('pct'), 0):+.2f}%</div>
                    <div style="font-size:12px;color:#64748b;">{_safe_str((item_data or {}).get('date'))}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    with st.expander("外盤快取明細", expanded=False):
        df = _safe_us_cache_to_df_v285()
        st.dataframe(df, use_container_width=True, hide_index=True)
        if df is not None and not df.empty:
            st.download_button(
                "下載外盤快取CSV",
                data=df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"),
                file_name="macro_us_market_cache.csv",
                mime="text/csv",
                use_container_width=True,
            )


def _background_taifex_worker_v285(target_date_text: str) -> None:
    try:
        d = pd.to_datetime(target_date_text, errors="coerce")
        if pd.isna(d):
            d = pd.Timestamp(_tw_now().date())
        d = d.date()

        if "_fetch_taifex_futures_manual" not in globals():
            _set_job_status("taifex_auto_bg", "error", "目前版本未載入TAIFEX抓取函式")
            return

        row = _fetch_taifex_futures_manual(d, timeout=2.0)
        if isinstance(row, dict) and row.get("ok"):
            _save_taifex_row(row)
            _set_job_status("taifex_auto_bg", "finished", f"期貨更新成功：{row.get('source')} / 收盤 {row.get('tx_close')} / 漲跌 {row.get('tx_change')}")
        else:
            _set_job_status("taifex_auto_bg", "error", f"期貨更新失敗：{(row or {}).get('error', 'unknown')}")
    except Exception as e:
        _set_job_status("taifex_auto_bg", "error", f"期貨背景更新例外：{e}")


def _start_taifex_background_update_v285(target_date: date, force: bool = False) -> None:
    try:
        if not force and _job_is_recent("taifex_auto_bg", MACRO_AUTO_REFRESH_SECONDS):
            return
        _set_job_status("taifex_auto_bg", "running", "期貨背景更新中")
        t = threading.Thread(
            target=_background_taifex_worker_v285,
            args=(pd.to_datetime(target_date).strftime("%Y-%m-%d"),),
            daemon=True,
        )
        t.start()
    except Exception as e:
        _set_job_status("taifex_auto_bg", "error", f"期貨背景啟動失敗：{e}")


def _render_taifex_bg_status_v285():
    _sync_taifex_job_status_from_cache()
    _cleanup_stale_jobs()
    jobs = _read_bg_jobs() if "_read_bg_jobs" in globals() else {}
    item = jobs.get("taifex_auto_bg", {}) if isinstance(jobs, dict) else {}
    status = _safe_str(item.get("status")) or "尚未啟動"
    msg = _safe_str(item.get("message"))
    updated = _safe_str(item.get("updated_at"))
    c1, c2, c3, c4 = st.columns([1.1, 3.0, 1.5, 1.1])
    with c1:
        st.metric("期貨背景狀態", status)
    with c2:
        st.caption(msg or "按下更新後會背景抓 TAIFEX；若逾時會自動熔斷。")
    with c3:
        st.caption(updated or "尚未更新")
    with c4:
        if st.button("重置期貨狀態", use_container_width=True, key=_k("reset_taifex_job")):
            jobs = _read_bg_jobs()
            if "taifex_auto_bg" in jobs:
                jobs.pop("taifex_auto_bg", None)
                _write_bg_jobs(jobs)
            st.success("已重置期貨背景狀態。")
            st.rerun()
    if status == "running":
        st.info(f"期貨背景更新中；若超過 {MACRO_BG_MAX_RUNNING_SECONDS} 秒會自動熔斷。")
    elif status == "timeout":
        st.warning("期貨背景更新逾時，已熔斷；保留舊快取。")





def _render_taifex_block(target_date: date):
    """
    v29.5：期貨狀態同步版。
    確保期貨不會因 v29.5 patch 遺失函式而消失。
    """
    st.markdown("### 期貨自動背景更新")
    row = _default_taifex_row(target_date) if "_default_taifex_row" in globals() else {}
    score = _taifex_score(row) if "_taifex_score" in globals() else {"期貨分數": 50, "期貨狀態": "尚未更新", "期貨建議": "尚未納入期貨資料"}

    c1, c2, c3, c4, c5 = st.columns([1.3, 1.1, 1.1, 1.1, 2.1])
    with c1:
        update_taifex = st.button("背景更新台指期", use_container_width=True)
    with c2:
        st.metric("期貨分數", f"{_safe_float(score.get('期貨分數'), 50):.1f}")
    with c3:
        st.metric("期貨狀態", _safe_str(score.get("期貨狀態")))
    with c4:
        st.metric("期貨漲跌", f"{_safe_float(row.get('tx_change'), 0):+.0f}" + (f"｜{_safe_float(row.get('tx_pct'), 0):+.2f}%" if row.get("tx_pct") is not None else ""))
    with c5:
        st.caption("期貨採背景自動更新，不在主畫面等待。")

    if update_taifex:
        _start_taifex_background_update_v285(target_date, force=True)
        st.success("已啟動期貨背景更新。頁面不會等待，稍後重新整理或切頁回來即可看到結果。")

    _render_taifex_bg_status_v285()

    cols = st.columns(4)
    cards = [
        ("資料日期", _safe_str(row.get("date")) or "尚未更新"),
        ("期貨收盤/指數", f"{_safe_float(row.get('tx_close'), 0):,.0f}"),
        ("期貨漲跌", f"{_safe_float(row.get('tx_change'), 0):+.0f}" + (f"｜{_safe_float(row.get('tx_pct'), 0):+.2f}%" if row.get("tx_pct") is not None else "")),
        ("期貨建議", _safe_str(score.get("期貨建議"))),
    ]
    for col, (title, value) in zip(cols, cards):
        with col:
            st.markdown(
                f"""
                <div style="border:1px solid #e2e8f0;border-radius:14px;padding:12px;background:#ffffff;min-height:88px;">
                    <div style="font-size:13px;color:#64748b;font-weight:800;">{title}</div>
                    <div style="font-size:15px;color:#0f172a;font-weight:900;margin-top:8px;">{value}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    with st.expander("期貨快取明細", expanded=False):
        df = _taifex_cache_to_df() if "_taifex_cache_to_df" in globals() else pd.DataFrame()
        st.dataframe(df, use_container_width=True, hide_index=True)
        if df is not None and not df.empty:
            st.download_button(
                "下載期貨快取CSV",
                data=df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"),
                file_name="macro_taifex_cache.csv",
                mime="text/csv",
                use_container_width=True,
            )



# ===== v29.5 multi-source fallback pool overrides =====
def _v29_source_grade(source: str, is_proxy: bool = False, is_cache: bool = False) -> str:
    s = _safe_str(source)
    if is_cache:
        return "快取"
    if is_proxy:
        return "代理"
    if "TWSE" in s or "TAIFEX" in s:
        return "官方"
    if "Yahoo" in s or "FinMind" in s:
        return "備援"
    return "未知"


def _v29_write_source_status(name: str, status: str, source: str = "", grade: str = "", msg: str = ""):
    jobs = _read_bg_jobs()
    jobs[f"source_{name}"] = {
        "status": status,
        "source": source,
        "grade": grade,
        "message": msg,
        "updated_at": _tw_now().strftime("%Y-%m-%d %H:%M:%S"),
        "started_ts": time.time(),
    }
    _write_bg_jobs(jobs)


def _v29_recent_cache_row(cache_file: str) -> dict[str, Any]:
    try:
        p = Path(cache_file)
        if not p.exists():
            return {}
        data = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(data, dict) or not data:
            return {}
        keys = sorted([k for k, v in data.items() if isinstance(v, dict)], reverse=True)
        if not keys:
            return {}
        row = dict(data[keys[0]])
        row["cache_key"] = keys[0]
        row["source"] = (_safe_str(row.get("source")) or cache_file) + "｜最近快取"
        row["is_cache_fallback"] = True
        return row
    except Exception:
        return {}


def _fetch_market_with_fallback(target_date: date, realtime: bool = False) -> dict[str, Any]:
    """
    v29.5 大盤多來源備援池：
    TWSE MIS / MI_INDEX -> Yahoo ^TWII -> 最近快取。
    """
    tried = []

    # 1) TWSE official
    try:
        row = _fetch_twse_realtime(timeout=1.8) if realtime else _fetch_twse_close(target_date, timeout=2.2)
        tried.append(f"TWSE:{row.get('error') if isinstance(row, dict) else 'unknown'}")
        if isinstance(row, dict) and row.get("ok") and row.get("close") is not None:
            row["source_grade"] = "官方"
            _v29_write_source_status("大盤", "success", row.get("source"), "官方", "TWSE成功")
            return row
    except Exception as e:
        tried.append(f"TWSE例外:{e}")

    # 2) Yahoo backup
    try:
        if "_fetch_yahoo_chart" in globals():
            y = _fetch_yahoo_chart("^TWII", target_date, timeout=2.2)
            tried.append(f"Yahoo:{y.get('error') if isinstance(y, dict) else 'unknown'}")
            if isinstance(y, dict) and y.get("ok") and y.get("close") is not None:
                row = {
                    "ok": True,
                    "source": "Yahoo ^TWII 備援",
                    "source_grade": "備援",
                    "date": _safe_str(y.get("date")) or pd.to_datetime(target_date).strftime("%Y-%m-%d"),
                    "used_date": _safe_str(y.get("date")) or pd.to_datetime(target_date).strftime("%Y-%m-%d"),
                    "close": _safe_float(y.get("close")),
                    "pct": _safe_float(y.get("pct")),
                    "change_points": _calc_market_change_points({"close": _safe_float(y.get("close")), "pct": _safe_float(y.get("pct"))}),
                    "is_realtime": False,
                    "backup": True,
                    "updated_at": _tw_now().strftime("%Y-%m-%d %H:%M:%S"),
                }
                _v29_write_source_status("大盤", "success", row.get("source"), "備援", "TWSE失敗，Yahoo成功")
                return row
    except Exception as e:
        tried.append(f"Yahoo例外:{e}")

    # 3) cache fallback
    cache = _v29_recent_cache_row(CACHE_FILE)
    if cache and cache.get("close") is not None:
        cache["ok"] = True
        cache["source_grade"] = "快取"
        _v29_write_source_status("大盤", "cache", cache.get("source"), "快取", "使用最近可用快取")
        return cache

    _v29_write_source_status("大盤", "failed", "多來源", "失敗", " / ".join(tried[-6:]))
    return {
        "ok": False,
        "source": "大盤多來源備援",
        "source_grade": "失敗",
        "date": pd.to_datetime(target_date).strftime("%Y-%m-%d"),
        "error": "大盤多來源皆失敗：" + " / ".join(tried[-6:]),
    }


def _fetch_twse_institutional_manual(target_date: date, timeout: float = 2.5) -> dict[str, Any]:
    """
    v29.5 法人多來源備援池：
    TWSE BFI82U 金額 -> TWSE T86 代理 -> FinMind 代理 -> 最近快取。
    """
    target_dt = pd.to_datetime(target_date).date()

    def _candidate_dates(end_date: date, max_days: int = 15) -> list[date]:
        out, cur, guard = [], end_date, 0
        while len(out) < max_days and guard < 35:
            if cur.weekday() < 5:
                out.append(cur)
            cur -= timedelta(days=1)
            guard += 1
        return out

    def _to_float_plain(v):
        s = _safe_str(v).replace(",", "").replace("+", "")
        if not s or s in {"-", "--", "None", "nan"}:
            return None
        try:
            return float(s)
        except Exception:
            return None

    def _parse_bfi82u(data: dict[str, Any], used_date: date) -> dict[str, Any]:
        rows = []
        if isinstance(data, dict):
            rows = data.get("data") or data.get("aaData") or []
            if not rows and isinstance(data.get("tables"), list):
                for t in data.get("tables") or []:
                    if isinstance(t, dict) and isinstance(t.get("data"), list):
                        rows.extend(t.get("data") or [])
        if not isinstance(rows, list) or not rows:
            return {}

        foreign = invest = dealer = total = None
        raw_rows = []
        for row in rows:
            if not isinstance(row, list) or len(row) < 2:
                continue
            label = _safe_str(row[0])
            joined = " ".join(_safe_str(x) for x in row)
            nums = [_extract_money_100m(x) for x in row[1:]]
            nums = [x for x in nums if x is not None]
            if not nums:
                continue
            val = nums[-1]
            raw_rows.append({"項目": label or joined[:20], "買賣超億元": val})
            if "外資" in label or "外資" in joined:
                foreign = val
            elif "投信" in label or "投信" in joined:
                invest = val
            elif "自營商" in label or "自營商" in joined:
                dealer = val
            elif "合計" in label or "總計" in label or "三大法人" in joined:
                total = val
        if total is None and any(x is not None for x in [foreign, invest, dealer]):
            total = sum([x for x in [foreign, invest, dealer] if x is not None])
        if any(x is not None for x in [foreign, invest, dealer, total]):
            return {
                "ok": True,
                "date": pd.to_datetime(used_date).strftime("%Y-%m-%d"),
                "source": "TWSE BFI82U 三大法人",
                "source_grade": "官方",
                "unit": "億元",
                "foreign_100m": foreign,
                "investment_trust_100m": invest,
                "dealer_100m": dealer,
                "total_100m": total,
                "raw_rows": raw_rows,
                "updated_at": _tw_now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        return {}

    def _parse_t86_proxy(data: dict[str, Any], used_date: date, source_name: str = "TWSE T86 法人方向代理") -> dict[str, Any]:
        rows, fields = [], []
        if isinstance(data, dict):
            rows = data.get("data") or data.get("aaData") or []
            fields = data.get("fields") or data.get("stat") or []
        if not isinstance(rows, list) or not rows:
            return {}

        field_names = [_safe_str(x) for x in fields] if isinstance(fields, list) else []
        foreign = invest = dealer = 0.0
        used = 0
        for row in rows:
            if not isinstance(row, list):
                continue
            used += 1
            row_map = {field_names[i]: row[i] for i in range(min(len(field_names), len(row)))}
            f_val = i_val = d_val = None
            for k, v in row_map.items():
                kk = _safe_str(k)
                if f_val is None and ("外陸資買賣超" in kk or "外資買賣超" in kk):
                    f_val = _to_float_plain(v)
                if i_val is None and "投信買賣超" in kk:
                    i_val = _to_float_plain(v)
                if d_val is None and "自營商買賣超" in kk:
                    d_val = _to_float_plain(v)
            if f_val is None and len(row) > 4:
                f_val = _to_float_plain(row[4])
            if i_val is None and len(row) > 7:
                i_val = _to_float_plain(row[7])
            if d_val is None and len(row) > 10:
                d_val = _to_float_plain(row[10])
            foreign += f_val or 0
            invest += i_val or 0
            dealer += d_val or 0

        f = foreign / 100000000
        i = invest / 100000000
        d = dealer / 100000000
        total = f + i + d
        if used <= 0 or (abs(f) < 1e-9 and abs(i) < 1e-9 and abs(d) < 1e-9):
            return {}
        return {
            "ok": True,
            "date": pd.to_datetime(used_date).strftime("%Y-%m-%d"),
            "source": source_name,
            "source_grade": "代理",
            "unit": "億股代理",
            "is_proxy": True,
            "foreign_100m": f,
            "investment_trust_100m": i,
            "dealer_100m": d,
            "total_100m": total,
            "raw_rows": [{"項目": "全市場加總", "樣本數": used, "單位": "億股代理"}],
            "updated_at": _tw_now().strftime("%Y-%m-%d %H:%M:%S"),
            "note": "金額資料不可用，使用股數方向代理。",
        }

    def _fetch_finmind_proxy(used_date: date) -> dict[str, Any]:
        day = pd.to_datetime(used_date).strftime("%Y-%m-%d")
        params = {"dataset": "TaiwanStockInstitutionalInvestorsBuySell", "start_date": day, "end_date": day}
        for url in ["https://api.finmindtrade.com/api/v4/data", "https://api.finmindtrade.com/api/v3/data"]:
            try:
                r = requests.get(url, params=params, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout, verify=False)
                if r.status_code != 200:
                    continue
                rows = (r.json() or {}).get("data") or []
                if not isinstance(rows, list) or not rows:
                    continue
                foreign = invest = dealer = 0.0
                used = 0
                for item in rows:
                    if not isinstance(item, dict):
                        continue
                    name = _safe_str(item.get("name") or item.get("institutional_investors"))
                    buy = _safe_float(item.get("buy"), 0) or 0
                    sell = _safe_float(item.get("sell"), 0) or 0
                    net = buy - sell
                    used += 1
                    lname = name.lower()
                    if "foreign" in lname or "外資" in name:
                        foreign += net
                    elif "investment" in lname or "trust" in lname or "投信" in name:
                        invest += net
                    elif "dealer" in lname or "自營" in name:
                        dealer += net
                f, i, d = foreign / 100000000, invest / 100000000, dealer / 100000000
                total = f + i + d
                if used > 0 and abs(total) > 1e-9:
                    return {
                        "ok": True,
                        "date": day,
                        "source": "FinMind 法人方向代理",
                        "source_grade": "代理",
                        "unit": "億股代理",
                        "is_proxy": True,
                        "foreign_100m": f,
                        "investment_trust_100m": i,
                        "dealer_100m": d,
                        "total_100m": total,
                        "raw_rows": [{"項目": "FinMind全市場加總", "樣本數": used, "單位": "億股代理"}],
                        "updated_at": _tw_now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
            except Exception:
                continue
        return {}

    tried = []
    for d in _candidate_dates(target_dt, max_days=15):
        ymd = pd.to_datetime(d).strftime("%Y%m%d")

        for url in [
            f"https://www.twse.com.tw/rwd/zh/fund/BFI82U?dayDate={ymd}&weekDate={ymd}&monthDate={ymd}&type=day&response=json",
            f"https://www.twse.com.tw/fund/BFI82U?dayDate={ymd}&weekDate={ymd}&monthDate={ymd}&type=day&response=json",
        ]:
            tried.append(f"{ymd} BFI82U")
            try:
                r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout, verify=False)
                if r.status_code == 200:
                    row = _parse_bfi82u(r.json(), d)
                    if row:
                        _v29_write_source_status("法人", "success", row.get("source"), "官方", "BFI82U成功")
                        return row
            except Exception:
                pass

        for url in [
            f"https://www.twse.com.tw/rwd/zh/fund/T86?date={ymd}&selectType=ALLBUT0999&response=json",
            f"https://www.twse.com.tw/fund/T86?response=json&date={ymd}&selectType=ALLBUT0999",
            f"https://www.twse.com.tw/rwd/zh/fund/T86?date={ymd}&selectType=ALL&response=json",
        ]:
            tried.append(f"{ymd} T86代理")
            try:
                r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout, verify=False)
                if r.status_code == 200:
                    row = _parse_t86_proxy(r.json(), d)
                    if row:
                        _v29_write_source_status("法人", "success", row.get("source"), "代理", "T86代理成功")
                        return row
            except Exception:
                pass

        tried.append(f"{ymd} FinMind代理")
        row = _fetch_finmind_proxy(d)
        if row:
            _v29_write_source_status("法人", "success", row.get("source"), "代理", "FinMind代理成功")
            return row

    cache = _v29_recent_cache_row(INST_CACHE_FILE)
    if cache and cache.get("total_100m") is not None:
        cache["ok"] = True
        cache["source_grade"] = "快取"
        _v29_write_source_status("法人", "cache", cache.get("source"), "快取", "使用最近可用法人快取")
        return cache

    _v29_write_source_status("法人", "failed", "多來源", "失敗", " / ".join(tried[-10:]))
    return {
        "ok": False,
        "date": pd.to_datetime(target_date).strftime("%Y-%m-%d"),
        "source": "法人多來源備援",
        "source_grade": "失敗",
        "error": "BFI82U、T86、FinMind、快取皆失敗。",
        "tried": tried[-18:],
    }


def _fetch_taifex_futures_manual(target_date: date, timeout: float = 2.2) -> dict[str, Any]:
    """
    v29.5 期貨來源優先順序修正版：
    1. TAIFEX OpenAPI：官方台指期，優先使用。
    2. 最近 TAIFEX/官方快取：若官方端點短暫失敗，優先沿用最近官方快取。
    3. Yahoo IX0126.TW：只作備援參考；若無漲跌點/與台指期級距差太大，不覆蓋官方快取。
    4. 任一來源都不在主畫面等待；背景執行成功即寫快取。
    """
    target_dt = pd.to_datetime(target_date).date()
    tried = []

    def _official_cache_row():
        cache = _v29_recent_cache_row(TAIFEX_CACHE_FILE)
        if cache and cache.get("tx_close") is not None:
            src = _safe_str(cache.get("source"))
            # 優先使用 TAIFEX/官方快取，不讓 Yahoo 備援覆蓋官方資料。
            if "TAIFEX" in src or _safe_str(cache.get("source_grade")) == "官方":
                cache["ok"] = True
                cache["source_grade"] = "快取"
                cache["note"] = "TAIFEX 官方端點暫時不可用，沿用最近官方期貨快取。"
                return cache
        return {}

    # 1) TAIFEX OpenAPI official first
    ymd_dash = pd.to_datetime(target_dt).strftime("%Y-%m-%d")
    ymd_slash = pd.to_datetime(target_dt).strftime("%Y/%m/%d")
    ymd_plain = pd.to_datetime(target_dt).strftime("%Y%m%d")

    for url in [
        "https://openapi.taifex.com.tw/v1/DailyMarketReportFut",
        "https://openapi.taifex.com.tw/v1/FutDailyMarketReport",
    ]:
        tried.append(url.split("/")[-1])
        try:
            r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout, verify=False)
            if r.status_code != 200:
                tried.append(f"{url.split('/')[-1]} HTTP {r.status_code}")
                continue
            data = r.json()
            if not isinstance(data, list):
                continue

            chosen = None
            for item in data:
                if not isinstance(item, dict):
                    continue
                txt = " ".join(_safe_str(v) for v in item.values())
                is_tx = ("TX" in txt or "TXF" in txt or "臺股期貨" in txt or "台股期貨" in txt or "臺指" in txt or "台指" in txt)
                is_date = (ymd_dash in txt or ymd_slash in txt or ymd_plain in txt)
                if is_tx and is_date:
                    chosen = item
                    break
            if chosen is None:
                for item in data:
                    if isinstance(item, dict):
                        txt = " ".join(_safe_str(v) for v in item.values())
                        if "TX" in txt or "TXF" in txt or "臺股期貨" in txt or "台股期貨" in txt:
                            chosen = item
                            break

            if chosen:
                close_val = None
                change_val = None
                vol_val = None
                for k, v in chosen.items():
                    kk = _safe_str(k)
                    if close_val is None and any(x in kk for x in ["收盤", "最後", "Close", "close"]):
                        close_val = _safe_float(v)
                    if change_val is None and any(x in kk for x in ["漲跌", "Change", "change"]):
                        change_val = _safe_float(v)
                    if vol_val is None and any(x in kk for x in ["成交量", "Volume", "volume"]):
                        vol_val = _safe_float(v)

                nums = [_safe_float(v) for v in chosen.values()]
                nums = [x for x in nums if x is not None]
                if close_val is None:
                    cands = [x for x in nums if 5000 <= abs(x) <= 50000]
                    if cands:
                        close_val = cands[-1]
                if change_val is None:
                    cands = [x for x in nums if -2000 <= x <= 2000 and x != 0]
                    if cands:
                        change_val = cands[-1]

                if close_val is not None:
                    row = {
                        "ok": True,
                        "date": ymd_dash,
                        "source": "TAIFEX OpenAPI 台指期",
                        "source_grade": "官方",
                        "tx_close": close_val,
                        "tx_change": change_val,
                        "tx_pct": None,
                        "tx_volume": vol_val,
                        "raw": chosen,
                        "updated_at": _tw_now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    _v29_write_source_status("期貨", "success", row.get("source"), "官方", "TAIFEX OpenAPI成功")
                    return row
        except Exception as e:
            tried.append(f"TAIFEX例外:{e}")

    # 2) Use official cache before Yahoo backup
    official_cache = _official_cache_row()
    if official_cache:
        _v29_write_source_status("期貨", "cache", official_cache.get("source"), "快取", "沿用最近官方期貨快取")
        return official_cache

    # 3) Yahoo futures index as low-confidence reference only
    try:
        if "_fetch_yahoo_chart" in globals():
            y = _fetch_yahoo_chart("IX0126.TW", target_dt, timeout=timeout)
            tried.append(f"Yahoo IX0126.TW:{y.get('error') if isinstance(y, dict) else 'unknown'}")
            if isinstance(y, dict) and y.get("ok") and y.get("close") is not None:
                close_val = _safe_float(y.get("close"))
                pct_val = _safe_float(y.get("pct"))
                change_val = None
                if close_val is not None and pct_val is not None and abs(100 + pct_val) > 1e-9:
                    change_val = close_val * pct_val / (100 + pct_val)

                # 如果 Yahoo 沒有漲跌幅/漲跌點，只能當參考，不用來決策。
                if pct_val is None and change_val is None:
                    raise ValueError("Yahoo IX0126.TW 無漲跌幅，僅能參考，不寫入主要期貨資料")

                row = {
                    "ok": True,
                    "date": _safe_str(y.get("date")) or pd.to_datetime(target_dt).strftime("%Y-%m-%d"),
                    "source": "Yahoo IX0126.TW 期貨指數備援",
                    "source_grade": "備援",
                    "is_reference_only": True,
                    "tx_close": close_val,
                    "tx_change": change_val,
                    "tx_pct": pct_val,
                    "tx_volume": _safe_float(y.get("volume")),
                    "raw": {"symbol": "IX0126.TW", "name": "TIP TAIFEX TAIEX Futures Index"},
                    "updated_at": _tw_now().strftime("%Y-%m-%d %H:%M:%S"),
                    "note": "Yahoo IX0126.TW 是期貨指數備援，不等同台指期近月合約；僅作方向參考。",
                }
                _v29_write_source_status("期貨", "success", row.get("source"), "備援", "Yahoo IX0126.TW成功，僅作參考")
                return row
    except Exception as e:
        tried.append(f"Yahoo IX0126例外:{e}")

    # 4) Any cache fallback
    cache = _v29_recent_cache_row(TAIFEX_CACHE_FILE)
    if cache and cache.get("tx_close") is not None:
        cache["ok"] = True
        cache["source_grade"] = "快取"
        _v29_write_source_status("期貨", "cache", cache.get("source"), "快取", "使用最近可用期貨快取")
        return cache

    _v29_write_source_status("期貨", "failed", "TAIFEX / Yahoo / 快取", "失敗", " / ".join(tried[-8:]))
    return {
        "ok": False,
        "date": ymd_dash,
        "source": "期貨多來源備援",
        "source_grade": "失敗",
        "error": "TAIFEX OpenAPI、官方快取、Yahoo IX0126.TW與一般快取皆失敗。",
        "tried": tried[-10:],
    }


def _background_update_worker(target_date_text: str) -> None:
    """
    v29：各資料源獨立狀態，不再全部綁在一個 running。
    """
    _set_job_status("macro_auto_bg", "running", "背景更新啟動")
    try:
        d = pd.to_datetime(target_date_text, errors="coerce")
        if pd.isna(d):
            d = pd.Timestamp(_tw_now().date())
        d = d.date()

        summary = []

        try:
            mkt = _fetch_market_with_fallback(d, realtime=(9 <= _tw_now().hour < 14))
            if isinstance(mkt, dict) and mkt.get("ok"):
                _save_market_row(mkt)
                summary.append(f"大盤:{mkt.get('source_grade') or mkt.get('source')}")
            else:
                summary.append("大盤:失敗")
        except Exception as e:
            _v29_write_source_status("大盤", "error", "大盤", "失敗", str(e))
            summary.append("大盤:例外")

        try:
            inst = _fetch_twse_institutional_manual(d, timeout=2.2)
            if isinstance(inst, dict) and inst.get("ok"):
                _save_inst_row(inst)
                summary.append(f"法人:{inst.get('source_grade') or inst.get('source')}")
            else:
                summary.append("法人:失敗")
        except Exception as e:
            _v29_write_source_status("法人", "error", "法人", "失敗", str(e))
            summary.append("法人:例外")

        try:
            if "_fetch_us_market_manual" in globals():
                added, _ = _fetch_us_market_manual(d)
                _v29_write_source_status("外盤", "success" if added > 0 else "failed", "Yahoo 外盤", "備援", f"成功 {added} 項")
                summary.append(f"外盤:{added}項")
            else:
                _v29_write_source_status("外盤", "failed", "外盤", "失敗", "外盤函式不存在")
                summary.append("外盤:函式缺失")
        except Exception as e:
            _v29_write_source_status("外盤", "error", "外盤", "失敗", str(e))
            summary.append("外盤:例外")

        try:
            otc = _fetch_otc_with_fallback(d, timeout=2.2) if "_fetch_otc_with_fallback" in globals() else {}
            if isinstance(otc, dict) and otc.get("ok"):
                _save_otc_row(otc)
                summary.append(f"櫃買:{otc.get('source_grade') or otc.get('source')}")
            else:
                summary.append("櫃買:失敗")
        except Exception as e:
            _v29_write_source_status("櫃買", "error", "櫃買", "失敗", str(e))
            summary.append("櫃買:例外")

        try:
            tx = _fetch_taifex_futures_manual(d, timeout=2.2)
            if isinstance(tx, dict) and tx.get("ok"):
                _save_taifex_row(tx)
                summary.append(f"期貨:{tx.get('source_grade') or tx.get('source')}")
            else:
                summary.append("期貨:失敗")
        except Exception as e:
            _v29_write_source_status("期貨", "error", "期貨", "失敗", str(e))
            summary.append("期貨:例外")

        try:
            row = _default_market_row(d)
            ok, bridge_msg = _write_macro_bridge(row)
            summary.append("橋接:成功" if ok else "橋接:失敗")
        except Exception:
            summary.append("橋接:例外")

        _set_job_status("macro_auto_bg", "finished", " / ".join(summary))
    except Exception as e:
        _set_job_status("macro_auto_bg", "error", str(e))


def _render_background_update_status():
    _cleanup_stale_jobs()
    jobs = _read_bg_jobs()
    item = jobs.get("macro_auto_bg", {}) if isinstance(jobs, dict) else {}
    status = _safe_str(item.get("status")) or "尚未啟動"
    msg = _safe_str(item.get("message"))
    updated = _safe_str(item.get("updated_at"))
    st.markdown("### v29.5 多來源背景更新狀態")
    c1, c2, c3, c4 = st.columns([1.1, 2.8, 1.5, 1.1])
    with c1:
        st.metric("總狀態", status)
    with c2:
        st.caption(msg or "多來源背景更新：官方 → 備援 → 代理 → 快取。")
    with c3:
        st.caption(updated or "尚未更新")
    with c4:
        if st.button("重置背景狀態", use_container_width=True, key=_k("reset_bg_jobs_v29")):
            _reset_bg_jobs()
            st.success("已重置背景狀態。")
            st.rerun()

    source_rows = []
    for name in ["大盤", "櫃買", "法人", "外盤", "期貨"]:
        s = jobs.get(f"source_{name}", {}) if isinstance(jobs, dict) else {}
        source_rows.append({
            "資料": name,
            "狀態": _safe_str(s.get("status")) or "尚未更新",
            "來源": _safe_str(s.get("source")),
            "可信度": _safe_str(s.get("grade")),
            "訊息": _safe_str(s.get("message")),
            "時間": _safe_str(s.get("updated_at")),
        })
    st.dataframe(pd.DataFrame(source_rows), use_container_width=True, hide_index=True)

    if status == "running":
        st.info(f"背景更新執行中；若超過 {MACRO_BG_MAX_RUNNING_SECONDS} 秒會自動熔斷。")
    elif status in {"timeout", "error"}:
        st.warning("背景更新未完整成功；已保留可用快取，成功的資料源仍會顯示在上方表格。")



# ===== v29.5 source panel + auto bridge helpers =====
def _v294_source_status_df() -> pd.DataFrame:
    try:
        jobs = _read_bg_jobs()
    except Exception:
        jobs = {}
    rows = []
    for name in ["大盤", "櫃買", "法人", "外盤", "期貨"]:
        s = jobs.get(f"source_{name}", {}) if isinstance(jobs, dict) else {}
        rows.append({
            "資料源": name,
            "狀態": _safe_str(s.get("status")) or "尚未更新",
            "來源": _safe_str(s.get("source")),
            "可信度": _safe_str(s.get("grade")),
            "訊息": _safe_str(s.get("message")),
            "更新時間": _safe_str(s.get("updated_at")),
        })
    return pd.DataFrame(rows)


def _v294_count_source_health() -> dict[str, int]:
    df = _v294_source_status_df()
    if df.empty:
        return {"success": 0, "cache": 0, "failed": 0, "official": 0}
    success = int(df["狀態"].isin(["success", "cache", "finished"]).sum())
    cache = int((df["可信度"] == "快取").sum())
    failed = int(df["狀態"].isin(["failed", "error", "timeout"]).sum())
    official = int((df["可信度"] == "官方").sum())
    return {"success": success, "cache": cache, "failed": failed, "official": official}


def _v294_render_source_health_panel():
    st.markdown("### 資料源健康度")
    h = _v294_count_source_health()
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("可用資料源", h["success"])
    with c2:
        st.metric("官方來源", h["official"])
    with c3:
        st.metric("快取來源", h["cache"])
    with c4:
        st.metric("失敗來源", h["failed"])
    st.dataframe(_v294_source_status_df(), use_container_width=True, hide_index=True)


def _v294_bridge_quality(row: dict[str, Any]) -> dict[str, Any]:
    h = _v294_count_source_health()
    score = 50 + h["success"] * 10 + h["official"] * 5 - h["failed"] * 10 - h["cache"] * 4
    score = max(0, min(100, score))
    if score >= 80:
        level = "高"
        advice = "資料完整，可正常納入股神推薦風控。"
    elif score >= 60:
        level = "中高"
        advice = "資料大致可用，仍需留意非官方或快取來源。"
    elif score >= 40:
        level = "中"
        advice = "部分資料缺失，股神推薦應偏保守。"
    else:
        level = "低"
        advice = "資料不足，建議降低大盤因子權重。"
    return {
        "data_quality_score": round(score, 1),
        "data_quality_level": level,
        "data_quality_advice": advice,
        "source_health": h,
    }


def _v294_write_bridge_with_quality(row: dict[str, Any]) -> tuple[bool, str]:
    ok, msg = _write_macro_bridge(row)
    if not ok:
        return ok, msg
    try:
        p = Path(BRIDGE_FILE)
        data = json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
        if not isinstance(data, dict):
            data = {}
        data.update(_v294_bridge_quality(row))
        data["source_status_table"] = _v294_source_status_df().to_dict(orient="records")
        data["version"] = "v29.5_macro_bridge_quality"
        data["updated_at"] = _tw_now().strftime("%Y-%m-%d %H:%M:%S")
        p.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        return True, f"已寫入 {BRIDGE_FILE}，並加入資料源健康度與可信度。"
    except Exception as e:
        return False, f"橋接檔品質資訊寫入失敗：{e}"


def _v294_start_all_background(target_date: date):
    try:
        _reset_bg_jobs()
    except Exception:
        pass
    _maybe_start_background_update(target_date, enabled=True)
    try:
        _start_taifex_background_update_v285(target_date, force=True)
    except Exception:
        try:
            _start_taifex_background_update(target_date, force=True)
        except Exception:
            pass


# ===== v30.0 01大盤趨勢未完成項目補完：櫃買 + market_snapshot + 完整橋接 =====
OTC_CACHE_FILE = "macro_otc_cache.json"
MARKET_SNAPSHOT_FILE = "market_snapshot.json"


def _v30_read_json_dict(path_text: str) -> dict[str, Any]:
    try:
        p = Path(path_text)
        if not p.exists():
            return {}
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _v30_write_json_dict(path_text: str, data: dict[str, Any]) -> bool:
    try:
        p = Path(path_text)
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        tmp.replace(p)
        return True
    except Exception:
        return False


def _read_otc_cache() -> dict[str, Any]:
    return _v30_read_json_dict(OTC_CACHE_FILE)


def _write_otc_cache(cache: dict[str, Any]) -> None:
    _v30_write_json_dict(OTC_CACHE_FILE, cache)


def _save_otc_row(row: dict[str, Any]) -> None:
    if not isinstance(row, dict) or not row.get("ok"):
        return
    ymd = _safe_str(row.get("used_date") or row.get("date")) or _tw_now().strftime("%Y-%m-%d")
    key = pd.to_datetime(ymd, errors="coerce")
    key = key.strftime("%Y%m%d") if pd.notna(key) else _tw_now().strftime("%Y%m%d")
    cache = _read_otc_cache()
    cache[key] = row
    _write_otc_cache(cache)


def _default_otc_row(target_date: date) -> dict[str, Any]:
    cache = _read_otc_cache()
    ymd = pd.to_datetime(target_date).strftime("%Y%m%d")
    if isinstance(cache.get(ymd), dict) and cache[ymd].get("close") is not None:
        return cache[ymd]
    keys = sorted([k for k, v in cache.items() if isinstance(v, dict) and v.get("close") is not None], reverse=True)
    if keys:
        row = dict(cache[keys[0]])
        row["is_cache_fallback"] = True
        row["source"] = (_safe_str(row.get("source")) or "櫃買快取") + "｜最近快取"
        return row
    return {"ok": False, "source": "櫃買快取", "error": "尚無櫃買資料"}


def _fetch_otc_with_fallback(target_date: date, timeout: float = 2.2) -> dict[str, Any]:
    """
    v30：櫃買指數自動取得。
    優先使用 Yahoo ^TWOII 備援資料；失敗時保留最近快取，不偽裝今天、不偽裝 0。
    """
    tried = []
    try:
        y = _fetch_yahoo_chart("^TWOII", target_date, timeout=timeout)
        tried.append(f"Yahoo ^TWOII:{y.get('error') if isinstance(y, dict) else 'unknown'}")
        if isinstance(y, dict) and y.get("ok") and y.get("close") is not None:
            close_val = _safe_float(y.get("close"))
            pct_val = _safe_float(y.get("pct"))
            change_points = None
            if close_val is not None and pct_val is not None and abs(100 + pct_val) > 1e-9:
                change_points = close_val * pct_val / (100 + pct_val)
            row = {
                "ok": True,
                "source": "Yahoo ^TWOII 櫃買指數備援",
                "source_grade": "備援",
                "date": _safe_str(y.get("date")) or pd.to_datetime(target_date).strftime("%Y-%m-%d"),
                "used_date": _safe_str(y.get("date")) or pd.to_datetime(target_date).strftime("%Y-%m-%d"),
                "close": close_val,
                "pct": pct_val,
                "change_points": change_points,
                "is_realtime": False,
                "updated_at": _tw_now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            _v29_write_source_status("櫃買", "success", row.get("source"), "備援", "櫃買指數取得成功")
            return row
    except Exception as e:
        tried.append(f"Yahoo ^TWOII例外:{e}")

    cache = _default_otc_row(target_date)
    if isinstance(cache, dict) and cache.get("close") is not None:
        cache["ok"] = True
        cache["source_grade"] = "快取"
        _v29_write_source_status("櫃買", "cache", cache.get("source"), "快取", "使用最近可用櫃買快取")
        return cache

    _v29_write_source_status("櫃買", "failed", "Yahoo ^TWOII / 快取", "失敗", " / ".join(tried[-5:]))
    return {
        "ok": False,
        "source": "櫃買多來源備援",
        "source_grade": "失敗",
        "date": pd.to_datetime(target_date).strftime("%Y-%m-%d"),
        "error": "櫃買資料取得失敗：" + " / ".join(tried[-5:]),
    }


def _background_otc_worker(target_date_text: str) -> None:
    try:
        d = pd.to_datetime(target_date_text, errors="coerce")
        if pd.isna(d):
            d = pd.Timestamp(_tw_now().date())
        row = _fetch_otc_with_fallback(d.date(), timeout=2.2)
        if row.get("ok"):
            _save_otc_row(row)
            _set_job_status("otc_auto_bg", "finished", f"櫃買更新成功：{row.get('source')} / {row.get('close')} / {row.get('change_points')}")
        else:
            _set_job_status("otc_auto_bg", "error", row.get("error", "櫃買更新失敗"))
    except Exception as e:
        _set_job_status("otc_auto_bg", "error", f"櫃買背景更新例外：{e}")


def _start_otc_background_update(target_date: date, force: bool = False) -> None:
    if not force and _job_is_recent("otc_auto_bg", MACRO_AUTO_REFRESH_SECONDS):
        return
    _set_job_status("otc_auto_bg", "running", "櫃買背景更新中")
    threading.Thread(
        target=_background_otc_worker,
        args=(pd.to_datetime(target_date).strftime("%Y-%m-%d"),),
        daemon=True,
    ).start()


def _otc_cache_to_df() -> pd.DataFrame:
    cache = _read_otc_cache()
    rows = []
    for k, v in sorted(cache.items(), reverse=True):
        if isinstance(v, dict):
            rows.append({
                "日期": v.get("used_date") or v.get("date") or k,
                "櫃買指數": v.get("close"),
                "櫃買漲跌點數": v.get("change_points"),
                "櫃買漲跌幅%": v.get("pct"),
                "來源": v.get("source"),
                "更新時間": v.get("updated_at"),
            })
    return pd.DataFrame(rows)


def _render_otc_block(target_date: date):
    st.markdown("### 櫃買指數自動背景更新")
    row = _default_otc_row(target_date)
    c1, c2, c3, c4, c5 = st.columns([1.3, 1.1, 1.1, 1.2, 2.1])
    close_val = _safe_float(row.get("close"))
    chg = _safe_float(row.get("change_points"))
    pct = _safe_float(row.get("pct"))
    with c1:
        if st.button("背景更新櫃買", use_container_width=True):
            _start_otc_background_update(target_date, force=True)
            st.success("已啟動櫃買背景更新，頁面不會等待。")
    with c2:
        st.metric("櫃買指數", f"{close_val:,.2f}" if close_val is not None else "尚未更新")
    with c3:
        st.metric("櫃買漲跌", (f"{chg:+.2f} 點" if chg is not None else "—"))
    with c4:
        st.metric("櫃買漲跌幅", (f"{pct:+.2f}%" if pct is not None else "—"))
    with c5:
        st.caption(f"來源：{_safe_str(row.get('source')) or '尚未取得'}｜日期：{_safe_str(row.get('used_date') or row.get('date')) or '—'}")

    jobs = _read_bg_jobs()
    item = jobs.get("otc_auto_bg", {}) if isinstance(jobs, dict) else {}
    if item:
        st.caption(f"櫃買背景狀態：{item.get('status')}｜{item.get('message')}｜{item.get('updated_at')}")

    with st.expander("櫃買快取明細", expanded=False):
        df = _otc_cache_to_df()
        st.dataframe(df, use_container_width=True, hide_index=True)
        if df is not None and not df.empty:
            st.download_button(
                "下載櫃買快取CSV",
                data=df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"),
                file_name="macro_otc_cache.csv",
                mime="text/csv",
                use_container_width=True,
            )


def _v30_market_trend_from_score(score: float) -> tuple[str, str, str]:
    score = _safe_float(score, 50) or 50
    if score >= 75:
        return "偏多", "低", "大盤與風險情緒偏多，可正常尋找強勢突破與剛起漲股。"
    if score >= 60:
        return "中性偏多", "中低", "可找轉強股，但需避免高位爆量追價。"
    if score >= 45:
        return "震盪", "中", "盤勢震盪，股神推薦應偏重低位階剛起漲、量價轉強股。"
    if score >= 30:
        return "中性偏空", "中高", "盤勢逆風，降低追價分數並提高風險提示。"
    return "偏空", "高", "市場風險偏高，建議保守控倉，避免追高與弱勢股。"


def _build_market_snapshot_v30(row: dict[str, Any]) -> dict[str, Any]:
    bridge_date = pd.to_datetime(row.get("date") or row.get("used_date") or date.today(), errors="coerce")
    bridge_date = bridge_date.date() if pd.notna(bridge_date) else date.today()
    otc = _default_otc_row(bridge_date)
    tx = _default_taifex_row(bridge_date)
    factors = _calc_stable_market_factors(row)
    inst = _default_inst_row(bridge_date)
    inst_score = _institutional_score(inst)
    us_row = _default_us_market_row(bridge_date)
    us_score = _us_market_score(us_row)
    tx_score = _taifex_score(tx)

    base_score = _safe_float(factors.get("大盤穩定分"), 50) or 50
    otc_pct = _safe_float(otc.get("pct"))
    tx_score_val = _safe_float(tx_score.get("期貨分數"), 50) or 50
    inst_score_val = _safe_float(inst_score.get("法人分數"), 50) or 50
    us_score_val = _safe_float(us_score.get("外盤分數"), 50) or 50
    otc_score = 50 if otc_pct is None else max(0, min(100, 50 + otc_pct * 20))

    market_score = round(
        base_score * 0.45
        + otc_score * 0.18
        + tx_score_val * 0.17
        + inst_score_val * 0.10
        + us_score_val * 0.10,
        1,
    )
    trend, risk, comment = _v30_market_trend_from_score(market_score)

    data_status = {
        "twse_ok": bool(row.get("ok") or row.get("close") is not None),
        "otc_ok": bool(otc.get("ok") or otc.get("close") is not None),
        "taifex_ok": bool(tx.get("ok") or tx.get("tx_close") is not None),
        "institutional_ok": bool(inst.get("ok")),
        "us_market_ok": bool(us_row),
    }
    ok_count = sum(1 for v in data_status.values() if v)
    data_quality = "良好" if ok_count >= 4 else "部分成功" if ok_count >= 2 else "不足" if ok_count == 1 else "失敗"

    snapshot = {
        "version": "v30.0_macro_trend_complete",
        "updated_at": _tw_now().strftime("%Y-%m-%d %H:%M:%S"),
        "data_quality": data_quality,
        "data_status": data_status,
        "market_score": market_score,
        "market_trend": trend,
        "market_risk_level": risk,
        "market_bias": comment,
        "trend_comment": comment,
        "twse_index": _safe_float(row.get("close")),
        "twse_change": _calc_market_change_points(row),
        "twse_change_pct": _safe_float(row.get("pct")),
        "twse_data_date": _safe_str(row.get("used_date") or row.get("date")),
        "twse_source": _safe_str(row.get("source")),
        "otc_index": _safe_float(otc.get("close")),
        "otc_change": _safe_float(otc.get("change_points")),
        "otc_change_pct": _safe_float(otc.get("pct")),
        "otc_data_date": _safe_str(otc.get("used_date") or otc.get("date")),
        "otc_source": _safe_str(otc.get("source")),
        "futures_index": _safe_float(tx.get("tx_close")),
        "futures_change": _safe_float(tx.get("tx_change")),
        "futures_change_pct": _safe_float(tx.get("tx_pct")),
        "futures_data_date": _safe_str(tx.get("date")),
        "futures_source": _safe_str(tx.get("source")),
        "taifex_score": tx_score_val,
        "taifex_state": _safe_str(tx_score.get("期貨狀態")),
        "institutional_score": inst_score_val,
        "us_market_score": us_score_val,
        "recommendation_bias": _macro_bias_from_score(market_score),
        "source_status_table": _v294_source_status_df().to_dict(orient="records") if "_v294_source_status_df" in globals() else [],
    }
    return snapshot


def _write_market_snapshot_v30(row: dict[str, Any]) -> tuple[bool, str]:
    snapshot = _build_market_snapshot_v30(row)
    ok1 = _v30_write_json_dict(MARKET_SNAPSHOT_FILE, snapshot)
    # 同步更新舊橋接檔，避免 7_股神推薦.py 還讀 macro_mode_bridge.json 時串接不到。
    bridge = _build_macro_bridge_payload(row)
    bridge.update(snapshot)
    ok2 = _v30_write_json_dict(BRIDGE_FILE, bridge)
    if ok1 and ok2:
        return True, f"已寫入 {MARKET_SNAPSHOT_FILE} 與 {BRIDGE_FILE}，股神推薦可讀取 market_score。"
    if ok1:
        return True, f"已寫入 {MARKET_SNAPSHOT_FILE}，但 {BRIDGE_FILE} 更新失敗。"
    return False, f"寫入 {MARKET_SNAPSHOT_FILE} 失敗。"


# 覆寫舊橋接函式：保留原欄位，同時補齊 market_snapshot.json 所需欄位。
def _write_macro_bridge(row: dict[str, Any]) -> tuple[bool, str]:
    return _write_market_snapshot_v30(row)


def _render_market_snapshot_block(row: dict[str, Any]):
    st.markdown("### 股神推薦 market_snapshot 串接")
    snapshot = _build_market_snapshot_v30(row)
    c1, c2, c3, c4 = st.columns([1.1, 1.1, 1.1, 2.6])
    with c1:
        st.metric("Market Score", f"{_safe_float(snapshot.get('market_score'), 50):.1f}")
    with c2:
        st.metric("市場趨勢", snapshot.get("market_trend", "—"))
    with c3:
        st.metric("風險等級", snapshot.get("market_risk_level", "—"))
    with c4:
        st.caption(snapshot.get("trend_comment", ""))

    if st.button("立即寫入 market_snapshot.json", use_container_width=True, type="primary"):
        ok, msg = _write_market_snapshot_v30(row)
        if ok:
            st.success(msg)
        else:
            st.warning(msg)

    # 每次頁面載入都寫一次最新可用快照；失敗不阻塞畫面。
    try:
        _write_market_snapshot_v30(row)
    except Exception:
        pass

    with st.expander("market_snapshot.json 預覽", expanded=False):
        st.json(snapshot)


# 覆寫一鍵背景更新：補入櫃買背景更新。
def _v294_start_all_background(target_date: date):
    try:
        _reset_bg_jobs()
    except Exception:
        pass
    _maybe_start_background_update(target_date, enabled=True)
    try:
        _start_taifex_background_update_v285(target_date, force=True)
    except Exception:
        try:
            _start_taifex_background_update(target_date, force=True)
        except Exception:
            pass
    try:
        _start_otc_background_update(target_date, force=True)
    except Exception:
        pass







# ===== v32.0 股神推薦風控輸出完成版：資料鮮度 / 風控閘門 / 紀錄檔 / 串接按鈕同步 =====
MARKET_TREND_RECORDS_FILE = "macro_trend_records.json"


def _v32_parse_any_date(v: Any):
    try:
        if v is None:
            return None
        s = _safe_str(v)
        if not s:
            return None
        # 支援 20260429 / 2026-04-29 / 2026/04/29
        if s.isdigit() and len(s) == 8:
            return datetime.strptime(s, "%Y%m%d").date()
        dt = pd.to_datetime(s, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.date()
    except Exception:
        return None


def _v32_age_days(v: Any) -> int | None:
    d = _v32_parse_any_date(v)
    if d is None:
        return None
    return max(0, (_tw_now().date() - d).days)


def _v32_freshness_label(v: Any) -> str:
    age = _v32_age_days(v)
    if age is None:
        return "無日期"
    if age <= 1:
        return "新鮮"
    if age <= 3:
        return "可用"
    if age <= 7:
        return "偏舊"
    return "過舊"


def _v32_calc_volume_status(row: dict[str, Any], otc: dict[str, Any], tx: dict[str, Any], mtx: dict[str, Any]) -> str:
    vals = []
    for obj, keys in [
        (row, ["volume", "total_volume", "成交量", "market_volume"]),
        (otc, ["volume", "total_volume", "成交量"]),
        (tx, ["tx_volume", "volume", "成交量"]),
        (mtx, ["mtx_volume", "volume", "成交量"]),
    ]:
        if not isinstance(obj, dict):
            continue
        for k in keys:
            try:
                v = _safe_float(obj.get(k))
                if v is not None and v > 0:
                    vals.append(v)
                    break
            except Exception:
                pass
    if not vals:
        return "量能資料不足"
    if len(vals) >= 2:
        return "量能資料可用"
    return "量能部分可用"


def _v32_score_freshness_adjust(snapshot: dict[str, Any]) -> tuple[float, list[str]]:
    score = _safe_float(snapshot.get("market_score"), 50) or 50
    notes: list[str] = []

    date_fields = [
        ("加權", snapshot.get("twse_data_date")),
        ("櫃買", snapshot.get("otc_data_date")),
        ("台指期", snapshot.get("futures_data_date")),
        ("小台期", snapshot.get("mini_futures_data_date")),
    ]
    stale_count = 0
    missing_count = 0
    for name, d in date_fields:
        age = _v32_age_days(d)
        if age is None:
            missing_count += 1
            notes.append(f"{name}缺日期")
        elif age > 7:
            stale_count += 1
            notes.append(f"{name}資料過舊{age}天")
        elif age > 3:
            stale_count += 1
            notes.append(f"{name}資料偏舊{age}天")

    if stale_count >= 2:
        score -= 12
    elif stale_count == 1:
        score -= 5

    if missing_count >= 2:
        score -= 8
    elif missing_count == 1:
        score -= 3

    # 期貨明顯轉弱時直接壓低風險分數，避免推薦頁追高。
    fut_pct = _safe_float(snapshot.get("futures_change_pct"))
    if fut_pct is not None and fut_pct <= -1.0:
        score -= 8
        notes.append("台指期跌幅超過1%，推薦需保守")

    otc_pct = _safe_float(snapshot.get("otc_change_pct"))
    twse_pct = _safe_float(snapshot.get("twse_change_pct"))
    if otc_pct is not None and twse_pct is not None:
        spread = otc_pct - twse_pct
        if spread >= 0.5:
            notes.append("櫃買強於加權，中小型股相對有利")
        elif spread <= -0.5:
            score -= 4
            notes.append("櫃買弱於加權，中小型股篩選需提高門檻")

    return round(max(0, min(100, score)), 1), notes


def _v32_risk_gate_from_score(score: float, source_health: dict[str, Any] | None = None) -> str:
    success = 0
    failed = 0
    if isinstance(source_health, dict):
        success = int(_safe_float(source_health.get("success"), 0) or 0)
        failed = int(_safe_float(source_health.get("failed"), 0) or 0)

    if failed >= 3 or success <= 1:
        return "data_guard"
    if score >= 65:
        return "normal"
    if score >= 45:
        return "selective"
    return "conservative"


def _v32_position_hint(gate: str, score: float) -> str:
    if gate == "normal":
        return "可正常篩選；仍需避開過熱追高。"
    if gate == "selective":
        return "只挑低位階剛起漲、量價轉強股。"
    if gate == "data_guard":
        return "資料不足，推薦頁應降低大盤因子權重，避免誤判。"
    return "保守控倉，降低追價與高風險股票分數。"


def _v32_read_records() -> list[dict[str, Any]]:
    try:
        p = Path(MARKET_TREND_RECORDS_FILE)
        if not p.exists():
            return []
        data = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        if isinstance(data, dict) and isinstance(data.get("records"), list):
            return [x for x in data.get("records") if isinstance(x, dict)]
    except Exception:
        return []
    return []


def _v32_append_record(snapshot: dict[str, Any]) -> None:
    try:
        records = _v32_read_records()
        item = {
            "updated_at": snapshot.get("updated_at") or _tw_now().strftime("%Y-%m-%d %H:%M:%S"),
            "market_score": snapshot.get("market_score"),
            "market_trend": snapshot.get("market_trend"),
            "market_risk_level": snapshot.get("market_risk_level"),
            "risk_gate": snapshot.get("risk_gate"),
            "twse_index": snapshot.get("twse_index"),
            "twse_change": snapshot.get("twse_change"),
            "twse_change_pct": snapshot.get("twse_change_pct"),
            "otc_index": snapshot.get("otc_index"),
            "otc_change": snapshot.get("otc_change"),
            "otc_change_pct": snapshot.get("otc_change_pct"),
            "futures_index": snapshot.get("futures_index"),
            "futures_change": snapshot.get("futures_change"),
            "futures_change_pct": snapshot.get("futures_change_pct"),
            "data_quality": snapshot.get("data_quality"),
            "version": snapshot.get("version"),
        }
        # 防止同一分鐘重複洗版
        key = item.get("updated_at", "")[:16]
        records = [r for r in records if _safe_str(r.get("updated_at"))[:16] != key]
        records.insert(0, item)
        records = records[:300]
        Path(MARKET_TREND_RECORDS_FILE).write_text(json.dumps(records, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    except Exception:
        pass


_v30_build_market_snapshot_original = _build_market_snapshot_v30

def _build_market_snapshot_v30(row: dict[str, Any]) -> dict[str, Any]:
    """v32：保留 v30 原本欄位，再補齊推薦頁風控必讀欄位。"""
    snapshot = _v30_build_market_snapshot_original(row)
    try:
        bridge_date = pd.to_datetime(row.get("date") or row.get("used_date") or date.today(), errors="coerce")
        bridge_date = bridge_date.date() if pd.notna(bridge_date) else date.today()

        otc = _default_otc_row(bridge_date)
        tx = _default_taifex_row(bridge_date)
        mtx = _default_taifex_row(bridge_date)

        snapshot["volume_status"] = _v32_calc_volume_status(row, otc, tx, mtx)
        source_health = _v294_count_source_health() if "_v294_count_source_health" in globals() else {}
        snapshot["data_source_health"] = source_health

        adjusted_score, guard_notes = _v32_score_freshness_adjust(snapshot)
        snapshot["market_score_raw"] = snapshot.get("market_score")
        snapshot["market_score"] = adjusted_score
        trend, risk, comment = _v30_market_trend_from_score(adjusted_score)
        snapshot["market_trend"] = trend
        snapshot["market_risk_level"] = risk
        snapshot["market_bias"] = _macro_bias_from_score(adjusted_score) if "_macro_bias_from_score" in globals() else comment
        snapshot["recommendation_adjustment"] = snapshot["market_bias"]
        snapshot["risk_gate"] = _v32_risk_gate_from_score(adjusted_score, source_health)
        snapshot["position_hint"] = _v32_position_hint(snapshot["risk_gate"], adjusted_score)
        snapshot["market_reference_level"] = "可納入推薦" if snapshot["risk_gate"] in ["normal", "selective"] else "僅作保守參考"
        snapshot["data_guard_notes"] = guard_notes
        snapshot["trend_comment"] = (snapshot.get("trend_comment") or comment) + ("｜" + "；".join(guard_notes) if guard_notes else "")

        snapshot["freshness"] = {
            "twse": _v32_freshness_label(snapshot.get("twse_data_date")),
            "otc": _v32_freshness_label(snapshot.get("otc_data_date")),
            "futures": _v32_freshness_label(snapshot.get("futures_data_date")),
            "mini_futures": _v32_freshness_label(snapshot.get("mini_futures_data_date")),
        }

        snapshot["required_by_godpick"] = {
            "market_score": snapshot.get("market_score"),
            "market_score_raw": snapshot.get("market_score_raw"),
            "market_trend": snapshot.get("market_trend"),
            "market_risk_level": snapshot.get("market_risk_level"),
            "market_bias": snapshot.get("market_bias"),
            "risk_gate": snapshot.get("risk_gate"),
            "position_hint": snapshot.get("position_hint"),
            "twse_change": snapshot.get("twse_change"),
            "twse_change_pct": snapshot.get("twse_change_pct"),
            "otc_change": snapshot.get("otc_change"),
            "otc_change_pct": snapshot.get("otc_change_pct"),
            "futures_change": snapshot.get("futures_change"),
            "futures_change_pct": snapshot.get("futures_change_pct"),
            "volume_status": snapshot.get("volume_status"),
            "trend_comment": snapshot.get("trend_comment"),
            "data_quality": snapshot.get("data_quality"),
            "freshness": snapshot.get("freshness"),
        }
        snapshot["version"] = "v32.0_macro_trend_godpick_risk_gate"
    except Exception as e:
        snapshot["v32_warning"] = str(e)
    return snapshot


def _write_market_snapshot_v30(row: dict[str, Any]) -> tuple[bool, str]:
    """v32：統一寫 market_snapshot、macro_mode_bridge、macro_trend_records。"""
    snapshot = _build_market_snapshot_v30(row)
    ok1 = _v30_write_json_dict(MARKET_SNAPSHOT_FILE, snapshot)

    bridge = {
        "updated_at": snapshot.get("updated_at") or _tw_now().strftime("%Y-%m-%d %H:%M:%S"),
        "version": snapshot.get("version"),
        "market_score": snapshot.get("market_score"),
        "market_score_raw": snapshot.get("market_score_raw"),
        "market_trend": snapshot.get("market_trend"),
        "market_risk_level": snapshot.get("market_risk_level"),
        "market_bias": snapshot.get("market_bias"),
        "risk_gate": snapshot.get("risk_gate"),
        "position_hint": snapshot.get("position_hint"),
        "recommendation_adjustment": snapshot.get("recommendation_adjustment"),
        "volume_status": snapshot.get("volume_status"),
        "trend_comment": snapshot.get("trend_comment"),
        "required_by_godpick": snapshot.get("required_by_godpick"),
        "data_source_health": snapshot.get("data_source_health"),
        "data_quality": snapshot.get("data_quality"),
        "freshness": snapshot.get("freshness"),
        "twse_index": snapshot.get("twse_index"),
        "twse_change": snapshot.get("twse_change"),
        "twse_change_pct": snapshot.get("twse_change_pct"),
        "otc_index": snapshot.get("otc_index"),
        "otc_change": snapshot.get("otc_change"),
        "otc_change_pct": snapshot.get("otc_change_pct"),
        "futures_index": snapshot.get("futures_index"),
        "futures_change": snapshot.get("futures_change"),
        "futures_change_pct": snapshot.get("futures_change_pct"),
        "mini_futures_index": snapshot.get("mini_futures_index"),
        "mini_futures_change": snapshot.get("mini_futures_change"),
        "mini_futures_change_pct": snapshot.get("mini_futures_change_pct"),
    }
    ok2 = _v30_write_json_dict(BRIDGE_FILE, bridge)
    if ok1:
        _v32_append_record(snapshot)
    if ok1 and ok2:
        return True, f"已同步寫入 {MARKET_SNAPSHOT_FILE}、{BRIDGE_FILE}、{MARKET_TREND_RECORDS_FILE}。"
    if ok1:
        return True, f"已寫入 {MARKET_SNAPSHOT_FILE}，但 {BRIDGE_FILE} 更新失敗。"
    return False, f"{MARKET_SNAPSHOT_FILE} 寫入失敗。"


# 讓舊按鈕「立即寫入股神橋接」也同步寫 market_snapshot，不再只寫舊橋接。
def _write_macro_bridge(row: dict[str, Any]) -> tuple[bool, str]:
    return _write_market_snapshot_v30(row)


def _v294_write_bridge_with_quality(row: dict[str, Any]) -> tuple[bool, str]:
    return _write_market_snapshot_v30(row)


def _v32_render_godpick_bridge_summary(row: dict[str, Any]):
    st.markdown("### 7_股神推薦串接摘要｜v32風控閘門")
    s = _build_market_snapshot_v30(row)
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.metric("market_score", f"{_safe_float(s.get('market_score'), 50):.1f}")
    with c2:
        st.metric("market_trend", _safe_str(s.get("market_trend")) or "—")
    with c3:
        st.metric("risk_gate", _safe_str(s.get("risk_gate")) or "—")
    with c4:
        st.metric("volume_status", _safe_str(s.get("volume_status")) or "—")
    with c5:
        st.metric("data_quality", _safe_str(s.get("data_quality")) or "—")

    st.info(_safe_str(s.get("position_hint")) or "已產生大盤風控輸出。")

    with st.expander("推薦頁必讀欄位 required_by_godpick", expanded=False):
        st.json(s.get("required_by_godpick") or {})

    with st.expander("資料鮮度 / 風控註記", expanded=False):
        st.json({
            "freshness": s.get("freshness"),
            "data_guard_notes": s.get("data_guard_notes"),
            "data_source_health": s.get("data_source_health"),
        })

    cc1, cc2 = st.columns([1.2, 4])
    with cc1:
        if st.button("強制重寫股神串接檔", use_container_width=True, type="primary", key=_k("force_write_v32_bridge")):
            ok, msg = _write_market_snapshot_v30(row)
            if ok:
                st.success(msg)
            else:
                st.warning(msg)
    with cc2:
        st.caption("v32：此按鈕會同步寫 market_snapshot.json、macro_mode_bridge.json、macro_trend_records.json。")


def _v32_render_records_block():
    st.markdown("### 大盤風控紀錄")
    records = _v32_read_records()
    if not records:
        st.caption("尚無 macro_trend_records.json 紀錄；寫入 market_snapshot 後會自動建立。")
        return
    df = pd.DataFrame(records)
    show_cols = [c for c in [
        "updated_at", "market_score", "market_trend", "market_risk_level", "risk_gate",
        "twse_index", "twse_change", "twse_change_pct", "otc_index", "otc_change", "otc_change_pct",
        "futures_index", "futures_change", "futures_change_pct", "data_quality", "version"
    ] if c in df.columns]
    st.dataframe(df[show_cols].head(80), use_container_width=True, hide_index=True)
    st.download_button(
        "下載大盤風控紀錄CSV",
        data=df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"),
        file_name="macro_trend_records.csv",
        mime="text/csv",
        use_container_width=True,
    )


# 覆寫一鍵背景更新：補入櫃買，並維持不阻塞。
def _v294_start_all_background(target_date: date):
    try:
        _reset_bg_jobs()
    except Exception:
        pass
    try:
        _maybe_start_background_update(target_date, enabled=True)
    except Exception:
        pass
    try:
        _start_taifex_background_update_v285(target_date, force=True)
    except Exception:
        try:
            _start_taifex_background_update(target_date, force=True)
        except Exception:
            pass
    try:
        _start_otc_background_update(target_date, force=True)
    except Exception:
        pass


def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    inject_pro_theme()

    render_pro_hero(
        title="01 大盤趨勢｜v32股神風控完成版",
        subtitle="加權、櫃買、期貨、外盤與法人採背景更新；輸出 market_snapshot.json / macro_mode_bridge.json 給股神推薦。",
    )

    st.info("v32.0：補齊資料鮮度、risk_gate、position_hint、required_by_godpick、macro_trend_records；仍維持背景更新不卡頁。")

    c1, c2, c3, c4, c5 = st.columns([1.25, 1.25, 1.35, 1.2, 2.1])
    with c1:
        update_realtime = st.button("更新盤中即時大盤", use_container_width=True, type="primary")
    with c2:
        update_close = st.button("更新收盤紀錄", use_container_width=True)
    with c3:
        batch_close = st.button("背景更新近20日收盤", use_container_width=True)
    with c4:
        clear_cache = st.button("清除大盤快取", use_container_width=True)
    with c5:
        target_date = st.date_input("大盤日期", value=date.today(), key=_k("target_date"))

    auto_bg = st.toggle("自動背景更新，不卡頁", value=True, key=_k("auto_bg_update"))
    _maybe_start_background_update(target_date, enabled=auto_bg)
    _render_background_update_status()

    ac1, ac2, ac3 = st.columns([1.2, 1.2, 3])
    with ac1:
        if st.button("一鍵背景更新全部", use_container_width=True, type="primary"):
            _v294_start_all_background(target_date)
            st.success("已啟動大盤 / 櫃買 / 法人 / 外盤 / 期貨背景更新。頁面不會等待，稍後重新整理即可看結果。")
    with ac2:
        if st.button("立即寫入股神橋接", use_container_width=True):
            _row_for_bridge = _default_market_row(target_date)
            ok, msg = _v294_write_bridge_with_quality(_row_for_bridge)
            if ok:
                st.success(msg)
            else:
                st.warning(msg)
    with ac3:
        st.caption("v32：橋接檔會同步寫 market_snapshot、macro_mode_bridge 與大盤風控紀錄，推薦頁可直接讀取 risk_gate。")

    _v294_render_source_health_panel()

    if clear_cache:
        _write_cache({})
        st.success("已清除 macro_market_close_cache.json")
        st.rerun()

    if update_realtime:
        with st.spinner("正在抓取大盤資料；TWSE失敗會自動改用Yahoo備援..."):
            row = _fetch_market_with_fallback(target_date, realtime=True)
        if row.get("ok"):
            _save_market_row(row)
            st.success(f"大盤更新成功：{row.get('close')}｜{row.get('source')}")
        else:
            st.warning(f"大盤更新失敗：{row.get('error')}")

    if update_close:
        with st.spinner("正在抓取收盤紀錄；TWSE失敗會自動改用Yahoo備援..."):
            row = _fetch_market_with_fallback(target_date, realtime=False)
        if row.get("ok"):
            _save_market_row(row)
            st.success(f"收盤紀錄更新成功：{row.get('close')}｜{row.get('source')}")
        else:
            st.warning(f"收盤紀錄更新失敗：{row.get('error')}")

    if batch_close:
        with st.spinner("正在背景更新近20日收盤資料；只在按下時執行，不會卡住頁面..."):
            added, msgs = _batch_fetch_close_cache(target_date, days=20)
        if added > 0:
            st.success(f"近20日收盤補抓完成，新增 {added} 筆。")
        else:
            st.warning("近20日收盤沒有新增資料，可能都已存在或交易所尚未提供。")
        with st.expander("補抓明細", expanded=False):
            for msg in msgs:
                st.write(f"- {msg}")

    row = _default_market_row(target_date)
    ctx = _score_context(row)

    close_val = _safe_float(row.get("close"))
    pct_val = _safe_float(row.get("pct"))
    change_points = _calc_market_change_points(row)
    market_delta_text = "無漲跌幅"
    if pct_val is not None and change_points is not None:
        market_delta_text = f"{change_points:+,.2f} 點｜{pct_val:+.2f}%"
    elif pct_val is not None:
        market_delta_text = f"{pct_val:+.2f}%"
    elif change_points is not None:
        market_delta_text = f"{change_points:+,.2f} 點"

    render_pro_kpi_row([
        {
            "label": "目前大盤",
            "value": f"{close_val:,.2f}" if close_val is not None else "尚未更新",
            "delta": market_delta_text,
            "delta_class": "pro-kpi-delta-up" if (change_points if change_points is not None else (pct_val or 0)) >= 0 else "pro-kpi-delta-down",
        },
        {
            "label": "資料型態",
            "value": "盤中即時" if row.get("is_realtime") else "收盤/快取",
            "delta": _safe_str(row.get("source")),
            "delta_class": "pro-kpi-delta-flat",
        },
        {
            "label": "資料日期",
            "value": _safe_str(row.get("used_date") or row.get("date")),
            "delta": _safe_str(row.get("time")),
            "delta_class": "pro-kpi-delta-flat",
        },
        {
            "label": "大盤狀態",
            "value": ctx["mood"],
            "delta": ctx["advice"],
            "delta_class": "pro-kpi-delta-flat",
        },
    ])

    _render_stable_factor_block(row)
    _render_otc_block(target_date)
    _render_institutional_block(target_date)
    _render_us_market_block(target_date)
    _render_taifex_block(target_date)
    _render_market_cache_chart()
    _render_market_snapshot_block(row)
    _v32_render_godpick_bridge_summary(row)
    _render_macro_bridge_block(row)
    _v32_render_records_block()
    _render_macro_feature_center()

    dl_cols = st.columns([1.2, 4])
    with dl_cols[0]:
        st.download_button(
            "下載大盤快取CSV",
            data=_cache_download_csv_bytes(),
            file_name="macro_market_close_cache.csv",
            mime="text/csv",
            use_container_width=True,
            disabled=_cache_to_market_df().empty,
        )
    with dl_cols[1]:
        st.caption("v32：大盤、櫃買、期貨與資料鮮度會寫入股神推薦風控檔。")

    st.markdown("### 大盤操作參考")
    ref_cols = st.columns(4)
    snapshot_now = _build_market_snapshot_v30(row)
    ref_items = [
        ("目前狀態", snapshot_now.get("market_trend") or ctx["mood"]),
        ("風控閘門", snapshot_now.get("risk_gate") or "—"),
        ("操作建議", snapshot_now.get("position_hint") or ctx["advice"]),
        ("資料來源", _safe_str(row.get("source"))),
    ]
    for _col, (_title, _value) in zip(ref_cols, ref_items):
        with _col:
            st.markdown(
                f"""
                <div style="border:1px solid #e2e8f0;border-radius:14px;padding:14px;background:#ffffff;min-height:92px;">
                    <div style="font-size:13px;color:#64748b;font-weight:800;">{_title}</div>
                    <div style="font-size:16px;color:#0f172a;font-weight:900;margin-top:8px;">{_value}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    with st.expander("大盤快取 / 除錯資料", expanded=False):
        st.json(_read_cache())
        st.write("目前列：")
        st.json(row)
        st.write("v32 snapshot：")
        st.json(snapshot_now)


if __name__ == "__main__":
    main()
