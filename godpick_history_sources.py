# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import date, timedelta
from io import StringIO
from typing import Any

import pandas as pd
import requests

_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


def _num(x: Any) -> float | None:
    try:
        if x is None:
            return None
        s = str(x).replace(",", "").replace("--", "").strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def _norm_code(code: Any) -> str:
    s = str(code or "").strip()
    s = s.split(".")[0]
    return "".join(ch for ch in s if ch.isdigit())[:6]


def normalize_history_df(df: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame()
    temp = df.copy()
    rename = {}
    for c in temp.columns:
        k = str(c).strip().lower()
        if k in {"date", "日期", "datetime", "time"}:
            rename[c] = "日期"
        elif k in {"open", "開盤", "開盤價"}:
            rename[c] = "開盤價"
        elif k in {"high", "最高", "最高價"}:
            rename[c] = "最高價"
        elif k in {"low", "最低", "最低價"}:
            rename[c] = "最低價"
        elif k in {"close", "收盤", "收盤價", "收盤價(元)"}:
            rename[c] = "收盤價"
        elif k in {"volume", "vol", "成交股數", "成交量"}:
            rename[c] = "成交量"
    if rename:
        temp = temp.rename(columns=rename)
    if "日期" not in temp.columns or "收盤價" not in temp.columns:
        return pd.DataFrame()
    temp["日期"] = pd.to_datetime(temp["日期"], errors="coerce")
    for c in ["開盤價", "最高價", "最低價", "收盤價", "成交量"]:
        if c in temp.columns:
            temp[c] = pd.to_numeric(temp[c], errors="coerce")
    keep = [c for c in ["日期", "開盤價", "最高價", "最低價", "收盤價", "成交量"] if c in temp.columns]
    return temp[keep].dropna(subset=["日期", "收盤價"]).sort_values("日期").reset_index(drop=True)


def fetch_yahoo_chart(code: str, market: str, start: date, end: date, timeout: int = 8) -> pd.DataFrame:
    code = _norm_code(code)
    if not code:
        return pd.DataFrame()
    suffixes = ["TWO", "TW"] if str(market) in {"上櫃", "興櫃", "OTC", "TPEX"} else ["TW", "TWO"]
    p1 = int(pd.Timestamp(start).timestamp())
    p2 = int((pd.Timestamp(end) + pd.Timedelta(days=1)).timestamp())
    for suf in suffixes:
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{code}.{suf}"
            r = requests.get(url, params={"period1": p1, "period2": p2, "interval": "1d", "events": "history", "includeAdjustedClose": "true"}, headers=_HEADERS, timeout=timeout)
            if r.status_code != 200:
                continue
            js = r.json()
            result = (((js or {}).get("chart") or {}).get("result") or [])
            if not result:
                continue
            item = result[0]
            ts = item.get("timestamp") or []
            q = (((item.get("indicators") or {}).get("quote") or [{}])[0])
            rows = []
            for i, t in enumerate(ts):
                close = (q.get("close") or [None])[i] if i < len(q.get("close") or []) else None
                if close is None:
                    continue
                rows.append({
                    "日期": pd.to_datetime(int(t), unit="s").tz_localize("UTC").tz_convert("Asia/Taipei").tz_localize(None).date(),
                    "開盤價": (q.get("open") or [None])[i] if i < len(q.get("open") or []) else None,
                    "最高價": (q.get("high") or [None])[i] if i < len(q.get("high") or []) else None,
                    "最低價": (q.get("low") or [None])[i] if i < len(q.get("low") or []) else None,
                    "收盤價": close,
                    "成交量": (q.get("volume") or [None])[i] if i < len(q.get("volume") or []) else None,
                })
            df = normalize_history_df(pd.DataFrame(rows))
            if not df.empty:
                df.attrs["source"] = f"YahooChart:{code}.{suf}"
                return df
        except Exception:
            continue
    return pd.DataFrame()


def fetch_stooq_csv(code: str, market: str, start: date, end: date, timeout: int = 8) -> pd.DataFrame:
    code = _norm_code(code)
    if not code:
        return pd.DataFrame()
    # Stooq 有時可用 tw / two 後綴；失敗就回傳空表，不拖慢主流程。
    suffixes = ["two", "tw"] if str(market) in {"上櫃", "興櫃", "OTC", "TPEX"} else ["tw", "two"]
    d1 = pd.Timestamp(start).strftime("%Y%m%d")
    d2 = pd.Timestamp(end).strftime("%Y%m%d")
    for suf in suffixes:
        try:
            url = "https://stooq.com/q/d/l/"
            r = requests.get(url, params={"s": f"{code}.{suf}", "d1": d1, "d2": d2, "i": "d"}, headers=_HEADERS, timeout=timeout)
            if r.status_code != 200 or "No data" in r.text[:80] or len(r.text) < 30:
                continue
            raw = pd.read_csv(StringIO(r.text))
            if raw.empty:
                continue
            df = normalize_history_df(raw)
            if not df.empty:
                df.attrs["source"] = f"Stooq:{code}.{suf}"
                return df
        except Exception:
            continue
    return pd.DataFrame()


def _twse_one_day(code: str, d: date, timeout: int = 8) -> dict[str, Any] | None:
    # TWSE 官方日成交資訊。只作備援，避免整批 ONLINE_FAIL。
    try:
        url = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX"
        r = requests.get(url, params={"date": pd.Timestamp(d).strftime("%Y%m%d"), "type": "ALLBUT0999", "response": "json"}, headers=_HEADERS, timeout=timeout)
        if r.status_code != 200:
            return None
        js = r.json()
        tables = []
        for key in ["tables", "data9", "data"]:
            v = js.get(key)
            if isinstance(v, list):
                tables.append(v)
        for table in tables:
            for row in table:
                if not isinstance(row, list) or len(row) < 9:
                    continue
                if str(row[0]).strip() == code:
                    return {"日期": d, "成交量": _num(row[2]), "開盤價": _num(row[5]), "最高價": _num(row[6]), "最低價": _num(row[7]), "收盤價": _num(row[8])}
    except Exception:
        return None
    return None


def _tpex_one_day(code: str, d: date, timeout: int = 8) -> dict[str, Any] | None:
    # TPEx 官方日行情。網站欄位曾改版，採多表格寬鬆解析。
    roc = f"{d.year-1911}/{d.month:02d}/{d.day:02d}"
    candidates = [
        ("https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes", {"date": roc, "response": "json"}),
        ("https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php", {"l": "zh-tw", "d": roc, "_": ""}),
    ]
    for url, params in candidates:
        try:
            r = requests.get(url, params=params, headers=_HEADERS, timeout=timeout)
            if r.status_code != 200:
                continue
            js = r.json()
            pools = []
            for key in ["tables", "aaData", "data", "result"]:
                v = js.get(key) if isinstance(js, dict) else None
                if isinstance(v, list):
                    pools.append(v)
            for pool in pools:
                for item in pool:
                    rows = item.get("data", []) if isinstance(item, dict) else [item]
                    if isinstance(rows, list):
                        for row in rows:
                            if not isinstance(row, list) or not row:
                                continue
                            if str(row[0]).strip() != code:
                                continue
                            nums = [_num(x) for x in row]
                            # 常見欄位：代號 名稱 收盤 漲跌 開盤 最高 最低 成交股數...
                            close = nums[2] if len(nums) > 2 else None
                            openp = nums[4] if len(nums) > 4 else None
                            high = nums[5] if len(nums) > 5 else None
                            low = nums[6] if len(nums) > 6 else None
                            vol = nums[8] if len(nums) > 8 else None
                            if close is not None:
                                return {"日期": d, "開盤價": openp, "最高價": high, "最低價": low, "收盤價": close, "成交量": vol}
        except Exception:
            continue
    return None


def fetch_twse_tpex_daily_range(code: str, market: str, start: date, end: date, max_days: int = 120) -> pd.DataFrame:
    code = _norm_code(code)
    if not code:
        return pd.DataFrame()
    days = pd.date_range(start=start, end=end, freq="D")[-max_days:]
    fetchers = [_tpex_one_day, _twse_one_day] if str(market) in {"上櫃", "興櫃", "OTC", "TPEX"} else [_twse_one_day, _tpex_one_day]
    rows = []
    for ts in days:
        d = ts.date()
        # 週末略過，減少請求量
        if d.weekday() >= 5:
            continue
        for fn in fetchers:
            row = fn(code, d)
            if row:
                rows.append(row)
                break
    df = normalize_history_df(pd.DataFrame(rows))
    if not df.empty:
        df.attrs["source"] = "TWSE/TPExDailyFallback"
    return df


def fetch_multi_source_history(code: str, stock_name: str = "", market: str = "", start_date: date | str | None = None, end_date: date | str | None = None, timeout: int = 8) -> tuple[pd.DataFrame, str]:
    start = pd.to_datetime(start_date, errors="coerce")
    end = pd.to_datetime(end_date, errors="coerce")
    if pd.isna(start) or pd.isna(end):
        return pd.DataFrame(), "BAD_DATE"
    start_d, end_d = start.date(), end.date()
    code = _norm_code(code)
    sources = [
        ("YahooChart", lambda: fetch_yahoo_chart(code, market, start_d, end_d, timeout=timeout)),
        ("StooqCSV", lambda: fetch_stooq_csv(code, market, start_d, end_d, timeout=timeout)),
        ("TWSE_TPExDaily", lambda: fetch_twse_tpex_daily_range(code, market, start_d, end_d)),
    ]
    errors = []
    for name, fn in sources:
        try:
            df = fn()
            df = normalize_history_df(df)
            if isinstance(df, pd.DataFrame) and not df.empty:
                return df, getattr(df, "attrs", {}).get("source") or name
            errors.append(f"{name}:EMPTY")
        except Exception as e:
            errors.append(f"{name}:{str(e)[:50]}")
    return pd.DataFrame(), " | ".join(errors)[:240] or "ALL_SOURCE_EMPTY"
