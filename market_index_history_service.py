# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
import time

import requests

TAIPEI_TZ = timezone(timedelta(hours=8))
CACHE_FILE = Path(__file__).resolve().parent / "macro_market_index_history_cache.json"
VERSION = "v110_market_index_multi_source_20260803"


def _num(v: Any):
    try:
        if v is None or isinstance(v, bool):
            return None
        s = str(v).strip().replace(",", "").replace("%", "").replace("+", "")
        if not s or s in {"-", "--", "nan", "None", "null"}:
            return None
        return float(s)
    except Exception:
        return None


def _date_text(v: Any) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            return datetime.strptime(s[:10] if fmt != "%Y%m%d" else s[:8], fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    # 民國日期
    try:
        parts = s.replace(".", "/").replace("-", "/").split("/")
        if len(parts) >= 3 and int(parts[0]) < 1911:
            return f"{int(parts[0])+1911:04d}-{int(parts[1]):02d}-{int(parts[2]):02d}"
    except Exception:
        pass
    return ""


def _normalize(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for r in rows:
        if not isinstance(r, dict):
            continue
        d = _date_text(r.get("date") or r.get("日期") or r.get("Date"))
        c = _num(r.get("close") if "close" in r else r.get("收盤指數", r.get("收盤", r.get("指數"))))
        if not d or c is None or c <= 0:
            continue
        item = {
            "date": d,
            "open": _num(r.get("open") if "open" in r else r.get("開盤")),
            "high": _num(r.get("high") if "high" in r else r.get("最高")),
            "low": _num(r.get("low") if "low" in r else r.get("最低")),
            "close": c,
            "volume": _num(r.get("volume") if "volume" in r else r.get("成交量")),
        }
        out[d] = item
    return [out[k] for k in sorted(out)]


def _fetch_yahoo(symbol: str, days: int, timeout: float, deadline: float | None = None) -> tuple[list[dict[str, Any]], str]:
    now = datetime.now(timezone.utc)
    p2 = int((now + timedelta(days=1)).timestamp())
    p1 = int((now - timedelta(days=max(20, days * 2))).timestamp())
    errors = []
    for host in ("query1.finance.yahoo.com", "query2.finance.yahoo.com"):
        if deadline is not None and time.monotonic() >= deadline:
            errors.append("Yahoo 多來源已達時間上限")
            break
        url = f"https://{host}/v8/finance/chart/{requests.utils.quote(symbol, safe='^=')}"
        try:
            remaining = max(0.8, deadline - time.monotonic()) if deadline is not None else timeout
            r = requests.get(url, params={"period1": p1, "period2": p2, "interval": "1d", "includePrePost": "false", "events": "div,splits"}, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"}, timeout=min(timeout, remaining))
            if r.status_code != 200:
                errors.append(f"{host} HTTP {r.status_code}")
                continue
            result = (((r.json() or {}).get("chart") or {}).get("result") or [])
            if not result:
                errors.append(f"{host} empty result")
                continue
            item = result[0]
            ts = item.get("timestamp") or []
            q = ((item.get("indicators") or {}).get("quote") or [{}])[0]
            closes = q.get("close") or []
            rows = []
            for i, c in enumerate(closes):
                c = _num(c)
                if c is None or i >= len(ts):
                    continue
                d = datetime.fromtimestamp(ts[i], tz=timezone.utc).astimezone(TAIPEI_TZ).strftime("%Y-%m-%d")
                rows.append({"date": d, "open": (q.get("open") or [None]*len(closes))[i] if i < len(q.get("open") or []) else None, "high": (q.get("high") or [None]*len(closes))[i] if i < len(q.get("high") or []) else None, "low": (q.get("low") or [None]*len(closes))[i] if i < len(q.get("low") or []) else None, "close": c, "volume": (q.get("volume") or [None]*len(closes))[i] if i < len(q.get("volume") or []) else None})
            rows = _normalize(rows)[-days:]
            if len(rows) >= 2:
                return rows, f"Yahoo {host} {symbol}"
            errors.append(f"{host} rows={len(rows)}")
        except Exception as e:
            errors.append(f"{host} {type(e).__name__}: {e}")
    return [], " / ".join(errors[-4:])


def _walk_tables(obj: Any) -> list[list[Any]]:
    found = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in {"data", "aaData", "rows"} and isinstance(v, list):
                found.append(v)
            found.extend(_walk_tables(v))
    elif isinstance(obj, list):
        for v in obj:
            found.extend(_walk_tables(v))
    return found


def _fetch_tpex_official(target: date, timeout: float, deadline: float | None = None) -> tuple[list[dict[str, Any]], str]:
    roc = f"{target.year-1911:03d}/{target.month:02d}"
    urls = [
        f"https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingIndex?date={target:%Y/%m/%d}&response=json",
        f"https://www.tpex.org.tw/www/zh-tw/afterTrading/indexInfo?date={target:%Y/%m/%d}&response=json",
        f"https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_index/st41_result.php?l=zh-tw&d={roc}",
        f"https://www.tpex.org.tw/web/stock/aftertrading/index_summary/summary_result.php?l=zh-tw&d={roc}",
    ]
    errors = []
    for url in urls:
        if deadline is not None and time.monotonic() >= deadline:
            errors.append("TPEx 官方備援已達時間上限")
            break
        try:
            remaining = max(0.8, deadline - time.monotonic()) if deadline is not None else timeout
            r = requests.get(url, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.tpex.org.tw/", "Accept": "application/json,text/plain,*/*"}, timeout=min(timeout, remaining))
            if r.status_code != 200:
                errors.append(f"HTTP {r.status_code} {url.split('/')[-1][:40]}")
                continue
            payload = r.json()
            candidates = []
            for table in _walk_tables(payload):
                for row in table:
                    if isinstance(row, dict):
                        txt = " ".join(str(x) for x in row.values())
                        if "櫃買" in txt or "OTC" in txt or any(k in row for k in ["日期", "Date", "date"]):
                            candidates.append(row)
                    elif isinstance(row, list) and len(row) >= 2:
                        # 常見歷史表：[日期, 開盤, 最高, 最低, 收盤, 漲跌...]
                        nums = [_num(x) for x in row]
                        d = _date_text(row[0])
                        if d:
                            valid = [x for x in nums[1:] if x is not None and x > 0]
                            if valid:
                                candidates.append({"date": d, "close": valid[-1]})
            rows = _normalize(candidates)
            if len(rows) >= 2:
                return rows, "TPEx 官方櫃買指數"
            errors.append(f"official rows={len(rows)}")
        except Exception as e:
            errors.append(f"{type(e).__name__}: {e}")
    return [], " / ".join(errors[-4:])


def _read_cache() -> dict[str, Any]:
    try:
        data = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_cache(data: dict[str, Any]) -> None:
    try:
        tmp = CACHE_FILE.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(CACHE_FILE)
    except Exception:
        pass


def fetch_market_index_history(kind: str, days: int = 90, target_date: date | None = None, timeout: float = 3.0, max_seconds: float = 10.0) -> dict[str, Any]:
    kind = "otc" if str(kind).lower() in {"otc", "tpex", "櫃買"} else "twse"
    target_date = target_date or datetime.now(TAIPEI_TZ).date()
    symbols = ["^TWOII", "TWOII"] if kind == "otc" else ["^TWII"]
    diagnostics = []
    deadline = time.monotonic() + max(1.0, float(max_seconds))
    rows = []
    source = ""
    for symbol in symbols:
        rows, msg = _fetch_yahoo(symbol, days, timeout, deadline=deadline)
        diagnostics.append(msg)
        if len(rows) >= 2:
            source = msg
            break
    if kind == "otc" and len(rows) < 2:
        rows, msg = _fetch_tpex_official(target_date, timeout, deadline=deadline)
        diagnostics.append(msg)
        if len(rows) >= 2:
            source = msg
    cache = _read_cache()
    if len(rows) < 2:
        cached_rows = _normalize(((cache.get(kind) or {}).get("history") or []))
        if len(cached_rows) >= 2:
            rows = cached_rows[-days:]
            source = f"最近有效歷史快取｜{(cache.get(kind) or {}).get('source','')}"
    if len(rows) < 2:
        return {"ok": False, "kind": kind, "history": [], "rows": 0, "source": "多來源失敗", "error": "；".join(x for x in diagnostics if x)[-1000:], "version": VERSION}
    rows = rows[-days:]
    last, prev = rows[-1], rows[-2]
    change = last["close"] - prev["close"]
    pct = change / prev["close"] * 100 if prev["close"] else None
    result = {"ok": True, "kind": kind, "date": last["date"], "close": last["close"], "prev_close": prev["close"], "change": change, "change_pct": pct, "history": rows, "rows": len(rows), "source": source, "updated_at": datetime.now(TAIPEI_TZ).strftime("%Y-%m-%d %H:%M:%S"), "version": VERSION, "diagnostics": diagnostics}
    cache[kind] = result
    _write_cache(cache)
    return result
