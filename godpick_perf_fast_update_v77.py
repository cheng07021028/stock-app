# -*- coding: utf-8 -*-
from __future__ import annotations

"""
godpick_perf_fast_update_v77.py

推薦後績效「快速防卡版」共用工具。

設計目標：
1. 不要每次更新都重算全部推薦紀錄。
2. 優先更新「尚未有最新價 / 已過期 / 未完成追蹤」的資料。
3. Yahoo Quote 使用批次 + 併發，降低等待時間。
4. 失敗不卡住，不重試太多次，保留原資料。
5. 可被 pages/8_股神推薦紀錄.py、pages/10_推薦清單.py、pages/14_股神權重校正.py 呼叫。

注意：
- 這是輔助模組，不會自動覆蓋你的頁面。
- 若要整合到頁面，請在原本「更新推薦後績效」按鈕區塊呼叫 update_recommendation_perf_fast_v77()。
"""

import json
import math
import time
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Tuple

import pandas as pd
import requests


DEFAULT_TRACK_DAYS = [1, 3, 5, 10, 20]
DEFAULT_JSON_FILES = [
    "godpick_records.json",
    "godpick_recommend_list.json",
    "godpick_latest_recommendations.json",
]


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def _safe_float(v: Any, default=None):
    if v is None:
        return default
    try:
        if pd.isna(v):
            return default
    except Exception:
        pass
    try:
        s = str(v).replace(",", "").replace("%", "").strip()
        if s == "":
            return default
        return float(s)
    except Exception:
        return default


def _normalize_code(v: Any) -> str:
    text = _safe_str(v)
    if not text:
        return ""
    digits = "".join(ch for ch in text if ch.isdigit())
    if 4 <= len(digits) <= 6:
        return digits[:4]
    return text


def _tw_symbol(code: str) -> str:
    code = _normalize_code(code)
    if not code:
        return ""
    # Yahoo 台股可用 .TW / .TWO。先抓 .TW，失敗再 .TWO。
    return f"{code}.TW"


def _read_json_records(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []

    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        # 常見格式相容
        for k in ["records", "data", "items", "recommendations"]:
            if isinstance(data.get(k), list):
                return [x for x in data[k] if isinstance(x, dict)]
    return []


def _write_json_records(path: Path, records: List[Dict[str, Any]]) -> None:
    try:
        if path.exists():
            backup = path.with_suffix(path.suffix + f".bak_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
            backup.write_text(path.read_text(encoding="utf-8", errors="replace"), encoding="utf-8")
    except Exception:
        pass

    path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")


def _is_stale(row: Dict[str, Any], stale_minutes: int = 60) -> bool:
    t = _safe_str(row.get("最新更新時間") or row.get("績效更新時間") or row.get("更新時間"))
    if not t:
        return True
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            dt = datetime.strptime(t[:19], fmt)
            return datetime.now() - dt > timedelta(minutes=stale_minutes)
        except Exception:
            continue
    return True


def _needs_update(row: Dict[str, Any], stale_minutes: int = 60) -> bool:
    # 已停損/已賣出也可少更新，避免無限重算
    status = _safe_str(row.get("目前狀態"))
    if status in ["已賣出", "停損", "達標出場", "結案"]:
        return False

    if _safe_float(row.get("最新價")) is None:
        return True

    # 追蹤績效欄位缺值就更新
    for c in ["推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%"]:
        if c in row and _safe_float(row.get(c)) is None:
            return True

    return _is_stale(row, stale_minutes=stale_minutes)


def _fetch_yahoo_chart_one(code: str, timeout: float = 5.0) -> Tuple[str, Dict[str, Any]]:
    code = _normalize_code(code)
    if not code:
        return code, {"ok": False, "error": "empty code"}

    symbols = [f"{code}.TW", f"{code}.TWO"]
    last_error = ""

    for sym in symbols:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
        params = {
            "range": "1mo",
            "interval": "1d",
            "events": "history",
        }
        try:
            r = requests.get(url, params=params, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code != 200:
                last_error = f"HTTP {r.status_code}"
                continue
            data = r.json()
            result = ((data.get("chart") or {}).get("result") or [None])[0]
            if not result:
                last_error = "empty result"
                continue

            meta = result.get("meta") or {}
            quote = ((result.get("indicators") or {}).get("quote") or [{}])[0]
            timestamps = result.get("timestamp") or []
            closes = quote.get("close") or []

            pairs = []
            for ts, close in zip(timestamps, closes):
                if close is None:
                    continue
                try:
                    pairs.append((datetime.fromtimestamp(ts).strftime("%Y-%m-%d"), float(close)))
                except Exception:
                    pass

            latest = _safe_float(meta.get("regularMarketPrice"))
            if latest is None and pairs:
                latest = pairs[-1][1]

            if latest is None:
                last_error = "latest none"
                continue

            return code, {
                "ok": True,
                "symbol": sym,
                "latest": latest,
                "history": pairs,
                "source": "Yahoo",
                "fetched_at": _now_str(),
            }

        except Exception as e:
            last_error = str(e)

    return code, {"ok": False, "error": last_error or "fetch failed"}


def fetch_latest_quotes_fast_v77(codes: List[str], max_workers: int = 16, timeout: float = 5.0) -> Dict[str, Dict[str, Any]]:
    codes = sorted(set(_normalize_code(c) for c in codes if _normalize_code(c)))
    if not codes:
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    workers = max(1, min(int(max_workers or 1), 32))

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(_fetch_yahoo_chart_one, code, timeout): code for code in codes}
        for fut in as_completed(futs):
            code = futs[fut]
            try:
                c, data = fut.result()
                out[c or code] = data
            except Exception as e:
                out[code] = {"ok": False, "error": str(e)}
    return out


def _calc_return(latest: float, base_price: Any):
    base = _safe_float(base_price)
    if base is None or base == 0 or latest is None:
        return None
    return round((float(latest) - base) / base * 100, 2)


def _price_on_or_after(history: List[Tuple[str, float]], start_date: str, days: int):
    if not history or not start_date:
        return None
    try:
        sd = datetime.strptime(start_date[:10].replace("/", "-"), "%Y-%m-%d")
    except Exception:
        return None

    target = sd + timedelta(days=int(days))
    target_s = target.strftime("%Y-%m-%d")
    for d, p in history:
        if d >= target_s:
            return p
    return None


def update_record_perf(row: Dict[str, Any], quote: Dict[str, Any], track_days: List[int] | None = None) -> Dict[str, Any]:
    track_days = track_days or DEFAULT_TRACK_DAYS
    out = dict(row)

    if not quote or not quote.get("ok"):
        out["績效更新狀態"] = "ONLINE_FAIL"
        out["績效更新錯誤"] = _safe_str((quote or {}).get("error"))
        return out

    latest = _safe_float(quote.get("latest"))
    if latest is None:
        out["績效更新狀態"] = "ONLINE_FAIL"
        out["績效更新錯誤"] = "latest none"
        return out

    base_price = (
        out.get("推薦價格")
        or out.get("推薦日價格")
        or out.get("買進價")
        or out.get("最新價")
    )

    out["最新價"] = latest
    out["最新更新時間"] = quote.get("fetched_at") or _now_str()
    out["資料來源"] = out.get("資料來源") or quote.get("source", "Yahoo")
    out["績效更新狀態"] = "OK"
    out["績效更新錯誤"] = ""

    total_ret = _calc_return(latest, base_price)
    if total_ret is not None:
        out["目前損益幅%"] = total_ret
        out["損益幅%"] = total_ret

    rec_date = _safe_str(out.get("推薦日期") or out.get("建立日期") or out.get("建立時間"))
    history = quote.get("history") or []

    for d in track_days:
        col = f"推薦後{d}日%"
        px = _price_on_or_after(history, rec_date, d)
        ret = _calc_return(px, base_price) if px is not None else None
        if ret is not None:
            out[col] = ret

    return out


def update_recommendation_perf_fast_v77(
    json_files: List[str] | None = None,
    max_records: int = 80,
    batch_limit: int = 60,
    max_workers: int = 16,
    stale_minutes: int = 60,
    track_days: List[int] | None = None,
) -> Dict[str, Any]:
    """
    直接更新專案根目錄 JSON 紀錄。

    回傳摘要：
    {
      processed_files, total_records, candidates, success, fail, updated_files
    }
    """
    json_files = json_files or DEFAULT_JSON_FILES
    track_days = track_days or DEFAULT_TRACK_DAYS

    summary = {
        "processed_files": [],
        "total_records": 0,
        "candidates": 0,
        "success": 0,
        "fail": 0,
        "updated_files": [],
        "messages": [],
    }

    root = Path(".")
    all_work_items = []

    for fn in json_files:
        path = root / fn
        records = _read_json_records(path)
        if not records:
            continue

        summary["processed_files"].append(fn)
        summary["total_records"] += len(records)

        # 從最新的資料開始處理，避免一次跑太久
        indexed = list(enumerate(records))
        indexed = indexed[-int(max_records):] if max_records else indexed

        for idx, row in indexed:
            if _needs_update(row, stale_minutes=stale_minutes):
                code = _normalize_code(row.get("股票代號") or row.get("代號"))
                if code:
                    all_work_items.append((fn, idx, code))

    # 去重並限制本次抓取數量
    all_work_items = all_work_items[: int(batch_limit or 60)]
    summary["candidates"] = len(all_work_items)

    codes = [x[2] for x in all_work_items]
    quotes = fetch_latest_quotes_fast_v77(codes, max_workers=max_workers)

    # 回寫各檔
    by_file: Dict[str, List[Tuple[int, str]]] = {}
    for fn, idx, code in all_work_items:
        by_file.setdefault(fn, []).append((idx, code))

    for fn, items in by_file.items():
        path = root / fn
        records = _read_json_records(path)
        changed = False

        for idx, code in items:
            if idx >= len(records):
                continue
            q = quotes.get(code) or {}
            new_row = update_record_perf(records[idx], q, track_days=track_days)
            if new_row != records[idx]:
                records[idx] = new_row
                changed = True
            if q.get("ok"):
                summary["success"] += 1
            else:
                summary["fail"] += 1

        if changed:
            _write_json_records(path, records)
            summary["updated_files"].append(fn)

    if summary["candidates"] == 0:
        summary["messages"].append("沒有需要更新的資料；可提高 max_records 或縮短 stale_minutes。")

    return summary


if __name__ == "__main__":
    s = update_recommendation_perf_fast_v77()
    print(json.dumps(s, ensure_ascii=False, indent=2))
