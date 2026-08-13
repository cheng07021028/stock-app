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
from datetime import datetime, timedelta, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Tuple

import pandas as pd
import requests

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None


PERF_FAST_UPDATE_VERSION = "v191_h9_execution_truth_20260813"
DEFAULT_TRACK_DAYS = [1, 3, 5, 10, 20]
# H9 keeps the published retest-zone semantics but separates the theoretical
# support reference from the OHLC-verifiable execution price.
ENTRY_TOLERANCE_PCT = 1.5
DEFAULT_JSON_FILES = [
    "godpick_records.json",
    "godpick_calibration_samples.json",
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


def _read_json_container(path: Path) -> tuple[Any, str | None, List[Dict[str, Any]]]:
    """讀取 JSON 並保留原始容器格式。

    godpick_latest_recommendations.json 是 dict + recommendations；舊版只取 list
    並在回寫時把整個 dict 覆蓋成 list，會遺失 saved_at、weights、
    category_strength、hot_pick。V171 之後一律保存原容器與非紀錄欄位。
    """
    if not path.exists():
        return None, None, []
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return None, None, []
    if isinstance(data, list):
        return data, None, [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        for key in ["records", "data", "items", "recommendations"]:
            if isinstance(data.get(key), list):
                return data, key, [x for x in data[key] if isinstance(x, dict)]
    return data, None, []


def _read_json_records(path: Path) -> List[Dict[str, Any]]:
    return _read_json_container(path)[2]


def _cleanup_old_backups(path: Path, keep: int = 5) -> None:
    try:
        backups = sorted(
            path.parent.glob(path.name + ".bak_*"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        for old in backups[max(1, int(keep)):]:
            try:
                old.unlink()
            except Exception:
                pass
    except Exception:
        pass


def _write_json_records(path: Path, records: List[Dict[str, Any]]) -> None:
    """原子回寫並保留 dict wrapper；備份只保留最近 5 份。"""
    original, record_key, _ = _read_json_container(path)
    if isinstance(original, dict) and record_key:
        payload: Any = dict(original)
        payload[record_key] = records
        payload["performance_updated_at"] = _now_str()
        payload["performance_update_version"] = PERF_FAST_UPDATE_VERSION
    else:
        payload = records

    try:
        if path.exists():
            backup = path.with_name(path.name + f".bak_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
            backup.write_text(path.read_text(encoding="utf-8-sig", errors="replace"), encoding="utf-8")
            _cleanup_old_backups(path, keep=5)
    except Exception:
        pass

    tmp = path.with_name(path.name + ".tmp_v171")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


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

    # V103：舊紀錄沒有觸發/還原績效欄位時必須回補，否則模型仍會把
    # 未觸發候選的收盤漲跌誤當成可交易績效。
    if not _safe_str(row.get("進場觸發狀態")) or not _safe_str(row.get("績效計算口徑")):
        return True
    # V104：回補隔日執行檢討欄位，避免只看到候選漲跌、看不到是否真正觸發。
    if not _safe_str(row.get("隔日執行命中結果")):
        return True

    # 追蹤績效欄位缺值就更新
    for c in ["推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%"]:
        if c in row and _safe_float(row.get(c)) is None:
            return True

    return _is_stale(row, stale_minutes=stale_minutes)


def _fetch_yahoo_chart_one(code: str, timeout: float = 5.0) -> Tuple[str, Dict[str, Any]]:
    """抓取可回放的 OHLC 與還原收盤價。

    以前只保存 close，無法判斷「盤中是否觸價、收盤是否守價」，也會把
    除權息造成的價格跳空誤算成選股虧損。新版保留 OHLC/adjusted close，
    讓候選績效與真正可執行交易績效分開計算。
    """
    code = _normalize_code(code)
    if not code:
        return code, {"ok": False, "error": "empty code"}

    symbols = [f"{code}.TW", f"{code}.TWO"]
    last_error = ""
    taipei_tz = ZoneInfo("Asia/Taipei") if ZoneInfo is not None else None

    for sym in symbols:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
        params = {
            "range": "6mo",
            "interval": "1d",
            "events": "div,splits,history",
            "includeAdjustedClose": "true",
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
            indicators = result.get("indicators") or {}
            quote = (indicators.get("quote") or [{}])[0]
            adj_block = (indicators.get("adjclose") or [{}])[0]
            timestamps = result.get("timestamp") or []
            opens = quote.get("open") or []
            highs = quote.get("high") or []
            lows = quote.get("low") or []
            closes = quote.get("close") or []
            volumes = quote.get("volume") or []
            adjcloses = adj_block.get("adjclose") or []

            history: list[dict[str, Any]] = []
            for i, ts in enumerate(timestamps):
                close = closes[i] if i < len(closes) else None
                if close is None:
                    continue
                try:
                    dt_utc = datetime.fromtimestamp(int(ts), tz=timezone.utc)
                    dt_local = dt_utc.astimezone(taipei_tz) if taipei_tz is not None else dt_utc
                    raw_close = float(close)
                    adj = adjcloses[i] if i < len(adjcloses) else None
                    adj = float(adj) if adj is not None else raw_close
                    history.append({
                        "日期": dt_local.strftime("%Y-%m-%d"),
                        "開盤價": _safe_float(opens[i] if i < len(opens) else None),
                        "最高價": _safe_float(highs[i] if i < len(highs) else None),
                        "最低價": _safe_float(lows[i] if i < len(lows) else None),
                        "收盤價": raw_close,
                        "還原收盤價": adj,
                        "成交量": _safe_float(volumes[i] if i < len(volumes) else None, 0.0),
                    })
                except Exception:
                    continue
            history.sort(key=lambda x: _safe_str(x.get("日期")))

            latest = history[-1]["收盤價"] if history else _safe_float(meta.get("regularMarketPrice"))
            if latest is None:
                last_error = "latest none"
                continue

            return code, {
                "ok": True,
                "symbol": sym,
                "latest": float(latest),
                "latest_date": history[-1]["日期"] if history else "",
                "history": history,
                "source": "Yahoo adjusted OHLC",
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


def _parse_date(value: Any) -> datetime | None:
    s = _safe_str(value)[:10].replace("/", "-")
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except Exception:
        return None


def _history_rows(history: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in history or []:
        if isinstance(item, dict):
            date_s = _safe_str(item.get("日期") or item.get("date"))[:10]
            close = _safe_float(item.get("收盤價") if "收盤價" in item else item.get("close"))
            if not date_s or close is None:
                continue
            adj = _safe_float(item.get("還原收盤價") if "還原收盤價" in item else item.get("adjclose"), close)
            rows.append({
                "日期": date_s,
                "開盤價": _safe_float(item.get("開盤價") if "開盤價" in item else item.get("open"), close),
                "最高價": _safe_float(item.get("最高價") if "最高價" in item else item.get("high"), close),
                "最低價": _safe_float(item.get("最低價") if "最低價" in item else item.get("low"), close),
                "收盤價": close,
                "還原收盤價": adj,
                "成交量": _safe_float(item.get("成交量") if "成交量" in item else item.get("volume"), 0.0),
            })
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            date_s = _safe_str(item[0])[:10]
            close = _safe_float(item[1])
            if date_s and close is not None:
                rows.append({"日期": date_s, "開盤價": close, "最高價": close, "最低價": close,
                             "收盤價": close, "還原收盤價": close, "成交量": 0.0})
    rows.sort(key=lambda x: x["日期"])
    # 同日重複時保留最後一筆。
    return list({row["日期"]: row for row in rows}.values())


def _row_factor(row: dict[str, Any] | None) -> float:
    if not row:
        return 1.0
    raw = _safe_float(row.get("收盤價"), 0.0) or 0.0
    adj = _safe_float(row.get("還原收盤價"), raw) or raw
    if raw <= 0 or adj <= 0:
        return 1.0
    return float(adj / raw)


def _recommendation_row(history: list[dict[str, Any]], rec_date: str) -> dict[str, Any] | None:
    if not history:
        return None
    key = _safe_str(rec_date)[:10].replace("/", "-")
    exact = [r for r in history if r["日期"] == key]
    if exact:
        return exact[-1]
    previous = [r for r in history if r["日期"] <= key]
    return previous[-1] if previous else None


def _trading_rows_after(history: list[dict[str, Any]], start_date: str) -> list[dict[str, Any]]:
    key = _safe_str(start_date)[:10].replace("/", "-")
    return [r for r in history if r["日期"] > key]


def _price_after_sessions(history: list[dict[str, Any]], start_date: str, sessions: int, adjusted: bool = True):
    rows = _trading_rows_after(history, start_date)
    index = int(sessions) - 1
    if index < 0 or index >= len(rows):
        return None
    col = "還原收盤價" if adjusted else "收盤價"
    return _safe_float(rows[index].get(col))


def _level_for_row(level: float, rec_factor: float, target_row: dict[str, Any]) -> float:
    """將推薦日未還原價位換算到目標交易日的可比較價位。"""
    target_factor = _row_factor(target_row)
    if target_factor <= 0:
        target_factor = 1.0
    return float(level) * float(rec_factor or 1.0) / target_factor


def _first_positive(row: Dict[str, Any], names: list[str]) -> float | None:
    for name in names:
        value = _safe_float(row.get(name))
        if value is not None and value > 0:
            return float(value)
    return None


def _close_retention_ratio(high: float, low: float, close: float) -> float:
    spread = float(high) - float(low)
    if spread <= 1e-12:
        return 1.0 if close >= high else 0.5
    return max(0.0, min(1.0, (float(close) - float(low)) / spread))


def _breakout_confirmation_meta(high: float, low: float, close: float, trigger: float, guard: float) -> dict[str, Any]:
    retention = _close_retention_ratio(high, low, close)
    if close + 1e-9 < guard:
        return {"level": "F｜假突破失守", "quality": max(8.0, 32.0 * retention), "retention": retention}
    if close + 1e-9 >= trigger:
        if retention >= 0.55:
            return {"level": "C+｜收盤站上觸發價", "quality": 94.0 if retention >= 0.70 else 89.0, "retention": retention}
        return {"level": "C｜突破成立但尾盤偏弱", "quality": 78.0, "retention": retention}
    # 守價沒有失效，但收盤仍低於真正執行價，不能算成功訊號。
    return {"level": "H｜守價未失效待確認", "quality": 68.0 if retention >= 0.60 else 58.0 if retention >= 0.45 else 48.0, "retention": retention}


def _tradable_buy_limit_fill(session: dict[str, Any], limit_price: float) -> tuple[float | None, str]:
    """Return a fill that actually existed inside the published OHLC range.

    H9 truth rule: a support/pullback reference is *not* a fill simply because
    price came within 1.5%.  A buy-limit can fill only if the session traded at
    or through the limit.  If the market gaps fully below the limit, the open is
    a real observable price and is used as a conservative/verifiable fill.
    """
    low = _safe_float(session.get("最低價"))
    high = _safe_float(session.get("最高價"))
    open_px = _safe_float(session.get("開盤價"))
    if low is None or high is None or limit_price <= 0:
        return None, "OHLC不足，無法驗證成交"
    limit_price = float(limit_price)
    # Never touched the limit: no hypothetical fill below the day's low.
    if low > limit_price + 1e-9:
        return None, "參考價低於當日最低價，未實際成交"
    # The printed limit was traded inside the day's range.
    if high + 1e-9 >= limit_price:
        return limit_price, "限價實際觸及"
    # Whole session traded below the buy limit.  A resting buy-limit would fill
    # at/near the opening auction; use the observable open, never a non-traded
    # theoretical limit above the day's high.
    if open_px is not None and low - 1e-9 <= open_px <= high + 1e-9 and open_px <= limit_price + 1e-9:
        return float(open_px), "跳空低開，採實際開盤成交價"
    return None, "參考價不在可驗證成交區間"


def _tradable_buy_zone_fill(session: dict[str, Any], reference_price: float, tolerance_pct: float = ENTRY_TOLERANCE_PCT) -> tuple[float | None, str]:
    """Resolve a tradable fill for a published pullback/support *zone*.

    The legacy engine intentionally treated prices within 1.5% above a support
    reference as a valid retest.  H9 keeps that strategy semantics, but the fill
    must be a price the market actually crossed.  When price falls from above
    into the zone, execution is the zone's upper boundary; when the session opens
    already inside the zone, execution is the observable open.  The lower
    theoretical support is never used if the market did not trade there.
    """
    low = _safe_float(session.get("最低價"))
    high = _safe_float(session.get("最高價"))
    open_px = _safe_float(session.get("開盤價"))
    if low is None or high is None or reference_price <= 0:
        return None, "OHLC不足，無法驗證回測帶成交"
    ref = float(reference_price)
    upper = ref * (1.0 + max(0.0, float(tolerance_pct or 0.0)) / 100.0)
    if low > upper + 1e-9:
        return None, "尚未進入回測容許帶"
    # Open already in/below the zone: the opening auction is the first observable
    # tradable price, so never back-fill a nicer theoretical support.
    if open_px is not None and low - 1e-9 <= open_px <= high + 1e-9 and open_px <= upper + 1e-9:
        return float(open_px), "開盤已進入回測容許帶，採實際開盤價"
    # Price traded down from above and crossed the upper edge of the published
    # zone.  That boundary is within the day's OHLC and is a conservative first
    # executable price, unlike using the untouched lower support reference.
    if low - 1e-9 <= upper <= high + 1e-9:
        return float(upper), f"回測容許帶上緣{float(tolerance_pct):.1f}%實際觸及"
    return None, "回測容許帶不在可驗證成交區間"


def _tradable_buy_stop_fill(session: dict[str, Any], trigger_price: float) -> tuple[float | None, str]:
    """Return observable buy-stop execution price after trigger touch.

    If the market gaps above the stop, execution cannot be pretended at the old
    trigger; use the actual open.  Otherwise the trigger itself traded in-range.
    """
    low = _safe_float(session.get("最低價"))
    high = _safe_float(session.get("最高價"))
    open_px = _safe_float(session.get("開盤價"))
    if low is None or high is None or trigger_price <= 0 or high + 1e-9 < trigger_price:
        return None, "突破價未觸及"
    trigger_price = float(trigger_price)
    if open_px is not None and open_px >= trigger_price - 1e-9:
        if low - 1e-9 <= open_px <= high + 1e-9:
            return float(open_px), "跳空越過觸發價，採實際開盤價"
    if low - 1e-9 <= trigger_price <= high + 1e-9:
        return trigger_price, "突破價實際觸及"
    return None, "觸發價不在可驗證成交區間"


def _event_date_key(event: dict[str, Any] | None) -> str:
    return _safe_str((event or {}).get("date")) or "9999-12-31"


def _evaluate_entry_trigger(out: Dict[str, Any], history: list[dict[str, Any]], rec_date: str,
                            base_price: float, rec_factor: float) -> dict[str, Any]:
    after = _trading_rows_after(history, rec_date)
    result: dict[str, Any] = {
        "status": "未觸發｜不計交易勝負", "path": "等待條件", "date": "", "executable": False,
        "entry_price": None, "entry_adj": None, "quality": 50.0, "trigger_row": None,
        "theoretical_entry_price": None, "fill_source": "", "fill_verified": False,
    }
    if not after:
        return result

    path_text = _safe_str(out.get("主要進場路徑") or out.get("進場路徑") or out.get("進場時機"))
    breakout = _first_positive(out, ["突破確認參考價", "實戰觸發價", "突破確認價", "盤中轉強觸發價"])
    hold = _first_positive(out, ["觸發後守價", "突破後守價"])
    pullback = _first_positive(out, ["回測承接參考價", "回測承接價", "推薦買點_拉回", "預估進場點_拉回", "近端支撐"])
    if pullback is None and "回測" in _safe_str(out.get("預估進場點")):
        pullback = _first_positive(out, ["預估進場點", "支撐參考"])
    guard_retest = _first_positive(out, ["守價回測參考價", "觸發後守價", "突破後守價"])

    def _prior_breakout_confirmed() -> bool:
        # 守價回測必須建立在「此前真的突破且收盤守價」之後。
        # 只檢查推薦日前最近 5 個交易日，避免把數週前的舊突破誤當成當前有效結構。
        if breakout is None:
            return False
        prior = [r for r in history if r.get("日期", "") <= rec_date][-5:]
        for prior_row in prior:
            high = _safe_float(prior_row.get("最高價"), 0.0) or 0.0
            close = _safe_float(prior_row.get("收盤價"), 0.0) or 0.0
            trig = _level_for_row(breakout, rec_factor, prior_row)
            guard = _level_for_row(hold if hold is not None else breakout * 0.985, rec_factor, prior_row)
            if high + 1e-9 >= trig and close + 1e-9 >= guard:
                return True
        return False

    def guard_retest_event():
        if guard_retest is None:
            return None
        confirmed_before = _prior_breakout_confirmed()
        for session in after:
            low = _safe_float(session.get("最低價"), 0.0) or 0.0
            high = _safe_float(session.get("最高價"), 0.0) or 0.0
            close = _safe_float(session.get("收盤價"), 0.0) or 0.0
            ref = _level_for_row(guard_retest, rec_factor, session)
            trig = _level_for_row(breakout, rec_factor, session) if breakout is not None else 0.0
            breakout_attempt = bool(trig > 0 and high + 1e-9 >= trig)
            # 守價回測只能發生在前一個交易日以前已完成突破後。
            # 當日同時碰到突破價與守價價時，OHLC 無法知道先後順序，必須交由
            # breakout_event 以突破價計算，禁止用較低守價價產生回看式漂亮績效。
            if confirmed_before and low <= ref * (1.0 + ENTRY_TOLERANCE_PCT / 100.0) + 1e-9:
                fill, fill_source = _tradable_buy_zone_fill(session, ref)
                if low >= ref * 0.975 and close >= ref and fill is not None:
                    factor = _row_factor(session)
                    return {"status": "守價回測成立｜納入可執行績效", "path": "觸發守價回測", "date": session["日期"],
                            "executable": True, "entry_price": fill, "entry_adj": fill * factor,
                            "theoretical_entry_price": ref, "fill_source": fill_source, "fill_verified": True,
                            "quality": 91.0 if close >= ref * 1.01 else 84.0, "trigger_row": session,
                            "guard_price": ref, "breakout_price": trig}
                if low < ref * 0.975 and close < ref:
                    return {"status": "守價回測跌破｜取消交易", "path": "觸發守價回測", "date": session["日期"],
                            "executable": False, "entry_price": fill, "entry_adj": None,
                            "theoretical_entry_price": ref, "fill_source": fill_source, "fill_verified": bool(fill is not None),
                            "quality": 24.0, "trigger_row": session,
                            "guard_price": ref, "breakout_price": trig}
            if breakout_attempt:
                fill, fill_source = _tradable_buy_stop_fill(session, trig)
                if close + 1e-9 < ref:
                    return {"status": "觸發後失守｜假突破取消交易", "path": "突破確認", "date": session["日期"],
                            "executable": False, "entry_price": fill, "entry_adj": None,
                            "theoretical_entry_price": trig, "fill_source": fill_source, "fill_verified": bool(fill is not None),
                            "quality": 28.0, "trigger_row": session,
                            "guard_price": ref, "breakout_price": trig}
                confirmed_before = True
        return None

    def breakout_event():
        if breakout is None:
            return None
        for session in after:
            high = _safe_float(session.get("最高價"), 0.0) or 0.0
            low = _safe_float(session.get("最低價"), 0.0) or 0.0
            close = _safe_float(session.get("收盤價"), 0.0) or 0.0
            trig = _level_for_row(breakout, rec_factor, session)
            guard = _level_for_row(hold if hold is not None else breakout * 0.985, rec_factor, session)
            if high + 1e-9 >= trig:
                meta = _breakout_confirmation_meta(high, low, close, trig, guard)
                fill, fill_source = _tradable_buy_stop_fill(session, trig)
                if close + 1e-9 >= guard and fill is not None:
                    factor = _row_factor(session)
                    return {"status": "觸發且守價｜納入可執行績效", "path": "突破確認", "date": session["日期"],
                            "executable": True, "entry_price": fill, "entry_adj": fill * factor,
                            "theoretical_entry_price": trig, "fill_source": fill_source, "fill_verified": True,
                            "quality": meta["quality"], "trigger_row": session,
                            "guard_price": guard, "breakout_price": trig,
                            "confirmation_level": meta["level"], "close_retention": meta["retention"]}
                return {"status": "觸發後失守｜假突破取消交易", "path": "突破確認", "date": session["日期"],
                        "executable": False, "entry_price": fill, "entry_adj": None,
                        "theoretical_entry_price": trig, "fill_source": fill_source, "fill_verified": bool(fill is not None),
                        "quality": meta["quality"], "trigger_row": session,
                        "guard_price": guard, "breakout_price": trig,
                        "confirmation_level": meta["level"], "close_retention": meta["retention"]}
        return None

    def pullback_event():
        if pullback is None:
            return None
        for session in after:
            low = _safe_float(session.get("最低價"), 0.0) or 0.0
            close = _safe_float(session.get("收盤價"), 0.0) or 0.0
            ref = _level_for_row(pullback, rec_factor, session)
            # H9：保留既有 1.5% 回測容許帶，但成交價必須落在真實 OHLC
            # 可驗證區間；不能再把尚未交易到的 ref 當成漂亮成交價。
            if low <= ref * (1.0 + ENTRY_TOLERANCE_PCT / 100.0) + 1e-9:
                fill, fill_source = _tradable_buy_zone_fill(session, ref)
                if low >= ref * 0.975 and close >= ref and fill is not None:
                    factor = _row_factor(session)
                    return {"status": "回測承接成立｜納入可執行績效", "path": "回測承接", "date": session["日期"],
                            "executable": True, "entry_price": fill, "entry_adj": fill * factor,
                            "theoretical_entry_price": ref, "fill_source": fill_source, "fill_verified": True,
                            "quality": 88.0 if close >= ref * 1.01 else 80.0, "trigger_row": session}
                return {"status": "回測跌破｜取消交易", "path": "回測承接", "date": session["日期"],
                        "executable": False, "entry_price": fill, "entry_adj": None,
                        "theoretical_entry_price": ref, "fill_source": fill_source, "fill_verified": bool(fill is not None),
                        "quality": 25.0, "trigger_row": session}
        return None

    if "守價回測" in path_text:
        route_fns = [guard_retest_event, breakout_event, pullback_event]
    elif "回測" in path_text:
        route_fns = [pullback_event, guard_retest_event, breakout_event]
    else:
        route_fns = [breakout_event, guard_retest_event, pullback_event]

    primary = route_fns[0]()
    if primary:
        return primary
    # H9：主路徑尚未成立時，備用途徑若「真的碰價但失守」也是真實市場事件，
    # 不能把它丟掉後寫成未觸發。收集所有備用事件並採最早發生者，避免
    # 看過較早失敗後再用較晚成功回填成漂亮交易（look-ahead bias）。
    alternates: list[dict[str, Any]] = []
    for fn in route_fns[1:]:
        alternate = fn()
        if alternate:
            alternates.append(alternate)
    if alternates:
        alternates.sort(key=_event_date_key)
        return alternates[0]
    return result


def _execution_returns(history: list[dict[str, Any]], event: dict[str, Any], track_days: list[int]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if not event.get("executable") or not event.get("date") or not event.get("entry_adj"):
        return out
    entry_adj = float(event["entry_adj"])
    future = _trading_rows_after(history, event["date"])
    trigger_row = event.get("trigger_row") or {}
    trigger_close_adj = (_safe_float(trigger_row.get("還原收盤價")) or 0.0)
    if trigger_close_adj > 0:
        out["觸發後收盤績效%"] = _calc_return(trigger_close_adj, entry_adj)
    for d in track_days:
        idx = int(d) - 1
        if 0 <= idx < len(future):
            px = _safe_float(future[idx].get("還原收盤價"))
            ret = _calc_return(px, entry_adj) if px is not None else None
            if ret is not None:
                out[f"可執行交易{d}日%"] = ret
    replay_rows = [trigger_row] + future
    highs_adj: list[float] = []
    lows_adj: list[float] = []
    for r in replay_rows:
        factor = _row_factor(r)
        hi = _safe_float(r.get("最高價"))
        lo = _safe_float(r.get("最低價"))
        if hi is not None:
            highs_adj.append(hi * factor)
        if lo is not None:
            lows_adj.append(lo * factor)
    if highs_adj:
        out["可執行交易最大漲幅%"] = _calc_return(max(highs_adj), entry_adj)
    if lows_adj:
        out["可執行交易最大回撤%"] = _calc_return(min(lows_adj), entry_adj)
    return out


def _daily_execution_diagnostics(history: list[dict[str, Any]], rec_date: str, base_adjusted: float, event: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    after = _trading_rows_after(history, rec_date)
    if not after:
        return out
    first = after[0]
    factor = _row_factor(first)
    close_adj = (_safe_float(first.get("收盤價"), 0.0) or 0.0) * factor
    candidate_ret = _calc_return(close_adj, base_adjusted)
    if candidate_ret is not None:
        out["隔日候選漲跌%"] = candidate_ret

    status = _safe_str(event.get("status"))
    trigger_row = event.get("trigger_row") or {}
    entry = _safe_float(event.get("entry_price"))
    if trigger_row and entry and entry > 0:
        row_factor = _row_factor(trigger_row)
        entry_adj = entry * row_factor
        high_adj = (_safe_float(trigger_row.get("最高價"), entry) or entry) * row_factor
        low_adj = (_safe_float(trigger_row.get("最低價"), entry) or entry) * row_factor
        close_adj_event = (_safe_float(trigger_row.get("收盤價"), entry) or entry) * row_factor
        out["觸發當日最高報酬%"] = _calc_return(high_adj, entry_adj)
        out["觸發當日最大回撤%"] = _calc_return(low_adj, entry_adj)
        out["觸發當日收盤績效%"] = _calc_return(close_adj_event, entry_adj)
        retention = event.get("close_retention")
        if retention is None:
            retention = _close_retention_ratio(high_adj, low_adj, close_adj_event)
        out["觸發當日收盤保留率%"] = round(float(retention) * 100.0, 2)
        confirmation = _safe_str(event.get("confirmation_level"))
        if not confirmation and status.startswith("觸發且守價"):
            guard_price = (_safe_float(event.get("guard_price"), entry) or entry) * row_factor
            confirmation = _breakout_confirmation_meta(high_adj, low_adj, close_adj_event, entry_adj, guard_price)["level"]
        out["觸發收盤確認層級"] = confirmation

    if status.startswith("觸發且守價"):
        confirmation = _safe_str(out.get("觸發收盤確認層級"))
        if confirmation.startswith("C+"):
            result = "確認成功｜收盤站上觸發價"
            review = "正式列為隔日確認成功；納入觸發成功率與後續1/3/5/10/20日績效。"
        elif confirmation.startswith("C｜"):
            result = "觸發成立｜但尾盤確認不足"
            review = "已觸發但尾盤偏弱；納入交易損益，不列為高品質成功訊號。"
        else:
            result = "中性｜守價未失效但未站回觸發價"
            review = "已觸發且仍在守價之上，但收盤未站回執行價；納入交易損益，成功率暫不計勝。"
    elif status.startswith("守價回測成立") or status.startswith("回測承接成立"):
        same_day = _safe_float(out.get("觸發當日收盤績效%"), 0.0) or 0.0
        result = "回測承接成功" if same_day >= 0 else "回測守價成立｜收盤仍低於執行價"
        review = "納入可執行績效；持續追蹤後續1/3/5/10/20日。"
    elif "假突破" in status or "跌破" in status:
        result = "訊號失敗｜假突破或回測跌破"
        review = "不納入交易報酬，但必須納入觸發品質與假突破率校正。"
    else:
        result = "未觸發｜不計交易勝負"
        if candidate_ret is not None and candidate_ret >= 3.0:
            review = "候選上漲但未觸發；列入觸發價過遠/漏選檢討，不可冒充交易獲利。"
            out["未觸發漏選標記"] = "是｜隔日上漲3%以上"
        elif candidate_ret is not None and candidate_ret <= -3.0:
            review = "未觸發並避開明顯下跌；維持不交易紀律。"
            out["未觸發漏選標記"] = "否｜避開下跌"
        else:
            review = "未觸發，維持觀察；不納入交易勝負。"
            out["未觸發漏選標記"] = "否"
    out["隔日執行命中結果"] = result
    out["隔日績效檢討標籤"] = review
    out["候選與交易分流說明"] = "候選漲跌只用於召回檢討；碰價不等於成功。收盤站上觸發價才算確認成功，僅守住守價屬中性待確認，跌破守價為假突破。"
    return out


def update_record_perf(row: Dict[str, Any], quote: Dict[str, Any], track_days: List[int] | None = None) -> Dict[str, Any]:
    track_days = track_days or DEFAULT_TRACK_DAYS
    out = dict(row)

    if not quote or not quote.get("ok"):
        out["績效更新狀態"] = "ONLINE_FAIL"
        out["績效更新錯誤"] = _safe_str((quote or {}).get("error"))
        return out

    history = _history_rows(quote.get("history") or [])
    latest_row = history[-1] if history else None
    latest = _safe_float(latest_row.get("收盤價") if latest_row else quote.get("latest"))
    if latest is None:
        out["績效更新狀態"] = "ONLINE_FAIL"
        out["績效更新錯誤"] = "latest none"
        return out

    base_price = _first_positive(out, ["推薦價格", "推薦日價格", "買進價", "最新價"])
    if base_price is None:
        out["績效更新狀態"] = "BASE_PRICE_MISSING"
        out["績效更新錯誤"] = "base price missing"
        return out

    rec_date = _safe_str(out.get("推薦日期") or out.get("建立日期") or out.get("建立時間"))
    rec_row = _recommendation_row(history, rec_date)
    rec_factor = _row_factor(rec_row)
    base_adjusted = float(base_price) * rec_factor
    latest_adj = _safe_float(latest_row.get("還原收盤價") if latest_row else None, latest) or latest

    out["最新價"] = latest
    out["最新更新時間"] = quote.get("fetched_at") or _now_str()
    out["追蹤更新時間"] = quote.get("fetched_at") or _now_str()
    out["資料來源"] = out.get("資料來源") or quote.get("source", "Yahoo adjusted OHLC")
    out["績效資料來源"] = "Yahoo adjusted OHLC｜候選與觸發績效分流"
    out["績效更新狀態"] = "OK"
    out["績效更新錯誤"] = ""
    out["績效行情日期"] = _safe_str((latest_row or {}).get("日期"))
    out["還原價格調整係數"] = round(rec_factor, 8)
    out["除權息調整旗標"] = "是｜已用還原價" if abs(rec_factor - 1.0) >= 0.002 else "否"
    out["績效計算口徑"] = "還原收盤價候選績效＋觸發前置條件＋觸發後可執行交易績效"
    out["績效更新版本"] = PERF_FAST_UPDATE_VERSION

    total_ret = _calc_return(latest_adj, base_adjusted)
    if total_ret is not None:
        out["目前損益幅%"] = total_ret
        out["損益幅%"] = total_ret

    for d in track_days:
        px = _price_after_sessions(history, rec_date, d, adjusted=True)
        ret = _calc_return(px, base_adjusted) if px is not None else None
        if ret is not None:
            out[f"推薦後{d}日%"] = ret

    event = _evaluate_entry_trigger(out, history, rec_date, float(base_price), rec_factor)
    out["進場觸發狀態"] = event.get("status")
    out["進場觸發日期"] = event.get("date")
    out["進場評估路徑"] = event.get("path")
    out["是否納入可執行績效"] = bool(event.get("executable"))
    out["執行基準價"] = round(float(event["entry_price"]), 4) if event.get("entry_price") else None
    out["理論進場參考價"] = round(float(event["theoretical_entry_price"]), 4) if event.get("theoretical_entry_price") else None
    out["執行價來源"] = _safe_str(event.get("fill_source"))
    out["執行價可成交驗證"] = bool(event.get("fill_verified"))
    out["觸發訊號品質分"] = round(float(event.get("quality", 50.0)), 1)
    out.update(_execution_returns(history, event, track_days))
    out.update(_daily_execution_diagnostics(history, rec_date, base_adjusted, event))
    return out

def update_recommendation_perf_fast_v77(
    json_files: List[str] | None = None,
    max_records: int = 80,
    batch_limit: int = 60,
    max_workers: int = 16,
    stale_minutes: int = 60,
    track_days: List[int] | None = None,
    process_all: bool = False,
    max_total_records: int | None = None,
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
        "process_all": bool(process_all),
        "batches": 0,
        "candidate_by_file": {},
        "max_total_records": 0,
        "selection_strategy": "all-scan_newest-first_fair-round-robin",
    }

    root = Path(".")
    work_by_file: Dict[str, List[Tuple[str, int, str]]] = {}

    for fn in json_files:
        path = root / fn
        records = _read_json_records(path)
        if not records:
            continue

        summary["processed_files"].append(fn)
        summary["total_records"] += len(records)

        # V171：候選判斷本身掃描整份 JSON（1,796 筆僅為本機迴圈，成本很低），
        # 但非完整模式只對 max_records 筆執行外部報價。由最新紀錄往舊紀錄排，
        # 已更新者會因 stale_minutes 自動略過，後續一鍵更新可逐批追上更舊的缺值，
        # 不再讓「最近 max_records 筆」以外的歷史資料永遠沒有機會補績效。
        indexed = list(enumerate(records))
        if not process_all:
            indexed.reverse()

        for idx, row in indexed:
            if _needs_update(row, stale_minutes=stale_minutes):
                code = _normalize_code(row.get("股票代號") or row.get("代號"))
                if code:
                    work_by_file.setdefault(fn, []).append((fn, idx, code))

    # V171：公平輪詢每個檔案，避免 godpick_records.json 先塞滿上限後，
    # godpick_recommend_list / latest recommendations 永遠得不到更新。
    all_work_items: List[Tuple[str, int, str]] = []
    file_names = [fn for fn in json_files if work_by_file.get(fn)]
    cursor = {fn: 0 for fn in file_names}
    while file_names:
        next_names = []
        for fn in file_names:
            pos = cursor[fn]
            items = work_by_file.get(fn, [])
            if pos < len(items):
                all_work_items.append(items[pos])
                cursor[fn] = pos + 1
            if cursor[fn] < len(items):
                next_names.append(fn)
        file_names = next_names

    if not process_all:
        total_cap = int(max_total_records if max_total_records is not None else (max_records or 0))
        if total_cap > 0:
            all_work_items = all_work_items[:total_cap]
    summary["candidate_by_file"] = {fn: len(items) for fn, items in work_by_file.items()}
    summary["max_total_records"] = 0 if process_all else int(max_total_records if max_total_records is not None else (max_records or 0))
    summary["candidates"] = len(all_work_items)

    codes = sorted(set(x[2] for x in all_work_items))
    quotes = {}
    chunk_size = max(1, int(batch_limit or 60))
    for start_i in range(0, len(codes), chunk_size):
        chunk = codes[start_i:start_i + chunk_size]
        if not chunk:
            continue
        summary["batches"] += 1
        quotes.update(fetch_latest_quotes_fast_v77(chunk, max_workers=max_workers))

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
