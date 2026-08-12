# -*- coding: utf-8 -*-
"""V188 T+1 execution-truth and probability calibration service.

The service keeps three truths separate:
- Selection Alpha: did the selected stock outperform its benchmark?
- Entry Alpha: if the published trigger actually fired, was the entry good?
- Risk Alpha: did the guard/stop logic avoid or contain adverse excursions?

Radar names that never triggered are *never* counted as profitable trades.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable
import json
import math
import threading

import pandas as pd

from godpick_perf_fast_update_v77 import update_record_perf

try:
    from godpick_durability_service import persist_json_async, persist_json_permanent
except Exception:
    persist_json_async = None
    persist_json_permanent = None

TRUTH_VERSION = "godpick_t1_trade_truth_v188_20260812"
TRUTH_FILE = "godpick_t1_trade_truth.json"
CALIBRATION_FILE = "godpick_probability_calibration.json"
BASE_DIR = Path(__file__).resolve().parent

_BG_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="godpick-t1-truth-v188")
_BG_LOCK = threading.Lock()
_BG_RUNNING = False


def _now() -> str:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Taipei")).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _today() -> date:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Taipei")).date()
    except Exception:
        return date.today()


def _s(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    t = str(v).strip()
    return "" if t.lower() in {"", "nan", "none", "null", "nat", "<na>", "--"} else t


def _f(v: Any, default: float | None = None) -> float | None:
    try:
        if v is None:
            return default
        t = str(v).strip().replace(",", "").replace("％", "%")
        if t.endswith("%"):
            t = t[:-1]
        if not t:
            return default
        x = float(t)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _date(v: Any) -> str:
    t = _s(v)[:10].replace("/", "-")
    if not t:
        return ""
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(t.replace("-", "") if fmt == "%Y%m%d" else t, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    return ""


def _rows(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, pd.DataFrame):
        raw = data.to_dict(orient="records")
    elif isinstance(data, list):
        raw = data
    elif isinstance(data, dict):
        raw = data.get("records") or data.get("rows") or data.get("data") or []
    else:
        raw = []
    return [dict(x) for x in raw if isinstance(x, dict)]


def _read_json(path_name: str, default: Any) -> Any:
    try:
        p = BASE_DIR / path_name
        if not p.exists():
            return default
        return json.loads(p.read_text(encoding="utf-8-sig"))
    except Exception:
        return default


def _persist(path_name: str, payload: Any, reason: str) -> tuple[bool, str]:
    # V188 truth/calibration are learning authority.  A background T+1 replay may
    # take network time, but once it finishes the result must be remotely confirmed
    # before we call that truth durable.  This does NOT block the 1,700-stock scan:
    # the automatic replay itself already runs in _BG_EXECUTOR.
    if callable(persist_json_permanent):
        try:
            return persist_json_permanent(path_name, payload, reason=reason)
        except Exception as exc:
            return False, f"permanent persistence exception:{exc}"
    if callable(persist_json_async):
        try:
            ok, msg = persist_json_async(path_name, payload, reason=reason)
            return bool(ok), f"{msg}｜remote confirmation pending"
        except Exception as exc:
            return False, f"async persistence exception:{exc}"
    try:
        p = BASE_DIR / path_name
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        tmp.replace(p)
        return True, "local atomic only; no remote durability service available"
    except Exception as exc:
        return False, f"local persistence failed:{exc}"


def _normalize_market(v: Any) -> str:
    t = _s(v).lower()
    if any(x in t for x in ["上櫃", "otc", "tpex"]):
        return "上櫃"
    return "上市"


def _business_key(row: dict[str, Any]) -> str:
    code = "".join(ch for ch in _s(row.get("股票代號") or row.get("代號")) if ch.isdigit())[:4]
    rec = _date(row.get("推薦日期") or row.get("建立日期") or row.get("建立時間") or row.get("K線最後交易日"))
    role = _s(row.get("正式推薦分區") or row.get("推薦角色") or row.get("盤中雷達優先級") or row.get("SuperAI進場狀態"))
    return f"{rec}|{code}|{role}"


def _recommendation_date(row: dict[str, Any]) -> str:
    return _date(row.get("推薦日期") or row.get("建立日期") or row.get("建立時間") or row.get("K線最後交易日"))


def _eligible_record(row: dict[str, Any], today: date, max_age_days: int) -> bool:
    rec = _recommendation_date(row)
    if not rec:
        return False
    try:
        rd = datetime.strptime(rec, "%Y-%m-%d").date()
    except Exception:
        return False
    if rd >= today or (today - rd).days > max_age_days:
        return False
    # Keep all V188/SuperAI decisions plus actionable legacy roles.  This avoids
    # learning only from formal winners while not downloading the entire archive.
    model = _s(row.get("SuperAI模型版本"))
    role = "｜".join(_s(row.get(k)) for k in ["正式推薦分區", "推薦角色", "盤中雷達優先級", "SuperAI進場狀態"] if _s(row.get(k)))
    return model.startswith("super_ai") or bool(role)


def _history_to_quote(df: pd.DataFrame, source: str = "V188 multi-source history") -> dict[str, Any]:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return {"ok": False, "error": "empty history"}
    rows = []
    for _, r in df.iterrows():
        d = _date(r.get("日期") if "日期" in df.columns else r.get("date"))
        close = _f(r.get("收盤價") if "收盤價" in df.columns else r.get("close"))
        if not d or close is None or close <= 0:
            continue
        rows.append({
            "日期": d,
            "開盤價": _f(r.get("開盤價") if "開盤價" in df.columns else r.get("open"), close),
            "最高價": _f(r.get("最高價") if "最高價" in df.columns else r.get("high"), close),
            "最低價": _f(r.get("最低價") if "最低價" in df.columns else r.get("low"), close),
            "收盤價": close,
            "還原收盤價": _f(r.get("還原收盤價") if "還原收盤價" in df.columns else r.get("adjclose"), close),
            "成交量": _f(r.get("成交量") if "成交量" in df.columns else r.get("volume"), 0.0),
        })
    rows.sort(key=lambda x: x["日期"])
    if not rows:
        return {"ok": False, "error": "normalized history empty"}
    return {"ok": True, "history": rows, "latest": rows[-1]["收盤價"], "latest_date": rows[-1]["日期"], "source": source, "fetched_at": _now()}


def _default_history_provider(row: dict[str, Any]) -> dict[str, Any]:
    # Lazy import avoids pulling Streamlit-only project dependencies in offline
    # unit tests.  Production already runs inside Streamlit.
    try:
        from utils import get_history_data
    except Exception as exc:
        return {"ok": False, "error": f"history provider unavailable:{exc}"}
    rec = _recommendation_date(row)
    if not rec:
        return {"ok": False, "error": "recommendation date missing"}
    rd = datetime.strptime(rec, "%Y-%m-%d").date()
    start = rd - timedelta(days=25)
    end = _today()
    code = _s(row.get("股票代號") or row.get("代號"))
    name = _s(row.get("股票名稱") or row.get("名稱"))
    market = _normalize_market(row.get("市場別") or row.get("市場"))
    try:
        df = get_history_data(code, name, market, start_date=start, end_date=end)
        return _history_to_quote(df, source="V188 utils multi-source OHLC")
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def _default_benchmark_provider() -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {"twse": [], "otc": []}
    try:
        from market_index_history_service import fetch_market_index_history
        for kind in ("twse", "otc"):
            result = fetch_market_index_history(kind, days=120, timeout=2.5, max_seconds=8.0)
            if isinstance(result, dict) and result.get("ok"):
                out[kind] = list(result.get("history") or [])
    except Exception:
        pass
    return out


def _next_row(history: list[dict[str, Any]], rec_date: str) -> dict[str, Any] | None:
    rows = sorted((r for r in history if isinstance(r, dict)), key=lambda r: _date(r.get("日期") or r.get("date")))
    for row in rows:
        d = _date(row.get("日期") or row.get("date"))
        if d and d > rec_date:
            return row
    return None


def _benchmark_return(history: list[dict[str, Any]], rec_date: str) -> float | None:
    if not history:
        return None
    norm = []
    for r in history:
        d = _date(r.get("date") or r.get("日期"))
        close = _f(r.get("close") if "close" in r else r.get("收盤價"))
        if d and close is not None and close > 0:
            norm.append((d, close))
    norm.sort()
    base = None
    nxt = None
    for d, c in norm:
        if d <= rec_date:
            base = c
        elif d > rec_date and nxt is None:
            nxt = c
            break
    if base and nxt:
        return round((nxt - base) / base * 100.0, 4)
    return None


def _selection_result(alpha: float | None, ret: float | None) -> str:
    if ret is None:
        return "待成熟"
    if alpha is None:
        return "UP" if ret > 0 else "DOWN" if ret < 0 else "FLAT"
    if alpha >= 1.0:
        return "WIN｜明顯跑贏大盤"
    if alpha >= 0:
        return "PASS｜小幅跑贏大盤"
    if alpha <= -1.0:
        return "LOSS｜明顯落後大盤"
    return "NEUTRAL｜近似大盤"


def _entry_result(updated: dict[str, Any]) -> str:
    executable = bool(updated.get("是否納入可執行績效"))
    status = _s(updated.get("進場觸發狀態"))
    if not executable:
        if "假突破" in status or "跌破" in status:
            return "FAIL-SIGNAL｜觸發品質失敗，不計交易勝負"
        return "NO-TRADE｜未觸發，不計交易勝負"
    r1 = _f(updated.get("可執行交易1日%"))
    same = _f(updated.get("觸發當日收盤績效%"))
    metric = r1 if r1 is not None else same
    if metric is None:
        return "OPEN｜已觸發，後續績效待成熟"
    return "WIN" if metric > 0 else "LOSS" if metric < 0 else "FLAT"


def _risk_result(updated: dict[str, Any]) -> str:
    status = _s(updated.get("進場觸發狀態"))
    mae = _f(updated.get("可執行交易最大回撤%"))
    if "假突破" in status or "跌破" in status:
        return "PASS｜風控取消假突破/跌破"
    if not bool(updated.get("是否納入可執行績效")):
        return "N/A｜未建立交易"
    if mae is None:
        return "OPEN｜回撤資料待成熟"
    if mae >= -2.0:
        return "A｜回撤受控"
    if mae >= -4.0:
        return "B｜可接受回撤"
    return "C｜回撤偏大，需檢討停損/買點"


def _truth_from_updated(original: dict[str, Any], updated: dict[str, Any], quote: dict[str, Any], benchmark_ret: float | None) -> dict[str, Any]:
    rec = _recommendation_date(original)
    hist = quote.get("history") or []
    next_session = _next_row(hist, rec) or {}
    cand_ret = _f(updated.get("隔日候選漲跌%") if _s(updated.get("隔日候選漲跌%")) else updated.get("推薦後1日%"))
    selection_alpha = round(cand_ret - benchmark_ret, 4) if cand_ret is not None and benchmark_ret is not None else None
    raw_prob = _f(original.get("SuperAI隔日上漲機率%"))
    cal_prob = _f(original.get("SuperAI校準後隔日上漲機率%"), raw_prob)
    code = _s(original.get("股票代號") or original.get("代號"))
    return {
        "version": TRUTH_VERSION,
        "business_key": _business_key(original),
        "推薦日期": rec,
        "股票代號": code,
        "股票名稱": _s(original.get("股票名稱") or original.get("名稱")),
        "市場別": _normalize_market(original.get("市場別") or original.get("市場")),
        "類別": _s(original.get("類別") or original.get("產業") or original.get("族群名稱")) or "未分類",
        "推薦角色": _s(original.get("正式推薦分區") or original.get("推薦角色") or original.get("盤中雷達優先級")),
        "SuperAI原始上漲機率%": raw_prob,
        "SuperAI校準後上漲機率%": cal_prob,
        "SuperAI Alpha等級": _s(original.get("SuperAI Alpha等級")),
        "SuperAI Trade等級": _s(original.get("SuperAI Trade等級")),
        "V188交易許可": _s(original.get("V188交易許可")),
        "V188執行RR": _f(original.get("SuperAI執行風報比") or original.get("路徑風險報酬比") or original.get("風險報酬比")),
        "隔日日期": _date(next_session.get("日期") or next_session.get("date")),
        "隔日開盤": _f(next_session.get("開盤價") if "開盤價" in next_session else next_session.get("open")),
        "隔日最高": _f(next_session.get("最高價") if "最高價" in next_session else next_session.get("high")),
        "隔日最低": _f(next_session.get("最低價") if "最低價" in next_session else next_session.get("low")),
        "隔日收盤": _f(next_session.get("收盤價") if "收盤價" in next_session else next_session.get("close")),
        "隔日候選漲跌%": cand_ret,
        "市場基準隔日%": benchmark_ret,
        "Selection Alpha%": selection_alpha,
        "類股平均隔日%": None,
        "Sector Alpha%": None,
        "Selection結果": _selection_result(selection_alpha, cand_ret),
        "進場觸發狀態": _s(updated.get("進場觸發狀態")),
        "進場觸發日期": _s(updated.get("進場觸發日期")),
        "是否納入可執行績效": bool(updated.get("是否納入可執行績效")),
        "執行基準價": _f(updated.get("執行基準價")),
        "觸發訊號品質分": _f(updated.get("觸發訊號品質分")),
        "觸發當日最高報酬%": _f(updated.get("觸發當日最高報酬%")),
        "觸發當日最大回撤%": _f(updated.get("觸發當日最大回撤%")),
        "觸發當日收盤績效%": _f(updated.get("觸發當日收盤績效%")),
        "可執行交易1日%": _f(updated.get("可執行交易1日%")),
        "MFE%": _f(updated.get("可執行交易最大漲幅%")),
        "MAE%": _f(updated.get("可執行交易最大回撤%")),
        "Entry結果": _entry_result(updated),
        "Risk結果": _risk_result(updated),
        "是否假突破": bool("假突破" in _s(updated.get("進場觸發狀態"))),
        "T1成熟": bool(_date(next_session.get("日期") or next_session.get("date"))),
        "績效行情日期": _s(updated.get("績效行情日期")),
        "績效資料來源": _s(updated.get("績效資料來源") or quote.get("source")),
        "updated_at": _now(),
    }


def build_probability_calibration(truth_rows: Any) -> dict[str, Any]:
    rows = _rows(truth_rows)
    samples = []
    executable = []
    for r in rows:
        if not bool(r.get("T1成熟")):
            continue
        prob = _f(r.get("SuperAI校準後上漲機率%") if _s(r.get("SuperAI校準後上漲機率%")) else r.get("SuperAI原始上漲機率%"))
        ret = _f(r.get("隔日候選漲跌%"))
        if prob is None or ret is None:
            continue
        samples.append((prob, 1.0 if ret > 0 else 0.0, ret, _f(r.get("Selection Alpha%"))))
        if bool(r.get("是否納入可執行績效")):
            er = _f(r.get("可執行交易1日%") if _s(r.get("可執行交易1日%")) else r.get("觸發當日收盤績效%"))
            if er is not None:
                executable.append(er)
    bins = []
    for low in range(0, 100, 5):
        high = low + 5
        sub = [x for x in samples if low <= x[0] < high or (high == 100 and x[0] == 100)]
        if not sub:
            continue
        n = len(sub)
        mean_p = sum(x[0] for x in sub) / n
        actual = sum(x[1] for x in sub) / n * 100.0
        brier = sum(((x[0] / 100.0) - x[1]) ** 2 for x in sub) / n
        alpha_vals = [x[3] for x in sub if x[3] is not None]
        bins.append({
            "low": low, "high": high, "n": n,
            "mean_predicted_pct": round(mean_p, 2), "actual_up_rate_pct": round(actual, 2),
            "brier_score": round(brier, 5),
            "avg_selection_alpha_pct": round(sum(alpha_vals) / len(alpha_vals), 4) if alpha_vals else None,
        })
    n = len(samples)
    global_brier = sum(((x[0] / 100.0) - x[1]) ** 2 for x in samples) / n if n else None
    actual_rate = sum(x[1] for x in samples) / n * 100.0 if n else None
    mean_pred = sum(x[0] for x in samples) / n if n else None
    return {
        "version": TRUTH_VERSION,
        "updated_at": _now(),
        "eligible_samples": n,
        "mean_predicted_up_pct": round(mean_pred, 2) if mean_pred is not None else None,
        "actual_up_rate_pct": round(actual_rate, 2) if actual_rate is not None else None,
        "brier_score": round(global_brier, 5) if global_brier is not None else None,
        "executable_samples": len(executable),
        "executable_win_rate_pct": round(sum(1 for x in executable if x > 0) / len(executable) * 100.0, 2) if executable else None,
        "avg_executable_ret_pct": round(sum(executable) / len(executable), 4) if executable else None,
        "bins": bins,
        "policy": "T1 selection outcome calibrates probability; untriggered radar is never counted as a trade win",
    }


def _sector_alpha(rows: list[dict[str, Any]]) -> None:
    groups: dict[tuple[str, str], list[float]] = {}
    for r in rows:
        ret = _f(r.get("隔日候選漲跌%"))
        if ret is None:
            continue
        key = (_s(r.get("推薦日期")), _s(r.get("類別")) or "未分類")
        groups.setdefault(key, []).append(ret)
    for r in rows:
        ret = _f(r.get("隔日候選漲跌%"))
        key = (_s(r.get("推薦日期")), _s(r.get("類別")) or "未分類")
        vals = groups.get(key) or []
        if ret is None or len(vals) < 2:
            continue
        mean = sum(vals) / len(vals)
        r["類股平均隔日%"] = round(mean, 4)
        r["Sector Alpha%"] = round(ret - mean, 4)


def refresh_t1_trade_truth(
    records: Any | None = None,
    *,
    history_provider: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    benchmark_provider: Callable[[], dict[str, list[dict[str, Any]]]] | None = None,
    max_records: int = 160,
    max_workers: int = 8,
    max_age_days: int = 45,
    persist: bool = True,
) -> dict[str, Any]:
    raw_records = _rows(records) if records is not None else _rows(_read_json("godpick_records.json", []))
    today = _today()
    eligible = [r for r in raw_records if _eligible_record(r, today, max_age_days)]
    eligible.sort(key=lambda r: (_recommendation_date(r), _s(r.get("股票代號"))), reverse=True)
    eligible = eligible[: max(1, int(max_records or 1))]
    provider = history_provider or _default_history_provider
    bprovider = benchmark_provider or _default_benchmark_provider
    benchmarks = bprovider() if callable(bprovider) else {"twse": [], "otc": []}
    if not isinstance(benchmarks, dict):
        benchmarks = {"twse": [], "otc": []}

    processed: list[dict[str, Any]] = []
    failures: list[str] = []

    def one(row: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        quote = provider(row)
        if not isinstance(quote, dict) or not quote.get("ok"):
            return None, f"{_s(row.get('股票代號'))}:{_s((quote or {}).get('error')) or 'history failed'}"
        updated = update_record_perf(row, quote, track_days=[1, 3, 5, 10, 20])
        market = _normalize_market(row.get("市場別") or row.get("市場"))
        bench_ret = _benchmark_return(benchmarks.get("otc" if market == "上櫃" else "twse", []), _recommendation_date(row))
        return _truth_from_updated(row, updated, quote, bench_ret), ""

    workers = max(1, min(int(max_workers or 1), 12))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(one, row) for row in eligible]
        for fut in as_completed(futures):
            try:
                item, err = fut.result()
            except Exception as exc:
                item, err = None, str(exc)
            if item:
                processed.append(item)
            elif err:
                failures.append(err)

    # Merge by immutable business key.  Newer truth replaces the same event only;
    # historical events are never deleted simply because this refresh was partial.
    old_payload = _read_json(TRUTH_FILE, {})
    old_rows = _rows(old_payload)
    merged = {(_s(r.get("business_key")) or _business_key(r)): r for r in old_rows if isinstance(r, dict)}
    for r in processed:
        merged[_s(r.get("business_key")) or _business_key(r)] = r
    truth_rows = list(merged.values())
    truth_rows.sort(key=lambda r: (_s(r.get("推薦日期")), _s(r.get("股票代號")), _s(r.get("推薦角色"))))
    _sector_alpha(truth_rows)
    calibration = build_probability_calibration(truth_rows)

    matured = [r for r in truth_rows if bool(r.get("T1成熟"))]
    executable = [r for r in matured if bool(r.get("是否納入可執行績效"))]
    selection_alpha = [_f(r.get("Selection Alpha%")) for r in matured]
    selection_alpha = [x for x in selection_alpha if x is not None]
    payload = {
        "version": TRUTH_VERSION,
        "updated_at": _now(),
        "records": truth_rows,
        "summary": {
            "total_truth_rows": len(truth_rows), "processed_this_run": len(processed), "failed_this_run": len(failures),
            "matured_t1_samples": len(matured), "executable_samples": len(executable),
            "trigger_rate_pct": round(len(executable) / len(matured) * 100.0, 2) if matured else None,
            "executable_win_rate_pct": calibration.get("executable_win_rate_pct"),
            "avg_selection_alpha_pct": round(sum(selection_alpha) / len(selection_alpha), 4) if selection_alpha else None,
            "brier_score": calibration.get("brier_score"),
        },
        "failures": failures[:80],
    }
    persist_ok = True
    persist_messages: list[str] = []
    if persist:
        ok1, msg1 = _persist(TRUTH_FILE, payload, "V188 T+1 executable truth")
        ok2, msg2 = _persist(CALIBRATION_FILE, calibration, "V188 probability calibration")
        persist_ok = bool(ok1 and ok2)
        persist_messages = [f"{TRUTH_FILE}: {msg1}", f"{CALIBRATION_FILE}: {msg2}"]
    return {
        "ok": True, **payload["summary"], "failures": failures,
        "calibration": calibration, "records": truth_rows,
        "persistence_ok": persist_ok, "persistence_messages": persist_messages,
    }


def load_t1_truth_rows(limit: int | None = None) -> list[dict[str, Any]]:
    rows = _rows(_read_json(TRUTH_FILE, {}))
    rows.sort(key=lambda r: (_s(r.get("推薦日期")), _s(r.get("股票代號"))), reverse=True)
    return rows[: int(limit)] if limit else rows


def load_t1_truth_summary() -> dict[str, Any]:
    payload = _read_json(TRUTH_FILE, {})
    summary = payload.get("summary") if isinstance(payload, dict) else {}
    calibration = _read_json(CALIBRATION_FILE, {})
    return {
        **(summary if isinstance(summary, dict) else {}),
        "updated_at": _s(payload.get("updated_at") if isinstance(payload, dict) else ""),
        "calibration_samples": int(_f(calibration.get("eligible_samples") if isinstance(calibration, dict) else 0, 0) or 0),
        "brier_score": calibration.get("brier_score") if isinstance(calibration, dict) else None,
        "executable_win_rate_pct": calibration.get("executable_win_rate_pct") if isinstance(calibration, dict) else None,
    }


def load_probability_calibration() -> dict[str, Any]:
    data = _read_json(CALIBRATION_FILE, {})
    return data if isinstance(data, dict) else {}


def refresh_t1_truth_async(*, max_records: int = 160, max_workers: int = 8) -> tuple[bool, str]:
    global _BG_RUNNING
    with _BG_LOCK:
        if _BG_RUNNING:
            return True, "V188 T+1真相更新已在背景執行"
        _BG_RUNNING = True

    def worker() -> None:
        global _BG_RUNNING
        try:
            refresh_t1_trade_truth(max_records=max_records, max_workers=max_workers, persist=True)
        finally:
            with _BG_LOCK:
                _BG_RUNNING = False

    _BG_EXECUTOR.submit(worker)
    return True, f"V188 T+1真相更新已排入背景（最多{max_records}筆成熟候選）"


__all__ = [
    "TRUTH_VERSION", "TRUTH_FILE", "CALIBRATION_FILE",
    "refresh_t1_trade_truth", "refresh_t1_truth_async", "load_t1_truth_rows", "load_t1_truth_summary",
    "build_probability_calibration", "load_probability_calibration",
]
