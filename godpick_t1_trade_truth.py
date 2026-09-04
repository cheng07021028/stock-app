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
import time

import pandas as pd

from godpick_perf_fast_update_v77 import update_record_perf

try:
    from godpick_durability_service import persist_json_async, persist_json_permanent
except Exception:
    persist_json_async = None
    persist_json_permanent = None

TRUTH_VERSION = "godpick_t1_trade_truth_v191_h60_mainrise_holder_snowball_truth_20260904"
TRUTH_FILE = "godpick_t1_trade_truth.json"
CALIBRATION_FILE = "godpick_probability_calibration.json"
BASE_DIR = Path(__file__).resolve().parent

_BG_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="godpick-t1-truth-v188")
_BG_LOCK = threading.Lock()
_BG_RUNNING = False

_PERMANENT_RESTORE_LOCK = threading.Lock()
_PERMANENT_RESTORE_LAST_ATTEMPT: dict[str, float] = {}
_PERMANENT_RESTORE_CACHE: dict[str, Any] = {}
_PERMANENT_RESTORE_COOLDOWN_SECONDS = 60.0


def _payload_has_learning(path_name: str, payload: Any) -> bool:
    if path_name == TRUTH_FILE:
        return bool(_rows(payload))
    if path_name == CALIBRATION_FILE and isinstance(payload, dict):
        return bool(int(_f(payload.get("eligible_samples"), 0) or 0) > 0 or payload.get("bins"))
    return bool(payload)


def _read_json_authority(path_name: str, default: Any) -> Any:
    """Read local learning authority and restore permanent copy when local is empty.

    V188/H36 persisted truth remotely but only read the ephemeral local JSON. A
    Streamlit redeploy could therefore reset AI learning to zero until another
    expensive replay happened. H38 restores GitHub/Firestore/local elected
    authority when the local learning payload is absent/empty, throttled to avoid
    rerun-time network loops.
    """
    local = _read_json(path_name, default)
    if _payload_has_learning(path_name, local):
        return local
    cached = _PERMANENT_RESTORE_CACHE.get(path_name)
    if _payload_has_learning(path_name, cached):
        return cached
    now = time.monotonic()
    with _PERMANENT_RESTORE_LOCK:
        last = _PERMANENT_RESTORE_LAST_ATTEMPT.get(path_name, 0.0)
        if now - last < _PERMANENT_RESTORE_COOLDOWN_SECONDS:
            return local
        _PERMANENT_RESTORE_LAST_ATTEMPT[path_name] = now
    try:
        from godpick_persistence_service import load_named_json_permanent
        restored, _messages = load_named_json_permanent(path_name, default)
        if _payload_has_learning(path_name, restored):
            _PERMANENT_RESTORE_CACHE[path_name] = restored
            return restored
        return local
    except Exception:
        return local


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


def _original_recommendation_date(row: dict[str, Any]) -> str:
    return _date(row.get("原始推薦日期") or row.get("推薦日期") or row.get("建立日期") or row.get("建立時間") or row.get("K線最後交易日"))


def _recommendation_date(row: dict[str, Any]) -> str:
    """Return the daily decision cohort date, not merely first-seen date.

    H9 rows use the explicit batch/run date.  For legacy Page07 rows created by
    the old builder, ``推薦日期`` could accidentally retain the candidate's
    first-seen date while ``建立時間`` still recorded the real scan/save date.
    A tightly bounded (1-3 day) Page07-only repair lets historical T+1 truth
    recover those daily cohorts without rewriting the record authority itself.
    """
    explicit = _date(row.get("推薦批次日期") or row.get("run_date"))
    if explicit:
        return explicit

    legacy = _date(row.get("推薦日期"))
    created = _date(row.get("建立時間") or row.get("建立日期"))
    source = _s(row.get("紀錄來源") or row.get("推薦執行來源")).lower()
    auto_flag = _s(row.get("自動記錄")).lower()
    page07_like = ("07" in source) or ("股神推薦" in source) or auto_flag in {"是", "true", "1", "yes", "y"}
    if legacy and created and page07_like:
        try:
            lag_days = (date.fromisoformat(created) - date.fromisoformat(legacy)).days
        except Exception:
            lag_days = 0
        if 1 <= lag_days <= 3:
            return created

    return legacy or created or _date(row.get("K線最後交易日"))


def _run_id(row: dict[str, Any]) -> str:
    return _s(row.get("推薦執行ID") or row.get("run_id") or row.get("scan_run_id"))


def _business_key(row: dict[str, Any]) -> str:
    code = "".join(ch for ch in _s(row.get("股票代號") or row.get("代號")) if ch.isdigit())[:4]
    rec = _recommendation_date(row)
    role = _s(row.get("正式推薦分區") or row.get("推薦角色") or row.get("盤中雷達優先級") or row.get("SuperAI進場狀態"))
    return f"{rec}|{code}|{role}"


def _cohort_key(row: dict[str, Any]) -> str:
    # Audit identity keeps role/provenance, so the truth table can still show how
    # the same stock was classified by different decision layers on the same day.
    return _business_key(row)


def _performance_key(row: dict[str, Any]) -> str:
    """One economic outcome per recommendation-date + stock.

    A single stock/day can legitimately have multiple audit roles (A-, radar,
    high-risk radar), but its next-session price path only happened once.  Using
    role in the learning key double-counts the same realized outcome and can
    materially inflate/deflate win rate and return statistics.
    """
    code = "".join(ch for ch in _s(row.get("股票代號") or row.get("代號")) if ch.isdigit())[:4]
    rec = _recommendation_date(row)
    return f"{rec}|{code}" if rec and code else ""


def _performance_role_priority(row: dict[str, Any]) -> int:
    role = "｜".join(_s(row.get(k)) for k in [
        "正式推薦分區", "推薦角色", "盤中雷達優先級", "SuperAI進場狀態"
    ] if _s(row.get(k))).lower()
    if "正式下週主推薦" in role or ("正式" in role and "準主" not in role):
        return 500
    if "a-" in role or "準主" in role:
        return 400
    if "h34" in role:
        return 350
    if "盤中" in role or "核心雷達" in role:
        return 300
    if "高風險" in role:
        return 100
    return 200


def dedupe_performance_truth_rows(data: Any) -> list[dict[str, Any]]:
    """Return canonical rows for learning/summary without deleting audit rows."""
    rows = _rows(data)
    best: dict[str, dict[str, Any]] = {}
    for pos, row in enumerate(rows):
        key = _performance_key(row) or f"__unkeyed__{pos}"
        current = best.get(key)
        score = (
            _performance_role_priority(row),
            int(bool(row.get("是否納入可執行績效"))),
            int(_f(row.get("SuperAI校準後上漲機率%")) is not None or _f(row.get("SuperAI原始上漲機率%")) is not None),
            _s(row.get("updated_at")),
        )
        if current is None:
            best[key] = row
            continue
        current_score = (
            _performance_role_priority(current),
            int(bool(current.get("是否納入可執行績效"))),
            int(_f(current.get("SuperAI校準後上漲機率%")) is not None or _f(current.get("SuperAI原始上漲機率%")) is not None),
            _s(current.get("updated_at")),
        )
        if score > current_score:
            best[key] = row
    return list(best.values())


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


def _h59_tier_from_original(original: dict[str, Any]) -> str:
    """Best-effort immutable decision cohort for T+1 learning.

    New H59 records should persist H59唯一決策 directly. Older H57/H58 records
    are mapped from the immutable H56/H57 snapshot without changing trade truth.
    """
    direct = _s(original.get("H60唯一決策") or original.get("H59唯一決策") or original.get("H58唯一決策"))
    if direct:
        return direct
    auth = _s(original.get("H56上游權威層級") or original.get("H56上游權威"))
    h56 = _s(original.get("H56最終參考層級") or original.get("H56盤前狀態"))
    elite = _s(original.get("H57精選雷達層級") or original.get("H57頂級前兆"))
    phase = _s(original.get("H57前兆階段"))
    if auth == "FORMAL":
        if h56.startswith("X2"):
            return "X1｜正式候選暫停"
        if h56.startswith("A1"):
            return "A1｜可執行"
        return "A0｜Formal盤前待確認"
    if elite.startswith("E1"):
        return "E1｜頂級發動前兆"
    if phase.startswith("PI3"):
        return "PI3｜高品質前兆"
    if h56.startswith("P0") and "AUTHORITY-CAPPED" in h56:
        return "P0｜權威限制等待"
    if h56.startswith("P1"):
        return "P1｜第一優先等待"
    if h56.startswith(("P0", "P2")):
        return "P2｜高品質等待"
    return ""


def _selection_cohort_metrics(rows: list[dict[str, Any]], predicate: Callable[[dict[str, Any]], bool], prefix: str) -> dict[str, Any]:
    cohort = [r for r in rows if predicate(r)]
    rets = [_f(r.get("隔日候選漲跌%")) for r in cohort]
    rets = [x for x in rets if x is not None]
    alpha = [_f(r.get("Selection Alpha%")) for r in cohort]
    alpha = [x for x in alpha if x is not None]
    return {
        f"{prefix}成熟樣本": len(cohort),
        f"{prefix}正報酬率%": round(sum(1 for x in rets if x > 0) / len(rets) * 100.0, 2) if rets else None,
        f"{prefix}平均1日報酬%": round(sum(rets) / len(rets), 4) if rets else None,
        f"{prefix}平均SelectionAlpha%": round(sum(alpha) / len(alpha), 4) if alpha else None,
    }


def build_h57_h60_learning_summary(rows: Any) -> dict[str, Any]:
    """Selection-quality telemetry for H57/H59/H60 research cohorts.

    H60 Main-rise/Snowball/T3 metrics are *selection* metrics only. A research
    label never creates an executable trade; Entry/Risk truth remains governed
    by 是否納入可執行績效.
    """
    items = [r for r in _rows(rows) if isinstance(r, dict) and bool(r.get("T1成熟"))]
    out: dict[str, Any] = {}
    out.update(_selection_cohort_metrics(items, lambda r: _s(r.get("H57精選雷達層級")).startswith("E1"), "H57_E1"))
    out.update(_selection_cohort_metrics(items, lambda r: _s(r.get("H57前兆階段")).startswith("PI3"), "H57_PI3"))
    out.update(_selection_cohort_metrics(items, lambda r: _s(r.get("H59唯一決策") or r.get("H60唯一決策")).startswith("A1"), "H59_A1"))
    out.update(_selection_cohort_metrics(items, lambda r: _s(r.get("H59唯一決策") or r.get("H60唯一決策")).startswith("A0"), "H59_A0"))
    out.update(_selection_cohort_metrics(items, lambda r: _s(r.get("H60主升階段")).startswith("MR1"), "H60_MR1"))
    out.update(_selection_cohort_metrics(items, lambda r: _s(r.get("H60雪球股層級")).startswith("SB1"), "H60_SB1"))
    out.update(_selection_cohort_metrics(items, lambda r: _s(r.get("H60三因子層級")).startswith("T3"), "H60_T3"))
    return out


def build_h57_h59_learning_summary(rows: Any) -> dict[str, Any]:
    """Backward-compatible alias for H60 telemetry."""
    return build_h57_h60_learning_summary(rows)


def _truth_from_updated(original: dict[str, Any], updated: dict[str, Any], quote: dict[str, Any], benchmark_ret: float | None) -> dict[str, Any]:
    rec = _recommendation_date(original)
    hist = quote.get("history") or []
    next_session = _next_row(hist, rec) or {}
    cand_ret = _f(updated.get("隔日候選漲跌%") if _s(updated.get("隔日候選漲跌%")) else updated.get("推薦後1日%"))
    selection_alpha = round(cand_ret - benchmark_ret, 4) if cand_ret is not None and benchmark_ret is not None else None
    raw_prob = _f(original.get("SuperAI隔日上漲機率%"))
    cal_prob = _f(original.get("SuperAI校準後隔日上漲機率%"), raw_prob)
    code = _s(original.get("股票代號") or original.get("代號"))

    # V191-H32：把發布當下的報酬預測與後續真實績效放進同一筆不可變真相。
    _h32_pred1 = _f(original.get("H32隔日預估漲跌幅%"))
    _h32_low1 = _f(original.get("H32隔日90%區間下緣%"))
    _h32_high1 = _f(original.get("H32隔日90%區間上緣%"))
    _h32_actual = {
        1: cand_ret,
        5: _f(updated.get("推薦後5日%")),
        10: _f(updated.get("推薦後10日%")),
        20: _f(updated.get("推薦後20日%")),
    }
    def _h32_inside(actual, low, high):
        if actual is None or low is None or high is None:
            return None
        return bool(min(float(low), float(high)) <= float(actual) <= max(float(low), float(high)))
    def _h32_direction_hit(actual, pred):
        if actual is None or pred is None:
            return None
        if abs(float(actual)) < 1e-12 and abs(float(pred)) < 0.15:
            return True
        return bool((float(actual) > 0) == (float(pred) > 0))

    return {
        "version": TRUTH_VERSION,
        "business_key": _business_key(original),
        "cohort_key": _cohort_key(original),
        "推薦日期": rec,
        "推薦批次日期": rec,
        "原始推薦日期": _original_recommendation_date(original),
        "推薦執行ID": _run_id(original),
        "推薦執行來源": _s(original.get("推薦執行來源") or original.get("execution_owner")),
        "推薦觸發方式": _s(original.get("推薦觸發方式") or original.get("execution_trigger")),
        "推薦執行版本": _s(original.get("推薦執行版本") or original.get("本輪推薦版本")),
        "股票代號": code,
        "股票名稱": _s(original.get("股票名稱") or original.get("名稱")),
        "市場別": _normalize_market(original.get("市場別") or original.get("市場")),
        "類別": _s(original.get("類別") or original.get("產業") or original.get("族群名稱")) or "未分類",
        "推薦角色": _s(original.get("正式推薦分區") or original.get("推薦角色") or original.get("盤中雷達優先級")),
        "SuperAI原始上漲機率%": raw_prob,
        "SuperAI校準後上漲機率%": cal_prob,
        "SuperAI Alpha等級": _s(original.get("SuperAI Alpha等級")),
        "SuperAI Trade等級": _s(original.get("SuperAI Trade等級")),
        "H32隔日預估漲跌幅%": _h32_pred1,
        "H32隔日90%區間下緣%": _h32_low1,
        "H32隔日90%區間上緣%": _h32_high1,
        "H32隔日方向預測命中": _h32_direction_hit(_h32_actual[1], _h32_pred1),
        "H32隔日90%區間命中": _h32_inside(_h32_actual[1], _h32_low1, _h32_high1),
        "H32_5日預估報酬%": _f(original.get("H32_5日預估報酬%")),
        "H32_5日90%區間下緣%": _f(original.get("H32_5日90%區間下緣%")),
        "H32_5日90%區間上緣%": _f(original.get("H32_5日90%區間上緣%")),
        "H32_10日預估報酬%": _f(original.get("H32_10日預估報酬%")),
        "H32_10日90%區間下緣%": _f(original.get("H32_10日90%區間下緣%")),
        "H32_10日90%區間上緣%": _f(original.get("H32_10日90%區間上緣%")),
        "H32_20日預估報酬%": _f(original.get("H32_20日預估報酬%")),
        "H32_20日90%區間下緣%": _f(original.get("H32_20日90%區間下緣%")),
        "H32_20日90%區間上緣%": _f(original.get("H32_20日90%區間上緣%")),
        "H32實際5日報酬%": _h32_actual[5],
        "H32實際10日報酬%": _h32_actual[10],
        "H32實際20日報酬%": _h32_actual[20],
        "H32_5日90%區間命中": _h32_inside(_h32_actual[5], _f(original.get("H32_5日90%區間下緣%")), _f(original.get("H32_5日90%區間上緣%"))),
        "H32_10日90%區間命中": _h32_inside(_h32_actual[10], _f(original.get("H32_10日90%區間下緣%")), _f(original.get("H32_10日90%區間上緣%"))),
        "H32_20日90%區間命中": _h32_inside(_h32_actual[20], _f(original.get("H32_20日90%區間下緣%")), _f(original.get("H32_20日90%區間上緣%"))),
        "H32預測版本": _s(original.get("H32預測版本")),
        "V188交易許可": _s(original.get("V188交易許可")),
        "V188執行RR": _f(original.get("路徑風險報酬比") or original.get("SuperAI執行風報比") or original.get("風險報酬比") or original.get("實戰風險報酬比")),
        "H49上漲潛力分": _f(original.get("H49上漲潛力分")),
        "H49潛力等級": _s(original.get("H49潛力等級")),
        "H49潛力階段": _s(original.get("H49潛力階段")),
        "H49可執行分": _f(original.get("H49可執行分")),
        "H49交易決策": _s(original.get("H49交易決策")),
        "H49版本": _s(original.get("H49版本")),
        "H50族群生命週期": _s(original.get("H50族群生命週期")),
        "H50族群可買主流分": _f(original.get("H50族群可買主流分")),
        "H50波段機會階段": _s(original.get("H50波段機會階段")),
        "H50主流購買優先分": _f(original.get("H50主流購買優先分")),
        "H50主流購買狀態": _s(original.get("H50主流購買狀態")),
        "H50推薦優先分": _f(original.get("H50推薦優先分")),
        "H50推薦等級": _s(original.get("H50推薦等級")),
        "H50推薦決策": _s(original.get("H50推薦決策")),
        "H50主流版本": _s(original.get("H50主流版本")),
        "H50推薦版本": _s(original.get("H50推薦版本")),
        "H50版本": _s(original.get("H50版本")),
        "H51專業參考分": _f(original.get("H51專業參考分")),
        "H51可執行分": _f(original.get("H51可執行分")),
        "H51市場地位": _s(original.get("H51市場地位")),
        "H51交易許可": _s(original.get("H51交易許可")),
        "H51推薦等級": _s(original.get("H51推薦等級")),
        "H51族群主線分": _f(original.get("H51族群主線分")),
        "H51個股領漲品質分": _f(original.get("H51個股領漲品質分")),
        "H51Pivot起漲分": _f(original.get("H51Pivot起漲分")),
        "H51路徑RR": _f(original.get("H51路徑RR")),
        "H51版本": _s(original.get("H51版本")),
        "H56上游權威層級": _s(original.get("H56上游權威層級") or original.get("H56上游權威")),
        "H56隔夜證據狀態": _s(original.get("H56隔夜證據狀態")),
        "H56最終參考層級": _s(original.get("H56最終參考層級") or original.get("H56盤前狀態")),
        "H56T1確認分": _f(original.get("H56T1確認分")),
        "H57精選雷達層級": _s(original.get("H57精選雷達層級") or original.get("H57頂級前兆")),
        "H57前兆階段": _s(original.get("H57前兆階段")),
        "H57飆股發動前兆分": _f(original.get("H57飆股發動前兆分") or original.get("H57前兆分")),
        "H57全市場前兆百分位%": _f(original.get("H57全市場前兆百分位%") or original.get("H57全市場百分位%")),
        "H57資金加速度分": _f(original.get("H57資金加速度分")),
        "H57主流形成前兆分": _f(original.get("H57主流形成前兆分")),
        "H58唯一決策": _s(original.get("H58唯一決策")),
        "H59唯一決策": _h59_tier_from_original(original),
        "H59是否可買": _s(original.get("H59是否可買") or original.get("H58是否可買")),
        "H59版本": _s(original.get("H59版本")) or "v191_h59_formal_recall_learning_truth_20260903",
        "H60唯一決策": _s(original.get("H60唯一決策") or original.get("H59唯一決策") or original.get("H58唯一決策")),
        "H60主升段分": _f(original.get("H60主升段分")),
        "H60主升階段": _s(original.get("H60主升階段")),
        "H60鎖碼來源": _s(original.get("H60鎖碼來源")),
        "H60大戶資料日期": _s(original.get("H60大戶資料日期")),
        "H60千張大戶持股比%": _f(original.get("H60千張大戶持股比%")),
        "H60千張大戶週變化pp": _f(original.get("H60千張大戶週變化pp")),
        "H60大戶鎖碼真相分": _f(original.get("H60大戶鎖碼真相分")),
        "H60大戶鎖碼層級": _s(original.get("H60大戶鎖碼層級")),
        "H60雪球複利分": _f(original.get("H60雪球複利分")),
        "H60雪球股層級": _s(original.get("H60雪球股層級")),
        "H60三因子共振分": _f(original.get("H60三因子共振分")),
        "H60三因子層級": _s(original.get("H60三因子層級")),
        "H60版本": _s(original.get("H60版本")) or "v191_h60_mainrise_holder_snowball_truth_20260904",
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
        "理論進場參考價": _f(updated.get("理論進場參考價")),
        "執行價來源": _s(updated.get("執行價來源")),
        "執行價可成交驗證": bool(updated.get("執行價可成交驗證")),
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
        "是否觸發後失守": bool("觸發後失守" in _s(updated.get("進場觸發狀態"))),
        "T1成熟": bool(_date(next_session.get("日期") or next_session.get("date"))),
        "績效行情日期": _s(updated.get("績效行情日期")),
        "績效資料來源": _s(updated.get("績效資料來源") or quote.get("source")),
        "updated_at": _now(),
    }


def build_probability_calibration(truth_rows: Any) -> dict[str, Any]:
    audit_rows = _rows(truth_rows)
    rows = dedupe_performance_truth_rows(audit_rows)
    duplicate_rows_excluded = max(0, len(audit_rows) - len(rows))
    samples = []
    executable = []
    for r in rows:
        if not bool(r.get("T1成熟")):
            continue
        # Executable performance is an independent truth metric.  It must not be
        # filtered by whether this older row happened to carry a probability
        # forecast; otherwise win-rate/sample-count silently shrink to the tiny
        # calibration subset and can look much better than real execution.
        if bool(r.get("是否納入可執行績效")):
            er = _f(r.get("可執行交易1日%") if _s(r.get("可執行交易1日%")) else r.get("觸發當日收盤績效%"))
            if er is not None:
                executable.append(er)
        prob = _f(r.get("SuperAI校準後上漲機率%") if _s(r.get("SuperAI校準後上漲機率%")) else r.get("SuperAI原始上漲機率%"))
        ret = _f(r.get("隔日候選漲跌%"))
        if prob is None or ret is None:
            continue
        samples.append((prob, 1.0 if ret > 0 else 0.0, ret, _f(r.get("Selection Alpha%"))))
    bins = []
    for low in range(0, 100, 5):
        high = low + 5
        sub = [x for x in samples if low <= x[0] < high or (high == 100 and x[0] == 100)]
        if not sub:
            continue
        n = len(sub)
        wins = sum(x[1] for x in sub)
        mean_p = sum(x[0] for x in sub) / n
        actual = wins / n * 100.0
        # H36: Beta(10,10) = 20 pseudo-samples centred at 50%.  This does not
        # pretend a tiny bin is statistically mature; it only provides a stable
        # conservative target for bounded early calibration.
        posterior = (wins + 10.0) / (n + 20.0) * 100.0
        brier = sum(((x[0] / 100.0) - x[1]) ** 2 for x in sub) / n
        alpha_vals = [x[3] for x in sub if x[3] is not None]
        bins.append({
            "low": low, "high": high, "n": n,
            "mean_predicted_pct": round(mean_p, 2), "actual_up_rate_pct": round(actual, 2),
            "posterior_up_rate_pct": round(posterior, 2),
            "calibration_state": "EARLY" if n < 30 else "SUPPORTED" if n < 100 else "MATURE",
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
        "naive_50_brier_score": 0.25 if n else None,
        "brier_skill_vs_50_pct": round((1.0 - global_brier / 0.25) * 100.0, 2) if global_brier is not None else None,
        "audit_rows_seen": len(audit_rows),
        "unique_performance_rows": len(rows),
        "duplicate_performance_rows_excluded": duplicate_rows_excluded,
        "executable_samples": len(executable),
        "executable_win_rate_pct": round(sum(1 for x in executable if x > 0) / len(executable) * 100.0, 2) if executable else None,
        "avg_executable_ret_pct": round(sum(executable) / len(executable), 4) if executable else None,
        "bins": bins,
        "policy": "H36: audit rows preserve roles, but performance/calibration counts one realized outcome per date+stock; small samples use Beta(10,10) posterior only as a bounded conservative calibration target; untriggered signals are never wins/losses",
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

    # H9 merges by daily cohort, not by first-seen identity or run invocation.
    # Newer truth replaces the same date/code/role only; different dates remain
    # separate learning samples, while repeated reruns on one date do not inflate
    # sample counts.  Legacy rows without cohort metadata are mapped compatibly.
    old_payload = _read_json(TRUTH_FILE, {})
    old_rows = _rows(old_payload)
    merged = {(_s(r.get("cohort_key")) or _cohort_key(r)): r for r in old_rows if isinstance(r, dict)}
    for r in processed:
        merged[_s(r.get("cohort_key")) or _cohort_key(r)] = r
    truth_rows = list(merged.values())
    truth_rows.sort(key=lambda r: (_s(r.get("推薦日期")), _s(r.get("股票代號")), _s(r.get("推薦角色"))))
    _sector_alpha(truth_rows)

    # Preserve every audit role, but explicitly mark which row is allowed to
    # contribute to economic performance/learning for each date+stock outcome.
    canonical = {_performance_key(r): r for r in dedupe_performance_truth_rows(truth_rows)}
    for r in truth_rows:
        key = _performance_key(r)
        chosen = canonical.get(key, r)
        r["performance_key"] = key
        r["績效主角色"] = _s(chosen.get("推薦角色") or chosen.get("正式推薦分區"))
        r["績效唯一樣本"] = "是" if _business_key(r) == _business_key(chosen) else "否"

    calibration = build_probability_calibration(truth_rows)
    performance_rows = dedupe_performance_truth_rows(truth_rows)
    matured = [r for r in performance_rows if bool(r.get("T1成熟"))]
    executable = [r for r in matured if bool(r.get("是否納入可執行績效"))]
    selection_alpha = [_f(r.get("Selection Alpha%")) for r in matured]
    selection_alpha = [x for x in selection_alpha if x is not None]
    h49_high = [r for r in matured if _s(r.get("H49潛力等級")).startswith(("P1", "P2"))]
    h49_rets = [_f(r.get("隔日候選漲跌%")) for r in h49_high]
    h49_rets = [x for x in h49_rets if x is not None]
    h49_alpha = [_f(r.get("Selection Alpha%")) for r in h49_high]
    h49_alpha = [x for x in h49_alpha if x is not None]
    h50_focus = [r for r in matured if _s(r.get("H50推薦決策")).startswith(("R-READY", "R-PREP"))]
    h50_ready = [r for r in matured if _s(r.get("H50推薦決策")).startswith("R-READY")]
    h50_rets = [_f(r.get("隔日候選漲跌%")) for r in h50_focus]
    h50_rets = [x for x in h50_rets if x is not None]
    h50_alpha = [_f(r.get("Selection Alpha%")) for r in h50_focus]
    h50_alpha = [x for x in h50_alpha if x is not None]
    h51_focus = [r for r in matured if _s(r.get("H51交易許可")).startswith(("BUY-READY", "SETUP-PREP"))]
    h51_ready = [r for r in matured if _s(r.get("H51交易許可")).startswith("BUY-READY")]
    h51_rets = [_f(r.get("隔日候選漲跌%")) for r in h51_focus]
    h51_rets = [x for x in h51_rets if x is not None]
    h51_alpha = [_f(r.get("Selection Alpha%")) for r in h51_focus]
    h51_alpha = [x for x in h51_alpha if x is not None]
    h57_h59_learning = build_h57_h60_learning_summary(matured)
    payload = {
        "version": TRUTH_VERSION,
        "updated_at": _now(),
        "records": truth_rows,
        "summary": {
            "total_truth_rows": len(truth_rows), "processed_this_run": len(processed), "failed_this_run": len(failures),
            "unique_performance_rows": len(performance_rows),
            "duplicate_performance_rows_excluded": max(0, len(truth_rows) - len(performance_rows)),
            "matured_t1_samples": len(matured), "executable_samples": len(executable),
            "trigger_rate_pct": round(len(executable) / len(matured) * 100.0, 2) if matured else None,
            "executable_win_rate_pct": calibration.get("executable_win_rate_pct"),
            "avg_selection_alpha_pct": round(sum(selection_alpha) / len(selection_alpha), 4) if selection_alpha else None,
            "H49高潛力成熟樣本": len(h49_high),
            "H49高潛力正報酬率%": round(sum(1 for x in h49_rets if x > 0) / len(h49_rets) * 100.0, 2) if h49_rets else None,
            "H49高潛力平均1日報酬%": round(sum(h49_rets) / len(h49_rets), 4) if h49_rets else None,
            "H49高潛力平均SelectionAlpha%": round(sum(h49_alpha) / len(h49_alpha), 4) if h49_alpha else None,
            "H50主流推薦成熟樣本": len(h50_focus),
            "H50_R_READY成熟樣本": len(h50_ready),
            "H50主流推薦正報酬率%": round(sum(1 for x in h50_rets if x > 0) / len(h50_rets) * 100.0, 2) if h50_rets else None,
            "H50主流推薦平均1日報酬%": round(sum(h50_rets) / len(h50_rets), 4) if h50_rets else None,
            "H50主流推薦平均SelectionAlpha%": round(sum(h50_alpha) / len(h50_alpha), 4) if h50_alpha else None,
            "H51專業主線成熟樣本": len(h51_focus),
            "H51_BUY_READY成熟樣本": len(h51_ready),
            "H51專業主線正報酬率%": round(sum(1 for x in h51_rets if x > 0) / len(h51_rets) * 100.0, 2) if h51_rets else None,
            "H51專業主線平均1日報酬%": round(sum(h51_rets) / len(h51_rets), 4) if h51_rets else None,
            "H51專業主線平均SelectionAlpha%": round(sum(h51_alpha) / len(h51_alpha), 4) if h51_alpha else None,
            **h57_h59_learning,
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
    rows = _rows(_read_json_authority(TRUTH_FILE, {}))
    rows.sort(key=lambda r: (_s(r.get("推薦日期")), _s(r.get("股票代號"))), reverse=True)
    return rows[: int(limit)] if limit else rows


def load_t1_truth_summary() -> dict[str, Any]:
    payload = _read_json_authority(TRUTH_FILE, {})
    summary = payload.get("summary") if isinstance(payload, dict) else {}
    calibration = _read_json_authority(CALIBRATION_FILE, {})
    return {
        **(summary if isinstance(summary, dict) else {}),
        "updated_at": _s(payload.get("updated_at") if isinstance(payload, dict) else ""),
        "calibration_samples": int(_f(calibration.get("eligible_samples") if isinstance(calibration, dict) else 0, 0) or 0),
        "brier_score": calibration.get("brier_score") if isinstance(calibration, dict) else None,
        "executable_win_rate_pct": calibration.get("executable_win_rate_pct") if isinstance(calibration, dict) else None,
    }


def load_probability_calibration() -> dict[str, Any]:
    data = _read_json_authority(CALIBRATION_FILE, {})
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
    "TRUTH_VERSION", "TRUTH_FILE", "CALIBRATION_FILE", "build_h57_h60_learning_summary", "build_h57_h59_learning_summary",
    "refresh_t1_trade_truth", "refresh_t1_truth_async", "load_t1_truth_rows", "load_t1_truth_summary",
    "build_probability_calibration", "load_probability_calibration", "dedupe_performance_truth_rows",
]
