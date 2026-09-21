# -*- coding: utf-8 -*-
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo
import json
import math
import time
import xml.etree.ElementTree as ET

VERSION = "v191_h87_autofresh_transparent_progress_smart_refresh_20260921"
BASE_DIR = Path(__file__).resolve().parent
STATUS_FILE = "godpick_h83_autofresh_status.json"
PROGRESS_FILE = "godpick_h87_autofresh_progress.json"
NEWS_FILE = "godpick_h83_news_cache.json"
TZ = ZoneInfo("Asia/Taipei")

# Conservative ETA seeds. H87 replaces these with the most recent successful
# measured duration when available. They are estimates only, never truth gates.
DEFAULT_STAGE_SECONDS = {
    "audit": 2.0,
    "stock_master": 75.0,
    "macro_full": 20.0,
    "market_repair": 2.0,
    "official_factors": 50.0,
    "super_ai_context": 15.0,
    "watchlist_runtime": 2.0,
    "h83_news": 12.0,
    "performance_feedback": 25.0,
    "h82_adaptive_learning": 20.0,
    "final_validation": 3.0,
}

STAGE_LABELS = {
    "audit": "檢查目前資料狀態",
    "stock_master": "股票主檔更新",
    "macro_full": "大盤／外盤／期貨／法人橋接",
    "market_repair": "市場日期與異常值治理",
    "official_factors": "官方因子更新與交易日驗證",
    "super_ai_context": "融資券／期貨／PCR／ETF 情境",
    "watchlist_runtime": "自選股 Runtime 同步",
    "h83_news": "新聞事件快取",
    "performance_feedback": "推薦績效與學習摘要",
    "h82_adaptive_learning": "H82 成熟樣本學習重建",
    "final_validation": "最終 Formal 資料驗證",
}


def _now() -> datetime:
    return datetime.now(TZ)


def _txt(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _num(v: Any, d: float | None = None) -> float | None:
    try:
        if isinstance(v, str):
            v = v.replace(",", "").replace("%", "").replace("％", "").strip()
        x = float(v)
        return x if math.isfinite(x) else d
    except Exception:
        return d


def _read(name: str, default: Any) -> Any:
    try:
        return json.loads((BASE_DIR / name).read_text(encoding="utf-8-sig"))
    except Exception:
        return default


def _write(name: str, payload: Any) -> None:
    p = BASE_DIR / name
    p.parent.mkdir(parents=True, exist_ok=True)
    t = p.with_suffix(p.suffix + ".tmp_h87")
    t.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    t.replace(p)


def _persist(name: str, payload: Any, doc: str) -> bool:
    """Persist business data synchronously when it is authority-bearing."""
    _write(name, payload)
    try:
        from godpick_persistence_service import save_named_json_permanent

        r = save_named_json_permanent(name, payload, firestore_doc=doc)
        return bool(getattr(r, "permanent_ok", False))
    except Exception:
        # Local copy is still retained. Caller decides whether remote confirmation
        # is a formal requirement.
        return False


def _persist_status_fast(payload: dict[str, Any]) -> None:
    """Status/progress is diagnostic state; never block UI on remote round-trip."""
    _write(STATUS_FILE, payload)
    try:
        from godpick_durability_service import persist_json_async

        persist_json_async(
            STATUS_FILE,
            payload,
            github_path=STATUS_FILE,
            firestore_doc="godpick_h83_autofresh_status",
            reason="H87 autofresh diagnostic status",
        )
    except Exception:
        pass


def _date(v: Any) -> date | None:
    try:
        return datetime.fromisoformat(_txt(v)[:10]).date()
    except Exception:
        return None


def previous_weekday(d: date | None) -> date | None:
    if d is None:
        return None
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def business_lag(later: date | None, earlier: date | None) -> int | None:
    if not later or not earlier:
        return None
    if earlier > later:
        return -1
    n = 0
    cur = earlier
    while cur < later:
        cur += timedelta(days=1)
        if cur.weekday() < 5:
            n += 1
    return n


def _age(name: str) -> float | None:
    try:
        return max(0.0, (time.time() - (BASE_DIR / name).stat().st_mtime) / 60.0)
    except Exception:
        return None


def _rows(name: str) -> int:
    d = _read(name, [])
    if isinstance(d, list):
        return len(d)
    if isinstance(d, dict):
        for k in ["records", "items", "rows", "data"]:
            if isinstance(d.get(k), list):
                return len(d[k])
    return 0


def _official_date(d: Any) -> date | None:
    if not isinstance(d, dict):
        return None
    for v in [
        d.get("data_date"),
        (d.get("meta") or {}).get("data_date") if isinstance(d.get("meta"), dict) else None,
    ]:
        q = previous_weekday(_date(v))
        if q:
            return q
    vals: list[date] = []
    for r in d.get("records", []) if isinstance(d.get("records"), list) else []:
        if not isinstance(r, dict):
            continue
        for k in ["官方資料日期", "官方因子資料日期", "法人資料日期"]:
            q = previous_weekday(_date(r.get(k)))
            if q:
                vals.append(q)
                break
    return max(vals) if vals else None


def repair_market_snapshot_payload(payload: Any, max_abs_change_pct: float = 15) -> tuple[dict[str, Any], dict[str, Any]]:
    out = dict(payload or {})
    diag: dict[str, Any] = {"changed": False, "repairs": [], "quarantined": []}
    orig: dict[str, Any] = {}

    for k in ["twse_data_date", "otc_data_date", "futures_data_date", "data_date", "market_date"]:
        a = _date(out.get(k))
        b = previous_weekday(a)
        if a and b and a != b:
            orig[k] = out.get(k)
            out[k] = b.isoformat()
            diag["repairs"].append(f"{k}:{a}→{b}")
            diag["changed"] = True

    tw = previous_weekday(_date(out.get("twse_data_date")))
    ot = previous_weekday(_date(out.get("otc_data_date")))
    if tw and out.get("market_date") != tw.isoformat():
        out["market_date"] = out["data_date"] = tw.isoformat()
        diag["changed"] = True

    bad: list[str] = []
    for label, k in [
        ("TWSE", "twse_change_pct"),
        ("OTC", "otc_change_pct"),
        ("FUTURES", "futures_change_pct"),
    ]:
        x = _num(out.get(k))
        if x is not None and abs(x) > max_abs_change_pct:
            bad.append(label)

    if "OTC" in bad:
        for k in ["otc_index", "otc_change", "otc_change_pct"]:
            orig[k] = out.get(k)
            out[k] = None
        diag["quarantined"].append("OTC")
        diag["changed"] = True

        req = dict(out.get("required_by_godpick") or {})
        req["otc_change"] = req["otc_change_pct"] = None
        out["required_by_godpick"] = req

        eff = dict((out.get("next_day_forecast") or {}).get("godpick_effect") or {})
        reason = _txt(eff.get("lockdown_reason") or out.get("next_day_lockdown_reason"))
        twpct = _num(out.get("twse_change_pct"), 0) or 0
        if eff.get("hard_filter") and "櫃買" in reason and twpct > -8:
            eff.update(
                {
                    "hard_filter": False,
                    "mode": "H87-DATA-GUARD｜異常市場值隔離",
                    "score_delta": max(-2, _num(eff.get("score_delta"), -2) or -2),
                    "market_weight_delta": -2,
                    "position_cap_pct": max(30, int(_num(eff.get("position_cap_pct"), 30) or 30)),
                    "lockdown_reason": "H87已隔離櫃買異常值；不以失真數據啟動極端封鎖",
                }
            )
            f = dict(out.get("next_day_forecast") or {})
            f["godpick_effect"] = eff
            out["next_day_forecast"] = f
            out["next_day_effect_mode"] = eff["mode"]
            out["next_day_lockdown_reason"] = eff["lockdown_reason"]
            diag["repairs"].append("false OTC lockdown neutralized")

    out["h83_truth_guard"] = {
        "version": VERSION,
        "checked_at": _now().strftime("%Y-%m-%d %H:%M:%S"),
        "business_date": (tw or ot).isoformat() if (tw or ot) else "",
        "invalid_market_domains": bad,
        "formal_data_valid": "TWSE" not in bad,
    }
    if orig:
        out["h83_original_values"] = orig
    return out, diag


def repair_market_snapshot(settings: dict[str, Any] | None = None, persist: bool = True) -> dict[str, Any]:
    from godpick_h83_autofresh_settings import load_settings_safe

    cfg = settings or load_settings_safe()
    out, diag = repair_market_snapshot_payload(
        _read("market_snapshot.json", {}), cfg["quality"]["max_market_abs_change_pct"]
    )
    if diag["changed"]:
        if persist:
            _persist("market_snapshot.json", out, "market_snapshot")
        else:
            _write("market_snapshot.json", out)
    return diag


def _fetch_news_one(query: str, max_items: int, http_get: Callable[..., Any]) -> tuple[list[dict[str, str]], str]:
    try:
        r = http_get(
            "https://news.google.com/rss/search?q=" + quote_plus(query) + "&hl=zh-TW&gl=TW&ceid=TW:zh-Hant",
            headers={"User-Agent": "Mozilla/5.0 GodPick-H87"},
            timeout=8,
        )
        if getattr(r, "status_code", 0) != 200:
            return [], f"{query}:HTTP {getattr(r, 'status_code', '?')}"
        raw = r.content if getattr(r, "content", None) else str(r.text).encode()
        root = ET.fromstring(raw)
        rows: list[dict[str, str]] = []
        for it in root.findall(".//item")[:max_items]:
            title = _txt(it.findtext("title"))
            if title:
                rows.append(
                    {
                        "query": query,
                        "title": title,
                        "source": _txt(it.findtext("source")),
                        "published_at": _txt(it.findtext("pubDate")),
                        "link": _txt(it.findtext("link")),
                    }
                )
        return rows, ""
    except Exception as e:
        return [], f"{query}:{type(e).__name__}"


def refresh_news_cache(
    settings: dict[str, Any] | None = None,
    http_get: Callable[..., Any] | None = None,
    force: bool = False,
) -> dict[str, Any]:
    from godpick_h83_autofresh_settings import load_settings_safe

    cfg = settings or load_settings_safe()
    age = _age(NEWS_FILE)
    if not force and age is not None and age <= cfg["ttl_minutes"]["news"]:
        return {"ok": True, "skipped": True, "message": "H87新聞快取仍新鮮", "cache": _read(NEWS_FILE, {})}

    import requests

    get = http_get or requests.get
    queries = list(cfg["news"]["queries"])
    max_items = int(cfg["news"]["max_items_per_query"])
    items: list[dict[str, str]] = []
    errors: list[str] = []

    # H87: previous code did up to 8-second HTTP calls serially for every query.
    # Parallel bounded fetch keeps the same sources but makes worst-case wait
    # roughly one timeout window rather than N timeout windows.
    with ThreadPoolExecutor(max_workers=min(4, max(1, len(queries)))) as ex:
        futures = {ex.submit(_fetch_news_one, q, max_items, get): q for q in queries}
        for fut in as_completed(futures):
            rows, err = fut.result()
            items.extend(rows)
            if err:
                errors.append(err)

    seen: set[str] = set()
    unique: list[dict[str, str]] = []
    for x in items:
        if x["title"] not in seen:
            seen.add(x["title"])
            unique.append(x)

    p = {
        "version": VERSION,
        "updated_at": _now().strftime("%Y-%m-%d %H:%M:%S"),
        "source": "Google News RSS",
        "items": unique[:80],
        "errors": errors,
        "status": "ok" if unique else "degraded",
    }
    _persist(NEWS_FILE, p, "godpick_h83_news_cache")
    return {"ok": bool(unique), "message": f"H87新聞更新 {len(unique)} 則", "cache": p}


def get_news_context(row: dict[str, Any] | None = None, max_items: int = 8) -> list[dict[str, Any]]:
    items = (_read(NEWS_FILE, {}) or {}).get("items", [])
    raw = dict(row or {})
    tokens = [
        _txt(raw.get(k))
        for k in ["股票代號", "股票名稱", "類別", "產業", "族群名稱"]
        if len(_txt(raw.get(k))) >= 2
    ]
    matched = [
        x
        for x in items
        if isinstance(x, dict) and any(t in _txt(x.get("title")) for t in tokens)
    ]
    return (matched or items)[:max_items]


def _snapshot(cfg: dict[str, Any]) -> dict[str, Any]:
    m = _read("market_snapshot.json", {})
    o = _read("official_factors_cache.json", {})
    truth = m.get("h83_truth_guard", {}) if isinstance(m, dict) else {}
    md = (
        previous_weekday(_date(m.get("market_date") or m.get("data_date") or m.get("twse_data_date")))
        if isinstance(m, dict)
        else None
    )
    od = _official_date(o)
    lag = business_lag(md, od)
    mr = _rows("stock_master_cache.json")
    orows = _rows("official_factors_cache.json")
    bad = set(truth.get("invalid_market_domains") or [])
    ready = bool(
        mr >= cfg["quality"]["min_stock_master_rows"]
        and md
        and "TWSE" not in bad
        and orows >= cfg["quality"]["min_official_factor_rows"]
        and lag is not None
        and 0 <= lag <= cfg["quality"]["max_official_business_lag"]
    )
    issues: list[str] = []
    if mr < cfg["quality"]["min_stock_master_rows"]:
        issues.append(f"股票主檔僅{mr}筆")
    if not md:
        issues.append("市場業務日期未驗證")
    if "TWSE" in bad:
        issues.append("TWSE市場值異常")
    if bad - {"TWSE"}:
        issues.append("部分市場異常值已隔離")
    if not (
        orows >= cfg["quality"]["min_official_factor_rows"]
        and lag is not None
        and 0 <= lag <= cfg["quality"]["max_official_business_lag"]
    ):
        issues.append(f"官方因子未對齊：market={md} official={od} lag={lag}")

    return {
        "market_date": md.isoformat() if md else "",
        "official_date": od.isoformat() if od else "",
        "official_business_lag": lag,
        "stock_master_rows": mr,
        "official_factor_rows": orows,
        "invalid_market_domains": sorted(bad),
        "formal_ready": ready,
        "issues": issues,
        "ages_minutes": {
            k: _age(v)
            for k, v in {
                "stock_master": "stock_master_cache.json",
                "macro_full": "market_snapshot.json",
                "super_ai_context": "super_ai_market_context.json",
                "watchlist_runtime": "watchlist_runtime_snapshot.json",
                "performance_feedback": "godpick_performance_profile.json",
                "h82_adaptive_learning": "godpick_adaptive_learning_state.json",
                "news": NEWS_FILE,
            }.items()
        },
    }


def _format_seconds(seconds: float | int | None) -> str:
    if seconds is None:
        return "估算中"
    sec = max(0, int(round(float(seconds))))
    if sec < 60:
        return f"約 {sec} 秒"
    m, s = divmod(sec, 60)
    if m < 60:
        return f"約 {m} 分 {s:02d} 秒"
    h, m = divmod(m, 60)
    return f"約 {h} 小時 {m} 分"


def _historical_estimates() -> dict[str, float]:
    out = dict(DEFAULT_STAGE_SECONDS)
    old = load_status()
    for action in old.get("actions", []) if isinstance(old, dict) else []:
        if not isinstance(action, dict):
            continue
        task = _txt(action.get("task"))
        dur = _num(action.get("duration_seconds"))
        if task in out and dur is not None and 0.05 <= dur <= 1800:
            # Blend rather than fully trust one noisy run.
            out[task] = max(0.5, 0.65 * dur + 0.35 * out[task])
    return out


def _emit_progress(
    callback: Callable[[dict[str, Any]], None] | None,
    *,
    stage: str,
    state: str,
    completed_weight: float,
    total_weight: float,
    started_at: float,
    eta_seconds: float | None,
    message: str,
    step_index: int,
    step_count: int,
    detail: dict[str, Any] | None = None,
) -> None:
    percent = 100.0 if total_weight <= 0 else max(0.0, min(100.0, completed_weight / total_weight * 100.0))
    payload = {
        "version": VERSION,
        "updated_at": _now().strftime("%Y-%m-%d %H:%M:%S"),
        "running": state not in {"done", "error"},
        "stage": stage,
        "stage_label": STAGE_LABELS.get(stage, stage),
        "state": state,
        "percent": round(percent, 1),
        "elapsed_seconds": round(max(0.0, time.perf_counter() - started_at), 1),
        "eta_seconds": None if eta_seconds is None else round(max(0.0, eta_seconds), 1),
        "eta_text": _format_seconds(eta_seconds),
        "message": message,
        "step_index": int(step_index),
        "step_count": int(step_count),
        "detail": detail or {},
    }
    # Local-only progress heartbeat is intentionally cheap and recoverable.
    try:
        _write(PROGRESS_FILE, payload)
    except Exception:
        pass
    if callable(callback):
        try:
            callback(payload)
        except Exception:
            pass


def load_progress() -> dict[str, Any]:
    d = _read(PROGRESS_FILE, {})
    return d if isinstance(d, dict) else {}


def run_autofresh_preflight(
    reason: str = "before_recommendation",
    force: bool = False,
    settings: dict[str, Any] | None = None,
    handlers: dict[str, Callable[..., Any]] | None = None,
    allow_network: bool = True,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """H87 transparent, bounded-by-source autofresh orchestrator.

    `force=False` is the normal/recommended path: update only stale/missing
    dependencies. `force=True` is an explicit diagnostic full refresh and can
    take several minutes.

    Progress callback receives deterministic stage, percentage, elapsed time and
    estimated remaining time. It does not alter any recommendation threshold.
    """
    from godpick_h83_autofresh_settings import load_settings_safe

    cfg = settings or load_settings_safe()
    actions: list[dict[str, Any]] = []
    taskmap = handlers
    if taskmap is None:
        try:
            from godpick_auto_update_tasks import TASK_HANDLERS

            taskmap = dict(TASK_HANDLERS or {})
        except Exception:
            taskmap = {}

    estimates = _historical_estimates()
    all_stages = [
        "audit",
        "stock_master",
        "macro_full",
        "market_repair",
        "official_factors",
        "super_ai_context",
        "watchlist_runtime",
        "h83_news",
        "performance_feedback",
        "h82_adaptive_learning",
        "final_validation",
    ]
    total_weight = sum(estimates.get(x, 1.0) for x in all_stages)
    completed_weight = 0.0
    started = time.perf_counter()

    def remaining_eta(current_stage: str | None = None) -> float:
        rem = 0.0
        seen_current = current_stage is None
        for s in all_stages:
            if not seen_current:
                if s == current_stage:
                    seen_current = True
                continue
            if s == current_stage:
                continue
            rem += estimates.get(s, 1.0)
        return rem

    def stage_start(stage: str, index: int, message: str) -> float:
        eta = sum(estimates.get(s, 1.0) for s in all_stages[index - 1 :])
        _emit_progress(
            progress_callback,
            stage=stage,
            state="running",
            completed_weight=completed_weight,
            total_weight=total_weight,
            started_at=started,
            eta_seconds=eta,
            message=message,
            step_index=index,
            step_count=len(all_stages),
        )
        return time.perf_counter()

    def stage_done(stage: str, index: int, t0: float, message: str, *, skipped: bool = False, ok: bool = True, detail: dict[str, Any] | None = None) -> None:
        nonlocal completed_weight
        dur = max(0.0, time.perf_counter() - t0)
        completed_weight += estimates.get(stage, 1.0)
        # Future runs learn actual duration from the action row.
        actions.append(
            {
                "task": stage,
                "ok": bool(ok),
                "skipped": bool(skipped),
                "message": message,
                "duration_seconds": round(dur, 3),
            }
        )
        eta = max(0.0, total_weight - completed_weight)
        _emit_progress(
            progress_callback,
            stage=stage,
            state="skipped" if skipped else ("done" if ok else "error"),
            completed_weight=completed_weight,
            total_weight=total_weight,
            started_at=started,
            eta_seconds=eta,
            message=message,
            step_index=index,
            step_count=len(all_stages),
            detail=detail,
        )

    def run_handler(stage: str, index: int, handler_name: str, cfg_payload: dict[str, Any] | None, should_run: bool, skip_message: str) -> dict[str, Any]:
        t0 = stage_start(stage, index, f"{STAGE_LABELS[stage]}：{'準備更新' if should_run else '檢查中'}")
        if not should_run:
            stage_done(stage, index, t0, skip_message, skipped=True)
            return {"ok": True, "skipped": True, "message": skip_message}
        f = taskmap.get(handler_name)
        if not callable(f):
            msg = "任務未載入"
            stage_done(stage, index, t0, msg, ok=False)
            return {"ok": False, "message": msg}
        try:
            r = f(cfg_payload or {}) or {}
            ok = bool(r.get("ok"))
            msg = _txt(r.get("message")) or ("完成" if ok else "失敗")
            stage_done(stage, index, t0, msg, ok=ok, detail={"handler": handler_name})
            return dict(r)
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            stage_done(stage, index, t0, msg, ok=False, detail={"handler": handler_name})
            return {"ok": False, "message": msg}

    # 1/11 initial audit
    t0 = stage_start("audit", 1, "讀取本機權威快照與 TTL")
    before = _snapshot(cfg)
    stage_done("audit", 1, t0, "初始資料盤點完成", detail=before)

    ages = before["ages_minutes"]
    ttl = cfg["ttl_minutes"]
    rr = cfg["refresh"]

    stock_due = bool(
        allow_network
        and rr["stock_master"]
        and (
            force
            or ages["stock_master"] is None
            or ages["stock_master"] > ttl["stock_master"]
            or before["stock_master_rows"] < cfg["quality"]["min_stock_master_rows"]
        )
    )
    run_handler(
        "stock_master",
        2,
        "stock_master",
        None,
        stock_due,
        "股票主檔仍新鮮且筆數正常，智慧略過",
    )

    macro_due = bool(
        allow_network
        and rr["macro_full"]
        and (
            force
            or ages["macro_full"] is None
            or ages["macro_full"] > ttl["macro_full"]
            or before["invalid_market_domains"]
        )
    )
    run_handler(
        "macro_full",
        3,
        "macro_full",
        None,
        macro_due,
        "大盤資料仍新鮮且無異常值，智慧略過",
    )

    t0 = stage_start("market_repair", 4, "校正週末日期與隔離不合理市場變動")
    repair_diag = repair_market_snapshot(cfg, True)
    stage_done(
        "market_repair",
        4,
        t0,
        "市場語意檢查完成" if not repair_diag.get("changed") else "市場日期／異常值已修復",
        detail=repair_diag,
    )

    mid = _snapshot(cfg)
    official_due = bool(
        allow_network
        and rr["official_factors"]
        and (
            force
            or mid["official_business_lag"] is None
            or mid["official_business_lag"] < 0
            or mid["official_business_lag"] > cfg["quality"]["max_official_business_lag"]
            or mid["official_factor_rows"] < cfg["quality"]["min_official_factor_rows"]
        )
    )
    run_handler(
        "official_factors",
        5,
        "official_factors",
        None,
        official_due,
        "官方因子已對齊允許交易日落差，智慧略過",
    )

    super_due = bool(
        allow_network
        and rr["super_ai_context"]
        and (
            force
            or ages["super_ai_context"] is None
            or ages["super_ai_context"] > ttl["super_ai_context"]
        )
    )
    run_handler(
        "super_ai_context",
        6,
        "super_ai_context",
        {"fetch_etf": True},
        super_due,
        "SuperAI市場情境仍在 TTL 內，智慧略過",
    )

    watch_due = bool(
        allow_network
        and rr["watchlist_runtime"]
        and (
            force
            or ages["watchlist_runtime"] is None
            or ages["watchlist_runtime"] > ttl["watchlist_runtime"]
        )
    )
    run_handler(
        "watchlist_runtime",
        7,
        "watchlist_runtime",
        None,
        watch_due,
        "自選股 Runtime 仍在 TTL 內，智慧略過",
    )

    news_due = bool(
        allow_network
        and cfg["auto_refresh_news"]
        and (force or ages["news"] is None or ages["news"] > ttl["news"])
    )
    t0 = stage_start("h83_news", 8, "檢查新聞快取")
    if news_due:
        nr = refresh_news_cache(cfg, force=force)
        stage_done("h83_news", 8, t0, _txt(nr.get("message")), ok=bool(nr.get("ok")), detail={"skipped": nr.get("skipped", False)})
    else:
        stage_done("h83_news", 8, t0, "新聞快取仍在 TTL 內，智慧略過", skipped=True)

    perf_due = bool(
        rr["performance_feedback"]
        and (
            force
            or ages["performance_feedback"] is None
            or ages["performance_feedback"] > ttl["performance_feedback"]
        )
    )
    run_handler(
        "performance_feedback",
        9,
        "feedback_learning",
        None,
        perf_due,
        "績效／回饋模型仍在 TTL 內，智慧略過",
    )

    # H83 used to rebuild H82 every single preflight. H87 adds a real TTL gate.
    h82_ttl = int(ttl.get("h82_learning", ttl.get("performance_feedback", 360)))
    h82_due = bool(
        cfg["auto_refresh_h82_learning"]
        and (
            force
            or ages.get("h82_adaptive_learning") is None
            or float(ages.get("h82_adaptive_learning") or 0) > h82_ttl
        )
    )
    t0 = stage_start("h82_adaptive_learning", 10, "檢查 H82 成熟學習狀態")
    if h82_due:
        try:
            from godpick_h82_adaptive_learning import refresh_learning_state

            state, _ = refresh_learning_state(persist_remote=True)
            ok = isinstance(state, dict)
            msg = "H82成熟學習已重建" if ok else "H82成熟學習重建未取得有效狀態"
            stage_done("h82_adaptive_learning", 10, t0, msg, ok=ok)
        except Exception as e:
            stage_done("h82_adaptive_learning", 10, t0, f"{type(e).__name__}: {e}", ok=False)
    else:
        stage_done("h82_adaptive_learning", 10, t0, "H82學習狀態仍在 TTL 內，智慧略過", skipped=True)

    t0 = stage_start("final_validation", 11, "重讀所有權威資料並決定 Formal READY")
    repair_market_snapshot(cfg, True)
    after = _snapshot(cfg)
    stage_done(
        "final_validation",
        11,
        t0,
        "Formal資料 READY" if after["formal_ready"] else "Formal資料未完全就緒，維持研究模式",
        detail=after,
        ok=True,
    )

    status = {
        "version": VERSION,
        "updated_at": _now().strftime("%Y-%m-%d %H:%M:%S"),
        "reason": reason,
        "mode": "FORCE_ALL" if force else "SMART_REFRESH",
        "formal_ready": after["formal_ready"],
        "research_only": not after["formal_ready"],
        "snapshot": after,
        "actions": actions,
        "total_duration_seconds": round(time.perf_counter() - started, 3),
    }
    status["message"] = (
        "H87前置資料通過"
        if after["formal_ready"]
        else "H87前置資料未完全就緒，Formal已降為研究模式：" + "；".join(after["issues"])
    )
    _persist_status_fast(status)

    # Final 100% event is explicit; Page20 can immediately switch from spinner to result.
    _emit_progress(
        progress_callback,
        stage="final_validation",
        state="done",
        completed_weight=total_weight,
        total_weight=total_weight,
        started_at=started,
        eta_seconds=0.0,
        message=status["message"],
        step_index=len(all_stages),
        step_count=len(all_stages),
        detail={"formal_ready": after["formal_ready"]},
    )
    return status


def load_status() -> dict[str, Any]:
    d = _read(STATUS_FILE, {})
    return d if isinstance(d, dict) else {}


def apply_h83_freshness_overlay(frame: Any, status: dict[str, Any] | None = None):
    import pandas as pd

    out = frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame(frame)
    st = status or load_status()
    if isinstance(st.get("snapshot"), dict):
        snap = st["snapshot"]
    else:
        from godpick_h83_autofresh_settings import load_settings_safe

        snap = _snapshot(load_settings_safe())
    out["H83版本"] = VERSION
    out["H83正式資料可用"] = "是" if snap.get("formal_ready") else "否"
    out["H83市場資料日期"] = snap.get("market_date", "")
    out["H83官方因子日期"] = snap.get("official_date", "")
    out["H83官方落後交易日"] = snap.get("official_business_lag")
    out["H83資料異常隔離"] = "；".join(snap.get("invalid_market_domains") or [])
    out["H83資料治理摘要"] = (
        "READY" if snap.get("formal_ready") else "RESEARCH-ONLY｜" + "；".join(snap.get("issues") or [])
    )
    return out
