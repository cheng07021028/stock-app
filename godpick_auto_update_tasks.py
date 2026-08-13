# -*- coding: utf-8 -*-
"""V191 central automation task adapters.

Every adapter reuses existing production services/page business functions so the
scheduled result is the same data contract as a manual button press.  No task
clears caches/settings or deletes data.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo
import json
import time

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
TZ = ZoneInfo("Asia/Taipei")


def _now() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")


def _report(ok: bool, message: str, *, details: Any = None, changed_files: list[str] | None = None, started: float | None = None) -> dict[str, Any]:
    return {
        "ok": bool(ok),
        "message": str(message or ""),
        "details": details if details is not None else {},
        "changed_files": list(changed_files or []),
        "finished_at": _now(),
        "duration_seconds": round(time.perf_counter() - started, 3) if started is not None else None,
    }


def task_stock_master(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    t0 = time.perf_counter()
    try:
        from stock_master_service import refresh_stock_master
        df, msgs = refresh_stock_master(sync_github=True)
        ok = isinstance(df, pd.DataFrame) and len(df) >= 1000
        return _report(ok, f"股票主檔更新 {'成功' if ok else '未達完整標準'}：{len(df) if isinstance(df,pd.DataFrame) else 0} 筆", details={"messages": msgs[-20:]}, changed_files=["stock_master_cache.json"], started=t0)
    except Exception as exc:
        return _report(False, f"股票主檔更新失敗：{type(exc).__name__}: {exc}", started=t0)


def task_macro_full(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    """Exact Page0 V70 one-click: TWSE/TPEx/institutional/US/TAIFEX/overnight + bridge."""
    t0 = time.perf_counter()
    try:
        from godpick_headless_page_loader import load_page_namespace
        ns = load_page_namespace("pages/0_大盤走勢.py", base_dir=BASE_DIR)
        fn = ns.get("_v70_run_one_click_update_and_write")
        if not callable(fn):
            return _report(False, "大盤頁 V70 一鍵更新函式未載入", started=t0)
        today = datetime.now(TZ).date()
        extras: dict[str, Any] = {}
        # Page0 manual items that are not duplicated by V70 but are useful to the
        # next-day model are also refreshed before the bridge is written.
        if callable(ns.get("_v45_write_neutral_event_cache")):
            try:
                eok, emsg = ns["_v45_write_neutral_event_cache"]()
                extras["event_safety_cache"] = {"ok": bool(eok), "message": str(emsg)}
            except Exception as exc:
                extras["event_safety_cache"] = {"ok": False, "message": str(exc)}
        if callable(ns.get("_batch_fetch_close_cache")):
            try:
                added, messages = ns["_batch_fetch_close_cache"](today, days=20)
                extras["close_20d_backfill"] = {"ok": True, "added": int(added or 0), "messages": list(messages or [])[-20:]}
            except Exception as exc:
                extras["close_20d_backfill"] = {"ok": False, "message": str(exc)}
        result = fn(today)
        if isinstance(result, dict):
            result["v191_extra_page0_updates"] = extras
        ok = bool(result.get("all_required_updated") and result.get("all_required_written")) if isinstance(result, dict) else False
        failed = result.get("failed_items", []) if isinstance(result, dict) else []
        msg = "大盤全頁必要更新、20日收盤補值與股神橋接完成" if ok else f"大盤更新部分失敗：{len(failed)} 項"
        return _report(ok, msg, details=result, changed_files=[
            "market_snapshot.json", "macro_mode_bridge.json", "macro_trend_records.json",
            "macro_market_close_cache.json", "macro_institutional_cache.json", "macro_taifex_cache.json",
            "macro_us_market_cache.json", "macro_otc_cache.json", "overnight_global_market_cache.json", "macro_news_event_cache.json",
        ], started=t0)
    except Exception as exc:
        return _report(False, f"大盤全項自動更新失敗：{type(exc).__name__}: {exc}", started=t0)


def task_official_factors(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    t0 = time.perf_counter(); cfg = cfg or {}
    try:
        from godpick_system_health_service import load_schedule_settings, run_official_factor_update_once
        official = dict(load_schedule_settings() or {})
        official.update(cfg.get("official_options", {}) if isinstance(cfg.get("official_options"), dict) else {})
        official["enabled"] = True
        result = run_official_factor_update_once(official, push_github=True)
        return _report(bool(result.get("ok")), result.get("message") or "官方因子更新完成", details=result, changed_files=["official_factors_cache.json", "official_factors_update_log.json"], started=t0)
    except Exception as exc:
        return _report(False, f"官方因子更新失敗：{type(exc).__name__}: {exc}", started=t0)


def task_super_ai_context(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    t0 = time.perf_counter(); cfg = cfg or {}
    try:
        from godpick_super_ai_market_context import refresh_super_ai_market_context
        payload, diagnostics = refresh_super_ai_market_context(fetch_etf=bool(cfg.get("fetch_etf", True)))
        has_margin = bool((payload or {}).get("margin_by_stock"))
        has_taifex = bool((payload or {}).get("taifex"))
        ok = has_margin or has_taifex
        return _report(ok, f"SuperAI融資券/期貨/PCR/ETF情境更新 {'完成' if ok else '資料不足'}", details={"diagnostics": diagnostics[-30:], "margin_count": len((payload or {}).get("margin_by_stock", {})), "taifex": (payload or {}).get("taifex", {}), "etf": (payload or {}).get("etf", {})}, changed_files=["super_ai_market_context.json"], started=t0)
    except Exception as exc:
        return _report(False, f"SuperAI市場情境更新失敗：{type(exc).__name__}: {exc}", started=t0)


def _require_headless_callables(ns: dict[str, Any], page_name: str, names: list[str]) -> None:
    missing = [name for name in names if not callable(ns.get(name))]
    if missing:
        raise RuntimeError(
            f"{page_name} headless載入不完整，缺少可呼叫函式：{', '.join(missing)}；"
            "請檢查 APP_AUTH_GUARD_V84 與 headless loader 相容性"
        )


def _load_page8_records():
    from godpick_persistence_service import load_records_permanent
    rows, msgs = load_records_permanent()
    return pd.DataFrame(rows or []), msgs


def task_watchlist_runtime(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    t0=time.perf_counter()
    try:
        from godpick_global_update_service import step_watchlist
        res=step_watchlist(BASE_DIR)
        ok=bool(res.get("ok",True))
        return _report(ok,res.get("message") or ("自選股runtime同步完成" if ok else "自選股runtime同步失敗"),details=res,changed_files=["watchlist_runtime_snapshot.json","watchlist_normalized.json"],started=t0)
    except Exception as exc:
        return _report(False,f"自選股runtime同步失敗：{type(exc).__name__}: {exc}",started=t0)


def task_record_latest_price(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    """Exact Page8 latest price semantics + immediate permanent save."""
    t0 = time.perf_counter(); cfg = cfg or {}
    try:
        from godpick_headless_page_loader import load_page_namespace
        ns = load_page_namespace("pages/8_股神推薦紀錄.py", base_dir=BASE_DIR)
        _require_headless_callables(ns, "Page8", ["_refresh_latest_prices", "_save_records_dual", "save_records_sync_fast"])
        df, load_msgs = _load_page8_records()
        ensure = ns.get("_ensure_godpick_record_columns")
        if callable(ensure): df = ensure(df)
        st = ns["__headless_st__"]
        st.session_state[ns.get("_k", lambda x:x)("latest_price_batch_size")] = int(cfg.get("batch_size", 100) or 100)
        updated = ns["_refresh_latest_prices"](df, only_active=bool(cfg.get("only_active", False)))
        summary = dict(getattr(updated, "attrs", {}).get("latest_refresh_summary", {}) or {})
        save_ok = bool(ns["_save_records_dual"](updated))
        success = int(summary.get("success", 0) or 0)
        target = int(summary.get("target", len(updated)) or len(updated))
        ok = bool(save_ok and (success > 0 or target == 0 or int(summary.get("unchanged_price", 0) or 0) > 0))
        return _report(ok, f"推薦紀錄最新價：目標 {target}／成功 {success}／永久同步 {'成功' if save_ok else '失敗'}", details={"summary": summary, "load_messages": load_msgs[-10:]}, changed_files=["godpick_records.json"], started=t0)
    except Exception as exc:
        return _report(False, f"推薦紀錄最新價更新失敗：{type(exc).__name__}: {exc}", started=t0)


def task_record_performance(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    t0 = time.perf_counter(); cfg = cfg or {}
    try:
        from godpick_perf_fast_update_v77 import update_recommendation_perf_fast_v77
        from godpick_persistence_service import load_records_permanent, save_records_sync_fast
        result = update_recommendation_perf_fast_v77(
            json_files=["godpick_records.json", "godpick_calibration_samples.json", "godpick_latest_recommendations.json"],
            max_records=int(cfg.get("max_records", 2000) or 2000),
            batch_limit=int(cfg.get("batch_limit", 100) or 100),
            max_workers=int(cfg.get("max_workers", 12) or 12),
            stale_minutes=int(cfg.get("stale_minutes", 30) or 30),
            process_all=bool(cfg.get("process_all", True)),
        )
        rows, load_msgs = load_records_permanent()
        report = save_records_sync_fast(rows or [], reason="V191 scheduled Page8 performance + save sync")
        persist_ok = bool(getattr(report, "permanent_ok", False))
        ok = bool((result or {}).get("ok", True) and persist_ok)
        return _report(ok, f"推薦後績效更新完成；推薦紀錄永久同步 {'成功' if persist_ok else '失敗'}", details={"performance": result, "load_messages": load_msgs[-10:], "persistence": getattr(report, "__dict__", str(report))}, changed_files=["godpick_records.json", "godpick_recommend_list.json", "godpick_latest_recommendations.json", "godpick_calibration_samples.json"], started=t0)
    except Exception as exc:
        return _report(False, f"推薦後績效/儲存同步失敗：{type(exc).__name__}: {exc}", started=t0)


def _load_recommend_list() -> tuple[pd.DataFrame, list[str]]:
    from godpick_persistence_service import load_named_json_permanent
    payload, msgs = load_named_json_permanent("godpick_recommend_list.json", [])
    if isinstance(payload, dict):
        rows = payload.get("records") or payload.get("data") or payload.get("rows") or []
    else:
        rows = payload if isinstance(payload, list) else []
    return pd.DataFrame(rows or []), msgs


def task_recommend_list_performance(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    t0=time.perf_counter(); cfg=cfg or {}
    try:
        from godpick_perf_fast_update_v77 import update_recommendation_perf_fast_v77
        from godpick_headless_page_loader import load_page_namespace
        result=update_recommendation_perf_fast_v77(
            json_files=["godpick_recommend_list.json"],
            max_records=int(cfg.get("max_records",5000) or 5000), batch_limit=int(cfg.get("batch_limit",120) or 120),
            max_workers=int(cfg.get("max_workers",12) or 12), stale_minutes=int(cfg.get("stale_minutes",30) or 30), process_all=bool(cfg.get("process_all",True)),
        )
        ns=load_page_namespace("pages/10_推薦清單.py",base_dir=BASE_DIR)
        _require_headless_callables(ns, "Page10", ["_sync_records", "save_records_permanent", "save_named_json_permanent"])
        df,msgs=_load_recommend_list(); sync_ok,sync_msgs=ns["_sync_records"](df)
        ok=bool((result or {}).get("ok",True) and sync_ok)
        return _report(ok,f"推薦清單績效更新完成；永久同步 {'成功' if sync_ok else '失敗'}",details={"performance":result,"load_messages":msgs[-10:],"sync_messages":sync_msgs[-20:]},changed_files=["godpick_recommend_list.json","godpick_records.json"],started=t0)
    except Exception as exc:
        return _report(False,f"推薦清單績效更新失敗：{type(exc).__name__}: {exc}",started=t0)


def task_recommend_list_n_day(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    t0=time.perf_counter(); cfg=cfg or {}
    try:
        from godpick_headless_page_loader import load_page_namespace
        ns=load_page_namespace("pages/10_推薦清單.py", base_dir=BASE_DIR)
        _require_headless_callables(ns, "Page10", ["_update_formal_n_day_metrics_v98", "_sync_records", "save_records_permanent", "save_named_json_permanent"])
        df,msgs=_load_recommend_list()
        out,summary=ns["_update_formal_n_day_metrics_v98"](df,max_rows=int(cfg.get("max_rows",300) or 300),show_progress=False)
        ok_sync,sync_msgs=ns["_sync_records"](out)
        ok=bool(ok_sync and (int(summary.get("processed",0) or 0)>=0))
        return _report(ok,f"正式N日績效回補：處理 {summary.get('processed',0)}／成功 {summary.get('success',0)}／剩餘 {summary.get('remaining',0)}；同步 {'成功' if ok_sync else '失敗'}",details={"summary":summary,"load_messages":msgs[-10:],"sync_messages":sync_msgs[-20:]},changed_files=["godpick_recommend_list.json","godpick_records.json"],started=t0)
    except Exception as exc:
        return _report(False,f"正式N日績效回補失敗：{type(exc).__name__}: {exc}",started=t0)


def task_recommend_list_hits(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    t0=time.perf_counter(); cfg=cfg or {}
    try:
        from godpick_headless_page_loader import load_page_namespace
        ns=load_page_namespace("pages/10_推薦清單.py", base_dir=BASE_DIR)
        _require_headless_callables(ns, "Page10", ["_update_night_hit_tracking_v101", "_sync_records", "save_records_permanent", "save_named_json_permanent"])
        df,msgs=_load_recommend_list()
        out,summary=ns["_update_night_hit_tracking_v101"](df,max_rows=int(cfg.get("max_rows",300) or 300),show_progress=False)
        ok_sync,sync_msgs=ns["_sync_records"](out)
        return _report(bool(ok_sync),f"隔日命中追蹤：處理 {summary.get('processed',0)}／成功 {summary.get('success',0)}／失敗 {summary.get('fail',0)}；同步 {'成功' if ok_sync else '失敗'}",details={"summary":summary,"load_messages":msgs[-10:],"sync_messages":sync_msgs[-20:]},changed_files=["godpick_recommend_list.json","godpick_records.json"],started=t0)
    except Exception as exc:
        return _report(False,f"隔日命中追蹤失敗：{type(exc).__name__}: {exc}",started=t0)


def task_t1_truth(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    t0=time.perf_counter(); cfg=cfg or {}
    try:
        from godpick_t1_trade_truth import refresh_t1_trade_truth
        res=refresh_t1_trade_truth(max_records=int(cfg.get("max_records",500) or 500),max_workers=int(cfg.get("max_workers",8) or 8),persist=True)
        ok=bool(res.get("ok") and res.get("persistence_ok",True))
        return _report(ok,f"T+1實戰真相：成熟 {res.get('matured_t1_samples',0)}／可執行 {res.get('executable_samples',0)}／永久化 {'成功' if res.get('persistence_ok',True) else '失敗'}",details=res,changed_files=["godpick_t1_trade_truth.json","godpick_probability_calibration.json"],started=t0)
    except Exception as exc:
        return _report(False,f"T+1實戰真相更新失敗：{type(exc).__name__}: {exc}",started=t0)


def task_feedback_learning(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    t0=time.perf_counter()
    try:
        from godpick_global_update_service import step_feedback_profile, step_learning_profile
        a=step_feedback_profile(BASE_DIR); b=step_learning_profile(BASE_DIR)
        ok=bool(a.get("ok",True) and b.get("ok",True))
        return _report(ok, f"AI績效回饋/每日學習重建 {'完成' if ok else '部分失敗'}", details={"feedback":a,"learning":b}, changed_files=["godpick_performance_profile.json","godpick_learning_state.json"], started=t0)
    except Exception as exc:
        return _report(False,f"AI績效回饋/每日學習重建失敗：{type(exc).__name__}: {exc}",started=t0)


def task_durability_retry(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    t0=time.perf_counter()
    try:
        from godpick_durability_service import retry_failed_durability, audit_core_durability
        msgs=retry_failed_durability(base_dir=BASE_DIR)
        audit=audit_core_durability(base_dir=BASE_DIR,write_audit=True)
        # Queueing is asynchronous; report current evidence, not a false permanent claim.
        failed=int(audit.get("critical_total",0) or 0)-int(audit.get("critical_remote_confirmed",0) or 0)
        return _report(True,f"永久化待同步/失敗項目已重試排程 {len(msgs)} 項；目前尚待遠端Hash確認 {max(failed,0)} 項",details={"messages":msgs[-30:],"audit":audit},changed_files=["godpick_durability_outbox.json","godpick_durability_audit.json"],started=t0)
    except Exception as exc:
        return _report(False,f"永久化重試失敗：{type(exc).__name__}: {exc}",started=t0)


def _page7_auto_universe(ns: dict[str,Any], master_df: pd.DataFrame, watchlist_map: dict[str,Any], settings: dict[str,Any]):
    mode=str(settings.get("universe_mode") or "全市場")
    if mode=="自選群組":
        return watchlist_map.get(str(settings.get("group") or ""),[])
    if mode=="手動輸入":
        return ns["_parse_manual_codes"](settings.get("manual_codes", ""),master_df)
    return ns["_build_universe_from_market"](
        master_df=master_df, market_mode=mode,
        limit_count=settings.get("scan_limit",1000),
        selected_categories=settings.get("selected_categories") or ["全部"],
    )


def task_auto_recommendation(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    """Run Page7 recommendation headlessly with the user's saved scan settings."""
    t0=time.perf_counter(); cfg=cfg or {}
    try:
        from godpick_headless_page_loader import load_page_namespace
        ns=load_page_namespace("pages/7_股神推薦.py",base_dir=BASE_DIR)
        _require_headless_callables(ns, "Page7", ["load_watchlist_permanent", "save_records_sync_fast", "_load_watchlist_map", "_build_recommend_df"])
        st=ns["__headless_st__"]
        watch=ns["_load_watchlist_map"]() or {}
        master=ns["_load_master_df"]()
        if master is None or master.empty:
            master=ns["_load_master_df_fallback_only"]()
        if master is None or master.empty:
            return _report(False,"自動股神推薦失敗：股票主檔為空",started=t0)
        cats=["全部"]+(ns["_collect_all_categories"](master,watch) or [])
        settings=ns["_load_persistent_recommend_scan_settings"](watch,cats)
        if bool(cfg.get("force_full_market",False)):
            settings["universe_mode"]="全市場"; settings["scan_limit"]="全部"
        ns["_apply_recommend_scan_settings_to_state"](settings,sync_widgets=False)
        payload=ns["_load_persistent_settings"](local_first=True)
        weights=ns["_normalize_weight_map"](payload.get("applied_weights") or payload.get("score_weights"))
        bridge=ns["_read_macro_mode_bridge"]()
        weights=ns["_apply_macro_bridge_to_weights"](weights,bridge,enabled=True)
        ns["GODPICK_ACTIVE_SCORE_WEIGHTS"]=weights.copy()
        universe=_page7_auto_universe(ns,master,watch,settings)
        if not universe:
            return _report(False,"自動股神推薦失敗：掃描池為空",started=t0)
        today=datetime.now(TZ).date(); start=today-timedelta(days=int(settings.get("days",120) or 120))
        rec,category,hot=ns["_build_recommend_df"](
            universe_items=universe,master_df=master,start_dt=start,end_dt=today,
            min_total_score=float(settings.get("min_total_score",55)),min_signal_score=float(settings.get("min_signal_score",-2)),
            selected_categories=settings.get("selected_categories") or ["全部"],mode=str(settings.get("recommend_mode") or "飆股模式"),
            risk_strictness=str(settings.get("risk_strictness") or "標準"),min_prelaunch_score=float(settings.get("min_prelaunch_score",45)),
            min_trade_score=float(settings.get("min_trade_score",45)),resume_scan=False,reuse_finished_checkpoint=False,
        )
        rec,hot,_=ns["_postprocess_recommend_result_v164"](rec,hot,bridge,True,force=True)
        candidate=st.session_state.get(ns["_k"]("candidate_diagnosis_store"))
        if rec is None or rec.empty:
            cond=ns["_conditional_reference_rows"](candidate,max_rows=8) if isinstance(candidate,pd.DataFrame) else pd.DataFrame()
            if not cond.empty: rec=cond
        save_ok=bool(ns["_save_recommend_result_to_state"](rec,category,hot))
        source=candidate if isinstance(candidate,pd.DataFrame) and not candidate.empty else rec
        # Synchronous permanent record upsert: a scheduler must never call a run complete before history is durable.
        added,msgs=ns["_v159_auto_record_actionable_recommendations"](source,background_write=False)
        extra=[]
        if callable(ns.get("save_rotation_snapshot")):
            try: extra.append(str(ns["save_rotation_snapshot"](source,background_remote=False)))
            except Exception as e: extra.append(f"rotation:{e}")
        if callable(ns.get("save_learning_run")):
            try: extra.append(str(ns["save_learning_run"](source,rec,scan_report=st.session_state.get(ns["_k"]("scan_quality_report"),{}),metadata={"automation":"V191","scan_settings":settings},persist_remote=True,background_remote=False,pre_scored=True)[:2]))
            except Exception as e: extra.append(f"learning:{e}")
        if callable(ns.get("save_super_ai_run")):
            try: extra.append(str(ns["save_super_ai_run"](source,rec,metadata={"automation":"V191","scan_settings":settings})))
            except Exception as e: extra.append(f"super_ai:{e}")
        scan_report=st.session_state.get(ns["_k"]("scan_quality_report"),{}) or {}
        ok=bool(save_ok and isinstance(rec,pd.DataFrame))
        return _report(ok,f"自動股神推薦完成：掃描 {len(universe)}／候選 {len(source) if isinstance(source,pd.DataFrame) else 0}／顯示 {len(rec) if isinstance(rec,pd.DataFrame) else 0}／永久紀錄 {added}",details={"settings":settings,"scan_report":scan_report,"record_messages":msgs[-20:],"experience_messages":extra[-10:]},changed_files=["godpick_latest_recommendations.json","godpick_latest_run_anchor.json","godpick_records.json","godpick_rotation_history.json","godpick_learning_state.json"],started=t0)
    except Exception as exc:
        return _report(False,f"自動股神推薦失敗：{type(exc).__name__}: {exc}",started=t0)


TASK_HANDLERS: dict[str, Callable[[dict[str, Any] | None], dict[str, Any]]] = {
    "stock_master": task_stock_master,
    "macro_full": task_macro_full,
    "official_factors": task_official_factors,
    "super_ai_context": task_super_ai_context,
    "watchlist_runtime": task_watchlist_runtime,
    "record_latest_price": task_record_latest_price,
    "record_performance": task_record_performance,
    "recommend_list_performance": task_recommend_list_performance,
    "recommend_list_n_day": task_recommend_list_n_day,
    "recommend_list_hits": task_recommend_list_hits,
    "t1_truth": task_t1_truth,
    "feedback_learning": task_feedback_learning,
    "durability_retry": task_durability_retry,
    "godpick_recommendation": task_auto_recommendation,
}

__all__=["TASK_HANDLERS"]+[f.__name__ for f in TASK_HANDLERS.values()]
