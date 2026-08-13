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


def _report(ok: bool, message: str, *, details: Any = None, changed_files: list[str] | None = None, started: float | None = None, warning: bool = False) -> dict[str, Any]:
    return {
        "ok": bool(ok),
        "warning": bool(warning and ok),
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
    """Refresh official factors and verify the *content* business date.

    V191 hotfix2 prevents a successful HTTP/save operation from being reported
    as scheduler SUCCESS when the official cache itself did not advance.
    """
    t0 = time.perf_counter(); cfg = cfg or {}
    try:
        from godpick_system_health_service import load_schedule_settings, run_official_factor_update_once
        from official_factor_service import load_factor_cache, _factor_payload_business_date
        from godpick_official_release_timing import evaluate_twse_t86_release_timing
        official = dict(load_schedule_settings() or {})
        official.update(cfg.get("official_options", {}) if isinstance(cfg.get("official_options"), dict) else {})
        official["enabled"] = True
        result = run_official_factor_update_once(official, push_github=True)

        cache = load_factor_cache(force_authority_restore=False)
        official_date = _factor_payload_business_date(cache)
        market = {}
        try:
            market = json.loads((BASE_DIR / "market_snapshot.json").read_text(encoding="utf-8-sig"))
            if not isinstance(market, dict): market = {}
        except Exception:
            market = {}
        market_date = str(
            market.get("market_date") or market.get("twse_data_date") or market.get("otc_data_date")
            or market.get("data_date") or ""
        ).strip()[:10]
        timing = evaluate_twse_t86_release_timing(
            market_date=market_date, official_date=official_date, now=datetime.now(TZ)
        )
        timing_ok = bool(
            timing.get("phase") == "T0_READY" or timing.get("t1_is_normal_now")
        )
        fetch_ok = bool(result.get("ok"))
        content_ok = bool(official_date and market_date and timing_ok)
        ok = bool(fetch_ok and content_ok)
        details = dict(result) if isinstance(result, dict) else {"raw_result": result}
        details["business_date_validation"] = {
            "market_date": market_date, "official_date": official_date, **timing
        }
        if ok:
            msg = f"官方因子更新完成且內容日期驗證通過：官方 {official_date}／市場 {market_date}｜{timing.get('headline','')}"
        elif fetch_ok:
            msg = (
                f"官方因子抓取/保存完成，但內容日期未通過新鮮度驗證：官方 {official_date or '未驗證'}／"
                f"市場 {market_date or '未驗證'}｜{timing.get('headline') or timing.get('detail') or '內容日期未前進'}；不得列為SUCCESS"
            )
        else:
            msg = str(result.get("message") or "官方因子更新失敗")
        return _report(ok, msg, details=details, changed_files=["official_factors_cache.json", "official_factors_update_log.json"], started=t0)
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


def _ensure_records_authority_safe() -> tuple[list[dict[str, Any]], list[str], bool]:
    """Resolve/repair Page08 history before any scheduled history-based job.

    V191-H3 treats an accidental 0/4-row full replacement as a data-integrity
    incident.  All scheduled consumers (latest price, performance, T+1 and AI
    learning) must pass through the same authority recovery gate before reading
    recommendation history, so a force-all run repairs history early instead of
    propagating an empty dataset into downstream truth/learning files.
    """
    try:
        from godpick_persistence_service import ensure_records_local_authority_current
        rows, msgs, restored = ensure_records_local_authority_current()
        return list(rows or []), list(msgs or []), bool(restored)
    except ModuleNotFoundError as import_exc:
        # The repository smoke environment may omit Streamlit; production does
        # not take this fallback.  Keep the guard testable without claiming a
        # remote recovery happened.
        if "streamlit" not in str(import_exc).lower():
            raise
        try:
            payload = json.loads((BASE_DIR / "godpick_records.json").read_text(encoding="utf-8-sig"))
            rows = payload if isinstance(payload, list) else []
        except Exception:
            rows = []
        return list(rows or []), ["headless smoke fallback：僅檢查本機 godpick_records.json"], False


def _load_page8_records():
    rows, msgs, restored = _ensure_records_authority_safe()
    if restored:
        msgs = [*msgs, f"V191-H3：排程執行前已先恢復推薦歷史權威，共 {len(rows)} 筆。"]
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
        if df.empty:
            return _report(
                False,
                "推薦紀錄最新價已安全暫停：股神推薦歷史目前為0筆；V191-H3拒絕把空資料再次保存成權威。",
                details={"load_messages": load_msgs[-30:]}, started=t0,
            )
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
        from godpick_persistence_service import save_records_sync_fast
        authority_rows, authority_msgs, authority_restored = _ensure_records_authority_safe()
        if not authority_rows:
            return _report(
                False,
                "推薦後績效已安全暫停：股神推薦歷史目前為0筆；V191-H3先阻止空資料擴散，請完成歷史權威救援。",
                details={"authority_messages": authority_msgs[-30:], "authority_restored": authority_restored},
                started=t0,
            )
        result = update_recommendation_perf_fast_v77(
            json_files=["godpick_records.json", "godpick_calibration_samples.json", "godpick_latest_recommendations.json"],
            max_records=int(cfg.get("max_records", 2000) or 2000),
            batch_limit=int(cfg.get("batch_limit", 100) or 100),
            max_workers=int(cfg.get("max_workers", 12) or 12),
            stale_minutes=int(cfg.get("stale_minutes", 30) or 30),
            process_all=bool(cfg.get("process_all", True)),
        )
        rows, load_msgs, _ = _ensure_records_authority_safe()
        report = save_records_sync_fast(rows or authority_rows, reason="V191-H3 scheduled Page8 performance + save sync")
        persist_ok = bool(getattr(report, "permanent_ok", False))
        ok = bool((result or {}).get("ok", True) and persist_ok)
        return _report(ok, f"推薦後績效更新完成；推薦紀錄永久同步 {'成功' if persist_ok else '失敗'}", details={"performance": result, "authority_messages": authority_msgs[-20:], "authority_restored": authority_restored, "load_messages": load_msgs[-10:], "persistence": getattr(report, "__dict__", str(report))}, changed_files=["godpick_records.json", "godpick_recommend_list.json", "godpick_latest_recommendations.json", "godpick_calibration_samples.json"], started=t0)
    except Exception as exc:
        return _report(False, f"推薦後績效/儲存同步失敗：{type(exc).__name__}: {exc}", started=t0)


def _extract_named_rows_v191_h7(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [dict(x) for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ("recommendations", "records", "data", "rows", "items"):
            rows = payload.get(key)
            if isinstance(rows, list):
                return [dict(x) for x in rows if isinstance(x, dict)]
    return []


def _recover_current_list_from_records_v191_h7(latest_payload: dict[str, Any], records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Recover only rows belonging to the same Page07 run as the latest snapshot.

    This prevents an actually-empty new run from resurrecting an older list while
    repairing the H6 split-brain case where Page08 got R1 rows but the latest
    snapshot/Page10 list were persisted as empty by double governance.
    """
    if not isinstance(latest_payload, dict) or not records:
        return []
    saved_at = str(latest_payload.get("saved_at") or "").strip()
    rec_date = str(latest_payload.get("recommendation_date") or saved_at[:10] or "").strip()[:10]
    try:
        snap_ts = pd.to_datetime(saved_at, errors="coerce")
    except Exception:
        snap_ts = pd.NaT
    rdf = pd.DataFrame(records)
    if rdf.empty or "股票代號" not in rdf.columns:
        return []
    if rec_date and "推薦日期" in rdf.columns:
        rdf = rdf[rdf["推薦日期"].astype(str).str[:10].eq(rec_date)].copy()
    if rdf.empty:
        return []
    src = pd.Series([False] * len(rdf), index=rdf.index)
    if "推薦執行來源" in rdf.columns:
        src = src | rdf["推薦執行來源"].fillna("").astype(str).str.contains("07_股神推薦", regex=False)
    if "紀錄來源" in rdf.columns:
        src = src | rdf["紀錄來源"].fillna("").astype(str).str.contains("07_股神推薦", regex=False)
    if src.any():
        rdf = rdf.loc[src].copy()
    if rdf.empty:
        return []
    if "紀錄層級" in rdf.columns:
        level = rdf["紀錄層級"].fillna("").astype(str)
        allowed = level.str.startswith("正式") | level.str.startswith("A-") | level.str.startswith("R1")
        if allowed.any():
            rdf = rdf.loc[allowed].copy()
    if rdf.empty:
        return []

    # Same-run guard: Page07 snapshot and Page08 rows are written within minutes.
    # Prefer 建立時間; fallback to 推薦日期+推薦時間.
    ts = pd.Series(pd.NaT, index=rdf.index, dtype="datetime64[ns]")
    if "建立時間" in rdf.columns:
        ts = pd.to_datetime(rdf["建立時間"], errors="coerce")
    if "推薦時間" in rdf.columns:
        fallback = pd.to_datetime(rdf.get("推薦日期", rec_date).astype(str).str[:10] + " " + rdf["推薦時間"].astype(str), errors="coerce") if isinstance(rdf.get("推薦日期"), pd.Series) else pd.to_datetime(rec_date + " " + rdf["推薦時間"].astype(str), errors="coerce")
        ts = ts.fillna(fallback)
    if pd.notna(snap_ts) and ts.notna().any():
        delta = (ts - snap_ts).abs().dt.total_seconds()
        same_run = delta.le(15 * 60)
        if same_run.any():
            rdf = rdf.loc[same_run].copy()
        else:
            return []
    elif ts.notna().any():
        max_ts = ts.max()
        rdf = rdf.loc[(max_ts - ts).dt.total_seconds().between(0, 15 * 60)].copy()
    else:
        return []
    if "股票代號" in rdf.columns:
        rdf = rdf.drop_duplicates(subset=["股票代號"], keep="last")
    return rdf.to_dict(orient="records")


def _load_recommend_list() -> tuple[pd.DataFrame, list[str]]:
    """H7 current-list authority election and self-repair.

    Priority: Page10 current list -> Page07 latest snapshot recommendations ->
    same-run Page08 records.  An empty current-list file no longer hides a valid
    Page07 run.  The Page08 fallback is guarded by latest-run timestamps.
    """
    from godpick_persistence_service import load_named_json_permanent, load_records_permanent, save_named_json_permanent
    msgs: list[str] = []
    payload, m0 = load_named_json_permanent("godpick_recommend_list.json", [])
    msgs.extend(list(m0 or []))
    rows = _extract_named_rows_v191_h7(payload)
    if rows:
        msgs.append(f"H7目前推薦清單權威：godpick_recommend_list.json {len(rows)}筆")
        return pd.DataFrame(rows), msgs

    latest, m1 = load_named_json_permanent("godpick_latest_recommendations.json", {})
    msgs.extend(list(m1 or []))
    latest = latest if isinstance(latest, dict) else {}
    rows = _extract_named_rows_v191_h7(latest)
    if rows:
        try:
            rep = save_named_json_permanent("godpick_recommend_list.json", rows)
            msgs.append(f"H7由Page07最新快照修復Page10清單：{len(rows)}筆｜永久化={'成功' if rep.permanent_ok else '警示'}")
        except Exception as exc:
            msgs.append(f"H7最新快照可用，但Page10清單修復保存例外：{exc}")
        return pd.DataFrame(rows), msgs

    try:
        records, m2 = load_records_permanent()
        msgs.extend(list(m2 or []))
    except Exception as exc:
        records = []
        msgs.append(f"H7 Page08同輪備援讀取失敗：{exc}")
    recovered = _recover_current_list_from_records_v191_h7(latest, list(records or []))
    if recovered:
        try:
            rep = save_named_json_permanent("godpick_recommend_list.json", recovered)
            msgs.append(f"H7由Page08同輪紀錄修復Page10清單：{len(recovered)}筆｜永久化={'成功' if rep.permanent_ok else '警示'}")
            # Repair the split-brain latest snapshot without changing run identity.
            repaired_latest = dict(latest)
            repaired_latest["recommendations"] = recovered
            repaired_latest["recommendation_count"] = len(recovered)
            repaired_latest["h7_current_list_repaired_at"] = _now()
            repaired_latest["h7_current_list_repair_source"] = "Page08 same-run records"
            rep2 = save_named_json_permanent("godpick_latest_recommendations.json", repaired_latest)
            msgs.append(f"H7同步修復Page07最新快照 recommendations：{'成功' if rep2.permanent_ok else '警示'}")
        except Exception as exc:
            msgs.append(f"H7同輪紀錄已找到，但清單/快照修復保存例外：{exc}")
        return pd.DataFrame(recovered), msgs

    msgs.append("H7目前推薦清單為0：最新快照亦無recommendations，且Page08沒有同一推薦批次可安全回補；視為真0，不復活舊名單。")
    return pd.DataFrame(), msgs


def task_recommend_list_performance(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    t0=time.perf_counter(); cfg=cfg or {}
    try:
        from godpick_perf_fast_update_v77 import update_recommendation_perf_fast_v77
        from godpick_headless_page_loader import load_page_namespace
        ns=load_page_namespace("pages/10_推薦清單.py",base_dir=BASE_DIR)
        _require_headless_callables(ns, "Page10", ["_sync_records", "upsert_records_authority_fast", "save_named_json_permanent"])
        df,msgs=_load_recommend_list()
        result=update_recommendation_perf_fast_v77(
            json_files=["godpick_recommend_list.json"],
            max_records=int(cfg.get("max_records",5000) or 5000), batch_limit=int(cfg.get("batch_limit",120) or 120),
            max_workers=int(cfg.get("max_workers",12) or 12), stale_minutes=int(cfg.get("stale_minutes",30) or 30), process_all=bool(cfg.get("process_all",True)),
        )
        df_after,msgs_after=_load_recommend_list()
        if not df_after.empty:
            df=df_after
        msgs.extend(msgs_after[-8:])
        sync_ok,sync_msgs=ns["_sync_records"](df)
        ok=bool((result or {}).get("ok",True) and sync_ok)
        no_data=bool(df.empty)
        return _report(ok,f"推薦清單績效更新完成；{'0筆安全略過（未清空歷史）' if no_data else ('永久同步成功' if sync_ok else '永久同步失敗')}",details={"performance":result,"load_messages":msgs[-10:],"sync_messages":sync_msgs[-20:]},changed_files=["godpick_recommend_list.json","godpick_records.json"],started=t0,warning=bool(no_data and ok))
    except Exception as exc:
        return _report(False,f"推薦清單績效更新失敗：{type(exc).__name__}: {exc}",started=t0)


def task_recommend_list_n_day(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    t0=time.perf_counter(); cfg=cfg or {}
    try:
        from godpick_headless_page_loader import load_page_namespace
        ns=load_page_namespace("pages/10_推薦清單.py", base_dir=BASE_DIR)
        _require_headless_callables(ns, "Page10", ["_update_formal_n_day_metrics_v98", "_sync_records", "upsert_records_authority_fast", "save_named_json_permanent"])
        df,msgs=_load_recommend_list()
        out,summary=ns["_update_formal_n_day_metrics_v98"](df,max_rows=int(cfg.get("max_rows",300) or 300),show_progress=False)
        ok_sync,sync_msgs=ns["_sync_records"](out)
        ok=bool(ok_sync and (int(summary.get("processed",0) or 0)>=0))
        no_data=bool(df.empty)
        return _report(ok,f"正式N日績效回補：處理 {summary.get('processed',0)}／成功 {summary.get('success',0)}／剩餘 {summary.get('remaining',0)}；{'0筆安全略過（未清空歷史）' if no_data else ('同步成功' if ok_sync else '同步失敗')}",details={"summary":summary,"load_messages":msgs[-10:],"sync_messages":sync_msgs[-20:]},changed_files=["godpick_recommend_list.json","godpick_records.json"],started=t0,warning=bool(no_data and ok))
    except Exception as exc:
        return _report(False,f"正式N日績效回補失敗：{type(exc).__name__}: {exc}",started=t0)


def task_recommend_list_hits(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    t0=time.perf_counter(); cfg=cfg or {}
    try:
        from godpick_headless_page_loader import load_page_namespace
        ns=load_page_namespace("pages/10_推薦清單.py", base_dir=BASE_DIR)
        _require_headless_callables(ns, "Page10", ["_update_night_hit_tracking_v101", "_sync_records", "upsert_records_authority_fast", "save_named_json_permanent"])
        df,msgs=_load_recommend_list()
        out,summary=ns["_update_night_hit_tracking_v101"](df,max_rows=int(cfg.get("max_rows",300) or 300),show_progress=False)
        ok_sync,sync_msgs=ns["_sync_records"](out)
        no_data=bool(df.empty)
        return _report(bool(ok_sync),f"隔日命中追蹤：處理 {summary.get('processed',0)}／成功 {summary.get('success',0)}／失敗 {summary.get('fail',0)}；{'0筆安全略過（未清空歷史）' if no_data else ('同步成功' if ok_sync else '同步失敗')}",details={"summary":summary,"load_messages":msgs[-10:],"sync_messages":sync_msgs[-20:]},changed_files=["godpick_recommend_list.json","godpick_records.json"],started=t0,warning=bool(no_data and ok_sync))
    except Exception as exc:
        return _report(False,f"隔日命中追蹤失敗：{type(exc).__name__}: {exc}",started=t0)


def task_t1_truth(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    t0=time.perf_counter(); cfg=cfg or {}
    try:
        authority_rows, authority_msgs, authority_restored = _ensure_records_authority_safe()
        if not authority_rows:
            return _report(
                False,
                "T+1實戰真相已安全暫停：股神推薦歷史目前為0筆；V191-H3拒絕以空歷史重建真相/校準。",
                details={"authority_messages": authority_msgs[-30:], "authority_restored": authority_restored},
                started=t0,
            )
        from godpick_t1_trade_truth import refresh_t1_trade_truth
        res=refresh_t1_trade_truth(max_records=int(cfg.get("max_records",500) or 500),max_workers=int(cfg.get("max_workers",8) or 8),persist=True)
        ok=bool(res.get("ok") and res.get("persistence_ok",True))
        return _report(ok,f"T+1實戰真相：成熟 {res.get('matured_t1_samples',0)}／可執行 {res.get('executable_samples',0)}／永久化 {'成功' if res.get('persistence_ok',True) else '失敗'}",details={"authority_count":len(authority_rows),"authority_restored":authority_restored,"authority_messages":authority_msgs[-20:],"truth":res},changed_files=["godpick_t1_trade_truth.json","godpick_probability_calibration.json"],started=t0)
    except Exception as exc:
        return _report(False,f"T+1實戰真相更新失敗：{type(exc).__name__}: {exc}",started=t0)


def task_feedback_learning(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    t0=time.perf_counter()
    try:
        # Never rebuild AI learning from an accidentally empty authority.  First
        # run the same Page08 authority election/history-recovery gate used by
        # every other scheduled history consumer.
        rows, authority_msgs, restored = _ensure_records_authority_safe()
        if not rows:
            return _report(
                False,
                "AI學習已安全暫停：股神推薦歷史目前為0筆，V191-H3拒絕用空資料重建/覆蓋模型；請先完成推薦紀錄歷史救援。",
                details={"authority_messages": authority_msgs[-30:], "restored": restored},
                changed_files=[], started=t0,
            )
        from godpick_global_update_service import step_feedback_profile, step_learning_profile
        a=step_feedback_profile(BASE_DIR); b=step_learning_profile(BASE_DIR)
        ok=bool(a.get("ok",True) and b.get("ok",True))
        warning=bool(ok and (a.get("warning") or b.get("warning") or not a.get("available", True)))
        state_text = "完成（樣本/遠端仍有警示）" if warning else ("完成" if ok else "部分失敗")
        return _report(
            ok, f"AI績效回饋/每日學習重建 {state_text}｜權威紀錄 {len(rows)} 筆",
            details={"authority_count":len(rows),"authority_restored":restored,"authority_messages":authority_msgs[-20:],"feedback":a,"learning":b},
            changed_files=["godpick_performance_profile.json","godpick_learning_state.json"], started=t0, warning=warning
        )
    except Exception as exc:
        return _report(False,f"AI績效回饋/每日學習重建失敗：{type(exc).__name__}: {exc}",started=t0)


def task_durability_retry(cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    """H7 bounded durability convergence.

    The old task re-queued every unconfirmed file into one background thread and
    audited after only 8 seconds, so Streamlit could show the same 16/17 pending
    forever.  H7 first verifies remote sync-state hashes, then synchronously
    confirms only a bounded remaining batch and checkpoints every file.
    """
    t0=time.perf_counter(); cfg=cfg or {}
    try:
        authority_rows, authority_msgs, authority_restored = _ensure_records_authority_safe()
        from godpick_durability_service import sync_unconfirmed_critical_bounded
        result=sync_unconfirmed_critical_bounded(
            base_dir=BASE_DIR,
            max_files=int(cfg.get("max_sync_per_run",6) or 6),
            time_budget_seconds=float(cfg.get("time_budget_seconds",35) or 35),
        )
        audit=result.get("audit") if isinstance(result,dict) else {}
        pending=int(result.get("remaining",0) or 0)
        attempted=int(result.get("attempted",0) or 0)
        confirmed=int(result.get("confirmed_this_run",0) or 0)
        failed=int(result.get("failed_this_run",0) or 0)
        history_incident = bool(not authority_rows and any("歷史救援" in str(x) or "意外" in str(x) or "縮水" in str(x) for x in authority_msgs))
        hard_missing=[r for r in (audit.get("rows") or []) if isinstance(r,dict) and r.get("critical") and not r.get("exists")] if isinstance(audit,dict) else []
        ok=not bool(hard_missing) and not history_incident and failed==0
        if history_incident:
            msg="永久化修復未完成：推薦歷史仍為0筆且符合意外縮水/歷史救援情境；已停止把0筆當成正常遠端狀態。"
        else:
            msg=(
                f"永久化H7收斂：本輪同步 {attempted} 項／新增確認 {confirmed} 項／失敗 {failed} 項；"
                f"剩餘遠端Hash未確認 {pending} 項"
                + ("；已逐檔checkpoint，下次只續跑剩餘項目" if pending and ok else "")
                + (f"；推薦歷史權威 {'已救援' if authority_restored else '已確認'} {len(authority_rows)} 筆" if authority_rows else "")
            )
        return _report(
            ok,msg,details={
                "authority_count":len(authority_rows),"authority_restored":authority_restored,"authority_messages":authority_msgs[-30:],
                "convergence":result,"audit":audit,
            },
            changed_files=["godpick_durability_outbox.json","godpick_durability_audit.json"],started=t0,
            warning=bool(ok and pending)
        )
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
    """Invoke Page7's canonical automation runner; scheduler owns no pick logic.

    V191-H5 adds a recommendation-history authority preflight.  A force-all run
    previously reached Page07 before Page08's later jobs had a chance to finish
    H4 history recovery, so Page07 could scan successfully yet fail its record
    write with the integrity lock.  The same batch then repaired Page08, but the
    already-failed Page07 status stayed FAILED.  Preflight makes the authority
    safe *before* the expensive scan and exposes recovery diagnostics to Page07.
    """
    t0=time.perf_counter(); cfg=cfg or {}
    try:
        authority_rows, authority_msgs, authority_restored = _ensure_records_authority_safe()
        if not authority_rows:
            return _report(
                False,
                "07股神推薦前置未通過：08推薦歷史權威仍為0筆/未完成救援；已停止掃描，避免推薦完成後無法永久記錄。",
                details={"authority_messages": authority_msgs[-40:], "authority_restored": authority_restored},
                started=t0,
            )
        from godpick_headless_page_loader import load_page_namespace
        ns=load_page_namespace("pages/7_股神推薦.py",base_dir=BASE_DIR)
        _require_headless_callables(ns, "Page7", ["_run_page07_automation_v191_h2"])
        run_cfg=dict(cfg)
        run_cfg["authority_preflight_count_v191_h5"]=len(authority_rows)
        run_cfg["authority_preflight_restored_v191_h5"]=bool(authority_restored)
        result=ns["_run_page07_automation_v191_h2"](run_cfg) or {}
        result.setdefault("authority_preflight_count", len(authority_rows))
        result.setdefault("authority_preflight_restored", bool(authority_restored))
        result.setdefault("authority_preflight_messages", authority_msgs[-20:])
        ok=bool(result.get("ok"))
        msg=str(result.get("message") or ("07股神推薦模組自動執行完成" if ok else "07股神推薦模組自動執行失敗"))
        return _report(
            ok, msg, details=result,
            changed_files=list(result.get("changed_files") or [
                "godpick_latest_recommendations.json","godpick_latest_run_anchor.json","godpick_records.json",
                "godpick_rotation_history.json","godpick_learning_state.json"
            ]), started=t0, warning=bool(result.get("warning"))
        )
    except Exception as exc:
        return _report(False,f"07股神推薦模組自動執行失敗：{type(exc).__name__}: {exc}",started=t0)


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
