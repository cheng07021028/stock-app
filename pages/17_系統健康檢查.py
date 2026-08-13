# -*- coding: utf-8 -*-
from __future__ import annotations

try:
    from app_auth import require_login
    require_login()
except Exception as _auth_e:
    import streamlit as st
    st.error(f"登入系統載入失敗：{_auth_e}")
    st.stop()

import json
from pathlib import Path

import pandas as pd
import streamlit as st

try:
    from utils import inject_pro_theme
except Exception:
    def inject_pro_theme() -> None:  # type: ignore
        return None

from godpick_system_health_service import (
    DEFAULT_SCHEDULE_SETTINGS,
    full_safe_repair,
    load_schedule_settings,
    run_compile_smoke_test,
    run_health_check,
    run_official_factor_update_once,
    save_schedule_settings,
)

from godpick_global_update_service import (
    check_all_module_data_status,
    load_global_update_settings,
    run_global_update,
    save_global_update_settings,
)

try:
    from godpick_auto_scheduler import (
        VERSION as AUTO_SCHEDULER_VERSION, JOB_LABELS as AUTO_JOB_LABELS, DEFAULT_SETTINGS as AUTO_DEFAULT_SETTINGS,
        load_settings as load_auto_scheduler_settings, save_settings as save_auto_scheduler_settings,
        load_status as load_auto_scheduler_status, load_history as load_auto_scheduler_history,
        run_due_jobs as run_auto_due_jobs, next_run_rows as auto_next_run_rows, normalize_settings as normalize_auto_scheduler_settings,
    )
except Exception:
    AUTO_SCHEDULER_VERSION = "V191 scheduler unavailable"
    AUTO_JOB_LABELS = {}
    AUTO_DEFAULT_SETTINGS = {}
    load_auto_scheduler_settings = save_auto_scheduler_settings = load_auto_scheduler_status = load_auto_scheduler_history = None
    run_auto_due_jobs = auto_next_run_rows = normalize_auto_scheduler_settings = None

try:
    from godpick_durability_service import audit_core_durability, retry_failed_durability, queue_existing_critical_for_migration
except Exception:
    audit_core_durability = None
    retry_failed_durability = None
    queue_existing_critical_for_migration = None
try:
    from godpick_super_ai_market_context import refresh_super_ai_market_context, load_super_ai_market_context
except Exception:
    refresh_super_ai_market_context = None
    load_super_ai_market_context = None

st.set_page_config(page_title="17_系統健康檢查", layout="wide")
inject_pro_theme()

st.title("17_系統健康檢查 / 全模組一鍵更新中心")
st.caption("V191｜中央自動排程＋永久化監控：股票主檔→大盤→官方因子→SuperAI情境→股神推薦→最新價/績效→N日/命中→T+1學習，全鏈可設定台灣時間自動執行。")


# ---------------------------------------------------------------------------
# V191 Central Scheduler UI
# ---------------------------------------------------------------------------
if callable(load_auto_scheduler_settings):
    _auto_cfg = load_auto_scheduler_settings()
    _auto_status = load_auto_scheduler_status() if callable(load_auto_scheduler_status) else {}
    with st.expander("⏰ V191｜中央自動排程中心（每日自動更新＋永久記錄＋自動股神推薦）", expanded=True):
        st.info(
            "中央排程只負責『到期檢查與依序執行』；正式無人值守由 GitHub Actions 每10分鐘喚醒一次。"
            "所有時間均為 Asia/Taipei。預設不會自動啟用，必須由你勾選總開關並永久保存後才執行。"
            "V191 2026-08-13 Hotfix3：FAILED/BLOCKED 不再誤標已完成；每一項工作完成後立即 checkpoint；"
            "強制全部批次若因 rerun/redeploy 中斷，12 小時內下一次中央喚醒會續跑未完成工作；失效 PID 鎖會自動回收。"
            "強制驗證使用獨立 FORCE 執行鍵，不會再吃掉當日晚間正式排程時段；官方因子只有『抓取/保存＋內容日期驗證』都通過才算 SUCCESS。"
            "07 自動推薦由第7頁自己的正式推薦流程執行，中央排程只負責觸發；結果仍永久寫入第8頁推薦紀錄。"
            "Hotfix3 另加入推薦歷史防歸零：10推薦清單只能增量更新08既有紀錄，絕不可用本輪4/0筆整檔覆蓋歷史；"
            "AI學習遇到08權威0筆會安全暫停，並先嘗試GitHub歷史版本救援；救援會掃描近期 state commit、挑選明顯較完整的歷史快照，合併當日存活資料後重建本機與遠端權威。"
            "永久化重試補排後會短暫等待後台Hash確認；能在等待窗內完成就回SUCCESS，仍在同步才回WARNING。WARNING不是FAILED。"
        )
        _active_run = (_auto_status.get("active_run") or {}) if isinstance(_auto_status, dict) else {}
        if isinstance(_active_run, dict) and _active_run.get("pending_jobs"):
            _pending_labels = [AUTO_JOB_LABELS.get(str(x), str(x)) for x in (_active_run.get("pending_jobs") or [])]
            st.warning(
                f"偵測到未完成排程批次：模式={_active_run.get('mode', 'unknown')}；"
                f"目前/最後工作={AUTO_JOB_LABELS.get(str(_active_run.get('last_job') or ''), str(_active_run.get('last_job') or '尚未開始'))}；"
                f"待續跑 {len(_pending_labels)} 項。下一次中央喚醒會依 Hotfix 規則續跑。"
            )
        _m1, _m2, _m3, _m4 = st.columns(4)
        _m1.metric("總開關", "啟用" if _auto_cfg.get("enabled") else "停用")
        _m2.metric("已啟用工作", sum(1 for x in (_auto_cfg.get("jobs") or {}).values() if isinstance(x, dict) and x.get("enabled")))
        _sum = (_auto_status.get("last_summary") or {}) if isinstance(_auto_status, dict) else {}
        _m3.metric("最近 成功/警示/失敗", f"{_sum.get('success', 0)}/{_sum.get('warning', 0)}/{_sum.get('failed', 0)}")
        _m4.metric("最後喚醒", str((_auto_status or {}).get("last_wakeup_at") or "尚未執行"))
        st.caption("狀態定義：SUCCESS＝完整完成；WARNING＝工作已完成但有非致命待確認事項（不是失敗）；FAILED＝執行失敗；BLOCKED＝前置條件未通過。")

        with st.form("v191_central_scheduler_form", clear_on_submit=False):
            _enable_all = st.checkbox("啟用 V191 中央自動排程", value=bool(_auto_cfg.get("enabled", False)))
            _weekdays = st.checkbox("僅交易日週一～週五執行", value=bool(_auto_cfg.get("weekdays_only", True)))
            _rec_force_full = st.checkbox(
                "自動股神推薦固定使用全市場完整掃描",
                value=bool((((_auto_cfg.get("jobs") or {}).get("godpick_recommendation") or {}).get("options") or {}).get("force_full_market", False)),
                help="未勾選時，沿用第7頁已永久保存的掃描範圍/群組/市場/門檻；勾選時只覆寫自動排程的掃描範圍為全市場，不修改第7頁人工設定。",
            )
            st.caption("每日時間可填一個或多個，例如 14:20,20:40。GitHub Actions 是喚醒器，真正是否到期由本設定判斷。")
            _edited_jobs = {}
            for _job, _label in AUTO_JOB_LABELS.items():
                _jc = ((_auto_cfg.get("jobs") or {}).get(_job) or {})
                _c1, _c2, _c3 = st.columns([0.7, 4.0, 2.2])
                with _c1:
                    _jen = st.checkbox("啟用", value=bool(_jc.get("enabled", False)), key=f"v191_en_{_job}")
                with _c2:
                    st.markdown(f"**{_label}**")
                    if _job == "godpick_recommendation":
                        st.caption("只有股票主檔、大盤、官方因子『內容日期驗證』、SuperAI市場情境、自選股runtime於今日前置成功，才允許自動推薦；真正選股由第7頁模組執行。若本輪0檔通過可操作底線，狀態會是WARNING並保存候選診斷，不會硬塞弱股。")
                with _c3:
                    _jtimes = st.text_input("台灣時間", value=",".join(_jc.get("times") or []), key=f"v191_times_{_job}", label_visibility="collapsed")
                _newj = dict(_jc)
                _newj["enabled"] = bool(_jen)
                _newj["times"] = [x.strip() for x in str(_jtimes).replace("，", ",").split(",") if x.strip()]
                if _job == "godpick_recommendation":
                    _opts = dict(_newj.get("options") or {})
                    _opts["force_full_market"] = bool(_rec_force_full)
                    _newj["options"] = _opts
                _edited_jobs[_job] = _newj
            _s1, _s2, _s3 = st.columns(3)
            _grace = _s1.number_input("到期容許分鐘", min_value=10, max_value=120, value=int(_auto_cfg.get("grace_minutes", 35) or 35), step=5)
            _retry = _s2.number_input("失敗重試次數", min_value=0, max_value=5, value=int(_auto_cfg.get("retry_count", 2) or 2), step=1)
            _delay = _s3.number_input("重試間隔秒", min_value=5, max_value=180, value=int(_auto_cfg.get("retry_delay_seconds", 20) or 20), step=5)
            _save_auto = st.form_submit_button("💾 永久保存 V191 自動排程設定", type="primary", use_container_width=True)
        if _save_auto:
            _new_cfg = dict(_auto_cfg)
            _new_cfg.update({"enabled": bool(_enable_all), "weekdays_only": bool(_weekdays), "grace_minutes": int(_grace), "retry_count": int(_retry), "retry_delay_seconds": int(_delay), "jobs": _edited_jobs})
            _ok, _msg = save_auto_scheduler_settings(_new_cfg)
            if _ok:
                st.success("V191 中央排程設定已永久保存；GitHub Actions 下一次喚醒後會依新時間執行。")
                st.caption(_msg)
            else:
                st.error("V191 排程本機可能已寫入，但遠端永久化未確認；Reboot 前請先處理。")
                st.caption(_msg)

        _b1, _b2, _b3 = st.columns(3)
        if _b1.button("🧭 模擬目前到期項目（不執行）", use_container_width=True):
            _sim = run_auto_due_jobs(simulate=True)
            if _sim.get("executed"):
                st.dataframe(pd.DataFrame(_sim.get("executed", [])), use_container_width=True, hide_index=True)
            else:
                st.info(_sim.get("message"))
        if _b2.button("▶ 執行目前已到期項目", use_container_width=True):
            with st.spinner("V191 正在依排程執行到期項目；完成/失敗都會永久記錄..."):
                _run = run_auto_due_jobs()
            _has_warning = any(str(x.get("status")) == "WARNING" for x in (_run.get("executed") or []) if isinstance(x, dict))
            (st.warning if _has_warning or not _run.get("ok") else st.success)(_run.get("message"))
            if _run.get("executed"):
                st.dataframe(pd.DataFrame(_run.get("executed", [])), use_container_width=True, hide_index=True)
        _force_confirm = _b3.checkbox("允許本次強制跑全部已啟用工作", value=False, help="會包含第7頁股神推薦，可能需要數分鐘；只用於部署驗證。Hotfix2 使用獨立 FORCE 執行鍵，不會佔用今天尚未到時的正式排程時段。")
        if st.button("🧪 強制執行全部已啟用工作（部署驗證）", disabled=not _force_confirm, use_container_width=True):
            with st.spinner("V191 強制驗證執行中..."):
                _runall = run_auto_due_jobs(force_all_enabled=True)
            _has_warning_all = any(str(x.get("status")) == "WARNING" for x in (_runall.get("executed") or []) if isinstance(x, dict))
            (st.warning if _has_warning_all or not _runall.get("ok") else st.success)(_runall.get("message"))
            if _runall.get("executed"):
                st.dataframe(pd.DataFrame(_runall.get("executed", [])), use_container_width=True, hide_index=True)

        st.markdown("#### 排程狀態 / 成功失敗訊息")
        _next_rows = auto_next_run_rows(_auto_cfg, _auto_status) if callable(auto_next_run_rows) else []
        if _next_rows:
            st.dataframe(pd.DataFrame(_next_rows), use_container_width=True, hide_index=True, height=480)
        _hist = load_auto_scheduler_history() if callable(load_auto_scheduler_history) else []
        with st.expander("最近自動更新履歷", expanded=False):
            if _hist:
                _hdf = pd.DataFrame(_hist[-120:][::-1])
                _cols = [c for c in ["finished_at", "job_label", "status", "duration_seconds", "attempt", "message"] if c in _hdf.columns]
                st.dataframe(_hdf[_cols], use_container_width=True, hide_index=True, height=420)
            else:
                st.caption("尚無 V191 自動執行履歷。")

        st.markdown("#### 已納入自動化的完整盤點")
        st.markdown(
            "- **00 大盤走勢**：加權指數、櫃買、三大法人、外盤/美盤、台指期/期貨、隔夜國際盤、market_snapshot、macro_mode_bridge、macro_trend_records。\n"
            "- **08 股神推薦紀錄**：最新價、推薦後績效、權威儲存同步；另納入 V188 T+1 實戰真相/機率校準。\n"
            "- **09 股票主檔**：股票主檔智慧更新並永久保存。\n"
            "- **10 推薦清單**：推薦後績效、正式 N 日績效、隔日命中追蹤及同步。\n"
            "- **16 官方因子**：法人/營收/PER/PBR/EPS 多來源快取＋V187來源可信度＋V190盤後時序治理；Hotfix2 另驗證官方內容業務日期，抓取成功但日期沒前進會列 FAILED 而非假 SUCCESS。\n"
            "- **補充納入**：SuperAI 融資券/期貨/PCR/ETF情境、AI績效回饋/每日學習、永久化失敗重試。\n"
            "- **07 股神推薦**：只在必要前置工作今日成功後，由 Page7 自己的 canonical runner 自動執行；中央排程不再複製選股邏輯。沿用 Page7 永久保存的掃描範圍、門檻、模式與權重，完成後仍同步永久寫入 08 股神推薦紀錄。"
        )
        st.caption("不自動化的按鈕：清除快取、清空紀錄、重置設定、強制修復/編譯測試等維護或破壞性操作，仍保留人工確認。")

with st.sidebar:
    st.header("V191 操作")
    do_check = st.button("🔍 重新健康檢查", use_container_width=True, type="primary")
    do_repair = st.button("🛠 一鍵安全修復缺檔/缺欄", use_container_width=True)
    do_compile = st.button("🧪 執行編譯煙霧測試", use_container_width=True)
    do_durability = st.button("🧾 稽核所有核心資料永久化", use_container_width=True)
    do_migrate_durable = st.button("📦 將既有權威資料排入永久化", use_container_width=True, help="只排程尚未以相同Hash完成遠端確認的既有核心JSON；本機先保留，GitHub/Firestore背景同步。")
    do_retry_durable = st.button("🔁 重試失敗/待同步永久化", use_container_width=True)
    do_super_context = st.button("🧠 更新SuperAI融資/期貨/ETF情境", use_container_width=True)
    st.divider()
    st.subheader("舊版官方因子排程（V191相容設定）")
    st.caption("V191中央排程啟用後，建議由上方中央排程統一控制；此區保留舊設定與手動更新功能，不再需要另一套獨立cron。")
    cfg = load_schedule_settings()
    schedule_options = ["21:00", "21:30", "22:00", "22:30", "23:00", "23:30"]
    limit_options = [0, 200, 500, 1000, 1500, 2000]
    market_options = ["全部", "上市", "上櫃"]

    def _first_valid_time(value):
        try:
            t = (value or ["23:00"])[0]
        except Exception:
            t = "23:00"
        return t if t in schedule_options else "23:00"

    def _valid_limit(value):
        try:
            v = int(value or 0)
        except Exception:
            v = 0
        return v if v in limit_options else 0

    def _valid_market(value):
        v = str(value or "全部")
        return v if v in market_options else "全部"

    widget_defaults = {
        "official_schedule_enabled": bool(cfg.get("enabled", True)),
        "official_schedule_time": _first_valid_time(cfg.get("times")),
        "official_schedule_weekdays_only": bool(cfg.get("weekdays_only", True)),
        "official_schedule_market_filter": _valid_market(cfg.get("market_filter", "全部")),
        "official_schedule_limit": _valid_limit(cfg.get("limit", 0)),
        "official_schedule_include_institutional": bool(cfg.get("include_institutional", True)),
        "official_schedule_include_revenue": bool(cfg.get("include_revenue", True)),
        "official_schedule_include_valuation": bool(cfg.get("include_valuation", True)),
    }
    for _k, _v in widget_defaults.items():
        if _k not in st.session_state:
            st.session_state[_k] = _v

    enabled = st.checkbox("啟用官方因子自動更新", key="official_schedule_enabled")
    schedule_time = st.selectbox("預計更新時間（台灣）", schedule_options, key="official_schedule_time")
    weekdays_only = st.checkbox("僅週一至週五", key="official_schedule_weekdays_only")
    market_filter = st.selectbox("更新市場", market_options, key="official_schedule_market_filter")
    limit = st.selectbox("更新筆數限制", limit_options, key="official_schedule_limit", help="0 = 全部股票")
    include_institutional = st.checkbox("更新法人", key="official_schedule_include_institutional")
    include_revenue = st.checkbox("更新營收", key="official_schedule_include_revenue")
    include_valuation = st.checkbox("更新 PER / PBR / EPS", key="official_schedule_include_valuation")

    saved_time = (cfg.get("last_saved_at") or cfg.get("updated_at") or "尚未保存")
    st.caption(f"目前讀取設定：{_first_valid_time(cfg.get('times'))}｜最後保存：{saved_time}")

    if st.button("💾 套用官方因子排程設定（本機 + GitHub 永久保存）", use_container_width=True):
        new_cfg = dict(DEFAULT_SCHEDULE_SETTINGS)
        new_cfg.update({
            "enabled": bool(st.session_state["official_schedule_enabled"]),
            "times": [str(st.session_state["official_schedule_time"])],
            "weekdays_only": bool(st.session_state["official_schedule_weekdays_only"]),
            "market_filter": str(st.session_state["official_schedule_market_filter"]),
            "limit": int(st.session_state["official_schedule_limit"]),
            "include_institutional": bool(st.session_state["official_schedule_include_institutional"]),
            "include_revenue": bool(st.session_state["official_schedule_include_revenue"]),
            "include_valuation": bool(st.session_state["official_schedule_include_valuation"]),
        })
        ok, msg = save_schedule_settings(new_cfg)
        if ok:
            st.success("已保存官方因子排程設定。")
            if "未設定 GITHUB_TOKEN" in msg or "GitHub" in msg:
                st.info(msg)
        else:
            st.error(msg)
    if st.button("⚡ 立即手動更新官方因子快取", use_container_width=True):
        with st.spinner("正在更新官方因子快取..."):
            _manual_cfg = dict(load_schedule_settings() or {})
            _manual_cfg["enabled"] = True
            result = run_official_factor_update_once(_manual_cfg, push_github=True)
        if result.get("ok"):
            st.success(result.get("message"))
            if result.get("github_msg"):
                st.info(result.get("github_msg"))
        else:
            st.error(result.get("message"))


    st.divider()
    st.subheader("股神推薦全域更新")
    global_cfg = load_global_update_settings()
    g_update_stock_master = st.checkbox("更新股票主檔", value=bool(global_cfg.get("update_stock_master", True)))
    g_update_macro = st.checkbox("更新大盤快照 / 股神橋接", value=bool(global_cfg.get("update_macro", True)))
    g_update_official = st.checkbox("更新官方因子快取", value=bool(global_cfg.get("update_official_factors", True)))
    g_update_watchlist = st.checkbox("同步自選股 runtime 檔", value=bool(global_cfg.get("update_watchlist_runtime", True)))
    g_update_perf = st.checkbox("更新推薦紀錄/清單最新價與績效", value=bool(global_cfg.get("update_performance", True)))
    g_repair_schema = st.checkbox("先安全補缺檔/缺欄", value=bool(global_cfg.get("repair_schema", True)))
    g_process_all_perf = st.checkbox("績效完整更新全部紀錄（較慢）", value=bool(global_cfg.get("process_all_performance", False)))
    g_max_records = st.selectbox("智慧增量模式單次最多更新筆數", [80, 150, 300, 500, 800, 1200], index=[80, 150, 300, 500, 800, 1200].index(int(global_cfg.get("max_records", 300)) if int(global_cfg.get("max_records", 300)) in [80, 150, 300, 500, 800, 1200] else 300))
    g_batch_limit = st.selectbox("每批更新股票數", [30, 50, 80, 120, 200], index=[30, 50, 80, 120, 200].index(int(global_cfg.get("batch_limit", 80)) if int(global_cfg.get("batch_limit", 80)) in [30, 50, 80, 120, 200] else 80))
    g_stale_minutes = st.selectbox("推薦績效幾分鐘內已更新則略過", [0, 15, 30, 60, 120, 360], index=[0, 15, 30, 60, 120, 360].index(int(global_cfg.get("stale_minutes", 30)) if int(global_cfg.get("stale_minutes", 30)) in [0, 15, 30, 60, 120, 360] else 30))
    g_force_source_refresh = st.checkbox("手動強制全部來源重抓（疑難排除才勾）", value=bool(global_cfg.get("force_source_refresh", False)), help="正常不必勾。新版智慧略過會驗證內容日期、完整度與外資欄位；只要內容落後或異常便自動強制重抓，不會只看檔案時間。")
    with st.expander("⏱️ 網路來源逾時保護（建議維持預設）", expanded=False):
        g_macro_timeout = st.selectbox("大盤快照/橋接最長執行秒數", [20, 30, 35, 45, 60], index=[20, 30, 35, 45, 60].index(int(global_cfg.get("macro_max_runtime_seconds", 35)) if int(global_cfg.get("macro_max_runtime_seconds", 35)) in [20, 30, 35, 45, 60] else 35))
        g_official_timeout = st.selectbox("官方因子最長執行秒數", [45, 60, 75, 90, 120], index=[45, 60, 75, 90, 120].index(int(global_cfg.get("official_max_runtime_seconds", 75)) if int(global_cfg.get("official_max_runtime_seconds", 75)) in [45, 60, 75, 90, 120] else 75))
        g_official_requests = st.selectbox("官方/備援本輪最多網路請求", [24, 36, 48, 60, 90], index=[24, 36, 48, 60, 90].index(int(global_cfg.get("official_max_requests", 48)) if int(global_cfg.get("official_max_requests", 48)) in [24, 36, 48, 60, 90] else 48))
        g_official_quick = st.checkbox("一鍵更新使用快速安全模式（FinMind僅批次、不逐檔）", value=bool(global_cfg.get("official_quick_mode", True)), help="避免120檔×多資料集逐檔請求造成數十分鐘卡住。完整補值交由第16頁或排程分批執行。")
    g_rebuild_feedback = st.checkbox("重建績效回饋與精準度摘要", value=bool(global_cfg.get("rebuild_feedback_profile", True)))
    g_invalidate_cache = st.checkbox("更新後清除所有模組舊表格快取", value=bool(global_cfg.get("invalidate_runtime_caches", True)))
    g_push_github = st.checkbox("更新後背景同步 GitHub（不阻塞按鈕）", value=bool(global_cfg.get("push_github", True)))
    if st.button("💾 套用全域更新設定（永久保存）", use_container_width=True):
        ok, msg = save_global_update_settings({
            "update_stock_master": g_update_stock_master,
            "update_macro": g_update_macro,
            "update_official_factors": g_update_official,
            "update_watchlist_runtime": g_update_watchlist,
            "update_performance": g_update_perf,
            "repair_schema": g_repair_schema,
            "process_all_performance": g_process_all_perf,
            "max_records": g_max_records,
            "batch_limit": g_batch_limit,
            "stale_minutes": g_stale_minutes,
            "force_source_refresh": g_force_source_refresh,
            "rebuild_feedback_profile": g_rebuild_feedback,
            "invalidate_runtime_caches": g_invalidate_cache,
            "push_github": g_push_github,
            "macro_max_runtime_seconds": g_macro_timeout,
            "official_max_runtime_seconds": g_official_timeout,
            "official_max_requests": g_official_requests,
            "official_quick_mode": g_official_quick,
        })
        if ok:
            st.success("已永久保存全域更新設定。")
        else:
            st.error(msg)
    run_global_update_now = st.button("🚀 一鍵依序更新股神所需資訊", use_container_width=True, type="primary", help="沿用本按鈕依序更新：核心→股票主檔→大盤→官方因子→自選股→最新推薦/清單/紀錄績效→績效回饋→推薦就緒度→全模組表格快取。")
    if st.button("🧹 解除逾時更新鎖定", use_container_width=True, help="只在畫面長時間卡住且已確認沒有其他人正在更新時使用。"):
        removed = []
        for lock_name in [".godpick_global_update.lock", "macro_startup_update.lock"]:
            lock_path = Path(__file__).resolve().parents[1] / lock_name
            try:
                if lock_path.exists():
                    lock_path.unlink()
                    removed.append(lock_name)
            except Exception as exc:
                st.warning(f"無法移除 {lock_name}：{exc}")
        if removed:
            st.success("已解除逾時鎖定：" + "、".join(removed))
        else:
            st.info("目前沒有殘留更新鎖定。")

# V183｜核心資料永久化與SuperAI市場情境。
if 'do_super_context' in locals() and do_super_context:
    if callable(refresh_super_ai_market_context):
        with st.spinner("正在更新 TWSE/TPEx 融資券、TAIFEX 期貨/PCR 與 ETF 情境..."):
            _ctx, _ctx_msgs = refresh_super_ai_market_context(fetch_etf=True)
        st.success(f"SuperAI市場情境更新完成｜融資個股 {len((_ctx or {}).get('margin_by_stock', {}))} 筆")
        for _m in (_ctx_msgs or [])[:12]: st.caption(str(_m))
    else:
        st.error("SuperAI市場情境服務未載入。")

if 'do_migrate_durable' in locals() and do_migrate_durable:
    if callable(queue_existing_critical_for_migration):
        _mig_msgs = queue_existing_critical_for_migration(base_dir=Path(__file__).resolve().parents[1], critical_only=True)
        _queued = sum('queued' in str(x) for x in _mig_msgs)
        st.info(f"既有核心權威資料已排入永久化：{_queued} 項。遠端以Hash完成確認前仍標示待同步。")
        for _m in _mig_msgs[:24]: st.caption(str(_m))
    else:
        st.error("永久化遷移服務未載入。")

if 'do_retry_durable' in locals() and do_retry_durable:
    if callable(retry_failed_durability):
        _retry_msgs = retry_failed_durability(base_dir=Path(__file__).resolve().parents[1])
        st.info(f"已重新排程 {len(_retry_msgs)} 個待同步/失敗項目。")
        for _m in _retry_msgs[:20]: st.caption(_m)
    else:
        st.error("永久化服務未載入。")

if callable(audit_core_durability):
    _durability_audit = audit_core_durability(base_dir=Path(__file__).resolve().parents[1], write_audit=True)
    with st.expander("V183｜核心資料永久保存稽核", expanded=bool('do_durability' in locals() and do_durability)):
        _d1, _d2, _d3 = st.columns(3)
        _d1.metric("核心資料本機存在", f"{_durability_audit.get('critical_local', 0)}/{_durability_audit.get('critical_total', 0)}")
        _d2.metric("遠端Hash已確認", f"{_durability_audit.get('critical_remote_confirmed', 0)}/{_durability_audit.get('critical_total', 0)}")
        _d3.metric("遠端確認率", f"{_durability_audit.get('critical_remote_confirmed_rate_pct', 0):.1f}%")
        _audit_df = pd.DataFrame(_durability_audit.get('rows', []))
        if not _audit_df.empty:
            st.dataframe(_audit_df[[c for c in ['file','critical','purpose','status','remote_status','bytes'] if c in _audit_df.columns]], use_container_width=True, hide_index=True)
        st.caption("只有本機 payload hash 與成功遠端同步 hash 一致才標示 REMOTE_CONFIRMED；不再把『檔案存在』冒充永久保存。")

if 'run_global_update_now' in locals() and run_global_update_now:
    global_settings = {
        "update_stock_master": g_update_stock_master,
        "update_macro": g_update_macro,
        "update_official_factors": g_update_official,
        "update_watchlist_runtime": g_update_watchlist,
        "update_performance": g_update_perf,
        "repair_schema": g_repair_schema,
        "process_all_performance": g_process_all_perf,
        "max_records": g_max_records,
        "batch_limit": g_batch_limit,
        "stale_minutes": g_stale_minutes,
        "force_source_refresh": g_force_source_refresh,
        "rebuild_feedback_profile": g_rebuild_feedback,
        "invalidate_runtime_caches": g_invalidate_cache,
        "push_github": g_push_github,
        "macro_max_runtime_seconds": g_macro_timeout,
        "official_max_runtime_seconds": g_official_timeout,
        "official_max_requests": g_official_requests,
        "official_quick_mode": g_official_quick,
    }
    progress_box = st.empty()
    progress_table = st.empty()
    def _on_global_progress(row, all_rows):
        progress_box.info(f"{row.get('步驟')}｜{row.get('狀態')}｜{row.get('說明')}｜{row.get('耗時秒', 0)} 秒")
        progress_table.dataframe(pd.DataFrame(all_rows).drop(columns=["明細"], errors="ignore"), use_container_width=True, hide_index=True)
    with st.status("正在更新全模組資料；大盤與官方因子均有硬性逾時保護，不會無限運轉...", expanded=True) as update_status:
        global_result = run_global_update(Path(__file__).resolve().parents[1], global_settings, progress_callback=_on_global_progress)
        if global_result.get("message") and not global_result.get("steps"):
            update_status.update(label=global_result.get("message"), state="error", expanded=True)
        else:
            update_status.update(label="全模組資料更新流程已完成", state="complete", expanded=False)
    if g_invalidate_cache:
        try:
            st.cache_data.clear()
        except Exception:
            pass
    steps_df = pd.DataFrame(global_result.get("steps", []))
    ok_count = int((steps_df.get("狀態") == "OK").sum()) if not steps_df.empty and "狀態" in steps_df.columns else 0
    fail_count = int((steps_df.get("狀態") != "OK").sum()) if not steps_df.empty and "狀態" in steps_df.columns else 0
    if fail_count == 0:
        st.success(f"全域更新完成：{ok_count} 個步驟成功。")
    else:
        st.warning(f"全域更新完成：{ok_count} 個步驟成功，{fail_count} 個步驟需檢查。")
    st.dataframe(steps_df.drop(columns=["明細"], errors="ignore"), use_container_width=True, hide_index=True)
    try:
        audit_step = next((x for x in global_result.get("steps", []) if "資料鏈完整性檢查" in str(x.get("步驟", ""))), {})
        audit_detail = audit_step.get("明細", {}) if isinstance(audit_step, dict) else {}
        audit_rows = audit_detail.get("audit_rows", []) if isinstance(audit_detail, dict) else []
        if audit_rows:
            st.subheader("7／8 股神推薦資料鏈實際更新檢查")
            st.dataframe(pd.DataFrame(audit_rows), use_container_width=True, hide_index=True)
    except Exception:
        pass
    with st.expander("全域更新詳細明細", expanded=False):
        st.json(global_result)
    readiness = global_result.get("recommendation_readiness", {}) if isinstance(global_result, dict) else {}
    if readiness:
        st.subheader("股神推薦資料就緒度")
        r1, r2, r3 = st.columns(3)
        r1.metric("就緒度", f"{readiness.get('score', 0)}/{readiness.get('full_score', 100)}")
        r2.metric("狀態", readiness.get("status", ""))
        r3.metric("建議", readiness.get("recommended_action", ""))
        st.dataframe(pd.DataFrame(readiness.get("checks", [])), use_container_width=True, hide_index=True)

if do_repair:
    with st.spinner("正在備份並安全修復..."):
        repair = full_safe_repair(Path(__file__).resolve().parents[1])
    st.success("一鍵安全修復完成。")
    with st.expander("修復明細", expanded=True):
        st.write("備份")
        st.dataframe(pd.DataFrame(repair.get("backup_rows", [])), use_container_width=True)
        st.write("核心檔案")
        st.dataframe(pd.DataFrame(repair.get("core_rows", [])), use_container_width=True)
        st.write("欄位修復")
        st.dataframe(pd.DataFrame(repair.get("schema_rows", [])), use_container_width=True)

if do_compile:
    with st.spinner("正在執行編譯煙霧測試..."):
        comp = run_compile_smoke_test(Path(__file__).resolve().parents[1])
    if comp.get("ok"):
        st.success("編譯煙霧測試通過。")
    else:
        st.error("編譯煙霧測試失敗。")
        st.code(comp.get("stderr", ""))

health = run_health_check(Path(__file__).resolve().parents[1])
summary = health.get("summary", {})
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("整體狀態", summary.get("整體狀態", ""))
c2.metric("正常", summary.get("正常", 0))
c3.metric("注意", summary.get("注意", 0))
c4.metric("異常", summary.get("異常", 0))
c5.metric("檢查時間", summary.get("檢查時間", ""))

rows = pd.DataFrame(health.get("rows", []))
if not rows.empty:
    status_filter = st.multiselect("狀態篩選", sorted(rows["狀態"].dropna().unique().tolist()), default=sorted(rows["狀態"].dropna().unique().tolist()))
    group_filter = st.multiselect("群組篩選", sorted(rows["群組"].dropna().unique().tolist()), default=sorted(rows["群組"].dropna().unique().tolist()))
    view = rows[rows["狀態"].isin(status_filter) & rows["群組"].isin(group_filter)].copy()
    st.dataframe(view, use_container_width=True, hide_index=True)
else:
    st.info("尚無健康檢查資料。")

st.divider()
st.subheader("V171 全模組資料填入 / 更新方式 / 永久設定檢查")
all_status = check_all_module_data_status(Path(__file__).resolve().parents[1])
asum = all_status.get("summary", {})
a1, a2, a3, a4 = st.columns(4)
a1.metric("資料檔案", asum.get("資料檔案數", 0))
a2.metric("正常", asum.get("正常", 0))
a3.metric("注意", asum.get("注意", 0))
a4.metric("異常", asum.get("異常", 0))

file_status_df = pd.DataFrame(all_status.get("file_rows", []))
module_plan_df = pd.DataFrame(all_status.get("module_rows", []))
setting_status_df = pd.DataFrame(all_status.get("setting_rows", []))

with st.expander("一、哪些資料沒有填入 / 為什麼 / 怎麼更新", expanded=True):
    st.dataframe(file_status_df, use_container_width=True, hide_index=True)

with st.expander("二、每個模組哪些資訊會由全域更新、哪些仍需手動", expanded=True):
    st.dataframe(module_plan_df, use_container_width=True, hide_index=True)

with st.expander("三、每個模組參數永久保存檢查", expanded=True):
    st.dataframe(setting_status_df, use_container_width=True, hide_index=True)

with st.expander("V191 無人值守排程架構說明", expanded=False):
    st.markdown("""
- V191 使用 `.github/workflows/godpick_auto_scheduler_v191.yml` 每 10 分鐘喚醒一次中央 due-check；真正工作時間仍由本頁永久設定的 Asia/Taipei 時間決定。
- 所有更新結果與成功/失敗訊息寫入 `godpick_auto_scheduler_status.json` / `godpick_auto_scheduler_history.json`，並同步 `runtime-data`。
- 舊 `.github/workflows/update_official_factors_v112.yml` 已改為 **只保留手動 emergency dispatch**，避免和中央排程重複抓官方因子。
- 自動股神推薦不是固定時間硬跑：只有股票主檔、大盤、官方因子、SuperAI 市場情境等前置工作於同日成功後才會執行。
- 清除快取、刪除紀錄、恢復預設、編譯/修復等維護性或破壞性動作不會被自動排程。
""")
    if callable(load_auto_scheduler_settings):
        st.code(json.dumps(load_auto_scheduler_settings(), ensure_ascii=False, indent=2), language="json")
