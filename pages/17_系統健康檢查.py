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

st.set_page_config(page_title="17_系統健康檢查", layout="wide")
inject_pro_theme()

st.title("17_系統健康檢查 / 全模組一鍵更新中心")
st.caption("V140｜新增全模組空白資料檢查、股神推薦前置資料一鍵依序更新、永久設定檢查。")

with st.sidebar:
    st.header("V112 操作")
    do_check = st.button("🔍 重新健康檢查", use_container_width=True, type="primary")
    do_repair = st.button("🛠 一鍵安全修復缺檔/缺欄", use_container_width=True)
    do_compile = st.button("🧪 執行編譯煙霧測試", use_container_width=True)
    st.divider()
    st.subheader("官方因子自動更新排程")
    cfg = load_schedule_settings()
    enabled = st.checkbox("啟用官方因子自動更新", value=bool(cfg.get("enabled", True)))
    schedule_options = ["21:00", "21:30", "22:00", "22:30", "23:00", "23:30"]
    saved_schedule_time = (cfg.get("times") or ["23:00"])[0]
    if saved_schedule_time not in schedule_options:
        saved_schedule_time = "23:00"
    schedule_time = st.selectbox("預計更新時間（台灣）", schedule_options, index=schedule_options.index(saved_schedule_time))
    weekdays_only = st.checkbox("僅週一至週五", value=bool(cfg.get("weekdays_only", True)))
    market_filter = st.selectbox("更新市場", ["全部", "上市", "上櫃"], index=["全部", "上市", "上櫃"].index(cfg.get("market_filter", "全部") if cfg.get("market_filter", "全部") in ["全部", "上市", "上櫃"] else "全部"))
    limit = st.selectbox("更新筆數限制", [0, 200, 500, 1000, 1500, 2000], index=[0, 200, 500, 1000, 1500, 2000].index(int(cfg.get("limit") or 0) if int(cfg.get("limit") or 0) in [0, 200, 500, 1000, 1500, 2000] else 0), help="0 = 全部股票")
    include_institutional = st.checkbox("更新法人", value=bool(cfg.get("include_institutional", True)))
    include_revenue = st.checkbox("更新營收", value=bool(cfg.get("include_revenue", True)))
    include_valuation = st.checkbox("更新 PER / PBR / EPS", value=bool(cfg.get("include_valuation", True)))
    if st.button("💾 套用官方因子排程設定（永久設定）", use_container_width=True):
        new_cfg = dict(DEFAULT_SCHEDULE_SETTINGS)
        new_cfg.update({
            "enabled": enabled,
            "times": [schedule_time],
            "weekdays_only": weekdays_only,
            "market_filter": market_filter,
            "limit": limit,
            "include_institutional": include_institutional,
            "include_revenue": include_revenue,
            "include_valuation": include_valuation,
        })
        ok, msg = save_schedule_settings(new_cfg)
        if ok:
            st.success("已保存官方因子排程設定。")
        else:
            st.error(msg)
    if st.button("⚡ 立即手動更新官方因子快取", use_container_width=True):
        with st.spinner("正在更新官方因子快取..."):
            result = run_official_factor_update_once(load_schedule_settings(), push_github=True)
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
    g_max_records = st.selectbox("非完整模式掃描最近筆數", [80, 150, 300, 500, 800, 1200], index=[80, 150, 300, 500, 800, 1200].index(int(global_cfg.get("max_records", 300)) if int(global_cfg.get("max_records", 300)) in [80, 150, 300, 500, 800, 1200] else 300))
    g_batch_limit = st.selectbox("每批更新股票數", [30, 50, 80, 120, 200], index=[30, 50, 80, 120, 200].index(int(global_cfg.get("batch_limit", 80)) if int(global_cfg.get("batch_limit", 80)) in [30, 50, 80, 120, 200] else 80))
    g_stale_minutes = st.selectbox("幾分鐘內已更新則略過", [0, 15, 30, 60, 120, 360], index=[0, 15, 30, 60, 120, 360].index(int(global_cfg.get("stale_minutes", 30)) if int(global_cfg.get("stale_minutes", 30)) in [0, 15, 30, 60, 120, 360] else 30))
    g_push_github = st.checkbox("更新後同步 GitHub（有 token 才會成功）", value=bool(global_cfg.get("push_github", True)))
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
            "push_github": g_push_github,
        })
        if ok:
            st.success("已永久保存全域更新設定。")
        else:
            st.error(msg)
    run_global_update_now = st.button("🚀 一鍵依序更新股神所需資訊", use_container_width=True, type="primary", help="依序更新：核心檔案→主檔→大盤→官方因子→自選股→推薦紀錄/清單績效→全模組檢查。")

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
        "push_github": g_push_github,
    }
    with st.spinner("正在依序更新股神推薦所需資訊，單一步驟失敗不會覆蓋既有有效資料..."):
        global_result = run_global_update(Path(__file__).resolve().parents[1], global_settings)
    steps_df = pd.DataFrame(global_result.get("steps", []))
    ok_count = int((steps_df.get("狀態") == "OK").sum()) if not steps_df.empty and "狀態" in steps_df.columns else 0
    fail_count = int((steps_df.get("狀態") != "OK").sum()) if not steps_df.empty and "狀態" in steps_df.columns else 0
    if fail_count == 0:
        st.success(f"全域更新完成：{ok_count} 個步驟成功。")
    else:
        st.warning(f"全域更新完成：{ok_count} 個步驟成功，{fail_count} 個步驟需檢查。")
    st.dataframe(steps_df.drop(columns=["明細"], errors="ignore"), use_container_width=True, hide_index=True)
    with st.expander("全域更新詳細明細", expanded=False):
        st.json(global_result)

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
st.subheader("V140 全模組資料填入 / 更新方式 / 永久設定檢查")
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

with st.expander("官方因子自動更新排程說明", expanded=False):
    st.markdown("""
- Streamlit 頁面本身不會背景常駐執行排程，避免拖慢 07/10/8/14。
- V112 已加入 GitHub Actions workflow：`.github/workflows/update_official_factors_v112.yml`。
- 預設排程為台灣時間約 23:00；實際 cron 使用 UTC 15:00。
- GitHub Actions 會執行 `tools/update_official_factors_scheduled.py`，更新 `official_factors_cache.json` 後自動 commit/push。
- 若排程設定停用，workflow 仍會被觸發，但腳本會直接略過更新。
""")
    st.code(json.dumps(load_schedule_settings(), ensure_ascii=False, indent=2), language="json")
