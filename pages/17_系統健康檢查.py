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

st.set_page_config(page_title="17_系統健康檢查", layout="wide")
inject_pro_theme()

st.title("17_系統健康檢查與一鍵修復中心")
st.caption("V112｜檢查 07 / 10 / 8 / 14 / 16 串接、JSON 欄位、官方因子快取與官方因子自動更新排程。")

with st.sidebar:
    st.header("V112 操作")
    do_check = st.button("🔍 重新健康檢查", use_container_width=True, type="primary")
    do_repair = st.button("🛠 一鍵安全修復缺檔/缺欄", use_container_width=True)
    do_compile = st.button("🧪 執行編譯煙霧測試", use_container_width=True)
    st.divider()
    st.subheader("官方因子自動更新排程")
    cfg = load_schedule_settings()
    enabled = st.checkbox("啟用官方因子自動更新", value=bool(cfg.get("enabled", True)))
    schedule_time = st.selectbox("預計更新時間（台灣）", ["21:30", "22:00", "22:30", "23:00", "23:30"], index=["21:30", "22:00", "22:30", "23:00", "23:30"].index((cfg.get("times") or ["23:00"])[0] if (cfg.get("times") or ["23:00"])[0] in ["21:30", "22:00", "22:30", "23:00", "23:30"] else "23:00"))
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

with st.expander("官方因子自動更新排程說明", expanded=False):
    st.markdown("""
- Streamlit 頁面本身不會背景常駐執行排程，避免拖慢 07/10/8/14。
- V112 已加入 GitHub Actions workflow：`.github/workflows/update_official_factors_v112.yml`。
- 預設排程為台灣時間約 23:00；實際 cron 使用 UTC 15:00。
- GitHub Actions 會執行 `tools/update_official_factors_scheduled.py`，更新 `official_factors_cache.json` 後自動 commit/push。
- 若排程設定停用，workflow 仍會被觸發，但腳本會直接略過更新。
""")
    st.code(json.dumps(load_schedule_settings(), ensure_ascii=False, indent=2), language="json")
