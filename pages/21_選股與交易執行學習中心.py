# -*- coding: utf-8 -*-
from __future__ import annotations
import streamlit as st

try:
    from app_auth import require_login
    require_login()
except Exception as e:
    st.error(f"登入載入失敗：{e}")
    st.stop()

from godpick_h89_execution_settings import (
    VERSION as SETTINGS_VERSION,
    load_settings_safe,
    load_settings,
    save_settings,
    normalize_settings,
)
from godpick_h89_selection_execution_core import (
    VERSION as CORE_VERSION,
    load_learning_state,
    refresh_learning_state,
)

st.set_page_config(page_title="21_選股與交易執行學習中心", layout="wide")
st.title("21. 選股 × 交易執行學習中心｜H89")
st.caption("把『選對股票』與『買點/停損/目標設計』分開檢討。H89只改善研究排序與價格計畫，不建立H64/H68 Formal權限。")

cfg = load_settings_safe()
state = load_learning_state()
sel = state.get("selection", {}) if isinstance(state, dict) else {}
exe = state.get("execution", {}) if isinstance(state, dict) else {}
policy = state.get("execution_policy", {}) if isinstance(state, dict) else {}

c = st.columns(6)
c[0].metric("H89狀態", "ACTIVE" if state.get("available") else "WARMUP")
c[1].metric("乾淨成熟樣本", int(state.get("clean_samples", 0) or 0))
c[2].metric("排除可疑代理", int(state.get("excluded_suspicious_proxy", 0) or 0))
c[3].metric("選股成功率", f"{float(sel.get('success_rate',0) or 0)*100:.1f}%")
c[4].metric("錯失機會率", f"{float(exe.get('opportunity_cost_rate',0) or 0)*100:.1f}%")
c[5].metric("進場積極度校正", f"{float(policy.get('entry_bias_pct',0) or 0):+.2f}%")

b1,b2 = st.columns(2)
if b1.button("重新建立 H89 選股/執行學習狀態", type="primary", use_container_width=True):
    with st.spinner("只使用成熟且非可疑代理樣本重建 H89 學習..."):
        s, msgs = refresh_learning_state(persist_remote=True)
    st.success(f"H89學習已重建：乾淨樣本 {int(s.get('clean_samples',0) or 0)}")
    for m in msgs:
        st.caption(str(m))
    st.rerun()

if b2.button("從永久權威重新載入 H89 設定", use_container_width=True):
    with st.spinner("同步永久設定..."):
        remote, details = load_settings()
    st.session_state["h89_remote_settings"] = remote
    st.success("H89永久設定已讀取；下方表單會以遠端版本為基準。")
    if details:
        st.caption("｜".join(str(x) for x in details[-3:]))
    st.rerun()

base_cfg = st.session_state.pop("h89_remote_settings", None) or cfg
base_cfg = normalize_settings(base_cfg)

st.subheader("交易計畫治理")
with st.form("h89_settings"):
    enabled = st.checkbox("啟用H89雙軌研究層", value=bool(base_cfg.get("enabled", True)))
    t = base_cfg["trade_plan"]
    a,b,c,d = st.columns(4)
    formal_rr = a.number_input("Formal最低成本後RR", 1.0, 4.0, float(t["formal_min_net_rr"]), 0.05)
    research_rr = b.number_input("研究計畫最低成本後RR", 0.8, 3.0, float(t["research_min_net_rr"]), 0.05)
    max_stop = c.number_input("最大停損距離%", 2.0, 20.0, float(t["max_stop_distance_pct"]), 0.25)
    max_pullback = d.number_input("最大等待拉回%", 2.0, 25.0, float(t["max_pullback_from_reference_pct"]), 0.5)
    e,f,g = st.columns(3)
    model_up = e.number_input("研究模型目標最大上行%", 3.0, 40.0, float(t["max_model_target_upside_pct"]), 0.5)
    zone_min = f.number_input("進場區最小寬度%", 0.1, 3.0, float(t["min_entry_zone_width_pct"]), 0.05)
    zone_max = g.number_input("進場區最大寬度%", 0.2, 5.0, float(t["max_entry_zone_width_pct"]), 0.05)
    st.checkbox("禁止為了湊RR而移動停損（固定）", value=True, disabled=True)
    st.checkbox("Formal必須有真實結構目標；模型目標只能研究（固定）", value=True, disabled=True)

    st.markdown("#### 學習治理")
    l = base_cfg["learning"]
    x1,x2,x3,x4 = st.columns(4)
    horizon = x1.selectbox("主要成熟週期", [1,3,5,10,20], index=[1,3,5,10,20].index(int(l["primary_horizon"])))
    min_clean = x2.number_input("最低乾淨成熟樣本", 10, 1000, int(l["minimum_clean_samples"]), 5)
    min_seg = x3.number_input("最低分群樣本", 5, 500, int(l["minimum_segment_samples"]), 1)
    max_adj = x4.number_input("研究排序最大加減分", 0.0, 3.0, float(l["max_rank_adjustment_points"]), 0.1)
    y1,y2 = st.columns(2)
    opp = y1.number_input("錯失機會判定報酬%", 1.0, 30.0, float(l["opportunity_cost_return_pct"]), 0.5)
    max_bias = y2.number_input("進場積極度最大校正%", 0.0, 3.0, float(l["max_entry_bias_pct"]), 0.1)
    st.checkbox("可疑代理樣本直接排除，不凍結整套學習（固定）", value=True, disabled=True)

    save = st.form_submit_button("永久保存 H89 設定", type="primary", use_container_width=True)

if save:
    new = normalize_settings(base_cfg)
    new["enabled"] = enabled
    new["trade_plan"].update({
        "formal_min_net_rr": formal_rr,
        "research_min_net_rr": research_rr,
        "max_stop_distance_pct": max_stop,
        "max_pullback_from_reference_pct": max_pullback,
        "max_model_target_upside_pct": model_up,
        "min_entry_zone_width_pct": zone_min,
        "max_entry_zone_width_pct": zone_max,
    })
    new["learning"].update({
        "primary_horizon": horizon,
        "minimum_clean_samples": min_clean,
        "minimum_segment_samples": min_seg,
        "max_rank_adjustment_points": max_adj,
        "opportunity_cost_return_pct": opp,
        "max_entry_bias_pct": max_bias,
        "exclude_suspicious_proxy_instead_of_freezing_all": True,
    })
    ok,msg,_ = save_settings(new)
    (st.success if ok else st.error)(msg)

with st.expander("H89學習狀態明細", expanded=False):
    st.json(state)

st.caption(f"Core: {CORE_VERSION}｜Settings: {SETTINGS_VERSION}")
