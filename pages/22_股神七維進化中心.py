# -*- coding: utf-8 -*-
from __future__ import annotations

import pandas as pd
import streamlit as st

from godpick_h93_evolution_engine import VERSION, load_state_safe, refresh_evolution_state
from godpick_h93_evolution_settings import load_settings_safe, save_settings_safe

st.set_page_config(page_title="股神七維進化中心", layout="wide")
st.title("22. 股神七維進化中心｜H93")
st.caption("深入研究 × 學習計畫 × 顧問模式 × 系統優化 × 成長機會 × 持續優化 × 效率。H93只影響研究排序，不建立Formal權限。")

cfg=load_settings_safe(); state=load_state_safe()
cols=st.columns(4)
cols[0].metric("版本",VERSION.split("_")[1] if "_" in VERSION else VERSION)
cols[1].metric("成熟樣本",int(state.get("mature_samples") or 0))
cols[2].metric("學習狀態",state.get("learning_status") or "WARMUP")
cols[3].metric("Formal治理","LOCKED")

st.subheader("七大方向")
rows=[
    ["1 深入研究","市場定價、基本面、技術、新聞、官方因子交叉驗證；區分已反映/可能未反映。"],
    ["2 學習計畫","只用成熟乾淨樣本；Selection與Execution分開學，代理績效不得調門檻。"],
    ["3 顧問模式","輸出執行/等待買點/成長研究/條件式顧問，附成立與失效條件。"],
    ["4 優化系統方案","抓資料新鮮度、覆蓋、學習治理、價格結構瓶頸，先修根因。"],
    ["5 成長機會","營收/EPS/族群/相對強度/法人/催化整合，不把題材直接當買進。"],
    ["6 持續優化","T+1/T+5/T+10閉環檢討，追蹤PF、回撤、MFE/MAE與機會成本。"],
    ["7 提高效率","Local-first、永久快照、有限補算；避免開頁/Excel重跑全市場。"],
]
st.dataframe(pd.DataFrame(rows,columns=["方向","執行原則"]),use_container_width=True,hide_index=True)

with st.expander("七維權重設定",expanded=False):
    weights=dict(cfg.get("weights") or {})
    labels={"deep_research":"深入研究","learning_plan":"學習計畫","advisor_mode":"顧問模式","system_optimization":"系統優化","growth_opportunity":"成長機會","continuous_improvement":"持續優化","efficiency":"提高效率"}
    new={}
    c=st.columns(4)
    for i,(key,label) in enumerate(labels.items()):
        new[key]=c[i%4].number_input(label,min_value=0.0,max_value=100.0,value=float(weights.get(key,0)),step=1.0,key=f"h93_w_{key}")
    st.caption("儲存時會自動正規化為100%；Formal權限鎖不可關閉。")
    if st.button("永久保存H93設定",type="primary"):
        cfg["weights"]=new
        ok,msg=save_settings_safe(cfg)
        (st.success if ok else st.error)(msg)

st.subheader("持續學習狀態")
st.json(state or {"status":"尚未建立H93學習狀態；可在股神推薦完成後自動建立。"})
if st.button("重新整理H93學習狀態"):
    state=refresh_evolution_state(persist=True)
    st.success(f"完成｜成熟樣本 {state.get('mature_samples',0)}｜{state.get('learning_status','WARMUP')}")
    st.rerun()
