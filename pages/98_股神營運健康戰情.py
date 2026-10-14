# -*- coding: utf-8 -*-
"""H122 admin-only, read-only health; no secrets, no execution buttons."""
from __future__ import annotations
from pathlib import Path
import json
import streamlit as st
st.set_page_config(page_title='H122｜AI 股神營運健康戰情',layout='wide')
try:
    from app_auth import require_login, current_role
    require_login()
    if current_role() != 'admin':
        st.error('僅系統管理員可檢視營運健康戰情。')
        st.stop()
except Exception:
    st.error('無法驗證管理員權限；已拒絕顯示。')
    st.stop()

ROOT=Path(__file__).resolve().parents[1]
state_path=ROOT/'reports/h122/H122_dashboard_state.json'
st.title('AI 股神｜H122 營運健康戰情')
st.caption('只讀營運稽核，不自動重跑、不修改權重、不代表推薦績效已認證。')
if not state_path.is_file():
    st.warning('尚無 H122 夜間監控結果。請先部署並執行 H121/H122 GitHub Actions，再核對 GitHub 執行收據。')
    st.stop()
try:
    state=json.loads(state_path.read_text(encoding='utf-8'))
    if not isinstance(state,dict) or not isinstance(state.get('alert_codes'),list):
        raise ValueError('Invalid dashboard')
except (OSError, ValueError, UnicodeError):
    st.error('營運戰情資料損壞或格式不符；不可宣稱資料健康。')
    st.stop()

cols=st.columns(4)
cols[0].metric('健康狀態',str(state.get('status','UNKNOWN')))
cols[1].metric('三市場最新封存',str(state.get('data_freshness',{}).get('last_three_source_day') or '無'))
cols[2].metric('推薦快照批次',str(state.get('recommendation_snapshots') or 0))
cols[3].metric('H120核對交易日',str(state.get('h120_attested_days') or 0))
st.subheader('市場資料來源')
st.dataframe([{'來源':key,'最新封存交易日':state.get('data_freshness',{}).get('last_source_days',{}).get(key) or '待核對'} for key in ('TWSE','TPEX','TAIEX')],use_container_width=True,hide_index=True)
st.subheader('風險與待處理事項')
alerts=state.get('alert_codes',[])
if alerts:
    for code in alerts[:45]: st.warning(str(code)[:90])
else: st.info('本次尚無已記錄的問題，但不代表正式交易策略獲認證。')
st.caption('遠端保存狀態：'+str(state.get('persistence','REMOTE_UNVERIFIED')))
st.caption('工作流程：'+str(state.get('pipeline_steps',{})))
st.info('故障補跑限官方資料蒐集。系統會拒絕跳過企業行動、事前資料與正式買進風控的任何捷徑。')
