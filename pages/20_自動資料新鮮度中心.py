# -*- coding: utf-8 -*-
from __future__ import annotations
import streamlit as st
try:
 from app_auth import require_login;require_login()
except Exception as e:st.error(f'登入載入失敗：{e}');st.stop()
from godpick_h83_autofresh_settings import load_settings,save_settings,normalize_settings
from godpick_h83_autofresh import load_status,run_autofresh_preflight,repair_market_snapshot,VERSION
st.set_page_config(page_title='20_自動資料新鮮度中心',layout='wide');st.title('20. 自動資料新鮮度中心｜H83');st.caption('推薦前自動補更新；用業務日期與合理性驗證，而不是只看檔案有沒有被寫過。')
cfg,details=load_settings();status=load_status();snap=status.get('snapshot',{}) if isinstance(status,dict) else {};c=st.columns(5);c[0].metric('Formal資料','READY' if snap.get('formal_ready') else 'RESEARCH-ONLY');c[1].metric('市場日期',snap.get('market_date') or '未驗證');c[2].metric('官方因子',snap.get('official_date') or '未驗證');c[3].metric('主檔筆數',snap.get('stock_master_rows',0));c[4].metric('官方落後',snap.get('official_business_lag','—'))
if st.button('立即執行 H83 全自動前置更新',type='primary',use_container_width=True):
 with st.spinner('自動檢查與更新中...'):r=run_autofresh_preflight(reason='page20_force',force=True)
 (st.success if r.get('formal_ready') else st.warning)(r.get('message'));st.rerun()
if st.button('只修復市場日期／異常值語意',use_container_width=True):st.info(str(repair_market_snapshot(cfg,True)));st.rerun()
st.subheader('永久設定')
with st.form('h83_settings'):
 enabled=st.checkbox('啟用H83',value=cfg['enabled']);am=st.checkbox('手動推薦前自動更新',value=cfg['auto_refresh_before_manual_recommendation']);au=st.checkbox('排程推薦前自動更新',value=cfg['auto_refresh_before_scheduled_recommendation']);st.checkbox('關鍵資料未通過時Formal降為研究模式（固定）',value=True,disabled=True);news=st.checkbox('自動新聞快取',value=cfg['auto_refresh_news']);learn=st.checkbox('自動重建H82學習',value=cfg['auto_refresh_h82_learning']);a,b,c,d=st.columns(4);tm=a.number_input('大盤TTL分鐘',15,1440,int(cfg['ttl_minutes']['macro_full']));ts=b.number_input('情境TTL分鐘',30,1440,int(cfg['ttl_minutes']['super_ai_context']));tp=c.number_input('績效TTL分鐘',30,1440,int(cfg['ttl_minutes']['performance_feedback']));tn=d.number_input('新聞TTL分鐘',15,1440,int(cfg['ttl_minutes']['news']));save=st.form_submit_button('永久保存H83設定',type='primary')
if save:
 n=normalize_settings(cfg);n.update({'enabled':enabled,'auto_refresh_before_manual_recommendation':am,'auto_refresh_before_scheduled_recommendation':au,'auto_refresh_news':news,'auto_refresh_h82_learning':learn});n['ttl_minutes'].update({'macro_full':tm,'super_ai_context':ts,'performance_feedback':tp,'news':tn});ok,msg,_=save_settings(n);(st.success if ok else st.error)(msg)
with st.expander('永久權威與狀態',expanded=False):st.write(details);st.json(status);st.caption(VERSION)
