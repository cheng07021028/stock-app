# -*- coding: utf-8 -*-
"""H136 remote research hub audit; read-only, no credentials displayed."""
from __future__ import annotations
from datetime import datetime
from zoneinfo import ZoneInfo

import streamlit as st
from app_auth import require_login
require_login()
from godpick_h136_auto_hub import remote_cache, VERSION

st.set_page_config(page_title='27｜AI自動資料中樞', layout='wide')
st.title('27｜H136 AI 自動資料中樞')
st.caption(f'{VERSION}｜盤後排程 × 遠端封存 × 行情對帳 × 自動研究輸入')
st.warning('只整合已有 H134 H126 決策封存及 H117 實際盤後資料。缺少資料就顯示未就緒；不自行產生買進訊號，也不回補假交易日。')
st.page_link('pages/26_訊號生命週期與策略審查.py', label='前往 H135｜自動策略審查', icon='📈')
st.page_link('pages/25_主管智慧決策戰情.py', label='前往 H134｜建立研究封存', icon='📊')

@st.cache_data(ttl=1800, show_spinner=False)
def _data():
    return remote_cache(asof=datetime.now(ZoneInfo('Asia/Taipei')).date())

if st.button('重新檢查 GitHub runtime-data 研究資料', type='primary', key='h136_refresh'):
    _data.clear()
frames, meta = _data()
st.write('**遠端狀態：**', meta.get('status', 'UNKNOWN'))
if not frames:
    st.error('未取得經 SHA-256 核對且最新一次排程有效的研究資料集。')
    st.caption('請先檢查 GitHub Actions → H136 nightly read-only research data bridge，確認 runtime-data 分支存在 H134封存與 H117盤後CSV。')
    st.json({k: v for k, v in meta.items() if k in ('status', 'missing_file', 'last_run_day', 'reason')})
else:
    columns = st.columns(4)
    for col, label, key in zip(columns, ['歷史研究交易日', 'A／B 研究訊號', '個股日 K', '大盤日 K'],
                               ['signal_days','signal_rows','stock_rows','benchmark_rows']):
        col.metric(label, int(meta.get(key, 0)))
    st.caption('研究資料日：' + str(meta.get('asof')) + '｜最新股票：' + str(meta.get('latest_stock_date')) +
               '｜最新大盤：' + str(meta.get('latest_benchmark_date')))
    st.caption('市場資料來源：' + str(meta.get('market_source')) + '；訊號事前證明：' + str(meta.get('PIT')))
    st.info('檔案內容已作遠端 SHA-256 回讀檢查；原始來源、PIT時間及除權息仍須額外驗證。')
    with st.expander('檢視 H136 資料品質及可追溯性'):
        st.json({k: v for k, v in meta.items() if k != 'files'})
    for key, name in [('signals','研究訊號'),('stock','個股日K'),('benchmark','大盤日K')]:
        with st.expander(name + '｜預覽前 15 筆'):
            st.dataframe(frames[key].head(15), use_container_width=True, hide_index=True)
st.caption('如果盤後封存未建立、GitHub Actions 無權限或H117官方資料缺漏，H136不會替代人工創造訊號；個人持股與實際成交需要另外授權。')


st.divider()
st.subheader('H137｜決策封存 × 夜間同步異常告警')
st.caption('GitHub Actions 每個工作日台灣時間23:00做盤後檢查；僅為內部稽核，並非獨立PIT認證或真正不可修改的存證。')
from godpick_h137_auto_seal import read_remote_audit
if st.button('重新檢查 H137 決策封存與同步稽核', key='h137_audit_refresh'):
    st.session_state.pop('h137_latest_audit', None)
if 'h137_latest_audit' not in st.session_state:
    st.session_state['h137_latest_audit'] = read_remote_audit(
        asof=datetime.now(ZoneInfo('Asia/Taipei')).date())
audit = st.session_state['h137_latest_audit']
if audit.get('status') == 'INTERNAL_CHECK_PASS_NOT_PIT':
    st.success('H137 最近稽核：內部檢查通過（不代表獨立時間存證）')
elif audit.get('status') == 'BLOCKED_REQUIRES_ATTENTION':
    st.error('H137 最近稽核：決策封存或自動同步有缺漏，需查明原因')
else:
    st.warning('H137 稽核尚未取得最新有效資訊：' + str(audit.get('status')))
st.caption('稽核資料日：' + str(audit.get('asof_tw', audit.get('last_day', 'UNKNOWN')))
           + '｜當日有效主管決策封存：' + str(audit.get('same_day_live_h126_archives', 'UNKNOWN')))
if audit.get('findings'):
    st.write('需處理：' + '；'.join(audit['findings']))
st.caption('若當天沒有人在第7頁建立主管Excel，系統不會憑空製造H126決策；H137將呈現未封存警示。')
