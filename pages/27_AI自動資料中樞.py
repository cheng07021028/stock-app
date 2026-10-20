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
