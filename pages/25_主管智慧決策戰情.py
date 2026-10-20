# -*- coding: utf-8 -*-
"""H134 management dashboard and explicit GitHub create-only decision archive."""
from __future__ import annotations
from datetime import datetime
from zoneinfo import ZoneInfo
import json
import pandas as pd
import streamlit as st

from app_auth import require_login, current_role
require_login()
from godpick_h131_data_bridge import read_h126_workbook
from godpick_h134_decision_vault import (
    VERSION, build_snapshot, summarize, archive_once, list_recent,
    read_archive, snapshot_signals, compare_decisions,
)

st.set_page_config(page_title='H134｜主管智慧決策戰情', layout='wide')
st.title('25｜主管智慧決策戰情中心')
st.page_link('pages/26_訊號生命週期與策略審查.py', label='前往 H135｜訊號生命週期與策略優勝劣汰', icon='📈')
st.page_link('pages/27_AI自動資料中樞.py', label='H136｜自動資料中樞同步狀態', icon='🔄')
st.caption(f'{VERSION}｜研究權威只讀看板 × 遠端只新增封存 × 回讀驗證｜不改正式買進資格／不下單')
st.warning('H126「A級」是認證研究、不是買進許可。GitHub 只新增＋雜湊回讀能偵測改動，但不能防止有權限的管理員刪除、改寫歷史或變更 GitHub 時間。尚非獨立驗證的 PIT 證據。')
now_tw = datetime.now(ZoneInfo('Asia/Taipei'))

source = ''
frame = pd.DataFrame()
transferred = st.session_state.get('h130_h126_current_authority')
if isinstance(transferred, pd.DataFrame) and not transferred.empty:
    frame = transferred.copy(deep=True)
    source = 'LIVE_SESSION_H126'
    st.caption('來源：第7頁同一工作階段生成的H126決策。原始建立時間：' +
               str(st.session_state.get('h130_h126_manager_built_at') or 'UNKNOWN') +
               '（工作階段時鐘不構成獨立可信時間戳）')

with st.expander('讀取本期 H126 決策｜不接受舊 H102 排名冒充', expanded=frame.empty):
    upload = st.file_uploader('選擇 H126 主管決策 Excel／CSV（有檔案會覆蓋本頁暫存畫面，但不更動正式推薦）',
                              type=['xlsx', 'csv'], key='h134_h126_file')
    if upload is not None:
        frame, reason = read_h126_workbook(upload.getvalue(), upload.name)
        source = 'UPLOADED_H126_UNVERIFIED'
        st.caption(f'匯入狀態：{reason}；匯入的歷史報表一律不等於事前不可變證據。')
    elif not source:
        st.info('請先在第7頁建立本期H126主管決策，或提供含 00_H126主管決策 的Excel。')

if not frame.empty and {'股票代號', 'H126權威層別'}.issubset(frame.columns):
    metrics = summarize(frame[frame['股票代號'].fillna('').astype(str).str.fullmatch(r'\d{4}')])
    cols = st.columns(5)
    for widget, key in zip(cols, ('研究股', 'A級研究', 'B級觀察', '禁止新倉', '法人待驗證')):
        widget.metric(key, metrics[key])
    st.caption('最新候選僅是這次輸入的 H126 名單；B/C/禁止新倉不代表正式交易授權。')
    h126_health = st.session_state.get('h134_h126_health') if source == 'LIVE_SESSION_H126' else None
    if isinstance(h126_health, pd.DataFrame) and {'項目','數值'}.issubset(h126_health.columns):
        found = h126_health.loc[h126_health['項目'] == '原正式可執行表個股數（不代表H126批准）','數值']
        if not found.empty:
            st.caption('原正式交易計畫股票數（不代表H126重新批准）：' + str(found.iloc[-1]))
    st.dataframe(frame, hide_index=True, use_container_width=True)
    st.markdown('#### 決策證據缺口')
    st.write(f"H123 法人未實證：{metrics['法人待驗證']} 檔｜H124 TDCC 未實證：{metrics['TDCC待驗證']} 檔")
    if 'H126缺失與處置' in frame:
        st.dataframe(frame[['股票代號','H126權威層別','H126缺失與處置']].head(40),
                     hide_index=True, use_container_width=True)

st.divider()
st.subheader('H134｜研究決策遠端封存（手動補救／複核）')
st.caption('H137 已於管理員新建立第7頁主管Excel時嘗試自動只新增封存；本頁手動功能供遠端失敗或研究補登時使用。')
st.caption('每一份封存以 H126 研究日、來源類型與實際欄位內容計算固定 SHA-256。遠端以 GitHub runtime-data 分支建立新檔；若同名已存在，只驗證、永不覆寫。')
st.caption('單純將Excel匯入、或事後補登都只能記錄為補登，不會提升為原始研究日的 PIT 已驗證證據。')
research_day = st.date_input('研究日（非匯出時間；請與當日H126研究資料核對）',
                              value=now_tw.date(), max_value=now_tw.date(), key='h134_research_day')
bundle = None
if not frame.empty and source:
    try:
        bundle = build_snapshot(frame, research_day=research_day, source=source, captured_at=now_tw)
        st.info('預計封存：' + bundle['path'] + '｜資料新鮮度：' + bundle['payload']['freshness'] +
                '｜PIT狀態：' + bundle['payload']['pit_status'])
    except ValueError as exc:
        st.error(f'不可封存：{exc}')
else:
    st.caption('尚無可封存的H126主管決策資料。')

admin = current_role() == 'admin'
if not admin:
    st.info('只有管理員可將研究決策寫入遠端；一般登入使用者只能檢視及下載自己的研究資料。')
ack = st.checkbox('我已核對研究日、證據截止日，並了解這是只新增的研究封存，非買進授權／非獨立PIT證明',
                  value=False, key='h134_ack')
if st.button('H134｜建立遠端封存並回讀驗證', type='primary',
             disabled=(bundle is None or not admin or not ack), key='h134_archive'):
    with st.spinner('正在嘗試GitHub只新增保存及回讀SHA驗證…'):
        receipt = archive_once(bundle)
    st.session_state['h134_receipt'] = receipt
    if receipt['confirmed']:
        st.success('遠端回讀雜湊已確認：' + receipt['status'])
    else:
        st.error('遠端封存未確認：' + receipt['status'] + '。不能宣稱永久保存或Reboot後存在。')

receipt = st.session_state.get('h134_receipt')
if isinstance(receipt, dict):
    st.caption(f"上一次操作：{receipt.get('status')}｜{receipt.get('path')}")

st.divider()
st.subheader('遠端歷史封存查核｜不使用本機 JSON 冒充')
if st.button('重新查詢 GitHub 歷史封存及 SHA 驗證', key='h134_reload'):
    with st.spinner('正在讀取遠端快照清單與個別內容…'):
        items, status = list_recent()
    st.session_state['h134_listing'] = (items, status)
listing = st.session_state.get('h134_listing')
if listing:
    items, status = listing
    st.caption('清單查詢結果：' + status + '（最多檢查最新12份；並非全量檔案盤點）')
    if items:
        st.dataframe(pd.DataFrame(items), hide_index=True, use_container_width=True)
        available = [entry['封存路徑'] for entry in items if entry['遠端驗證'] == 'READBACK_HASH_VERIFIED']
        selected = st.selectbox('選擇已通過遠端SHA校驗的封存', available, key='h134_selected') if available else None
        if selected and st.button('打開遠端封存逐筆檢視', key='h134_open'):
            payload, state = read_archive(selected)
            st.session_state['h134_selected_payload'] = payload if state == 'READBACK_HASH_VERIFIED' else None
            st.session_state['h134_selected_status'] = state
            st.session_state['h134_selected_path'] = selected
        payload = st.session_state.get('h134_selected_payload')
        if (payload and st.session_state.get('h134_selected_status') == 'READBACK_HASH_VERIFIED'
                and st.session_state.get('h134_selected_path') == selected):
            st.caption('研究日：' + str(payload['research_day']) + '｜保存：' + str(payload['captured_at_tw']) +
                       '｜來源：' + str(payload['source_type']) + '｜' + str(payload['pit_status']))
            st.dataframe(pd.DataFrame(payload['rows']), hide_index=True, use_container_width=True)
            if not frame.empty and {'股票代號','H126權威層別'}.issubset(frame.columns):
                changes = compare_decisions(frame, pd.DataFrame(payload['rows']))
                st.markdown('**本期 H126 與所選封存的研究層別比較（不是交易績效）**')
                st.dataframe(changes, hide_index=True, use_container_width=True)
            st.download_button('下載此封存 JSON（保存遠端證據欄位）',
                               json.dumps(payload, ensure_ascii=False, indent=2).encode('utf-8'),
                               file_name='H134_Archive.json', mime='application/json', key='h134_download_arch')
        if st.button('產生 H133 多日期研究訊號草稿（仍非 PIT 驗證）', key='h134_signal_export'):
            archives = []
            for path in available:
                payload, state = read_archive(path)
                if state == 'READBACK_HASH_VERIFIED':
                    archives.append(payload)
            st.session_state['h134_signal_data'] = snapshot_signals(archives)
        sig = st.session_state.get('h134_signal_data')
        if isinstance(sig, pd.DataFrame) and not sig.empty:
            st.warning('H133 回測仍需獨立查驗事前訊號與調整行情。GitHub 保存時間無法自動證明該訊號於研究收盤之前已存在。')
            st.dataframe(sig.head(30), use_container_width=True, hide_index=True)
            safe = sig.copy()
            for col in safe.select_dtypes(include='object'):
                safe[col] = safe[col].map(lambda x: "'" + x if isinstance(x,str) and x.lstrip().startswith(('=','+','-','@')) else x)
            st.download_button('下載H133歷史研究訊號草稿CSV', safe.to_csv(index=False).encode('utf-8-sig'),
                               file_name='H134_H133_Signals.csv', mime='text/csv', key='h134_dl_signals')
    else:
        st.info('尚無遠端可查封存，或GitHub目前未配置/拒絕讀取。')

st.caption('此頁不會自動同步、刪除或改動第7／8頁及正式推薦權威；封存須手動按鍵。管理員可操作GitHub本身，故不保證法律或稽核意義上的不可變。')
