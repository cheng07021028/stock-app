# -*- coding: utf-8 -*-
"""V191-H184 | 28 十二研究員決策研究中心 (additive Streamlit page)."""
from __future__ import annotations
import io
import json
import hashlib
import zipfile
from pathlib import Path
from datetime import datetime, timezone, timedelta

import pandas as pd
import streamlit as st

try:
    from app_auth import require_login
    require_login()
except Exception as e:
    st.error(f'登入權限驗證失敗：{type(e).__name__}')
    st.stop()

try:
    from utils import inject_pro_theme
except ImportError:
    def inject_pro_theme():return None

from godpick_h184_research_lab import (
    VERSION, ROLES, SHEETS, run_role, normalize_frame, research_manifest, export_workbook, export_event_ics,
)

st.set_page_config(page_title='28｜十二研究員決策研究中心',layout='wide')
inject_pro_theme()
st.title('28｜十二研究員決策研究中心')
st.caption(f'{VERSION}｜把研究角色變成可驗收的資料與分析，不替代現有股神推薦模組')
st.info('研究層與 H109／H126 正式交易授權完全隔離；新資料僅供研究。缺來源、期間或公告時點不補值，不給保證漲跌或勝率。')

# Uploaded bytes and each resulting report live in this Streamlit browser session.
# Intentionally no silent writes to the production recommendation records or remote GitHub/Firestore.
def _sample_template():
    from openpyxl import Workbook
    wb=Workbook();wb.remove(wb.active)
    for name,cols in SHEETS.items():
        s=wb.create_sheet(name);s.append(list(cols));s.freeze_panes='A2'
        for c in s[1]:c.font=__import__('openpyxl').styles.Font(bold=True,color='FFFFFF');c.fill=__import__('openpyxl').styles.PatternFill('solid',fgColor='18334E')
        for col in 'ABCDEFGH':s.column_dimensions[col].width=21
    out=io.BytesIO();wb.save(out);return out.getvalue()

@st.cache_data(show_spinner=False,max_entries=8)
def _parse_upload(raw:bytes,filename:str)->dict[str,pd.DataFrame]:
    if len(raw)>12_000_000:raise ValueError('單檔上限12MB，請先依研究期間縮小資料')
    if filename.lower().endswith('.csv'):
        for encoding in ('utf-8-sig','big5'):
            try:return {'csv':pd.read_csv(io.BytesIO(raw),encoding=encoding,dtype={'code':'string','股票代號':'string'})}
            except UnicodeDecodeError:continue
        raise ValueError('CSV 編碼無法讀取，請改UTF-8')
    if filename.lower().endswith('.xlsx'):
        # Guard decompression of large/hostile Office containers.
        with zipfile.ZipFile(io.BytesIO(raw)) as z:
            info=z.infolist()
            if len(info)>250 or sum(x.file_size for x in info)>60_000_000:
                raise ValueError('Excel 展開內容太大，請先減少工作表或資料列')
        sheets=pd.read_excel(io.BytesIO(raw),sheet_name=None,engine='openpyxl',dtype={'code':'string','股票代號':'string'})
        return {name:df for name,df in sheets.items() if not df.empty}
    raise ValueError('只支援 xlsx/csv')

with st.expander('01｜資料來源與 Excel／CSV 匯入',expanded=True):
    st.markdown('**可使用的資料集：**OHLCV、指數、財報、新聞事件、持股、交易日誌、研究證據、個股股價。資料不需要每次全部填寫，按研究角色使用。')
    st.download_button('下載12角色標準資料模板（8個工作表）',data=_sample_template(),
                       file_name='StockResearch_H184_Inputs.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    up=st.file_uploader('匯入資料檔（可多檔；CSV/XLSX）',type=['xlsx','csv'],accept_multiple_files=True,
                        help='匯入內容只供本次研究，不自動寫入推薦永久紀錄')
    datasets={}
    issues=[]
    for i,file in enumerate(up or []):
        blob=file.getvalue()
        try:
            contents=_parse_upload(blob,file.name)
            for sheet,frame in contents.items():
                first=str(sheet).lower().strip()
                matches=[k for k in SHEETS if first==k or first.startswith(k+'_')]
                default=matches[0] if matches else None
                keys=list(SHEETS)
                lab=st.selectbox(f'{file.name}｜工作表 {sheet} 對應資料類型',
                                 options=['不使用']+keys,index=1+keys.index(default) if default else 0,
                                 key=f'h184_type_{i}_{sheet}')
                if lab!='不使用':
                    obj=normalize_frame(frame)
                    datasets[lab]=pd.concat([datasets[lab],obj],ignore_index=True) if lab in datasets else obj
        except Exception as e:
            issues.append(f'{file.name} 無法讀取：{type(e).__name__} {str(e)[:160]}')
    for msg in issues:st.warning(msg)
    if datasets:
        st.caption('已載入 '+ '｜'.join(f'{key} {len(frame)}筆' for key,frame in datasets.items()))
        with st.expander('資料逐格預覽（僅顯示前20筆）'):
            for name,frame in datasets.items():
                st.markdown('**'+name+'**')
                st.dataframe(frame.head(20),width='stretch',hide_index=True)
    else:st.caption('目前沒有匯入檔案。第7角色可直接讀取系統既有 H115 官方營收／季度利潤率快照（若可取得）。')

# Evidence version guard: never re-label previous result as using newly uploaded files.
current_fingerprint={k:hashlib.sha256(f.to_json(orient='records',force_ascii=False).encode('utf-8')).hexdigest()
                     for k,f in datasets.items()}
prior_fingerprint=st.session_state.get('h184_dataset_fingerprint')
if prior_fingerprint is not None and prior_fingerprint!=current_fingerprint:
    for k in ROLES:
        st.session_state.pop('h184_result_'+k,None)
        st.session_state.pop('h184_options_'+k,None)
    st.warning('來源檔已變更，舊版研究結果已從本工作階段清除，請重新執行研究。')
st.session_state['h184_dataset_fingerprint']=current_fingerprint

role_opts=[f'{k}｜{v[0]}' for k,v in ROLES.items()]
selected=st.selectbox('02｜選擇研究角色',role_opts,index=0)
role=selected.split('｜',1)[0]
st.markdown('**研究任務：**'+ROLES[role][1])
try:
    contracts=json.loads((Path(__file__).resolve().parents[1]/'config/h184_role_contracts.json').read_text('utf-8'))
    selected_contract=next((v for v in contracts['roles'] if v['role_id']==role),None)
except (OSError,ValueError,KeyError):
    selected_contract=None
if selected_contract:
    st.caption('需要資料：'+ '／'.join(selected_contract['requires'])+'｜更新頻率：'+selected_contract['frequency'])
    with st.expander('研究員原始任務／資料契約與能力限制'):
        st.write(selected_contract['prompt'])
        st.write('**結果欄位**：'+'、'.join(selected_contract['outputs']))
        st.warning(selected_contract['status'])
opts={}
with st.form('h184_run_form'):
    if role in ('01','02','04','07','09','11','03'):
        a,b=st.columns(2)
        with a:opts['code']=st.text_input('股票代號（選填；有多檔行情時必填）',value='')
        with b:opts['asof']=st.date_input('研究截止日（歷史觀察時點）',value=datetime.now(timezone(timedelta(hours=8))).date()).isoformat()
    if role=='04':
        c1,c2,c3=st.columns(3)
        with c1:opts['fee_bps']=st.number_input('單邊手續費 bps',0.0,200.0,14.25,0.25)
        with c2:opts['tax_bps']=st.number_input('賣出交易稅 bps',0.0,200.0,30.0,0.25)
        with c3:opts['slippage_bps']=st.number_input('單邊滑價 bps',0.0,200.0,5.0,0.25)
    if role=='05':opts['cash_pct']=st.number_input('現金配置比例 %（請依真實持股輸入）',0.0,100.0,0.0,0.5)
    if role=='08':opts['thesis']=st.text_area('貼上投資論點（不會自動視為事實）',height=150)
    if role=='09':opts['period']=st.text_input('共同財報期間（不填則用最新一個期間）',value='')
    if role=='10':
        st.caption('簡化三年自由現金流估值：金額請全數使用相同單位；百分比欄位輸入 0～100')
        cols=st.columns(3)
        fields=[('revenue','起始營收（同單位）',0.),('growth','基準年營收成長率%',8.),
                ('margin','營業利益率%',15.),('tax','有效稅率%',20.),('capex','資本支出占營收%',5.),
                ('working_capital','營運資金增額占營收%',2.),('wacc','折現率 WACC%',10.),
                ('terminal_growth','終值成長率%',2.),('shares','流通股數（同估值單位）',0.),
                ('debt','有息負債（同單位）',0.),('cash','現金（同單位）',0.)]
        for idx,(key,label,default) in enumerate(fields):
            with cols[idx%3]:
                val=st.number_input(label,value=float(default),step=1.0 if idx in (0,8,9,10) else 0.5,format='%.3f')
                opts[key]=val/100 if key in ('growth','margin','tax','capex','working_capital','wacc','terminal_growth') else val
        opts['acknowledge_assumptions']=st.checkbox('我已逐項確認所有數字為本次研究假設，不代表官方財報或股價')
    run=st.form_submit_button('執行本角色研究／產生證據與缺口',type='primary',width='stretch')

if run:
    result=run_role(role,datasets,opts)
    st.session_state['h184_result_'+role]=result
    st.session_state['h184_options_'+role]=opts

outcomes={k:st.session_state.get('h184_result_'+k) for k in ROLES}
outcomes={k:v for k,v in outcomes.items() if v is not None}
if role in outcomes:
    r=outcomes[role]
    st.subheader(f'03｜{ROLES[role][0]} 結果')
    st.write('判定：**'+r.status+'**｜'+r.conclusion)
    if r.metrics:
        st.markdown('#### 可核對數據與參數')
        st.dataframe(pd.DataFrame([{'項目':k,'值':v} for k,v in r.metrics.items()]),width='stretch',hide_index=True)
    for name,frame in r.tables.items():
        st.markdown('#### '+name)
        st.dataframe(frame,width='stretch',hide_index=True)
        st.download_button('下載 '+name+' CSV',data=frame.to_csv(index=False).encode('utf-8-sig'),
                           file_name=f'StockResearch_H184_Role{role}_{name}.csv',mime='text/csv',key=f'h184_export_{role}_{name}')
    if role=='11':
        events_ics=export_event_ics(r)
        if events_ics:
            st.download_button('下載事件日曆 ICS（暫定事件，須核實）',data=events_ics,
                               file_name='StockResearch_H184_TentativeEvents.ics',mime='text/calendar')
    if r.gaps:
        st.markdown('#### 尚未核實／資料缺口')
        for gap in r.gaps:st.warning(gap)
    if r.provenance:
        st.markdown('**實際資料來源**：'+'；'.join(r.provenance))

st.divider()
st.subheader('04｜十二研究員研究總覽／獨立匯出')
st.caption('每個角色產生的研究結果彼此隔離；匯出不會改動既有股神推薦的14張工作表或 H126權限。')
nav=pd.DataFrame([{'角色':k,'研究員':v[0],'狀態':outcomes[k].status if k in outcomes else '尚未執行',
                   '缺口數':len(outcomes[k].gaps) if k in outcomes else None} for k,v in ROLES.items()])
st.dataframe(nav,width='stretch',hide_index=True)
if outcomes:
    options_for_report={k:st.session_state.get('h184_options_'+k,{}) for k in outcomes}
    manifest=research_manifest(datasets,{'active_role':role},outcomes)
    xlsx=export_workbook(outcomes,manifest)
    c1,c2=st.columns(2)
    with c1:st.download_button('下載12研究員稽核 Excel',data=xlsx,
                                file_name='StockResearch_V191_H184_12Roles.xlsx',
                                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',width='stretch')
    with c2:st.download_button('下載可重現結果 JSON（研究紀錄備份）',
                                data=json.dumps(manifest,ensure_ascii=False,indent=2).encode('utf-8'),
                                file_name='StockResearch_V191_H184_Research.json',mime='application/json',width='stretch')
    st.caption('注意：此頁研究結果在 Streamlit 工作階段內暫存；雲端 Reboot 不保證保存。請下載研究 JSON／Excel 歸檔，本版不會偷偷寫入正式推薦資料庫。')
