# -*- coding: utf-8 -*-
from pathlib import Path
import streamlit as st
import pandas as pd
st.set_page_config(page_title='H195｜超級AI股神決策總指揮中心',layout='wide')
from app_auth import require_login
require_login()
from godpick_h195_command_center import VERSION,TIERS,load,workbook,render_inline
from godpick_h200_integrated import ordered, VERSION as H200_VERSION
from godpick_h211_counterevidence import ordered as h211_ordered, FIELDS as H211_FIELDS
try:
    from godpick_bi_theme import install_bi_dashboard_style
    install_bi_dashboard_style('33_h195')
except Exception:pass
root=Path(__file__).resolve().parents[1]
st.title('33｜超級AI股神決策總指揮中心')
st.caption(H200_VERSION+'｜'+VERSION+'｜自動封存、只讀資料；H126 正式授權維持獨立。')
for c,p,label in zip(st.columns(4),['7_股神推薦.py','29_主流趨勢與潛力黑馬.py','30_財報成長與合理估值.py','32_隔日預測與AI績效學習.py'],['正式推薦與風控','技術研究','財報研究','績效與預測']):c.page_link('pages/'+p,label=label)
render_inline(root)
try:
    from godpick_h206_market_gap import render as _h206_render
    _h206_render(root)
except Exception as _h206_exc:
    st.caption('H206 稽核暫不可用：'+type(_h206_exc).__name__)
b,s,msg=load(root)
if not b:st.info('請確認 GitHub Actions H195 已執行並提交 data/h195/latest.json.gz。');st.stop()
if s!='CURRENT':
    st.warning('以下為歷史快照，不是目前研究或當日預測。')
    if not st.checkbox('查看歷史快照（僅供查核）'):st.stop()
if b.get('h211'):
    st.subheader('H211｜完整反證與重複研究清單')
    for tab,t in zip(st.tabs(list(TIERS)),TIERS):
        with tab:
            rr=h211_ordered([r for r in b['rows'] if r['研究分級']==t])
            st.dataframe(pd.DataFrame(rr).reindex(columns=['股票代號','股票名稱','H200研究優先序',*H211_FIELDS]),hide_index=True,use_container_width=True)
for tab,t in zip(st.tabs(list(TIERS)),TIERS):
    with tab:
        df=pd.DataFrame(ordered([r for r in b['rows'] if r['研究分級']==t]));st.dataframe(df,hide_index=True,use_container_width=True) if not df.empty else st.info('此池未找到具備對應證據的股票。')
st.subheader('研究機會雷達');st.dataframe(pd.DataFrame([{'研究池':k,'檔數':v} for k,v in b['radar'].items()]),hide_index=True,use_container_width=True)
st.subheader('十二研究員與反方查核')
items={(r['市場'],r['股票代號']):r for r in b['reviews']}
if items:
    k=st.selectbox('選擇已封存詳細研究',list(items),format_func=lambda k:' '.join(k))
    r=items[k]
    if r.get('error'):st.warning(r['error'])
    for o in r['roles']:
        with st.expander(o['id']+'｜'+o['status']):
            st.write(o['conclusion']);st.write(o['gaps']);st.json(o['metrics']);st.caption('來源：'+'；'.join(o['provenance']))
            for name,rows in o['tables'].items():st.caption(name);st.dataframe(pd.DataFrame(rows),hide_index=True,use_container_width=True)
st.page_link('pages/31_十二研究員聯合決策.py',label='其他股票完整十二研究員研究')
st.subheader('H194 真實歷史績效（未還原價格，非投資獲利）')
p=b['performance'];st.write({k:p.get(k) for k in ('signals','mature','pending','prediction_metrics')})
st.dataframe(pd.DataFrame(p.get('metrics',[])),hide_index=True,use_container_width=True)
with st.expander('資料健康、覆蓋率及錯誤',expanded=not b['technical_current']):st.json(b['health']);st.json(b['errors'])
if st.checkbox('準備 H195 決策 Excel'):st.download_button('下載 H195 決策報表',workbook(root),'H195_CommandCenter.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
st.caption('每日 23:55 及 08:45 台灣時間自動更新；開頁及下載不重新掃描或逐檔連網。')
