# -*- coding: utf-8 -*-
from pathlib import Path
from datetime import datetime
import pandas as pd
import streamlit as st
st.set_page_config(page_title='H194｜隔日預測與AI績效學習',layout='wide')
from app_auth import require_login
require_login()
from godpick_h194_performance import VERSION,CLASSES,read_ledger,Market,models,report,excel,ar
try:
    from godpick_bi_theme import install_bi_dashboard_style
    install_bi_dashboard_style('32_h194')
except Exception:pass
st.title('32｜隔日預測與 AI 績效學習')
st.caption(VERSION+'｜研究用原始收盤價格，非含息／可成交投資報酬，不修改買進授權。')
st.page_link('pages/31_十二研究員聯合決策.py',label='H193 十二研究員聯合決策',icon='🧠')
root=Path(__file__).resolve().parents[1]
@st.cache_data(ttl=120,show_spinner=False)
def load(path):
    root=Path(path);now=datetime.now(ar.TZ);ss,oo=read_ledger(root);m=Market(root,now)
    r=report(ss,oo,m,now,models(ss,oo,now))
    p=root/'data/h194/latest.json.gz';health=ar._read(p) if p.is_file() else {}
    return r,health,m.errors
if st.button('重新讀取已封存績效'):load.clear()
with st.spinner('讀取已核實研究及成熟績效…'):
    try:r,health,errors=load(str(root))
    except Exception as e:st.error('封存資料驗證失敗：'+str(e)[:180]);st.stop()
st.caption('最後排程：'+str(health.get('asof','尚未建立'))+'｜'+str(health.get('capture_status','等待首次自動封存')))
if errors:st.warning('行情封存存在錯誤；不可新增成熟結果。');st.json(errors[:30])
for c,k,t in zip(st.columns(4),('signals','mature','pending','prediction_metrics'),('已封存研究筆數','成熟T+N筆數','待成熟／補證','有效隔日機率樣本')):c.metric(t,r[k] if k!='prediction_metrics' else r[k]['mature_forecasts'])
if not r['signals']:st.info('尚無同日有效研究快照。請先確認 H190/H192/H193 自動資料與 H194 Actions；不會回填或虛構歷史推薦。')
if not r['prediction_metrics']['mature_forecasts']:st.warning('目前尚無已成熟的有效隔日機率；樣本不足不預測。')
a,b,c,d=st.tabs(['隔日預測','策略績效','逐筆T+N','模型驗證'])
with a:
    rows=[]
    for s in r['latest_signals']:
        p=s['forecast'];rows.append({'研究日':s['signal_day'],'市場':s['market'],'股票代號':s['code'],'股票名稱':s['name'],'分級':s['tier'],'原始收盤價':s['base_close'],**{k+'機率%':round(p[i]*100,2) if p is not None else None for i,k in enumerate(CLASSES)},'預測狀態':s['forecast_status']})
    st.caption('盤整定義：原始收盤價格變動在 ±0.5% 內。只顯示當時凍結的研究機率；不事後重算歷史預測。')
    st.dataframe(pd.DataFrame(rows),hide_index=True,use_container_width=True)
with b:
    st.caption('策略可能重疊，不可加總為組合績效；同市場 TAIEX 僅供上市股票比較，上櫃缺官方指數不計超額。')
    st.dataframe(pd.DataFrame(r['metrics']),hide_index=True,use_container_width=True)
with c:st.dataframe(pd.DataFrame(r['outcomes']),hide_index=True,use_container_width=True)
with d:
    st.write(r['prediction_metrics']);st.dataframe(pd.DataFrame(r['calibration']),hide_index=True,use_container_width=True);st.dataframe(pd.DataFrame([{'分組':k,**v} for k,v in r['models'].items()]),hide_index=True,use_container_width=True)
    st.caption('至少20個訓練日/100筆、10個留出日/50筆；訓練結果必須在留出日之前已被觀測。訓練及留出成熟覆蓋率至少95%；Brier低於同市場研究母體基準至少0.01才發布研究機率。這不是已驗證獲利策略。')
with st.expander('最近自動執行診斷'):st.json({k:v for k,v in health.items() if k not in ('models','metrics')})
if st.checkbox('產生完整研究績效Excel'):st.download_button('下載 H194 研究績效',excel(r),'H194_Performance.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
st.caption('每日22:55與23:45 台灣時間 Actions 封存，延遲跨日不回填。頁面僅讀取 GitHub 已同步資料；不逐檔連網、不改原有推薦紀錄。')

st.divider()
st.subheader('H197｜六大選股策略實戰比較與複核')
st.caption('只讀取 H194 當時封存的研究與成熟結果；不重跑選股、不調整正式權重。')
@st.cache_data(ttl=120,show_spinner=False)
def load_h197(path):
    from godpick_h197_strategy_review import run
    return run(Path(path))
if st.checkbox('載入 H197 六策略比較（本機封存資料）'):
    try:
        if st.button('重新整理 H197'):load_h197.clear()
        with st.spinner('核對封存與同日對照…'):review=load_h197(str(root))
        st.caption(review['basis'])
        if review['status']=='DATA_PENDING':st.info('尚無足夠成熟研究，不能判斷策略優劣。')
        if review['archive_errors']:st.warning('行情封存異常，暫停策略評估。')
        st.dataframe(pd.DataFrame(review['evaluation']),hide_index=True,use_container_width=True)
        st.caption('主要 T+5：非重疊交易窗；至少20個訓練窗、10個留出窗，訓練結果在留出前已觀測。初始複核門檻並非統計顯著或獲利保證。')
        st.dataframe(pd.DataFrame(review['summary']),hide_index=True,use_container_width=True)
        with st.expander('逐日對照與資料覆蓋'):st.dataframe(pd.DataFrame(review['daily']),hide_index=True,use_container_width=True)
        if st.checkbox('產生 H197 策略 Excel'):
            from godpick_h197_strategy_review import excel as excel_h197
            st.download_button('下載 H197 策略比較',excel_h197(review),'H197_StrategyReview.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:st.error('H197 封存資料驗證失敗：'+str(e)[:180])

st.divider()
st.subheader('H199｜AI權重學習與市場輪動適應')
st.caption('獨立研究建議；未通過時序留出驗證時維持1.0倍。正式推薦排名與買進授權不受影響。')
@st.cache_data(ttl=300,show_spinner=False)
def load_h199(path):
    from godpick_h199_adaptive_weights import run
    return run(Path(path))
if st.checkbox('載入 H199 市場輪動與權重研究'):
    try:
        if st.button('重新整理 H199'):load_h199.clear()
        with st.spinner('讀取封存行情與前瞻績效…'):adaptive=load_h199(str(root))
        st.caption(adaptive['asof']+'｜'+adaptive['basis'])
        if adaptive['status']=='BASELINE_ONLY':st.info('目前維持原研究權重；尚未通過市場狀態與留出驗證。')
        if adaptive['archive_errors']:st.warning('行情封存異常，禁止調高權重。')
        st.dataframe(pd.DataFrame(adaptive['current_regime']),hide_index=True,use_container_width=True)
        st.dataframe(pd.DataFrame(adaptive['recommendations']),hide_index=True,use_container_width=True)
        with st.expander('時序留出驗證'):st.dataframe(pd.DataFrame(adaptive['models']),hide_index=True,use_container_width=True)
        st.markdown('**產業相對強度變化及成交額活躍度**')
        if not adaptive['rotation']:st.info('產業分類／連續行情覆蓋不足，暫無可信輪動比較。')
        st.dataframe(pd.DataFrame(adaptive['rotation']),hide_index=True,use_container_width=True)
        with st.expander('重複研究監控（不扣分）'):
            st.dataframe(pd.DataFrame(adaptive['repetition_summary']),hide_index=True,use_container_width=True)
            st.dataframe(pd.DataFrame(adaptive['repetition']),hide_index=True,use_container_width=True)
        if st.checkbox('產生 H199 權重研究 Excel'):
            from godpick_h199_adaptive_weights import excel as excel_h199
            st.download_button('下載 H199 權重研究',excel_h199(adaptive),'H199_AdaptiveWeights.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:st.error('H199 研究驗證失敗：'+str(e)[:180])
