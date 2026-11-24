# -*- coding: utf-8 -*-
"""H191 read-only automatically sourced opportunity dashboard."""
import io
from pathlib import Path
import pandas as pd
import streamlit as st
st.set_page_config(page_title='H191｜主流趨勢與潛力黑馬',layout='wide')
from app_auth import require_login
require_login()
from godpick_h191_opportunity_engine import run,POOLS,VERSION
try:
    from godpick_bi_theme import install_bi_dashboard_style
    install_bi_dashboard_style('29_h191')
except Exception:
    pass
st.title('29｜主流趨勢與潛力黑馬引擎')
st.caption(VERSION+'｜自動讀取 H190 官方行情、既有法人／TDCC／H117 指數快照；不需上傳 Excel，不寫入正式推薦與交易紀錄。')
st.page_link('pages/7_股神推薦.py',label='返回股神正式推薦',icon='📊')
if Path(__file__).with_name('30_財報成長與合理估值.py').is_file():
    st.page_link('pages/30_財報成長與合理估值.py',label='H192｜財報成長與合理估值',icon='📋')
root=Path(__file__).resolve().parents[1]
a,b,c=st.columns([2,2,1]);top=a.selectbox('各研究池顯示上限',[10,20,30,50],index=1);amount=b.selectbox('當日及20日均成交額下限（萬元）',[1000,2000,3000,5000,10000],index=1);refresh=c.button('重新讀取',use_container_width=True)
@st.cache_data(ttl=60,show_spinner=False)
def cached(root,top,amount):return run(Path(root),top=top,min_amount=amount*10000)
if refresh:cached.clear()
with st.spinner('讀取已封存行情並建立獨立研究排名…'):
    try:result=cached(str(root),top,amount)
    except Exception as exc:st.error('研究資料無法讀取：'+type(exc).__name__);st.stop()
status={'DATA_PENDING':'待資料補證','PARTIAL_RESEARCH':'部分母體研究','CROSS_MARKET_RESEARCH_READY':'跨市場研究條件達標'}[result['status']]
a,b,c,d=st.columns(4);a.metric('研究日',result['asof']);b.metric('有效60日個股',f"{result['qualified']:,} / {result['universe']:,}");c.metric('雙市場已封存日',f"{result['paired_sessions']} / 60");d.metric('研究狀態',status)
st.caption(f"應檢查截止日 {result['cutoff']}｜日曆新鮮度 {'通過' if result['calendar_fresh'] else '未通過'}｜原始未還原價格，尚未完成除權息驗證；研究排名不是勝率或交易授權。")
if result['status']=='DATA_PENDING':st.warning('尚未取得當期完整個股資料。請確認 H190 GitHub Actions 已執行並保存行情，勿將零候選解讀為沒有投資機會。')
elif result['status']=='PARTIAL_RESEARCH':st.warning('只對已驗證個股提供研究線索；市場主流排行需兩市場各達80%且產業至少70%覆蓋，未達不得稱全市場主流。')
if not result['index_available']:st.info('H117 指數20日報酬不齊：大盤抗跌池暫停；不以ETF或個股平均冒充加權指數。')
cols=st.columns(5)
for col,p in zip(cols,POOLS):col.metric(p,result.get('pool_counts',{}).get(p,len(result['pools'][p])))
with st.expander('行情覆蓋率、排除原因與來源缺口',expanded=True):
    st.dataframe(pd.DataFrame([{'市場':m,**v} for m,v in result['coverage'].items()]),hide_index=True,use_container_width=True)
    st.json(result['reasons']);st.caption('全市場主流門檻：各市場主檔至少100檔、有效率80%；產業至少5檔、有效率70%。分母為現有股票主檔，非官方上市家數認證。')
    if result['archive_errors']:st.error('封存驗證有錯誤');st.write(result['archive_errors'])
st.subheader('產業輪動與成交活躍度')
st.caption('成交額活躍比＝當日成交額 / 前20日平均成交額（相同有效成分股）；不是資金淨流入。')
st.dataframe(pd.DataFrame(result['sectors']),hide_index=True,use_container_width=True)
for tab,p in zip(st.tabs(list(POOLS)),POOLS):
    with tab:
        st.caption('獨立研究排名，只在同研究池內比較；不代表 H126 正式推薦。')
        f=pd.DataFrame(result['pools'][p]);
        if f.empty:st.info('沒有符合該研究池條件的可核驗資料。')
        else:
            order=['研究排名','股票代號','股票名稱','產業','研究分','收盤','日漲跌%','5日漲跌%','20日漲跌%','量比','當日成交額','研究理由','交易狀態','資料日期'];st.dataframe(f[order+[x for x in f if x not in order]],hide_index=True,use_container_width=True)
with st.expander('逐股稽核（含缺口）'):st.dataframe(pd.DataFrame(result['diagnostic']),hide_index=True,use_container_width=True)
def safe(frame):
    f=pd.DataFrame(frame);return f.apply(lambda s:s.map(lambda x:"'"+x if isinstance(x,str) and x.lstrip().startswith(('=','+','-','@')) else x))
buf=io.BytesIO()
with pd.ExcelWriter(buf,engine='openpyxl') as writer:
    safe([{'研究日':result['asof'],'狀態':status,'有效檔數':result['qualified'],'母體':result['universe'],'交易授權':'無','說明':result['corporate_actions']}]).to_excel(writer,sheet_name='00_摘要',index=False)
    safe(result['sectors']).to_excel(writer,sheet_name='01_產業',index=False)
    for i,p in enumerate(POOLS,2):safe(result['pools'][p]).to_excel(writer,sheet_name=f'{i:02d}_{p}',index=False)
    safe(result['diagnostic']).to_excel(writer,sheet_name='07_逐股稽核',index=False)
st.download_button('下載 H191 獨立研究報表',buf.getvalue(),f'H191_Research_{result["asof"]}.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
