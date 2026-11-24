# -*- coding: utf-8 -*-
"""H192 automatically sourced, read-only financial research center."""
import io
from pathlib import Path
import pandas as pd
import streamlit as st
st.set_page_config(page_title='H192｜財報成長與合理估值',layout='wide')
from app_auth import require_login
require_login()
from godpick_h192_financial_growth import run,POOLS,VERSION
try:
    from godpick_bi_theme import install_bi_dashboard_style
    install_bi_dashboard_style('30_h192')
except Exception:
    pass
st.title('30｜財報成長與合理估值引擎')
st.caption(VERSION+'｜自動讀取官方財報、估值與 H191 技術研究；不需手動匯入、不修改正式買進權限。')
a,b=st.columns(2)
a.page_link('pages/7_股神推薦.py',label='返回股神推薦',icon='📊')
b.page_link('pages/29_主流趨勢與潛力黑馬.py',label='H191 市場機會研究',icon='📈')
root=Path(__file__).resolve().parents[1]
a,b=st.columns([3,1]);top=a.selectbox('各研究池顯示上限',[10,20,30,50,100],index=2);refresh=b.button('重新讀取',use_container_width=True)
@st.cache_data(ttl=120,show_spinner=False)
def cached(root,top):return run(Path(root),top=top)
if refresh:cached.clear()
with st.spinner('讀取已封存財報與 H191 研究證據…'):
    try:r=cached(str(root),top)
    except Exception as exc:st.error('資料讀取失敗：'+type(exc).__name__);st.stop()
a,b,c,d=st.columns(4);a.metric('營收基準月',r['expected_month']);b.metric('財報基準季',r['expected_quarter']);c.metric('PE核對交易日',r['market_day'] or '待核實');d.metric('有研究結果的池',f"{sum(v>0 for v in r['counts'].values())}/{len(POOLS)}")
st.caption('基準期採一般業保守公告窗口，非個別公司法定申報期限；所有資料使用實際觀察時點。低PE、高EPS不等於低估；TTM是累計差分重建，非獨立核實。')
if r['status']!='RESEARCH_AVAILABLE':st.warning('目前沒有通過條件的研究股或資料仍缺證，請先檢查來源健康及逐股缺口；不可解讀為市場無機會。')
if not r['market_day']:st.info('H190 交易日尚未核實，PE 顯示原日期但暫不進入低 PE 高 EPS 池；營收研究不受此限制。')
st.subheader('官方證據覆蓋率')
st.dataframe(pd.DataFrame([{'市場':k,**v} for k,v in r['coverage'].items()]),hide_index=True,use_container_width=True)
with st.expander('自動更新健康度與資料衝突',expanded=True):
    st.caption('GitHub Actions：每日台灣時間22:05，部署後可立即手動觸發一次。頁面本身不逐檔連網。')
    st.write('來源狀態：'+r['health'].get('status','UNKNOWN'))
    st.dataframe(pd.DataFrame([{'來源':k,**v} for k,v in r['health'].get('sources',{}).items()]),hide_index=True,use_container_width=True)
    st.json({'拒絕資料':r['rejected'],'衝突資料':r['conflicts'],'讀取錯誤':r['errors']})
st.subheader('獨立財報研究池')
for group in (POOLS[:4],POOLS[4:]):
    for col,p in zip(st.columns(len(group)),group):col.metric(p,r['counts'][p])
for tab,p in zip(st.tabs(list(POOLS)),POOLS):
    with tab:
        st.caption('各池研究分口徑不同，不可跨池比較；未經績效驗證，不是機率或H126交易授權。')
        df=pd.DataFrame(r['pools'][p]);order=['研究排名','股票代號','股票名稱','市場','產業','研究理由','研究分','營收年月','營收年增%','營收月增%','三率季度','毛利率%','營益率%','淨利率%','累計EPS','TTM EPS重建','官方PE','PE日期','同業PE中位數','H191研究池','交易狀態']
        if df.empty:st.info('此研究池目前無合格證據。')
        else:st.dataframe(df[[x for x in order if x in df]+[x for x in df if x not in order]],hide_index=True,use_container_width=True)
with st.expander('全母體財報與缺口（可搜尋）'):
    query=st.text_input('股票代號／名稱',key='h192_search');rows=r['profiles']
    if query.strip():rows=[x for x in rows if query.strip() in x['股票代號'] or query.strip() in x['股票名稱']]
    st.dataframe(pd.DataFrame(rows),hide_index=True,use_container_width=True)
    if query.strip():
        codes={x['股票代號'] for x in rows};st.dataframe(pd.DataFrame([x for x in r['evidence'] if x['股票代號'] in codes]),hide_index=True,use_container_width=True)
with st.expander('研究規則與未完成的證據'):
    st.markdown('單月觀察：年增≥10%、月增為正。月增待年增補證：月增≥10%但缺年增證據，非成長認證。持續成長：3個實際月份逐月成長、最新年增≥10%。近13月新高：與前12個實際月份比較。三率：連兩季各自同比三率提高且最新營益、淨利率為正。獲利突破：單季營業利益創近5季高（前4季有正值）且淨利同比增加。低PE高EPS：同交易日PE≤15、≤同市場同行業其他至少5家公司中位數80%、重建TTM EPS≥5元、累計EPS同比上升、獲利率為正。所有門檻為初始研究規則。')
    st.warning('現金流、負債、股本變動、一次性損益及未來營收預估尚無完整證據；不提供DCF、目標價或勝率。H191交集僅比較其各池前50筆，並非全母體交集。')
def safe(rows):
    f=pd.DataFrame(rows)
    return f.apply(lambda s:s.map(lambda x:"'"+x if isinstance(x,str) and x.lstrip().startswith(('=','+','-','@')) else x))
@st.cache_data(ttl=120,show_spinner=False)
def excel(result):
    buf=io.BytesIO()
    with pd.ExcelWriter(buf,engine='openpyxl') as writer:
        safe([{'研究時間':result['asof'],'營收基準月':result['expected_month'],'財報基準季':result['expected_quarter'],'PE核對日':result['market_day'],'交易授權':'無',**result['counts']}]).to_excel(writer,sheet_name='00_摘要',index=False)
        safe([{'市場':k,**v} for k,v in result['coverage'].items()]).to_excel(writer,sheet_name='01_覆蓋率',index=False)
        for i,p in enumerate(POOLS,2):safe(result['pools'][p]).to_excel(writer,sheet_name=f'{i:02d}_{p}',index=False)
        safe(result['profiles']).to_excel(writer,sheet_name='10_逐股稽核',index=False)
        safe(result['evidence']).to_excel(writer,sheet_name='11_逐項證據',index=False)
        safe([{'來源':k,**v} for k,v in result['health'].get('sources',{}).items()]).to_excel(writer,sheet_name='12_來源健康',index=False)
    return buf.getvalue()
if st.checkbox('產生完整研究Excel（含逐項證據）',key='h192_export'):
    st.download_button('下載 H192 財報研究',excel(r),f'H192_Financial_{r["asof"][:10]}.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
