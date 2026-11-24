# -*- coding: utf-8 -*-
"""Read-only joint decision research; no personal brokerage inputs."""
from pathlib import Path
import pandas as pd
import streamlit as st
st.set_page_config(page_title='H193｜十二研究員聯合決策',layout='wide')
from app_auth import require_login
require_login()
from godpick_h193_joint_decision import VERSION,TIERS,run,detail,excel
from godpick_h184_research_lab import ROLES
try:
    from godpick_bi_theme import install_bi_dashboard_style
    install_bi_dashboard_style('31_h193')
except Exception:pass
st.title('31｜十二研究員聯合決策引擎')
st.caption(VERSION+'｜只讀取現有自動行情／財報／重大訊息；不授權下單，不手動匯入。')
a,b,c=st.columns(3)
a.page_link('pages/7_股神推薦.py',label='股神推薦',icon='📊')
b.page_link('pages/29_主流趨勢與潛力黑馬.py',label='H191 技術研究',icon='📈')
c.page_link('pages/30_財報成長與合理估值.py',label='H192 財報研究',icon='📋')
root=Path(__file__).resolve().parents[1]
@st.cache_data(ttl=120,show_spinner=False)
def cached(path):return run(Path(path))
if st.button('重新讀取自動封存資料'):cached.clear()
with st.spinner('整合 H191／H192 全母體研究資格…'):
    try:r,f=cached(str(root))
    except Exception as e:st.error('研究資料無法讀取：'+type(e).__name__);st.stop()
st.caption(f"研究時點 {r['asof']}｜行情交易日 {r['market_day'] or '待核實'}｜技術狀態 {r['technical_status']}｜資料均為研究證據，不是買進授權。")
for col,p in zip(st.columns(4),TIERS):col.metric(p,r['counts'][p])
if not r['same_session']:st.warning('目前尚無同交易日技術／財報交叉證據。單側合格及補證研究仍獨立顯示。')
if not any(r['counts'].values()):st.warning('沒有研究候選：請查看完整母體與來源缺口，不能解讀為市場沒有機會。')
with st.expander('資料健康與來源衝突',expanded=not r['same_session']):
    st.json({k:r[k] for k in ('technical_coverage','financial_coverage','financial_errors','financial_conflicts','archive_errors','duplicate_identities')})
st.caption('雙證據須同交易日且具H192完整研究條件；單月觀察不能升格雙證據。財報池按已知年增、月增排序；技術池依各池最佳已顯示名次排序，未顯示名次者依代號；不跨策略比較原始分數。')
for tab,p in zip(st.tabs(list(TIERS)),TIERS):
    with tab:
        df=pd.DataFrame(r['pools'][p]);st.dataframe(df,hide_index=True,use_container_width=True) if not df.empty else st.info('此池無符合證據的候選。')
with st.expander('全母體稽核'):
    st.dataframe(pd.DataFrame(r['audit']),hide_index=True,use_container_width=True)
st.subheader('逐股十二研究員聯合審查')
items={(x['市場'],x['股票代號']):x for p in TIERS for x in r['pools'][p]}
# All candidates plus non-selected tickers remain searchable for failure diagnosis.
items.update({(x['市場'],x['股票代號']):x for x in r['audit'] if (x['市場'],x['股票代號']) not in items})
review=None
if items:
    k=st.selectbox('選擇股票（含未入池診斷）',list(items),format_func=lambda k:f"{k[1]} {items[k]['股票名稱']}｜{items[k]['研究分級']}")
    try:review=detail(r,f,*k,root=root)
    except Exception as exc:st.error('個股資料無法讀取：'+type(exc).__name__)
    if review:
        st.dataframe(pd.DataFrame([{'研究員':i+' '+ROLES[i][0],'狀態':o['status'],'結論':o['conclusion'],'缺口':'；'.join(o['gaps'])} for i,o in review['roles'].items()]),hide_index=True,use_container_width=True)
        for i,o in review['roles'].items():
            with st.expander(i+'｜'+ROLES[i][0]+'｜'+o['status']):
                st.write(o['conclusion']);st.json(o['metrics'])
                for name,rows in o['tables'].items():st.caption(name);st.dataframe(pd.DataFrame(rows),hide_index=True,use_container_width=True)
                for gap in o['gaps']:st.caption(gap)
                if o['provenance']:st.caption('來源：'+'；'.join(o['provenance']))
if st.checkbox('產生聯合研究Excel（含目前選股十二角色）'):
    st.download_button('下載 H193 聯合研究',excel(r,review),f'H193_Joint_{r["asof"][:10]}.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
st.caption('每日23:30台灣時間 GitHub Actions 建立全母體研究報告及各池前3檔角色審查；下載由 Actions artifacts 提供，非永久交易紀錄。頁面重新讀取使用現有本機資料，不逐檔連網。')
