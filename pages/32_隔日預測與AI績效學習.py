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

st.divider()
st.subheader('H209.1｜多策略滾動回測與除權息績效')
st.caption('研究封存後下一交易日開盤模擬進場，按官方交易日出場；含現金股利應收及交易成本。複雜權息只列參考價代理，非真實成交、非買進授權。')
@st.cache_data(ttl=300,show_spinner=False)
def load_h209(path):
    from godpick_h209_rolling_backtest import run
    return run(Path(path))
if st.checkbox('載入 H209 滾動回測（只讀封存，不連網）'):
    try:
        if st.button('重新讀取 H209'):load_h209.clear()
        with st.spinner('驗證研究、官方行情及權息證據…'):backtest=load_h209(str(root))
        st.caption(backtest['revision']+'｜'+backtest['basis'])
        if backtest['pending']:st.info('尚待處理：'+str(backtest['pending']))
        with st.expander('上游封存狀態與訊號來源'):
            st.json(backtest['upstream']);st.json(backtest['source_counts'])
        if backtest['status']=='DATA_PENDING':st.warning('尚無有效成熟淨報酬樣本，不能認定策略有效。')
        st.write({'研究封存筆數':backtest['signal_count'],'成熟策略樣本':backtest['mature_strategy_rows'],'有效淨報酬樣本':backtest['valid_rows'],'排除原因':backtest['reasons']})
        st.dataframe(pd.DataFrame(backtest['summary']),hide_index=True,use_container_width=True)
        st.caption('非重疊固定研究窗：20窗訓練期觀察、10窗樣本外；不調參。任一候選缺證不產生該窗組合淨績效。期末再平衡回撤不是盤中最大回撤。')
        st.dataframe(pd.DataFrame(backtest['folds']),hide_index=True,use_container_width=True)
        with st.expander('逐筆與非重疊研究窗'):
            st.dataframe(pd.DataFrame(backtest['cohorts']),hide_index=True,use_container_width=True)
            st.dataframe(pd.DataFrame(backtest['detail']),hide_index=True,use_container_width=True)
        with st.expander('H209 成本假設及最近自動排程'):
            st.json(backtest['cost'])
            try:st.json(ar._read(root/'data/h209/collector.json'))
            except (OSError,ValueError):st.info('尚無權息抓取診斷。')
        if st.checkbox('產生 H209 回測 Excel'):
            from godpick_h209_rolling_backtest import excel as excel_h209
            st.download_button('下載 H209 滾動回測',excel_h209(backtest),'H209_Backtest.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:st.error('H209 研究證據驗證失敗：'+str(e)[:180])

st.divider()
st.subheader('H210｜多策略競爭與市場環境適應')
st.caption('同市場／同持有期間／同研究日，採 H209.1 權息及成本後模擬淨報酬。只提出研究複核；不自動淘汰策略、不修改正式買進權重。')
@st.cache_data(ttl=300,show_spinner=False)
def load_h210(path):
    from godpick_h210_strategy_competition import run
    return run(Path(path))
if st.checkbox('載入 H210 策略競爭與失效監控（唯讀）'):
    try:
        if st.button('重新讀取 H210'):load_h210.clear()
        with st.spinner('核對同日績效與市場環境…'):competition=load_h210(str(root))
        st.caption(competition['asof']+'｜'+competition['basis'])
        st.write({'研究封存筆數':competition['upstream']['signal_count'],'有效策略交易樣本':competition['upstream']['valid_rows'],'研究複核候選':competition['review_count']})
        if competition['status']=='BASELINE_ONLY':st.info('尚無符合全部驗證門檻的研究複核候選，維持原權重 1.0 倍。無樣本不代表策略失效。')
        if competition['archive_errors']:st.warning('行情封存異常，禁止策略競爭判定。')
        st.dataframe(pd.DataFrame(competition['current_regime']),hide_index=True,use_container_width=True)
        market_filter=st.selectbox('H210 市場',['全部','TWSE','TPEX'])
        horizon_filter=st.selectbox('H210 持有期間',[5,1,3,10,20],format_func=lambda n:f'T+{n}')
        def h210_filtered(rows):return [x for x in rows if (market_filter=='全部' or x['市場']==market_filter) and x['期間']==horizon_filter]
        st.caption('主要 T+5：20訓練窗＋10留出窗，至少兩輪連續完整留出（相同母體），其他期間僅供觀察。每個策略窗至少3檔且100%有效。')
        st.dataframe(pd.DataFrame(h210_filtered(competition['recommendations'])),hide_index=True,use_container_width=True,column_config={k:st.column_config.NumberColumn(k,format='percent') for k in ('最近留出均值','最近留出相對差','最近留出勝窗率','前輪留出相對差','近期未滿輪相對差')})
        with st.expander('市場環境與滾動留出驗證'):
            st.dataframe(pd.DataFrame(h210_filtered(competition['models'])),hide_index=True,use_container_width=True)
            st.dataframe(pd.DataFrame(h210_filtered(competition['folds'])),hide_index=True,use_container_width=True)
            st.dataframe(pd.DataFrame(h210_filtered(competition['scores'])),hide_index=True,use_container_width=True)
        with st.expander('候選樣本、權息與資料覆蓋稽核'):
            st.json(competition['upstream']);st.json(competition['thresholds'])
            st.dataframe(pd.DataFrame(h210_filtered(competition['audit'])),hide_index=True,use_container_width=True)
        if st.checkbox('產生 H210 策略競爭 Excel'):
            from godpick_h210_strategy_competition import excel as excel_h210
            st.download_button('下載 H210 策略競爭',excel_h210(competition),'H210_StrategyCompetition.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:st.error('H210 研究驗證失敗：'+str(e)[:180])


st.divider()
st.subheader('H212｜全市場資料修復與績效失敗歸因')
st.caption('查核60個官方交易日、逐股缺日、財技交叉及封存狀態；修復由後台排程執行，頁面只讀。不得用零值、推算K線或事後資料補造研究。')
@st.cache_data(ttl=300,show_spinner=False)
def load_h212(path):
    from godpick_h212_data_recovery import run
    return run(Path(path))
if st.checkbox('載入 H212 資料修復與研究歸因（唯讀）'):
    try:
        if st.button('重新讀取 H212'):load_h212.clear()
        with st.spinner('核對官方日線、研究封存與成熟績效…'):h212=load_h212(str(root))
        st.caption(h212['version']+'｜'+h212['asof']+'｜'+h212['status'])
        if h212['status']=='DATA_PENDING':st.warning('資料覆蓋或成熟績效尚不足；不宣稱已達有效策略學習。')
        st.write(h212['counts'])
        st.dataframe(pd.DataFrame([{'市場':m,**v} for m,v in h212['gap']['coverage'].items()]),hide_index=True,use_container_width=True,column_config={'ratio':st.column_config.NumberColumn('60日有效覆蓋',format='percent')})
        st.dataframe(pd.DataFrame(h212['gap']['gap_summary']),hide_index=True,use_container_width=True)
        st.markdown('**前瞻封存管線**');st.dataframe(pd.DataFrame(h212['capture']),hide_index=True,use_container_width=True)
        st.markdown('**財報與技術交叉診斷**');st.dataframe(pd.DataFrame(h212['intersection_summary']),hide_index=True,use_container_width=True)
        with st.expander('逐股缺日、日期補抓與最近修復結果'):
            st.dataframe(pd.DataFrame(h212['gap']['stock_gaps']),hide_index=True,use_container_width=True)
            st.dataframe(pd.DataFrame(h212['gap']['day_queue']),hide_index=True,use_container_width=True)
            st.dataframe(pd.DataFrame(h212['gap']['market_missing']),hide_index=True,use_container_width=True)
            st.json(h212['gap']['archive_errors'])
            st.write({k:v for k,v in h212['repair'].items() if k not in ('attempts','tried')})
            st.dataframe(pd.DataFrame(h212['repair'].get('tried',[])),hide_index=True,use_container_width=True)
        with st.expander('強勢股對帳與財技逐股分析（非正式漏選證明）'):
            st.dataframe(pd.DataFrame(h212['leaders']),hide_index=True,use_container_width=True)
            st.dataframe(pd.DataFrame(h212['intersection']),hide_index=True,use_container_width=True)
        with st.expander('成熟績效描述性歸因（非因果）'):
            st.dataframe(pd.DataFrame(h212['outcome_summary']),hide_index=True,use_container_width=True)
            st.dataframe(pd.DataFrame(h212['outcomes']),hide_index=True,use_container_width=True)
        if st.checkbox('產生 H212 診斷 Excel'):
            from godpick_h212_data_recovery import excel as excel_h212
            st.download_button('下載 H212 資料修復與歸因',excel_h212(h212),'H212_DataRecovery.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:st.error('H212 資料驗證失敗：'+str(e)[:180])


st.divider()
st.subheader('H213｜每日推薦品質驗收與官方多源補證')
st.caption('品質環節通過數不是選股勝率；月表補證僅供核實，來源範圍不一致不直接注入K線。復原由GitHub Actions執行。')
@st.cache_data(ttl=300,show_spinner=False)
def load_h213(path):
    from godpick_h213_quality_gate import quality
    return quality(Path(path))
if st.checkbox('載入 H213 每日品質驗收（唯讀）'):
    try:
        if st.button('重新讀取 H213'):load_h213.clear()
        with st.spinner('核對資料品質、前瞻封存與召回證據…'):h213=load_h213(str(root))
        st.caption(h213['version']+'｜'+h213['asof']+'｜資料日 '+str(h213['session']))
        st.metric('品質環節通過（非勝率）',f"{h213['passed']}/{h213['total']}")
        if h213['status']!='RESEARCH_PIPELINE_READY':st.warning('尚待證據；不得據此提升正式權重。')
        st.dataframe(pd.DataFrame(h213['checks']),hide_index=True,use_container_width=True)
        st.dataframe(pd.DataFrame(h213['recall']),hide_index=True,use_container_width=True,column_config={'研究榜召回率':st.column_config.NumberColumn('研究榜召回率',format='percent')})
        st.write('復原建議（非已完成）',h213['repair_plan']);st.write('錯誤',h213['errors'])
        with st.expander('官方個股月表補證（隔離證據）'):
            st.dataframe(pd.DataFrame(h213['monthly_detail']),hide_index=True,use_container_width=True)
            st.write(h213['monthly'])
        with st.expander('權息跳空證據核對（原始價格不改寫）'):
            st.dataframe(pd.DataFrame(h213['jumps']),hide_index=True,use_container_width=True)
        if st.checkbox('產生 H213 品質驗收 Excel'):
            from godpick_h213_quality_gate import excel as excel_h213
            st.download_button('下載 H213 品質驗收',excel_h213(h213),'H213_Quality.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:st.error('H213 資料驗證失敗：'+str(e)[:180])

st.divider()
st.subheader('H214｜雙母體、權息還原與實戰品質校正')
st.caption('唯讀影子研究：保留原始覆蓋率、原始價格及正式買進權威；只分流官方證實的短歷史，不將缺日當作停牌。')
@st.cache_data(ttl=300,show_spinner=False)
def load_h214(path):
    from godpick_h214_universe import run
    return run(Path(path))
if st.checkbox('載入 H214 有效母體與還原技術研究（唯讀）'):
    try:
        if st.button('重新讀取 H214'):load_h214.clear()
        with st.spinner('核對官方名冊、權息與技術證據…'):h214=load_h214(str(root))
        st.caption(h214['version']+'｜'+h214['asof']+'｜資料日 '+h214['session'])
        st.write(h214['status'],h214['basis'])
        st.dataframe(pd.DataFrame(h214['coverage']),hide_index=True,use_container_width=True,column_config={k:st.column_config.NumberColumn(k,format='percent') for k in ('原始覆蓋率','有效母體原價覆蓋率','還原覆蓋率')})
        st.write('正式推薦對帳',h214['formal_comparison']);st.write('資料異常',h214['errors'])
        with st.expander('短歷史獨立研究（非正式推薦）'):st.dataframe(pd.DataFrame(h214['short']),hide_index=True,use_container_width=True)
        with st.expander('原始與還原技術差異'):st.dataframe(pd.DataFrame(h214['changes']),hide_index=True,use_container_width=True)
        with st.expander('全母體逐股稽核'):st.dataframe(pd.DataFrame(h214['detail']),hide_index=True,use_container_width=True)
        with st.expander('官方名冊與主檔差異'):st.dataframe(pd.DataFrame(h214['roster_reconcile']),hide_index=True,use_container_width=True)
        if st.checkbox('產生 H214 品質 Excel'):
            from godpick_h214_universe import excel as excel_h214
            st.download_button('下載 H214 品質校正',excel_h214(h214),'H214_Universe.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:st.error('H214 資料驗證失敗：'+str(e)[:180])

st.divider()
st.subheader('H215｜資料實證、交易狀態及每日強制驗收')
st.caption('只讀驗收；公告命中不等於完整停牌，缺日不補零，日終閘門與成熟績效分開，不更改H126正式買進授權。')
@st.cache_data(ttl=300,show_spinner=False)
def load_h215(path):
    from godpick_h215_proof import run
    return run(Path(path))
if st.checkbox('載入 H215 每日實證驗收（唯讀）'):
    try:
        if st.button('重新讀取 H215'):load_h215.clear()
        with st.spinner('核實官方狀態、每日封存及成熟績效…'):h215=load_h215(str(root))
        st.caption(h215['version']+'｜'+h215['asof']+'｜資料日 '+str(h215['session']))
        g215=h215['gate'];st.metric('每日資料閘門（非勝率）',str(g215['daily_passed'])+'/'+str(g215['daily_total']))
        if g215['daily_status']=='FAIL_DAILY':st.error('日終驗收未通過；不得宣稱資料管線就緒。')
        st.write(g215['daily_status'],g215['mature_status'],g215['shadow_status'])
        st.dataframe(pd.DataFrame(g215['daily_checks']),hide_index=True,use_container_width=True)
        st.dataframe(pd.DataFrame(h215['coverage']),hide_index=True,use_container_width=True,column_config={k:st.column_config.NumberColumn(k,format='percent') for k in ('原始覆蓋率','有效母體原價覆蓋率','還原覆蓋率')})
        st.write('官方狀態來源',h215['status_sources'],'缺日分類',h215['status_summary'])
        with st.expander('逐股缺日與官方交易狀態核對'):st.dataframe(pd.DataFrame(h215['status_detail']),hide_index=True,use_container_width=True)
        with st.expander('管線健康與來源回應診斷'):
            st.caption('H216 同輪運算耗時（秒）');st.json(h215.get('h216_stage_seconds',{}));st.json(h215['health']);st.write(h215['errors'])
        if st.checkbox('產生 H215 實證 Excel'):
            from godpick_h215_proof import excel as excel_h215
            st.download_button('下載 H215 實證驗收',excel_h215(h215),'H215_Proof.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:st.error('H215 資料驗收失敗：'+str(e)[:180])
