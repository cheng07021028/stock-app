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

st.divider()
st.subheader('H217｜前瞻績效閉環與強勢股漏選實證')
st.caption('H194／H209 獨立研究對帳，不等同 H126 正式買進。含成本模擬淨報酬非真實成交；未成熟或未封存時不顯示虛假勝率。')
@st.cache_data(ttl=300,show_spinner=False)
def load_h217(path):
    from godpick_h217_prospective_quality import run
    return run(Path(path))
if st.checkbox('載入 H217 前瞻品質閉環（唯讀）'):
    try:
        if st.button('重新讀取 H217'):load_h217.clear()
        with st.spinner('核對前瞻封存、成熟樣本與強勢股…'):h217=load_h217(str(root))
        st.caption(h217['version']+'｜'+h217['asof']+'｜資料日 '+h217['day'])
        if h217['status']=='DATA_PENDING':st.warning('前瞻或成熟證據待補，維持原有研究權重。')
        st.write(h217['counts']);st.write(h217['pending']);st.write(h217['errors'])
        st.markdown('**當日研究榜召回（非正式漏選）**');st.dataframe(pd.DataFrame(h217['recall']),hide_index=True,use_container_width=True,column_config={k:st.column_config.NumberColumn(k,format='percent') for k in ('雙日覆蓋率','研究召回率')})
        with st.expander('逐股漏選與當時原因證據'):st.dataframe(pd.DataFrame(h217['miss']),hide_index=True,use_container_width=True)
        with st.expander('歷史不可覆寫召回驗收'):st.dataframe(pd.DataFrame(h217['history']),hide_index=True,use_container_width=True)
        with st.expander('成熟績效及同日新舊研究對照'):
            st.dataframe(pd.DataFrame(h217['summary']),hide_index=True,use_container_width=True,column_config={k:st.column_config.NumberColumn(k,format='percent') for k in ('覆蓋率','平均淨報酬','勝率')})
            st.dataframe(pd.DataFrame(h217['paired']),hide_index=True,use_container_width=True,column_config={k:st.column_config.NumberColumn(k,format='percent') for k in ('重複組平均淨報酬','近窗未見組平均淨報酬','未見組相對差')})
        with st.expander('H194／H209 管線健康證據'):st.json(h217['pipeline'])
        with st.expander('樣本外研究優先序人工複核（不自動調權）'):st.dataframe(pd.DataFrame(h217['review']),hide_index=True,use_container_width=True,column_config={k:st.column_config.NumberColumn(k,format='percent') for k in ('訓練平均差','留出平均差')})
        if st.checkbox('產生 H217 前瞻品質 Excel'):
            from godpick_h217_prospective_quality import excel as excel_h217
            st.download_button('下載 H217 前瞻品質',excel_h217(h217),'H217_Quality.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:st.error('H217 資料驗證失敗：'+str(e)[:180])

st.divider()
st.subheader('H218｜真實前瞻績效驗收、權息復原與策略優勢')
st.caption('逐日重讀官方權息證據；H194原價績效與H209含成本模擬分列。來源優勢僅人工複核，不改正式權重。')
@st.cache_data(ttl=120,show_spinner=False)
def load_h218(path):
    from godpick_h218_evidence import run
    return run(Path(path))
if st.checkbox('載入 H218 實證驗收（唯讀）'):
    try:
        if st.button('重新讀取 H218'):load_h218.clear()
        with st.spinner('核對官方權息及成熟績效…'):h218=load_h218(str(root))
        st.caption(h218['version']+'｜'+h218['asof']+'｜'+h218['day'])
        if h218['status']=='DATA_PENDING':st.warning('績效尚待成熟或補證；不是零勝率。')
        st.write(h218['counts']);st.write(h218['errors'])
        st.markdown('**權息逐日獨立驗收**');st.dataframe(pd.DataFrame(h218['action_summary']),hide_index=True,use_container_width=True,column_config={'覆蓋率':st.column_config.NumberColumn('覆蓋率',format='percent')})
        with st.expander('逐日權息缺口與原始證據'):st.dataframe(pd.DataFrame(h218['action_rows']),hide_index=True,use_container_width=True)
        st.markdown('**成熟績效對帳**');st.dataframe(pd.DataFrame(h218['summary']),hide_index=True,use_container_width=True,column_config={k:st.column_config.NumberColumn(k,format='percent') for k in ('淨績效覆蓋率','平均淨報酬','淨勝率')})
        with st.expander('H194／H209 同日獨有股票優勢（排除共同股票）'):
            st.dataframe(pd.DataFrame(h218['pairs']),hide_index=True,use_container_width=True,column_config={k:st.column_config.NumberColumn(k,format='percent') for k in ('H194獨有均值','H209獨有均值','H209相對差')})
            st.dataframe(pd.DataFrame(h218['review']),hide_index=True,use_container_width=True,column_config={k:st.column_config.NumberColumn(k,format='percent') for k in ('訓練差','留出差')})
        with st.expander('H208／H209 封存診斷（非事後重建）'):st.json(h218['recall'])
        if st.checkbox('產生 H218 實證驗收 Excel'):
            from godpick_h218_evidence import excel as excel_h218
            st.download_button('下載 H218 實證驗收',excel_h218(h218),'H218_Evidence.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:st.error('H218 資料驗證失敗：'+str(e)[:180])

st.divider()
st.subheader('H219｜首批前瞻績效檢討、強勢命中與精簡研究')
st.caption('原價與模擬淨報酬分離；弱市比較採起始四碼有價證券廣度（非指數）；所有研究均不等同正式買進。')
@st.cache_data(ttl=120,show_spinner=False)
def load_h219(path):
    from godpick_h219_first_review import run
    return run(Path(path))
if st.checkbox('載入 H219 首批績效與精簡研究（唯讀）'):
    try:
        if st.button('重新讀取 H219'):load_h219.clear()
        with st.spinner('核對首批研究、正式行情、成熟績效及漏選證據…'):h219=load_h219(str(root))
        st.caption(h219['version']+'｜'+h219['asof'])
        st.write(h219['status'],h219['counts'],h219['errors'])
        st.markdown('**首批績效（按市場、期間、研究分級）**')
        st.dataframe(pd.DataFrame(h219['first']),hide_index=True,use_container_width=True,column_config={k:st.column_config.NumberColumn(k,format='percent') for k in ('原價平均','原價上漲比例','淨報酬平均','淨勝率','原價相對差','弱市不跌比例','弱市跑贏比例')})
        st.markdown('**強勢股命中及召回缺口**');st.dataframe(pd.DataFrame(h219['recall']),hide_index=True,use_container_width=True,column_config={k:st.column_config.NumberColumn(k,format='percent') for k in ('雙日覆蓋率','研究召回率')});st.dataframe(pd.DataFrame(h219['recall_gaps']),hide_index=True,use_container_width=True,column_config={'覆蓋率':st.column_config.NumberColumn('覆蓋率',format='percent')})
        st.caption(h219['shortlist_note']);st.dataframe(pd.DataFrame(h219['shortlist']),hide_index=True,use_container_width=True)
        with st.expander('漏選原始證據'):st.dataframe(pd.DataFrame(h219['miss']),hide_index=True,use_container_width=True)
        with st.expander('弱市廣度與原價相對績效'):st.dataframe(pd.DataFrame(h219['breadth']),hide_index=True,use_container_width=True,column_config={k:st.column_config.NumberColumn(k,format='percent') for k in ('覆蓋率','市場原價中位數','下跌比例')})
        if st.checkbox('產生 H219 績效 Excel'):
            from godpick_h219_first_review import excel as excel_h219
            st.download_button('下載 H219 首批績效',excel_h219(h219),'H219_Review.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as ex:st.error('H219 資料驗證失敗：'+str(ex)[:180])

st.divider()
st.subheader('H220｜主流成交活躍、高成長起漲與實戰績效反饋')
st.caption('分市場同成分成交額非資金淨流入；H123/H124官方籌碼與H192/H198證據分列。新研究只供複核，權重不變。')
@st.cache_data(ttl=120,show_spinner=False)
def load_h220(path):
    from godpick_h220_growth_rotation import run
    return run(Path(path))
if st.checkbox('載入 H220 成長起漲與績效反饋（唯讀）'):
    try:
        if st.button('重新讀取 H220'):load_h220.clear()
        with st.spinner('核對同市場主流、財技證據、官方籌碼與成熟績效…'):h220=load_h220(str(root))
        st.caption(h220['version']+'｜'+h220['asof']+'｜資料日 '+str(h220['day']))
        st.write(h220['status'],h220['counts'],h220['first_counts'])
        if not h220['rows']:st.warning('沒有通過本版研究條件，或資料待補；不等同沒有市場機會。')
        st.markdown('**高成長起漲研究（不等同正式買進）**');st.dataframe(pd.DataFrame(h220['rows']),hide_index=True,use_container_width=True)
        st.markdown('**分市場產業成交活躍度**');st.dataframe(pd.DataFrame(h220['sectors']),hide_index=True,use_container_width=True,column_config={k:st.column_config.NumberColumn(k,format='percent') for k in ('覆蓋率','上漲比例')})
        with st.expander('H221 主升段初期補證清單（不是推薦）'):
            from godpick_h198_stage_review import POOLS as h221_stage_pools
            st.dataframe(pd.DataFrame([r for r in h220['audit'] if r.get('H198階段') in h221_stage_pools[:3] and r.get('狀態')!='成長起漲複核']),hide_index=True,use_container_width=True)
        with st.expander('完整逐股缺口及籌碼來源'):st.write(h220['coverage'],h220['source_errors'],h220['financial_errors'],h220['errors']);st.dataframe(pd.DataFrame(h220['source_summary']),hide_index=True,use_container_width=True);st.dataframe(pd.DataFrame(h220['audit']),hide_index=True,use_container_width=True)
        with st.expander('真實首批績效與強勢召回'):st.dataframe(pd.DataFrame(h220['first']),hide_index=True,use_container_width=True);st.dataframe(pd.DataFrame(h220['recall']),hide_index=True,use_container_width=True)
        with st.expander('H220封存籌碼績效與樣本外複核'):st.dataframe(pd.DataFrame(h220['pairs']),hide_index=True,use_container_width=True);st.dataframe(pd.DataFrame(h220['review']),hide_index=True,use_container_width=True)
        if st.checkbox('產生 H220 研究驗收 Excel'):
            from godpick_h220_growth_rotation import excel as excel_h220
            st.download_button('下載 H220 研究驗收',excel_h220(h220),'H220_Review.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as ex:st.error('H220 資料驗證失敗：'+str(ex)[:180])


st.subheader('H221｜官方法人與高成長證據閉環')
@st.cache_data(ttl=300,show_spinner=False)
def load_h221(path):
    from godpick_h221_evidence_closure import inspect
    return inspect(Path(path))
if st.checkbox('載入 H221 法人與財報證據驗收（唯讀）'):
    try:
        if st.button('重新讀取 H221'):load_h221.clear()
        h221=load_h221(str(root))
        st.caption(h221['version']+'｜'+h221['asof'])
        st.warning('期數或來源不足只列為補證，不等同買進或負面訊號。')
        st.markdown('**最近五個官方交易日法人逐日證據**');st.dataframe(pd.DataFrame(h221['flows']),hide_index=True,use_container_width=True)
        st.markdown('**財報必要歷史期數**');st.dataframe(pd.DataFrame(h221['financial']),hide_index=True,use_container_width=True)
        with st.expander('官方來源診斷'):st.write(h221['financial_health'],h221['chip_health'],h221['errors'],h221['rejected'],h221['conflicts'])
    except Exception as ex:st.error('H221 證據驗收失敗：'+str(ex)[:180])

st.subheader('H222｜歷史財報回補與成長證據驗收')
@st.cache_data(ttl=300,show_spinner=False)
def load_h222(path):
    from godpick_h222_financial_history import inspect
    return inspect(Path(path))
with st.expander('H236｜雲端持久財報還原（選擇性、僅讀雲端）'):
    import os as _h236_os
    st.caption('原始ZIP由專用私有GCS保存。僅還原H235直取核實證據；手動H234不升格。正式權重與買進授權不變。')
    st.code('python tools/h236_cloud.py collect-publish --quarter 2025Q3 --official-url "完整官方HTTPS網址"\npython tools/h236_cloud.py restore --quarter 2025Q3',language='bash')
    if _h236_os.getenv('H236_ENABLE_UI_RESTORE')=='1' and _h236_os.getenv('H236_GCS_BUCKET'):
        from godpick_h192_financial_growth import expected_periods as _h236_periods,qstr as _h236_qstr
        _,_h236_last=_h236_periods(datetime.now(ar.TZ))
        _h236_q=st.selectbox('雲端財報季度',list(range(_h236_last,_h236_last-8,-1)),format_func=_h236_qstr,key='h236_quarter')
        _h236_a,_h236_b=st.columns(2)
        if _h236_a.button('查詢雲端封存',key='h236_status'):
            try:
                from godpick_h236_cloud_financial import cloud_status as _h236_status
                st.json(_h236_status(root,_h236_q))
            except Exception as _h236_e:st.error('H236 雲端查詢失敗：'+type(_h236_e).__name__)
        if _h236_b.button('還原所選季度至本機快取',key='h236_restore'):
            try:
                from godpick_h236_cloud_financial import restore as _h236_restore
                with st.spinner('還原並重新核實原始財報，可能涉及大型檔案…'):_h236_result=_h236_restore(root,_h236_q)
                st.json(_h236_result);load_h222.clear()
                st.info('原H222已清除快取；其他研究頁依原有TTL或重新整理更新。')
            except Exception as _h236_e:st.error('H236 還原失敗：'+type(_h236_e).__name__+'；未核實資料不會升格。')
    else:st.info('H236雲端還原預設關閉。請由管理員配置私有儲存與唯讀憑證，再明確啟用。')
if st.checkbox('載入 H222 歷史財報驗收（唯讀）'):
    try:
        if st.button('重新讀取 H222'):load_h222.clear()
        h222=load_h222(str(root))
        st.caption(h222['version']+'｜'+h222['asof'])
        st.warning('回補資料自實際首次取得後才可使用。TTM為累計EPS差分研究，不代表已核實股本變動。')
        st.dataframe(pd.DataFrame(h222['summary']),hide_index=True,use_container_width=True)
        st.markdown('**高成長財報複核（非正式推薦）**');st.dataframe(pd.DataFrame(h222['candidates']),hide_index=True,use_container_width=True)
        with st.expander('H232 EPS同分母成長複核（非買進授權）'):
            st.caption('明示EPS分子與加權平均股數一致；仍需獨立核實股本事件及追溯重編。')
            st.dataframe(pd.DataFrame(h222.get('h232_candidates',[])),hide_index=True,use_container_width=True)
        if st.checkbox('產生 H222 歷史財報驗收 Excel'):
            from godpick_h222_financial_history import excel as excel_h222
            st.download_button('下載 H222 財報驗收',excel_h222(h222),'H222_FinancialHistory.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        with st.expander('逐股歷史缺期與成長證據'):
            st.dataframe(pd.DataFrame(h222['stocks']),hide_index=True,use_container_width=True)
        with st.expander('官方回補來源驗收'):
            st.dataframe(pd.DataFrame(h222['health'].get('scope',[])),hide_index=True,use_container_width=True)
            st.markdown('**H231 個股歷史財報補證（不計入全市場分期達標）**')
            ch=h222.get('company_health',{})
            st.caption('個股補證支援 H233 限額自動佇列及 H231 手動指定；正式年度、累計起訖及單位必須核實。')
            if ch:st.dataframe(pd.DataFrame(ch.get('scope',[])),hide_index=True,use_container_width=True)
            else:st.info('尚無個股補證收集紀錄。GitHub Actions H222 可輸入 company_codes。')
            st.markdown('**H233 自動回補佇列（非推薦順位）**')
            auto=h222.get('auto_health',{})
            if auto:
                st.caption(str(auto.get('asof',''))+'｜'+str(auto.get('status','')))
                st.write(auto.get('counts',{}),{'彙總請求':auto.get('bulk_requests'),'個股請求':auto.get('company_requests'),'錯誤':auto.get('errors',[])})
                st.dataframe(pd.DataFrame(auto.get('queue',[])),hide_index=True,use_container_width=True)
            else:st.info('尚無H233自動補證紀錄；原H222工作流保留手動及全市場模式。')
            st.markdown('**H234 官方XBRL批次文件研究（本機匯入，非正式財報或買進授權）**')
            st.caption('由官方案例文件整批下載頁取得各季度原始ZIP，保留原檔；以本機CLI批次解析一般業合併報表。原H222及H126資料不變。')
            st.markdown('[官方 XBRL 案例文件整批下載](https://mopsov.twse.com.tw/mops/web/t203sb02)')
            st.code('python tools/h222_history.py --batch-dir "D:/MOPS_XBRL" --out "D:/MOPS_Review"',language='bash')
            st.caption('匯出 H234_BatchFinancialReview.xlsx，檢查逐股科目、季度覆蓋與官方重疊衝突。大型ZIP請在本機執行，避免阻塞Streamlit。')
            st.markdown('**H235 官方網址直取入庫（本機／持久磁碟）**')
            st.caption('僅接受官方原始HTTPS下載網址；本機ZIP仍只供H234研究。直取後原始ZIP及封存憑據保存在 data/h235，不可上傳大型ZIP到GitHub。')
            st.code('python tools/h222_history.py --official-quarter 2025Q3 --official-batch-url "從官方下載頁複製的完整HTTPS網址" --out "D:/MOPS_Review"',language='bash')
            if h222.get('batch_scope'):st.dataframe(pd.DataFrame(h222['batch_scope']),hide_index=True,use_container_width=True)
            else:st.info('尚無H235官方直取封存。')
            st.write(h222['errors'],h222['rejected'],h222['conflicts'])
    except Exception as ex:st.error('H222 歷史財報驗收失敗：'+str(ex)[:180])

st.subheader('H223｜歷史財報實證與營收成長情境')
@st.cache_data(ttl=300,show_spinner=False)
def load_h223(path):
    from godpick_h223_growth_scenarios import run
    return run(Path(path))
if st.checkbox('載入 H223 財報、情境與優勢研究（唯讀）'):
    try:
        if st.button('重新讀取 H223'):load_h223.clear()
        with st.spinner('重驗歷史財報並核對營收情境及實際結果…'):h223=load_h223(str(root))
        st.caption(h223['version']+'｜'+h223['asof']+'｜資料日 '+h223['day'])
        st.warning('營收情境不是公告財報或機率區間；EPS股本變動尚待核實；所有順位均為獨立研究，未修改H126。')
        st.write(h223['status'],h223['counts'])
        st.markdown('**高成長財報優勢順位**');st.dataframe(pd.DataFrame(h223['ranked']),hide_index=True,use_container_width=True)
        st.markdown('**保守／基準／樂觀營收情境**');st.dataframe(pd.DataFrame(h223['forecast']),hide_index=True,use_container_width=True)
        with st.expander('歷史回補獨立重驗及逐股缺口'):
            st.dataframe(pd.DataFrame(h223['scope']),hide_index=True,use_container_width=True);st.dataframe(pd.DataFrame(h223['audit']),hide_index=True,use_container_width=True);st.write(h223['errors'],h223['rejected'],h223['conflicts'])
        with st.expander('前瞻營收誤差與去年同期基準（第一筆封存、最新可見公告）'):
            st.dataframe(pd.DataFrame(h223['feedback']),hide_index=True,use_container_width=True);st.dataframe(pd.DataFrame(h223['outcomes']),hide_index=True,use_container_width=True)
        if st.checkbox('產生 H223 情境研究 Excel'):
            from godpick_h223_growth_scenarios import excel as excel_h223
            st.download_button('下載 H223 成長情境',excel_h223(h223),'H223_GrowthScenarios.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as ex:st.error('H223 成長情境驗收失敗：'+str(ex)[:180])

st.subheader('H224｜營收加速度與財報籌碼共振驗收')
@st.cache_data(ttl=120,show_spinner=False)
def load_h224(path):
    from godpick_h224_growth_resonance import run
    return run(Path(path))
if st.checkbox('載入 H224 成長共振研究（唯讀）'):
    try:
        if st.button('重新讀取 H224'):load_h224.clear()
        with st.spinner('核對實際營收、財報、同日技術及籌碼證據…'):h224=load_h224(str(root))
        st.caption(h224['version']+'｜'+h224['asof']+'｜資料日 '+h224['day'])
        st.warning('所有分層均為獨立研究，不等於買進；營收加速度是實際同比中位數差，非預測。EPS股本變動仍待核。')
        st.write(h224['status'],h224['counts'])
        st.dataframe(pd.DataFrame(h224['rows']),hide_index=True,use_container_width=True)
        with st.expander('全市場營收加速度與回補驗收'):
            st.dataframe(pd.DataFrame(h224['acceleration']),hide_index=True,use_container_width=True)
            st.dataframe(pd.DataFrame(h224['scope']),hide_index=True,use_container_width=True);st.write(h224['errors'])
        with st.expander('H223 營收情境誤差（非H224交易績效）'):st.dataframe(pd.DataFrame(h224['forecast_feedback']),hide_index=True,use_container_width=True)
        if st.checkbox('產生 H224 共振驗收 Excel'):
            from godpick_h224_growth_resonance import excel as excel_h224
            st.download_button('下載 H224 共振驗收',excel_h224(h224),'H224_GrowthResonance.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as ex:st.error('H224 共振驗收失敗：'+str(ex)[:180])


st.subheader('H225｜全市場資料品質與主管研究整合')
@st.cache_data(ttl=120,show_spinner=False)
def load_h225(path):
    from godpick_h225_integrated_quality import run
    return run(Path(path))
if st.checkbox('載入 H225 統一驗收（唯讀）'):
    try:
        if st.button('重新讀取 H225'):load_h225.clear()
        with st.spinner('重驗資料管線及同日研究封存…'):h225=load_h225(str(root))
        st.caption(h225['version']+'｜'+h225['asof']+'｜資料日 '+h225['day'])
        st.warning('獨立研究去重，不等同原掃描同輪或買進；正式授權僅依第7頁原H126。')
        st.write(h225['status'],h225['first_counts'])
        st.dataframe(pd.DataFrame(h225['gates']),hide_index=True,use_container_width=True)
        st.dataframe(pd.DataFrame(h225['shortlist']),hide_index=True,use_container_width=True)
        with st.expander('來源、全量研究及未入前段'):st.dataframe(pd.DataFrame(h225['sources']),hide_index=True,use_container_width=True);st.dataframe(pd.DataFrame(h225['rows']),hide_index=True,use_container_width=True);st.write(h225['errors'])
        with st.expander('H224 獨立含成本模擬績效'):st.dataframe(pd.DataFrame(h225['maturity']),hide_index=True,use_container_width=True);st.dataframe(pd.DataFrame(h225['detail']),hide_index=True,use_container_width=True)
        with st.expander('H219 首批研究召回及技術缺口'):st.dataframe(pd.DataFrame(h225['recall']),hide_index=True,use_container_width=True);st.dataframe(pd.DataFrame(h225['recall_gaps']),hide_index=True,use_container_width=True)
        if st.checkbox('產生 H225 統一驗收 Excel'):
            from godpick_h225_integrated_quality import excel as excel_h225
            st.download_button('下載 H225 統一驗收',excel_h225(h225),'H225_IntegratedQuality.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as ex:st.error('H225 統一驗收失敗：'+str(ex)[:180])


st.subheader('H226｜正式實戰驗收及資料缺口待辦')
if st.checkbox('載入 H226 實戰驗收（唯讀）'):
    try:
        from godpick_h226_live_acceptance import run as live_acceptance,excel as live_excel
        with st.spinner('核對首批績效、實際資料缺口及研究證據…'):h226=live_acceptance(Path(root))
        st.caption(h226['version']+'｜'+h226['asof']+'｜'+h226['day'])
        st.warning('原價、淨績效及預測準確率分開；單批描述性結果不得調整正式權重。')
        st.write(h226['status'],h226['batch_status'],h226['counts'])
        st.dataframe(pd.DataFrame(h226['batch']),hide_index=True,use_container_width=True)
        st.dataframe(pd.DataFrame(h226['tasks']),hide_index=True,use_container_width=True)
        with st.expander('個股缺日與強勢漏選'):st.dataframe(pd.DataFrame(h226['stock_gaps']),hide_index=True,use_container_width=True);st.dataframe(pd.DataFrame(h226['miss']),hide_index=True,use_container_width=True)
        if st.checkbox('產生 H226 實戰驗收 Excel'):
            st.download_button('下載 H226 實戰驗收',live_excel(h226),'H226_LiveAcceptance.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as ex:st.error('H226 驗收失敗：'+str(ex)[:180])


st.subheader('H227｜強勢命中精度、績效反證與策略校正')
if st.checkbox('載入 H227 真實績效反證（唯讀）'):
    try:
        from godpick_h227_strategy_validation import run as strategy_review,excel as strategy_excel
        with st.spinner('核對前瞻命中、原始決策與完整策略日窗…'):h227=strategy_review(Path(root))
        st.caption(h227['version']+'｜'+h227['asof']+'｜'+h227['day'])
        st.warning('強勢榜為當前主檔回顧；非正式買進命中。缺前瞻證據或成熟績效不算零；不自動調權。')
        st.write(h227['status'],h227['batch_status'],h227['counts'])
        st.dataframe(pd.DataFrame(h227['precision']),hide_index=True,use_container_width=True)
        st.dataframe(pd.DataFrame(h227['validation']),hide_index=True,use_container_width=True)
        with st.expander('產業與逐股漏選、當時初期型態'):
            for k in ('sectors','miss','stages','pairs'):st.dataframe(pd.DataFrame(h227[k]),hide_index=True,use_container_width=True)
        if st.checkbox('產生 H227 策略驗收 Excel'):
            st.download_button('下載 H227 策略驗收',strategy_excel(h227),'H227_StrategyValidation.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as ex:st.error('H227 驗收失敗：'+str(ex)[:180])


st.subheader('H228｜首批成熟實證、全市場排名競爭與優先複核')
if st.checkbox('載入 H228 全市場競爭（唯讀）'):
    try:
        from godpick_h228_market_competition import run as market_competition,excel as competition_excel
        with st.spinner('核對前瞻研究與全市場原價排名…'):h228=market_competition(Path(root))
        st.caption(h228['version']+'｜'+h228['asof']+'｜'+h228['day'])
        st.warning('排名為當前主檔回顧、非含息績效；等量隨機基準不是可交易策略；正式授權仍依原H126。')
        st.write(h228['status'],h228['batch_status'],h228['counts'])
        for k in ('batch','competition','shortlist'):st.dataframe(pd.DataFrame(h228[k]),hide_index=True,use_container_width=True)
        with st.expander('逐股排名、強勢榜與樣本外'):
            for k in ('detail','precision','validation'):st.dataframe(pd.DataFrame(h228[k]),hide_index=True,use_container_width=True)
        if st.checkbox('產生 H228 全市場競爭 Excel'):
            st.download_button('下載 H228 全市場競爭',competition_excel(h228),'H228_MarketCompetition.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as ex:st.error('H228 驗收失敗：'+str(ex)[:180])


st.subheader('H229｜同成本實戰報酬優勢與首批深層歸因')
if st.checkbox('載入 H229 同成本報酬驗收（唯讀）'):
    try:
        from godpick_h229_net_edge import run as net_edge,excel as net_excel
        with st.spinner('核對首批隔夜、日內、成本及市場同模型比較…'):h229=net_edge(Path(root))
        st.caption(h229['version']+'｜'+h229['asof']+'｜'+h229['day'])
        st.warning('當前主檔回顧，固定千股同成本模擬，非券商成交。跳空為未還原價格，可能受權息影響；歸因為算術分類，不是因果證明。樣本外僅時序複核，不自動調權。')
        st.write(h229['status'],h229['batch_status'],h229['counts'])
        for k in ('batch','net','causes','validation'):st.dataframe(pd.DataFrame(h229[k]),hide_index=True,use_container_width=True)
        with st.expander('首批逐股歸因、基準缺口及優先複核'):
            for k in ('detail','benchmark_gaps','shortlist'):st.dataframe(pd.DataFrame(h229[k]),hide_index=True,use_container_width=True)
        if st.checkbox('產生 H229 同成本報酬 Excel'):
            st.download_button('下載 H229 報酬驗收',net_excel(h229),'H229_NetEdge.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as ex:st.error('H229 驗收失敗：'+str(ex)[:180])


st.subheader('H237｜正式資料管線驗收（唯讀）')
if st.checkbox('載入 H237 原始證據與成熟績效驗收'):
    try:
        from godpick_h237_pipeline_acceptance import run as pipeline_acceptance,excel as pipeline_excel
        h237_full=st.checkbox('一併重新核對 H225～H229 完整成熟績效（較耗時）',key='h237_full')
        with st.spinner('核對原始財報、封存及實機資料缺口…'):h237=pipeline_acceptance(Path(root),full=h237_full)
        st.caption(h237['version']+'｜'+h237['asof']+'｜'+h237['status'])
        st.info('收集器健康不等於原始重驗；無雲端下載、無買進授權或權重變更。')
        st.dataframe(pd.DataFrame(h237['checks']),hide_index=True,use_container_width=True)
        for k in ('summary','candidates','shortlist','first'):st.dataframe(pd.DataFrame(h237[k]),hide_index=True,use_container_width=True)
        with st.expander('分期對帳、原始批次證據、品質閘門及異常'):
            for k in ('periods','local_batch','gates','errors'):st.dataframe(pd.DataFrame(h237[k]),hide_index=True,use_container_width=True)
        if st.checkbox('產生 H237 正式管線 Excel'):
            st.download_button('下載 H237 正式管線驗收',pipeline_excel(h237),'H237_PipelineAcceptance.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as ex:st.error('H237 驗收失敗：'+type(ex).__name__)


st.subheader('H238｜首批T+1固定日期績效解剖（唯讀）')
if st.checkbox('載入 H238 首批強勢漏選與高成長複核'):
    try:
        from godpick_h238_first_attribution import run as first_attribution,excel as first_attribution_excel
        with st.spinner('重新核對固定首批研究日與下一交易日…'):h238=first_attribution(Path(root))
        st.caption(h238['version']+'｜'+h238['asof']+'｜'+h238['status'])
        st.info('首批研究日 '+str(h238['first_day'] or '待封存')+'；T+1 '+str(h238['target'] or '待官方盤後確認')+'。研究命中與正式H126授權不同，單批不調權。')
        for k in ('attribution','miss_summary','growth'):st.dataframe(pd.DataFrame(h238[k]),hide_index=True,use_container_width=True)
        with st.expander('逐股績效、漏選原因及同成本競爭'):
            for k in ('detail','miss','precision','market_net','tasks','errors'):st.dataframe(pd.DataFrame(h238[k]),hide_index=True,use_container_width=True)
        if st.checkbox('產生 H238 首批績效 Excel'):
            st.download_button('下載 H238 首批績效解剖',first_attribution_excel(h238),'H238_FirstT1Attribution.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as ex:st.error('H238 驗收失敗：'+type(ex).__name__)


st.subheader('H239｜主流輪動與重複研究反證（唯讀）')
if st.checkbox('載入 H239 同市場同分級反證'):
    try:
        from godpick_h239_rotation_counterproof import run as rotation_review,excel as rotation_excel
        from godpick_h238_first_attribution import run as first_review
        from godpick_h225_integrated_quality import run as integrated_review
        with st.spinner('核對原始封存、前五交易日及決策前產業輪動…'):
            _h239base=integrated_review(Path(root),include_detail=True)
            _h239first=first_review(Path(root),_h239base['asof'],base=_h239base)
            h239=rotation_review(Path(root),_h239base['asof'],base=_h239base,first_review=_h239first)
        st.caption(h239['version']+'｜'+h239['asof']+'｜'+h239['status'])
        st.info('近五交易日封存缺期不視為新股；產業為當前主檔回顧，同市場同分級完整成熟樣本才比較。非正式買進，不調權。')
        st.dataframe(pd.DataFrame([h239['counts']]),hide_index=True,use_container_width=True)
        for k in ('pairs','sectors'):st.dataframe(pd.DataFrame(h239[k]),hide_index=True,use_container_width=True)
        with st.expander('逐股反證、首批漏選及異常'):
            for k in ('detail','first_miss','errors'):st.dataframe(pd.DataFrame(h239[k]),hide_index=True,use_container_width=True)
        if st.checkbox('產生 H239 反證 Excel'):
            st.download_button('下載 H239 主流輪動反證',rotation_excel(h239),'H239_RotationCounterproof.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as ex:st.error('H239 驗收失敗：'+type(ex).__name__)
