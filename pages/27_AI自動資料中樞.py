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


st.divider()
st.subheader('H137｜決策封存 × 夜間同步異常告警')
st.caption('GitHub Actions 每個工作日台灣時間23:00做盤後檢查；僅為內部稽核，並非獨立PIT認證或真正不可修改的存證。')
from godpick_h137_auto_seal import read_remote_audit
if st.button('重新檢查 H137 決策封存與同步稽核', key='h137_audit_refresh'):
    st.session_state.pop('h137_latest_audit', None)
if 'h137_latest_audit' not in st.session_state:
    st.session_state['h137_latest_audit'] = read_remote_audit(
        asof=datetime.now(ZoneInfo('Asia/Taipei')).date())
audit = st.session_state['h137_latest_audit']
if audit.get('status') == 'INTERNAL_CHECK_PASS_NOT_PIT':
    st.success('H137 最近稽核：內部檢查通過（不代表獨立時間存證）')
elif audit.get('status') == 'BLOCKED_REQUIRES_ATTENTION':
    st.error('H137 最近稽核：決策封存或自動同步有缺漏，需查明原因')
else:
    st.warning('H137 稽核尚未取得最新有效資訊：' + str(audit.get('status')))
st.caption('稽核資料日：' + str(audit.get('asof_tw', audit.get('last_day', 'UNKNOWN')))
           + '｜當日有效主管決策封存：' + str(audit.get('same_day_live_h126_archives', 'UNKNOWN')))
if audit.get('findings'):
    st.write('需處理：' + '；'.join(audit['findings']))
st.caption('H138可封存既有排程Page07本輪通過驗證的H126決策；若手動/排程皆未成功，H137仍會警告缺失。')


st.divider()
st.subheader('H138｜背景選股封存與主管晨報')
st.caption('背景選股沿用 V191 中央排程的 Page07 正式演算法；H138 只橋接當期 H126 決策。晨報不含實際交易損益或買進授權。')
from godpick_h138_daily_brief import read_remote_brief
if st.button('重新讀取 H138 主管晨報', key='h138_refresh'):
    st.session_state.pop('h138_latest_brief', None)
if 'h138_latest_brief' not in st.session_state:
    st.session_state['h138_latest_brief'] = read_remote_brief(asof=datetime.now(ZoneInfo('Asia/Taipei')).date())
brief = st.session_state['h138_latest_brief']
if brief.get('status') == 'READY_FOR_MANUAL_RESEARCH_NOT_TRADES':
    st.success(brief.get('headline', '可檢閱當日研究封存（非買進）'))
elif brief.get('status') == 'REVIEW_REQUIRED':
    st.warning(brief.get('headline', '需人工複核'))
else:
    st.error('H138晨報尚未驗證：' + str(brief.get('status', 'UNKNOWN')))
st.caption('晨報資料日：' + str(brief.get('asof_tw', brief.get('last_day', '尚無'))) +
           '｜背景封存數：' + str(brief.get('today_confirmed_archives', '未知')))
if brief.get('decision_summary'):
    st.json(brief['decision_summary'])
if brief.get('findings'):
    st.write('待處理：' + '；'.join(map(str, brief['findings'][:20])))
st.info('GitHub內部SHA與建立時間不等於獨立PIT存證；真正交易績效仍須由成交與除權息資料核對。')

st.divider()
st.subheader('H139｜自動同步復原 × T+績效結算 × 研究策略漂移')
st.caption('H139不取得券商成交，也不自動改選股權重。只結算經H136/H137當期核對的收盤價研究觀測；缺少資料即封鎖統計。')
from godpick_h139_settlement import read_remote_settlement, read_remote_recovery
if st.button('重新讀取 H139 研究績效結算', key='h139_refresh'):
    st.session_state.pop('h139_latest_settlement', None)
if 'h139_latest_settlement' not in st.session_state:
    st.session_state['h139_latest_settlement'] = read_remote_settlement(
        asof=datetime.now(ZoneInfo('Asia/Taipei')).date())
settled = st.session_state['h139_latest_settlement']
if settled.get('status') == 'RESEARCH_MATURED_OBSERVATIONS':
    st.success('H139 最近結算：有完成觀測的研究交易日（非實際成交獲利）')
elif settled.get('status') == 'RESEARCH_SETTLED_NO_NEW_MATURITY':
    st.info('H139：資料已通過內部驗證，但目前沒有新的成熟研究訊號')
elif settled.get('status') == 'BLOCKED_NO_VERIFIED_RESEARCH_DATA':
    st.warning('H139：結算已封鎖；缺少當期通過驗證的決策、行情或夜間稽核')
else:
    st.warning('H139 尚未取得有效遠端結算：' + str(settled.get('status', 'UNKNOWN')))
if st.button('重新讀取 H139 重試狀態', key='h139_repair_refresh'):
    st.session_state.pop('h139_latest_recovery', None)
if 'h139_latest_recovery' not in st.session_state:
    st.session_state['h139_latest_recovery'] = read_remote_recovery(
        asof=datetime.now(ZoneInfo('Asia/Taipei')).date())
repair = st.session_state['h139_latest_recovery']
if repair.get('status') == 'RECOVERED_INTERNAL_SHA_EOD_CURRENT':
    st.success('H139 重試：暫存重建及當日資料驗證通過')
elif repair.get('status') == 'ALREADY_READY_NO_RETRY':
    st.info('H139 重試：資料原本就緒，因此未執行重建')
else:
    st.warning('H139 重試狀態：' + str(repair.get('status','尚無資料')))
st.caption('重試次數：' + str(repair.get('attempts', '未知')) +
           '｜未完成原因：' + str(repair.get('remaining_blocker', repair.get('initial_blocker','無'))))
st.caption('研究結算日：' + str(settled.get('asof_tw', settled.get('last_day', '尚無'))) +
           '｜狀態說明：' + str(settled.get('reason', '尚無資料')))
for tag, policy in settled.get('strategies', {}).items():
    with st.expander('H139｜' + tag + ' 研究政策（非交易訊號）'):
        table = [{'期間': h, '成熟完整研究日': v.get('full_cohort_days'),
                  '缺漏研究日': v.get('partial_or_missing_days'),
                  '覆蓋率%': v.get('coverage_pct'),
                  '歷史研究平均超額pp': v.get('mean_research_alpha_pp'),
                  '今日成熟研究日': v.get('newly_settled_research_days'),
                  '策略漂移警示': v.get('drift_advice')}
                 for h, v in policy.get('horizons', {}).items()]
        if table:
            st.dataframe(table, use_container_width=True, hide_index=True)
st.info('提醒：歷史訊號尚未有獨立不可變PIT認證，也未完成企業行動、含息報酬、費稅與實際成交驗證。任何統計結果都不能自動授權買進。')


st.divider()
st.subheader('H140｜雲端排程端到端健康檢查')
st.caption('依 H136～H139 遠端回執核對五個階段；只有當日回執狀態一致才列為內部就緒，不能獨立證明GitHub Actions是否執行。')
from godpick_h140_observability import read_remote_monitor
if st.button('重新檢查 H140 遠端五階段監控', key='h140_refresh'):
    st.session_state.pop('h140_latest_monitor', None)
if 'h140_latest_monitor' not in st.session_state:
    st.session_state['h140_latest_monitor'] = read_remote_monitor(
        asof=datetime.now(ZoneInfo('Asia/Taipei')).date())
h140 = st.session_state['h140_latest_monitor']
if h140.get('status') == 'INTERNAL_PIPELINE_READY_NOT_LIVE_ATTESTED':
    st.success('H140：五階段遠端回執當日一致（內部檢查；仍需查看Actions執行紀錄）')
elif h140.get('status') == 'MARKET_CLOSED_MANUAL_OVERRIDE_NOT_ATTESTED':
    st.info('H140：人工宣告休市，未驗證正式交易所日曆；不宣稱排程成功')
else:
    st.error('H140：未達當日驗收條件：' + str(h140.get('status', 'UNKNOWN')))
st.caption('稽核資料日：' + str(h140.get('asof_tw', h140.get('last_day','未知'))) +
           '｜完成階段：' + str(h140.get('pipeline_stages_ok','?')) + '/' +
           str(h140.get('pipeline_stages_total','5')))
for stage, obj in h140.get('stages',{}).items():
    with st.expander(stage + '｜' + str(obj.get('state','UNKNOWN'))):
        st.write('回執狀態：' + str(obj.get('reported_status','未取得')))
        if obj.get('recommended_action'):
            st.warning(obj['recommended_action'])
if h140.get('findings'):
    st.caption('缺失代碼：' + '；'.join(map(str,h140['findings'][:12])))
st.info('仍需人工核對正式Actions執行記錄、通知、部署分支與Streamlit重啟；此頁不會自動交易或變更正式推薦。')
