# -*- coding: utf-8 -*-
"""H135 read-only manager research oversight; no writes to recommendation authority."""
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo
from io import BytesIO
import json

import pandas as pd
import streamlit as st

from app_auth import require_login
require_login()
from godpick_h134_decision_vault import list_recent, read_archive
from godpick_h136_auto_hub import remote_cache
from godpick_h135_signal_governance import (
    VERSION, lifecycle_report, fixed_challenger_comparison,
    MAX_ARCHIVES,
)

st.set_page_config(page_title='H135｜交易訊號生命週期與策略審查', layout='wide')
st.title('26｜交易訊號生命週期 × 策略優勝劣汰')
st.caption(f'{VERSION}｜H134封存核對 × H133樣本外研究 × Top3/Top5固定政策｜僅檢視，不修改H109/H126。')
st.warning('本頁是研究與資料稽核，不是買進／賣出／自動調整模型權重。GitHub SHA驗證不等於獨立PIT時點證明。')
st.page_link('pages/25_主管智慧決策戰情.py', label='返回 H134｜主管決策封存', icon='📊')
st.page_link('pages/24_六引擎交易研究.py', label='返回 H133｜六引擎研究', icon='🔬')
st.page_link('pages/27_AI自動資料中樞.py', label='H136｜AI自動資料中樞／更新狀態', icon='🔄')

asof = st.date_input('研究截止日（台北時間；不使用截止日之後的訊號或價格）',
                     value=datetime.now(ZoneInfo('Asia/Taipei')).date(),
                     max_value=datetime.now(ZoneInfo('Asia/Taipei')).date(), key='h135_asof')


def safe_csv(frame: pd.DataFrame) -> bytes:
    data = frame.copy()
    for col in data.select_dtypes(include='object'):
        data[col] = data[col].map(lambda v: "'" + v if isinstance(v, str) and v.lstrip().startswith(('=', '+', '-', '@')) else v)
    return data.to_csv(index=False).encode('utf-8-sig')


def render_table(title: str, data: pd.DataFrame, key: str) -> None:
    st.markdown('**' + title + '**')
    if not isinstance(data, pd.DataFrame) or data.empty:
        st.info('沒有可使用的資料。')
        return
    st.dataframe(data, hide_index=True, use_container_width=True)
    st.download_button('下載研究稽核 CSV', safe_csv(data),
                       file_name=f'StockApp_H135_{key}.csv', mime='text/csv', key=f'h135_dl_{key}')


def read_sheet(up, *, byte_limit: int = 20_000_000) -> pd.DataFrame:
    if up is None:
        return pd.DataFrame()
    if up.size > byte_limit:
        st.error('單一資料檔超過20MB，請先精簡必要資料。')
        return pd.DataFrame()
    try:
        raw = up.getvalue()
        if up.name.lower().endswith('.xlsx'):
            return pd.read_excel(BytesIO(raw), sheet_name=0, dtype={'股票代號': str})
        return pd.read_csv(BytesIO(raw), encoding='utf-8-sig', dtype={'股票代號': str})
    except (ValueError, UnicodeError, OSError, ImportError):
        st.error('檔案讀取失敗：需要有效的Excel或UTF-8 CSV，以及標準欄位。')
        return pd.DataFrame()


lifecycle_tab, strategy_tab = st.tabs(['01｜每日決策與訊號生命週期', '02｜研究策略 Top3／Top5 樣本外比較'])
with lifecycle_tab:
    st.subheader('H134 歷史封存差異｜資料內雜湊不能代表已證實事前訊號')
    st.caption('從 GitHub 按需讀取最近12份封存；若只有12份，仍不足以認證H133長期績效。')
    if st.button('從GitHub重新讀取最近12份封存並驗證', key='h135_reload'):
        snapshots = []
        entries, state = list_recent()
        for entry in entries:
            if entry.get('遠端驗證') != 'READBACK_HASH_VERIFIED':
                continue
            payload, read_state = read_archive(entry.get('封存路徑', ''))
            if read_state == 'READBACK_HASH_VERIFIED':
                snapshots.append(payload)
        st.session_state['h135_snapshots'] = snapshots
        st.session_state['h135_remote_status'] = state
    st.caption('遠端查詢狀態：' + str(st.session_state.get('h135_remote_status', 'NOT_REQUESTED')))
    archives = list(st.session_state.get('h135_snapshots', []))
    uploaded = st.file_uploader('也可加入 H134 JSON（多選；補登不會變成PIT）',
                                type=['json'], accept_multiple_files=True, key='h135_archives')
    if uploaded:
        for up in uploaded[:MAX_ARCHIVES]:
            if up.size > 500_000:
                st.warning('跳過大於500KB的JSON：' + up.name[:60])
                continue
            try:
                candidate = json.loads(up.getvalue())
                if isinstance(candidate, dict):
                    archives.append(candidate)
                else:
                    st.warning('跳過非H134 JSON物件：' + up.name[:60])
            except (ValueError, UnicodeError):
                st.warning('無法解析JSON：' + up.name[:60])
    if st.button('執行 H135 生命週期稽核', key='h135_lifecycle'):
        st.session_state['h135_lifecycle_result'] = lifecycle_report(archives, asof=asof)
        st.session_state['h135_lifecycle_day'] = asof
    found = st.session_state.get('h135_lifecycle_result') if st.session_state.get('h135_lifecycle_day') == asof else None
    if isinstance(found, dict):
        st.write(f"狀態：{found['狀態']}｜有效不同研究日封存：{found['有效封存數']} 份")
        render_table('封存日期／內容稽核', found['封存稽核'], 'ArchiveAudit')
        render_table('研究層別與進出觀察（移出≠賣出）', found['逐日生命週期'], 'Lifecycle')
        render_table('變化類型統計', found['摘要'], 'Transitions')
        st.caption('同日不同內容全部隔離。首次載入僅屬基準，不是新交易訊號；中間日期缺封存時不得推算每日連續持股。')
        signals = found['歷史研究訊號']
        if isinstance(signals, pd.DataFrame) and not signals.empty:
            render_table('已知封存來源產生的 H133 訊號草稿（PIT未獨立認證）', signals, 'SignalsDraft')

with strategy_tab:
    st.subheader('H135｜固定研究政策：H126 Top3 vs Top5')
    st.caption('兩種預先固定候選數上限，不能在看到樣本外成績後擇優宣稱已驗證。需要多個歷史交易日的事前訊號、個股與大盤價格。')
    st.warning('H134最近12份與上傳舊報表仍不足以證明真實PIT；本頁不使用樣本外結果自動優化選股門檻。')
    st.markdown('**H136｜自動載入已核對遠端研究資料（優先，不需上傳三份檔案）**')
    use_auto = st.checkbox('優先使用 H136 自動同步資料；缺資料才改為手動上傳', value=True, key='h136_use_remote')
    @st.cache_data(ttl=1800, show_spinner=False)
    def _h136_cached_data():
        return remote_cache(asof=datetime.now(ZoneInfo('Asia/Taipei')).date())
    auto_frames, auto_health = _h136_cached_data() if use_auto else ({}, {'status': 'DISABLED'})
    if use_auto:
        st.caption('遠端同步狀態：' + str(auto_health.get('status', 'UNKNOWN')) + '｜資料日：' +
                   str(auto_health.get('asof', auto_health.get('last_run_day', '無'))))
        if auto_frames:
            st.info('已自動取得 H134 歷史訊號、H117 衍生個股行情及大盤資料；仍不是獨立PIT／含息還原績效證明。')
        else:
            st.warning('自動資料未就緒。可保留手動匯入；不會自動以舊資料冒充今日結果。')
    if st.button('重新檢查 H136 雲端資料', key='h136_clear_cache'):
        _h136_cached_data.clear()
        st.rerun()
    with st.expander('H136 無資料時才需要｜手動上傳備援', expanded=not bool(auto_frames)):
        signals_up = st.file_uploader('跨日期研究訊號 CSV／Excel', type=['csv', 'xlsx'], key='h135_signals')
        stock_up = st.file_uploader('個股歷史日K（股票代號、日期、收盤價）', type=['csv', 'xlsx'], key='h135_stocks')
        benchmark_up = st.file_uploader('同期大盤日K（日期、收盤價）', type=['csv', 'xlsx'], key='h135_benchmark')
    use_lifecycle = st.checkbox('使用上方H134已核對封存生成的訊號草稿（仍非獨立PIT）', key='h135_use_snapshots')
    signals = auto_frames.get('signals', pd.DataFrame()) if auto_frames else read_sheet(signals_up)
    if use_lifecycle:
        found = st.session_state.get('h135_lifecycle_result')
        if isinstance(found, dict) and st.session_state.get('h135_lifecycle_day') == asof:
            signals = found.get('歷史研究訊號', pd.DataFrame())
    stocks = auto_frames.get('stock', pd.DataFrame()) if auto_frames else read_sheet(stock_up)
    benchmark = auto_frames.get('benchmark', pd.DataFrame()) if auto_frames else read_sheet(benchmark_up)
    adjusted = st.checkbox('我已核對個股價格已處理拆併股／除權息調整（仍非來源獨立稽核）', key='h135_adjusted',
                           disabled=bool(auto_frames))
    if auto_frames:
        adjusted = False  # H117 EOD衍生檔不含已核對除權息證明，嚴禁自行勾成真。
    min_days = st.number_input('樣本外最低有效研究交易日', min_value=10, max_value=120, value=20, step=5, key='h135_min_oos')
    if st.button('執行 H135 雙政策樣本外審查（不調權）', key='h135_compare'):
        if signals.empty or stocks.empty or benchmark.empty:
            st.error('三份完整輸入缺一不可，不會填入虛構市場行情。')
        else:
            with st.spinner('正在進行時間切分與完整日樣本驗證…'):
                st.session_state['h135_policy_report'] = fixed_challenger_comparison(
                    signals, stocks, benchmark, asof=asof,
                    adjusted_confirmed=adjusted, min_oos_days=int(min_days))
                st.session_state['h135_policy_day'] = asof
                st.session_state['h135_policy_setting'] = (bool(adjusted), int(min_days))
    report = st.session_state.get('h135_policy_report')
    if (isinstance(report, dict) and st.session_state.get('h135_policy_day') == asof
            and st.session_state.get('h135_policy_setting') == (bool(adjusted), int(min_days))):
        st.warning(report['研究限制'])
        render_table('兩組策略的逐期間稽核，不代表買進建議', report['比較決策'], 'PolicyReview')
        for name, details in report['策略研究'].items():
            with st.expander(name + '｜逐日樣本及績效口徑'):
                st.caption(str(details.get('狀態')) + '｜' + str(details.get('說明', '')))
                for key in ('分割摘要', '樣本外績效', '訊號稽核'):
                    render_table(key, details.get(key, pd.DataFrame()), name + '_' + key)

st.caption('H135不寫GitHub／Firestore、不建立買進訊號、不改正式H109/H126決策、不回填任何歷史績效。')
