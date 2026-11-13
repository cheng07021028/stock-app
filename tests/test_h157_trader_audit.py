# -*- coding: utf-8 -*-
"""Offline, deterministic H157 synthetic and actual Excel contract tests."""
from datetime import date,timedelta
from pathlib import Path
import io
import pytest
import pandas as pd
from openpyxl import Workbook,load_workbook
from godpick_h1532_technical_evidence import evaluate_prebreakout
from godpick_h114_parallel_alpha import analyze_history
from godpick_h157_trader_audit import (OVERVIEW,DETAIL,_workday,
    analyze_research_rows,render_trader_audit)

D='2026-11-12'

def sample(code='1476',stock=4.68):
    return {'股票代號':code,'股票名稱':'測試','排名等級':'B｜研究證據待補',
        '研究排名':1,'研究證據分':48,'H114行情日期':D,
        '大盤同日漲跌%':-2.63,'個股同日漲跌%':stock,'相對強度pp':stock+2.63,
        '對照指數':'加權指數','本益比':None,'近四季EPS':None,
        '原始PE_EPS狀態':'原始PE/EPS待來源或時點驗證',
        '黃金交叉':'均線交叉未確認','箱型型態':'確認突破｜箱頂放量',
        '營收創高':'未驗證','H1534三率三升狀態':'待補官方單季財報',
        'H1534毛利營益突破':'未認證'}


def workbook():
    w=Workbook();w.active.title='00_主管重點觀察榜';w.create_sheet('00_H126主管決策')
    ws=w.create_sheet('01_具名強勢研究')
    rows=[sample(),sample('2355',9.964),sample('3149',-0.44),
          {**sample('2033',3),'排名等級':'W｜非交易核查'},
          {**sample('2499',2),'H114行情日期':'2026-11-11'},
          {**sample('1477',5),'對照指數':'加權指數','相對強度pp':999},
          {**sample('2881',1),'本益比':12,'近四季EPS':5.5,
            '原始PE_EPS狀態':'官方同日可得PE/EPS',
            '營收年增率%':22,'基本面來源狀態':'營收:VERIFIED_ASOF',
            '營收創高':'近12月新高',
            'H1534三率三升狀態':'連兩季三率年增｜獲利為正',
            'H1534毛利營益突破':'近五季新高'}]
    cols=list(dict.fromkeys(k for row in rows for k in row))
    ws.append(cols)
    for row in rows:ws.append([row.get(k) for k in cols])
    w.create_sheet('02_研究召回統計')
    return w


def test_pool_split_dated_relative_and_overheat():
    wb=workbook()
    x=render_trader_audit(wb,research_day=D,
        h145_status='BLOCKED_FINAL_RESULT_COUNT_GAP',h126_buyable=0,run_id='run123')
    assert x['groups']['B｜待補證研究']==6 and x['groups']['W｜非交易核查']==1
    assert x['anti_fall']==5 and x['chase_risk']==1 and x['valid_comparison']==5
    assert x['growth_verified']==1 and x['margin_verified']==1
    assert x['next_workday'].startswith('2026-11-13')
    assert wb.sheetnames[:3]==['00_主管重點觀察榜','00_H126主管決策','01_具名強勢研究']
    assert wb.sheetnames[-2:]==[OVERVIEW,DETAIL]
    assert wb[DETAIL]['A3'].value=='2355'
    assert wb[DETAIL].freeze_panes=='D2'
    assert 'H145完整性阻擋' in str(wb[DETAIL].cell(2, [c.value for c in wb[DETAIL][1]].index('H126正式交易資格')+1).value)
    assert 'BLOCKED' in wb[OVERVIEW]['B5'].value
    assert '模型' in wb[OVERVIEW]['B23'].value
    b=io.BytesIO();wb.save(b);b.seek(0)
    again=load_workbook(b,read_only=True)
    assert len(again.sheetnames)==6 and again[DETAIL]['A3'].value=='2355'


def test_missing_growth_valuation_never_certifies():
    row=sample()
    row.update({'本益比':8,'近四季EPS':12,'營收年增率%':80,
                '原始PE_EPS狀態':'沒有官方來源',
                '基本面來源狀態':'營收:MISSING','營收創高':'近12月新高',
                '營收預估創高判定':'模型情境可能創高｜未驗證'})
    x=analyze_research_rows([row],research_day=D)
    assert x['growth_verified']==0 and x['margin_verified']==0
    assert '不足' in x['rows'][0]['成長價值證據']
    assert '近12月新高' not in x['rows'][0]['成長價值證據']
    assert '未授權' in x['rows'][0]['H126正式交易資格']


def test_asof_mismatch_does_not_make_resilience_or_forecast():
    r=sample()
    r['H114行情日期']='2026-11-13'
    result=analyze_research_rows([r],research_day=D)
    assert result['anti_fall']==0
    assert result['valid_comparison']==0
    assert result['rows'][0]['大盤漲跌%'] is None
    assert '未充分核實' in result['rows'][0]['跌市抗跌']
    assert result['rows'][0]['下一交易日觀察情境'].startswith('下一交易日：缺')


def test_duplicate_not_cherry_picked():
    b=sample();strong=sample(stock=9.95)
    x=analyze_research_rows([b,strong],research_day=D)
    assert len(x['rows'])==1 and x['chase_risk']==0 and x['duplicated']==1


def test_no_unsupported_tomorrow_prediction_and_weekend():
    assert _workday(date(2026,11,13)).startswith('2026-11-16')
    assert '假日' in _workday(date(2026,11,13))
    x=analyze_research_rows([sample()],research_day=D)
    assert '未校準' in x['rows'][0]['下一交易日觀察情境']


def market_bars(n=105):
    days=pd.bdate_range(end='2026-11-12',periods=n)
    items=[]
    for i,d in enumerate(days):
        close=100+i*.12
        items.append({'day':d.date(),'open':close-.05,'high':close+.2,
                      'low':close-.2,'close':close,'lots':1200+i%5*100})
    return pd.DataFrame(items)


def test_new_indicators_require_complete_same_day_ohlcv():
    d=market_bars()
    result=evaluate_prebreakout(d)
    assert 0<=result['H157_RSI14']<=100
    assert 0<=result['H157_ATR14_PCT']<100
    assert result['H157_MACD'] not in ('', '未驗證')
    assert result['H157_VOLUME_BREAKOUT'] not in ('','未驗證')
    missing=evaluate_prebreakout(d.iloc[-60:])
    assert missing['H157_RSI14'] is None
    assert missing['H157_MACD']=='未驗證'
    assert missing['H157_VOLUME_BREAKOUT']=='未驗證'
    broken=d.copy();broken.iloc[-1,broken.columns.get_loc('lots')]=0
    result=evaluate_prebreakout(broken)
    assert result['H157_RSI14'] is None and result['H157_MACD']=='未驗證'


def test_h114_future_candle_has_no_effect_on_h157_indicators():
    d=market_bars(); idx={'day':'日期','open':'開盤價','high':'最高價',
        'low':'最低價','close':'收盤價','lots':'成交量(張)'}
    w=d.rename(columns=idx)
    first=analyze_history(w,asof='2026-11-12')
    future=w.iloc[-1:].copy()
    future['日期']=[date(2026,11,16)]
    for col in ('開盤價','最高價','最低價','收盤價'):future[col]=1200
    second=analyze_history(pd.concat([w,future]),asof='2026-11-12')
    for name in ('H157_RSI14','H157_MACD','H157_ATR14_PCT','H157_VOLUME_BREAKOUT'):
        assert first.get(name)==second.get(name),name


def test_old_worksheet_is_not_overwritten_and_second_render_blocked():
    w=workbook();before=w['01_具名強勢研究']['A2'].value
    render_trader_audit(w,research_day=D)
    assert w['01_具名強勢研究']['A2'].value==before
    with pytest.raises(ValueError,match='禁止覆蓋'):
        render_trader_audit(w,research_day=D)
