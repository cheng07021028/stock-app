# -*- coding: utf-8 -*-
from __future__ import annotations
import io
import pytest
import pandas as pd
from openpyxl import Workbook, load_workbook
from godpick_h165_evidence_gate import (
    severe_fall_reason, leader_start_gate, build_evidence_review, append_h165_review,
)
from godpick_h157_trader_audit import render_trader_audit, analyze_research_rows
from godpick_h153_growth_setup_rank import build_growth_setup_ranking

DAY='2026-11-16'


def ranked_record(code='6199', five=2.5):
    return {'股票代號':code,'股票名稱':'測試股','研究排名':1,
        '排名等級':'B｜研究證據待補（不得交易）','研究證據分':35,
        'H154研究排序分':30,'行情證據日':DAY,
        '近5日漲幅%':five,'當日漲跌幅%':0,'H114行情日期':DAY,
        '本益比':None,'近四季EPS':None,'營收年增率%':None,
        '原始PE_EPS狀態':'原始PE/EPS待來源或時點驗證',
        '基本面來源狀態':'營收:MISSING','H1534三率三升狀態':'待補官方單季財報',
        'H1534毛利營益突破':'未認證','H157_RSI14':None,'H157_MACD':'未驗證',
        '多日抗跌來源狀態':'上櫃或市場未證：不得使用加權指數代替櫃買指數'}


def diagnostic(code='3595', ret5=-28.804):
    return {'股票代號':code,'股票名稱':'測試','類別':'其他',
        'H114K線證據日':DAY,'H114K線資料狀態':'VALID_OHLCV','H114K線有效根數':85,
        'H114當日收盤價':100,'H113當日成交額百萬':250,
        'H113二十日均成交額百萬':160,'H114當日成交量張':2000,
        'H114K線量比':2.0,'H114當前5日漲跌幅%':ret5,
        'H114當日漲跌幅%':-1.5,'H114黃金交叉':'確認黃金交叉',
        'H114箱型狀態':'接近箱頂','H114量縮墊高':'是','H114波動收斂':'是',
        'H114距20日高點%':2.0,'H109決策權威':'UNKNOWN'}


def base_wb():
    w=Workbook();w.active.title='00_主管重點觀察榜'
    w.create_sheet('00_H126主管決策')
    ws=w.create_sheet('01_具名強勢研究')
    records=[ranked_record('6199'),ranked_record('3595',-28.804)]
    head=list(records[0]);ws.append(head)
    for r in records:ws.append([r.get(k) for k in head])
    sm=w.create_sheet('02_研究召回統計');sm.append(['檢查項目','結果','說明'])
    for k,v in {'診斷母體原始列':1196,'H113稽核原始列數':120,
               'H153.1 H115原始PE/EPS帳本實際列數':0}.items():sm.append([k,v,''])
    for i in range(3,11):w.create_sheet(f'{i:02}_原表{i}')
    return w


def test_extreme_negative_five_day_is_not_a_launch_setup():
    assert '急跌' in severe_fall_reason(five_day=-28.804)
    assert '急跌' in severe_fall_reason(five_day=0,today=-8.5)
    assert severe_fall_reason(five_day=-4,today=-2)==''
    assert severe_fall_reason(five_day=None,today=None)==''


def test_h153_excludes_extreme_crash_but_keeps_normal_setup():
    r,diag,meta=build_growth_setup_ranking(diagnosis_frame=pd.DataFrame([diagnostic()]),
        research_frame=pd.DataFrame(),research_day=DAY)
    assert r.empty
    assert meta['reasons'].get('H165極端跌勢隔離／待止跌認證')==1
    normal=diagnostic(code='6199',ret5=3)
    r,diag,meta=build_growth_setup_ranking(diagnosis_frame=pd.DataFrame([normal]),
        research_frame=pd.DataFrame(),research_day=DAY)
    assert len(r)==1
    assert r.iloc[0]['股票代號']=='6199'
    assert 'H165主升起漲研究核查' in r.columns
    assert '不可認定主升' in r.iloc[0]['H165主升起漲研究核查']


def test_leader_gate_only_evidence_not_h126():
    props={'yoy_margin_two':True,'profit_breakout':True,'revenue_verified':True,
           'revenue_yoy':20,'revenue_3m_yoy':17,'price_cross':True,
           'volume_ratio':1.7,'price_bars':85,'macd_status':'確認MACD金叉','five_day':5}
    assert '研究共振' in leader_start_gate(**props)
    assert '非H126' in leader_start_gate(**props)
    assert '不可認定' in leader_start_gate(**{**props,'yoy_margin_two':False})
    assert '不可認定' in leader_start_gate(**{**props,'price_bars':60})
    assert '不可認定' in leader_start_gate(**{**props,'volume_ratio':1})
    assert '不可認定' in leader_start_gate(**{**props,'five_day':-23})
    assert '不可認定' in leader_start_gate(**{**props,'revenue_verified':False})


def test_audit_never_certifies_missing_fundamentals_or_bad_benchmark():
    r=ranked_record()
    r.update({'對照指數':'加權指數','大盤同日漲跌%':-6,
              '個股同日漲跌%':0,'相對強度pp':6})
    report=build_evidence_review([r],{'H153.1 H115原始PE/EPS帳本實際列數':0},
        research_day=DAY,h126_buyable=0,h145_status='SUPPLIED_FULL_SCAN_MATCHED')
    assert report['verified_antifall']==0
    assert report['verified_growth']==0
    assert report['verified_margin']==0
    assert '帳本0筆' in report['warnings'][0]
    assert '不可下單' in report['rows'][0]['交易授權']


def test_market_validated_relative_still_needs_source():
    r=ranked_record()
    r.update({'對照指數':'櫃買指數','多日抗跌來源狀態':'同日櫃買已核',
              '大盤同日漲跌%':-4,'個股同日漲跌%':-0.2,'相對強度pp':3.8})
    report=build_evidence_review([r],{},research_day=DAY)
    assert report['verified_antifall']==1
    r['相對強度pp']=99
    assert build_evidence_review([r],{},research_day=DAY)['verified_antifall']==0


def test_duplicate_guard_and_five_day_risk_visible():
    r=ranked_record('3595',-28.804)
    review=build_evidence_review([r,r],{},research_day=DAY)
    assert review['checked']==1
    assert review['risk_crash_codes']==['3595']
    assert review['rows'][0]['交易員證據分池'].startswith('急跌隔離')
    assert review['rows'][0]['原研究分']==35


def test_full_wb_keep_14_tabs_and_append_below_h164():
    w=base_wb();names=list(w.sheetnames)
    original=[w['01_具名強勢研究'].cell(2,i).value for i in range(1,10)]
    result=render_trader_audit(w,research_day=DAY,h145_status='SUPPLIED_FULL_SCAN_MATCHED',h126_buyable=0)
    assert len(w.sheetnames)==len(names)+2 == 14 # H157 adds original 11/12 only
    assert 'h165_evidence' in result
    assert result['h165_evidence']['risk_crash_codes']==['3595']
    assert result['h165_evidence']['start_row']>result['h164_focus']['table_row']
    assert [w['01_具名強勢研究'].cell(2,i).value for i in range(1,10)]==original
    b=io.BytesIO();w.save(b);b.seek(0)
    z=load_workbook(b,read_only=True)
    assert 'H165' in z['11_交易員重點導航'].cell(result['h165_evidence']['start_row'],1).value
    with pytest.raises(ValueError,match='禁止重複'):
        append_h165_review(w,research_day=DAY)


def test_failed_input_no_empty_replacement():
    with pytest.raises(ValueError,match='缺11導航'):
        append_h165_review(base_wb(),research_day=DAY)


def test_h157_missing_otc_market_source_not_false_antifall():
    r=ranked_record()
    r.update({'對照指數':'加權指數','大盤同日漲跌%':-6,'個股同日漲跌%':0,
              '相對強度pp':6,'H114行情日期':DAY})
    res=analyze_research_rows([r],research_day=DAY)
    assert res['anti_fall']==0
    assert res['valid_comparison']==0


def test_no_future_day_awarded():
    r=ranked_record()
    r.update({'行情證據日':'2026-11-17','H114行情日期':'2026-11-17',
              '大盤同日漲跌%':-4,'個股同日漲跌%':0,'相對強度pp':4,
              '對照指數':'櫃買指數','多日抗跌來源狀態':'同日櫃買已核'})
    assert build_evidence_review([r],{},research_day=DAY)['verified_antifall']==0

