# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
from datetime import date
import pandas as pd
from openpyxl import Workbook
from godpick_h175_evidence_rescue import build_h175_audit,append_h175_excel
from godpick_h114_parallel_alpha import analyze_history

DAY='2026-11-19'
def mock_input():
    diagnosis=pd.DataFrame([
      {'股票代號':'2892','股票名稱':'第一金','H114K線資料狀態':'INVALID_OHLCV_VOLUME',
       'H114K線證據日':DAY,'H114K線有效根數':164,'H175最近60根量價完整數':59,
       'H175最近60根成交量非正數':1,'H109決策權威':'FADE_VETO'},
      {'股票代號':'3016','股票名稱':'嘉晶','H114K線資料狀態':'VALID_OHLCV',
       'H114K線證據日':DAY,'H114K線有效根數':100,'H175最近60根量價完整數':60,
       'H109決策權威':'RESEARCH_ELIGIBLE'},
    ])
    tables={'waiting':pd.DataFrame([
        {'股票代號':'2892','股票名稱':'第一金'},
        {'股票代號':'3016','股票名稱':'嘉晶'},
        {'股票代號':'0055','股票名稱':'ETF'}]),
        'emerging_watch':pd.DataFrame([{'股票代號':'3016','股票名稱':'嘉晶'}])}
    audit={'run_identity':'MATCHED','run_id':'gprun_2026-11-19','h145_status':'SUPPLIED_FULL_SCAN_MATCHED'}
    return diagnosis,tables,audit

def build():
    d,t,a=mock_input()
    return build_h175_audit(diagnosis=d,tables=t,h154_audit=a,study_day=DAY)

def book():
    wb=Workbook(); wb.active.title='00_主管重點觀察榜'
    wb.active['A1']='original manager';wb.active['A8']='preserve'
    ws=wb.create_sheet('00_H126主管決策');ws['G2']='禁止買進';ws['A1']='authority'
    for n in ('01_具名強勢研究','02_研究召回統計','03_五檔研究計畫',
              '04_六維研究摘要','05_五關量價審查','06_強勢股漏網追蹤',
              '07_族群證據分析','08_未入榜候選隔離','09_資料來源品質',
              '10_逐股排除明細','11_交易員重點導航','12_逐股分類與反證'):
        wb.create_sheet(n)
    wb['11_交易員重點導航']['A1']='existing H157'
    return wb

def test_gap_counts_remain_strict_and_radar_names_are_not_recommendations():
    r=build()
    assert r['same_run'] and r['invalid_volume_rows']==1 and r['strict_ohlcv_rows']==1
    assert [x['股票代號'] for x in r['radar']]==['2892','3016','0055']
    assert r['radar'][0]['缺量根數']==1
    assert 'H109' in r['radar'][1]['先補的資料／禁止誤判'] or 'H126' in r['radar'][1]['先補的資料／禁止誤判']
    assert 'ETF' in r['radar'][2]['先補的資料／禁止誤判']
    assert r['h126_trade_authority_changed'] is False
    assert all('禁止' in x['資格'] for x in r['radar'])

def test_no_current_run_identity_cannot_promote_old_waiting():
    d,t,a=mock_input()
    a['run_identity']='MISMATCH'
    r=build_h175_audit(diagnosis=d,tables=t,h154_audit=a,study_day=DAY)
    assert not r['same_run'] and not r['radar']
    assert r['isolated_rows']==3

def test_blocked_h145_keeps_radar_empty():
    d,t,a=mock_input();a['h145_status']='BLOCKED_FINAL_RESULT_COUNT_GAP'
    r=build_h175_audit(diagnosis=d,tables=t,h154_audit=a,study_day=DAY)
    assert not r['radar']

def test_repeated_diagnosis_stock_cannot_choose_favorable_record():
    d,t,a=mock_input();d=pd.concat([d,d.iloc[[1]]],ignore_index=True)
    r=build_h175_audit(diagnosis=d,tables=t,h154_audit=a,study_day=DAY)
    assert '重複' in [x for x in r['radar'] if x['股票代號']=='3016'][0]['先補的資料／禁止誤判']

def test_workbook_14_sheets_and_h126_unchanged_and_names_visible():
    wb=book();original=wb.sheetnames[:]
    report=build();res=append_h175_excel(wb,report)
    assert len(wb.sheetnames)==14 and wb.sheetnames==original
    assert wb['00_H126主管決策']['G2'].value=='禁止買進'
    assert wb['00_主管重點觀察榜']['A1'].value=='original manager'
    assert wb['00_主管重點觀察榜']['A8'].value=='preserve'
    assert '2892' in str(wb['00_主管重點觀察榜'].cell(res['manager_start_row']+3,1).value)
    assert '0055' in str(wb['11_交易員重點導航'].cell(res['navigation_start_row']+6,2).value)
    assert res['radar_rows']==3

def test_no_radar_does_not_fabricate_codes():
    w=book();d,t,a=mock_input();a['run_identity']='UNVERIFIED'
    r=build_h175_audit(diagnosis=d,tables=t,h154_audit=a,study_day=DAY)
    x=append_h175_excel(w,r)
    assert x['radar_rows']==0 and '沒有足以證明同輪' in w['11_交易員重點導航'].cell(x['navigation_start_row']+4,1).value

def test_h114_volume_gap_recorded_without_relaxing_60_bar_contract():
    days=pd.bdate_range(end=DAY,periods=90)
    records=[]
    for i,day in enumerate(days):
        p=100+i*.02
        records.append({'日期':day.date().isoformat(),'開盤價':p,
                        '最高價':p+1,'最低價':p-1,'收盤價':p+.1,
                        '成交量(張)':0 if i==65 else 1000+i})
    s=analyze_history(pd.DataFrame(records),asof=DAY)
    assert s['H114K線資料狀態']=='INVALID_OHLCV_VOLUME'
    assert s['H175最近60根成交量非正數']==1
    assert s['H175最近60根量價完整數']==59
    assert s['H114黃金交叉']=='未驗證'

def test_h114_complete_lots_retain_original_valid_signal():
    days=pd.bdate_range(end=DAY,periods=90)
    f=pd.DataFrame([{'日期':day.date().isoformat(),'開盤價':100+i*.03,
                  '最高價':101+i*.03,'最低價':99+i*.03,'收盤價':100.2+i*.03,
                  '成交量(張)':1000+i} for i,day in enumerate(days)])
    r=analyze_history(f,asof=DAY)
    assert r['H114K線資料狀態']=='VALID_OHLCV'
    assert r['H175最近60根量價完整數']==60
    assert r['H175最近60根成交量非正數']==0

def test_app_wiring_calls_after_h157_and_preserves_h154_validation():
    page=(Path(__file__).resolve().parents[1]/'pages'/'7_股神推薦.py').read_text('utf-8')
    assert page.index('_h154_verify(wb,_h154_audit)') < page.index('_h175_append_excel(wb, _h175_report)')
    assert page.index('_h157_report = _h157_audit(') < page.index('_h175_append_excel(wb, _h175_report)')


def test_new_price_only_setup_does_not_change_vol_gate_or_h126():
    days=pd.bdate_range(end=DAY,periods=90)
    rows=[]
    for i,day in enumerate(days):
        c=100+(i*0.01)
        rows.append({'日期':day.date().isoformat(),'開盤價':c-.2,
                     '最高價':c+.4,'最低價':c-.5,'收盤價':c,
                     '成交量(張)':0 if i==70 else 1500})
    out=analyze_history(pd.DataFrame(rows),asof=DAY)
    assert out['H114K線資料狀態']=='INVALID_OHLCV_VOLUME'
    assert out['H114黃金交叉']=='未驗證'
    assert out['H175價格型態（僅OHLC）'].startswith('初步蓄勢')
    d,t,a=mock_input()
    record={'股票代號':'3227','股票名稱':'原相','H114K線資料狀態':out['H114K線資料狀態'],
            'H114K線證據日':DAY,'H114K線有效根數':out['H114K線有效根數'],
            'H175價格型態（僅OHLC）':out['H175價格型態（僅OHLC）'],
            'H175價格20日報酬%':out['H175價格20日報酬%'],
            'H109決策權威':'RESEARCH_ELIGIBLE','H113當日成交額百萬':400,
            'H113二十日均成交額百萬':350,'最新成交量_張':3200}
    d=pd.concat([d,pd.DataFrame([record])],ignore_index=True)
    report=build_h175_audit(diagnosis=d,tables=t,h154_audit=a,study_day=DAY)
    assert report['price_only_research']==1
    assert report['radar'][0]['股票代號']=='3227'
    assert report['radar'][0]['資格'].startswith('僅價格初篩')
    assert report['h126_trade_authority_changed'] is False

def test_price_only_rejects_veto_stale_and_low_liquidity():
    d,t,a=mock_input()
    b={'股票代號':'3227','股票名稱':'原相','H114K線資料狀態':'INVALID_OHLCV_VOLUME',
       'H114K線證據日':DAY,'H114K線有效根數':100,
       'H175價格型態（僅OHLC）':'初步蓄勢｜僅價型，量能／財報未證',
       'H109決策權威':'FADE_VETO','H113當日成交額百萬':500,
       'H113二十日均成交額百萬':500,'最新成交量_張':1000}
    d=pd.concat([d,pd.DataFrame([b])],ignore_index=True)
    report=build_h175_audit(diagnosis=d,tables=t,h154_audit=a,study_day=DAY)
    assert report['price_only_research']==0
    d.loc[d['股票代號']=='3227','H109決策權威']='RESEARCH_ELIGIBLE'
    d.loc[d['股票代號']=='3227','H114K線證據日']='2026-11-18'
    assert build_h175_audit(diagnosis=d,tables=t,h154_audit=a,study_day=DAY)['price_only_research']==0
    d.loc[d['股票代號']=='3227','H114K線證據日']=DAY
    d.loc[d['股票代號']=='3227','H113當日成交額百萬']=10
    assert build_h175_audit(diagnosis=d,tables=t,h154_audit=a,study_day=DAY)['price_only_research']==0
