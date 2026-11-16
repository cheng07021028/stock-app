# -*- coding: utf-8 -*-
"""H164 evidence priority no-invention and legacy workbook regression."""
from datetime import date
import io
from openpyxl import Workbook,load_workbook
import pytest
from godpick_h164_trader_focus import build_priority_rows,next_workday,append_focus_to_overview
from godpick_h157_trader_audit import render_trader_audit

D='2026-11-13'

def rec(code='2892', name='測試', tier='B｜待補證研究', rank=1, technical='接近箱頂（尚未突破）', market=None, stock=None):
 return {'股票代號':code,'股票名稱':name,'來源股票池':tier,'H153研究排名':rank,'原研究證據分':47,
         '研究資料日':D,'成長價值證據':'官方PE/EPS或逐月營收證據不足｜不可列為已認證高成長',
         '三率×獲利突破':'三率×獲利突破尚未同時認證','跌市抗跌':'同日大盤／個股／比較值未充分核實',
         '技術型態與反證':technical,'大盤漲跌%':market,'個股漲跌%':stock,'RSI14':None,
         '下一交易日觀察情境':'下一交易日：缺足夠方向優勢，觀望補證｜方向未校準',
         'H126正式交易資格':'H145完整性阻擋｜研究表不可下單'}

def ledger(rows):
 return {'rows':rows,'anti_fall':0,'valid_comparison':0,'growth_verified':0,'margin_verified':0}

def test_weekend_and_unknown_dates():
 assert next_workday(D)=='2026-11-16'
 assert next_workday('2026-11-16')=='2026-11-17'
 assert next_workday('invalid')=='未驗證'

def test_no_fundamental_or_next_day_hallucinations():
 result=build_priority_rows(ledger([rec()]))
 row=result['rows'][0]
 assert row['交易員分池']=='P｜預發動待突破'
 assert '官方PE/EPS' in row['關鍵缺口']
 assert '隔日漲跌不能預測' in row['隔日條件情境（非預測機率）']
 assert result['sources_complete'] is False

def test_verified_resilience_requires_same_date_values():
 x=rec(market=-6.03,stock=0.1)
 x['跌市抗跌']='逆勢收紅｜需確認續強'
 assert build_priority_rows(ledger([x]))['rows'][0]['交易員分池'].startswith('R')
 x['個股漲跌%']=None
 assert not build_priority_rows(ledger([x]))['rows'][0]['交易員分池'].startswith('R')

def test_technical_and_growth_sources_stay_separate():
 rows=[rec('1477',technical='箱型突破（需量價及後續站穩）'),rec('2891',technical='尚無確認型態')]
 out=build_priority_rows(ledger(rows))['rows']
 assert out[0]['股票代號']=='1477' and out[0]['交易員分池'].startswith('T')
 assert out[1]['交易員分池'].startswith('U')
 rows[1]['成長價值證據']='已核PE/EPS＋已公告營收；PE≤15、EPS≥5、營收YoY≥15%之初篩'
 rows[1]['三率×獲利突破']='已核三率連兩季同比改善＋毛利／營益突破（非股價突破）'
 out=build_priority_rows(ledger(rows))['rows']
 assert out[0]['股票代號']=='2891' and out[0]['交易員分池']=='G｜三率＋高成長同時認證'

def test_w_and_c_are_not_promoted():
 rows=[rec('2618',tier='W｜非交易核查',technical='箱型突破'),rec('2033',tier='C｜風控隔離',technical='MA黃金交叉')]
 out=build_priority_rows(ledger(rows))['rows']
 assert [r['交易員分池'][0] for r in out]==['W','C']

def test_limit_and_duplicate_preserve_first():
 rows=[rec(str(2000+i),rank=i+1) for i in range(45)]
 rows[1]['股票代號']=rows[0]['股票代號']
 out=build_priority_rows(ledger(rows),limit=99)
 assert out['total']==44
 assert len(out['rows'])==30 and out['omitted']==14

def test_append_without_touching_existing_14_tabs():
 w=Workbook(); ws=w.active;ws.title='11_交易員重點導航';ws['A1']='H157';ws['B5']='BLOCKED'
 for i in range(2,31):ws.cell(i,1,'legacy '+str(i))
 originals=list(w.sheetnames); before=(ws['A1'].value,ws['B5'].value,ws['A30'].value)
 res=append_focus_to_overview(w,ledger([rec()]),research_day=D,h145_status='BLOCKED_FINAL_RESULT_COUNT_GAP')
 assert res['total']==1 and res['shown']==1
 assert w.sheetnames==originals
 assert (ws['A1'].value,ws['B5'].value,ws['A30'].value)==before
 assert ws.cell(res['table_row']+1,1).value=='2892'
 assert ws.cell(res['focus_row'],1).value.startswith('H164')
 b=io.BytesIO();w.save(b);b.seek(0);copy=load_workbook(b,read_only=True)
 assert copy['11_交易員重點導航'].cell(res['table_row']+1,1).value=='2892'

def test_second_append_is_rejected_not_overwriting():
 w=Workbook();w.active.title='11_交易員重點導航';led=ledger([rec()]);append_focus_to_overview(w,led,research_day=D)
 with pytest.raises(ValueError,match='重複'):append_focus_to_overview(w,led,research_day=D)

def test_missing_sheet_fails_closed():
 with pytest.raises(ValueError,match='缺原11'):append_focus_to_overview(Workbook(),ledger([]),research_day=D)

def test_relative_only_fall_is_not_mislabelled_flat():
 item=rec(code='2891', market=-6.03, stock=-2.41)
 item['相對強度pp']=3.62
 item['跌市抗跌']='尚未符合跌市抗跌條件'
 row=build_priority_rows(ledger([item]))['rows'][0]
 assert row['交易員分池']=='R｜相對抗跌但仍下跌（研究）'
 assert '仍下跌' in row['有憑據的訊號']
 item['相對強度pp']=99
 row=build_priority_rows(ledger([item]))['rows'][0]
 assert not row['交易員分池'].startswith('R')
