from datetime import datetime,timedelta,date
import copy
from pathlib import Path
import pytest
import godpick_h211_counterevidence as h
import godpick_h195_command_center as command
from test_h195_command_center import candidate,joint,write,NOW
from openpyxl import Workbook
DAY='2026-11-23'
DAYS=['2026-11-09','2026-11-10','2026-11-11','2026-11-12','2026-11-13','2026-11-16','2026-11-17','2026-11-18','2026-11-19','2026-11-20']

def row(day=DAY,code='1000',market='TWSE'):
    r=candidate(code,command.TIERS[1],market);r.update({'技術日':day,'H198資料日期':day,'H198階段':h.stage.POOLS[1],'H198突破基準日':'2026-11-19','H198反證':'未觸發','H200研究優先序':1,'H211技術證據':{'收盤':100.,'MA20':97.,'MA20前5日':95.,'5日漲跌%':3.,'20日漲跌%':8.,'60日區間位置%':65.,'量比':1.5,'全市場技術覆蓋':True,'流動性通過':True,'產業覆蓋率%':95.,'產業有效檔數':10,'產業成交額活躍比':1.2,'產業上漲家數比%':70.,'產業主流成立':True},'產業':'半導體','營收年月':'202609','營收年增%':15,'財報缺口':'','財報研究池':'營收成長'});return r

def hist(r=None):
    a={d:{} for d in DAYS}
    if r is not None:a[DAYS[-1]][h.identity(r)]=r
    return a

def ev(r,history=None,legacy=None):return h.evaluate(r,DAY,history if history is not None else hist(),DAYS,legacy or {},NOW)

def test_repeat_without_evidence():
    x=ev(row(),hist(row(DAYS[-1])));assert x['H211狀態']=='重複無新增證據' and x['H211近10日已見次數']==1

def test_new_pool():
    r=row();r['技術研究池']+='、低位階蓄勢';x=ev(r,hist(row(DAYS[-1])));assert x['H211狀態']=='有新增證據' and '低位階蓄勢' in x['H211新證據']

def test_new_anchor():
    r=row();r['H198突破基準日']=DAY;assert '新突破基準日' in ev(r,hist(row(DAYS[-1])))['H211新證據']

def test_retest():
    r=row();r['H198階段']=h.stage.POOLS[2];assert '回測守穩' in ev(r,hist(row(DAYS[-1])))['H211新證據']

def test_revenue_only_with_positive_and_evidence():
    r=row();r['營收年月']='202610';r['H211營收證據']={'新鮮':True,'可得時間':NOW.isoformat(),'來源':'official'};assert '新營收期間' in ev(r,hist(row(DAYS[-1])))['H211新證據'];r['H211營收證據']['可得時間']=(NOW+timedelta(seconds=1)).isoformat();assert ev(r,hist(row(DAYS[-1])))['H211狀態']=='重複無新增證據'

@pytest.mark.parametrize('change',[{'5日漲跌%':16.},{'20日漲跌%':31.},{'收盤':120.},{'收盤':90.,'MA20前5日':100.,'5日漲跌%':-1.},{'60日區間位置%':95.,'20日漲跌%':25.,'5日漲跌%':0.}])
def test_technical_risk(change):
    r=row();r['H211技術證據'].update(change);assert ev(r)['H211狀態']=='反證排除前段'

def test_h198_overrides_new():
    r=row();r['H198階段']=h.stage.POOLS[3];r['技術研究池']+='、低位階蓄勢';assert ev(r,hist(row(DAYS[-1])))['H211狀態']=='反證排除前段'

def test_industry_fading():
    p=row(DAYS[-1]);p['技術研究池']='主流強勢';r=row();r['H211技術證據'].update({'產業主流成立':False,'產業成交額活躍比':.8,'產業上漲家數比%':40.});assert '同步衰退' in ev(r,hist(p))['H211反證'];r['H211技術證據']['產業覆蓋率%']=50.;assert '同步衰退' not in ev(r,hist(p))['H211反證']

def test_no_false_novelty():
    assert ev(row(),{})['H211狀態']=='歷史待補證';assert ev(row(),hist())['H211狀態']=='近窗新出現';assert ev(row(),hist(),{(DAYS[-1],'TWSE','1000'):1})['H211狀態']=='歷史待補證'

def test_market_identity():assert ev(row(),hist(row(DAYS[-1],market='TPEX')))['H211狀態']=='近窗新出現'

def test_missing_current_not_safe():
    r=row();r['技術日']=DAYS[-1];assert ev(r)['H211狀態']=='資料待補證'

def test_archive_idempotency_and_no_future(tmp_path):
    b={'expected_session':DAY,'asof':NOW.isoformat(),'rows':[row()],'h211':{'status':'RESEARCH_READY'}};assert h.freeze(b,tmp_path);p=tmp_path/h.REL/(DAY+'.json.gz');orig=p.read_bytes();b['rows'][0]['H198反證']='new';assert not h.freeze(b,tmp_path);assert p.read_bytes()==orig
    assert h.read_history(tmp_path,[DAY],NOW)[0][DAY][('TWSE','1000')]['H198反證']=='未觸發';assert h.read_history(tmp_path,[DAY],NOW-timedelta(seconds=1))[1];p.write_bytes(b'bad');assert h.read_history(tmp_path,[DAY],NOW)[1]

def test_no_pending_freeze(tmp_path):assert not h.freeze({'expected_session':DAY,'h211':{'status':'DATA_PENDING'}},tmp_path)

def test_enrich_order_original_preserved(tmp_path):
    rs=[row(),row(code='1001')];rs[0]['H198階段']=h.stage.POOLS[3];rs[1]['H200研究優先序']=2
    b={'expected_session':DAY,'rows':rs,'health':{},'technical_current':True};before=[r['H200研究優先序'] for r in rs];h.enrich(b,tmp_path,None,{},NOW);assert [r['H200研究優先序'] for r in rs]==before;assert h.ordered(rs)[0]['股票代號']=='1001';assert b['h211']['trade_authority_changed'] is False

def test_excel_existing_authority_and_name(tmp_path):
    b=command.build(joint([row()]),{},tmp_path,NOW,0);write(tmp_path,b);w=Workbook();w.active.title='00_H126主管決策';w.active['A1']='禁止買進';ws=command.append_manager(w,tmp_path,DAY,NOW);assert w.active['A1'].value=='禁止買進';assert 'H211狀態' in [c.value for c in ws[7]];assert ws['E8'].data_type!='f';assert ws['Y8'].value=='僅研究／未授權'

def test_archive_rejects_backdated(tmp_path):
    b={'expected_session':DAY,'asof':(NOW+timedelta(days=1)).isoformat(),'rows':[row()],'h211':{'status':'RESEARCH_READY'}};assert not h.freeze(b,tmp_path)

def test_no_fading_from_partial_market():
    p=row(DAYS[-1]);p['技術研究池']='主流強勢';r=row();r['H211技術證據'].update({'全市場技術覆蓋':False,'產業主流成立':False,'產業成交額活躍比':.8,'產業上漲家數比%':40.});assert '同步衰退' not in ev(r,hist(p))['H211反證']

def test_complete_enrich_and_freeze_gate(tmp_path):
    class Market:
        days=DAYS+[DAY];errors=[]
        def today_confirmed(self,d):return d==DAY
    b={'expected_session':DAY,'market_day':DAY,'asof':NOW.isoformat(),'technical_current':True,'rows':[row()],'health':{'technical_status':'CROSS_MARKET_RESEARCH_READY','technical_coverage':{'TWSE':{'ratio':.95},'TPEX':{'ratio':.94}}}}
    assert not h.enrich(b,tmp_path,Market(),{},NOW);assert not h.freeze(b,tmp_path)
    b['health']['technical_coverage']['TPEX']['ratio']=.95;assert h.enrich(b,tmp_path,Market(),{},NOW);assert h.freeze(b,tmp_path)
