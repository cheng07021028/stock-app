from datetime import datetime
from types import SimpleNamespace
import io
import pytest
import openpyxl
import godpick_h219_first_review as h
from test_h217_prospective_quality import sig, D,T,NOW

def mk_market(n=100,missing=0,ret=-.02):
    a={str(1000+i):100 for i in range(n)};b={str(1000+i):100*(1+ret) for i in range(n-missing)}
    return SimpleNamespace(errors=[],quotes=lambda m,d:a if d==D else b)

def test_breadth_fixed_start():
    r=h.breadth(mk_market(), 'TWSE',D,T);assert r['市場狀態']=='弱勢廣度' and r['市場原價中位數']==pytest.approx(-.02) and r['下跌比例']==1
@pytest.mark.parametrize('n,missing',[(99,0),(100,6)])
def test_breadth_insufficient(n,missing):
    r=h.breadth(mk_market(n,missing),'TWSE',D,T);assert r['市場原價中位數'] is None and r['市場狀態']=='DATA_PENDING'

def test_breadth_new_stocks_not_added():
    m=mk_market(100,6);old=m.quotes;m.quotes=lambda m,d:old(m,d) if d==D else {**old(m,d),'9999':500};r=h.breadth(m,'TWSE',D,T);assert r['雙日有價']==94

def detail(s,raw=.02,net=.01,due=True):
    return {'訊號來源':s['source'],'研究日':s['signal_day'],'市場':s['market'],'股票代號':s['code'],'研究分級':s.get('tier',''),'期間':1,'出場日':T if due else None,'原始績效狀態':'VERIFIED_RAW' if raw is not None and due else '待核','原始收盤績效':raw if due else None,'淨績效狀態':'VALID_CASH_MODEL' if net is not None and due else '待核','淨報酬':net if due else None}

def test_cohort_complete_and_resilience():
    s=sig('1001');r,b=h.cohorts([detail(s)],mk_market(),[s]);a=next(x for x in r if x['研究群組']=='全批次');assert a['原價平均']==.02 and a['淨報酬平均']==.01 and a['弱市不跌比例']==1 and a['原價相對差']==pytest.approx(.04)

def test_missing_not_survivor_only():
    s=[sig('1001'),sig('1002')];r,_=h.cohorts([detail(s[0]),detail(s[1],raw=None,net=None)],mk_market(),s);a=next(x for x in r if x['研究群組']=='全批次');assert a['原價平均'] is None and a['淨勝率'] is None and a['到期']==2

def test_immature():
    s=sig();r,b=h.cohorts([detail(s,due=False)],mk_market(),[s]);assert not b and all(x['狀態']=='未到期／日曆待核' and x['原價上漲比例'] is None for x in r)

def test_market_and_tier_split():
    a=sig('1001');b=sig('1001',market='TPEX');b['tier']='財報研究';r,_=h.cohorts([detail(a),detail(b)],mk_market(),[a,b]);assert len(r)==4 and {x['市場'] for x in r}=={'TWSE','TPEX'}

def test_h209_phase_is_frozen():
    s=sig('1001',source='H209');s['strategies']=['H198｜蓄勢待突破'];r,_=h.cohorts([detail(s,raw=None)],mk_market(),[s]);assert any(x['研究群組']=='H198已封存初期' for x in r)

def row(c='1001',rank=1):
    return {'市場':'TWSE','股票代號':c,'股票名稱':'=1+1','研究分級':'技術研究','技術日':D,'技術研究池':'突破','H198資料日期':D,'H198階段':'蓄勢待突破','H211狀態':'歷史待補證','H211反證':'未觸發本規則（非無風險）','H211研究序':rank,'H211技術證據':{'流動性通過':True,'收盤':100,'MA20':99,'MA20前5日':98,'5日漲跌%':1,'20日漲跌%':2,'量比':1.2}}

def body(rows):return {'expected_session':D,'market_day':D,'technical_current':True,'same_session':True,'trade_authority_changed':False,'rows':rows}

def test_manager_limit_sort_and_safe():
    rows=[row(str(1000+i),9-i) for i in range(8)];r,n=h.manager_rows(body(rows),D);assert len(r)==5 and r[0]['股票代號']=='1007' and all(x['用途']=='研究複核／未授權' for x in r)
    w=openpyxl.Workbook();h.table(w,'H219',r);assert w['H219']['C2'].data_type=='s'
@pytest.mark.parametrize('field,value',[('H211反證','追高'),('H211狀態','資料待補證'),('技術日',T),('研究分級','財報研究'),('H198階段','突破反證與追高警示')])
def test_manager_excludes(field,value):
    r=row();r[field]=value;assert h.manager_rows(body([r]),D)[0]==[]

def test_manager_stale_duplicate():
    assert not h.manager_rows(body([row()]),T)[0]
    with pytest.raises(ValueError):h.manager_rows(body([row(),row()]),D)

def test_recall_gap_integer_threshold():
    r=h.recall_gaps({'coverage':{'TWSE':{'主檔':1083,'有效60日':1021},'TPEX':{'主檔':883,'有效60日':675}}});assert r[0]['距門檻有效股數']==8 and r[1]['距門檻有效股數']==164

def test_empty_and_workbook(tmp_path):
    r=h.run(tmp_path,NOW);assert r['counts']['首批訊號']==0 and r['status']=='DATA_PENDING' and r['weight_multiplier']==1.0;w=openpyxl.load_workbook(io.BytesIO(h.excel(r)));assert len(w.sheetnames)==9 and not list(tmp_path.iterdir())
