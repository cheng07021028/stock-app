from datetime import datetime,timedelta
from types import SimpleNamespace
import io
import pytest
import openpyxl
import godpick_h217_prospective_quality as h
import godpick_h194_performance as p
import godpick_h190_market_archive as ar
from test_h194_performance import setup_market,result
NOW=datetime.fromisoformat('2026-11-25T23:25:00+08:00')
D='2026-11-24'; T='2026-11-25'
def sig(code='2330',market='TWSE',source='H194',day=D,base=100):
    return {'source':source,'signal_day':day,'market':market,'code':code,'name':'test','tier':'技術研究','base_close':base,'created_at':day+'T20:00:00+08:00','strategies':['a','b']}
def board(cov=99):
    return {'day':T,'previous_day':D,'errors':[],'coverage':{m:{'雙日覆蓋率%':cov,'主檔一般股':100,'雙日有價':cov} for m in ar.URLS},'rows':[{'市場':'TWSE','股票代號':'2330','股票名稱':'A','前收盤':100},{'市場':'TWSE','股票代號':'2331','股票名稱':'B','前收盤':100}]}
def test_missing_is_not_zero():
    r,d=h.compare_leaders(board(),{}, {},'NO_PARTITION',NOW)
    assert r[0]['研究召回率'] is None and r[0]['待核']==2 and r[0]['未入榜']==0
@pytest.mark.parametrize('cov,expected',[(99,.5),(94,None)])
def test_market_coverage_and_source(cov,expected):
    r,d=h.compare_leaders(board(cov),h.source_index([sig()]),{},'LEGACY_NO_AUDIT',NOW)
    assert r[0]['研究召回率']==expected and r[1]['研究召回率'] is None
    assert d[1]['當時原因證據']=='缺當時逐股原因／待核'
def test_original_cause():
    a={('TWSE','2331'):{'tier':'未入池','tech_gap':'缺日'}}
    _,d=h.compare_leaders(board(),h.source_index([sig()]),a,'FROZEN_DECISION_AUDIT',NOW)
    assert '缺日' in d[1]['當時原因證據'] and d[1]['正式買進漏選']=='未驗證H126'
def test_other_market_not_match():
    r,_=h.compare_leaders(board(),h.source_index([sig(market='TPEX')]),{},'',NOW)
    assert r[0]['待核']==2
@pytest.mark.parametrize('base,created',[(101,D+'T20:00:00+08:00'),(100,T+'T20:00:00+08:00')])
def test_conflict(base,created):
    s=sig(base=base);s['created_at']=created
    r,_=h.compare_leaders(board(),h.source_index([s]),{},'',NOW)
    assert r[0]['研究召回率'] is None and r[0]['待核']==1

def test_duplicate():
    with pytest.raises(ValueError):h.source_index([sig(),sig()])

def market(days):
    class M:
        def span(self,d,n):
            if d not in days:return []
            ds=days[days.index(d):days.index(d)+n+1];return ds if len(ds)==n+1 else []
    m=M();m.days=days;return m

def test_freshness():
    days=[(NOW.date()-timedelta(days=x)).isoformat() for x in range(11,0,-1)];d=days[-1];m=market(days)
    s=sig(day=d); prior=[sig(code='9999',day=x) for x in days[:-1]]
    assert h.freshness(s,h.source_index(prior),m)=='近窗封存未見'
    assert h.freshness(s,h.source_index(prior[:-1]),m)=='歷史待補'
    assert h.freshness(s,h.source_index([sig(day=days[-2])]),m)=='近窗曾入研究'

def test_maturity_unique_source_no_strategy_double_count(monkeypatch):
    m=market([D,T]);ss=[sig(),sig(source='H209')]
    monkeypatch.setattr(h.back,'trade',lambda *a:{'status':'VALID_CASH_MODEL','net_return':.03})
    r,s,pairs,pending=h.outcomes(ss,h.source_index(ss),m,None,NOW)
    assert len(r)==2 and sum(x['有效'] for x in s)==2 and pending['未成熟或日曆不足']==8
    assert all(x['狀態']=='DATA_PENDING' for x in pairs)

def test_no_survivor_win(monkeypatch):
    ss=[sig(),sig('2331')];m=market([D,T]);monkeypatch.setattr(h.back,'trade',lambda s,*a:{'status':'VALID_CASH_MODEL','net_return':.1} if s['code']=='2330' else {'status':'除權息涵蓋待補證'})
    r,s,_,_=h.outcomes(ss,h.source_index(ss),m,None,NOW)
    assert s[0]['覆蓋率']==.5 and s[0]['勝率'] is None and s[0]['平均淨報酬'] is None

def test_pair_comparison(monkeypatch):
    ds=[(NOW.date()-timedelta(days=x)).isoformat() for x in range(11,-1,-1)];d=ds[-2];m=market(ds)
    prior=[sig(code=str(c),day=x) for x in ds[:-2] for c in (2330,2331,2332)]
    today=[sig(code=str(c),day=d) for c in range(2330,2336)]
    monkeypatch.setattr(h.back,'trade',lambda s,*a:{'status':'VALID_CASH_MODEL','net_return':.02 if int(s['code'])<2333 else .04})
    _,_,pairs,_=h.outcomes(today,h.source_index(prior+today),m,None,NOW)
    assert pairs[0]['未見組相對差']==pytest.approx(.02) and pairs[0]['狀態']=='DESCRIPTIVE_ONLY'

def test_immutable_and_future(tmp_path):
    b={'version':h.VERSION,'day':T,'asof':NOW.isoformat(),'previous_day':D,'summary':[],'detail':[],'trade_authority_changed':False}
    assert h.freeze(tmp_path,b);assert not h.freeze(tmp_path,{**b,'summary':[{}]})
    assert h.read_daily(tmp_path,NOW)==[b] and h.read_daily(tmp_path,NOW-timedelta(hours=1))==[]

def test_new_h194_audit_and_legacy(tmp_path):
    setup_market(tmp_path);now=datetime.fromisoformat('2026-11-23T23:45:00+08:00');m=p.Market(tmp_path,now);r=result();r['audit']=[{'市場':'TWSE','股票代號':'2331','研究分級':'未入池','技術缺口':'缺日'}]
    model=p.models({}, {},now);b,_=p.freeze(r,m,model,now);assert b and b['h217_audit']['rows'][0]['tech_gap']=='缺日'
    day=now.date().isoformat();path=tmp_path/'data/h194/signals'/(day+'.json.gz');p.immutable(path,b)
    a,state=h.prior_audit(tmp_path,day,now);assert a[('TWSE','2331')]['tech_gap']=='缺日' and state=='FROZEN_DECISION_AUDIT'
    assert h.prior_audit(tmp_path,day,now-timedelta(hours=1))[0]=={}
    path.unlink();b.pop('h217_audit');p.immutable(path,b);assert h.prior_audit(tmp_path,day,now)[1]=='LEGACY_NO_AUDIT'

def test_actual_readonly_no_data(tmp_path):
    r=h.run(tmp_path,NOW);assert r['status']=='DATA_PENDING' and r['weight_multiplier']==1 and r['counts']['valid_net_rows']==0
    assert not (tmp_path/h.REL).exists();w=openpyxl.load_workbook(io.BytesIO(h.excel(r)));assert len(w.sheetnames)==10

def test_excel_formula_safe(tmp_path):
    r=h.run(tmp_path,NOW);r['miss']=[{'股票名稱':'=1+2','研究召回率':.5}];w=openpyxl.load_workbook(io.BytesIO(h.excel(r)));s=w['03_強勢漏選證據'];assert s['A2'].data_type!='f' and s['B2'].number_format=='0.00%'


@pytest.mark.parametrize('mode,expected',[('good','REVIEW_NEW_PRIORITY'),('negative','REVIEW_REPEAT_PRIORITY'),('late','BASELINE_ONLY'),('missing','BASELINE_ONLY'),('flat','NO_CONFIRMED_EDGE')])
def test_time_split(mode,expected):
    a=[]
    for i in range(30):
        d=(NOW.date()-timedelta(days=(31-i)*3));end=d+timedelta(days=1);obs=(end.isoformat()+'T20:00:00+08:00')
        a.append({'訊號來源':'H194','市場':'TWSE','期間':1,'研究日':d.isoformat(),'出場日':end.isoformat(),'決策時間':d.isoformat()+'T20:00:00+08:00','結果觀測時間':NOW.isoformat() if mode=='late' and i==19 else obs,'狀態':'DATA_PENDING' if mode=='missing' and i==25 else 'DESCRIPTIVE_ONLY','未見組相對差':-.02 if mode=='negative' else 0 if mode=='flat' else .02})
    r=h.review_windows(a)[0];assert r['狀態']==expected and r['權重倍率']==1.0


# Shape-only fixtures from live GitHub H216 request_log; rows are synthetic.
from test_h209_rolling_backtest import source,event
@pytest.mark.parametrize('mode',['twse','tpex','tpex_zero','twse_conflict','tpex_conflict','twse_incomplete','tpex_reverse'])
def test_live_metadata_shapes(mode):
    lo,hi='2026-11-19','2026-11-25'
    now=NOW
    t=source(hi,[event(hi)])
    if mode.startswith('twse'):
        t.update(strDate='20261119',endDate='20261125',title='115年11月19日 至 115年11月25日 除權除息計算結果表')
        if mode=='twse_conflict':t['startDate']='20261120'
        if mode=='twse_incomplete':t.pop('strDate')
        raw=t;m='TWSE'
    else:
        t['title']='';t['totalCount']=len(t['data']);raw={'date':'20261119~20261125','stat':'ok','tables':[t]};m='TPEX'
        if mode=='tpex_zero':t['data']=[];t['totalCount']=0;t.pop('total',None)
        if mode=='tpex_conflict':raw['date']='20261120~20261125'
        if mode=='tpex_reverse':raw['date']='20261125~20261119'
    if 'conflict' in mode or 'incomplete' in mode or 'reverse' in mode:
        with pytest.raises(ValueError):h.back.parse_actions(raw,m,hi,now,start=lo,end=hi)
    else:
        v=h.back.parse_actions(raw,m,hi,now,start=lo,end=hi);assert len(v['rows'])==(0 if mode=='tpex_zero' else 1)

def test_no_data_status_not_empty_evidence():
    with pytest.raises(ValueError):h.back.parse_actions({'stat':'很抱歉，沒有符合條件的資料!'},'TWSE',T,NOW)


def test_no_rounded_coverage_pass():
    b=board();b['coverage']['TWSE'].update({'主檔一般股':20001,'雙日有價':19000,'雙日覆蓋率%':95.0})
    r,_=h.compare_leaders(b,h.source_index([sig()]),{},'',NOW);assert r[0]['研究召回率'] is None and r[0]['雙日覆蓋率']<.95


def test_daily_capture_pending_and_no_backdate(tmp_path,monkeypatch):
    now=datetime.fromisoformat('2026-11-23T23:45:00+08:00');setup_market(tmp_path)
    b=board();b.update(day='2026-11-23',previous_day='2026-11-20')
    monkeypatch.setattr(h.gap,'build',lambda *a,**k:b)
    r=h.run(tmp_path,now,capture=True);assert r['capture']=='CAPTURED' and r['counts']['frozen_leader_days']==1
    assert all(x['研究召回率'] is None for x in r['history'])
    path=tmp_path/h.REL/'2026-11-23.json.gz';raw=path.read_bytes()
    r=h.run(tmp_path,now+timedelta(minutes=1),capture=True);assert r['capture']=='ALREADY_CAPTURED' and path.read_bytes()==raw
    r=h.run(tmp_path,now+timedelta(days=1),capture=True);assert r['capture']=='WAITING_SAME_SESSION' and len(list((tmp_path/h.REL).glob('*.gz')))==1


def test_invalid_existing_distinct_from_missing(tmp_path):
    from test_h209_rolling_backtest import NOW as BNOW,DAYS
    f=h.back.action_path(tmp_path,'TWSE',DAYS[0]);f.parent.mkdir(parents=True);f.write_bytes(b'corrupt')
    r=h.back.collect_actions(tmp_path,SimpleNamespace(days=[]),BNOW,[('TWSE',DAYS[0]),('TPEX',DAYS[0])],budget=0)
    assert r['pending']==1 and r['invalid_existing_count']==1 and r['unverified_total']==2 and f.read_bytes()==b'corrupt'
