from datetime import datetime,timedelta
import io
import pytest
import openpyxl
import godpick_h218_evidence as h
from test_h217_prospective_quality import sig, market, D, T, NOW
from test_h209_rolling_backtest import source

def fixture_market(days=(D,T)):
    m=market(list(days));m.errors=[];m.quotes=lambda market,day:{'2330':100 if day==D else 105};return m

def test_before_cutoff():
    now=NOW.replace(hour=10);m=fixture_market();assert h.due_span(m,D,1,now)==[]
    rr,ss=h.reconcile([sig()],{},m,None,now)
    assert len(rr)==5 and not any(r['出場日'] for r in rr) and all(r['淨勝率'] is None for r in ss)
    assert h.p.settle({h.p.sid(sig()):sig()},{},m,now)[0]==[]

def test_raw_net_distinct(monkeypatch):
    s=sig();m=fixture_market();o={'target_day':T,'target_close':105,'raw_return':.05,'resolved_at':NOW.isoformat()}
    monkeypatch.setattr(h.b,'trade',lambda *a:{'status':'除權息涵蓋待補證'})
    r,ss=h.reconcile([s],{(*h.p.sid(s),1):o},m,None,NOW)
    assert r[0]['原始績效狀態']=='VERIFIED_RAW' and r[0]['原始收盤績效']==.05 and r[0]['淨報酬'] is None
    assert ss[0]['原價有效']==1 and ss[0]['淨勝率'] is None

def test_conflicting_outcome(monkeypatch):
    s=sig();m=fixture_market();o={'target_day':T,'target_close':106,'raw_return':.06,'resolved_at':NOW.isoformat()}
    monkeypatch.setattr(h.b,'trade',lambda *a:{'status':'行情缺口'})
    rr,_=h.reconcile([s],{(*h.p.sid(s),1):o},m,None,NOW);assert rr[0]['原始績效狀態']=='結算／官方基準衝突'

def test_observation_tier_excluded(monkeypatch):
    s=sig();s['tier']=h.p.TIERS[-1];m=fixture_market();monkeypatch.setattr(h.b,'trade',lambda *a:pytest.fail('observation tier not eligible'))
    rr,ss=h.reconcile([s],{},m,None,NOW);assert rr[0]['淨績效狀態']=='觀察級不納入淨績效' and ss[0]['淨績效應結算']==0
    assert h.scope(m,[s],NOW)[1]==set()

@pytest.mark.parametrize('mode',['valid','future','missing','nan'])
def test_maturity_coverage(monkeypatch,mode):
    m=fixture_market();ss=[sig(),sig('2331')]
    def trade(s,*args):
        if mode=='missing' and s['code']=='2331':return {'status':'行情缺口'}
        return {'status':'VALID_CASH_MODEL','net_return':float('nan') if mode=='nan' else .02,'resolved_at':(NOW+timedelta(days=1) if mode=='future' else NOW).isoformat()}
    monkeypatch.setattr(h.b,'trade',trade);_,r=h.reconcile(ss,{},m,None,NOW);assert r[0]['淨績效有效']=={'valid':2,'missing':1,'future':0,'nan':0}[mode];assert r[0]['淨勝率']==(1.0 if mode=='valid' else None)

def test_independent_action_revalidation(tmp_path):
    ds=[(NOW.date()-timedelta(days=i)).isoformat() for i in range(59,-1,-1)];m=fixture_market(ds)
    v=h.b.parse_actions(source(T),'TWSE',T,NOW);p=h.b.action_path(tmp_path,'TWSE',T);h.ar._write(p,h.ar._seal(v))
    q=h.b.action_path(tmp_path,'TPEX',T);q.parent.mkdir(parents=True);q.write_bytes(b'invalid')
    r,ss=h.audit_actions(tmp_path,m,[],NOW);assert len(r)==118
    a=ss[0];assert a['明確零事件日']==1 and a['缺檔']==58 and a['覆蓋率']==pytest.approx(1/59)
    assert ss[3]['驗證異常']==1 and ss[3]['明確零事件日']==0

def pairrow(code,src,value):
    return {'市場':'TWSE','期間':1,'研究日':D,'出場日':T,'訊號來源':src,'股票代號':code,'研究分級':'技術研究','淨績效狀態':'VALID_CASH_MODEL','淨報酬':value,'結果觀測時間':NOW.isoformat(),'決策時間':D+'T20:00:00+08:00'}

def test_exclusive_comparison():
    a=[pairrow(str(c),'H194',.01) for c in range(1000,1004)]+[pairrow(str(c),'H209',.03) for c in range(1003,1007)]
    r=h.source_pairs(a)[0];assert (r['H194獨有'],r['H209獨有'],r['共同股票'])==(3,3,1) and r['H209相對差']==pytest.approx(.02)
    a[-1]['淨績效狀態']='行情缺口';assert h.source_pairs(a)[0]['H209相對差'] is None
    assert h.source_pairs(a[:4])[0]['狀態']=='DATA_PENDING'
    with pytest.raises(ValueError):h.source_pairs(a+[a[0]])

@pytest.mark.parametrize('mode,want',[('good','REVIEW_H209_EDGE'),('negative','REVIEW_H194_EDGE'),('late','BASELINE_ONLY'),('missing','BASELINE_ONLY'),('flat','NO_CONFIRMED_EDGE')])
def test_oos(mode,want):
    a=[]
    for i in range(30):
        d=(NOW.date()-timedelta(days=(31-i)*3));end=d+timedelta(days=1)
        a.append({'市場':'TWSE','期間':1,'研究日':d.isoformat(),'出場日':end.isoformat(),'決策時間':d.isoformat()+'T20:00:00+08:00','結果觀測時間':NOW.isoformat() if mode=='late' and i==19 else end.isoformat()+'T20:00:00+08:00','狀態':'DATA_PENDING' if mode=='missing' and i==25 else 'DESCRIPTIVE_ONLY','H209相對差':-.02 if mode=='negative' else 0 if mode=='flat' else .02})
    r=h.review(a)[0];assert r['狀態']==want and r['權重倍率']==1

def test_empty_and_excel(tmp_path):
    r=h.run(tmp_path,NOW);assert r['status']=='DATA_PENDING' and r['weight_multiplier']==1 and not list(tmp_path.iterdir())
    r['detail']=[{'股票名稱':'=1+2','淨報酬':.1}];w=openpyxl.load_workbook(io.BytesIO(h.excel(r)));assert len(w.sheetnames)==8;assert w['05_成熟逐筆']['A2'].data_type=='s' and w['05_成熟逐筆']['B2'].number_format=='0.00%'
