from datetime import timedelta
import io
import pytest
from openpyxl import load_workbook
import godpick_h238_first_attribution as h
from test_h217_prospective_quality import sig,D,T,NOW,market
from test_h229_net_edge import Evidence,reconcile
import godpick_h229_net_edge as net


def fixture():
    m=market([D,T]);m.errors=[];m.today_confirmed=lambda d:True
    a={str(i):(100,30_000_000) for i in range(1000,1100)};z=dict(a);z['1000']=(105,30_000_000);z['1001']=(104,30_000_000);z['1002']=(103.1,30_000_000)
    master={c:{'市場':'TWSE','股票名稱':'測試'+c,'產業':'測試業'} for c in a}
    m.quotes=lambda market,d:{c:v[0] for c,v in (a if d==D else z).items()} if market=='TWSE' else {}
    reader=lambda root,market,d,now:((a if d==D else z) if market=='TWSE' else {},'')
    ss=[sig('1000'),{**sig('1001'),'tier':h.p.TIERS[-1]},sig('1003')]
    return m,master,reader,ss,a,z


def test_fixed_first_not_latest():
    m,master,reader,ss,*_=fixture();later=(NOW+timedelta(days=2));m.days=[D,T,later.date().isoformat()]
    b=h.first_board('.',m,master,D,later,reader);assert b['day']==T and b['previous_day']==D
    assert len(b['rows'])==3


def test_unconfirmed_target():
    m,master,reader,*_=fixture();m.today_confirmed=lambda d:False
    b=h.first_board('.',m,master,D,NOW,reader);assert not b['day'] and not b['rows']


def test_three_way_membership():
    m,master,reader,ss,*_=fixture();b=h.first_board('.',m,master,D,NOW,reader)
    a={('TWSE','1002'):{'tier':'未入池','tech_gap':'缺5日'}}
    _,rows,s=h.diagnose(b,ss,m,master,a,'FROZEN_DECISION_AUDIT',NOW)
    assert [r['對帳'] for r in rows]==['前三研究命中','觀察級命中','未入研究封存']
    assert rows[-1]['處理']=='H212行情補證' and s[0]['未入研究封存']==1

@pytest.mark.parametrize('mode',['absent','base','future','coverage','jump'])
def test_unknown_not_miss(mode):
    m,master,reader,ss,a,z=fixture()
    if mode=='absent':ss=[]
    elif mode=='base':ss[0]['base_close']=99
    elif mode=='future':ss[0]['created_at']=T+'T20:00:00+08:00'
    elif mode=='coverage':
        for c in range(1090,1100):z.pop(str(c))
    elif mode=='jump':
        for c in range(1090,1100):z[str(c)]=(130,30_000_000)
    b=h.first_board('.',m,master,D,NOW,reader);_,rows,s=h.diagnose(b,ss,m,master,{},'NO',NOW)
    assert all(r['對帳']=='待核' for r in rows) and s[0]['未入研究封存'] is None

@pytest.mark.parametrize('audit,expected', [({},'無當時逐股證據'),({'tier':'技術研究'},'當時入池但無封存'),({'tier':'未入池','liquid':False},'當時流動性未通過'),({'tier':'未入池','financial_gap':'缺季'},'當時財報缺口')])
def test_audit_not_causal(audit,expected):
    m,master,reader,ss,*_=fixture();b=h.first_board('.',m,master,D,NOW,reader);_,rows,_=h.diagnose(b,ss,m,master,{('TWSE','1002'):audit},'FROZEN_DECISION_AUDIT',NOW)
    assert rows[-1]['原因線索']==expected and '非因果' in rows[-1]['口徑']


def test_attribution_complete():
    e=Evidence();ss=[sig('1000'),sig('1001')];e.set('1000',104,103);e.set('1001',100,100.1)
    rr=[net.attribution(s,reconcile(s,e),[D,T],e,NOW) for s in ss]
    r=h.attribution_summary(ss,rr,T)[0];assert r['淨績效有效']==2 and r['淨勝率']==0 and r['跳空未延續']==1 and r['成本轉非正']==1
    r=h.attribution_summary(ss,rr[:1],T)[0];assert r['原價平均'] is None and r['淨勝率'] is None


def test_observation_not_net():
    e=Evidence();s={**sig('1000'),'tier':h.p.TIERS[-1]};r=net.attribution(s,reconcile(s,e),[D,T],e,NOW)
    a=h.attribution_summary([s],[r],T)[0];assert a['淨績效應結算']==0 and a['淨勝率'] is None and a['原價有效']==1


def test_attribution_mismatch():
    e=Evidence();s=sig('1000');r=net.attribution(s,reconcile(s,e),[D,T],e,NOW)
    with pytest.raises(ValueError):h.attribution_summary([s],[r,r],T)
    assert h.attribution_summary([s],[r],None)[0]['原價平均'] is None


def test_growth_is_subset():
    base={'市場':'TWSE','股票代號':'1000','股票名稱':'測試','資料日':D,'官方收盤':100,'研究分層':'高成長起漲研究'}
    rr=[base,{**base,'股票代號':'1001','研究分層':'技術優先複核'},{**base,'股票代號':'1002','反證／衝突':'衝突'},{**base,'股票代號':'1003','資料日':T}]
    assert [r['股票代號'] for r in h.growth_subset(rr,D)]==['1000']


def test_empty_and_excel(tmp_path):
    r=h.run(tmp_path,NOW);assert r['status']=='DATA_PENDING' and r['target'] is None and r['weight_multiplier']==1 and not r['trade_authority_changed'];assert not list(tmp_path.iterdir())
    w=load_workbook(io.BytesIO(h.excel(r)));assert len(w.sheetnames)==10
    r['growth']=[{'市場':'TWSE','股票代號':'1000','股票名稱':'測試'}];w=load_workbook(io.BytesIO(h.excel(r)));assert w['08_高成長優先複核']['B1'].value=='股票名稱'

@pytest.mark.parametrize('mode',['valid','late_base','late_target','early_settle','missing'])
def test_raw_original_observation(mode):
    e=Evidence();s=sig('1000');r=net.attribution(s,reconcile(s,e),[D,T],e,NOW)
    o={'target_day':T,'target_close':101,'raw_return':.01,'resolved_at':T+'T20:00:00+08:00'}
    if mode=='late_base':e.data[D,'1000']=(e.data[D,'1000'][0],h.p.stamp(D+'T21:00:00+08:00'))
    if mode=='late_target':e.data[T,'1000']=(e.data[T,'1000'][0],h.p.stamp(T+'T21:00:00+08:00'))
    if mode=='early_settle':o['resolved_at']=T+'T15:00:00+08:00';e.data[T,'1000']=(e.data[T,'1000'][0],h.p.stamp(T+'T14:00:00+08:00'))
    v=h.recheck_raw([s],[r],{(*h.p.sid(s),1):o} if mode!='missing' else {},e,T,NOW)[0]
    assert (v['原價時點重驗']=='VERIFIED')==(mode=='valid')
    assert (v['原價收盤報酬'] is not None)==(mode=='valid')
    assert v['淨報酬']==r['淨報酬']
