from datetime import timedelta
import io
import pytest
from openpyxl import load_workbook
import godpick_h229_net_edge as h
import godpick_h209_rolling_backtest as b
from test_h217_prospective_quality import sig,D,T,NOW

class Evidence:
    def __init__(self,n=120):
        self.master={str(i):{'市場':'TWSE'} for i in range(1000,1000+n)};self.data={};self.actions=True
        for c in self.master:
            self.data[D,c]=([c,100,102,98,100,300000,30000000],h.p.stamp(D+'T19:00:00+08:00'))
            self.data[T,c]=([c,100,106,95,101,300000,30000000],h.p.stamp(T+'T19:00:00+08:00'))
    def quote(self,m,d,c):return self.data.get((d,c),(None,None))
    def action(self,m,d,c):return None,h.p.stamp(T+'T20:00:00+08:00') if self.actions else None
    def set(self,c,o,z):self.data[T,c][0][1:5]=[o,max(o,z)+1,min(o,z)-1,z]

def reconcile(s,e,raw=True):
    t=b.trade(s,[D,T],e)
    return {'出場日':T,'原始績效狀態':'VERIFIED_RAW' if raw else '成熟待結算','原始收盤績效':e.data[T,s['code']][0][4]/100-1 if raw else None,'淨績效狀態':t['status'],'淨報酬':t.get('net_return'),'結果觀測時間':t.get('resolved_at')}

@pytest.mark.parametrize('o,z,tag',[(100,101,'淨報酬為正'),(100,100.1,'成本使日內正報酬轉非正'),(104,103,'跳空上漲但日內未延續'),(99,98,'日內未獲利')])
def test_attribution(o,z,tag):
    e=Evidence();s=sig('1000');e.set('1000',o,z);r=h.attribution(s,reconcile(s,e),[D,T],e,NOW)
    assert r['歸因']==tag and r['淨績效狀態']=='VERIFIED_NET';assert (1+r['隔夜跳空'])*(1+r['日內原價報酬'])-1==pytest.approx(r['原價收盤報酬']);assert r['成本拖累']>0

def test_raw_missing_not_filled():
    e=Evidence();s=sig('1000');r=h.attribution(s,reconcile(s,e,False),[D,T],e,NOW);assert r['原價收盤報酬'] is None and r['淨績效狀態']=='VERIFIED_NET'

@pytest.mark.parametrize('mode',['pending','ledger','raw','missing_action','supplement','late'])
def test_reject(mode):
    e=Evidence();s=sig('1000');r=reconcile(s,e);days=[D,T]
    if mode=='pending':days=[]
    if mode=='ledger':r['淨報酬']+=.1
    if mode=='raw':r['原始收盤績效']+=.1
    if mode=='missing_action':e.actions=False
    if mode=='supplement':e.data[D,'1000']=(e.data[D,'1000'][0],h.p.stamp(D+'T21:00:00+08:00'))
    if mode=='late':e.data[T,'1000']=(e.data[T,'1000'][0],NOW+timedelta(seconds=1))
    v=h.attribution(s,r,days,e,NOW);assert v['淨報酬'] is None and v['淨績效狀態']!='VERIFIED_NET'

def calculate(e,missing=False):
    ss=[sig(str(i)) for i in range(1000,1003)];attrs=[h.attribution(s,reconcile(s,e),[D,T],e,NOW) for s in ss]
    if missing:attrs=attrs[:2]
    bench=h.market_net(e.master,'TWSE',D,T,h.p.stamp(ss[0]['created_at']),e,NOW)
    return h.group_rows(D,'TWSE',ss,attrs,bench,T)[0],bench

def test_disjoint_reference():
    e=Evidence();[e.set(str(i),100,105) for i in range(1000,1003)];r,bench=calculate(e)
    assert r['狀態']=='DESCRIPTIVE_ONLY' and r['比較母體']==117 and r['比較有效']==117 and r['相對淨差']>0 and r['完整比較窗']
    assert r['非本組市場淨均值']==pytest.approx(bench['results']['1003']);assert r['淨勝率']==1

def test_partial_group_no_metric():
    r,_=calculate(Evidence(),True);assert r['平均淨報酬'] is None and r['相對淨差'] is None

def test_preday_liquidity():
    e=Evidence();e.data[D,'1000'][0][6]=1000;r,_=calculate(e);assert r['研究符合流動性']==2 and r['相對淨差'] is None and r['平均淨報酬'] is not None

@pytest.mark.parametrize('count,ok',[(6,True),(7,False)])
def test_market_coverage(count,ok):
    e=Evidence();[e.data.pop((T,str(i))) for i in range(1100,1100+count)];r,bench=calculate(e);assert bench['ready']==ok
    # Excluded group has a smaller denominator; even 6 gaps fail disjoint 95%.
    assert r['相對淨差'] is None and not r['完整比較窗']

def test_benchmark_obs_bound():
    e=Evidence();e.data[T,'1100']=(e.data[T,'1100'][0],NOW-timedelta(minutes=5));r,_=calculate(e);assert r['結果觀測時間']==(NOW-timedelta(minutes=5)).isoformat()

def test_zero_loss_no_infinite_profit_factor():
    assert h.metrics([.1,.2])['報酬獲利因子'] is None
    assert h.metrics([.1,-.05])['報酬獲利因子']==2

def test_empty_run(tmp_path):
    r=h.run(tmp_path,NOW);assert r['status']=='DATA_PENDING' and r['weight_multiplier']==1 and not r['trade_authority_changed'];assert len(load_workbook(io.BytesIO(h.excel(r))).sheetnames)==11;assert not list(tmp_path.iterdir())

def test_temporal_gate():
    from datetime import datetime
    start=datetime.fromisoformat('2026-01-01T20:00:00+08:00');rows=[]
    for i in range(30):
        d=start+timedelta(days=i*2);t=d+timedelta(days=1);rows.append({'市場':'TWSE','研究群組':h.GROUPS[0],'出場日':t.date().isoformat(),'研究日':d.date().isoformat(),'研究封存':3,'決策時間':d.isoformat(),'結果觀測時間':t.isoformat(),'相對淨差':.01,'平均淨報酬':.02,'淨勝率':.67,'完整比較窗':True,'狀態':'DESCRIPTIVE_ONLY'})
    assert h.validate(rows,NOW)[0]['狀態']=='REVIEW_EDGE'
    rows[0]['完整比較窗']=False;assert h.validate(rows,NOW)[0]['狀態']=='BASELINE_ONLY'
    rows[0]['完整比較窗']=True;rows[0]['結果觀測時間']=rows[20]['決策時間'];assert h.validate(rows,NOW)[0]['狀態']=='BASELINE_ONLY'
