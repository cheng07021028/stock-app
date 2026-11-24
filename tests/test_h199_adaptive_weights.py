from datetime import datetime,date,timedelta
import io, json, pytest
import godpick_h199_adaptive_weights as h
NOW=datetime.fromisoformat('2026-11-24T23:00:00+08:00')
A,B=h.STRATEGIES[:2]

def sample(reverse=False,missing=False,late=False,flat=False):
    rows=[]
    for i in range(30):
        d=date(2025,1,1)+timedelta(days=i*7);t=d+timedelta(days=5)
        a,b=(4.,0.) if not flat else (0.,0.)
        if reverse and i>=20:a,b=b,a
        s={k:{'有效':True,'相對對照pp':x-y,'候選日均%':x,'結果最晚觀測':(date(2026,1,1) if late and i<20 else t).isoformat()+'T22:00:00+08:00'} for k,x,y in [(A,a,b),(B,b,a)]}
        if missing and i==25:s[B]['有效']=False
        rows.append({'day':d.isoformat(),'target':t.isoformat(),'decision':d.isoformat()+'T22:00:00+08:00','strategies':s})
    return rows

def test_train_only_and_holdout():
    r=h.fit(sample());assert r['weights'][A]==1.1 and r['weights'][B]==.9 and r['holdout_windows']==10 and r['train_windows']==20
    assert r['holdout_improvement_pp']==pytest.approx(.2)
    assert all(r['weights'][s]==1 for s in h.STRATEGIES[2:])

def test_reversal_no_publication():
    r=h.fit(sample(reverse=True));assert r['trial_weights'][A]==1.1 and r['weights']==h.neutral() and r['holdout_improvement_pp']<0

@pytest.mark.parametrize('kw',[{'missing':True},{'late':True},{'flat':True}])
def test_no_unsupported_adjustment(kw):assert h.fit(sample(**kw))['weights']==h.neutral()

def test_overlap_and_blocked():
    a=sample();a[1]['day']=a[0]['target'];assert h.fit(a)['weights']==h.neutral();assert h.fit(sample(),True)['weights']==h.neutral()

class FakeEvidence(h.Evidence):
    def __init__(self,mode):self.mode=mode
    def window(self,m,d,cut,n):
        rows=[{str(c):(100+i*self.mode,1e7) for c in range(1000,1120)} for i in range(n)]
        return ['2026-11-23']*n,rows

@pytest.mark.parametrize('mode,reg',[(1,h.REGIMES[0]),(-1,h.REGIMES[1]),(0,h.REGIMES[2])])
def test_market_regime(mode,reg):assert FakeEvidence(mode).regime('TWSE','2026-11-23',NOW)['狀態']==reg

def test_missing_market():
    e=FakeEvidence(1);e.window=lambda *args:(['x']*6,[{}]*6);assert e.regime('TWSE','2026-11-23',NOW)['狀態']=='待補證'

def test_future_archive(tmp_path):
    class M:errors=[];market={'TWSE':{'2026-11-23':1}};p=tmp_path
    path=tmp_path/'daily/TWSE/2026-11-23.json.gz';h.ar._write(path,h.ar._seal({'observed_at':'2026-11-24T18:30:00+08:00','rows':[['2330',100,101,99,100,100,10000,1]]}))
    e=h.Evidence(M());assert not e.quotes('TWSE','2026-11-23',h.p.stamp('2026-11-23T22:00:00+08:00'));assert e.quotes('TWSE','2026-11-23',NOW)['2330']==(100,10000)

def test_repeat_no_penalty():
    ss={}
    for i in range(5):
        d=(date(2026,11,10)+timedelta(days=i)).isoformat();s={'market':'TWSE','signal_day':d,'code':'2330','name':'台積電','tier':h.p.TIERS[0],'authority':'RESEARCH_ONLY','created_at':d+'T22:00:00+08:00'};ss[h.p.sid(s)]=s
    su,r=h.repetition(ss,NOW);assert r[0]['近5封存日出現次數']==5 and su[0]['前次重疊率']==1 and '不扣分' in r[0]['提示'] and su[0]['今日資料狀態']=='歷史封存／非今日'

def test_empty_and_export(tmp_path):
    import openpyxl
    r=h.run(tmp_path,NOW);assert r['status']=='BASELINE_ONLY' and len(r['recommendations'])==12 and not r['weights_changed'] and not r['trade_authority_changed'] and not (tmp_path/'data').exists()
    assert len(openpyxl.load_workbook(io.BytesIO(h.excel(r))).sheetnames)==8

def test_rotation(tmp_path):
    # current master descriptive, never used in learning
    master=[{'code':str(c),'market':'上市','official_industry':'電子' if c<1060 else '其他產業'} for c in range(1000,1120)]
    (tmp_path/'stock_master_cache.json').write_text(json.dumps(master,ensure_ascii=False),'utf-8')
    e=FakeEvidence(1);r=h.rotation(e,tmp_path,'2026-11-23',NOW)
    assert len(r)==2 and all(x['兩期成交額比']==1 and '非淨流入' in x['口徑'] for x in r)
