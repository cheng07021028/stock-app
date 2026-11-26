from datetime import timedelta
import io
import pytest
from openpyxl import load_workbook
import godpick_h239_rotation_counterproof as h
from test_h217_prospective_quality import sig,D,T,NOW,market
from test_h229_net_edge import Evidence,reconcile


def window():
    ds=[(NOW.date()-timedelta(days=i)).isoformat() for i in range(6,0,-1)];s=sig(day=ds[-1]);m=market(ds);m.errors=[]
    ix={(d,'TWSE'):{'9999':sig('9999',day=d)} for d in ds[:-1]}
    return ds,s,m,ix


def test_full_prior_not_seen():
    ds,s,m,ix=window();assert h.membership(s,ix,m)==(h.NEW,0,5,0)


def test_watch_not_repeat():
    ds,s,m,ix=window();ix[ds[-2],'TWSE'][s['code']]={**sig(day=ds[-2]),'tier':h.p.TIERS[-1]}
    assert h.membership(s,ix,m)==(h.NEW,0,5,1)


def test_repeat_on_known_recent():
    ds,s,m,ix=window();ix.pop((ds[0],'TWSE'));ix[ds[-2],'TWSE'][s['code']]=sig(day=ds[-2])
    assert h.membership(s,ix,m)==(h.REPEAT,1,4,0)

@pytest.mark.parametrize('mode',['missing','wrong_market','future','calendar'])
def test_unknown(mode):
    ds,s,m,ix=window()
    if mode=='missing':ix.pop((ds[0],'TWSE'))
    elif mode=='wrong_market':ix[ds[0],'TPEX']=ix.pop((ds[0],'TWSE'))
    elif mode=='future':ix[ds[0],'TWSE']['9999']['created_at']=s['created_at']
    else:m.span=lambda *a:[]
    assert h.membership(s,ix,m)[0]==h.UNKNOWN


def market_data():
    master={str(i):{'股票名稱':str(i),'市場':'TWSE','產業':'A' if i<1010 else 'B' if i<1020 else 'C'} for i in range(1000,1100)}
    a={c:(100,30_000_000) for c in master};z={c:(103 if int(c)<1010 else 100,30_000_000) for c in master}
    return master,a,z


def test_sector_strength_and_rotation():
    master,a,z=market_data();before,_=h.sector_stats(master,'TWSE',a,a);current,ready=h.sector_stats(master,'TWSE',a,z)
    assert ready and current['A']['strong'] and current['A']['relative']==pytest.approx(.03)
    assert h.rotation(before['A'],current['A'])==h.EMERGING
    assert h.rotation(current['A'],current['A'])==h.PERSIST
    assert h.rotation(current['A'],before['A'])==h.FADE
    assert h.rotation(None,current['A'])==h.UNVERIFIED


def test_sector_small_or_incomplete():
    master,a,z=market_data();z.pop('1000');r,ready=h.sector_stats(master,'TWSE',a,z)
    assert not ready and r=={} # minimum100 eligible
    for c in list(master)[:6]:master[c]['產業']='C'
    z['1000']=(103,30_000_000);r,ready=h.sector_stats(master,'TWSE',a,z);assert ready and 'A' not in r


def test_sector_future_quote_rejected():
    master,a,z=market_data();ds,s,m,ix=window();anchor=h.p.stamp(s['created_at'])
    def reader(root,m,d,cut):return a,(cut+timedelta(seconds=1)).isoformat()
    rows,classes,err=h.sector_windows('.',m,master,'TWSE',s['signal_day'],None,anchor,NOW,reader)
    assert not rows and not classes and err


def test_sector_preday_only():
    master,a,z=market_data();ds,s,m,ix=window();m.days=ds+[T];anchor=h.p.stamp(s['created_at'])
    data={ds[-3]:a,ds[-2]:a,ds[-1]:z,T:a}
    def reader(root,m,d,cut):return data[d],d+'T19:00:00+08:00'
    rows,classes,err=h.sector_windows('.',m,master,'TWSE',s['signal_day'],T,anchor,NOW,reader)
    assert not err and classes['A']==h.EMERGING and next(x for x in rows if x['產業']=='A')['次日強勢'] is False


def pair_fixture():
    return [{'研究日':D,'市場':'TWSE','研究分級':'技術研究','研究新鮮度':h.REPEAT if i<3 else h.NEW,'決策前輪動':h.PERSIST if i<3 else h.EMERGING,'原價時點重驗':'VERIFIED','淨績效狀態':'VERIFIED_NET','淨報酬':.01 if i<3 else .03,'出場日':T} for i in range(6)]


def test_same_day_tier_pair():
    rows=h.paired(pair_fixture());assert len(rows)==2 and all(r['相對淨差']==pytest.approx(.02) for r in rows)

@pytest.mark.parametrize('mode',['raw','net','tier','target','market','day','short','nan'])
def test_no_partial_or_mixed_pair(mode):
    r=pair_fixture()
    if mode=='raw':r[0]['原價時點重驗']='PENDING'
    elif mode=='net':r[0]['淨績效狀態']='PENDING'
    elif mode=='tier':r[0]['研究分級']='財報研究'
    elif mode=='target':r[0]['出場日']=None
    elif mode=='market':r[0]['市場']='TPEX'
    elif mode=='day':r[0]['研究日']=T
    elif mode=='short':r.pop()
    else:r[0]['淨報酬']=float('nan')
    assert all(x['相對淨差'] is None for x in h.paired(r))


def test_empty_excel(tmp_path):
    r=h.run(tmp_path,NOW);assert r['status']=='DATA_PENDING' and not r['trade_authority_changed'] and r['weight_multiplier']==1 and not list(tmp_path.iterdir())
    r['detail']=[{'股票代號':'2330','股票名稱':'台積電'}];w=load_workbook(io.BytesIO(h.excel(r)));assert len(w.sheetnames)==6 and w['03_逐股反證']['B1'].value=='股票名稱'
