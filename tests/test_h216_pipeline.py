from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
import gzip,json
import pytest
import godpick_h190_market_archive as ar
import godpick_h194_performance as perf
import godpick_h209_rolling_backtest as back
from test_h190_market_archive import raw
from test_h209_rolling_backtest import NOW,DAYS,source,event
from test_h194_performance import setup_market,result

@pytest.mark.parametrize('kind',['query','queryParams','dateRange','queryDateRange','reportRange'])
def test_explicit_metadata(kind):
    lo,hi=DAYS[0],DAYS[2];r=source(hi,[event(hi)]);r.pop('title')
    if kind=='reportRange':r.update(reportStartDate='1151123',reportEndDate='1151125')
    elif kind=='dateRange':r[kind]=['115/11/23','115/11/25']
    elif kind=='queryDateRange':r[kind]='115/11/23 ～ 115/11/25'
    else:r[kind]={'startDate':lo,'endDate':hi}
    assert back.parse_actions(r,'TPEX',hi,NOW,start=lo,end=hi)['rows'][0]['cash']==5
    assert back.parse_actions(r,'TPEX',DAYS[1],NOW,start=lo,end=hi)['rows']==[]
@pytest.mark.parametrize('kind',['missing','reversed','conflict','empty_unverified'])
def test_reject_unproven_range(kind):
    lo,hi=DAYS[0],DAYS[2];r=source(hi);r.pop('title')
    if kind=='missing':r['query']={'startDate':lo}
    elif kind=='reversed':r['dateRange']=[hi,lo]
    elif kind=='conflict':r.update(query={'startDate':lo,'endDate':hi},dateRange=[DAYS[1],hi])
    with pytest.raises(ValueError):back.parse_actions(r,'TPEX',hi,NOW,start=lo,end=hi)

def test_request_diagnostics_both_attempts(tmp_path):
    class R:
        def raise_for_status(self):pass
        def json(self):return {'stat':'OK','title':'unconfirmed','fields':source(DAYS[2])['fields'],'data':[],'total':0,'ignored_secret':'never retain'}
    class H:
        def get(self,*a,**k):return R()
    v=back.collect_actions(tmp_path,SimpleNamespace(days=[]),NOW,[('TPEX',DAYS[-1])],budget=2,http=H())
    assert v['attempted']==len(v['request_log'])==2 and v['saved']==0 and v['pending']==1
    assert v['request_log'][0]['start']!=v['request_log'][1]['start'] and all(x['status']=='REJECTED' for x in v['request_log'])
    assert 'ignored_secret' in str(v['request_log']) # field name alone can appear; never its value
    assert 'never retain' not in str(v['request_log'])

def test_cache_reuse_cutoff_and_mutation(tmp_path,monkeypatch):
    setup_market(tmp_path);p=tmp_path/'data/h190';ar._invalidate(p)
    n=[0];orig=ar._read
    def read(*a,**kw):n[0]+=1;return orig(*a,**kw)
    monkeypatch.setattr(ar,'_read',read)
    t=datetime.fromisoformat('2026-11-23T23:45:00+08:00')
    a=ar._index(p,asof=t);before=n[0];assert ar._index(p,asof=t) is a and n[0]==before
    # A prior cutoff cannot reuse current quotes or calendar.
    old=ar._index(p,asof=t-timedelta(days=1));assert '2026-11-23' not in old[1]['TWSE']
    ar._index(p,asof=t)
    q=p/'daily/TWSE/2026-11-23.json.gz';q.write_bytes(b'corrupt')
    assert ar._index(p,asof=t)[3]

def test_capture_preflight_avoids_joint(tmp_path,monkeypatch):
    import godpick_h193_joint_decision as joint
    def no(*a,**kw):pytest.fail('joint should not run without confirmed calendar')
    monkeypatch.setattr(joint,'run',no)
    r=perf.run(tmp_path,NOW);assert r['capture_gate']['reason']=='CALENDAR_NOT_CONFIRMED'
    r=perf.run(tmp_path,NOW.replace(hour=17));assert r['capture_gate']['reason']=='BEFORE_CAPTURE_WINDOW'

def test_capture_gate_and_original_ledger(tmp_path,monkeypatch):
    import godpick_h193_joint_decision as joint
    setup_market(tmp_path);monkeypatch.setattr(joint,'run',lambda *a,**k:(result(),{}))
    now=datetime.fromisoformat('2026-11-23T23:45:00+08:00');r=perf.run(tmp_path,now)
    assert r['capture_gate']['reason']=='CAPTURED' and r['signals']==1
    p=tmp_path/'data/h194/signals/2026-11-23.json.gz';before=p.read_bytes()
    assert perf.run(tmp_path,now+timedelta(minutes=1))['capture_gate']['reason']=='ALREADY_CAPTURED';assert p.read_bytes()==before

def test_live_midnight_no_backdate(tmp_path,monkeypatch):
    import godpick_h193_joint_decision as joint
    setup_market(tmp_path)
    now=datetime.fromisoformat('2026-11-23T23:45:00+08:00')
    class Clock(datetime):
        pass
        @classmethod
        def now(cls,tz=None):return next(cls.ticks)
    Clock.ticks=iter([now,now+timedelta(minutes=20)])
    monkeypatch.setattr(perf,'datetime',Clock);monkeypatch.setattr(joint,'run',lambda *a,**k:(result(),{}))
    r=perf.run(tmp_path);assert r['capture_gate']['reason']=='CAPTURE_WINDOW_EXPIRED';assert not (tmp_path/'data/h194/signals').exists()

class HTTP:
    def __init__(self,fail=None,closed=False):self.calls=[];self.fail=fail;self.closed=closed
    def get(self,url,params,timeout,**kw):
        self.calls.append(url)
        if url==self.fail:raise RuntimeError('test failure')
        if url==ar.CAL_URL:obj={'stat':'OK','fields':['日期','收盤指數'],'data':[[DAYS[1] if self.closed else DAYS[2],30000]]}
        else:obj=raw(DAYS[2],next(m for m,u in ar.URLS.items() if u==url))
        return SimpleNamespace(status_code=200,json=lambda:obj)

def test_today_first_and_idempotent(tmp_path):
    h=HTTP();v=ar.collect_session(tmp_path,http=h,now=NOW,pause=0)
    assert v['status']=='SESSION_QUOTES_VERIFIED' and v['new_files']==2 and v['requests']==3
    assert not (tmp_path/'health.json').exists()
    v=ar.collect_session(tmp_path,http=h,now=NOW+timedelta(minutes=1),pause=0)
    assert v['new_files']==0 and v['requests']==1 and len(h.calls)==4
@pytest.mark.parametrize('mode',['before','closed','market_failed'])
def test_today_gate(tmp_path,mode):
    h=HTTP(fail=ar.URLS['TPEX'] if mode=='market_failed' else None,closed=mode=='closed')
    v=ar.collect_session(tmp_path,http=h,now=NOW.replace(hour=17) if mode=='before' else NOW,pause=0)
    if mode=='before':assert v['status']=='BEFORE_CAPTURE_WINDOW' and not h.calls
    elif mode=='closed':assert v['status']=='CALENDAR_NO_SESSION_REVIEW' and len(h.calls)==1
    else:assert v['status']=='SESSION_QUOTES_PENDING' and v['new_files']==1 and v['markets']['TWSE']['status']=='VERIFIED'

def test_report_rejects_concurrent_archive_generation(tmp_path,monkeypatch):
    import godpick_h215_proof as proof
    from godpick_h214_universe import run as real
    import godpick_h214_universe as uni
    def changed(*a,**kw):
        r=real(*a,**kw);path=tmp_path/'data/h190/calendar/2026-11.json';path.parent.mkdir(parents=True,exist_ok=True);path.write_text('{}');return r
    monkeypatch.setattr(uni,'run',changed)
    with pytest.raises(RuntimeError,match='single generation'):proof.run(tmp_path,NOW)
