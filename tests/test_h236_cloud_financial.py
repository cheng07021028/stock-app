from datetime import timedelta
from pathlib import Path
import json,sys,types
import pytest
from test_h235_official_batch import setup,collect,raw,Q,NOW,url
from test_h234_batch_financial import name,fixture
import godpick_h236_cloud_financial as c
import godpick_h235_official_batch as h
import godpick_h192_financial_growth as f
import godpick_h190_market_archive as ar

class Store:
    def __init__(self):self.objects={};self.next=0;self.writes=[];self.reads=[];self.fail_manifest=False
    def head(self,k):
        v=self.objects.get(k);return (v[0],len(v[1])) if v else None
    def put(self,k,path,content_type):
        self.writes.append(k)
        if self.fail_manifest and k.endswith('manifest.json'):raise OSError('simulated interruption')
        if k in self.objects:return None
        self.next+=1;self.objects[k]=(self.next,Path(path).read_bytes());return self.next
    def get(self,k,g,path,limit):
        self.reads.append(k);generation,blob=self.objects[k]
        assert generation==g and 0<len(blob)<=limit
        Path(path).write_bytes(blob)

def roots(tmp_path,monkeypatch):
    a=tmp_path/'publisher';b=tmp_path/'reader';a.mkdir();b.mkdir();setup(a,monkeypatch);setup(b,monkeypatch);return a,b

def test_publish_restore_original_time(tmp_path,monkeypatch):
    a,b=roots(tmp_path,monkeypatch);collect(a);s=Store();future=NOW+timedelta(hours=3)
    r=c.publish(a,Q,s,future);assert r['status']=='PUBLISHED' and r['observed_at']==NOW.isoformat()
    assert s.writes[-1]==c.manifest_key(Q) and s.writes[0].endswith('.zip')
    assert c.restore(b,Q,s,future)['status']=='RESTORED'
    assert h.paths(a,Q)[0].read_bytes()==h.paths(b,Q)[0].read_bytes()
    assert h.paths(a,Q)[1].read_bytes()==h.paths(b,Q)[1].read_bytes()
    data,err=f.load_inputs(b);assert not err
    old,_,_=f.build_ledgers(data,NOW-timedelta(seconds=1));assert not old['ytd','TWSE','2330']
    new,_,_=f.build_ledgers(data,NOW);assert Q in new['ytd','TWSE','2330']

def test_idempotent_no_raw_network(tmp_path,monkeypatch):
    a,b=roots(tmp_path,monkeypatch);collect(a);s=Store();c.publish(a,Q,s,NOW);c.restore(b,Q,s,NOW);writes=list(s.writes);s.reads.clear()
    assert c.publish(a,Q,s,NOW)['status']=='ALREADY_PUBLISHED'
    assert c.restore(b,Q,s,NOW)['status']=='ALREADY_RESTORED'
    assert s.writes==writes and all(k.endswith('manifest.json') for k in s.reads)

def test_status_metadata_only(tmp_path,monkeypatch):
    a,b=roots(tmp_path,monkeypatch);s=Store();assert c.cloud_status(b,Q,s,NOW)['status']=='NOT_COMMITTED';collect(a);c.publish(a,Q,s,NOW);s.reads.clear()
    assert c.cloud_status(b,Q,s,NOW)['status']=='CLOUD_MANIFEST_PRESENT';assert s.reads==[c.manifest_key(Q)]

def test_orphan_resume_verified(tmp_path,monkeypatch):
    a,b=roots(tmp_path,monkeypatch);collect(a);s=Store();s.fail_manifest=True
    with pytest.raises(OSError):c.publish(a,Q,s,NOW)
    assert c.cloud_status(b,Q,s,NOW)['status']=='NOT_COMMITTED'
    with pytest.raises(ValueError):c.restore(b,Q,s,NOW)
    s.fail_manifest=False;assert c.publish(a,Q,s,NOW)['status']=='PUBLISHED';assert any(k.endswith('.zip') for k in s.reads)
    assert c.restore(b,Q,s,NOW)['status']=='RESTORED'

def test_corrupt_orphan_rejected(tmp_path,monkeypatch):
    a,b=roots(tmp_path,monkeypatch);collect(a);s=Store();s.fail_manifest=True
    with pytest.raises(OSError):c.publish(a,Q,s,NOW)
    key=next(iter(s.objects));g,blob=s.objects[key];s.objects[key]=(g,b'x'*len(blob));s.fail_manifest=False
    with pytest.raises(ValueError,match='SHA256'):c.publish(a,Q,s,NOW)
    assert c.manifest_key(Q) not in s.objects

@pytest.mark.parametrize('mode',['bytes','generation','missing','manifest','future','period','key','size'])
def test_corrupt_cloud_rejected(tmp_path,monkeypatch,mode):
    a,b=roots(tmp_path,monkeypatch);collect(a);s=Store();c.publish(a,Q,s,NOW)
    mk=c.manifest_key(Q);g,blob=s.objects[mk];m=ar._verify(json.loads(blob));key=m['archive_key'];kg,kb=s.objects[key]
    if mode=='bytes':s.objects[key]=(kg,b'x'*len(kb))
    elif mode=='generation':s.objects[key]=(kg+1,kb)
    elif mode=='missing':del s.objects[key]
    elif mode=='manifest':s.objects[mk]=(g,blob[:-1]+b'x')
    else:
        if mode=='future':m['published_at']=(NOW+timedelta(days=1)).isoformat()
        elif mode=='period':m['period']=Q-1
        elif mode=='key':m['archive_key']='other/evil.zip'
        elif mode=='size':m['receipt']['archive_bytes']=h.batch.MAX_ARCHIVE+1
        s.objects[mk]=(g,ar._bytes(ar._seal(m)))
    with pytest.raises((ValueError,KeyError)):c.restore(b,Q,s,NOW)
    assert not h.paths(b,Q)[1].exists()

def test_existing_local_conflict_no_overwrite(tmp_path,monkeypatch):
    a,b=roots(tmp_path,monkeypatch);collect(a);collect(b,blob=raw(Q,[(name(Q),fixture(Q,growth=1.4))]));s=Store();c.publish(a,Q,s,NOW);before=h.paths(b,Q)[0].read_bytes()
    with pytest.raises(ValueError,match='no overwrite'):c.restore(b,Q,s,NOW)
    with pytest.raises(ValueError,match='different evidence'):c.publish(b,Q,s,NOW)
    assert h.paths(b,Q)[0].read_bytes()==before

def test_no_h234_upgrade(tmp_path,monkeypatch):
    a,b=roots(tmp_path,monkeypatch);z,r=h.paths(a,Q);z.parent.mkdir(parents=True);z.write_bytes(raw());s=Store()
    with pytest.raises(Exception):c.publish(a,Q,s,NOW)
    assert not s.writes

def test_configured_empty_no_connection(tmp_path,monkeypatch):
    monkeypatch.delenv('H236_RESTORE_QUARTERS',raising=False);monkeypatch.setattr(c,'GCS',lambda:pytest.fail('network disabled'))
    assert c.restore_configured(tmp_path)==[]

@pytest.mark.parametrize('periods',['2026Q3,2026Q3','bad','2020Q1'])
def test_configured_bad_plan(tmp_path,periods):
    with pytest.raises(ValueError):c.restore_configured(tmp_path,Store(),NOW,periods)

def test_configured_preflight_budget_and_missing(tmp_path,monkeypatch):
    a,b=roots(tmp_path,monkeypatch);collect(a);s=Store();c.publish(a,Q,s,NOW)
    with pytest.raises(ValueError,match='not committed'):c.restore_configured(b,s,NOW,f'{f.qstr(Q)},{f.qstr(Q-1)}')
    with pytest.raises(ValueError,match='budget'):c.restore_configured(b,s,NOW,f.qstr(Q),max_bytes=1)
    assert not h.paths(b,Q)[1].exists()
    assert c.restore_configured(b,s,NOW,f.qstr(Q))[0]['status']=='RESTORED'

def test_signature_includes_restored_batch(tmp_path,monkeypatch):
    a,b=roots(tmp_path,monkeypatch);collect(a);s=Store();c.publish(a,Q,s,NOW)
    from godpick_h223_growth_scenarios import signature
    before=signature(b);c.restore(b,Q,s,NOW);assert before!=signature(b)

def test_gcs_adapter_preconditions(tmp_path,monkeypatch):
    # Validate actual SDK call contract without credentials/network or installed SDK.
    class PreconditionFailed(Exception):pass
    calls=[]
    class Blob:
        generation=42;size=3
        def reload(self,**kw):calls.append(('reload',kw))
        def upload_from_filename(self,p,**kw):calls.append(('put',kw));assert Path(p).read_bytes()==b'abc'
        def download_to_filename(self,p,**kw):calls.append(('get',kw));Path(p).write_bytes(b'abc')
    class Bucket:
        def get_blob(self,k,**kw):return Blob()
        def blob(self,k,**kw):calls.append(('blob',kw));return Blob()
    adapter=c.GCS.__new__(c.GCS);adapter.bucket=Bucket();p=tmp_path/'data';p.write_bytes(b'abc')
    api=types.ModuleType('google.api_core');exc=types.ModuleType('google.api_core.exceptions');exc.PreconditionFailed=PreconditionFailed;api.exceptions=exc
    monkeypatch.setitem(sys.modules,'google.api_core',api);monkeypatch.setitem(sys.modules,'google.api_core.exceptions',exc)
    assert adapter.put('x',p,'application/zip')==42;adapter.get('x',42,p,3)
    assert next(v for k,v in calls if k=='put')['if_generation_match']==0
    assert next(v for k,v in calls if k=='get')['if_generation_match']==42
    assert any(k=='blob' and v=={'generation':42} for k,v in calls)

def test_eight_cloud_quarters_named_candidate(tmp_path,monkeypatch):
    a,b=roots(tmp_path,monkeypatch);s=Store()
    for p in range(Q-7,Q+1):
        collect(a,p,raw(p,[(name(p),fixture(p,growth=1.2 if p//4==Q//4 else 1))]));c.publish(a,p,s,NOW)
    assert len(c.restore_configured(b,s,NOW,','.join(f.qstr(p) for p in range(Q-7,Q+1))))==8
    m,_=f.expected_periods(NOW);path=b/'data/h115/revenue.jsonl';path.parent.mkdir(parents=True)
    path.write_text('\n'.join(json.dumps({'code':'2330','source':'TWSE','source_url':f.URLS['revenue','TWSE'],'availability_basis':'FIRST_OBSERVED','available_at':NOW.isoformat(),'year_month':f.monthstr(i),'revenue_twd':1000000+(i-(m-12))*100000,'previous_month_twd':None,'prior_year_month_twd':None}) for i in range(m-12,m+1)))
    import godpick_h222_financial_history as hist
    r=hist.inspect(b,NOW);assert len(r['candidates'])==1 and r['candidates'][0]['股票代號']=='2330' and not r['h232_candidates']
    assert len(r['batch_scope'])==8
