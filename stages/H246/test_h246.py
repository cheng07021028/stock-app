import copy, json, subprocess
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import pytest
import godpick_h246_seal_consistency as h

TW=ZoneInfo('Asia/Taipei'); DAY='2026-11-27'
CTX={'run_id':'gprun_new','run_date':DAY,'started_at':DAY+' 17:52:21','owner':'07_股神推薦','trigger':'V191中央自動排程'}
ROW={'record_id':'r1','股票代號':'2330','市場別':'上市','推薦日期':DAY}
def snap(ctx=CTX,rs=None,saved=None):
    rs=[] if rs is None else rs
    return {'execution_context':ctx,'run_id':ctx['run_id'],'recommendation_date':ctx['run_date'],'saved_at':saved or ctx['run_date']+' 17:56:53','recommendations':rs,'recommendation_count':len(rs)}
def state(p,n):return {'revision':n,'payload_hash':h.digest(p)}
def fixture(nonempty=False):
    rs=[ROW] if nonempty else []; hist=[ROW]; new=snap(rs=rs); old=snap({**CTX,'run_id':'old','run_date':'2026-11-26','started_at':'2026-11-26 21:00:00'}); anchor={**new,'full_snapshot_payload_hash':h.digest(new)}
    d={h.ANCHOR:anchor,h.LATEST:old,h.LIST:rs,h.RECORDS_STATE:{'count':len(hist),'payload_hash':h.digest(hist),'storage_mode':'h127-gzip-shards-v1'}}
    for k,n in [(h.ANCHOR,20),(h.LATEST,10),(h.LIST,21)]:d[h.state_name(k)]=state(d[k],n)
    return copy.deepcopy(d),copy.deepcopy(new),copy.deepcopy(hist)
def ev(d,new,hist):return h.evaluate(d.get,CTX,hist,new)
@pytest.mark.parametrize('nonempty',[False,True])
def test_exact_repair(nonempty):
    d,new,hist=fixture(nonempty);assert ev(d,new,hist)['status']=='EXACT_REPAIR_AVAILABLE'
    d[h.LATEST]=new;d[h.state_name(h.LATEST)]=state(new,22);assert ev(d,new,hist)['confirmed']
@pytest.mark.parametrize('case,expect',[
 ('anchor_hash','ANCHOR_NOT_CURRENT_OR_UNVERIFIED'),('anchor_run','ANCHOR_NOT_CURRENT_OR_UNVERIFIED'),('missing_list','LIST_UNVERIFIED'),('list_conflict','LIST_IDENTITY_CONFLICT'),('history_hash','HISTORY_CONTENT_UNVERIFIED'),('no_local','EXACT_FULL_SNAPSHOT_UNAVAILABLE'),('wrong_local','EXACT_FULL_SNAPSHOT_UNAVAILABLE'),('bad_latest','LATEST_REMOTE_UNVERIFIED'),('new_revision','LATEST_NEWER_OR_CONFLICT'),('future_latest','LATEST_NEWER_OR_CONFLICT')])
def test_guard(case,expect):
    d,new,hist=fixture()
    if case=='anchor_hash':d[h.ANCHOR]['saved_at']='bad'
    if case=='anchor_run':d[h.ANCHOR]['execution_context']['run_id']='other';d[h.state_name(h.ANCHOR)]=state(d[h.ANCHOR],20)
    if case=='missing_list':del d[h.LIST]
    if case=='list_conflict':d[h.LIST]=[ROW];d[h.state_name(h.LIST)]=state(d[h.LIST],21)
    if case=='history_hash':hist=[]
    if case=='no_local':new=None
    if case=='wrong_local':new['x']=1
    if case=='bad_latest':d[h.state_name(h.LATEST)]['payload_hash']='0'*64
    if case=='new_revision':d[h.state_name(h.LATEST)]['revision']=21
    if case=='future_latest':d[h.LATEST]=snap({**CTX,'run_id':'other'},saved=DAY+' 18:00:00');d[h.state_name(h.LATEST)]=state(d[h.LATEST],10)
    assert ev(d,new,hist)['status']==expect

def test_deleted_never_revived():
    d,new,hist=fixture(True);hist=[];d[h.RECORDS_STATE].update(count=0,payload_hash=h.digest(hist));assert ev(d,new,hist)['status']=='HISTORY_IDENTITY_CONFLICT'
def test_duplicate_list():
    d,new,hist=fixture(True);d[h.LIST]=[ROW,ROW];d[h.state_name(h.LIST)]=state(d[h.LIST],21);assert ev(d,new,hist)['status']=='LIST_IDENTITY_CONFLICT'
def test_wait_no_fetch(tmp_path):
    class V:
        def fetch(self,*a):pytest.fail('too early')
    now=datetime(2026,11,27,17,57,tzinfo=TW);assert h.check_eod(tmp_path,now.date(),now=now,checker=lambda *a,**k:pytest.fail('too early'),view=V())['status']=='WAIT_FOR_H117_OFFICIAL_EOD_WINDOW'
def test_eod_main_reuses_validator(tmp_path):
    now=datetime(2026,11,27,19,0,tzinfo=TW)
    class V:
        def fetch(self,branch):assert branch=='main';return 'abc'
        def blob(self,head,path):assert head=='abc';return b'official fixture'
    def checker(root,day,now):
        return {'status':'EOD_THREE_MARKETS_INTERNAL_HASH_VERIFIED' if len(list(Path(root).rglob('*.gz')))==3 else 'EOD_NOT_VERIFIED'}
    out=h.check_eod(tmp_path,now.date(),now=now,checker=checker,view=V());assert out['status']=='EOD_THREE_MARKETS_INTERNAL_HASH_VERIFIED' and out['main_commit']=='abc' and not list(tmp_path.rglob('*.gz'))
def test_eod_partial(tmp_path):
    now=datetime(2026,11,27,19,0,tzinfo=TW)
    class V:
        def fetch(self,b):return 'abc'
        def blob(self,head,path):return None
    assert h.check_eod(tmp_path,now.date(),now=now,checker=lambda *a,**k:{'status':'EOD_NOT_VERIFIED'},view=V())['status']=='EOD_NOT_VERIFIED'

def git(root,*args):return subprocess.check_output(['git',*args],cwd=root,stderr=subprocess.STDOUT).decode().strip()
def test_real_git_atomic_and_conflict(tmp_path):
    bare=tmp_path/'bare';bare.mkdir();git(bare,'init','--bare');w=tmp_path/'work';w.mkdir();git(w,'init');git(w,'config','user.name','test');git(w,'config','user.email','test@example.invalid');git(w,'remote','add','origin',str(bare));d,new,hist=fixture()
    for p,x in d.items():(w/p).write_text(json.dumps(x),encoding='utf-8')
    git(w,'add','.');git(w,'commit','-m','initial');git(w,'push','origin','HEAD:refs/heads/runtime-data');v=h.GitView(w);parent=v.fetch('runtime-data');c=v.publish(parent,{h.LATEST:new,h.state_name(h.LATEST):state(new,22)})
    assert v.fetch('runtime-data')==c;assert h.evaluate(lambda p:v.read(c,p),CTX,hist)['confirmed'];assert v.read(c,h.RECORDS_STATE)==d[h.RECORDS_STATE]
    assert set(git(w,'diff','--name-only',parent,c).splitlines())=={h.LATEST,h.state_name(h.LATEST)}
    with pytest.raises(subprocess.CalledProcessError):v.publish(parent,{h.LATEST:new,h.state_name(h.LATEST):state(new,23)})
    assert v.fetch('runtime-data')==c
    with pytest.raises(ValueError):v.publish(c,{h.RECORDS_STATE:{}})

def test_worker_repair_and_readback(tmp_path,monkeypatch):
    day=datetime.now(TW).date().isoformat();ctx={**CTX,'run_date':day,'started_at':day+' 17:52:21'}
    hist=[];new=snap(ctx);old=snap({**CTX,'run_id':'old','run_date':'2000-01-01','started_at':'2000-01-01 17:00:00'});anchor={**new,'full_snapshot_payload_hash':h.digest(new)}
    d={h.ANCHOR:anchor,h.LATEST:old,h.LIST:[],h.RECORDS_STATE:{'count':0,'payload_hash':h.digest(hist),'storage_mode':'h127-gzip-shards-v1'}}
    for k,n in [(h.ANCHOR,20),(h.LATEST,10),(h.LIST,21)]:d[h.state_name(k)]=state(d[k],n)
    (tmp_path/h.LATEST).write_text(json.dumps(new),encoding='utf-8')
    class V:
        def __init__(self):self.head='parent';self.calls=0
        def fetch(self,branch):assert branch=='runtime-data';return self.head
        def read(self,head,p):assert head==self.head;return d.get(p)
        def publish(self,parent,files):assert parent==self.head;self.calls+=1;d.update(files);self.head='child';return self.head
    v=V();r07={'ok':True,'execution_context':ctx,'execution_owner':'pages/7_股神推薦.py'}
    assert h.run_check(r07,tmp_path,view=v,history_loader=lambda:hist,allow_write=False)['status']=='EXACT_REPAIR_REQUIRES_CENTRAL_WORKER';assert v.calls==0
    out=h.run_check(r07,tmp_path,view=v,history_loader=lambda:hist,allow_write=True);assert out['confirmed'] and out['repaired'] and v.calls==1
    assert h.run_check(r07,tmp_path,view=v,history_loader=lambda:hist,allow_write=True)['confirmed'] and v.calls==1

def test_wrong_owner_prevents_read(tmp_path):
    class V:
        def fetch(self,*a):pytest.fail('wrong owner attempted read')
    assert h.run_check({'ok':True,'execution_context':CTX,'execution_owner':'other'},tmp_path,view=V())['status']=='NOT_CANONICAL_RUN'

def test_invalid_scan_prevents_read(tmp_path):
    assert h.run_check({'ok':True,'record_integrity_failure':True},tmp_path)['status']=='SCAN_OR_HISTORY_FAILED'

def test_patcher_version_guard(tmp_path,monkeypatch):
    p=pytest.importorskip("apply_h246")
    from h245_baseline import PATCHES as prev
    # Synthetic source is used only to test version guard/transform mechanics.
    old='\n'.join(x for x,y in prev);new='\n'.join(y for x,y in prev)
    (tmp_path/p.TASK).write_text(new,encoding='utf-8');(tmp_path/'godpick_h245_list_authority.py').write_text('# test')
    monkeypatch.setattr(p,'H245_BASE',p.blob(old.encode()))
    assert p.verified_source(tmp_path,p.TASK)==new
    (tmp_path/p.TASK).write_text(new+'\n# unapproved change',encoding='utf-8')
    with pytest.raises(ValueError):p.verified_source(tmp_path,p.TASK)
    with pytest.raises(ValueError):p.replace_once('x x','x','y')
