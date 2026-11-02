from datetime import date,datetime,time,timezone
from tools.h150_actions_chain import select_prior_run,run_stage

def item(created, status='completed',conclusion='success'):
 return {'created_at':created,'head_branch':'main','status':status,'conclusion':conclusion}

def test_stage_cannot_use_pre_release_run():
 runs=[item('2026-11-02T06:00:00Z')]
 assert select_prior_run(runs,today=date(2026,11,2),release=time(18,45)) is None

def test_stage_prefers_latest_after_release():
 runs=[item('2026-11-02T11:05:00Z'),item('2026-11-02T11:00:00Z',conclusion='failure')]
 assert select_prior_run(runs,today=date(2026,11,2),release=time(18,45))==runs[0]

class Stub:
 def __init__(self,runs):self.runs=runs;self.calls=0
 def latest(self,w):return self.runs
 def launch(self,w):self.calls+=1

def test_before_release_does_not_start():
 client=Stub([])
 assert run_stage(client,'test.yml',time(21,45),1,now=datetime(2026,11,2,12,tzinfo=timezone.utc))=='WAIT_OFFICIAL_RELEASE'
 assert client.calls==0

def test_existing_success_skips_dispatch():
 client=Stub([item('2026-11-02T14:00:00Z')])
 assert run_stage(client,'test.yml',time(21,45),1,now=datetime(2026,11,2,15,tzinfo=timezone.utc))=='WORKFLOW_DONE_SUCCESS'
 assert client.calls==0
