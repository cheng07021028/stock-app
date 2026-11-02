from datetime import datetime,timezone,timedelta
from godpick_h150_login_sync import plan_from_runs,start_once

NOW=datetime(2026,11,2,14,tzinfo=timezone.utc)
CFG={'token':'TEST_SECRET','owner':'cheng07021028','repo':'stock-app','branch':'main'}
class R:
 def __init__(self,status,payload=None):self.status_code=status;self.payload=payload or {}
 def json(self):return self.payload
class HTTP:
 def __init__(self,runs=None, get_status=200,post_status=204):
  self.runs=runs if runs is not None else [];self.get_status=get_status;self.post_status=post_status;self.posts=0;self.calls=[]
 def get(self,*a,**k):self.calls.append('GET');return R(self.get_status,{'workflow_runs':self.runs})
 def post(self,*a,**k):self.posts+=1;self.calls.append('POST');assert k['json']['ref']=='main';return R(self.post_status)
def run(at,status='queued',conclusion=None,branch='main'):
 return {'created_at':at.isoformat().replace('+00:00','Z'),'status':status,'conclusion':conclusion,'head_branch':branch}
def test_missing_permission_never_dispatches():
 h=HTTP();result=start_once(config={**CFG,'token':''},session={},http=h,now=NOW)
 assert result['status']=='NOT_CONFIGURED' and not h.calls

def test_first_login_dispatches_once_per_session():
 h=HTTP();session={}
 assert start_once(config=CFG,session=session,http=h,now=NOW)['status']=='QUEUED'
 assert start_once(config=CFG,session=session,http=h,now=NOW)['status']=='QUEUED'
 assert h.calls==['GET','POST']

def test_other_user_is_deduped_by_remote_runs():
 h=HTTP([run(NOW-timedelta(minutes=10))])
 assert start_once(config=CFG,session={},http=h,now=NOW)['status']=='EXISTING_JOB'
 assert h.posts==0

def test_completed_same_day_needs_no_refresh():
 h=HTTP([run(NOW-timedelta(hours=3),'completed','success')])
 assert start_once(config=CFG,session={},http=h,now=NOW)['status']=='EXISTING_JOB'

def test_failed_recent_does_not_retry_storm():
 h=HTTP([run(NOW-timedelta(minutes=5),'completed','failure')])
 assert h.posts==0
 assert plan_from_runs(h.runs,now=NOW)['action']=='SKIP'

def test_check_failure_does_not_trigger_any_jobs():
 h=HTTP(get_status=403)
 assert start_once(config=CFG,session={},http=h,now=NOW)['status']=='CHECK_FAILED'
 assert h.posts==0

def test_dispatch_permission_error_exposes_nonsecret_status():
 h=HTTP(post_status=403)
 status=start_once(config=CFG,session={},http=h,now=NOW)
 assert status['status']=='DISPATCH_FAILED' and 'TEST_SECRET' not in str(status)
