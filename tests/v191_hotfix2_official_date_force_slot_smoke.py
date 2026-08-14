# -*- coding: utf-8 -*-
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import copy, json, sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
TZ=ZoneInfo('Asia/Taipei')

# 1) Sticky derived-date repair: old composite must not drag fresh raw domains backwards.
import official_factor_service as ofs
row={'股票代號':'2330','官方資料日期':'20260811','官方因子資料日期':'20260811','法人資料日期':'20260812','估值資料日期':'20260812'}
assert ofs._row_daily_factor_date_v184(row)=='20260812'
fixed=ofs._repair_derived_daily_dates_v191({'data_date':'20260811','meta':{'data_date':'20260811'},'records':[row]})
assert fixed['data_date']=='20260812',fixed
assert fixed['records'][0]['官方資料日期']=='20260812',fixed
assert ofs._factor_payload_business_date(fixed)=='20260812'

# 2) Official scheduler adapter: fetch/save success alone is insufficient; content date must pass timing governance.
import godpick_system_health_service as health
import godpick_auto_update_tasks as tasks
orig_load_sched=health.load_schedule_settings
orig_run=health.run_official_factor_update_once
orig_load_cache=ofs.load_factor_cache
orig_tasks_datetime=tasks.datetime
class _FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        fixed=datetime(2026,8,13,17,0,tzinfo=TZ)
        return fixed if tz is None else fixed.astimezone(tz)
tasks.datetime=_FixedDateTime
market_path=ROOT/'market_snapshot.json'
market_backup=market_path.read_bytes() if market_path.exists() else None
try:
    health.load_schedule_settings=lambda:{}
    health.run_official_factor_update_once=lambda *a,**kw:{'ok':True,'message':'fetch/save ok'}
    market_path.write_text(json.dumps({'market_date':'2026-08-13'},ensure_ascii=False),encoding='utf-8')
    ofs.load_factor_cache=lambda force_authority_restore=False:fixed
    ok=tasks.task_official_factors({})
    assert ok['ok'],ok
    assert '內容日期驗證通過' in ok['message'],ok
    stale={'records':[{'官方因子資料日期':'20260811'}],'data_date':'20260811'}
    ofs.load_factor_cache=lambda force_authority_restore=False:stale
    bad=tasks.task_official_factors({})
    assert not bad['ok'],bad
    assert '不得列為SUCCESS' in bad['message'],bad
finally:
    health.load_schedule_settings=orig_load_sched
    health.run_official_factor_update_once=orig_run
    ofs.load_factor_cache=orig_load_cache
    tasks.datetime=orig_tasks_datetime
    if market_backup is None:
        market_path.unlink(missing_ok=True)
    else:
        market_path.write_bytes(market_backup)

# 3) Feedback rebuild with insufficient model-ready samples is a WARNING, not FAILED.
import godpick_global_update_service as gus
orig_feedback=gus.step_feedback_profile; orig_learning=gus.step_learning_profile
try:
    gus.step_feedback_profile=lambda base:{'ok':True,'available':False,'warning':True,'message':'cache rebuilt; insufficient samples'}
    gus.step_learning_profile=lambda base:{'ok':True,'warning':False,'message':'learning rebuilt'}
    fb=tasks.task_feedback_learning({})
    assert fb['ok'] and fb['warning'],fb
    assert '有警示' in fb['message'],fb
finally:
    gus.step_feedback_profile=orig_feedback; gus.step_learning_profile=orig_learning

# 4) Force-all must never consume a future scheduled production slot.
import godpick_auto_scheduler as sched
cfg={
    'version':sched.VERSION,'enabled':True,'weekdays_only':False,'grace_minutes':35,'retry_count':0,'retry_delay_seconds':0,'history_keep':100,
    'jobs':{'official_factors':{'enabled':True,'times':['02:25','06:25','20:25'],'options':{},'require_dependencies':False}}
}
status={'version':sched.VERSION,'jobs':{},'completed_run_keys':[]}
history=[]
orig=(sched.load_settings,sched.load_status,sched.load_history,sched._execute_one,sched._acquire_lock,sched._release_lock,sched._checkpoint_runtime)
try:
    sched.load_settings=lambda:cfg
    sched.load_status=lambda:status
    sched.load_history=lambda:history
    calls=[]
    sched._execute_one=lambda job,jc,gc:(calls.append(job) or {'ok':True,'message':'ok','finished_at':'2026-08-13 10:00:01'})
    sched._acquire_lock=lambda *a,**k:(True,'')
    sched._release_lock=lambda:None
    sched._checkpoint_runtime=lambda *a,**k:None
    forced=sched.run_due_jobs(now=datetime(2026,8,13,10,0,tzinfo=TZ),force_all_enabled=True)
    assert forced['executed'] and forced['executed'][0]['run_key'].startswith('official_factors|FORCE|'),forced
    assert not any(k=='official_factors|2026-08-13 20:25' for k in status['completed_run_keys']),status
    scheduled=sched.run_due_jobs(now=datetime(2026,8,13,20,25,tzinfo=TZ))
    assert scheduled['executed'],scheduled
    assert scheduled['executed'][0]['run_key']=='official_factors|2026-08-13 20:25',scheduled
    assert calls==['official_factors','official_factors'],calls
finally:
    sched.load_settings,sched.load_status,sched.load_history,sched._execute_one,sched._acquire_lock,sched._release_lock,sched._checkpoint_runtime=orig

# 5) Degraded-but-complete jobs are WARNING, not fake SUCCESS or retry-loop FAILED.
status2={'version':sched.VERSION,'jobs':{},'completed_run_keys':[]}; history2=[]
orig=(sched.load_settings,sched.load_status,sched.load_history,sched._execute_one,sched._acquire_lock,sched._release_lock,sched._checkpoint_runtime)
try:
    cfg2=copy.deepcopy(cfg); cfg2['jobs']['official_factors']['times']=['10:30']
    sched.load_settings=lambda:cfg2; sched.load_status=lambda:status2; sched.load_history=lambda:history2
    sched._execute_one=lambda *a,**k:{'ok':True,'warning':True,'message':'remote hash pending','finished_at':'2026-08-13 10:30:01'}
    sched._acquire_lock=lambda *a,**k:(True,''); sched._release_lock=lambda:None; sched._checkpoint_runtime=lambda *a,**k:None
    res=sched.run_due_jobs(now=datetime(2026,8,13,10,30,tzinfo=TZ))
    assert res['executed'][0]['status']=='WARNING',res
    assert res['status']['last_summary']['warning']==1,res
    assert res['executed'][0]['run_key'] in status2['completed_run_keys'],status2
finally:
    sched.load_settings,sched.load_status,sched.load_history,sched._execute_one,sched._acquire_lock,sched._release_lock,sched._checkpoint_runtime=orig

print('PASS V191 Hotfix2 | raw official date repair | content freshness gate | FORCE slot isolation | WARNING semantics')
