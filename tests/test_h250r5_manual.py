# -*- coding: utf-8 -*-
import copy, hashlib, json, os, sys, tempfile, unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from godpick_h250r5_manual import MODE,REC,plan_enabled,validation,verify_receipt,verify_restored_settings,active_manual

LABELS={'stock_master':'主檔','macro_full':'大盤',REC:'推薦','durability_retry':'同步'}

def config(enabled=None):
    keys=list(LABELS) if enabled is None else enabled
    return {'enabled':False,'weekdays_only':True,'execution_order':[REC,'durability_retry','macro_full','stock_master'],'jobs':{k:{'enabled':k in keys} for k in LABELS}}

def receipt(plan=None):
    plan=plan or ['macro_full',REC]; ident='123.2';rows=[{'job':k,'status':'SUCCESS','run_key':k+'|FORCE|2026-11-28 18:10|'+ident,'batch_id':ident} for k in plan]
    summary={'executed':len(plan),'success':len(plan),'warning':0,'failed':0,'blocked':0,'in_progress':False}
    return {'last_wakeup_run_id':'123','last_wakeup_attempt':'2','last_wakeup_result':'COMPLETED','last_summary':summary,'last_manual_batch':{'batch_mode':MODE,'batch_id':ident,'planned_jobs':plan,'summary':summary,'validation':validation(rows,plan)}},{'records':rows}

class Scope(unittest.TestCase):
    def test_exact_enabled_ignores_master(self):self.assertEqual(plan_enabled(config(['macro_full',REC]),LABELS),['macro_full',REC])
    def test_all_in_saved_order(self):self.assertEqual(plan_enabled(config(),LABELS),['durability_retry','macro_full','stock_master',REC])
    def test_rec_disabled(self):self.assertEqual(plan_enabled(config(['stock_master']),LABELS),['stock_master'])
    def test_empty(self):self.assertEqual(plan_enabled(config([]),LABELS),[])
    def test_no_config(self):self.assertRaises(ValueError,plan_enabled,{},LABELS)
    def test_missing_job(self):
        c=config();c['jobs'].pop('macro_full');self.assertRaises(ValueError,plan_enabled,c,LABELS)
    def test_not_boolean(self):
        c=config();c['jobs']['macro_full']['enabled']='false';self.assertRaises(ValueError,plan_enabled,c,LABELS)
    def test_raw_disabled_conflict(self):self.assertRaises(ValueError,plan_enabled,config(),LABELS,raw=config([REC]))
    def test_raw_incomplete(self):self.assertRaises(ValueError,plan_enabled,config(),LABELS,raw={})
    def test_r1_authority_unavailable(self):
        c=config();c['_h250r1_authority']='AUTHORITY_UNAVAILABLE';self.assertRaises(ValueError,plan_enabled,c,LABELS)
    def test_no_config_mutation(self):
        c=config();before=copy.deepcopy(c);plan_enabled(c,LABELS,raw=before);self.assertEqual(c,before)
    def test_duplicate_order_dedup(self):
        c=config();c['execution_order']=[REC,REC,'macro_full','macro_full','invalid'];self.assertEqual(plan_enabled(c,LABELS),['macro_full','stock_master','durability_retry',REC])
    def test_restored_pair(self):
        c=config();digest=hashlib.sha256(json.dumps(c,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode()).hexdigest();d={'data/config/godpick_auto_scheduler_settings.json':c,'godpick_auto_scheduler_settings_sync_state.json':{'path':'data/config/godpick_auto_scheduler_settings.json','payload_hash':digest}}
        self.assertTrue(verify_restored_settings(c,None,git=d.__getitem__))
        d['godpick_auto_scheduler_settings_sync_state.json']['payload_hash']='invalid';self.assertRaises(ValueError,verify_restored_settings,c,None,git=d.__getitem__)

class Verdict(unittest.TestCase):
    def test_all_verified(self):
        s,h=receipt();self.assertTrue(verify_receipt(s,'123','2',history=h)[0])
    def test_zero_not_success(self):self.assertFalse(validation([],[])['all_success'])
    def test_disabled_rec_not_needed(self):
        s,h=receipt(['macro_full']);self.assertTrue(verify_receipt(s,'123','2',history=h)[0]);self.assertEqual(s['last_manual_batch']['validation']['recommendation_status'],'NOT_SELECTED')
    def test_wrong_attempt(self):
        s,h=receipt();self.assertFalse(verify_receipt(s,'123','1',history=h)[0])
    def test_wrong_id(self):
        s,h=receipt();self.assertFalse(verify_receipt(s,'124','2',history=h)[0])
    def test_wrong_mode(self):
        s,h=receipt();s['last_manual_batch']['batch_mode']='full_refresh_and_recommend';self.assertFalse(verify_receipt(s,'123','2',history=h)[0])
    def test_missing_history(self):
        s,h=receipt();self.assertFalse(verify_receipt(s,'123','2',history={})[0])
    def test_duplicate_history(self):
        s,h=receipt();h['records'].append(h['records'][0]);self.assertFalse(verify_receipt(s,'123','2',history=h)[0])
    def test_warning(self):
        s,h=receipt();s['last_summary']['warning']=1;self.assertFalse(verify_receipt(s,'123','2',history=h)[0])
    def test_failed_row(self):
        s,h=receipt();s['last_manual_batch']['validation']['job_results'][0]['status']='FAILED';self.assertFalse(verify_receipt(s,'123','2',history=h)[0])
    def test_missing_row(self):
        s,h=receipt();s['last_manual_batch']['validation']['job_results'].pop();self.assertFalse(verify_receipt(s,'123','2',history=h)[0])
    def test_force_key_scope(self):
        s,h=receipt();s['last_manual_batch']['validation']['job_results'][0]['run_key']='macro_full|2026-11-28 18:10';self.assertFalse(verify_receipt(s,'123','2',history=h)[0])
    def test_wrong_order(self):
        s,h=receipt();s['last_manual_batch']['planned_jobs'].reverse();self.assertFalse(verify_receipt(s,'123','2',history=h)[0])
    def test_partial_history(self):
        s,h=receipt();h['records'].pop();self.assertFalse(verify_receipt(s,'123','2',history=h)[0])
    def test_in_progress(self):
        s,h=receipt();s['last_summary']['in_progress']=True;self.assertFalse(verify_receipt(s,'123','2',history=h)[0])

class Resp:
    def __init__(self,code,rows=None):self.status_code=code;self.rows=rows
    def json(self):return {'workflow_runs':self.rows}
class Dispatch(unittest.TestCase):
    def get(self,resp):return active_manual(token='secret',owner='owner',repo='repo',http_get=lambda *a,**k:resp)[0]
    def test_active(self):self.assertEqual(self.get(Resp(200,[{'id':123,'status':'queued','display_title':'Central | force_enabled_jobs | x'}])),'123')
    def test_routine_not_active(self):self.assertEqual(self.get(Resp(200,[{'id':123,'status':'queued','display_title':'Central | due_only | schedule'}])),'')
    def test_completed_not_active(self):self.assertEqual(self.get(Resp(200,[{'id':123,'status':'completed','display_title':'Central | force_enabled_jobs | x'}])),'')
    def test_http_block(self):self.assertIsNone(self.get(Resp(403)))
    def test_bad_schema(self):self.assertIsNone(self.get(Resp(200,None)))
    def test_token_missing(self):self.assertIsNone(active_manual(token='',owner='owner',repo='repo')[0])

@unittest.skipUnless(os.getenv('R5_REQUIRE_ACTUAL')=='1','完整專案未提供；部署準備工作必須執行')
class ActualScheduler(unittest.TestCase):
    def run_actual(self, enabled, *, fail=None):
        import godpick_auto_scheduler as s
        cfg=copy.deepcopy(s.DEFAULT_SETTINGS);cfg['enabled']=False;cfg['weekdays_only']=True
        for k,v in cfg['jobs'].items():v['enabled']=k in enabled
        raw=copy.deepcopy(cfg);original={'jobs':{'macro_full':{'last_status':'SUCCESS','last_slot':'2026-11-27 20:40'}},'completed_run_keys':['macro_full|2026-11-27 20:40']};state=copy.deepcopy(original);calls=[]
        def execute(k,*a):
            calls.append(k);return {'ok':k!=fail,'message':'fixture','finished_at':'2026-11-28 18:10:10'}
        with patch.dict(os.environ,{'GITHUB_RUN_ID':'123','GITHUB_RUN_ATTEMPT':'2','GITHUB_ACTIONS':'false'},clear=False),patch.multiple(s,load_settings=lambda:copy.deepcopy(cfg),load_status=lambda:state,load_history=lambda:[],_read_local=lambda *a:raw,_acquire_lock=lambda:(True,''),_touch_lock=lambda *a:None,_release_lock=lambda:None,_checkpoint_runtime=lambda *a:None,_persist_runtime=lambda *a:None,_execute_one=execute):
            res=s.run_due_jobs(now=datetime(2026,11,28,18,10,tzinfo=ZoneInfo('Asia/Taipei')),force_all_enabled=True,strict_same_run=True,manual_batch_mode=MODE)
        self.assertEqual(state['jobs'],original['jobs']);self.assertEqual(state['completed_run_keys'],original['completed_run_keys']);return res,calls,state
    def test_weekend_master_off_enabled_only(self):
        res,calls,state=self.run_actual(['macro_full','stock_master']);self.assertEqual(calls,['stock_master','macro_full']);self.assertTrue(res['ok']);self.assertTrue(verify_receipt(state,'123','2')[0])
    def test_disabled_rec_not_executed(self):
        res,calls,state=self.run_actual(['stock_master']);self.assertNotIn(REC,calls);self.assertTrue(res['ok'])
    def test_rec_blocked_missing_core(self):
        res,calls,state=self.run_actual(['stock_master',REC]);self.assertNotIn(REC,calls);self.assertFalse(res['ok']);self.assertEqual(res['executed'][-1]['status'],'BLOCKED')
    def test_rec_last_and_all_fourteen(self):
        import godpick_auto_scheduler as s
        res,calls,state=self.run_actual(list(s.JOB_LABELS));self.assertEqual(len(calls),14);self.assertEqual(calls[-1],REC);self.assertTrue(res['ok'])
    def test_no_jobs_no_call(self):
        res,calls,state=self.run_actual([]);self.assertEqual(calls,[]);self.assertFalse(res['ok'])
    def test_failure_not_green(self):
        res,calls,state=self.run_actual(['stock_master','macro_full'],fail='macro_full');self.assertFalse(res['ok']);self.assertFalse(verify_receipt(state,'123','2')[0])

if __name__=='__main__':unittest.main()
