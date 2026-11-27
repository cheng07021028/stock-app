# -*- coding: utf-8 -*-
"""Offline policy tests + actual scheduler integration tests when run in project.
Run after overlay installation; no real API, no original data writes.
"""
from __future__ import annotations
import copy
import importlib.util
import json
import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import godpick_h248_retry_policy as h
NOW=datetime(2026,11,27,11,0,tzinfo=h.TZ)
SLOT=NOW.replace(hour=10)
TAG=SLOT.strftime('%Y-%m-%d %H:%M')


def saved(code='TRANSIENT',attempts=1,at=None):
    return {'h248_scheduled':{'slot':TAG,'code':code,'attempts':attempts,'at':at or (NOW-timedelta(minutes=30)).isoformat()}}


def failed(msg,**kw):return {'ok':False,'message':msg,**kw}

def warning(msg,**kw):return {'ok':True,'warning':True,'message':msg,**kw}


def summary(success=1,warning=0,failed=0,blocked=0):
    return {'executed':success+warning+failed+blocked,'success':success,'warning':warning,'failed':failed,'blocked':blocked,'in_progress':False}


class Policy(unittest.TestCase):
    def test_01_success_not_retry(self):self.assertEqual(h.classify('stock_master',{'ok':True})['code'],'SUCCESS')
    def test_02_permission_over_timeout(self):self.assertEqual(h.classify('stock_master',failed('HTTP 403 timeout'))['code'],'PERMISSION')
    def test_03_code_error(self):self.assertEqual(h.classify('stock_master',failed('ModuleNotFoundError: No module named lxml'))['code'],'CODE_ENV')
    def test_04_id_integrity_priority(self):self.assertEqual(h.classify('godpick_recommendation',warning('timeout',details={'record_integrity_failure':True}))['code'],'AUTHORITY_CONFLICT')
    def test_05_h243_missing_identity(self):self.assertEqual(h.classify('recommend_list_hits',warning('57筆缺原始推薦日期／唯一ID，隔離待補證',retry_pending=True))['code'],'EVIDENCE_PENDING')
    def test_06_h138_eod_missing(self):self.assertEqual(h.classify('godpick_recommendation',warning('遠端封存尚未確認',details={'h138_auto_seal':{'status':'MARKET_DAY_UNVERIFIED','eod_status':'EOD_NOT_VERIFIED'}}))['code'],'EVIDENCE_PENDING')
    def test_07_h246_conflict(self):self.assertEqual(h.classify('godpick_recommendation',warning('timeout',details={'h246_consistency':{'status':'HISTORY_IDENTITY_CONFLICT','confirmed':False}}))['code'],'AUTHORITY_CONFLICT')
    def test_08_remote_uncertain_never_rescan(self):self.assertEqual(h.classify('godpick_recommendation',warning('H138主管決策遠端封存尚未確認'))['code'],'DURABILITY_ONLY')
    def test_09_explicit_continuation(self):self.assertEqual(h.classify('recommend_list_hits',warning('待續6筆',retry_pending=True))['code'],'CONTINUATION')
    def test_10_transient_refresh(self):self.assertEqual(h.classify('macro_full',failed('HTTP 503'))['code'],'TRANSIENT')
    def test_11_unknown_write(self):self.assertEqual(h.classify('godpick_recommendation',failed('timeout'))['code'],'UNKNOWN')
    def test_12_tracker_timeout_without_resume(self):self.assertEqual(h.classify('record_performance',failed('timeout'))['code'],'UNKNOWN')
    def test_13_dependency(self):self.assertEqual(h.classify('godpick_recommendation',failed('H28最終推薦閘門等待：official',blocked=True))['code'],'WAIT_DEPENDENCY')
    def test_14_new_slot(self):self.assertEqual(h.decision('stock_master',{},SLOT,NOW)['reason'],'NEW_SLOT')
    def test_15_cooldown(self):self.assertEqual(h.decision('stock_master',saved(at=(NOW-timedelta(minutes=10)).isoformat()),SLOT,NOW)['reason'],'COOLDOWN')
    def test_16_cooldown_boundary(self):self.assertTrue(h.decision('stock_master',saved(at=(NOW-timedelta(minutes=20)).isoformat()),SLOT,NOW)['retry'])
    def test_17_retry_limit(self):self.assertEqual(h.decision('stock_master',saved(attempts=3),SLOT,NOW)['reason'],'RETRY_LIMIT')
    def test_18_new_formal_slot_unaffected(self):self.assertTrue(h.decision('stock_master',saved(attempts=3),SLOT+timedelta(minutes=30),NOW)['allow'])
    def test_19_next_day_unaffected(self):self.assertTrue(h.decision('stock_master',saved(attempts=3),SLOT+timedelta(days=1),NOW+timedelta(days=1))['allow'])
    def test_20_invalid_clock(self):self.assertEqual(h.decision('stock_master',saved(at='invalid'),SLOT,NOW)['reason'],'RETRY_CLOCK_UNVERIFIED')
    def test_21_future_clock(self):self.assertFalse(h.decision('stock_master',saved(at=(NOW+timedelta(minutes=1)).isoformat()),SLOT,NOW)['allow'])
    def test_22_corrupt_attempt_count(self):self.assertFalse(h.decision('stock_master',saved(attempts=-1),SLOT,NOW)['allow'])
    def test_23_dependency_wait(self):self.assertFalse(h.decision('godpick_recommendation',saved('WAIT_DEPENDENCY',0),SLOT,NOW)['allow'])
    def test_24_dependency_pass(self):self.assertTrue(h.decision('godpick_recommendation',saved('WAIT_DEPENDENCY',0),SLOT,NOW,dependency_ready=True)['retry'])
    def test_25_no_data_retry(self):self.assertFalse(h.decision('recommend_list_hits',saved('EVIDENCE_PENDING'),SLOT,NOW)['allow'])
    def test_26_success_no_repeat(self):self.assertFalse(h.decision('stock_master',saved('SUCCESS'),SLOT,NOW)['allow'])
    def test_27_legacy_success_does_not_consume_force_slot(self):self.assertTrue(h.decision('stock_master',{'last_slot':TAG,'last_status':'SUCCESS'},SLOT,NOW)['allow'])
    def test_28_legacy_failed_cooldown(self):self.assertFalse(h.decision('stock_master',{'last_slot':TAG,'last_status':'FAILED','last_message':'HTTP 503','last_run_at':NOW.isoformat()},SLOT,NOW)['allow'])
    def test_29_record_counts_calls(self):
        js=saved(attempts=1);h.record_result('stock_master',js,failed('HTTP 503',attempt=1),{'status':'FAILED','finished_at':NOW.isoformat()},SLOT)
        self.assertEqual(js['h248_scheduled']['attempts'],2)
    def test_30_force_independent(self):
        js=saved(attempts=2);old=copy.deepcopy(js['h248_scheduled']);h.record_result('stock_master',js,{'ok':True},{'status':'SUCCESS','finished_at':NOW.isoformat()},SLOT,force=True)
        self.assertEqual(js['h248_scheduled'],old);self.assertTrue(js['h248_diagnostic']['force'])
    def test_31_blocked_not_counted(self):
        js={};h.record_result('godpick_recommendation',js,failed('前置尚未於今日成功',blocked=True),{'status':'BLOCKED','finished_at':NOW.isoformat()},SLOT)
        self.assertEqual(js['h248_scheduled']['attempts'],0)
    def test_32_simulation_no_mutation(self):
        js=saved();old=copy.deepcopy(js);h.record_result('stock_master',js,{'ok':True},{'status':'SUCCESS'},SLOT,simulate=True);self.assertEqual(js,old)
    def test_33_immediate_safe(self):self.assertTrue(h.immediate_retry_allowed('stock_master',failed('HTTP 503')))
    def test_34_immediate_hard_block(self):self.assertFalse(h.immediate_retry_allowed('stock_master',failed('HTTP 403')))
    def test_35_display_unknown(self):self.assertEqual(h.display('stock_master',{},None,NOW)['H248診斷'],'尚無執行紀錄')
    def test_36_display_due(self):self.assertTrue(h.display('stock_master',saved(),SLOT,NOW,due=True)['H248可選擇補跑'])
    def test_37_display_not_due(self):self.assertFalse(h.display('stock_master',saved(),SLOT,NOW,due=False)['H248可選擇補跑'])
    def test_38_receipt_success(self):
        r=h.retry_receipt('123.1',['stock_master'],[{'job':'stock_master','status':'SUCCESS'}],summary(),NOW.isoformat());self.assertTrue(h.verify_retry_receipt(r,'123','1')[0])
    def test_39_wrong_run(self):
        r=h.retry_receipt('123.1',['stock_master'],[{'job':'stock_master','status':'SUCCESS'}],summary(),NOW.isoformat());self.assertFalse(h.verify_retry_receipt(r,'124','1')[0]);self.assertFalse(h.verify_retry_receipt(r,'123','2')[0])
    def test_40_empty_receipt(self):
        r=h.retry_receipt('123.1',[],[],summary(0),NOW.isoformat());self.assertEqual(r['status'],'NO_ELIGIBLE_RETRY');self.assertFalse(h.verify_retry_receipt(r,'123','1')[0])
    def test_41_warning_receipt(self):
        r=h.retry_receipt('123.1',['stock_master'],[{'job':'stock_master','status':'WARNING'}],summary(0,1),NOW.isoformat());self.assertFalse(h.verify_retry_receipt(r,'123','1')[0])
    def test_42_missing_planned_job(self):
        r=h.retry_receipt('123.1',['stock_master','macro_full'],[{'job':'stock_master','status':'SUCCESS'}],summary(),NOW.isoformat());self.assertFalse(h.verify_retry_receipt(r,'123','1')[0])
    def test_43_duplicates(self):
        r=h.retry_receipt('123.1',['stock_master','stock_master'],[{'job':'stock_master','status':'SUCCESS'}]*2,summary(2),NOW.isoformat());self.assertFalse(r['all_success'])
    def test_44_tamper_receipt(self):
        r=h.retry_receipt('123.1',['stock_master'],[{'job':'stock_master','status':'SUCCESS'}],summary(),NOW.isoformat());r['summary']['success']=0;self.assertFalse(h.verify_retry_receipt(r,'123','1')[0])
    def test_45_json_roundtrip(self):self.assertTrue(h.decision('stock_master',json.loads(json.dumps(saved())),SLOT,NOW)['retry'])


@unittest.skipUnless((ROOT/'godpick_auto_scheduler.py').is_file(),'requires complete installed project; not a local production integration pass')
class ActualScheduler(unittest.TestCase):
    def setUp(self):
        import godpick_auto_scheduler as s
        self.s=s
        self.cfg=copy.deepcopy(s.DEFAULT_SETTINGS);self.cfg['enabled']=True;self.cfg['weekdays_only']=False
        for k,v in self.cfg['jobs'].items():v['enabled']=k in ('stock_master','macro_full');v['times']=['10:00']
        self.status={'jobs':{'stock_master':saved()},'completed_run_keys':[]}
    def run_mock(self,**kwargs):
        s=self.s
        with patch.multiple(s,load_settings=lambda:self.cfg,load_status=lambda:self.status,load_history=lambda:[],_acquire_lock=lambda:(True,''),_release_lock=lambda:None,_touch_lock=lambda *a:None,_checkpoint_runtime=lambda *a:None,_persist_runtime=lambda *a:(True,'mock')):
            with patch.object(s,'_execute_one',return_value={'ok':True,'message':'mock success','finished_at':NOW.isoformat(),'attempt':1}) as m:
                result=s.run_due_jobs(now=NOW,**kwargs)
                return result,[c.args[0] for c in m.call_args_list]
    def test_46_selected_not_all(self):
        r,calls=self.run_mock(retry_only=True);self.assertEqual(calls,['stock_master']);self.assertTrue(r['status']['last_h248_retry_batch']['all_success'])
    def test_47_empty_not_all(self):
        self.status={'jobs':{},'completed_run_keys':[]};r,calls=self.run_mock(retry_only=True);self.assertEqual(calls,[]);self.assertFalse(r['status']['last_h248_retry_batch']['all_success'])
    def test_48_routine_keeps_new_due(self):
        _,calls=self.run_mock();self.assertEqual(calls,['stock_master','macro_full'])
    def test_49_same_slot_limit(self):
        self.status['jobs']['stock_master']=saved(attempts=3);_,calls=self.run_mock();self.assertEqual(calls,['macro_full'])
    def test_50_invalid_force_combination(self):
        with self.assertRaises(ValueError):self.s.run_due_jobs(now=NOW,retry_only=True,force_all_enabled=True)
    def test_51_preview_matches_retry_candidates(self):
        rows=self.s.next_run_rows(self.cfg,self.status,NOW);self.assertEqual([r['工作ID'] for r in rows if r.get('H248可選擇補跑')],['stock_master'])

if __name__=='__main__':unittest.main(verbosity=2)
