# -*- coding: utf-8 -*-
import copy, importlib.util, json, os, sys, types, unittest
from datetime import datetime
from pathlib import Path
from threading import RLock
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import godpick_h250r7_tracking as r
from godpick_h250r2_nday import Calendar,TZ

DAYS=['2026-11-02','2026-11-03','2026-11-04','2026-11-05','2026-11-06','2026-11-09','2026-11-10','2026-11-11','2026-11-12','2026-11-13','2026-11-16','2026-11-17','2026-11-18','2026-11-19','2026-11-20','2026-11-23','2026-11-24','2026-11-25','2026-11-26','2026-11-27']
NOW=datetime(2026,11,28,12,tzinfo=TZ)

def row(i='A',**kw):
    return dict({'record_id':i,'股票代號':'2330','市場別':'上市','推薦日期':DAYS[0],'推薦價格':100,'推薦後1日%':99,'目前損益幅%':20,'最新價':119,'績效資料來源':'Yahoo adjusted OHLC','績效行情日期':DAYS[-1],'績效更新狀態':'OK','突破確認價':105,'停損價':98,'第一壓力價':110},**kw)

def archive(missing=None,invalid=None):
    cal=Calendar({'2026-11':set(DAYS)},{'2026-11':datetime(2026,11,27,19,tzinfo=TZ)},NOW)
    bars=[(d,100+i,103+i,98+i,100+i) for i,d in enumerate(DAYS) if d!=missing]
    if invalid:bars.append((invalid,100,90,110,100))
    return r.Archive(cal,{('TWSE','2330'):bars},{})

def delta(x,a,kind):return (r.current_delta,r.nday_delta,r.hits_delta)[kind](x,a,r.identity(x,a.cal.now))

class Tracking(unittest.TestCase):
    def test_current_real_raw_no_adjusted_overwrite(self):
        x=row();p,s,reuse=delta(x,archive(),0);self.assertEqual(p[r.RAW_NOW],19);self.assertEqual(s,'READY');self.assertTrue(reuse);self.assertNotIn('目前損益幅%',p)
    def test_current_stale_adjusted_pending(self):
        p,s,reuse=delta(row(績效行情日期=DAYS[2]),archive(),0);self.assertFalse(reuse);self.assertIn(r.RAW_PROOF,p)
    def test_current_no_reference_from_latest(self):
        x=row(推薦價格=None);p,s,_=delta(x,archive(missing=DAYS[0]),0);self.assertFalse(p);self.assertIn(s,('BASE_PENDING','QUOTE_PENDING'))
    def test_nday_exact_trade_sessions(self):
        p,s,_=delta(row(),archive(),1);self.assertEqual(p[r.N_COLS[0]],1);self.assertEqual(p[r.N_COLS[1]],3);self.assertEqual(p[r.N_COLS[3]],10);self.assertNotIn(r.N_COLS[-1],p);self.assertNotIn('推薦後1日%',p);self.assertEqual(s,'READY')
    def test_nday_partial(self):
        p,s,_=delta(row(),archive(missing=DAYS[5]),1);self.assertEqual(p[r.N_COLS[0]],1);self.assertNotIn(r.N_COLS[2],p);self.assertEqual(s,'QUOTE_PENDING')
    def test_nday_new_not_due(self):
        p,s,_=delta(row(推薦日期=DAYS[-1]),archive(),1);self.assertEqual((p,s),({},'NOT_DUE'))
    def test_nday_idempotent(self):
        a=archive();x=row();p,_,_=delta(x,a,1);self.assertEqual(delta(dict(x,**p),a,1)[0],{})
    def test_hits_from_following_session(self):
        p,s,_=delta(row(),archive(),2);self.assertEqual(p['突破價命中日期'],DAYS[2]);self.assertEqual(p['隔日最高漲幅%'],4);self.assertEqual(p['隔日最低回撤%'],-1);self.assertEqual(p['停損價觸發'],'否');self.assertEqual(s,'READY')
    def test_hits_gap_no_false_negative(self):
        p,s,_=delta(row(),archive(missing=DAYS[3]),2);self.assertEqual((p,s),({},'QUOTE_PENDING'))
    def test_hits_same_day_not_fill(self):
        p,_,_=delta(row(停損價=100),archive(),2);self.assertTrue(json.loads(p[r.HIT_PROOF])['same_day_ambiguous']);self.assertIn('非實際成交',p['作戰命中摘要'])
    def test_hits_unknown_level(self):
        p,_,_=delta(row(突破確認價=None),archive(),2);self.assertEqual(p['突破價命中'],'未設定')
    def test_hits_repeat_no_write(self):
        a=archive();x=row();p,_,_=delta(x,a,2);self.assertEqual(delta(dict(x,**p),a,2)[0],{})
    def test_invalid_duplicate_bar_rejected(self):
        a=archive(invalid=DAYS[-1]);self.assertNotIn(DAYS[-1],a.quotes[('TWSE','2330')]);self.assertEqual(delta(row(),a,0)[1],'QUOTE_PENDING')
    def test_invalid_id_no_patch(self):
        p,s=r.build_plan([row(''),row('B',推薦日期=''),row('C',市場別='')],archive(),r.TASKS[0],300);self.assertFalse(p);self.assertEqual(s['IDENTITY_PENDING'],3)
    def test_duplicate_id_no_patch(self):
        p,s=r.build_plan([row(),row()],archive(),r.TASKS[1],300);self.assertFalse(p);self.assertEqual(s['IDENTITY_PENDING'],2)
    def test_batch_no_first_n_starvation(self):
        records=[row('A',股票代號='9999'),row('B'),row('C')];p,s=r.build_plan(records,archive(),r.TASKS[1],1);self.assertEqual(list(p),['B']);self.assertEqual(s['DEFERRED'],1);self.assertEqual(s['QUOTE_PENDING'],1)
    def test_unknown_level_not_latest_price(self):
        self.assertIsNone(r.level(row(推薦價格=None,最新價=150),['推薦價格']))
    def test_300_row_continuation(self):
        a=archive();rows=[row(str(i)) for i in range(650)];p,s=r.build_plan(rows,a,r.TASKS[1],300);self.assertEqual(len(p),300);self.assertEqual(s['DEFERRED'],350);rows=[dict(x,**p.get(x['record_id'],{})) for x in rows];p,s=r.build_plan(rows,a,r.TASKS[1],300);self.assertEqual(len(p),300);self.assertEqual(s['DEFERRED'],50);self.assertEqual(next(iter(p)),'300')

class Authority(unittest.TestCase):
    def setUp(self):
        self.rows=[row('A'),row('B')];self.commit_calls=[];self.report=types.SimpleNamespace(local_ok=True,github_ok=True,permanent_ok=True,messages=lambda:['confirmed']);self.h=types.SimpleNamespace(ensure=lambda:(copy.deepcopy(self.rows),[],False),_base=lambda sig:(copy.deepcopy(self.rows),{'revision':1},'sha'),_commit=self.commit);self.p=types.SimpleNamespace(_RECORDS_LOCAL_LOCK=RLock(),_RECORDS_GITHUB_UPLOAD_LOCK=RLock(),records_authority_signature=lambda:'signature',records_authority_verification_h240=lambda:{'status':'VERIFIED'});self.tasks=types.SimpleNamespace(_report=lambda ok,message,**kw:dict(ok=ok,message=message,**kw),TASK_HANDLERS={**{k:lambda cfg:None for k in r.TASKS},'unrelated':lambda cfg:None})
    def commit(self,base,head,sha,final,removed,reason):
        self.commit_calls.append((base,head,sha,final,removed,reason));return self.report
    def run_task(self,kind,cfg=None):
        with patch.dict(sys.modules,{'godpick_h241_github_records':self.h,'godpick_persistence_service':self.p,'godpick_auto_update_tasks':self.tasks}),patch.object(r,'load_archive',return_value=archive()):return r.task(kind,cfg)
    def test_all_three_history_not_current_list(self):
        for k in r.TASKS:
            self.commit_calls.clear();out=self.run_task(k);self.assertTrue(out['ok']);self.assertEqual(out['details']['confirmed'],2);self.assertEqual(len(self.commit_calls),1);base,head,sha,final,removed,reason=self.commit_calls[0];self.assertEqual([x['record_id'] for x in final],['A','B']);self.assertEqual(removed,set());self.assertEqual([x['推薦後1日%'] for x in final],[99,99]);self.assertEqual([x['目前損益幅%'] for x in final],[20,20]);self.assertEqual(out['changed_files'],['godpick_records.json'])
    def test_remote_unconfirmed_is_fail(self):
        self.report.github_ok=False;out=self.run_task(r.TASKS[1]);self.assertFalse(out['ok']);self.assertEqual(out['details']['confirmed'],0);self.assertEqual(out['changed_files'],[])
    def test_unverified_authority_no_write(self):
        self.p.records_authority_verification_h240=lambda:{'status':'BLOCKED'};out=self.run_task(r.TASKS[0]);self.assertFalse(out['ok']);self.assertFalse(self.commit_calls)
    def test_conflict_no_write(self):
        self.h._base=lambda sig:([row('C')],{},'new');out=self.run_task(r.TASKS[1]);self.assertFalse(out['ok']);self.assertFalse(self.commit_calls)
    def test_unsafe_patch_denied(self):
        with self.assertRaisesRegex(ValueError,'ILLEGAL'):r.exact_commit(self.rows,{'A':{'股票代號':'9999'}},'signature',self.h,self.p)
        self.assertFalse(self.commit_calls)
    def test_unchanged_no_commit(self):
        p,_=r.build_plan(self.rows,archive(),r.TASKS[1],300);self.rows=[dict(x,**p[x['record_id']]) for x in self.rows];out=self.run_task(r.TASKS[1]);self.assertTrue(out['ok']);self.assertEqual(out['details']['outcome'],'NO_NEW_DUE');self.assertFalse(self.commit_calls)
    def test_deferred_warning(self):
        out=self.run_task(r.TASKS[1],{'max_rows':1});self.assertTrue(out['warning']);self.assertTrue(out['retry_pending']);self.assertEqual(out['details']['confirmed'],1);self.assertEqual(out['details']['outcome'],'PARTIAL_SAVED')
    def test_only_three_handlers(self):
        prior=self.tasks.TASK_HANDLERS['unrelated'];r.install_handlers(self.tasks);self.assertIs(self.tasks.TASK_HANDLERS['unrelated'],prior);self.assertTrue(all(self.tasks.TASK_HANDLERS[k].__name__=='h250r7_'+k for k in r.TASKS))
    def test_no_maturity_is_success_not_update(self):
        self.rows=[row(推薦日期=DAYS[-1])];out=self.run_task(r.TASKS[1]);self.assertTrue(out['ok']);self.assertFalse(out['warning']);self.assertEqual(out['details']['confirmed'],0);self.assertFalse(self.commit_calls)


class WorkerContract(unittest.TestCase):
    def load(self):
        path=Path(__file__).resolve().parents[1]/'tools/run_godpick_auto_scheduler_v191.py'
        spec=importlib.util.spec_from_file_location('_r7_test_worker',path);mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod);return mod
    def fake(self):
        calls=[]
        def run(**kw):calls.append(kw);return {'ok':True}
        tasks=types.SimpleNamespace(TASK_HANDLERS={**{k:lambda cfg:None for k in r.TASKS},'other':lambda cfg:None},_report=lambda ok,message,**kw:dict(ok=ok,message=message,**kw))
        sch=types.SimpleNamespace(JOB_LABELS={**dict.fromkeys(r.TASKS,'label'),'other':'label'},H155_REQUIRED_REFRESH_JOBS=[],H156_MANUAL_BATCH_MODES=set(),h156_plan_manual_batch=lambda *x:[],load_settings=lambda:{},run_due_jobs=run)
        pub=types.SimpleNamespace(ProgressPublisher=types.SimpleNamespace(worker=lambda labels:'R6-callback'))
        return calls,tasks,sch,pub
    def test_manual_worker_r6_callback(self):
        calls,tasks,sch,pub=self.fake();other=tasks.TASK_HANDLERS['other']
        with patch.dict(sys.modules,{'godpick_auto_scheduler':sch,'godpick_auto_update_tasks':tasks,'godpick_h250r6_monitor':pub}):
            m=self.load();m.run_worker_dispatch('force_enabled_jobs')
        self.assertEqual(calls[0]['progress_callback'],'R6-callback');self.assertTrue(calls[0]['strict_same_run']);self.assertIs(tasks.TASK_HANDLERS['other'],other);self.assertTrue(all(tasks.TASK_HANDLERS[k].__name__.startswith('h250r7_') for k in r.TASKS))
    def test_ordinary_worker_new_handler(self):
        calls,tasks,sch,pub=self.fake()
        with patch.dict(sys.modules,{'godpick_auto_scheduler':sch,'godpick_auto_update_tasks':tasks}):self.load().run_worker_dispatch('')
        self.assertEqual(calls,[{}]);self.assertTrue(all(tasks.TASK_HANDLERS[k].__name__.startswith('h250r7_') for k in r.TASKS))
    def test_import_failure_only_three_jobs(self):
        calls,tasks,sch,pub=self.fake();other=tasks.TASK_HANDLERS['other']
        with patch.dict(sys.modules,{'godpick_auto_scheduler':sch,'godpick_auto_update_tasks':tasks,'godpick_h250r7_tracking':None}):self.load().run_worker_dispatch('')
        self.assertEqual(calls,[{}]);self.assertIs(tasks.TASK_HANDLERS['other'],other);self.assertTrue(all(tasks.TASK_HANDLERS[k]()['ok'] is False for k in r.TASKS))

if __name__=='__main__':unittest.main()
