import copy, unittest, json, base64
from godpick_h250r6_monitor import *
from godpick_h250r5_manual import plan_enabled, validation
L={'a':'A','b':'B','godpick_recommendation':'07'}
P=list(L)
class Store:
    def __init__(self):self.writes=[];self.fail=False
    def write(self,path,doc,sha):
        if self.fail:raise RuntimeError('secret')
        self.writes.append((path,copy.deepcopy(doc),sha));return 'a'*40
class Test(unittest.TestCase):
    def pub(self):
        s=Store();p=ProgressPublisher(s,'123','1',L);p({'event':'run_start','pending_jobs':P});return s,p
    def snap(self,**kw):return {'available':True,'run_id':'123','attempt':'1','status':'in_progress',**kw}
    def batch(self):
        rows=[{'job':k,'status':'SUCCESS','run_key':k+'|FORCE|2026-11-28 12:00|123.1','message':'ok'} for k in P]
        return {'batch_id':'123.1','batch_mode':MODE,'planned_jobs':P,'validation':validation(rows,P),'summary':{'executed':3,'success':3,'warning':0,'failed':0,'blocked':0,'in_progress':False}}
    def test_path(self):self.assertEqual(path_for('123','2'),'data/h250r6/progress/123.2.json')
    def test_invalid_path(self):
        for x in ('../1','0','',True):
            with self.assertRaises(ValueError):path_for(x,'1')
    def test_publish(self):
        s,p=self.pub();p({'event':'job_start','job':'a','started_at':'t'});p({'event':'job_complete','job':'a','status':'WARNING','message':'SECRET','duration_seconds':3});p({'event':'run_complete'});self.assertEqual(len(s.writes),4);self.assertEqual(s.writes[-1][1]['rows']['a']['status'],'WARNING');self.assertNotIn('SECRET',str(s.writes));self.assertEqual(s.writes[-1][2],'a'*40)
    def test_fail_open_monitor_only(self):
        s,p=self.pub();s.fail=True;p({'event':'job_start','job':'a'});self.assertFalse(p.enabled);p({'event':'job_complete','job':'a','status':'SUCCESS'});self.assertEqual(len(s.writes),1)
    def test_unknown_job(self):
        s,p=self.pub();p({'event':'job_start','job':'x'});self.assertFalse(p.enabled);self.assertEqual(len(s.writes),1)
    def test_simulation(self):
        s=Store();p=ProgressPublisher(s,'123','1',L);p({'event':'run_start','pending_jobs':P,'simulate':True});self.assertFalse(s.writes)
    def test_queued(self):
        m=model(self.snap(status='pending'),{},P,L);self.assertEqual(m['finished'],0);self.assertEqual(len(m['plan']),3);self.assertEqual(m['origin'],'設定預覽');self.assertFalse(m['all_ok'])
    def test_other_batch(self):
        m=model(self.snap(),{'last_force_enabled_batch':{**self.batch(),'batch_id':'999.1'}},P,L);self.assertFalse(m['batch']);self.assertEqual(m['finished'],0)
    def test_other_attempt(self):
        s,p=self.pub();m=model(self.snap(attempt='2',progress=p.doc),{},P,L);self.assertEqual(m['origin'],'設定預覽')
    def test_progress(self):
        s,p=self.pub();p({'event':'job_start','job':'a'});p({'event':'job_complete','job':'a','status':'FAILED'});p({'event':'job_start','job':'b'});m=model(self.snap(progress=p.doc),{},P,L);self.assertEqual(m['finished'],1);self.assertEqual(m['counts']['FAILED'],1);self.assertFalse(m['all_ok'])
    def test_terminal(self):
        m=model(self.snap(status='completed',conclusion='success'),{'last_force_enabled_batch':self.batch()},P,L);self.assertTrue(m['all_ok'])
    def test_no_receipt(self):self.assertFalse(model(self.snap(status='completed',conclusion='success'),{},P,L)['all_ok'])
    def test_cancel(self):self.assertFalse(model(self.snap(status='completed',conclusion='cancelled'),{'last_force_enabled_batch':self.batch()},P,L)['all_ok'])
    def test_wrong_run_key(self):
        b=self.batch();b['validation']['job_results'][0]['run_key']='a|FORCE|x|456.1';self.assertFalse(model(self.snap(status='completed',conclusion='success'),{'last_force_enabled_batch':b},P,L)['all_ok'])
    def test_summary_wrong(self):
        b=self.batch();b['summary']['warning']=1;self.assertFalse(model(self.snap(status='completed',conclusion='success'),{'last_force_enabled_batch':b},P,L)['all_ok'])
    def test_bad_plan(self):
        for p in [[],['a','a'],['x'],['godpick_recommendation','a']]:self.assertFalse(valid_plan(p,L))
    def test_id(self):
        r={'id':123,'path':WORKFLOW,'event':'workflow_dispatch','display_title':'Central | force_enabled_jobs | x','run_attempt':1};self.assertTrue(matches_run(r,'123'));self.assertFalse(matches_run(r,'124'));self.assertFalse(matches_run({**r,'event':'schedule'},'123'))
    def test_settings(self):self.assertEqual(plan_enabled({'jobs':{k:{'enabled':k!='b'} for k in P},'execution_order':P},L),['a','godpick_recommendation'])
    def test_snapshot_queue(self):
        class A:
            def get(self,path,**kw):return {'id':123,'path':WORKFLOW,'event':'workflow_dispatch','display_title':'Central | force_enabled_jobs | x','run_attempt':1,'status':'pending'},''
        self.assertEqual(snapshot(A(),'123')['status'],'pending')
    def test_snapshot_running(self):
        class A:
            def get(self,path,**kw):
                if path.endswith('/jobs'):return {'jobs':[{'steps':[{'status':'in_progress','name':'Install dependencies'}]}]},''
                return {'id':123,'path':WORKFLOW,'event':'workflow_dispatch','display_title':'Central | force_enabled_jobs | x','run_attempt':1,'status':'in_progress'},''
            def read_json(self,path):return None,'HTTP 404'
        self.assertEqual(snapshot(A(),'123')['phase'],'Install dependencies')
    def test_missing_authority(self):self.assertFalse(model({'available':False},{'last_force_enabled_batch':self.batch()},P,L)['all_ok'])
if __name__=='__main__':unittest.main()

class TransportTest(unittest.TestCase):
    def test_sha_and_branch(self):
        class H:
            def __init__(self):self.b=[]
            def put(self,url,**kw):
                self.b.append((url,kw));return type('R',(),{'status_code':201,'json':lambda self:{'content':{'sha':'c'*40}}})()
        h=H();a=API('token','owner','repo',h);p=ProgressPublisher(a,'123','1',L);p({'event':'run_start','pending_jobs':P});p({'event':'job_start','job':'a'});self.assertNotIn('sha',h.b[0][1]['json']);self.assertEqual(h.b[1][1]['json']['sha'],'c'*40);self.assertEqual(h.b[1][1]['json']['branch'],'runtime-data');self.assertNotIn('token',h.b[1][0]);self.assertNotIn('token',json.dumps(h.b[1][1]['json']))
    def test_real_worker_entry_with_mock_scheduler(self):
        import sys, types, runpy, os
        from pathlib import Path
        from unittest.mock import patch
        m=types.ModuleType('godpick_auto_scheduler');m.JOB_LABELS=L;m.H155_REQUIRED_REFRESH_JOBS=();m.H156_MANUAL_BATCH_MODES=('full_refresh_and_recommend','market_refresh_and_recommend');m.h156_plan_manual_batch=lambda *a:P;m.load_settings=lambda:{};calls=[];m.run_due_jobs=lambda **kw:calls.append(kw) or {'ok':True}
        with patch.dict(sys.modules,{'godpick_auto_scheduler':m}),patch.dict(os.environ,{'GITHUB_ACTIONS':'false'}):
            mod=runpy.run_path(str(Path(__file__).resolve().parents[1]/'tools/run_godpick_auto_scheduler_v191.py'));f=mod['run_worker_dispatch'];f(MODE);f('');f('a')
        self.assertIn('progress_callback',calls[0]);self.assertEqual(calls[0]['manual_batch_mode'],MODE);self.assertEqual(calls[1],{});self.assertEqual(calls[2]['selected_jobs'],['a']);self.assertNotIn('progress_callback',calls[2])
