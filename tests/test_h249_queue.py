# -*- coding: utf-8 -*-
"""H249 offline GitHub Contents CAS contract tests. No production access."""
import base64, copy, hashlib, json, os, sys, tempfile, unittest
from datetime import datetime,timedelta
from pathlib import Path
from unittest.mock import patch
from urllib.parse import unquote
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
import godpick_h249_queue as h
NOW=datetime(2026,11,27,20,0,tzinfo=h.TZ)
IDENTITY='123.1'
class Response:
    def __init__(self,code,body=None):self.status_code=code;self.body=body
    def json(self):return copy.deepcopy(self.body)
class MemoryHTTP:
    def __init__(self):self.files={};self.completed=set();self.puts=0;self.conflicts=0;self.deny=False;self.bad_read=False;self.timeout=False
    def get(self,url,**kw):
        if self.deny:return Response(403)
        if '/git/ref/heads/' in url:return Response(200,{})
        if '/actions/runs/' in url:
            rid,_,at=url.split('/actions/runs/')[1].split('/')
            return Response(200,{'id':int(rid),'run_attempt':int(at),'status':'completed' if rid+'.'+at in self.completed else 'in_progress'})
        path=unquote(url.split('/contents/')[1]);v=self.files.get(path)
        if not v:return Response(404)
        return Response(200,{'sha':v[0],'content':'invalid!' if self.bad_read else base64.b64encode(v[1]).decode()})
    def put(self,url,**kw):
        self.puts+=1
        if self.deny:return Response(403)
        if self.conflicts:self.conflicts-=1;return Response(409)
        p=unquote(url.split('/contents/')[1]);body=kw['json'];old=self.files.get(p)
        if (old and body.get('sha')!=old[0]) or (not old and body.get('sha')):return Response(409)
        raw=base64.b64decode(body['content']);sha=hashlib.sha1(b'blob '+str(len(raw)).encode()+b'\0'+raw).hexdigest();self.files[p]=(sha,raw)
        if self.timeout:raise TimeoutError()
        return Response(200 if old else 201)

class Contract(unittest.TestCase):
    def setUp(self):self.http=MemoryHTTP();self.s=h.Store('fake-token','owner','repo',http=self.http)
    def enqueue(self,mode='stock_master',now=NOW):return h.enqueue(self.s,mode,now)[0]
    def read(self,r):return self.s.read(h.reqpath(r['id']))[0]['requests'][r['id']]
    def claim(self,r,identity=IDENTITY,now=NOW):return h.claim(self.s,r['id'],r['mode'],identity,now)
    def returned(self,r):
        self.claim(r);h.note_job(self.s,r['id'],IDENTITY,r['mode'],'STARTED');h.note_job(self.s,r['id'],IDENTITY,r['mode'],'RETURNED')
    def res(self,r,status='SUCCESS'):
        return {'ok':status=='SUCCESS','batch_id':IDENTITY,'executed':[{'job':r['mode'],'status':status,'run_key':'x'}],'status':{}}
    def test_enqueue_and_readback(self):
        r=self.enqueue();self.assertEqual(self.read(r)['state'],'QUEUED');self.assertEqual(self.http.puts,1)
    def test_same_mode_dedup(self):
        r=self.enqueue();v,new=h.enqueue(self.s,'stock_master',NOW);self.assertFalse(new);self.assertEqual(v['id'],r['id']);self.assertEqual(self.http.puts,1)
    def test_cross_midnight_dedup(self):
        r=self.enqueue(now=NOW.replace(hour=23));v,new=h.enqueue(self.s,'stock_master',NOW+timedelta(hours=5));self.assertFalse(new);self.assertEqual(v['id'],r['id'])
    def test_other_mode_independent(self):self.assertNotEqual(self.enqueue()['id'],self.enqueue('macro_full')['id'])
    def test_unknown_mode(self):
        with self.assertRaises(h.GuardError):self.enqueue('bogus')
    def test_cas_retries_bounded(self):
        self.http.conflicts=2;self.enqueue();self.assertEqual(self.http.puts,3)
        self.http.conflicts=10
        with self.assertRaises(h.GuardError):self.enqueue('macro_full')
    def test_ambiguous_put_still_dedup(self):
        self.http.timeout=True
        with self.assertRaises(h.GuardError):self.enqueue()
        self.http.timeout=False;r,new=h.enqueue(self.s,'stock_master',NOW);self.assertFalse(new);self.assertEqual(r['state'],'QUEUED')
    def test_claim_identity(self):
        r=self.enqueue();self.assertEqual(self.claim(r)['owner'],IDENTITY);self.assertIsNone(self.claim(r,'124.1'))
    def test_mismatched_mode(self):
        r=self.enqueue()
        with self.assertRaises(h.GuardError):h.claim(self.s,r['id'],'macro_full',IDENTITY,NOW)
    def test_recover_unstarted_cancelled(self):
        r=self.enqueue();self.claim(r);self.http.completed.add(IDENTITY);v=self.claim(r,'124.1');self.assertEqual(v['claims'],2);self.assertEqual(v['owner'],'124.1')
    def test_started_cancelled_isolated(self):
        r=self.enqueue();self.claim(r);h.note_job(self.s,r['id'],IDENTITY,r['mode'],'STARTED');self.http.completed.add(IDENTITY);self.assertIsNone(self.claim(r,'124.1'));self.assertEqual(self.read(r)['state'],'REVIEW_REQUIRED')
    def test_returned_cancelled_isolated(self):
        r=self.enqueue();self.returned(r);self.http.completed.add(IDENTITY);self.assertIsNone(self.claim(r,'124.1'));self.assertEqual(self.read(r)['state'],'REVIEW_REQUIRED')
    def test_expired(self):
        r=self.enqueue();self.assertIsNone(self.claim(r,now=NOW+timedelta(hours=24)));self.assertEqual(self.read(r)['state'],'EXPIRED')
    def test_limit_claims(self):
        r=self.enqueue();self.claim(r)
        for prev,cur in [('123.1','124.1'),('124.1','125.1')]:self.http.completed.add(prev);self.assertIsNotNone(self.claim(r,cur))
        self.http.completed.add('125.1');self.assertIsNone(self.claim(r,'126.1'));self.assertEqual(self.read(r)['state'],'EXPIRED')
    def test_duplicate_job(self):
        r=self.enqueue();self.claim(r);h.note_job(self.s,r['id'],IDENTITY,'stock_master','STARTED')
        with self.assertRaises(h.GuardError):h.note_job(self.s,r['id'],IDENTITY,'stock_master','STARTED')
    def test_owner_mismatch(self):
        r=self.enqueue();self.claim(r)
        with self.assertRaises(h.GuardError):h.note_job(self.s,r['id'],'124.1','stock_master','STARTED')
    def test_execution_and_finalize(self):
        r=self.enqueue();self.returned(r);h.record_execution(self.s,r['id'],IDENTITY,self.res(r));self.assertTrue(self.read(r)['execution_ok']);self.assertEqual(h.finalize(self.s,r['id'],IDENTITY,True),'VERIFIED')
    def test_warning_not_success(self):
        r=self.enqueue();self.returned(r);h.record_execution(self.s,r['id'],IDENTITY,self.res(r,'WARNING'));self.assertEqual(h.finalize(self.s,r['id'],IDENTITY,True),'INCOMPLETE')
    def test_zero_not_success(self):
        r=self.enqueue();self.claim(r);h.record_execution(self.s,r['id'],IDENTITY,{'ok':True,'batch_id':IDENTITY,'executed':[]});self.assertEqual(h.finalize(self.s,r['id'],IDENTITY,True),'INCOMPLETE')
    def test_remote_unconfirmed(self):
        r=self.enqueue();self.returned(r);h.record_execution(self.s,r['id'],IDENTITY,self.res(r));self.assertEqual(h.finalize(self.s,r['id'],IDENTITY,False),'INCOMPLETE')
    def test_execution_missing_verifier(self):
        r=self.enqueue();self.returned(r);h.record_execution(self.s,r['id'],IDENTITY,self.res(r));self.http.completed.add(IDENTITY);self.assertIsNone(self.claim(r,'124.1'));self.assertEqual(self.read(r)['state'],'REVIEW_REQUIRED')
    def test_wrong_run(self):
        r=self.enqueue();self.returned(r);res=self.res(r);res['batch_id']='124.1';h.record_execution(self.s,r['id'],IDENTITY,res);self.assertFalse(self.read(r)['execution_ok'])
    def test_remote_exact_history(self):
        r=self.enqueue();self.returned(r);h.record_execution(self.s,r['id'],IDENTITY,self.res(r));status={'last_wakeup_run_id':'123','last_wakeup_attempt':'1','last_wakeup_result':'COMPLETED','last_summary':{'executed':1,'success':1,'warning':0,'failed':0,'blocked':0,'in_progress':False}};hist={'records':[{'batch_id':IDENTITY,'job':'stock_master','status':'SUCCESS','run_key':'x'}]}
        def read(p,d):return (status if 'status' in p else hist),'ok'
        self.assertTrue(h.verify_remote_execution(self.s,r['id'],IDENTITY,read));hist['records'][0]['batch_id']='123.2';self.assertFalse(h.verify_remote_execution(self.s,r['id'],IDENTITY,read));hist['records'][0]['batch_id']=IDENTITY;status['last_wakeup_attempt']='2';self.assertFalse(h.verify_remote_execution(self.s,r['id'],IDENTITY,read))
    def test_orphan_readonly_recovery(self):
        r=self.enqueue();self.returned(r);h.record_execution(self.s,r['id'],IDENTITY,self.res(r));self.http.completed.add(IDENTITY)
        status={'last_wakeup_run_id':'123','last_wakeup_attempt':'1','last_wakeup_result':'COMPLETED','last_summary':{'executed':1,'success':1,'warning':0,'failed':0,'blocked':0,'in_progress':False}}
        hist={'records':[{'batch_id':IDENTITY,'job':'stock_master','status':'SUCCESS','run_key':'x'}]}
        def read(p,d):return (status if 'status' in p else hist),'ok'
        self.assertEqual(h.reconcile_orphans(self.s,read,NOW),[r['id']]);self.assertEqual(self.read(r)['state'],'VERIFIED')
    def test_orphan_no_substitute_from_newer_batch(self):
        r=self.enqueue();self.returned(r);h.record_execution(self.s,r['id'],IDENTITY,self.res(r));self.http.completed.add(IDENTITY)
        self.assertEqual(h.reconcile_orphans(self.s,lambda p,d:({'last_wakeup_run_id':'124'},'ok'),NOW),[]);self.assertEqual(self.read(r)['state'],'EXECUTION_RECORDED')
    def test_full_scope_not_partial(self):
        rows=[{'job':'stock_master','status':'SUCCESS','run_key':'x'}];self.assertFalse(h.scope_verified('full_refresh_and_recommend',IDENTITY,rows,{}))
    def test_corrupt_remote_rejected(self):
        self.enqueue();self.http.bad_read=True
        with self.assertRaises(h.GuardError):self.enqueue()
    def test_permission(self):
        self.http.deny=True
        with self.assertRaises(h.GuardError):self.enqueue()
    def test_journal_start_and_ambiguous_block(self):
        p=h.journal_path('stock_master',NOW,False,IDENTITY);h.journal_begin(self.s,p,IDENTITY,None)
        with self.assertRaises(h.GuardError):h.journal_begin(self.s,p,'124.1',None)
    def test_journal_success_no_repeat(self):
        p=h.journal_path('stock_master',NOW,False,IDENTITY);h.journal_begin(self.s,p,IDENTITY,None);h.journal_finish(self.s,p,IDENTITY,{'ok':True})
        with self.assertRaises(h.GuardError):h.journal_begin(self.s,p,'124.1',{'batch_id':IDENTITY,'attempts':1,'code':'TRANSIENT'})
    def test_journal_failed_without_checkpoint(self):
        p=h.journal_path('stock_master',NOW,False,IDENTITY);h.journal_begin(self.s,p,IDENTITY,None);h.journal_finish(self.s,p,IDENTITY,{'ok':False})
        with self.assertRaises(h.GuardError):h.journal_begin(self.s,p,'124.1',None)
    def test_journal_retry_cooldown_and_limit(self):
        p=h.journal_path('stock_master',NOW,False,IDENTITY)
        with patch.object(h,'clock',return_value=NOW):h.journal_begin(self.s,p,IDENTITY,None);h.journal_finish(self.s,p,IDENTITY,{'ok':False})
        prior={'batch_id':IDENTITY,'attempts':1,'code':'TRANSIENT'}
        with patch.object(h,'clock',return_value=NOW+timedelta(minutes=19)):
            with self.assertRaises(h.GuardError):h.journal_begin(self.s,p,'124.1',prior)
        with patch.object(h,'clock',return_value=NOW+timedelta(minutes=20)):h.journal_begin(self.s,p,'124.1',prior);h.journal_finish(self.s,p,'124.1',{'ok':False})
        with patch.object(h,'clock',return_value=NOW+timedelta(minutes=40)):h.journal_begin(self.s,p,'125.1',{'batch_id':'124.1','attempts':2,'code':'TRANSIENT'});h.journal_finish(self.s,p,'125.1',{'ok':False})
        with patch.object(h,'clock',return_value=NOW+timedelta(minutes=60)):
            with self.assertRaises(h.GuardError):h.journal_begin(self.s,p,'126.1',{'batch_id':'125.1','attempts':3,'code':'TRANSIENT'})
    def test_legacy_attempt_count(self):
        p=h.journal_path('stock_master',NOW,False,IDENTITY)
        h.journal_begin(self.s,p,IDENTITY,{'attempts':2});h.journal_finish(self.s,p,IDENTITY,{'ok':False})
        with self.assertRaises(h.GuardError):h.journal_begin(self.s,p,'124.1',{'batch_id':IDENTITY,'attempts':3,'code':'TRANSIENT'})
    def test_manual_attempt_same_run_guard(self):
        p=h.journal_path('stock_master',NOW,True,IDENTITY);self.assertEqual(p,h.journal_path('stock_master',NOW,True,'123.2'));h.journal_begin(self.s,p,IDENTITY,None,force=True);h.journal_finish(self.s,p,IDENTITY,{'ok':False})
        with self.assertRaises(h.GuardError):h.journal_begin(self.s,p,'123.2',None,force=True)
    def test_new_slot_independent(self):
        p=h.journal_path('stock_master',NOW,False,IDENTITY);p2=h.journal_path('stock_master',NOW+timedelta(minutes=10),False,'124.1');h.journal_begin(self.s,p,IDENTITY,None);self.assertTrue(h.journal_begin(self.s,p2,'124.1',None))
    def test_guarded_execute_blocks_before_handler(self):
        self.http.deny=True
        with patch.dict(os.environ,{'GITHUB_ACTIONS':'true'}),patch.object(h.Store,'worker',return_value=self.s):
            r=h.guarded_execute('stock_master',NOW,False,IDENTITY,{},lambda:self.fail('called'))
        self.assertTrue(r['blocked'])
    def test_guarded_execute_journaled(self):
        calls=[]
        with patch.dict(os.environ,{'GITHUB_ACTIONS':'true','GODPICK_H249_REQUEST':''}),patch.object(h.Store,'worker',return_value=self.s):
            r=h.guarded_execute('stock_master',NOW,False,IDENTITY,{},lambda:(calls.append(1) or {'ok':True}));self.assertTrue(r['ok']);r=h.guarded_execute('stock_master',NOW,False,'124.1',{},lambda:self.fail('called'));self.assertTrue(r['blocked'])
        self.assertEqual(calls,[1])
    def test_guarded_ambiguous_start_never_invokes(self):
        self.http.timeout=True
        with patch.dict(os.environ,{'GITHUB_ACTIONS':'true'}),patch.object(h.Store,'worker',return_value=self.s):
            r=h.guarded_execute('stock_master',NOW,False,IDENTITY,{},lambda:self.fail('called'))
        self.assertTrue(r['blocked'])
    def test_page17_failed_wake_preserves_durable_queue(self):
        import types
        m=types.ModuleType('godpick_scheduler_wakeup_service');calls=[]
        def direct(**kw):calls.append(kw);return False,'timeout'
        m.dispatch_scheduler_wakeup=direct
        with patch.dict(sys.modules,{'godpick_scheduler_wakeup_service':m}),patch.object(h,'Store',return_value=self.s),patch.object(h,'clock',return_value=NOW):
            receipt={};ok,msg=h.dispatch_page17(token='secret',owner='owner',repo='repo',manual_job='stock_master',receipt=receipt)
            self.assertTrue(ok);self.assertIn('未確認',msg);self.assertNotIn('secret',msg);self.assertTrue(calls[0]['wakeup_source'].startswith('h249_'))
            self.assertEqual(self.s.read(h.reqpath(receipt['request_id']))[0]['requests'][receipt['request_id']]['state'],'QUEUED')
            h.dispatch_page17(token='secret',owner='owner',repo='repo',manual_job='stock_master');self.assertEqual(len(calls),1)
    def test_worker_routine_due_first(self):
        calls=[]
        with patch.dict(os.environ,{'GODPICK_WAKEUP_SOURCE':'schedule','GITHUB_RUN_ID':'123','GITHUB_RUN_ATTEMPT':'1'}):
            r=h.worker_dispatch('',lambda mode:(calls.append(mode) or {'ok':True,'executed':[{}]}),store=self.s)
        self.assertEqual(calls,['']);self.assertTrue(r['ok'])
    def test_worker_invalid_id_never_direct(self):
        with patch.dict(os.environ,{'GODPICK_WAKEUP_SOURCE':'h249_bad','GITHUB_RUN_ID':'123'}):
            r=h.worker_dispatch('stock_master',lambda _:self.fail('called'),store=self.s)
        self.assertFalse(r['ok'])
    def test_worker_idle_claims_durable(self):
        r=self.enqueue(now=h.clock());calls=[]
        with tempfile.TemporaryDirectory() as td,patch.object(h,'MARKER',Path(td)/'receipt.json'),patch.dict(os.environ,{'GODPICK_WAKEUP_SOURCE':'schedule','GITHUB_RUN_ID':'123','GITHUB_RUN_ATTEMPT':'1'}):
            def direct(mode):
                calls.append(mode)
                if not mode:return {'ok':True,'executed':[]}
                h.note_job(self.s,r['id'],IDENTITY,mode,'STARTED');h.note_job(self.s,r['id'],IDENTITY,mode,'RETURNED');return self.res(r)
            result=h.worker_dispatch('',direct,store=self.s);self.assertEqual(calls,['','stock_master']);self.assertTrue(result['ok']);self.assertEqual(self.read(r)['state'],'EXECUTION_RECORDED')
    def test_worker_cancelled_explicit_never_falls_back(self):
        r=self.enqueue(now=h.clock());self.claim(r,now=h.clock());h.note_job(self.s,r['id'],IDENTITY,'stock_master','STARTED');self.http.completed.add(IDENTITY)
        with patch.dict(os.environ,{'GODPICK_WAKEUP_SOURCE':'h249_'+r['id'],'GITHUB_RUN_ID':'124','GITHUB_RUN_ATTEMPT':'1'}):result=h.worker_dispatch('stock_master',lambda _:self.fail('called'),store=self.s)
        self.assertFalse(result['ok']);self.assertEqual(self.read(r)['state'],'REVIEW_REQUIRED')

if __name__=='__main__':unittest.main()
