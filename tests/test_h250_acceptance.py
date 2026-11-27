"""H250 acceptance contracts; no live HTTP or data changes."""
import importlib.util, json, tempfile, unittest
from pathlib import Path
from datetime import datetime
P=Path(__file__).resolve().parents[1]/'tools'/'h250_acceptance.py'
spec=importlib.util.spec_from_file_location('h250_acceptance',P);h=importlib.util.module_from_spec(spec);spec.loader.exec_module(h)
NOW=datetime(2026,11,27,20,tzinfo=h.TZ)
class Snapshot:
    def __init__(self,files):self.files=files;self.paths=set(files)
    def read(self,p):return self.files.get(p)
def snapshot(**kw):
    base={'godpick_auto_scheduler_status.json':{},'godpick_auto_scheduler_history.json':{'records':[]},'data/config/godpick_auto_scheduler_settings.json':{'enabled':True,'jobs':{j:{'enabled':True} for j in h.JOBS}}};base.update(kw);return Snapshot(base)
def gate(gates,name):return next(x for x in gates if x['gate']==name)
class Tests(unittest.TestCase):
    def test_empty_not_success(self):
        g,_=h.runtime_gates(snapshot(),snapshot(),[],NOW)
        self.assertEqual(gate(g,'DURABLE_QUEUE')['status'],'PENDING');self.assertEqual(gate(g,'UNATTENDED_TWO_DAYS')['status'],'PENDING')
    def test_zero_wake_not_manual(self):
        s=snapshot(**{'godpick_auto_scheduler_status.json':{'last_summary':{'executed':0,'success':0}}});g,_=h.runtime_gates(s,s,[],NOW);self.assertEqual(gate(g,'MANUAL_FULL_14')['status'],'PENDING')
    def test_start_uncertain(self):
        s=snapshot(**{'data/h249/slots/20261126/stock_master_0730.json':{'version':'V191-H249-DURABLE-QUEUE-1','attempts':[{'owner':'1.1','state':'STARTED'}]}});g,_=h.runtime_gates(s,s,[],NOW);self.assertEqual(len(gate(g,'DURABLE_QUEUE')['detail']['unresolved']),1)
    def test_partial_full_rejected(self):
        r={'id':'x','owner':'1.1','state':'VERIFIED','mode':'full_refresh_and_recommend','execution_ok':True,'verification':'REMOTE_VERIFIER_PASS','result':[{'job':'stock_master','status':'SUCCESS','run_key':'x'}]};s=snapshot(**{'data/h249/queue/20261127.json':{'version':'V191-H249-DURABLE-QUEUE-1','requests':{'x':r}}});g,_=h.runtime_gates(s,s,[{'id':1,'run_attempt':1,'status':'completed','conclusion':'success','event':'workflow_dispatch'}],NOW);self.assertEqual(gate(g,'MANUAL_FULL_14')['status'],'PENDING')
    def test_two_days_exact(self):
        f={};hist=[];runs=[];n=1
        for day in ('2026-11-25','2026-11-26'):
            for j in h.JOBS:
                i=f'{n}.1';runs.append({'id':n,'run_attempt':1,'status':'completed','conclusion':'success','event':'schedule'});n+=1
                f[f'data/h249/slots/{day.replace("-","")}/{j}_0730.json']={'version':'V191-H249-DURABLE-QUEUE-1','attempts':[{'owner':i,'state':'RETURNED','status':'SUCCESS'}]};hist.append({'job':j,'batch_id':i,'run_key':j+'|'+day+' 07:30','status':'SUCCESS','batch_mode':'scheduled'})
        f['godpick_auto_scheduler_history.json']={'records':hist};s=snapshot(**f);g,_=h.runtime_gates(s,s,runs,NOW);self.assertEqual(gate(g,'UNATTENDED_TWO_DAYS')['status'],'PASS')
        runs[-1]['event']='workflow_dispatch';g,_=h.runtime_gates(s,s,runs,NOW);self.assertEqual(gate(g,'UNATTENDED_TWO_DAYS')['status'],'PENDING')
    def test_snapshot_mismatch(self):
        self.assertEqual(h.pair_gate(snapshot())['status'],'PENDING')
    def test_pair_valid(self):
        f={'run_id':'r','recommendation_date':'2026-11-27'};a={**f,'full_snapshot_payload_hash':h.jsonhash(f)};v={'godpick_latest_recommendations.json':f,'godpick_latest_run_anchor.json':a,'godpick_recommend_list.json':[]};v.update({p[:-5]+'_sync_state.json':{'payload_hash':h.jsonhash(x)} for p,x in list(v.items())});self.assertEqual(h.pair_gate(snapshot(**v))['status'],'PASS');v['godpick_latest_run_anchor.json']={'run_id':'different'};self.assertEqual(h.pair_gate(snapshot(**v))['status'],'PENDING')
    def test_missing_manifest(self):
        with tempfile.TemporaryDirectory() as d:self.assertEqual(h.local_manifest(Path(d))['status'],'PENDING')
    def test_missing_junit(self):self.assertEqual(h.junit_gate(Path('/nonexistent'))['status'],'PENDING')
    def test_skipped_suite(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'j.xml';p.write_text('<testsuite><testcase classname="test_h248_scheduler_contract.ActualScheduler" name="t"><skipped/></testcase></testsuite>');self.assertEqual(h.junit_gate(p)['status'],'PENDING')
if __name__=='__main__':unittest.main()
