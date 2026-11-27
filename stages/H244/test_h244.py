# -*- coding: utf-8 -*-
import ast, unittest, tempfile
from pathlib import Path
from unittest.mock import patch as mock_patch
from godpick_h244_dispatch_truth import describe, read_run, taipei_now
from Apply_H244 import build, patch, replace, blob_hash

class Resp:
    def __init__(self, status, body): self.status_code=status; self.body=body
    def json(self): return self.body

class Tests(unittest.TestCase):
    def setUp(self):
        self.rec={'ok':True,'manual_job':'full_refresh_and_recommend','run_id':'101'}
        self.run={'available':True,'run_id':'101','attempt':'1','status':'completed','conclusion':'success'}
        self.summary={'executed':14,'success':14,'warning':0,'failed':0,'blocked':0,'in_progress':False}
        self.status={'last_manual_batch':{'batch_id':'101.1','batch_mode':'full_refresh_and_recommend','summary':self.summary,'validation':{'all_success':True}}}
    def test_success_matches_exact_batch(self): self.assertEqual(describe(self.rec,self.run,self.status)[0],'success')
    def test_other_run_not_success(self): self.assertEqual(describe(self.rec,{**self.run,'run_id':'999'},self.status)[0],'warning')
    def test_attempt_mismatch(self): self.assertEqual(describe(self.rec,{**self.run,'attempt':'2'},self.status)[0],'warning')
    def test_warning_fourteen(self):
        self.summary.update(success=10,warning=4);self.status['last_manual_batch']['validation']['all_success']=False
        level,msg=describe(self.rec,{**self.run,'conclusion':'failure'},self.status)
        self.assertEqual(level,'error');self.assertIn('執行 14／成功 10／警示 4',msg)
    def test_cancelled(self): self.assertEqual(describe(self.rec,{**self.run,'conclusion':'cancelled'},self.status)[0],'error')
    def test_running(self): self.assertEqual(describe(self.rec,{**self.run,'status':'in_progress'},self.status)[0],'info')
    def test_pending(self): self.assertIn('安全排隊',describe(self.rec,{**self.run,'status':'pending'},self.status)[1])
    def test_due_zero(self):
        level,msg=describe({**self.rec,'manual_job':''},self.run,{'last_wakeup_run_id':'101','last_wakeup_attempt':'1','last_summary':{'executed':0}})
        self.assertEqual(level,'info');self.assertIn('零項',msg)
    def test_manual_zero(self):
        self.summary.update(executed=0,success=0)
        self.assertEqual(describe(self.rec,self.run,self.status)[0],'warning')
    def test_receipt_missing_id(self): self.assertEqual(describe({**self.rec,'run_id':''},{},self.status)[0],'warning')
    def test_failure_dispatch(self): self.assertEqual(describe({'ok':False},{},{})[0],'error')
    def test_read_run(self):
        out=read_run(token='PRIVATE',owner='o',repo='r',run_id='101',http_get=lambda *a,**k:Resp(200,{'id':101,'status':'completed','conclusion':'success'}))
        self.assertTrue(out['available']);self.assertNotIn('PRIVATE',str(out))
    def test_read_wrong_id(self): self.assertFalse(read_run(token='x',owner='o',repo='r',run_id='101',http_get=lambda *a,**k:Resp(200,{'id':102}))['available'])
    def test_read_exception_safe(self):
        def f(*a,**k): raise RuntimeError('PRIVATE')
        self.assertNotIn('PRIVATE',str(read_run(token='PRIVATE',owner='o',repo='r',run_id='101',http_get=f)))
    def test_read_403(self): self.assertFalse(read_run(token='x',owner='o',repo='r',run_id='101',http_get=lambda *a,**k:Resp(403,{}))['available'])
    def test_taipei(self): self.assertTrue(taipei_now().endswith('UTC+8'))
    def test_anchor_strict(self):
        for s in ['abc','xx']:
            with self.assertRaises(ValueError): replace(s,'x','y')
    def test_wrong_source_no_output(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); p=root/'.github/workflows/godpick_auto_scheduler_v191.yml';p.parent.mkdir(parents=True);p.write_text('wrong');out=root/'out.zip'
            with self.assertRaises(ValueError): build(root,out)
            self.assertFalse(out.exists()); self.assertEqual(p.read_text(),'wrong')
    def test_workflow_patch_fixture(self):
        import yaml
        s='name: GodPick V191 Central Auto Scheduler\nconcurrency:\n  group: godpick-v191-central-auto-scheduler\n  cancel-in-progress: false\nscript: |\n  echo "- wake source: ${{ github.event.inputs.wakeup_source || github.event_name }}"\n'
        d=yaml.safe_load(patch('.github/workflows/godpick_auto_scheduler_v191.yml',s))
        self.assertEqual(d['concurrency']['queue'],'max');self.assertFalse(d['concurrency']['cancel-in-progress']);self.assertIn('inputs.manual_job',d['run-name'])
    def test_dependency_patch_fixture(self):
        import yaml
        s="steps:\n      - run: python -m pip install 'pandas==2.3.3' 'numpy>=1.26,<3' 'requests>=2.31,<3' 'openpyxl>=3.1.2,<4'\n"
        for f in ['h195_daily_command.yml','h220_review.yml']:
            d=yaml.safe_load(patch('.github/workflows/'+f,s));self.assertIn('lxml>=5.4,<7',d['steps'][0]['run']);self.assertIn('import lxml.etree',d['steps'][1]['run'])

if __name__=='__main__': unittest.main(verbosity=2)
