# -*- coding: utf-8 -*-
"""Full offline CLI smoke with no real H120 human proofs; fail closed."""
import csv,json,sys,tempfile,subprocess,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from godpick_h116_daily_feedback import make_decision_snapshot
from tests.h119_entitlement_regression import DAYS,prices,bm

class H120CLITests(unittest.TestCase):
 def test_h120_missing_proof_means_zero_verified_and_no_false_comparison(self):
  with tempfile.TemporaryDirectory() as td:
   p=Path(td);folder=p/'decisions';folder.mkdir()
   snap=make_decision_snapshot({'run_id':'h120strict','blackhorse_overview':[{'股票代號':'2382','H114主導策略':'H114_B','latest_evidence_at':'2026-09-07T18:00:00+08:00'}]},run_id='h120strict',decided_at='2026-09-07T20:00:00+08:00',market_day='2026-09-07')
   (folder/'test.json').write_text(json.dumps(snap,ensure_ascii=False),encoding='utf-8')
   def w(path,items):
    with path.open('w',encoding='utf-8',newline='') as f:
     writer=csv.DictWriter(f,fieldnames=list(items[0]));writer.writeheader();writer.writerows(items)
   w(p/'price.csv',prices());w(p/'bench.csv',bm())
   args=[sys.executable,str(ROOT/'tools/h116_closed_loop_cli.py'),'review','--decisions',str(folder),'--prices',str(p/'price.csv'),'--benchmark',str(p/'bench.csv'),'--out',str(p/'reports'),'--h118-strict','--h119-entitlement','--h120-proof-enforced']
   run=subprocess.run(args,capture_output=True,text=True,cwd=ROOT)
   self.assertEqual(run.returncode,0,run.stderr)
   manifest=json.loads((p/'reports/H116_source_manifest.json').read_text(encoding='utf-8'))
   self.assertEqual(manifest['h120_proof_accepted_days'],0)
   self.assertEqual(manifest['h120_policy'],'MANUAL_RAW_PROOF_ENFORCED')
   with (p/'reports/H116_T1_T3_T5_details.csv').open(encoding='utf-8-sig',newline='') as f:rows=list(csv.DictReader(f))
   self.assertTrue(all(r['data_status']=='CORPORATE_ACTION_COVERAGE_UNVERIFIED' for r in rows))
   again=subprocess.run(args,capture_output=True,text=True,cwd=ROOT)
   self.assertIn('NO_NEW_INPUT',again.stdout)
   detail=p/'reports/H116_T1_T3_T5_details.csv'
   detail.write_text('manually tampered',encoding='utf-8')
   rerun=subprocess.run(args,capture_output=True,text=True,cwd=ROOT)
   self.assertIn('REVIEW_FINISHED_WITH_LIMITATIONS',rerun.stdout)
   compare=[sys.executable,str(ROOT/'tools/h120_validate_cli.py'),'compare','--details',str(detail),'--benchmark',str(p/'bench.csv'),'--source-manifest',str(p/'reports/H116_source_manifest.json'),'--out',str(p/'compare.json')]
   verdict=subprocess.run(compare,capture_output=True,text=True,cwd=ROOT)
   self.assertEqual(verdict.returncode,2)
   self.assertIn('UNVERIFIED_OR_STALE_STRATEGY_DETAILS',verdict.stdout)
if __name__=='__main__':unittest.main(verbosity=2)
