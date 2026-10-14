# -*- coding: utf-8 -*-
"""H120 fake official schema samples; NO online source or full-market certification claims."""
from __future__ import annotations
import sys,unittest,tempfile,json
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from godpick_h119_official_source import capture_raw
from godpick_h120_evidence_reconciliation import reconcile_coverage, read_immutable_capture
from godpick_h118_performance_authority import load_actions,load_coverage

TWSE={'stat':'OK','fields':['資料日期','股票代號','股票名稱','權/息'],'data':[['115年10月13日','1727','中華化','權']]}
TPEX=[{'除權息日期':'115/10/13','代號':'6411','現金股利':'1.5','每仟股無償配股':'0'}]
TS='2026-10-13T18:30:00+08:00'
TWURL='https://www.twse.com.tw/exchangeReport/TWT49U?response=json'
TPURL='https://www.tpex.org.tw/openapi/v1/tpex_exright_cal'

def inputs(root,market='TWSE',payload=None):
    payload=payload if payload is not None else (TWSE if market=='TWSE' else TPEX)
    digest=capture_raw(market,json.dumps(payload,ensure_ascii=False).encode(),retrieved_at=TS,root=root)
    url=TWURL if market=='TWSE' else TPURL
    proof={'market':market,'date':'2026-10-13','raw_sha256':digest['source_sha256'],
           'reviewer':'auditor-demo','reviewed_at':'2026-10-13T19:00:00+08:00',
           'source_scope':'FULL_MARKET_EX_DATE','expected_stock_events':'1',
           'review_status':'HUMAN_ATTESTED','source_url':url}
    co={'market':market,'date':'2026-10-13','status':'OFFICIAL_COMPLETE',
        'source_url':url,'verified_at':'2026-10-13T20:00:00+08:00'}
    ac={'code':'1727' if market=='TWSE' else '6411','ex_date':'2026-10-13',
        'action_type':'STOCK_DIVIDEND' if market=='TWSE' else 'CASH_DIVIDEND',
        'source_url':url,'verified_at':'2026-10-13T20:00:00+08:00',
        'verification_status':'OFFICIAL_DOCUMENTED'}
    return proof,co,ac

class H120EvidenceTests(unittest.TestCase):
 def test_full_manual_positive_day_evidence(self):
    with tempfile.TemporaryDirectory() as td:
      p,c,a=inputs(Path(td))
      report=reconcile_coverage(raw_root=Path(td),proofs=[p],coverage=load_coverage([c]),actions=load_actions([a]))
      self.assertEqual(report['accepted_days'],1)
      self.assertEqual(report['rejected_days'],0)
      self.assertFalse(report['autocertified'])
 def test_fake_empty_source_cannot_claim_zero_event(self):
    with tempfile.TemporaryDirectory() as td:
      p,c,a=inputs(Path(td),payload={'stat':'OK','fields':TWSE['fields'],'data':[]})
      p['expected_stock_events']='0'
      r=reconcile_coverage(raw_root=Path(td),proofs=[p],coverage=load_coverage([c]),actions=[])
      self.assertEqual(r['accepted_days'],0)
      self.assertIn('零事件',r['issues'][0]['reason'])
 def test_raw_catalog_mismatch_blocked(self):
    with tempfile.TemporaryDirectory() as td:
      p,c,a=inputs(Path(td))
      a['code']='2382'
      r=reconcile_coverage(raw_root=Path(td),proofs=[p],coverage=load_coverage([c]),actions=load_actions([a]))
      self.assertEqual(r['accepted_days'],0)
      self.assertIn('不一致',r['issues'][0]['reason'])
 def test_raw_hash_and_scope_tampering_blocked(self):
    with tempfile.TemporaryDirectory() as td:
      p,c,a=inputs(Path(td))
      p['source_scope']='UNSPECIFIED'
      r=reconcile_coverage(raw_root=Path(td),proofs=[p],coverage=load_coverage([c]),actions=load_actions([a]))
      self.assertEqual(r['accepted_days'],0)
      p['source_scope']='FULL_MARKET_EX_DATE';p['raw_sha256']='a'*64
      r=reconcile_coverage(raw_root=Path(td),proofs=[p],coverage=load_coverage([c]),actions=load_actions([a]))
      self.assertEqual(r['accepted_days'],0)
 def test_bullish_type_guess_does_not_overwrite_official_action_type(self):
    with tempfile.TemporaryDirectory() as td:
      p,c,a=inputs(Path(td))
      a['action_type']='CASH_DIVIDEND' # raw 權 instead of 息
      r=reconcile_coverage(raw_root=Path(td),proofs=[p],coverage=load_coverage([c]),actions=load_actions([a]))
      self.assertEqual(r['accepted_days'],0)
      self.assertIn('類別',r['issues'][0]['reason'])
 def test_missing_proof_blocks_even_if_coverage_says_complete(self):
    with tempfile.TemporaryDirectory() as td:
      p,c,a=inputs(Path(td))
      r=reconcile_coverage(raw_root=Path(td),proofs=[],coverage=load_coverage([c]),actions=load_actions([a]))
      self.assertEqual(r['accepted_days'],0);self.assertEqual(r['issues'][0]['status'],'NOT_ATTESTED')
 def test_tpex_scope_and_market_specific(self):
    with tempfile.TemporaryDirectory() as td:
      p,c,a=inputs(Path(td),'TPEX')
      r=reconcile_coverage(raw_root=Path(td),proofs=[p],coverage=load_coverage([c]),actions=load_actions([a]))
      self.assertEqual(r['accepted_days'],1)
 def test_duplicate_proof_invalidates_previously_accepted_day(self):
    with tempfile.TemporaryDirectory() as td:
      p,c,a=inputs(Path(td))
      r=reconcile_coverage(raw_root=Path(td),proofs=[p,dict(p)],coverage=load_coverage([c]),actions=load_actions([a]))
      self.assertEqual(r['accepted_days'],0)
      self.assertGreater(r['rejected_days'],0)
 def test_nonmatching_market_event_does_not_pollute(self):
    with tempfile.TemporaryDirectory() as td:
      p,c,a=inputs(Path(td),'TWSE')
      other={'code':'6411','ex_date':'2026-10-13','action_type':'CASH_DIVIDEND',
        'source_url':TPURL,'verified_at':'2026-10-13T19:00:00+08:00','verification_status':'OFFICIAL_DOCUMENTED'}
      r=reconcile_coverage(raw_root=Path(td),proofs=[p],coverage=load_coverage([c]),actions=load_actions([a,other]))
      self.assertEqual(r['accepted_days'],1)
 def test_invalid_actual_file_bytes_rejected(self):
    with tempfile.TemporaryDirectory() as td:
      p,c,a=inputs(Path(td))
      file=next(Path(td).glob('*/TWSE_*.json.gz'))
      file.write_bytes(b'bad-archive')
      with self.assertRaises((OSError,EOFError)):
          read_immutable_capture(Path(td),'TWSE',p['raw_sha256'])
if __name__=='__main__':unittest.main(verbosity=2)
