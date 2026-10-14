# -*- coding: utf-8 -*-
"""H121 no-live-network offline regression: fail closed and idempotent state."""
from __future__ import annotations
import gzip
import json
import hashlib
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import tempfile
import sys
import unittest
from unittest.mock import patch
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from godpick_h121_operations import inspect_repository, TZ
from godpick_h117_official_market_closure import make_official_day_manifest
from godpick_h116_daily_feedback import make_decision_snapshot
from tools.h121_operations_cli import _remote_verify, main

DAY = '2026-10-14'
NOW = datetime(2026, 10, 14, 20, 30, tzinfo=TZ)

def _data_root(base: Path, *, sources=('TWSE', 'TPEX', 'TAIEX')):
    for src in sources:
        rows = [{'code':f'{2000+i}', 'date':DAY, 'open':10.0, 'high':11.0, 'low':9.0, 'close':10.5,
                 'exchange':src} for i in range(100)] if src != 'TAIEX' else [
                 {'code':'BENCHMARK','date':DAY,'open':20000.,'high':20100.,'low':19900.,'close':20030.,'exchange':src}]
        obj = {'source':src,'day':DAY,'rows':rows,'source_url':'https://example.org/raw'}
        m = make_official_day_manifest(obj, collected_at='2026-10-14T19:00:00+08:00')
        path = base/'data/h117/daily'/src/(DAY+'.json.gz')
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path,'wt',encoding='utf-8') as out:json.dump(m,out)

class H121Tests(unittest.TestCase):
    def test_no_market_data_fails_closed(self):
        with tempfile.TemporaryDirectory() as d:
            state = inspect_repository(Path(d), now=NOW)
            self.assertEqual(state['status'], 'PENDING_VALIDATION')
            self.assertIn('NO_THREE_SOURCE_DAY', [x['code'] for x in state['issues']])
            self.assertFalse(state['remote_commit_verified'])

    def test_incomplete_three_source_detected(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d);_data_root(p,sources=('TWSE','TAIEX'))
            state=inspect_repository(p,now=NOW)
            self.assertEqual(state['market']['complete_session_count'],0)
            self.assertIn('PARTIAL_OFFICIAL_SESSIONS',[x['code'] for x in state['issues']])

    def test_one_complete_day_no_false_certificate(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d);_data_root(p)
            state=inspect_repository(p,now=NOW)
            self.assertEqual(state['market']['complete_session_count'],1)
            self.assertIn('NO_IMMUTABLE_DECISIONS',[x['code'] for x in state['issues']])
            self.assertFalse(state['remote_commit_verified'])
            self.assertEqual(state['market']['last_complete_session'],DAY)

    def test_corrupted_official_archive_is_hard_block(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d);_data_root(p)
            target=p/'data/h117/daily/TWSE'/(DAY+'.json.gz')
            with gzip.open(target,'rt',encoding='utf-8') as f:data=json.load(f)
            data['rows'][0]['close']=999
            with gzip.open(target,'wt',encoding='utf-8') as f:json.dump(data,f)
            state=inspect_repository(p,now=NOW)
            self.assertEqual(state['status'],'BLOCKED_EVIDENCE_INVALID')

    def test_snapshot_and_manifest_hash_tamper_detected(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d);_data_root(p)
            snap=make_decision_snapshot({'run_id':'h121_demo','blackhorse_overview':[
                {'股票代號':'2382','H114主導策略':'H114_B','latest_evidence_at':'2026-10-14T18:30:00+08:00'}]},
                run_id='h121_demo',decided_at='2026-10-14T20:00:00+08:00',market_day=DAY)
            snapfile=p/'data/h116/decisions'/DAY/'h121_demo.json'
            snapfile.parent.mkdir(parents=True,exist_ok=True)
            snapfile.write_text(json.dumps(snap,ensure_ascii=False),encoding='utf-8')
            for name in ('verified_ohlc.csv','benchmark_ohlc.csv'):
                f=p/'data/h116'/name;f.parent.mkdir(parents=True,exist_ok=True);f.write_bytes(b'code,date\n')
            details=p/'reports/h116/H116_T1_T3_T5_details.csv';details.parent.mkdir(parents=True,exist_ok=True);details.write_bytes(b'status\n')
            manifest={'price_input_sha256':hashlib.sha256(b'code,date\n').hexdigest(),
                      'benchmark_input_sha256':hashlib.sha256(b'code,date\n').hexdigest(),
                      'h120_details_csv_sha256':hashlib.sha256(b'status\n').hexdigest(),
                      'h118_quality_policy':'STRICT','h120_policy':'MANUAL_RAW_PROOF_ENFORCED',
                      'h120_proof_accepted_days':0,
                      'snapshot_runs':[['h121_demo',snap['snapshot_sha256']]]}
            (details.parent/'H116_source_manifest.json').write_text(json.dumps(manifest),encoding='utf-8')
            a=inspect_repository(p,now=NOW)
            self.assertEqual(a['market']['h116_manifest'],'HASH_MATCH')
            self.assertIn('NO_H120_ATTESTED_DAYS',[x['code'] for x in a['issues']])
            details.write_bytes(b'tampered\n')
            b=inspect_repository(p,now=NOW)
            self.assertEqual(b['status'],'BLOCKED_EVIDENCE_INVALID')
            self.assertIn('PERFORMANCE_SOURCE_HASH_MISMATCH',[x['code'] for x in b['issues']])

    def test_stable_status_file_does_not_change_on_repeat(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d);_data_root(p)
            cmd=['inspect','--root',str(p),'--state',str(p/'state.json'),'--receipt',str(p/'receipt.json')]
            self.assertEqual(main(cmd),0)
            original=(p/'state.json').read_bytes()
            self.assertEqual(main(cmd),0)
            self.assertEqual(original,(p/'state.json').read_bytes())
            self.assertFalse(json.loads((p/'receipt.json').read_text())['state_changed'])

    def test_remote_head_must_match_and_no_secret_in_failure(self):
        with tempfile.TemporaryDirectory() as d:
            with patch('tools.h121_operations_cli.subprocess.run') as run:
                run.side_effect=[type('P',(),{'stdout':'a'*40+'\n'})(),type('P',(),{'stdout':'b'*40+'\trefs/heads/main\n'})()]
                self.assertFalse(_remote_verify(Path(d),'main')['remote_verified'])
            with patch('tools.h121_operations_cli.subprocess.run',side_effect=OSError('secret-in-stderr')):
                self.assertNotIn('secret',json.dumps(_remote_verify(Path(d),'main')))

if __name__ == '__main__':unittest.main(verbosity=2)
