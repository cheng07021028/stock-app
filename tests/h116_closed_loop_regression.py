# -*- coding: utf-8 -*-
"""All synthetic fixtures; does not imply live market alpha."""
import base64
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from godpick_h116_daily_feedback import (
    make_decision_snapshot, verify_snapshot, snapshot_path, github_publish_verified,
    load_snapshot_files, review_strategies,
)
from tools.apply_h116_snapshot_hook import patch as patch_page


class Response:
    def __init__(self,status,content=None):self.status_code=status;self.content=content
    def json(self):return self.content


class InMemoryGithub:
    def __init__(self):self.files={};self.conflicts=0
    def get(self,url,*,params,headers,timeout):
        if url not in self.files:return Response(404)
        return Response(200,{'encoding':'base64','content':base64.b64encode(self.files[url]).decode('ascii')})
    def put(self,url,*,headers,json,timeout):
        if self.conflicts:
            self.conflicts-=1
            return Response(409)
        self.files[url]=base64.b64decode(json['content'])
        return Response(201)


class ClosedLoopTests(unittest.TestCase):
    @staticmethod
    def core():
        return {'version':'test','run_id':'synth_run1','created_at':'2026-10-12T22:00:00+08:00',
          'blackhorse_overview':[
           {'股票代號':'2382','股票名稱':'股票A','H114主導策略':'H114_B','H114研究優先分':80,
            'H114入榜身分':'B_RESEARCH','latest_evidence_at':'2026-10-12T18:00:00+08:00'},
           {'股票代號':'2330','股票名稱':'股票B','H114主導策略':'H114_A','H114研究優先分':90,
            'H114入榜身分':'A_CERTIFIED_RESEARCH'},
          ],'research':[{'股票代號':'2382','H114主導策略':'H114_B','H114研究優先分':80,
                         'latest_evidence_at':'2026-10-12T18:00:00+08:00'}]}

    @classmethod
    def snap(cls):
        return make_decision_snapshot(cls.core(),run_id='synth_run1',
                 decided_at='2026-10-12T20:00:00+08:00',market_day='2026-10-12')

    def test_actual_snapshot_has_verifiable_hash_and_no_invented_evidence(self):
        s=self.snap()
        self.assertTrue(verify_snapshot(s))
        self.assertEqual(s['decision_count'],3)
        self.assertTrue(any(x['asof_status']=='UNVERIFIED_PER_ROW' for x in s['decisions']))
        self.assertTrue(all(x['permission']=='RESEARCH_ONLY_NO_TRADE' for x in s['decisions']))
        self.assertEqual(snapshot_path(s),'data/h116/decisions/2026-10-12/synth_run1.json')
        s['decisions'][0]['score']=100
        self.assertFalse(verify_snapshot(s))

    def test_stale_scan_core_or_future_evidence_never_verified(self):
        with self.assertRaises(ValueError):make_decision_snapshot(self.core(),run_id='wrong',
            decided_at='2026-10-12T20:00:00+08:00',market_day='2026-10-12')
        base=self.core();base['blackhorse_overview'][0]['latest_evidence_at']='2026-10-13T20:00:00+08:00'
        snap=make_decision_snapshot(base,run_id='synth_run1',decided_at='2026-10-12T20:00:00+08:00',market_day='2026-10-12')
        self.assertEqual(snap['decisions'][0]['asof_status'],'UNVERIFIED_PER_ROW')

    def test_durable_only_after_readback_and_idempotence(self):
        fake=InMemoryGithub();s=self.snap()
        credentials={'H116_GITHUB_PERSIST':'1','H116_GITHUB_REPOSITORY':'example/stock-app',
                     'H116_GITHUB_BRANCH':'main','GITHUB_TOKEN':'fake-test-token'}
        ok=github_publish_verified(s,secrets=credentials,session=fake)
        self.assertTrue(ok['permanent']);self.assertEqual(ok['status'],'WRITTEN_AND_READBACK_VERIFIED')
        again=github_publish_verified(s,secrets=credentials,session=fake)
        self.assertEqual(again['status'],'ALREADY_VERIFIED')
        url=list(fake.files)[0]
        fake.files[url]=b'{"different":"contents"}'
        mismatch=github_publish_verified(s,secrets=credentials,session=fake)
        self.assertEqual(mismatch['status'],'IMMUTABLE_CONFLICT')
        self.assertFalse(mismatch['ok'])

    def test_unavailable_or_failed_remote_never_success(self):
        snap=self.snap()
        r=github_publish_verified(snap,secrets={'H116_GITHUB_PERSIST':'0'})
        self.assertFalse(r['permanent'])
        fake=InMemoryGithub();fake.conflicts=1
        r=github_publish_verified(snap,secrets={'H116_GITHUB_PERSIST':'1',
                     'H116_GITHUB_REPOSITORY':'example/test','GITHUB_TOKEN':'dummy'},session=fake)
        self.assertFalse(r['permanent'])

    def test_snapshot_persistent_hash_verification(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'2026-10-12'/'synth_run1.json';p.parent.mkdir()
            p.write_text(json.dumps(self.snap(),ensure_ascii=False),encoding='utf-8')
            self.assertEqual(len(load_snapshot_files(Path(d))),1)
            p.write_text('{}'.format('{"run_id":"tampered"}'),encoding='utf-8')
            with self.assertRaises(ValueError):load_snapshot_files(Path(d))

    def test_paper_only_pending_and_dedup_same_stock_day(self):
        snaps=[self.snap()]
        dates=['2026-10-12','2026-10-13','2026-10-14','2026-10-15','2026-10-16','2026-10-19']
        prices=[];bench=[]
        for i,day in enumerate(dates):
            prices.append({'code':'2382','date':day,'open':100,'high':105,'low':99,'close':101})
            prices.append({'code':'2330','date':day,'open':200,'high':205,'low':198,'close':201})
            bench.append({'code':'BENCHMARK','date':day,'open':100,'high':101,'low':99,'close':100})
        result=review_strategies(snaps,prices,benchmark=bench,
                                 buy_commission_bps=0,sell_commission_bps=0,sell_tax_bps=0,slippage_bps=0)
        self.assertEqual(result['source_events'],3)
        self.assertEqual(result['same_stock_day_strategy_duplicate_rows'],3)
        self.assertEqual(result['data_failures'].get('UNVERIFIED_ASOF_SNAPSHOT'),1)
        self.assertTrue(all(x['validation']=='INSUFFICIENT_OBSERVATIONS' for x in result['summary']))
        self.assertTrue(all(not x['auto_apply'] for x in result['recommendations']))
        self.assertFalse(result['guardrails']['production_weights_changed'])

    def test_page7_surgical_patch_and_idempotence(self):
        base='''H85_H79_CORE_LIMITS = {\n    \"research\": 80,\n}\n\ndef dummy():\n'''+'''    if True:\n'''+'''        _h88_anchor_ok, _h88_publish_msg = _h88_publish_result_nonblocking(\n            rec_df, category_strength_df, hot_pick_df\n        )\n        _h86_candidate = st.session_state.get(_k("candidate_diagnosis_store"))\n'''
        new=patch_page(base)
        self.assertIn('H116_ACTUAL_SCAN_SNAPSHOT_HOOK_BEGIN',new)
        self.assertEqual(patch_page(new),new)
        with self.assertRaises(ValueError):patch_page('not compatible')

if __name__=='__main__':unittest.main(verbosity=2)
