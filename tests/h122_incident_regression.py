# -*- coding: utf-8 -*-
from __future__ import annotations
import json
import tempfile
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from godpick_h122_incident_center import retry_decision, build_dashboard, alert_transition, to_html
from tools.h122_incident_cli import main, run_safe_collect
from tools.h122_notify_issue import notify


def state(status='PENDING_VALIDATION', issues=None):
    return {'status':status,'issues':issues or [],'market':{'last_complete_session':'2026-10-14',
            'last_day_by_source':{'TWSE':'2026-10-14','TPEX':'2026-10-14','TAIEX':'2026-10-14'},
            'decision_snapshot_count':3,'h120_proof_accepted_days':0}}

def receipt(collect='success', review='success', corporate='success'):
    return {'collect_step':collect,'review_step':review,'corporate_step':corporate}

class H122Tests(unittest.TestCase):
    def test_successful_whole_market_is_not_pretended_certification(self):
        result=retry_decision('market_eod',exit_code=0,result={'status':'COLLECTED_LOCALLY_GIT_COMMIT_STILL_REQUIRED'})
        self.assertTrue(result['ok'])
        d=build_dashboard(state(),receipt())
        self.assertEqual(d['status'],'PENDING')
        self.assertEqual(d['persistence'],'REMOTE_UNVERIFIED')
        self.assertFalse(d['strategy_approved'])

    def test_corporate_partial_exit_zero_is_error(self):
        d=retry_decision('corporate_raw',exit_code=0,result={'status':'PARTIAL_OR_FAILED_SOURCE_REVIEW_REQUIRED',
                                'captures':{'TWSE':{}}, 'errors':{'TPEX':'HTTP 403'}})
        self.assertFalse(d['ok']);self.assertFalse(d['retry'])

    def test_both_corporate_sources_required(self):
        d=retry_decision('corporate_raw',exit_code=0,result={'status':'RAW_CAPTURED_REVIEW_REQUIRED','captures':{'TWSE':{},'TPEX':{}},'errors':{}})
        self.assertTrue(d['ok'])

    def test_only_network_transient_retry(self):
        self.assertTrue(retry_decision('market_eod',exit_code=2,result={'errors':{'TWSE':'HTTP 503'}})['retry'])
        self.assertFalse(retry_decision('market_eod',exit_code=2,result={'errors':{'TWSE':'HTTP 403'}})['retry'])
        self.assertFalse(retry_decision('market_eod',exit_code=2,result={'errors':{'TWSE':'IMMUTABLE_CONFLICT HTTP 503'}})['retry'])
        self.assertFalse(retry_decision('market_eod',exit_code=2,result={'status':'INVALID_DATE','errors':{'TWSE':'日期不一致'}})['retry'])

    def test_unknown_task_rejected(self):
        with self.assertRaises(ValueError): retry_decision('h116_trade',exit_code=2,result={})
        with self.assertRaises(ValueError): run_safe_collect('modify_weights')

    def test_retry_tries_twice_not_unbounded(self):
        calls=[]
        def runner(*args,**kw):
            calls.append(args[0]);return type('Result',(),{'returncode':2,'stdout':json.dumps({'status':'INCOMPLETE_OFFICIAL_SOURCES','errors':{'TPEX':'HTTP 503'}})})()
        out=run_safe_collect('market_eod',root=Path('.'),runner=runner,sleep=lambda _:None)
        self.assertEqual(out['attempts'],2);self.assertFalse(out['ok']);self.assertEqual(len(calls),2)

    def test_retry_recovers(self):
        calls=[]
        def runner(*args,**kw):
            calls.append(1)
            v={'status':'INCOMPLETE_OFFICIAL_SOURCES','errors':{'TPEX':'HTTP 502'}} if len(calls)==1 else {'status':'COLLECTED_LOCALLY_GIT_COMMIT_STILL_REQUIRED'}
            return type('Result',(),{'returncode':2 if len(calls)==1 else 0,'stdout':json.dumps(v)})()
        out=run_safe_collect('market_eod',root=Path('.'),runner=runner,sleep=lambda _:None)
        self.assertTrue(out['ok']);self.assertEqual(out['attempts'],2)

    def test_broken_hash_hard_block_and_corporate_failure(self):
        d=build_dashboard(state('BLOCKED_EVIDENCE_INVALID',[{'code':'INVALID_DAILY_HASH'}]),receipt())
        self.assertEqual(d['status'],'BLOCKED')
        d=build_dashboard(state(),receipt(corporate='failure'))
        self.assertIn('NIGHTLY_PIPELINE_STEP_FAILED',d['alert_codes'])
        self.assertEqual(d['status'],'BLOCKED')

    def test_alert_dedup_and_recovery_not_false_certification(self):
        s=build_dashboard(state('BLOCKED_EVIDENCE_INVALID',[{'code':'INVALID_DAILY_HASH'}]),receipt())
        self.assertEqual(alert_transition({},s),'ALERT')
        self.assertEqual(alert_transition(s,s),'NO_CHANGE')
        self.assertEqual(alert_transition(s,build_dashboard(state(),receipt())),'RECOVERY_REQUIRES_REVIEW')

    def test_remote_old_receipt_is_not_proof_by_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'reports/h121').mkdir(parents=True)
            (root/'reports/h121/H121_operational_state.json').write_text(json.dumps(state()),encoding='utf-8')
            (root/'reports/h121/H121_run_receipt.json').write_text(json.dumps(receipt()),encoding='utf-8')
            (root/'reports/h121/H121_remote_receipt.json').write_text(json.dumps({'status':'REMOTE_HEAD_MATCH','remote_verified':True,'local_head':'a'*40,'remote_head':'a'*40}),encoding='utf-8')
            self.assertEqual(main(['dashboard','--root',str(root)]),0)
            d=json.loads((root/'reports/h122/H122_dashboard_state.json').read_text())
            self.assertEqual(d['persistence'],'REMOTE_UNVERIFIED')
            original=(root/'reports/h122/H122_dashboard_state.json').read_bytes()
            self.assertEqual(main(['dashboard','--root',str(root)]),0)
            self.assertEqual((root/'reports/h122/H122_dashboard_state.json').read_bytes(),original)

    def test_dashboard_escaped_against_html_injection(self):
        d=build_dashboard(state(),receipt());d['alert_codes']=['<script>alert(1)</script>']
        h=to_html(d);self.assertNotIn('<script>',h);self.assertIn('&lt;script&gt;',h)

    def test_issues_dedup_and_non_critical_no_issue(self):
        history=[]
        def gh(args):
            history.append(args)
            if 'list' in args:return '[]'
            return 'https://github.com/a/b/issues/1'
        blocked=build_dashboard(state(),receipt(collect='failure'))
        self.assertEqual(notify(blocked,'a/b',exec_gh=gh)['status'],'INCIDENT_CREATED')
        self.assertEqual(len(history),2)
        history=[]
        self.assertEqual(notify(build_dashboard(state(),receipt()),'a/b',exec_gh=gh)['status'],'NO_INCIDENT_ACTION')
        self.assertEqual(len(history),1)

    def test_issue_changes_only_when_fingerprint_changes(self):
        calls=[]
        d=build_dashboard(state(),receipt(collect='failure'))
        def gh(args):
            calls.append(args)
            if 'list' in args:return '[{"number":7,"title":"[STOCKAPP-H122-AUTO-INCIDENT]"}]'
            if 'view' in args:return json.dumps({'body':'H122-fingerprint: '+d['fingerprint_sha256']})
            return 'updated'
        self.assertEqual(notify(d,'a/b',exec_gh=gh)['status'],'OPEN_INCIDENT_EXISTS')
        self.assertEqual(len(calls),2)
        calls.clear()
        def gh_changed(args):
            calls.append(args)
            if 'list' in args:return '[{"number":7,"title":"[STOCKAPP-H122-AUTO-INCIDENT]"}]'
            if 'view' in args:return json.dumps({'body':'H122-fingerprint: old'})
            return 'updated'
        self.assertEqual(notify(d,'a/b',exec_gh=gh_changed)['status'],'INCIDENT_UPDATED')
        self.assertEqual(len(calls),3)

    def test_issue_not_closed_on_pending(self):
        gh=lambda args: '[{"number":1,"title":"[STOCKAPP-H122-AUTO-INCIDENT]"}]' if 'list' in args else 'closed'
        self.assertEqual(notify(build_dashboard(state(),receipt()),'a/b',exec_gh=gh)['status'],'OPEN_INCIDENT_AWAITING_RECOVERY')

if __name__=='__main__': unittest.main(verbosity=2)
