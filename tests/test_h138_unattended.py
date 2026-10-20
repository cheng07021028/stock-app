# -*- coding: utf-8 -*-
"""H138: offline, mock remote; no real market or cloud writes."""
from __future__ import annotations
import base64
from datetime import date, datetime, timedelta
import gzip
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import TestCase

import pandas as pd
from zoneinfo import ZoneInfo

from godpick_h117_official_market_closure import MARKET_URLS, MIN_COUNTS, make_official_day_manifest
from godpick_h138_unattended_bridge import verified_eod, seal_scheduled_decision
from godpick_h138_daily_brief import build_brief, read_remote_brief
from godpick_h134_decision_vault import build_snapshot, _json_bytes
from tools.h138_brief_cli import main as brief_cli

TW = ZoneInfo('Asia/Taipei')
NOW = datetime(2026, 10, 20, 21, 45, tzinfo=TW)
CFG = {'owner': 'demo', 'repo': 'stock', 'branch': 'runtime-data', 'token': 'OFFLINE_ONLY'}

class Response:
    def __init__(self, code, obj=None): self.status_code, self.obj = code, obj
    def json(self): return self.obj

class MockGitHub:
    def __init__(self): self.blobs = {}; self.writes = 0; self.fail_readback = False
    def get(self, url, **kw):
        if self.fail_readback and self.writes: return Response(500)
        path = url.split('/contents/')[-1]
        if path not in self.blobs: return Response(404)
        return Response(200, {'encoding': 'base64',
                              'content': base64.b64encode(self.blobs[path]).decode()})
    def put(self, url, **kw):
        path = url.split('/contents/')[-1]
        if path in self.blobs: return Response(422)
        self.blobs[path] = base64.b64decode(kw['json']['content'])
        self.writes += 1
        return Response(201)


def sample(day=date(2026,10,20)):
    return pd.DataFrame([{'股票代號': '2330', '股票名稱': '測試公司',
                          '市場別': '上市','H113入榜資格': 'WATCH_ONLY',
                          'H110研究資訊截止日': day.isoformat(),
                          'H109決策權威': 'RESEARCH_ELIGIBLE',
                          'H123法人實證狀態': 'UNVERIFIED', 'H124大戶實證狀態': 'UNVERIFIED'}])


def setup():
    ctx = {'owner': '07_股神推薦', 'trigger': 'V191中央自動排程',
           'run_date': '2026-10-20', 'run_id': 'RUN_SAME', 'started_at': '2026-10-20 21:44:00'}
    ns = {'__headless_st__': SimpleNamespace(session_state={
        'recommend_execution_context_v191': dict(ctx), 'candidate_diagnosis_store': sample(),
        'h85_h79_core_tables': {'dummy': True}, 'scan_quality_report': {}}),
        '_k': lambda x:x, 'H85_H79_CORE_SESSION_KEY':'h85_h79_core_tables',
        '_h90_normalize_core_snapshot': lambda x:x,
        'validate_h97_core_for_scan': lambda raw,candidate,run_id,require_audit: (run_id == 'RUN_SAME','OK'),
        '_h85_decode_h79_core_tables': lambda x: {'audit': sample()}}
    result = {'ok': True, 'execution_context': ctx, 'execution_owner': 'pages/7_股神推薦.py'}
    return result, ns

def ok_eod(root, day, *, now):
    return {'status': 'EOD_THREE_MARKETS_INTERNAL_HASH_VERIFIED'}

class H138Tests(TestCase):
    def test_eod_weekend(self):
        t = datetime(2026, 10, 17, 21, tzinfo=TW)
        self.assertEqual(verified_eod(Path('/nonexistent'), t.date(), now=t)['status'], 'CLOSED_WEEKEND')
    def test_eod_before_close(self):
        t = datetime(2026,10,20,11,tzinfo=TW)
        self.assertEqual(verified_eod(Path('/nonexistent'), t.date(), now=t)['status'],'WAIT_FOR_MARKET_CLOSE')
    def test_eod_empty(self):
        with TemporaryDirectory() as temp:
            self.assertEqual(verified_eod(Path(temp), NOW.date(), now=NOW)['status'],'EOD_NOT_VERIFIED')
    def test_eod_three_manifests(self):
        with TemporaryDirectory() as temp:
            for source in MARKET_URLS:
                rows = [{'code':'2330','date':NOW.date().isoformat(),'close':100}] * MIN_COUNTS[source]
                manifest = make_official_day_manifest({'source':source,'day':NOW.date().isoformat(),
                    'source_url':MARKET_URLS[source], 'rows':rows}, collected_at='2026-10-20T16:00:00+08:00')
                p=Path(temp)/'data'/'h117'/'daily'/source/(NOW.date().isoformat()+'.json.gz')
                p.parent.mkdir(parents=True)
                with gzip.open(p,'wt',encoding='utf-8') as f: json.dump(manifest,f)
            self.assertEqual(verified_eod(Path(temp),NOW.date(),now=NOW)['status'],
                             'EOD_THREE_MARKETS_INTERNAL_HASH_VERIFIED')
            src=Path(temp)/'data'/'h117'/'daily'/'TWSE'/(NOW.date().isoformat()+'.json.gz')
            with gzip.open(src,'rt',encoding='utf-8') as f: obj=json.load(f)
            obj['rows'][0]['close'] = 77
            with gzip.open(src,'wt',encoding='utf-8') as f: json.dump(obj,f)
            self.assertEqual(verified_eod(Path(temp),NOW.date(),now=NOW)['status'],'EOD_PROVENANCE_INVALID')
    def test_new_headless_archived_and_idempotent(self):
        result,ns=setup(); api=MockGitHub()
        for _ in range(2):
            receipt=seal_scheduled_decision(result,ns,now=NOW,transport=api,config=CFG,eod_checker=ok_eod)
            self.assertEqual(receipt['status'],'REMOTE_CONFIRMED',receipt)
        self.assertEqual(api.writes,1)
        self.assertEqual(receipt['trading_authority'],False)
        self.assertIn('SCHEDULED', next(iter(api.blobs.values())).decode())
    def test_remote_readback_must_confirm(self):
        result,ns=setup(); api=MockGitHub();api.fail_readback=True
        r=seal_scheduled_decision(result,ns,now=NOW,config=CFG,transport=api,eod_checker=ok_eod)
        self.assertFalse(r['confirmed']);self.assertEqual(r['status'],'REMOTE_UNCONFIRMED')
    def test_no_token_no_durability(self):
        result,ns=setup()
        r=seal_scheduled_decision(result,ns,now=NOW,config={**CFG,'token':''},eod_checker=ok_eod)
        self.assertFalse(r['confirmed'])
    def test_missing_market_is_not_success(self):
        result,ns=setup()
        r=seal_scheduled_decision(result,ns,now=NOW,eod_checker=lambda *a,**k: {'status':'NO_EOD'})
        self.assertEqual(r['status'],'MARKET_DAY_UNVERIFIED')
    def test_previous_scan_rejected(self):
        result,ns=setup();result['execution_context']['run_date']='2026-10-19'
        self.assertEqual(seal_scheduled_decision(result,ns,now=NOW,eod_checker=ok_eod)['status'],'STALE_RUN_DATE')
    def test_non_canonical_result(self):
        result,ns=setup();result['execution_owner']='fake.py'
        self.assertEqual(seal_scheduled_decision(result,ns,now=NOW,eod_checker=ok_eod)['status'],'NOT_CANONICAL_SCHEDULED_RUN')
    def test_failed_scan(self):
        result,ns=setup();result['ok']=False
        self.assertEqual(seal_scheduled_decision(result,ns,now=NOW,eod_checker=ok_eod)['status'],'SCAN_FAILED_NO_SEAL')
    def test_mismatched_session(self):
        result,ns=setup();ns['__headless_st__'].session_state['recommend_execution_context_v191']['run_id']='OTHER'
        self.assertEqual(seal_scheduled_decision(result,ns,now=NOW,eod_checker=ok_eod)['status'],'SESSION_SCAN_ID_MISMATCH')
    def test_old_evidence_rejected(self):
        result,ns=setup();ns['_h85_decode_h79_core_tables']=lambda raw:{'audit':sample(date(2026,10,19))}
        self.assertEqual(seal_scheduled_decision(result,ns,now=NOW,eod_checker=ok_eod)['status'],'H126_EVIDENCE_NOT_CURRENT')
    def test_h97_rejection(self):
        result,ns=setup();ns['validate_h97_core_for_scan']=lambda *a,**k:(False,'BAD')
        self.assertEqual(seal_scheduled_decision(result,ns,now=NOW,eod_checker=ok_eod)['status'],'H97_CURRENT_CORE_REJECTED')
    def test_brief_missing(self):
        with TemporaryDirectory() as temp:
            p=build_brief(root=Path(temp),asof=NOW.date(),audit={'status':'BLOCKED_REQUIRES_ATTENTION','findings':['EOD_MISSING']})
            self.assertEqual(p['status'],'REVIEW_REQUIRED')
            self.assertIn('TODAY_H126_ARCHIVE_NOT_CONFIRMED',p['findings'])
    def test_brief_confirmed(self):
        with TemporaryDirectory() as temp:
            bundle=build_snapshot(pd.DataFrame([{'股票代號':'2330','H126權威層別':'B｜待驗證觀察（非買進）','H126研究資訊截止日':'2026-10-20'}]),
                research_day=NOW.date(),source='SCHEDULED_PAGE07_H126',captured_at=NOW)
            p=Path(temp)/bundle['path'];p.parent.mkdir(parents=True);p.write_bytes(_json_bytes(bundle['payload']))
            summary=build_brief(root=Path(temp),asof=NOW.date(),audit={'status':'INTERNAL_CHECK_PASS_NOT_PIT','findings':[]})
            self.assertEqual(summary['status'],'READY_FOR_MANUAL_RESEARCH_NOT_TRADES')
            self.assertEqual(summary['decision_source'],'SCHEDULED_PAGE07_H126')
            self.assertEqual(brief_cli(['--runtime-root',temp,'--asof','2026-10-20']),0)
            self.assertTrue((Path(temp)/'data'/'h138'/'H138_manager_brief.json').exists())
    def test_remote_brief_read(self):
        api=MockGitHub()
        b={'schema':'H138-manager-brief-1','asof_tw':'2026-10-20','status':'REVIEW_REQUIRED','findings':['X']}
        api.blobs['data/h138/H138_manager_brief.json']=_json_bytes(b)
        self.assertEqual(read_remote_brief(config=CFG,transport=api,asof=NOW.date())['status'],'REVIEW_REQUIRED')
        self.assertEqual(read_remote_brief(config={**CFG,'token':''})['status'],'NOT_CONFIGURED')
    def test_h134_archive_source_accepted(self):
        b=build_snapshot(pd.DataFrame([{'股票代號':'2330','H126權威層別':'B｜待驗證觀察','H126研究資訊截止日':'2026-10-20'}]),
                         research_day=NOW.date(),source='SCHEDULED_PAGE07_H126',captured_at=NOW)
        self.assertEqual(b['payload']['source_type'],'SCHEDULED_PAGE07_H126')
        self.assertIn('NOT_INDEPENDENTLY',b['payload']['pit_status'])
