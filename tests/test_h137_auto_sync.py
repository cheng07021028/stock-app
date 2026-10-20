# -*- coding: utf-8 -*-
"""H137 regression tests; no live GitHub, no Secrets and no invented market results."""
from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from zoneinfo import ZoneInfo
import base64
import json
import unittest

import pandas as pd

from godpick_h134_decision_vault import build_snapshot, _json_bytes
from godpick_h136_auto_hub import create_bundle, make_artifacts
from godpick_h137_auto_seal import seal_new_manager_report, read_remote_audit
from tools.h137_ops_audit import audit

TW = ZoneInfo('Asia/Taipei')
NOW = datetime(2026, 10, 20, 19, 0, tzinfo=TW)
CFG = {'owner':'demo', 'repo':'stock', 'branch':'runtime-data', 'token':'OFFLINE_TEST_ONLY'}

def authority(day='2026-10-20'):
    return pd.DataFrame([{'股票代號':'2330', 'H126權威層別':'B｜觀察',
                          'H126研究資訊截止日':day, 'H126最後研究分':60,
                          'H123法人實證狀態':'UNVERIFIED','H124大戶實證狀態':'UNVERIFIED'}])

class Response:
    def __init__(self, status, obj=None): self.status_code, self.obj = status, obj
    def json(self): return self.obj

class GitHubFake:
    def __init__(self): self.blobs={}; self.writes=0; self.fail_after_write=False
    def get(self, url, **kwargs):
        path=url.split('/contents/')[-1]
        if path not in self.blobs or (self.fail_after_write and self.writes):return Response(404 if not self.fail_after_write else 502)
        return Response(200,{'encoding':'base64','content':base64.b64encode(self.blobs[path]).decode()})
    def put(self, url, **kwargs):
        path=url.split('/contents/')[-1]
        if path in self.blobs:return Response(422)
        self.writes+=1
        self.blobs[path]=base64.b64decode(kwargs['json']['content'])
        return Response(201)

class Tests(unittest.TestCase):
    def test_auto_archives_admin_and_readback(self):
        api=GitHubFake()
        result=seal_new_manager_report(authority(), role='admin', generated_at=NOW, now=NOW,config=CFG,transport=api)
        self.assertTrue(result['confirmed']);self.assertEqual(api.writes,1)
    def test_duplicate_idempotent(self):
        api=GitHubFake()
        for _ in range(3):
            self.assertTrue(seal_new_manager_report(authority(),role='admin',generated_at=NOW,now=NOW,config=CFG,transport=api)['confirmed'])
        self.assertEqual(api.writes,1)
    def test_non_admin_no_write(self):
        api=GitHubFake()
        self.assertEqual(seal_new_manager_report(authority(),role='viewer',generated_at=NOW,now=NOW,config=CFG,transport=api)['status'],'SKIPPED_ADMIN_ONLY')
        self.assertEqual(api.writes,0)
    def test_weekend_no_write(self):
        t=NOW+timedelta(days=4);api=GitHubFake()
        self.assertEqual(seal_new_manager_report(authority(),role='admin',generated_at=t,now=t,config=CFG,transport=api)['status'],'SKIPPED_NON_TRADING_WEEKEND')
    def test_non_current_cached_build(self):
        api=GitHubFake()
        self.assertEqual(seal_new_manager_report(authority(),role='admin',generated_at=NOW-timedelta(days=1),now=NOW,config=CFG,transport=api)['status'],'SKIPPED_NOT_FRESH_REPORT')
    def test_stale_five_minutes(self):
        api=GitHubFake()
        self.assertEqual(seal_new_manager_report(authority(),role='admin',generated_at=NOW-timedelta(minutes=6),now=NOW,config=CFG,transport=api)['status'],'SKIPPED_NOT_FRESH_REPORT')
    def test_bad_timezone(self):
        self.assertEqual(seal_new_manager_report(authority(),role='admin',generated_at=NOW.replace(tzinfo=None),now=NOW)['status'],'REJECTED_TIMEZONE_MISSING')
    def test_future_evidence(self):
        api=GitHubFake()
        self.assertEqual(seal_new_manager_report(authority('2026-10-21'),role='admin',generated_at=NOW,now=NOW,config=CFG,transport=api)['status'],'REJECTED_FUTURE_EVIDENCE')
    def test_invalid_evidence(self):
        api=GitHubFake()
        self.assertEqual(seal_new_manager_report(authority('yesterday'),role='admin',generated_at=NOW,now=NOW,config=CFG,transport=api)['status'],'REJECTED_EVIDENCE_DATE_INVALID')
    def test_malformed_h126(self):
        self.assertEqual(seal_new_manager_report(pd.DataFrame([{'foo':1}]),role='admin',generated_at=NOW,now=NOW)['status'],'REJECTED_H126_SCHEMA')
    def test_empty_no_write(self):
        self.assertEqual(seal_new_manager_report(pd.DataFrame(),role='admin',generated_at=NOW,now=NOW)['status'],'SKIPPED_NO_H126_ROWS')
    def test_no_remote_readback_no_confirmation(self):
        api=GitHubFake();api.fail_after_write=True
        result=seal_new_manager_report(authority(),role='admin',generated_at=NOW,now=NOW,config=CFG,transport=api)
        self.assertFalse(result['confirmed']);self.assertIn(result['status'], ('READ_HTTP_502', 'WRITE_UNCONFIRMED_READ_HTTP_502'))
    def test_missing_config_no_fake_confirmation(self):
        result=seal_new_manager_report(authority(),role='admin',generated_at=NOW,now=NOW,config={**CFG,'token':''},transport=GitHubFake())
        self.assertFalse(result['confirmed'])
    def test_remote_receipt(self):
        api=GitHubFake();path='data/h137/H137_audit.json';api.blobs[path]=_json_bytes({
            'schema':'H137-audit-1','asof_tw':'2026-10-20','status':'BLOCKED_REQUIRES_ATTENTION','findings':['MISSING'],'same_day_live_h126_archives':0})
        r=read_remote_audit(config=CFG,transport=api,asof=NOW.date())
        self.assertEqual(r['status'],'BLOCKED_REQUIRES_ATTENTION')
    def test_remote_no_key(self):
        self.assertEqual(read_remote_audit(config={**CFG,'token':''})['status'],'H137_REMOTE_NOT_CONFIGURED')
    def test_remote_future(self):
        api=GitHubFake();api.blobs['data/h137/H137_audit.json']=_json_bytes({'schema':'H137-audit-1','asof_tw':'2026-10-21','status':'INTERNAL_CHECK_PASS_NOT_PIT','findings':[]})
        self.assertEqual(read_remote_audit(config=CFG,transport=api,asof=NOW.date())['status'],'H137_REMOTE_FUTURE_DATE')
    def test_remote_stale(self):
        api=GitHubFake();api.blobs['data/h137/H137_audit.json']=_json_bytes({'schema':'H137-audit-1','asof_tw':'2026-10-01','status':'INTERNAL_CHECK_PASS_NOT_PIT','findings':[]})
        self.assertEqual(read_remote_audit(config=CFG,transport=api,asof=NOW.date())['status'],'H137_REMOTE_STALE')
    def _setup_vault(self, root, day=date(2026,10,20)):
        b=build_snapshot(authority(day.isoformat()),research_day=day,source='LIVE_SESSION_H126',captured_at=NOW)
        archive_path=root/b['path'];archive_path.parent.mkdir(parents=True,exist_ok=True)
        archive_path.write_bytes(_json_bytes(b['payload']))
    def test_audit_missing_everything(self):
        with TemporaryDirectory() as d:
            status=audit(root=Path(d),asof=NOW.date())
            self.assertEqual(status['status'],'BLOCKED_REQUIRES_ATTENTION')
            self.assertIn('H136_HEALTH_MISSING',status['findings'])
            self.assertIn('H126_LIVE_REPORT_NOT_ARCHIVED_FOR_TODAY',status['findings'])
    def test_audit_vault_only(self):
        with TemporaryDirectory() as d:
            root=Path(d);self._setup_vault(root)
            status=audit(root=root,asof=NOW.date())
            self.assertEqual(status['same_day_live_h126_archives'],1)
            self.assertIn('H136_HEALTH_MISSING',status['findings'])
    def test_audit_all_valid_internal(self):
        with TemporaryDirectory() as d:
            root=Path(d);self._setup_vault(root)
            # H136 requires two independently dated snapshots in the derived cache,
            # but today's H137 audit can verify an internally consistent status receipt.
            older=date(2026,10,19)
            b=build_snapshot(authority(older.isoformat()), research_day=older,
                            source='LIVE_SESSION_H126',captured_at=NOW-timedelta(days=1))
            path=root/b['path'];path.parent.mkdir(parents=True,exist_ok=True)
            path.write_bytes(_json_bytes(b['payload']))
            archives=[b['payload']]
            current = build_snapshot(authority(),research_day=NOW.date(),source='LIVE_SESSION_H126',captured_at=NOW)
            archives.append(current['payload'])
            stocks=pd.DataFrame([{'code':'2330','date':str(x),'close':100+i}
                                 for i,x in enumerate([older,NOW.date()])])
            benchmark=pd.DataFrame([{'date':str(x),'close':20000+i*10}
                                    for i,x in enumerate([older,NOW.date()])])
            frames, summary=create_bundle(archives,stocks,benchmark,asof=NOW.date())
            blobs=make_artifacts(frames,summary)
            folder=root/'data'/'h136';folder.mkdir(parents=True)
            for filename, contents in blobs.items(): (folder/filename).write_bytes(contents)
            (folder/'H136_health.json').write_bytes(_json_bytes({
                'schema':'H136-health-1','run_day_tw':NOW.date().isoformat(),
                'status':'UPDATED_VERIFIED_INTERNAL_SHA_RESEARCH_ONLY'}))
            result=audit(root=root,asof=NOW.date())
            self.assertEqual(result['status'],'INTERNAL_CHECK_PASS_NOT_PIT')
    def test_audit_health_old(self):
        with TemporaryDirectory() as d:
            root=Path(d);folder=root/'data'/'h136';folder.mkdir(parents=True)
            (folder/'H136_health.json').write_bytes(_json_bytes({'schema':'H136-health-1', 'run_day_tw':'2026-10-19', 'status':'NOT_READY_OLD_DATA_PRESERVED'}))
            status=audit(root=root,asof=NOW.date())
            self.assertIn('H136_NOT_UPDATED_TODAY',status['findings'])
            self.assertIn('H136_LATEST_RUN_NOT_READY',status['findings'])
    def test_audit_doesnt_write_anything(self):
        with TemporaryDirectory() as d:
            root=Path(d);audit(root=root,asof=NOW.date())
            self.assertEqual(list(root.glob('**/*')),[])
    def test_source_code_has_new_report_hook(self):
        source=(Path(__file__).resolve().parents[1]/'pages'/'7_股神推薦.py').read_text(encoding='utf-8')
        self.assertGreaterEqual(source.count("st.session_state['h137_last_auto_archive'] = _h137_seal"),2)
        self.assertIn('if not ready and st.button(', source)

if __name__=='__main__':unittest.main()
