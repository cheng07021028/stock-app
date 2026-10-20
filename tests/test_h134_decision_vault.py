# -*- coding: utf-8 -*-
"""H134 offline-only tests. Uses fake GitHub HTTP; no user account, token or network."""
import base64
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
import json
import unittest

import pandas as pd

from godpick_h134_decision_vault import (
    VERSION, MAX_ROWS, REMOTE_DIRECTORY, build_snapshot, summarize,
    archive_once, list_recent, read_archive, snapshot_signals, _download, _json_bytes,
)

NOW = datetime(2026, 10, 20, 15, 30, tzinfo=ZoneInfo('Asia/Taipei'))
CFG = {'token':'FAKE_TOKEN_FOR_TESTS', 'owner':'example', 'repo':'testing', 'branch':'runtime-data'}


def df():
    return pd.DataFrame([
        {'股票代號':'2330', 'H126權威層別':'B｜待驗證觀察（非買進）',
         'H126認證資格':'PENDING_EVIDENCE', 'H126最後研究分':78,
         'H123法人實證狀態':'UNVERIFIED', 'H124大戶實證狀態':'UNVERIFIED',
         'H126研究資訊截止日':'2026-10-20'},
        {'股票代號':'2317', 'H126權威層別':'禁止新倉／隔離觀察',
         'H123法人實證狀態':'PROVEN_FLOW','H124大戶實證狀態':'VERIFIED_RISING',
         'H126研究資訊截止日':'2026-10-20'},
    ])


class Response:
    def __init__(self, code, payload=None):
        self.status_code = code
        self._data = payload
    def json(self):
        return self._data


class FakeGitHub:
    def __init__(self):
        self.files = {}
        self.put_calls = []
        self.fail_readback = False
        self.put_code = None
    def get(self, url, **kwargs):
        name = url.split('/contents/',1)[-1]
        if name.endswith('/'):
            return Response(200, [{'name':p.split('/')[-1], 'type':'file'} for p in self.files]) if self.files else Response(404)
        if self.fail_readback:
            return Response(503)
        if name not in self.files:
            return Response(404)
        data = self.files[name]
        return Response(200, {'encoding':'base64','content':base64.b64encode(data).decode('ascii')})
    def put(self, url, **kwargs):
        name = url.split('/contents/',1)[-1]
        self.put_calls.append(kwargs['json'])
        if self.put_code is not None:
            return Response(self.put_code)
        if name in self.files:
            return Response(422)
        self.files[name] = base64.b64decode(kwargs['json']['content'])
        return Response(201)


def snapshot(source='LIVE_SESSION_H126', day=date(2026,10,20), rows=None, now=NOW):
    return build_snapshot(df() if rows is None else rows, research_day=day, source=source, captured_at=now)


class H134(unittest.TestCase):
    def test_01_version(self):
        self.assertEqual(VERSION, 'V191-H134.0')
    def test_02_summary_accurate(self):
        s = summarize(df())
        self.assertEqual((s['研究股'],s['B級觀察'],s['禁止新倉']), (2,1,1))
    def test_03_missing_schema_rejected(self):
        with self.assertRaisesRegex(ValueError,'NO_H126_AUTHORITY'):
            snapshot(rows=pd.DataFrame([{'股票代號':'2330'}]))
    def test_04_future_date_rejected(self):
        with self.assertRaisesRegex(ValueError,'FUTURE_RESEARCH_DAY'):
            snapshot(day=date(2026,10,21))
    def test_05_no_source_unknown(self):
        with self.assertRaisesRegex(ValueError,'UNKNOWN_SOURCE'):
            snapshot(source='FAKED_LIVE')
    def test_06_duplicate_stock_rejected(self):
        with self.assertRaisesRegex(ValueError,'DUPLICATED_STOCK_CODE'):
            snapshot(rows=pd.concat([df(), df().head(1)]))
    def test_07_invalid_code_rejected(self):
        rows = df();rows.loc[0,'股票代號']='23303'
        with self.assertRaisesRegex(ValueError,'INVALID_STOCK_CODE'):
            snapshot(rows=rows)
    def test_08_invalid_layer_rejected(self):
        rows = df();rows.loc[0,'H126權威層別']='H102優先'
        with self.assertRaisesRegex(ValueError,'INVALID_H126_LAYER'):
            snapshot(rows=rows)
    def test_09_max_rows(self):
        rows = pd.DataFrame([{'股票代號':'1234','H126權威層別':'B'}] * (MAX_ROWS+1))
        with self.assertRaisesRegex(ValueError,'TOO_MANY_ROWS'):
            snapshot(rows=rows)
    def test_10_order_independent_fingerprint(self):
        self.assertEqual(snapshot()['fingerprint'], snapshot(rows=df().iloc[::-1])['fingerprint'])
    def test_11_new_click_same_decision_same_path(self):
        now_later = NOW + timedelta(minutes=30)
        self.assertEqual(snapshot()['path'], snapshot(now=now_later)['path'])
    def test_12_changed_decision_new_file(self):
        rows = df();rows.loc[0,'H126最後研究分'] = 90
        self.assertNotEqual(snapshot()['path'], snapshot(rows=rows)['path'])
    def test_13_path_contains_day(self):
        self.assertIn('H134_20261020_', snapshot()['path'])
    def test_14_payload_does_not_contain_token(self):
        self.assertNotIn(CFG['token'], str(snapshot()['payload']))
    def test_15_uploaded_never_pit_proven(self):
        self.assertIn('USER_BACKFILL', snapshot(source='UPLOADED_H126_UNVERIFIED')['payload']['pit_status'])
    def test_16_live_session_not_pit_proven(self):
        self.assertIn('NOT_INDEPENDENTLY', snapshot()['payload']['pit_status'])
    def test_17_old_live_date_backfill(self):
        self.assertIn('BACKDATED_SESSION', snapshot(day=date(2026,10,16))['payload']['pit_status'])
    def test_18_future_evidence_not_certified(self):
        rows = df();rows['H126研究資訊截止日']='2026-10-21'
        self.assertEqual(snapshot(rows=rows)['payload']['freshness'],'EVIDENCE_FROM_FUTURE_RECHECK')
    def test_19_weekend_not_trading_day(self):
        self.assertEqual(snapshot(day=date(2026,10,17))['payload']['pit_status'], 'NON_TRADING_DAY_NOT_PIT')
    def test_20_remote_create_and_readback(self):
        fake=FakeGitHub();out=archive_once(snapshot(), config=CFG, transport=fake)
        self.assertTrue(out['confirmed']);self.assertIn('CONFIRMED',out['status'])
        self.assertNotIn('sha',fake.put_calls[0])
        self.assertEqual(fake.put_calls[0]['branch'],'runtime-data')
    def test_21_repeat_idempotent_no_put(self):
        fake=FakeGitHub();archive_once(snapshot(),config=CFG,transport=fake)
        out=archive_once(snapshot(now=NOW + timedelta(minutes=9)),config=CFG,transport=fake)
        self.assertEqual(out['status'],'ALREADY_CONFIRMED')
        self.assertEqual(len(fake.put_calls),1)
    def test_22_failed_write_not_confirmed(self):
        fake=FakeGitHub();fake.put_code=403
        self.assertFalse(archive_once(snapshot(),config=CFG,transport=fake)['confirmed'])
    def test_23_read_failure_not_confirmed(self):
        fake=FakeGitHub();fake.fail_readback=True
        self.assertFalse(archive_once(snapshot(),config=CFG,transport=fake)['confirmed'])
    def test_24_missing_config_no_write(self):
        fake=FakeGitHub();out=archive_once(snapshot(),config={**CFG,'token':''},transport=fake)
        self.assertFalse(out['confirmed']);self.assertEqual(fake.put_calls,[])
    def test_25_remote_tamper_detected(self):
        fake=FakeGitHub();bundle=snapshot();archive_once(bundle,config=CFG,transport=fake)
        corrupt = json.loads(fake.files[bundle['path']]);corrupt['rows'][0]['股票代號']='0000'
        fake.files[bundle['path']] = _json_bytes(corrupt)
        payload,state=read_archive(bundle['path'],config=CFG,transport=fake)
        self.assertIsNone(payload);self.assertEqual(state,'REMOTE_FINGERPRINT_MISMATCH')
    def test_26_remote_existing_invalid_not_overwritten(self):
        fake=FakeGitHub();bundle=snapshot();fake.files[bundle['path']]=b'{}'
        out=archive_once(bundle,config=CFG,transport=fake)
        self.assertFalse(out['confirmed']);self.assertEqual(len(fake.put_calls),0)
    def test_27_list_recent(self):
        fake=FakeGitHub();bundle=snapshot();archive_once(bundle,config=CFG,transport=fake)
        listed,state=list_recent(config=CFG,transport=fake)
        self.assertEqual(state,'LIST_OK');self.assertEqual(len(listed),1)
        self.assertEqual(listed[0]['遠端驗證'],'READBACK_HASH_VERIFIED')
    def test_28_list_empty(self):
        self.assertEqual(list_recent(config=CFG,transport=FakeGitHub())[1],'REMOTE_DIRECTORY_EMPTY')
    def test_29_read_only_path_guard(self):
        self.assertEqual(read_archive('../secrets.toml',config=CFG,transport=FakeGitHub())[1],'INVALID_ARCHIVE_PATH')
    def test_30_h133_signal_backfill_unverified(self):
        out=snapshot_signals([snapshot()['payload']])
        self.assertEqual(len(out),1)
        self.assertEqual(out.iloc[0]['H134_PIT狀態'],'NOT_INDEPENDENTLY_VERIFIED')
    def test_31_hard_veto_not_promoted_in_signals(self):
        out=snapshot_signals([snapshot()['payload']]);self.assertNotIn('2317',out['股票代號'].values)
    def test_32_risk_override_not_present(self):
        self.assertNotIn('BUY',snapshot()['payload']['pit_status'])
    def test_33_placeholder_allowed(self):
        f=pd.DataFrame([{'股票代號':'','H126權威層別':'無合格推薦'}])
        self.assertEqual(snapshot(rows=f)['payload']['summary']['研究股'],0)
    def test_34_missing_date_status(self):
        rows=df();rows['H126研究資訊截止日']=None
        self.assertEqual(snapshot(rows=rows)['payload']['freshness'],'NO_EVIDENCE_DATE')
    def test_35_timestamp_naive_refused(self):
        with self.assertRaisesRegex(ValueError,'TIMEZONE'):
            snapshot(now=datetime(2026,10,20,15,30))

if __name__ == '__main__':
    unittest.main()

class CompareTests(unittest.TestCase):
    def test_36_layer_transitions(self):
        from godpick_h134_decision_vault import compare_decisions
        old = df();new = df().copy()
        new.loc[0, 'H126權威層別'] = 'A｜認證黑馬研究（非買進）'
        change = compare_decisions(new, old)
        self.assertEqual(change.query('股票代號 == "2330"').iloc[0]['變化類型'],'層別變化')
    def test_37_new_stock_not_formal(self):
        from godpick_h134_decision_vault import compare_decisions
        change = compare_decisions(df(), pd.DataFrame(columns=df().columns))
        self.assertTrue((change['變化類型']=='新增研究').all())
    def test_38_missing_removed(self):
        from godpick_h134_decision_vault import compare_decisions
        change = compare_decisions(df().iloc[:1],df())
        self.assertEqual(change.query('股票代號 == "2317"').iloc[0]['變化類型'],'移出研究')
