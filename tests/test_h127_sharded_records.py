# -*- coding: utf-8 -*-
import hashlib
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from godpick_h127_sharded_records import write_snapshot, load_snapshot


def digest(payload):
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(',', ':'), default=str).encode('utf-8')).hexdigest()


class FakeRemote:
    def __init__(self):
        self.files = {}
        self.fail_at = None
        self.calls = 0

    def get(self, key, default):
        if key not in self.files:
            return default, '404: 尚未建立'
        return self.files[key], 'remote read OK'

    def put(self, key, payload):
        self.calls += 1
        if self.fail_at == self.calls:
            return False, 'simulated interrupted network'
        if len(json.dumps(payload, ensure_ascii=False).encode('utf-8')) >= 650_000:
            return False, 'simulated Github oversized body'
        self.files[key] = json.loads(json.dumps(payload, ensure_ascii=False))
        return True, 'remote commit confirmed'


class H127Tests(unittest.TestCase):
    @staticmethod
    def records():
        return [{'record_id':f'2026-10-14T12:00:{i:04d}:1234', '股票代號':'1234',
                 '推薦理由摘要': '強勢但待實證' * 100, '推薦日期':'2026-10-14',
                 'factor':i%10, 'series':list(range(8))} for i in range(900)]

    def make(self):
        rows = self.records()
        return rows, {'payload_hash':digest(rows), 'revision':125, 'count':len(rows)}

    def test_complete_roundtrip_and_no_legacy_overwrite(self):
        rows, state = self.make()
        remote = FakeRemote()
        pub, msg = write_snapshot(rows, state, get_json=remote.get, put_json=remote.put,
                                  json_safe=lambda x:x, record_hash_func=digest)
        self.assertIsNotNone(pub, msg)
        restored, result = load_snapshot({**state,**pub},get_json=remote.get,record_hash_func=digest)
        self.assertEqual(restored, rows, result)
        self.assertTrue(all('godpick_records_h127/' in k for k in remote.files))
        self.assertNotIn('godpick_records.json', remote.files)

    def test_interrupted_write_does_not_publish_pointer(self):
        rows, state = self.make()
        remote = FakeRemote(); remote.fail_at = 1
        pub, msg = write_snapshot(rows, state, get_json=remote.get, put_json=remote.put,
                                  json_safe=lambda x:x,record_hash_func=digest)
        self.assertIsNone(pub)
        self.assertIn('state unchanged', msg)
        self.assertFalse(any(x.endswith('manifest.json') for x in remote.files))

    def test_corrupt_and_missing_shard(self):
        rows, state = self.make()
        remote = FakeRemote()
        pub, _ = write_snapshot(rows, state, get_json=remote.get, put_json=remote.put,
                                 json_safe=lambda x:x,record_hash_func=digest)
        part = next(p for p in remote.files if '/part-' in p)
        old = remote.files[part]['data_b64']
        remote.files[part]['data_b64'] = 'AAAA'+old[4:]
        restored, msg = load_snapshot({**state,**pub},get_json=remote.get,record_hash_func=digest)
        self.assertIsNone(restored,msg)
        remote.files[part]['data_b64'] = old
        del remote.files[part]
        restored, msg = load_snapshot({**state,**pub},get_json=remote.get,record_hash_func=digest)
        self.assertIsNone(restored,msg)

    def test_idempotent_same_content_and_immutable_conflict(self):
        rows, state = self.make(); remote=FakeRemote()
        pub, _ = write_snapshot(rows, state, get_json=remote.get, put_json=remote.put,
                                 json_safe=lambda x:x,record_hash_func=digest)
        count=remote.calls
        again, _ = write_snapshot(rows, state, get_json=remote.get, put_json=remote.put,
                                 json_safe=lambda x:x,record_hash_func=digest)
        self.assertEqual(pub,again)
        self.assertEqual(count,remote.calls)
        part = next(p for p in remote.files if '/part-' in p)
        remote.files[part]['data_b64']='tampered'
        conflict, err=write_snapshot(rows,state,get_json=remote.get,put_json=remote.put,
                                    json_safe=lambda x:x,record_hash_func=digest)
        self.assertIsNone(conflict)
        self.assertIn('immutable shard conflict',err)

    def test_wrong_payload_hash_rejected(self):
        rows, state=self.make();state['payload_hash']='a'*64
        result,msg=write_snapshot(rows,state,get_json=FakeRemote().get,put_json=FakeRemote().put,
                                  json_safe=lambda x:x,record_hash_func=digest)
        self.assertIsNone(result)
        self.assertIn('authority state',msg)

    def test_incomplete_remote_manifest_does_not_fallback(self):
        rows,state=self.make(); remote=FakeRemote()
        pub,_=write_snapshot(rows,state,get_json=remote.get,put_json=remote.put,
                              json_safe=lambda x:x,record_hash_func=digest)
        del remote.files[pub['shard_manifest_path']]
        restored,msg=load_snapshot({**state,**pub},get_json=remote.get,record_hash_func=digest)
        self.assertIsNone(restored)
        self.assertIn('manifest',msg)

    def test_precise_patch_no_second_rewrite(self):
        from tools.apply_h127_large_records_fix import patch_source
        source_path = ROOT / 'godpick_persistence_service.py'
        if not source_path.is_file():
            source_path = ROOT / 'stock-app-main' / 'godpick_persistence_service.py'
        source = source_path.read_text(encoding='utf-8')
        again,result=patch_source(source)
        self.assertEqual(result,'ALREADY_PATCHED')
        self.assertEqual(source,again)

if __name__=='__main__': unittest.main()
