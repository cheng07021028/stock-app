# -*- coding: utf-8 -*-
"""H136 synthetic/offline tests: no external API, trades, account data or real prices."""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from zoneinfo import ZoneInfo
import base64
import copy
import json
import unittest

import pandas as pd

from godpick_h134_decision_vault import build_snapshot
from godpick_h136_auto_hub import (
    VERSION, FILE_NAMES, _price_table, collect_local_archives, create_bundle,
    make_artifacts, validate_artifacts, remote_cache, _json_bytes,
)
from tools.h136_nightly_sync import sync, _load_h117_archived

DAY = date(2026, 10, 16)
TW = ZoneInfo('Asia/Taipei')


def archive(day: date, *, code='2330', layer='B｜待驗證'):
    record = {'股票代號': code, 'H126權威層別': layer,
              'H126研究資訊截止日': day.isoformat(), 'H126最後研究分': 54,
              'H123法人實證狀態': 'UNVERIFIED', 'H124大戶實證狀態': 'UNVERIFIED'}
    return build_snapshot(pd.DataFrame([record]), research_day=day,
                          source='LIVE_SESSION_H126',
                          captured_at=datetime(day.year, day.month, day.day, 17, tzinfo=TW))


def samples():
    first, second = date(2026, 10, 14), date(2026, 10, 15)
    a = [archive(first)['payload'], archive(second)['payload']]
    s = pd.DataFrame([{'code': '2330', 'date': day, 'close': px}
                      for day, px in [('2026-10-14', 1500), ('2026-10-15', 1505), ('2026-10-16', 1510)]])
    b = pd.DataFrame([{'date': day, 'close': px}
                      for day, px in [('2026-10-14', 46000), ('2026-10-15', 46300), ('2026-10-16', 46800)]])
    return a, s, b


class Response:
    def __init__(self, code, payload):
        self.status_code = code
        self.payload = payload
    def json(self):
        return self.payload


class MockTransport:
    def __init__(self, artifacts):
        self.artifacts = artifacts
    def get(self, url, **kwargs):
        name = url.rsplit('/', 1)[-1]
        if name not in self.artifacts:
            return Response(404, {})
        return Response(200, {'encoding': 'base64', 'content': base64.b64encode(self.artifacts[name]).decode('ascii')})


class Tests(unittest.TestCase):
    def setUp(self):
        self.archives, self.stock, self.benchmark = samples()
        self.frames, self.summary = create_bundle(self.archives, self.stock, self.benchmark, asof=DAY)
        self.blobs = make_artifacts(self.frames, self.summary)

    def test_01_version(self): self.assertIn('H136', VERSION)
    def test_02_two_days(self): self.assertEqual(self.summary['signal_days'], 2)
    def test_03_non_trading_permission(self): self.assertEqual(self.summary['trade_permission'], 'UNCHANGED')
    def test_04_not_certified(self): self.assertEqual(self.summary['PIT'], 'NOT_INDEPENDENTLY_VERIFIED')
    def test_05_full_cache_verifies(self): self.assertEqual(validate_artifacts(self.blobs, asof=DAY)[0]['stock'].shape[0], 3)
    def test_06_hash_mutation_rejected(self):
        tampered=dict(self.blobs);tampered[FILE_NAMES['stock']]+=b'changed'
        with self.assertRaises(ValueError):validate_artifacts(tampered,asof=DAY)
    def test_07_missing_file_rejected(self):
        bad=dict(self.blobs);del bad[FILE_NAMES['stock']]
        with self.assertRaises(ValueError):validate_artifacts(bad,asof=DAY)
    def test_08_bad_scheme_rejected(self):
        bad=dict(self.blobs);raw=json.loads(bad['H136_manifest.json']);raw['schema']='H102-old'
        bad['H136_manifest.json']=_json_bytes(raw)
        with self.assertRaises(ValueError):validate_artifacts(bad,asof=DAY)
    def test_09_future_manifest_rejected(self):
        with self.assertRaises(ValueError):validate_artifacts(self.blobs,asof=date(2026,10,15))
    def test_10_stock_conflict(self):
        bad=pd.concat([self.stock,pd.DataFrame([{'code':'2330','date':'2026-10-14','close':1700}])])
        self.assertEqual(_price_table(bad,asof=DAY,benchmark=False)[1],'CONFLICTING_CLOSE')
    def test_11_benchmark_conflict(self):
        bad=pd.concat([self.benchmark,pd.DataFrame([{'date':'2026-10-14','close':40000}])])
        self.assertEqual(_price_table(bad,asof=DAY,benchmark=True)[1],'CONFLICTING_CLOSE')
    def test_12_stock_weekend_rejected(self):
        bad=pd.concat([self.stock,pd.DataFrame([{'code':'2330','date':'2026-10-17','close':1520}])])
        self.assertEqual(_price_table(bad,asof=date(2026,10,20),benchmark=False)[1],'WEEKEND_PRICE_SOURCE')
    def test_13_future_stock_dropped(self):
        bad=pd.concat([self.stock,pd.DataFrame([{'code':'2330','date':'2026-10-19','close':1540}])])
        self.assertEqual(len(_price_table(bad,asof=DAY,benchmark=False)[0]),3)
    def test_14_negative_stock_dropped(self):
        bad=self.stock.copy();bad.loc[0,'close']=-40
        self.assertEqual(len(_price_table(bad,asof=DAY,benchmark=False)[0]),2)
    def test_15_benchmark_jump_rejected(self):
        bad=self.benchmark.copy();bad.loc[1,'close']=70000
        self.assertEqual(_price_table(bad,asof=DAY,benchmark=True)[1],'BENCHMARK_JUMP_RECHECK')
    def test_16_split_rejected(self):
        bad=self.stock.copy();bad.loc[1,'close']=900
        self.assertEqual(_price_table(bad,asof=DAY,benchmark=False)[1],'STOCK_SPLIT_OR_EXTREME_MOVE_RECHECK')
    def test_17_stock_code_preserved(self):
        self.assertEqual(self.frames['stock'].iloc[0]['股票代號'],'2330')
    def test_18_incomplete_no_benchmark(self):
        _,h=create_bundle(self.archives,self.stock,pd.DataFrame(),asof=DAY)
        self.assertEqual(h['status'],'INCOMPLETE_NO_VALID_BACKTEST')
    def test_19_one_archive_incomplete(self):
        _,h=create_bundle(self.archives[:1],self.stock,self.benchmark,asof=DAY)
        self.assertEqual(h['status'],'INCOMPLETE_NO_VALID_BACKTEST')
    def test_20_future_archive_not_accepted(self):
        late=archive(date(2026,10,19))['payload']
        _,h=create_bundle(self.archives+[late],self.stock,self.benchmark,asof=DAY)
        self.assertEqual(h['signal_days'],2)
    def test_21_same_day_conflict_ignored(self):
        another=archive(date(2026,10,15),code='2317')['payload']
        _,h=create_bundle(self.archives+[another],self.stock,self.benchmark,asof=DAY)
        self.assertEqual(h['signal_days'],1)
    def test_22_no_archive_directory(self):
        with TemporaryDirectory() as tmp:
            found,audit=collect_local_archives(Path(tmp),asof=DAY)
            self.assertEqual((len(found),audit['archive_files']),(0,0))
    def test_23_verified_local_archive(self):
        with TemporaryDirectory() as tmp:
            p=Path(tmp)/'godpick_h134_decision_vault';p.mkdir()
            b=archive(date(2026,10,14));(p/Path(b['path']).name).write_bytes(_json_bytes(b['payload']))
            found,audit=collect_local_archives(Path(tmp),asof=DAY)
            self.assertEqual((len(found),audit['archive_valid']),(1,1))
    def test_24_filename_mismatch_rejected(self):
        with TemporaryDirectory() as tmp:
            p=Path(tmp)/'godpick_h134_decision_vault';p.mkdir()
            b=archive(date(2026,10,14));(p/('H134_20261014_'+'0'*32+'.json')).write_bytes(_json_bytes(b['payload']))
            found,audit=collect_local_archives(Path(tmp),asof=DAY)
            self.assertEqual((len(found),audit['archive_rejected']),(0,1))
    def test_25_remote_happy_path(self):
        blobs={**self.blobs,'H136_health.json':_json_bytes({'schema':'H136-health-1','status':'UPDATED_VERIFIED_INTERNAL_SHA_RESEARCH_ONLY','run_day_tw':DAY.isoformat()})}
        _,meta=remote_cache(config={'owner':'test','repo':'stock','branch':'runtime-data','token':'dummy'}, transport=MockTransport(blobs),asof=DAY)
        self.assertEqual(meta['remote_readback'],'SHA256_VERIFIED_NOT_PIT')
    def test_26_remote_refuse_unready(self):
        blobs={**self.blobs,'H136_health.json':_json_bytes({'schema':'H136-health-1','status':'NOT_READY_OLD_DATA_PRESERVED','run_day_tw':DAY.isoformat()})}
        got,meta=remote_cache(config={'owner':'test','repo':'stock','branch':'runtime-data','token':'dummy'},transport=MockTransport(blobs),asof=DAY)
        self.assertEqual((len(got),meta['status']),(0,'LAST_SYNC_NOT_READY'))
    def test_27_remote_no_config(self):
        got,meta=remote_cache(config={}, transport=MockTransport({}),asof=DAY)
        self.assertEqual((len(got),meta['status']),(0,'GITHUB_AUTH_NOT_CONFIGURED'))
    def test_28_remote_latest_date_mismatch(self):
        blobs={**self.blobs,'H136_health.json':_json_bytes({'schema':'H136-health-1','status':'UPDATED_VERIFIED_INTERNAL_SHA_RESEARCH_ONLY','run_day_tw':'2026-10-15'})}
        _,meta=remote_cache(config={'owner':'test','repo':'stock','branch':'runtime-data','token':'dummy'}, transport=MockTransport(blobs),asof=DAY)
        self.assertEqual(meta['status'],'LATEST_RUN_AND_CACHE_DATE_MISMATCH')
    def test_29_no_data_preserves_previous_cache(self):
        with TemporaryDirectory() as tmp:
            root=Path(tmp);out=root/'data'/'h136';out.mkdir(parents=True)
            (out/'H136_manifest.json').write_bytes(b'last valid')
            result=sync(vault_root=root,market_root=root,output_root=root,asof=DAY)
            self.assertEqual(result['status'],'NOT_READY_OLD_DATA_PRESERVED')
            self.assertEqual((out/'H136_manifest.json').read_bytes(),b'last valid')
    def test_30_nightly_csv_local_sources(self):
        with TemporaryDirectory() as tmp:
            root=Path(tmp);folder=root/'godpick_h134_decision_vault';folder.mkdir()
            for d in (date(2026,10,14),date(2026,10,15)):
                b=archive(d);(folder/Path(b['path']).name).write_bytes(_json_bytes(b['payload']))
            data=root/'data'/'h116';data.mkdir(parents=True)
            self.stock.to_csv(data/'verified_ohlc.csv',index=False)
            self.benchmark.to_csv(data/'benchmark_ohlc.csv',index=False)
            result=sync(vault_root=root,market_root=root,output_root=root,asof=DAY)
            self.assertEqual(result['status'],'UPDATED_VERIFIED_INTERNAL_SHA_RESEARCH_ONLY')
            self.assertTrue((root/'data'/'h136'/'H136_manifest.json').exists())
    def test_31_no_private_holdings(self):
        self.assertNotIn('持股',self.blobs['H136_manifest.json'].decode('utf-8'))
    def test_32_empty_source(self):
        self.assertEqual(_price_table(pd.DataFrame(),asof=DAY,benchmark=True)[1],'SOURCE_EMPTY')
    def test_33_bad_source_schema(self):
        self.assertEqual(_price_table(pd.DataFrame([{'a':1}]),asof=DAY,benchmark=True)[1],'SOURCE_SCHEMA_MISSING')
    def test_34_oversized_source(self):
        big=pd.DataFrame({'date':['2026-10-14']*200001,'close':[1]*200001})
        self.assertEqual(_price_table(big,asof=DAY,benchmark=True)[1],'PRICE_ROW_LIMIT')
    def test_35_remote_missing_file(self):
        _,meta=remote_cache(config={'owner':'t','repo':'t','branch':'runtime-data','token':'dummy'},transport=MockTransport({}),asof=DAY)
        self.assertEqual(meta['status'],'REMOTE_FILE_UNAVAILABLE_404')
    def test_39_nonmatching_prices_not_ready(self):
        s=self.stock.copy(); s['date']=['2026-10-16']*3
        _,summary=create_bundle(self.archives,s,self.benchmark,asof=DAY)
        self.assertEqual(summary['status'],'INCOMPLETE_NO_VALID_BACKTEST')
    def test_40_h117_original_daily_archives(self):
        import gzip
        from godpick_h117_official_market_closure import make_official_day_manifest, MARKET_URLS
        with TemporaryDirectory() as tmp:
            root=Path(tmp)
            for day in ('2026-10-14','2026-10-15'):
                for src in ('TWSE','TPEX','TAIEX'):
                    if src == 'TAIEX':
                        rows=[{'code':'BENCHMARK','date':day,'close':46000 if day.endswith('14') else 46300}]
                    else:
                        rows=[{'code':str(i),'date':day,'close':1500 if day.endswith('14') else 1505}
                              for i in range(2000 if src=='TWSE' else 3000,2101 if src=='TWSE' else 3101)]
                        if src=='TWSE':rows.append({'code':'2330','date':day,'close':1500})
                    base=root/'data'/'h117'/'daily'/src;base.mkdir(parents=True,exist_ok=True)
                    normalized={'source':src,'day':day,'source_url':MARKET_URLS[src],'rows':rows}
                    payload=make_official_day_manifest(normalized,collected_at=day+'T20:00:00+08:00')
                    with gzip.open(base/(day+'.json.gz'),'wt',encoding='utf-8') as f:json.dump(payload,f,ensure_ascii=False)
            s,b,status=_load_h117_archived(root,self.archives,asof=DAY)
            self.assertEqual((len(s),len(b),status),(2,2,'H117_FULL_EOD_LOCAL_HASH_VERIFIED'))
    def test_41_corrupt_h117_archives_refused(self):
        import gzip
        from godpick_h117_official_market_closure import make_official_day_manifest, MARKET_URLS
        with TemporaryDirectory() as tmp:
            root=Path(tmp)
            day='2026-10-14'
            for src in ('TWSE','TPEX','TAIEX'):
                base=root/'data'/'h117'/'daily'/src;base.mkdir(parents=True,exist_ok=True)
                rows=[{'code':'2330' if src!='TAIEX' else 'BENCHMARK','date':day,'close':500}]*101
                payload=make_official_day_manifest({'source':src,'day':day,'source_url':MARKET_URLS[src],'rows':rows},collected_at=day+'T19:00:00+08:00')
                payload['content_sha256']='0'*64
                with gzip.open(base/(day+'.json.gz'),'wt',encoding='utf-8') as f:json.dump(payload,f)
            _,_,status=_load_h117_archived(root,self.archives,asof=DAY)
            self.assertEqual(status,'H117_NO_COMPLETE_VERIFIED_DAYS')

    def test_37_large_gh_raw_fallback(self):
        blobs={**self.blobs,'H136_health.json':_json_bytes({'schema':'H136-health-1','status':'UPDATED_VERIFIED_INTERNAL_SHA_RESEARCH_ONLY','run_day_tw':DAY.isoformat()})}
        class LargeMock(MockTransport):
            def get(self, url, **kwargs):
                filename=url.rsplit('/',1)[-1]
                if filename == 'H136_stock.csv' and 'raw+json' not in kwargs['headers']['Accept']:
                    return Response(200,{'encoding':'none','content':''})
                if filename == 'H136_stock.csv':
                    res=Response(200,{})
                    res.content=self.artifacts[filename]
                    return res
                return super().get(url, **kwargs)
        got,meta=remote_cache(config={'owner':'t','repo':'t','branch':'runtime-data','token':'dummy'},transport=LargeMock(blobs),asof=DAY)
        self.assertEqual(meta['status'],'RESEARCH_DATA_AVAILABLE')
    def test_38_raw_redirect_refused(self):
        blobs={**self.blobs,'H136_health.json':_json_bytes({'schema':'H136-health-1','status':'UPDATED_VERIFIED_INTERNAL_SHA_RESEARCH_ONLY','run_day_tw':DAY.isoformat()})}
        class RedirectMock(MockTransport):
            def get(self,url,**kwargs):
                if url.endswith('H136_stock.csv'):
                    if 'raw+json' in kwargs['headers']['Accept']:
                        self_observed=kwargs.get('allow_redirects')
                        assert self_observed is False
                        return Response(302,{})
                    return Response(200,{'encoding':'none','content':''})
                return super().get(url,**kwargs)
        got,meta=remote_cache(config={'owner':'t','repo':'t','branch':'runtime-data','token':'dummy'},transport=RedirectMock(blobs),asof=DAY)
        self.assertEqual((len(got),meta['status']),(0,'REMOTE_RAW_UNAVAILABLE'))

    def test_36_remote_stale(self):
        blobs={**self.blobs,'H136_health.json':_json_bytes({'schema':'H136-health-1','status':'UPDATED_VERIFIED_INTERNAL_SHA_RESEARCH_ONLY','run_day_tw':DAY.isoformat()})}
        _,meta=remote_cache(config={'owner':'t','repo':'t','branch':'runtime-data','token':'dummy'},transport=MockTransport(blobs),asof=date(2026,11,5))
        self.assertEqual(meta['status'],'CACHE_STALE_MORE_THAN_14_DAYS')


if __name__ == '__main__':
    unittest.main()
