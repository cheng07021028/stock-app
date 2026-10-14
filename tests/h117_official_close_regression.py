# -*- coding: utf-8 -*-
"""Synthetic/contract fixtures only; NOT a live TWSE/TPEx API availability test."""
import csv
import gzip
import json
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parent.parent))
from godpick_h117_official_market_closure import (
    normalize_official,resolve_freshness,persist_official_daily,load_official_days,
    export_verified_h116_csv,make_official_day_manifest,verify_manifest,TZ,fetch_sources,
)
from godpick_h116_daily_feedback import make_decision_snapshot,review_strategies,load_snapshot_files


def raw_stocks(source, day, step=0):
    raw=[]
    for i in range(120):
        code = '2382' if source=='TWSE' and i==0 else str((1000 if source=='TWSE' else 3000)+i)
        if source=='TWSE':
            r={'Date':day,'Code':code,'Name':'測試股份','TradeVolume':'2,000,000',
               'TradeValue':'123,000,000','OpeningPrice':str(100+step),
               'HighestPrice':str(105+step),'LowestPrice':str(98+step),
               'ClosingPrice':str(101+step),'Transaction':'1234'}
        else:
            r={'Date':day,'SecuritiesCompanyCode':code,'Open':str(100+step),
               'High':str(105+step),'Low':str(98+step),'Close':str(101+step),
               'TradingShares':'1,900,000','TransactionAmount':'108,000,000'}
        raw.append(r)
    return raw


def raw_taiex(day,step=0):
    return {'Date':day,'OpeningIndex':str(50000+step), 'HighestIndex':str(50100+step),
            'LowestIndex':str(49900+step),'ClosingIndex':str(50050+step)}


def day_batch(day,step=0):
    return {
      'TWSE':normalize_official('TWSE',raw_stocks('TWSE',day,step)),
      'TPEX':normalize_official('TPEX',raw_stocks('TPEX',day,step)),
      'TAIEX':normalize_official('TAIEX',[raw_taiex(day,step)]),
    }

class Resp:
    def __init__(self, status, obj=None):self.status_code=status;self.obj=obj
    def json(self):return self.obj

class HTTP:
    def __init__(self, payloads):self.payloads=payloads
    def get(self,url,**kw):
        for k,v in self.payloads.items():
            if k in url:
                if isinstance(v,int):return Resp(v)
                return Resp(200,v)
        raise AssertionError('unexpected URL')

class H117Tests(unittest.TestCase):
    def test_roc_dates_and_units(self):
        normalized=normalize_official('TWSE',raw_stocks('TWSE','1151014'))
        self.assertEqual(normalized['day'],'2026-10-14')
        self.assertEqual(normalized['rows'][0]['volume_shares'],2000000)
        self.assertNotIn('1000', [r['code'] for r in normalized['rows'] if r['code']=='0050'])
        self.assertEqual(len(normalized['rows']),120)
        idx=normalize_official('TAIEX',[raw_taiex('1151013'),raw_taiex('1151014')])
        self.assertEqual(idx['rows'][0]['code'],'BENCHMARK')
        self.assertEqual(idx['day'],'2026-10-14')

    def test_invalid_candles_and_duplicate_prevent_false_success(self):
        src=raw_stocks('TWSE','1151014')
        src[0]['HighestPrice']='90'  # OHLC impossible
        with self.assertRaises(ValueError):normalize_official('TWSE',src,minimum=120)
        src=raw_stocks('TWSE','1151014');src[1]['Code']='2382'
        with self.assertRaisesRegex(ValueError,'重複'):normalize_official('TWSE',src)
        src=raw_stocks('TWSE','1151014');src[2]['Date']='1151013'
        with self.assertRaisesRegex(ValueError,'多個交易日'):normalize_official('TWSE',src)

    def test_no_earlier_session_or_intraday_fake_update(self):
        b=day_batch('1151013')
        self.assertEqual(resolve_freshness({'sources':b,'errors':{}},now=datetime(2026,10,14,21,tzinfo=TZ))['status'],'NO_NEW_MARKET_SESSION')
        b['TWSE']=day_batch('1151012')['TWSE']
        self.assertEqual(resolve_freshness({'sources':b,'errors':{}},now=datetime(2026,10,14,21,tzinfo=TZ))['status'],'PARTIAL_MARKET_DAY')
        b['TWSE']=day_batch('1151014')['TWSE']
        self.assertEqual(resolve_freshness({'sources':b,'errors':{}},now=datetime(2026,10,14,21,tzinfo=TZ))['status'],'PARTIAL_MARKET_DAY')
        b=day_batch('1151014')
        self.assertEqual(resolve_freshness({'sources':b,'errors':{}},now=datetime(2026,10,14,14,tzinfo=TZ))['status'],'NOT_EOD_YET')
        self.assertEqual(resolve_freshness({'sources':b,'errors':{}},now=datetime(2026,10,14,20,tzinfo=TZ))['status'],'READY_FOR_IMMUTABLE_PUBLISH')
        b['TAIEX']=day_batch('1151015')['TAIEX']
        self.assertEqual(resolve_freshness({'sources':b,'errors':{}},now=datetime(2026,10,14,20,tzinfo=TZ))['status'],'FUTURE_DATE_REJECTED')

    def test_failed_exchange_http_prevents_publish(self):
        http=HTTP({'STOCK_DAY_ALL':raw_stocks('TWSE','1151014'),
                  'tpex_mainboard_daily_close_quotes':403,
                  'MI_5MINS_HIST':[raw_taiex('1151014')]})
        result=fetch_sources(http=http,attempts=1)
        self.assertIn('TPEX',result['errors'])
        self.assertEqual(resolve_freshness(result,now=datetime(2026,10,14,21,tzinfo=TZ))['status'],'INCOMPLETE_OFFICIAL_SOURCES')

    def test_immutability_and_corrupt_manifest(self):
        b=day_batch('1151014')
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            a=persist_official_daily(root,b,collected_at='2026-10-14T20:00:00+08:00',require_day='2026-10-14')
            self.assertEqual(a['new_files'],3)
            b2=day_batch('1151014')
            a=persist_official_daily(root,b2,collected_at='2026-10-14T21:00:00+08:00',require_day='2026-10-14')
            self.assertEqual(a['new_files'],0)
            b2['TWSE']['rows'][0]['close']+=0.1
            with self.assertRaisesRegex(ValueError,'IMMUTABLE_OFFICIAL_CONFLICT'):
                persist_official_daily(root,b2,collected_at='2026-10-14T22:00:00+08:00',require_day='2026-10-14')
            m=load_official_days(root)['TWSE']['2026-10-14']
            self.assertTrue(verify_manifest(m))
            p=root/'TWSE'/'2026-10-14.json.gz'
            m['rows'][0]['close']=0
            with gzip.open(p,'wt',encoding='utf-8') as f:json.dump(m,f)
            with self.assertRaisesRegex(ValueError,'遭修改'):
                load_official_days(root)

    def test_integration_with_immutable_h116_next_market_session(self):
        core={'version':'synthetic','run_id':'h117_fixture',
              'blackhorse_overview':[{'股票代號':'2382','H114主導策略':'H114_B',
                'H114入榜身分':'B_RESEARCH','latest_evidence_at':'2026-10-12T15:40:00+08:00'}]}
        snap=make_decision_snapshot(core,run_id='h117_fixture',decided_at='2026-10-12T21:00:00+08:00',market_day='2026-10-12')
        days=['1151012','1151013','1151014','1151015','1151016','1151019']
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            decisions=root/'data'/'h116'/'decisions'/'2026-10-12';decisions.mkdir(parents=True)
            (decisions/'h117_fixture.json').write_text(json.dumps(snap,ensure_ascii=False),encoding='utf-8')
            daily=root/'data'/'h117'/'daily'; out=root/'data'/'h116'
            for i,d in enumerate(days):
                b=day_batch(d,step=i)
                persist_official_daily(daily,b,collected_at='2026-10-19T20:00:00+08:00',require_day=b['TWSE']['day'])
            status=export_verified_h116_csv(daily,root/'data'/'h116'/'decisions',out)
            self.assertEqual(status['session_days'],6)
            with (out/'verified_ohlc.csv').open(encoding='utf-8-sig') as f: stocks=list(csv.DictReader(f))
            with (out/'benchmark_ohlc.csv').open(encoding='utf-8-sig') as f: benchmark=list(csv.DictReader(f))
            self.assertEqual(len(stocks),6)
            self.assertEqual(len(benchmark),6)
            review=review_strategies(load_snapshot_files(root/'data'/'h116'/'decisions'),stocks,benchmark=benchmark)
            horizons={row['horizon'] for row in review['details'] if row['data_status']=='OK'}
            self.assertEqual(horizons,{1,3,5})
            self.assertTrue(all(row['alpha_pp'] is not None for row in review['details'] if row['data_status']=='OK'))
            self.assertFalse(review['guardrails']['production_weights_changed'])
            # if a day has no official TPEx, that day must disappear from benchmark too
            (daily/'TPEX'/'2026-10-15.json.gz').unlink()
            status=export_verified_h116_csv(daily,root/'data'/'h116'/'decisions',out)
            self.assertIn('2026-10-15',status['missing_partial_market_days'])

if __name__=='__main__': unittest.main(verbosity=2)
