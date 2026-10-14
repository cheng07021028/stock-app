# -*- coding: utf-8 -*-
"""H115 synthetic-only deterministic regression; NEVER a market performance claim."""
from __future__ import annotations
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from godpick_h115_pit_evidence import append_revenue,append_valuations,build_h114_evidence,evidence_digest
from godpick_h115_walkforward import evaluate_snapshots
from godpick_h114_parallel_alpha import _revenue,load_verified_evidence


class H115Tests(unittest.TestCase):
    def setUp(self):
        self.tmp=TemporaryDirectory(); self.root=Path(self.tmp.name)
        self.store=self.root/'data'/'h115'

    def tearDown(self):self.tmp.cleanup()

    def test_revenue_pit_and_18months(self):
        records=[]
        for m in range(18):
            year=2025+m//12
            month=m%12+1
            # 2025-01 through 2026-06 (synthetic); available only after reported month
            yearmonth=f'{year}{month:02d}'
            avail=(datetime(year,month,15) + timedelta(days=25)).strftime('%Y-%m-%d')
            records.append({'code':'2345','year_month':yearmonth,'revenue_twd':100000000+m*30000000,
                            'available_at':avail,'source':'TWSE','availability_basis':'PUBLISHED_VERIFIED',
                            'source_url':'https://mops.twse.com.tw/example-test-url'})
        info=append_revenue(self.store,records)
        self.assertEqual(info['received'],18)
        self.assertEqual(append_revenue(self.store,records)['inserted'],0)
        # The same monthly value observed later is not re-added every day.
        repeat=dict(records[0], available_at='2026-02-01T21:00:00+08:00')
        self.assertEqual(append_revenue(self.store,[repeat])['inserted'],0)
        cutoff='2026-08-31T23:00:00+08:00'
        pit=build_h114_evidence(self.root,cutoff)['2345']['H114月營收歷史']
        self.assertTrue(all(x['released_at'][:10] <= cutoff[:10] for x in pit))
        self.assertFalse(any(x['year_month']=='202610' for x in pit))
        self.assertGreater(len(pit),12)
        status=_revenue(pit,date(2026,8,31))
        self.assertEqual(status['H114營收資料狀態'],'VERIFIED_ASOF')
        self.assertEqual(status['H114營收創高'],'近12月新高')

    def test_same_day_peer_cohort_no_future_and_minimum_five(self):
        vals=[]
        for i in range(7):
            vals.append({'code':str(2300+i),'sector':'半導體','market':'上市',
                         'asof_date':'2026-10-12','available_at':'2026-10-13T20:00:00+08:00',
                         'pe':10 if i==0 else 20+i,'eps_ttm':3,
                         'source':'TWSE','availability_basis':'PUBLISHED_VERIFIED',
                         'source_url':'https://www.twse.com.tw/example-synthetic'})
        append_valuations(self.store,vals)
        self.assertNotIn('2300',build_h114_evidence(self.root,'2026-10-12T23:00:00+08:00'))
        row=build_h114_evidence(self.root,'2026-10-13T23:00:00+08:00')['2300']
        self.assertEqual(row['H115同業有效樣本數'],6)
        self.assertEqual(row['H114本益比實值'],10)
        self.assertTrue(row['H114同業本益比中位數_官方']>20)
        self.assertEqual(row['H114估值資料日期'],'2026-10-12')
        self.assertEqual(load_verified_evidence(self.root,asof='2026-10-13')['2300']['H114本益比實值'],10)
        # One older peer: fewer than 5 same-date peers, no computed median.
        vals2=[{'code':'9999','sector':'光學','market':'上市','asof_date':'2026-10-13',
                'available_at':datetime.now(timezone(timedelta(hours=8))).isoformat(timespec='seconds'),'pe':8,'eps_ttm':5,
                'source':'TWSE','availability_basis':'FIRST_OBSERVED'}]
        append_valuations(self.store,vals2)
        self.assertNotIn('H114同業本益比中位數_官方', build_h114_evidence(self.root,'2026-10-14')['9999'])

    def test_import_rejects_ambiguous_and_future(self):
        good={'code':'2382','year_month':'202609','revenue_twd':120000000,
              'available_at':datetime.now(timezone(timedelta(hours=8))).isoformat(timespec='seconds'),'source':'TWSE','availability_basis':'FIRST_OBSERVED'}
        self.assertEqual(append_revenue(self.store,[good])['inserted'],1)
        self.assertEqual(len(build_h114_evidence(self.root,'2026-10-11').get('2382',{})),0)
        wrong=dict(good,available_at='2026-11-01')
        with self.assertRaises(ValueError):append_revenue(self.store,[wrong])
        wrong=dict(good,availability_basis='PUBLISHED_VERIFIED')
        with self.assertRaises(ValueError):append_revenue(self.store,[wrong])
        wrong=dict(good, available_at='2025-01-05')
        with self.assertRaises(ValueError):append_revenue(self.store,[wrong])

    def test_backtest_next_open_no_future_and_benchmark(self):
        # All fabricated; next session open 100, closes 104, 106, 108.
        days=['2026-10-12','2026-10-13','2026-10-14','2026-10-15','2026-10-16','2026-10-19']
        closes=[100,104,105,106,107,108]
        prices=[dict(code='2468',date=d,open=(100 if i==1 else closes[i]),
                      high=closes[i]+2,low=(99 if i==1 else closes[i]-2),close=closes[i]) for i,d in enumerate(days)]
        # fix OHLC for bullish synthetic shape
        prices[1]['high']=105;prices[1]['low']=99
        bm=[dict(code='BENCHMARK',date=d,open=200,high=205,low=197,close=200) for d in days]
        signals=[{'event_id':'s1','code':'2468','decided_at':'2026-10-12T20:00:00+08:00',
                  'latest_evidence_at':'2026-10-12T19:00:00+08:00','strategy':'H114','tier':'B_ENHANCED_RESEARCH_ONLY','score':'80'},
                 {'event_id':'s2','code':'2468','decided_at':'2026-10-12T20:00:00+08:00',
                  'latest_evidence_at':'2026-10-13T00:00:00+08:00','strategy':'H113','tier':'A_CERTIFIED_RESEARCH'}]
        out=evaluate_snapshots(signals,prices,benchmark=bm,slippage_bps=0,buy_commission_bps=0,sell_commission_bps=0,sell_tax_bps=0)
        rr={r['horizon']:r for r in out['details'] if r['event_id']=='s1'}
        self.assertAlmostEqual(rr[1]['net_return_pct'],4)
        self.assertAlmostEqual(rr[3]['net_return_pct'],6)
        self.assertAlmostEqual(rr[5]['net_return_pct'],8)
        self.assertEqual(rr[1]['mfe_pct'],5)
        self.assertEqual(rr[1]['mae_pct'],-1)
        self.assertEqual(rr[3]['alpha_pp'],6)
        self.assertEqual(next(r for r in out['details'] if r['event_id']=='s2')['data_status'],'UNVERIFIED_ASOF_SNAPSHOT')
        self.assertTrue(all(x['confidence']=='EXPLORATORY_ONLY' for x in out['summary']))

    def test_costs_reduce_backtest_return(self):
        prices=[{'code':'1010','date':d,'open':100,'high':103,'low':99,'close':101}
                for d in ['2026-10-13','2026-10-14','2026-10-15','2026-10-16','2026-10-19','2026-10-20']]
        signal=[{'code':'1010','decided_at':'2026-10-12T20:00:00+08:00',
                 'latest_evidence_at':'2026-10-12T20:00:00+08:00','strategy':'H114'}]
        out=evaluate_snapshots(signal,prices)
        self.assertLess(out['details'][0]['net_return_pct'],out['details'][0]['gross_return_pct'])
        self.assertEqual(evidence_digest(self.root)['files']['revenue.jsonl']['rows'],0)

if __name__=='__main__':unittest.main(verbosity=2)
