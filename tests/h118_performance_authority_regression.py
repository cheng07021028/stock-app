# -*- coding: utf-8 -*-
"""H118 offline *synthetic* evidence quality regression, not real investment performance."""
from __future__ import annotations
import csv
import io
import tempfile
import unittest
from pathlib import Path
from datetime import date,timedelta
import sys
sys.path.insert(0,str(Path(__file__).resolve().parent.parent))
from godpick_h118_performance_authority import (
    assess_window,load_actions,load_coverage,read_csv,manifest,
    CSV_ACTION_COLUMNS,CSV_COVERAGE_COLUMNS)
from godpick_h115_walkforward import evaluate_snapshots
from godpick_h116_daily_feedback import make_decision_snapshot,review_strategies
from godpick_h117_official_market_closure import CSV_COLUMNS, _csv_bytes

DAYS=['2026-09-07','2026-09-08','2026-09-09','2026-09-10','2026-09-11','2026-09-14']
URL='https://www.twse.com.tw/zh/page/trading/exchange/TWT48U.html'
TPEX_URL='https://www.tpex.org.tw/zh-tw/announce/market/ex/announce.html'
STAMP='2026-09-15T09:00:00+08:00'


def coverage(market='TWSE',days=DAYS):
    return [dict(market=market,date=d,source_url=URL if market=='TWSE' else TPEX_URL,
                 verified_at=STAMP,status='OFFICIAL_COMPLETE') for d in days]


def action(ex_date='2026-09-10',code='2382',typ='CASH_DIVIDEND'):
    return [dict(code=code,ex_date=ex_date,action_type=typ,
                 source_url=URL,verified_at=STAMP,verification_status='OFFICIAL_DOCUMENTED')]


def prices(code='2382',market='TWSE',closes=None):
    closes=closes or [100,102,103,104,105,106]
    a=[]
    for i,d in enumerate(DAYS):
        c=closes[i];a.append(dict(code=code,date=d,open=c-1,high=c+2,
                                  low=c-3,close=c,exchange=market))
    return a


def bench():
    return [dict(code='BENCHMARK',date=d,open=100,high=105,low=96,close=101,exchange='TAIEX') for d in DAYS]


def signal(code='2382'):
    return dict(event_id='e1',code=code,strategy='H114_B',tier='B_RESEARCH',
                decided_at='2026-09-07T20:00:00+08:00',
                latest_evidence_at='2026-09-07T19:00:00+08:00')


def evaluate(*,a=(),cov=(),p=None,s=None,b=None):
    return evaluate_snapshots(s or [signal()],p if p is not None else prices(),benchmark=b if b is not None else bench(),
        h118_policy='STRICT',h118_actions=a,h118_coverage=cov,
        buy_commission_bps=0,sell_commission_bps=0,sell_tax_bps=0,slippage_bps=0)


class H118Tests(unittest.TestCase):
    def test_no_coverage_never_claims_certified_returns(self):
        result=evaluate()
        self.assertEqual(result['h118_policy'],'STRICT')
        self.assertEqual({x['data_status'] for x in result['details']},{'CORPORATE_ACTION_COVERAGE_UNVERIFIED'})
        self.assertEqual(result['summary'],[])
        self.assertTrue(all(x['net_return_pct'] is None for x in result['details']))

    def test_complete_market_and_company_action_check_allows_price_only(self):
        result=evaluate(cov=coverage())
        self.assertEqual({x['data_status'] for x in result['details']},{'OK'})
        self.assertEqual({x['h118_quality_status'] for x in result['details']},
                         {'CERTIFIED_UNADJUSTED_PRICE_WINDOW'})
        self.assertEqual({x['horizon'] for x in result['details']},{1,3,5})
        self.assertEqual(len(result['summary']),3)

    def test_ex_dividend_in_holding_window_quarantines_later_horizons(self):
        result=evaluate(a=action('2026-09-10'),cov=coverage())
        by={x['horizon']:x for x in result['details']}
        self.assertEqual(by[1]['data_status'],'OK')
        self.assertEqual(by[3]['data_status'],'CORPORATE_ACTION_QUARANTINE')
        self.assertEqual(by[5]['data_status'],'CORPORATE_ACTION_QUARANTINE')
        self.assertIsNone(by[5]['net_return_pct'])
        self.assertEqual(len(result['summary']),1)

    def test_ex_day_entry_no_unearned_dividend_credit(self):
        result=evaluate(a=action('2026-09-08'),cov=coverage())
        self.assertEqual({x['data_status'] for x in result['details']},{'OK'})

    def test_extreme_price_jump_quarantined_without_split_inference(self):
        altered=prices(closes=[100,102,104,130,130,129])
        altered[3].update(open=130,high=132,low=128)
        altered[4].update(open=130,high=132,low=128)
        altered[5].update(open=129,high=132,low=127)
        result=evaluate(p=altered,cov=coverage())
        by={x['horizon']:x for x in result['details']}
        self.assertEqual(by[1]['data_status'],'OK')
        self.assertEqual(by[3]['data_status'],'UNEXPLAINED_PRICE_DISCONTINUITY')
        self.assertEqual(by[5]['data_status'],'UNEXPLAINED_PRICE_DISCONTINUITY')

    def test_tpex_stock_not_certified_by_twse_coverage(self):
        result=evaluate(p=prices('2382','TPEX'),cov=coverage('TWSE'))
        self.assertTrue(all(x['data_status']=='CORPORATE_ACTION_COVERAGE_UNVERIFIED' for x in result['details']))
        result=evaluate(p=prices('2382','TPEX'),cov=coverage('TPEX'))
        self.assertTrue(all(x['data_status']=='OK' for x in result['details']))

    def test_absent_market_provenance_is_never_certified(self):
        p=prices()
        for x in p:x.pop('exchange')
        result=evaluate(p=p,cov=coverage())
        self.assertEqual({x['data_status'] for x in result['details']},{'MARKET_PROVENANCE_UNKNOWN'})

    def test_missing_benchmark_never_certified(self):
        result=evaluate(cov=coverage(),b=[])
        # When benchmark is omitted, no official session calendar can be proven.
        self.assertEqual({x['data_status'] for x in result['details']},{'UNVERIFIED_MARKET_SESSIONS'})

    def test_official_inputs_validation_and_hash(self):
        self.assertEqual(len(load_actions(action())),1)
        announced=action(ex_date='2026-10-30')
        announced[0]['verified_at']='2026-09-15T09:00:00+08:00'
        self.assertEqual(len(load_actions(announced)),1)
        self.assertEqual(len(load_coverage(coverage())),6)
        self.assertEqual(len(manifest(load_actions(action()),load_coverage(coverage()))['sha256']),64)
        with self.assertRaises(ValueError):load_actions(action(typ='UNKNOWN'))
        with self.assertRaises(ValueError):load_actions(action()+action())
        with self.assertRaises(ValueError):load_coverage(coverage()+coverage()[:1])
        wrong=action();wrong[0]['source_url']='https://evil.example.com/foo'
        with self.assertRaises(ValueError):load_actions(wrong)
        bad=coverage();bad[0]['verified_at']='2026-09-06T20:00:00+08:00'
        with self.assertRaises(ValueError):load_coverage(bad)

    def test_review_group_excludes_provisional(self):
        core={'run_id':'case118','blackhorse_overview':[{
            '股票代號':'2382','H114主導策略':'H114_B',
            'latest_evidence_at':'2026-09-07T18:00:00+08:00'}]}
        snap=make_decision_snapshot(core,run_id='case118',decided_at='2026-09-07T21:00:00+08:00',market_day='2026-09-07')
        unverified=review_strategies([snap],prices(),benchmark=bench(),h118_policy='STRICT')
        self.assertEqual(len(unverified['summary']),0)
        self.assertEqual(unverified['recommendations'],[])
        self.assertEqual(unverified['guardrails']['production_weights_changed'],False)
        verified=review_strategies([snap],prices(),benchmark=bench(),h118_policy='STRICT',h118_coverage=coverage())
        self.assertEqual(len(verified['summary']),3)
        self.assertTrue(all(x['auto_apply'] is False for x in verified['recommendations']))

    def test_h117_export_includes_exchange_provenance(self):
        import csv
        rows=[dict(code='2382',date='2026-09-07',open=100,high=102,low=99,close=101,exchange='TWSE')]
        txt=_csv_bytes(rows).decode('utf-8-sig')
        self.assertIn('exchange',next(csv.DictReader(io.StringIO(txt))).keys())

if __name__=='__main__':unittest.main(verbosity=2)
