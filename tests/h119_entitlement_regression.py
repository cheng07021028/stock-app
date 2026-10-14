# -*- coding: utf-8 -*-
"""H119 offline synthetic tests. They do NOT prove actual official feed connectivity."""
import unittest, sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from godpick_h115_walkforward import evaluate_snapshots
from godpick_h116_daily_feedback import make_decision_snapshot,review_strategies
from godpick_h119_total_return import load_adjustments,assess_entitlement_window

DAYS=['2026-09-07','2026-09-08','2026-09-09','2026-09-10','2026-09-11','2026-09-14']
URL='https://www.twse.com.tw/exchangeReport/TWT49U?response=json'
STAMP='2026-10-01T10:00:00+08:00'

def coverage():return [dict(market='TWSE',date=d,source_url=URL,verified_at=STAMP,status='OFFICIAL_COMPLETE') for d in DAYS]
def action(typ='CASH_DIVIDEND',d='2026-09-10'):
 return dict(code='2382',ex_date=d,action_type=typ,source_url=URL,verified_at=STAMP,verification_status='OFFICIAL_DOCUMENTED')
def adj(cash=2,ratio=0,day='2026-09-10'):
 return dict(code='2382',market='TWSE',ex_date=day,cash_dividend_per_share=str(cash),stock_bonus_ratio=str(ratio),source_url=URL,verified_at=STAMP,verification_status='OFFICIAL_NUMERIC_REVIEWED')
def prices():
 return [dict(code='2382',date=d,open=(100+i),high=(103+i),low=99+i,close=(101+i),exchange='TWSE') for i,d in enumerate(DAYS)]
def bm():return [dict(code='BENCHMARK',date=d,open=100,high=103,low=99,close=101,exchange='TAIEX') for d in DAYS]
def signal():return dict(code='2382',event_id='abc',strategy='H114_B',tier='RESEARCH',decided_at='2026-09-07T20:00:00+08:00',latest_evidence_at='2026-09-07T18:00:00+08:00')
def evaluate(actions=(),adjustments=(),cov=None,p=None):
 return evaluate_snapshots([signal()],p or prices(),benchmark=bm(),h118_policy='STRICT',h118_actions=list(actions),h118_coverage=coverage() if cov is None else cov,h119_policy='ENTITLEMENT',h119_adjustments=list(adjustments),buy_commission_bps=0,sell_commission_bps=0,sell_tax_bps=0,slippage_bps=0)
class H119Tests(unittest.TestCase):
 def test_no_actions_compatible_verified_price_only(self):
  rows=evaluate()['details'];self.assertEqual({r['data_status'] for r in rows},{'OK'});self.assertEqual(len(evaluate()['summary']),3)
 def test_cash_dividend_is_receivable_not_approved_alpha(self):
  rows=evaluate([action()], [adj()])['details']; by={x['horizon']:x for x in rows}
  self.assertEqual(by[1]['data_status'],'OK')
  self.assertEqual(by[3]['data_status'],'OK_CASH_ENTITLEMENT_ESTIMATE')
  self.assertIsNone(by[3]['alpha_pp']);self.assertEqual(by[3]['cash_entitlement_per_initial_share'],2)
  self.assertAlmostEqual(by[3]['gross_return_pct'],(106/101-1)*100,places=3)
 def test_stock_dividend_unsettled_not_certified(self):
  rows=evaluate([action('STOCK_DIVIDEND')], [adj(cash=0,ratio=.1)])['details'];by={r['horizon']:r for r in rows}
  self.assertEqual(by[3]['data_status'],'STOCK_RIGHTS_DELIVERY_UNVERIFIED')
  self.assertIsNone(by[3]['net_return_pct']);self.assertGreater(by[3]['indicative_total_return_pct'],0)
 def test_both_components_require_both_h118_events(self):
  res=evaluate([action()], [adj(cash=1,ratio=.1)])
  self.assertEqual(res['details'][1]['data_status'],'ACTION_EVIDENCE_MISMATCH')
  res=evaluate([action(),action('STOCK_DIVIDEND')], [adj(cash=1,ratio=.1)])
  self.assertEqual(res['details'][1]['data_status'],'STOCK_RIGHTS_DELIVERY_UNVERIFIED')
 def test_rights_issue_quarantine(self):
  self.assertEqual(evaluate([action('RIGHTS_ISSUE')],[])['details'][1]['data_status'],'UNSUPPORTED_CORPORATE_ACTION')
 def test_missing_coverage_rejected(self):
  self.assertEqual(evaluate(cov=[])['details'][0]['data_status'],'CORPORATE_ACTION_COVERAGE_UNVERIFIED')
 def test_missing_cash_component_rejected(self):
  self.assertEqual(evaluate([action()],[])['details'][1]['data_status'],'CORPORATE_COMPONENTS_UNVERIFIED')
 def test_ex_date_at_entry_not_earned(self):
  res=evaluate([action(d='2026-09-08')],[adj(day='2026-09-08')]);self.assertTrue(all(x['data_status']=='OK' for x in res['details']))
 def test_market_mismatch_across_trading_days_is_blocked(self):
  p=prices();p[2]['exchange']='TPEX'
  self.assertEqual(evaluate(p=p)['details'][1]['data_status'],'MARKET_PROVENANCE_CONFLICT')
 def test_official_numeric_gate_and_conflicts(self):
  self.assertEqual(len(load_adjustments([adj()])),1)
  with self.assertRaises(ValueError):load_adjustments([adj(),adj()])
  v=adj();v['verification_status']='UNVERIFIED'
  with self.assertRaises(ValueError):load_adjustments([v])
  v=adj();v['source_url']='https://evil.example.net/a'
  with self.assertRaises(ValueError):load_adjustments([v])
  v=adj();v['cash_dividend_per_share']='abc'
  with self.assertRaises(ValueError):load_adjustments([v])
  v=adj();v['rights_subscription_ratio']='0.01'
  with self.assertRaises(ValueError):load_adjustments([v])
 def test_legacy_h118_does_not_adjust(self):
  res=evaluate_snapshots([signal()],prices(),benchmark=bm(),h118_policy='STRICT',h118_actions=[action()],h118_coverage=coverage(),buy_commission_bps=0,sell_commission_bps=0,sell_tax_bps=0,slippage_bps=0)
  self.assertEqual(res['details'][1]['data_status'],'CORPORATE_ACTION_QUARANTINE')
 def test_h116_never_auto_approves_cash_estimate(self):
  core={'run_id':'h119demo','blackhorse_overview':[dict(股票代號='2382',H114主導策略='H114_B',latest_evidence_at='2026-09-07T18:00:00+08:00')]}
  snap=make_decision_snapshot(core,run_id='h119demo',decided_at='2026-09-07T20:00:00+08:00',market_day='2026-09-07')
  result=review_strategies([snap],prices(),benchmark=bm(),h118_policy='STRICT',h118_actions=[action()],h118_coverage=coverage(),h119_policy='ENTITLEMENT',h119_adjustments=[adj()])
  self.assertEqual(len(result['h119_cash_research_only']),2)
  self.assertTrue(all(x['auto_apply'] is False for x in result['recommendations']))
  self.assertTrue(all(x['horizon']==1 for x in result['summary']))
if __name__=='__main__':unittest.main(verbosity=2)
