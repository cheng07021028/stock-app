# -*- coding: utf-8 -*-
import sys,unittest
from pathlib import Path
from datetime import date,timedelta
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from godpick_h120_strategy_validation import rolling_comparison

SESSIONS=[(date(2026,1,1)+timedelta(days=x)).isoformat() for x in range(220) if (date(2026,1,1)+timedelta(days=x)).weekday()<5]

def row(day,strategy,alpha,h=3,status='OK',basis='PRICE_ONLY_NO_ACTION',code='2382'):
    return dict(decision_date=day,code=code,strategy=strategy,tier='RESEARCH',horizon=str(h),data_status=status,
                return_basis=basis,alpha_pp=alpha,net_return_pct=alpha+.5)
class H120StrategyTests(unittest.TestCase):
 def test_empty_no_calendar_returns_no_fabrication(self):
    self.assertEqual(rolling_comparison([],market_sessions=[])['status'],'NO_VERIFIED_MARKET_CALENDAR')
 def test_no_benchmark_or_dividend_estimations_never_count(self):
    days=SESSIONS[-30:]
    rows=[row(d,'H113_LEGACY',0.3) for d in days]+[row(d,'H114_A',.5,status='OK_CASH_ENTITLEMENT_ESTIMATE') for d in days]
    result=rolling_comparison(rows,market_sessions=SESSIONS)
    self.assertTrue(all(x['status']=='NO_CANDIDATE_EVIDENCE' for x in result['windows']))
 def test_paired_day_matching_and_sparse_rejection(self):
    days=SESSIONS[-12:]
    rows=[row(d,'H113_LEGACY',0.1) for d in days]+[row(d,'H114_B',.7) for d in days[::2]]
    result=rolling_comparison(rows,market_sessions=SESSIONS)
    got=[x for x in result['windows'] if x['window_sessions']==60 and x['horizon']==3][0]
    self.assertEqual(got['paired_days'],6)
    self.assertEqual(got['status'],'INSUFFICIENT_PAIRED_TRADING_DAYS')
 def test_60_120_session_pairs_reproducible_not_auto_approve(self):
    days=SESSIONS[-100:]
    rows=[row(d,'H113_LEGACY',0.2) for d in days]+[row(d,'H114_C',0.7) for d in days]
    a=rolling_comparison(rows,market_sessions=SESSIONS)
    b=rolling_comparison(rows,market_sessions=SESSIONS)
    self.assertEqual(a,b)
    ok=[x for x in a['windows'] if x['window_sessions']==60 and x['horizon']==3][0]
    self.assertEqual(ok['paired_days'],60)
    self.assertEqual(ok['paired_mean_alpha_diff_pp'],.5)
    self.assertEqual(ok['status'],'ELIGIBLE_FOR_EXPLORATORY_HUMAN_REVIEW_NOT_APPROVED')
    self.assertFalse(ok['auto_apply'])
 def test_de_dup_same_day_symbol_strategy(self):
    days=SESSIONS[-30:]
    base=[row(d,'H113_LEGACY',0.1) for d in days]
    others=[row(d,'H114_D',0.7) for d in days]
    others+= [row(days[0],'H114_D',900)]  # must not bias duplicate
    result=rolling_comparison(base+others,market_sessions=SESSIONS)
    got=[x for x in result['windows'] if x['window_sessions']==60 and x['horizon']==3][0]
    self.assertLess(got['paired_mean_alpha_diff_pp'],10)
if __name__=='__main__':unittest.main(verbosity=2)
