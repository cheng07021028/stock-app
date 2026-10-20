# -*- coding: utf-8 -*-
"""H133 deterministic offline regression: all data synthetic, no network calls."""
from __future__ import annotations

from datetime import date
import unittest

import numpy as np
import pandas as pd

from godpick_h133_blackhorse_validation import (
    VERSION, _ci, _stock_prices, _benchmark, _normalize_signals, evaluate_blackhorse,
)

ASOF = date(2026, 10, 20)
DAYS = pd.bdate_range(end='2026-10-20', periods=145)


def make_benchmark(days=DAYS):
    return pd.DataFrame({'日期': list(days), '收盤價': [20000 * (1 + .0004 * i) for i in range(len(days))]})


def make_prices(days=DAYS):
    records = []
    for ticker, strength in [('2330', .0012), ('2317', .0005)]:
        for i, d in enumerate(days):
            records.append({'股票代號': ticker, '日期': d, '收盤價': round(100 * (1 + strength * i), 4)})
    return pd.DataFrame(records)


def make_signals(days=DAYS, scores=(90, 80)):
    rows = []
    for d in days[:-5]:
        for ticker, score in zip(('2330', '2317'), scores):
            rows.append({'股票代號': ticker, '訊號研究日': str(d.date()),
                         'H126權威層別': 'B｜待驗證觀察', 'H109決策權威': '',
                         'H126最後研究分': score,
                         'H123法人實證狀態': 'PROVEN_FLOW' if ticker == '2330' else 'UNVERIFIED',
                         'H124大戶實證狀態': 'VERIFIED_RISING' if ticker == '2330' else 'UNVERIFIED'})
    return pd.DataFrame(rows)


class H133(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.result = evaluate_blackhorse(make_signals(), make_prices(), make_benchmark(), asof=ASOF, adjusted_confirmed=True, top_k=1)

    def test_01_version(self):
        self.assertEqual(VERSION, 'V191-H133.0')

    def test_02_oos_retrospective_not_trade(self):
        self.assertEqual(self.result['狀態'], 'RETROSPECTIVE_RESEARCH_ONLY')
        self.assertIn('非成交', self.result['分割摘要'].iloc[0]['資料型態'])

    def test_03_horizons(self):
        self.assertEqual(list(self.result['樣本外績效']['期間']), ['T+1', 'T+3', 'T+5'])

    def test_04_top1_historical_score_only(self):
        self.assertEqual(set(self.result['逐筆觀測']['股票代號']), {'2330'})

    def test_05_oos_time_split(self):
        f = self.result['分割摘要'].iloc[0]
        self.assertGreater(f['訓練訊號交易日數_禁運後'], 40)
        self.assertGreater(f['樣本外訊號交易日數'], 20)
        self.assertEqual(f['禁運交易日'], 5)

    def test_06_positive_research_excess(self):
        s = self.result['樣本外績效']
        self.assertGreater(s.iloc[0]['日等權超額pp'], 0)
        self.assertIn('PIT', s.iloc[0]['研究判斷'])

    def test_07_ci_deterministic(self):
        self.assertEqual(_ci(np.linspace(-1, 1, 30)), _ci(np.linspace(-1, 1, 30)))

    def test_08_ci_short_insufficient(self):
        self.assertEqual(_ci(np.arange(19)), (None, None))

    def test_09_missing_signals_safe(self):
        result = evaluate_blackhorse(None, make_prices(), make_benchmark(), asof=ASOF)
        self.assertEqual(result['狀態'], 'NO_VALID_SIGNALS')

    def test_10_no_benchmark_refused(self):
        r = evaluate_blackhorse(make_signals(), make_prices(), None, asof=ASOF)
        self.assertEqual(r['狀態'], 'NO_BENCHMARK')

    def test_11_no_stock_prices_refused(self):
        r = evaluate_blackhorse(make_signals(), None, make_benchmark(), asof=ASOF)
        self.assertEqual(r['狀態'], 'NO_STOCK_PRICES')

    def test_12_benchmark_duplicate_conflict_rejected(self):
        data = make_benchmark()
        wrong = data.iloc[-1].copy()
        wrong['收盤價'] *= 1.2
        b, state = _benchmark(pd.concat([data, wrong.to_frame().T]), asof=ASOF)
        self.assertTrue(b.empty)
        self.assertEqual(state, 'CONFLICTING_BENCHMARK_CLOSE')

    def test_13_stock_conflict_rejected(self):
        bars = make_prices()
        wrong = bars.iloc[0].copy()
        wrong['收盤價'] += 9
        f, state = _stock_prices(pd.concat([bars, wrong.to_frame().T]), asof=ASOF)
        self.assertEqual(state, 'CONFLICTING_STOCK_CLOSE')
        self.assertTrue(f.empty)

    def test_14_weekend_signal_rejected(self):
        f, audit = _normalize_signals(pd.DataFrame([{'股票代號': '2330', '訊號研究日': '2026-10-17',
                                                    'H126權威層別': 'A｜認證'}]), asof=ASOF)
        self.assertTrue(f.empty)
        self.assertIn('INVALID_SIGNAL_DAY_OR_CODE', audit['資料狀態'].values)

    def test_15_future_signal_rejected(self):
        f, _ = _normalize_signals(pd.DataFrame([{'股票代號': '2330', '訊號研究日': '2026-10-21',
                                                'H126權威層別': 'A｜認證'}]), asof=ASOF)
        self.assertTrue(f.empty)

    def test_16_veto_cannot_promote(self):
        s = make_signals().copy()
        s['H109決策權威'] = 'FADE_VETO'
        r = evaluate_blackhorse(s, make_prices(), make_benchmark(), asof=ASOF)
        self.assertEqual(r['狀態'], 'NO_VALID_SIGNALS')

    def test_17_legacy_research_rejected(self):
        s = make_signals().copy()
        s['H126權威層別'] = '舊H102研究榜'
        r = evaluate_blackhorse(s, make_prices(), make_benchmark(), asof=ASOF)
        self.assertEqual(r['狀態'], 'NO_VALID_SIGNALS')

    def test_18_missing_adjustment_blocks_inference(self):
        r = evaluate_blackhorse(make_signals(), make_prices(), make_benchmark(), asof=ASOF)
        self.assertEqual(r['狀態'], 'ADJUSTMENT_UNVERIFIED')
        self.assertTrue(all('調整價未確認' in str(v) for v in r['樣本外績效']['研究判斷']))

    def test_19_snapshot_created_next_day_refused(self):
        s = make_signals().head(1).copy()
        s['訊號產生時間'] = '2026-10-20 14:00:00'
        accepted, audit = _normalize_signals(s, asof=ASOF)
        self.assertTrue(accepted.empty)
        self.assertIn('SNAPSHOT_CREATED_AFTER_SIGNAL_DAY', audit['資料狀態'].values)

    def test_20_self_declared_timestamp_doesnt_prove_pit(self):
        s = make_signals().head(1).copy()
        first_day = s.iloc[0]['訊號研究日']
        s['訊號產生時間'] = first_day + ' 16:00:00'
        accepted, audit = _normalize_signals(s, asof=ASOF)
        self.assertEqual(len(accepted), 1)
        self.assertIn('NOT_INDEPENDENTLY', accepted.iloc[0]['PIT證明狀態'])

    def test_21_missing_stock_bar_no_fill(self):
        p = make_prices()
        missing_day = str(DAYS[-10].date())
        p = p[~((p['股票代號'] == '2330') & (p['日期'] == DAYS[-9]))]
        r = evaluate_blackhorse(make_signals(), p, make_benchmark(), asof=ASOF, top_k=1)
        target = r['逐筆觀測'].query('股票代號 == "2330" and 訊號研究日 == @missing_day')
        self.assertEqual(target.iloc[0]['T+1狀態'], 'STOCK_BAR_MISSING_NO_FILL')

    def test_22_missing_top_stock_prevents_partial_cohort(self):
        p = make_prices()
        p = p[~((p['股票代號'] == '2317') & (p['日期'] == DAYS[-10]))]
        r = evaluate_blackhorse(make_signals(), p, make_benchmark(), asof=ASOF, top_k=2, adjusted_confirmed=True)
        self.assertLess(r['樣本外績效'].iloc[0]['涵蓋率%'], 100.)

    def test_23_no_future_horizon_filled(self):
        s = make_signals(days=DAYS).copy()
        s.loc[len(s)] = {'股票代號':'2330', '訊號研究日': str(ASOF), 'H126權威層別':'B',
                         'H109決策權威':'', 'H126最後研究分':90}
        r = evaluate_blackhorse(s, make_prices(), make_benchmark(), asof=ASOF)
        row = r['逐筆觀測'].query('訊號研究日 == "2026-10-20"').iloc[0]
        self.assertEqual(row['T+1狀態'], 'NOT_MATURED')

    def test_24_no_auto_promotion_fields(self):
        self.assertNotIn('正式買進', self.result['狀態'])
        self.assertNotIn('正式推薦', self.result['樣本外績效'].columns)

    def test_25_conflicting_signal_rejected_both(self):
        s = make_signals().head(2).copy()
        collision = s.iloc[0].copy()
        collision['H126最後研究分'] = 6
        normalized, audit = _normalize_signals(pd.concat([s, collision.to_frame().T]), asof=ASOF)
        self.assertEqual(len(normalized), 1)
        self.assertIn('CONFLICTING_SIGNAL_SNAPSHOT', audit['資料狀態'].values)

    def test_26_duplicate_identical_signal_not_double_counted(self):
        s = make_signals().head(1).copy()
        normalized, audit = _normalize_signals(pd.concat([s, s]), asof=ASOF)
        self.assertEqual(len(normalized), 1)

    def test_27_future_market_bars_not_used(self):
        extended_days = pd.bdate_range(end='2026-10-21', periods=146)
        b, _ = _benchmark(make_benchmark(extended_days), asof=ASOF)
        self.assertLessEqual(max(b['日期']), ASOF)

    def test_28_does_not_claim_daily_alpha_is_real_pnl(self):
        self.assertTrue(self.result['逐筆觀測']['PIT證明狀態'].str.contains('UNVERIFIED').all())
        self.assertIn('不能', self.result['說明'])

    def test_29_invalid_settings(self):
        r = evaluate_blackhorse(make_signals(), make_prices(), make_benchmark(), asof=ASOF, top_k=0)
        self.assertEqual(r['狀態'], 'INVALID_SETTINGS')

    def test_30_factors_not_verified_from_submitted_labels(self):
        self.assertTrue(self.result['因子分組']['來源宣稱狀態'].str.contains('未重查|未證明').all())


if __name__ == '__main__':
    unittest.main()
