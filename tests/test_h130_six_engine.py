# -*- coding: utf-8 -*-
"""H130 safeguards: deterministic data, no live credentials or market access."""
import unittest
from datetime import date
import numpy as np
import pandas as pd

from godpick_h130_six_engine import (
    VERSION, normalize_prices, trade_ideas, technical_analysis, news_impacts,
    backtest_sma, portfolio_risk, journal_review, make_template,
)


def candles(code="2330", count=250):
    rng = pd.bdate_range("2025-01-01", periods=count)
    # Oscillating moving averages produce a few crossings deterministically.
    close = 100 + np.linspace(0, 4, count) + 15 * np.sin(np.arange(count) / 11)
    return pd.DataFrame({"股票代號": code, "日期": rng, "開盤價": close + 0.1,
                         "最高價": close + 2, "最低價": close - 2,
                         "收盤價": close, "成交量": 2000})


class H130SixEngine(unittest.TestCase):
    def test_01_version(self):
        self.assertTrue(VERSION.startswith("V191-H130"))

    def test_02_empty_opportunity_not_buy(self):
        self.assertEqual(trade_ideas(pd.DataFrame()).iloc[0]["資料狀態"], "NO_INPUT")

    def test_03_old_score_not_buy(self):
        inp = pd.DataFrame([{"股票代號": "3090", "股票名稱": "電商", "H102動態分": 99}])
        result = trade_ideas(inp, asof=date(2026,10,20))
        self.assertIn("未驗證", result.iloc[0]["研究層別"])
        self.assertIn("未授權", result.iloc[0]["進場參考價"])

    def test_04_real_veto_still_block(self):
        inp = pd.DataFrame([{"股票代號": "4979", "H109決策權威": "FADE_VETO",
                             "H126權威層別": "A｜認證黑馬研究（非買進)",
                             "H126認證資格": "PASS", "H126研究資訊截止日": "2026-10-20"}])
        self.assertEqual(trade_ideas(inp, asof=date(2026,10,20)).iloc[0]["研究層別"], "隔離觀察")

    def test_05_trusted_research_still_not_buy(self):
        inp = pd.DataFrame([{"股票代號": "2330", "H126權威層別": "A｜認證黑馬研究（非買進）",
                             "H126認證資格": "PASS", "H126研究資訊截止日": "2026-10-20"}])
        r = trade_ideas(inp, asof=date(2026,10,20)).iloc[0]
        self.assertIn("非買進", r["研究層別"])
        self.assertIn("未授權", r["進場參考價"])

    def test_06_technical_60_candles(self):
        r = technical_analysis(candles(count=80), asof=date(2025,4,22))
        self.assertEqual(len(r), 1)
        self.assertTrue("MA60" in r)
        self.assertEqual(r.iloc[0]["研究判斷"], "觀察／不構成買進授權")

    def test_07_mixed_ticker_rejected(self):
        x = pd.concat([candles("2330", 70), candles("2317", 70)], ignore_index=True)
        self.assertEqual(technical_analysis(x).iloc[0]["資料狀態"], "MULTI_TICKER")

    def test_08_future_prices_stripped(self):
        p = candles(count=80)
        r = normalize_prices(p, asof=date(2025, 1, 10))
        self.assertLessEqual(r["日期"].max().date(), date(2025,1,10))

    def test_09_unsourced_news_rejected(self):
        r = news_impacts(pd.DataFrame([{"日期": "2026-10-20", "標題": "消息", "來源網址": ""}]), asof=date(2026,10,20))
        self.assertEqual(r.iloc[0]["證據狀態"], "SOURCE_INVALID")

    def test_10_future_news_rejected(self):
        r = news_impacts(pd.DataFrame([{"日期": "2026-10-21", "標題": "消息", "來源網址": "https://x.com/a"}]), asof=date(2026,10,20))
        self.assertEqual(r.iloc[0]["證據狀態"], "FUTURE_LEAKAGE")

    def test_11_backtest_not_adjusted(self):
        s, t = backtest_sma(candles(), adjusted_confirmed=False)
        self.assertEqual(s.iloc[0]["資料狀態"], "PRICE_ADJUSTMENT_UNCONFIRMED")
        self.assertTrue(t.empty)

    def test_12_backtest_missing_open(self):
        x = candles().drop(columns="開盤價")
        s, t = backtest_sma(x, adjusted_confirmed=True)
        self.assertEqual(s.iloc[0]["資料狀態"], "MISSING_OPEN")

    def test_13_backtest_next_open(self):
        s, t = backtest_sma(candles(count=500), adjusted_confirmed=True, min_test_trades=1)
        if t.empty:
            self.fail("deterministic fixture should produce SMA cross")
        for _, tr in t.iterrows():
            self.assertGreater(tr["進場日期"], tr["訊號日期"])
            self.assertGreater(tr["出場日期"], tr["進場日期"])
        self.assertTrue("樣本類別" in t)

    def test_14_backtest_tiny_holdout_no_win_rate(self):
        s, _ = backtest_sma(candles(), adjusted_confirmed=True, min_test_trades=100)
        self.assertEqual(s.iloc[0]["資料狀態"], "TEST_SAMPLE_SMALL")

    def test_15_portfolio_no_holdings(self):
        s, corr = portfolio_risk(None)
        self.assertEqual(s.iloc[0]["資料狀態"], "NO_HOLDINGS")

    def test_16_portfolio_pressure_no_fabricated_beta(self):
        h = pd.DataFrame([{"股票代號":"2330", "產業":"半導體", "權重%":50},
                          {"股票代號":"2317", "產業":"電子", "權重%":20}])
        s, corr = portfolio_risk(h)
        rec = dict(zip(s["項目"], s["結果"]))
        self.assertAlmostEqual(rec["所有股票同步下跌20%簡易壓力損失%"], -14)
        self.assertTrue(corr.empty)

    def test_17_journal_no_transactions(self):
        s, t = journal_review(pd.DataFrame())
        self.assertEqual(s.iloc[0]["資料狀態"], "NO_TRADES")
        self.assertTrue(t.empty)

    def test_18_journal_do_not_invent_fees(self):
        data = pd.DataFrame([{"股票代號":"2330", "進場日":"2026-09-01", "出場日":"2026-09-04",
                              "進場價":100, "出場價":101, "股數":1000}])
        s,t = journal_review(data)
        self.assertIn("毛報酬", t.iloc[0]["報酬型態"])
        self.assertTrue(pd.isna(t.iloc[0]["已知淨損益"]))

    def test_19_journal_dates_valid(self):
        data = pd.DataFrame([{"股票代號":"2330", "進場日":"2026-09-04", "出場日":"2026-09-01",
                              "進場價":100, "出場價":101, "股數":1000}])
        s,t = journal_review(data)
        self.assertEqual(s.iloc[0]["資料狀態"], "NO_VALID_TRADES")

    def test_20_csv_templates_are_utf8_chinese(self):
        for t in ("行情","新聞","持股","交易"):
            self.assertIn("股票代號", make_template(t))

if __name__ == "__main__":
    unittest.main()
