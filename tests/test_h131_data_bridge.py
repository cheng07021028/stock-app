# -*- coding: utf-8 -*-
"""Offline H131 deterministic tests; never reach network, GitHub or Cloud."""
from __future__ import annotations

import gzip
import io
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from godpick_h131_data_bridge import (
    VERSION, MAX_BATCH, read_h126_workbook, verified_calendar,
    candidate_evidence_audit, batch_history, historical_forward_observations,
)
from godpick_h117_official_market_closure import make_official_day_manifest, MARKET_URLS


D = date(2026, 10, 20)


def history(code="2330", days=160):
    dates = pd.bdate_range("2026-02-25", periods=days)
    x = np.arange(days)
    return pd.DataFrame({"股票代號": code, "日期": dates,
                         "開盤價": 100 + x * .1, "最高價": 102 + x * .1,
                         "最低價": 99 + x * .1, "收盤價": 101 + x * .1,
                         "成交量": 100000 + x})


def small():
    dates = pd.to_datetime(["2026-10-15", "2026-10-16", "2026-10-19", "2026-10-20"])
    return pd.DataFrame({"股票代號": ["2330"]*4, "日期": dates,
                         "開盤價": [99, 100, 101, 104], "最高價": [101, 104, 106, 108],
                         "最低價": [98, 99, 100, 103], "收盤價": [100, 103, 105, 107]})


def decisions(day="2026-10-15", veto=""):
    return pd.DataFrame([{"股票代號": "2330", "H126權威層別": "B｜待驗證觀察",
                          "H126研究資訊截止日": day, "H109決策權威": veto,
                          "H123法人實證狀態": "PROVEN_FLOW",
                          "H124大戶實證狀態": "VERIFIED_RISING"}])


class H131Bridge(unittest.TestCase):
    def test_01_version(self):
        self.assertTrue(VERSION.startswith("V191-H131"))

    def test_02_refuse_unrecognized_csv_schema(self):
        f, status = read_h126_workbook("股票代號,分數\n2330,99\n".encode(), "a.csv")
        self.assertTrue(f.empty)
        self.assertEqual(status, "H126_SCHEMA_MISSING")

    def test_03_read_h126_csv(self):
        f, status = read_h126_workbook("股票代號,H126權威層別\n2330,B\n".encode(), "a.csv")
        self.assertEqual(f.iloc[0]["股票代號"], "2330")
        self.assertTrue(status.startswith("H126_AUTHORITY"))

    def test_04_xlsx_prefers_manager_over_old(self):
        data = io.BytesIO()
        with pd.ExcelWriter(data, engine="openpyxl") as out:
            pd.DataFrame({"股票代號": ["3090"], "H102分數": [99]}).to_excel(out, sheet_name="09_舊排名", index=False)
            decisions().to_excel(out, sheet_name="00_H126主管決策", index=False)
        f, src = read_h126_workbook(data.getvalue(), "workbook.xlsx")
        self.assertEqual(f.iloc[0]["股票代號"], "2330")
        self.assertTrue(src.endswith("00_H126主管決策"))

    def test_05_xlsx_without_h126_rejected(self):
        data = io.BytesIO()
        with pd.ExcelWriter(data, engine="openpyxl") as out:
            pd.DataFrame({"股票代號": ["3090"]}).to_excel(out, sheet_name="01_舊榜", index=False)
        f, src = read_h126_workbook(data.getvalue(), "s.xlsx")
        self.assertTrue(f.empty)
        self.assertEqual(src, "NO_H126_AUTHORITY_SHEET")

    def test_06_limit_prevents_memory_flood(self):
        content = "股票代號,H126權威層別\n" + "".join(f"{i+1000},B\n" for i in range(300))
        f, src = read_h126_workbook(content.encode(), "x.csv")
        self.assertTrue(f.empty)
        self.assertEqual(src, "CANDIDATE_LIMIT_EXCEEDED")

    def test_07_evidence_hard_veto(self):
        f = candidate_evidence_audit(decisions(day=D.isoformat(), veto="FADE_VETO"), asof=D)
        self.assertIn("風控否決", f.iloc[0]["查核結論"])

    def test_08_evidence_still_uncertified_even_if_labels_proven(self):
        f = candidate_evidence_audit(decisions(day=D.isoformat()), asof=D)
        self.assertIn("尚須", f.iloc[0]["查核結論"])

    def test_09_evidence_old_day(self):
        f = candidate_evidence_audit(decisions(), asof=D)
        self.assertIn("研究日不符", f.iloc[0]["查核結論"])

    def test_10_empty_calendar_does_not_guess_weekday(self):
        with tempfile.TemporaryDirectory() as td:
            r = verified_calendar(Path(td), asof=D)
        self.assertEqual(r["status"], "NO_VERIFIED_H117")
        self.assertEqual(r["official_market_day"], "UNKNOWN")

    def test_11_verified_three_sources(self):
        with tempfile.TemporaryDirectory() as td:
            for src in ("TWSE", "TPEX", "TAIEX"):
                rows = [{"code": str(1000+i)} for i in range(100 if src != "TAIEX" else 1)]
                payload = make_official_day_manifest({"source": src, "day": str(D),
                                                      "source_url": MARKET_URLS[src], "rows": rows},
                                                     collected_at="2026-10-20T16:05:00+08:00")
                folder = Path(td) / "data/h117/daily" / src
                folder.mkdir(parents=True)
                with gzip.open(folder / f"{D}.json.gz", "wt", encoding="utf-8") as f:
                    json.dump(payload, f, ensure_ascii=False)
            result = verified_calendar(Path(td), asof=D)
            self.assertEqual(result["status"], "VERIFIED_ASOF")
            self.assertEqual(result["official_market_day"], str(D))

    def test_12_modified_official_archive_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            for src in ("TWSE", "TPEX", "TAIEX"):
                rows = [{"code": str(1000+i)} for i in range(100 if src != "TAIEX" else 1)]
                payload = make_official_day_manifest({"source": src, "day": str(D),
                                                      "source_url": MARKET_URLS[src], "rows": rows},
                                                     collected_at="2026-10-20T16:05:00+08:00")
                if src == "TPEX":
                    payload["rows"][0]["code"] = "9999"
                folder = Path(td) / "data/h117/daily" / src
                folder.mkdir(parents=True)
                with gzip.open(folder / f"{D}.json.gz", "wt", encoding="utf-8") as f:
                    json.dump(payload, f)
            self.assertEqual(verified_calendar(Path(td), asof=D)["status"], "NO_VERIFIED_H117")

    def test_13_batch_reject_over_cap(self):
        h, r = batch_history([str(i) for i in range(1000,1006)], asof=D, fetcher=lambda *a, **kw: None)
        self.assertTrue(h.empty)
        self.assertEqual(r.iloc[0]["資料狀態"], "INVALID_BATCH")

    def test_14_batch_empty_has_no_fake_prices(self):
        h, r = batch_history(["2330"], asof=D, fetcher=lambda *a, **kw: pd.DataFrame())
        self.assertTrue(h.empty)
        self.assertEqual(r.iloc[0]["資料狀態"], "NO_DATA")

    def test_15_batch_wrong_ticker_rejected(self):
        h, r = batch_history(["2330"], asof=D, fetcher=lambda *a, **kw: history("2317"))
        self.assertTrue(h.empty)
        self.assertEqual(r.iloc[0]["資料狀態"], "TICKER_MISMATCH")

    def test_16_batch_label_as_non_official(self):
        h, r = batch_history(["2330"], asof=D, fetcher=lambda *a, **kw: history())
        self.assertFalse(h.empty)
        self.assertEqual(r.iloc[0]["資料狀態"], "SESSION_NOT_VERIFIED")
        self.assertIn("Yahoo", r.iloc[0]["來源"])

    def test_17_batch_day_aligned_not_official_certified(self):
        def fetcher(*a, **kw):
            d = pd.bdate_range(end="2026-10-20", periods=160)
            return pd.DataFrame({"日期":d,"開盤價":100,"最高價":103,"最低價":99,
                                 "收盤價":101,"成交量":1000})
        h,r = batch_history(["2330"], asof=D, fetcher=fetcher, expected_session=str(D))
        self.assertEqual(r.iloc[0]["資料狀態"], "DATE_ALIGNED_SOURCE_UNCERTIFIED")

    def test_18_batch_per_stock_isolation(self):
        def fetcher(code,*a,**kw):
            if code == "2330": raise ValueError("FAIL")
            return history(code)
        h,r = batch_history(["2330","2317"], asof=D, fetcher=fetcher)
        self.assertEqual(len(r), 2)
        self.assertTrue(r.iloc[0]["資料狀態"].startswith("FETCH_ERROR"))
        self.assertFalse(h.empty)

    def test_19_forward_close_not_fill(self):
        f = historical_forward_observations(decisions(), small(), asof=D)
        self.assertAlmostEqual(f.iloc[0]["T+1收盤變化%"], 3)
        self.assertAlmostEqual(f.iloc[0]["T+3收盤變化%"], 7)
        self.assertIn("非成交", f.columns[2])

    def test_20_forward_no_future_prices(self):
        f = historical_forward_observations(decisions(), small(), asof=date(2026,10,16))
        self.assertIsNone(f.iloc[0]["T+3收盤變化%"])

    def test_21_forward_weekend_signal_rejected(self):
        f = historical_forward_observations(decisions("2026-10-17"), small(), asof=D)
        self.assertTrue(f.empty)

    def test_22_forward_no_same_session_signal(self):
        f = historical_forward_observations(decisions(str(D)), small(), asof=D)
        self.assertTrue(f.empty)

    def test_23_forward_never_promotes_hard_veto(self):
        f = historical_forward_observations(decisions(veto="FADE_VETO"), small(), asof=D)
        self.assertIn("僅研究觀測", f.iloc[0]["研究資格"])

    def test_25_conflicting_duplicate_candles_quarantined(self):
        def provider(*a, **kw):
            raw = history()
            extra = raw.iloc[10:11].copy()
            extra["收盤價"] = extra["收盤價"] * 1.05
            return pd.concat([raw, extra], ignore_index=True)
        frame, report = batch_history(["2330"], asof=D, fetcher=provider)
        self.assertTrue(frame.empty)
        self.assertEqual(report.iloc[0]["資料狀態"], "CONFLICTING_DAILY_BAR")

    def test_26_identical_candles_dedup_allowed(self):
        def provider(*a, **kw):
            raw = history()
            return pd.concat([raw, raw.iloc[5:6]], ignore_index=True)
        frame, report = batch_history(["2330"], asof=D, fetcher=provider)
        self.assertFalse(frame.empty)
        self.assertNotEqual(report.iloc[0]["資料狀態"], "CONFLICTING_DAILY_BAR")

    def test_27_future_dated_bar_never_promoted(self):
        def provider(*a, **kw):
            raw = history()
            extra = raw.iloc[2:3].copy()
            extra["日期"] = pd.to_datetime("2026-10-21")
            return pd.concat([raw, extra], ignore_index=True)
        frame, report = batch_history(["2330"], asof=D, fetcher=provider)
        self.assertLessEqual(frame["日期"].max().date(), D)
        self.assertEqual(report.iloc[0]["資料狀態"], "FUTURE_DATES_REMOVED_RECHECK")

    def test_28_forward_no_future_provided_after_asof(self):
        raw = small().copy()
        raw.loc[len(raw)] = ["2330", pd.Timestamp("2026-10-21"), 110, 112, 109, 111]
        res = historical_forward_observations(decisions(), raw, asof=D)
        self.assertAlmostEqual(res.iloc[0]["T+3收盤變化%"], 7)

    def test_24_empty_candidates_safe(self):
        self.assertTrue(candidate_evidence_audit(pd.DataFrame(), asof=D).empty)
        self.assertTrue(historical_forward_observations(pd.DataFrame(), small(), asof=D).empty)


if __name__ == "__main__":
    unittest.main()
