# -*- coding: utf-8 -*-
"""Deterministic H132 tests: no live network, no production authority writes."""
import io
import json
import unittest
from datetime import date, datetime
from zoneinfo import ZoneInfo
import pandas as pd

from godpick_h132_official_events import (
    VERSION, SOURCE_URL, MAX_CONTENT_BYTES, MAX_CODES, _roc_date, _hms, _classify,
    parse_twse_disclosures, fetch_twse_disclosures, candidate_event_watch,
)

DAY = date(2026, 10, 20)
TZ = ZoneInfo("Asia/Taipei")
NOW = datetime(2026, 10, 20, 17, 30, tzinfo=TZ)


def example(**changes):
    item = {"出表日期": "1151020", "發言日期": "1151020", "發言時間": "143526",
            "公司代號": "2330", "公司名稱": "示例半導體", "主旨 ": "公告董事會重大投資決議",
            "符合條款": "第20款", "事實發生日": "1151019", "說明": "董事會通過投資案。"}
    item.update(changes)
    return item


class Response:
    def __init__(self, payload=None, status=200, headers=None):
        self.payload = json.dumps(payload if payload is not None else [example()], ensure_ascii=False).encode('utf-8')
        self.status_code = status
        self.headers = headers if headers is not None else {"Content-Type": "application/json"}
        self.closed = False

    def iter_content(self, chunk_size):
        yield self.payload[:8]
        yield self.payload[8:]

    def close(self):
        self.closed = True


def decode(rows=None, *, codes=None, asof=DAY, now=NOW):
    return parse_twse_disclosures(rows if rows is not None else [example()], asof=asof, fetched_at=now, codes=codes)


class TestH132OfficialEvents(unittest.TestCase):
    def test_01_version(self):
        self.assertTrue(VERSION.startswith("V191-H132"))

    def test_02_official_fixed_https_url(self):
        self.assertEqual(SOURCE_URL, 'https://openapi.twse.com.tw/v1/opendata/t187ap04_L')

    def test_03_roc_day(self):
        self.assertEqual(_roc_date("115/10/20"), DAY)
        self.assertEqual(_roc_date("1151020"), DAY)
        self.assertEqual(_roc_date("2026-10-20"), DAY)

    def test_04_invalid_roc_rejected(self):
        self.assertIsNone(_roc_date("1150230"))
        self.assertIsNone(_roc_date("11510"))

    def test_05_times_padding(self):
        self.assertEqual(str(_hms("93015")), "09:30:15")
        self.assertEqual(str(_hms("14:05")), "14:05:00")
        self.assertIsNone(_hms("256161"))

    def test_06_title_trailing_blank_stripped(self):
        frame, audit = decode()
        self.assertEqual(frame.iloc[0]["公告標題"], "公告董事會重大投資決議")
        self.assertEqual(audit.iloc[0]["資料狀態"], "RECEIVED")

    def test_07_fact_not_publication_date(self):
        frame, _ = decode()
        self.assertEqual(frame.iloc[0]["公告日期"], "2026-10-20")
        self.assertEqual(frame.iloc[0]["事實發生日"], "2026-10-19")

    def test_08_future_publication_rejected(self):
        frame, audit = decode([example(**{"發言日期": "1151021"})])
        self.assertTrue(frame.empty)
        self.assertGreater(audit.iloc[0]["隔離筆數"], 0)

    def test_09_future_time_same_day_rejected(self):
        frame, audit = decode([example(**{"發言時間": "235959"})])
        self.assertTrue(frame.empty)

    def test_10_missing_publish_date_rejected(self):
        frame, _ = decode([example(**{"發言日期": ""})])
        self.assertTrue(frame.empty)

    def test_11_missing_publish_time_requires_recheck(self):
        frame, audit = decode([example(**{"發言時間": ""})])
        self.assertEqual(frame.iloc[0]["資料狀態"], "TIME_UNKNOWN_RECHECK")
        self.assertEqual(audit.iloc[0]["資料狀態"], "RECEIVED_RECHECK_REQUIRED")

    def test_12_batch_future_rejected(self):
        frame, audit = decode([example(**{"出表日期": "1151021"})])
        self.assertTrue(frame.empty)

    def test_13_historical_no_retroactive_current_feed(self):
        frame, audit = decode(asof=date(2026,10,19))
        self.assertTrue(frame.empty)
        self.assertEqual(audit.iloc[0]["資料狀態"], "HISTORICAL_PIT_UNAVAILABLE")

    def test_14_future_asof_rejected(self):
        frame, audit = decode(asof=date(2026,10,21))
        self.assertTrue(frame.empty)
        self.assertEqual(audit.iloc[0]["資料狀態"], "FUTURE_ASOF_REFUSED")

    def test_15_filter_only_requested(self):
        frame, audit = decode([example(), example(**{"公司代號": "2317", "主旨 ": "其他"})], codes=["2330"])
        self.assertEqual(frame["股票代號"].tolist(), ["2330"])
        self.assertEqual(audit.iloc[0]["原始資料筆數"], 2)

    def test_16_nonmatch_not_synthetically_filled(self):
        frame, audit = decode(codes=["2317"])
        self.assertTrue(frame.empty)
        self.assertEqual(audit.iloc[0]["資料狀態"], "NO_MATCHING_EVENTS")

    def test_17_identical_event_dedup(self):
        frame, _ = decode([example(), example()])
        self.assertEqual(len(frame), 1)

    def test_18_malformed_payload_fails_closed(self):
        for payload in (None, {"data": []}, ["bad"]):
            frame, audit = decode(payload if payload is not None else {})
            self.assertTrue(frame.empty)

    def test_19_limit_codes(self):
        frame, audit = decode(codes=["1000"] * (MAX_CODES+1))
        self.assertEqual(audit.iloc[0]["資料狀態"], "INVALID_CODES")

    def test_20_expired_batch_flag(self):
        frame, audit = decode([example(**{"出表日期":"1151010", "發言日期":"1151010"})])
        self.assertEqual(frame.iloc[0]["資料狀態"], "BATCH_STALE_RECHECK")

    def test_prior_day_batch_requires_recheck(self):
        frame, audit = decode([example(**{"出表日期":"1151019", "發言日期":"1151019"})])
        self.assertEqual(frame.iloc[0]["資料狀態"], "BATCH_PRIOR_DAY_RECHECK")
        self.assertEqual(audit.iloc[0]["資料狀態"], "RECEIVED_RECHECK_REQUIRED")

    def test_21_no_bullish_bearish_from_title(self):
        frame, _ = decode([example(**{"主旨 ": "接獲重大訂單"})])
        self.assertEqual(frame.iloc[0]["公告類別"], "訂單／合約")
        self.assertIn("非方向預測", frame.iloc[0]["研究提醒"])

    def test_22_fix_revision_priority(self):
        self.assertEqual(_classify("更正原重大財報公告")[0], "更正／澄清")

    def test_23_unset_timezone_refused(self):
        with self.assertRaises(ValueError):
            decode(now=datetime(2026,10,20,17,30))

    def test_24_fetch_requires_today_no_http(self):
        called = []
        def fetch(*a, **kw): called.append(a); raise RuntimeError()
        ev, audit = fetch_twse_disclosures(asof=date(2026,10,19), codes=["2330"], http_get=fetch, clock=lambda: NOW)
        self.assertEqual(audit.iloc[0]["資料狀態"], "NOT_TODAY_NO_HISTORY")
        self.assertFalse(called)

    def test_25_fetch_limited_single_fixed_endpoint(self):
        called = []
        response = Response()
        def fetch(url, **kw):
            called.append((url, kw))
            return response
        ev, audit = fetch_twse_disclosures(asof=DAY, codes=["2330"], http_get=fetch, clock=lambda: NOW)
        self.assertEqual(len(ev), 1)
        self.assertEqual(len(called), 1)
        self.assertEqual(called[0][0], SOURCE_URL)
        self.assertFalse(called[0][1]["allow_redirects"])
        self.assertTrue(response.closed)

    def test_26_non_json_content_type_refused(self):
        ev, audit = fetch_twse_disclosures(asof=DAY, codes=["2330"], http_get=lambda *a,**kw: Response(headers={"Content-Type":"text/html"}), clock=lambda:NOW)
        self.assertEqual(audit.iloc[0]["資料狀態"], "FETCH_FAILED")

    def test_27_http_non200_refused(self):
        ev, audit = fetch_twse_disclosures(asof=DAY, codes=["2330"], http_get=lambda *a,**kw: Response(status=302), clock=lambda:NOW)
        self.assertEqual(audit.iloc[0]["資料狀態"], "FETCH_FAILED")

    def test_28_declared_data_oversize_refused(self):
        response = Response(headers={"Content-Type":"application/json","Content-Length":str(MAX_CONTENT_BYTES+1)})
        ev, audit = fetch_twse_disclosures(asof=DAY, codes=["2330"], http_get=lambda *a,**kw: response, clock=lambda:NOW)
        self.assertEqual(audit.iloc[0]["資料狀態"], "FETCH_FAILED")

    def test_29_bad_json_refused(self):
        r = Response()
        r.payload = b'{broken'
        ev, audit = fetch_twse_disclosures(asof=DAY, codes=["2330"], http_get=lambda *a,**kw: r, clock=lambda:NOW)
        self.assertEqual(audit.iloc[0]["資料狀態"], "FETCH_FAILED")

    def test_30_fetch_not_a_trade_authority(self):
        frame, _ = decode()
        self.assertNotIn("買進", frame.columns)
        self.assertNotIn("目標價", frame.columns)
        self.assertNotIn("交易分數", frame.columns)

    def test_31_watchlist_hard_veto_retained(self):
        frame, _ = decode()
        old = pd.DataFrame([{"股票代號":"2330", "股票名稱":"示例", "H126權威層別":"禁止新倉", "H109決策權威":"FADE_VETO"}])
        after = candidate_event_watch(old,frame)
        self.assertEqual(after.iloc[0]["H126研究層別_原樣"], "禁止新倉")
        self.assertEqual(after.iloc[0]["H109決策_原樣"], "FADE_VETO")
        self.assertEqual(after.iloc[0]["當期公告筆數"], 1)
        self.assertEqual(old.iloc[0]["H126權威層別"], "禁止新倉")

    def test_32_watchlist_no_events_no_fabrication(self):
        old = pd.DataFrame([{"股票代號":"2330"}])
        after = candidate_event_watch(old,pd.DataFrame())
        self.assertEqual(after.iloc[0]["當期公告筆數"], 0)
        self.assertIn("不代表", after.iloc[0]["研究動作"])

    def test_33_oversize_rows_rejected(self):
        frame, audit = decode([{}]*30001)
        self.assertEqual(audit.iloc[0]["資料狀態"], "SOURCE_SCHEMA_INVALID")

    def test_34_bad_code_cannot_fetch(self):
        invoked = []
        def fake(*a,**kw): invoked.append(1)
        _, audit = fetch_twse_disclosures(asof=DAY, codes=["2330;rm -rf"],http_get=fake,clock=lambda: NOW)
        self.assertEqual(audit.iloc[0]["資料狀態"], "INVALID_CODES")
        self.assertFalse(invoked)

    def test_35_no_future_event_even_if_occurrence_old(self):
        frame, _ = decode([example(**{"事實發生日":"1150101","發言日期":"1151021"})])
        self.assertTrue(frame.empty)

    def test_36_earlier_publication_allowed_if_batch_current(self):
        frame, _ = decode([example(**{"發言日期":"1151019","出表日期":"1151020"})])
        self.assertEqual(frame.iloc[0]["公告日期"], "2026-10-19")

    def test_37_refuse_clock_time_provider_failure(self):
        def get(*a,**kw): raise TimeoutError('fake')
        frame, audit = fetch_twse_disclosures(asof=DAY,codes=["2330"],http_get=get,clock=lambda:NOW)
        self.assertTrue(frame.empty)
        self.assertEqual(audit.iloc[0]["資料狀態"], "FETCH_FAILED")


if __name__ == '__main__':
    unittest.main()
