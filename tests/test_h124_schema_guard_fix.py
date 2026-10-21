# -*- coding: utf-8 -*-
"""H124 live-schema drift diagnostics; local fixtures ONLY, no official live API calls."""
from __future__ import annotations
import csv
import io
import json
import unittest
from datetime import datetime
from unittest.mock import patch

from godpick_h124_ownership_flow import build_tdcc, _parse_tdcc_bytes
from tools.h124_official_ownership_cli import main

DATE='20260918'
STAMP='2026-09-25T19:00:00+08:00'
HEADERS=['資料日期','證券代號','持股分級','人數','股數','占集保庫存數比例%']
BASE=[dict(zip(HEADERS,[DATE,'1234','15','20','1200000','12'])),
      dict(zip(HEADERS,[DATE,'1234','17','20','10000000','100']))]

def csv_rows(rows, fields=None):
    fields=fields or list(rows[0]);buf=io.StringIO();w=csv.DictWriter(buf,fieldnames=fields)
    w.writeheader();w.writerows(rows);return buf.getvalue().encode('utf-8-sig')

class TDCCSchemaGuard(unittest.TestCase):
    def test_original_official_columns(self):
        ev=build_tdcc(csv_rows(BASE),collected_at=STAMP)
        self.assertEqual(ev['trade_date'],'2026-09-18')
        self.assertEqual(ev['stocks']['1234']['bracket15_pct'],12)

    def test_unicode_presentation_whitespace_normalized(self):
        rows=[{'\ufeff 資料日期\xa0':DATE,'證券代號 ': '1234', '持股分級':'15',
              '人數':'20','股數':'1200000','占集保庫存數比例%':'12'},
              {'\ufeff 資料日期\xa0':DATE,'證券代號 ': '1234', '持股分級':'17',
              '人數':'20','股數':'10000000','占集保庫存數比例%':'100'}]
        ev=build_tdcc(csv_rows(rows),collected_at=STAMP)
        self.assertEqual(ev['stocks']['1234']['over_1000_lots_bracket_shares'],1200000)

    def test_safe_date_alias_documented_not_guessed(self):
        rows=[{'Date':row['資料日期'], **{k:v for k,v in row.items() if k!='資料日期'}} for row in BASE]
        ev=build_tdcc(json.dumps(rows).encode('utf-8'),collected_at=STAMP)
        self.assertEqual(ev['trade_date'],'2026-09-18')

    def test_absent_date_reports_actual_header_names(self):
        rows=[{'異動日期':row['資料日期'], **{k:v for k,v in row.items() if k!='資料日期'}} for row in BASE]
        with self.assertRaisesRegex(ValueError,'TDCC實際欄名.*異動日期'):
            build_tdcc(json.dumps(rows).encode('utf-8'),collected_at=STAMP)

    def test_duplicated_after_normalization_fails(self):
        columns=['資料日期',' 資料日期 ', '證券代號','持股分級','人數','股數','占集保庫存數比例%']
        lines='\n'.join([','.join(columns),
                           '20260918,20260918,1234,15,20,1200000,12'])
        with self.assertRaisesRegex(ValueError,'重複'):
            _parse_tdcc_bytes(lines.encode('utf-8'))

    def test_malformed_api_object_no_false_evidence(self):
        with self.assertRaisesRegex(ValueError,'非官方1-5資料'):
            build_tdcc(b'{"message":"something wrong"}',collected_at=STAMP)

    def test_cli_failure_prints_safe_header_names_only(self):
        class FakeResponse:
            status_code=200
            is_redirect=False
            url='https://openapi.tdcc.com.tw/v1/opendata/1-5'
            headers={'Content-Type':'application/json'}
            content=json.dumps([{'異動日期':DATE,'訊息':'internal-only-do-not-display'}]).encode('utf-8')
            def raise_for_status(self): pass
        from contextlib import redirect_stdout
        buf=io.StringIO()
        with patch('requests.get',return_value=FakeResponse()),redirect_stdout(buf):
            code=main(['collect-tdcc'])
        obj=json.loads(buf.getvalue())
        self.assertEqual(code,2)
        self.assertEqual(obj['status'],'TDCC_SCHEMA_REJECTED_NO_EVIDENCE_WRITTEN')
        self.assertIn('異動日期',obj['observed_header_names_only'])
        self.assertNotIn('internal-only-do-not-display',buf.getvalue())

if __name__=='__main__': unittest.main()
