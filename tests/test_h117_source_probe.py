from datetime import datetime
from zoneinfo import ZoneInfo
import importlib.util
from pathlib import Path
import unittest

SRC=Path(__file__).resolve().parents[1]/'tools'/'h117_source_probe.py'
spec=importlib.util.spec_from_file_location('h117_source_probe',SRC)
m=importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

class Response:
    def __init__(self,status=200,rows=None):
        self.status_code=status
        self.content=b'[{"Date":"1151020"}]'
        self.rows=rows if rows is not None else [{'Date':'1151020','Code':'1101'}]
    def json(self):return self.rows

NOW=datetime(2026,10,21,19,0,tzinfo=ZoneInfo('Asia/Taipei'))
URLS={'TWSE':'https://a','TPEX':'https://b','TAIEX':'https://c'}

def good_normalize(source, rows):return {'rows':rows,'day':'2026-10-21'}

class DiagnosticTests(unittest.TestCase):
    def test_success(self):
        r=m.inspect_market(lambda *a,**k:Response(),good_normalize,URLS,now=NOW)
        self.assertEqual(r['status'],'SAME_MARKET_DAY_OBSERVED')
    def test_before_close_does_not_request(self):
        def fail(*a,**k):raise AssertionError('MUST_NOT_CONNECT')
        r=m.inspect_market(fail,good_normalize,URLS,now=datetime(2026,10,21,17,0,tzinfo=NOW.tzinfo))
        self.assertEqual(r['status'],'WAIT_UNTIL_TW_18_15')
    def test_http_403(self):
        r=m.inspect_market(lambda *a,**k:Response(status=403),good_normalize,URLS,now=NOW)
        self.assertEqual(r['sources']['TWSE']['issue_code'],'HTTP_403')
    def test_zero_rows(self):
        def normalize(*args):raise ValueError('TWSE: 有效行情僅0筆，不符合最低資料量')
        r=m.inspect_market(lambda *a,**k:Response(),normalize,URLS,now=NOW)
        self.assertEqual(r['sources']['TWSE']['issue_code'],'NORMALIZED_ROWS_BELOW_MINIMUM')
    def test_missing_field_diagnostic(self):
        def normalize(*args):raise ValueError('官方欄位缺失：資料日期')
        r=m.inspect_market(lambda *a,**k:Response(),normalize,URLS,now=NOW)
        self.assertEqual(r['sources']['TWSE']['issue_code'],'INVALID_MARKET_DATE')
    def test_dates_conflict(self):
        r=m.inspect_market(lambda *a,**k:Response(),lambda src, rows:{'rows':rows,'day':'2026-10-20' if src=='TPEX' else '2026-10-21'},URLS,now=NOW)
        self.assertEqual(r['status'],'MARKET_DAY_MISMATCH')
    def test_no_query_leak(self):
        def get(*args,**kwargs):raise ConnectionError('https://host/private?token=SECRET123')
        r=m.inspect_market(get,good_normalize,URLS,now=NOW)
        self.assertNotIn('SECRET123',str(r))
    def test_fields_only(self):
        self.assertEqual(m.safe_fields({'Date':'SECRET','Code':'1234'}),['Date','Code'])

if __name__=='__main__':unittest.main()
