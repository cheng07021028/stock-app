# -*- coding: utf-8 -*-
"""Synthetic official-schema shape checks only, no live provider claims."""
import sys, unittest, tempfile, json
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from godpick_h119_official_source import capture_raw,validate_official_payload,fetch_official_sources,URLS

TWSE={'stat':'OK','fields':['資料日期','股票代號','股票名稱','權/息'],
      'data':[['115年10月13日','1727','中華化','權']]}
TPEX=[{'除權息日期':'115/10/13','代號':'1234','現金股利':'1.5','每仟股無償配股':'0'}]
STAMP='2026-10-14T18:30:00+08:00'
class H119CaptureTests(unittest.TestCase):
 def test_twse_mixed_rights_not_auto_cash(self):
  self.assertEqual(validate_official_payload('TWSE',TWSE)['status'],'RAW_OFFICIAL_TABLE_NOT_NUMERIC_REVIEWED')
 def test_tpex_known_schema_not_auto_certified(self):
  self.assertEqual(validate_official_payload('TPEX',TPEX)['rows'],1)
 def test_unknown_schema_rejected(self):
  with self.assertRaises(ValueError):validate_official_payload('TPEX',[{'fake':'row'}])
  with self.assertRaises(ValueError):validate_official_payload('TWSE',{'stat':'OK','fields':[],'data':[]})
 def test_immutable_idempotent_raw(self):
  with tempfile.TemporaryDirectory() as td:
   raw=json.dumps(TWSE,ensure_ascii=False).encode()
   p=Path(td)
   first=capture_raw('TWSE',raw,retrieved_at=STAMP,root=p)
   later=capture_raw('TWSE',raw,retrieved_at='2026-10-14T19:30:00+08:00',root=p)
   self.assertEqual(first['status'],'CAPTURED_RAW_NOT_CERTIFIED')
   self.assertEqual(later['status'],'ALREADY_IDENTICAL')
   self.assertEqual(len(list(p.rglob('*.json.gz'))),1)
 def test_revised_official_response_keeps_both_versions(self):
  with tempfile.TemporaryDirectory() as td:
   a=json.dumps(TWSE,ensure_ascii=False).encode();b=json.dumps({**TWSE,'data':[]},ensure_ascii=False).encode()
   capture_raw('TWSE',a,retrieved_at=STAMP,root=Path(td))
   capture_raw('TWSE',b,retrieved_at=STAMP,root=Path(td))
   self.assertEqual(len(list(Path(td).rglob('*.json.gz'))),2)
if __name__=='__main__':unittest.main(verbosity=2)
