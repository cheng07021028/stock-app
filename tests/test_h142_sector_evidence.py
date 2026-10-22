# -*- coding: utf-8 -*-
"""H142 manager audit safety and sector evidence provenance."""
from __future__ import annotations
import unittest
from datetime import date
from pathlib import Path
import sys
import pandas as pd
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from godpick_h142_evidence_review import assess_sector_fade,build_h142_review
from godpick_h105_future_blackhorse_engine import _sector_lifecycle_detail
from godpick_h109_authority_governor import govern


def record(i, **patch):
    row={'股票代號':str(2000+i),'股票名稱':f'樣本{i}','類別':('AI伺服器' if i%2 else '化工'),
         'H109決策權威':'FADE_VETO','H108族群生命週期':'FADE｜退潮',
         'H110研究資訊截止日':'2026-10-21',
         'H110證據狀態':'PARTIAL｜來源/資料不足',
         'H123法人實證狀態':'PENDING_EVIDENCE','H124大戶實證狀態':'PENDING_EVIDENCE'}
    row.update(patch)
    return row


class H142Tests(unittest.TestCase):
    def test_01_label_alone_is_not_evidence(self):
        d=assess_sector_fade(record(1))
        self.assertEqual(d['H142退潮證據級別'],'UNVERIFIED_FADE_LABEL')
    def test_02_dated_negative_double_is_internal_only(self):
        d=assess_sector_fade(record(1,H108族群前次資料日='2026-10-20',H108族群資金流變化=-12,H108族群廣度變化=-9))
        self.assertEqual(d['H142退潮證據級別'],'TWO_INTERNAL_METRICS')
        self.assertIn('尚未獨立驗證',d['H142證據說明'])
    def test_03_future_prevdate_cannot_count(self):
        d=assess_sector_fade(record(1,H108族群前次資料日='2026-10-22',H108族群資金流變化=-30,H108族群廣度變化=-30))
        self.assertEqual(d['H142退潮證據級別'],'UNVERIFIED_FADE_LABEL')
    def test_04_equal_prevdate_cannot_count(self):
        d=assess_sector_fade(record(1,H108族群前次資料日='2026-10-21',H108族群資金流變化=-30,H108族群廣度變化=-30))
        self.assertEqual(d['H142退潮證據級別'],'UNVERIFIED_FADE_LABEL')
    def test_05_one_signal_not_full(self):
        d=assess_sector_fade(record(1,H108族群前次資料日='2026-10-20',H108族群資金流變化=-15,H108族群廣度變化=5))
        self.assertEqual(d['H142退潮證據級別'],'SINGLE_INTERNAL_METRIC')
    def test_06_no_fade_not_fade(self):
        d=assess_sector_fade(record(1,H109決策權威='RESEARCH_ELIGIBLE',H108族群生命週期='PREHEAT'))
        self.assertEqual(d['H142退潮證據級別'],'NOT_FADE')
    def test_07_no_trading_grant(self):
        for r in [record(1),record(2,H108族群前次資料日='2026-10-20',H108族群資金流變化=-15,H108族群廣度變化=-8)]:
            self.assertIn('不得解除',assess_sector_fade(r)['H142保留風控'])
    def test_08_120_realistic(self):
        audit=pd.DataFrame([record(i) for i in range(1,119)]+[record(119,H109決策權威='RESEARCH_ELIGIBLE',H108族群生命週期='PREHEAT'),record(120,H109決策權威='RESEARCH_ELIGIBLE',H108族群生命週期='PREHEAT')]);orig=audit.copy(deep=True)
        a,d,sector=build_h142_review({'audit':audit,'h126_authority':pd.DataFrame([{'H126權威層別':'B｜待驗證觀察（非買進）','H126認證資格':'PENDING_EVIDENCE'}]*2)})
        summary=dict(a.values)
        self.assertEqual(summary['H113稽核去重檔數'],120)
        self.assertEqual(summary['H109 FADE_VETO檔數'],118)
        self.assertEqual(summary['FADE佔比%'],98.33)
        self.assertEqual(summary['H126 B級待驗證研究檔數'],2)
        self.assertEqual(summary['退潮僅標籤、無雙量化證據檔數'],118)
        self.assertEqual(len(d),120)
        self.assertEqual(len(sector),2)
        pd.testing.assert_frame_equal(audit,orig)
    def test_09_deduplicate_codes(self):
        rows=[record(1,H109決策權威='RESEARCH_ELIGIBLE'),record(1),record(2)]
        _,details,_=build_h142_review({'audit':pd.DataFrame(rows)})
        self.assertEqual(len(details),2)
        self.assertEqual(details.loc[details['股票代號']=='2001','H109原風控'].iloc[0],'FADE_VETO')
    def test_10_no_rows(self):
        summary,d,sec=build_h142_review({})
        self.assertEqual(dict(summary.values)['H113稽核去重檔數'],0)
        self.assertTrue(d.empty);self.assertTrue(sec.empty)
    def test_11_date_replay_not_authorized(self):
        o=govern({'H108族群生命週期':'FADE｜退潮','H108研究優先分':90},asof=date(2026,10,21))
        self.assertEqual(o['H109決策權威'],'FADE_VETO')
    def test_12_valid_negative_sector_state_still_fade(self):
        d=_sector_lifecycle_detail({'H102族群動態狀態':'FADE｜退潮','H102族群衝擊分':80},{'族群資金流分數':20,'同族群強勢比例':18,'族群樣本可信度':85,'H108族群前次資料日':'2026-10-20'})
        self.assertIn('FADE',d['lifecycle'])
        self.assertEqual(d['sample_conf'],85)
    def test_13_empty_fade_counts(self):
        _, d, _=build_h142_review({'audit':pd.DataFrame([record(1,H109決策權威='RESEARCH_ELIGIBLE',H108族群生命週期='PREHEAT')])})
        self.assertEqual(d.iloc[0]['H142退潮證據級別'],'NOT_FADE')
    def test_14_source_failed_not_verified(self):
        d=assess_sector_fade(record(1,H108族群前次資料日='bad',H108族群資金流變化=-100,H108族群廣度變化=-100))
        self.assertEqual(d['H142退潮證據級別'],'UNVERIFIED_FADE_LABEL')

if __name__=='__main__':unittest.main()
