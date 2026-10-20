# -*- coding: utf-8 -*-
"""Offline H135 regression; no GitHub, personal records or real trading credentials."""
import copy
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
import unittest

import pandas as pd

from godpick_h134_decision_vault import build_snapshot
from godpick_h135_signal_governance import (
    VERSION, MAX_ARCHIVES, _tier, _archive_integrity, lifecycle_report,
    research_policy_review, fixed_challenger_comparison,
)

TW = ZoneInfo('Asia/Taipei')
D1, D2, D3, D4 = (date(2026, 10, 14), date(2026, 10, 15),
                  date(2026, 10, 16), date(2026, 10, 19))
ASOF = date(2026, 10, 20)


def frame(*tuples):
    return pd.DataFrame([{
        '股票代號': code, 'H126權威層別': layer,
        'H126風險狀態': risk, 'H126最後研究分': score,
        'H126研究資訊截止日': '2026-10-14',
        'H123法人實證狀態': 'UNVERIFIED',
        'H124大戶實證狀態': 'UNVERIFIED',
    } for code, layer, risk, score in tuples])


def archive(day=D1, rows=None, capture=None, src='LIVE_SESSION_H126'):
    if rows is None:
        rows = frame(('2330', 'B｜待驗證觀察', '', 50))
    if capture is None:
        capture = datetime(day.year, day.month, day.day, 17, 0, tzinfo=TW)
    return build_snapshot(rows, research_day=day, source=src, captured_at=capture)['payload']


def one_horizon(days, coverage, lo=None, hi=None, alpha=None):
    return {'狀態': 'RETROSPECTIVE_RESEARCH_ONLY',
            '樣本外績效': pd.DataFrame([{'期間':'T+1', '有效獨立交易日': days,
                                    '涵蓋率%':coverage, 'Bootstrap95%下界pp':lo,
                                    'Bootstrap95%上界pp':hi, '日等權超額pp':alpha}])}


class H135Tests(unittest.TestCase):
    def test_01_version(self): self.assertIn('H135', VERSION)
    def test_02_tier_a(self): self.assertEqual(_tier('A｜認證', ''), 'A')
    def test_03_tier_b(self): self.assertEqual(_tier('B｜待驗證', ''), 'B')
    def test_04_tier_c(self): self.assertEqual(_tier('C｜稽核', ''), 'C')
    def test_05_tier_isolated(self): self.assertEqual(_tier('禁止新倉', ''), 'ISOLATED')
    def test_06_risk_overrides_a(self): self.assertEqual(_tier('A｜認證', 'FADE_VETO'), 'ISOLATED')
    def test_07_valid_hash(self): self.assertEqual(_archive_integrity(archive())[1], 'VALID_INTERNAL_HASH_NOT_INDEPENDENT_PIT')
    def test_08_changed_row_rejected(self):
        item=archive();item['rows'][0]['H126最後研究分']=99
        self.assertEqual(_archive_integrity(item)[1], 'FINGERPRINT_MISMATCH')
    def test_09_changed_summary_rejected(self):
        item=archive();item['summary']['研究股']=88
        self.assertEqual(_archive_integrity(item)[1], 'FINGERPRINT_MISMATCH')
    def test_10_bad_schema(self): self.assertEqual(_archive_integrity({})[1], 'NOT_H134_SCHEMA')
    def test_11_future_asof_rejected(self):
        out=lifecycle_report([archive(D4)],asof=D3)
        self.assertEqual(out['有效封存數'],0)
        self.assertIn('FUTURE_RESEARCH_DATE',out['封存稽核']['狀態'].values)
    def test_12_weekend_rejected(self):
        item=archive(D3);item['research_day']='2026-10-17'
        self.assertNotEqual(_archive_integrity(item)[1], 'VALID_INTERNAL_HASH_NOT_INDEPENDENT_PIT')
    def test_13_dedup_same_snapshot(self):
        x=archive();out=lifecycle_report([x,x],asof=ASOF)
        self.assertEqual(out['有效封存數'],1)
    def test_14_conflicting_day_quarantined(self):
        a=archive();b=archive(rows=frame(('2330','A｜認證','',70)))
        out=lifecycle_report([a,b],asof=ASOF)
        self.assertEqual(out['有效封存數'],0)
        self.assertIn('SAME_DAY_CONFLICT_QUARANTINED',out['封存稽核']['狀態'].values)
    def test_15_first_day_baseline_not_new(self):
        tab=lifecycle_report([archive()],asof=ASOF)['逐日生命週期']
        self.assertEqual(tab.iloc[0]['生命週期事件'],'BASELINE_FIRST_ARCHIVE')
    def test_16_new_after_previous(self):
        a=archive();b=archive(D2,frame(('2330','B｜觀察','',60),('2317','B｜觀察','',60)))
        out=lifecycle_report([a,b],asof=ASOF)['逐日生命週期']
        self.assertEqual(out[(out['研究日']==str(D2)) & (out['股票代號']=='2317')].iloc[0]['生命週期事件'],'NEW_OR_REENTRY')
    def test_17_upgrade(self):
        out=lifecycle_report([archive(),archive(D2,frame(('2330','A｜研究','',65)))],asof=ASOF)['逐日生命週期']
        self.assertIn('RESEARCH_LAYER_UP',out['生命週期事件'].values)
    def test_18_downgrade(self):
        out=lifecycle_report([archive(D1,frame(('2330','A｜研究','',65))),archive(D2)],asof=ASOF)['逐日生命週期']
        self.assertIn('RESEARCH_LAYER_DOWN',out['生命週期事件'].values)
    def test_19_veto_quarantine_not_buy(self):
        out=lifecycle_report([archive(),archive(D2,frame(('2330','A｜研究','SHOCK_QUARANTINE',60)))],asof=ASOF)
        self.assertIn('RISK_QUARANTINED',out['逐日生命週期']['生命週期事件'].values)
        self.assertEqual(len(out['歷史研究訊號']),1)
    def test_20_remove_not_sell(self):
        out=lifecycle_report([archive(),archive(D2,frame(('2317','B｜觀察','',50)))],asof=ASOF)
        self.assertIn('EXITED_FROM_ARCHIVED_RESEARCH_NOT_SELL',out['逐日生命週期']['生命週期事件'].values)
    def test_21_streak_archive_count_not_market_days(self):
        out=lifecycle_report([archive(),archive(D4)],asof=ASOF)['逐日生命週期']
        self.assertEqual(out.iloc[-1]['連續封存出現次數'],2)
        self.assertEqual(out.iloc[-1]['前後封存間隔日'],5)
    def test_22_source_retro_not_promoted(self):
        out=lifecycle_report([archive(D1,capture=datetime(2026,10,20,14,tzinfo=TW),src='UPLOADED_H126_UNVERIFIED')],asof=ASOF)
        self.assertEqual(out.iloc[0]['PIT獨立證明'] if isinstance(out,pd.DataFrame) else out['逐日生命週期'].iloc[0]['PIT獨立證明'],'NOT_VERIFIED')
    def test_23_archives_limit(self):
        self.assertEqual(lifecycle_report([{}]*(MAX_ARCHIVES+1),asof=ASOF)['狀態'],'ARCHIVE_LIMIT')
    def test_24_invalid_asof(self):
        self.assertEqual(lifecycle_report([],asof=datetime.now(TW))['狀態'],'INVALID_ASOF')
    def test_25_empty(self):
        self.assertEqual(lifecycle_report([],asof=ASOF)['狀態'],'NO_USABLE_ARCHIVES')
    def test_26_bad_capture(self):
        a=archive();a['captured_at_tw']='not a date'
        self.assertEqual(_archive_integrity(a)[1],'MALFORMED_ARCHIVE')
    def test_27_positive_research_not_auto_promote(self):
        out=research_policy_review(one_horizon(30,100,.2,1.1,.7))
        self.assertEqual(out.iloc[0]['研究判定'],'RESEARCH_POSITIVE_REQUIRES_INDEPENDENT_PIT')
        self.assertEqual(out.iloc[0]['自動調權'],'DISABLED')
    def test_28_negative_review_not_auto_block(self):
        out=research_policy_review(one_horizon(35,90,-1.3,-.2,-.7))
        self.assertEqual(out.iloc[0]['研究判定'],'REVIEW_FOR_RESEARCH_SUSPENSION_NEGATIVE_OOS')
        self.assertEqual(out.iloc[0]['正式交易權限'],'UNCHANGED')
    def test_29_insufficient_days(self):
        self.assertEqual(research_policy_review(one_horizon(19,100,.1,1,.5)).iloc[0]['研究判定'],'SAMPLE_OR_COVERAGE_INSUFFICIENT')
    def test_30_insufficient_coverage(self):
        self.assertEqual(research_policy_review(one_horizon(35,79,.1,1,.5)).iloc[0]['研究判定'],'SAMPLE_OR_COVERAGE_INSUFFICIENT')
    def test_31_crosses_zero(self):
        self.assertEqual(research_policy_review(one_horizon(35,90,-.5,.7,.1)).iloc[0]['研究判定'],'NO_RELIABLE_EDGE_DEMONSTRATED')
    def test_32_no_ci(self):
        self.assertEqual(research_policy_review(one_horizon(35,90,None,None,.1)).iloc[0]['研究判定'],'UNCERTAINTY_NOT_ESTIMABLE')
    def test_33_adjustment_unverified(self):
        obj=one_horizon(35,95,.4,1,.6);obj['狀態']='ADJUSTMENT_UNVERIFIED'
        self.assertEqual(research_policy_review(obj).iloc[0]['研究判定'],'DATA_OR_ADJUSTMENT_UNVERIFIED')
    def test_34_missing_oos(self):
        self.assertEqual(research_policy_review({}).iloc[0]['研究判定'],'MISSING_OOS_PERFORMANCE')
    def test_35_invalid_threshold(self):
        with self.assertRaises(ValueError): research_policy_review({},coverage_threshold=20)
    def test_36_predeclared_policy_names(self):
        result=fixed_challenger_comparison(None,None,None,asof=ASOF)
        self.assertEqual(list(result['策略研究']),['H126_TOP3_RESEARCH','H126_TOP5_REFERENCE'])
    def test_37_both_read_only(self):
        result=fixed_challenger_comparison(None,None,None,asof=ASOF)
        self.assertTrue((result['比較決策']['正式交易權限']=='UNCHANGED').all())
    def test_38_result_contains_limits(self):
        self.assertIn('PIT',fixed_challenger_comparison(None,None,None,asof=ASOF)['研究限制'])
    def test_39_h109_not_blank(self):
        tab=lifecycle_report([archive(rows=frame(('2330','B｜觀察','',40)))],asof=ASOF)['歷史研究訊號']
        self.assertEqual(tab.iloc[0]['H109決策權威'],'')
    def test_40_same_day_repeat_does_not_overwrite(self):
        one=archive();two=copy.deepcopy(one)
        out=lifecycle_report([one,two],asof=ASOF)
        self.assertEqual(out['有效封存數'],1)
    def test_41_fullwidth_tier(self): self.assertEqual(_tier('Ａ 級研究',''), 'A')
    def test_42_none_archive(self): self.assertEqual(_archive_integrity(None)[1],'NOT_H134_SCHEMA')
    def test_43_invalid_evidence_date_quarantined(self):
        item=archive(rows=frame(('2330','B｜觀察','',40)))
        item['rows'][0]['H126研究資訊截止日']='not-a-date'
        from godpick_h134_decision_vault import _json_bytes
        import hashlib
        stable={k:item.get(k) for k in ('schema','research_day','source_type','rows','summary','freshness')}
        item['decision_fingerprint_sha256']=hashlib.sha256(_json_bytes(stable)).hexdigest()
        self.assertEqual(_archive_integrity(item)[1],'INVALID_EVIDENCE_DATE')
    def test_44_future_evidence_day_quarantined(self):
        item=archive(rows=frame(('2330','B｜觀察','',40)))
        item['rows'][0]['H126研究資訊截止日']='2026-10-20'
        from godpick_h134_decision_vault import _json_bytes
        import hashlib
        stable={k:item.get(k) for k in ('schema','research_day','source_type','rows','summary','freshness')}
        item['decision_fingerprint_sha256']=hashlib.sha256(_json_bytes(stable)).hexdigest()
        self.assertEqual(_archive_integrity(item)[1],'FUTURE_EVIDENCE_DAY')

if __name__ == '__main__':
    unittest.main()
