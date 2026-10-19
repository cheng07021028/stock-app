# -*- coding: utf-8 -*-
import unittest
import pandas as pd
from godpick_h129_wallstreet_audit import audit_t1_history, audit_risk_watch, VERSION
from godpick_h126_authority_report import build_authority

class WallStreetH129Tests(unittest.TestCase):
    def row(self, **kw):
        x = {'推薦日期':'2026-10-16','隔日日期':'2026-10-19','股票代號':'6285',
             '隔日候選漲跌%':-3.33,'Selection Alpha%':-2.72,
             'H103執行學習可用':'是', 'H103績效口徑':'FIXED_PLAN_REPLAY', 'H103路徑歧義':'否'}
        x.update(kw);return x
    def test_h129_allows_only_verified_stable_t1(self):
        frame, diag=audit_t1_history(pd.DataFrame([self.row()]))
        self.assertEqual(frame.iloc[0]['H129績效資料資格'],'VALIDATED_T1')
        self.assertEqual(diag.loc[diag['項目']=='H129經H103驗證可用筆數','數值'].iloc[0],1)
    def test_extreme_alpha_quarantined_not_mutated(self):
        frame, diag=audit_t1_history(pd.DataFrame([self.row(**{'Selection Alpha%':-7664.5132})]))
        self.assertEqual(frame.iloc[0]['Selection Alpha%'],-7664.5132)
        self.assertEqual(frame.iloc[0]['H129績效資料資格'],'QUARANTINED')
        self.assertTrue(pd.isna(frame.iloc[0]['H129有效Selection Alpha%']))
        self.assertEqual(diag.loc[diag['項目']=='H129極端報酬/Alpha異常筆數','數值'].iloc[0],1)
    def test_legacy_unverified_quarantined_even_if_win(self):
        frame,_=audit_t1_history(pd.DataFrame([self.row(**{'H103執行學習可用':'否｜缺少固定計畫或路徑證據','H103績效口徑':'LEGACY'})]))
        self.assertIn('H103_NOT_VALIDATED',frame.iloc[0]['H129隔離原因'])
    def test_weekend_signal_rejected(self):
        frame,_=audit_t1_history(pd.DataFrame([self.row(推薦日期='2026-10-17')]))
        self.assertIn('INVALID_SESSION_DATE',frame.iloc[0]['H129隔離原因'])
    def test_empty_report_produces_health(self):
        frame,health=audit_t1_history(None)
        self.assertEqual(len(frame),0)
        self.assertEqual(int(health.loc[health['項目']=='H129原T+1檢討筆數','數值'].iloc[0]),0)
    def test_hard_veto_stays_blocked_but_visible_as_audit(self):
        r={'股票代號':'6285','股票名稱':'啟碁','類別':'網通','市場別':'上市',
           'H109決策權威':'FADE_VETO','H113入榜資格':'BLOCKED',
           'H110最終研究優先分':75.04,'H123法人實證狀態':'PENDING_EVIDENCE',
           'H124大戶實證狀態':'PENDING_EVIDENCE'}
        result=audit_risk_watch({'audit':pd.DataFrame([r])})
        self.assertEqual(len(result),1)
        self.assertIn('禁止新倉',result.iloc[0]['H129主管操作'])
        self.assertIn('H123官方法人',result.iloc[0]['H129缺口分類'])
        authority=build_authority({'audit':pd.DataFrame([r])})['h126_authority']
        self.assertFalse((authority['H126認證資格']=='PASS').any())
    def test_h113_missing_evidence_is_not_a_hard_risk_veto(self):
        r={'股票代號':'6163','股票名稱':'華電網','市場別':'上櫃','類別':'網通',
           'H113入榜資格':'BLOCKED','H109決策權威':'RESEARCH_ELIGIBLE',
           'H123法人實證狀態':'PENDING_EVIDENCE','H124大戶實證狀態':'PENDING_EVIDENCE',
           'H110研究資訊截止日':'2026-10-16'}
        report=build_authority({'audit':pd.DataFrame([r])})
        health=dict(zip(report['h126_health']['項目'],report['h126_health']['數值']))
        self.assertEqual(health['禁止新倉／隔離觀察'],0)
        self.assertEqual(health['C稽核候選'],1)
        self.assertEqual(health['A認證黑馬研究'],0)
    def test_h129_export_hooks_are_wired(self):
        from pathlib import Path
        code=(Path(__file__).resolve().parent.parent/'pages/7_股神推薦.py').read_text(encoding='utf-8')
        for mark in ('_h129_audit_t1(', '_h129_risk_watch(', '00B_H129風控複核', '08A_H129績效品質'):
            self.assertIn(mark, code)

if __name__=='__main__':unittest.main()

class H129LearningPoisoningTests(unittest.TestCase):
    def test_legacy_categories_cannot_poison_h102_learning(self):
        import json, tempfile
        from pathlib import Path
        from unittest.mock import patch
        import godpick_h102_sector_rotation_closed_loop as h102
        with tempfile.TemporaryDirectory() as folder:
            records = []
            for d, a, valid in [
                ('2026-10-01',-7664.5, '是'),
                ('2026-10-04',1.0,'是'),  # Sunday is not a valid signal day
                ('2026-10-05',2.5,'否'),
                ('2026-10-06',3.5,'是'),
            ]:
                records.append({'股票代號':'6285','類別':'網通','推薦日期':d,
                                '隔日日期':'2026-10-07' if d!='2026-10-04' else '2026-10-05',
                                'T1成熟':'是','績效唯一樣本':'是',
                                'H103執行學習可用':valid,'H103績效口徑':'FIXED_PLAN_REPLAY',
                                'H103路徑歧義':'否','Selection Alpha%':a,'隔日候選漲跌%':1.0})
            (Path(folder)/'godpick_t1_trade_truth.json').write_text(json.dumps({'records':records},ensure_ascii=False),encoding='utf-8')
            with patch.object(h102,'BASE_DIR',Path(folder)):
                h102._T1_CACHE.clear()
                out = h102._local_t1_category_stats('2026-10-08')
                self.assertEqual(out.get('網通',{}).get('n'),1)
                self.assertEqual(out['網通']['mean'],3.5)
