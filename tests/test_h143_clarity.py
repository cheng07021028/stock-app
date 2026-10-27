# -*- coding: utf-8 -*-
"""H143 fail-closed clarity and adversarial provenance regressions."""
from pathlib import Path
import sys
import unittest
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from godpick_h143_clarity import audit_h143

class H143Clarity(unittest.TestCase):
    def input(self, n=4):
        codes=['2324','2413','3044','2453'][:n]
        a=pd.DataFrame([{'股票代號':c,'股票名稱':'測試'+c,'類別':'EMS代工' if i==0 else '電子',
                         'H113流動性合格':'是' if i%2==0 else '否',
                         'H113大戶鎖碼合格':'是' if i in (0,1) else '否',
                         'H123法人實證狀態':'PENDING_EVIDENCE',
                         'H124大戶實證狀態':'PENDING_EVIDENCE',
                         'H109決策權威':'FADE_VETO'} for i,c in enumerate(codes)])
        f=pd.DataFrame([{'股票代號':c,'產業類別':'EMS代工' if i==0 else '電子',
                         'H109原風控':'FADE_VETO','H142退潮證據級別':'UNVERIFIED_FADE_LABEL'} for i,c in enumerate(codes)])
        h=pd.DataFrame([{'項目':'原候選去重數','數值':6},
                        {'項目':'禁止新倉／隔離觀察','數值':6},
                        {'項目':'H128研究資訊截止日（非匯出時間）','數值':'2026-10-22'}])
        t1=pd.DataFrame([{'項目':'H129原T+1檢討筆數','數值':80},
                         {'項目':'H129經H103驗證可用筆數','數值':7},
                         {'項目':'H129有效Alpha平均pp','數值':-0.4974}])
        return {'h126_health':h,'h126_authority':pd.DataFrame(), 'audit':a,
                'h142_evidence':f,'h129_t1_health':t1}

    def test_four_tabs_have_explicit_research_only_status(self):
        summary,blind,pool,challenge=audit_h143(self.input(),generated_at='2026-10-26T13:46:21')
        self.assertEqual(len(pool),4)
        self.assertTrue((pool['研究處置']=='核資料來源／日期；不得復權為買進').all())
        self.assertTrue((blind['是否可買進']=='否；仍依H126/H109').all())
        self.assertGreaterEqual(len(challenge),5)

    def test_staleness_and_unmatched_populations(self):
        summary,blind,_,_=audit_h143(self.input(),generated_at='2026-10-26T13:46:21')
        scores=dict(zip(summary['重點'],summary['結果']))
        self.assertEqual(scores['與匯出日相差天數'],4)
        self.assertEqual(scores['H126原候選去重'],6)
        self.assertEqual(scores['H113/H142可稽核股票'],4)
        self.assertEqual(scores['H126－H113/H142母體差'],2)
        self.assertEqual(scores['H142退潮僅標籤未證明'],4)
        self.assertEqual(scores['H123法人尚未PROVEN'],4)
        self.assertEqual(scores['H124大戶尚未VERIFIED'],4)
        self.assertIn('B02',set(blind['編號']))

    def test_does_not_modify_canonical_frames(self):
        frames=self.input()
        original={k:v.copy(deep=True) for k,v in frames.items()}
        audit_h143(frames,generated_at='2026-10-26T13:46:21')
        for k,v in frames.items():pd.testing.assert_frame_equal(v,original[k])

    def test_fa_de_mark_is_not_auto_overridden(self):
        _,_,pool,claims=audit_h143(self.input(),generated_at='2026-10-26T13:46:21')
        self.assertTrue(pool['H109硬風控'].eq('FADE_VETO').all())
        self.assertIn('不成立',claims.loc[claims['編號']=='C01','判斷'].iloc[0])

    def test_empty_inputs_fail_closed(self):
        tabs=audit_h143({},generated_at='2026-10-26')
        self.assertEqual(len(tabs),4)
        self.assertEqual(len(tabs[2]),0)
        self.assertNotIn('買進',str(tabs[0].loc[tabs[0]['重點']=='主管指示','結果'].iloc[0]))

    def test_no_rank_or_price_fields_in_triage(self):
        _,_,pool,_=audit_h143(self.input(),generated_at='2026-10-26')
        forbidden=('目標價','買入價','進場價','推薦分','停損','勝率','預期報酬')
        for column in pool.columns:
            self.assertFalse(any(t in column for t in forbidden),column)

    def test_zero_length_missing_cutoff_cannot_claim_fresh(self):
        d=self.input()
        d['h126_health']=pd.DataFrame()
        summary,blind,_,_=audit_h143(d,generated_at='2026-10-27T16:00:00')
        self.assertEqual(dict(zip(summary['重點'],summary['結果']))['與匯出日相差天數'],'未取得可核對證據')
        self.assertIn('B01',set(blind['編號']))

if __name__=='__main__': unittest.main()
