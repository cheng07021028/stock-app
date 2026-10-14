# -*- coding: utf-8 -*-
import unittest
import pandas as pd
from godpick_h126_authority_report import build_authority,attach_authority


def stock(code='3090', **kw):
    x={'股票代號':code,'股票名稱':'測試股','市場別':'上市','類別':'電子',
       'H109決策權威':'RESEARCH_ELIGIBLE','H110證據狀態':'VERIFIED',
       'H113入榜資格':'QUALIFIED','H113流動性合格':'是',
       'H113大戶鎖碼合格':'是','H113法人連買合格':'是',
       'H123法人實證狀態':'PROVEN_FLOW','H124大戶實證狀態':'VERIFIED_RISING',
       'H123來源雜湊':'a'*64,'H124大戶來源雜湊':'b'*64,
       'H123法人逐日有效天數':5,'H124大戶變化pp':0.12,
       'H123研究實際時間':'2026-10-14T16:30:00+08:00',
       'H105今日強勢排除':'否','H105發動階段':'BH1｜PREIGNITION',
       'H110最終研究優先分':77.5,'H110研究資訊截止日':'2026-10-14'}
    x.update(kw);return x

def tab(**frames):
    return {k:pd.DataFrame(v) for k,v in frames.items()}

class H126Tests(unittest.TestCase):
    def test_qualified_requires_three_proven_gates(self):
        result=build_authority(tab(research=[stock()]))['h126_authority']
        self.assertEqual(result.iloc[0]['H126認證資格'],'PASS')
        self.assertIn('非買進',result.iloc[0]['H126權威層別'])
        self.assertIn('禁止',result.iloc[0]['H126能否買進'])

    def test_h102_high_score_never_promotes(self):
        t=tab(priority_overview=[{'股票代號':'3090','股票名稱':'日電貿','H102動態優先分':99}])
        out=build_authority(t)
        self.assertEqual(out['h126_health'].iloc[2]['數值'],0)
        self.assertEqual(out['h126_authority'].iloc[0]['H126認證資格'],'NO_QUALIFIED')

    def test_fake_status_without_source_hash_is_blocked(self):
        s=stock();s.pop('H123來源雜湊');s.pop('H124大戶來源雜湊')
        out=build_authority(tab(research=[s]))['h126_authority']
        self.assertEqual(out.iloc[0]['H126認證資格'],'PENDING_EVIDENCE')

    def test_future_provenance_timestamp_not_accepted(self):
        s=stock(H123研究實際時間='2026-10-15T09:00:00+08:00')
        out=build_authority(tab(research=[s]))['h126_authority']
        self.assertEqual(out.iloc[0]['H126認證資格'],'PENDING_EVIDENCE')

    def test_missing_h123_h124_unverified_even_if_h113_pass(self):
        s=stock();s.pop('H123法人實證狀態');s.pop('H124大戶實證狀態')
        result=build_authority(tab(research=[s]))['h126_authority']
        self.assertEqual(result.iloc[0]['H126認證資格'],'PENDING_EVIDENCE')
        self.assertIn('未接入',result.iloc[0]['H126缺失與處置'])

    def test_h109_fade_veto(self):
        out=build_authority(tab(research=[stock(H109決策權威='FADE_VETO')]))['h126_authority']
        self.assertEqual(out.iloc[0]['H126認證資格'],'NO_QUALIFIED')
        # A hard veto may be omitted from the concise manager list, but never passed.
        self.assertNotIn('PASS',out['H126認證資格'].tolist())

    def test_h105_already_ignited_must_not_certify(self):
        out=build_authority(tab(research=[stock(H105發動階段='IG2｜ALREADY-IGNITED')]))['h126_authority']
        self.assertNotEqual(out.iloc[0]['H126認證資格'],'PASS')

    def test_conflicting_audit_overrides_positive_research(self):
        r=stock();a=stock(H113入榜資格='BLOCKED',H113法人連買合格='否')
        out=build_authority(tab(research=[r],audit=[a]))['h126_authority']
        self.assertNotEqual(out.iloc[0]['H126認證資格'],'PASS')

    def test_split_provenance_cannot_join_to_certify(self):
        a=stock();a.pop('H124大戶實證狀態')
        b=stock();b.pop('H123法人實證狀態')
        out=build_authority(tab(research=[a],audit=[b]))['h126_authority']
        self.assertEqual(out.iloc[0]['H126認證資格'],'PENDING_EVIDENCE')

    def test_legacy_formal_not_modified(self):
        t=tab(actionable=[stock('2330')],research=[stock('3090')])
        original=t['actionable'].copy(deep=True)
        out=attach_authority(t)
        pd.testing.assert_frame_equal(out['actionable'],original)
        self.assertEqual(out['h126_health'].loc[out['h126_health']['項目']=='A認證黑馬研究','數值'].iloc[0],2)

    def test_deterministic_and_bounded_watch(self):
        rows=[stock(f'{i:04d}',H113入榜資格='WATCH_ONLY',H110最終研究優先分=i) for i in range(10,50)]
        a=build_authority(tab(audit=rows,waiting=rows),watch_limit=6)['h126_authority']
        self.assertLessEqual(len(a),6)
        self.assertEqual(len(a['股票代號'].unique()),len(a))
        self.assertEqual(a.iloc[0]['股票代號'],'0049')

    def test_no_evidence_never_mislabels_qualification(self):
        out=build_authority(tab(audit=[{'股票代號':'2375','股票名稱':'凱美',
           'H113入榜資格':'BLOCKED','H102動態優先分':92.68}]))
        self.assertEqual(out['h126_health'].loc[out['h126_health']['項目']=='A認證黑馬研究','數值'].iloc[0],0)
        self.assertFalse((out['h126_authority']['H126認證資格']=='PASS').any())

if __name__=='__main__':unittest.main()
