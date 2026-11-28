# -*- coding: utf-8 -*-
import copy,json,sys,types,unittest
from pathlib import Path
from unittest.mock import patch
import pandas as pd
import test_h250r7_tracking as legacy
import godpick_h250r7_tracking as r
import godpick_h250r9_evidence as e
from godpick_h250r6_monitor import safe_tracking_message

DAYS,NOW=legacy.DAYS,legacy.NOW

def market_archive(other=False,both=False,missing=None):
    a=legacy.archive(missing=missing)
    a.markets={m:{d:1000 for d in DAYS} for m in ('TWSE','TPEX')}
    if other:
        for source in ('bars','quotes'):
            d=getattr(a,source);d['TPEX','2330']=copy.deepcopy(d['TWSE','2330'])
            if not both:del d['TWSE','2330']
    return a

class Evidence(unittest.TestCase):
    def test_market_resolution(self):
        x=legacy.row();y,m,reason=e.resolve(x,market_archive(other=True));self.assertEqual(y['市場別'],'TPEX');self.assertEqual(m['original'][1],'TWSE');self.assertEqual(x['市場別'],'上市');self.assertEqual(reason,'')
    def test_closed_day_uses_preceding_quote_and_next_session(self):
        a=market_archive();x=legacy.row(推薦日期='2026-11-07',推薦價格=104);y,m,reason=e.resolve(x,a)
        self.assertEqual(y['推薦日期'],'2026-11-06');self.assertEqual(x['推薦日期'],'2026-11-07');self.assertTrue(m['publication_closed'])
        p,s,g=r.build_extended_plan([x],a,r.TASKS[1],5000,{});z=dict(x,**p['A']);self.assertEqual(s['CLOSED_PUBLICATION_RESOLVED'],1)
        self.assertAlmostEqual(e.saved_nday(z,1,NOW),round((105/104-1)*100,2));self.assertEqual(json.loads(z[r.NPROOF])['periods']['1']['target'],'2026-11-09')
    def test_closed_date_price_conflict_not_guessed(self):
        p,s,g=r.build_extended_plan([legacy.row(推薦日期='2026-11-07',推薦價格=99)],market_archive(),r.TASKS[1],5000,{})
        self.assertFalse(p);self.assertEqual(s['QUOTE_PENDING'],1);self.assertEqual(g['CLOSED_REFERENCE_PRICE_UNCORROBORATED'],1)
    def test_missing_both_partitions_no_market_switch(self):
        a=market_archive(other=True);del a.markets['TWSE'][DAYS[0]]
        p,s,g=r.build_extended_plan([legacy.row()],a,r.TASKS[1],5000,{});self.assertFalse(p);self.assertNotIn('OFFICIAL_MARKET_RESOLVED',s)
    def test_ambiguous_market_blocked(self):
        p,s,g=r.build_extended_plan([legacy.row()],market_archive(other=True,both=True),r.TASKS[1],5000,{});self.assertFalse(p);self.assertEqual(g['AMBIGUOUS_OFFICIAL_MARKET'],1)
    def test_missing_date_never_uses_update_time(self):
        x=legacy.row(推薦日期='',更新時間=DAYS[0],推薦時間='08:00:00');p,s,g=r.build_extended_plan([x],market_archive(),r.TASKS[1],5000,{})
        self.assertFalse(p);self.assertEqual(s['IDENTITY_PENDING'],1)
    def test_closed_latest_day_not_due(self):
        p,s,g=r.build_extended_plan([legacy.row(推薦日期='2026-11-28',推薦價格=119)],market_archive(),r.TASKS[1],5000,{})
        self.assertFalse(p);self.assertEqual(s['NOT_DUE'],1)
    def test_market_mapping_all_three_idempotent(self):
        a=market_archive(other=True);x=legacy.row();rows=[x]
        for k in r.TASKS:
            p,s,g=r.build_extended_plan(rows,a,k,5000,{'adjusted_max_symbols':0});self.assertIn(e.MAP,p['A']);self.assertNotIn('市場別',p['A']);self.assertNotIn('推薦日期',p['A']);rows=[dict(rows[0],**p['A'])]
            p2,_,_=r.build_extended_plan(rows,a,k,5000,{'adjusted_max_symbols':0});self.assertFalse(p2)
        self.assertEqual(e.saved_nday(rows[0],1,NOW),1);self.assertEqual(rows[0]['市場別'],'上市');self.assertEqual(rows[0]['推薦後1日%'],99)
    def test_mapping_cannot_use_legacy_adjusted_reuse(self):
        p,s,g=r.build_extended_plan([legacy.row()],market_archive(other=True),r.TASKS[0],5000,{'adjusted_max_symbols':0});self.assertEqual(s.get('PAGE08_CURRENT_REUSED',0),0);self.assertEqual(s['ADJUSTED_PENDING'],1)
    def test_tampered_mapping_or_formula_rejected(self):
        a=market_archive(other=True);x=legacy.row();p,_,_=r.build_extended_plan([x],a,r.TASKS[1],5000,{});y=dict(x,**p['A']);self.assertEqual(e.saved_nday(y,1,NOW),1)
        self.assertIsNone(e.saved_nday(dict(y,股票代號='9999'),1,NOW));self.assertIsNone(e.saved_nday(dict(y,推薦價格=999),1,NOW));self.assertIsNone(e.saved_nday(dict(y,**{'官方原價推薦後1日%':999}),1,NOW))
    def test_legacy_values_not_verified(self):
        self.assertIsNone(e.saved_nday(legacy.row(),1,NOW))
    def test_snapshot_not_history_statistics(self):
        x=legacy.row();p,_,_=r.build_extended_plan([x],market_archive(),r.TASKS[1],5000,{});y=dict(x,**p['A']);self.assertIsNone(e.saved_nday(dict(y,資料來源='最新推薦快照'),1,NOW))
    def test_mixed_dates_no_clock_inference(self):
        s=e.publication_dates(pd.Series(['2026-05-13','2026-05-13 03:10:59','2026/5/13','03:10:59','2026-02-30','']));self.assertEqual(s.notna().sum(),3);self.assertEqual(len(set(s[:3])),1)
    def test_numeric_telemetry_no_secrets(self):
        msg='R9 正式N日績效｜08權威 2385 筆／永久確認 1408 筆／IDENTITY_PENDING 47、DEFERRED 0｜secret=https://token@example.com'
        v=safe_tracking_message(r.TASKS[1],msg);self.assertIn('永久確認 1408 筆',v);self.assertIn('原始身分待證 47',v);self.assertNotIn('secret',v);self.assertFalse(safe_tracking_message('other',msg))
    def test_partial_mapping_no_original_mutation(self):
        a=market_archive(other=True);x=legacy.row();p,s,g=r.build_extended_plan([x],a,r.TASKS[1],5000,{});obj=legacy.Authority();obj.setUp();obj.rows=[x]
        report=r.exact_commit(obj.rows,p,'signature',obj.h,obj.p);final=obj.commit_calls[0][3][0];self.assertEqual(final['市場別'],x['市場別']);self.assertEqual(final['推薦日期'],x['推薦日期']);self.assertEqual(final['record_id'],x['record_id']);self.assertEqual(obj.commit_calls[0][4],set())
    def test_page08_and_10_schema_and_buttons(self):
        root=Path(__file__).resolve().parents[1];p8=(root/'pages/8_股神推薦紀錄.py').read_text();p10=(root/'pages/10_推薦清單.py').read_text()
        self.assertIn('NORMALIZED_RECORD_CACHE_VERSION_V165 = "v178_integrity_r9_',p8);self.assertIn('官方原價核實統計',p8);self.assertIn('官方原價核實統計',p10)
        self.assertIn('drop_duplicates(subset=["_merge_key"], keep="first")',p10);self.assertIn('publication_dates(work["推薦日期"])',p10)
        for k in r.TASKS:self.assertIn(f'dispatch_one(st, "{k}")',p10)
    def test_legacy_merge_preserves_proof(self):
        if 'streamlit' not in sys.modules:sys.modules['streamlit']=types.SimpleNamespace(secrets={})
        import godpick_h241_github_records as h
        x=dict(legacy.row(),**{e.MAP:'old proof',r.NPROOF:'old N proof',r.HIT_PROOF:'hit','進場點命中':'是'})
        y=h._merge(x,dict(x,**{e.MAP:'',r.NPROOF:'replacement','進場點命中':'否','備註':'new'}));self.assertEqual(y[e.MAP],'old proof');self.assertEqual(y[r.NPROOF],'old N proof');self.assertEqual(y['進場點命中'],'是');self.assertEqual(y['備註'],'new')

if __name__=='__main__':unittest.main()
