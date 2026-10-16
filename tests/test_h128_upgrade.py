# -*- coding: utf-8 -*-
"""H128 synthetic regression: no real prices, no financial performance claims."""
import sys
import unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from godpick_h115_walkforward import evaluate_snapshots

class H128Upgrade(unittest.TestCase):
    @staticmethod
    def event():
        return [{'event_id':'synth', 'code':'2468','strategy':'H128TEST',
            'tier':'RESEARCH_ONLY', 'decided_at':'2026-10-12T20:00:00+08:00',
            'latest_evidence_at':'2026-10-12T19:00:00+08:00'}]

    @staticmethod
    def bars(extreme=False):
        days=['2026-10-12','2026-10-13','2026-10-14','2026-10-15','2026-10-16','2026-10-19']
        out=[]
        for i,day in enumerate(days):
            c=1200 if i==1 and extreme else 101
            out.append({'code':'2468','date':day,'open':100,'close':c,'high':max(110,c), 'low':99})
        return out

    def test_extreme_t1_price_is_quarantined_and_no_strategy_learning(self):
        report=evaluate_snapshots(self.event(), self.bars(extreme=True))
        r=next(x for x in report['details'] if x['horizon']==1)
        self.assertEqual(r['data_status'],'H128_ANOMALOUS_RETURN_QUARANTINE')
        self.assertIsNone(r['net_return_pct'])
        self.assertIsNone(r['alpha_pp'])
        self.assertGreater(r['h128_suspect_returns']['gross_return_pct'],100)
        self.assertNotIn(1,[x['horizon'] for x in report['summary']])
        self.assertGreaterEqual(report['failures'].get('H128_ANOMALOUS_RETURN_QUARANTINE',0),1)

    def test_normal_price_is_unaffected(self):
        report=evaluate_snapshots(self.event(),self.bars())
        r=next(x for x in report['details'] if x['horizon']==1)
        self.assertEqual(r['data_status'],'OK')
        self.assertIsNotNone(r['net_return_pct'])

    def test_benchmark_corporate_action_scale_error_is_quarantined(self):
        days=['2026-10-12','2026-10-13','2026-10-14','2026-10-15','2026-10-16','2026-10-19']
        benchmark=[{'code':'BENCHMARK','date':d,'open':100,
                    'close':200 if i==1 else 100,
                    'high':201 if i==1 else 101,'low':99} for i,d in enumerate(days)]
        report=evaluate_snapshots(self.event(),self.bars(),benchmark=benchmark)
        r=next(x for x in report['details'] if x['horizon']==1)
        self.assertEqual(r['data_status'],'H128_ANOMALOUS_RETURN_QUARANTINE')
        self.assertIsNone(r['benchmark_net_pct'])

    def test_page8_destructive_remote_confirm_guard_is_wired(self):
        page=(ROOT/'pages'/'8_股神推薦紀錄.py').read_text(encoding='utf-8-sig')
        self.assertIn('require_remote_confirm=bool(deleted_ids)',page)
        self.assertIn('Reboot 仍可能恢復',page)

    def test_manager_report_discloses_original_data_dates(self):
        import pandas as pd
        from godpick_h126_authority_report import build_authority
        out=build_authority({'research':pd.DataFrame([{
            '股票代號':'2468','股票名稱':'合成樣本','市場別':'上市',
            'H110行情交易日':'2026-10-14','H110研究資訊截止日':'2026-10-14',
            'H110研究模式':'SAFE_REPLAY_NO_FETCH'}])})
        metadata=dict(zip(out['h126_health']['項目'],out['h126_health']['數值']))
        self.assertEqual(metadata['H128市場資料交易日（非匯出時間）'],'2026-10-14')
        self.assertIn('SAFE_REPLAY_NO_FETCH',metadata['H128回放／研究模式'])
        self.assertTrue(all('禁止' in str(x)
                            for x in out['h126_authority']['H126能否買進']))

    def test_h127_h126_h123_h124_h125_all_active(self):
        page=(ROOT/'pages'/'7_股神推薦.py').read_text(encoding='utf-8-sig')
        core=(ROOT/'godpick_h105_future_blackhorse_engine.py').read_text(encoding='utf-8-sig')
        service=(ROOT/'godpick_persistence_service.py').read_text(encoding='utf-8-sig')
        for mark in ('# H125_SELECTED_CATEGORIES_AUTHORITY','# H111_VERIFIED_SCAN_SETTINGS_AUTHORITY','H126'):
            self.assertIn(mark,page)
        for mark in ('godpick_h124_runtime_bridge','h123_load_context','h123_strict','_h126_attach_authority'):
            self.assertIn(mark,core)
        self.assertIn('# >>> V191_H127_RECORDS_SHARD_AUTHORITY',service)

if __name__=='__main__':unittest.main()
