# -*- coding: utf-8 -*-
from __future__ import annotations
import json
import tempfile
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path: sys.path.insert(0,str(ROOT))
import pandas as pd

import godpick_execution_governance as gov
import godpick_formal_recommendation_engine as formal
import official_factor_service as ofs


def _frame(official_date: str, n: int = 100, effective: int = 84) -> pd.DataFrame:
    rows=[]
    for i in range(n):
        good=i<effective
        rows.append({
            '股票代號': f'{1100+i:04d}',
            '成交額百萬': 300,
            '20日均成交額百萬': 250,
            '最新成交量_張': 1000,
            '20日均量_張': 900,
            '官方資料完整度': 100 if good else 0,
            '官方因子資料狀態': '完整' if good else '未取得官方資料',
            '因子來源可信度': 100 if good else 0,
            # Critical regression: first schema column exists but is blank.
            '官方因子資料日期': '',
            '官方資料日期': official_date if good else '',
            '法人資料日期': official_date if good else '',
            '本輪市場最新交易日': '2026-08-11',
            'K線最後交易日': '2026-08-11',
        })
    return pd.DataFrame(rows)


def _summary(n=100):
    return {'total_count':n,'analyzed_ok':n,'history_ok':n}


def test_blank_primary_date_column_falls_back_and_t1_is_usable():
    report=gov.build_scan_quality_report(_summary(), universe_size=100, candidate_count=100, final_count=8, candidate_frame=_frame('2026-08-10'))
    assert report['官方有效因子覆蓋率%'] == 84.0, report
    assert report['官方最新可信覆蓋率%'] == 84.0, report
    assert report['官方落後1日覆蓋率%'] == 84.0, report
    assert report['正式推薦可用'] is True, report
    assert abs(float(report['倉位折減係數']) - 0.75) < 1e-9, report
    assert 'T-1' in report['掃描品質狀態'], report


def test_lag_two_days_still_blocks():
    report=gov.build_scan_quality_report(_summary(), universe_size=100, candidate_count=100, final_count=8, candidate_frame=_frame('2026-08-07'))
    assert report['官方最新可信覆蓋率%'] == 0.0, report
    assert report['正式推薦可用'] is False, report
    assert float(report['倉位折減係數']) == 0.0, report


def test_formal_engine_t1_is_verified_degraded_not_stale():
    row=pd.Series({
        '本輪市場最新交易日':'2026-08-11',
        'K線最後交易日':'2026-08-11',
        '官方因子資料日期':'',
        '官方資料日期':'2026-08-10',
        '官方因子資料狀態':'完整',
    })
    info=formal._official_factor_freshness_info(row)
    assert info['one_day_lag'] is True, info
    assert info['effective_ready'] is True, info
    assert info['formal_ready'] is True, info
    assert 'T-1' in info['status'], info


def test_legacy_factor_cache_derives_conservative_daily_date():
    old_cache=ofs.CACHE_FILE
    try:
        with tempfile.TemporaryDirectory() as td:
            ofs.CACHE_FILE=Path(td)/'official_factors_cache.json'
            payload={
                'version':'legacy','updated_at':'2026-08-11 15:00:00','records':[
                    {'股票代號':'2330','官方資料日期':'','官方因子資料日期':'','法人資料日期':'20260810','估值資料日期':'20260811','官方資料完整度':100,'官方因子資料狀態':'完整'},
                ],'meta':{}
            }
            ofs.CACHE_FILE.write_text(json.dumps(payload,ensure_ascii=False),encoding='utf-8')
            df=ofs.load_factor_frame()
            assert df.loc[0,'官方資料日期']=='20260810', df.loc[0].to_dict()
            assert df.loc[0,'官方因子資料日期']=='20260810', df.loc[0].to_dict()
    finally:
        ofs.CACHE_FILE=old_cache


def test_page7_v184_wiring():
    src=(Path(__file__).resolve().parents[1]/'pages'/'7_股神推薦.py').read_text(encoding='utf-8')
    assert '真正資料/分析失敗' in src
    assert '策略軟門檻提示' in src
    assert 'official_lag >= 2' in src
    assert any(x in src for x in ['V184 latest recommendation authority','V185 latest recommendation authority'])
    assert 'v184_post_scan_ui_refresh' in src


if __name__=='__main__':
    test_blank_primary_date_column_falls_back_and_t1_is_usable()
    test_lag_two_days_still_blocks()
    test_formal_engine_t1_is_verified_degraded_not_stale()
    test_legacy_factor_cache_derives_conservative_daily_date()
    test_page7_v184_wiring()
    print('PASS V184 freshness/governance: blank-date fallback, verified T-1 degraded use, lag>=2 block, legacy date migration, persistence/UI wiring')
