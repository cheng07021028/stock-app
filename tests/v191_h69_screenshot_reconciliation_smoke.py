# -*- coding: utf-8 -*-
from __future__ import annotations
from datetime import datetime
import pandas as pd

from official_factor_service import merge_official_factors
from godpick_execution_governance import build_scan_quality_report


def report(frame: pd.DataFrame) -> dict:
    n=len(frame)
    return build_scan_quality_report(
        {'total_count': n, 'analyzed_ok': n, 'history_ok': n},
        universe_size=n, candidate_count=n, final_count=4,
        candidate_frame=frame,
        now_taipei=datetime(2026,9,13,9,0),
    )


def main() -> None:
    major_n=1096
    old_effective=671  # 61.22%, reproduces the Page07 symptom in the screenshot.
    base=[]
    cache=[]
    for i in range(major_n):
        code=f'{1000+i:04d}'[-4:]
        base.append({
            '股票代號':code, '市場別':'上市' if i<700 else '上櫃', '成交額百萬':100,
            '官方資料完整度':60 if i<old_effective else 40,
            '官方因子資料狀態':'完整' if i<old_effective else '部分資料',
            '因子來源可信度':100 if i<old_effective else 60,
            '每日因子來源可信度':100 if i<old_effective else 60,
            '官方因子資料日期':'20260911' if i<old_effective else '20260909',
            '本輪市場最新交易日':'20260911',
        })
        complete = i < 1084
        cache.append({
            '股票代號':code,
            '官方資料完整度':90 if complete else 40,
            '官方因子資料狀態':'完整' if complete else '部分資料',
            '因子來源可信度':100,
            '每日因子來源可信度':100,
            '官方因子資料日期':'20260911',
            '官方資料日期':'20260911',
            '法人資料日期':'20260911',
            '估值資料日期':'20260911',
            '官方因子資料源':'TWSE/TPEx current OpenAPI',
        })
    # Extension rows are not part of Page16's listed+OTC health denominator.
    for i in range(122):
        base.append({
            '股票代號':f'{8000+i:04d}'[-4:], '市場別':'興櫃', '成交額百萬':100,
            '官方資料完整度':0, '官方因子資料狀態':'', '因子來源可信度':0,
            '每日因子來源可信度':0, '官方因子資料日期':'', '本輪市場最新交易日':'20260911',
        })
    base_df=pd.DataFrame(base)
    pre=report(base_df)
    assert 61.0 <= pre['官方有效因子覆蓋率%'] <= 61.4, pre
    merged=merge_official_factors(base_df, pd.DataFrame(cache))
    post=report(merged)
    expected=1084/1096*100
    assert abs(post['官方有效因子覆蓋率%']-expected) < 0.02, post
    assert post['官方治理母體數']==1096, post
    assert post['官方治理排除非適用數']==122, post
    print(f"PASS screenshot reconciliation: {pre['官方有效因子覆蓋率%']:.1f}% -> {post['官方有效因子覆蓋率%']:.1f}%")

if __name__=='__main__':
    main()
