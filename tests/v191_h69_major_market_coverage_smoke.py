# -*- coding: utf-8 -*-
from __future__ import annotations
from datetime import datetime
import pandas as pd

from godpick_execution_governance import build_scan_quality_report, EXECUTION_GOVERNANCE_VERSION


def main() -> None:
    rows = []
    # 8 major-market rows: all current, trusted, complete.
    for i in range(8):
        rows.append({
            '股票代號': f'{1100+i}',
            '市場別': '上市' if i < 5 else '上櫃',
            '官方資料完整度': 90,
            '官方因子資料狀態': '完整',
            '每日因子來源可信度': 100,
            '因子來源可信度': 100,
            '官方因子資料日期': '20260911',
            '本輪市場最新交易日': '20260911',
            '成交額百萬': 100,
        })
    # 12 extension rows deliberately have no official factors. They must not
    # dilute the listed+OTC official governance denominator.
    for i in range(12):
        rows.append({
            '股票代號': f'{7000+i}', '市場別': '興櫃',
            '官方資料完整度': 0, '官方因子資料狀態': '',
            '每日因子來源可信度': 0, '因子來源可信度': 0,
            '官方因子資料日期': '', '本輪市場最新交易日': '20260911',
            '成交額百萬': 100,
        })
    frame = pd.DataFrame(rows)
    summary = {
        'total_count': 20,
        'analyzed_ok': 20,
        'history_ok': 20,
    }
    report = build_scan_quality_report(
        summary,
        universe_size=20,
        candidate_count=20,
        final_count=2,
        candidate_frame=frame,
        now_taipei=datetime(2026, 9, 13, 9, 0),
    )
    assert report['官方治理口徑'] == '上市＋上櫃', report
    assert report['官方治理母體數'] == 8, report
    assert report['官方治理排除非適用數'] == 12, report
    assert abs(report['官方有效因子覆蓋率%'] - 100.0) < 1e-9, report
    assert abs(report['官方來源可信覆蓋率%'] - 100.0) < 1e-9, report
    assert abs(report['官方日期T-1內覆蓋率%'] - 100.0) < 1e-9, report
    assert 'h69' in EXECUTION_GOVERNANCE_VERSION.lower()
    print('PASS major-market single truth')


if __name__ == '__main__':
    main()
