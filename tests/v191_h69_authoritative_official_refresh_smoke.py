# -*- coding: utf-8 -*-
from __future__ import annotations
import pandas as pd

from official_factor_service import merge_official_factors, CACHE_VERSION


def main() -> None:
    base = pd.DataFrame([
        {
            '股票代號': '1101', '股票名稱': 'A',
            '官方資料完整度': 20, '官方因子資料狀態': '部分資料',
            '因子來源可信度': 60, '每日因子來源可信度': 60,
            '官方資料日期': '20260909', '官方因子資料日期': '20260909',
            '法人資料日期': '20260909', '估值資料日期': '20260909',
        },
        {
            '股票代號': '1102', '股票名稱': 'B',
            '官方資料完整度': 40, '官方因子資料狀態': '部分資料',
            '因子來源可信度': 60, '每日因子來源可信度': 60,
            '官方資料日期': '20260909', '官方因子資料日期': '20260909',
            '法人資料日期': '20260909', '估值資料日期': '20260909',
        },
    ])
    cache = pd.DataFrame([
        {
            '股票代號': '1101', '官方資料完整度': 95, '官方因子資料狀態': '完整',
            '因子來源可信度': 100, '每日因子來源可信度': 100,
            '官方資料日期': '20260911', '官方因子資料日期': '20260911',
            '法人資料日期': '20260911', '估值資料日期': '20260911',
            '官方因子資料源': 'TWSE current OpenAPI',
        },
        {
            '股票代號': '1102', '官方資料完整度': 100, '官方因子資料狀態': '完整',
            '因子來源可信度': 100, '每日因子來源可信度': 100,
            '官方資料日期': '20260911', '官方因子資料日期': '20260911',
            '法人資料日期': '20260911', '估值資料日期': '20260911',
            '官方因子資料源': 'TWSE current OpenAPI',
        },
    ])
    out = merge_official_factors(base, cache).sort_values('股票代號').reset_index(drop=True)
    assert out['官方資料完整度'].tolist() == [95, 100], out[['股票代號','官方資料完整度']]
    assert out['每日因子來源可信度'].tolist() == [100, 100]
    assert out['官方因子資料日期'].astype(str).tolist() == ['20260911', '20260911']
    assert out['官方因子快取匹配'].tolist() == ['是', '是']
    assert out['官方因子權威同步版本'].str.contains('H69').all()
    assert 'h69' in CACHE_VERSION.lower()
    print('PASS authoritative refresh')


if __name__ == '__main__':
    main()
