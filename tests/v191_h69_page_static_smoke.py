# -*- coding: utf-8 -*-
from pathlib import Path


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    p7 = (root / 'pages' / '7_股神推薦.py').read_text(encoding='utf-8')
    p16 = (root / 'pages' / '16_官方因子快取中心.py').read_text(encoding='utf-8')
    assert 'V191-H69 官方因子單一真相' in p7
    assert "官方治理母體數" in p7
    assert "H69 前保存的掃描品質快照" in p7
    assert '每日官方高可信列' in p16
    assert '曾使用備援補值' in p16
    assert '純官方資料列' not in p16
    assert '備援補值' in p16 and '不代表整列不是官方高可信' in p16
    print('PASS page static')


if __name__ == '__main__':
    main()
