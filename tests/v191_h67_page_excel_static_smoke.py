# -*- coding: utf-8 -*-
from pathlib import Path


def main():
    p=Path(__file__).resolve().parents[1]/'pages'/'7_股神推薦.py'
    s=p.read_text(encoding='utf-8')
    required=[
        'H67_GOVERNANCE_EXPECTED_VERSION','apply_h67_regime_consensus','build_h67_priority_table','build_h67_governance_table',
        '超級AI次日治理｜H67 Regime × Sector × Consensus','H67次日優先治理','H67治理摘要',
        'H67T1治理分','_H67研究優先','P1/P2/C1',
        'page07_v191_h67_regime_sector_consensus_preopen_truth_20260910',
    ]
    for k in required: assert k in s, k
    assert 'H64正式推薦門檻不降低' in s
    print('PASS v191_h67_page_excel_static_smoke')

if __name__=='__main__': main()
