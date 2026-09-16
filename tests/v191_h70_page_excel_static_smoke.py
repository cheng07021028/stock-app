# -*- coding: utf-8 -*-
from pathlib import Path

def main():
    s=Path('pages/7_股神推薦.py').read_text(encoding='utf-8')
    for token in [
        'H70_COUNTER_REGIME_EXPECTED_VERSION', 'apply_h70_counter_regime_session_truth',
        '超級AI逆勢Alpha｜H70', 'H70逆勢Alpha觀察', 'H70治理摘要',
        'V191-H70-COUNTER-REGIME-SESSION-TRUTH-EXCEL',
    ]:
        assert token in s, token
    assert 'page07_v191_h73_leadership_breadth_distribution_truth_20260916' in s
    print('PASS H70 Page07/Excel static')

if __name__ == '__main__': main()
