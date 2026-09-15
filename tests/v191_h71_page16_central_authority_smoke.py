# -*- coding: utf-8 -*-
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
text=(ROOT/'pages/16_官方因子快取中心.py').read_text(encoding='utf-8')
assert 'V191-H71' in text
assert '由中央 worker 立即更新官方因子' in text
assert 'manual_job="official_factors"' in text
assert 'load_factor_cache(force_authority_restore=True)' in text
assert 'H71中央官方因子工作' in text
print('PASS H71 Page16 central authority')
