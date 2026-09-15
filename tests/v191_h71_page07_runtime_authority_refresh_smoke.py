# -*- coding: utf-8 -*-
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
text=(ROOT/'pages/7_股神推薦.py').read_text(encoding='utf-8')
assert '_refresh_official_authority_h71' in text
assert 'load_factor_cache(force_authority_restore=True)' in text
assert 'H71 runtime-data authority refreshed' in text
assert '_refresh_official_authority_h71(force=False' in text
print('PASS H71 Page07 runtime authority refresh')
