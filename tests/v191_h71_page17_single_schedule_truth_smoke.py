# -*- coding: utf-8 -*-
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
text=(ROOT/'pages/17_系統健康檢查.py').read_text(encoding='utf-8')
assert 'V191-H71' in text
assert '官方因子排程｜V191 中央權威' in text
assert '立即由中央 worker 更新官方因子' in text
assert 'manual_job="official_factors"' in text
assert '舊版官方因子排程（唯讀相容資訊）' in text
assert 'st.checkbox("啟用官方因子自動更新"' not in text
assert 'st.selectbox("預計更新時間（台灣）"' not in text
print('PASS H71 Page17 single schedule truth')
