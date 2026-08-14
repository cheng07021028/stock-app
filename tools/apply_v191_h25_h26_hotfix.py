# -*- coding: utf-8 -*-
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]

page = ROOT / 'pages' / '7_股神推薦.py'
src = page.read_text(encoding='utf-8-sig')
old = '''    work = _apply_official_factor_cache_v109(work)\n    try:\n        from godpick_formal_recommendation_engine import apply_formal_recommendation_engine\n'''
new = '''    work = _apply_official_factor_cache_v109(work)\n    # V191-H25: formal/A- market alignment must use the same authoritative\n    # market_snapshot business date restored by H21/H22. Never manufacture\n    # today's date and never relax stale-Kline governance.\n    try:\n        from godpick_market_candidate_bridge import apply_authoritative_market_context\n        work, _h25_market_report = apply_authoritative_market_context(work, base_dir=BASE_DIR)\n        try:\n            st.session_state[_k("v191_h25_market_bridge_report")] = dict(_h25_market_report)\n        except Exception:\n            pass\n    except Exception as _h25_market_exc:\n        try:\n            st.session_state[_k("v191_h25_market_bridge_report")] = {\n                "ok": False, "message": f"H25大盤權威橋接失敗：{_h25_market_exc}"\n            }\n        except Exception:\n            pass\n    try:\n        from godpick_formal_recommendation_engine import apply_formal_recommendation_engine\n'''
if 'apply_authoritative_market_context(work, base_dir=BASE_DIR)' not in src:
    if old not in src:
        raise SystemExit('Page7 H20 insertion anchor not found')
    src = src.replace(old, new, 1)
    page.write_text(src, encoding='utf-8')

utils = ROOT / 'utils.py'
usrc = utils.read_text(encoding='utf-8-sig')
if 'V191-H26 completed-session cache policy' not in usrc:
    pattern = r'def _history_cache_allowed_business_lag\(end_date\) -> int:\n.*?(?=\ndef _history_disk_cache_key)'
    replacement = '''def _history_cache_allowed_business_lag(end_date, now_tw=None) -> int:\n    """V191-H26 completed-session cache policy.\n\n    T-1 cache is permitted only while today's session is still open. Once a\n    market day is completed, that completed day requires business lag=0 even\n    after midnight or across the weekend.\n    """\n    try:\n        target = pd.to_datetime(end_date, errors="coerce")\n        if pd.isna(target):\n            return 1\n        target = target.normalize()\n        if now_tw is None:\n            now_tw = datetime.now(ZoneInfo("Asia/Taipei"))\n        elif getattr(now_tw, "tzinfo", None) is None:\n            now_tw = now_tw.replace(tzinfo=ZoneInfo("Asia/Taipei"))\n        else:\n            now_tw = now_tw.astimezone(ZoneInfo("Asia/Taipei"))\n        today = pd.Timestamp(now_tw.date())\n        after_close = (now_tw.hour, now_tw.minute) >= (14, 15)\n        if today.weekday() < 5 and after_close:\n            latest_completed = today\n        else:\n            latest_completed = today - pd.Timedelta(days=1)\n            while latest_completed.weekday() >= 5:\n                latest_completed -= pd.Timedelta(days=1)\n        if target == latest_completed:\n            return 0\n        if target == today and today.weekday() < 5:\n            return 0 if after_close else 1\n        if target > latest_completed and target.weekday() >= 5:\n            return 0\n        return 1\n    except Exception:\n        return 1\n\n'''
    updated, n = re.subn(pattern, replacement, usrc, count=1, flags=re.S)
    if n != 1:
        raise SystemExit(f'utils H26 replacement count={n}')
    utils.write_text(updated, encoding='utf-8')

print('V191-H25/H26 production source patch applied/verified')
