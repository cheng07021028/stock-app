# -*- coding: utf-8 -*-
"""H116 targeted Page07 hook; backup + compile + atomic install; never overwrite full page."""
from __future__ import annotations

import argparse
import ast
import hashlib
from datetime import datetime
from pathlib import Path
import os
import tempfile

ROOT=Path(__file__).resolve().parent.parent
TARGET=ROOT/'pages'/'7_股神推薦.py'
MARKER='    # H116_ACTUAL_SCAN_SNAPSHOT_HOOK_BEGIN'
LIMITS_HEAD='H85_H79_CORE_LIMITS = {\n'
LIMITS_ADD='    "blackhorse_overview": 30,\n    "h114_parallel_watch": 40,\n'
ANCHOR='''        _h86_candidate = st.session_state.get(_k("candidate_diagnosis_store"))'''
PRE='''        _h88_anchor_ok, _h88_publish_msg = _h88_publish_result_nonblocking('''
INSERT='''        # H116_ACTUAL_SCAN_SNAPSHOT_HOOK_BEGIN
        # Only during an actual scan (NOT on Streamlit rerun/Excel export).
        # Synchronous remote readback: queued/local writes never count as permanent.
        try:
            from godpick_h116_daily_feedback import make_decision_snapshot, github_publish_verified
            _h116_ctx = st.session_state.get(_k("recommend_execution_context_v191"), {}) or {}
            _h116_core = st.session_state.get(_k(H85_H79_CORE_SESSION_KEY), {}) or {}
            _h116_rid = _safe_str(_h116_ctx.get("run_id"))
            _h116_market_day = ""
            for _h116_table in ("blackhorse_overview", "research", "audit"):
                for _h116_row in (_h116_core.get(_h116_table, []) or [])[:10]:
                    if isinstance(_h116_row, dict):
                        _h116_market_day = _safe_str(_h116_row.get("H110行情交易日"))[:10]
                    if _h116_market_day:
                        break
                if _h116_market_day:
                    break
            _h116_market_basis = "H110_ROW_MARKET_DATE" if _h116_market_day else "SCAN_DATE_FALLBACK_UNVERIFIED"
            if not _h116_market_day:
                _h116_market_day = _safe_str(_h116_ctx.get("run_date"))[:10]
            _h116_snapshot = make_decision_snapshot(
                _h116_core,
                run_id=_h116_rid,
                decided_at=_safe_str(st.session_state.get(_k("result_saved_at"))),
                market_day=_h116_market_day,
                market_day_basis=_h116_market_basis,
            )
            _h116_result = github_publish_verified(_h116_snapshot, secrets=st.secrets)
            st.session_state[_k("h116_snapshot_result")] = dict(_h116_result)
            if _h116_result.get("permanent"):
                st.success("H116：本輪研究快照已寫入 GitHub 並回讀驗證；" + _safe_str(_h116_result.get("path")))
            else:
                st.warning("H116：本輪研究快照尚未永久保存；" + _safe_str(_h116_result.get("status")))
        except Exception as _h116_exc:
            st.session_state[_k("h116_snapshot_result")] = {
                "permanent": False, "status": "FAILED", "error_type": type(_h116_exc).__name__
            }
            st.warning("H116：本輪研究快照保存失敗；請檢查 GitHub 設定或 H97 core 版本，不影響原推薦。")
        # H116_ACTUAL_SCAN_SNAPSHOT_HOOK_END
'''


def patch(page:str) -> str:
    if MARKER in page:
        if page.count(MARKER)!=1 or '    "h114_parallel_watch": 40,' not in page:
            raise ValueError('H116部分修補，停止避免混合版本')
        return page
    if page.count(LIMITS_HEAD)!=1:
        raise ValueError('H85 compact core缺少唯一設定區塊，無法安全保留H114兩個候選表')
    if page.count(ANCHOR)!=1 or page.count(PRE)!=1:
        raise ValueError('第7頁版本不符合H116單點hook契約，不執行修改')
    at=page.index(ANCHOR)
    begin=page.index(PRE)
    if not (begin < at and at-begin < 1500):
        raise ValueError('掃描保存區段與錨點不相鄰，不允許跨區修改')
    new=page[:at]+INSERT+page[at:]
    # The original H85 compact encoder DROPS both H114/blackhorse tables.
    # Preserve bounded actual decision-time versions for H116, not a rebuilt
    # next-day ranking. Without this, H116 would only snapshot Research rows.
    if '    "h114_parallel_watch": 40,' not in new:
        new=new.replace(LIMITS_HEAD, LIMITS_HEAD+LIMITS_ADD, 1)
    ast.parse(new)
    return new


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--check-only',action='store_true')
    args=ap.parse_args()
    if not TARGET.exists():raise SystemExit(f'找不到 {TARGET}')
    before=TARGET.read_text(encoding='utf-8-sig')
    new=patch(before)
    print('H116 PAGE07', 'ALREADY_PATCHED' if new==before else 'READY',
          'previous_sha256='+hashlib.sha256(before.encode()).hexdigest(),
          'mode=' + ('CHECK_ONLY' if args.check_only else 'APPLY'))
    if args.check_only or new==before:return
    backup=TARGET.with_name(TARGET.name+'.h116backup.'+datetime.now().strftime('%Y%m%d_%H%M%S'))
    backup.write_text(before,encoding='utf-8')
    fd,tmp=tempfile.mkstemp(dir=str(TARGET.parent),prefix='.h116_',suffix='.tmp')
    try:
        with os.fdopen(fd,'w',encoding='utf-8',newline='\n') as f:
            f.write(new);f.flush();os.fsync(f.fileno())
        os.chmod(tmp, TARGET.stat().st_mode & 0o777)
        os.replace(tmp,TARGET)
        if patch(TARGET.read_text(encoding='utf-8'))!=new:raise RuntimeError('回讀與預期修正不一致')
    finally:
        if os.path.exists(tmp):os.unlink(tmp)
    print('H116 PAGE07 PASS; backup=',backup.name)

if __name__=='__main__':main()
