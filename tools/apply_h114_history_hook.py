# -*- coding: utf-8 -*-
"""H114 optional safe Page-7 adapter: reuse existing historical K, no extra API.
Run in the stock-app project root:
  python tools/apply_h114_history_hook.py --check-only
  python tools/apply_h114_history_hook.py
No automatic modification on import; never touches H111 / Page 8 / user JSON.
"""
from __future__ import annotations
from pathlib import Path
import argparse
import datetime
import hashlib
import shutil

VER = "H114.2_SAFE_HISTORY_HOOK"
A = '        signal_snapshot = compute_signal_snapshot(hist_df)'
B = '            "signal_snapshot": signal_snapshot,'
C = '            "K線最後交易日": _safe_str(bundle.get("history_last_date")),'
D = '        ("00C_已發動觀察", _h78_export.get("ignited_watch", pd.DataFrame()), "本輪沒有已發動觀察股。"),'
INJECT_A = '''        # H114: reuse the OHLCV dataframe already fetched in this scan.
        # This is an optional RESEARCH-ONLY adapter; no extra network call.
        try:
            from godpick_h114_parallel_alpha import analyze_history as _h114_history
            h114_technical = _h114_history(hist_df, asof=end_dt)
        except Exception as _h114_error:
            h114_technical = {"H114K線資料狀態": "ADAPTER_ERROR:" + type(_h114_error).__name__}
'''
INJECT_B = '            "h114_technical": h114_technical,\n'
INJECT_C = '            **bundle.get("h114_technical", {}),\n'
INJECT_D = '        ("00D_H114平行策略", _h78_export.get("h114_parallel_watch", pd.DataFrame()), "目前沒有通過H114策略證據的研究候選；非正式買進。"),\n'


def patch(source: str) -> tuple[str, str]:
    if "# H114: reuse the OHLCV dataframe" in source:
        if "h114_parallel_watch" in source and INJECT_C.strip() in source:
            return source, "ALREADY_PATCHED"
        raise RuntimeError("部分 H114 標記已存在，不能疊加不完整修補；請先人工核對")
    patterns = {A:1,B:1,C:1,D:1}
    count={k:source.count(k) for k in patterns}
    if count!=patterns:
        raise RuntimeError("Page 7 版本結構不同："+repr(count)+"；拒絕覆蓋")
    s=source.replace(A, INJECT_A+A,1)
    s=s.replace(B,B+'\n'+INJECT_B.rstrip(),1)
    s=s.replace(C,C+'\n'+INJECT_C.rstrip(),1)
    s=s.replace(D,D+'\n'+INJECT_D.rstrip(),1)
    compile(s, "pages/7_股神推薦.py", "exec")
    return s, "PATCH_READY"


def main() -> int:
    ap=argparse.ArgumentParser()
    ap.add_argument("--check-only",action="store_true")
    ap.add_argument("--root",default=".",help="stock-app project root")
    args=ap.parse_args()
    root=Path(args.root).resolve()
    target=root/"pages"/"7_股神推薦.py"
    module=root/"godpick_h114_parallel_alpha.py"
    if not target.is_file():
        raise SystemExit(f"FAIL: 不存在 {target}")
    if not module.is_file():
        raise SystemExit("FAIL: 請先將 godpick_h114_parallel_alpha.py 放至專案根目錄")
    source=target.read_text(encoding="utf-8-sig")
    sha=hashlib.sha256(source.encode('utf-8')).hexdigest()
    new, status=patch(source)
    print(f"{VER} {status}; BASE_SHA256={sha}")
    if args.check_only or status=="ALREADY_PATCHED":
        print("PASS: 原頁面保留、H111 設定保存不受影響")
        return 0
    backup_dir=root/"backups"/"h114_page7"
    backup_dir.mkdir(parents=True,exist_ok=True)
    stamp=datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup=backup_dir/f"7_股神推薦_{stamp}_{sha[:10]}.py"
    shutil.copy2(target,backup)
    temporary=target.with_suffix(".py.h114tmp")
    try:
        temporary.write_text(new,encoding="utf-8")
        compile(temporary.read_text(encoding="utf-8"),str(target),"exec")
        temporary.replace(target)
        if target.read_text("utf-8")!=new:
            raise RuntimeError("寫入驗證不一致")
        print("PASS: 已定點修補 Page 7；原檔備份：",backup)
    except Exception:
        if temporary.exists():temporary.unlink()
        shutil.copy2(backup,target)
        raise
    return 0

if __name__=='__main__':
    raise SystemExit(main())
