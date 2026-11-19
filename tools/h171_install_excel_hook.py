# -*- coding: utf-8 -*-
"""Fail-closed, H169-exact-version optional H171 Excel integration installer.

Does not touch page 7 or trading logic. Direct hook is placed at the end of the
existing H169 Sheet11 audit, after earlier rows are written and before return.
Requires manual --apply, makes a .bak copy, refuses unknown H169 revisions.
"""
from __future__ import annotations
import argparse
import hashlib
import os
import shutil
import sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
TARGET=ROOT/'godpick_h169_pe_watch.py'
EXPECTED_GIT_BLOB='d80461ef5955f1ceecc48e6248513013276d7bc9'
ANCHOR='    return {"sheet": tab, "start_row": start, "checked_codes": len(seen),\n'
END='            "eps_ttm_verified": False}\n'
INSERT='''    # H171 research-only financial appendix. Never touches H126 or ranking.
    try:
        from godpick_h171_workbook_audit import append_h171_research as _h171_append
        _h171_path = ROOT / "data" / "h115" / "official_ytd_archive.json"
        _h171_rows = eps_snap.get("eps_ytd_rows", [])
        if _h171_path.is_file() and _h171_path.stat().st_size <= 50_000_000:
            _h171_json = json.loads(_h171_path.read_text(encoding="utf-8-sig"))
            if isinstance(_h171_json, dict) and isinstance(_h171_json.get("rows"), list):
                _h171_rows = _h171_json["rows"]
        _h171_result = _h171_append(wb, financial_rows=_h171_rows, research_day=study)
    except Exception as _h171_error:
        _h171_result = {"status": "H171_APPEND_FAILED", "error_type": type(_h171_error).__name__}
'''


def git_blob(data: bytes) -> str:
    return hashlib.sha1(b'blob '+str(len(data)).encode()+b'\x00'+data).hexdigest()


def patched(data: bytes, *, allowed_sha: str = EXPECTED_GIT_BLOB) -> bytes:
    normal = data.replace(b'\r\n',b'\n')
    if git_blob(normal) != allowed_sha:
        raise ValueError('H169 檔案與已驗證的 GitHub H170 基準不相符，拒絕改寫')
    s=normal.decode('utf-8')
    if s.count(ANCHOR)!=1 or s.count(END)!=1:
        raise ValueError('H169 return 錨點不同，拒絕改寫')
    s=s.replace(ANCHOR, INSERT+ANCHOR,1)
    s=s.replace(END,'            "eps_ttm_verified": False,\n            "h171_audit": _h171_result}\n',1)
    import ast
    ast.parse(s)
    final=s.encode('utf-8')
    return final.replace(b'\n',b'\r\n') if b'\r\n' in data else final


def install(path: Path, *, apply: bool) -> dict:
    path=Path(path)
    if not path.is_file():
        return {'status':'MISSING_H169','changed':False,'file':str(path)}
    raw=path.read_bytes()
    try:
        new=patched(raw)
    except (ValueError, UnicodeError) as exc:
        return {'status':'PRECHECK_FAILED','changed':False,'reason':str(exc),
                'git_blob_sha':git_blob(raw.replace(b'\r\n',b'\n'))}
    if not apply:
        return {'status':'READY_APPLY_EXACT_H169','changed':False,'path':str(path)}
    backup=path.with_suffix(path.suffix+'.h171.bak')
    if backup.exists():
        return {'status':'BACKUP_ALREADY_EXISTS_ABORT','changed':False,'backup':str(backup)}
    shutil.copy2(path,backup)
    temp=path.with_suffix('.h171.tmp')
    try:
        with temp.open('wb') as f:
            f.write(new);f.flush();os.fsync(f.fileno())
        temp.replace(path)
    finally:
        if temp.exists():temp.unlink()
    return {'status':'H171_SHEET11_HOOK_INSTALLED','changed':True,
            'path':str(path),'backup':str(backup),
            'trade_authority_changed':False}


def main():
    p=argparse.ArgumentParser(description='H171 validated H169 export hook (safe optional install)')
    p.add_argument('--apply',action='store_true',help='Explicitly patch current H169 after SHA preflight')
    a=p.parse_args()
    import json
    result=install(TARGET,apply=a.apply)
    print(json.dumps(result,ensure_ascii=False,indent=2))
    if result['status'] not in {'READY_APPLY_EXACT_H169','H171_SHEET11_HOOK_INSTALLED'}:
        raise SystemExit(2)

if __name__ == '__main__':
    main()
