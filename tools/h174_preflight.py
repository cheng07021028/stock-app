# -*- coding: utf-8 -*-
"""H174 guarded two-file Excel export patch; no writes and no network calls."""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path

BASE_SHA256 = {
    'godpick_h151_workbook_sections.py': '91c894df2986631fca35894af8aff8fba5ff7d06470c2a9071f8130cf8ae84a9',
    'godpick_h154_export_parity.py': 'c3b151a32e15cd0b180d672adbfd3fb90a547be268db88d658252911b990244d',
}
H174_SHA256 = {
    'godpick_h151_workbook_sections.py': 'c172b28859e429dc7d7cf02092f7c6a3a9f11d3e7a901be2322f97dc22ac9678',
    'godpick_h154_export_parity.py': 'df3bba74414416ff388429f739d7c7b9cc7c7748c12bcbd5690bc55776c715e4',
}


def verify(root: Path) -> dict:
    root = Path(root)
    states = {}
    for filename, expected in BASE_SHA256.items():
        path = root / filename
        if not path.is_file():
            states[filename] = 'MISSING'
            continue
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        states[filename] = ('H174' if actual == H174_SHA256[filename] else
                            'BASE' if actual == expected else 'UNKNOWN_SOURCE')
    unique = set(states.values())
    if unique == {'BASE'}:
        status = 'READY_TO_APPLY_H174'
    elif unique == {'H174'}:
        status = 'ALREADY_H174_INSTALLED'
    elif unique <= {'BASE','H174'}:
        status = 'BLOCKED_PARTIAL_INSTALL'
    else:
        status = 'BLOCKED_UNKNOWN_VERSION'
    page = root / 'pages' / '7_股神推薦.py'
    try:
        code = page.read_text('utf-8')
        has_hook = all(token in code for token in (
            'add_research_section_sheets as _h151_section_sheets',
            'verify_final_xlsx as _h154_verify',
        ))
    except (OSError,UnicodeError):
        has_hook = False
    if not has_hook:
        status = 'BLOCKED_EXPORT_HOOK_NOT_FOUND'
    return {'status':status,'files':states,
            'safe_to_apply':status=='READY_TO_APPLY_H174',
            'correctly_installed':status=='ALREADY_H174_INSTALLED',
            'export_hook_present':has_hook,
            'notice':'本工具只讀；未知版本不可強行覆蓋。'}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1])
    ap.add_argument('--after',action='store_true',help='Require both modified H174 files')
    args=ap.parse_args()
    result=verify(args.root)
    print(json.dumps(result,ensure_ascii=False,indent=2))
    if args.after:
        return 0 if result['correctly_installed'] else 2
    return 0 if result['safe_to_apply'] or result['correctly_installed'] else 2


if __name__=='__main__':
    raise SystemExit(main())
