# -*- coding: utf-8 -*-
"""H175 strict source SHA preflight and guarded 3-file local deployment.

No network, secrets, JSON recommendation history or H126 authority writes.
Rejects unknown/currently-newer versions instead of downgrading an installation.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import shutil
from datetime import datetime
from pathlib import Path

PACKAGE = Path(__file__).resolve().parents[1]
SOURCES = (
    'godpick_h114_parallel_alpha.py',
    'pages/7_股神推薦.py',
    'godpick_h175_evidence_rescue.py',
)
BASE = {
    'godpick_h114_parallel_alpha.py':'c0c177f6dd1b784f7bc48f45d72ec35eedf8493604f1931fc81b7a93577332f0',
    'pages/7_股神推薦.py':'d9d6396a3be9732188f17999ea5520c1f563d032aaba344484a0c5d83169e454',
    'godpick_h154_export_parity.py':'df3bba74414416ff388429f739d7c7b9cc7c7748c12bcbd5690bc55776c715e4',
}
H175 = {
    'godpick_h114_parallel_alpha.py':'abd094af567ee63f8471d0ec4d9c4d2fe6e723a63c63e587ad879ab2d3f4bcfc',
    'pages/7_股神推薦.py':'5435adb89821e12552431f6eb26ef8f0af0e784b72652f9e4dd65afb3dcd6dea',
    'godpick_h175_evidence_rescue.py':'e104494be70e09b22b8843c83245cb28ae3099d7aa93d63769dc30bfe815a44b',
}


def _digest(p:Path)->str:
    return hashlib.sha256(p.read_bytes()).hexdigest() if p.is_file() else ''


def verify(root:Path)->dict:
    root=Path(root)
    source={n:_digest(PACKAGE/n) for n in SOURCES}
    state={}
    for n in BASE:
        actual=_digest(root/n)
        state[n] = ('H175' if n in H175 and actual==H175[n] else
                    'BASE' if n in H175 and actual==BASE[n] else
                    'H174' if n=='godpick_h154_export_parity.py' and actual==BASE[n] else
                    'MISSING' if not actual else 'UNKNOWN')
    new=_digest(root/'godpick_h175_evidence_rescue.py')
    new_module = 'H175' if new==H175['godpick_h175_evidence_rescue.py'] else 'MISSING' if not new else 'UNKNOWN'
    st=[state[n] for n in SOURCES if n!= 'godpick_h175_evidence_rescue.py']
    # A correct H174 overlay is a prerequisite for installing this incremental H175.
    if state['godpick_h154_export_parity.py']!='H174':
        status='BLOCKED_H174_NOT_INSTALLED_OR_UNKNOWN'
    elif st==['BASE','BASE'] and new_module=='MISSING':
        status='READY_APPLY_H175'
    elif st==['H175','H175'] and new_module=='H175':
        status='ALREADY_H175_INSTALLED'
    elif ('UNKNOWN' in st or 'MISSING' in st or new_module=='UNKNOWN'):
        status='BLOCKED_UNKNOWN_SOURCE_VERSION'
    else:
        status='BLOCKED_PARTIAL_INSTALL'
    return {'status':status,'root':str(root),'files':state,'new_module':new_module,
            'safe_to_apply':status=='READY_APPLY_H175',
            'no_overwrite':status=='ALREADY_H175_INSTALLED',
            'source_matches_release':all(source[n]==H175[n] for n in SOURCES),
            'notice':'未知新版／部分安裝不得強制覆蓋；維持原H109/H126及永久紀錄。'}


def main()->int:
    parser=argparse.ArgumentParser(description='H175 guarded patch installer')
    parser.add_argument('--root',type=Path,required=True,help='已安裝H174的股票推薦程式根目錄')
    parser.add_argument('--apply',action='store_true',help='SHA驗證通過後備份、覆蓋3個正式程式檔')
    args=parser.parse_args()
    report=verify(args.root)
    if args.apply and report['safe_to_apply'] and report['source_matches_release']:
        # Backup and write same-directory atomically per file, no other mutations.
        backup=args.root/'.h175_backup'/datetime.now().strftime('%Y%m%d_%H%M%S')
        for n in SOURCES:
            target=args.root/n
            if target.is_file():
                old=backup/n;old.parent.mkdir(parents=True,exist_ok=True)
                shutil.copy2(target,old)
        for n in SOURCES:
            target=args.root/n
            target.parent.mkdir(parents=True,exist_ok=True)
            tmp=target.with_name(target.name+'.h175.new')
            shutil.copy2(PACKAGE/n,tmp)
            tmp.replace(target)
        report=verify(args.root)
        report['backup']=str(backup)
    print(json.dumps(report,ensure_ascii=False,indent=2))
    return 0 if report['status'] in {'READY_APPLY_H175','ALREADY_H175_INSTALLED'} and report['source_matches_release'] else 2

if __name__=='__main__':
    raise SystemExit(main())
