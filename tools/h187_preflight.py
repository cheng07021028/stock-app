# -*- coding: utf-8 -*-
"""Exact-H186 -> H187 two-file installer, backup + refusal on unknown code."""
from __future__ import annotations
import argparse
import hashlib
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

H186 = {
    'utils.py': '2f75bd1cc9e0a6a0cb960b4adc44aa8f90c0ef7e5e9af1724cfccf6f0709cb17',
    'pages/7_股神推薦.py': '6f73c7414e43a9d7ddd6d045a228af88add23db9c3229c41f6877f74efdf1234',
}
H186_DEPENDENCY = ('godpick_h186_evidence_integrity.py',
                   '1547a49a8755b556580fc67783d27fefbc846f69b35ef9d57b74c540da8c7eee')
FILES = tuple(H186)
NEW:dict[str,str] = {'utils.py': '62845b480caf1c33ae9f37eb42225deed5e51bae0c4d0f0f1349e51732444c3e', 'pages/7_股神推薦.py': '3b238839afb6c8ba6bc6d0df2e9fa7cf1ab19307903dce07d59a013d4b55a694'}


def digest(path:Path)->str:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else ''


def status(root:Path,payload:Path)->tuple[str,list[str]]:
    messages=[]; states=[]
    dep,expected=H186_DEPENDENCY
    if digest(root/dep)!=expected:
        return 'BLOCKED_MISSING_OR_UNKNOWN_H186_DEPENDENCY',[f'{dep}: missing or unknown H186 evidence contract']
    for rel in FILES:
        source=payload/rel
        if digest(source)!=NEW[rel]:
            messages.append(f'CORRUPT_PAYLOAD: {rel}'); states.append('BLOCKED');continue
        live=digest(root/rel)
        if live==NEW[rel]:states.append('UPDATED');messages.append(f'UPDATED: {rel}')
        elif live==H186[rel]:states.append('BASE');messages.append(f'BASE: {rel}')
        else:states.append('BLOCKED');messages.append(f'UNKNOWN_OR_OLDER: {rel}: {live[:16]}')
    if 'BLOCKED' in states:return 'BLOCKED_UNKNOWN_OR_CORRUPT_VERSION',messages
    return ('ALREADY_H187' if all(x=='UPDATED' for x in states)
            else 'READY_APPLY_H187'),messages


def apply(root:Path,payload:Path)->str:
    code,detail=status(root,payload)
    if code=='ALREADY_H187':return code
    if code!='READY_APPLY_H187':raise RuntimeError('; '.join(detail))
    now=datetime.now().strftime('%Y%m%d_%H%M%S')
    backup=root.parent/(root.name+'_H187_backup_'+now)
    backup.mkdir(parents=True,exist_ok=False)
    modified=[]
    try:
        for rel in FILES:
            target=root/rel
            saved=backup/rel
            saved.parent.mkdir(parents=True,exist_ok=True)
            shutil.copy2(target,saved)
        for rel in FILES:
            target=root/rel
            if digest(target)==NEW[rel]:continue
            tmp=target.with_name(target.name+'.h187_tmp')
            shutil.copy2(payload/rel,tmp)
            if digest(tmp)!=NEW[rel]:raise RuntimeError(f'copy verify failed: {rel}')
            os.replace(tmp,target)
            modified.append(rel)
        final,_=status(root,payload)
        if final!='ALREADY_H187':raise RuntimeError('post-install verification failed')
        return 'APPLIED_H187 / backup='+str(backup)
    except Exception:
        for rel in reversed(modified):
            target=root/rel
            saved=backup/rel
            if saved.is_file():shutil.copy2(saved,target)
            elif target.exists():target.unlink()
        raise


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--root',type=Path,required=True)
    p.add_argument('--apply',action='store_true')
    args=p.parse_args()
    payload=Path(__file__).resolve().parents[1]
    root=args.root.expanduser().resolve()
    code,detail=status(root,payload)
    print(code)
    for item in detail:print(' -',item)
    if args.apply:
        if code.startswith('BLOCKED'):sys.exit(2)
        print(apply(root,payload))

if __name__=='__main__':main()
