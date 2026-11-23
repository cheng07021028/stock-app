# -*- coding: utf-8 -*-
"""H186 exact-version and transactional local installer. No secrets or data edits."""
from __future__ import annotations
import argparse
import hashlib
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

OLD = {'utils.py': '74e61e3d8ea4eb1d523066c9010058c52d2becf0ee1de8ee11dffb1e33aa1bf0', 'godpick_h114_parallel_alpha.py': 'abd094af567ee63f8471d0ec4d9c4d2fe6e723a63c63e587ad879ab2d3f4bcfc', 'pages/7_股神推薦.py': '5435adb89821e12552431f6eb26ef8f0af0e784b72652f9e4dd65afb3dcd6dea'}
NEW = {'utils.py': '2f75bd1cc9e0a6a0cb960b4adc44aa8f90c0ef7e5e9af1724cfccf6f0709cb17', 'godpick_h114_parallel_alpha.py': '92c76119a20d8c4db47ea28ab9c04d985a34185241b7d8929f379d99ae0a76eb', 'pages/7_股神推薦.py': '6f73c7414e43a9d7ddd6d045a228af88add23db9c3229c41f6877f74efdf1234', 'godpick_h186_evidence_integrity.py': '1547a49a8755b556580fc67783d27fefbc846f69b35ef9d57b74c540da8c7eee'}
FILES = ['utils.py', 'godpick_h114_parallel_alpha.py', 'pages/7_股神推薦.py', 'godpick_h186_evidence_integrity.py']


def digest(path:Path)->str:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else ''


def status(root:Path, payload:Path)->tuple[str,list[str]]:
    details=[];states=[]
    for rel in FILES:
        target=root/rel; value=digest(target)
        patch=payload/rel
        if digest(patch)!=NEW[rel]:
            details.append(f'PAYLOAD_CORRUPT: {rel}');states.append('BLOCKED');continue
        if value==NEW[rel]:states.append('UPDATED');details.append(f'UPDATED: {rel}')
        elif value==OLD.get(rel,'') or (rel not in OLD and not target.exists()):
            states.append('BASE');details.append(f'BASE: {rel}')
        else:
            states.append('BLOCKED');details.append(f'UNKNOWN_OR_MISSING: {rel}: {value[:16]}')
    if 'BLOCKED' in states:return 'BLOCKED_UNKNOWN_OR_CORRUPT_VERSION',details
    if all(s=='UPDATED' for s in states):return 'ALREADY_H186',details
    return 'READY_APPLY_H186',details


def apply(root:Path,payload:Path)->str:
    code,details=status(root,payload)
    if code=='ALREADY_H186':return code
    if code!='READY_APPLY_H186':raise RuntimeError('; '.join(details))
    now=datetime.now().strftime('%Y%m%d_%H%M%S')
    backup=root.parent/(root.name+'_H186_backup_'+now)
    backup.mkdir(parents=True,exist_ok=False)
    modified=[]
    try:
        for rel in FILES:
            target=root/rel
            if target.is_file():
                saved=backup/rel;saved.parent.mkdir(parents=True,exist_ok=True)
                shutil.copy2(target,saved)
        for rel in FILES:
            target=root/rel
            if digest(target)==NEW[rel]:continue
            target.parent.mkdir(parents=True,exist_ok=True)
            temp=target.with_name(target.name+'.h186_tmp')
            shutil.copy2(payload/rel,temp)
            if digest(temp)!=NEW[rel]:raise RuntimeError('copy verification failed: '+rel)
            os.replace(temp,target);modified.append(rel)
        final,details=status(root,payload)
        if final!='ALREADY_H186':raise RuntimeError('post-install integrity not green')
        return f'APPLIED_H186 / backup={backup}'
    except Exception:
        for rel in reversed(modified):
            target=root/rel;saved=backup/rel
            if saved.is_file():shutil.copy2(saved,target)
            elif target.exists():target.unlink()
        raise


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--root',type=Path,required=True,help='Your installed stock-app repository directory')
    p.add_argument('--apply',action='store_true')
    args=p.parse_args()
    payload=Path(__file__).resolve().parents[1]
    root=args.root.expanduser().resolve()
    code,detail=status(root,payload)
    print(code)
    for line in detail:print(' - '+line)
    if args.apply:
        if code.startswith('BLOCKED'):sys.exit(2)
        print(apply(root,payload))

if __name__=='__main__':main()
