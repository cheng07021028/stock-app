# -*- coding: utf-8 -*-
"""V191-H184 strictly additive installer. No existing production file is overwritten."""
from __future__ import annotations
import argparse
import hashlib
import os
from pathlib import Path
import tempfile

NEW_FILES=(
    'godpick_h184_research_lab.py',
    'config/h184_role_contracts.json',
    'pages/28_十二研究員決策研究中心.py',
)


def sha(path:Path):return hashlib.sha256(path.read_bytes()).hexdigest()


def inspect(root:Path,bundle:Path):
    if not (root/'streamlit_app_core.py').is_file() or not (root/'pages/7_股神推薦.py').is_file():
        return 'BLOCKED',['不是完整 stock-app 系統資料夾；請先核對路徑']
    status=[]
    for item in NEW_FILES:
        src=bundle/item;dst=root/item
        if not src.is_file():return 'BLOCKED',['安裝包缺少：'+item]
        if not dst.exists():status.append('新增：'+item)
        elif dst.is_file() and sha(src)==sha(dst):status.append('已安裝：'+item)
        else:return 'BLOCKED',['拒絕覆蓋既有或較新版檔案：'+item]
    return ('ALREADY_H184' if all(x.startswith('已安裝') for x in status) else 'READY_APPLY_H184'),status


def install(root:Path,bundle:Path):
    state,notes=inspect(root,bundle)
    if state=='BLOCKED':return state,notes
    if state=='ALREADY_H184':return state,notes
    done=[]
    try:
        for name in NEW_FILES:
            src=bundle/name;dest=root/name
            if dest.exists():continue
            dest.parent.mkdir(parents=True,exist_ok=True)
            # Exclusive create: no overwrite, even if concurrently installed.
            with dest.open('xb') as out:out.write(src.read_bytes())
            done.append(dest)
            if sha(dest)!=sha(src):raise OSError('hash mismatch for '+name)
    except (OSError,FileExistsError) as e:
        for file in done:file.unlink(missing_ok=True)
        return 'BLOCKED',[f'安裝已回復（沒有覆蓋舊檔）：{e}']
    return 'APPLIED_H184',notes


def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root',required=True,help='stock-app 專案根目錄，例如 D:/stock-app')
    p.add_argument('--apply',action='store_true',help='不指定時只做唯讀預檢')
    args=p.parse_args(argv)
    root=Path(args.root).expanduser().resolve();bundle=Path(__file__).resolve().parents[1]
    if not root.is_dir():print('BLOCKED | 專案路徑不存在');return 2
    state,notes=(install(root,bundle) if args.apply else inspect(root,bundle))
    print(state)
    for x in notes:print(' -',x)
    return 2 if state=='BLOCKED' else 0

if __name__=='__main__':raise SystemExit(main())
