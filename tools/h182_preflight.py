# -*- coding: utf-8 -*-
"""H182 minimal update: safe SHA-guarded diff diagnostics (NO history mutations)."""
from __future__ import annotations
import argparse
import hashlib
import os
from pathlib import Path
import shutil
import sys
import tempfile

# Original H177/H181-compatible official file, as verified against GitHub main
EXPECTED_SHA={
    'godpick_h176_delete_reconcile.py':'58e374d08a5488953b491cc5a048b4792db15385',
    'godpick_persistence_service.py':'6371787d5900ef146452160a6333a53b4441112a',
    'pages/8_股神推薦紀錄.py':'7cef8f6f89950e38cb2b365825da51fdc8727eaf',
}
PATCH_NAMES=['godpick_h176_delete_reconcile.py','godpick_persistence_service.py',
             'pages/8_股神推薦紀錄.py','godpick_h182_record_diff_audit.py']


def blob_sha(path: Path)->str:
    raw=path.read_bytes()
    return hashlib.sha1(f'blob {len(raw)}\0'.encode()+raw).hexdigest()


def inspect(root:Path, bundle:Path)->tuple[str,list[str]]:
    notes=[]
    for name in PATCH_NAMES:
        src=bundle/name
        dst=root/name
        if not src.is_file():return 'ERROR',[f'缺少修正來源：{src}']
        if not dst.is_file():
            if name in EXPECTED_SHA:return 'BLOCKED',[f'找不到原模組：{dst}；禁止盲目加入不同版本']
            notes.append(f'新增檔：{name}')
            continue
        actual=blob_sha(dst); target=blob_sha(src)
        if actual==target:
            notes.append(f'已安裝：{name}')
        elif name in EXPECTED_SHA and actual==EXPECTED_SHA[name]:
            notes.append(f'允許更新：{name}')
        else:
            return 'BLOCKED',[f'拒絕覆蓋未知程式檔：{name}，blob SHA {actual}']
    return ('ALREADY_H182' if all(x.startswith('已安裝') for x in notes) else 'READY_APPLY_H182'),notes


def main(argv=None)->int:
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root',required=True,help='正式 stock-app 目錄')
    p.add_argument('--apply',action='store_true',help='明確執行必要程式覆蓋（不動資料JSON）')
    args=p.parse_args(argv)
    root=Path(args.root).expanduser().resolve();bundle=Path(__file__).resolve().parents[1]
    if not root.is_dir():print(f'ERROR\n目標目錄不存在：{root}');return 2
    status,notes=inspect(root,bundle)
    print(status)
    for x in notes:print(' -',x)
    if status in ('BLOCKED','ERROR'):return 2
    if status=='ALREADY_H182':return 0
    if not args.apply:
        print('預檢結束，沒有寫入任何程式或正式紀錄。需加 --apply 才安裝。')
        return 0
    backup=root/'.h182_code_backup'
    backup.mkdir(exist_ok=True)
    for name in PATCH_NAMES:
        src=bundle/name;dst=root/name
        if dst.exists():
            current=blob_sha(dst)
            if current!=blob_sha(src):
                bk=backup/f'{name}.{current}.bak'
                bk.parent.mkdir(parents=True,exist_ok=True)
                if not bk.exists():shutil.copy2(dst,bk)
        fd,tmp=tempfile.mkstemp(prefix='.h182_',suffix='.tmp',dir=str(dst.parent))
        try:
            with os.fdopen(fd,'wb') as out:out.write(src.read_bytes())
            os.replace(tmp,dst)
        finally:
            if os.path.exists(tmp):os.unlink(tmp)
        if blob_sha(dst)!=blob_sha(src):
            print(f'ERROR\n校驗失敗：{name}');return 3
    print('APPLIED_H182｜只更新程式差異稽核，未刪除或同步任何正式紀錄。')
    return 0

if __name__=='__main__':sys.exit(main())
