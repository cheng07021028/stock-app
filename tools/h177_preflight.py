# -*- coding: utf-8 -*-
"""H177 safe incremental installer. Extract ZIP separately; --root is the target repo.

Never overlays unknown/newer source versions. Does not touch credentials,
JSON authorities, recommendations, git metadata or Firestore directly.
"""
from __future__ import annotations
import argparse
import ast
import hashlib
import os
import shutil
import tempfile
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
FILES = {
    "godpick_persistence_service.py": {"a77e3ea2b94499703ac5ef810237138e4ad66e7f75155ca2e6a3bec26d21bf12", "b9990346c920ab818db339fa6e80b03ed6013e592b153c9eb918eb74bf9f51cc"},
    "pages/8_股神推薦紀錄.py": {"bdd584ef5710113ca2bf243090e19b2f551c10d62cdc987dfc6ec72ab0b7a44c", "cbf093b98c751bc4ec782060cfd2d85ba7f697ae6244d9214c84c2499f0cc07e"},
    "godpick_h176_delete_reconcile.py": None,
}

def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def run(root: Path, apply: bool = False) -> int:
    root = root.expanduser().resolve()
    if not (root / "pages" / "7_股神推薦.py").exists() or not (root / "godpick_persistence_service.py").exists():
        print("BLOCKED_NOT_STOCK_APP_ROOT：需指向已有 pages/7_股神推薦.py 的專案根目錄")
        return 2
    updates: list[tuple[Path, bytes]] = []
    for rel, approved_hash in FILES.items():
        source = BASE / rel
        destination = root / rel
        if not source.is_file():
            print(f"BLOCKED_PACKAGE_MISSING {rel}")
            return 3
        body = source.read_bytes()
        ast.parse(body.decode("utf-8"), filename=rel)
        new_hash = sha(body)
        if destination.exists():
            old_hash = sha(destination.read_bytes())
            if old_hash == new_hash:
                print(f"UNCHANGED_H177 {rel}")
                continue
            if approved_hash is None or old_hash not in approved_hash:
                print(f"BLOCKED_UNKNOWN_OR_NEWER_FILE {rel}｜{old_hash[:16]}，禁止覆蓋")
                return 4
        elif approved_hash is not None:
            print(f"BLOCKED_BASE_FILE_MISSING {rel}")
            return 5
        updates.append((destination,body))
        print(f"READY {rel}")
    if not apply:
        print("READY_APPLY_H177" if updates else "ALREADY_H177")
        return 0
    if not updates:
        print("ALREADY_H177")
        return 0
    backup = root / ".h177_backups" / datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    backup.mkdir(parents=True, exist_ok=False)
    prepared=[]
    try:
        for destination,body in updates:
            rel=destination.relative_to(root)
            copy=backup/rel
            copy.parent.mkdir(parents=True,exist_ok=True)
            if destination.exists():shutil.copy2(destination,copy)
            prepared.append((destination,copy,destination.exists()))
        for destination,body in updates:
            destination.parent.mkdir(parents=True,exist_ok=True)
            fd,name=tempfile.mkstemp(prefix=".h177_",suffix=".tmp",dir=destination.parent)
            try:
                with os.fdopen(fd,"wb") as fh:
                    fh.write(body)
                    fh.flush();os.fsync(fh.fileno())
                os.replace(name,destination)
            finally:
                if os.path.exists(name):os.unlink(name)
        if any(sha(destination.read_bytes())!=sha(body) for destination,body in updates):
            raise RuntimeError("檔案Hash回讀不一致")
    except Exception as exc:
        for destination,copy,existed in prepared:
            if existed and copy.exists():shutil.copy2(copy,destination)
            elif not existed and destination.exists():destination.unlink()
        print(f"BLOCKED_APPLY_ROLLED_BACK：{type(exc).__name__}: {exc}")
        return 6
    print(f"APPLIED_H177｜更新{len(updates)}檔｜原檔備份：{backup}")
    return 0

if __name__ == "__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root",type=Path,required=True,help="現有 stock-app 專案根目錄")
    parser.add_argument("--apply",action="store_true",help="通過版本檢查後原子覆蓋並備份")
    args=parser.parse_args()
    raise SystemExit(run(args.root,args.apply))
