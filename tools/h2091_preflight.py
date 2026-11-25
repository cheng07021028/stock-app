#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""H2091 exact-hash overlay, including new files. Dry-run by default."""
import argparse,hashlib,json,os,shutil,sys,tempfile
from pathlib import Path
from datetime import datetime
BASE={'godpick_h209_rolling_backtest.py': '5535fe404e28b8020189964ce668049fbf7f0c34', 'godpick_h190_market_archive.py': '04293b16f557bae5b8a6108b138b138ee01bed78', 'pages/32_隔日預測與AI績效學習.py': '1d6b70e04449f7c0d63f9e3a07b0d3b7dcf308cb', '.github/workflows/h190_market_archive.yml': '5cb060ceccbc2d650cca4d51fb9d956d84433fa2', '.github/workflows/h194_daily_learning.yml': 'fe1ece1833cb7d627018cbe96e3678e4ffb245f6', '.github/workflows/h209_rolling_backtest.yml': '1f55780d4e033e45f9a85fe714d78ca35729eae3'}
DEPENDENCIES={'godpick_h194_performance.py': 'f8c433504d80f4719080754a140e189e89a1cb09', 'godpick_h197_strategy_review.py': '4fde5f6e884e68a3d80b7309b16b984eb5d6ac66', 'godpick_h191_opportunity_engine.py': 'e25f18ae0f78bc9a5c40d6415102b7c7b8bddc50', 'godpick_h198_stage_review.py': 'a5f7e390c87e16e378e273c4f9a014013f68ef50', 'godpick_h208_recall_review.py': '7ed42938bfbbd24f16ae0fb27cfe458df7ad3b6e', 'godpick_h206_market_gap.py': '4baaf35a67028b555eadbc3bf6af7a0991c95b60', 'tools/h209_daily_backtest.py': 'df2c4dd6dbc3845ba330baa18a9a74c79a7a42ac'}
def gitsha(b):return hashlib.sha1(f'blob {len(b)}\0'.encode()+b).hexdigest()
def sha(b):return hashlib.sha256(b).hexdigest()
def atomic(p,b):
    p.parent.mkdir(parents=True,exist_ok=True)
    fd,tmp=tempfile.mkstemp(prefix='.h2091-',dir=p.parent)
    try:
        with os.fdopen(fd,'wb') as f:f.write(b);f.flush();os.fsync(f.fileno())
        os.replace(tmp,p)
    finally:
        if os.path.exists(tmp):os.unlink(tmp)

def check(root,payload):
    root=Path(root);payload=Path(payload);errors=[];todo=[];details=[]
    try:
        manifest=dict((r.split('  ',1)[1],r.split('  ',1)[0]) for r in (payload/'H2091_SHA256SUMS.txt').read_text('utf-8').splitlines() if r)
    except Exception as e:return [],[],['manifest invalid: '+str(e)]
    for rel,want in DEPENDENCIES.items():
        p=root/rel
        if not p.is_file() or gitsha(p.read_bytes())!=want:errors.append('H2091_DEPENDENCY_MISMATCH: '+rel)
    for rel,base in BASE.items():
        src=payload/rel;dst=root/rel
        if not src.is_file() or sha(src.read_bytes())!=manifest.get(rel):errors.append('PATCH_MISSING_OR_DIGEST: '+rel);continue
        if dst.exists() and not dst.is_file():errors.append('NOT_FILE: '+rel);continue
        before=dst.read_bytes() if dst.is_file() else None;after=src.read_bytes()
        if before==after: state='ALREADY_H2091'
        elif (before is None and base is None) or (before is not None and base is not None and gitsha(before)==base):state='READY';todo.append(rel)
        else:state='UNKNOWN_VERSION';errors.append(state+': '+rel)
        details.append({'file':rel,'status':state})
    return details,todo,errors

def apply(root,payload):
    root=Path(root);payload=Path(payload);details,todo,errors=check(root,payload)
    if errors:raise ValueError('; '.join(errors))
    if not todo:return None
    backup=root/'h2091_install_backups'/datetime.now().strftime('%Y%m%d_%H%M%S_%f');backup.mkdir(parents=True,exist_ok=False)
    prior={rel:(root/rel).read_bytes() if (root/rel).is_file() else None for rel in todo}
    for rel,b in prior.items():
        if b is not None:atomic(backup/rel,b)
    try:
        for rel in todo:
            p=root/rel
            if (p.read_bytes() if p.is_file() else None)!=prior[rel]:raise RuntimeError('concurrent change: '+rel)
        for rel in todo:atomic(root/rel,(payload/rel).read_bytes())
        _,left,errs=check(root,payload)
        if left or errs:raise RuntimeError('post-install check failed')
    except Exception:
        for rel,b in prior.items():
            p=root/rel
            # Only undo our own bytes; don't overwrite unexpected concurrent updates.
            if p.is_file() and p.read_bytes()==(payload/rel).read_bytes():
                if b is None:p.unlink()
                else:atomic(p,b)
        raise
    return backup

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--apply',action='store_true');a=p.parse_args()
    root=a.root.resolve();payload=Path(__file__).resolve().parents[1]
    details,todo,errors=check(root,payload);print(json.dumps({'details':details,'errors':errors},ensure_ascii=False,indent=2))
    if errors:return 3
    if not todo:print('ALREADY_APPLIED_H2091');return 0
    if not a.apply:print('READY_APPLY_H2091');return 0
    print('APPLIED_H2091 / backup='+str(apply(root,payload)));return 0
if __name__=='__main__':raise SystemExit(main())
