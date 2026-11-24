#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""H200 exact-hash overlay, including new files. Dry-run by default."""
import argparse,hashlib,json,os,shutil,sys,tempfile
from pathlib import Path
from datetime import datetime
BASE={'godpick_h200_integrated.py': None, 'godpick_h195_command_center.py': '6edf52faaa0c39f82ad51e4e70f0166d0c027bc8', 'godpick_h199_adaptive_weights.py': 'ac04b2806e91744d0212156d9567e6006ef9fd57', 'pages/7_股神推薦.py': '8e91ca11faba502eeb4c2ebfb0f2d2c4c77c352f', 'pages/33_超級AI股神決策總指揮中心.py': '497e09ce4ff33fb460efd9d840bab8dd587580d2', '.github/workflows/h195_daily_command.yml': '93f535bf3cfad702867e438fbf98ae5da98475c4'}
DEPENDENCIES={'godpick_h190_market_archive.py': '04293b16f557bae5b8a6108b138b138ee01bed78', 'godpick_h191_opportunity_engine.py': 'e25f18ae0f78bc9a5c40d6415102b7c7b8bddc50', 'godpick_h192_financial_growth.py': 'caedecb97ca03374fedf5f3f99c1a3c559d5950c', 'godpick_h193_joint_decision.py': '20bba5529c617beb9120a0303ec0a1d776805c39', 'godpick_h194_performance.py': 'f8c433504d80f4719080754a140e189e89a1cb09', 'godpick_h197_strategy_review.py': '4fde5f6e884e68a3d80b7309b16b984eb5d6ac66', 'godpick_h198_stage_review.py': 'a5f7e390c87e16e378e273c4f9a014013f68ef50', 'tools/h195_daily_command.py': 'c38cbb0c3667e0e39eaa8b814b8b1d6fd870c797'}
def gitsha(b):return hashlib.sha1(f'blob {len(b)}\0'.encode()+b).hexdigest()
def sha(b):return hashlib.sha256(b).hexdigest()
def atomic(p,b):
    p.parent.mkdir(parents=True,exist_ok=True)
    fd,tmp=tempfile.mkstemp(prefix='.h200-',dir=p.parent)
    try:
        with os.fdopen(fd,'wb') as f:f.write(b);f.flush();os.fsync(f.fileno())
        os.replace(tmp,p)
    finally:
        if os.path.exists(tmp):os.unlink(tmp)

def check(root,payload):
    root=Path(root);payload=Path(payload);errors=[];todo=[];details=[]
    try:
        manifest=dict((r.split('  ',1)[1],r.split('  ',1)[0]) for r in (payload/'H200_SHA256SUMS.txt').read_text('utf-8').splitlines() if r)
    except Exception as e:return [],[],['manifest invalid: '+str(e)]
    for rel,want in DEPENDENCIES.items():
        p=root/rel
        if not p.is_file() or gitsha(p.read_bytes())!=want:errors.append('H196_H199_BASELINE_REQUIRED: '+rel)
    for rel,base in BASE.items():
        src=payload/rel;dst=root/rel
        if not src.is_file() or sha(src.read_bytes())!=manifest.get(rel):errors.append('PATCH_MISSING_OR_DIGEST: '+rel);continue
        if dst.exists() and not dst.is_file():errors.append('NOT_FILE: '+rel);continue
        before=dst.read_bytes() if dst.is_file() else None;after=src.read_bytes()
        if before==after: state='ALREADY_H200'
        elif (before is None and base is None) or (before is not None and base is not None and gitsha(before)==base):state='READY';todo.append(rel)
        else:state='UNKNOWN_VERSION';errors.append(state+': '+rel)
        details.append({'file':rel,'status':state})
    return details,todo,errors

def apply(root,payload):
    root=Path(root);payload=Path(payload);details,todo,errors=check(root,payload)
    if errors:raise ValueError('; '.join(errors))
    if not todo:return None
    backup=root/'h200_install_backups'/datetime.now().strftime('%Y%m%d_%H%M%S_%f');backup.mkdir(parents=True,exist_ok=False)
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
    if not todo:print('ALREADY_APPLIED_H200');return 0
    if not a.apply:print('READY_APPLY_H200');return 0
    print('APPLIED_H200 / backup='+str(apply(root,payload)));return 0
if __name__=='__main__':raise SystemExit(main())
