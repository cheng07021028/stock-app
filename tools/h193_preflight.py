#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""H193 exact-hash overlay, including new files. Dry-run by default."""
import argparse,hashlib,json,os,shutil,sys,tempfile
from pathlib import Path
from datetime import datetime
BASE={
 'godpick_h193_joint_decision.py':None,
 'pages/31_十二研究員聯合決策.py':None,
 'tools/h193_daily_review.py':None,
 '.github/workflows/h193_daily_review.yml':None,
 'pages/7_股神推薦.py':'1a95613dcaccd64fb0da08ebddfb04fdf3c65e87',
}
DEPENDENCIES={
 'godpick_h190_market_archive.py':'444e68cc2e74ed70d3f93bb763088d1d1dee948f',
 'godpick_h191_opportunity_engine.py':'1b1769af9067a75bc0bcc3c813475582c3d11e3e',
 'godpick_h192_financial_growth.py':'caedecb97ca03374fedf5f3f99c1a3c559d5950c',
 'godpick_h184_research_lab.py':'0f1591cf26475b3bbb00b404bfea54c473765234',
 'godpick_h185_auto_sources.py':'77f9dcff620430cf1df6b395ef0c2a53848104d9',
}
def gitsha(b):return hashlib.sha1(f'blob {len(b)}\0'.encode()+b).hexdigest()
def sha(b):return hashlib.sha256(b).hexdigest()
def atomic(p,b):
    p.parent.mkdir(parents=True,exist_ok=True)
    fd,tmp=tempfile.mkstemp(prefix='.h193-',dir=p.parent)
    try:
        with os.fdopen(fd,'wb') as f:f.write(b);f.flush();os.fsync(f.fileno())
        os.replace(tmp,p)
    finally:
        if os.path.exists(tmp):os.unlink(tmp)

def check(root,payload):
    root=Path(root);payload=Path(payload);errors=[];todo=[];details=[]
    try:
        manifest=dict((r.split('  ',1)[1],r.split('  ',1)[0]) for r in (payload/'H193_SHA256SUMS.txt').read_text('utf-8').splitlines() if r)
    except Exception as e:return [],[],['manifest invalid: '+str(e)]
    for rel,want in DEPENDENCIES.items():
        p=root/rel
        if not p.is_file() or gitsha(p.read_bytes())!=want:errors.append('H184_H185_H190_H191_H192_BASELINE_REQUIRED: '+rel)
    for rel,base in BASE.items():
        src=payload/rel;dst=root/rel
        if not src.is_file() or sha(src.read_bytes())!=manifest.get(rel):errors.append('PATCH_MISSING_OR_DIGEST: '+rel);continue
        if dst.exists() and not dst.is_file():errors.append('NOT_FILE: '+rel);continue
        before=dst.read_bytes() if dst.is_file() else None;after=src.read_bytes()
        if before==after: state='ALREADY_H193'
        elif (before is None and base is None) or (before is not None and base is not None and gitsha(before)==base):state='READY';todo.append(rel)
        else:state='UNKNOWN_VERSION';errors.append(state+': '+rel)
        details.append({'file':rel,'status':state})
    return details,todo,errors

def apply(root,payload):
    root=Path(root);payload=Path(payload);details,todo,errors=check(root,payload)
    if errors:raise ValueError('; '.join(errors))
    if not todo:return None
    backup=root/'h193_install_backups'/datetime.now().strftime('%Y%m%d_%H%M%S_%f');backup.mkdir(parents=True,exist_ok=False)
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
    if not todo:print('ALREADY_APPLIED_H193');return 0
    if not a.apply:print('READY_APPLY_H193');return 0
    print('APPLIED_H193 / backup='+str(apply(root,payload)));return 0
if __name__=='__main__':raise SystemExit(main())
