#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""H211 cumulative exact-hash overlay, including new files. Dry-run by default."""
import argparse,hashlib,json,os,shutil,sys,tempfile
from pathlib import Path
from datetime import datetime
BASE={'godpick_h190_market_archive.py': ['11c96c29395d8ce0b7514466861735873d3c75f9953eacd9ebcb1b46fc5c0c43', '623710a237fdf9a8cc09fb0a5c084470b97f8f5ec90a7eb135796d67ad8454af'], 'godpick_h209_rolling_backtest.py': ['402c643826ee1fef3b7aab89b975a92fd3126f12701388071908fbe0bc683af4', '697e17827c00c355a25a3aab3775d80284d5b6cc8326cdd603f7a396f23e3962'], 'pages/32_隔日預測與AI績效學習.py': ['41e25ec8b5c52ba77505a3063c3c870cc61bc0fd7cefef7917775efa0ec66ca2', '6be30f3fba0e0559dadeb8a8dfe36264dfa01761fcbd11c1194f34c0b8f732d4', '802416d4e20c0cb49c228dd72e220efc86df230d5008ebb8aa1203afc13cc5a9'], '.github/workflows/h190_market_archive.yml': ['966b8dfd6af8092e6c6dabb2040414040df95bc313831ce4b3da1e926f3f51c0', '4b7d68bc41be066be4d3d1970e08da99bd20c56b4ab8a84a243799f435dac230'], '.github/workflows/h194_daily_learning.yml': ['9259ed88ea615ade9e181f9f1299432e81cd440e495b7bd13b4a1818aacc63d9', '46c7c1514545d25875df7cd9c1241b0d92ec3ef14a59cbf6fba3619014e50e94'], '.github/workflows/h209_rolling_backtest.yml': ['ffdd7a360861246949ea54074e6fe6dd882124f3a146ce0dbc42c2b83a921911', 'b2c73c7e0a70e5f99ce1f68d2a85694b8b79114266a36b36d16e4951045086e1', 'c01f2ac7fc0974868af286cb2f3d0124c9cbc34b15147344cfc3a2dfd826b3a5'], 'godpick_h210_strategy_competition.py': [None, '8e4cb6ec512be00a54a7b94ad71000b62dc6b6ee863aba6d756b2ab63985f3b5'], 'tools/h210_daily_competition.py': [None, 'dd44e73b1fcf3db9a96374a90b34e62d40cf841b432e76d309a4874b4f0cfa54'], 'godpick_h211_counterevidence.py': [None], 'godpick_h191_opportunity_engine.py': ['bda8ca60e757cec96e00e95b386a8a00fab18a27e5ea5c8e76a67efc1fa455e8'], 'godpick_h193_joint_decision.py': ['a586f0198e4f31a88a67ae513d9feeb5725415f4d092d72b0b985312733ab9a4'], 'godpick_h195_command_center.py': ['ace6433045a5c992b45a230fb03e74d219430957562796a3f9fdbea1dac4abc5'], 'pages/33_超級AI股神決策總指揮中心.py': ['07d7706f10a97cde4350c7357aef9671fc320bfcab4233d92828b029fd0d7157'], '.github/workflows/h195_daily_command.yml': ['460bad76a089ee843cad4aa512be9c67e67601e3ae037254975b281f26a9a846'], 'tools/h195_daily_command.py': ['321cc287d5127e8e716909325114db757b0d9ff1a6ce79ff1cf6cc4b7e4b0776']}
DEPENDENCIES={'godpick_h194_performance.py': 'a2ed5197ae403d57bf9411b106bae45793117353e23aec7111ba225f4d2cf209', 'godpick_h197_strategy_review.py': '46d0b4e12463d5791ff7ccb3240e06b00c3608a3c4d46afe242c784d14910218', 'godpick_h198_stage_review.py': '248db22227ffd8d82a9811f5ed97b12571720d1978ee3d3ab7c78dfdb2c27a50', 'godpick_h208_recall_review.py': 'b2214a2dbc36d0978f29bace600b6f1224a4930b5d1d3bd0368f92eb1fc89931', 'godpick_h206_market_gap.py': '37bcd1722bd23683c12e4252c390e87aac06152c63ec99eb9a0d64728bf7faab', 'tools/h209_daily_backtest.py': '8a83d639762d28b373b3ffd8b5cb9d5af5a8947790ab054289026ae589487aac', 'godpick_h199_adaptive_weights.py': '7362784f77ea774b9c38e954612dd7dd222c52460af0bbe1c8919d324526ec54'}
def gitsha(b):return hashlib.sha1(f'blob {len(b)}\0'.encode()+b).hexdigest()
def sha(b):return hashlib.sha256(b).hexdigest()
def atomic(p,b):
    p.parent.mkdir(parents=True,exist_ok=True)
    fd,tmp=tempfile.mkstemp(prefix='.h211-',dir=p.parent)
    try:
        with os.fdopen(fd,'wb') as f:f.write(b);f.flush();os.fsync(f.fileno())
        os.replace(tmp,p)
    finally:
        if os.path.exists(tmp):os.unlink(tmp)

def check(root,payload):
    root=Path(root);payload=Path(payload);errors=[];todo=[];details=[]
    try:
        manifest=dict((r.split('  ',1)[1],r.split('  ',1)[0]) for r in (payload/'H211_SHA256SUMS.txt').read_text('utf-8').splitlines() if r)
    except Exception as e:return [],[],['manifest invalid: '+str(e)]
    for rel,want in DEPENDENCIES.items():
        p=root/rel
        if not p.is_file() or sha(p.read_bytes())!=want:errors.append('H211_DEPENDENCY_MISMATCH: '+rel)
    for rel,base in BASE.items():
        src=payload/rel;dst=root/rel
        if not src.is_file() or sha(src.read_bytes())!=manifest.get(rel):errors.append('PATCH_MISSING_OR_DIGEST: '+rel);continue
        if dst.exists() and not dst.is_file():errors.append('NOT_FILE: '+rel);continue
        before=dst.read_bytes() if dst.is_file() else None;after=src.read_bytes()
        if before==after: state='ALREADY_H211'
        elif (None if before is None else sha(before)) in base:state='READY';todo.append(rel)
        else:state='UNKNOWN_VERSION';errors.append(state+': '+rel)
        details.append({'file':rel,'status':state})
    return details,todo,errors

def apply(root,payload):
    root=Path(root);payload=Path(payload);details,todo,errors=check(root,payload)
    if errors:raise ValueError('; '.join(errors))
    if not todo:return None
    backup=root/'h211_install_backups'/datetime.now().strftime('%Y%m%d_%H%M%S_%f');backup.mkdir(parents=True,exist_ok=False)
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
    if not todo:print('ALREADY_APPLIED_H211');return 0
    if not a.apply:print('READY_APPLY_H211');return 0
    print('APPLIED_H211 / backup='+str(apply(root,payload)));return 0
if __name__=='__main__':raise SystemExit(main())
