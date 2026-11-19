# -*- coding: utf-8 -*-
"""H173 cumulative patch read-only SHA gate for H170/H171/H172 exact known bases.

Does not auto-apply or overwrite any file. Unknown newer production content stops
installation instead of silently erasing other fixes. H173 already installed
is NOT permission to re-apply an older H172 patch.
"""
from __future__ import annotations
import argparse,hashlib,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from tools.h172_preflight import EXPECTED as BASE

EXACT={
    name:dict(hashes) for name,hashes in BASE.items()
}
EXACT['godpick_h169_pe_watch.py']['H173_CUMULATIVE']='7c18e93643564ca950875926d5095fd4ff805e9800b5dbef01ad215276e2b2cf'
EXACT['.github/workflows/h117_official_ohlcv_feedback.yml']['H173_CUMULATIVE']='67cf508127d058585b5b74b81a8a3b45c9f0e021b2ce7a07425240ba493efa59'
ACCEPT={('H170_BASE','H170_BASE'),('H171_HOOK','H170_BASE'),
        ('H172_CUMULATIVE','H172_CUMULATIVE')}


def preflight(root:Path)->dict:
    root=Path(root)
    checks=[]
    missing=[]
    for name,hashes in EXACT.items():
        p=root/name
        digest=hashlib.sha256(p.read_bytes()).hexdigest() if p.is_file() else None
        label=next((kind for kind,known in hashes.items() if digest==known),'UNRECOGNIZED_OR_MISSING')
        checks.append({'file':name,'sha256':digest,'version':label})
        if label=='UNRECOGNIZED_OR_MISSING':missing.append(name)
    if missing:
        return {'status':'BLOCK_UNKNOWN_OR_MISSING_BASE','safe_to_overlay':False,
                'unrecognized':missing,'checks':checks}
    pair=(checks[0]['version'],checks[1]['version'])
    if pair==('H173_CUMULATIVE','H173_CUMULATIVE'):
        return {'status':'ALREADY_H173_INSTALLED','safe_to_overlay':False,'checks':checks}
    if pair not in ACCEPT:
        return {'status':'BLOCK_MIXED_OR_PARTIAL_INSTALL','safe_to_overlay':False,
                'checks':checks}
    return {'status':'READY_H173_CUMULATIVE_OVERLAY','safe_to_overlay':True,
            'base_versions':pair,'checks':checks,
            'note':'核對來源程式SHA，不代表已驗證雲端執行、歷史資料或交易權限。'}


def main(argv=None):
    p=argparse.ArgumentParser(description='H173 cumulative version gate (read-only)')
    p.add_argument('--target',type=Path,required=True,help='The existing app project root (not this ZIP)')
    args=p.parse_args(argv)
    r=preflight(args.target)
    print(json.dumps(r,ensure_ascii=False,indent=2))
    if r['status'] not in {'READY_H173_CUMULATIVE_OVERLAY','ALREADY_H173_INSTALLED'}:
        raise SystemExit(2)
    return r

if __name__=='__main__':main()
