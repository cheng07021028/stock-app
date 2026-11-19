# -*- coding: utf-8 -*-
"""Read-only preflight before copying V191-H172 cumulative patch over an app.

Run this script from the extracted H172 patch directory with --target pointed
at the *existing* stock-app project root. Unknown newer files fail closed.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path

EXPECTED={
 'godpick_h169_pe_watch.py':{
    'H173_CUMULATIVE':'7c18e93643564ca950875926d5095fd4ff805e9800b5dbef01ad215276e2b2cf',
    'H170_BASE':'048a4ba00d17dbacfbc7ea07e659d10c9f6c195e78849de3756c17cfa34b83f7',
    'H171_HOOK':'14a8926900e24636d4b9cabdc75b928274107157cfa52551783d969d3530e887',
    'H172_CUMULATIVE':'a194c5acc845d26f71e9c5168208ec227195ece2186c6e70ed3dd72a3998efee',
 },
 '.github/workflows/h117_official_ohlcv_feedback.yml':{
    'H173_CUMULATIVE':'67cf508127d058585b5b74b81a8a3b45c9f0e021b2ce7a07425240ba493efa59',
    'H170_BASE':'0dabbf3ce8f05c57da311be08808b059f46ff0918203c349e78297d8daebc0ac',
    'H172_CUMULATIVE':'dce3b0e49650040886bd72e324e71dc28c830bc0b568eef5d7a6b4e8d3ac6a28',
 },
 'godpick_h116_daily_feedback.py':{
    'H170_BASE':'cd403da1870c9e94b6ff8a4561687acb88d6d95421bbff250d484381c3ad8a9d'},
 'godpick_h117_official_market_closure.py':{
    'H170_BASE':'d38587cfea9d6bf8524b5a0fb16bc6badc81a87729276ba784e15875c71076d9'},
 'godpick_h170_eps_evidence.py':{
    'H170_BASE':'8f5cc426c59e5ae410a8736f1fd74b93a91df62646f5756e5b5259b63acf6146'},
}


def preflight(root:Path)->dict:
    root=Path(root)
    checks=[]
    mismatches=[]
    for name,allowed in EXPECTED.items():
        target=root/name
        if not target.is_file():
            checks.append({'file':name,'status':'MISSING_DEPENDENCY','sha256':None})
            mismatches.append(name)
            continue
        digest=hashlib.sha256(target.read_bytes()).hexdigest()
        match=next((ver for ver,sha in allowed.items() if sha==digest),'UNRECOGNIZED_VERSION')
        checks.append({'file':name,'status':match,'sha256':digest})
        if match=='UNRECOGNIZED_VERSION':mismatches.append(name)
    if mismatches:
        return {'status':'BLOCK_UNKNOWN_OR_MISSING_BASE','safe_to_overlay':False,
                'mismatched_files':mismatches,'checks':checks}
    # A newer H173 must never be downgraded by accidentally reusing an H172 patch.
    if all(c['status']=='H173_CUMULATIVE' for c in checks[:2]):
        return {'status':'ALREADY_H173_FILES','safe_to_overlay':False,'checks':checks,
                'note':'H172 older overlay must not overwrite H173.'}
    if any(c['status']=='H173_CUMULATIVE' for c in checks[:2]):
        return {'status':'BLOCK_PARTIALLY_INSTALLED_H173','safe_to_overlay':False,'checks':checks}
    already=all(c['status']=='H172_CUMULATIVE' for c in checks[:2])
    if any(c['status']=='H172_CUMULATIVE' for c in checks[:2]) and not already:
        return {'status':'BLOCK_PARTIALLY_INSTALLED_H172','safe_to_overlay':False,
                'checks':checks}
    return {'status':'ALREADY_H172_FILES' if already else 'READY_H172_CUMULATIVE_OVERLAY',
            'safe_to_overlay':True,'checks':checks,
            'note':'核對原碼版本，不代表已完成正式雲端部署或資料完整性驗收'}


def main(argv=None):
    p=argparse.ArgumentParser(description='H172 read-only safe version preflight')
    p.add_argument('--target',type=Path,required=True,help='Current unmodified stock-app main root')
    args=p.parse_args(argv)
    result=preflight(args.target)
    print(json.dumps(result,ensure_ascii=False,indent=2))
    if not result['safe_to_overlay']:
        raise SystemExit(2)
    return result

if __name__=='__main__':main()
