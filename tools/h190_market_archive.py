# -*- coding: utf-8 -*-
from pathlib import Path
import argparse,json,sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from godpick_h190_market_archive import collect,inspect

def main():
    p=argparse.ArgumentParser(description='H190 date-batch official history collector / read-only inspection')
    p.add_argument('action',choices=('collect','inspect'))
    p.add_argument('--root',type=Path,default=ROOT/'data'/'h190')
    p.add_argument('--budget',type=int,default=24)
    args=p.parse_args()
    if args.action=='collect':
        result=collect(args.root,budget=args.budget)
        print(json.dumps(result,ensure_ascii=False,indent=2))
    else: result=None
    summary=inspect(args.root)
    print(json.dumps(summary,ensure_ascii=False,indent=2))
    # A partial run is a warning, not complete success. Still publish valid immutable days.
    return 2 if summary['read_errors'] or (result is not None and (result['errors'] or not summary['paired_verified_sessions'])) else 0
if __name__=='__main__':raise SystemExit(main())
