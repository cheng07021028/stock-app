# -*- coding: utf-8 -*-
"""Official nightly H117 collector; no fabricated quotes, no trading signals."""
from __future__ import annotations
import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(ROOT))
from godpick_h117_official_market_closure import (
    TZ, fetch_sources, resolve_freshness, persist_official_daily,
    export_verified_h116_csv, load_official_days,
)

def main(argv=None):
    p=argparse.ArgumentParser(description='H117 Official daily OHLCV close + H116 review readiness')
    s=p.add_subparsers(dest='cmd',required=True)
    s.add_parser('collect')
    s.add_parser('export')
    s.add_parser('inspect')
    p.add_argument('--root',type=Path,default=ROOT/'data'/'h117'/'daily')
    p.add_argument('--decisions',type=Path,default=ROOT/'data'/'h116'/'decisions')
    p.add_argument('--out',type=Path,default=ROOT/'data'/'h116')
    args=p.parse_args(argv)
    if args.cmd=='inspect':
        existing=load_official_days(args.root)
        report={'version':'H117','sources':{k:len(v) for k,v in existing.items()},
                'days':{k:sorted(v)[-1] if v else None for k,v in existing.items()}}
        print(json.dumps(report,ensure_ascii=False,indent=2));return 0
    if args.cmd=='export':
        report=export_verified_h116_csv(args.root,args.decisions,args.out)
        print(json.dumps(report,ensure_ascii=False,indent=2));return 0
    now=datetime.now(TZ)
    if now.hour<18 or now.hour==18 and now.minute<15:
        print(json.dumps({'status':'NOT_EOD_YET','time':now.isoformat()},ensure_ascii=False));return 0
    result=fetch_sources()
    quality=resolve_freshness(result,now=now)
    if quality['status']!='READY_FOR_IMMUTABLE_PUBLISH':
        print(json.dumps({'status':quality['status'],'errors':result['errors'],
                          'trading_dates':quality['details'],
                          'action':'RETRY_AFTER_ALL_SOURCES_RELEASE_OR_CHECK_TPEX_NETWORK;NOT_SAVED'},ensure_ascii=False))
        return 0 if quality['status']=='NO_NEW_MARKET_SESSION' else 2
    stored=persist_official_daily(args.root,result['sources'],
               collected_at=now.isoformat(timespec='seconds'),require_day=now.date().isoformat())
    exported=export_verified_h116_csv(args.root,args.decisions,args.out)
    print(json.dumps({'collector':stored,'export':exported,'status':'COLLECTED_LOCALLY_GIT_COMMIT_STILL_REQUIRED'},ensure_ascii=False,indent=2))
    return 0

if __name__=='__main__':raise SystemExit(main())
