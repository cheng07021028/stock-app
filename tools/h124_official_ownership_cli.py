# -*- coding: utf-8 -*-
"""H124 official source capture CLI. Fail closed, no guessed dates/units."""
from __future__ import annotations
import argparse,json,sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from godpick_h124_ownership_flow import (TDCC_URL,build_tdcc,build_tpex,save_capture,
                                          load_captures,assess_tdcc,assess_tpex)
from godpick_h123_runtime_bridge import load_context


def main(argv=None):
    p=argparse.ArgumentParser(description='H124: official TPEx/TDCC evidence; no trades')
    p.add_argument('--root',type=Path,default=ROOT)
    sub=p.add_subparsers(dest='cmd',required=True)
    sub.add_parser('inspect');sub.add_parser('collect-tdcc')
    x=sub.add_parser('collect-tpex-url');x.add_argument('--url',required=True);x.add_argument('--day',required=True)
    y=sub.add_parser('audit');y.add_argument('--code',required=True);y.add_argument('--market',choices=['上市','上櫃'],required=True)
    y.add_argument('--decision-at',required=True,help='with timezone e.g. 2026-10-14T20:00:00+08:00')
    args=p.parse_args(argv);now=datetime.now(ZoneInfo('Asia/Taipei')).isoformat(timespec='seconds')
    if args.cmd=='inspect':
        counts={};issues=[]
        for source in ('TPEX','TDCC'):
            try:counts[source]=sorted(load_captures(args.root,source).keys())
            except (ValueError,OSError) as e:counts[source]=[];issues.append(source+':'+str(e))
        print(json.dumps({'version':'H124','status':'PENDING_EVIDENCE' if issues or not all(len(v)>0 for v in counts.values()) else 'LOCAL_CAPTURE_PRESENT_NOT_REMOTE_PROVEN',
            'capture_dates':counts,'issues':issues,'durability':'GITHUB_COMMIT_AND_REMOTE_HEAD_VERIFICATION_REQUIRED'},ensure_ascii=False,indent=2));return 0
    if args.cmd in ('collect-tdcc','collect-tpex-url'):
        import requests
        if args.cmd=='collect-tdcc':
            url=TDCC_URL
            response=requests.get(url,headers={'User-Agent':'SPT-StockApp-H124/1.0','Accept':'text/csv,application/json'},timeout=65)
            response.raise_for_status()
            ev=build_tdcc(response.content,collected_at=now,source_url=response.url)
        else:
            url=args.url
            # Only official TPEx URL may be requested: block SSRF and redirects.
            from godpick_h124_ownership_flow import _official_https
            _official_https(url,'www.tpex.org.tw')
            if not url.startswith('https://www.tpex.org.tw/openapi/'):
                raise ValueError('TPEx官方資料擷取僅允許openapi前綴')
            if args.day>datetime.now(ZoneInfo('Asia/Taipei')).date().isoformat():raise ValueError('未來交易日不允許')
            response=requests.get(url,headers={'User-Agent':'SPT-StockApp-H124/1.0','Accept':'application/json'},timeout=35,allow_redirects=False)
            if response.is_redirect:raise ValueError('拒絕API重新導向')
            response.raise_for_status()
            rows=response.json()
            ev=build_tpex(rows,trade_day=args.day,collected_at=now,source_url=response.url)
        path=save_capture(args.root,ev)
        print(json.dumps({'status':'LOCAL_VERIFIED_NOT_DURABLE_UNTIL_REMOTE_COMMIT','source':ev['source'],
            'date':ev['trade_date'],'stocks':len(ev['stocks']),'path':str(path),'source_hash':ev['raw_sha256']},ensure_ascii=False,indent=2))
        return 0
    ctx=load_context(args.root)
    if args.market=='上市':
        tpex={'H123法人實證狀態':'NOT_APPLICABLE_TWSE'}
    else:tpex=assess_tpex(code=args.code,evidence=load_captures(args.root,'TPEX'),
                          sessions=ctx.get('calendar',[]),cutoff=args.decision_at,preverified=True)
    owner=assess_tdcc(code=args.code,evidence=load_captures(args.root,'TDCC'),cutoff=args.decision_at,preverified=True)
    print(json.dumps({'market':args.market,'flow':tpex,'holder':owner},ensure_ascii=False,indent=2))
    return 0 if (args.market=='上市' or tpex['H123法人實證狀態']=='PROVEN_FLOW') and owner['H124大戶實證狀態']=='VERIFIED_RISING' else 2

if __name__=='__main__':raise SystemExit(main())
