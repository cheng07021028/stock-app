# -*- coding: utf-8 -*-
"""H123 CLI: TWSE official historical T86 capture, H113 proof audit, no trades."""
from __future__ import annotations
import argparse,json,sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from godpick_h123_flow_provenance import build_twse_evidence,save_evidence,evidence_by_day,assess_flow,iso_date
from godpick_h123_runtime_bridge import load_context


def main(argv=None):
    ap=argparse.ArgumentParser(description='H123 official flow evidence; never backdates publication times')
    sub=ap.add_subparsers(dest='cmd',required=True)
    sub.add_parser('inspect')
    sub.add_parser('collect-latest')
    collect=sub.add_parser('collect-twse')
    collect.add_argument('--day',required=True,help='YYYY-MM-DD exchange trade date (not current clock date)')
    audit=sub.add_parser('audit')
    audit.add_argument('--code',required=True)
    audit.add_argument('--market',default='上市')
    audit.add_argument('--decision-at',required=True,help='ISO8601 timestamp with offset, e.g. 2026-10-14T19:00:00+08:00')
    ap.add_argument('--root',type=Path,default=ROOT)
    args=ap.parse_args(argv)
    root=args.root
    if args.cmd in ('collect-twse','collect-latest'):
        import requests
        if args.cmd=='collect-latest':
            from godpick_h117_official_market_closure import load_official_days
            archived=load_official_days(root/'data/h117/daily')
            latest=[max(days) if days else None for days in archived.values()]
            if len(latest)!=3 or any(x is None for x in latest) or len(set(latest))!=1:
                print(json.dumps({'status':'H117_OFFICIAL_SESSION_INCOMPLETE','markets_latest':latest},ensure_ascii=False))
                return 2
            day=latest[0]
        else:
            day=iso_date(args.day)

        if day>datetime.now(ZoneInfo('Asia/Taipei')).date().isoformat():
            raise ValueError('不可抓未來日期')
        endpoint='https://www.twse.com.tw/fund/T86'
        rsp=requests.get(endpoint,params={'response':'json','date':day.replace('-',''),'selectType':'ALLBUT0999'},
             headers={'User-Agent':'SPT-StockApp-H123/1.0','Accept':'application/json'},timeout=25)
        rsp.raise_for_status();raw=rsp.json()
        ts=datetime.now(ZoneInfo('Asia/Taipei')).isoformat(timespec='seconds')
        ev=build_twse_evidence(raw,trade_day=day,collected_at=ts,source_url=endpoint)
        path=save_evidence(root,ev)
        print(json.dumps({'status':'LOCAL_VERIFIED_NOT_DURABLE_UNTIL_REMOTE_COMMIT',
            'trade_date':day,'stocks':len(ev['stocks']),'raw_sha256':ev['raw_sha256'],
            'path':str(path),'first_observed_at':ts},ensure_ascii=False,indent=2))
        return 0
    ctx=load_context(root)
    if args.cmd=='inspect':
        print(json.dumps({'version':'H123','mode':ctx['mode'],'status':ctx['status'],
           'official_sessions':len(ctx['calendar']),'latest_session':ctx['calendar'][-1] if ctx['calendar'] else None,
           'twse_t86_captures':len(ctx['evidence']),'tpex':'PENDING_VERIFIED_SCHEMA',
           'tdcc':'PENDING_VERIFIED_WEEKLY_SOURCE',
           'reboot':'LOCAL_NOT_PERMANENT_UNTIL_GITHUB_COMMIT'},ensure_ascii=False,indent=2))
        return 0
    out=assess_flow(code=args.code,market=args.market,evidence_days=evidence_by_day(root),
        info_cutoff=args.decision_at,official_sessions=ctx['calendar'])
    print(json.dumps(out,ensure_ascii=False,indent=2))
    return 0 if out['H123法人實證狀態']=='PROVEN_FLOW' else 2

if __name__=='__main__':raise SystemExit(main())
