# -*- coding: utf-8 -*-
"""Collect or import official single-quarter profitability ratios into H115 PIT.

Examples:
  python tools/h1534_import_margins.py --fetch-twse --store data/h115
  python tools/h1534_import_margins.py --source-json official.json --market TWSE --store data/h115

As-of availability is NOW (first observed), NEVER the historic quarter itself.
Official API supplies only snapshots. No fabricated old-quarter publication dates.
"""
from __future__ import annotations
import argparse
import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from urllib.request import urlopen, Request
sys.path.insert(0,str(Path(__file__).resolve().parent.parent))
from godpick_h115_pit_evidence import append_quarterly_margins, TZ_TW

TWSE_URL='https://openapi.twse.com.tw/v1/opendata/t187ap17_L'
# TPEx provides the same 187ap17 category but the endpoint may be unavailable;
# a failed fetch is an explicit failure and does not turn into a success report.
TPEX_URL='https://www.tpex.org.tw/openapi/v1/mopsfin_187ap17_O'

def parse_rows(data, *, source: str, source_url: str, observed_at: str):
    if not isinstance(data,list):raise ValueError('官方資料回應不是陣列')
    out=[]
    def get(r, start):
        for k,v in r.items():
            if str(k).strip().replace(' ','').startswith(start):return v
        return None
    for r in data:
        if not isinstance(r,dict):continue
        code=str(get(r,'公司代號') or '').strip()
        year=str(get(r,'年度') or '').strip();q=str(get(r,'季別') or '').strip()
        q=q.replace('第','').replace('季','').replace('Q','')
        if not (code.isdigit() and 4<=len(code)<=6 and year.isdigit() and q.isdigit()):continue
        try:
            gm=float(str(get(r,'毛利率(%)') or '').replace(',',''))
            om=float(str(get(r,'營業利益率(%)') or '').replace(',',''))
            nm=float(str(get(r,'稅後純益率(%)') or '').replace(',',''))
        except (ValueError,TypeError):continue
        out.append({'code':code,'year_quarter':f'{int(year)}Q{int(q)}',
            'gross_margin_pct':gm,'operating_margin_pct':om,'net_margin_pct':nm,
            'industry_type':'GENERAL','statement_basis':'SINGLE_QUARTER',
            'source':source,'source_url':source_url,'available_at':observed_at,
            'availability_basis':'FIRST_OBSERVED'})
    return out

def main():
    ap=argparse.ArgumentParser(description='H153.4官方三率單季快照→H115永久證據帳本')
    ap.add_argument('--fetch-twse',action='store_true')
    ap.add_argument('--fetch-tpex',action='store_true')
    ap.add_argument('--source-json',type=Path)
    ap.add_argument('--market',choices=['TWSE','TPEX'],default='TWSE')
    ap.add_argument('--store',type=Path,default=Path('data/h115'))
    args=ap.parse_args()
    if not (args.fetch_twse or args.fetch_tpex or args.source_json):ap.error('請指定來源')
    now=datetime.now(TZ_TW).isoformat(timespec='seconds')
    chunks=[]
    for source,url,enabled in [('TWSE',TWSE_URL,args.fetch_twse),('TPEX',TPEX_URL,args.fetch_tpex)]:
        if enabled:
            req=Request(url,headers={'User-Agent':'StockResearch/1.0 (public financial snapshot)'})
            with urlopen(req,timeout=12) as res:
                payload=json.loads(res.read(15_000_000).decode('utf-8-sig'))
            chunks.extend(parse_rows(payload,source=source,source_url=url,observed_at=now))
    if args.source_json:
        payload=json.loads(args.source_json.read_text(encoding='utf-8-sig'))
        url=TWSE_URL if args.market=='TWSE' else TPEX_URL
        chunks.extend(parse_rows(payload,source=args.market,source_url=url,observed_at=now))
    if not chunks:raise SystemExit('FAIL: 官方快照沒有可解析的單季三率，未寫入任何資料')
    result=append_quarterly_margins(args.store,chunks)
    print('H153.4 official first-observed snapshot:',result, 'at',now)
    print('注意：此快照不構成舊季度的提前可得證據，須永久保存帳本並回讀確認。')

if __name__=='__main__':main()
