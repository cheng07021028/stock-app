#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Run public research automatically; output artifact only, never write production records."""
import argparse,sys,json
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from godpick_h193_joint_decision import run,detail,excel,TIERS

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1]);p.add_argument('--out',type=Path,required=True);a=p.parse_args()
    r,f=run(a.root);a.out.mkdir(parents=True,exist_ok=True)
    (a.out/'summary.json').write_text(json.dumps(r,ensure_ascii=False,allow_nan=False,default=str),encoding='utf-8')
    (a.out/'H193_market.xlsx').write_bytes(excel(r))
    for i,pool in enumerate(TIERS,1):
        for x in r['pools'][pool][:3]:
            o=detail(r,f,x['市場'],x['股票代號'],root=a.root)
            (a.out/f'{i}_{x["市場"]}_{x["股票代號"]}.json').write_text(json.dumps(o,ensure_ascii=False,allow_nan=False,default=str),encoding='utf-8')
    print(json.dumps({'status':r['status'],'counts':r['counts'],'same_session':r['same_session'],'trade_authority_changed':False},ensure_ascii=False))
    # Explicit non-success for unavailable joint evidence; artifact still uploaded.
    return 0 if r['same_session'] and not r['archive_errors'] and not r['financial_errors'] else 2
if __name__=='__main__':raise SystemExit(main())
