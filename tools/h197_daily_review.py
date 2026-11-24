#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse,json,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from godpick_h197_strategy_review import run,excel

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1]);p.add_argument('--out',type=Path,required=True);a=p.parse_args()
    r=run(a.root);a.out.mkdir(parents=True,exist_ok=True)
    (a.out/'H197_strategy_review.xlsx').write_bytes(excel(r));(a.out/'health.json').write_text(json.dumps({k:v for k,v in r.items() if k!='daily'},ensure_ascii=False,allow_nan=False,indent=2),encoding='utf-8')
    print(r['status'], 'signals=',r['signals'],'archive_errors=',len(r['archive_errors']))
    return 2 if r['archive_errors'] else 0
if __name__=='__main__':raise SystemExit(main())
