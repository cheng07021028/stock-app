#!/usr/bin/env python3
import argparse,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from godpick_h214_universe import collect,run,excel
if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,default=ROOT);p.add_argument('--out',type=Path,required=True);p.add_argument('--collect',action='store_true');a=p.parse_args();a.out.mkdir(parents=True,exist_ok=True)
    if a.collect:
        health=collect(a.root);(a.out/'roster_health.json').write_text(json.dumps(health,ensure_ascii=False,indent=2),encoding='utf-8')
        print(health)
    r=run(a.root);(a.out/'H214_Universe.xlsx').write_bytes(excel(r));(a.out/'h214.json').write_text(json.dumps(r,ensure_ascii=False,allow_nan=False,indent=2),encoding='utf-8');print(r['status'],r['coverage'])
