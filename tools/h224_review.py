#!/usr/bin/env python3
import argparse,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from godpick_h224_growth_resonance import run,excel

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,default=ROOT);p.add_argument('--out',type=Path,required=True);p.add_argument('--capture',action='store_true');a=p.parse_args();a.out.mkdir(parents=True,exist_ok=True)
    r=run(a.root,capture=a.capture);h={k:v for k,v in r.items() if k not in ('rows','acceleration')};(a.out/'health.json').write_text(json.dumps(h,ensure_ascii=False,allow_nan=False,indent=2),'utf-8');(a.out/'H224_GrowthResonance.xlsx').write_bytes(excel(r));print(json.dumps(h,ensure_ascii=False));return 2 if any(x['狀態']=='INVALID' for x in r['scope']) else 0
if __name__=='__main__':raise SystemExit(main())
