#!/usr/bin/env python3
import argparse, json, sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from godpick_h218_evidence import run,excel

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1]);p.add_argument('--out',type=Path,required=True);a=p.parse_args();a.out.mkdir(parents=True,exist_ok=True)
    r=run(a.root);h={k:v for k,v in r.items() if k not in ('action_rows','detail','pairs')};(a.out/'health.json').write_text(json.dumps(h,ensure_ascii=False,allow_nan=False,indent=2),'utf-8');(a.out/'H218_Evidence.xlsx').write_bytes(excel(r));print(json.dumps(h,ensure_ascii=False));return 2 if r['errors'] or any(x['狀態']=='DATA_PENDING' for x in r['action_summary'] if x['範圍']=='60日技術窗') else 0
if __name__=='__main__':raise SystemExit(main())
