#!/usr/bin/env python3
import argparse,json,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from godpick_h221_evidence_closure import collect,inspect

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1]);p.add_argument('--inspect',action='store_true');a=p.parse_args()
    r=inspect(a.root) if a.inspect else collect(a.root)
    print(json.dumps(r,ensure_ascii=False,indent=2));return 0 if a.inspect or r['status']=='VERIFIED_5_BOTH' else 2
if __name__=='__main__':raise SystemExit(main())
