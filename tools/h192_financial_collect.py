#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from godpick_h192_financial_growth import collect

def main():
    p=argparse.ArgumentParser(description='H192 official financial archive collector, no trading changes');p.add_argument('--root',type=Path,default=ROOT);a=p.parse_args()
    r=collect(a.root);print(json.dumps(r,ensure_ascii=False,indent=2));return 0 if r['status']=='ALL_OK' else 2
if __name__=='__main__':raise SystemExit(main())
