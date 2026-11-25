#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse,json,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from godpick_h195_command_center import publish,workbook

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1]);p.add_argument('--out',type=Path,required=True);a=p.parse_args()
    b=publish(a.root);a.out.mkdir(parents=True,exist_ok=True);(a.out/'H195_CommandCenter.xlsx').write_bytes(workbook(a.root));(a.out/'health.json').write_text(json.dumps({k:b.get(k) for k in ('asof','status','counts','health','errors','market_day','technical_current','h211')},ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({'status':b['status'],'counts':b['counts'],'errors':b['errors']},ensure_ascii=False));return 0
if __name__=='__main__':raise SystemExit(main())
