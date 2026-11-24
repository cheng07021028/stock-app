#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse,json,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from godpick_h194_performance import run,excel

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1]);p.add_argument('--out',type=Path,required=True);a=p.parse_args()
    r=run(a.root);a.out.mkdir(parents=True,exist_ok=True)
    (a.out/'H194_performance.xlsx').write_bytes(excel(r))
    (a.out/'health.json').write_text(json.dumps({k:v for k,v in r.items() if k not in ('outcomes','latest_signals')},ensure_ascii=False,allow_nan=False,indent=2),encoding='utf-8')
    print(json.dumps({k:r[k] for k in ('capture_status','signals','new_outcomes','mature','pending','archive_errors')},ensure_ascii=False))
    if r['archive_errors'] or '未核實' in r['capture_status'] or '沒有可封存' in r['capture_status']:
        print('::warning::H194 evidence pending: '+r['capture_status'])
    return 0
if __name__=='__main__':raise SystemExit(main())
