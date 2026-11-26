#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from godpick_h237_pipeline_acceptance import run,compact,excel
from godpick_h190_market_archive import _write,_seal

def write(r,out,*,json_only=False):
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    _write(out/'health.json',_seal(compact(r)))
    if not json_only:(out/'H237_PipelineAcceptance.xlsx').write_bytes(excel(r))

def main():
    p=argparse.ArgumentParser(description='Read-only actual-data acceptance, no capture or automatic downloads');p.add_argument('--root',type=Path,default=ROOT);p.add_argument('--out',type=Path,required=True);p.add_argument('--full',action='store_true');p.add_argument('--cloud-check',action='store_true');p.add_argument('--json-only',action='store_true');p.add_argument('--report-only',action='store_true',help='report pending without changing workflow failure status');a=p.parse_args()
    r=run(a.root,full=a.full,cloud_check=a.cloud_check);write(r,a.out,json_only=a.json_only);print(json.dumps(compact(r),ensure_ascii=False,allow_nan=False));return 0 if r['status']=='REVIEW_AVAILABLE' or a.report_only else 2
if __name__=='__main__':raise SystemExit(main())
