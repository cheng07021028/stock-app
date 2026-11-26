#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from godpick_h192_financial_growth import quarter
from godpick_h236_cloud_financial import publish,restore,cloud_status,restore_configured

def main():
    p=argparse.ArgumentParser(description='H236 private GCS: verified H235 archive publish/restore/status')
    p.add_argument('action',choices=['collect-publish','publish','restore','status','restore-configured']);p.add_argument('--quarter');p.add_argument('--root',type=Path,default=ROOT);p.add_argument('--official-url');a=p.parse_args();q=quarter(a.quarter)
    if a.action=='restore-configured':
        if a.quarter or a.official_url:p.error('restore-configured uses H236_RESTORE_QUARTERS only')
        try:print(json.dumps(restore_configured(a.root),ensure_ascii=False,indent=2));return 0
        except Exception as e:print('H236 restore failed:',type(e).__name__,file=sys.stderr);return 2
    if q is None:p.error('invalid YYYYQn')
    if bool(a.official_url)!=(a.action=='collect-publish'):p.error('--official-url only for collect-publish')
    try:
        if a.action=='collect-publish':
            from godpick_h235_official_batch import collect
            collect(a.root,q,a.official_url)
        r=({'collect-publish':publish,'publish':publish,'restore':restore,'status':cloud_status}[a.action])(a.root,q)
        print(json.dumps(r,ensure_ascii=False,indent=2));return 0 if r['status'] not in ('NOT_COMMITTED','CLOUD_ARCHIVE_MISSING_OR_CHANGED') else 2
    except Exception as e:
        # SDK exception messages may include signed URLs or sensitive account details.
        print('H236 failed:',type(e).__name__,file=sys.stderr);return 2
if __name__=='__main__':raise SystemExit(main())
