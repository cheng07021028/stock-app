# -*- coding: utf-8 -*-
"""Opt-in GitHub issue notification; idempotent by exact title marker.

This module is called in the Actions job, never from Streamlit. Do not send
arbitrary logs, evidence rows, stock data or secrets to issues.
"""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
MARKER='[STOCKAPP-H122-AUTO-INCIDENT]'
from godpick_h122_incident_center import to_markdown


def run_gh(argv):
    try:
        r=subprocess.run(['gh',*argv],capture_output=True,text=True,timeout=35,check=False)
    except (OSError,subprocess.TimeoutExpired): return None
    return r.stdout.strip() if r.returncode==0 else None


def notify(state:dict, repo:str, *, exec_gh=run_gh) ->dict:
    if not isinstance(state,dict) or not repo or '/' not in repo:
        return {'status':'INVALID_NOTIFICATION_CONTEXT'}
    listed=exec_gh(['issue','list','--repo',repo,'--state','open','--limit','100','--json','number,title'])
    if listed is None:return {'status':'GITHUB_NOTIFICATION_API_FAILED'}
    try: items=json.loads(listed)
    except ValueError:return {'status':'GITHUB_NOTIFICATION_API_FAILED'}
    open_issue=next((i for i in items if i.get('title')==MARKER),None)
    severe=state.get('status')=='BLOCKED'
    if severe:
        fingerprint=str(state.get('fingerprint_sha256') or '')[:64]
        body=(to_markdown(state)
              + '\n> 自動事件單，僅提供錯誤代碼與狀態。不包含日誌、帳號或憑證。\n'
              + '\nH122-fingerprint: '+fingerprint+'\n')
        if open_issue:
            # Update only when the incident fingerprint changed, never every run.
            number=str(open_issue['number'])
            existing=exec_gh(['issue','view',number,'--repo',repo,'--json','body'])
            if existing is None:return {'status':'GITHUB_NOTIFICATION_API_FAILED'}
            try: old_body=json.loads(existing).get('body','')
            except (ValueError,AttributeError):return {'status':'GITHUB_NOTIFICATION_API_FAILED'}
            if 'H122-fingerprint: '+fingerprint in str(old_body):
                return {'status':'OPEN_INCIDENT_EXISTS','issue_number':open_issue['number']}
            changed=exec_gh(['issue','edit',number,'--repo',repo,'--body',body])
            return {'status':'INCIDENT_UPDATED' if changed is not None else 'GITHUB_NOTIFICATION_API_FAILED'}
        created=exec_gh(['issue','create','--repo',repo,'--title',MARKER,'--body',body])
        return {'status':'INCIDENT_CREATED' if created else 'GITHUB_NOTIFICATION_API_FAILED'}
    if open_issue:
        # PENDING does not mean recovered; only close on actual review readiness.
        if state.get('status')!='READY_FOR_REVIEW':return {'status':'OPEN_INCIDENT_AWAITING_RECOVERY'}
        closed=exec_gh(['issue','close',str(open_issue['number']),'--repo',repo,'--comment','H122 已恢復至可人工檢視狀態；仍不代表策略獲准。'])
        return {'status':'INCIDENT_CLOSED' if closed is not None else 'GITHUB_NOTIFICATION_API_FAILED'}
    return {'status':'NO_INCIDENT_ACTION'}


def main(argv=None):
    parser=argparse.ArgumentParser()
    parser.add_argument('--state',type=Path,default=ROOT/'reports/h122/H122_dashboard_state.json')
    args=parser.parse_args(argv)
    if os.environ.get('H122_NOTIFY_GITHUB_ISSUE')!='1':
        print(json.dumps({'status':'OPT_IN_DISABLED'}));return 0
    try:state=json.loads(args.state.read_text(encoding='utf-8'))
    except (ValueError,OSError):state={'status':'BLOCKED','alert_codes':['DASHBOARD_FILE_MISSING']}
    result=notify(state,os.environ.get('GITHUB_REPOSITORY',''))
    print(json.dumps(result,ensure_ascii=False))
    return 0 if result['status'] not in ('GITHUB_NOTIFICATION_API_FAILED','INVALID_NOTIFICATION_CONTEXT') else 2

if __name__=='__main__':raise SystemExit(main())
