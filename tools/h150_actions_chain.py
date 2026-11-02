# -*- coding: utf-8 -*-
"""H150 GitHub Actions worker: serialize already installed official collectors.

Intentionally triggers existing independently guarded scheduled workflows and
waits for completion. Does NOT claim that a successful workflow proves an
individual market datum is current; H117/H123/H124/H136/H137 own validation.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone, time as tod
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from zoneinfo import ZoneInfo
import json
import os
import sys
import time

TW = ZoneInfo('Asia/Taipei')
ROOT = 'https://api.github.com'
# Stage order protects provenance: EOD -> institutions -> research -> audit;
# monthly/weekly evidence can be collected independently by their cron jobs.
STAGES = (
    ('h117_official_ohlcv_feedback.yml', tod(18, 45), 45),
    ('h123_official_institutional_daily.yml', tod(21, 45), 23),
    ('h136_research_data_hub.yml', tod(22, 30), 32),
    ('h137_post_sync_audit.yml', tod(23, 0), 27),
)
EXTRA = (
    ('h115_official_revenue_snapshot.yml', tod(21, 15), 22),
    ('h124_tpex_tdcc_weekly_provenance.yml', tod(23, 15), 25),
)


def _stamp(s):
    try:
        return datetime.fromisoformat(str(s).replace('Z', '+00:00')).astimezone(TW)
    except (ValueError, TypeError):
        return None


def select_prior_run(runs, *, today, release: tod):
    """All service results remain provisional; never treat stale runs as current."""
    valid=[]
    for item in runs if isinstance(runs, list) else []:
        if not isinstance(item, dict) or item.get('head_branch')!='main':
            continue
        when=_stamp(item.get('created_at',''))
        if when and when.date()==today and when.time()>=release:
            valid.append((when,item))
    return max(valid, key=lambda pair: pair[0])[1] if valid else None


class Client:
    def __init__(self, token, repo):
        if not token or not repo or '/' not in repo:
            raise ValueError('GITHUB_ACTIONS_CREDENTIALS_MISSING')
        self.token,self.repo=token,repo
    def request(self, path, payload=None):
        blob=json.dumps(payload).encode() if payload is not None else None
        req=Request(ROOT+'/repos/'+self.repo+path, data=blob,
                    headers={'Authorization':'Bearer '+self.token,
                             'Accept':'application/vnd.github+json',
                             'X-GitHub-Api-Version':'2022-11-28',
                             **({'Content-Type':'application/json'} if payload is not None else {})},
                    method='POST' if payload is not None else 'GET')
        with urlopen(req,timeout=20) as r:
            data=r.read(1_000_000)
            return r.status, json.loads(data) if data else {}
    def latest(self, workflow):
        status,reply=self.request('/actions/workflows/'+workflow+'/runs?per_page=12&branch=main')
        if status!=200 or not isinstance(reply,dict):raise ValueError('WORKFLOW_RUNS_UNAVAILABLE')
        return reply.get('workflow_runs',[])
    def launch(self,workflow):
        status,_=self.request('/actions/workflows/'+workflow+'/dispatches',{'ref':'main'})
        if status not in (200,204):raise ValueError('WORKFLOW_DISPATCH_FAILED')


def run_stage(client, workflow, release, timeout_minutes, *, now, sleep=time.sleep):
    current=now.astimezone(TW)
    if current.weekday()>4:
        return 'SKIP_WEEKEND'
    if current.time()<release:
        return 'WAIT_OFFICIAL_RELEASE'
    latest=select_prior_run(client.latest(workflow),today=current.date(),release=release)
    if latest and latest.get('status')=='completed':
        return 'WORKFLOW_DONE_'+str(latest.get('conclusion') or 'UNKNOWN').upper()
    if not latest:
        client.launch(workflow)
    deadline=time.monotonic()+timeout_minutes*60
    while time.monotonic()<deadline:
        sleep(14)
        current_run=select_prior_run(client.latest(workflow),today=current.date(),release=release)
        if current_run and current_run.get('status')=='completed':
            return 'WORKFLOW_DONE_'+str(current_run.get('conclusion') or 'UNKNOWN').upper()
    return 'WORKFLOW_TIMEOUT'


def main() -> int:
    token=os.environ.get('GH_TOKEN','')
    repo=os.environ.get('GITHUB_REPOSITORY','')
    try:client=Client(token,repo)
    except ValueError as exc:
        print(str(exc));return 2
    now=datetime.now(TW)
    outcome={}
    for workflow,release,limit in STAGES:
        try: result=run_stage(client,workflow,release,limit,now=now)
        except (HTTPError,URLError,ValueError,TimeoutError,OSError) as exc:
            result='UNAVAILABLE_'+type(exc).__name__
        outcome[workflow]=result
        print(f'H150 {workflow}: {result}',flush=True)
        if result not in ('WORKFLOW_DONE_SUCCESS','WAIT_OFFICIAL_RELEASE','SKIP_WEEKEND'):
            # Don't attempt derived caches with an unverified upstream workflow.
            break
        if result != 'WORKFLOW_DONE_SUCCESS':
            break
    # Monthly revenue / weekly TDCC do not affect whether a decision is allowed.
    for workflow,release,limit in EXTRA:
        try: outcome[workflow]=run_stage(client,workflow,release,limit,now=now)
        except (HTTPError,URLError,ValueError,TimeoutError,OSError) as exc:
            outcome[workflow]='UNAVAILABLE_'+type(exc).__name__
        print(f'H150 {workflow}: {outcome[workflow]}',flush=True)
    text=json.dumps({'schema':'H150-actions-orchestrator-1','asof_tw':now.date().isoformat(),
                     'trigger':os.environ.get('H150_TRIGGER_SOURCE',''),
                     'stages':outcome,'note':'Workflow outcomes only, not official data source attestation'},
                    ensure_ascii=False,indent=2)
    summary=os.environ.get('GITHUB_STEP_SUMMARY','')
    if summary:
        with open(summary,'a',encoding='utf-8') as f:f.write('## H150 login/source refresh\n\n```json\n'+text+'\n```\n')
    # Missing/late sources are reflected clearly, not silently claimed done.
    return 0 if outcome else 1


if __name__=='__main__':sys.exit(main())
