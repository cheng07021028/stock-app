# -*- coding: utf-8 -*-
"""Small, independent GitHub wake receipt. Not business-job success.
Uses stdlib before pip; fixed parent SHA; never writes recommendation data.
"""
from __future__ import annotations
import base64,json,os,re,sys
from datetime import datetime,timezone
from pathlib import Path
from urllib.request import Request,urlopen
from urllib.error import HTTPError

PATH='godpick_scheduler_wake_probe.json'
LOCAL=Path('h243_wake_receipt.json')


def make(phase, env):
    if phase not in ('START','FINISH'):raise ValueError('unknown phase')
    run=env.get('GITHUB_RUN_ID','');attempt=env.get('GITHUB_RUN_ATTEMPT','1')
    if not (run.isdecimal() and attempt.isdecimal()):raise ValueError('missing workflow run identity')
    return {'version':'V191-H243','run_id':run,'attempt':attempt,'phase':phase,
            'observed_at':datetime.now(timezone.utc).isoformat(timespec='seconds'),
            'event':env.get('GODPICK_WAKEUP_EVENT','')[:80],
            'wake_source':env.get('GODPICK_WAKEUP_SOURCE','')[:80],
            'stages':{k:env.get('H243_'+k.upper(),'')[:40] for k in ('checkout','python','deps','restore','due','verify')},
            'business_success_not_certified':True}


def can_publish(old,new):
    if not isinstance(old,dict) or not old:return True
    try:
        # Compare numeric run identifiers, then attempt and phase.
        a=(int(old['run_id']),int(old['attempt']),old['phase']=='FINISH')
        b=(int(new['run_id']),int(new['attempt']),new['phase']=='FINISH')
        return b>=a
    except (KeyError,TypeError,ValueError):return False


def publish(receipt,env,*,request=None):
    repo=env.get('GITHUB_REPOSITORY','');token=env.get('GITHUB_TOKEN','')
    if not token or not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+',repo):raise ValueError('missing GitHub config')
    url=f'https://api.github.com/repos/{repo}/contents/{PATH}'
    headers={'Authorization':f'Bearer {token}','Accept':'application/vnd.github+json','User-Agent':'GodPick-H243','X-GitHub-Api-Version':'2022-11-28'}
    def call(method,endpoint,body=None):
        req=Request(endpoint,data=json.dumps(body).encode() if body is not None else None,headers=headers,method=method)
        try:
            with urlopen(req,timeout=12) as r:return r.status,json.loads(r.read())
        except HTTPError as e:return e.code,{}
    call=request or call
    code,meta=call('GET',url+'?ref=runtime-data')
    data={'message':'H243 scheduler wake receipt','branch':'runtime-data','content':base64.b64encode(json.dumps(receipt,ensure_ascii=False).encode()).decode()}
    if code==200:
        old=json.loads(base64.b64decode(meta['content']))
        if not can_publish(old,receipt):return False,'newer wake receipt exists; not overwritten'
        data['sha']=meta['sha']
    elif code!=404:return False,f'GET HTTP {code}'
    code,_=call('PUT',url,data)
    if code not in (200,201):return False,f'PUT HTTP {code}; no overwrite retry'
    code,meta=call('GET',url+'?ref=runtime-data')
    if code!=200:return False,f'readback HTTP {code}'
    return json.loads(base64.b64decode(meta['content']))==receipt,'readback checked'


def main():
    receipt=make(sys.argv[1].upper(),os.environ)
    LOCAL.write_text(json.dumps(receipt,ensure_ascii=False,indent=2),encoding='utf-8')
    try:ok,msg=publish(receipt,os.environ)
    except Exception as exc:ok,msg=False,type(exc).__name__
    print('H243 wake receipt: '+str(ok)+' '+msg)
    raise SystemExit(0 if ok else 1)

if __name__=='__main__':main()
