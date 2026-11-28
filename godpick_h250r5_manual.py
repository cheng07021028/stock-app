# -*- coding: utf-8 -*-
"""R5: exact saved enabled scope and independent manual receipts."""
from __future__ import annotations
import re
import time
import hashlib
import json
import subprocess
MODE='force_enabled_jobs'
REC='godpick_recommendation'
SOURCE='page17_h250r5_force_enabled'
ACTIVE={'queued','in_progress','requested','waiting','pending'}


def plan_enabled(config, labels, *, raw=None):
    keys=list(labels)
    jobs=config.get('jobs') if isinstance(config,dict) else None
    if not isinstance(jobs,dict):raise ValueError('R5 排程設定缺少工作清單')
    if config.get('_h250r1_authority')=='AUTHORITY_UNAVAILABLE':raise ValueError('R5 設定權威未確認')
    if raw is not None:
        src=raw.get('jobs') if isinstance(raw,dict) else None
        if not isinstance(src,dict) or any(not isinstance(src.get(k),dict) or type(src[k].get('enabled')) is not bool or src[k]['enabled'] != (jobs.get(k) or {}).get('enabled') for k in keys):
            raise ValueError('R5 永久設定缺件或與執行設定不符；禁止以預設值代替')
    if any(not isinstance(jobs.get(k),dict) or type(jobs[k].get('enabled')) is not bool for k in keys):raise ValueError('R5 工作啟用狀態未確認')
    selected={k for k in keys if jobs[k]['enabled']}
    order=list(dict.fromkeys([*(config.get('execution_order') or []),*keys]))
    result=[k for k in order if k in selected and k!=REC]
    if REC in selected:result.append(REC)
    return result


def verify_restored_settings(raw, root, git=None):
    """Read same restored Git ref, never overwrite the runtime control plane."""
    def read(path):
        if git is not None:return git(path)
        return json.loads(subprocess.check_output(['git','-C',str(root),'show','origin/runtime-data:'+path],timeout=15,stderr=subprocess.DEVNULL).decode('utf-8-sig'))
    config=read('data/config/godpick_auto_scheduler_settings.json')
    state=read('godpick_auto_scheduler_settings_sync_state.json')
    digest=lambda v:hashlib.sha256(json.dumps(v,ensure_ascii=False,sort_keys=True,separators=(',',':'),default=str).encode('utf-8')).hexdigest()
    if not isinstance(raw,dict) or not isinstance(config,dict) or not isinstance(state,dict) or digest(raw)!=digest(config) or state.get('payload_hash')!=digest(config) or state.get('path')!='data/config/godpick_auto_scheduler_settings.json':
        raise ValueError('R5 遠端排程設定與同步摘要不一致；未執行人工批次，請先永久保存設定')
    return True


def validation(rows, planned, essential=()):
    planned=list(planned); rows=list(rows)
    status={r.get('job'):r.get('status') for r in rows}
    actual=[r.get('job') for r in rows]
    all_ok=bool(planned and len(set(planned))==len(planned) and actual==planned and all(r.get('status')=='SUCCESS' for r in rows))
    return {'all_success':all_ok,'essential_jobs':{k:status.get(k,'NOT_RUN') for k in essential},
            'essential_success':all(status.get(k)=='SUCCESS' for k in essential),'recommendation_status':status.get(REC,'NOT_SELECTED' if REC not in planned else 'NOT_RUN'),
            'recommendation_executed':status.get(REC) in ('SUCCESS','WARNING'),'unfinished_jobs':[k for k in planned if status.get(k)!='SUCCESS'],
            'job_results':[{'job':r.get('job'),'status':r.get('status'),'run_key':r.get('run_key'),'message':str(r.get('message') or '')[:500]} for r in rows]}


def verify_receipt(status, run_id, attempt, *, history=None):
    if not isinstance(status,dict):return False,'R5 遠端狀態格式不符'
    if not str(run_id).isdigit() or not str(attempt).isdigit():return False,'R5 Run ID/Attempt 不合法'
    ident=f'{run_id}.{attempt}'
    if str(status.get('last_wakeup_run_id'))!=str(run_id) or str(status.get('last_wakeup_attempt'))!=str(attempt):return False,'R5 本輪遠端身份不一致'
    batch=status.get('last_manual_batch') or {}
    if not isinstance(batch,dict):return False,'R5 批次回執格式不符'
    summary=batch.get('summary') or {};valid=batch.get('validation') or {};plan=batch.get('planned_jobs') or []
    if not isinstance(summary,dict) or not isinstance(valid,dict):return False,'R5 驗收摘要格式不符'
    rows=valid.get('job_results') or []
    if not isinstance(rows,list) or any(not isinstance(r,dict) for r in rows):return False,'R5 工作回執格式不符'
    if batch.get('batch_id')!=ident or batch.get('batch_mode')!=MODE:return False,'R5 未取得此手動批次回執'
    if not plan or not isinstance(plan,list) or any(not isinstance(k,str) for k in plan) or len(set(plan))!=len(plan) or REC in plan[:-1]:return False,'R5 工作計畫無效'
    if status.get('last_wakeup_result')!='COMPLETED' or summary.get('in_progress') is not False or not valid.get('all_success') or summary.get('executed')!=len(plan) or summary.get('success')!=len(plan) or any(summary.get(k)!=0 for k in ('warning','failed','blocked')) or status.get('last_summary')!=summary:return False,'R5 全部工作尚未成功：'+str(summary)
    if [r.get('job') for r in rows]!=plan or any(r.get('status')!='SUCCESS' or not str(r.get('run_key') or '').startswith(str(r.get('job'))+'|FORCE|') or not str(r.get('run_key') or '').endswith('|'+ident) for r in rows):return False,'R5 逐項執行回執不一致'
    if history is not None:
        records=history.get('records',[]) if isinstance(history,dict) else history
        matched=[r for r in records if isinstance(r,dict) and r.get('batch_id')==ident] if isinstance(records,list) else []
        key=lambda r:(r.get('job'),r.get('status'),r.get('run_key'))
        if [key(r) for r in matched]!=[key(r) for r in rows]:return False,'R5 遠端歷史與批次回執不一致'
    return True,f'R5 手動批次 {ident} 全部 {len(plan)} 項成功'


def active_manual(*,token,owner,repo,http_get=None):
    """Conservative active-mode probe. H249 CAS queue remains the stronger dedup."""
    if not token or not all(re.fullmatch(r'[A-Za-z0-9_.-]{1,100}',str(v or '')) for v in (owner,repo)):return None,'GitHub 設定不完整'
    if http_get is None:
        import requests
        http_get=requests.get
    try:
        resp=http_get(f'https://api.github.com/repos/{owner}/{repo}/actions/workflows/godpick_auto_scheduler_v191.yml/runs',headers={'Accept':'application/vnd.github+json','Authorization':'Bearer '+token,'X-GitHub-Api-Version':'2022-11-28'},params={'event':'workflow_dispatch','per_page':100},timeout=(3,8))
        if resp.status_code!=200:return None,f'GitHub HTTP {resp.status_code}'
        runs=resp.json().get('workflow_runs')
        if not isinstance(runs,list):return None,'GitHub 回應無工作清單'
        for r in runs:
            if r.get('status') in ACTIVE and ('| '+MODE+' |' in str(r.get('display_title') or '') or SOURCE in str(r.get('display_title') or '')):
                rid=str(r.get('id') or '')
                return (rid if rid.isdigit() else None),'已有手動批次排隊／執行中'
        return '', '無已辨識的同模式活動批次'
    except Exception as exc:return None,type(exc).__name__


def render_button(st,config,labels,dispatch):
    try:plan=plan_enabled(config,labels)
    except (ValueError,TypeError):plan=[]
    st.markdown('#### H250-R5｜手動強制執行（獨立於例行排程）')
    st.caption(f'依已保存設定：{len(plan)} 項已啟用。忽略日期、排程時間、總開關；07若啟用仍固定最後並保留前置門檻。Worker 接手時重新核對永久設定。')
    with st.expander('查看本次預計執行順序',expanded=False):
        for i,job in enumerate(plan,1):st.write(f'{i:02d}｜{labels.get(job,job)}')
    cool=time.time()-float(st.session_state.get('_r5_click_epoch',0) or 0)<120
    if st.button(f'🚀 手動強制執行全部已啟用工作（{len(plan)}項）',key='h250r5_force_enabled',type='primary',disabled=not plan or cool or not callable(dispatch),use_container_width=True):
        st.session_state['_r5_click_epoch']=time.time()
        st.session_state['_v191_h19_autocatchup_try_epoch']=time.time()
        try:
            token=str(st.secrets.get('GITHUB_TOKEN','') or '').strip();owner=str(st.secrets.get('GITHUB_REPO_OWNER','cheng07021028') or '').strip();repo=str(st.secrets.get('GITHUB_REPO_NAME','stock-app') or '').strip();branch=str(st.secrets.get('GITHUB_CODE_BRANCH','main') or 'main').strip()
            if branch!='main':raise ValueError('中央 Worker 固定使用 main；請確認正式分支')
            active,msg=active_manual(token=token,owner=owner,repo=repo)
            if active is None:
                st.warning('未派送：無法確認活動批次｜'+msg);return
            if active:
                st.session_state['_r5_receipt']={'ok':True,'run_id':active,'manual_job':MODE,'message':msg};st.info(msg+'｜Run '+active);return
            receipt={};ok,msg=dispatch(token=token,owner=owner,repo=repo,branch=branch,wakeup_source=SOURCE,manual_job=MODE,receipt=receipt)
            st.session_state['_r5_receipt']={'ok':bool(ok),'manual_job':MODE,'message':str(msg),**receipt}
            if ok:st.info('手動派送已受理，尚未完成｜'+str(msg))
            else:st.error(str(msg))
        except Exception as exc:st.error('手動派送未確認：'+type(exc).__name__+'；請先查 Actions，不要連續重送。')
    if cool:st.caption('已啟用120秒防連點；非工作完成時間。')


def render_status(st,status,labels):
    batch=(status or {}).get('last_force_enabled_batch') or (status or {}).get('last_manual_batch') or {}; active=(status or {}).get('active_run') or {}; receipt=st.session_state.get('_r5_receipt') or {}
    if receipt:
        rid=str(receipt.get('run_id') or '')
        st.caption('本頁最近人工派送：'+str(receipt.get('message') or '')[:300])
        if rid.isdigit():
            owner=str(st.secrets.get('GITHUB_REPO_OWNER','cheng07021028'));repo=str(st.secrets.get('GITHUB_REPO_NAME','stock-app'))
            if all(re.fullmatch(r'[A-Za-z0-9_.-]{1,100}',s) for s in (owner,repo)):st.markdown(f'[查看本次 GitHub Run {rid}](https://github.com/{owner}/{repo}/actions/runs/{rid})')
        if receipt.get('request_id'):st.caption('永久派工ID：'+str(receipt['request_id']))
    if active.get('batch_mode')==MODE:
        st.info('最近人工批次進度（遠端狀態可能延遲）：'+str(active.get('batch_id'))+'｜'+labels.get(active.get('current_job'),str(active.get('current_job') or '等待／已暫停')))
    if batch.get('batch_mode')==MODE:
        summary=batch.get('summary') or {};valid=batch.get('validation') or {}
        st.markdown('##### 最近手動全啟用批次（遠端紀錄）')
        st.caption(f"批次 {batch.get('batch_id')}｜計畫 {len(batch.get('planned_jobs') or [])}｜成功 {summary.get('success',0)}／警示 {summary.get('warning',0)}／失敗 {summary.get('failed',0)}／阻擋 {summary.get('blocked',0)}；最終以該 Run 的遠端驗收為準。")
        if valid.get('job_results'):
            st.dataframe([{'工作':labels.get(r['job'],r['job']),'狀態':r['status'],'訊息':r['message']} for r in valid['job_results']],hide_index=True,use_container_width=True)
