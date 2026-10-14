# -*- coding: utf-8 -*-
"""H122 report / allowlisted, bounded retry of idempotent official collectors.
No arbitrary shell, no external secrets in outputs, no mutation of strategy.
"""
from __future__ import annotations
import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import subprocess
import sys
import time
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from godpick_h122_incident_center import (VERSION, alert_transition, build_dashboard,
                                           retry_decision, to_html, to_markdown)


def read_json(path: Path):
    try: return json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError, UnicodeError): return {}


def atomic_if_changed(path: Path, text: str) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_file() and path.read_text(encoding='utf-8') == text: return False
    temp = path.with_name('.'+path.name+'.new')
    temp.write_text(text, encoding='utf-8')
    temp.replace(path)
    return True


def parsed_last_json(text: str):
    # H119 emits one JSON object; H117 logs may include preambles.
    text = text.strip()
    try: return json.loads(text)
    except (ValueError, TypeError):
        for start in reversed([i for i,ch in enumerate(text) if ch == '{']):
            try: return json.loads(text[start:])
            except ValueError: continue
    return None


def run_safe_collect(task: str, *, root: Path = ROOT, attempts: int = 2,
                     timeout_seconds: int = 160, wait_seconds: float = 5,
                     runner=None, sleep=None) -> dict:
    if task not in ('market_eod','corporate_raw'): raise ValueError('ONLY_OFFICIAL_COLLECTION_ALLOWED')
    if not 1 <= attempts <= 3 or not 10 <= timeout_seconds <= 300 or not 0 <= wait_seconds <= 30:
        raise ValueError('RETRY_BOUND_INVALID')
    commands = {'market_eod':['tools/h117_official_eod_cli.py','collect'],
                'corporate_raw':['tools/h119_corporate_action_cli.py','collect']}
    execute = runner or subprocess.run
    sleeper = sleep or time.sleep
    history = []
    for attempt in range(1,attempts+1):
        try:
            done = execute([sys.executable, *commands[task]], cwd=root, text=True,
                           capture_output=True, timeout=timeout_seconds, check=False)
            outcome = retry_decision(task, exit_code=done.returncode,
                                     result=parsed_last_json(done.stdout), output=done.stdout)
        except subprocess.TimeoutExpired:
            outcome = {'ok': False, 'retry': True, 'status': 'TRANSIENT_RETRY_ALLOWED', 'code':'SUBPROCESS_TIMEOUT'}
        except OSError:
            outcome = {'ok': False, 'retry': False, 'status':'EXECUTION_FAILED', 'code':'TOOL_NOT_AVAILABLE'}
        history.append({'attempt':attempt, **outcome})
        if outcome['ok'] or not outcome['retry'] or attempt == attempts:break
        sleeper(min(30, wait_seconds * attempt))
    last = history[-1]
    return {'version':VERSION, 'task':task, 'status':last['status'], 'ok':last['ok'],
            'attempts':len(history), 'history':history, 'automatic_trade':False,
            'durability':'COLLECTION_LOCAL_UNTIL_GITHUB_PUSH'}


def main(argv=None):
    p = argparse.ArgumentParser(description='H122 health dashboard, bounded official retry, read-only evidence')
    sub = p.add_subparsers(dest='action',required=True)
    dash = sub.add_parser('dashboard')
    dash.add_argument('--root',type=Path,default=ROOT)
    dash.add_argument('--health',type=Path,default=None)
    dash.add_argument('--receipt',type=Path,default=None)
    dash.add_argument('--remote',type=Path,default=None)
    dash.add_argument('--out',type=Path,default=None)
    dash.add_argument('--corporate-outcome', choices=['success','failure','skipped','cancelled','unknown'], default=None)
    retry = sub.add_parser('collect-safe')
    retry.add_argument('task',choices=['market_eod','corporate_raw'])
    retry.add_argument('--root',type=Path,default=ROOT)
    retry.add_argument('--attempts',type=int,default=2)
    args = p.parse_args(argv)
    if args.action=='collect-safe':
        result = run_safe_collect(args.task,root=args.root,attempts=args.attempts)
        # Only the allowlisted status is exposed, not raw HTTP errors, tokens, or stack traces.
        print(json.dumps(result,ensure_ascii=False))
        return 0 if result['ok'] else 2
    root=args.root
    health=read_json(args.health or root/'reports/h121/H121_operational_state.json')
    receipt=read_json(args.receipt or root/'reports/h121/H121_run_receipt.json')
    # Never accept a possibly stale old receipt as proof for the current run.
    remote=read_json(args.remote) if args.remote else {}
    if args.corporate_outcome is not None:receipt['corporate_step']=args.corporate_outcome
    out=args.out or root/'reports/h122/H122_dashboard_state.json'
    previous=read_json(out)
    dashboard=build_dashboard(health,receipt,remote)
    dashboard['transition']=alert_transition(previous,dashboard)
    # transition is run-local; do not include it in the persistent stable fingerprint.
    persist={k:v for k,v in dashboard.items() if k!='transition'}
    changed=atomic_if_changed(out,json.dumps(persist,ensure_ascii=False,sort_keys=True,indent=2)+'\n')
    md=to_markdown(persist)
    atomic_if_changed(out.parent/'H122_dashboard.md',md)
    atomic_if_changed(out.parent/'H122_dashboard.html',to_html(persist))
    print(json.dumps({'status':persist['status'],'transition':dashboard['transition'],
                      'state_changed':changed,'alerts':persist['alert_codes']},ensure_ascii=False))
    summary=os.environ.get('GITHUB_STEP_SUMMARY')
    if summary:
        with open(summary,'a',encoding='utf-8') as f:f.write(md)
    # PENDING is not a crash; BLOCKED is handled by workflow status/notification.
    return 0

if __name__=='__main__':raise SystemExit(main())
