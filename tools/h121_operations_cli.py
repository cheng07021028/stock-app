# -*- coding: utf-8 -*-
"""H121 inspect (no market writes), summarize, and prove remote Git HEAD after push."""
from __future__ import annotations
import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from zoneinfo import ZoneInfo
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from godpick_h121_operations import inspect_repository, TZ, VERSION


def _replace_if_changed(path: Path, content: str) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_file() and path.read_text(encoding='utf-8') == content:
        return False
    tmp = path.with_name('.' + path.name + '.new')
    tmp.write_text(content, encoding='utf-8')
    tmp.replace(path)
    return True


def _remote_verify(root: Path, branch: str) -> dict:
    # Branch from GitHub workflow; no user-provided executable components.
    if not re.fullmatch(r'[A-Za-z0-9_./-]{1,150}', branch) or '..' in branch or branch.startswith('-'):
        return {'status': 'INVALID_BRANCH', 'remote_verified': False}
    try:
        head = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=root, capture_output=True, text=True, timeout=15, check=True).stdout.strip()
        remote = subprocess.run(['git', 'ls-remote', '--exit-code', 'origin', 'refs/heads/' + branch],
                                cwd=root, capture_output=True, text=True, timeout=30, check=True).stdout.strip().split()[0]
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired, IndexError):
        # Never include stderr: remote URL / credential helpers may contain credentials.
        return {'status': 'REMOTE_QUERY_FAILED', 'remote_verified': False}
    ok = bool(re.fullmatch(r'[0-9a-f]{40}', head) and head == remote)
    return {'status': 'REMOTE_HEAD_MATCH' if ok else 'REMOTE_HEAD_DIFFERS',
            'remote_verified': ok, 'local_head': head if re.fullmatch(r'[0-9a-f]{40}', head) else '',
            'remote_head': remote if re.fullmatch(r'[0-9a-f]{40}', remote) else ''}


def main(argv=None):
    p = argparse.ArgumentParser(description='H121 stockapp operations / persistence health; no trade authority')
    commands = p.add_subparsers(dest='command', required=True)
    inspect = commands.add_parser('inspect')
    inspect.add_argument('--root', type=Path, default=ROOT)
    inspect.add_argument('--state', type=Path, default=ROOT / 'reports/h121/H121_operational_state.json')
    inspect.add_argument('--receipt', type=Path, default=ROOT / 'reports/h121/H121_run_receipt.json')
    inspect.add_argument('--collect-outcome', default='unknown')
    inspect.add_argument('--review-outcome', default='unknown')
    verify = commands.add_parser('verify-remote')
    verify.add_argument('--root', type=Path, default=ROOT)
    verify.add_argument('--branch', default=os.environ.get('GITHUB_REF_NAME', 'main'))
    verify.add_argument('--receipt', type=Path, default=ROOT / 'reports/h121/H121_remote_receipt.json')
    summary = commands.add_parser('summary')
    summary.add_argument('--receipt', type=Path, default=ROOT / 'reports/h121/H121_run_receipt.json')
    summary.add_argument('--remote-receipt', type=Path, default=ROOT / 'reports/h121/H121_remote_receipt.json')
    args = p.parse_args(argv)
    if args.command == 'inspect':
        state = inspect_repository(args.root)
        stable = json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + '\n'
        changed = _replace_if_changed(args.state, stable)
        receipt = {'version': VERSION, 'inspected_at': datetime.now(TZ).isoformat(timespec='seconds'),
                   'state_sha256': state['state_sha256'], 'status': state['status'],
                   'issues': state['issues'], 'state_changed': changed,
                   'collect_step': args.collect_outcome, 'review_step': args.review_outcome,
                   'remote_status': 'UNVERIFIED_UNTIL_GIT_PUSH_AND_LS_REMOTE',
                   'durability': 'LOCAL_ONLY_PENDING_REMOTE_CHECK'}
        _replace_if_changed(args.receipt, json.dumps(receipt, ensure_ascii=False, indent=2) + '\n')
        print(json.dumps({'status': state['status'], 'issues': [x['code'] for x in state['issues']],
                          'state_changed': changed, 'remote_verified': False}, ensure_ascii=False))
        return 0  # no readiness proof is not an execution crash; review receipt.
    if args.command == 'verify-remote':
        result = _remote_verify(args.root, args.branch)
        _replace_if_changed(args.receipt, json.dumps(result, ensure_ascii=False, indent=2) + '\n')
        print(json.dumps(result, ensure_ascii=False))
        return 0 if result['remote_verified'] else 2
    text = '# H121 股神每日執行健康檢查\n\n'
    if args.receipt.is_file():
        rec = json.loads(args.receipt.read_text(encoding='utf-8'))
        text += f"- 健康狀態：`{rec.get('status')}`\n"
        text += f"- 官方行情收集步驟：`{rec.get('collect_step')}`\n"
        text += f"- 績效計算步驟：`{rec.get('review_step')}`\n"
        text += f"- 阻礙事項：{', '.join(x.get('code','') for x in rec.get('issues', [])) or '無'}\n"
    else:
        text += '- 健康收據不存在，不能宣稱已完成\n'
    if args.remote_receipt.is_file():
        remote = json.loads(args.remote_receipt.read_text(encoding='utf-8'))
        text += f"- GitHub 遠端提交確認：`{remote.get('status')}`\n"
    else:
        text += '- GitHub 遠端提交確認：`NOT_VERIFIED`\n'
    text += '\n> 所有選股和績效資訊僅供研究，H120 人工認證與樣本要求未放寬。\n'
    dest = os.getenv('GITHUB_STEP_SUMMARY')
    if dest:
        with open(dest, 'a', encoding='utf-8') as f: f.write(text)
    print(text)
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
