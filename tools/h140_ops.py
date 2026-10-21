# -*- coding: utf-8 -*-
"""H140 operational monitor CLI. Writes ONLY data/h140/H140_monitor.json."""
from __future__ import annotations
import argparse
from datetime import date, datetime
from pathlib import Path
import json
import os
import sys
import tempfile
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from godpick_h140_observability import diagnose
from godpick_h134_decision_vault import _json_bytes


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument('--runtime-root', type=Path, required=True)
    p.add_argument('--asof', type=date.fromisoformat, default=datetime.now(ZoneInfo('Asia/Taipei')).date())
    p.add_argument('--market-closed', action='store_true')
    p.add_argument('--workflow-run-id', default=os.environ.get('GITHUB_RUN_ID', ''))
    p.add_argument('--source-ref', default=os.environ.get('GITHUB_REF_NAME', ''))
    args = p.parse_args(argv)
    receipt = diagnose(runtime_root=args.runtime_root, asof=args.asof,
                       market_closed=args.market_closed,
                       workflow_run_id=args.workflow_run_id, source_ref=args.source_ref)
    target = args.runtime_root / 'data/h140/H140_monitor.json'
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix='.h140_',suffix='.tmp',dir=target.parent)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(_json_bytes(receipt))
            stream.flush(); os.fsync(stream.fileno())
        os.replace(tmp,target)
    finally:
        if os.path.exists(tmp): os.unlink(tmp)
    print(json.dumps({'status': receipt['status'], 'asof':receipt['asof_tw'],
                      'ok': receipt['pipeline_stages_ok'], 'total':receipt['pipeline_stages_total'],
                      'findings':receipt['findings']},ensure_ascii=False))
    # Only the existing H137 pipeline owns the final failure exit; always emit a receipt.
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
