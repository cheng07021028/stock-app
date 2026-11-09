# -*- coding: utf-8 -*-
"""V191-H153.3 cumulative runtime preflight (no network, no writes)."""
from __future__ import annotations
from pathlib import Path
import argparse
import ast

REQUIRED = (
    'godpick_auto_scheduler.py', 'godpick_auto_update_tasks.py', 'godpick_history_sources.py',
    'pages/10_推薦清單.py','pages/17_系統健康檢查.py',
    'godpick_h1532_technical_evidence.py','godpick_h1532_scope_repair.py',
    'godpick_h1512_excel_integrity.py','godpick_h153_growth_setup_rank.py',
    'pages/7_股神推薦.py',
    'tests/test_h1533_scheduler_stall.py',
)

def verify(root: Path) -> list[str]:
    errors=[]
    for rel in REQUIRED:
        p=root/rel
        if not p.is_file():
            errors.append(f'missing: {rel}')
            continue
        if p.suffix=='.py':
            try:
                ast.parse(p.read_text(encoding='utf-8-sig'), filename=str(p))
            except Exception as exc:
                errors.append(f'not parsable: {rel}: {exc}')
    source=(root/'godpick_auto_scheduler.py').read_text(encoding='utf-8') if (root/'godpick_auto_scheduler.py').exists() else ''
    page=(root/'pages/10_推薦清單.py').read_text(encoding='utf-8') if (root/'pages/10_推薦清單.py').exists() else ''
    history=(root/'godpick_history_sources.py').read_text(encoding='utf-8') if (root/'godpick_history_sources.py').exists() else ''
    checks={
        'scheduled_batch_budget': 'time_budget_seconds' in page and 'per_request_timeout_seconds' in page,
        'fast_source_no_daily_fallback':'allow_daily_fallback=allow_slow_fallback' in page and 'if allow_daily_fallback:' in history,
        'no_infinite_sync_retries': '_h1533_remote_failure' in source and '_h1533_sync_breaker' in source,
        'local_job_checkpoint': 'write_local_json_atomic(path, payload)' in source,
        'terminal_remote_checkpoint':'H153.3 terminal remote checkpoint' in source,
        'preserve_H126': (root/'pages/7_股神推薦.py').exists(),
    }
    errors.extend(f'missing contract: {key}' for key,ok in checks.items() if not ok)
    return errors

if __name__=='__main__':
    ap=argparse.ArgumentParser();ap.add_argument('--root',default='.')
    args=ap.parse_args()
    errors=verify(Path(args.root).resolve())
    if errors:
        for item in errors:print('FAIL',item)
        raise SystemExit(1)
    print('PASS H153.3 cumulative deployment | bounded tracking, sync circuit, checkpoint, H126 retained')
