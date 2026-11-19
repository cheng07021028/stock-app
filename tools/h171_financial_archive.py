# -*- coding: utf-8 -*-
"""Append-only H171 official financial observation archive.

Default storage is LOCAL FILE, NOT a durable Streamlit Cloud database.
Use persistent managed storage / a controlled repository pipeline in production.
No retroactive first-observation timestamps or fake historical quarter availability.
"""
from __future__ import annotations
import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Callable
from zoneinfo import ZoneInfo
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from godpick_h171_pit_validation import SOURCE_URLS, normalize_official_ytd

DEFAULT = ROOT / 'data' / 'h115' / 'official_ytd_archive.json'


def refresh(*, get: Callable, observed_at: str, output: Path = DEFAULT) -> dict:
    """Only append successful official responses. Fail without clobbering history."""
    output = Path(output)
    source_health = {}
    collected = []
    for market, url in SOURCE_URLS.items():
        try:
            res = get(url, timeout=12)
            if getattr(res,'status_code',None) != 200:
                source_health[market] = f'HTTP_{getattr(res,"status_code","UNKNOWN")}'
                continue
            payload = res.json()
            rows = normalize_official_ytd(payload, market=market, observed_at=observed_at)
            if len(rows) < 20:
                source_health[market] = 'INSUFFICIENT_COVERAGE'
                continue
            collected.extend(rows)
            source_health[market] = f'OK_{len(rows)}'
        except (Exception,) as exc:
            source_health[market] = f'SOURCE_ERROR_{type(exc).__name__}'
    if not collected:
        return {'saved':False, 'status':'NO_VALID_OFFICIAL_RESPONSE', 'sources':source_health,
                'trade_authority_changed':False}
    old_rows=[]
    if output.is_file():
        if output.stat().st_size > 50_000_000:
            return {'saved':False,'status':'ARCHIVE_TOO_LARGE_REQUIRES_MAINTENANCE',
                    'sources':source_health,'trade_authority_changed':False}
        try:
            old=json.loads(output.read_text(encoding='utf-8-sig'))
            if not isinstance(old, dict) or not isinstance(old.get('rows'), list):
                raise ValueError('invalid previous archive schema')
            old_rows=old['rows']
            # No silent overwrite or repair of corrupted prior data.
            if any(not isinstance(x,dict) for x in old_rows):
                raise ValueError('invalid prior row')
        except (OSError, ValueError, UnicodeError, json.JSONDecodeError):
            return {'saved':False, 'status':'CORRUPT_ARCHIVE_NO_OVERWRITE',
                    'sources':source_health, 'trade_authority_changed':False}
    def fingerprint(item):
        columns=('code','market','year_quarter','source','eps_ytd',
                 'revenue_ytd','gross_profit_ytd','operating_profit_ytd','net_profit_ytd')
        return tuple(str(item.get(k)) for k in columns)
    seen={fingerprint(r) for r in old_rows}
    added=[]
    for rec in collected:
        fp=fingerprint(rec)
        if fp not in seen:
            seen.add(fp);added.append(rec)
    # If there are no changes, do not rewrite authoritative timestamp/history.
    if not added:
        return {'saved':False,'status':'NO_NEW_FINANCIAL_OBSERVATIONS',
                'sources':source_health,'existing':len(old_rows),
                'trade_authority_changed':False}
    doc={'version':'V191-H171-FIRST-OBSERVED-ARCHIVE',
         'updated_at':observed_at,'rows':old_rows+added,
         'source_health':source_health,
         'notice':'僅實際首次觀察日；先前季度首次收集不代表過去當時已可取得。不得回填歷史買進權限。'}
    output.parent.mkdir(parents=True,exist_ok=True)
    # Atomic write protects previous good snapshot from mid-write interruption.
    tmp=None
    try:
        with NamedTemporaryFile('w', dir=output.parent, suffix='.tmp',prefix='.h171_',
                                delete=False,encoding='utf-8',newline='\n') as f:
            tmp=Path(f.name)
            json.dump(doc,f,ensure_ascii=False,separators=(',',':'),allow_nan=False)
            f.flush();os.fsync(f.fileno())
        tmp.replace(output)
    finally:
        if tmp is not None and tmp.exists():
            tmp.unlink()
    return {'saved':True, 'status':'PARTIAL_OFFICIAL' if len([s for s in source_health.values() if s.startswith('OK_')])<2 else 'BOTH_MARKETS_OBSERVED',
            'added_rows':len(added), 'total_rows':len(doc['rows']), 'sources':source_health,
            'trade_authority_changed':False}


def main():
    parser=argparse.ArgumentParser(description='Collect official TWSE/TPEx cumulative YTD first-observed snapshots')
    parser.add_argument('--output',type=Path,default=DEFAULT)
    args=parser.parse_args()
    try:
        import requests
    except ImportError:
        parser.error('Install requests (existing app requirements)')
    now=datetime.now(ZoneInfo('Asia/Taipei')).isoformat(timespec='seconds')
    result=refresh(get=requests.get,observed_at=now,output=args.output)
    print(json.dumps(result,ensure_ascii=False,indent=2))
    if result['status'] in {'NO_VALID_OFFICIAL_RESPONSE','CORRUPT_ARCHIVE_NO_OVERWRITE',
                            'ARCHIVE_TOO_LARGE_REQUIRES_MAINTENANCE'}:
        raise SystemExit(2)

if __name__=='__main__':
    main()
