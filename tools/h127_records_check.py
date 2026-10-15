# -*- coding: utf-8 -*-
"""Safe, read-only preflight for H127. Does not upload or change records."""
from __future__ import annotations
import argparse
import gzip
import hashlib
import json
import sys
from pathlib import Path


def run():
    ap = argparse.ArgumentParser()
    ap.add_argument('--root', type=Path, default=Path(__file__).resolve().parent.parent)
    args = ap.parse_args()
    root = args.root.resolve()
    local = root / 'godpick_records.json'
    state_path = root / 'godpick_records_sync_state.json'
    if not local.is_file():
        print('H127_LOCAL_MISSING | file does not exist in this workspace')
        sys.exit(2)
    raw = local.read_bytes()
    rows = json.loads(raw.decode('utf-8-sig'))
    if not isinstance(rows, list):
        print('H127_WRONG_FORMAT | expected list')
        sys.exit(2)
    cleaned = json.dumps(rows, ensure_ascii=False, separators=(',', ':'), default=str).encode('utf-8')
    packed = gzip.compress(cleaned, compresslevel=6, mtime=0)
    state = json.loads(state_path.read_text(encoding='utf-8-sig')) if state_path.exists() else {}
    local_hash = hashlib.sha256(json.dumps(rows, ensure_ascii=False, sort_keys=True, separators=(',', ':'), default=str).encode('utf-8')).hexdigest()
    matches = state.get('payload_hash') == local_hash if isinstance(state, dict) else False
    print(f'H127_LOCAL_RECORDS={len(rows)}')
    print(f'H127_LEGACY_BYTES={len(raw)}')
    print(f'H127_COMPRESSED_BYTES_ESTIMATE={len(packed)}')
    print(f'H127_SHARD_COUNT_ESTIMATE={(len(packed)+359999)//360000}')
    print(f'H127_STATE_HASH_MATCH={matches}')
    print(f'H127_STATE_COUNT={(state.get("count") if isinstance(state,dict) else "unknown")}')
    print('NOTE: This is a local-file check only; remote GitHub persistence is NOT verified.')
    if not matches:
        print('ACTION: investigate state/hash mismatch; do not force overwrite remote authority.')

if __name__=='__main__':
    run()
