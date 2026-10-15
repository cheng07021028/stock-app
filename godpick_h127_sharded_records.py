# -*- coding: utf-8 -*-
"""V191-H127: immutable, compressed GitHub recommendation-record shards.

Each record snapshot is content-addressed.  The caller publishes the tiny
`godpick_records_sync_state.json` *only after* all chunks and their manifest
have been independently verified.  Legacy godpick_records.json is never
modified or deleted by this module.
"""
from __future__ import annotations

import base64
import gzip
import hashlib
import json
from typing import Any, Callable

MODE = 'h127-gzip-shards-v1'
PART_RAW_BYTES = 360_000
PREFIX = 'data/godpick_records_h127'


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _json_bytes(payload: Any) -> bytes:
    return json.dumps(payload, ensure_ascii=False, separators=(',', ':'), default=str).encode('utf-8')


def manifest_path(record_hash: str) -> str:
    if len(record_hash) != 64 or not all(ch in '0123456789abcdef' for ch in record_hash):
        raise ValueError('invalid authority content hash')
    return f'{PREFIX}/{record_hash[:2]}/{record_hash}/manifest.json'


def _put_verified(path: str, body: dict, get_json: Callable, put_json: Callable) -> tuple[bool, str]:
    existing, existing_message = get_json(path, None)
    if isinstance(existing, dict):
        if existing == body:
            return True, 'same verified content'
        return False, f'immutable shard conflict at {path}; refused overwrite'
    # For missing GitHub files, the repository reader reports 404; do not turn
    # auth errors/timeouts into a possibly unsafe create attempt.
    if existing is None and not any(token in str(existing_message).lower() for token in ('尚未建立', '404', 'not found')):
        return False, f'cannot establish shard absence: {path} | {existing_message}'
    ok, message = put_json(path, body)
    if not ok:
        return False, str(message)
    check, check_message = get_json(path, None)
    if check != body:
        return False, f'remote shard readback mismatch at {path}: {check_message}'
    return True, str(message)


def write_snapshot(records: list, state: dict, *, get_json: Callable, put_json: Callable,
                   json_safe: Callable, record_hash_func: Callable) -> tuple[dict | None, str]:
    """Write and verify all immutable parts; return state additions, not a commit."""
    try:
        safe = json_safe(records)
        expected_hash = str(state.get('payload_hash', ''))
        # Legacy recommendation archives contain NaN floats. The old state
        # hashes those values as NaN, while _json_safe_value converts them to
        # null. Preserve the exact already-published authority semantics; do
        # not silently rewrite 2,490 historical records on migration.
        if record_hash_func(safe) == expected_hash:
            snapshot = safe
        elif record_hash_func(records) == expected_hash:
            snapshot = records
        else:
            return None, 'local record payload hash does not match authority state'
        if len(snapshot) != int(state.get('count', len(snapshot))):
            return None, 'local record count does not match authority state'
        raw = _json_bytes(snapshot)
        compressed = gzip.compress(raw, compresslevel=6, mtime=0)
        parts = [compressed[i:i+PART_RAW_BYTES] for i in range(0, len(compressed), PART_RAW_BYTES)]
        if not parts:
            return None, 'empty gzip snapshot'
        base = manifest_path(expected_hash).rsplit('/', 1)[0]
        entries = []
        for idx, part in enumerate(parts):
            path = f'{base}/part-{idx:05d}.json'
            body = {
                'format': MODE, 'payload_hash': expected_hash, 'part_index': idx,
                'part_sha256': _sha(part), 'data_b64': base64.b64encode(part).decode('ascii'),
            }
            if len(_json_bytes(body)) >= 650_000:
                return None, f'shard exceeds GitHub safe payload size: {idx}'
            ok, message = _put_verified(path, body, get_json, put_json)
            if not ok:
                return None, f'chunk {idx+1}/{len(parts)} failed; authority state unchanged: {message}'
            entries.append({'path': path, 'sha256': _sha(part), 'bytes': len(part)})
        metadata = {
            'format': MODE, 'payload_hash': expected_hash, 'record_count': len(snapshot),
            'raw_sha256': _sha(raw), 'gzip_sha256': _sha(compressed),
            'raw_bytes': len(raw), 'gzip_bytes': len(compressed), 'parts': entries,
        }
        target = manifest_path(expected_hash)
        ok, message = _put_verified(target, metadata, get_json, put_json)
        if not ok:
            return None, f'manifest failed; authority state unchanged: {message}'
        return {'storage_mode': MODE, 'shard_manifest_path': target,
                'shard_manifest_sha256': _sha(_json_bytes(metadata))}, (
            f'H127 verified {len(snapshot)} records in {len(parts)} immutable shards; '
            f'raw={len(raw)/1048576:.1f}MB compressed={len(compressed)/1048576:.1f}MB'
        )
    except Exception as exc:
        return None, f'H127 shard save error; state unchanged: {type(exc).__name__}: {exc}'


def load_snapshot(state: dict, *, get_json: Callable, record_hash_func: Callable,
                  max_raw_bytes: int = 400_000_000) -> tuple[list | None, str]:
    """Reject missing/modified/incomplete shards, never return partial records."""
    try:
        if state.get('storage_mode') != MODE:
            return None, 'not H127 shard authority'
        expected_hash = str(state.get('payload_hash', ''))
        path = manifest_path(expected_hash)
        if state.get('shard_manifest_path') != path:
            return None, 'state shard path/hash mismatch'
        manifest, message = get_json(path, None)
        if not isinstance(manifest, dict) or manifest.get('format') != MODE or manifest.get('payload_hash') != expected_hash:
            return None, f'missing/mismatched shard manifest: {message}'
        if _sha(_json_bytes(manifest)) != state.get('shard_manifest_sha256'):
            return None, 'shard manifest integrity mismatch'
        entries = manifest.get('parts')
        if not isinstance(entries, list) or not entries or len(entries) > 500:
            return None, 'invalid shard count'
        if not isinstance(manifest.get('raw_bytes'), int) or manifest['raw_bytes'] > max_raw_bytes:
            return None, 'snapshot exceeds safe decompression ceiling'
        gathered = []
        for i, e in enumerate(entries):
            if e.get('path') != f'{path.rsplit("/",1)[0]}/part-{i:05d}.json':
                return None, f'shard path/index mismatch at {i}'
            part, message = get_json(e['path'], None)
            if not isinstance(part, dict) or part.get('format') != MODE or part.get('payload_hash') != expected_hash or part.get('part_index') != i:
                return None, f'incomplete/incorrect shard {i}: {message}'
            compressed_part = base64.b64decode(part['data_b64'], validate=True)
            if _sha(compressed_part) != e['sha256'] or _sha(compressed_part) != part['part_sha256'] or len(compressed_part) != e['bytes']:
                return None, f'shard content checksum failure {i}'
            gathered.append(compressed_part)
        compressed = b''.join(gathered)
        if len(compressed) != manifest['gzip_bytes'] or _sha(compressed) != manifest['gzip_sha256']:
            return None, 'compressed payload checksum mismatch'
        # Bounded decompression prevents unexpected bombs from malicious blobs.
        import zlib
        obj = zlib.decompressobj(wbits=31)
        raw = obj.decompress(compressed, max_raw_bytes + 1)
        if len(raw) > max_raw_bytes or not obj.eof:
            return None, 'decompression safety limit or incomplete stream'
        raw += obj.flush()
        if len(raw) != manifest['raw_bytes'] or _sha(raw) != manifest['raw_sha256']:
            return None, 'reconstructed payload SHA mismatch'
        rows = json.loads(raw.decode('utf-8'))
        if not isinstance(rows, list) or len(rows) != manifest['record_count'] or len(rows) != int(state.get('count', -1)):
            return None, 'record count mismatch'
        if record_hash_func(rows) != expected_hash:
            return None, 'authority semantic payload hash mismatch'
        return rows, f'H127 fully verified {len(rows)} records from {len(entries)} immutable shards'
    except Exception as exc:
        return None, f'H127 restore integrity failure: {type(exc).__name__}: {exc}'
