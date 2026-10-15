# -*- coding: utf-8 -*-
"""H127 safe precision patch. Run at the root of the active stock-app repository."""
from __future__ import annotations
import argparse
import ast
import shutil
from datetime import datetime
from pathlib import Path

MARKER = '# >>> V191_H127_RECORDS_SHARD_AUTHORITY'


def patch_source(text: str) -> tuple[str, str]:
    if MARKER in text:
        return text, 'ALREADY_PATCHED'
    old_read = '''def read_github_json(path_name: str, default: Any) -> tuple[Any, str]:
    cfg = github_config()
    if not cfg["token"]:
        return copy.deepcopy(default), "未設定 GITHUB_TOKEN"
    try:
'''
    new_read = '''def read_github_json(path_name: str, default: Any) -> tuple[Any, str]:
    cfg = github_config()
    if not cfg["token"]:
        return copy.deepcopy(default), "未設定 GITHUB_TOKEN"
    # >>> V191_H127_RECORDS_SHARD_AUTHORITY
    # The remote state is the publication pointer; a failed/incomplete shard
    # set must not be replaced with an older legacy JSON as if it were current.
    if (Path(path_name).name == RECORDS_FILE
            or path_name == (_secret("GODPICK_RECORDS_GITHUB_PATH", RECORDS_FILE) or RECORDS_FILE)):
        state_h127, _state_msg_h127 = read_github_json(RECORDS_STATE_FILE, {})
        if isinstance(state_h127, dict) and state_h127.get("storage_mode") == "h127-gzip-shards-v1":
            from godpick_h127_sharded_records import load_snapshot
            rows_h127, msg_h127 = load_snapshot(
                state_h127, get_json=read_github_json, record_hash_func=_json_hash
            )
            return (rows_h127 if rows_h127 is not None else copy.deepcopy(default)), msg_h127
    # <<< V191_H127_RECORDS_SHARD_AUTHORITY
    try:
'''
    if text.count(old_read) != 1:
        raise ValueError('read_github_json anchor incompatible or missing')
    text = text.replace(old_read, new_read, 1)
    old_422 = '''                elif response.status_code in (409, 422):
                    conflict_count += 1
'''
    new_422 = '''                elif response.status_code == 422 and any(
                    phrase in response.text.lower() for phrase in
                    ("too large", "file is too large", "too big", "exceeds the size")
                ):
                    # H127: validation error is NOT a CAS conflict. Retrying
                    # the identical 54MB body cannot succeed.
                    return False, (
                        f"GitHub 文件過大 HTTP 422：{path_name}｜{size_mb:.1f} MB｜"
                        "需使用 H127 壓縮分片，不再做無效 CAS 重傳"
                    )
                elif response.status_code in (409, 422):
                    conflict_count += 1
'''
    if text.count(old_422) != 1:
        raise ValueError('HTTP 422 CAS anchor incompatible or missing')
    text = text.replace(old_422, new_422, 1)
    old_write = '''        gh_path = _secret("GODPICK_RECORDS_GITHUB_PATH", RECORDS_FILE) or RECORDS_FILE
        gh1, ghm1 = write_github_json(gh_path, records, f"{reason}: persist godpick records")
'''
    new_write = '''        gh_path = _secret("GODPICK_RECORDS_GITHUB_PATH", RECORDS_FILE) or RECORDS_FILE
        from godpick_h127_sharded_records import write_snapshot
        h127_publication, ghm1 = write_snapshot(
            records, state,
            get_json=read_github_json,
            put_json=lambda p, obj: write_github_json(
                p, obj, f"{reason}: H127 immutable compressed shard"
            ),
            json_safe=_json_safe_value,
            record_hash_func=_json_hash,
        )
        gh1 = h127_publication is not None
'''
    if text.count(old_write) != 1:
        raise ValueError('H8 gateway anchor incompatible or missing')
    text = text.replace(old_write, new_write, 1)
    old_state = '''        gh2, ghm2 = write_github_json(RECORDS_STATE_FILE, state, f"{reason}: persist godpick records state")
'''
    new_state = '''        # Commit the state pointer only after the complete immutable chunk
        # collection and manifest have been verified by remote readback.
        h127_published_state = dict(state)
        h127_published_state.update(h127_publication)
        gh2, ghm2 = write_github_json(
            RECORDS_STATE_FILE, h127_published_state,
            f"{reason}: publish H127 sharded records authority state",
        )
'''
    if text.count(old_state) != 1:
        raise ValueError('H8 publish-state anchor incompatible or missing')
    text = text.replace(old_state, new_state, 1)

    # Prevent stale record states from superseding a newer state on CAS retry.
    old_pre_put = '''            try:
                body: dict[str, Any] = {
                    "message": f"{message} @ {_now_text()}",
'''
    new_pre_put = '''            try:
                if path_name == RECORDS_STATE_FILE:
                    existing_state, _ = read_github_json(RECORDS_STATE_FILE, {})
                    if isinstance(existing_state, dict) and isinstance(safe_payload, dict):
                        try:
                            remote_rev = int(existing_state.get("revision") or 0)
                            incoming_rev = int(safe_payload.get("revision") or 0)
                        except Exception:
                            remote_rev = incoming_rev = 0
                        if remote_rev and incoming_rev and remote_rev > incoming_rev:
                            return False, (
                                "GitHub records state 已有較新revision；拒絕舊快照覆寫"
                            )
                body: dict[str, Any] = {
                    "message": f"{message} @ {_now_text()}",
'''
    if text.count(old_pre_put) != 1:
        raise ValueError('state monotonic guard anchor incompatible or missing')
    text = text.replace(old_pre_put, new_pre_put, 1)
    ast.parse(text)
    return text, 'H127_READY'


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--check-only', action='store_true')
    p.add_argument('--root', type=Path, default=Path(__file__).resolve().parent.parent)
    args = p.parse_args()
    root = args.root.resolve()
    source = root / 'godpick_persistence_service.py'
    new_module = root / 'godpick_h127_sharded_records.py'
    if not source.is_file() or not new_module.is_file():
        raise SystemExit('H127_BLOCKED: missing current persistence file or H127 module; do not change files')
    current = source.read_text(encoding='utf-8-sig')
    updated, result = patch_source(current)
    print(f'{result} | root={root} | source={source.name}')
    if args.check_only or result == 'ALREADY_PATCHED':
        return
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup = source.with_name(source.name + f'.H127_backup_{timestamp}.bak')
    shutil.copy2(source, backup)
    temp = source.with_suffix(source.suffix + '.h127tmp')
    temp.write_text(updated, encoding='utf-8')
    temp.replace(source)
    print(f'H127_PATCHED | backup={backup.name} | authoritative existing records not modified')


if __name__ == '__main__':
    main()
