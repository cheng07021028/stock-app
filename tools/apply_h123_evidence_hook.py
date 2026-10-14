# -*- coding: utf-8 -*-
"""Idempotent, guarded, backup-first patch of the user's CURRENT H105 module.
Never ship an old H105 or Page7 file as a destructive replacement.
"""
from __future__ import annotations
import argparse,ast,shutil,hashlib,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
TARGET=ROOT/'godpick_h105_future_blackhorse_engine.py'
SNAP=ROOT/'godpick_h116_daily_feedback.py'
MARK='H123_SAFE_PROVENANCE_BRIDGE_V1'


def patch_source(text:str)->str:
    if MARK in text:return text
    pairs=[
       ('from godpick_h113_quality_gate import audit as h113_audit, VERSION as H113_VERSION',
        'from godpick_h113_quality_gate import audit as h113_audit, VERSION as H113_VERSION\n'
        'from godpick_h123_runtime_bridge import (load_context as h123_load_context, review as h123_review, '
        'official_fields_if_proven as h123_official_fields, apply_strict_h113 as h123_strict)'),
       ('source_status: str = "", fundamental_map: dict[str, dict[str, Any]] | None = None) -> pd.DataFrame:',
        'source_status: str = "", fundamental_map: dict[str, dict[str, Any]] | None = None, h123_context: dict[str, Any] | None = None, h123_live: bool = False) -> pd.DataFrame:'),
       ('        merged.update(h113_audit(merged, info_asof=info_asof or market_day))',
        '        # '+MARK+'; OFF/MONITOR/STRICT never grants a Formal trade.\n'
        '        _h123_result = h123_review(merged, context=h123_context or {},\n'
        '              live_source=h123_live, info_asof=info_asof, market_day=market_day)\n'
        '        merged.update(_h123_result)\n'
        '        if not str(merged.get("H110證據狀態") or "").startswith("CONFLICT"):\n'
        '            merged.update(h123_official_fields(_h123_result))\n'
        '        merged.update(h113_audit(merged, info_asof=info_asof or market_day))\n'
        '        h123_strict(merged, _h123_result, mode=(h123_context or {}).get("mode", "OFF"))'),
       ('    fundamental_map = h114_load_verified_evidence(base_dir, asof=info_asof)',
        '    fundamental_map = h114_load_verified_evidence(base_dir, asof=info_asof)\n'
        '    h123_context = h123_load_context(base_dir)'),
       ('                                  fundamental_map=fundamental_map)',
        '                                  fundamental_map=fundamental_map, h123_context=h123_context, h123_live=live_source)'),
       ('    return out.loc[:, BLACKHORSE_OVERVIEW_COLUMNS].copy()',
        '    return out.loc[:, BLACKHORSE_OVERVIEW_COLUMNS + [c for c in out.columns if c.startswith("H123") and c not in BLACKHORSE_OVERVIEW_COLUMNS]].copy()'),
    ]
    for old,new in pairs:
        if text.count(old)!=1:raise RuntimeError('PATCH_COMPATIBILITY_BLOCKED: '+old[:85]+' count='+str(text.count(old)))
        text=text.replace(old,new,1)
    ast.parse(text)
    return text


def patch_snapshot(text:str)->str:
    if "'H123法人實證狀態'" in text:return text
    old="FIELDS = ('H114主導策略'"
    if text.count(old)!=1:raise RuntimeError('H116_SNAPSHOT_COMPATIBILITY_BLOCKED')
    # Include evidence diagnostics in *new* immutable snapshots only.
    text=text.replace(old,"FIELDS = ('H123法人實證狀態','H123來源雜湊','H123法人逐日有效天數','H123法人實證原因','H114主導策略'",1)
    ast.parse(text)
    return text


def main(argv=None):
    ap=argparse.ArgumentParser(description='safe H123 H105/H116 evidence hook with backups')
    ap.add_argument('--check-only',action='store_true')
    args=ap.parse_args(argv)
    if not TARGET.is_file() or not SNAP.is_file():raise RuntimeError('找不到既有H105/H116程式，拒絕建立空檔')
    original=TARGET.read_text(encoding='utf-8-sig');snapshot=SNAP.read_text(encoding='utf-8-sig')
    new=patch_source(original);next_snapshot=patch_snapshot(snapshot)
    if args.check_only:
        print('H123_READY: H105='+('already patched' if original==new else 'patchable')+', H116='+('already patched' if snapshot==next_snapshot else 'patchable'))
        return 0
    changed=[]
    for path,before,after in [(TARGET,original,new),(SNAP,snapshot,next_snapshot)]:
        if before==after:continue
        backup=path.with_suffix(path.suffix+'.before_h123.bak')
        if backup.exists() and backup.read_text(encoding='utf-8-sig')!=before:
            raise RuntimeError('備份已存在但來源版本不符，拒絕覆蓋：'+str(backup))
        if not backup.exists():shutil.copy2(path,backup)
        path.write_text(after,encoding='utf-8')
        changed.append(str(path.name))
    for path in [TARGET,SNAP]:ast.parse(path.read_text(encoding='utf-8'))
    print('H123_PATCH_PASS:',changed or 'ALREADY_PATCHED','; keep backup files LOCAL and do not commit them')
    return 0

if __name__=='__main__':raise SystemExit(main())
