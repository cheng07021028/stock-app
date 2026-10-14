# -*- coding: utf-8 -*-
"""Backup-first literal guarded patch: ONLY H123 import targets; never Page7/8."""
from __future__ import annotations
import argparse,ast,shutil
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
H105=ROOT/'godpick_h105_future_blackhorse_engine.py'
H116=ROOT/'godpick_h116_daily_feedback.py'
MARK='from godpick_h123_runtime_bridge import (load_context as h123_load_context'
REPLACE='from godpick_h124_runtime_bridge import (load_context as h123_load_context'


def transform(source:str)->str:
    if REPLACE in source:return source
    if source.count(MARK)!=1:raise ValueError('H124_COMPATIBILITY_BLOCKED: 需要已安裝H123安全修補版本')
    result=source.replace(MARK,REPLACE,1);ast.parse(result);return result


def snapshot(source:str)->str:
    key="FIELDS = ('H123法人實證狀態'"
    if "'H124大戶實證狀態'" in source:return source
    if source.count(key)!=1:raise ValueError('H124_SNAPSHOT_COMPATIBILITY_BLOCKED: 缺H123快照欄')
    new=source.replace(key,"FIELDS = ('H124大戶實證狀態','H124大戶變化pp','H124大戶來源雜湊','H124大戶原因','H124主榜決策','H123法人實證狀態'",1)
    ast.parse(new);return new


def main(argv=None):
    p=argparse.ArgumentParser();p.add_argument('--check-only',action='store_true');args=p.parse_args(argv)
    if not H105.is_file() or not H116.is_file():raise ValueError('找不到正式H105/H116，禁止不完整修補')
    before={H105:H105.read_text(encoding='utf-8-sig'),H116:H116.read_text(encoding='utf-8-sig')}
    after={H105:transform(before[H105]),H116:snapshot(before[H116])}
    if args.check_only:
        print('H124_READY; '+', '.join(p.name+':'+('PATCHABLE' if before[p]!=after[p] else 'ALREADY_PATCHED') for p in before));return 0
    for path in before:
        if before[path]==after[path]:continue
        backup=path.with_suffix(path.suffix+'.before_h124.bak')
        if backup.exists() and backup.read_text(encoding='utf-8-sig')!=before[path]:
            raise ValueError('已有不同版本備份，拒絕覆蓋:'+str(backup))
        if not backup.exists():shutil.copy2(path,backup)
        path.write_text(after[path],encoding='utf-8');ast.parse(path.read_text(encoding='utf-8'))
    print('H124_PATCH_PASS; GitHub提交請排除*.bak; 保留第7/8頁與JSON資料')
    return 0
if __name__=='__main__':raise SystemExit(main())
