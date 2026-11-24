#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""H189 conservative three-file overlay. Nothing changes until --apply."""
from __future__ import annotations
import argparse, hashlib, json, os, shutil, sys
from pathlib import Path
from datetime import datetime

EXPECTED_GIT_BLOB_SHA={
 'godpick_h114_parallel_alpha.py':'e67888cbb5e704736dbf47b1a4b06c536364a2da',
 'godpick_h153_growth_setup_rank.py':'8c3b82d2e9547209342027a6d7f094090625c511',
 'godpick_h175_evidence_rescue.py':'f3eee26c95aa3bdfd37985b7b4f887cb71ff9c44',
}

def blobsha(data: bytes) -> str:
 return hashlib.sha1(f'blob {len(data)}\0'.encode()+data).hexdigest()

def preflight(root:Path, payload:Path):
 details=[]; errors=[]
 for rel,expected in EXPECTED_GIT_BLOB_SHA.items():
  source=payload/rel;target=root/rel
  if not source.is_file():errors.append('PATCH_MISSING: '+rel);continue
  if not target.is_file():errors.append('DEST_MISSING: '+rel);continue
  current=blobsha(target.read_bytes());wanted=blobsha(source.read_bytes())
  status=('ALREADY_H189' if current==wanted else
          'MATCHED_BASELINE' if current==expected else 'UNKNOWN_CHANGED_VERSION')
  if status=='UNKNOWN_CHANGED_VERSION':errors.append(status+': '+rel+' '+current)
  details.append({'file':rel,'status':status,'before_sha':current,'after_sha':wanted})
 return details,errors

def main():
 ap=argparse.ArgumentParser();ap.add_argument('--root',required=True);ap.add_argument('--apply',action='store_true')
 args=ap.parse_args();root=Path(args.root).expanduser().resolve();payload=Path(__file__).resolve().parents[1]
 details,errors=preflight(root,payload)
 print(json.dumps({'details':details,'errors':errors},ensure_ascii=False,indent=2))
 if errors:return 3
 if not args.apply:
  print('READY_APPLY_H189' if any(x['status']=='MATCHED_BASELINE' for x in details) else 'ALREADY_APPLIED_H189');return 0
 todo=[x['file'] for x in details if x['status']=='MATCHED_BASELINE']
 if not todo:print('ALREADY_APPLIED_H189');return 0
 backups=root/'h189_install_backups'/datetime.now().strftime('%Y%m%d_%H%M%S')
 backups.mkdir(parents=True,exist_ok=False)
 try:
  for rel in todo:
   source=payload/rel;target=root/rel;backup=backups/rel;backup.parent.mkdir(parents=True,exist_ok=True)
   shutil.copy2(target,backup)
  # Only after all backups exist, do atomic file updates.
  for rel in todo:
   source=payload/rel;target=root/rel;tmp=target.with_name(target.name+'.h189.tmp')
   tmp.write_bytes(source.read_bytes());os.replace(tmp,target)
 except Exception as exc:
  print('PARTIAL_APPLY_ERROR: '+str(exc),file=sys.stderr)
  for rel in todo:
   backup=backups/rel;target=root/rel
   if backup.is_file():shutil.copy2(backup,target)
  return 4
 details2,errors2=preflight(root,payload)
 if errors2 or any(x['status']!='ALREADY_H189' for x in details2):
  print('POST_INSTALL_VERIFICATION_FAILED; backup at '+str(backups));return 5
 print('APPLIED_H189 / verified / backup='+str(backups));return 0

if __name__=='__main__':sys.exit(main())
