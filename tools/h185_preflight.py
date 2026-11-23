# -*- coding: utf-8 -*-
"""H185 safe incremental installation; no deletion, no unknown-version overwrite."""
from __future__ import annotations
import argparse,hashlib,json,os,shutil,sys,tempfile
from datetime import datetime
from pathlib import Path

PAYLOAD=Path(__file__).resolve().parent.parent
FILES=(
 'godpick_h184_research_lab.py',
 'config/h184_role_contracts.json',
 'godpick_h185_auto_sources.py',
 'config/h185_auto_role_map.json',
 'pages/28_十二研究員決策研究中心.py',
 'tools/h185_auto_collect.py',
 '.github/workflows/h185_autonomous_research.yml',
)
# Original H184 checksums are fixed at packaging time (not taken from target machine).
H184_PAGE_SHA='a0a1803dd77118c55296d7d4021ce7b25d22e8733cad52757963b6216aeb305e'
H184_ENGINE_SHA='ccc74dbb6c1973d48a7b79286f432c55eb3061a7f765edab28affd85d326c1cd'
H184_CONTRACT_SHA='7f940b34128c771fe532620ff1c8eb42ec950932a4f2274026fee184ec9172a9'

def sha(path:Path)->str:return hashlib.sha256(path.read_bytes()).hexdigest()

def verify(root:Path)->dict:
 root=root.resolve()
 if not (root/'requirements.txt').is_file() or not (root/'pages/7_股神推薦.py').is_file():
  return {'status':'BLOCKED_NOT_STOCKAPP','root':str(root),'details':[]}
 entries=[];conflicts=[];install=[]
 for rel in FILES:
  src=PAYLOAD/rel;dst=root/rel
  if not src.is_file():return {'status':'BLOCKED_PAYLOAD_MISSING','root':str(root),'details':[rel]}
  current_sha=sha(dst) if dst.is_file() else ''
  new_sha=sha(src)
  baseline=(H184_PAGE_SHA if rel.startswith('pages/28_') else
            H184_ENGINE_SHA if rel=='godpick_h184_research_lab.py' else
            H184_CONTRACT_SHA if rel=='config/h184_role_contracts.json' else '')
  if current_sha==new_sha:mode='ALREADY_INSTALLED'
  elif current_sha=='':mode='CREATE';install.append(rel)
  elif current_sha==baseline and rel.startswith('pages/28_'):mode='UPDATE_H184_PAGE';install.append(rel)
  else:mode='BLOCKED_UNKNOWN_CHANGED_FILE';conflicts.append(rel)
  entries.append({'path':rel,'mode':mode})
 return {'status':'BLOCKED_UNKNOWN_VERSION' if conflicts else (
    'ALREADY_H185' if not install else 'READY_APPLY_H185'),
    'root':str(root),'details':entries,'blockers':conflicts}

def apply(root:Path)->dict:
 result=verify(root)
 if result['status']!='READY_APPLY_H185':return result
 files=[r['path'] for r in result['details'] if r['mode'] in ('CREATE','UPDATE_H184_PAGE')]
 root=root.resolve()
 # Back up only preexisting H184 page, do not alter production secrets or data.
 backup_root=root/'.h185_backup'/datetime.now().strftime('%Y%m%d_%H%M%S')
 for rel in files:
  dst=root/rel
  if dst.exists():
   bp=backup_root/rel;bp.parent.mkdir(parents=True,exist_ok=True)
   shutil.copy2(dst,bp)
 for rel in files:
  src=PAYLOAD/rel;dst=root/rel
  dst.parent.mkdir(parents=True,exist_ok=True)
  fd,temp=tempfile.mkstemp(dir=dst.parent,prefix='.h185_')
  try:
   with os.fdopen(fd,'wb') as f:f.write(src.read_bytes());f.flush();os.fsync(f.fileno())
   os.replace(temp,dst)
  finally:
   if os.path.exists(temp):os.unlink(temp)
 complete=verify(root)
 if complete['status']!='ALREADY_H185':
  raise RuntimeError('Post-install file verification failed; review .h185_backup')
 return {'status':'APPLIED_H185','count':len(files),'backup':str(backup_root) if backup_root.exists() else '',
         'new_file_check':'ALREADY_H185'}

def main(argv=None):
 p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--apply',action='store_true')
 a=p.parse_args(argv)
 try:r=apply(a.root) if a.apply else verify(a.root)
 except Exception as exc:r={'status':'ERROR','error':f'{type(exc).__name__}: {exc}'}
 print(json.dumps(r,ensure_ascii=False,indent=2))
 return 0 if r['status'] in ('READY_APPLY_H185','ALREADY_H185','APPLIED_H185') else 2

if __name__=='__main__':raise SystemExit(main())
