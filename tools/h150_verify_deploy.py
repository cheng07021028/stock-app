# -*- coding: utf-8 -*-
"""Offline H150 cumulative deployment check, no token/network required."""
import argparse
import ast
from pathlib import Path
import sys

ROOT_FILES=(
 'pages/7_股神推薦.py','app_auth_core.py','godpick_h149_trade_intelligence.py',
 'godpick_h150_login_sync.py','godpick_h150_news_evidence.py',
 'tools/h150_actions_chain.py','tools/h150_public_news.py',
 '.github/workflows/h150_data_orchestrator.yml',
)

def verify(root):
 root=Path(root).resolve()
 if str(root) not in sys.path: sys.path.insert(0,str(root))
 notes=[]
 from tools.h149_verify_deploy import verify as check_h149
 old_ok,old_notes=check_h149(root)
 if not old_ok:notes+=old_notes
 for rel in ROOT_FILES:
  p=root/rel
  if not p.is_file():notes.append('MISSING: '+rel);continue
  if rel.endswith('.py'):
   try:ast.parse(p.read_text(encoding='utf-8'),filename=rel)
   except (SyntaxError,OSError,UnicodeError) as e:notes.append('BAD_PY: '+rel+':'+type(e).__name__)
 auth=(root/'app_auth_core.py').read_text(encoding='utf-8') if (root/'app_auth_core.py').is_file() else ''
 for fragment in ('_ensure_h150_auto_data_after_login()', 'streamlit_login_hook', 'h150_source_report'):
  if fragment not in auth:notes.append('MISSING_LOGIN_HOOK: '+fragment)
 report=(root/'godpick_h149_trade_intelligence.py').read_text(encoding='utf-8') if (root/'godpick_h149_trade_intelligence.py').is_file() else ''
 if 'match_existing_news(raw, day)' not in report:notes.append('MISSING_NEWS_BRIDGE')
 flow=(root/'.github/workflows/h150_data_orchestrator.yml').read_text(encoding='utf-8') if (root/'.github/workflows/h150_data_orchestrator.yml').is_file() else ''
 if 'workflow_dispatch:' not in flow or 'tools/h150_actions_chain.py' not in flow or 'h150_public_news.py' not in flow:
  notes.append('MISSING_CLOUD_WORKFLOW')
 return not notes,notes or ['PASS H150: all H149 dependencies, login trigger, public news and cloud workflow present']

if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--root',default=str(Path(__file__).resolve().parents[1]));args=p.parse_args()
 ok,messages=verify(args.root)
 for msg in messages:print(msg)
 sys.exit(0 if ok else 1)
