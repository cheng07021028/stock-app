# -*- coding: utf-8 -*-
"""Run autonomous public official data harvest. No secrets or trading writes."""
from __future__ import annotations
import argparse,json,sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from godpick_h185_auto_sources import SOURCES,fetch_official,adopt_h150_news

def main(argv=None):
 p=argparse.ArgumentParser()
 p.add_argument('--root',type=Path,default=ROOT)
 p.add_argument('--only',choices=list(SOURCES),action='append')
 p.add_argument('--h150-source',type=Path,default=None,help='optional existing H150 public RSS cache')
 args=p.parse_args(argv)
 report=fetch_official(root=args.root,source_names=tuple(args.only) if args.only else None)
 if args.h150_source is not None:
  report['h150_public_news']=adopt_h150_news(args.h150_source,root=args.root)
 print(json.dumps(report,ensure_ascii=False,indent=2))
 if report['failures']:
  print('::warning::H185 partial official data; inspect data/h185/health.json. No fabrication.')
 return 0 if report['success'] else 2

if __name__=='__main__':raise SystemExit(main())
