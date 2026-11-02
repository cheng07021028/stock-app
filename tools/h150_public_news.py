# -*- coding: utf-8 -*-
"""Generate a public-news-only H150 cache; never claim individual stock impact.

Titles/links/dates from Google News RSS. No scraped full text, private data,
price predictions, fake sources, or remote writes from the worker script.
"""
from __future__ import annotations
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from urllib.parse import quote_plus, urlparse
from urllib.request import Request, urlopen
import argparse
import hashlib
import json
from pathlib import Path
import xml.etree.ElementTree as ET

QUERIES=('台股 重大訊息','台灣 半導體 AI 伺服器','台股 上市櫃 公司 財報','台灣 產業 訂單')

def _canonical(obj):
 return json.dumps(obj,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode('utf-8')

def _fetch(query, opener=urlopen):
 try:
  url='https://news.google.com/rss/search?q='+quote_plus(query)+'&hl=zh-TW&gl=TW&ceid=TW:zh-Hant'
  req=Request(url,headers={'User-Agent':'StockApp-H150/1.0 (research RSS)'})
  with opener(req,timeout=8) as response:
   if getattr(response,'status',200)!=200:return []
   raw=response.read(400_000)
  root=ET.fromstring(raw)
  rows=[]
  for item in root.findall('.//item')[:15]:
   title=str(item.findtext('title') or '').strip()[:180]
   link=str(item.findtext('link') or '').strip()[:700]
   pub=str(item.findtext('pubDate') or '').strip()
   source=str(item.findtext('source') or '').strip()[:80]
   parsed=urlparse(link)
   if not title or parsed.scheme!='https' or not parsed.hostname:continue
   try:
    dt=parsedate_to_datetime(pub)
    if dt.tzinfo is None:continue
    dt=dt.astimezone(timezone.utc)
   except (ValueError,TypeError):continue
   if datetime.now(timezone.utc)-dt>timedelta(days=7) or dt>datetime.now(timezone.utc)+timedelta(minutes=3):continue
   rows.append({'title':title,'link':link,'published_at':dt.isoformat(),'source':source,'query':query})
  return rows
 except (OSError,ValueError,ET.ParseError,TimeoutError,TypeError):
  return []

def make_cache(items, generated_at=None):
 good=[];seen=set()
 for item in items:
  if not isinstance(item,dict):continue
  title=str(item.get('title') or '')[:180].strip()
  link=str(item.get('link') or '')[:700].strip()
  pub=str(item.get('published_at') or '').strip()
  parsed=urlparse(link)
  if not title or parsed.scheme!='https' or not parsed.hostname or not pub or (title,link) in seen:continue
  try:
   dt=datetime.fromisoformat(pub.replace('Z','+00:00'))
   if dt.tzinfo is None:continue
  except ValueError:continue
  seen.add((title,link))
  good.append({'title':title,'link':link,'published_at':dt.isoformat(),
               'source':str(item.get('source') or '')[:80],'query':str(item.get('query') or '')[:100]})
 good=good[:80]
 stamp=generated_at or datetime.now(timezone.utc)
 data={'schema':'H150-public-news-1','updated_at':stamp.isoformat(),'source':'Google News RSS','items':good}
 data['items_sha256']=hashlib.sha256(_canonical(good)).hexdigest()
 return data

def main(argv=None):
 ap=argparse.ArgumentParser();ap.add_argument('--output',required=True);args=ap.parse_args(argv)
 with ThreadPoolExecutor(max_workers=4) as pool:
  combined=[v for bundle in pool.map(_fetch,QUERIES) for v in bundle]
 cache=make_cache(combined)
 if not cache['items']:
  print('H150 NEWS_SOURCE_UNAVAILABLE, previous cache retained');return 2
 p=Path(args.output);p.parent.mkdir(parents=True,exist_ok=True)
 temp=p.with_suffix('.tmp_h150');temp.write_bytes(_canonical(cache)+b'\n');temp.replace(p)
 print('H150 public news cache rows:',len(cache['items']));return 0

if __name__=='__main__':raise SystemExit(main())
