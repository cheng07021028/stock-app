# -*- coding: utf-8 -*-
"""H185 autonomous, research-only TWSE/TPEx evidence bridge.

The collector may write ONLY data/h185; the Streamlit adapter is read-only.
No brokerage credentials, no historical decision rewrites, no trading authority.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import math
import os
import re
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import pandas as pd

from godpick_h117_official_market_closure import normalize_official, verify_manifest

TZ=ZoneInfo('Asia/Taipei')
VERSION='V191-H185-AUTONOMOUS-OFFICIAL-RESEARCH'
ROOT=Path(__file__).resolve().parent
AUTODIR=ROOT/'data'/'h185'
SOURCES={
 'TWSE_QUOTES':('https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL','TWSE'),
 'TPEX_QUOTES':('https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes','TPEX'),
 'TWSE_EVENTS':('https://openapi.twse.com.tw/v1/opendata/t187ap04_L','TWSE'),
 'TPEX_EVENTS':('https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap04_O','TPEX'),
}
FILE_NAMES={k:k.lower()+'.json' for k in SOURCES}
CODE_RE=re.compile(r'[1-9][0-9]{3}\Z')


def _canonical(obj:Any)->bytes:
 return json.dumps(obj,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode('utf-8')

def _digest(obj:Any)->str:return hashlib.sha256(_canonical(obj)).hexdigest()

def _atomic(path:Path,content:bytes)->None:
 path.parent.mkdir(parents=True,exist_ok=True)
 fd,temp=tempfile.mkstemp(prefix='.'+path.name+'.',dir=str(path.parent))
 try:
  with os.fdopen(fd,'wb') as f:f.write(content);f.flush();os.fsync(f.fileno())
  os.replace(temp,path)
 finally:
  if os.path.exists(temp):os.unlink(temp)

def _field(row:Mapping,*keys:str)->Any:
 for key in keys:
  if key in row and row[key] not in (None,''):return row[key]
 return ''

def _day(v:Any)->str:
 text=str(v or '').strip().replace('/','').replace('-','')
 if text.isdecimal() and len(text) in (7,8):
  year=int(text[:-4]);year=year+1911 if year<1911 else year
  try:return date(year,int(text[-4:-2]),int(text[-2:])).isoformat()
  except ValueError:return ''
 return ''

def normalize_events(raw:Any,*,market:str,observed_at:str)->dict:
 """Use only official exchange rows. Published day is NOT scheduled-event day."""
 if not isinstance(raw,list):raise ValueError('官方重大訊息回應不是陣列')
 now=datetime.fromisoformat(observed_at)
 if not now.tzinfo:raise ValueError('缺來源觀測時區')
 future=now.astimezone(TZ).date().isoformat()
 seen=set();items=[];source=SOURCES[f'{market}_EVENTS'][0]
 for item in raw:
  if not isinstance(item,dict):continue
  code=str(_field(item,'公司代號','SecuritiesCompanyCode','CompanyCode','Code')).strip()
  published=_day(_field(item,'發言日期','AnnouncementDate','Date','發布日期','公告日期'))
  event_day=_day(_field(item,'事實發生日','FactDate','EventDate'))
  title=str(_field(item,'主旨 ','主旨','Subject','Title','公告事項')).strip()
  if not CODE_RE.fullmatch(code) or not published or not title or published>future:continue
  unique=(market,code,published,title)
  if unique in seen:continue
  seen.add(unique)
  items.append({'code':code,'event_date':event_day or published,'published_at':published,
      'event_type':'公司重大訊息','title':title[:350],
      'source_url':source,'source':market,'fact':'官方公司公告主旨：'+title[:350],
      'hypothesis':'市場影響待實際行情及財報驗證','confirmed':False,
      'description_excerpt':str(_field(item,'說明','Description','內容'))[:600],
      'availability_basis':'FIRST_OBSERVED','available_at':observed_at})
 # This is an explicitly capped RECENT EVENT STUDY WINDOW, not a complete archive.
 items.sort(key=lambda e:(e['published_at'],e['code'],e['title']),reverse=True)
 if not items:raise ValueError('無可識別的正式事件／日期／代號，保留舊版快照')
 return {'rows':items[:1500],'asof_day':max(x['published_at'] for x in items),
         'truncated':len(items)>1500,'raw_count':len(raw)}

def normalize_quotes(raw:Any,*,market:str,observed_at:str)->dict:
 result=normalize_official(market,raw,minimum=50)
 observed=datetime.fromisoformat(observed_at).astimezone(TZ).date()
 trading=date.fromisoformat(result['day'])
 if trading>observed:raise ValueError('官方來源交易日未來值')
 if observed-trading>timedelta(days=7):raise ValueError('來源超過七天未更新，拒絕當作最新行情')
 return {'rows':[{'code':r['code'],'date':r['date'],'open':r['open'],'high':r['high'],
                  'low':r['low'],'close':r['close'],'volume':r['volume_shares'],
                  'source':result['source_url'],'exchange':market}
                 for r in result['rows']], 'asof_day':result['day'],
         'truncated':False,'raw_count':len(raw)}

def _verified_payload(value:Any,name:str)->dict|None:
 if not isinstance(value,dict) or value.get('source_id')!=name or value.get('schema')!=VERSION:return None
 rows=value.get('rows')
 if not isinstance(rows,list) or not rows or value.get('rows_sha256')!=_digest(rows):return None
 try:
  when=datetime.fromisoformat(value['observed_at'])
  if not when.tzinfo:return None
 except (KeyError,TypeError,ValueError):return None
 return value

def load_source(name:str,*,root:Path=ROOT)->dict|None:
 if name not in SOURCES:return None
 p=Path(root)/'data'/'h185'/FILE_NAMES[name]
 try:
  if p.stat().st_size>5_000_000:return None
  return _verified_payload(json.loads(p.read_text('utf-8-sig')),name)
 except (OSError,ValueError,UnicodeError,TypeError):return None

def fetch_official(*,root:Path=ROOT,http:Any=None,now:datetime|None=None,source_names:tuple[str,...]|None=None,
                   timeout:int=18)->dict:
 """Bounded requests; save independently; never overwrite on a source error."""
 if http is None:
  import requests
  http=requests.Session()
 now=now or datetime.now(TZ)
 if not now.tzinfo:raise ValueError('now requires timezone')
 stamp=now.astimezone(TZ).isoformat(timespec='seconds')
 ids=tuple(source_names) if source_names is not None else tuple(SOURCES)
 report={'version':VERSION,'checked_at':stamp,'sources':{},'success':0,'failures':0}
 for name in ids:
  if name not in SOURCES:raise ValueError('unknown source')
  url,market=SOURCES[name]; old=load_source(name,root=root)
  path=Path(root)/'data'/'h185'/FILE_NAMES[name]
  try:
   if path.exists() and old is None:
    raise RuntimeError('現有研究來源快照驗證失敗或版本未知，不允許覆蓋')
   response=http.get(url,headers={'Accept':'application/json','User-Agent':'SPT-StockApp-H185/1.0 (official research only)'},timeout=timeout)
   if response.status_code!=200:raise RuntimeError('HTTP '+str(response.status_code))
   raw=response.json()
   obj=normalize_events(raw,market=market,observed_at=stamp) if name.endswith('EVENTS') else normalize_quotes(raw,market=market,observed_at=stamp)
   if name.endswith('EVENTS') and old:
    # Maintain first-observed time for unchanged historic announcements. A rolling
    # OpenAPI window must never re-date an older fact to today's collection time.
    def identity(row):return (row['source'],row['code'],row['published_at'],row['title'])
    merged={identity(row):row for row in old['rows']}
    for row in obj['rows']:
     key=identity(row)
     if key not in merged:merged[key]=row
     # Same key but modified text: retain previous observation, never rewrite it.
    combined=sorted(merged.values(),key=lambda x:(x['published_at'],x['code'],x['title']),reverse=True)
    obj['truncated']=obj['truncated'] or len(combined)>1500
    obj['rows']=combined[:1500]
    obj['asof_day']=max(x['published_at'] for x in obj['rows'])
   payload={'schema':VERSION,'source_id':name,'source_url':url,'observed_at':stamp,
            'availability_basis':'FIRST_OBSERVED','asof_day':obj['asof_day'],
            'raw_count':obj['raw_count'],'truncated':obj['truncated'],
            'rows':obj['rows'],'rows_sha256':_digest(obj['rows'])}
   if old and old['asof_day']>payload['asof_day']:
    raise RuntimeError('官方資料日期倒退，拒絕覆蓋已保存快照')
   # Different same-day rows could be a correction. Keep old facts and flag.
   if old and name.endswith('QUOTES') and old['asof_day']==payload['asof_day'] and old['rows_sha256']!=payload['rows_sha256']:
    raise RuntimeError('同日行情內容變動，需確認修訂版本；不靜默覆蓋')
   if not old or old['rows_sha256']!=payload['rows_sha256']:
    _atomic(Path(root)/'data'/'h185'/FILE_NAMES[name],_canonical(payload)+b'\n')
    saved=True
   else:saved=False
   report['sources'][name]={'status':'OK' if not obj['truncated'] else 'PARTIAL_TRUNCATED',
      'asof_day':obj['asof_day'],'row_count':len(obj['rows']),'saved':saved,'stale':False}
   report['success']+=1
  except Exception as exc:
   report['failures']+=1
   report['sources'][name]={'status':'FAILED_RETAIN_PREVIOUS','error':f'{type(exc).__name__}: {exc}'[:180],
      'previous_asof':old.get('asof_day') if old else None,'stale':True}
 report['status']='ALL_OK' if report['failures']==0 else ('PARTIAL' if report['success'] else 'ALL_FAILED')
 # Health is operational telemetry, not evidence; always separate from official datasets.
 _atomic(Path(root)/'data'/'h185'/'health.json',_canonical(report)+b'\n')
 return report

def _jsonl(path:Path,max_bytes:int=16000000)->list[dict]:
 try:
  if path.stat().st_size>max_bytes:return []
  return [x for line in path.read_text('utf-8-sig').splitlines() if line.strip()
          if isinstance((x:=json.loads(line)),dict)]
 except (OSError,ValueError,UnicodeError,TypeError):return []

def _date_ok(observed:Any,asof:str)->bool:
 """Date-only research decisions must not use same-day first-observed data."""
 try:
  got=datetime.fromisoformat(str(observed))
  if not got.tzinfo:return False
  taipei=got.astimezone(TZ)
  # Live research now knows the observation time; historical date-only decisions don't.
  if taipei.date().isoformat()<asof:return True
  current=datetime.now(TZ)
  return asof==current.date().isoformat() and taipei<=current
 except (ValueError,TypeError):return False

def _empty(name:str)->pd.DataFrame:
 from godpick_h184_research_lab import SHEETS
 return pd.DataFrame(columns=SHEETS.get(name,()))

def _h117_history(code:str,asof:str,*,root:Path,limit:int=220)->list[dict]:
 """Only hash-verified immutable H117 daily files. No market-day interpolation."""
 folder=Path(root)/'data'/'h117'/'daily'
 bars=[]
 for market in ('TWSE','TPEX'):
  paths=sorted((folder/market).glob('*.json.gz'))[-limit:]
  for file in paths:
   if file.stem.replace('.json','')>asof:continue
   try:
    if file.stat().st_size>8_000_000:continue
    with gzip.open(file,'rt',encoding='utf-8') as f:manifest=json.load(f)
    if not verify_manifest(manifest) or manifest.get('date')+'.json.gz'!=file.name or manifest.get('source')!=market:continue
    for r in manifest['rows']:
     if r.get('code')==code:
      bars.append({'date':r['date'],'open':r['open'],'high':r['high'],'low':r['low'],
                   'close':r['close'],'volume':r['volume_shares'],'code':code,
                   'source':str(manifest.get('source_url') or '')});break
   except (OSError,ValueError,TypeError,KeyError,EOFError):continue
 return bars

def load_auto_datasets(*,code:str='',asof:str='',root:Path=ROOT,include_h117:bool=True)->tuple[dict[str,pd.DataFrame],dict]:
 """Read-only automatic dataset assembly from approved official H115/H117/H169/H170/H185 caches.

    Do not translate YTD EPS to TTM, or personal recommendation records to trades.
 """
 today=datetime.now(TZ).date().isoformat();asof=asof or today
 if not re.fullmatch(r'\d{4}-\d{2}-\d{2}',asof):raise ValueError('Invalid asof')
 code=str(code).strip().removesuffix('.0')
 if code and not CODE_RE.fullmatch(code):raise ValueError('股票代號需為四位普通股代號')
 out={};status={'version':VERSION,'asof':asof,'code':code,'sources':{},'gaps':[],'trade_authority_changed':False}
 quote_rows=[];event_rows=[];evidence=[]
 for key in SOURCES:
  snap=load_source(key,root=root)
  if not snap:
   status['sources'][key]={'status':'NOT_COLLECTED_OR_INVALID','row_count':0};continue
  event_type=key.endswith('EVENTS')
  check=event_type or _date_ok(snap['observed_at'],asof)
  if not check:
   status['sources'][key]={'status':'AFTER_RESEARCH_ASOF','asof_day':snap['asof_day'],'row_count':0};continue
  age=(date.fromisoformat(asof)-date.fromisoformat(snap['asof_day'])).days
  if age<0: state=('PIT_PER_RECORD_OLDER_EVENTS' if event_type else 'FUTURE_SOURCE_DAY')
  elif age>(6 if key.endswith('QUOTES') else 14):state='STALE_RESEARCH_REFERENCE'
  elif snap.get('truncated'):state='PARTIAL_TRUNCATED'
  else:state='FIRST_OBSERVED'
  status['sources'][key]={'status':state,'asof_day':snap['asof_day'],'row_count':len(snap['rows']),
                          'available_at':snap['observed_at'],'url':snap['source_url']}
  if age<0 and not event_type:continue
  subset=snap['rows'];subset=[r for r in subset if not code or r.get('code')==code]
  if key.endswith('QUOTES'):quote_rows.extend(subset)
  else:event_rows.extend(r for r in subset if r['published_at']<=asof and _date_ok(r.get('available_at'),asof))
 if quote_rows:
  latest={}
  for r in quote_rows:
   k=r['code']
   if k not in latest or r['date']>latest[k]['date']:latest[k]=r
  quotes=list(latest.values())
  out['quotes']=pd.DataFrame([{'code':r['code'],'date':r['date'],'close':r['close'],'source':r['source']} for r in quotes])
  if code:out['ohlcv']=pd.DataFrame(quote_rows).drop(columns=['exchange'],errors='ignore')
 if event_rows:out['events']=pd.DataFrame(event_rows)
 news=load_h150_news(root=root,asof=asof,code=code)
 if not news.empty:out['news_events']=news
 if code and include_h117:
  historic=_h117_history(code,asof,root=root)
  if historic:
   f=pd.DataFrame(historic+quote_rows)
   f=f.drop_duplicates(['date','code'],keep='first').sort_values('date')
   out['ohlcv']=f.reset_index(drop=True)
   status['sources']['H117_IMMUTABLE_OHLCV']={'status':'VERIFIED_ONLY','row_count':len(historic),'asof_day':f['date'].max()}
 financial=[]
 rev=_jsonl(Path(root)/'data'/'h115'/'revenue.jsonl')
 margins=_jsonl(Path(root)/'data'/'h115'/'quarterly_margins.jsonl')
 for r in margins:
  if code and str(r.get('code'))!=code:continue
  if r.get('statement_basis')!='SINGLE_QUARTER' or not _date_ok(r.get('available_at'),asof):continue
  financial.append({'code':str(r['code']),'fiscal_period':str(r.get('year_quarter') or ''),
          'disclosed_at':str(r['available_at'])[:10],'revenue':None,
          'gross_margin_pct':r.get('gross_margin_pct'),
          'operating_margin_pct':r.get('operating_margin_pct'),'net_margin_pct':r.get('net_margin_pct'),
          'net_income':None,'operating_cashflow':None,'debt':None,'shares':None,
          'source_url':r.get('source_url'),'unit':'利潤率 %／單季；無本季營收金額',
          'availability_basis':'FIRST_OBSERVED'})
 for r in rev:
  if code and str(r.get('code'))!=code:continue
  if not _date_ok(r.get('available_at'),asof):continue
  evidence.append({'code':r['code'],'metric':'monthly_revenue_twd','value':r.get('revenue_twd'),
      'unit':'TWD','period':r.get('year_month'),'asof_date':str(r['available_at'])[:10],
      'source_url':r.get('source_url'),'quote':'官方每月營收 FIRST_OBSERVED','claim':'月營收公布值，非季報營收'})
 for r in financial:
  evidence.append({'code':r['code'],'metric':'gross_margin_pct','value':r.get('gross_margin_pct'),
      'unit':'%','period':r['fiscal_period'],'asof_date':r['disclosed_at'],
      'source_url':r['source_url'],'quote':'單季毛利率首次觀測快照','claim':'單季毛利率觀測，非年度比率'})
 if financial:out['financial']=pd.DataFrame(financial)
 if evidence:out['evidence']=pd.DataFrame(evidence)
 # Evidence coverage is NOT the same as a successful API response.
 rev_asof=[r for r in rev if _date_ok(r.get('available_at'),asof) and
           re.fullmatch(r'\d{6}',str(r.get('year_month') or ''))]
 margin_asof=[r for r in margins if _date_ok(r.get('available_at'),asof) and
              re.fullmatch(r'\d{4}Q[1-4]',str(r.get('year_quarter') or ''))]
 periods={str(r['year_month']) for r in rev_asof}
 latest_rev=max(periods,default='')
 ref_day=date.fromisoformat(asof)
 previous_month=(ref_day.year*12+ref_day.month-2)
 target_year=previous_month//12
 target_month=previous_month%12+1
 expected_month=f'{target_year:04d}{target_month:02d}'
 if ref_day.day<12:
  old_month=previous_month-1
  expected_month=f'{old_month//12:04d}{old_month%12+1:02d}'
 period_codes={p:{str(r.get('code')) for r in rev_asof if r.get('year_month')==p} for p in periods}
 coverage=len(period_codes.get(latest_rev,set()))
 prior=sorted(periods)[-2] if len(periods)>=2 else ''
 prior_count=len(period_codes.get(prior,set()))
 partial=bool(prior_count>=20 and coverage<prior_count*.5)
 rev_status=('NO_ARCHIVE' if not rev_asof else
             'PERIOD_LAG' if latest_rev<expected_month else
             'PARTIAL_COVERAGE' if partial else 'FIRST_OBSERVED')
 status['sources']['H115_MONTHLY_REVENUE']={'status':rev_status,'row_count':len(rev_asof),
   'latest_period':latest_rev,'expected_period':expected_month,'latest_period_companies':coverage}
 margin_period=max((str(r['year_quarter']) for r in margin_asof),default='')
 status['sources']['H115_SINGLE_QUARTER_MARGINS']={'status':'FIRST_OBSERVED_GENERAL_INDUSTRY_ONLY' if margin_asof else 'NO_ARCHIVE',
      'row_count':len(margin_asof),'latest_period':margin_period}
 if rev_status in ('PERIOD_LAG','PARTIAL_COVERAGE'):
  status['gaps'].append('官方月營收快照尚未涵蓋應有月份或涵蓋率不足，不可宣稱全市場資料最新')
 # PE and EPS are separate datasets. Never derive independent TTM EPS from PE or YTD EPS.
 pe_path=Path(root)/'data'/'h115'/'pe_only_latest.json'
 eps_path=Path(root)/'data'/'h115'/'eps_ytd_latest.json'
 for key,path,field in [('H169_OFFICIAL_PE',pe_path,'pe_only'),('H170_EPS_YTD',eps_path,'eps_ytd_rows')]:
  try:
   raw=json.loads(path.read_text('utf-8-sig'))
   rows=[r for r in raw.get(field,[]) if isinstance(r,dict) and (not code or r.get('code')==code) and _date_ok(r.get('available_at'),asof)]
   if rows:out['pe_observations' if field=='pe_only' else 'eps_ytd_observations']=pd.DataFrame(rows)
   status['sources'][key]={'status':'FIRST_OBSERVED' if rows else 'NO_ASOF_EVIDENCE','row_count':len(rows),
                           'ttm_eps_certified':False}
  except (OSError,ValueError,TypeError):status['sources'][key]={'status':'NOT_COLLECTED_OR_INVALID','row_count':0}
 for key in ('portfolio','trades'):
  # Never pass recommended stocks off as actual holdings or orders.
  status['gaps'].append(('真實持股與現金配置需券商或已授權持倉資料源' if key=='portfolio'
                       else '真實成交日誌需券商成交回報／已授權帳戶來源'))
 status['gaps'].append('DCF三情境的折現率／資本支出／股數須來自可追溯財報與明示假設，不會自動補假數字')
 if 'ohlcv' not in out or len(out['ohlcv'])<80:
  status['gaps'].append('已保存的官方日K不足80根；歷史回測／MA60需等待H117累積或補官方歷史行情')
 return out,status

__all__=['VERSION','ROOT','SOURCES','normalize_events','normalize_quotes','fetch_official',
         'load_source','load_auto_datasets']

# Historical EOD bars for research at page view. Only a selected code is fetched;
# never issue 2,000 * 9 per-symbol requests at every Streamlit rerun.
HISTORY_URLS={
 'TWSE':'https://www.twse.com.tw/exchangeReport/STOCK_DAY',
 'TPEX':'https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock',
}


def _price(v:Any)->float|None:
 try:
  f=float(str(v).strip().replace(',',''))
  return f if math.isfinite(f) and f>0 else None
 except (TypeError,ValueError):return None


def parse_month_history(raw:Any,*,market:str,code:str,asof:str)->list[dict]:
 """Official monthly per-symbol history, named-column parsing (never positional guesses)."""
 if market not in HISTORY_URLS or not CODE_RE.fullmatch(code):raise ValueError('invalid history request')
 if not isinstance(raw,dict):raise ValueError('unexpected history response')
 tables=raw.get('tables')
 obj=(tables[0] if isinstance(tables,list) and tables else raw)
 if not isinstance(obj,dict):raise ValueError('missing history table')
 fields=[re.sub(r'\s+','',re.sub(r'<[^>]*>','',str(x))) for x in obj.get('fields',[])]
 rows=obj.get('data',[])
 if not isinstance(rows,list):raise ValueError('history rows not list')
 required={'date':('日期',),'volume':('成交股數','成交仟股'),
           'open':('開盤價','開盤'),'high':('最高價','最高'),
           'low':('最低價','最低'),'close':('收盤價','收盤')}
 mapping={}
 for k,aliases in required.items():
  idx=next((fields.index(a) for a in aliases if a in fields),None)
  if idx is None:raise ValueError('official history missing column '+k)
  mapping[k]=idx
 items=[];seen=set()
 for arr in rows:
  if not isinstance(arr,list) or len(arr)<=max(mapping.values()):continue
  d=_day(arr[mapping['date']])
  if not d or d>asof:continue
  values={k:_price(arr[i]) for k,i in mapping.items() if k!='date'}
  if any(v is None for v in values.values()):continue
  o,h,l,c=(values[k] for k in ('open','high','low','close'))
  if h<max(o,l,c) or l>min(o,h,c):continue
  if d in seen:raise ValueError('duplicate official history date')
  seen.add(d)
  volume=values['volume']*1000 if market=='TPEX' and '成交仟股' in fields else values['volume']
  if volume<=0:continue
  items.append({'code':code,'date':d,'open':o,'high':h,'low':l,'close':c,
                'volume':volume,'source':HISTORY_URLS[market]})
 return items


def market_for_code(code:str,*,root:Path=ROOT)->str:
 markets=[m for m in ('TWSE','TPEX')
          if (r:=load_source(m+'_QUOTES',root=root)) and any(x.get('code')==code for x in r['rows'])]
 return markets[0] if len(markets)==1 else ''


def fetch_selected_history(code:str,*,asof:str,root:Path=ROOT,http:Any=None,months:int=9,timeout:int=8)->tuple[pd.DataFrame,dict]:
 """Per-symbol official public history. Non-PIT backfill, not execution evidence."""
 code=str(code or '').strip()
 if not CODE_RE.fullmatch(code):return _empty('ohlcv'),{'status':'INVALID_CODE','requests':0}
 market=market_for_code(code,root=root)
 if not market:return _empty('ohlcv'),{'status':'MARKET_UNVERIFIED','requests':0}
 if http is None:
  import requests
  http=requests.Session()
 try:
  end=date.fromisoformat(asof)
  if end>datetime.now(TZ).date():return _empty('ohlcv'),{'status':'FUTURE_ASOF','requests':0}
 except ValueError:return _empty('ohlcv'),{'status':'INVALID_ASOF','requests':0}
 months=max(1,min(10,months));params=[]
 ym=end.year*12+end.month-1
 for i in range(months-1,-1,-1):
  idx=ym-i;y=(idx//12);m=idx%12+1
  params.append({'response':'json','date':f'{y:04d}{m:02d}01','stockNo':code}
                if market=='TWSE' else {'response':'json','date':f'{y:04d}/{m:02d}/01','code':code})
 import concurrent.futures
 def get_one(param):
  try:
   response=http.get(HISTORY_URLS[market],params=param,timeout=timeout,
       headers={'Accept':'application/json','User-Agent':'SPT-StockApp-H185/1.0 historical research'})
   if response.status_code!=200:return [],'HTTP '+str(response.status_code)
   body=response.json()
   if isinstance(body,dict) and str(body.get('stat','')).lower() not in ('','ok'):
    return [],'upstream '+str(body['stat'])[:50]
   return parse_month_history(body,market=market,code=code,asof=asof),''
  except Exception as exc:return [],type(exc).__name__+': '+str(exc)[:80]
 bars=[];errors=[]
 with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
  for arr,err in pool.map(get_one,params):
   bars.extend(arr)
   if err:errors.append(err)
 dates=[r['date'] for r in bars]
 if len(dates)!=len(set(dates)):
  return _empty('ohlcv'),{'status':'DUPLICATED_DAILY_BAR','requests':len(params)}
 frame=pd.DataFrame(sorted(bars,key=lambda r:r['date'])) if bars else _empty('ohlcv')
 status='HISTORY_OFFICIAL_BACKFILL_UNADJUSTED' if not errors and len(frame)>=60 else (
         'PARTIAL_HISTORY' if len(frame) else 'SOURCE_UNAVAILABLE')
 return frame,{'status':status,'market':market,'requests':len(params),'bars':len(frame),
               'errors':errors[:4], 'corporate_actions':'UNADJUSTED; backtest return requires ex-rights/stock-split review',
               'historical_PIT_certification':False}

__all__.extend(['HISTORY_URLS','parse_month_history','market_for_code','fetch_selected_history'])


def adopt_h150_news(source:Path,*,root:Path=ROOT,now:datetime|None=None)->dict:
 """Copy only checksum-verified public headline cache from existing H150 automation.

    Never treat a news headline as an exchange-verified material event.
 """
 now=now or datetime.now(TZ)
 try:
  if source.stat().st_size>500000:raise ValueError('headline source exceeds bound')
  value=json.loads(source.read_text('utf-8-sig'))
  items=value.get('items',[])
  if not isinstance(items,list) or not items or _digest(items)!=value.get('items_sha256'):
   raise ValueError('H150 headline checksum invalid')
  generated=datetime.fromisoformat(value['updated_at'])
  if not generated.tzinfo or now-generated>timedelta(days=7) or generated>now+timedelta(minutes=5):
   raise ValueError('H150 cache stale, future, or timezone missing')
  from urllib.parse import urlsplit
  sanitized=[]
  for v in items[:100]:
   if not isinstance(v,dict):continue
   url=str(v.get('link') or '')
   parts=urlsplit(url)
   if parts.scheme!='https' or not parts.netloc:continue
   at=str(v.get('published_at') or '')
   try:
    day=datetime.fromisoformat(at)
    if not day.tzinfo or day>now+timedelta(minutes=5):continue
   except ValueError:continue
   title=str(v.get('title') or '').strip()
   if not title:continue
   sanitized.append({'title':title[:180],'link':url[:700],
      'published_at':at,'source':str(v.get('source') or 'Google News RSS')[:80],
      'query':str(v.get('query') or '')[:80]})
  if not sanitized:raise ValueError('No usable H150 headlines')
  dest=Path(root)/'data'/'h185'/'h150_public_news.json'
  payload={'schema':VERSION,'source_id':'H150_PUBLIC_RSS','updated_at':value['updated_at'],
           'availability_basis':'PUBLISHED_HEADLINE_NOT_ARTICLE_VERIFIED',
           'items':sanitized,'items_sha256':_digest(sanitized)}
  if not dest.is_file() or dest.read_bytes()!=_canonical(payload)+b'\n':
   _atomic(dest,_canonical(payload)+b'\n')
  return {'status':'HEADLINES_ONLY','row_count':len(sanitized),'updated_at':value['updated_at']}
 except (OSError,ValueError,TypeError,KeyError) as exc:
  return {'status':'NO_VALID_H150_NEWS','reason':str(exc)[:160]}


def load_h150_news(*,root:Path=ROOT,asof:str='',code:str='')->pd.DataFrame:
 """Nonofficial general-market headlines: NEVER assign an unverified stock code."""
 p=Path(root)/'data'/'h185'/'h150_public_news.json'
 try:
  if p.stat().st_size>500000:return pd.DataFrame()
  value=json.loads(p.read_text('utf-8-sig'))
  if (value.get('schema')!=VERSION or value.get('source_id')!='H150_PUBLIC_RSS' or
      not isinstance(value.get('items'),list) or _digest(value['items'])!=value.get('items_sha256')):
   return pd.DataFrame()
  if not _date_ok(value.get('updated_at'),asof):return pd.DataFrame()
  out=[]
  for r in value['items']:
   dt=datetime.fromisoformat(r['published_at'])
   if not dt.tzinfo or dt.astimezone(TZ).date().isoformat()>asof:continue
   # General headline cannot be attributed to a selected company by keyword guess.
   if code:continue
   out.append({'code':'','event_date':dt.astimezone(TZ).date().isoformat(),
       'published_at':r['published_at'],'event_type':'新聞標題（未核原文）','title':r['title'],
       'source_url':r['link'],'fact':'RSS出現此標題；原文仍須核實：'+r['title'],
       'hypothesis':'是否影響標的待事件與價格證據交叉查證', 'confirmed':False})
  return pd.DataFrame(out)
 except (OSError,ValueError,UnicodeError,KeyError,TypeError):return pd.DataFrame()

__all__.extend(['adopt_h150_news','load_h150_news'])
