# -*- coding: utf-8 -*-
from __future__ import annotations
import ast,gzip,hashlib,io,json,sys
from datetime import datetime
from pathlib import Path
import pandas as pd
import pytest
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from godpick_h185_auto_sources import (_canonical,_digest,_day,VERSION,SOURCES,fetch_official,normalize_events,
    normalize_quotes,load_source,load_auto_datasets,parse_month_history,fetch_selected_history,
    _date_ok,market_for_code,FILE_NAMES)
from godpick_h117_official_market_closure import make_official_day_manifest

NOW=datetime.fromisoformat('2026-11-23T21:30:00+08:00')

def twse_rows(n=60,day='1151120'):
 return [{'Date':day,'Code':str(3000+i),'OpeningPrice':'30','HighestPrice':'33','LowestPrice':'29',
          'ClosingPrice':'32','TradeVolume':'100000','TradeValue':'3200000'} for i in range(n)]

def tpex_rows(n=60,day='1151120'):
 return [{'Date':day,'SecuritiesCompanyCode':str(6000+i),'Open':'30','High':'33','Low':'29',
          'Close':'32','TradingShares':'150000','TransactionAmount':'3200000'} for i in range(n)]

class Reply:
 def __init__(self,data,status_code=200):self.data=data;self.status_code=status_code
 def json(self):return self.data

class HTTP:
 def __init__(self,data):self.data=data;self.calls=[]
 def get(self,url,**kwargs):
  self.calls.append((url,kwargs))
  return self.data[url]

def _save(root,name,rows,at='2026-11-20T21:15:00+08:00',day='2026-11-20'):
 data=dict(schema=VERSION,source_id=name,source_url=SOURCES[name][0],observed_at=at,
           availability_basis='FIRST_OBSERVED',asof_day=day,raw_count=len(rows),
           rows=rows,rows_sha256=_digest(rows),truncated=False)
 p=root/'data'/'h185'/FILE_NAMES[name];p.parent.mkdir(parents=True,exist_ok=True)
 p.write_bytes(_canonical(data));return p

def test_dates_roc_and_iso():
 assert _day('1151120')=='2026-11-20'
 assert _day('115/11/23')=='2026-11-23'
 assert _day('2026-11-20')=='2026-11-20'
 assert not _day('1150231')

def test_twse_normalized_volume_shares():
 q=normalize_quotes(twse_rows(),market='TWSE',observed_at=NOW.isoformat())
 assert len(q['rows'])==60 and q['rows'][0]['volume']==100000 and q['asof_day']=='2026-11-20'

def test_tpex_normalized_volume_shares():
 q=normalize_quotes(tpex_rows(),market='TPEX',observed_at=NOW.isoformat())
 assert len(q['rows'])==60 and q['rows'][0]['volume']==150000

def test_quotes_reject_future_date():
 with pytest.raises(ValueError):normalize_quotes(twse_rows(day='1151124'),market='TWSE',observed_at=NOW.isoformat())

def test_quotes_reject_stale_date():
 with pytest.raises(ValueError):normalize_quotes(twse_rows(day='1151001'),market='TWSE',observed_at=NOW.isoformat())

def test_quotes_reject_invalid_volume():
 q=twse_rows();[x.update({'TradeVolume':'0'}) for x in q]
 with pytest.raises(ValueError):normalize_quotes(q,market='TWSE',observed_at=NOW.isoformat())

def test_event_official_dates_and_forward_schedule():
 rows=[{'公司代號':'3000','發言日期':'1151122','事實發生日':'1151124',
        '主旨 ':'法說會預定舉辦','說明':'不是已發生的財報'}]
 obj=normalize_events(rows,market='TWSE',observed_at=NOW.isoformat())
 assert obj['rows'][0]['published_at']=='2026-11-22'
 assert obj['rows'][0]['event_date']=='2026-11-24'
 assert obj['rows'][0]['confirmed'] is False

def test_event_requires_published_date_and_code():
 with pytest.raises(ValueError):normalize_events([{'公司代號':'3000','主旨 ':'公告'}],market='TWSE',observed_at=NOW.isoformat())

def test_event_rejects_future_publication():
 with pytest.raises(ValueError):normalize_events([{'公司代號':'3000','發言日期':'1151124','主旨':'未來公告'}],market='TWSE',observed_at=NOW.isoformat())

def test_partial_source_retains_existing(tmp_path):
 events=[{'公司代號':'3000','發言日期':'1151120','主旨':'已觀測公告'}]
 old=normalize_events(events,market='TWSE',observed_at=NOW.isoformat())
 prev=_save(tmp_path,'TWSE_EVENTS',old['rows'])
 data={url:Reply(twse_rows()) for name,(url,market) in SOURCES.items() if name=='TWSE_QUOTES'}
 for name,(url,market) in SOURCES.items():
  if name=='TWSE_EVENTS':data[url]=Reply(events)
  if name=='TPEX_QUOTES':data[url]=Reply([],403)
  if name=='TPEX_EVENTS':data[url]=Reply([],403)
 before=prev.read_bytes()
 report=fetch_official(root=tmp_path,http=HTTP(data),now=NOW)
 assert report['success']==2 and report['failures']==2 and report['status']=='PARTIAL'
 assert prev.read_bytes()==before
 assert load_source('TWSE_QUOTES',root=tmp_path)['asof_day']=='2026-11-20'
 assert not load_source('TPEX_QUOTES',root=tmp_path)

def test_same_day_snapshot_correction_blocked(tmp_path):
 _save(tmp_path,'TWSE_QUOTES',normalize_quotes(twse_rows(),market='TWSE',observed_at=NOW.isoformat())['rows'])
 before=(tmp_path/'data/h185/twse_quotes.json').read_bytes()
 changed=twse_rows();changed[0]['ClosingPrice']='31.5'
 http=HTTP({SOURCES['TWSE_QUOTES'][0]:Reply(changed)})
 result=fetch_official(root=tmp_path,http=http,now=NOW,source_names=('TWSE_QUOTES',))
 assert result['status']=='ALL_FAILED'
 assert '同日行情內容變動' in result['sources']['TWSE_QUOTES']['error']
 assert (tmp_path/'data/h185/twse_quotes.json').read_bytes()==before

def test_corrupt_existing_snapshot_never_overwritten(tmp_path):
 p=tmp_path/'data/h185/twse_quotes.json';p.parent.mkdir(parents=True)
 p.write_text('{"schema":"future"}',encoding='utf-8')
 result=fetch_official(root=tmp_path,http=HTTP({SOURCES['TWSE_QUOTES'][0]:Reply(twse_rows())}),
        now=NOW,source_names=('TWSE_QUOTES',))
 assert result['status']=='ALL_FAILED'
 assert p.read_text('utf-8')=='{"schema":"future"}'

def test_failed_previous_snapshot_not_erased(tmp_path):
 prev=_save(tmp_path,'TPEX_QUOTES',normalize_quotes(tpex_rows(),market='TPEX',observed_at=NOW.isoformat())['rows'])
 before=prev.read_bytes()
 report=fetch_official(root=tmp_path,http=HTTP({SOURCES['TPEX_QUOTES'][0]:Reply(None,403)}),
         now=NOW,source_names=('TPEX_QUOTES',))
 assert report['failures']==1 and prev.read_bytes()==before

def test_readonly_asof_rejects_future(tmp_path):
 _save(tmp_path,'TWSE_QUOTES',normalize_quotes(twse_rows(),market='TWSE',observed_at=NOW.isoformat())['rows'])
 data,health=load_auto_datasets(code='3000',asof='2026-11-19',root=tmp_path,include_h117=False)
 assert 'quotes' not in data
 assert health['sources']['TWSE_QUOTES']['status']=='AFTER_RESEARCH_ASOF'

def test_asof_same_day_historical_unverified():
 assert not _date_ok('2026-11-22T20:01:00+08:00','2026-11-22')
 assert _date_ok('2026-11-20T20:01:00+08:00','2026-11-23')

def test_real_h115_reuse_and_exposure(tmp_path):
 data_dir=tmp_path/'data/h115';data_dir.mkdir(parents=True)
 with (data_dir/'quarterly_margins.jsonl').open('w') as f:
  f.write(json.dumps({'code':'3000','statement_basis':'SINGLE_QUARTER','year_quarter':'2026Q3',
      'available_at':'2026-11-14T12:00:00+08:00','gross_margin_pct':22,'operating_margin_pct':10,
      'source_url':'https://openapi.twse.com.tw/v1/opendata/t187ap17_L'})+'\n')
 with (data_dir/'revenue.jsonl').open('w') as f:
  f.write(json.dumps({'code':'3000','revenue_twd':10000000,'year_month':'202610',
    'source_url':'https://openapi.twse.com.tw/v1/opendata/t187ap05_L','available_at':'2026-11-15T01:00:00+08:00'})+'\n')
 data,health=load_auto_datasets(code='3000',asof='2026-11-20',root=tmp_path,include_h117=False)
 assert len(data['financial'])==1 and data['financial'].iloc[0]['operating_margin_pct']==10
 assert len(data['evidence'])==2
 assert data['financial'].iloc[0]['revenue'] is None
 assert not any(k in data for k in ('portfolio','trades'))
 assert len(health['gaps'])>=2

def test_h117_daily_sha_verified_only(tmp_path):
 root=tmp_path/'data/h117/daily/TWSE';root.mkdir(parents=True)
 frame={'source':'TWSE','day':'2026-11-19','source_url':'https://official.example',
        'rows':[{'code':'3000','date':'2026-11-19','open':10,'high':12,'low':9,'close':11,'volume_shares':1000}]}
 stamp='2026-11-19T22:00:00+08:00'
 obj=make_official_day_manifest(frame,collected_at=stamp)
 with gzip.open(root/'2026-11-19.json.gz','wt') as f:json.dump(obj,f)
 data,_=load_auto_datasets(code='3000',asof='2026-11-20',root=tmp_path)
 assert len(data['ohlcv'])==1
 # Tamper manifest: do not silently load a fake daily bar.
 obj['rows'][0]['close']=999
 with gzip.open(root/'2026-11-19.json.gz','wt') as f:json.dump(obj,f)
 data,_=load_auto_datasets(code='3000',asof='2026-11-20',root=tmp_path)
 assert 'ohlcv' not in data

def test_parse_twse_month_named_columns():
 x={'stat':'OK','fields':['日期','成交股數','成交金額','開盤價','最高價','最低價','收盤價'],
     'data':[['115/11/20','123,000','200','31','33','30','32'],['115/11/21','0','0','--','--','--','--']]}
 rows=parse_month_history(x,market='TWSE',code='3000',asof='2026-11-20')
 assert len(rows)==1 and rows[0]['volume']==123000

def test_parse_tpex_month_volume_is_thousands():
 x={'tables':[{'fields':['日 期','成交仟股','成交仟元','開盤','最高','最低','收盤'],
      'data':[['115/11/20','123','200','31','33','30','32']]}]}
 rows=parse_month_history(x,market='TPEX',code='6000',asof='2026-11-20')
 assert len(rows)==1 and rows[0]['volume']==123000

def test_month_history_missing_named_columns_fail_closed():
 with pytest.raises(ValueError):parse_month_history({'fields':['日期'],'data':[]},market='TWSE',code='3000',asof='2026-11-20')

def test_month_history_future_price_filtered():
 x={'fields':['日期','成交股數','開盤價','最高價','最低價','收盤價'],
    'data':[['115/11/24','123000','31','33','30','32']]}
 assert parse_month_history(x,market='TWSE',code='3000',asof='2026-11-20')==[]

def test_market_identification_doesnt_guess(tmp_path):
 q=normalize_quotes(twse_rows(),market='TWSE',observed_at=NOW.isoformat())['rows']
 _save(tmp_path,'TWSE_QUOTES',q)
 assert market_for_code('3000',root=tmp_path)=='TWSE'
 assert market_for_code('9000',root=tmp_path)==''

def test_fetch_selected_history_uses_fixed_domain_and_symbol(tmp_path):
 q=normalize_quotes(twse_rows(),market='TWSE',observed_at=NOW.isoformat())['rows']
 _save(tmp_path,'TWSE_QUOTES',q)
 class H:
  def __init__(self):self.urls=[]
  def get(self,url,**kw):
   self.urls.append((url,kw['params']));
   return Reply({'stat':'OK','fields':['日期','成交股數','開盤價','最高價','最低價','收盤價'],
      'data':[['115/11/20','10000','31','33','30','32']]})
 h=H()
 f,health=fetch_selected_history('3000',asof='2026-11-20',root=tmp_path,http=h,months=2)
 # Duplicate official dates across 2 months must be rejected, not silently counted twice.
 assert health['status']=='DUPLICATED_DAILY_BAR'
 assert len(h.urls)==2 and all(url.startswith('https://www.twse.com.tw/') for url,_ in h.urls)

def test_symbol_history_refuses_unknown_market(tmp_path):
 f,h=fetch_selected_history('9000',asof='2026-11-20',root=tmp_path)
 assert f.empty and h['status']=='MARKET_UNVERIFIED'

def test_all_twelve_roles_still_present_and_permissions_untouched():
 from godpick_h184_research_lab import ROLES
 assert len(ROLES)==12
 for filename in ('godpick_h185_auto_sources.py','tools/h185_auto_collect.py','pages/28_十二研究員決策研究中心.py'):
  s=(ROOT/filename).read_text('utf-8');ast.parse(s)
  assert 'godpick_records.json' not in s
 assert 'require_login()' in (ROOT/'pages/28_十二研究員決策研究中心.py').read_text('utf-8')
 assert 'expanded=False' in (ROOT/'pages/28_十二研究員決策研究中心.py').read_text('utf-8')


def test_h150_public_headline_integrity(tmp_path):
 from godpick_h185_auto_sources import adopt_h150_news,load_h150_news
 from datetime import timezone
 now=datetime.fromisoformat('2026-11-23T11:00:00+08:00')
 items=[{'title':'測試新聞標題','link':'https://news.google.com/articles/demo',
    'published_at':'2026-11-22T08:10:00+00:00','source':'測試站','query':'台股'}]
 inp=tmp_path/'h150.json'
 inp.write_text(json.dumps({'items':items,'items_sha256':_digest(items),'updated_at':'2026-11-23T10:40:00+08:00'}),'utf-8')
 status=adopt_h150_news(inp,root=tmp_path,now=now)
 assert status['status']=='HEADLINES_ONLY'
 data=load_h150_news(root=tmp_path,asof='2026-11-23')
 assert len(data)==1 and data.iloc[0]['confirmed']==False
 assert 'RSS出現此標題' in data.iloc[0]['fact']
 assert load_h150_news(root=tmp_path,asof='2026-11-23',code='2330').empty


def test_h150_invalid_hash_not_accepted(tmp_path):
 from godpick_h185_auto_sources import adopt_h150_news
 inp=tmp_path/'h150.json';inp.write_text(json.dumps({'items':[{'title':'fake'}],
    'items_sha256':'0'*64,'updated_at':'2026-11-23T10:00:00+08:00'}),'utf-8')
 assert adopt_h150_news(inp,root=tmp_path,now=NOW)['status']=='NO_VALID_H150_NEWS'
 assert not (tmp_path/'data/h185/h150_public_news.json').exists()


def test_full_auto_technical_and_backtest_from_approved_history(tmp_path):
 from godpick_h184_research_lab import run_role
 q=normalize_quotes(twse_rows(),market='TWSE',observed_at=NOW.isoformat())['rows']
 _save(tmp_path,'TWSE_QUOTES',q)
 class MockHistory:
  def get(self,url,**kw):
   y=int(kw['params']['date'][:4]);mo=int(kw['params']['date'][4:6]);
   from calendar import monthrange
   dat=[]
   for d in range(1,monthrange(y,mo)[1]+1):
    day=datetime(y,mo,d)
    if day.weekday()>4 or day.date().isoformat()>'2026-11-20':continue
    price=30+(y-2026)*2+mo*.15+d*.025
    dat.append([f'{y-1911:03d}/{mo:02d}/{d:02d}','100,000',str(price),str(price+1),str(price-1),str(price+.3)])
   return Reply({'stat':'OK','fields':['日期','成交股數','開盤價','最高價','最低價','收盤價'],'data':dat})
 df,health=fetch_selected_history('3000',asof='2026-11-20',root=tmp_path,http=MockHistory(),months=9)
 assert len(df)>=150 and health['status']=='HISTORY_OFFICIAL_BACKFILL_UNADJUSTED'
 a=run_role('02',{'ohlcv':df},{'code':'3000','asof':'2026-11-20'})
 assert a.metrics['MA60']>0 and a.status=='技術指標已計算'
 b=run_role('04',{'ohlcv':df},{'code':'3000','asof':'2026-11-20'})
 assert b.status!='待補證' and b.metrics['回測迄日']


def test_ui_no_manual_upload_required_and_auto_analysis_exists():
 s=(ROOT/'pages/28_十二研究員決策研究中心.py').read_text('utf-8')
 assert '使用者不需手動匯入行情或財報' in s
 assert 'auto_execute=st.checkbox' in s
 assert 'st.session_state.get(\'h184_result_\'+role) is None' in s
 assert "role!='10'" in s


def test_git_workflow_scheduled_and_no_records_writes():
 s=(ROOT/'.github/workflows/h185_autonomous_research.yml').read_text('utf-8')
 assert "cron: '15 16 * * 1-5'" in s and 'workflow_dispatch:' in s
 assert 'git add -f -- data/h185/' in s
 assert not any(x in s for x in ('godpick_records.json','secrets.toml','git push --force'))


def test_event_append_preserves_historic_first_observed(tmp_path):
 old=normalize_events([{'公司代號':'3000','發言日期':'1151120','主旨':'首次公告'}],
      market='TWSE',observed_at='2026-11-20T20:30:00+08:00')
 _save(tmp_path,'TWSE_EVENTS',old['rows'],at='2026-11-20T20:30:00+08:00')
 feed=[{'公司代號':'3000','發言日期':'1151120','主旨':'首次公告'},
       {'公司代號':'3000','發言日期':'1151122','主旨':'新增公開事件'}]
 result=fetch_official(root=tmp_path,http=HTTP({SOURCES['TWSE_EVENTS'][0]:Reply(feed)}),
            now=NOW,source_names=('TWSE_EVENTS',))
 assert result['success']==1 and result['failures']==0
 snap=load_source('TWSE_EVENTS',root=tmp_path)
 assert len(snap['rows'])==2
 assert next(x for x in snap['rows'] if x['title']=='首次公告')['available_at']=='2026-11-20T20:30:00+08:00'
 data,health=load_auto_datasets(code='3000',asof='2026-11-21',root=tmp_path,include_h117=False)
 assert len(data['events'])==1 and data['events'].iloc[0]['title']=='首次公告'


def test_event_same_day_new_event_does_not_overwrite_old(tmp_path):
 before=normalize_events([{'公司代號':'3000','發言日期':'1151120','主旨':'舊公告'}],
      market='TWSE',observed_at='2026-11-20T20:30:00+08:00')
 _save(tmp_path,'TWSE_EVENTS',before['rows'],at='2026-11-20T20:30:00+08:00')
 added=[{'公司代號':'3000','發言日期':'1151120','主旨':'舊公告'},
        {'公司代號':'3001','發言日期':'1151120','主旨':'同日後公告'}]
 result=fetch_official(root=tmp_path,http=HTTP({SOURCES['TWSE_EVENTS'][0]:Reply(added)}),
            now=NOW,source_names=('TWSE_EVENTS',))
 assert result['success']==1
 snap=load_source('TWSE_EVENTS',root=tmp_path)
 assert len(snap['rows'])==2


def test_structured_role_source_contract_explicit_incomplete():
 s=json.loads((ROOT/'config/h185_auto_role_map.json').read_text('utf-8'))
 rows=s['roles']
 assert {r['role_id'] for r in rows}=={f'{i:02d}' for i in range(1,13)}
 assert all(r['primary'] and r['limits'] for r in rows)
 assert next(r for r in rows if r['role_id']=='05')['status']=='NEEDS_ACCOUNT_PERMISSION'
 assert next(r for r in rows if r['role_id']=='10')['status']=='MODEL_REQUIRES_ASSUMPTIONS'
 assert 'H109_H126_UNCHANGED' in s['research_authority']
