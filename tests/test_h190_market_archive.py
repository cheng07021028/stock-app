# -*- coding: utf-8 -*-
from datetime import datetime,date,timedelta
from pathlib import Path
import ast,gzip,json,sys,time
import pandas as pd
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import godpick_h190_market_archive as h
NOW='2026-11-24T21:40:00+08:00'
DAY='2026-11-23'
FIELDS=['證券代號','證券名稱','成交股數','成交金額','開盤價','最高價','最低價','收盤價','成交筆數']

def raw(day=DAY,market='TWSE',n=None):
    n=n or h.MIN_ROWS[market]
    fields=FIELDS if market=='TWSE' else ['代號','名稱','成交股數','成交金額(元)','開盤','最高','最低','收盤','成交筆數']
    base=1000 if market=='TWSE' else 5000
    return {'stat':'OK','date':day.replace('-',''), 'tables':[{'title':day,'fields':fields,'data':[[str(base+i),'測試','100,000','10,000,000','100','101','99','100','20'] for i in range(n)]}]}

def calendar(days):
    return h.parse_calendar({'stat':'OK','fields':['日期','收盤指數'],'data':[[x,'30000'] for x in days]},days[0][:7],observed_at=NOW)

def save(root,day=DAY,market='TWSE'):
    return h.save_day(root,h.parse_daily(raw(day,market),market,day,observed_at=NOW))

def test_parse_twse_shares_amount():
    p=h.parse_daily(raw(),'TWSE',DAY,observed_at=NOW)
    assert p['rows'][0]==['1000',100,101,99,100,100000,10000000,20]
    assert p['row_count']==700 and 'UNADJUSTED' in p['basis']

def test_parse_tpex_real_units():
    p=h.parse_daily(raw(market='TPEX'),'TPEX',DAY,observed_at=NOW)
    assert p['rows'][0][5:7]==[100000,10000000]

@pytest.mark.parametrize('change',["wrong_date","missing_date","duplicate","unknown_unit","zero_volume","unit_mismatch","partial"])
def test_bad_daily_rejected(change):
    r=raw(); table=r['tables'][0]
    if change=='wrong_date':r['date']='20261120'
    if change=='missing_date':r.pop('date');table.pop('title')
    if change=='duplicate':table['data'].append(table['data'][0])
    if change=='unknown_unit':table['fields']=list(table['fields']);table['fields'][2]='成交量'
    if change=='zero_volume':
        for row in table['data']:row[2]='0'
    if change=='unit_mismatch':
        for row in table['data']:row[2]='100'
    if change=='partial':table['data']=table['data'][:50]
    with pytest.raises(ValueError):h.parse_daily(r,'TWSE',DAY,observed_at=NOW)

def test_calendar_actual_sessions_not_weekdays():
    p=calendar(['2026-11-20','2026-11-23']);assert p['days']==['2026-11-20','2026-11-23']

def test_archive_history_strict_market_and_session(tmp_path):
    days=['2026-11-20','2026-11-23'];h.save_calendar(tmp_path,calendar(days))
    for d in days:save(tmp_path,d)
    f=h.archive_history('1000','上市',days[0],days[-1],root=tmp_path)
    assert len(f)==2 and f['成交股數'].tolist()==[100000]*2
    assert f['成交金額'].tolist()==[10000000]*2
    assert h.archive_history('1000','上櫃',days[0],days[-1],root=tmp_path).empty
    assert h.archive_history('1000','上市','2026-10-01',DAY,root=tmp_path).empty

def test_missing_session_never_filled(tmp_path):
    days=['2026-11-20','2026-11-23'];h.save_calendar(tmp_path,calendar(days));save(tmp_path)
    assert h.archive_history('1000','TWSE',days[0],days[-1],root=tmp_path).empty

def test_immutable_and_digest(tmp_path):
    assert save(tmp_path);assert not save(tmp_path)
    r=raw();r['tables'][0]['data'][0][7]='100.5'
    with pytest.raises(ValueError,match='conflict'):h.save_day(tmp_path,h.parse_daily(r,'TWSE',DAY,observed_at=NOW))
    path=tmp_path/'daily/TWSE'/f'{DAY}.json.gz';v=json.loads(gzip.decompress(path.read_bytes()));v['rows'][0][5]=3
    path.write_bytes(gzip.compress(json.dumps(v).encode()))
    assert h.inspect(tmp_path)['read_errors']

def test_calendar_no_shrink(tmp_path):
    h.save_calendar(tmp_path,calendar(['2026-11-20','2026-11-23']))
    with pytest.raises(ValueError):h.save_calendar(tmp_path,calendar(['2026-11-20']))

class Reply:
    status_code=200
    def __init__(self,obj):self.obj=obj
    def json(self):return self.obj
class HTTP:
    def __init__(self,fail=None):self.calls=[];self.fail=fail
    def get(self,url,params,**kw):
        self.calls.append((url,params))
        if url==self.fail:raise RuntimeError('network unavailable')
        if url==h.CAL_URL:
            month=params['date'][:6];d=month[:4]+'-'+month[4:]+'-01'
            return Reply({'stat':'OK','fields':['日期','收盤指數'],'data':[[d,'30000']]})
        market=next(m for m,u in h.URLS.items() if u==url)
        return Reply(raw(h._day(params['date']),market))

def test_bounded_collector_and_restart(tmp_path):
    http=HTTP();now=datetime.fromisoformat(NOW)
    x=h.collect(tmp_path,http=http,now=now,budget=2,pause=0)
    assert x['new_files']==4 and len([1 for u,p in http.calls if u in h.URLS.values()])==4
    assert h.inspect(tmp_path)['paired_verified_sessions']==2
    y=h.collect(tmp_path,http=HTTP(),now=now,budget=2,pause=0)
    assert y['new_files']==4 and h.inspect(tmp_path)['paired_verified_sessions']==4

def test_partial_market_failure_preserves_other_market(tmp_path):
    x=h.collect(tmp_path,http=HTTP(fail=h.URLS['TPEX']),now=datetime.fromisoformat(NOW),budget=4,pause=0)
    assert x['errors'] and x['new_files']>0
    assert h.inspect(tmp_path)['paired_verified_sessions']==0
    assert h.inspect(tmp_path)['market_verified_sessions']['TWSE']>0

def test_no_current_intraday_download(tmp_path):
    http=HTTP();now=datetime.fromisoformat('2026-11-01T14:00:00+08:00')
    h.collect(tmp_path,http=http,now=now,budget=1,pause=0)
    assert all(h._day(p['date'])<'2026-11-01' for u,p in http.calls if u in h.URLS.values())

def test_actual_utils_entry_uses_archive_first(monkeypatch):
    from test_h187_scan_speed import invoke,frame,DATES
    f=frame();f['資料源']='H190_TWSE_OFFICIAL_RAW_UNADJUSTED'
    monkeypatch.setattr(h,'archive_history',lambda *a,**k:f)
    get,calls=invoke(cache=frame(weak=True));result=get('2330',scan_fast=True,**DATES)
    assert len(result)==162 and not calls['yahoo'] and not calls['official']
    assert any(x[1]=='H190OfficialArchive' for x in calls['events'])

def test_h185_uses_same_archive_without_merge(monkeypatch,tmp_path):
    from godpick_h185_auto_sources import load_auto_datasets
    days=pd.bdate_range(end=DAY,periods=100)
    f=pd.DataFrame({'日期':days,'開盤價':100.,'最高價':101.,'最低價':99.,'收盤價':100.,'成交股數':100000,'成交金額':10000000,'資料源':'H190_TWSE_OFFICIAL_RAW_UNADJUSTED'})
    monkeypatch.setattr(h,'archive_history',lambda code,market,*a,**k:f if market=='TWSE' else pd.DataFrame())
    out,status=load_auto_datasets(code='1000',asof=DAY,root=tmp_path)
    assert len(out['ohlcv'])==100 and out['ohlcv']['volume'].iloc[0]==100000
    assert 'NOT_HISTORICAL_PIT_CERTIFIED' in status['sources']['H190_OFFICIAL_ARCHIVE']['status']

def test_install_guard_and_idempotency(tmp_path,monkeypatch):
    from tools import h190_preflight as p
    root=tmp_path/'root'; payload=tmp_path/'payload';root.mkdir();payload.mkdir()
    before=b'base';after=b'updated';added=b'new';dep=b'h189'
    monkeypatch.setattr(p,'BASE',{'utils.py':p.gitsha(before),'new.py':None})
    monkeypatch.setattr(p,'DEPENDENCIES',{'h189.py':p.gitsha(dep)})
    (root/'utils.py').write_bytes(before);(root/'h189.py').write_bytes(dep)
    (payload/'utils.py').write_bytes(after);(payload/'new.py').write_bytes(added)
    (payload/'H190_SHA256SUMS.txt').write_text(p.sha(after)+'  utils.py\n'+p.sha(added)+'  new.py\n')
    _,todo,err=p.check(root,payload);assert todo==['utils.py','new.py'] and not err
    assert not (root/'new.py').exists()
    backup=p.apply(root,payload);assert (backup/'utils.py').read_bytes()==before
    assert (root/'new.py').read_bytes()==added and p.apply(root,payload) is None
    (root/'utils.py').write_bytes(b'unknown')
    with pytest.raises(ValueError,match='UNKNOWN_VERSION'):p.apply(root,payload)
    assert (root/'utils.py').read_bytes()==b'unknown'
    (payload/'new.py').write_bytes(b'tampered')
    assert any('PATCH_MISSING_OR_DIGEST' in x for x in p.check(root,payload)[2])

def test_same_day_before_eod_rejected():
    with pytest.raises(ValueError,match='not yet eligible'):
        h.parse_daily(raw('2026-11-24'),'TWSE','2026-11-24',observed_at='2026-11-24T14:00:00+08:00')
