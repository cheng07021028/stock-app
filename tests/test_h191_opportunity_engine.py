from datetime import date,timedelta,datetime
from pathlib import Path
import sys,copy
import numpy as np
import pytest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import godpick_h191_opportunity_engine as h
import godpick_h190_market_archive as ar

DAYS=[(date(2026,11,23)-timedelta(days=i)).isoformat() for i in range(85,-1,-1) if (date(2026,11,23)-timedelta(days=i)).weekday()<5][-60:]
ASOF=DAYS[-1]
def bars(prices,vol=500000,lastvol=None):
    result=[]
    for i,(d,c) in enumerate(zip(DAYS,prices)):
        v=lastvol if i==59 and lastvol else vol
        result.append((d,c,c*1.005,c*.995,c,v,round(v*c),100))
    return result

def fixture():
    master=[];rows={};markets={m:{d:100 for d in DAYS} for m in ar.URLS};cal={}
    for d in DAYS:cal.setdefault(d[:7],set()).add(d)
    for m,base in [('TWSE',1000),('TPEX',5000)]:
        for i in range(100):
            code=str(base+i);industry='強勢產業' if i<20 else '普通產業'
            master.append({'code':code,'name':'測試'+code,'market':m,'official_industry':industry})
            p=np.linspace(100,110,60) if i>=20 else np.r_[np.linspace(90,100,55),[101,102,104,106,108]]
            rows[m,code]=bars(p,lastvol=1_000_000 if i<20 else None)
    return dict(master=master,calendars=cal,markets=markets,rows=rows,asof=ASOF)

def test_valid_features():
    f,reason=h.calc_features(bars(np.linspace(90,100,60)),DAYS); assert not reason and f['20日均成交額']>20_000_000
@pytest.mark.parametrize('kind',['missing','dup','zero','nan','unit','gap','ohlc'])
def test_invalid(kind):
    s=bars(np.linspace(90,100,60))
    if kind=='missing':s.pop(3)
    elif kind=='dup':s[3]=s[2]
    else:
        r=list(s[3]);j,v={'zero':(5,0),'nan':(4,float('nan')),'unit':(6,10),'gap':(1,200),'ohlc':(2,1)}[kind];r[j]=v;s[3]=tuple(r)
    assert h.calc_features(s,DAYS)[0] is None

def test_mainstream_and_breakout():
    f=fixture();r=h.evaluate(**f);assert r['status']=='CROSS_MARKET_RESEARCH_READY';assert r['qualified']==200
    assert r['pools']['主流強勢'];assert r['pools']['技術突破'];assert r['pool_counts']['主流強勢']==40
    assert all(x['交易狀態']=='僅研究／未授權' for x in r['pools']['主流強勢']);assert r['trade_authority_changed'] is False

def test_partial_no_market_rank():
    f=fixture();f['markets']['TPEX'].pop(DAYS[0]);r=h.evaluate(**f);assert r['status']=='PARTIAL_RESEARCH';assert not r['pools']['主流強勢'];assert r['pools']['技術突破'];assert len(r['diagnostic'])==200
@pytest.mark.parametrize('key,value',[('latest_ready',False),('errors',['bad digest'])])
def test_block(key,value):
    r=h.evaluate(**fixture(),**{key:value});assert r['status']=='DATA_PENDING';assert all(not x for x in r['pools'].values())
def test_etf_exclusion():
    f=fixture();f['master'].append({'code':'0050','market':'TWSE'});assert h.evaluate(**f)['universe']==200

def test_missing_index_never_proxy():
    r=h.evaluate(**fixture());assert not r['index_available'] and not r['pools']['大盤抗跌']
def test_relative_resilience():
    r=h.evaluate(**fixture(),index_returns=np.full(20,-1.));assert r['index_available'] and r['pools']['大盤抗跌']

def tdcc(code,d,ratio):return {'source':'TDCC_OPEN_DATA','source_url':'https://opendata.tdcc.com.tw/getOD.ashx?id=1-5','data_date':d,'rows':{code:{'證券代號':code,'持股分級':'15','資料日期':d,'占集保庫存數比例%':ratio}}}
def factor(code,d):return {'股票代號':code,'法人資料日期':d,'法人資料源':'TWSE_T86+DAILY_HISTORY','三大法人近5日合計':50000,'法人連買天數':3}
def test_tdcc_both_sources():
    d='2026-11-20';p='2026-11-13';r=h.evaluate(**fixture(),factors={'records':[factor('1000',ASOF)]},latest_tdcc=tdcc('1000',d,45.5),prior_tdcc=tdcc('1000',p,44.5));assert [x['股票代號'] for x in r['pools']['大戶持股增加']]==['1000']
@pytest.mark.parametrize('date_',['2026-11-19','2026-11-24',''])
def test_stale_factor(date_):assert h.institutional(factor('1000',date_),ASOF)==(None,None)
def test_stale_tdcc():assert h.tdcc_delta('1000',tdcc('1000','2026-11-06',45),tdcc('1000','2026-10-30',44),ASOF)[0] is None

def test_blackhorse_independent():
    f=fixture();p=np.r_[np.linspace(100,112,8),np.linspace(112,91,15),np.linspace(91,100,17),np.tile([99,100,101,100],5)];f['rows']['TWSE','1000']=bars(p);r=h.evaluate(**f);assert '1000' in [x['股票代號'] for x in r['pools']['低位階蓄勢']]
def test_empty_archive(tmp_path):
    r=h.run(tmp_path,now=datetime.fromisoformat('2026-11-24T10:00:00+08:00'));assert r['status']=='DATA_PENDING' and not r['calendar_fresh']
def test_deterministic_sort():
    r=h.evaluate(**fixture(),top=10);a=r['pools']['主流強勢'];assert len(a)==10 and [x['研究排名'] for x in a]==list(range(1,11));assert [x['股票代號'] for x in a]==sorted(x['股票代號'] for x in a)

def test_read_archive_end_to_end(tmp_path,monkeypatch):
    # Reduce fixture size only; production archive minimums unchanged.
    monkeypatch.setattr(ar,'MIN_ROWS',{'TWSE':1,'TPEX':1})
    import json
    r=tmp_path/'data/h190';now='2026-11-24T09:00:00+08:00'; f=fixture()
    for month,ds in f['calendars'].items():
        payload=ar.parse_calendar({'stat':'OK','fields':['日期','收盤指數'],'data':[[d,'30000'] for d in sorted(ds)]},month,observed_at=now);ar.save_calendar(r,payload)
    for i,d in enumerate(DAYS):
        for m,code in [('TWSE','1000'),('TPEX','5000')]:
            _,o,hi,lo,c,v,a,n=f['rows'][m,code][i]
            raw={'stat':'OK','date':d,'fields':['代號','開盤價','最高價','最低價','收盤價','成交股數','成交金額','成交筆數'],'data':[[code,o,hi,lo,c,v,a,n]]}
            ar.save_day(r,ar.parse_daily(raw,m,d,observed_at=now))
    (tmp_path/'stock_master_cache.json').write_text(json.dumps([x for x in f['master'] if x['code'] in ('1000','5000')]),'utf-8')
    result=h.run(tmp_path,now=datetime.fromisoformat(now));assert result['qualified']==2 and result['calendar_fresh'] and result['paired_sessions']==60 and result['pools']['技術突破'];assert not result['pools']['主流強勢']
    # Same archive must not be represented as the next day's data.
    stale=h.run(tmp_path,now=datetime.fromisoformat('2026-11-25T09:00:00+08:00'));assert stale['status']=='DATA_PENDING'

def test_liquidity_gate():
    r=h.evaluate(**fixture(),min_amount=1e12);assert not any(r['pools'].values()) and r['qualified']==200

def test_factors_no_proxy_and_no_single_day_sum():
    r=factor('1000',ASOF);r['法人資料源']='TPEX_OPENAPI_3INSTI';assert h.institutional(r,ASOF)==(None,None)
    r['法人資料源']='TWSE_T86+DAILY_HISTORY_PROXY';assert h.institutional(r,ASOF)==(None,None)
