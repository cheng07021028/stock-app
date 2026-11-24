from datetime import datetime
from pathlib import Path
import json
import pytest
import godpick_h192_financial_growth as h
NOW=datetime(2026,11,24,22,30,tzinfo=h.TZ)
MARKET_DAY='2026-11-23'
M=h.month('202610'); Q=h.quarter('2026Q3')

def obs(kind,code,period,market='TWSE',**kw):
    key={'revenue':'year_month','margin':'year_quarter','ytd':'year_quarter','pe':'asof_date'}[kind]
    p=h.monthstr(period) if kind=='revenue' else period if kind=='pe' else h.qstr(period)
    v={'code':code,'source':market,'source_url':h.URLS[kind,market],'available_at':NOW.isoformat(),'availability_basis':'FIRST_OBSERVED',key:p}
    if kind=='margin':v.update(industry_type='GENERAL',statement_basis='SINGLE_QUARTER')
    if kind=='ytd':v.update(basis='CUMULATIVE_YTD_NOT_SINGLE_QUARTER',unit='元／股',report_date='2026-11-23',profit_basis='SAME_SOURCE_DISCLOSED_UNIT',operating_profit_ytd_label='營業利益',net_profit_ytd_label='本期淨利')
    return {**v,**kw}

def full():
    master=[{'code':str(i),'name':'公司'+str(i),'market':'上市','official_industry':'半導體業'} for i in range(1101,1107)]
    data={k:[] for k in h.METRICS}
    for i in range(1101,1107):
        c=str(i)
        for m in range(M-12,M+1):data['revenue'].append(obs('revenue',c,m,revenue_twd=(m-M+20)*10_000_000))
        for q in range(Q-5,Q+1):
            data['margin'].append(obs('margin',c,q,gross_margin_pct=20+q-Q,operating_margin_pct=10+q-Q,net_margin_pct=8+q-Q))
            # Synthetic single-quarter EPS=2 and increasing profits; quarter totals reset each year.
            a=q%4+1; first=q-q%4; values=[(2 if j<Q else 3,100+j-Q,80+j-Q) for j in range(first,q+1)]
            data['ytd'].append(obs('ytd',c,q,eps_ytd=sum(v[0] for v in values),operating_profit_ytd=sum(v[1] for v in values),net_profit_ytd=sum(v[2] for v in values)))
        data['pe'].append(obs('pe',c,MARKET_DAY,pe=10 if i==1101 else 20))
    return master,data

def evaluate(master,data,**kw):return h.evaluate(master=master,data=data,now=NOW,market_day=kw.pop('market_day',MARKET_DAY),**kw)

def test_full_pools():
    m,d=full();r=evaluate(m,d)
    assert r['counts'][h.POOLS[0]]==6
    assert r['counts'][h.POOLS[1]]==6
    assert r['counts'][h.POOLS[2]]==6
    assert r['counts'][h.POOLS[3]]==6
    assert [x['股票代號'] for x in r['pools'][h.POOLS[4]]]==['1101']
    p=r['profiles'][0];assert p['TTM EPS重建']==9 and p['同業PE中位數']==20
    assert not r['trade_authority_changed'] and len(r['evidence'])>0

@pytest.mark.parametrize('raw,want',[('11510',M),('202610',M),('202613',None),('junk',None)])
def test_month(raw,want):assert h.month(raw)==want

def test_revenue_parse_comparatives():
    raw=[{'公司代號':'1101','資料年月':'11510','營業收入-當月營收':'20,000','營業收入-上月營收':'10,000','營業收入-去年當月營收':'12,000'}]
    r=h.parse('revenue','TWSE',raw,NOW.isoformat());assert len(r)==1 and r[0]['revenue_twd']==20_000_000 and r[0]['prior_year_month_twd']==12_000_000
    m,d=full();d={k:[] for k in d};d['revenue']=r
    result=evaluate(m,d);assert result['counts'][h.POOLS[6]]==1 and result['counts'][h.POOLS[0]]==0 and result['counts'][h.POOLS[1]]==0

@pytest.mark.parametrize('kind,raw',[('revenue',{'公司代號':'1101','資料年月':'11512','營業收入-當月營收':'10'}),('pe',{'Code':'1101','Date':'20261125','PEratio':'8'}),('pe',{'Code':'1101','PEratio':'8'}),('pe',{'Code':'1101','Date':'20261123','PEratio':'-3'}),('revenue',{'公司代號':'0050','資料年月':'11510','營業收入-當月營收':'10'})])
def test_reject_invalid_raw(kind,raw):assert h.parse(kind,'TWSE',[raw],NOW.isoformat())==[]

def test_duplicate_conflicting_raw():
    raw={'Code':'1101','Date':'20261123','PEratio':'8'};assert h.parse('pe','TWSE',[raw,{**raw,'PEratio':'9'}],NOW.isoformat())==[]

def test_ytd_parse_zero_and_source_label():
    raw={'公司代號':'1101','年度':'115','季別':'3','出表日期':'1151123','基本每股盈餘（元）':0,'營業利益（損失）':'1,000','本期淨利（淨損）':'200'}
    r=h.parse('ytd','TWSE',[raw],NOW.isoformat())[0];assert r['eps_ytd']==0 and r['operating_profit_ytd']==1000 and r['basis']=='CUMULATIVE_YTD_NOT_SINGLE_QUARTER'

def test_margin_parse():
    raw={'公司代號':'1101','年度':'115','季別':'3','毛利率(%)(營業毛利)/(營業收入)':20,'營業利益率(%)(營業利益)/(營業收入)':10,'稅後純益率(%)(稅後純益)/(營業收入)':0}
    assert h.parse('margin','TWSE',[raw],NOW.isoformat())[0]['net_margin_pct']==0

@pytest.mark.parametrize('mutation',['future','badsource','market','naive','badbasis'])
def test_asof_ledger_reject(mutation):
    r=obs('revenue','1101',M,revenue_twd=100)
    r.update({'future':{'available_at':'2026-11-25T01:00:00+08:00'},'badsource':{'source_url':'https://example.com'},'market':{'market':'上櫃'},'naive':{'available_at':'2026-11-24T01:00:00'},'badbasis':{'availability_basis':'GUESSED'}}[mutation])
    assert h.canonical('revenue',r,NOW) is None

def test_missing_quarters_not_ttm():
    m,d=full();d['ytd']=[x for x in d['ytd'] if x['year_quarter']==h.qstr(Q)];r=evaluate(m,d)
    assert r['profiles'][0]['TTM EPS重建'] is None and r['counts'][h.POOLS[4]]==0

def test_restatement_no_mixing():
    m,d=full();r=dict(d['ytd'][0]);r['eps_ytd']=999;r['available_at']='2026-11-24T21:00:00+08:00';d['ytd'].append(r);res=evaluate(m,d);assert res['conflicts']['ytd']>=1

def test_current_ytd_conflict_blocks_ttm():
    m,d=full();r=next(x for x in d['ytd'] if x['code']=='1101' and x['year_quarter']==h.qstr(Q));d['ytd'].append({**r,'eps_ytd':99,'available_at':'2026-11-24T21:00:00+08:00'});res=evaluate(m,d);assert res['profiles'][0]['TTM EPS重建'] is None

def test_ytd_profit_label_mismatch():
    m,d=full();d['ytd'][4]['operating_profit_ytd_label']='different';r=evaluate(m,d);assert r['profiles'][0]['獲利突破'] is None

def test_pe_no_calendar_or_five_peers():
    m,d=full();assert evaluate(m,d,market_day='')['counts'][h.POOLS[4]]==0
    assert evaluate(m[:-1],d)['counts'][h.POOLS[4]]==0

def test_pe_not_from_price_over_eps():
    m,d=full();d['pe']=[];r=evaluate(m,d);assert r['profiles'][0]['官方PE'] is None and r['counts'][h.POOLS[4]]==0

def test_low_pe_without_growth_not_qualified():
    m,d=full();d['ytd']=[{**x,'eps_ytd':2*(h.quarter(x['year_quarter'])%4+1)} for x in d['ytd']];assert evaluate(m,d)['counts'][h.POOLS[4]]==0

def test_stale_financial_month_and_quarter():
    m,d=full();d['revenue']=[x for x in d['revenue'] if x['year_month']!='202610'];d['margin']=[x for x in d['margin'] if x['year_quarter']!='2026Q3'];r=evaluate(m,d);assert r['counts'][h.POOLS[0]]==r['counts'][h.POOLS[2]]==0

def test_margin_same_timestamp_conflict_does_not_crash():
    m,d=full();r=dict(d['margin'][5]);r['operating_margin_pct']=99;d['margin'].append(r);res=evaluate(m,d);assert res['profiles'][0]['三率季度']!='2026Q3' and res['conflicts']['margin']>=1

def test_technical_intersection_and_stale():
    m,d=full();opp={'asof':MARKET_DAY,'calendar_fresh':True,'pools':{'低位階蓄勢':[{'股票代號':'1101','資料日期':MARKET_DAY}]}}
    assert evaluate(m,d,opportunities=opp)['counts'][h.POOLS[5]]==1
    opp['calendar_fresh']=False;assert evaluate(m,d,opportunities=opp)['counts'][h.POOLS[5]]==0

def test_two_months_pending_not_certified():
    m,d=full();d={k:[] for k in d};d['revenue']=[obs('revenue','1101',M-1,revenue_twd=100),obs('revenue','1101',M,revenue_twd=120)];r=evaluate(m,d);assert r['counts'][h.POOLS[7]]==1 and r['counts'][h.POOLS[0]]==0 and r['counts'][h.POOLS[6]]==0

class Resp:
    status_code=200
    def __init__(self,rows):self.rows=rows
    def json(self):return self.rows

def test_collection_failure_preserves_bytes(tmp_path):
    raw=[{'公司代號':'1101','資料年月':'11510','營業收入-當月營收':'1000','營業收入-上月營收':'900','營業收入-去年當月營收':'800'}]
    def get(url,**kw):return Resp(raw if url==h.URLS['revenue','TWSE'] else [])
    r=h.collect(tmp_path,get=get,now=NOW,min_rows={'TWSE':1,'TPEX':1});p=h.feed_path(tmp_path,'revenue','TWSE');b=p.read_bytes();assert r['status']=='PARTIAL_OR_STALE' and r['sources']['revenue_TWSE']['status']=='OK'
    r=h.collect(tmp_path,get=lambda *a,**k:Resp([]),now=NOW,min_rows={'TWSE':1,'TPEX':1});assert p.read_bytes()==b and r['sources']['revenue_TWSE']['status']=='ERROR'
    h.collect(tmp_path,get=get,now=NOW,min_rows={'TWSE':1,'TPEX':1});assert p.read_bytes()==b

def test_corrupt_archive_not_overwritten(tmp_path):
    p=h.feed_path(tmp_path,'revenue','TWSE');p.parent.mkdir(parents=True);p.write_bytes(b'broken');r=h.collect(tmp_path,get=lambda *a,**k:Resp([]),now=NOW,min_rows={'TWSE':1,'TPEX':1});assert p.read_bytes()==b'broken' and r['sources']['revenue_TWSE']['status']=='ERROR'

def test_official_seed_read():
    root=Path(__file__).resolve().parents[1];r=h.run(root,now=NOW);assert r['coverage']['TWSE']['當期月營收']>1000;assert r['counts'][h.POOLS[7]]>0;assert r['counts'][h.POOLS[4]]==0

def test_revision_reversion_preserved(tmp_path):
    from datetime import timedelta
    def get(value):
        raw=[{'公司代號':'1101','資料年月':'11510','營業收入-當月營收':value}]
        return lambda url,**kw:Resp(raw if url==h.URLS['revenue','TWSE'] else [])
    for i,v in enumerate([100,110,100]):h.collect(tmp_path,get=get(v),now=NOW+timedelta(minutes=i),min_rows={'TWSE':1,'TPEX':1})
    rows=h.read_feed(tmp_path,'revenue','TWSE');assert [x['revenue_twd'] for x in rows]==[100000,110000,100000]
    led,_,_=h.build_ledgers({'revenue':rows},NOW+timedelta(minutes=2));assert led['revenue','TWSE','1101'][M]['revenue_twd']==100000
