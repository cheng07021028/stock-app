from datetime import datetime
import json
import pytest
import godpick_h221_evidence_closure as h
import godpick_h192_financial_growth as f
import godpick_h124_ownership_flow as own
import godpick_h123_flow_provenance as flow
import godpick_h190_market_archive as ar
NOW=datetime.fromisoformat('2026-11-25T20:00:00+08:00')
DAYS=['2026-11-19','2026-11-20','2026-11-23','2026-11-24','2026-11-25']

def tpex(day='1151125'):
    return [{'資料日期':day,'代號':'2330','外資及陸資不含外資自營商買賣超股數':'123','外資及陸資買賣超股數':'999','投信買賣超股數':'12','三大法人買賣超股數合計':'135'}]

def twse(day):
    values={'證券代號':'2330','外陸資買賣超股數(不含外資自營商)':'123','投信買賣超股數':'12','三大法人買賣超股數':'135'}
    return {'stat':'OK','date':day.replace('-',''),'fields':list(values),'data':[list(values.values())]}

class Response:
    status_code=200
    def __init__(self,url,raw):self.url=url;self.raw=raw
    def json(self):return self.raw

def test_tpex_exclusive_and_roc(tmp_path):
    v=own.build_tpex(tpex(),trade_day=DAYS[-1],collected_at=NOW.isoformat(),source_url=h.TPEX)
    assert v['stocks']['2330']['foreign_shares']==123
    assert own.verify_capture(v)
    own.save_capture(tmp_path,v)
    assert own.load_captures(tmp_path,'TPEX')[DAYS[-1]]==v

def test_tpex_bad_date():
    with pytest.raises(ValueError):own.build_tpex(tpex('1151124'),trade_day=DAYS[-1],collected_at=NOW.isoformat(),source_url=h.TPEX)

def test_repair_and_repeat(tmp_path):
    calls=[]
    def http(url,**kw):
        calls.append(url);return Response(url,twse(kw['params']['date']) if url==h.TWSE else tpex())
    r=h.collect(tmp_path,http=http,now=NOW,sessions=DAYS,min_rows={'TWSE':1,'TPEX':1})
    assert r['status']=='EVIDENCE_PENDING';assert len(calls)==6
    assert r['sources'][0]['verified_days']==DAYS
    assert r['sources'][1]['missing_days']==DAYS[:-1]
    old=list((tmp_path/'data/h123').rglob('*.json'));assert len(old)==5
    assert all(json.loads(x.read_text())['first_observed_at']==NOW.isoformat() for x in old)
    calls.clear();h.collect(tmp_path,http=http,now=NOW,sessions=DAYS,min_rows={'TWSE':1,'TPEX':1});assert not calls

def test_reject_total_only(tmp_path):
    r=tpex();r[0].pop('外資及陸資不含外資自營商買賣超股數')
    def http(url,**kw):return Response(url,twse(kw['params']['date']) if url==h.TWSE else r)
    out=h.collect(tmp_path,http=http,now=NOW,sessions=DAYS,min_rows={'TWSE':1,'TPEX':1})
    assert out['sources'][1]['attempts'][0]['status']=='REJECTED'
    assert not own.load_captures(tmp_path,'TPEX')

def test_wrong_day_no_save(tmp_path):
    def http(url,**kw):return Response(url,twse('2026-11-18') if url==h.TWSE else tpex('1151124'))
    r=h.collect(tmp_path,http=http,now=NOW,sessions=DAYS,min_rows={'TWSE':1,'TPEX':1})
    assert all(a['status']=='REJECTED' for v in r['sources'] for a in v['attempts'])

def test_low_coverage(tmp_path):
    def http(url,**kw):return Response(url,twse(kw['params']['date']) if url==h.TWSE else tpex())
    r=h.collect(tmp_path,http=http,now=NOW,sessions=DAYS,min_rows={'TWSE':2,'TPEX':2})
    assert all(a['error']=='LOW_COVERAGE_1' for v in r['sources'] for a in v['attempts'])

def test_no_future(tmp_path):
    r=h.collect(tmp_path,http=lambda *a,**k:pytest.fail('network called'),now=NOW,sessions=DAYS[:-1]+['2026-11-26']);assert r['status']=='CALENDAR_PENDING'

def test_preclose(tmp_path):
    r=h.collect(tmp_path,http=lambda *a,**k:pytest.fail('network called'),now=NOW.replace(hour=12),sessions=DAYS);assert r['status']=='CALENDAR_PENDING'

def test_comparatives_not_fabricated():
    rev={10:{'revenue_twd':20,'previous_month_twd':10},11:{'revenue_twd':30,'previous_month_twd':20}}
    assert f.three_month_values(rev,11)==[10,20,30];assert 9 not in rev
    rev[9]={'revenue_twd':11};assert f.three_month_values(rev,11)==[None,20,30]

def test_only_latest_not_three():assert f.three_month_values({11:{'revenue_twd':30,'previous_month_twd':20}},11)==[None,20,30]

def test_pe_normalized():
    v=f.parse('pe','TPEX',[{' 股票代號 ':'2330','資料日期':'1151125','本益比（倍）':'10.5'}],NOW.isoformat());assert v[0]['pe']==10.5

def test_margin_zero_padded_quarter():
    v=f.parse('margin','TPEX',[{'公司代號':'2330','年度':'115','季別':'03','毛利率（％）':'30','營業利益率(%)':'20','稅後純益率(%)':'10'}],NOW.isoformat());assert v[0]['year_quarter']=='2026Q3'

def test_parse_colliding_headers():
    assert f.parse('pe','TPEX',[{'股票代號':'2330',' 股票代號 ':'1234','資料日期':'1151125','本益比':'10'}],NOW.isoformat())==[]

def test_financial_health_keeps_schema(tmp_path):
    def http(url,**kw):return Response(url,[{'unknown':'1'}])
    r=f.collect(tmp_path,get=http,now=NOW,min_rows={'TWSE':1,'TPEX':1})
    assert all(v['diagnostic']['fields']==['unknown'] and v['diagnostic']['parsed_rows']==0 for v in r['sources'].values())
