# -*- coding: utf-8 -*-
"""Run from real patched stock-app: original H124/H221 parser, no HTTP or production writes."""
from datetime import datetime
import pytest
import godpick_h124_ownership_flow as own
import godpick_h221_evidence_closure as h

DAY='2026-11-26'; NOW=datetime.fromisoformat('2026-11-26T20:00:00+08:00')
DAYS=['2026-11-20','2026-11-23','2026-11-24','2026-11-25',DAY]
FOREIGN='Foreign Investors include Mainland Area Investors (Foreign Dealers excluded)-Difference'
AGG='ForeignInvestorsInclude MainlandAreaInvestors-Difference'
TRUST='SecuritiesInvestmentTrustCompanies-Difference'

def rows():return [{'Date':'1151126','SecuritiesCompanyCode':'6488','CompanyName':'測試',FOREIGN:'1,234',AGG:'9999',TRUST:'-100','TotalDifference':'1134'}]

def build(v):return own.build_tpex(v,trade_day=DAY,collected_at=NOW.isoformat(),source_url=h.TPEX)

def test_english_row_original_parser_and_readback(tmp_path):
    v=build(rows());assert own.verify_capture(v)
    assert v['stocks']['6488']=={'foreign_shares':1234,'trust_shares':-100,'total_shares':1134}
    assert v['schema_map']['foreign']==FOREIGN
    own.save_capture(tmp_path,v);assert own.load_captures(tmp_path,'TPEX')[DAY]==v

@pytest.mark.parametrize('field',['Date','SecuritiesCompanyCode',FOREIGN,TRUST,'TotalDifference'])
def test_missing_official_field_rejected(field):
    v=rows();v[0].pop(field)
    with pytest.raises(ValueError):build(v)

def test_wrong_date_rejected():
    v=rows();v[0]['Date']='1151125'
    with pytest.raises(ValueError):build(v)

def test_duplicate_record_rejected():
    with pytest.raises(ValueError):build(rows()*2)

def test_duplicate_foreign_semantics_rejected():
    v=rows();v[0]['外資及陸資不含外資自營商買賣超股數']='1234'
    with pytest.raises(ValueError):build(v)

class Response:
    status_code=200
    def __init__(self,url,raw):self.url=url;self.raw=raw
    def json(self):return self.raw

def test_h221_keeps_5day_gate(tmp_path):
    calls=[]
    def http(url,**kw):
        calls.append(url)
        if url==h.TPEX:return Response(url,rows())
        date=kw['params']['date']; values={'證券代號':'2330','外陸資買賣超股數(不含外資自營商)':'123','投信買賣超股數':'12','三大法人買賣超股數':'135'}
        return Response(url,{'stat':'OK','date':date,'fields':list(values),'data':[list(values.values())]})
    result=h.collect(tmp_path,http=http,now=NOW,sessions=DAYS,min_rows={'TWSE':1,'TPEX':1})
    assert len(calls)==6 and result['status']=='EVIDENCE_PENDING'
    assert result['sources'][1]['verified_days']==[DAY] and result['sources'][1]['missing_days']==DAYS[:-1]
    assert not result['trade_authority_changed']
