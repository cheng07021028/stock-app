from datetime import datetime,timedelta
import json
import pytest
import godpick_h233_auto_history as a
import godpick_h231_company_history as c
import godpick_h222_financial_history as h
import godpick_h192_financial_growth as f
import godpick_h190_market_archive as ar
from test_h231_company_history import fixture
from test_h222_financial_history import Response
NOW=datetime.fromisoformat('2026-11-26T15:25:00+08:00'); M,Q=f.expected_periods(NOW)

def master(root):
    (root/'stock_master_cache.json').write_text(json.dumps([{'code':code,'market':market,'name':'公司'+code} for code,market in [('1101','TWSE'),('2330','TWSE'),('3105','TPEX'),('6488','TPEX')]]))

def stocks():
    return [{'股票代號':'2330','市場':'TWSE','營收新鮮':True,'營收年增%':25},{'股票代號':'6488','市場':'TPEX','營收新鮮':False,'營收年增%':100}]

def test_fair_and_growth(tmp_path):
    master(tmp_path);r=a.plan(tmp_path,NOW,stocks=stocks(),previous={},attempts={});assert [x['股票代號'] for x in r['queue']]==['1101','2330'];assert r['counts']['具成長線索股票']==1;assert r['counts']['可請求期數']==32

def test_market_rotation(tmp_path):
    master(tmp_path);r=a.plan(tmp_path,NOW,stocks=stocks(),previous={'fair_market':'TWSE','asof':NOW.isoformat()},attempts={});assert r['queue'][0]['股票代號']=='3105'

def test_oldest_attempt_first(tmp_path):
    master(tmp_path);r=a.plan(tmp_path,NOW,stocks=[],previous={},attempts={f'TWSE:1101:{Q}':{'at':(NOW-timedelta(days=1)).isoformat()}});assert r['queue'][0]['股票代號']=='2330'

def test_cooldown_and_period_order(tmp_path):
    master(tmp_path);r=a.plan(tmp_path,NOW,stocks=stocks(),previous={},attempts={f'TWSE:2330:{Q}':{'at':NOW.isoformat()}});assert r['queue'][1]['下個可補期間']==f.qstr(Q-1);assert r['counts']['冷卻期數']==1

def test_existing_never_queued(tmp_path):
    master(tmp_path);p=c.path_for(tmp_path,'TWSE','2330',Q);p.parent.mkdir(parents=True);p.write_bytes(b'bad')
    r=a.plan(tmp_path,NOW,stocks=stocks(),previous={},attempts={});assert r['queue'][1]['下個可補期間']==f.qstr(Q-1);assert r['counts']['封存檔案存在期數']==1

def test_future_health_refuses(tmp_path):
    master(tmp_path)
    with pytest.raises(ValueError):a.plan(tmp_path,NOW,stocks=[],previous={'asof':(NOW+timedelta(seconds=1)).isoformat()},attempts={})

def test_future_attempt_refuses(tmp_path):
    master(tmp_path)
    with pytest.raises(ValueError):a.plan(tmp_path,NOW,stocks=[],previous={},attempts={f'TWSE:2330:{Q}':{'at':(NOW+timedelta(seconds=1)).isoformat()}})

def test_bad_master_refuses(tmp_path):
    with pytest.raises(ValueError):a.plan(tmp_path,NOW,stocks=[],previous={},attempts={})

def test_no_growth_fair_fallback(tmp_path):
    master(tmp_path);r=a.plan(tmp_path,NOW,stocks=[],previous={},attempts={});assert len(r['queue'])==2 and r['queue'][1]['佇列']=='無成長線索／公平補證'

def test_full_budget_real_collectors(tmp_path,monkeypatch):
    master(tmp_path);calls=[]
    def http(method,url,**kw):
        calls.append((method,url));d=kw.get('data',{})
        if url==c.URL:
            p=(int(d['year'])+1911)*4+int(d['season'])-1;return Response(url,fixture(p,d['co_id']))
        return Response(url,'invalid')
    r=a.run(tmp_path,now=NOW,http=http);assert r['bulk_requests']<=6 and r['company_requests']==2 and len(calls)==r['bulk_requests']+r['company_requests']<=8
    assert all(x['狀態']=='VERIFIED_SAVED' for x in r['queue']) and r['growth_candidates']==0 and not r['trade_authority_changed']
    assert a.health(tmp_path)==r;assert a.health(tmp_path,a.COMPANY_REL)['attempts']
    assert h.inspect(tmp_path,NOW)['auto_health']==r
    import io,openpyxl
    w=openpyxl.load_workbook(io.BytesIO(h.excel(h.inspect(tmp_path,NOW))));assert '08_H233自動補證' in w.sheetnames and '09_H233佇列摘要' in w.sheetnames
    r2=a.run(tmp_path,now=NOW,http=http);assert r2['fair_market']=='TPEX' and r2['company_requests']<=2

def test_limits(tmp_path):
    for n in [0,2,9]:
        with pytest.raises(ValueError):a.run(tmp_path,budget=n,now=NOW)

def test_invalid_health_no_requests(tmp_path):
    master(tmp_path);p=tmp_path/a.REL;p.parent.mkdir(parents=True);p.write_text('{}')
    with pytest.raises(ValueError):a.run(tmp_path,now=NOW,http=lambda *args,**kw:pytest.fail('network must not run'))

def test_tiny_base_yoy_cannot_outrank_multi_month(tmp_path):
    master(tmp_path);r=a.plan(tmp_path,NOW,stocks=[{'股票代號':'2330','市場':'TWSE','營收新鮮':True,'營收年增%':11,'連3月成長':True},{'股票代號':'6488','市場':'TPEX','營收新鮮':True,'營收年增%':999999}],previous={},attempts={});assert r['queue'][1]['股票代號']=='2330'
