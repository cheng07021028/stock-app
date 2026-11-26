from datetime import datetime,timedelta
import pytest
import godpick_h222_financial_history as h
import godpick_h192_financial_growth as f
import godpick_h190_market_archive as ar
NOW=datetime.fromisoformat('2026-11-26T11:30:00+08:00')
M,Q=f.expected_periods(NOW)

def monthly(p=M,market='TWSE',rows=1):
    y,m=divmod(p,12);name=h.MARKETS[market][1]
    return f'<html><title>{name}公司{y-1911}年{m+1}月份(累計與當月)營業收入統計表</title><p>單位：千元</p><table><tr><th rowspan="2">公司代號</th><th colspan="3">營業收入</th></tr><tr><th>當月營收</th><th>上月營收</th><th>去年當月營收</th></tr>'+''.join(f'<tr><td>{1000+i}</td><td>1,200</td><td>1,000</td><td>800</td></tr>' for i in range(rows))+'</table></html>'

def quarterly(p=Q,market='TWSE',rows=1):
    y,q=divmod(p,4);name=h.MARKETS[market][1];n=q+1
    return f'<html><h2>{name}公司{y-1911}年第{n}季綜合損益表(累計)</h2><p>單位：千元</p><table><tr><th>公司代號</th><th>營業收入</th><th>營業毛利（毛損）</th><th>營業利益（損失）</th><th>本期淨利（淨損）</th><th>基本每股盈餘（元）</th></tr>'+''.join(f'<tr><td>{1000+i}</td><td>{1000*n}</td><td>{300*n}</td><td>{200*n}</td><td>{100*n}</td><td>{2*n}</td></tr>' for i in range(rows))+'</table></html>'

class Response:
    status_code=200
    def __init__(self,url,html):self.url=url;self.content=html.encode()

def test_monthly_span_and_units():
    r=h.parse('revenue','TWSE',M,monthly(),NOW)[0];assert r['revenue_twd']==1200000 and r['prior_year_month_twd']==800000

@pytest.mark.parametrize('change',[lambda s:s.replace('上市','上櫃'),lambda s:s.replace('千元','元'),lambda s:s.replace('115年','114年')])
def test_month_scope(change):
    with pytest.raises(ValueError):h.parse('revenue','TWSE',M,change(monthly()),NOW)

@pytest.mark.parametrize('change',[lambda s:s.replace('累計','單季'),lambda s:s.replace('上市','上櫃'),lambda s:s.replace('第3季','第2季'),lambda s:s.replace('千元','美元')])
def test_quarter_scope(change):
    with pytest.raises(ValueError):h.parse('ytd','TWSE',Q,change(quarterly()),NOW)

def test_quarter_fields():
    r=h.parse('ytd','TWSE',Q,quarterly(),NOW)[0];assert r['eps_ytd']==6 and r['operating_profit_ytd']==600

def test_duplicate_reject():
    with pytest.raises(ValueError):h.parse('revenue','TWSE',M,monthly(rows=2).replace('1001','1000'),NOW)

def test_collection_roundtrip(tmp_path,monkeypatch):
    monkeypatch.setattr(h,'LIMITS',{'TWSE':1,'TPEX':1})
    def http(method,url,**kwargs):
        if method=='GET':return Response(url,monthly(market='TWSE' if '/sii/' in url else 'TPEX'))
        return Response(url,quarterly(market='TWSE' if kwargs['data']['TYPEK']=='sii' else 'TPEX'))
    r=h.collect(tmp_path,now=NOW,budget=4,http=http);assert r['requests']==4 and sum(x['status']=='VERIFIED_SAVED' for x in r['scope'])==4
    data,err=f.load_inputs(tmp_path);assert not err
    l,_,_=f.build_ledgers(data,NOW);assert l['revenue','TWSE','1000'][M]['revenue_twd']==1200000
    l,_,_=f.build_ledgers(data,NOW-timedelta(seconds=1));assert not l
    p=h.partition(tmp_path,'revenue','TWSE',M);old=p.read_bytes()
    with pytest.raises(FileExistsError):h._persist(p,{'test':1})
    assert p.read_bytes()==old

def test_derived_margin_and_ttm(tmp_path,monkeypatch):
    monkeypatch.setattr(h,'LIMITS',{'TWSE':1,'TPEX':1})
    import hashlib
    for p in range(Q-7,Q+1):
        html=quarterly(p);rows=h.parse('ytd','TWSE',p,html,NOW);h._persist(h.partition(tmp_path,'ytd','TWSE',p),{'version':h.VERSION,'kind':'ytd','market':'TWSE','period':p,'url':h.QUARTER_URL,'observed_at':NOW.isoformat(),'html':html,'raw_sha256':hashlib.sha256(html.encode()).hexdigest(),'rows':rows})
    data,errors=f.load_inputs(tmp_path);assert not errors
    l,_,_=f.build_ledgers(data,NOW);mar=l['margin','TWSE','1000'];ytd=l['ytd','TWSE','1000'];assert mar[Q]['gross_margin_pct']==30
    p=f.financial_profile({},mar,ytd,{},M,Q,'');assert p['TTM EPS重建']==8 and p['連兩季三率年增'] is False

def test_conflict_not_overwrite():
    base=h._base('revenue','TWSE',M,'2330',NOW.isoformat());base.update(revenue_twd=100,previous_month_twd=None,prior_year_month_twd=None)
    old={**base,'source_url':f.URLS['revenue','TWSE'],'revenue_twd':101};old.pop('historical_source')
    l,_,c=f.build_ledgers({'revenue':[base,old]},NOW);assert not l and c['revenue']==1

def test_no_mixed_quarter_sources():
    assert f.single_q({Q:{'eps_ytd':6,'source_url':'a'},Q-1:{'eps_ytd':4,'source_url':'b'}},Q,'eps_ytd') is None

def test_budget_bounds():
    assert len(list(h.targets(NOW)))==42 and len(list(h.targets(NOW,36,12)))==96
    with pytest.raises(ValueError):list(h.targets(NOW,37,8))

def test_failed_requests_cooldown(tmp_path):
    def http(*a,**k):raise ValueError('network fail')
    r=h.collect(tmp_path,now=NOW,budget=8,http=http);assert r['requests']==8 and not list((tmp_path/'data/h222').rglob('*.json.gz'))
    r2=h.collect(tmp_path,now=NOW,budget=1,http=http);assert r2['requests']==1 and all(x['status']!='VERIFIED' for x in r2['scope'])

def test_excel_sheets():
    import io
    from openpyxl import load_workbook
    w=load_workbook(io.BytesIO(h.excel({'summary':[],'candidates':[],'stocks':[],'health':{},'errors':[],'conflicts':{}})))
    assert w.sheetnames==['01_母體驗收','02_高成長財報複核','03_逐股缺期','04_回補日誌','05_異常']
