from datetime import datetime,timedelta
import json,hashlib
import pytest
import godpick_h222_financial_history as h
import godpick_h192_financial_growth as f
import godpick_h190_market_archive as ar
import godpick_h223_growth_scenarios as scenarios
from test_h222_financial_history import monthly,quarterly,Response
NOW=datetime.fromisoformat('2026-11-26T14:00:00+08:00')
M,Q=f.expected_periods(NOW)

def sample(code='1101',market='TWSE',p=Q):
    y,q=divmod(p,4)
    # Values from official TWSE endpoint, 115Q3 1101, checked 2026-11-26.
    r={'出表日期':'1151126','年度':str(y-1911),'季別':str(q+1),'公司代號':code,'營業收入':'112522875.00','營業毛利（毛損）':'21229246.00','營業毛利（毛損）淨額':'21229245.00','營業利益（損失）':'9571599.00','本期淨利（淨損）':'7921300.00','基本每股盈餘（元）':'0.63'}
    # net gross is intentionally DIFFERENT in fixture to prove correct field choice.
    if market=='TPEX':
        for a,b in [('出表日期','Date'),('年度','Year'),('季別','Season'),('公司代號','SecuritiesCompanyCode')]:r[b]=r.pop(a)
    return r

def html(market='TWSE',codes=('1101',),p=Q):
    n=p%4+1;label=h.MARKETS[market][1];heads=['公司代號','公司名稱','營業收入','營業毛利(毛損)','營業毛利(毛損)淨額','營業利益(損失)','本期淨利(淨損)','基本每股盈餘(元)']
    return '<html><title>公開資訊觀測站</title><h2>'+label+'公司第'+'一二三四'[n-1]+'季資料</h2><table><tr>'+''.join('<th>'+x+'</th>' for x in heads)+'</tr>'+''.join('<tr><td>'+c+'</td><td>公司</td><td>112,522,875</td><td>21,229,246</td><td>21,229,245</td><td>9,571,599</td><td>7,921,300</td><td>0.63</td></tr>' for c in codes)+'</table></html>'

def anchor(market='TWSE',rows=None,at=NOW):
    raw=json.dumps(rows if rows is not None else [sample(market=market)],ensure_ascii=False)
    return {'url':f.URLS['ytd',market],'observed_at':at.isoformat(),'raw':raw,'sha256':hashlib.sha256(raw.encode()).hexdigest()}

@pytest.fixture(autouse=True)
def minimum(monkeypatch):monkeypatch.setattr(h,'LIMITS',{'TWSE':1,'TPEX':1})

@pytest.mark.parametrize('market',['TWSE','TPEX'])
def test_real_shape_anchor(market):
    r=h.anchored_rows(market,Q,html(market),NOW,anchor(market))[0]
    assert r['gross_profit_ytd']==21229246 and r['eps_ytd']==.63
    with pytest.raises(ValueError):h.parse('ytd',market,Q,html(market),NOW)

@pytest.mark.parametrize('mutate',[lambda a:a.update(url='https://wrong'),lambda a:a.update(sha256='bad'),lambda a:a.update(observed_at=(NOW+timedelta(seconds=1)).isoformat())])
def test_invalid_anchor(mutate):
    a=anchor();mutate(a)
    with pytest.raises(ValueError):h.anchored_rows('TWSE',Q,html(),NOW,a)

@pytest.mark.parametrize('value',['114','116'])
def test_wrong_year(value):
    r=sample();r['年度']=value
    with pytest.raises(ValueError):h.anchored_rows('TWSE',Q,html(),NOW,anchor(rows=[r]))

@pytest.mark.parametrize('fragment',['<h3>114年第三季</h3>','<h3>115年第二季</h3>','<p>單位：百萬元</p>','<p>單季</p>','<h3>上櫃公司第三季資料</h3>'])
def test_explicit_conflict(fragment):
    with pytest.raises(ValueError):h.anchored_rows('TWSE',Q,html()+fragment,NOW,anchor())

def test_mismatched_row():
    r=sample();r['營業利益（損失）']='0'
    with pytest.raises(ValueError):h.anchored_rows('TWSE',Q,html(),NOW,anchor(rows=[r]))

def test_duplicate_anchor():
    with pytest.raises(ValueError):h.anchored_rows('TWSE',Q,html(),NOW,anchor(rows=[sample(),sample()]))

def test_explicit_historical_gross_columns():
    raw=html(p=Q-1).replace('上市公司第二季資料','上市公司115年第二季綜合損益表(累計)').replace('</h2>','</h2><p>單位：千元</p>')
    assert h.parse('ytd','TWSE',Q-1,raw,NOW)[0]['gross_profit_ytd']==21229246

def test_collect_and_reaudit(tmp_path):
    def http(method,url,**kwargs):
        if url==h.QUARTER_URL:return Response(url,html('TWSE' if kwargs['data']['TYPEK']=='sii' else 'TPEX'))
        if url in [f.URLS['ytd',m] for m in h.MARKETS]:return Response(url,anchor('TWSE' if url==f.URLS['ytd','TWSE'] else 'TPEX')['raw'])
        return Response(url,monthly(market='TWSE' if '/sii/' in url else 'TPEX'))
    r=h.collect(tmp_path,now=NOW,budget=6,http=http)
    assert r['requests']==6 and sum(x.get('verification')=='DUAL_OFFICIAL_ROW_MATCH' for x in r['scope'])==2
    v=h.read_partition(h.partition(tmp_path,'ytd','TWSE',Q),'ytd','TWSE',Q)
    assert v['rows']==h.reparse_partition(v,'ytd','TWSE',Q)
    assert sum(x['狀態']=='VERIFIED' for x in scenarios.audit(tmp_path,NOW))==4
    data,errors=f.load_inputs(tmp_path);assert not errors
    l,_,_=f.build_ledgers(data,NOW);assert l['ytd','TWSE','1101'][Q]['eps_ytd']==.63
    l,_,_=f.build_ledgers(data,NOW-timedelta(seconds=1));assert not l

def test_invalid_existing_reparse(tmp_path):
    raw=html();a=anchor();rows=h.anchored_rows('TWSE',Q,raw,NOW,a);rows[0]['eps_ytd']=999
    path=h.partition(tmp_path,'ytd','TWSE',Q)
    h._persist(path,{'version':h.VERSION,'kind':'ytd','market':'TWSE','period':Q,'url':h.QUARTER_URL,'observed_at':NOW.isoformat(),'html':raw,'raw_sha256':hashlib.sha256(raw.encode()).hexdigest(),'rows':rows,'anchor':a})
    with pytest.raises(ValueError):h.read_partition(path,'ytd','TWSE',Q)
    before=path.read_bytes();r=h.collect(tmp_path,now=NOW,budget=1,http=lambda *a,**kw: (_ for _ in ()).throw(ValueError('offline')))
    assert path.read_bytes()==before and any(x['status']=='INVALID_EXISTING' for x in r['scope'])

def test_schema_retry_once(tmp_path):
    key=f'ytd:TWSE:{Q}';path=tmp_path/'data/h222/health.json'
    ar._write(path,ar._seal({'attempts':{key:{'at':NOW.isoformat(),'error':'quarter market/period/YTD/units not confirmed','status':'REJECTED'}}}))
    calls=[]
    def http(method,url,**kw):
        calls.append(url)
        if method=='POST':return Response(url,html())
        if url==f.URLS['ytd','TWSE']:return Response(url,'[]')
        return Response(url,monthly(market='TWSE' if '/sii/' in url else 'TPEX'))
    r=h.collect(tmp_path,now=NOW,budget=4,http=http);assert r['requests']==4 and r['attempts'][key]['schema_revision']=='H230'
    calls.clear();r=h.collect(tmp_path,now=NOW,budget=2,http=http);assert f.URLS['ytd','TWSE'] not in calls

def test_no_net_only_gross_or_duplicate_gross():
    for raw in [html().replace('營業毛利(毛損)</th>','其他</th>'),html().replace('營業毛利(毛損)淨額</th>','營業毛利(毛損)</th>')]:
        with pytest.raises(ValueError):h.anchored_rows('TWSE',Q,raw,NOW,anchor())

def test_95_percent_both_directions():
    codes=tuple(str(1100+i) for i in range(20))
    a=anchor(rows=[sample(code=c) for c in codes])
    assert len(h.anchored_rows('TWSE',Q,html(codes=codes[:-1]),NOW,a))==19
    with pytest.raises(ValueError):h.anchored_rows('TWSE',Q,html(codes=codes[:-2]),NOW,a)
    with pytest.raises(ValueError):h.anchored_rows('TWSE',Q,html(codes=codes),NOW,anchor(rows=[sample(code=c) for c in codes[:-2]]))

def test_explicit_currency_prefix_rejected():
    with pytest.raises(ValueError):h.anchored_rows('TWSE',Q,html()+'<p>單位：新臺幣百萬元</p>',NOW,anchor())

def test_budget_not_exceeded(tmp_path):
    calls=[]
    def http(method,url,**kwargs):
        calls.append(url)
        return Response(url,html() if method=='POST' else monthly(market='TWSE' if '/sii/' in url else 'TPEX'))
    r=h.collect(tmp_path,now=NOW,budget=3,http=http)
    assert len(calls)==r['requests']==3 and not h.partition(tmp_path,'ytd','TWSE',Q).exists()
