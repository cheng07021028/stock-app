from datetime import datetime,timedelta
import json,hashlib
import pytest
import godpick_h231_company_history as c
import godpick_h222_financial_history as h
import godpick_h192_financial_growth as f
import godpick_h223_growth_scenarios as scenarios
from test_h222_financial_history import Response
NOW=datetime.fromisoformat('2026-11-26T14:00:00+08:00'); M,Q=f.expected_periods(NOW)

def fixture(p=Q,code='2330',gain=1):
    y=p//4-1911;n=p%4+1;end=f.period_end('ytd',p);start=f'{y}年01月01日至{y}年{end.month:02d}月{end.day}日';prior=f'{y-1}年01月01日至{y-1}年{end.month:02d}月{end.day}日';v=100000*n
    # Synthetic values and layout; no actual stock financial performance asserted.
    rows=[('營業收入合計',v),('營業毛利(毛損)',v*.35*gain),('營業毛利(毛損)淨額',v*.30),('營業利益(損失)',v*.20*gain),('本期淨利(淨損)',v*.15*gain),('基本每股盈餘(元)',2*n*gain)]
    return f'<h2>公司代號：{code}</h2><h2>合併綜合損益表 民國{y}年第{n}季</h2><p>單位：新台幣仟元</p><table><tr><th>會計項目</th><th colspan="2">{start}</th><th colspan="2">{prior}</th></tr><tr><th>科目</th><th>金額</th><th>%</th><th>金額</th><th>%</th></tr>'+''.join(f'<tr><td>{k}</td><td>{x:,.2f}</td><td>99</td><td>1</td><td>1</td></tr>' for k,x in rows)+'</table>'

def master(root):
    (root/'stock_master_cache.json').write_text(json.dumps([{'code':'2330','name':'台積電','market':'TWSE'},{'code':'3105','name':'穩懋','market':'TPEX'}]))

def persist(root,p=Q,t=NOW,gain=1):
    html=fixture(p,gain=gain);row=c.parse(html,'2330',p,t)
    h._persist(c.path_for(root,'TWSE','2330',p),{'version':c.VERSION,'url':c.URL,'market':'TWSE','code':'2330','period':p,'observed_at':t.isoformat(),'html':html,'raw_sha256':hashlib.sha256(html.encode()).hexdigest(),'row':row});return row

def test_explicit_cumulative_amount():
    r=c.parse(fixture(),'2330',Q,NOW);assert r['revenue_ytd']==300000 and r['gross_profit_ytd']==105000 and r['eps_ytd']==6

@pytest.mark.parametrize('fragment,replace',[('2330','2331'),('合併綜合損益表','個體綜合損益表'),('115年第3季','114年第3季'),('新台幣仟元','美元'),('115年01月01日','115年07月01日'),('<th>金額</th>','<th>%</th>'),('營業毛利(毛損)</td>','營業毛利(毛損)淨額</td>'),('本期淨利(淨損)','歸屬母公司淨利')])
def test_fail_closed(fragment,replace):
    with pytest.raises(ValueError):c.parse(fixture().replace(fragment,replace),'2330',Q,NOW)

def test_company_not_market_coverage(tmp_path):
    master(tmp_path);persist(tmp_path)
    data,errors=f.load_inputs(tmp_path);assert not errors and len(data['ytd'])==1
    l,_,_=f.build_ledgers(data,NOW);assert l['ytd','TWSE','2330'][Q]['eps_ytd']==6
    l,_,_=f.build_ledgers(data,NOW-timedelta(seconds=1));assert not l
    assert all(x['狀態']=='MISSING' for x in scenarios.audit(tmp_path,NOW))

def test_complete_8_quarters_three_margins(tmp_path):
    master(tmp_path)
    for p in range(Q-7,Q+1):persist(tmp_path,p,gain=1.1 if p//4==Q//4 else 1)
    data,errors=f.load_inputs(tmp_path);assert not errors
    l,_,_=f.build_ledgers(data,NOW)
    r=f.financial_profile({},l['margin','TWSE','2330'],l['ytd','TWSE','2330'],{},M,Q,'')
    assert r['連兩季三率年增'] is True and r['TTM EPS重建'] is not None

def test_no_mixed_family_difference(tmp_path,monkeypatch):
    master(tmp_path);persist(tmp_path)
    monkeypatch.setattr(h,'LIMITS',{'TWSE':1,'TPEX':1})
    from test_h230_quarter_evidence import html
    raw=html(codes=('2330',),p=Q-1).replace('上市公司第二季資料','上市公司115年第二季綜合損益表(累計)').replace('</h2>','</h2><p>單位：千元</p>')
    rows=h.parse('ytd','TWSE',Q-1,raw,NOW)
    h._persist(h.partition(tmp_path,'ytd','TWSE',Q-1),{'version':h.VERSION,'kind':'ytd','market':'TWSE','period':Q-1,'url':h.QUARTER_URL,'observed_at':NOW.isoformat(),'html':raw,'raw_sha256':hashlib.sha256(raw.encode()).hexdigest(),'rows':rows})
    d,e=f.load_inputs(tmp_path);l,_,_=f.build_ledgers(d,NOW);assert Q not in l['margin','TWSE','2330'];assert f.single_q(l['ytd','TWSE','2330'],Q,'eps_ytd') is None

def test_future_conflict_cannot_erase_old(tmp_path,monkeypatch):
    master(tmp_path);persist(tmp_path,t=NOW+timedelta(hours=1));monkeypatch.setattr(h,'LIMITS',{'TWSE':1,'TPEX':1})
    from test_h230_quarter_evidence import html
    raw=html(codes=('2330',),p=Q).replace('上市公司第三季資料','上市公司115年第三季綜合損益表(累計)').replace('</h2>','</h2><p>單位：千元</p>');rows=h.parse('ytd','TWSE',Q,raw,NOW)
    h._persist(h.partition(tmp_path,'ytd','TWSE',Q),{'version':h.VERSION,'kind':'ytd','market':'TWSE','period':Q,'url':h.QUARTER_URL,'observed_at':NOW.isoformat(),'html':raw,'raw_sha256':hashlib.sha256(raw.encode()).hexdigest(),'rows':rows})
    d,e=f.load_inputs(tmp_path);l,_,_=f.build_ledgers(d,NOW);assert l['ytd','TWSE','2330'][Q]['eps_ytd']==.63
    l,_,conf=f.build_ledgers(d,NOW+timedelta(hours=1));assert Q not in l['ytd','TWSE','2330'] and conf['ytd']==1

def test_bounded_collection_no_repeat(tmp_path):
    master(tmp_path);calls=[]
    def http(method,url,**kw):
        calls.append(kw['data']);p=(int(kw['data']['year'])+1911)*4+int(kw['data']['season'])-1;return Response(url,fixture(p,kw['data']['co_id']))
    r=c.collect(tmp_path,codes=['2330'],now=NOW,budget=2,http=http);assert r['requests']==2 and not r['market_scope_verified'] and len(calls)==2
    r=c.collect(tmp_path,codes=['2330'],now=NOW,budget=2,http=http);assert r['requests']==2 and len(calls)==4 and sum(x['狀態']=='VERIFIED' for x in r['scope'])==2

def test_failure_budget_and_cooldown(tmp_path):
    master(tmp_path);r=c.collect(tmp_path,codes=['2330'],now=NOW,budget=8,http=lambda *a,**k:Response(c.URL,'invalid'));assert r['requests']==2 and sum(x['狀態']=='REJECTED' for x in r['scope'])==2
    r=c.collect(tmp_path,codes=['2330'],now=NOW,budget=1,http=lambda *a,**k:Response(c.URL,'invalid'));assert r['requests']==1

def test_invalid_code_and_cap(tmp_path):
    master(tmp_path)
    for codes,budget in [(['9999'],8),(['2330'],9),([],8)]:
        with pytest.raises(ValueError):c.collect(tmp_path,codes=codes,budget=budget,now=NOW)

def test_exact_unique_provider_name():
    html=fixture().replace('公司代號：2330','本資料由台積電公司提供')
    assert c.parse(html,'2330',Q,NOW,'臺積電')['eps_ytd']==6
    with pytest.raises(ValueError):c.parse(html,'2330',Q,NOW,'台積電二')
    with pytest.raises(ValueError):c.parse(html,'2330',Q,NOW)
    assert c.unique_name({'a':{'股票名稱':'台積電'},'b':{'股票名稱':'臺積電'}},'a') is None

def test_bad_provider_with_correct_code():
    with pytest.raises(ValueError):c.parse(fixture()+'<p>本資料由其他公司提供</p>','2330',Q,NOW,'台積電')
