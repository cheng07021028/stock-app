from datetime import datetime
import io,json,zipfile
import pytest
from openpyxl import load_workbook
import godpick_h234_batch_financial as b
import godpick_h192_financial_growth as f
from test_h231_company_history import master
NOW=datetime.fromisoformat('2026-11-26T16:00:00+08:00')
_,Q=f.expected_periods(NOW)

def name(p=Q,c='2330'):return f'tifrs-fr1-m1-ci-cr-{c}-{p//4}Q{p%4+1}.html'
def fixture(p=Q,c='2330',growth=1,actual=False):
    y,n=p//4,p%4+1;e=f.period_end('ytd',p);d=f'{e.month}月{e.day}日';start=f'{y}年1月1日至{d}{y}/1/1To{e.month}/{e.day}';previous=start.replace(str(y),str(y-1));single=start.replace('1月1日',f'{max(1,e.month-2)}月1日').replace('/1/1To',f'/{max(1,e.month-2)}/1To');
    if n==1:single=f'{y}年3月31日'
    v=100000*n
    values=(2404483690,1611606116,1425568793,1279582227,49.33) if actual else (v,v*.3*growth,v*.2*growth,v*.15*growth,2*n*growth)
    labels=['營業收入合計 Total operating revenue','營業毛利（毛損） Gross profit','營業利益（損失） Net operating income','本期淨利（淨損） Profit (loss)','基本每股盈餘合計 Total basic earnings per share']
    rows=''.join(f'<tr><td>{k}</td><td>{l}</td><td>1</td><td>2</td><td>{x:,.2f}</td><td>3</td></tr>' for k,l,x in zip(b.CODES,labels,values))
    rows+='<tr><td>5950</td><td>營業毛利（毛損）淨額</td><td>1</td><td>2</td><td>1</td><td>3</td></tr>'
    return f'<html><head><title>{c} {y}Q{n} Financial report</title></head><body><h1>{c} 台灣積體電路製造股份有限公司</h1><h2>{y}年第{n}季合併財務報告</h2><p>單位：新臺幣仟元　每股盈餘單位：新台幣元 Unit: NT$ thousands EPS Unit: NT$</p><h3>綜合損益表</h3><table><tr><th>代號Code</th><th>會計項目Accounting Title</th><th>{single}</th><th>2024年4月1日至6月30日</th><th>{start}</th><th>{previous}</th></tr>{rows}</table></body></html>'.encode()

def archive(path,items):
    with zipfile.ZipFile(path,'w',compression=zipfile.ZIP_DEFLATED) as z:
        for n,v in items:z.writestr(n,v)
    return path

def test_actual_public_layout_values():
    # Real published 2330 2026Q2 visible table values, synthetic HTML representation.
    r=b.parse(fixture(Q-1,actual=True),name(Q-1),NOW)
    assert r['eps_ytd']==49.33 and r['revenue_ytd']==2404483690 and r['gross_profit_ytd']==1611606116 and r['net_profit_ytd']==1279582227

@pytest.mark.parametrize('old,new',[('2330 2026Q3 Financial report','2331 2026Q3 Financial report'),('2026年第3季合併財務報告','2026年第3季個體財務報告'),('單位：新臺幣仟元','單位：美元'),('每股盈餘單位：新台幣元','每股盈餘單位：美元'),('5900</td>','5950</td>'),('本期淨利（淨損）','母公司業主淨利'),('2026年1月1日至9月30日','2026年7月1日至9月30日')])
def test_reject_basis(old,new):
    with pytest.raises(ValueError):b.parse(fixture().decode().replace(old,new).encode(),name(),NOW)

def test_nested_zip(tmp_path):
    master(tmp_path);buf=io.BytesIO()
    with zipfile.ZipFile(buf,'w') as z:z.writestr(name(),fixture())
    z=archive(tmp_path/'bulk.zip',[('nested.zip',buf.getvalue())]);r=b.review(tmp_path,[z],NOW)
    assert len(r['rows'])==1 and r['rows'][0]['狀態']=='LOCAL_DOCUMENT_ONLY' and not r['scope'][0]['正式市場達標']
    assert not (tmp_path/'data/h222').exists()

@pytest.mark.parametrize('bad',['../bad.html','/bad.html','C:/bad.html','bad\\file.html'])
def test_zip_atomic_reject(tmp_path,bad):
    master(tmp_path);z=archive(tmp_path/'bad.zip',[(name(),fixture()),(bad,b'bad')]);r=b.review(tmp_path,[z],NOW)
    assert not r['rows'] and r['log'][0]['狀態']=='ARCHIVE_REJECTED'

def test_conflict_no_overwrite(tmp_path):
    master(tmp_path);a=archive(tmp_path/'a.zip',[(name(),fixture())]);z=archive(tmp_path/'b.zip',[(name(),fixture(growth=1.1))]);r=b.review(tmp_path,[a,z],NOW)
    assert not r['rows'] and any(x['狀態']=='CONFLICT' for x in r['log'])

def test_eight_quarters_profile_and_excel(tmp_path):
    master(tmp_path);z=archive(tmp_path/'batch.zip',[(name(p),fixture(p,growth=1.2 if p//4==Q//4 else 1)) for p in range(Q-7,Q+1)])
    r=b.review(tmp_path,[z],NOW);assert len(r['rows'])==8 and len(r['profiles'])==1
    assert r['profiles'][0]['連兩季三率年增'] is True and r['profiles'][0]['TTM EPS重建'] is not None and not r['candidates']
    w=load_workbook(io.BytesIO(b.excel(r)));assert len(w.sheetnames)==6
    assert not (tmp_path/'data').exists()

def test_missing_archive(tmp_path):
    master(tmp_path);r=b.review(tmp_path,[tmp_path/'absent.zip'],NOW);assert not r['rows'] and r['log'][0]['狀態']=='ARCHIVE_REJECTED'

def test_future_file():
    with pytest.raises(ValueError):b.parse(fixture(Q+1),name(Q+1),NOW)

def test_bulk_already_verified_target_next_gap(tmp_path,monkeypatch):
    import godpick_h233_auto_history as a
    import godpick_h231_company_history as c
    import godpick_h222_financial_history as h
    from test_h231_company_history import fixture as company_fixture
    from test_h222_financial_history import quarterly,Response
    from hashlib import sha256
    master(tmp_path);monkeypatch.setattr(h,'LIMITS',{'TWSE':1,'TPEX':1})
    raw=quarterly(p=Q).replace('1000</td>','2330</td>',1);rows=h.parse('ytd','TWSE',Q,raw,NOW)
    h._persist(h.partition(tmp_path,'ytd','TWSE',Q),{'version':h.VERSION,'kind':'ytd','market':'TWSE','period':Q,'url':h.QUARTER_URL,'observed_at':NOW.isoformat(),'raw_sha256':sha256(raw.encode()).hexdigest(),'html':raw,'rows':rows})
    queue=a.plan(tmp_path,NOW,stocks=[],previous={},attempts={});r=queue['queue'][0]
    assert r['股票代號']=='2330' and r['下個可補期間']==f.qstr(Q-1) and r['全市場已核實期數']==1
    calls=[]
    def http(method,url,**kw):
        calls.append(kw['data']['season']);return Response(url,company_fixture(Q-1,'2330'))
    r=c.collect(tmp_path,codes=['2330'],target_period=Q-1,now=NOW,budget=1,http=http)
    assert r['status']=='COMPANY_TARGET_VERIFIED' and calls==['02'] and c.path_for(tmp_path,'TWSE','2330',Q-1).exists() and not c.path_for(tmp_path,'TWSE','2330',Q).exists()


def test_target_scope_guard(tmp_path):
    import godpick_h231_company_history as c
    master(tmp_path)
    for target in [Q+1,Q-8]:
        with pytest.raises(ValueError):c.collect(tmp_path,codes=['2330'],target_period=target,now=NOW,budget=1,http=lambda *a,**k:pytest.fail('unexpected request'))


def test_named_growth_candidate(tmp_path):
    master(tmp_path);z=archive(tmp_path/'batch.zip',[(name(p),fixture(p,growth=1.2 if p//4==Q//4 else 1)) for p in range(Q-7,Q+1)])
    m,_=f.expected_periods(NOW);path=tmp_path/'data/h115/revenue.jsonl';path.parent.mkdir(parents=True)
    path.write_text('\n'.join(json.dumps({'code':'2330','source':'TWSE','source_url':f.URLS['revenue','TWSE'],'availability_basis':'FIRST_OBSERVED','available_at':NOW.isoformat(),'year_month':f.monthstr(i),'revenue_twd':1000000+(i-(m-12))*100000,'previous_month_twd':None,'prior_year_month_twd':None}) for i in range(m-12,m+1)))
    r=b.review(tmp_path,[z],NOW)
    assert len(r['candidates'])==1 and r['candidates'][0]['股票代號']=='2330' and r['candidates'][0]['資料性質'].startswith('本機文件')
