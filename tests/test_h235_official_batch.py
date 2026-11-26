from datetime import timedelta
import io,json,zipfile
import pytest
from test_h234_batch_financial import NOW,Q,name,fixture,archive
from test_h231_company_history import master
import godpick_h235_official_batch as b
import godpick_h192_financial_growth as f
import godpick_h222_financial_history as h

def url(p=Q):return f'https://mopsov.twse.com.tw/mops/server-java/FileDownLoad?step=9&fileName=tifrs-{p//4}Q{p%4+1}.zip&filePath=/home/html/nas/ifrs/{p//4}/'
def raw(p=Q,items=None):
    a=io.BytesIO()
    with zipfile.ZipFile(a,'w',zipfile.ZIP_DEFLATED) as z:
        for n,r in items or [(name(p),fixture(p))]:z.writestr(n,r)
    return a.getvalue()
class Response:
    def __init__(self,u,blob,status=200,history=None,headers=None):self.url=u;self.blob=blob;self.status_code=status;self.history=history or [];self.headers=headers if headers is not None else {'Content-Length':str(len(blob))}
    def __enter__(self):return self
    def __exit__(self,*a):pass
    def iter_content(self,n):yield self.blob

def setup(root,monkeypatch):
    master(root);monkeypatch.setattr(b,'MIN_ROWS',{'TWSE':1,'TPEX':1})

def collect(root,p=Q,blob=None):return b.collect(root,p,url(p),now=NOW,http=lambda u,**kw:Response(u,blob if blob is not None else raw(p)))

@pytest.mark.parametrize('bad',[lambda u:u.replace('https:','http:'),lambda u:u.replace('mopsov.twse.com.tw','mopsov.twse.com.tw.example.com'),lambda u:u.replace('step=9','step=10'),lambda u:u.replace('filePath=/home/html/nas/ifrs/','filePath=/etc/'),lambda u:u+'&step=9',lambda u:u+'#x',lambda u:u.replace('mopsov.twse.com.tw','mopsov.twse.com.tw:443')])
def test_url_reject(bad):
    with pytest.raises(ValueError):b.validate_url(bad(url()),Q)

def test_direct_and_noop(tmp_path,monkeypatch):
    setup(tmp_path,monkeypatch);calls=[]
    def http(u,**kw):
        calls.append(kw);assert kw['verify'] is True and kw['stream'] and not kw['allow_redirects'];return Response(u,raw())
    r=b.collect(tmp_path,Q,url(),now=NOW,http=http);assert r['status']=='VERIFIED_SAVED' and not r['market_scope_verified']
    assert b.collect(tmp_path,Q,url(),now=NOW,http=lambda *a,**k:pytest.fail('must not refetch'))['status']=='ALREADY_VERIFIED'
    assert len(calls)==1
    rr,err,scope=b.load(tmp_path);assert not err and ('TWSE','2330',Q) in rr and scope[0]['正式市場達標'] is False

def test_no_local_zip_elevation(tmp_path,monkeypatch):
    setup(tmp_path,monkeypatch);z,r=b.paths(tmp_path,Q);z.parent.mkdir(parents=True);z.write_bytes(raw());data,errors,_=b.load(tmp_path);assert not data and errors
    with pytest.raises(ValueError):collect(tmp_path)

def test_modified_raw_rejected(tmp_path,monkeypatch):
    setup(tmp_path,monkeypatch);collect(tmp_path);z,_=b.paths(tmp_path,Q);z.write_bytes(raw(Q-1));assert not b.load(tmp_path)[0] and b.load(tmp_path)[1]

@pytest.mark.parametrize('kind',['redirect','wrong_url','truncated','wrong_quarter','unsafe','too_few'])
def test_fail_closed(tmp_path,monkeypatch,kind):
    setup(tmp_path,monkeypatch);blob=raw();kwargs={};u=url()
    if kind=='redirect':kwargs['status']=302
    if kind=='wrong_url':u=url(Q-1)
    if kind=='truncated':kwargs['headers']={'Content-Length':str(len(blob)+1)}
    if kind=='wrong_quarter':blob=raw(Q-1)
    if kind=='unsafe':blob=raw(items=[('../'+name(),fixture())])
    if kind=='too_few':monkeypatch.setattr(b,'MIN_ROWS',{'TWSE':2,'TPEX':2})
    with pytest.raises(ValueError):b.collect(tmp_path,Q,url(),now=NOW,http=lambda *a,**k:Response(u,blob,**kwargs))
    assert not b.paths(tmp_path,Q)[1].exists() and not b.load(tmp_path)[0]

def test_eight_periods_and_asof(tmp_path,monkeypatch):
    setup(tmp_path,monkeypatch)
    for p in range(Q-7,Q+1):collect(tmp_path,p,raw(p,[(name(p),fixture(p,growth=1.2 if p//4==Q//4 else 1))]))
    d,e=f.load_inputs(tmp_path);assert not e;led,_,_=f.build_ledgers(d,NOW);yy=led['ytd','TWSE','2330'];mm=led['margin','TWSE','2330'];assert len(yy)==8
    m,_=f.expected_periods(NOW);profile=f.financial_profile({},mm,yy,{},m,Q,'');assert profile['連兩季三率年增'] is True and profile['TTM EPS重建'] is not None
    old,_,_=f.build_ledgers(d,NOW-timedelta(seconds=1));assert not old['ytd','TWSE','2330']
    assert len(h.inspect(tmp_path,NOW)['batch_scope'])==8

def test_cross_source_conflict(tmp_path,monkeypatch):
    setup(tmp_path,monkeypatch);collect(tmp_path);d,_=f.load_inputs(tmp_path);r=d['ytd'][0];other={**r,'source_url':h.QUARTER_URL,'historical_source':h.VERSION,'eps_ytd':r['eps_ytd']+1,'history_financial_values':{**r['history_financial_values'],'eps_ytd':r['eps_ytd']+1}}
    d['ytd'].append(other);led,_,conf=f.build_ledgers(d,NOW);assert Q not in led['ytd','TWSE','2330'] and conf['ytd']

def test_queue_excludes_batch_gap(tmp_path,monkeypatch):
    setup(tmp_path,monkeypatch);collect(tmp_path)
    import godpick_h233_auto_history as a
    assert ('TWSE','2330',Q) in a.bulk_periods(tmp_path,NOW,8)

def test_named_growth_and_coherent_source_selection(tmp_path,monkeypatch):
    setup(tmp_path,monkeypatch)
    for p in range(Q-7,Q+1):collect(tmp_path,p,raw(p,[(name(p),fixture(p,growth=1.2 if p//4==Q//4 else 1))]))
    m,_=f.expected_periods(NOW);path=tmp_path/'data/h115/revenue.jsonl';path.parent.mkdir(parents=True)
    path.write_text('\n'.join(json.dumps({'code':'2330','source':'TWSE','source_url':f.URLS['revenue','TWSE'],'availability_basis':'FIRST_OBSERVED','available_at':NOW.isoformat(),'year_month':f.monthstr(i),'revenue_twd':1000000+(i-(m-12))*100000,'previous_month_twd':None,'prior_year_month_twd':None}) for i in range(m-12,m+1)))
    d,_=f.load_inputs(tmp_path);r=next(r for r in d['ytd'] if r['year_quarter']==f.qstr(Q));d['ytd'].append({**r,'source_url':h.QUARTER_URL,'historical_source':h.VERSION})
    led,_,_=f.build_ledgers(d,NOW);assert led['ytd','TWSE','2330'][Q]['historical_source']==b.VERSION
    res=h.inspect(tmp_path,NOW);assert len(res['candidates'])==1 and res['candidates'][0]['股票代號']=='2330' and not res['h232_candidates']
    from openpyxl import load_workbook
    assert '10_H235官方批次' in load_workbook(io.BytesIO(h.excel(res))).sheetnames
    assert not h.inspect(tmp_path,NOW-timedelta(seconds=1))['batch_scope']
