from datetime import datetime,date,timedelta
from pathlib import Path
import pytest,io
import godpick_h194_performance as h
from test_h190_market_archive import raw
D='2026-11-20'; NOW=datetime.fromisoformat('2026-11-23T23:45:00+08:00')
def setup_market(root,days=(D,'2026-11-23'),missing=False):
    a=h.ar; p=root/'data/h190'
    a.save_calendar(p,a.parse_calendar({'stat':'OK','fields':['日期','收盤指數'],'data':[[d,30000] for d in days]},'2026-11',observed_at=NOW.isoformat()))
    for d in days:
        if missing and d!=D:continue
        r=raw(d);row=r['tables'][0]['data'][0];row[5:8]=['105','99','105'] if d!=D else ['101','99','100']
        a.save_day(p,a.parse_daily(r,'TWSE',d,observed_at=f'{d}T21:00:00+08:00'))
    return h.Market(root,NOW)
def signal(day=D,code='1000',tier=h.TIERS[0]):
    return {'signal_day':day,'market':'TWSE','code':code,'name':'測試','tier':tier,'base_close':100.,'created_at':day+'T22:00:00+08:00','forecast':None,'tech_pools':'技術突破','financial_pools':'營收成長','authority':'RESEARCH_ONLY'}
def test_exact_calendar_and_t1(tmp_path):
    m=setup_market(tmp_path);s=signal();ss={h.sid(s):s};o,g=h.settle(ss,{},m,NOW)
    assert len(o)==1 and o[0]['target_day']=='2026-11-23' and o[0]['label']=='上漲' and o[0]['raw_return']==pytest.approx(.05)
    assert sum(g.values())==4
    assert not h.settle(ss,{h.okey(o[0]):o[0]},m,NOW)[0]
def test_no_missing_price(tmp_path):
    m=setup_market(tmp_path,missing=True);s=signal();assert not h.settle({h.sid(s):s},{},m,NOW)[0]
def test_base_conflict(tmp_path):
    m=setup_market(tmp_path);s=signal();s['base_close']=99;assert not h.settle({h.sid(s):s},{},m,NOW)[0]
def test_class_edges():
    assert h.label(.005)=='盤整' and h.label(-.005)=='盤整' and h.label(.0051)=='上漲' and h.label(-.0051)=='下跌'
def result(day='2026-11-23'):
    return {'asof':NOW.isoformat(),'market_day':day,'same_session':True,'pools':{p:([{'市場':'TWSE','股票代號':'1000','股票名稱':'=1+1','技術研究池':'技術突破','財報研究池':'營收成長','研究順序':1}] if p==h.TIERS[0] else []) for p in h.TIERS}}
def test_freeze_and_integrity(tmp_path):
    m=setup_market(tmp_path);model=h.models({}, {},NOW);body,_=h.freeze(result(),m,model,NOW);assert body['rows'][0]['forecast'] is None
    p=tmp_path/'data/h194/signals/2026-11-23.json.gz';h.immutable(p,body);ss,oo=h.read_ledger(tmp_path);assert len(ss)==1 and not oo
    with pytest.raises(ValueError):h.immutable(p,body)
    body['rows'][0]['forecast']=[.1,.2,.7];h.ar._write(p,h.ar._seal(body))
    with pytest.raises(ValueError):h.read_ledger(tmp_path)
def test_stale_not_backdated(tmp_path):
    m=setup_market(tmp_path);assert h.freeze(result(D),m,h.models({}, {},NOW),NOW)[0] is None
def test_future_joint_not_used(tmp_path):
    m=setup_market(tmp_path);r=result();r['asof']='2026-11-24T00:00:00+08:00';assert h.freeze(r,m,h.models({}, {},NOW),NOW)[0] is None
def test_no_market_mix(tmp_path):
    m=setup_market(tmp_path);r=result();r['pools'][h.TIERS[0]][0]['市場']='TPEX';assert h.freeze(r,m,h.models({}, {},NOW),NOW)[0] is None
def model_fixture(late=False):
    ss={};oo={};start=date(2026,9,1)
    for i in range(32):
        day=(start+timedelta(days=i)).isoformat();after=(start+timedelta(days=i+1)).isoformat()
        for j in range(20):
            tier=h.TIERS[0] if j<10 else h.TIERS[1];s=signal(day,str(1000+j),tier);ss[h.sid(s)]=s
            lbl='上漲' if (j<8 or j==19) else '下跌' if (j==8 or 10<=j<18) else '盤整'
            o={**s,'horizon':1,'label':lbl,'resolved_at':('2026-11-23' if late else after)+'T20:00:00+08:00'};oo[h.okey(o)]=o
    return ss,oo
def test_model_validated_and_no_lookahead():
    s,o=model_fixture();m=h.models(s,o,NOW)['TWSE|'+h.TIERS[0]];assert m['probabilities'] is not None and m['holdout_brier']<m['baseline_brier'] and sum(m['probabilities'])==pytest.approx(1)
    s,o=model_fixture(True);assert h.models(s,o,NOW)['TWSE|'+h.TIERS[0]]['probabilities'] is None

def test_model_no_edge():
    s,o=model_fixture()
    for r in o.values():r['label']='上漲' if int(r['code'])%10<8 else '下跌' if int(r['code'])%10==8 else '盤整'
    m=h.models(s,o,NOW)['TWSE|'+h.TIERS[0]];assert m['probabilities'] is None and '未優於' in m['status']

def test_report_no_benchmark_fabrication(tmp_path):
    m=setup_market(tmp_path);s=signal();ss={h.sid(s):s};oo,g=h.settle(ss,{},m,NOW);o=h.report(ss,{h.okey(r):r for r in oo},m,NOW,h.models({}, {},NOW),g);assert o['mature']==1 and o['outcomes'][0]['excess_return'] is None and o['pending']==4
    import openpyxl
    w=openpyxl.load_workbook(io.BytesIO(h.excel(o)));assert len(w.sheetnames)==6

def test_empty_run(tmp_path):
    r=h.run(tmp_path,now=NOW);assert r['signals']==0 and r['mature']==0 and r['new_outcomes']==0

def test_financial_only_can_start_early(tmp_path):
    m=setup_market(tmp_path);r=result('');r['same_session']=False
    r['pools'][h.TIERS[2]]=r['pools'][h.TIERS[0]];r['pools'][h.TIERS[0]]=[]
    body,_=h.freeze(r,m,h.models({}, {},NOW),NOW);assert body['rows'][0]['tier']==h.TIERS[2]
def test_incomplete_mature_coverage():
    s,o=model_fixture();o={k:v for k,v in o.items() if int(v['code'])%10<8};m=h.models(s,o,NOW)['TWSE|'+h.TIERS[0]];assert m['probabilities'] is None

def test_end_to_end_idempotent(tmp_path,monkeypatch):
    import godpick_h193_joint_decision as joint
    setup_market(tmp_path);monkeypatch.setattr(joint,'run',lambda *a,**k:(result(),{}))
    r=h.run(tmp_path,NOW);p=tmp_path/'data/h194/signals/2026-11-23.json.gz';old=p.read_bytes();assert r['signals']==1
    r=h.run(tmp_path,NOW+timedelta(minutes=1));assert p.read_bytes()==old and r['signals']==1 and '不可覆寫' in r['capture_status']
    assert not (tmp_path/'godpick_records.json').exists()

def test_verified_index_only(tmp_path):
    import gzip,json
    import godpick_h117_official_market_closure as b
    m=setup_market(tmp_path);p=tmp_path/'data/h117/daily/TAIEX';p.mkdir(parents=True)
    for d,c in [(D,30000),('2026-11-23',30300)]:
        v=b.make_official_day_manifest({'source':'TAIEX','day':d,'source_url':b.MARKET_URLS['TAIEX'],'rows':[{'code':'BENCHMARK','date':d,'exchange':'TAIEX','close':c}]},collected_at=d+'T22:00:00+08:00')
        (p/(d+'.json.gz')).write_bytes(gzip.compress(json.dumps(v).encode()))
    s=signal();ss={h.sid(s):s};oo,g=h.settle(ss,{},m,NOW);r=h.report(ss,{h.okey(x):x for x in oo},m,NOW,h.models({}, {},NOW));assert r['outcomes'][0]['excess_return']==pytest.approx(.04)
    assert r['outcomes'][0]['benchmark']=='TAIEX_PRICE_INDEX'

def test_exact_tn_spans(tmp_path):
    # Synthetic official session dates include weekend gaps; target is Nth session.
    days=['2026-11-16','2026-11-17','2026-11-18','2026-11-19','2026-11-20','2026-11-23']
    m=setup_market(tmp_path,days=days);s=signal(days[0]);s['base_close']=105;ss={h.sid(s):s};oo,g=h.settle(ss,{},m,NOW)
    assert {o['horizon']:o['target_day'] for o in oo}=={1:days[1],3:days[3],5:days[5]}

def test_preflight_guard(tmp_path,monkeypatch):
    from tools import h194_preflight as p
    root=tmp_path/'root';payload=tmp_path/'payload';root.mkdir();payload.mkdir()
    before=b'base';after=b'updated';added=b'new';dep=b'h193'
    monkeypatch.setattr(p,'BASE',{'page.py':p.gitsha(before),'new.py':None});monkeypatch.setattr(p,'DEPENDENCIES',{'h193.py':p.gitsha(dep)})
    (root/'page.py').write_bytes(before);(root/'h193.py').write_bytes(dep);(payload/'page.py').write_bytes(after);(payload/'new.py').write_bytes(added)
    (payload/'H194_SHA256SUMS.txt').write_text(p.sha(after)+'  page.py\n'+p.sha(added)+'  new.py\n')
    assert p.check(root,payload)[1]==['page.py','new.py'];bak=p.apply(root,payload);assert (bak/'page.py').read_bytes()==before;assert p.apply(root,payload) is None
    (root/'page.py').write_bytes(b'unknown');assert p.check(root,payload)[2];assert (root/'page.py').read_bytes()==b'unknown'
