# -*- coding: utf-8 -*-
"""Offline contract regressions; do not touch production data or network."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import ast, zipfile
import pytest
import yaml
import apply_h247 as a
from godpick_h247_source_health import fair_scope

NOW=datetime(2026,11,27,11,0,tzinfo=timezone.utc)
SCOPE=[('revenue','TWSE',1),('revenue','TPEX',1),('ytd','TWSE',2),('ytd','TPEX',2)]
CODE='SecuritiesCompanyCode'; FOREIGN='Foreign Investors include Mainland Area Investors (Foreign Dealers excluded)-Difference'; TRUST='SecuritiesInvestmentTrustCompanies-Difference'; TOTAL='TotalDifference'
LEGACY_FIELDS="""TPEX_FIELDS={
 'code':('證券代號','股票代號','SecuritiesCode','代號'),
 'foreign':('外資及陸資買賣超股數(不含外資自營商)','外資及陸資淨買賣超股數(不含自營商)','外資及陸資淨買股數(不含自營商)','外資及陸資不含外資自營商買賣超股數','外資及陸資買賣超股數'),
 'trust':('投信買賣超股數','投信淨買賣超股數','投信淨買股數'),
 'total':('三大法人買賣超股數合計','三大法人買賣超股數'),
 'day':('資料日期','交易日期','報表日期','Date'),
}
"""

def fields():
    # Actual H124 dictionary excerpt from audited GitHub source. Exercise transformed dictionary.
    txt=a.transform('godpick_h124_ownership_flow.py',LEGACY_FIELDS)
    return ast.literal_eval(ast.parse(txt).body[0].value)

def mapping(headers):
    # H124 exact-name cardinality and foreign-exclusive selection contract.
    f=fields()
    def field(names):
        found=[x for x in names if x in headers]
        if len(found)!=1:raise ValueError('ambiguous/missing official field')
        return found[0]
    out={k:field(v) for k,v in f.items() if k not in ('day','foreign')}
    exclusive=[x for x in f['foreign'][:-1] if x in headers]
    out['foreign']=field(exclusive or f['foreign']);out['day']=field(f['day']);return out

def test_english_official_schema():
    h=[CODE,FOREIGN,TRUST,TOTAL,'Date','ForeignInvestorsInclude MainlandAreaInvestors-Difference']
    assert mapping(h)=={'code':CODE,'trust':TRUST,'total':TOTAL,'foreign':FOREIGN,'day':'Date'}

def test_preserve_chinese_legacy():
    f=fields();h=[f['code'][0],f['foreign'][0],f['trust'][0],f['total'][0],f['day'][0]]
    assert mapping(h)['foreign']==f['foreign'][0]

def test_foreign_aggregate_remains_last():assert fields()['foreign'][-1]=='外資及陸資買賣超股數'

def test_foreign_exclusive_is_not_aggregate():
    assert mapping([CODE,FOREIGN,TRUST,TOTAL,'Date','外資及陸資買賣超股數'])['foreign']==FOREIGN

@pytest.mark.parametrize('missing',[CODE,FOREIGN,TRUST,TOTAL,'Date'])
def test_missing_never_guessed(missing):
    h=[CODE,FOREIGN,TRUST,TOTAL,'Date','ForeignInvestorsInclude MainlandAreaInvestors-Difference']
    with pytest.raises(ValueError):mapping([v for v in h if v!=missing])

def test_duplicate_alias_rejected():
    with pytest.raises(ValueError):mapping([CODE,'證券代號',FOREIGN,TRUST,TOTAL,'Date'])

def test_duplicate_exclusive_rejected():
    with pytest.raises(ValueError):mapping([CODE,FOREIGN,fields()['foreign'][0],TRUST,TOTAL,'Date'])

def ts(v):return datetime.fromisoformat(v) if v else None

def test_fair_first_run_same_order():assert fair_scope(SCOPE,{},now=NOW,parse_time=ts)==SCOPE

def test_never_attempted_first():
    d={'revenue:TWSE:1':{'at':(NOW-timedelta(days=1)).isoformat()}}
    assert fair_scope(SCOPE,d,now=NOW,parse_time=ts)==SCOPE[1:]+SCOPE[:1]

def test_oldest_first_preserves_all():
    d={f'{k}:{m}:{p}':{'at':(NOW-timedelta(days=i)).isoformat()} for i,(k,m,p) in enumerate(SCOPE)}
    assert fair_scope(SCOPE,d,now=NOW,parse_time=ts)==list(reversed(SCOPE))

def test_future_last_no_mutation():
    d={'revenue:TPEX:1':{'at':(NOW+timedelta(days=1)).isoformat()}}
    original=dict(d);got=fair_scope(SCOPE,d,now=NOW,parse_time=ts)
    assert got[-1]==SCOPE[1] and d==original and set(got)==set(SCOPE)

def test_bad_timestamp_handled():
    d={'revenue:TWSE:1':{'at':'invalid'}}
    assert fair_scope(SCOPE,d,now=NOW,parse_time=ts)==SCOPE

def test_naive_clock_rejected():
    with pytest.raises(ValueError):fair_scope(SCOPE,{},now=NOW.replace(tzinfo=None),parse_time=ts)

def test_bad_ledger_rejected():
    with pytest.raises(ValueError):fair_scope(SCOPE,[],now=NOW,parse_time=ts)

def workflow_fixture(path):
    # Minimal workflow with every exact source anchor in its actual structural context.
    if path.endswith('h221_official_chips.yml'):
        return "name: test\non:\n  push:\n    paths:\n      - 'godpick_h221_evidence_closure.py'\n  schedule:\n    - cron: '40 12 * * 1-5' # Taipei 20:40\njobs:\n  j:\n    steps:\n      - run: echo original\n"
    if path.endswith('h222_financial_history.yml'):
        return "name: test\non:\n  push:\n    paths:\n      - 'godpick_h222_financial_history.py'\njobs:\n  j:\n    steps:\n      - run: echo original\n"
    cron="  schedule:\n    - cron: '15 1 * * *'\n" if path.endswith('h206_market_gap.yml') else ''
    p=a.OLD_PIP+(" 'beautifulsoup4>=4.12,<5' 'lxml>=5.4,<7'" if path.endswith('h224_growth_resonance.yml') else '')
    return 'name: test\non:\n  workflow_dispatch:\n'+cron+'jobs:\n  j:\n    steps:\n'+p+'\n      - run: echo original\n'

@pytest.mark.parametrize('path',[p for p in a.PATCHES if p.endswith('.yml')])
def test_workflow_valid_and_preserves_original(path):
    s=workflow_fixture(path);got=a.transform(path,s);y=yaml.safe_load(got)
    assert y['jobs']['j']['steps'][-1]['run']=='echo original'
    if 'verify shared' in got:
        for name in ('lxml','beautifulsoup4','html5lib','openpyxl'):assert name in got
        assert len(y['jobs']['j']['steps'])==3
    if path.endswith('h206_market_gap.yml'):assert "5 14 * * 1-5" in got
    if path.endswith('h221_official_chips.yml'):assert 'godpick_h124_ownership_flow.py' in got

@pytest.mark.parametrize('path',[a.WF+'h195_daily_command.yml',a.WF+'h220_review.yml'])
def test_h244_exact_compatibility(tmp_path,monkeypatch,path):
    s=workflow_fixture(path);monkeypatch.setitem(a.BASE,path,a.blob(s.encode()))
    target=tmp_path/path;target.parent.mkdir(parents=True);target.write_text(s.replace(a.OLD_PIP,a.H244_PIP),'utf-8')
    assert a.source(tmp_path,path)==s
    got=a.transform(path,a.source(tmp_path,path));assert got.count('lxml>=5.4,<7')==1
    target.write_text(target.read_text('utf-8')+'# other edit\n','utf-8')
    with pytest.raises(ValueError):a.source(tmp_path,path)

def test_wrong_version_blocked(tmp_path):
    n='godpick_h124_ownership_flow.py';(tmp_path/n).write_text(LEGACY_FIELDS,'utf-8')
    with pytest.raises(ValueError):a.source(tmp_path,n)

def test_duplicate_anchor_blocked():
    with pytest.raises(ValueError):a.transform('godpick_h124_ownership_flow.py',LEGACY_FIELDS*2)

def test_prerequisite_blocked(tmp_path):
    with pytest.raises(ValueError,match='prerequisites'):a.create(tmp_path,tmp_path/'output.zip')
    assert not (tmp_path/'output.zip').exists()

def test_h222_guard_unchanged_in_patch():
    old,new=a.PATCHES['godpick_h222_financial_history.py'][1]
    assert old.split('\n')[0]==new.split('\n')[0]
    assert "row['status']='PENDING'" in new and 'pending_reason' in new

def test_h206_report_keeps_original_summary():
    old,new=a.PATCHES['tools/h206_daily_review.py'][0]
    assert old.split(';return')[0] in new and 'json.dumps(health' in new


def test_overlay_creation_synthetic_tree(tmp_path,monkeypatch):
    root=tmp_path/'project';root.mkdir()
    for n in ('godpick_h245_list_authority.py','godpick_h246_seal_consistency.py'):(root/n).write_text('# prerequisite fixture\n')
    (root/'godpick_auto_update_tasks.py').write_text('# run_check(result, BASE_DIR)\n')
    for path in a.PATCHES:
        t=root/path;t.parent.mkdir(parents=True,exist_ok=True)
        if path.endswith('.yml'):s=workflow_fixture(path)
        elif path=='godpick_h124_ownership_flow.py':s=LEGACY_FIELDS
        elif path=='godpick_h222_financial_history.py':
            s="def collect():\n"+a.PATCHES[path][0][0]+"{}\n"+a.PATCHES[path][1][0]+"\n"
        else:s="def main():\n    "+a.PATCHES[path][0][0]+"\n"
        t.write_text(s,'utf-8');monkeypatch.setitem(a.BASE,path,a.blob(s.encode()))
    # The test generator should not mutate the source fixture.
    old={n:(root/n).read_bytes() for n in a.PATCHES}
    out=tmp_path/'out.zip';a.create(root,out)
    with zipfile.ZipFile(out) as z:
        assert z.testzip() is None
        assert set(a.PATCHES)<=set(z.namelist())
        assert 'tests/test_h247_source_contract.py' in z.namelist()
        assert all(not n.startswith('data/') for n in z.namelist())
    assert old=={n:(root/n).read_bytes() for n in a.PATCHES}
