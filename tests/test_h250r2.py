import sys,json,types
from pathlib import Path
from datetime import datetime
import pytest,pandas as pd
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import godpick_h250r2_nday as h
from godpick_h250r2_table import render_native
NOW=datetime.fromisoformat('2026-11-27T21:00:00+08:00')
DAYS=['2026-11-20','2026-11-23','2026-11-24','2026-11-25','2026-11-26','2026-11-27']
CAL=h.Calendar({'2026-11':set(DAYS)},{'2026-11':NOW},NOW)
def row(rid='A',**extra):return {'record_id':rid,'股票代號':'2330','市場別':'上市','推薦日期':DAYS[0],'推薦價格':100,**extra}
def quotes():return {('TWSE','2330'):{d:100+i for i,d in enumerate(DAYS)}}
@pytest.mark.parametrize('kind', ['dataframe','data_editor'])
def test_native_without_patch(kind):
    def native(data,*,height=1):return data,height
    st=types.SimpleNamespace(**{kind:native});assert render_native(st,kind,'data',height=620,_godpick_bypass=True)==('data',620)
@pytest.mark.parametrize('kind',['dataframe','data_editor'])
def test_native_full_patch(kind):
    def reject(*a,**k):raise AssertionError('wrapper invoked')
    st=types.SimpleNamespace(**{kind:reject,'_godpick_table_patch_v105':True,'_godpick_original_'+kind+'_v105':lambda d,**kw:(d,kw)})
    assert render_native(st,kind,'x',key='k')==('x',{'key':'k'})
def test_light_editor():
    st=types.SimpleNamespace(data_editor=lambda *a:None,_godpick_light_checkbox_patch_v116=True,_godpick_original_data_editor_v116_light=lambda d,**kw:d)
    assert render_native(st,'data_editor','x',_godpick_bypass=True)=='x'
def test_trading_day_weekend():
    assert CAL.span(DAYS[0],1)==DAYS[:2]
    assert CAL.span(DAYS[0],3)==DAYS[:4]
    assert CAL.span(DAYS[0],10) is None
@pytest.mark.parametrize('value',['',None,'nan','<NA>'])
def test_missing_id(value):assert h.due(row(value),CAL)[1]=='IDENTITY_PENDING'
def test_unverified_legacy_proxy():
    r=row(**{'推薦後1日%':999});assert h.verified_value(r,1,NOW) is None;assert 1 in h.due(r,CAL)[0]
def test_calculate_only_mature():
    d,s=h.compute(row(),CAL,quotes());assert s=='UPDATED';assert [d[k] for k in h.COLS[:3]]==[1,3,5];assert h.COLS[3] not in d
    r=row(**d);assert h.verified_value(r,3,NOW)==3;assert h.due(r,CAL)[1]=='NOT_DUE'
def test_zero_is_verified():
    q=quotes();q[('TWSE','2330')][DAYS[1]]=100;d,_=h.compute(row(),CAL,q);assert h.verified_value(row(**d),1,NOW)==0
@pytest.mark.parametrize('field,value',[('市場別','上櫃'),('推薦價格',101),('推薦日期','2026-11-23'),('record_id','B'),('推薦後1日%',999)])
def test_modified_proof(field,value):
    d,_=h.compute(row(),CAL,quotes());r=row(**d);r[field]=value;assert h.verified_value(r,1,NOW) is None
def test_missing_session_never_skipped():
    q=quotes();q[('TWSE','2330')].pop(DAYS[1]);assert h.compute(row(),CAL,q)==({},'QUOTE_PENDING')
def test_missing_month():
    c=h.Calendar({'2026-09':{'2026-09-30'},'2026-11':set(DAYS)},{'2026-11':NOW},NOW);assert c.span('2026-09-30',1) is None
    assert h.due(row(**{'推薦日期':'2026-09-30'}),c)[1]=='CALENDAR_GAP'
def test_today_before_confirmed():
    now=NOW.replace(hour=17);c=h.Calendar({'2026-11':set(DAYS)},{'2026-11':now},now);assert DAYS[-1] not in c.dayset

def test_no_starvation_and_duplicate():
    bad=[row(str(i),**{'股票代號':'9999'}) for i in range(100)];p,s=h.plan(bad+[row('good'),row('dup'),row('dup')],CAL,quotes(),1);assert list(p)==['good'];assert s['QUOTE_PENDING']==100;assert s['IDENTITY_PENDING']==2

def test_bounded_batch():
    p,s=h.plan([row(str(i)) for i in range(10)],CAL,quotes(),3);assert len(p)==3 and s['DEFERRED']==7

def test_no_rewrite_identical_force():
    d,_=h.compute(row(),CAL,quotes());assert h.compute(row(**d),CAL,quotes(),True)==({},'VERIFIED')

def test_partial():
    q=quotes();q[('TWSE','2330')].pop(DAYS[3]);p,s=h.plan([row()],CAL,q);assert h.COLS[0] in p['A'];assert h.COLS[1] not in p['A'];assert s['PARTIAL_PENDING']==1

def test_overlay_only_matching_id():
    d,_=h.compute(row(),CAL,quotes());history=pd.DataFrame([row(**d)]);f=pd.DataFrame([row(),row('B')]);out=h.overlay_verified_history(f,history);assert out.loc[0,h.COLS[0]]==1;assert pd.isna(out.loc[1,h.COLS[0]])

def test_missing_baseline_uses_exact_official_day():
    r=row(**{'推薦價格':None});d,_=h.compute(r,CAL,quotes());assert d[h.COLS[0]]==1

def test_bad_proof():assert h.verified_value(row(**{h.PROOF:'[]'}),1,NOW) is None


def test_preserve_formal_against_proxy():
    d,_=h.compute(row(),CAL,quotes());old=row(**d);new=dict(old,**{'推薦後1日%':900,h.PROOF:'fake','最新價':110});r=h.preserve_verified(old,new);assert r[h.COLS[0]]==1 and r[h.PROOF]==d[h.PROOF] and r['最新價']==110

def test_generic_cannot_import_proof():
    d,_=h.compute(row(),CAL,quotes());assert h.PROOF not in h.preserve_verified(row(),row(**d))

@pytest.mark.parametrize('outcome',['success','remote_fail','conflict','unverified','no_quotes'])
def test_authority_exact_id_commit(monkeypatch,tmp_path,outcome):
    import threading
    rows=[row('A'),row('B',**{'股票代號':'9999'})];calls=[]
    class D(datetime):
        @classmethod
        def now(cls,tz=None):return NOW
    monkeypatch.setattr(h,'datetime',D)
    monkeypatch.setattr(h,'load_calendar',lambda *a:CAL)
    fakear=types.SimpleNamespace(_index=lambda *a,**k:(CAL.cal,{},{} if outcome=='no_quotes' else {('TWSE','2330'):[(d,100,101,99,px,1000) for d,px in quotes()[('TWSE','2330')].items()]},[]))
    def base(sig):
        assert sig=='original-signature'
        if outcome=='conflict':raise ValueError('version changed')
        return rows,{},'original-head'
    def commit(old,head,sha,new,removed,reason):
        assert old==rows and sha=='original-head' and removed==set()
        assert [r['record_id'] for r in new]==['A','B'] and new[1]==rows[1]
        assert set(new[0])-set(rows[0])==set(h.COLS[:3])|{h.PROOF}
        calls.append(new)
        ok=outcome=='success'
        return types.SimpleNamespace(permanent_ok=ok,local_ok=ok,github_ok=ok,messages=lambda:['receipt'])
    fakeh=types.SimpleNamespace(ensure=lambda:(rows,[],False),_base=base,_commit=commit)
    fakep=types.SimpleNamespace(records_authority_verification_h240=lambda:{'status':'BLOCKED' if outcome=='unverified' else 'VERIFIED'},records_authority_signature=lambda:'original-signature',_RECORDS_LOCAL_LOCK=threading.RLock(),_RECORDS_GITHUB_UPLOAD_LOCK=threading.RLock())
    for k,v in [('godpick_h190_market_archive',fakear),('godpick_h241_github_records',fakeh),('godpick_persistence_service',fakep)]:monkeypatch.setitem(sys.modules,k,v)
    ok,s,_=h.run_authority(tmp_path)
    assert s['success']==(1 if outcome=='success' else 0)
    assert len(calls)==(1 if outcome in ('success','remote_fail') else 0)
    assert ok==(outcome in ('success','no_quotes'))

def test_display_status_distinguishes_legacy():
    d,_=h.compute(row(),CAL,quotes());r=row(**d);r[h.COLS[4]]=99
    s=h.display_status(pd.DataFrame([r])).iloc[0];assert '1/3/5日' in s and '舊值待核 20日' in s
