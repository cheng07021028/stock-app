from datetime import datetime
from collections import defaultdict
from pathlib import Path
import json,io
import numpy as np
import pytest
from openpyxl import Workbook,load_workbook
from test_h191_opportunity_engine import DAYS,ASOF,bars
from test_h198_stage_review import make,pattern
from test_h206_market_gap import inputs
import godpick_h208_recall_review as h
import godpick_h206_market_gap as gap

NOW=datetime.fromisoformat('2026-11-24T08:00:00+08:00')

def base(v=300000):
    p=np.r_[np.linspace(110,95,20),np.linspace(95,100,20),np.tile([99,100,101,100],5)]
    return make(p,{i:v for i in range(54,60)})

def analyze(seq):
    f,why=h.tech.calc_features(seq,DAYS);assert f is not None,why
    return h.setup(seq,DAYS,f)

def test_strict_setup():
    r,e=analyze(base());assert r['召回池']==h.POOLS[0] and '不提供買進授權' in r['研究用途'] and r['H198失效參考']>0

def test_new_near_miss():
    # contraction .9 fails H198 .85 but passes independent H208 .95
    r,e=analyze(base(450000));assert r['召回池']==h.POOLS[1] and r['H198階段']=='其他型態'
    assert analyze(base(500000))[0] is None

@pytest.mark.parametrize('seq',[make(pattern([99,103,101.2,98]),{57:1000000,58:350000,59:1000000}),make(pattern([99,99,100,100]),{59:1000000},high=106),make(pattern([105,110,115,119]),{59:1000000})])
def test_counterevidence(seq):assert analyze(seq)[1]=='H198反證／追高警示'

def test_liquidity_not_relaxed():
    b=[(*r[:5],10000,round(r[4]*10000),r[-1]) for r in base()];assert analyze(b)[1]=='流動性未達研究門檻'

def test_independent_and_no_future_gain():
    m=h.tech.master_map([{'code':'1000','market':'TWSE','name':'A'},{'code':'5000','market':'TPEX','name':'B'},{'code':'0050','market':'TWSE'}]);markets={x:{d:1 for d in DAYS} for x in h.ar.URLS};rows={('TWSE','1000'):base(450000),('TPEX','5000'):base()};a,cv,_=h.evaluate(m,DAYS,markets,rows,ASOF,set(markets));assert len(a)==2 and cv['TWSE']['主檔']==1
    rows['TWSE','1000'].append(('2026-11-24',1,1,1,1,1,1,1));assert h.evaluate(m,DAYS,markets,rows,ASOF,set(markets))[0]==a
    assert all(x['池內研究序']==1 for x in a)

def test_bad_volume_missing_session():
    m=h.tech.master_map([{'code':'1000','market':'TWSE'},{'code':'1001','market':'TWSE'}]);markets={x:{d:1 for d in DAYS} for x in h.ar.URLS};b=base();broken=list(b);r=list(b[5]);r[5]=0;broken[5]=tuple(r)
    rr,cv,why=h.evaluate(m,DAYS,markets,{('TWSE','1000'):broken,('TWSE','1001'):b[:-1]},ASOF,set(markets));assert not rr and cv['TWSE']['有效60日']==0 and sum(why['TWSE'].values())==2

def test_no_market_report(tmp_path):assert h.build(tmp_path,{},NOW)['status']=='DATA_PENDING'

def fixture(root,monkeypatch,missing=False,future=False):
    ar=h.ar;monkeypatch.setattr(ar,'MIN_ROWS',{'TWSE':1,'TPEX':1});p=root/'data/h190';months=defaultdict(list)
    for d in DAYS:months[d[:7]].append(d)
    for m,ds in months.items():ar.save_calendar(p,ar.parse_calendar({'stat':'OK','fields':['日期','收盤指數'],'data':[[d,30000] for d in ds]},m,observed_at=NOW.isoformat()))
    for market,code,seq in [('TWSE','1000',base()),('TPEX','5000',base(450000))]:
        for i,(d,o,hi,lo,c,v,a,n) in enumerate(seq):
            if missing and market=='TPEX' and i==3:continue
            obs='2026-11-25T08:00:00+08:00' if future and market=='TPEX' and i==3 else NOW.isoformat()
            raw={'stat':'OK','date':d,'fields':['代號','開盤價','最高價','最低價','收盤價','成交股數','成交金額','成交筆數'],'data':[[code,o,hi,lo,c,v,a,n]]}
            ar.save_day(p,ar.parse_daily(raw,market,d,observed_at=obs))
    (root/'stock_master_cache.json').write_text(json.dumps([{'code':'1000','name':'A','market':'TWSE'},{'code':'5000','name':'B','market':'TPEX'}]),'utf-8')

@pytest.mark.parametrize('missing,future,expected',[(False,False,'RESEARCH_READY'),(True,False,'PARTIAL_RESEARCH'),(False,True,'PARTIAL_RESEARCH')])
def test_real_sealed_archives(tmp_path,monkeypatch,missing,future,expected):
    fixture(tmp_path,monkeypatch,missing,future);b=gap.publish(tmp_path,NOW);r=b['h208'];assert h.valid(b) and r['status']==expected and len(r['rows'])==(2 if expected=='RESEARCH_READY' else 1);assert gap.load(tmp_path,NOW)==b
    assert not (tmp_path/'godpick_records.json').exists()

def test_export_authority_and_recall():
    b,k=inputs();b['asof']='2026-11-24T23:00:00+08:00';b['h208']={'version':h.VERSION,'day':b['day'],'asof':b['asof'],'trade_authority_changed':False,'status':'RESEARCH_READY','rows':[{'市場':'TWSE','股票代號':'1003','股票名稱':'=bad','研究用途':'僅研究'}]};c=gap.compare_export(b,**k);assert c['h126_buyable']==1 and c['h208_comparison']['rows'][0]['H109原風控']=='FADE_VETO';assert '風控複核' in h.audit(c)[3]['H208原因分類']
    w=Workbook();w.active.title='00_H126主管決策';w.active['A1']='禁止買進';gap.append_sheet(w,b,c);o=io.BytesIO();w.save(o);o.seek(0);r=load_workbook(o);assert r['00_H126主管決策']['A1'].value=='禁止買進' and h.SHEET in r.sheetnames and h.AUDIT_SHEET in r.sheetnames;assert r[h.SHEET]['C4'].value=="'=bad"
    k['bridge']['candidate_complete']=False;assert h.audit(gap.compare_export(b,**k))[3]['H208原因分類']=='母體待核'
    k['research_day']='2026-11-23';assert h.audit(gap.compare_export(b,**k))[3]['H208原因分類']=='身分待核'

def test_old_snapshot_and_public_audit():
    b,k=inputs();w=Workbook();gap.append_sheet(w,b);assert h.SHEET in w.sheetnames and w[h.AUDIT_SHEET].max_row==7
