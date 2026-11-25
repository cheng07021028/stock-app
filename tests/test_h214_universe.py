import io
from datetime import datetime,date,timedelta
import pytest
from openpyxl import load_workbook,Workbook
import godpick_h214_universe as h
from godpick_h191_opportunity_engine import calc_features
NOW=datetime.fromisoformat('2026-11-25T15:34:00+08:00')
D=[(date(2026,9,1)+timedelta(days=i)).isoformat() for i in range(60)]
def bars(ds=D,p=100):return [(d,p,p*1.01,p*.99,p,1000000,int(p*1000000),1000) for d in ds]
def ev(ds=D):return {d:{} for d in ds[1:]}
def ex(p=100,ref=85,cash=15,kind='息'):return {'previous_close':p,'reference':ref,'cash':cash,'kind':kind}
def html(d='2026/09/20',m='上市',code='1234',isin='TW0001234000',cfi='ESVUFR'):
    return f'<table><tr><td>{code}　測試</td><td>{isin}</td><td>{d}</td><td>{m}</td><td>半導體業</td><td>{cfi}</td><td></td></tr></table>'
@pytest.fixture
def small(monkeypatch):monkeypatch.setitem(h.ar.MIN_ROWS,'TWSE',1)
def test_roster(small):assert h.parse_roster(html(),'TWSE',NOW)['1234']['listing_day']=='2026-09-20'
@pytest.mark.parametrize('kwargs',[{'m':'上櫃'},{'cfi':'CEOGEU'},{'d':'2027/01/01'},{'isin':'BAD'}])
def test_invalid_roster(small,kwargs):
    with pytest.raises(ValueError):h.parse_roster(html(**kwargs),'TWSE',NOW)
def test_roster_future(tmp_path,small):
    stamp=NOW+timedelta(minutes=1);raw=html();body={'version':h.VERSION,'market':'TWSE','source':h.URLS['TWSE'],'observed_at':stamp.isoformat(),'raw':raw,'rows':h.parse_roster(raw,'TWSE',stamp)}
    h.perf.immutable(tmp_path/h.REL/'TWSE'/(stamp.strftime('%Y%m%dT%H%M%S%f')+'.json.gz'),body)
    assert h.load_rosters(tmp_path,NOW)[0]=={}
def test_roster_load(tmp_path,small):
    raw=html();body={'version':h.VERSION,'market':'TWSE','source':h.URLS['TWSE'],'observed_at':NOW.isoformat(),'raw':raw,'rows':h.parse_roster(raw,'TWSE',NOW)}
    h.perf.immutable(tmp_path/h.REL/'TWSE'/(NOW.strftime('%Y%m%dT%H%M%S%f')+'.json.gz'),body)
    assert h.load_rosters(tmp_path,NOW)[0]['TWSE']['1234']['listing_day']=='2026-09-20'
    assert h.load_rosters(tmp_path,NOW+timedelta(days=8))[0]=={}
def test_cash_adjust():
    s=bars(D[:30])+bars(D[30:],85);e=ev();e[D[30]]={'1234':ex()};f,a,status,n=h.features(s,D,e,'1234');assert status=='CASH_REFERENCE_ADJUSTED' and n==1 and f['MA60']==pytest.approx(85);assert a[0][5:]==s[0][5:] and f['當日成交額']==85000000 and s[0][4]==100;assert calc_features(s,D)[0] is None
@pytest.mark.parametrize('event',[ex(kind='權'),ex(p=101),ex(ref=0),ex(cash=1)])
def test_bad_event(event):
    s=bars(D[:30])+bars(D[30:],85);e=ev();e[D[30]]={'1234':event};assert h.adjust(s,D,e,'1234')[1]=='COMPLEX_OR_CONFLICT_ACTION'
def test_unknown_event():assert h.adjust(bars(),D,{},'1234')[1]=='ACTION_COVERAGE_PENDING'
def test_no_event_large_gap():assert h.adjust(bars(D[:30])+bars(D[30:],85),D,ev(),'1234')[1]=='RESIDUAL_GAP'
def test_no_future():
    e=ev();e['2026-12-01']={'1234':ex()};assert h.adjust(bars(),D,e,'1234')[2]==0
@pytest.mark.parametrize('s',[bars()[1:],bars()+[bars()[0]],bars()[:20]+[(D[20],100,101,99,100,0,0,0)]+bars()[21:]])
def test_missing_duplicate_zero(s):assert h.adjust(s,D,ev(),'1234')[1]=='RAW_WINDOW_INVALID'
def test_turnover_unchanged():
    s=bars();f,adj,st,n=h.features(s,D,ev(),'1234');assert f['20日均成交額']==100000000 and st=='NO_REPORTED_ACTION' and f['MA20']==100

def scenario(listing=None,seq=None,ready=True):
    master=[{'code':'1234','market':'上市','name':'測試'}];market={'TWSE':{d:1 for d in D},'TPEX':{d:1 for d in D}};rows={('TWSE','1234'):seq if seq is not None else bars()};roster={'TWSE':{'1234':{'listing_day':listing}}} if listing else {}
    return h.analyze(master,D,market,rows,roster,{'TWSE':ev()},ready=ready)
def test_short_official():
    c,d,short,_=scenario(D[25],bars(D[25:]));assert c[0]['原始母體']==1 and c[0]['有效研究母體']==0 and c[0]['原始覆蓋率']==0 and short[0]['狀態']=='SHORT_NO_SETUP'
@pytest.mark.parametrize('listing,seq',[(None,bars(D[25:])),(D[25],bars(D[26:])),(D[25],bars())])
def test_no_false_exclusion(listing,seq):assert scenario(listing,seq)[0][0]['有效研究母體']==1
def test_short_under21():assert h.short_review(bars(D[-20:]),D[-20:],ev(),'1234')[0]=='SHORT_LT21'
def test_missing_coverage_no_upgrade():assert scenario(ready=False)[0][0]['原始有效']==0

def test_excel():
    c,d,short,ch=scenario();r={'coverage':c,'detail':d,'short':short,'changes':ch,'roster_reconcile':[],'status':'PENDING'};w=load_workbook(io.BytesIO(h.excel(r)));assert len(w.sheetnames)==6 and w['02_雙母體覆蓋']['D2'].number_format=='0.00%'
def test_formal_missing(tmp_path):
    w=Workbook();w.active['A1']='keep';r=h.append_formal_audit(w,tmp_path,research_day=D[-1],parity={},bridge={},authority=None,trace=None,now=NOW);assert r['h126_buyable'] is None and not r['same_run_verified'] and w.active['A1'].value=='keep'
