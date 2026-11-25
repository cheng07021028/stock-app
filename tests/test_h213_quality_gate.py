from datetime import datetime,timedelta
from types import SimpleNamespace
import io,json,pytest
import godpick_h213_quality_gate as h
import godpick_h190_market_archive as ar
import godpick_h212_data_recovery as h212
from tools.h213_dispatch import due,dispatch
from test_h190_market_archive import raw,calendar,NOW,DAY
OBS=datetime.fromisoformat(NOW)
DAYS=['2026-11-18','2026-11-19','2026-11-20',DAY]

def monthly_raw(code='1700',market='TWSE'):
    fields=['日期','成交股數','成交金額(元)','開盤價','最高價','最低價','收盤價','成交筆數']
    rows=[[d,'100000','10000000','100','101','99','100','20'] for d in DAYS]
    if market=='TPEX':
        fields[1:3]=['成交仟股','成交仟元']
        for x in rows:x[1:3]=['100','10000']
    return {'stat':'OK','code':code,'tables':[{'fields':fields,'data':rows}]}

@pytest.mark.parametrize('m',['TWSE','TPEX'])
def test_monthly_units(m):
    rows,states=h.monthly(monthly_raw(market=m),m,'1700','2026-11',OBS)
    assert rows[DAY]==['1700',100,101,99,100,100000,10000000,20] and states[DAY]=='POSITIVE_QUOTE'

@pytest.mark.parametrize('kind',['identity','duplicate','month','future','unknown_units','empty','short','ambiguous','missing_identity'])
def test_reject_bad_monthly(kind):
    r=monthly_raw();m='TWSE';t=r['tables'][0]
    if kind=='identity':r['code']='1234'
    elif kind=='duplicate':t['data'].append(t['data'][0])
    elif kind=='month':t['data'][0][0]='2026-10-20'
    elif kind=='future':t['data'][0][0]='2026-11-25'
    elif kind=='unknown_units':m='TPEX';t['fields'][2]='成交金額'
    elif kind=='empty':t['data']=[]
    elif kind=='short':t['data'][0]=[DAY]
    elif kind=='ambiguous':r['tables'].append(t)
    elif kind=='missing_identity':r.pop('code')
    with pytest.raises(ValueError):h.monthly(r,m,'1700','2026-11',OBS)


def seed(tmp):
    p=tmp/'data/h190';ar.save_calendar(p,calendar(DAYS))
    for d in DAYS:ar.save_day(p,ar.parse_daily(raw(d,n=700 if d==DAY else 701),'TWSE',d,observed_at=NOW))
    return p


def test_cross_report_only(tmp_path):
    p=seed(tmp_path);before=(p/'daily/TWSE'/f'{DAY}.json.gz').read_bytes();r,c=h.compare(tmp_path,'TWSE','1700','2026-11',monthly_raw(),OBS,[DAY]);assert r[0]['補證狀態']=='CROSS_REPORT_CANDIDATE' and not c
    assert '1700' not in ar.quote_rows(p,'TWSE',DAY,OBS) and (p/'daily/TWSE'/f'{DAY}.json.gz').read_bytes()==before


def test_scope_conflict(tmp_path):
    seed(tmp_path);raw_=monthly_raw();raw_['tables'][0]['data'][0][1:3]=['200000','20000000'];r,c=h.compare(tmp_path,'TWSE','1700','2026-11',raw_,OBS,[DAY]);assert r[0]['補證狀態']=='SCOPE_REVIEW' and c==[DAYS[0]]


def test_zero_not_suspension(tmp_path):
    seed(tmp_path);r=monthly_raw();r['tables'][0]['data'][-1][1:]=['0','0','--','--','--','--','0'];d,_=h.compare(tmp_path,'TWSE','1700','2026-11',r,OBS,[DAY]);assert d[0]['補證狀態']=='OFFICIAL_ZERO_VOLUME'


def test_alternate_collection_and_cooldown(tmp_path,monkeypatch):
    seed(tmp_path);monkeypatch.setattr(h212,'gaps',lambda *a,**k:{'stock_gaps':[{'市場':'TWSE','股票代號':'1700','缺日期':DAY}]});monkeypatch.setattr(ar,'_get',lambda *a,**kw:monthly_raw())
    r=h.collect(tmp_path,OBS,budget=1,http=object(),pause=0);assert r['requests']==1 and r['promoted_rows']==0 and r['tried'][0]['detail'][0]['補證狀態']=='CROSS_REPORT_CANDIDATE'
    assert len(list((tmp_path/h.REL/'monthly').rglob('*.json.gz')))==1
    assert h.collect(tmp_path,OBS+timedelta(days=1),budget=1,http=object(),pause=0)['requests']==0


def test_candidate_requeues_once(tmp_path,monkeypatch):
    p=seed(tmp_path);old=OBS-timedelta(hours=1);k='TWSE:'+DAY;ar._write(tmp_path/h212.REL,ar._seal({'version':h212.VERSION,'asof':old.isoformat(),'attempts':{k:{'status':'NO_NEW_VALID_ROWS','at':old.isoformat()}}}));ar._write(tmp_path/h.REL/'monthly_health.json',ar._seal({'version':h.VERSION,'asof':OBS.isoformat(),'attempts':{'TWSE:1700:2026-11':{'at':OBS.isoformat(),'detail':[{'市場':'TWSE','交易日':DAY,'補證狀態':'CROSS_REPORT_CANDIDATE'}]}}}))
    monkeypatch.setattr(h212,'gaps',lambda *a,**k:{'day_queue':[{'市場':'TWSE','交易日':DAY,'缺檔數':1}],'coverage':{},'archive_errors':[]});monkeypatch.setattr(ar,'_get',lambda *a,**k:raw(DAY,n=701));r=h212.repair(tmp_path,OBS,budget=1,http=object(),pause=0);assert r['new_valid_rows']==1;assert h212.repair(tmp_path,OBS+timedelta(hours=1),budget=1,http=object(),pause=0)['requests']==0

@pytest.mark.parametrize('checks,expect',[({'archive_ok':False},[]),({'archive_ok':True},['H190']),({'archive_ok':True,'calendar':True,'both_quotes':True},['H194','H195']),({'archive_ok':True,'calendar':True,'both_quotes':True,'H194':True,'H195':True},['H209'])])
def test_recovery(checks,expect):assert h.recovery_plan(OBS,OBS.date().isoformat(),checks)==expect

@pytest.mark.parametrize('at',['2026-11-24T08:20:00+08:00','2026-11-24T23:36:00+08:00','2026-11-28T21:00:00+08:00'])
def test_no_outside_window(at):assert h.recovery_plan(at,'2026-11-24',{'archive_ok':True})==[]


def test_dispatch_cooldown_cap_and_active():
    r=lambda t,status='completed',event='workflow_dispatch':{'createdAt':t.isoformat(),'status':status,'event':event}
    assert due([r(OBS-timedelta(hours=2),'in_progress')],OBS)[1]=='ACTIVE_RUN'
    assert due([r(OBS-timedelta(minutes=10))],OBS)[1]=='COOLDOWN'
    assert due([r(OBS-timedelta(hours=1)),r(OBS-timedelta(hours=2))],OBS)[1]=='DAILY_DISPATCH_LIMIT'
    assert due([],OBS)[0]


def test_dispatch_not_success_and_stale():
    calls=[]
    def fake(args,**kw):calls.append(args);return SimpleNamespace(stdout='[]')
    r={'version':h.VERSION,'asof':OBS.isoformat(),'trade_authority_changed':False,'repair_plan':['H194','H194'],'errors':[]}
    assert dispatch(r,OBS,invoke=fake)==[{'workflow':'H194','status':'DISPATCH_REQUESTED_NOT_COMPLETED'}] and len(calls)==2
    assert dispatch(r,OBS+timedelta(minutes=11),invoke=fake)[0]['status']=='REPORT_STALE_OR_OUTSIDE_WINDOW'


def test_empty_quality_not_zero_recall(tmp_path):
    r=h.quality(tmp_path,OBS);assert r['status']=='WAITING_EVIDENCE' and all(x['研究榜召回率'] is None for x in r['recall']);assert not (tmp_path/'data').exists()
    import openpyxl
    w=openpyxl.load_workbook(io.BytesIO(h.excel(r)));assert len(w.sheetnames)==9
