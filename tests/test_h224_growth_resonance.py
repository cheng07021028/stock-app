from datetime import datetime,timedelta
import io
import pytest
import godpick_h224_growth_resonance as h
import godpick_h192_financial_growth as fin
import godpick_h220_growth_rotation as rot
from test_h223_growth_scenarios import profile
NOW=datetime.fromisoformat('2026-11-26T19:00:00+08:00');M,Q=fin.expected_periods(NOW)

def raw_margin(**kw):
    return {'Date':'1151126','Year':'115','季別':'03','SecuritiesCompanyCode':'1234','CompanyName':'測試','營業收入百萬元':'100','毛利率':'30.5','營業利益率':'15','稅前純益率':'14','稅後純益率':'10',**kw}

def test_tpex_actual_schema():
    r=fin.parse('margin','TPEX',[raw_margin()],NOW.isoformat())[0];assert r['gross_margin_pct']==30.5 and r['operating_margin_pct']==15 and r['net_margin_pct']==10 and r['year_quarter']=='2026Q3'

@pytest.mark.parametrize('kw',[{'毛利率(%)':'31'},{'營業利益率':'--'},{'稅後純益率':'1001'},{'季別':'05'},{'Date':'1151127'}])
def test_reject_bad_margin(kw):assert fin.parse('margin','TPEX',[raw_margin(**kw)],NOW.isoformat())==[]

def test_tpex_only_bare_alias():assert fin.parse('margin','TWSE',[raw_margin()],NOW.isoformat())==[]

def rev(ratios=(1.1,1.1,1.1,1.3,1.3,1.3)):
    return {i:{'revenue_twd':100 if i<M-5 else 100*ratios[i-M+5],'available_at':NOW.isoformat(),'source_url':'test'} for i in range(M-17,M+1)}

def test_acceleration():
    r=h.acceleration(rev(),NOW,M);assert r['加速度狀態']=='加速成長' and r['營收加速度pp']==20 and len(r['18月實績雜湊'])==64

@pytest.mark.parametrize('ratios,state', [((1.3,1.3,1.3,1.1,1.1,1.1),'成長降速'),((1.2,)*6,'正成長未加速'),((.9,)*6,'未達正成長')])
def test_other_states(ratios,state):assert h.acceleration(rev(ratios),NOW,M)['加速度狀態']==state

@pytest.mark.parametrize('modify',[lambda d:d.pop(M-15),lambda d:d[M].update(available_at=(NOW+timedelta(seconds=1)).isoformat()),lambda d:d[M].update(previous_month_twd=1),lambda d:d[M].update(revenue_twd=0)])
def test_accel_missing(modify):
    d=rev();modify(d);r=h.acceleration(d,NOW,M);assert r['加速度狀態']=='待證' and r['營收加速度pp'] is None

def evidence():
    r={**profile(),'低PE高EPS複核':True};a=h.acceleration(rev(),NOW,M);c={'籌碼分組':rot.FULL,'法人來源雜湊':'x','TDCC來源雜湊':'y'};e={**c,'營收年月':r['營收年月'],'營收年增%':r['營收年增%'],'TTM EPS重建':r['TTM EPS重建'],'連兩季三率三升':True,'獲利突破':True,'原始收盤':100,'H198階段':'初期'};return r,a,e,c

def test_convergence():
    r,a,e,c=evidence();assert h.classify(r,a,e,c)['H224分層']==h.ORDER[0]
    c['法人來源雜湊']='changed';assert h.classify(r,a,e,c)['H224分層']==h.ORDER[2]
    e['TTM EPS重建']=99;assert h.classify(r,a,e,c)['H224分層']==h.ORDER[3]

def test_unknown_not_negative():
    r,a,e,c=evidence();assert h.classify(r,a,e,{'籌碼分組':rot.UNKNOWN})['H224分層']==h.ORDER[2]
    a['加速度狀態']='待證';assert h.classify(r,a,e,c)['H224分層']==h.ORDER[1]

def test_rank():
    r,a,e,c=evidence();q=h.classify(r,a,e,c);r['股票代號']='1001';b=h.classify(r,a,{},c);assert h.rank([b,q])[0]['H224研究順位']==1 and q['H224研究順位']==1

def test_snapshot(tmp_path):
    r,a,e,c=evidence();row={**h.classify(r,a,e,c),'資料日':'2026-11-26'};body={'version':h.VERSION,'day':'2026-11-26','created_at':NOW.isoformat(),'rows':[row],'trade_authority_changed':False};path=tmp_path/h.REL/'2026-11-26.json.gz';h.hist._persist(path,body);assert len(h.history(tmp_path,NOW))==1;assert not h.history(tmp_path,NOW-timedelta(seconds=1))
    with pytest.raises(FileExistsError):h.hist._persist(path,body)

def test_excel():
    from openpyxl import load_workbook
    r={'day':'','status':'EVIDENCE_PENDING','capture':'READ_ONLY','counts':{},'rows':[],'acceleration':[],'scope':[],'forecast_feedback':[],'errors':[]};assert len(load_workbook(io.BytesIO(h.excel(r))).sheetnames)==6
