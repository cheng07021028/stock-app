from datetime import datetime,timedelta
import copy,io
import pytest
from openpyxl import Workbook
import godpick_h200_integrated as h
import godpick_h195_command_center as c
from test_h195_command_center import candidate,joint,write,NOW
from test_h199_adaptive_weights import sample
import godpick_h199_adaptive_weights as a

def report(now=NOW):
    m=a.fit(sample());return {'version':a.VERSION,'expected_session':'2026-11-23','asof':now.isoformat(),'weights_changed':False,'trade_authority_changed':False,'archive_errors':[],'current_regime':[{'市場':'TWSE','研究日':'2026-11-23','狀態':'多頭廣度','有效檔數':200,'覆蓋率':.97}], 'models':[{'市場':'TWSE','市場狀態':'多頭廣度','歷史狀態覆蓋率':.99,**m}]}

def body(rows):
    return {'expected_session':'2026-11-23','technical_current':True,'rows':rows,'universe':len(rows),'counts':{c.TIERS[1]:len(rows)},'health':{'technical_status':'CROSS_MARKET_RESEARCH_READY'},'performance':{}}

def row(code,rank,pool='主流強勢',market='TWSE',tier=c.TIERS[1]):
    r=candidate(code,tier,market);r.update({'研究順序':rank,'技術研究池':pool,'交易狀態':'僅研究／未授權'});return r

def test_validated_model():
    w,state=h.validated(report(),'2026-11-23',NOW);assert w['TWSE']['主流強勢']==1.1 and w['TWSE']['低位階蓄勢']==.9 and 'TPEX' not in w

@pytest.mark.parametrize('mutate',[lambda r:r.update(expected_session='2026-11-20'),lambda r:r.update(asof=(NOW+timedelta(seconds=1)).isoformat()),lambda r:r['models'][0].update(holdout_win_rate=.4),lambda r:r['models'][0]['weights'].update({'主流強勢':1.4}),lambda r:r['current_regime'][0].update(覆蓋率=.8),lambda r:r.update(archive_errors=['bad']),lambda r:r['models'][0].update(status='樣本不足／維持原權重')])
def test_invalid_model_neutral(mutate):
    r=report();mutate(r);assert h.validated(r,'2026-11-23',NOW)[0]=={}

def test_risk_and_order_no_deletion():
    rows=[row('1000',1),row('1001',2,'低位階蓄勢'),row('1002',3),row('1003',4,market='TPEX')];rows[0].update({'H198階段':'突破反證與追高警示','H198資料日期':'2026-11-23'})
    b=h.enrich(body(rows),{'audit':rows},report(),NOW);assert [r['股票代號'] for r in h.ordered(b['rows'])]==['1001','1002','1003','1000'];assert [r['研究順序'] for r in b['rows']]==[1,2,3,4];assert b['rows'][3]['H200研究權重']==1.;assert b['rows'][0]['交易狀態']=='僅研究／未授權'

def test_weights_reorder_and_no_duplicate_votes():
    rows=[row('1000',10,'低位階蓄勢'),row('1001',11,'主流強勢'),row('1002',12,'主流強勢、主流強勢、低位階蓄勢')];b=h.enrich(body(rows),{'audit':rows},report(),NOW);assert h.ordered(b['rows'])[0]['股票代號']=='1001';assert rows[2]['H200研究權重']==1.0

def test_pending_neutral():
    r=row('1000',1,tier=c.TIERS[3]);b=h.enrich(body([r]),{'audit':[r]},report(),NOW);assert r['H200研究權重']==1 and b['h200']['trade_authority_changed'] is False

def test_stale_risk_neutral():
    r=row('1000',1);r.update({'H198階段':'突破反證與追高警示','H198資料日期':'2026-11-20'});b=body([r]);b['technical_current']=False;h.enrich(b,{'audit':[r]},report(),NOW);assert r['H200研究權重']==1 and r['H200風險複核'].startswith('反證資料待補')

def test_zero_diagnosis_and_gaps():
    b=body([]);b['technical_current']=False;b['universe']=2;b['health']={'technical_status':'DATA_PENDING'};r=h.enrich(b,{'audit':[{'研究分級':'未入池','技術缺口':'缺60日'}]*2},None,NOW);assert 'TECH_SESSION_NOT_CURRENT' in r['h200']['issues'] and r['h200']['top_gap_counts']['技術缺口：缺60日']==2

def test_formal_unknown_and_blocked():
    assert h.formal_diagnosis()['h126_buyable'] is None
    assert '母體未證實' in h.formal_diagnosis(buyable=0,h145={'candidate_complete':False})['reason']
    assert '授權0' in h.formal_diagnosis(buyable=0,h145={'candidate_complete':True,'status':'OK','audit_final_coverage':{'complete':True}})['reason']
    assert h.formal_diagnosis(buyable=True)['h126_buyable'] is None

def test_excel_and_same_priority(tmp_path):
    rs=[row('1000',1),row('1001',2)];rs[0].update({'H198階段':'突破反證與追高警示','H198資料日期':'2026-11-23'});b=c.build(joint(rs),{},tmp_path,NOW,0);write(tmp_path,b);w=Workbook();w.active.title='00_H126主管決策';w.active['A1']='禁止買進';ws=c.append_manager(w,tmp_path,'2026-11-23',NOW,formal=h.formal_diagnosis(buyable=0));assert w.active['A1'].value=='禁止買進' and ws['D8'].value=='1001' and ws['D9'].value=='1000' and ws['D5'].value==0

def test_reviews_match_priority(tmp_path,monkeypatch):
    rs=[row('1000',1),row('1001',2)];rs[0].update({'H198階段':'突破反證與追高警示','H198資料日期':'2026-11-23'});called=[]
    def detail(result,financial,market,code,**kwargs):called.append(code);return {'roles':{}}
    monkeypatch.setattr(c.joint,'detail',detail);b=c.build(joint(rs),{},tmp_path,NOW,1);assert called==['1001'] and b['reviews'][0]['股票代號']=='1001'

def test_read_only_snapshot_and_learning_exception(tmp_path,monkeypatch):
    r=joint([row('1000',1)]);monkeypatch.setattr(c.joint,'run',lambda *args,**kwargs:(r,{}));monkeypatch.setattr(a,'run',lambda *args,**kwargs:(_ for _ in ()).throw(RuntimeError('simulated')));b=c.publish(tmp_path,NOW);assert len(b['rows'])==1 and b['rows'][0]['H200研究權重']==1 and any('H199' in x for x in b['errors']);assert c.load(tmp_path,NOW)[1]=='CURRENT'


def test_displayed_diagnosis_not_full_market():
    import pandas as pd
    f=pd.DataFrame({'H114K線資料狀態':['INVALID_OHLCV_VOLUME','INVALID_OHLCV_VOLUME',None],'正式推薦阻擋原因':['A','B','A']})
    before=f.copy(deep=True);r=h.displayed_diagnosis(f);assert r['顯示列數']==3 and r['原因前10類']['H114K線資料狀態']['INVALID_OHLCV_VOLUME']==2 and 'H113流動性合格' in r['缺少診斷欄位'];pd.testing.assert_frame_equal(f,before)
