from datetime import datetime
import io
import pytest
from openpyxl import load_workbook
import godpick_h226_live_acceptance as h
import godpick_h218_evidence as e
import godpick_h209_rolling_backtest as b
from test_h217_prospective_quality import sig, D,T,NOW,market

@pytest.mark.parametrize('confirmed', [True,False])
def test_confirmed_target(confirmed,monkeypatch):
    m=market([D,T]);m.today_confirmed=lambda d:confirmed;m.errors=[];m.quotes=lambda market,day:{'2330':100 if day==D else 105}
    s=sig();s['base_close']=100
    assert bool(h.p.confirmed_span(m,D,1,NOW))==confirmed
    assert bool(e.due_span(m,D,1,NOW))==confirmed
    assert bool(h.p.settle({h.p.sid(s):s},{},m,NOW)[0])==confirmed
    monkeypatch.setattr(b,'trade',lambda *args:{'status':'行情缺口'})
    s['strategies']=['x'];assert bool(b.evaluate([s],m,None,NOW)[0])==confirmed

def rows(s,*,due=True,raw=True,net=True):
    return [{'訊號來源':'H194','研究日':s['signal_day'],'市場':s['market'],'股票代號':s['code'],'期間':n,'出場日':T if n==1 and due else None,'原始績效狀態':'VERIFIED_RAW' if n==1 and raw else '待核','原始收盤績效':.05 if n==1 and raw else None,'淨績效狀態':'VALID_CASH_MODEL' if n==1 and net else '待核','淨報酬':.02 if n==1 and net else None} for n in h.p.HORIZONS]

def test_no_forecast_not_zero_accuracy():
    s=sig();s['forecast']=None;r=h.first_batch(rows(s),[s])[0];assert r['原價上漲比例']==1 and r['淨勝率']==1 and r['方向準確率'] is None and r['Brier'] is None

def test_frozen_forecast_scores():
    s=sig();s['forecast']=[.1,.2,.7];r=h.first_batch(rows(s),[s])[0];assert r['方向準確率']==1 and r['Brier']==pytest.approx(.14)

def test_tie_no_direction_score():
    s=sig();s['forecast']=[.45,.1,.45];r=h.first_batch(rows(s),[s])[0];assert r['方向準確率'] is None and r['Brier'] is not None

def test_incomplete_no_win_rate():
    s=sig();z=sig('2331');r=h.first_batch(rows(s)+rows(z,raw=False,net=False),[s,z])[0];assert r['原價有效']==1 and r['封存']==2 and r['原價平均'] is None and r['淨勝率'] is None

def test_observation_not_net_denominator():
    s=sig();z=sig('2331');z['tier']=h.p.TIERS[-1];r=h.first_batch(rows(s)+rows(z,net=False),[s,z])[0];assert r['淨績效應結算']==1 and r['淨勝率']==1

def test_missing_horizon_reject():
    s=sig();with_rows=rows(s)[:-1]
    with pytest.raises(ValueError):h.first_batch(with_rows,[s])

def test_duplicate_reject():
    s=sig();rr=rows(s)
    with pytest.raises(ValueError):h.first_batch(rr+rr,[s])

def test_not_due():
    s=sig();r=h.first_batch(rows(s,due=False),[s])[0];assert r['到期']==0 and r['淨勝率'] is None

def test_blocked_errors():
    s=sig();r=h.first_batch(rows(s),[s],['error'])[0];assert r['原價平均'] is None and r['淨勝率'] is None

def test_queue():
    r={'scope':[{'必要範圍':True,'市場':'TWSE','期間':'2026-10','狀態':'MISSING','原因':'','來源':'revenue'}], 'flows':[{'市場':'TPEX','交易日':D,'狀態':'待補證'}],'action_rows':[{'市場':'TWSE','交易日':T,'狀態':'INVALID','原因':'digest'}], 'recovery':{'market_missing':[],'day_queue':[]},'sources':[],'gates':[],'day':T};q=h.queue(r);assert len(q)==3 and any('禁止覆寫' in x['既有工作流／處理'] for x in q) and any('歷史來源待驗證' in x['既有工作流／處理'] for x in q)

def test_original_and_excel(tmp_path):
    r=h.run(tmp_path,NOW);assert r['batch_status']=='NO_FROZEN_BATCH' and r['status']=='EVIDENCE_PENDING' and r['weight_multiplier']==1.0;w=load_workbook(io.BytesIO(h.excel(r)));assert len(w.sheetnames)==10 and not (tmp_path/'data').exists()
