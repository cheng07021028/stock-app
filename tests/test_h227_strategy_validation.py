from datetime import timedelta
from types import SimpleNamespace
import io
import pytest
from openpyxl import load_workbook
import godpick_h227_strategy_validation as h
from test_h217_prospective_quality import sig,D,T,NOW,market
from test_h218_evidence import pairrow

def fixture():
    m=market([D,T]);m.errors=[];m.today_confirmed=lambda d:True
    a={str(i):100 for i in range(1000,1100)};z={c:100 for c in a};z['1000']=105;z['1001']=104
    m.quotes=lambda m,d:a if d==D else z
    master={c:{'市場':'TWSE'} for c in a};b={'day':T,'previous_day':D,'errors':[],'coverage':{'TWSE':{'主檔一般股':100,'雙日有價':100}},'rows':[{'市場':'TWSE','股票代號':c,'前收盤':100,'收盤':z[c],'產業':'測試'} for c in ('1000','1001')]}
    return m,master,b,a,z

def all_group(rows,src='H194'):return next(r for r in rows if r['市場']=='TWSE' and r['訊號來源']==src and r['研究群組']=='全研究')

def test_precision_vs_recall():
    m,master,b,*_=fixture();ss=[sig(c) for c in ('1000','1002','1003','1004')];r,s=h.leader_precision(b,ss,m,master,NOW);a=all_group(r);assert a['榜單命中精度']==.25 and a['榜單召回率']==.5 and a['前日封存']==4;assert s[0]['未入前日研究']==1

def test_empty_not_zero():
    m,master,b,*_=fixture();r,_=h.leader_precision(b,[],m,master,NOW);assert all_group(r)['榜單命中精度'] is None

@pytest.mark.parametrize('mode',['calendar','coverage','base','missing','jump','other_market','future','board_conflict'])
def test_invalid_blocks(mode):
    m,master,b,a,z=fixture();ss=[sig('1000'),sig('1003')]
    if mode=='calendar':m.today_confirmed=lambda d:False
    if mode=='coverage':b['coverage']['TWSE']['雙日有價']=94
    if mode=='base':ss[1]['base_close']=101
    if mode=='missing':z.pop('1003');b['coverage']['TWSE']['雙日有價']=99
    if mode=='jump':z['1003']=120
    if mode=='other_market':ss=[sig('1000',market='TPEX')]
    if mode=='future':ss[1]['created_at']=T+'T20:00:00+08:00'
    if mode=='board_conflict':b['rows'][0]['收盤']=106
    r,_=h.leader_precision(b,ss,m,master,NOW);assert all_group(r)['榜單命中精度'] is None

def test_observation_tier_separate():
    m,master,b,*_=fixture();a=sig('1000');a['tier']=h.p.TIERS[-1];s=sig('1002');r,_=h.leader_precision(b,[a,s],m,master,NOW);x=next(t for t in r if t['市場']=='TWSE' and t['研究群組']=='前三研究級');assert x['前日封存']==1 and x['榜單召回率']==0

def detail_rows():
    rows=[pairrow(str(i),'H194',.01 if i<1003 else .03) for i in range(1000,1006)]
    for r in rows[3:]:r['研究分級']=h.p.TIERS[0]
    return rows

def test_pair_complete():
    r=h.tier_pairs(detail_rows(),NOW);assert r[0]['相對淨差']==pytest.approx(.02) and r[1]['相對淨差'] is None

@pytest.mark.parametrize('mode',['missing','future','immature','duplicate','conflicting_target'])
def test_pair_guard(mode):
    rr=detail_rows()
    if mode=='missing':rr[0]['淨績效狀態']='缺檔'
    if mode=='future':rr[0]['結果觀測時間']=(NOW+timedelta(days=1)).isoformat()
    if mode=='immature':
        for r in rr:r['出場日']=None
        assert not h.tier_pairs(rr,NOW);return
    if mode=='duplicate':rr.append(rr[0])
    if mode=='conflicting_target':rr[0]['出場日']=None
    if mode in ('duplicate','conflicting_target'):
        with pytest.raises(ValueError):h.tier_pairs(rr,NOW)
    else:assert h.tier_pairs(rr,NOW)[0]['相對淨差'] is None

def test_oos():
    a=[]
    for i in range(30):
        d=NOW.date()-timedelta(days=(31-i)*3);end=d+timedelta(days=1)
        a.append({'比較':h.PAIRS[0][0],'市場':'TWSE','期間':1,'研究日':d.isoformat(),'出場日':end.isoformat(),'決策時間':d.isoformat()+'T20:00:00+08:00','結果觀測時間':end.isoformat()+'T20:00:00+08:00','相對淨差':.02,'狀態':'DESCRIPTIVE_ONLY'})
    assert h.validation(a)[0]['狀態']=='REVIEW_CHALLENGER_EDGE'
    a[19]['結果觀測時間']=NOW.isoformat();assert h.validation(a)[0]['狀態']=='BASELINE_ONLY'
    assert all(r['權重倍率']==1 for r in h.validation(a))

def test_empty_run_and_excel(tmp_path):
    r=h.run(tmp_path,NOW);assert r['status']=='DATA_PENDING' and r['batch_status']=='NO_FROZEN_BATCH' and not list(tmp_path.iterdir())
    w=load_workbook(io.BytesIO(h.excel(r)));assert len(w.sheetnames)==10
