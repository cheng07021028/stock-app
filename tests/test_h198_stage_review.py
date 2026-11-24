import numpy as np
import pytest
from test_h191_opportunity_engine import bars,DAYS,fixture
import godpick_h191_opportunity_engine as h
import godpick_h198_stage_review as s
import godpick_h193_joint_decision as j
from test_h193_joint_decision import inputs,NOW
from test_h195_command_center import candidate,joint
import godpick_h195_command_center as cmd

def make(prices, volumes=None, high=None):
    b=bars(prices)
    for i,v in (volumes or {}).items():
        d,o,hi,lo,c,_,a,n=b[i];b[i]=(d,o,hi,lo,c,v,round(c*v),n)
    if high:
        d,o,hi,lo,c,v,a,n=b[-1];b[-1]=(d,o,high,lo,c,v,a,n)
    return b

def review(b,liquid=True):
    f,e=h.calc_features(b,DAYS);assert f is not None,e
    return s.assess(b,DAYS,f,liquid=liquid)

def pattern(tail):return np.r_[np.linspace(90,98,40),np.tile([98,99,100,99],4),tail]

def test_base():
    p=np.r_[np.linspace(110,95,20),np.linspace(95,100,20),np.tile([99,100,101,100],5)]
    r=review(make(p,{i:300000 for i in range(54,60)}));assert r['H198階段']==s.POOLS[0] and r['H198研究分'] is not None and r['H198整理量縮比']==.6
    assert '法人證據待補' in r['H198補充證據']

def test_breakout():
    r=review(make(pattern([99,99,100,103]),{59:1000000}));assert r['H198階段']==s.POOLS[1] and r['H198突破基準日']==DAYS[-1] and r['H198箱頂參考']==100.5

def test_retest():
    r=review(make(pattern([99,103,101.2,101.5]),{57:1000000,58:350000,59:350000}));assert r['H198階段']==s.POOLS[2] and r['H198箱頂參考']==100.5 and r['H198突破基準日']==DAYS[57]

def test_failed_breakout():
    r=review(make(pattern([99,103,101.2,98]),{57:1000000,58:350000,59:1000000}));assert r['H198階段']==s.POOLS[3] and '跌破原箱頂' in r['H198反證'] and r['H198研究分'] is None

def test_intraday_wick():
    r=review(make(pattern([99,99,100,100]),{59:1000000},high=106));assert r['H198階段']==s.POOLS[3] and '盤中越箱頂' in r['H198反證'] and '長上影' in r['H198反證']

def test_low_volume():
    r=review(make(pattern([99,99,100,103])));assert r['H198階段']==s.POOLS[3] and '量能未達' in r['H198反證']

def test_chase():
    r=review(make(pattern([105,110,115,119]),{59:1000000}));assert r['H198階段']==s.POOLS[3] and '乖離過大' in r['H198反證']

def test_liquidity():
    r=review(make(pattern([99,99,100,103]),{59:1000000}),False);assert r['H198階段']=='流動性待確認' and r['H198研究分'] is None

def test_future_ignored():
    b=make(pattern([99,99,100,103]),{59:1000000});f,_=h.calc_features(b,DAYS);r=s.assess(b,DAYS,f,liquid=True);assert s.assess(b+[('2099-01-01',1,1,1,1,1,1,1)],DAYS,f,liquid=True)==r

def test_missing():
    r=s.assess([],DAYS,{},liquid=True);assert r['H198階段']=='待補證'
    f=fixture();f['markets']['TPEX'].pop(DAYS[0]);x=h.evaluate(**f);assert x['h198']['valid']==100 and x['h198']['counts']['待補證']==100

def test_full_counts_and_original_pool():
    f=fixture();x=h.evaluate(**f,top=1);assert sum(x['h198']['counts'].values())==200 and x['pool_counts']['主流強勢']==40 and all(len(rs)<=1 for rs in x['h198']['pools'].values())

def test_joint_same_day_only():
    t,f=inputs();a=t['diagnostic'][0];a.update({'H198資料日期':t['asof'],'H198階段':s.POOLS[3],'H198反證':'測試'});r=j.combine(t,f);assert r['counts'][j.TIERS[0]]==1 and r['audit'][0]['H198反證']=='測試';a['H198資料日期']='2026-11-20';assert 'H198反證' not in j.combine(t,f)['audit'][0]

def test_joint_role08(tmp_path):
    t,f=inputs();t['diagnostic'][0].update({'H198資料日期':t['asof'],'H198階段':s.POOLS[3],'H198反證':'測試反證'});r=j.combine(t,f);d=j.detail(r,f,'TWSE','1101',root=tmp_path,now=NOW);assert any(x['事項'].startswith('H198') for x in d['roles']['08']['tables']['個股反方查核'])

def test_command_and_excel(tmp_path):
    from test_h194_performance import NOW as N
    from openpyxl import Workbook
    c=candidate();c['H198階段']=s.POOLS[3];c['H198反證']='測試反證';r=cmd.build(joint([c]),{},tmp_path,N,0);assert r['rows'][0]['H198階段']==s.POOLS[3] and r['rows'][0]['交易狀態']=='僅研究／未授權';cmd.ar._write(tmp_path/cmd.REL,cmd.ar._seal(r));w=Workbook();w.active.title='原表';ws=cmd.append_manager(w,tmp_path,'2026-11-23',N);assert ws['Y8'].value=='僅研究／未授權' and ws['Z8'].value==s.POOLS[3]
