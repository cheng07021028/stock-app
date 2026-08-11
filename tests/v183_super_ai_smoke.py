# -*- coding: utf-8 -*-
from __future__ import annotations
import math
import time
from pathlib import Path
import sys
import pandas as pd
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from godpick_super_ai_engine import score_super_ai_row, apply_super_ai_engine, SUPER_AI_COLUMNS
import godpick_super_ai_experience as expmod

BASE={
    '股票代號':'3711','股票名稱':'測試股','類別':'半導體業','最新價':650,
    'AI Alpha品質分':82,'AI Timing時機分':76,'AI Risk風控分':68,'AI Continuation延續分':81,
    '可操作分':72,'主流主升優先分':84,'AI戰術風報比':2.0,'追價風險分':35,'隔日耗竭風險分':32,
    '當日量比':1.5,'當日收盤位置%':78,'今日漲幅%':2.2,'近5日漲幅%':6,
    '實戰觸發價':655,'觸發後守價':648,'停損參考':620,'第一壓力價':700,
    '主要進場路徑':'回測守價','守價回測距離%':1.0,'操作許可':'條件式1%｜觸發、守價、大盤同步',
    '正式推薦分區':'A-｜準主推薦小量試單','資料完整度評分':85,'掃描品質狀態':'完整','正式推薦可用':True,
}
SUPPORT={
    'margin_by_stock':{'3711':{'融資前日餘額張':10000,'融資今日餘額張':9400,'融資增減張':-600,'融券前日餘額張':1000,'融券今日餘額張':1150,'融券增減張':150}},
    'taifex':{'外資臺指期未平倉淨口數':8000,'PCR未平倉量比%':105},
    'etf':{'ETF市場確認分':68,'ETFs':{'0052':{'score':72},'00881':{'score':70}}},
}
RISK={
    'margin_by_stock':{'3711':{'融資前日餘額張':10000,'融資今日餘額張':11200,'融資增減張':1200,'融券前日餘額張':1000,'融券今日餘額張':900,'融券增減張':-100}},
    'taifex':{'外資臺指期未平倉淨口數':-9000,'PCR未平倉量比%':165},
    'etf':{'ETF市場確認分':38,'ETFs':{'0052':{'score':35},'00881':{'score':40}}},
}

def main():
    a=score_super_ai_row(BASE,SUPPORT,{})
    probs=[a[x] for x in ['SuperAI開高走高%','SuperAI開高走低%','SuperAI開低走高%','SuperAI開低走低%','SuperAI平開震盪%']]
    assert abs(sum(probs)-100)<=0.25, probs
    assert all(0<=x<=100 for x in probs)
    assert a['SuperAI進場狀態'].startswith('READY') or a['SuperAI進場狀態'].startswith('WAIT-PULLBACK')

    blocked=dict(BASE); blocked['操作許可']='禁止新倉｜只觀察'
    b=score_super_ai_row(blocked,SUPPORT,{})
    assert b['SuperAI進場狀態'].startswith('AVOID')

    stale=dict(BASE); stale['掃描品質狀態']='官方因子日期/可信度不足｜正式推薦暫停'; stale['正式推薦可用']=False
    c=score_super_ai_row(stale,SUPPORT,{})
    assert c['SuperAI進場狀態'].startswith('WAIT-DATA')
    assert c['SuperAI本週進場等級'].startswith('DATA')

    d=score_super_ai_row(BASE,RISK,{})
    assert d['SuperAI進場狀態'].startswith('WAIT-MARKET'), d['SuperAI進場狀態']
    assert '融資券結構偏不利' in d['SuperAI反對理由']
    assert 'ETF市場確認偏弱' in d['SuperAI反對理由']

    # Missing macro inputs remain unknown instead of becoming false zero evidence.
    e=score_super_ai_row(BASE,{'margin_by_stock':{},'taifex':{},'etf':{'ETFs':{},'ETF市場確認分':None}}, {})
    assert e['SuperAI融資影響分'] is None and e['SuperAI_ETF確認分'] is None and e['SuperAI期貨情境分'] is None

    # 1715-row SuperAI advisory stage should stay lightweight after the heavy decision engine.
    df=pd.DataFrame([BASE]*1715)
    t=time.perf_counter(); out=apply_super_ai_engine(df,SUPPORT,{}); elapsed=time.perf_counter()-t
    assert len(out)==1715 and set(SUPER_AI_COLUMNS).issubset(out.columns)
    assert elapsed<3.0, f'SuperAI layer too slow: {elapsed:.3f}s'

    # Bounded experience calibration: 30-99 samples <= +/-3pp; 100+ <= +/-8pp.
    records=[]
    for i in range(50): records.append({'SuperAI模型版本':'super_ai_test','SuperAI隔日上漲機率%':80,'推薦後1日%':1 if i<20 else -1,'SuperAI進場狀態':'READY'})
    old_write=expmod._write_async; expmod._write_async=lambda *a,**k: None
    try:
        profile=expmod.refresh_super_ai_experience_profile(records)
    finally:
        expmod._write_async=old_write
    assert abs(float(profile['applied_probability_bias_pp']))<=3.0001
    print(f'PASS V183 SuperAI smoke｜1715 rows={elapsed:.3f}s｜risk gating / probabilities / bounded learning OK')

if __name__=='__main__': main()
