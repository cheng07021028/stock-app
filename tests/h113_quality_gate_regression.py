# -*- coding: utf-8 -*-
"""H113 risk regressions: no spoofed institution, TDCC and liquidity evidence."""
import os,sys
from pathlib import Path
os.environ['GODPICK_H109_OFFICIAL_FETCH']='0'
os.environ['GODPICK_H109_RISK_HISTORY']='0'
os.environ['GODPICK_H110_SCAN_MODE']='SAFE'
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import pandas as pd
from godpick_h113_quality_gate import audit
from godpick_h105_future_blackhorse_engine import _build_blackhorse_overview

def good(code='1234',cat='封測'):
 return {'股票代號':code,'股票名稱':code,'市場別':'上市','類別':cat,
  '成交額百萬':200,'20日均成交額百萬':180,'最新成交量_張':1200,
  'TDCC大戶資料狀態':'ACTUAL','TDCC大戶資料日期':'20261008','TDCC千張大戶週變化pp':0.18,
  'H110法人方向驗證':'OFFICIAL_DATED','外資連買天數_官方':3,
  '外資近3日買賣超_官方':150000,'外資近5日買賣超_官方':200000,
  '投信連買天數_官方':0,'投信近3日買賣超_官方':0,'投信近5日買賣超_官方':0,
  '三大法人近5日合計_官方':210000,
  'H105發動階段':'BH1｜EARLY-BUILD','H105未來發動窗口':'2～5個交易日',
  'H105技術蓄勢分':72,'主升起漲候選':'是','H109決策權威':'RESEARCH_ELIGIBLE',
  'H110證據狀態':'VERIFIED','H105今日強勢排除':'否',
  'H110最終研究優先分':85,'H105黑馬預發動分':79,'H105核心證據完整度%':100,
  'H105過熱追高風險分':10}

asof='2026-10-12'
base=good()
assert audit(base,info_asof=asof)['H113入榜資格']=='QUALIFIED'
for field,value in (
 ('成交額百萬',70),('20日均成交額百萬',70),('最新成交量_張',120),
 ('TDCC千張大戶週變化pp',-0.15),('TDCC大戶資料日期','20260920'),
 ('H110法人方向驗證','UNVERIFIED'),('外資連買天數_官方',None),
 ('外資近5日買賣超_官方',-100000),('三大法人近5日合計_官方',-100000),
 ('H105發動階段','BH0｜WATCH'),('H105未來發動窗口','未定'),
 ('H109決策權威','FADE_VETO'),('H105今日強勢排除','是'),
 ('市場別','興櫃')):
 row={**base, field:value}
 assert audit(row,info_asof=asof)['H113入榜資格']!='QUALIFIED',(field,audit(row,info_asof=asof))
# 3/5-day positive alone cannot masquerade as official consecutive buys.
row={**base,'外資連買天數_官方':None,'法人連買天數':5,'法人連買推估':'代理',
 '外資近3日買賣超':200000,'外資近5日買賣超':300000}
assert audit(row,info_asof=asof)['H113法人連買合格']=='否'
# Verify display governance only yields audited strict entries, not unqualified.
qualified={**base,**audit(base,info_asof=asof)}
weak=good('5678','光學鏡頭');weak['成交額百萬']=15;weak.update(audit(weak,info_asof=asof))
frame=pd.DataFrame([qualified,weak])
out=_build_blackhorse_overview({'research':frame,'waiting':pd.DataFrame(),'actionable':pd.DataFrame()})
assert set(out['股票代號'])=={'1234'},out.get('股票代號')
assert out.iloc[0]['H113入榜資格']=='QUALIFIED'
# Missing evidence -> do not fabricate a "Blackhorse" to fill a slot.
no=_build_blackhorse_overview({'research':pd.DataFrame([weak]),'waiting':pd.DataFrame(),'actionable':pd.DataFrame()})
assert no.empty
print('H113_QUALITY_GATE_REGRESSION PASS')
