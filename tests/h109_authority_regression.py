# -*- coding: utf-8 -*-
"""H109 no-lookahead / hard-veto / research rebalancing / compatibility tests."""
import os,sys
from pathlib import Path
import datetime as dt
import pandas as pd
os.environ['GODPICK_H109_OFFICIAL_FETCH']='0'
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import godpick_h105_future_blackhorse_engine as engine
from godpick_h109_authority_governor import govern, revenue_freshness, join_official, parse_month, _roc_day
from h108_full_regression import future
engine.save_discovery_snapshot = None
engine.save_sector_snapshot = None

# Veto overrides arbitrarily positive old novelty, and never alters actionable.
b= dict(future('2059','川湖','AI伺服器'), **{'今日漲幅%':-10.,'H102族群衝擊分':94.,'H108族群生命週期':'IGNITION｜族群點火',
                                   'H108研究優先分':98.,'進場可執行判定':'BLOCK','H51狀態':'WAIT-RECLAIM', '近期強勢狀態':'假強排除'})
res=govern(b,asof=dt.date(2026,10,7))
assert res['H109決策權威']=='SHOCK_QUARANTINE' and res['H109最終研究優先分']<=49,res
fade=dict(future('2382','廣達','AI伺服器'),**{'H108族群生命週期':'FADE｜退潮', 'H102族群衝擊分':94,'H108研究優先分':99})
assert govern(fade,asof=dt.date(2026,10,7))['H109決策權威']=='FADE_VETO'
assert govern(fade,asof=dt.date(2026,10,7))['H109跨層衝突']=='H102_IGNITION_vs_H108_FADE'

# Real H108 IGNITION before full price response remains eligible.
pre=future('3026','禾伸堂','被動元件')
pre.update({'H108族群生命週期':'IGNITION｜族群點火','H108研究優先分':82.,'H102族群衝擊分':94.})
assert govern(pre,asof=dt.date(2026,10,7))['H109決策權威']=='RESEARCH_ELIGIBLE'

# Conflict where H102 sparks but H108 is neutral is WAITING only until independently confirmed.
pending=dict(pre,**{'H108族群生命週期':'NEUTRAL｜未確認'})
assert govern(pending,asof=dt.date(2026,10,7))['H109決策權威']=='FAST_SHOCK_RECHECK'

# No hindsight. Revenue due around the 10th; 2026-08 on 10/07 is 'pending' not definitely delinquent.
assert revenue_freshness({'營收資料日期':'202608'},dt.date(2026,10,7))[0].startswith('PENDING')
assert revenue_freshness({'營收資料日期':'202608'},dt.date(2026,10,11))[0].startswith('STALE')
assert parse_month('11509')==(2026,9)
assert _roc_day('1151008') == dt.date(2026,10,8)
assert govern({'H109營收資料年月':'202611', 'H108研究優先分':100.},asof=dt.date(2026,10,7))['H109決策權威']=='FUTURE_DATA_VETO'

# No source -> cannot assert negative official event; revenue month-over-month fall + positive YoY is CAUTION.
rev=govern({'營收資料日期':'202609','月營收YoY%':196.2,'月營收MoM%':-30.9},asof=dt.date(2026,10,7))
assert rev['H109催化方向'].startswith('CAUTION') and rev['H109決策權威']=='RESEARCH_ELIGIBLE',rev
assert govern({'H109官方事件方向':'NEGATIVE'},asof=dt.date(2026,10,7))['H109催化方向'].startswith('UNVERIFIED')
assert govern({'H109官方事件方向':'NEGATIVE','H109事件來源':'https://mops.twse.com.tw/xx','今日漲幅%':-5},asof=dt.date(2026,10,7))['H109決策權威']=='SHOCK_QUARANTINE'

# Official data only replaces older row. No forced replacement of a newer historical record.
new={'H109營收資料年月':'202609','H109官方營收YoY%':25.1,'H109官方營收MoM%':6.3,'H109營收來源':'https://openapi.twse.com.tw/v1/opendata/t187ap05_L'}
assert join_official({'營收資料日期':'202608'},new)['月營收YoY%_官方']==25.1
assert '月營收YoY%_官方' not in join_official({'營收資料日期':'202610'},new)

# Integration: high-score crash + fade never enter Research nor Blackhorse even as only candidates.
cr=future('2059','川湖','AI伺服器'); cr.update({'今日漲幅%':-9.5,'H99市場資料日':'2026-10-07','進場可執行判定':'BLOCK','近期強勢狀態':'假強排除'})
fa=future('2382','廣達','弱族群'); fa.update({'H102族群動態狀態':'FADE｜退潮','H99市場資料日':'2026-10-07'})
base=pd.DataFrame([cr,fa])
out=engine.decorate_decision_tables({'research':base.copy(), 'waiting':pd.DataFrame(), 'actionable':pd.DataFrame(), 'health':pd.DataFrame()},candidate_df=base)
assert len(out['research'])==0, out['research'][['股票代號','H109決策權威']]
assert len(out['blackhorse_overview'])==0
assert set(out['waiting']['H109決策權威'])=={'SHOCK_QUARANTINE','FADE_VETO'},out['waiting'][['股票代號','H109決策權威','H108族群生命週期']]

# Good sector members and Formal keep legitimate paths.
good= pd.DataFrame([pre, future('2467','志聖','其他電子')])
good['H99市場資料日']='2026-10-07'
out=engine.decorate_decision_tables({'research':good.copy(),'waiting':pd.DataFrame(),'actionable':base.copy()},candidate_df=pd.concat([good,base],ignore_index=True))
assert set(out['research']['股票代號'])=={'3026','2467'},out['research'][['股票代號','H109決策權威']]
assert out['actionable']['股票代號'].astype(str).tolist()==['2059','2382'], 'Formal order must stay same'
assert 'H109決策權威' in out['blackhorse_overview'].columns
assert all(x=='RESEARCH_ELIGIBLE' for x in out['blackhorse_overview']['H109決策權威'])
print('H109_AUTHORITY_REGRESSION PASS (veto, lookahead, dated revenue, shock, sector, research, formal, Excel schema)')
