# -*- coding: utf-8 -*-
from pathlib import Path
import sys, pandas as pd
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from godpick_h105_future_blackhorse_engine import analyze_candidate, decorate_decision_tables, export_contract_summary, VERSION
from godpick_h80_record_feedback import build_research_tracking_frame


def future(code='9991', name='未來黑馬', sector='測試族群'):
    return {
        '股票代號':code,'股票名稱':name,'類別':sector,'市場別':'上市','H99市場資料日':'2026-10-06',
        '今日漲幅%':0.8,'近5日漲幅%':2.3,'近20日漲幅%':4.0,'均量比':1.25,
        '外資近1日買賣超':1800,'外資近3日買賣超':2400,'外資近5日買賣超':-500,
        '投信近1日買賣超':300,'投信近3日買賣超':450,'投信近5日買賣超':80,
        '三大法人近1日合計':2200,'三大法人近3日合計':3200,'三大法人近5日合計':600,
        '法人連買天數_官方':2,'法人籌碼官方分數':72,'大戶鎖碼分數':72,'大戶承接分':68,
        '起漲前兆分數':78,'型態突破分數':66,'技術結構分數':74,'20日壓力距離%':2.0,
        'Entry進場買點分':67,'Risk風控安全分':70,'交易可行分數':72,'風險報酬比_決策':1.7,
        'H102族群衝擊分':76,'H102族群動態狀態':'ROTATION','類股加速度':68,'族群資金流分數':70,'族群輪動分':73,
        '同族群強勢比例':58,'同族群平均量能分':60,'類股熱度排名':6,'類股熱度分數':68,'類股平均漲幅':3.0,
        '追價風險分數_決策':35,'近期強勢狀態':'未見近期強勢','H79決策層級':'研究推薦',
        'H104推薦性質':'近期首次觀察｜已知歷史未見','H104先前入選日數':0,'H104近期族群入選日數':0,
    }

def hot():
    r=future('9992','今日已噴')
    r.update({'今日漲幅%':8.2,'近5日漲幅%':19.0,'近20日漲幅%':36.0,'均量比':2.8,'追價風險分數_決策':82,'近期強勢狀態':'強勢主升'})
    return r

f=analyze_candidate(future()); h=analyze_candidate(hot())
assert f['H105今日強勢排除']=='否' and h['H105今日強勢排除']=='是'
assert f['H108研究優先分'] > h['H108研究優先分']
assert f['H105選配證據完整度%']==0 and f['H105證據完整度%']>=60
r=future(); r.pop('今日漲幅%'); r['區間漲跌幅%']=188
assert analyze_candidate(r)['H105今日強勢排除']=='否'

# Formal order untouched.
a=future('9910','FormalA','甲'); b=future('9911','FormalB','乙'); b['起漲前兆分數']=95
act=pd.DataFrame([a,b])
out=decorate_decision_tables({'actionable':act.copy(),'research':pd.DataFrame()},candidate_df=act)
assert out['actionable']['股票代號'].astype(str).tolist()==['9910','9911']

# Blackhorse excludes today's hot.
df=pd.DataFrame([future(),hot()])
out=decorate_decision_tables({'research':df.copy(),'waiting':pd.DataFrame(),'actionable':pd.DataFrame(),'health':pd.DataFrame()},candidate_df=df)
assert '9991' in set(out['blackhorse_overview']['股票代號'].astype(str))
assert '9992' not in set(out['blackhorse_overview']['股票代號'].astype(str))
assert export_contract_summary(out)['ok']

# Persistence remains research-only.
r=future(); r.update(analyze_candidate(r))
tracked=build_research_tracking_frame(pd.DataFrame([r]),pd.DataFrame([r]),excluded_codes=[])
assert len(tracked)==1
rec=tracked.iloc[0]
assert rec['是否可直接買進']=='否' and str(rec['H105Formal權限']).startswith('LOCKED')

# Broker optional, never faked.
assert analyze_candidate(future())['H105券商分點分'] is None
x=future(); x.update({'券商分點分數':76,'Top5分點淨買比%':16,'券商分點連買天數':3,'分點集中度%':68})
assert analyze_candidate(x)['H105券商分點分'] > 50

print('H108_FULL_REGRESSION: PASS |', VERSION)
