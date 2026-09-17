# -*- coding: utf-8 -*-
import pandas as pd
from godpick_h75_executive_decision_export import (
    VERSION, apply_h75_executive_decision, build_h75_today_ai_table,
    build_h75_sector_summary, build_h75_evidence_table,
    build_h75_performance_summary, build_h75_health_summary,
)

assert VERSION == 'v191_h75_executive_decision_export_20260917'

base = pd.DataFrame([
    {
        '股票代號':'1001','股票名稱':'Formal甲','族群名稱':'半導體','市場別':'上市',
        'H74版本':'v191_h74_fresh_mainstream_capital_rotation_truth_20260917',
        'H74研究層級':'F1｜新鮮主流資金核心','H74決策總分':82,'H74強勢加速度分':86,'H74主流新鮮度分':76,
        'H74法人資金加速度分':84,'H74成交資金加速度分':80,'H74大戶鎖碼真相分':74,'H74大戶鎖碼狀態':'LOCKING_CONFIRMED｜週增持',
        'H74訊號新鮮分':78,'H74熟面孔慣性扣分':0,'H74陳舊品質扣分':0,'H74主要優勢':'強勢＋主流＋資金','H74主要警示':'無重大H74警示','H74研究建議':'盤前重驗',
        'H64有效權威':'EFFECTIVE-FORMAL','H64核心共振分':80,'H64品質閘門':'PASS｜強勢×主流×TDCC鎖碼×RR對齊',
        'H61RR品質分':76,'H62增量機會分':74,'H68次日執行狀態':'EXECUTABLE｜盤前仍需重驗',
    },
    {
        '股票代號':'1002','股票名稱':'觀察乙','族群名稱':'塑化','市場別':'上市',
        'H74版本':'v191_h74_fresh_mainstream_capital_rotation_truth_20260917',
        'H74研究層級':'F2｜資金加速優先','H74決策總分':76,'H74強勢加速度分':82,'H74主流新鮮度分':56,
        'H74法人資金加速度分':88,'H74成交資金加速度分':91,'H74大戶鎖碼真相分':51,'H74大戶鎖碼狀態':'UNCONFIRMED_NO_PRIOR｜有當期持股但缺前期比較',
        'H74訊號新鮮分':70,'H74熟面孔慣性扣分':0,'H74陳舊品質扣分':0,'H74主要優勢':'法人＋成交資金','H74主要警示':'TDCC缺前期，鎖碼未確認','H74研究建議':'優先觀察',
        'H64有效權威':'A-MINUS','H64核心共振分':67,'H64品質閘門':'BLOCK-MAINSTREAM｜未通過當前主流/族群共振',
        'H61RR品質分':68,'H62增量機會分':66,'H68次日執行狀態':'NO-FORMAL｜僅研究觀察',
    },
    {
        '股票代號':'1003','股票名稱':'風險丙','族群名稱':'電子','市場別':'上櫃',
        'H74版本':'v191_h74_fresh_mainstream_capital_rotation_truth_20260917',
        'H74研究層級':'F2｜資金加速優先','H74決策總分':73,'H74強勢加速度分':78,'H74主流新鮮度分':68,
        'H74法人資金加速度分':72,'H74成交資金加速度分':75,'H74大戶鎖碼真相分':52,'H74大戶鎖碼狀態':'UNCONFIRMED_NO_PRIOR｜有當期持股但缺前期比較',
        'H74訊號新鮮分':66,'H74熟面孔慣性扣分':0,'H74陳舊品質扣分':0,'H74主要優勢':'主流＋資金','H74主要警示':'TDCC缺前期，鎖碼未確認','H74研究建議':'等待RR改善',
        'H64有效權威':'A-MINUS','H64核心共振分':70,'H64品質閘門':'BLOCK-EXEC｜RR/耗竭/增量空間未完成',
        'H61RR品質分':22,'H62增量機會分':50,'H68次日執行狀態':'NO-FORMAL｜僅研究觀察',
    },
])

out = apply_h75_executive_decision(base)
assert out.loc[0,'H75主管決策層級'].startswith('F0')
assert '主流' in out.loc[1,'H75Formal主要缺口']
assert 'RR' in out.loc[2,'H75Formal主要缺口']
assert out.loc[1,'H64有效權威'] == 'A-MINUS'  # H75 never promotes authority

today = build_h75_today_ai_table(base, max_rows=10)
assert list(today['股票代號'])[:1] == ['1001']
assert today.iloc[0]['是否正式推薦'].startswith('是')
assert today.loc[today['股票代號'].eq('1002'),'是否正式推薦'].iloc[0].startswith('否')

ev = build_h75_evidence_table(base, max_rows=10)
assert 'H75Formal主要缺口' in ev.columns and 'H64品質閘門' in ev.columns

sector = pd.DataFrame([{
    'H60族群排名':1,'類別':'半導體','H60族群複利機會分':72,'H60族群三因子分':71,'H53族群共振分':68,'H53族群廣度分':64,
    'H53族群攻擊分':70,'H53族群資金分':73,'H57族群前三資金加速':80,'H57族群點火廣度分':66,'H60族群主升分':74,'H60族群鎖碼分':69,'H60族群真實鎖碼覆蓋率%':100
}])
ss = build_h75_sector_summary(sector)
assert ss.iloc[0]['今日族群解讀'].startswith('主流優先')

perf = pd.DataFrame({'績效指標':['H74_F2成熟樣本','H74_F2正報酬率%','H74平均RankIC'],'目前數值':[12,58,0.12]})
ps = build_h75_performance_summary(perf)
assert '主管解讀' in ps.columns and '樣本仍少' in ps.iloc[0]['主管解讀']

health = pd.DataFrame({'類型':['資料健康','資料健康'],'項目':['TDCC缺前期比較','正式推薦可用'],'數值':[1687,False]})
hs = build_h75_health_summary(health)
assert '主管解讀' in hs.columns
print('PASS H75 executive decision/export')
