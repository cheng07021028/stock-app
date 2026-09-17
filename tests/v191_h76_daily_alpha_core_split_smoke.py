# -*- coding: utf-8 -*-
import pandas as pd
from godpick_h76_daily_alpha_core_split import (
    VERSION, apply_h76_daily_alpha_core_split, build_h76_daily_alpha_table,
    build_h76_structural_core_table, build_h76_evidence_table, build_h76_governance_summary,
)

assert VERSION == 'v191_h76_daily_alpha_core_split_repeat_evidence_truth_20260918'

rows = [
    { # Quanta-like: structurally strong, institutional strong, but not enough fresh daily evidence + familiar gate.
        '股票代號':'2382','股票名稱':'熟面孔甲','族群名稱':'AI伺服器','市場別':'上市',
        'H75版本':'v191_h75_executive_decision_export_20260917',
        'H74決策總分':60.60,'H74強勢加速度分':70.56,'H74主流新鮮度分':56.51,
        'H74法人資金加速度分':96.26,'H74成交資金加速度分':53.08,'H74訊號新鮮分':57.74,
        'H74大戶鎖碼狀態':'UNCONFIRMED_NO_PRIOR｜有當期持股但缺前期比較','H74熟面孔慣性扣分':8,
        '近5次入榜次數':2,'連續入榜次數':1,'H61重複慣性扣分':8,
        'H72研究層級':'E1｜多模型共振核心','H73研究層級':'L1｜領先共振核心','H72風險調整分':80.78,
        'H72品質獲利模型分':81.58,'H72成長動能模型分':91.52,'H72法人需求模型分':96.09,
        'H64核心共振分':66,'H61RR品質分':58,'H62增量機會分':58,'H64有效權威':'A-MINUS','H68次日執行狀態':'NO-FORMAL｜僅研究觀察',
    },
    { # genuinely fresh new alpha
        '股票代號':'1002','股票名稱':'新Alpha乙','族群名稱':'塑化','市場別':'上市',
        'H75版本':'v191_h75_executive_decision_export_20260917',
        'H74決策總分':78,'H74強勢加速度分':84,'H74主流新鮮度分':75,'H74法人資金加速度分':80,
        'H74成交資金加速度分':86,'H74訊號新鮮分':79,'H74大戶鎖碼狀態':'LOCKING_CONFIRMED｜週增持','H74熟面孔慣性扣分':0,
        '近5次入榜次數':0,'連續入榜次數':0,'H61重複慣性扣分':0,
        'H72研究層級':'E2｜多模型優先觀察','H73研究層級':'L2｜領先觀察','H72風險調整分':72,
        'H72品質獲利模型分':62,'H72成長動能模型分':78,'H72法人需求模型分':80,
        'H64核心共振分':69,'H61RR品質分':70,'H62增量機會分':74,'H64有效權威':'A-MINUS','H68次日執行狀態':'NO-FORMAL｜僅研究觀察',
    },
    { # familiar, but genuinely renewed with multiple new evidence dimensions
        '股票代號':'1003','股票名稱':'續強丙','族群名稱':'封測','市場別':'上市',
        'H75版本':'v191_h75_executive_decision_export_20260917',
        'H74決策總分':80,'H74強勢加速度分':82,'H74主流新鮮度分':72,'H74法人資金加速度分':76,
        'H74成交資金加速度分':84,'H74訊號新鮮分':78,'H74大戶鎖碼狀態':'ACCUMULATING｜小幅增持','H74熟面孔慣性扣分':10,
        '近5次入榜次數':4,'連續入榜次數':3,'H61重複慣性扣分':10,
        'H72研究層級':'E1｜多模型共振核心','H73研究層級':'L1｜領先共振核心','H72風險調整分':78,
        'H72品質獲利模型分':76,'H72成長動能模型分':82,'H72法人需求模型分':79,
        'H64核心共振分':74,'H61RR品質分':72,'H62增量機會分':76,'H64有效權威':'A-MINUS','H68次日執行狀態':'NO-FORMAL｜僅研究觀察',
    },
    { # Formal authority is never demoted by H76 even if familiar.
        '股票代號':'1004','股票名稱':'Formal丁','族群名稱':'半導體','市場別':'上市',
        'H75版本':'v191_h75_executive_decision_export_20260917',
        'H74決策總分':67,'H74強勢加速度分':70,'H74主流新鮮度分':60,'H74法人資金加速度分':65,
        'H74成交資金加速度分':62,'H74訊號新鮮分':58,'H74大戶鎖碼狀態':'LOCKING_CONFIRMED｜週增持','H74熟面孔慣性扣分':12,
        '近5次入榜次數':5,'連續入榜次數':4,'H61重複慣性扣分':12,
        'H72研究層級':'E1｜多模型共振核心','H73研究層級':'L1｜領先共振核心','H72風險調整分':80,
        'H72品質獲利模型分':82,'H72成長動能模型分':80,'H72法人需求模型分':82,
        'H64核心共振分':82,'H61RR品質分':80,'H62增量機會分':78,'H64有效權威':'EFFECTIVE-FORMAL','H68次日執行狀態':'RECHECK｜盤前重驗',
    },
]
base=pd.DataFrame(rows)
out=apply_h76_daily_alpha_core_split(base)
q=out.loc[out['股票代號'].eq('2382')].iloc[0]
assert q['H76分流層級'].startswith('C1'), q.to_dict()
assert q['H76熟面孔狀態'].startswith('REPEAT-GATED')
assert q['H76重複推薦門檻'].startswith('BLOCK-DAILY')
assert str(q['H76每日榜資格']).startswith('NO')

fresh=out.loc[out['股票代號'].eq('1002')].iloc[0]
assert fresh['H76分流層級'].startswith('D1')
renew=out.loc[out['股票代號'].eq('1003')].iloc[0]
assert renew['H76分流層級'].startswith(('D1','D2'))
assert renew['H76熟面孔狀態'].startswith('RENEWED')
formal=out.loc[out['股票代號'].eq('1004')].iloc[0]
assert formal['H76分流層級'].startswith('F0') and formal['H64有效權威']=='EFFECTIVE-FORMAL'

daily=build_h76_daily_alpha_table(base,max_rows=10)
assert '2382' not in set(daily.get('股票代號',pd.Series(dtype=str)).astype(str))
assert {'1002','1003','1004'}.issubset(set(daily['股票代號'].astype(str)))
core=build_h76_structural_core_table(base,max_rows=10)
assert '2382' in set(core['股票代號'].astype(str))
ev=build_h76_evidence_table(base,max_rows=10)
assert {'H76重複推薦門檻','H76新證據摘要','H76每日榜資格'}.issubset(ev.columns)
gov=build_h76_governance_summary(base)
assert int(gov.loc[gov['治理項目'].eq('熟面孔被每日榜阻擋'),'目前狀態'].iloc[0]) >= 1
print('PASS H76 daily alpha/core split + repeat evidence gate')
