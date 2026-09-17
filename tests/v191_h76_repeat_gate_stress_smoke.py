# -*- coding: utf-8 -*-
import time
import pandas as pd
from godpick_h76_daily_alpha_core_split import apply_h76_daily_alpha_core_split

rows=[]
for i in range(1700):
    familiar=(i%7==0)
    renewed=familiar and (i%14==0)
    rows.append({
        '股票代號':f'{1000+i:04d}','股票名稱':f'S{i}','族群名稱':f'G{i%18}',
        'H75版本':'v191_h75_executive_decision_export_20260917',
        'H74決策總分':72+(i%9) if renewed else 64+(i%11),
        'H74強勢加速度分':78 if renewed else 62+(i%15),
        'H74主流新鮮度分':70 if renewed else 50+(i%18),
        'H74法人資金加速度分':76 if renewed else 52+(i%25),
        'H74成交資金加速度分':80 if renewed else 48+(i%28),
        'H74訊號新鮮分':76 if renewed else 48+(i%24),
        'H74大戶鎖碼狀態':'LOCKING_CONFIRMED｜週增持' if renewed else 'UNCONFIRMED_NO_PRIOR｜有當期持股但缺前期比較',
        'H74熟面孔慣性扣分':10 if familiar else 0,
        '近5次入榜次數':4 if familiar else i%3,'連續入榜次數':3 if familiar else 0,'H61重複慣性扣分':10 if familiar else 0,
        'H72研究層級':'E1｜多模型共振核心' if i%5==0 else 'E2｜多模型優先觀察','H73研究層級':'L1｜領先共振核心' if i%6==0 else 'L2｜領先觀察',
        'H72風險調整分':65+(i%20),'H72品質獲利模型分':60+(i%25),'H72成長動能模型分':58+(i%30),'H72法人需求模型分':60+(i%28),
        'H64核心共振分':60+(i%22),'H61RR品質分':55+(i%25),'H62增量機會分':55+(i%27),
        'H64有效權威':'A-MINUS','H68次日執行狀態':'NO-FORMAL｜僅研究觀察',
    })
base=pd.DataFrame(rows)
t=time.perf_counter(); out=apply_h76_daily_alpha_core_split(base); dt=time.perf_counter()-t
assert len(out)==1700
assert out['H76每日新Alpha順位'].dropna().is_unique
assert out['H76結構核心順位'].dropna().is_unique
blocked=out['H76熟面孔狀態'].astype(str).str.startswith('REPEAT-GATED')
renewed_state=out['H76熟面孔狀態'].astype(str).str.startswith('RENEWED')
assert blocked.any() and renewed_state.any()
assert (~out.loc[blocked,'H76每日榜資格'].astype(str).str.startswith('YES')).all()
print(f'PASS H76 1700 stress rows={len(out)} seconds={dt:.4f} blocked={int(blocked.sum())} renewed={int(renewed_state.sum())}')
