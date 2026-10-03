"""Bounded candidate discovery and observed recommendation continuity. No buy authority."""
from pathlib import Path
from datetime import date,timedelta
import json
import pandas as pd
from godpick_h103_decision_integrity import text,number
VERSION='v191_h104_daily_discovery_20261003'
COLUMNS=['H104版本','H104推薦性質','H104先前入選日數','H104前次資料日','H104前次優先分','H104優先分變化','H104延續說明','H104歷史範圍']
SCORES=['H79研究推薦分','H89雙軌研究排序分','V188股神作戰優先分','股神推薦優先分','候選強度分','推薦總分']

def select_discovery_input(frame,limit=240):
    """75% existing global leaders, 25% category round-robin; all still face gates."""
    if not isinstance(frame,pd.DataFrame) or frame.empty:return pd.DataFrame()
    out=frame.copy()
    if '股票代號' not in out:return out.head(limit)
    out['股票代號']=out['股票代號'].map(lambda x:text(x).removesuffix('.0'))
    out=out.loc[out['股票代號'].ne('')].copy()
    keys=[]
    for i,c in enumerate(SCORES):
        if c in out:
            k=f'__h104_score{i}';out[k]=pd.to_numeric(out[c],errors='coerce');keys.append(k)
    out=out.sort_values(keys+['股票代號'],ascending=[False]*len(keys)+[True],kind='stable',na_position='last').drop_duplicates('股票代號')
    if len(out)>limit and '類別' in out:
        main=out.head(int(limit*.75)); remaining=out.loc[~out['股票代號'].isin(main['股票代號'])].copy()
        remaining['__h104_sector_rank']=remaining.groupby(remaining['類別'].fillna('未分類')).cumcount()
        remaining=remaining.sort_values(['__h104_sector_rank']+keys+['股票代號'],ascending=[True]+[False]*len(keys)+[True],kind='stable',na_position='last')
        out=pd.concat([main,remaining.head(limit-len(main))],ignore_index=True)
    return out.head(limit).drop(columns=[c for c in out if c.startswith('__h104_')]).reset_index(drop=True)

def _day(row):
    # Market observation date is distinct from the run/export date.
    for key in ('H99市場資料日','市場資料日期','最新K線日期','推薦日期'):
        s=text(row.get(key))[:10]
        try:return date.fromisoformat(s).isoformat()
        except ValueError:pass
    return ''

def continuity(row,history):
    day=_day(row);code=text(row.get('股票代號')).removesuffix('.0')
    cutoff=(date.fromisoformat(day)-timedelta(days=14)).isoformat() if day else ''
    past=[r for r in history if cutoff<=_day(r)<day and _day(r)] if day else []
    matches=[r for r in past if text(r.get('股票代號')).removesuffix('.0')==code]
    matches.sort(key=lambda r:(_day(r),text(r.get('推薦批次時間'))),reverse=True)
    prev=matches[0] if matches else {};old=number(prev.get('H102動態優先分'));cur=number(row.get('H102動態優先分'))
    delta=round(cur-old,2) if cur is not None and old is not None else None
    kind='待確認｜缺少可比較歷史' if not past else '已知歷史未見｜近期首次觀察' if not matches else '延續追蹤｜非首次推薦'
    note='只比較已保存的前14日紀錄；不代表完整市場或連續交易日。'
    if matches:
        note='前次已入選；本輪層別='+ (text(row.get('H103最終層別')) or '未提供') +'；延續標記不授予研究或買進資格。'
        if delta is not None and delta>=3:
            kind='延續追蹤｜排序指標改善';note+='優先分提高至少3分，僅為模型指標變化，非已驗證新催化。'
        elif delta is not None and delta<=-3:note+='優先分下降至少3分，需留意轉弱。'
        else:note+='尚無足夠可比較的排序改善，不包裝成全新機會。'
    return dict(zip(COLUMNS,[VERSION,kind,len({_day(r) for r in matches}),_day(prev),old,delta,note,f'{cutoff} ≤ 資料日 < {day}；已知{len({_day(r) for r in past})}個日期']))

def load_history(base_dir):
    path=Path(base_dir)/'godpick_records.json'
    try:
        data=json.loads(path.read_text(encoding='utf-8-sig'))
        rows=data if isinstance(data,list) else data.get('records',[])
        return [r for r in rows if isinstance(r,dict) and (text(r.get('H103最終層別')).upper() in ('ACTIONABLE','RESEARCH') or '研究' in text(r.get('紀錄層級')) or '正式' in text(r.get('紀錄層級'))) and '影子' not in text(r.get('紀錄層級'))]
    except (OSError,ValueError,AttributeError):return []
