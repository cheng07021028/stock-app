# -*- coding: utf-8 -*-
"""V191-H184 | Twelve research roles: evidence-first, research-only.

Additive research module. NEVER changes H109/H126 order authority, transactions,
recommendation records, or live settings. Uploaded data is untrusted research input.
Every computed result carries a named provenance; missing evidence is not zero.
"""
from __future__ import annotations

import hashlib
import io
import json
import math
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit

import numpy as np
import pandas as pd

VERSION = 'V191-H184-12-RESEARCH-ROLES'
TZ = timezone(timedelta(hours=8))
ROOT = Path(__file__).resolve().parent
ROLES = {
    '01': ('交易點子產生器', '情境觸發、條件式價格區間、失效條件與風險報酬', 'ohlcv'),
    '02': ('自動化技術分析師', '日週線、均線、動能、支撐壓力與三種情境', 'ohlcv'),
    '03': ('新聞轉交易研究', '日期與來源核查、事件假說、可能推翻條件', 'events'),
    '04': ('交易策略回測分析師', '無前視均線規則回測、成本、最大回撤與樣本數', 'ohlcv'),
    '05': ('投資組合風險管理', '集中度、現金與市場下跌20%情境', 'portfolio'),
    '06': ('交易日誌分析師', '近期20筆交易、執行偏差與三條檢查規則', 'trades'),
    '07': ('財報與基本面研究員', '營收、季度利潤率、現金流、負債與股本證據', 'financial'),
    '08': ('投資論點反方審查', '事實與推論拆分、反證條件與追蹤清單', 'thesis'),
    '09': ('同業估值比較', '同業商模與可比性，價格與財務缺口核查', 'financial'),
    '10': ('財報情境與敏感度分析', '保守／基準／樂觀 DCF 假設及敏感度', 'assumptions'),
    '11': ('事件追蹤與研究日曆', '官方來源、待查事件與每週檢查清單', 'events'),
    '12': ('研究資料查核員', '逐筆來源、日期、單位與結論證據分級', 'evidence'),
}
SHEETS = {
    'ohlcv': ('date','open','high','low','close','volume','code','source'),
    'benchmark': ('date','close','source'),
    'financial': ('code','fiscal_period','disclosed_at','revenue','gross_margin_pct','operating_margin_pct','net_income','operating_cashflow','debt','shares','source_url','unit'),
    'events': ('code','event_date','event_type','title','source_url','fact','hypothesis','confirmed'),
    'portfolio': ('code','weight_pct','sector','currency','region'),
    'trades': ('code','entry_date','exit_date','entry_price','exit_price','quantity','cost','plan','reason','rule_followed'),
    'evidence': ('code','metric','value','unit','period','asof_date','source_url','quote','claim'),
    'quotes': ('code','date','close','source'),
}
ALIASES = {
    '股票代號':'code','證券代號':'code','代號':'code','日期':'date','交易日期':'date',
    '開盤':'open','開盤價':'open','最高':'high','最高價':'high','最低':'low','最低價':'low',
    '收盤':'close','收盤價':'close','成交量':'volume','交易量':'volume','資料來源':'source',
    '財報期間':'fiscal_period','公告日期':'disclosed_at','營收':'revenue','毛利率':'gross_margin_pct',
    '營業利益率':'operating_margin_pct','營益率':'operating_margin_pct','淨利':'net_income',
    '營業現金流':'operating_cashflow','負債':'debt','股數':'shares','來源網址':'source_url',
    '新聞日期':'event_date','事件日期':'event_date','事件類型':'event_type','標題':'title',
    '事實':'fact','假設':'hypothesis','已核實':'confirmed',
    '配置比例':'weight_pct','產業':'sector','幣別':'currency','區域':'region',
    '進場日期':'entry_date','出場日期':'exit_date','買進價格':'entry_price',
    '賣出價格':'exit_price','數量':'quantity','交易成本':'cost','當時理由':'reason',
    '遵守規則':'rule_followed','資料日期':'asof_date','數值':'value','單位':'unit',
    '指標':'metric','研究結論':'claim','引用內容':'quote','股票':'code',
}

@dataclass
class Outcome:
    role: str
    status: str = '待補證'
    conclusion: str = ''
    metrics: dict[str, Any] = field(default_factory=dict)
    tables: dict[str, pd.DataFrame] = field(default_factory=dict)
    gaps: list[str] = field(default_factory=list)
    provenance: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    def tabular(self):
        return {'role':self.role,'status':self.status,'conclusion':self.conclusion,
                'metrics':self.metrics,'gaps':self.gaps,'provenance':self.provenance,
                'warnings':self.warnings,'tables':{k:frame_to_records(v) for k,v in self.tables.items()}}

def frame_to_records(df: pd.DataFrame) -> list[dict[str,Any]]:
    return json.loads(df.to_json(orient='records',date_format='iso',force_ascii=False)) if isinstance(df,pd.DataFrame) else []

def normalize_frame(data: Any) -> pd.DataFrame:
    if not isinstance(data,pd.DataFrame): return pd.DataFrame()
    f=data.copy()
    f.columns=[ALIASES.get(str(c).strip(),str(c).strip().lower()) for c in f.columns]
    f=f.loc[:,~f.columns.duplicated(keep='first')]
    if 'code' in f:
        f['code']=f['code'].astype(str).str.strip().str.replace(r'\.0$','',regex=True)
    return f

def _num(series: Any) -> pd.Series:
    if not isinstance(series,pd.Series):series=pd.Series(series)
    return pd.to_numeric(series.astype(str).str.replace(',','',regex=False).str.replace('%','',regex=False),errors='coerce')

def _f(v:Any)->float|None:
    try:
        n=float(v)
        return n if math.isfinite(n) else None
    except (ValueError,TypeError):return None

def _date_series(values: Any) -> pd.Series:
    s=values.astype(str).str.strip()
    return pd.to_datetime(s,errors='coerce',format='mixed').dt.tz_localize(None)

def _is_url(url:Any)->bool:
    try:
        x=urlsplit(str(url or '').strip())
        return x.scheme=='https' and bool(x.netloc) and not x.username and not x.password
    except (ValueError,TypeError):return False

def _asof(ts:Any)->str:
    x=pd.to_datetime(ts,errors='coerce')
    return str(x.date()) if not pd.isna(x) else ''

def _price_frame(raw: Any, code: str = '') -> tuple[pd.DataFrame,list[str]]:
    f=normalize_frame(raw); issues=[]
    needed={'date','open','high','low','close','volume'}
    if not needed.issubset(f.columns):return pd.DataFrame(),['OHLCV 欄位不足：'+','.join(sorted(needed-set(f.columns)))]
    if code and 'code' in f.columns: f=f[f['code']==code]
    if f.empty:return pd.DataFrame(),['沒有符合股票代號的行情']
    f['date']=_date_series(f['date'])
    for k in ('open','high','low','close','volume'):f[k]=_num(f[k])
    bad=f[['open','high','low','close','volume','date']].isna().any(axis=1)
    bad|=(f['open']<=0)|(f['high']<=0)|(f['low']<=0)|(f['close']<=0)|(f['volume']<0)
    bad|=(f['high']<f[['open','close','low']].max(axis=1))|(f['low']>f[['open','close','high']].min(axis=1))
    if bad.any():issues.append(f'{int(bad.sum())} 筆 OHLCV 欄位無效已排除')
    f=f[~bad].sort_values('date').drop_duplicates('date',keep='last').reset_index(drop=True)
    if 'source' not in f or not f['source'].astype(str).str.strip().ne('').all():issues.append('缺少部分行情來源名稱（僅可視為使用者資料）')
    if len(f) and f['date'].max().date()>datetime.now(TZ).date():
        issues.append('資料包含未來日期；已排除')
        f=f[f['date'].dt.date<=datetime.now(TZ).date()].reset_index(drop=True)
    return f,issues

def _technicals(f:pd.DataFrame)->pd.DataFrame:
    a=f.copy()
    if a.empty:return a
    for n in (5,20,60): a[f'MA{n}']=a['close'].rolling(n,min_periods=n).mean()
    change=a['close'].diff(); gain=change.clip(lower=0);loss=-change.clip(upper=0)
    up=gain.ewm(alpha=1/14,adjust=False,min_periods=14).mean()
    down=loss.ewm(alpha=1/14,adjust=False,min_periods=14).mean()
    rs=up/down.replace(0,np.nan)
    a['RSI14']=100-(100/(1+rs))
    a.loc[(down==0)&(up>0),'RSI14']=100
    a.loc[(down==0)&(up==0),'RSI14']=50
    tr=pd.concat([(a['high']-a['low']),(a['high']-a['close'].shift()).abs(),
                  (a['low']-a['close'].shift()).abs()],axis=1).max(axis=1)
    a['ATR14']=tr.rolling(14,min_periods=14).mean()
    a['RES20']=a['high'].shift(1).rolling(20,min_periods=20).max()
    a['SUP20']=a['low'].shift(1).rolling(20,min_periods=20).min()
    a['VOL20']=a['volume'].rolling(20,min_periods=20).mean()
    return a

def _code(f:pd.DataFrame, options:Mapping)->str:
    v=str(options.get('code') or '').strip().removesuffix('.0')
    if v:return v
    if 'code' in f and f['code'].nunique()==1:return str(f['code'].iloc[0])
    return ''

def _technical_context(data:Mapping,options:Mapping)->tuple[pd.DataFrame,Outcome]:
    o=Outcome('02');raw=normalize_frame(data.get('ohlcv'));code=_code(raw,options)
    if not code and 'code' in raw and raw['code'].nunique()>1:
        o.gaps.append('請指定股票代號，多檔行情不可混算')
        return pd.DataFrame(),o
    price,err=_price_frame(raw,code);o.gaps+=err
    if len(price)<60:
        o.gaps.append(f'需至少60根有效OHLCV，目前{len(price)}根；不產生已驗證均線／回測')
        return price,o
    if 'code' not in price and not code:o.gaps.append('缺少股票代號')
    return _technicals(price),o

def role01(data:Mapping, options:Mapping)->Outcome:
    f,o=_technical_context(data,options);o.role='01'
    if len(f)<60:return o
    r=f.iloc[-1]; c=float(r['close']);atr=float(r['ATR14']);hi=float(r['RES20']);lo=float(r['SUP20']);ma=float(r['MA20'])
    if not all(math.isfinite(x) and x>0 for x in (c,atr,hi,lo,ma)):
        o.gaps.append('ATR／支撐壓力計算不足');return o
    scenarios=[
        ('突破20日箱頂',f'收盤價突破先前20日最高 {hi:.2f}，須檢查成交量',hi,hi-1.5*atr,hi+3*atr,'突破後跌回箱內'),
        ('回測20日均線',f'回測MA20 {ma:.2f} 後重新收復',ma,ma-1.5*atr,ma+2.5*atr,'跌破回測低點'),
        ('接近20日支撐',f'跌至20日區間低點附近 {lo:.2f} 且出現止跌證據',lo,lo-1.5*atr,lo+2*atr,'支撐破位'),
        ('強勢延續',f'連續收盤站穩前高 {hi:.2f}，等待次日確認',max(c,hi),max(c,hi)-2*atr,max(c,hi)+4*atr,'量價結構轉弱'),
        ('跌破後觀察',f'跌破 {lo:.2f} 時不追空，等待重新站回',lo,lo-2*atr,lo+2*atr,'重新確認前無交易條件'),
    ]
    rows=[]
    for name,trigger,entry,stop,target,invalid in scenarios:
        risk=entry-stop; rr=(target-entry)/risk if risk>0 else None
        rows.append({'情境':name,'觀察觸發條件':trigger,'條件式參考進場':round(entry,2),
                     '條件式參考目標':round(target,2),'失效參考價':round(stop,2),
                     '假設報酬風險比':round(rr,2) if rr is not None else None,
                     '失效條件':invalid,'證據等級':'研究假設｜非買進授權'})
    o.tables['五種研究情境']=pd.DataFrame(rows)
    o.metrics={'股票代號':_code(normalize_frame(data.get('ohlcv')),options),'行情截止':_asof(r['date']),
               '最後收盤':round(c,2),'ATR14':round(atr,3)}
    o.provenance=['使用者OHLCV上傳檔／技術公式（原始來源仍需比對）']
    o.conclusion='五種條件式研究情境；價格為公式參考，並非實際下單建議或預測'
    if 'financial' not in data or normalize_frame(data.get('financial')).empty:
        o.gaps.append('沒有同步可驗證基本面資料，五種情境的基本面支持理由待補')
    o.status='研究假設已建立';return o

def role02(data:Mapping,options:Mapping)->Outcome:
    f,o=_technical_context(data,options)
    if len(f)<60:return o
    r=f.iloc[-1];weeks=f.set_index('date').resample('W-FRI').agg({'open':'first','high':'max','low':'min','close':'last','volume':'sum'}).dropna()
    wma=weeks['close'].rolling(10,min_periods=10).mean()
    rows=[{'情境':'偏多','成立條件':'收盤>MA20>MA60 且突破先前20日高點','失效條件':'跌破MA20或前低'},
          {'情境':'盤整','成立條件':'收盤在20日高低之間，均線糾結','失效條件':'有效突破區間上下緣'},
          {'情境':'偏空','成立條件':'收盤<MA20<MA60 且低點下移','失效條件':'收復MA20及箱頂'}]
    o.tables['三種情境規則']=pd.DataFrame(rows)
    o.metrics={k:round(float(r[k]),3) if pd.notna(r[k]) else None
                for k in ['close','MA5','MA20','MA60','RSI14','ATR14','RES20','SUP20','VOL20']}
    o.metrics['週線10週均']=round(float(wma.iloc[-1]),3) if len(weeks)>=10 else None
    o.metrics['行情截止']=_asof(r['date']);o.metrics['有效交易日']=len(f)
    o.gaps+=['未提供完整成交量來源稽核，量價突破僅作初步研究'] if 'source' not in f else []
    o.status='技術指標已計算';o.conclusion='三種情境為條件檢驗，不代表未來走向';o.provenance=['OHLCV（使用者提供）'];return o

def role03(data:Mapping,options:Mapping)->Outcome:
    o=Outcome('03');f=normalize_frame(data.get('events'))
    if f.empty:o.gaps.append('需要事件清單：events');return o
    required={'event_date','title','source_url','fact'}
    if not required.issubset(f):o.gaps.append('缺少欄位：'+','.join(sorted(required-set(f.columns))));return o
    f=f.copy();f['事件日期']=_date_series(f['event_date']).dt.strftime('%Y-%m-%d')
    f['來源是否HTTPS']=f['source_url'].map(_is_url)
    f['證據狀態']=np.where(f['來源是否HTTPS']&f['事件日期'].notna()&f['fact'].astype(str).str.strip().ne(''),
                               '已列來源／待查原文','待核實')
    if 'confirmed' in f:f['證據狀態']=np.where(f['confirmed'].astype(str).str.lower().isin(['1','true','yes','已核實'])&f['來源是否HTTPS'],'申報已核實／待獨立複查',f['證據狀態'])
    f['可能反證']='事件是否延期、取消、財務數字是否低於公告／預期，需取得原文核對'
    f['短期影響假說']='可能造成短期波動；方向與幅度皆待行情驗證'
    f['長期影響假說']='需要後續營收、利潤率與訂單資料，才能判斷長期影響'
    cols=[x for x in ['code','事件日期','title','fact','hypothesis','source_url','證據狀態','短期影響假說','長期影響假說','可能反證'] if x in f]
    o.tables['新聞事件查核']=f[cols];o.metrics={'事件數':len(f),'具來源網址':int(f['來源是否HTTPS'].sum())}
    o.gaps.append('僅整理使用者提供的事件；未自動開啟網址查證內容、未取得即時新聞')
    o.status='待來源複查';o.conclusion='事件日期與事實分列，假設不得當成已發生結果';return o

def role04(data:Mapping,options:Mapping)->Outcome:
    f,o=_technical_context(data,options);o.role='04'
    if len(f)<80:o.gaps.append('回測另需至少80筆行情');return o
    fee=float(options.get('fee_bps',14.25));tax=float(options.get('tax_bps',30));slip=float(options.get('slippage_bps',5))
    if min(fee,tax,slip)<0 or max(fee,tax,slip)>200:o.gaps.append('成本參數不合理（須介於0～200 bps）');return o
    x=f.copy();x['signal']=(x['MA5']>x['MA20'])&(x['MA5'].shift(1)<=x['MA20'].shift(1))
    x['exit']=(x['MA5']<x['MA20'])&(x['MA5'].shift(1)>=x['MA20'].shift(1))
    trades=[];entry=None;entry_day=None
    # Decisions are made after close; first possible fill is NEXT day's open.
    for i in range(1,len(x)):
        prev=x.iloc[i-1];day=x.iloc[i]
        if entry is None and bool(prev['signal']):
            entry=float(day['open'])*(1+(fee+slip)/10000)
            entry_day=day['date'];continue
        if entry is not None and bool(prev['exit']):
            exit_price=float(day['open'])*(1-(fee+slip+tax)/10000)
            trades.append({'進場日':_asof(entry_day),'出場日':_asof(day['date']),
                           '進場含成本':round(entry,4),'出場扣成本':round(exit_price,4),
                           '淨報酬%':round((exit_price/entry-1)*100,3)})
            entry=None;entry_day=None
    if entry is not None:
        o.gaps.append('最後一筆倉位仍未平倉，未納入已實現勝率')
    t=pd.DataFrame(trades,columns=['進場日','出場日','進場含成本','出場扣成本','淨報酬%']);o.tables['交易明細']=t
    rets=t['淨報酬%']/100 if len(t) else pd.Series(dtype=float)
    eq=(1+rets).cumprod();dd=eq/eq.cummax()-1 if len(eq) else pd.Series(dtype=float)
    gross_win=float(rets[rets>0].sum());gross_loss=float(-rets[rets<0].sum())
    cutoff=f.iloc[int(len(f)*.7)]['date']
    train=t[pd.to_datetime(t['出場日'])<=cutoff] if len(t) else t
    test=t[pd.to_datetime(t['出場日'])>cutoff] if len(t) else t
    o.tables['時間分割樣本內外對照']=pd.DataFrame([
        {'資料分割':'前70%行情區間','樣本筆數':len(train),'平均淨報酬%':round(train['淨報酬%'].mean(),3) if len(train) else None},
        {'資料分割':'後30%行情區間','樣本筆數':len(test),'平均淨報酬%':round(test['淨報酬%'].mean(),3) if len(test) else None}])
    o.metrics={'成交筆數':len(t),'勝率%':round(100*(rets>0).mean(),2) if len(t) else None,
               '平均淨報酬%':round(100*rets.mean(),3) if len(t) else None,
               '獲利因子':round(gross_win/gross_loss,3) if gross_loss>0 else None,
               '逐筆複利最大回撤%':round(dd.min()*100,3) if len(dd) else None,
               '交易成本_bps買':fee+slip,'交易成本_bps賣':fee+tax+slip,
               '回測起日':_asof(f.iloc[0]['date']),'回測迄日':_asof(f.iloc[-1]['date'])}
    if len(t)<30:o.gaps.append('已平倉樣本少於30筆：不可視為穩健績效，需樣本外與跨市場驗證')
    o.gaps.append('簡化單股逐筆策略，不含股利、公司行動及組合資金占用；不能用於宣稱實盤績效')
    o.status='離線策略模擬';o.conclusion='MA5/20 黃金交叉訊號，隔日開盤執行，含買賣成本；非交易建議'
    o.provenance=['使用者OHLCV、成本假設（均有標示）'];return o

def role05(data:Mapping,options:Mapping)->Outcome:
    o=Outcome('05');f=normalize_frame(data.get('portfolio'))
    if not {'code','weight_pct'}.issubset(f):o.gaps.append('需要 portfolio 股票代號／配置比例');return o
    a=f.copy();a['weight_pct']=_num(a['weight_pct'])
    if a['weight_pct'].isna().any() or (a['weight_pct']<0).any():o.gaps.append('配置比例不得為負或缺值');return o
    total=float(a['weight_pct'].sum());cash=_f(options.get('cash_pct'))
    if cash is None:cash=max(0.,100-total);o.gaps.append('現金比例未提供，先以100%減持股權重推算（待確認）')
    if abs(total+cash-100)>0.05:o.gaps.append(f'持股{total:.2f}% + 現金{cash:.2f}% 不等於100%，請檢查')
    result=a.copy();result['市場跌20%的組合損益貢獻%']=-(result['weight_pct']*0.2).round(4)
    o.tables['持股集中度']=result
    largest_idx=a['weight_pct'].idxmax() if len(a) else None
    reduced=a.copy()
    if largest_idx is not None:
        excess=max(0.,float(reduced.at[largest_idx,'weight_pct'])-20.)
        reduced.at[largest_idx,'weight_pct']-=excess
        o.tables['集中度情境比較']=pd.DataFrame([
            {'情境':'原始配置','最大單股%':round(a['weight_pct'].max(),2),'現金%':round(cash,2),'全持股跌20%組合損益%':round(-total*.2,2)},
            {'情境':'假設最大單股降至20%，差額轉現金','最大單股%':round(reduced['weight_pct'].max(),2),'現金%':round(cash+excess,2),'全持股跌20%組合損益%':round(-(total-excess)*.2,2)}])
    for col,table in [('sector','產業集中度'),('currency','幣別集中度'),('region','區域集中度')]:
        if col in a:o.tables[table]=a.groupby(col,dropna=False)['weight_pct'].sum().reset_index().sort_values('weight_pct',ascending=False)
        else:o.gaps.append('缺少'+col+'無法進行此類集中度比較')
    o.metrics={'持股配置%':round(total,2),'現金配置%':round(cash,2),
               '最大單檔配置%':round(a['weight_pct'].max(),2),
               '市場全持股跌20%假設組合損益%':round(-total*.2,2),
               'HHI集中度':round(float(((a['weight_pct']/100)**2).sum()),4)}
    o.status='壓力測試假設';o.conclusion='下跌20%是同幅度價格衝擊假設，不是預測；未包含匯率、跳空、流動性損失';return o

def role06(data:Mapping,options:Mapping)->Outcome:
    o=Outcome('06');f=normalize_frame(data.get('trades'))
    keys={'code','entry_price','exit_price','quantity'}
    if not keys.issubset(f):o.gaps.append('需要 trades 欄位：'+','.join(sorted(keys-set(f.columns))));return o
    a=f.tail(20).copy()
    for key in ('entry_price','exit_price','quantity'):a[key]=_num(a[key])
    a['cost']=_num(a['cost']) if 'cost' in a else 0.
    a['淨損益']=(a['exit_price']-a['entry_price'])*a['quantity']-a['cost']
    a['報酬率%']=((a['exit_price']-a['entry_price'])/a['entry_price']*100).round(3)
    a.loc[(a['entry_price']<=0)|(a['quantity']<=0),'報酬率%']=np.nan
    o.tables['最近20筆']=a
    valid=a['報酬率%'].dropna()
    o.metrics={'檢查筆數':len(a),'可算報酬筆數':len(valid),'勝率%':round(100*(valid>0).mean(),2) if len(valid) else None,
               '平均單筆報酬%':round(valid.mean(),3) if len(valid) else None}
    if 'rule_followed' in a:o.metrics['明確標記違規筆數']=int(a['rule_followed'].astype(str).str.lower().isin(['false','否','0','no']).sum())
    else:o.gaps.append('缺少 rule_followed，不能斷言哪些交易違規')
    o.tables['三條可檢查規則']=pd.DataFrame([
        {'規則':'進場前必填觸發條件、失效點和最大部位','驗證方法':'核對交易前日誌欄位，不可事後回填'},
        {'規則':'停損或失效條件成立就按事前計畫檢查執行','驗證方法':'每日逐筆比對計畫與實際出場'},
        {'規則':'每週分析執行差異與成本，不依單週勝率修改策略','驗證方法':'週報列違規次數、成本與樣本數'}])
    if len(valid)<20:o.gaps.append('有效交易樣本不足20筆，推論可信度低')
    o.status='交易日誌初檢';o.conclusion='報酬計算僅針對提供的交易資料，不代表成交憑證已核實';return o

def _official_jsonl(path:Path,code:str,asof:str,limit:int=20000)->pd.DataFrame:
    if not path.is_file() or path.stat().st_size>15_000_000:return pd.DataFrame()
    rows=[]
    with path.open('r',encoding='utf-8-sig') as h:
        for i,line in enumerate(h):
            if i>=limit:break
            try:r=json.loads(line)
            except json.JSONDecodeError:continue
            if not isinstance(r,dict) or str(r.get('code'))!=code:continue
            # FIRST_OBSERVED: never expose evidence to an earlier research day.
            available=str(r.get('available_at') or '')[:10]
            if not available or available>asof:continue
            rows.append(r)
    return pd.DataFrame(rows)

def role07(data:Mapping,options:Mapping)->Outcome:
    o=Outcome('07');raw=normalize_frame(data.get('financial'))
    code=str(options.get('code') or '').strip();asof=str(options.get('asof') or datetime.now(TZ).date().isoformat())[:10]
    if code and 'code' in raw:raw=raw[raw['code']==code]
    if not raw.empty:
        if 'disclosed_at' in raw:
            raw=raw.copy();raw['disclosed_at']=_date_series(raw['disclosed_at'])
            raw=raw[raw['disclosed_at']<=pd.Timestamp(asof)]
        else:o.gaps.append('未提供財報公告日，無法判斷當時是否已公開')
        if 'fiscal_period' not in raw:o.gaps.append('沒有財報期間，不可跨期比較')
        if 'unit' not in raw:o.gaps.append('財報金額單位未指定，不可比較金額')
        o.tables['提供之財報']=raw.tail(40)
    elif code:
        revenue=_official_jsonl(ROOT/'data/h115/revenue.jsonl',code,asof)
        margins=_official_jsonl(ROOT/'data/h115/quarterly_margins.jsonl',code,asof)
        if not revenue.empty:o.tables['官方月營收快照']=revenue.sort_values('year_month').tail(24)
        if not margins.empty:o.tables['官方單季利潤率']=margins.sort_values('year_quarter').tail(12)
        if not revenue.empty or not margins.empty:
            o.provenance=['本機 H115 FIRST_OBSERVED 官方快照；不能視作跨期完整財報']
        else:o.gaps.append('未取得股票代號對應的有效官方快照，請上傳財報')
    else:o.gaps.append('請提供股票代號或財報檔')
    if '提供之財報' in o.tables:
        x=o.tables['提供之財報']; metrics={}
        for k in ('revenue','gross_margin_pct','operating_margin_pct','net_income','operating_cashflow','debt','shares'):
            if k in x:
                s=_num(x[k]).dropna();metrics[k]=float(s.iloc[-1]) if len(s) else None
            else:o.gaps.append(k+'未提供')
        o.metrics=metrics
    o.gaps.append('若只取得月營收／季度利潤率快照，仍缺完整現金流、負債、股本與一次性項目查核')
    o.status='財務資料盤點';o.conclusion='僅使用公告時點不晚於研究日的資料；缺失項目不補零、不計 TTM EPS';return o

def role08(data:Mapping,options:Mapping)->Outcome:
    o=Outcome('08');thesis=str(options.get('thesis') or '').strip()
    if len(thesis)<15:o.gaps.append('請輸入至少15字的投資論點與原始依據');return o
    hypotheses=[
        ('財報可持續性','目前論點是否把一次性獲利當常態？','下一季毛利率／現金流與公告數字'),
        ('估值假設','本益比低是否源自景氣高峰EPS或會計口徑？','同業相同期間經調整估值'),
        ('資金與技術','短期放量是否可能只是事件性換手？','後續成交量、相對大盤強弱與失效價'),
        ('競爭與需求','成長是否依賴單一客戶／產品？','訂單轉營收、庫存、產能利用率'),
        ('證據時點','論點是否使用公告後才知道的資料？','首次可取得時間與決策時間比對')]
    o.tables['反方研究檢核']=pd.DataFrame([{'可能忽略因素':a,'反對假設（待驗證）':b,'新證據需求':c,'狀態':'待核實'} for a,b,c in hypotheses])
    o.metrics={'論點字數':len(thesis)};o.gaps.append('反方條件為檢核問題，不是已證明反對論點成立')
    o.status='待反證';o.conclusion='輸入論點已受理；反方假說均需財報／行情來源驗證';return o

def role09(data:Mapping,options:Mapping)->Outcome:
    o=Outcome('09');f=normalize_frame(data.get('financial'));quotes=normalize_frame(data.get('quotes'))
    if f.empty or 'code' not in f:o.gaps.append('上傳含至少兩家公司代號、期間的同業財報');return o
    if 'fiscal_period' not in f:o.gaps.append('缺財報期間，不能比較');return o
    f=f.copy()
    if 'disclosed_at' in f:
        f['disclosed_at']=_date_series(f['disclosed_at'])
        f=f[f['disclosed_at']<=pd.Timestamp(str(options.get('asof') or datetime.now(TZ).date().isoformat())[:10])]
    else:o.gaps.append('無財報公告日期，無法認證當時可得')
    if f.empty:o.gaps.append('研究日以前沒有可用財報');return o
    period=str(options.get('period') or f['fiscal_period'].astype(str).max())
    f=f[f['fiscal_period'].astype(str)==period].copy()
    if f['code'].nunique()<2:o.gaps.append('所選相同財報期間少於兩家公司');return o
    out=[]
    for _,r in f.drop_duplicates('code').iterrows():
        item={'股票代號':r['code'],'財報期間':period}
        for key in ('revenue','gross_margin_pct','operating_margin_pct','net_income','debt','shares','eps_ttm'):
            item[key]=_f(r.get(key))
        item['PE']=None
        # Price+EPS must both be independently supplied, valid and time-consistent.
        if not quotes.empty and 'code' in quotes and 'close' in quotes:
            q=quotes[quotes['code']==r['code']]
            if 'date' in q:
                q=q.copy();q['date']=_date_series(q['date']);q=q[q['date']<=pd.Timestamp(str(options.get('asof') or datetime.now(TZ).date().isoformat())[:10])].sort_values('date')
            if len(q) and item['eps_ttm'] and item['eps_ttm']>0 and str(r.get('eps_ttm_verified','')).lower() in ('true','1','yes'):
                price=_f(q.iloc[-1]['close']);item['PE']=round(price/item['eps_ttm'],2) if price and price>0 else None
        out.append(item)
    o.tables['同期間估值與營運比較']=pd.DataFrame(out)
    o.metrics={'比較家數':len(out),'共同財報期間':period,'具上傳驗證標記之PE筆數':sum(r['PE'] is not None for r in out)}
    o.gaps.append('本益比需獨立驗證TTM EPS與對應日期股價；缺資料不反推EPS')
    o.gaps.append('上傳檔的eps_ttm_verified只是提供者標記，尚未由本頁獨立核對官方財報原文')
    o.gaps.append('不同商業模式、會計準則、產業週期之間不可僅依低PE斷定便宜')
    o.status='同業比較待補證';o.conclusion='僅比較提供的同口徑數字，缺值保留空白';return o

def _dcf_calc(p:Mapping)->float|None:
    rev=_f(p.get('revenue'));growth=_f(p.get('growth'));margin=_f(p.get('margin'))
    tax=_f(p.get('tax'));capex=_f(p.get('capex'));wc=_f(p.get('working_capital'))
    wacc=_f(p.get('wacc'));g=_f(p.get('terminal_growth'))
    shares=_f(p.get('shares'));debt=_f(p.get('debt'));cash=_f(p.get('cash'))
    if any(v is None for v in (rev,growth,margin,tax,capex,wc,wacc,g,shares,debt,cash)):return None
    if rev<=0 or shares<=0 or wacc<=g or wacc<=0 or g<-.1 or tax<0 or tax>1:return None
    if abs(growth)>.8 or not 0<=margin<=1 or capex<0 or wc<0:return None
    fcf=[];revenue=rev
    for year in range(1,4):
        revenue*=(1+growth)
        operating_profit=revenue*margin
        free_cash_flow=operating_profit*(1-tax)-revenue*(capex+wc)
        fcf.append(free_cash_flow)
    terminal=fcf[-1]*(1+g)/(wacc-g)
    ev=sum(v/(1+wacc)**(i+1) for i,v in enumerate(fcf))+terminal/(1+wacc)**3
    return (ev-debt+cash)/shares

def role10(data:Mapping,options:Mapping)->Outcome:
    o=Outcome('10')
    if options.get('acknowledge_assumptions') is not True:
        o.gaps.append('須先確認所有DCF數值是使用者自行設定的研究假設；不接受預設範例冒充真實數字')
        return o
    keys=('revenue','growth','margin','tax','capex','working_capital','wacc','terminal_growth','shares','debt','cash')
    if any(_f(options.get(k)) is None for k in keys):o.gaps.append('DCF 缺少：'+','.join(k for k in keys if _f(options.get(k)) is None));return o
    scenarios=[]
    for name,growth_adj,margin_adj in [('保守',-.05,-.03),('基準',0,0),('樂觀',.05,.03)]:
        p={k:float(options[k]) for k in keys}
        p['growth']=max(-.79,min(.79,p['growth']+growth_adj));p['margin']=max(0,min(1,p['margin']+margin_adj))
        valuation=_dcf_calc(p)
        scenarios.append({'情境':name,'營收成長假設%':round(100*p['growth'],2),
                          '營業利益率假設%':round(100*p['margin'],2),'每股試算價值':round(valuation,3) if valuation is not None else None,
                          '性質':'使用者數值假設，非未來股價預測'})
    o.tables['三情境']=pd.DataFrame(scenarios)
    checks=[]
    for dw in (-.01,0,.01):
        for dg in (-.005,0,.005):
            p={k:float(options[k]) for k in keys};p['wacc']+=dw;p['terminal_growth']+=dg
            checks.append({'折現率%':round(100*p['wacc'],2),'終值成長率%':round(100*p['terminal_growth'],2),
                           '每股試算價值':_dcf_calc(p)})
    o.tables['折現率敏感度']=pd.DataFrame(checks)
    if any(r['每股試算價值'] is None for r in scenarios):o.gaps.append('財務假設不符合計算條件：需WACC>終值成長率、股數>0及合理比例')
    o.gaps.append('三情境變化幅度±5pp成長及±3pp利潤率為預設測試假設，需自行校正；未分派機率')
    o.gaps.append('FCF=營業利益×(1-稅率)-營收×(資本支出率+營運資金增額率)；缺折舊調整，屬簡化DCF')
    o.status='情境試算（非預測）' if all(r['每股試算價值'] is not None for r in scenarios) else '待修正假設'
    o.conclusion='可重現三年簡化DCF敏感度分析，不可視為實際成交價';return o

def role11(data:Mapping,options:Mapping)->Outcome:
    o=Outcome('11');f=normalize_frame(data.get('events'))
    if f.empty or 'event_date' not in f:o.gaps.append('需要 events 事件日期清單');return o
    f=f.copy();f['event_date']=_date_series(f['event_date'])
    f['來源狀態']=f['source_url'].map(lambda v:'有網址待查原文' if _is_url(v) else '待確認') if 'source_url' in f else '待確認'
    f['追蹤節奏']='事件前一週／前一日／事件後一日'
    f['待驗證假設']='公告是否如期、是否造成財務數字或指引改變'
    o.tables['追蹤日曆']=f.sort_values('event_date')
    o.metrics={'事件數':len(f),'可確認日期筆數':int(f['event_date'].notna().sum())}
    if f['event_date'].isna().any():o.gaps.append('部分事件未有已確認日期，不能直接建立提醒')
    o.gaps.append('事件日期由上傳者提供，未獨立至官網查證；不預測漲跌')
    o.status='研究日曆草稿';o.conclusion='每週核對事件日期及來源，事實與猜測分開保存';return o

def role12(data:Mapping,options:Mapping)->Outcome:
    o=Outcome('12');f=normalize_frame(data.get('evidence'))
    if f.empty:o.gaps.append('需要 evidence 表：指標、數值、單位、期間、日期、來源');return o
    needs=('metric','value','unit','period','asof_date','source_url')
    for k in needs:
        if k not in f:f[k]=None
    a=f.copy();a['問題']='';a['判定']='待核實'
    reasons=[];labels=[]
    for _,r in a.iterrows():
        problems=[]
        if not str(r['metric'] or '').strip():problems.append('缺指標')
        if _f(r['value']) is None:problems.append('缺可核對數值')
        if not str(r['unit'] or '').strip():problems.append('缺單位')
        if not str(r['period'] or '').strip():problems.append('缺財報／觀察期間')
        if not _asof(r['asof_date']):problems.append('缺有效日期')
        if not _is_url(r['source_url']):problems.append('缺HTTPS來源連結')
        if problems:label='待核實'
        else:label='待核實'  # Metadata complete is not independent verification.
        labels.append(label);reasons.append('；'.join(problems) if problems else '欄位齊備，但尚未核對來源原文，不得標成已核實')
    a['判定']=labels;a['問題']=reasons
    # Only flag a contradiction where exactly same identifier, metric, date and period
    # has demonstrably different numeric values. This is evidence of a conflict, not proof of which is wrong.
    keys=[k for k in ('code','metric','period','asof_date','unit') if k in a]
    if len(keys)>=4:
        nums=_num(a['value'])
        group=a.assign(_numeric=nums).groupby(keys,dropna=False)['_numeric'].transform('nunique')
        dup=group>1
        a.loc[dup,'判定']='有誤／相互矛盾';a.loc[dup,'問題']='相同指標／期間／日期有互斥數字，需核對原文'
    o.tables['逐項來源查核']=a
    o.metrics={'查核列數':len(a),'待核實':int((a['判定']=='待核實').sum()),
               '相互矛盾':int((a['判定']=='有誤／相互矛盾').sum()),'已核實':0}
    o.gaps.append('目前未獨立開啟或比對官方原文；欄位完整不等於已核實')
    o.status='待來源複核';o.conclusion='原始資料只足以檢查格式與內部矛盾，查不到不等於證明為錯';return o

RUNNERS={f'{i:02d}':globals()[f'role{i:02d}'] for i in range(1,13)}
def run_role(role: str,data:Mapping[str,pd.DataFrame],options:Mapping[str,Any]|None=None)->Outcome:
    if role not in RUNNERS:raise ValueError('unknown research role')
    try:
        return RUNNERS[role](data,options or {})
    except (ValueError,KeyError,TypeError,OverflowError,IndexError) as exc:
        return Outcome(role,status='待補證',gaps=[f'資料結構／輸入不符合要求：{type(exc).__name__}: {str(exc)[:160]}'])

def export_event_ics(outcome:Outcome)->bytes:
    """Export strictly tentative, supplied-dates-only calendar (no alerts or claims)."""
    if not isinstance(outcome,Outcome) or outcome.role!='11':return b''
    table=outcome.tables.get('追蹤日曆')
    if table is None or 'event_date' not in table:return b''
    def esc(v):
        return str(v or '').replace('\\','\\\\').replace(';','\\;').replace(',','\\,').replace('\n','\\n').replace('\r','')
    lines=['BEGIN:VCALENDAR','VERSION:2.0','PRODID:-//SPT//H184 Research Lab//ZH-TW','CALSCALE:GREGORIAN']
    for row in table.to_dict('records'):
        d=pd.to_datetime(row.get('event_date'),errors='coerce')
        if pd.isna(d):continue
        raw='|'.join([str(row.get('code','')),str(row.get('title','')),str(d.date())])
        uid=hashlib.sha256(raw.encode('utf8')).hexdigest()[:20]+'@h184-research'
        summary=esc('待核實｜'+str(row.get('title') or '研究事件'))
        lines+=['BEGIN:VEVENT','UID:'+uid,'DTSTART;VALUE=DATE:'+d.strftime('%Y%m%d'),
                'DTSTAMP:'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ'),
                'SUMMARY:'+summary,'STATUS:TENTATIVE','DESCRIPTION:事件日期由上傳資料提供，來源尚待獨立驗證','END:VEVENT']
    lines+=['END:VCALENDAR']
    return ('\r\n'.join(lines)+'\r\n').encode('utf-8')

def research_manifest(data:Mapping[str,pd.DataFrame],options:Mapping[str,Any],outcomes:Mapping[str,Outcome])->dict:
    datasets={}
    for key,frame in data.items():
        if isinstance(frame,pd.DataFrame):
            raw=frame.to_json(orient='records',force_ascii=False,date_format='iso').encode('utf-8')
            datasets[key]={'rows':len(frame),'sha256':hashlib.sha256(raw).hexdigest()}
    return {'version':VERSION,'created_at':datetime.now(TZ).isoformat(),
            'authority':'RESEARCH_ONLY_H109_H126_UNCHANGED','data':datasets,
            'options':{k:v for k,v in options.items() if isinstance(v,(str,int,float,bool))},
            'results':{k:v.tabular() for k,v in outcomes.items()}}

def _excel_safe(v:Any)->Any:
    # Uploaded text must never become an Excel formula when downloaded.
    if isinstance(v,str) and v.lstrip().startswith(('=','+','-','@')):
        return "'"+v
    return v

def export_workbook(outcomes:Mapping[str,Outcome],manifest:Mapping[str,Any])->bytes:
    from openpyxl import Workbook
    from openpyxl.styles import Font,PatternFill,Alignment
    wb=Workbook();ws=wb.active;ws.title='00_研究導航'
    ws.append(['角色','名稱','狀態','結論','資料缺口','交易權限'])
    for code,(name,_,_) in ROLES.items():
        o=outcomes.get(code,Outcome(code))
        ws.append([code,name,o.status,o.conclusion,'；'.join(o.gaps),'H109／H126 未改變'])
        page=wb.create_sheet(code+'_'+name[:19]);page.append(['V191-H184｜僅研究，不提供交易授權'])
        page.append(['狀態',o.status]);page.append(['結論',o.conclusion])
        for k,v in o.metrics.items():page.append([str(k),_excel_safe(str(v)) if v is not None else '待補證'])
        for gap in o.gaps:page.append(['待補證',_excel_safe(gap)])
        for src in o.provenance:page.append(['來源',_excel_safe(src)])
        for section,table in o.tables.items():
            page.append([]);page.append(['表格',section]);page.append(list(map(str,table.columns)))
            for row in table.itertuples(index=False,name=None):
                page.append([_excel_safe(str(v)[:30000]) if isinstance(v,(dict,list)) else (None if pd.isna(v) else _excel_safe(v)) for v in row])
        page.freeze_panes='A2';page.column_dimensions['A'].width=25;page.column_dimensions['B'].width=60
    audit=wb.create_sheet('99_資料來源與版本');audit.append(['欄位','內容'])
    audit.append(['系統版本',VERSION]);audit.append(['生成時間',_excel_safe(manifest.get('created_at'))]);audit.append(['權限','H109／H126 不變，僅供研究'])
    for k,v in (manifest.get('data') or {}).items():audit.append(['來源資料：'+k, json.dumps(v,ensure_ascii=False)])
    for sheet in wb.worksheets:
        sheet.sheet_view.showGridLines=False
        for cell in sheet[1]:
            cell.fill=PatternFill('solid',fgColor='15334F');cell.font=Font(bold=True,color='FFFFFF')
            cell.alignment=Alignment(wrap_text=True)
    out=io.BytesIO();wb.save(out);return out.getvalue()

__all__=['VERSION','ROLES','SHEETS','Outcome','run_role','research_manifest','export_workbook','normalize_frame','export_event_ics']
