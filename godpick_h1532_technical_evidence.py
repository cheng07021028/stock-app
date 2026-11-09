# -*- coding: utf-8 -*-
"""H153.2: complementary, as-of OHLCV research evidence (not trading signals).

No network, no trading permission, no forward bars; volume must have explicit lots
units from the caller. OBV/ADX do not prove institutional or TDCC ownership.
"""
from __future__ import annotations
import math
from typing import Any
import pandas as pd

VERSION = 'V191-H153.2-COMPLEMENTARY-TECHNICAL'
DEFAULT = {
    'H1532布林收斂': '未驗證', 'H1532布林帶寬%': None,
    'H1532OBV量價背離': '未驗證', 'H1532OBV近10日方向': '未驗證',
    'H1532ADX': None, 'H1532DMI方向': '未驗證',
    'H1532技術來源': '日K資料或成交量單位不足',
    'H1532技術反證': '收斂可能向下突破；OBV不能證明大戶鎖碼；ADX不預測漲跌',
}

def _finite(x: Any) -> bool:
    try: return math.isfinite(float(x))
    except (ValueError, TypeError, OverflowError): return False


def evaluate_prebreakout(frame: Any) -> dict[str, Any]:
    """`frame`: H114 canonical sorted dated `day/open/high/low/close/lots`.

    Caller enforces as-of cut, verifies session and OHLC geometry. If day is
    not unique or volume unknown, fail closed. Bollinger uses previous 60
    observed bandwidths for percentile, and never includes a future candle.
    """
    out = dict(DEFAULT)
    if not isinstance(frame, pd.DataFrame) or len(frame)<85:
        return out
    needed={'day','open','high','low','close','lots'}
    if not needed.issubset(frame.columns):return out
    if frame['day'].isna().any() or frame['day'].duplicated().any() or not frame['day'].is_monotonic_increasing:
        out['H1532技術來源']='日期不唯一／未按時間排序';return out
    d=frame.tail(85).copy()
    for key in ('open','high','low','close','lots'):
        d[key]=pd.to_numeric(d[key],errors='coerce')
        if not d[key].map(_finite).all():return out
    if ((d[['open','high','low','close']]<=0).any().any() or (d['lots']<=0).any() or
        (d['high']<d[['open','close','low']].max(axis=1)).any() or
        (d['low']>d[['open','close']].min(axis=1)).any()):
        out['H1532技術來源']='OHLCV 價格或成交量無效';return out
    c=d['close'];h=d['high'];l=d['low'];v=d['lots']
    mean=c.rolling(20,min_periods=20).mean()
    std=c.rolling(20,min_periods=20).std(ddof=0)
    width=(4*std/mean*100).replace([float('inf'),float('-inf')],float('nan'))
    today=float(width.iloc[-1]); historic=width.iloc[-61:-1].dropna()
    if len(historic)==60 and _finite(today):
        out['H1532布林帶寬%']=round(today,3)
        # Prior-history 20th percentile: low volatility, NOT bullish breakout.
        threshold=float(historic.quantile(.20))
        out['H1532布林收斂']=('是｜波動低於前60期20百分位' if today<=threshold
                               else '否｜尚非低波動收斂')
    delta=c.diff().fillna(0)
    signed=v.where(delta>0,-v.where(delta<0,0))
    obv=signed.cumsum()
    obv10=float(obv.iloc[-1]-obv.iloc[-11])
    prior10_close=float(c.iloc[-11]); chg10=(float(c.iloc[-1])/prior10_close-1)*100
    out['H1532OBV近10日方向']=('向上' if obv10>0 else '向下' if obv10<0 else '持平')
    out['H1532OBV量價背離']=('觀察｜OBV上升且股價尚未大漲' if obv10>0 and -5<=chg10<=7
                            else '未形成可核量價背離')
    # Wilder-style smoothing; ADX is non-directional trend strength.
    up=h.diff();down=-l.diff()
    plus=up.where((up>down)&(up>0),0.0)
    minus=down.where((down>up)&(down>0),0.0)
    tr=pd.concat([h-l,(h-c.shift()).abs(),(l-c.shift()).abs()],axis=1).max(axis=1)
    atr=tr.ewm(alpha=1/14,adjust=False,min_periods=14).mean()
    pdm=plus.ewm(alpha=1/14,adjust=False,min_periods=14).mean()
    mdm=minus.ewm(alpha=1/14,adjust=False,min_periods=14).mean()
    pdi=100*pdm/atr.replace(0,float('nan'))
    mdi=100*mdm/atr.replace(0,float('nan'))
    dx=(100*(pdi-mdi).abs()/(pdi+mdi).replace(0,float('nan')))
    adx=dx.ewm(alpha=1/14,adjust=False,min_periods=14).mean().iloc[-1]
    if _finite(adx):
        out['H1532ADX']=round(float(adx),2)
        out['H1532DMI方向']=('多向(+DI>-DI)' if pdi.iloc[-1]>mdi.iloc[-1] else
                              '空向(+DI<-DI)' if pdi.iloc[-1]<mdi.iloc[-1] else '方向不明')
    out['H1532技術來源']='同股同日H114已驗證OHLCV／連續85根／張數單位明確'
    return out
