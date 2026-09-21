# -*- coding: utf-8 -*-
from __future__ import annotations
from datetime import date,datetime,time,timedelta
from typing import Any
from zoneinfo import ZoneInfo
TAIPEI_TZ=ZoneInfo('Asia/Taipei');TWSE_MARKET_CLOSE=time(13,30);TWSE_T86_FIRST_RELEASE=time(18);TWSE_T86_FINAL_RELEASE=time(20);GODPICK_FINAL_RELEASE_GRACE_MINUTES=20;RELEASE_TIMING_VERSION='v191_h83_weekend_business_date_truth_20260921'
def _p(v:Any):
 if isinstance(v,datetime):return v.date()
 if isinstance(v,date):return v
 s=str(v or '').strip()
 for f in ('%Y-%m-%d','%Y/%m/%d','%Y%m%d'):
  try:return datetime.strptime(s[:10] if '-' in f or '/' in f else s[:8],f).date()
  except:pass
 return None
def _prev(d):
 if not d:return None
 while d.weekday()>=5:d-=timedelta(days=1)
 return d
def _lag(a,b):
 a=_prev(a);b=_prev(b)
 if not a or not b:return 999
 if a>=b:return 0
 n=0
 while a<b:
  a+=timedelta(days=1)
  if a.weekday()<5:n+=1
 return n
def evaluate_twse_t86_release_timing(*,market_date,official_date,now=None):
 now=(now or datetime.now(TAIPEI_TZ)); now=now.replace(tzinfo=TAIPEI_TZ) if now.tzinfo is None else now.astimezone(TAIPEI_TZ); rm,ro=_p(market_date),_p(official_date);m,o=_prev(rm),_prev(ro);lag=_lag(o,m); base={'version':RELEASE_TIMING_VERSION,'now_taipei':now.strftime('%Y-%m-%d %H:%M:%S'),'market_date_raw':rm.isoformat() if rm else '','official_date_raw':ro.isoformat() if ro else '','market_date':m.isoformat() if m else '','official_date':o.isoformat() if o else '','official_lag':lag,'business_date_normalized':bool((rm and rm!=m) or (ro and ro!=o)),'twse_first_release':'18:00','twse_final_release':'20:00','system_grace_minutes':GODPICK_FINAL_RELEASE_GRACE_MINUTES,'source_note':'18:00/20:00為TWSE三大法人買賣超檔官方產製時間；H83另排除週末偽交易日。','t1_is_normal_now':False,'same_day_final_expected':False,'level':'warning','phase':'UNKNOWN','headline':'官方因子時序未驗證','detail':'缺日期','next_milestone':''}
 if not m or not o:return base
 if o>=m:base.update({'phase':'T0_READY','level':'success','headline':'官方因子已對齊最新有效交易日','detail':f'官方 {o}／市場 {m}','same_day_final_expected':now.date()==m and now.time()>=TWSE_T86_FINAL_RELEASE});return base
 if lag>=2:base.update({'phase':'STALE','level':'error','headline':'官方因子落後超過1交易日','detail':f'官方 {o} 相對市場 {m} 落後 {lag} 個平日基準'});return base
 if now.date()!=m:base.update({'phase':'HISTORICAL_T1','level':'warning','headline':'官方因子為歷史T-1','detail':f'市場有效交易日為 {m}，目前已非該交易日產製時窗'});return base
 t=now.time()
 if t<TWSE_MARKET_CLOSE:phase='LIVE_SESSION_T1'
 elif t<TWSE_T86_FIRST_RELEASE:phase='WAIT_FIRST_T86'
 elif t<TWSE_T86_FINAL_RELEASE:phase='WAIT_FINAL_T86'
 elif now<datetime.combine(now.date(),TWSE_T86_FINAL_RELEASE,tzinfo=TAIPEI_TZ)+timedelta(minutes=20):phase='FINAL_RELEASE_GRACE'
 else:phase='T0_EXPECTED'
 base.update({'phase':phase,'level':'info' if phase!='T0_EXPECTED' else 'warning','t1_is_normal_now':phase!='T0_EXPECTED','same_day_final_expected':phase in {'FINAL_RELEASE_GRACE','T0_EXPECTED'},'headline':'T-1時序正常' if phase!='T0_EXPECTED' else '當日官方資料應已產製','detail':'H83依TWSE產製時窗判定，未將週六/週日列為交易日'});return base
