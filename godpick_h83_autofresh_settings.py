# -*- coding: utf-8 -*-
from __future__ import annotations
from copy import deepcopy
from datetime import datetime
from typing import Any
VERSION='v191_h83_autonomous_data_freshness_settings_20260921'
SETTINGS_FILE='godpick_h83_autofresh_settings.json'; FIRESTORE_DOC='godpick_h83_autofresh_settings'
DEFAULT_SETTINGS={
 'version':VERSION,'enabled':True,'updated_at':'','update_seq':0,
 'auto_refresh_before_manual_recommendation':True,'auto_refresh_before_scheduled_recommendation':True,
 'strict_formal_freshness_gate':True,'repair_market_semantics':True,'auto_refresh_news':True,'auto_refresh_h82_learning':True,
 'refresh':{'stock_master':True,'macro_full':True,'official_factors':True,'super_ai_context':True,'watchlist_runtime':True,'performance_feedback':True},
 'ttl_minutes':{'stock_master':10080,'macro_full':120,'super_ai_context':240,'watchlist_runtime':720,'performance_feedback':360,'news':90},
 'quality':{'max_market_abs_change_pct':15.0,'max_official_business_lag':1,'min_stock_master_rows':1000,'min_official_factor_rows':1000},
 'news':{'max_items_per_query':12,'queries':['台股 股市','台灣 半導體','AI 伺服器 台灣','台灣 央行 利率 經濟']},
 'governance':{'formal_permission_immutable':True,'weekend_is_not_trading_day':True,'invalid_market_outlier_must_be_quarantined':True}
}
def _i(v,a,b,d):
 try:x=int(v)
 except:x=d
 return max(a,min(b,x))
def _f(v,a,b,d):
 try:x=float(v)
 except:x=d
 return max(a,min(b,x))
def normalize_settings(raw:Any)->dict:
 out=deepcopy(DEFAULT_SETTINGS)
 if isinstance(raw,dict):
  for k,v in raw.items():
   if k not in {'refresh','ttl_minutes','quality','news','governance'}: out[k]=v
  for sec in ['refresh','ttl_minutes','quality','news']:
   if isinstance(raw.get(sec),dict): out[sec].update(raw[sec])
 for k in ['enabled','auto_refresh_before_manual_recommendation','auto_refresh_before_scheduled_recommendation','repair_market_semantics','auto_refresh_news','auto_refresh_h82_learning']: out[k]=bool(out.get(k,DEFAULT_SETTINGS[k]))
 out['strict_formal_freshness_gate']=True
 for k in DEFAULT_SETTINGS['refresh']: out['refresh'][k]=bool(out['refresh'].get(k,True))
 t=out['ttl_minutes']; t['stock_master']=_i(t.get('stock_master'),60,43200,10080); t['macro_full']=_i(t.get('macro_full'),15,1440,120); t['super_ai_context']=_i(t.get('super_ai_context'),30,1440,240); t['watchlist_runtime']=_i(t.get('watchlist_runtime'),30,4320,720); t['performance_feedback']=_i(t.get('performance_feedback'),30,1440,360); t['news']=_i(t.get('news'),15,1440,90)
 q=out['quality']; q['max_market_abs_change_pct']=_f(q.get('max_market_abs_change_pct'),5,30,15); q['max_official_business_lag']=_i(q.get('max_official_business_lag'),0,3,1); q['min_stock_master_rows']=_i(q.get('min_stock_master_rows'),100,10000,1000); q['min_official_factor_rows']=_i(q.get('min_official_factor_rows'),100,10000,1000)
 n=out['news']; n['max_items_per_query']=_i(n.get('max_items_per_query'),3,30,12); qs=n.get('queries') if isinstance(n.get('queries'),list) else []; n['queries']=[str(x).strip()[:120] for x in qs if str(x).strip()][:8] or list(DEFAULT_SETTINGS['news']['queries'])
 out['governance']=deepcopy(DEFAULT_SETTINGS['governance']); out['version']=VERSION
 try:out['update_seq']=max(0,int(out.get('update_seq',0) or 0))
 except:out['update_seq']=0
 return out
def load_settings():
 try:
  from godpick_persistence_service import load_named_json_permanent
  p,d=load_named_json_permanent(SETTINGS_FILE,DEFAULT_SETTINGS,firestore_doc=FIRESTORE_DOC); return normalize_settings(p),list(d or [])
 except Exception as e:return normalize_settings(DEFAULT_SETTINGS),[f'H83設定讀取採預設值：{type(e).__name__}: {e}']
def load_settings_safe(): return load_settings()[0]
def save_settings(settings):
 current,_=load_settings(); p=normalize_settings(settings); p['update_seq']=int(current.get('update_seq',0) or 0)+1; p['updated_at']=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
 try:
  from godpick_persistence_service import save_named_json_permanent
  r=save_named_json_permanent(SETTINGS_FILE,p,firestore_doc=FIRESTORE_DOC); ok=bool(getattr(r,'permanent_ok',False)); msg='｜'.join(x for x in [getattr(r,'local_message',''),getattr(r,'github_message',''),getattr(r,'firestore_message','')] if x)
  if not ok:return False,'H83設定未取得永久權威確認。'+msg,p
  v,_=load_settings()
  if int(v.get('update_seq',0) or 0)<p['update_seq']:return False,'H83設定寫入後回讀版本未更新。',p
  return True,f"H83自動新鮮度設定已永久保存（seq={p['update_seq']}）。{msg}",p
 except Exception as e:return False,f'H83設定保存失敗：{type(e).__name__}: {e}',p
