# -*- coding: utf-8 -*-
from __future__ import annotations
from datetime import date,datetime,timedelta
from pathlib import Path
from typing import Any,Callable
from urllib.parse import quote_plus
from zoneinfo import ZoneInfo
import json,math,time,xml.etree.ElementTree as ET
VERSION='v191_h83_autonomous_data_freshness_truth_core_20260921'; BASE_DIR=Path(__file__).resolve().parent
STATUS_FILE='godpick_h83_autofresh_status.json'; NEWS_FILE='godpick_h83_news_cache.json'; TZ=ZoneInfo('Asia/Taipei')
def _now():return datetime.now(TZ)
def _txt(v):return '' if v is None else str(v).strip()
def _num(v,d=None):
 try:
  if isinstance(v,str):v=v.replace(',','').replace('%','').replace('％','').strip()
  x=float(v); return x if math.isfinite(x) else d
 except:return d
def _read(name,default):
 try:return json.loads((BASE_DIR/name).read_text(encoding='utf-8-sig'))
 except:return default
def _write(name,payload):
 p=BASE_DIR/name; p.parent.mkdir(parents=True,exist_ok=True); t=p.with_suffix(p.suffix+'.tmp_h83'); t.write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding='utf-8'); t.replace(p)
def _persist(name,payload,doc):
 _write(name,payload)
 try:
  from godpick_persistence_service import save_named_json_permanent
  r=save_named_json_permanent(name,payload,firestore_doc=doc); return bool(getattr(r,'permanent_ok',False))
 except:return True
def _date(v):
 try:return datetime.fromisoformat(_txt(v)[:10]).date()
 except:return None
def previous_weekday(d):
 if d is None:return None
 while d.weekday()>=5:d-=timedelta(days=1)
 return d
def business_lag(later,earlier):
 if not later or not earlier:return None
 if earlier>later:return -1
 n=0; cur=earlier
 while cur<later:
  cur+=timedelta(days=1)
  if cur.weekday()<5:n+=1
 return n
def _age(name):
 try:return max(0,(time.time()-(BASE_DIR/name).stat().st_mtime)/60)
 except:return None
def _rows(name):
 d=_read(name,[])
 if isinstance(d,list):return len(d)
 if isinstance(d,dict):
  for k in ['records','items','rows','data']:
   if isinstance(d.get(k),list):return len(d[k])
 return 0
def _official_date(d):
 if not isinstance(d,dict):return None
 for v in [d.get('data_date'),(d.get('meta') or {}).get('data_date') if isinstance(d.get('meta'),dict) else None]:
  q=previous_weekday(_date(v))
  if q:return q
 vals=[]
 for r in d.get('records',[]) if isinstance(d.get('records'),list) else []:
  if not isinstance(r,dict):continue
  for k in ['官方資料日期','官方因子資料日期','法人資料日期']:
   q=previous_weekday(_date(r.get(k)))
   if q: vals.append(q); break
 return max(vals) if vals else None
def repair_market_snapshot_payload(payload,max_abs_change_pct=15):
 out=dict(payload or {}); diag={'changed':False,'repairs':[],'quarantined':[]}; orig={}
 for k in ['twse_data_date','otc_data_date','futures_data_date','data_date','market_date']:
  a=_date(out.get(k)); b=previous_weekday(a)
  if a and b and a!=b: orig[k]=out.get(k); out[k]=b.isoformat(); diag['repairs'].append(f'{k}:{a}→{b}'); diag['changed']=True
 tw=previous_weekday(_date(out.get('twse_data_date'))); ot=previous_weekday(_date(out.get('otc_data_date')))
 if tw and out.get('market_date')!=tw.isoformat():out['market_date']=out['data_date']=tw.isoformat(); diag['changed']=True
 bad=[]
 for label,k in [('TWSE','twse_change_pct'),('OTC','otc_change_pct'),('FUTURES','futures_change_pct')]:
  x=_num(out.get(k));
  if x is not None and abs(x)>max_abs_change_pct:bad.append(label)
 if 'OTC' in bad:
  for k in ['otc_index','otc_change','otc_change_pct']:orig[k]=out.get(k);out[k]=None
  diag['quarantined'].append('OTC');diag['changed']=True
  req=dict(out.get('required_by_godpick') or {});req['otc_change']=req['otc_change_pct']=None;out['required_by_godpick']=req
  eff=dict((out.get('next_day_forecast') or {}).get('godpick_effect') or {}); reason=_txt(eff.get('lockdown_reason') or out.get('next_day_lockdown_reason')); twpct=_num(out.get('twse_change_pct'),0) or 0
  if eff.get('hard_filter') and '櫃買' in reason and twpct>-8:
   eff.update({'hard_filter':False,'mode':'H83-DATA-GUARD｜異常市場值隔離','score_delta':max(-2,_num(eff.get('score_delta'),-2) or -2),'market_weight_delta':-2,'position_cap_pct':max(30,int(_num(eff.get('position_cap_pct'),30) or 30)),'lockdown_reason':'H83已隔離櫃買異常值；不以失真數據啟動極端封鎖'})
   f=dict(out.get('next_day_forecast') or {});f['godpick_effect']=eff;out['next_day_forecast']=f;out['next_day_effect_mode']=eff['mode'];out['next_day_lockdown_reason']=eff['lockdown_reason'];diag['repairs'].append('false OTC lockdown neutralized')
 out['h83_truth_guard']={'version':VERSION,'checked_at':_now().strftime('%Y-%m-%d %H:%M:%S'),'business_date':(tw or ot).isoformat() if (tw or ot) else '','invalid_market_domains':bad,'formal_data_valid':'TWSE' not in bad}
 if orig:out['h83_original_values']=orig
 return out,diag
def repair_market_snapshot(settings=None,persist=True):
 from godpick_h83_autofresh_settings import load_settings_safe
 cfg=settings or load_settings_safe(); out,diag=repair_market_snapshot_payload(_read('market_snapshot.json',{}),cfg['quality']['max_market_abs_change_pct'])
 if diag['changed']:
  if persist:_persist('market_snapshot.json',out,'market_snapshot')
  else:_write('market_snapshot.json',out)
 return diag
def refresh_news_cache(settings=None,http_get=None,force=False):
 from godpick_h83_autofresh_settings import load_settings_safe
 cfg=settings or load_settings_safe(); age=_age(NEWS_FILE)
 if not force and age is not None and age<=cfg['ttl_minutes']['news']:return {'ok':True,'skipped':True,'message':'H83新聞快取仍新鮮','cache':_read(NEWS_FILE,{})}
 import requests; get=http_get or requests.get; items=[]; errors=[]
 for q in cfg['news']['queries']:
  try:
   r=get('https://news.google.com/rss/search?q='+quote_plus(q)+'&hl=zh-TW&gl=TW&ceid=TW:zh-Hant',headers={'User-Agent':'Mozilla/5.0 GodPick-H83'},timeout=8)
   if getattr(r,'status_code',0)!=200:errors.append(f'{q}:HTTP');continue
   root=ET.fromstring(r.content if getattr(r,'content',None) else str(r.text).encode())
   for it in root.findall('.//item')[:cfg['news']['max_items_per_query']]:
    title=_txt(it.findtext('title'))
    if title:items.append({'query':q,'title':title,'source':_txt(it.findtext('source')),'published_at':_txt(it.findtext('pubDate')),'link':_txt(it.findtext('link'))})
  except Exception as e:errors.append(f'{q}:{type(e).__name__}')
 seen=set(); unique=[]
 for x in items:
  if x['title'] not in seen:seen.add(x['title']);unique.append(x)
 p={'version':VERSION,'updated_at':_now().strftime('%Y-%m-%d %H:%M:%S'),'source':'Google News RSS','items':unique[:80],'errors':errors,'status':'ok' if unique else 'degraded'}; _persist(NEWS_FILE,p,'godpick_h83_news_cache')
 return {'ok':bool(unique),'message':f'H83新聞更新 {len(unique)} 則','cache':p}
def get_news_context(row=None,max_items=8):
 items=(_read(NEWS_FILE,{}) or {}).get('items',[]); raw=dict(row or {}); tokens=[_txt(raw.get(k)) for k in ['股票代號','股票名稱','類別','產業','族群名稱'] if len(_txt(raw.get(k)))>=2]; matched=[x for x in items if isinstance(x,dict) and any(t in _txt(x.get('title')) for t in tokens)]; return (matched or items)[:max_items]
def _snapshot(cfg):
 m=_read('market_snapshot.json',{}); o=_read('official_factors_cache.json',{}); truth=m.get('h83_truth_guard',{}) if isinstance(m,dict) else {}; md=previous_weekday(_date(m.get('market_date') or m.get('data_date') or m.get('twse_data_date'))) if isinstance(m,dict) else None; od=_official_date(o); lag=business_lag(md,od); mr=_rows('stock_master_cache.json'); orows=_rows('official_factors_cache.json'); bad=set(truth.get('invalid_market_domains') or []); ready=bool(mr>=cfg['quality']['min_stock_master_rows'] and md and 'TWSE' not in bad and orows>=cfg['quality']['min_official_factor_rows'] and lag is not None and 0<=lag<=cfg['quality']['max_official_business_lag']); issues=[]
 if mr<cfg['quality']['min_stock_master_rows']:issues.append(f'股票主檔僅{mr}筆')
 if not md:issues.append('市場業務日期未驗證')
 if 'TWSE' in bad:issues.append('TWSE市場值異常')
 if bad-{'TWSE'}:issues.append('部分市場異常值已隔離')
 if not(orows>=cfg['quality']['min_official_factor_rows'] and lag is not None and 0<=lag<=cfg['quality']['max_official_business_lag']):issues.append(f'官方因子未對齊：market={md} official={od} lag={lag}')
 return {'market_date':md.isoformat() if md else '','official_date':od.isoformat() if od else '','official_business_lag':lag,'stock_master_rows':mr,'official_factor_rows':orows,'invalid_market_domains':sorted(bad),'formal_ready':ready,'issues':issues,'ages_minutes':{k:_age(v) for k,v in {'stock_master':'stock_master_cache.json','macro_full':'market_snapshot.json','super_ai_context':'super_ai_market_context.json','watchlist_runtime':'watchlist_runtime_snapshot.json','performance_feedback':'godpick_performance_profile.json','news':NEWS_FILE}.items()}}
def run_autofresh_preflight(reason='before_recommendation',force=False,settings=None,handlers=None,allow_network=True):
 from godpick_h83_autofresh_settings import load_settings_safe
 cfg=settings or load_settings_safe(); actions=[]; taskmap=handlers
 if taskmap is None:
  try:
   from godpick_auto_update_tasks import TASK_HANDLERS; taskmap=dict(TASK_HANDLERS or {})
  except:taskmap={}
 before=_snapshot(cfg)
 def run(name,c=None):
  f=taskmap.get(name)
  if not callable(f):actions.append({'task':name,'ok':False,'message':'任務未載入'});return
  try:r=f(c or {}) or {};actions.append({'task':name,'ok':bool(r.get('ok')),'message':_txt(r.get('message'))})
  except Exception as e:actions.append({'task':name,'ok':False,'message':f'{type(e).__name__}:{e}'})
 a=before['ages_minutes'];t=cfg['ttl_minutes'];rr=cfg['refresh']
 if allow_network and rr['stock_master'] and (force or a['stock_master'] is None or a['stock_master']>t['stock_master'] or before['stock_master_rows']<cfg['quality']['min_stock_master_rows']):run('stock_master')
 if allow_network and rr['macro_full'] and (force or a['macro_full'] is None or a['macro_full']>t['macro_full'] or before['invalid_market_domains']):run('macro_full')
 repair_market_snapshot(cfg,True); mid=_snapshot(cfg)
 if allow_network and rr['official_factors'] and (force or mid['official_business_lag'] is None or mid['official_business_lag']<0 or mid['official_business_lag']>cfg['quality']['max_official_business_lag'] or mid['official_factor_rows']<cfg['quality']['min_official_factor_rows']):run('official_factors')
 if allow_network and rr['super_ai_context'] and (force or a['super_ai_context'] is None or a['super_ai_context']>t['super_ai_context']):run('super_ai_context',{'fetch_etf':True})
 if allow_network and rr['watchlist_runtime'] and (force or a['watchlist_runtime'] is None or a['watchlist_runtime']>t['watchlist_runtime']):run('watchlist_runtime')
 if allow_network and cfg['auto_refresh_news']: nr=refresh_news_cache(cfg,force=force);actions.append({'task':'h83_news','ok':bool(nr.get('ok')),'message':nr.get('message','')})
 if rr['performance_feedback'] and (force or a['performance_feedback'] is None or a['performance_feedback']>t['performance_feedback']):run('feedback_learning')
 if cfg['auto_refresh_h82_learning']:
  try:
   from godpick_h82_adaptive_learning import refresh_learning_state; state,_=refresh_learning_state(persist_remote=True);actions.append({'task':'h82_adaptive_learning','ok':isinstance(state,dict),'message':'H82成熟學習已重建'})
  except Exception as e:actions.append({'task':'h82_adaptive_learning','ok':False,'message':str(e)})
 repair_market_snapshot(cfg,True); after=_snapshot(cfg); status={'version':VERSION,'updated_at':_now().strftime('%Y-%m-%d %H:%M:%S'),'reason':reason,'formal_ready':after['formal_ready'],'research_only':not after['formal_ready'],'snapshot':after,'actions':actions}; _persist(STATUS_FILE,status,'godpick_h83_autofresh_status'); status['message']=('H83前置資料通過' if after['formal_ready'] else 'H83前置資料未完全就緒，Formal已降為研究模式：'+'；'.join(after['issues'])); return status
def load_status():
 d=_read(STATUS_FILE,{});return d if isinstance(d,dict) else {}
def apply_h83_freshness_overlay(frame,status=None):
 import pandas as pd
 out=frame.copy() if isinstance(frame,pd.DataFrame) else pd.DataFrame(frame); st=status or load_status()
 if isinstance(st.get('snapshot'),dict):snap=st['snapshot']
 else:
  from godpick_h83_autofresh_settings import load_settings_safe;snap=_snapshot(load_settings_safe())
 out['H83版本']=VERSION;out['H83正式資料可用']='是' if snap.get('formal_ready') else '否';out['H83市場資料日期']=snap.get('market_date','');out['H83官方因子日期']=snap.get('official_date','');out['H83官方落後交易日']=snap.get('official_business_lag');out['H83資料異常隔離']='；'.join(snap.get('invalid_market_domains') or []);out['H83資料治理摘要']='READY' if snap.get('formal_ready') else 'RESEARCH-ONLY｜'+'；'.join(snap.get('issues') or []);return out
