# -*- coding: utf-8 -*-
from pathlib import Path
import sys
import pandas as pd
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from godpick_headless_page_loader import load_page_namespace

ns=load_page_namespace('pages/7_股神推薦.py',base_dir=ROOT)
assert callable(ns.get('_run_page07_automation_v191_h2'))
st=ns['__headless_st__']; k=ns['_k']
master=pd.DataFrame([{'code':f'{1000+i:04d}','name':f'T{i}','market':'上市','category':'電子'} for i in range(30)])
candidate=pd.DataFrame([{'股票代號':'2330','股票名稱':'台積電','正式推薦分區':'正式下週主推薦'}])
called={'record':0,'save':0}
ns['_load_watchlist_map']=lambda:{}
ns['_load_master_df']=lambda:master
ns['_load_master_df_fallback_only']=lambda:master
ns['_collect_all_categories']=lambda m,w:['電子']
ns['_load_persistent_recommend_scan_settings']=lambda w,c:{'universe_mode':'全市場','days':120,'scan_limit':30,'selected_categories':['全部'],'min_total_score':55,'min_signal_score':-2,'min_prelaunch_score':45,'min_trade_score':45,'recommend_mode':'綜合模式','risk_strictness':'標準','pick_strategy':'結合版'}
ns['_apply_recommend_scan_settings_to_state']=lambda settings,sync_widgets=False:None
ns['_load_persistent_settings']=lambda local_first=True:{'applied_weights':{'市場環境':10}}
ns['_normalize_weight_map']=lambda x:x
ns['_read_macro_mode_bridge']=lambda:{}
ns['_apply_macro_bridge_to_weights']=lambda w,b,enabled=True:w
ns['_build_universe_from_market']=lambda **kw:[{'code':f'{1000+i:04d}','name':f'T{i}'} for i in range(30)]
ns['_parse_manual_codes']=lambda *a:[]
ns['_load_recommend_result_from_state']=lambda:(pd.DataFrame(),pd.DataFrame(),pd.DataFrame())
def build(**kwargs):
    st.session_state[k('candidate_diagnosis_store')]=candidate.copy()
    st.session_state[k('scan_quality_report')]={'正式推薦可用':True,'success':30}
    return candidate.copy(),pd.DataFrame(),pd.DataFrame()
ns['_build_recommend_df']=build
ns['_postprocess_recommend_result_v164']=lambda rec,hot,bridge,enabled,force=True:(rec,hot,{})
ns['_conditional_reference_rows']=lambda src,max_rows=8:src.head(max_rows)
def save_result(*a): called['save']+=1; return True
ns['_save_recommend_result_to_state']=save_result
def record(src,background_write=False): called['record']+=1; return 1,['08 authority persisted']
ns['_v159_auto_record_actionable_recommendations']=record
ns['save_calibration_samples']=lambda *a,**kw:(1,['cal ok'],{'near':1,'missed':0,'total':1})
ns['_phase93_single_source_decision_frame']=lambda rec,src:src
ns['save_rotation_snapshot']=lambda *a,**kw:(True,'rotation ok')
ns['save_learning_run']=lambda *a,**kw:(True,['learning ok'],{})
ns['save_super_ai_run']=lambda *a,**kw:(True,'super ok',{})
out=ns['_run_page07_automation_v191_h2']({'force_full_market':True})
assert out['ok'],out
assert out['execution_owner']=='pages/7_股神推薦.py',out
assert called=={'record':1,'save':1},called
ctx=st.session_state[k('recommend_execution_context_v191')]
assert ctx['owner']=='07_股神推薦' and ctx['trigger']=='V191中央自動排程',ctx
assert out['record_added']==1,out
print('PASS Page07 canonical runner owns scheduled recommendation and persists once to Page08 authority')

# V191-H3: a completed scan with zero actionable rows is a WARNING with candidate
# diagnostics, not a fake recommendation and not a misleading "please press start".
called_zero={'record':0,'save':0}
zero_candidate=pd.DataFrame([
    {'股票代號':'2454','股票名稱':'聯發科','正式推薦分區':'候選觀察','V188選股分':78.0,'V188交易品質分':42.0,'阻擋原因':'風險報酬未達正式買進門檻'}
])
def build_zero(**kwargs):
    st.session_state[k('candidate_diagnosis_store')]=zero_candidate.copy()
    st.session_state[k('scan_quality_report')]={'正式推薦可用':True,'success':30}
    return pd.DataFrame(),pd.DataFrame(),pd.DataFrame()
ns['_build_recommend_df']=build_zero
ns['_conditional_reference_rows']=lambda src,max_rows=8: pd.DataFrame()
def save_zero(*a): called_zero['save']+=1; return True
ns['_save_recommend_result_to_state']=save_zero
def record_zero(src,background_write=False): called_zero['record']+=1; return 0,['沒有可操作推薦，未新增08歷史']
ns['_v159_auto_record_actionable_recommendations']=record_zero
out0=ns['_run_page07_automation_v191_h2']({'force_full_market':True})
assert out0['ok'] and out0.get('warning'),out0
assert int(out0.get('display_count',-1))==0,out0
assert int(out0.get('candidate_count',0))>=1,out0
assert called_zero=={'record':1,'save':1},called_zero
assert ('0檔' in out0.get('message','') or '0 檔' in out0.get('message','')),out0
print('PASS Page07 zero-actionable automation keeps diagnostics, records no weak fake pick, and returns WARNING')

# V191-H3: if Page08 authority persistence is blocked by the history-integrity
# lock, Page07 must not let the central scheduler report a false SUCCESS.
called_fail={'record':0,'save':0}
ns['_build_recommend_df']=build
ns['_save_recommend_result_to_state']=lambda *a: (called_fail.__setitem__('save',called_fail['save']+1) or True)
def record_fail(src,background_write=False):
    called_fail['record']+=1
    return 0,['V191-H3歷史救援尚未完成：目前權威僅 0 筆；推薦紀錄未寫入權威檔；本輪不得顯示保存成功。']
ns['_v159_auto_record_actionable_recommendations']=record_fail
out_fail=ns['_run_page07_automation_v191_h2']({'force_full_market':True})
assert not out_fail['ok'],out_fail
assert out_fail.get('record_integrity_failure'),out_fail
assert '不得標示SUCCESS' in out_fail.get('message',''),out_fail
assert called_fail=={'record':1,'save':1},called_fail
print('PASS Page07 refuses scheduler SUCCESS when Page08 history integrity/persistence is blocked')
