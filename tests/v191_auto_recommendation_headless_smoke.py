# -*- coding: utf-8 -*-
from pathlib import Path
import sys
import pandas as pd
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import godpick_headless_page_loader as loader
from godpick_headless_page_loader import HeadlessStreamlit
from godpick_auto_update_tasks import task_auto_recommendation

orig=loader.load_page_namespace
try:
    st=HeadlessStreamlit()
    def k(x): return f"auto::{x}"
    master=pd.DataFrame([{"code":f"{1000+i:04d}","name":f"測試{i}","market":"上市","category":"電子"} for i in range(100)])
    def apply_settings(settings,sync_widgets=False):
        for key,val in settings.items(): st.session_state[k(key)]=val
    def build_df(**kwargs):
        cand=pd.DataFrame([{"股票代號":"2330","股票名稱":"台積電","V188股神作戰優先分":80,"V188交易許可":"READY-COND"}])
        st.session_state[k("candidate_diagnosis_store")]=cand
        st.session_state[k("scan_quality_report")]={"planned":100,"success":100}
        return cand.copy(),pd.DataFrame(),pd.DataFrame()
    ns={
        "__headless_st__":st,"_k":k,
        "_load_watchlist_map":lambda:{},"_load_master_df":lambda:master,"_load_master_df_fallback_only":lambda:master,
        "_collect_all_categories":lambda m,w:["電子"],
        "_load_persistent_recommend_scan_settings":lambda w,c:{"universe_mode":"全市場","group":"","days":120,"scan_limit":100,"selected_categories":["全部"],"min_total_score":55.0,"min_signal_score":-2.0,"min_prelaunch_score":45.0,"min_trade_score":45.0,"recommend_mode":"綜合模式","risk_strictness":"標準","pick_strategy":"結合版"},
        "_apply_recommend_scan_settings_to_state":apply_settings,
        "_load_persistent_settings":lambda local_first=True:{"applied_weights":{"市場環境":10}},"_normalize_weight_map":lambda x:x,
        "_read_macro_mode_bridge":lambda:{},"_apply_macro_bridge_to_weights":lambda w,b,enabled=True:w,
        "_build_universe_from_market":lambda **kwargs:[{"code":f"{1000+i:04d}","name":f"測試{i}"} for i in range(100)],
        "_parse_manual_codes":lambda *a:[],"_build_recommend_df":build_df,
        "_postprocess_recommend_result_v164":lambda rec,hot,bridge,enabled,force=True:(rec,hot,{}),
        "_conditional_reference_rows":lambda source,max_rows=8:source.head(max_rows),
        "_save_recommend_result_to_state":lambda rec,cat,hot:True,
        "_v159_auto_record_actionable_recommendations":lambda source,background_write=False:(1,["永久權威成功"]),
        "save_rotation_snapshot":None,"save_learning_run":None,"save_super_ai_run":None,
    }
    loader.load_page_namespace=lambda *a,**kws:ns
    out=task_auto_recommendation({"force_full_market":True})
    assert out["ok"],out
    assert "掃描 100" in out["message"],out["message"]
    assert "永久紀錄 1" in out["message"]
    print("PASS V191 auto recommendation headless adapter | scan=100 | permanent_records=1")
finally:
    loader.load_page_namespace=orig
