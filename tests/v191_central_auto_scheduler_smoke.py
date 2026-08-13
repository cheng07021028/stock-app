# -*- coding: utf-8 -*-
from __future__ import annotations
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import sys,time
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import godpick_auto_scheduler as sched
from godpick_headless_page_loader import load_page_namespace

REQUIRED={
 "stock_master","macro_full","official_factors","super_ai_context","watchlist_runtime","godpick_recommendation",
 "record_latest_price","record_performance","recommend_list_performance","recommend_list_n_day",
 "recommend_list_hits","t1_truth","feedback_learning","durability_retry",
}
assert REQUIRED.issubset(set(sched.JOB_LABELS)), set(sched.JOB_LABELS)
assert not any(x in " ".join(sched.JOB_LABELS.values()) for x in ["清除快取","刪除","重置設定"])

# settings/times normalization
cfg=sched.normalize_settings({"enabled":True,"jobs":{"macro_full":{"enabled":True,"times":["14:20","20:40","bad"]}}})
assert cfg["jobs"]["macro_full"]["times"]==["14:20","20:40"]
assert cfg["jobs"]["godpick_recommendation"]["require_dependencies"] is True

# All enabled jobs can be dry-run without external network and dependency order allows recommendation.
t0=time.perf_counter()
res=sched.run_due_jobs(now=datetime(2026,8,12,21,0,tzinfo=ZoneInfo("Asia/Taipei")),force_all_enabled=True,simulate=True)
elapsed=time.perf_counter()-t0
rows=res.get("executed",[])
assert len(rows)>=len(REQUIRED), (len(rows),rows)
rr={r["job"]:r for r in rows}
assert rr["godpick_recommendation"]["status"]=="SUCCESS", rr["godpick_recommendation"]
assert elapsed<3, elapsed

# Deterministic failure simulation: official factor fail must BLOCK auto recommendation, and messages remain visible.
orig_load=sched.load_settings; orig_status=sched.load_status; orig_hist=sched.load_history; orig_exec=sched._execute_one
orig_persist=sched._persist_runtime; orig_lock=sched._acquire_lock; orig_unlock=sched._release_lock
try:
    fake_cfg=sched.normalize_settings(sched.DEFAULT_SETTINGS)
    fake_cfg["enabled"]=True
    sched.load_settings=lambda: fake_cfg
    sched.load_status=lambda: {"version":sched.VERSION,"jobs":{},"completed_run_keys":[]}
    sched.load_history=lambda: []
    sched._persist_runtime=lambda *a,**k:(True,"mock")
    sched._acquire_lock=lambda *a,**k:(True,"")
    sched._release_lock=lambda : None
    def fake_exec(job,jc,gc):
        if job=="official_factors": return {"ok":False,"message":"mock official failure","finished_at":"2026-08-12 21:00:00"}
        return {"ok":True,"message":"mock success","finished_at":"2026-08-12 21:00:00","duration_seconds":0.001}
    sched._execute_one=fake_exec
    out=sched.run_due_jobs(now=datetime(2026,8,12,21,0,tzinfo=ZoneInfo("Asia/Taipei")),force_all_enabled=True)
    m={r["job"]:r for r in out["executed"]}
    assert m["official_factors"]["status"]=="FAILED"
    assert m["godpick_recommendation"]["status"]=="BLOCKED"
    assert "官方因子" in m["godpick_recommendation"]["message"]
finally:
    sched.load_settings=orig_load; sched.load_status=orig_status; sched.load_history=orig_hist; sched._execute_one=orig_exec
    sched._persist_runtime=orig_persist; sched._acquire_lock=orig_lock; sched._release_lock=orig_unlock

# Headless production page functions are loadable without Streamlit installed in the test container.
page0=load_page_namespace("pages/0_大盤走勢.py",base_dir=ROOT)
page8=load_page_namespace("pages/8_股神推薦紀錄.py",base_dir=ROOT)
page10=load_page_namespace("pages/10_推薦清單.py",base_dir=ROOT)
page7=load_page_namespace("pages/7_股神推薦.py",base_dir=ROOT)
assert callable(page0.get("_v70_run_one_click_update_and_write"))
assert callable(page8.get("_refresh_latest_prices"))
assert callable(page8.get("save_records_sync_fast"))
assert callable(page8.get("save_records_permanent"))
assert callable(page10.get("_update_formal_n_day_metrics_v98"))
assert callable(page10.get("_update_night_hit_tracking_v101"))
assert callable(page10.get("save_records_permanent"))
assert callable(page10.get("save_named_json_permanent"))
assert callable(page7.get("_build_recommend_df"))
assert callable(page7.get("load_watchlist_permanent"))
assert callable(page7.get("save_records_sync_fast"))

# Page0 exact V70 one-click deterministic replay: all manual business substeps are included.
page0["_fetch_market_with_fallback"]=lambda d,realtime=False:{"ok":True,"close":100,"used_date":str(d),"source":"mock"}
page0["_save_market_row"]=lambda row:None
page0["_fetch_otc_with_fallback"]=lambda d,timeout=2.8:{"ok":True,"close":200,"used_date":str(d),"source":"mock"}
page0["_save_otc_row"]=lambda row:None
page0["_fetch_twse_institutional_manual"]=lambda d,timeout=2.8:{"ok":True,"total_100m":1,"date":str(d),"source":"mock"}
page0["_save_inst_row"]=lambda row:None
page0["_fetch_us_market_manual"]=lambda d:(3,["ok"])
page0["_fetch_taifex_futures_manual"]=lambda d,timeout=2.8:{"ok":True,"tx_close":1,"tx_change":1,"date":str(d),"source":"mock"}
page0["_save_taifex_row"]=lambda row:None
page0["_v68_fetch_overnight_global_market"]=lambda :{"items":{"x":1},"overnight_factor":{"overnight_ok_count":1},"updated_at":"2026-08-12 20:00:00","source_mode":"mock"}
page0["_v68_write_json"]=lambda *a,**k:(True,"mock")
page0["_write_market_snapshot_v30"]=lambda row:(True,"mock write")
page0["_v70_check_written_files"]=lambda :{"all_written":True,"missing_snapshot_keys":[],"missing_bridge_keys":[],"macro_trend_records_ok":True,"snapshot_updated_at":"2026-08-12","bridge_updated_at":"2026-08-12"}
page0["_v70_safe_write_json"]=lambda *a,**k:(True,"mock")
macro=page0["_v70_run_one_click_update_and_write"](datetime(2026,8,12).date())
assert macro["all_done"] is True, macro
assert [x["項目"] for x in macro["steps"]]==["加權指數","櫃買指數","三大法人","外盤 / 美盤","台指期 / 期貨","隔夜國際盤","寫入股神橋接檔"]

# Page8 latest-price deterministic replay: verified newer quote must update, not preserve old recommendation price.
df=pd.DataFrame([{"推薦日期":"2026-08-11","股票代號":"2330","股票名稱":"台積電","推薦價格":100.0,"最新價":100.0,"目前狀態":"觀察","市場別":"上市"}])
page8["_batch_latest_quotes"]=lambda rows:{"2330":(110.0,"上市","MOCK_OFFICIAL","2026-08-12","14:30:00")}
page8["_tw_today"]=lambda : datetime(2026,8,12).date()
out8=page8["_refresh_latest_prices"](df,only_active=False)
summary=out8.attrs.get("latest_refresh_summary",{})
assert summary.get("success")==1, summary
assert float(out8.iloc[0]["最新價"])==110.0

# Static GitHub wiring: one central cron, old official cron manual-only.
central=(ROOT/".github/workflows/godpick_auto_scheduler_v191.yml").read_text(encoding="utf-8")
old=(ROOT/".github/workflows/update_official_factors_v112.yml").read_text(encoding="utf-8")
assert 'cron: "*/10 * * * *"' in central
assert "tools/run_godpick_auto_scheduler_v191.py" in central
assert "schedule:" not in old

print(f"V191 PASS | jobs={len(REQUIRED)} | dry-run={elapsed:.4f}s | macro_steps=7 | latest_price_success=1")
