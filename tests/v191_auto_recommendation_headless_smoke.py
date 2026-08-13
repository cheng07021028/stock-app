# -*- coding: utf-8 -*-
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import godpick_headless_page_loader as loader
from godpick_auto_update_tasks import task_auto_recommendation

orig=loader.load_page_namespace
called={"count":0,"cfg":None}
try:
    def page7_runner(cfg=None):
        called["count"]+=1; called["cfg"]=dict(cfg or {})
        return {
            "ok":True,
            "message":"07股神推薦模組自動執行完成：掃描 100／候選 88／顯示 4／08永久紀錄 1",
            "execution_owner":"pages/7_股神推薦.py",
            "record_added":1,
            "changed_files":["godpick_latest_recommendations.json","godpick_records.json"],
        }
    loader.load_page_namespace=lambda *a,**kws:{"_run_page07_automation_v191_h2":page7_runner}
    out=task_auto_recommendation({"force_full_market":True})
    assert out["ok"],out
    assert called["count"]==1,called
    assert called["cfg"].get("force_full_market") is True,called
    assert "07股神推薦模組" in out["message"],out["message"]
    assert out["details"].get("execution_owner")=="pages/7_股神推薦.py",out
    print("PASS V191 auto recommendation adapter delegates exactly once to Page07 canonical runner")
finally:
    loader.load_page_namespace=orig
