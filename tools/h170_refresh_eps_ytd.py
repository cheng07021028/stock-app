# -*- coding: utf-8 -*-
"""Official TWSE/TPEx general-industry EPS YTD first-observed snapshot.

Do not infer quarterly EPS from YTD without historical snapshots of each
quarter. No trading authority, no old-date backfill, no records mutation.
"""
from __future__ import annotations
import json,os,sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from godpick_h170_eps_evidence import SOURCES,combine_sources,load_snapshot,EPS_SNAPSHOT

def refresh(get=None,*,observed_at=None,output=EPS_SNAPSHOT):
    import requests
    get=get or requests.get
    now=observed_at or datetime.now(ZoneInfo("Asia/Taipei")).isoformat(timespec="seconds")
    parts={};errors={}
    for market,url in SOURCES.items():
        try:
            r=get(url,timeout=(4,15),headers={"Accept":"application/json","User-Agent":"StockApp-H170-official-EPS/1.0"})
            if int(getattr(r,"status_code",0))!=200:
                errors[market]="HTTP "+str(getattr(r,"status_code","?"));continue
            raw=r.json()
            if not isinstance(raw,list):errors[market]="INVALID_JSON_ARRAY";continue
            parts[market]=raw
        except Exception as exc:
            errors[market]=type(exc).__name__
    data=combine_sources(parts,observed_at=now)
    data["errors"]=errors
    if not data["eps_ytd_rows"]:
        return {"saved":False,"status":"NO_VALID_OFFICIAL_EPS",
                "errors":errors,"previous":load_snapshot(output).get("status")}
    output=Path(output)
    output.parent.mkdir(parents=True,exist_ok=True)
    tmp=output.with_name(output.name+".tmp")
    tmp.write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding="utf-8")
    os.replace(tmp,output)
    return {"saved":True,"status":data["status"],"rows":len(data["eps_ytd_rows"]),
            "sources":data["sources"],"errors":errors,
            "eps_ttm_certified":False,"trading_changed":False}

if __name__=="__main__":
    result=refresh()
    print(json.dumps(result,ensure_ascii=False,indent=2))
    if not result["saved"]:raise SystemExit("No official EPS YTD snapshot; retained previous file.")
