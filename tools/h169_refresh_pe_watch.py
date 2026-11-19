# -*- coding: utf-8 -*-
"""Manual/scheduled H169 official PE-only capture, never a full EPS authority."""
from __future__ import annotations
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from godpick_h169_pe_watch import ENDPOINTS, merge_pe_observations, load_pe_snapshot, PE_ONLY_FILE


def refresh(*, get=None, observed_at: str | None = None, output: Path = PE_ONLY_FILE) -> dict:
    import requests
    get = get or requests.get
    observed_at = observed_at or datetime.now(ZoneInfo("Asia/Taipei")).isoformat(timespec="seconds")
    fetched = {}
    errors = {}
    for name, url in ENDPOINTS.items():
        try:
            res = get(url, headers={"User-Agent": "StockApp-H169-official-pe/1.0",
                                    "Accept": "application/json"}, timeout=(4, 15))
            code = int(getattr(res, "status_code", 0) or 0)
            if code != 200:
                errors[name] = f"HTTP {code}"
                continue
            raw = res.json()
            if not isinstance(raw, list):
                errors[name] = "官方回應不是 JSON 陣列"
                continue
            fetched[name] = raw
        except Exception as exc:
            errors[name] = type(exc).__name__
    payload = merge_pe_observations(fetched, observed_at=observed_at)
    payload["errors"] = errors
    if not payload["pe_only"]:
        return {"saved": False, "status": "NO_NEW_PE_OBSERVATION", "errors": errors,
                "previous": load_pe_snapshot(output).get("status"),
                "message": "沒有可核對的官方PE列；保留上次快照，不寫空資料。"}
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    tmp = output.with_name(output.name+".tmp")
    tmp.write_text(json.dumps(payload,ensure_ascii=False,indent=2),encoding="utf-8")
    os.replace(tmp,output)
    return {"saved": True,"status": payload["status"],
            "observed_at": observed_at,
            "pe_rows":len(payload["pe_only"]),
            "sources":payload["sources"],
            "errors":errors,
            "eps_independently_verified":False,
            "trade_authority_modified":False}


if __name__ == "__main__":
    result=refresh()
    print(json.dumps(result,ensure_ascii=False,indent=2))
    if not result["saved"]:
        raise SystemExit("H169 official PE snapshot missing; did not overwrite previous authority")
