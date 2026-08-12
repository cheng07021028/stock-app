# -*- coding: utf-8 -*-
"""V183 SuperAI experience store and bounded calibration feedback.

The important design rule is *experience, not self-confirmation*: model outputs are
saved together with later realized returns.  Calibration uses executable/realized
outcomes when available and only applies small bounded corrections.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any
import hashlib
import json
import math

try:
    import pandas as pd
except Exception:
    pd = None

try:
    from godpick_durability_service import persist_json_async
except Exception:
    persist_json_async = None

EXPERIENCE_VERSION = "super_ai_experience_v188_20260812"
INDEX_FILE = "super_ai_experience_index.json"
PROFILE_FILE = "super_ai_experience_profile.json"
RUN_DIR = "data/super_ai_runs"
BASE_DIR = Path(__file__).resolve().parent

SNAPSHOT_FIELDS = [
    "股票代號", "股票名稱", "市場別", "類別", "K線最後交易日", "最新價",
    "股神推薦總排名", "股神推薦優先分", "正式推薦分區", "操作許可",
    "AI推薦資格", "AI綜合決策分", "AI Alpha品質分", "AI Timing時機分", "AI Risk風控分",
    "AI Continuation延續分", "AI跨市場新強股分", "AI新證據分", "AI過熱型態",
    "Entry進場買點分", "Risk風控安全分", "實戰風險報酬比", "路徑風險報酬比",
    "主要進場路徑", "主要進場參考價", "實戰觸發價", "觸發後守價", "停損參考", "第一壓力價",
    "今日漲幅%", "近5日漲幅%", "近20日漲幅%", "當日量比", "當日收盤位置%", "上影線比例%",
    "主流資金分", "族群攻擊強度", "成交額百萬", "資料完整度評分", "官方因子新鮮度",
    "SuperAI進場狀態", "SuperAI進場信心%", "SuperAI建議進場區間下", "SuperAI建議進場區間上",
    "SuperAI出場狀態", "SuperAI動態停損價", "SuperAI第一減碼價", "SuperAI第二目標價",
    "SuperAI開高走高%", "SuperAI開高走低%", "SuperAI開低走高%", "SuperAI開低走低%", "SuperAI平開震盪%",
    "SuperAI隔日上漲機率%", "SuperAI本週進場適合度", "SuperAI融資影響分", "SuperAI_ETF確認分",
    "SuperAI市場情境分", "SuperAI資料覆蓋率%", "SuperAI最終決策分", "SuperAI最終決策理由", "SuperAI模型版本",
    "SuperAI Alpha分", "SuperAI Alpha等級", "SuperAI Trade分", "SuperAI Trade等級", "SuperAI最終作戰等級",
    "SuperAI執行風報比", "SuperAI風報比來源", "SuperAI校準後隔日上漲機率%",
    "V188股神作戰優先分", "V188交易許可", "V188正式推薦資格", "V188RR治理", "V188T+1追價治理",
    "V188個股資料證據", "V188市場對齊治理", "V188類股集中治理", "V188類股集中扣分",
]


def _now() -> str:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Taipei")).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _safe(v: Any) -> Any:
    if v is None or isinstance(v, (str, int, float, bool)):
        if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
            return None
        return v
    try:
        if pd is not None and pd.isna(v): return None
    except Exception: pass
    try: return v.item()
    except Exception: return str(v)


def _rows(data: Any) -> list[dict[str, Any]]:
    if pd is not None and isinstance(data, pd.DataFrame):
        raw = data.to_dict(orient="records")
    elif isinstance(data, list): raw = data
    elif isinstance(data, dict): raw = data.get("records") or data.get("rows") or []
    else: raw = []
    return [{str(k): _safe(v) for k,v in r.items()} for r in raw if isinstance(r, dict)]


def _read(path: str, default: Any) -> Any:
    try:
        p=BASE_DIR/path
        if not p.exists(): return default
        return json.loads(p.read_text(encoding="utf-8-sig"))
    except Exception: return default


def _write_async(path: str, payload: Any, reason: str) -> None:
    if callable(persist_json_async):
        persist_json_async(path, payload, reason=reason)
    else:
        p=BASE_DIR/path; p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def build_super_ai_experience_profile(records: Any | None = None) -> dict[str, Any]:
    # V188 prefers the T+1 truth table because it distinguishes selection from
    # triggered trades.  Legacy recommendation records are only a fallback.
    truth_rows=[]
    calibration={}
    if records is None:
        try:
            from godpick_t1_trade_truth import load_t1_truth_rows, load_probability_calibration
            truth_rows=load_t1_truth_rows()
            calibration=load_probability_calibration()
        except Exception:
            truth_rows=[]; calibration={}
    samples=[]
    executable_returns=[]
    if truth_rows:
        for r in truth_rows:
            if not bool(r.get("T1成熟")):
                continue
            prob=r.get("SuperAI原始上漲機率%")
            ret=r.get("隔日候選漲跌%")
            try: prob=float(prob); ret=float(ret)
            except Exception: continue
            if not (math.isfinite(ret) and math.isfinite(prob)): continue
            state=str(r.get("V188交易許可") or r.get("推薦角色") or "")
            samples.append((prob,1.0 if ret>0 else 0.0,ret,state))
            if bool(r.get("是否納入可執行績效")):
                er=r.get("可執行交易1日%")
                if er is None: er=r.get("觸發當日收盤績效%")
                try: er=float(er)
                except Exception: er=None
                if er is not None and math.isfinite(er): executable_returns.append(er)
    else:
        rows = _rows(records) if records is not None else _rows(_read("godpick_records.json", []))
        for r in rows:
            if not str(r.get("SuperAI模型版本") or "").startswith("super_ai"):
                continue
            ret = r.get("推薦後1日%")
            prob = r.get("SuperAI隔日上漲機率%")
            try: ret=float(ret); prob=float(prob)
            except Exception: continue
            if not (math.isfinite(ret) and math.isfinite(prob)): continue
            samples.append((prob, 1.0 if ret>0 else 0.0, ret, str(r.get("SuperAI進場狀態") or "")))
    n=len(samples)
    if n:
        mean_prob=sum(x[0] for x in samples)/n
        actual=sum(x[1] for x in samples)/n*100
        brier=sum(((x[0]/100)-x[1])**2 for x in samples)/n
        avg_ret=sum(x[2] for x in samples)/n
        bias=max(-8.0,min(8.0,actual-mean_prob))
    else:
        mean_prob=actual=brier=avg_ret=bias=0.0
    state_stats={}
    for state in sorted({x[3] for x in samples if x[3]}):
        sub=[x for x in samples if x[3]==state]
        state_stats[state]={"n":len(sub),"win_rate_pct":round(sum(x[1] for x in sub)/len(sub)*100,1),"avg_ret1_pct":round(sum(x[2] for x in sub)/len(sub),3)}
    bins = calibration.get("bins") if isinstance(calibration,dict) and isinstance(calibration.get("bins"),list) else []
    return {
        "version": EXPERIENCE_VERSION,
        "updated_at": _now(),
        "experience_source": "V188 T+1 truth" if truth_rows else "legacy godpick_records fallback",
        "eligible_samples": n,
        "mean_predicted_up_pct": round(mean_prob,2),
        "actual_up_rate_pct": round(actual,2),
        "brier_score": round(brier,5) if n else None,
        "avg_ret1_pct": round(avg_ret,4) if n else None,
        "probability_bias_pp": round(bias,2),
        "state_stats": state_stats,
        "calibration_bins": bins,
        "executable_samples": len(executable_returns),
        "executable_win_rate_pct": round(sum(1 for x in executable_returns if x>0)/len(executable_returns)*100,2) if executable_returns else None,
        "avg_executable_ret_pct": round(sum(executable_returns)/len(executable_returns),4) if executable_returns else None,
        "calibration_policy": "sample<30不調整；30~99最多±3pp；100+最多±8pp；未觸發雷達不計交易勝負",
    }


def refresh_super_ai_experience_profile(records: Any | None = None) -> dict[str, Any]:
    profile=build_super_ai_experience_profile(records)
    n=int(profile.get("eligible_samples") or 0)
    raw=float(profile.get("probability_bias_pp") or 0)
    limit=0.0 if n<30 else 3.0 if n<100 else 8.0
    profile["applied_probability_bias_pp"]=round(max(-limit,min(limit,raw)),2)
    _write_async(PROFILE_FILE, profile, "V188 SuperAI experience profile")
    return profile


def load_super_ai_experience_profile() -> dict[str, Any]:
    data=_read(PROFILE_FILE,{})
    if not isinstance(data,dict) or data.get("version")!=EXPERIENCE_VERSION:
        return refresh_super_ai_experience_profile()
    return data


def save_super_ai_run(candidate_data: Any, recommendation_data: Any | None = None, *, metadata: dict[str,Any] | None = None) -> tuple[bool,str,dict[str,Any]]:
    rows=_rows(candidate_data)
    if not rows: return False,"no candidate rows",{}
    now=_now(); date=now[:10].replace("-","")
    material=f"{now}|{len(rows)}|{rows[0].get('K線最後交易日','')}|{metadata or {}}"
    run_id=hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]
    compact=[{k:r.get(k) for k in SNAPSHOT_FIELDS if k in r} for r in rows]
    rec_rows=_rows(recommendation_data)
    payload={
        "version": EXPERIENCE_VERSION,"run_id":run_id,"created_at":now,
        "candidate_count":len(compact),"recommendation_count":len(rec_rows),
        "metadata":metadata or {},"candidates":compact,
    }
    path=f"{RUN_DIR}/{date}/{run_id}.json"
    _write_async(path,payload,"V188 SuperAI immutable decision run")
    index=_read(INDEX_FILE,{"version":EXPERIENCE_VERSION,"runs":[]})
    if not isinstance(index,dict): index={"version":EXPERIENCE_VERSION,"runs":[]}
    runs=[x for x in index.get("runs",[]) if isinstance(x,dict) and x.get("run_id")!=run_id]
    summary={"run_id":run_id,"created_at":now,"path":path,"candidate_count":len(compact),"recommendation_count":len(rec_rows),"data_date":str(rows[0].get("K線最後交易日") or ""),"model_version":str(rows[0].get("SuperAI模型版本") or "")}
    runs.append(summary); runs=runs[-500:]
    index={"version":EXPERIENCE_VERSION,"updated_at":now,"runs":runs}
    _write_async(INDEX_FILE,index,"V188 SuperAI run index")
    return True,f"saved SuperAI run {run_id} ({len(compact)} candidates)",summary


__all__=["EXPERIENCE_VERSION","SNAPSHOT_FIELDS","build_super_ai_experience_profile","refresh_super_ai_experience_profile","load_super_ai_experience_profile","save_super_ai_run"]
