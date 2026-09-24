# -*- coding: utf-8 -*-
"""V191-H94 Research Selection Intelligence.

Purpose
-------
Fix the observed gap where strong H79 momentum could remain in the research top
list even while H81 risk, H82 mature learning, H89 execution and sector regime
were collectively warning against the setup.

H94 is authoritative only for Research vs Waiting prioritisation. It never
creates, removes or relaxes Formal/A-/R1 authority.
"""
from __future__ import annotations

from typing import Any, Iterable
import math
import pandas as pd

VERSION = "v191_h94_research_selection_intelligence_20260923"

H94_COLUMNS = [
    "H94版本","H94Alpha品質分","H94研究層級","H94研究資格","H94研究排序加減分",
    "H94共識風險數","H94共識風險","H94耗竭風險","H94族群衝突","H94族群狀態",
    "H94新聞證據狀態","H94風控底線","H94選股/執行分離","H94主題群組",
    "H94集中度治理","H94升級理由","H94降級理由","H94學習標籤建議","H94T1驗證計畫",
    "H94Formal權限","H94決策摘要",
]

MANAGER_FRONT = [
    "股票代號","股票名稱","市場別","類別","H94研究層級","H94Alpha品質分","H94研究資格",
    "H94共識風險數","H94耗竭風險","H94族群衝突","H94族群狀態","H94新聞證據狀態",
    "H94風控底線","H94選股/執行分離","H94主題群組","H94集中度治理",
    "H94升級理由","H94降級理由","H94學習標籤建議","H94T1驗證計畫","H94Formal權限","H94決策摘要",
    "H93研究優先級","H93七維總分","H81專業研究總分","H81風險管理分","H82自適應加減分",
    "H89選股方向分","H89執行品質分","H89交易型態","H89主進場","H89防守停損","H89第一目標","H89成本後RR1","H89Formal價格計畫合格",
]


def _text(v: Any) -> str:
    if v is None: return ""
    try:
        if pd.isna(v): return ""
    except Exception: pass
    return str(v).strip()


def _num(v: Any, default: float | None = None) -> float | None:
    if v is None or isinstance(v, bool): return default
    if isinstance(v, str):
        v = v.replace(",", "").replace("％", "%").replace("%", "").replace("+", "").strip()
        if not v or v.lower() in {"nan","none","null","--","-","<na>"}: return default
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _clip(v: float | None, lo: float=0.0, hi: float=100.0, default: float=50.0) -> float:
    x = default if v is None else float(v)
    return max(lo, min(hi, x))


def _first_num(row: dict[str,Any], names: Iterable[str]) -> float | None:
    for n in names:
        x = _num(row.get(n), None)
        if x is not None: return x
    return None


def _first_text(row: dict[str,Any], names: Iterable[str]) -> str:
    for n in names:
        s = _text(row.get(n))
        if s: return s
    return ""


def _settings(settings: dict[str,Any] | None=None) -> dict[str,Any]:
    if isinstance(settings, dict):
        try:
            from godpick_h94_research_selection_settings import normalize_settings
            return normalize_settings(settings)
        except Exception: return settings
    from godpick_h94_research_selection_settings import load_settings_safe
    return load_settings_safe()


def _broad_theme(category: str) -> str:
    s = _text(category)
    if any(k in s for k in ["半導體","晶圓","封測","IC設計","記憶體","矽晶圓","測試介面"]): return "半導體鏈"
    if any(k in s for k in ["PCB","載板","被動元件","連接器","散熱","機殼","電源供應","光通訊","網通","電子"]): return "電子供應鏈"
    if any(k in s for k in ["金融","銀行","金控","保險","證券"]): return "金融"
    if any(k in s for k in ["生技","醫療","製藥"]): return "生技醫療"
    if any(k in s for k in ["航運","航空","觀光"]): return "運輸觀光"
    return s or "未分類"


def _sector_map(sector_df: pd.DataFrame | None) -> dict[str,dict[str,Any]]:
    if sector_df is None or not isinstance(sector_df,pd.DataFrame) or sector_df.empty or "類別" not in sector_df.columns:
        return {}
    out={}
    for r in sector_df.to_dict("records"):
        cat=_text(r.get("類別"))
        if cat: out[cat]=r
    return out


def _sector_truth(row: dict[str,Any], sector_lookup: dict[str,dict[str,Any]]) -> tuple[float,str,bool,bool,str]:
    cat=_text(row.get("類別"))
    sr=sector_lookup.get(cat,{})
    regime=_first_text(sr,["族群輪動狀態","強勢族群等級"])
    flow=_first_num(sr,["族群資金流分數","類股熱度分數"])
    advice=_first_text(sr,["族群策略建議","族群資金流說明"])
    pct=_first_num(row,["H79族群百分位%","H53族群共振分"])
    score = flow if flow is not None else pct if pct is not None else 50.0
    bad = any(k in regime for k in ["資金退潮","高檔鈍化","弱勢","降溫","退潮"])
    good = any(k in regime for k in ["主升","加速","資金流入","攻擊","強勢"])
    if not regime:
        if pct is not None and pct < 40: regime="橫截面弱勢"; bad=True
        elif pct is not None and pct >= 75: regime="橫截面強勢"; good=True
        else: regime="族群中性/待確認"
    return _clip(score), regime, bad, good, advice


def analyze_candidate(row: dict[str,Any] | pd.Series, *, sector_lookup: dict[str,dict[str,Any]] | None=None, settings: dict[str,Any] | None=None) -> dict[str,Any]:
    raw=row.to_dict() if isinstance(row,pd.Series) else dict(row or {})
    cfg=_settings(settings); sector_lookup=sector_lookup or {}
    h93=_first_num(raw,["H93七維總分"]); h79=_first_num(raw,["H79自適應機會分","H79研究推薦分"])
    h81=_first_num(raw,["H81專業研究總分"]); risk=_first_num(raw,["H81風險管理分","Risk風控安全分"])
    h89s=_first_num(raw,["H89選股方向分"]); h89e=_first_num(raw,["H89執行品質分"])
    h82=_first_num(raw,["H82自適應加減分"],); backtest=_first_num(raw,["H81歷史回測可信分"])
    strength=_first_num(raw,["H79強度百分位%","H47個股相對強度分"])
    chase=_first_num(raw,["追價風險分","追高風險分數_決策"])
    sector_score, sector_regime, sector_bad, sector_good, sector_advice = _sector_truth(raw,sector_lookup)
    sector_pct=_first_num(raw,["H79族群百分位%","H53族群共振分"])
    leader_exception=bool(
        sector_bad and (strength or 0) >= 80 and (sector_pct or 0) >= 75
        and (h89e or 0) >= 60 and (risk or 0) >= 40
    )
    category=_text(raw.get("類別")); theme=_broad_theme(category)

    risks_text="｜".join([_first_text(raw,["H81三大風險"]), _first_text(raw,["H81下一步關注"]), sector_advice])
    overheat_terms=["過熱","高檔鈍化","動能轉弱","已大幅上漲","追價風險高","耗竭"]
    overheat=any(k in risks_text for k in overheat_terms) or (chase is not None and chase >= float(cfg["anti_exhaustion"]["chase_risk_threshold"]))

    next_watch=_first_text(raw,["H81下一步關注"])
    news=_first_num(raw,["H81新聞事件影響分"])
    news_missing=(news is None) or (abs(news-50.0)<1e-9 and any(k in next_watch for k in ["沒有可驗證個股實體新聞","未知新聞維持中性","沒有可驗證新聞"]))
    news_status="MISSING｜不計為中性有效證據" if news_missing else "VERIFIED/AVAILABLE"

    consensus=[]
    if cfg["consensus"].get("risk_below_40",True) and risk is not None and risk < 40: consensus.append("H81風控<40")
    if h82 is not None and h82 <= float(cfg["consensus"]["h82_negative_threshold"]): consensus.append(f"H82成熟學習{h82:+.2f}")
    if h89e is not None and h89e < float(cfg["consensus"]["execution_below"]): consensus.append("H89執行偏弱")
    if backtest is not None and backtest < float(cfg["consensus"]["backtest_below"]): consensus.append("歷史回測偏弱")
    if cfg["consensus"].get("sector_bad_counts",True) and sector_bad and not leader_exception: consensus.append("族群退潮/鈍化")
    if cfg["consensus"].get("overheat_counts",True) and overheat: consensus.append("耗竭/追價風險")

    weights=cfg["weights"]
    comp={"h93":h93,"h79":h79,"h81":h81,"risk":risk,"h89_selection":h89s,"h89_execution":h89e,"sector":sector_score}
    num=0.0; den=0.0
    for k,v in comp.items():
        if v is not None:
            w=float(weights.get(k,0) or 0); num += _clip(v)*w; den += w
    score=num/den if den else 50.0
    penalties=[]; boosts=[]
    if risk is not None:
        if risk < 30: score-=12; penalties.append("極低風控")
        elif risk < 40: score-=8; penalties.append("低風控")
        elif risk < 50: score-=4; penalties.append("風控偏低")
    if h89e is not None:
        if h89e < 30: score-=10; penalties.append("執行品質極低")
        elif h89e < 45: score-=5; penalties.append("執行品質偏低")
    if h82 is not None:
        if h82 <= -.6: score-=4; penalties.append("成熟學習負向")
        elif h82 <= -.3: score-=2; penalties.append("成熟學習偏負")
        elif h82 >= .3: score+=1.5; boosts.append("成熟學習偏正")
    if sector_bad and leader_exception:
        score += 1.0; penalties.append("族群逆風") ; boosts.append("逆勢領先候選")
    elif sector_bad:
        score-=7; penalties.append("族群衝突")
    elif sector_good and (strength or 0)>=80:
        score+=2; boosts.append("個股/族群共振")
    if overheat: score-=float(cfg["anti_exhaustion"]["penalty_points"]); penalties.append("Anti-Exhaustion")
    if backtest is not None and backtest < 35: score-=4; penalties.append("回測可信偏弱")
    if news_missing and not bool(cfg["news"].get("missing_is_neutral_evidence",False)):
        score-=float(cfg["news"].get("missing_confidence_penalty",1.0)); penalties.append("新聞缺資料")
    if risk is not None and risk>=65 and h89e is not None and h89e>=65 and h89s is not None and h89s>=58:
        score+=3; boosts.append("風控/執行/選股共振")
    score=_clip(score)

    hard_floor=(risk is not None and risk < float(cfg["quality"]["hard_risk_floor"])) or (h89e is not None and h89e < float(cfg["quality"]["hard_execution_floor"]))
    hard_exhaust=bool(cfg["anti_exhaustion"].get("enabled",True) and overheat and (risk or 50)<float(cfg["anti_exhaustion"]["risk_score_threshold"]) and (h89e or 50)<float(cfg["anti_exhaustion"]["execution_score_threshold"]))
    max_cons=int(cfg["quality"]["max_consensus_risk_for_research"])
    standard_eligible=score>=float(cfg["quality"]["min_research_score"]) and len(consensus)<=max_cons and not hard_floor and not hard_exhaust
    leader_eligible=bool(leader_exception and score>=float(cfg["quality"].get("contrarian_leader_min_score",52.0)) and len(consensus)<=1 and not hard_floor and not hard_exhaust)
    eligible=standard_eligible or leader_eligible

    if hard_exhaust: level="BLOCK｜等待冷卻"
    elif score>=float(cfg["quality"]["a_score"]) and len(consensus)<=1: level="A｜Alpha優先研究"
    elif score>=float(cfg["quality"]["b_score"]) and len(consensus)<=2: level="B｜重點研究"
    elif leader_eligible: level="C｜逆勢領先研究"
    elif eligible: level="C｜條件研究"
    else: level="WAIT｜等待改善"
    eligibility="KEEP/PROMOTE" if eligible else "WAIT/BLOCK"

    formal_plan=_first_text(raw,["H89Formal價格計畫合格"])
    sep = "Selection優先驗證／Execution待修" if (h89s or 0)>=60 and (h89e or 100)<45 else "Execution可用／Selection需驗證" if (h89e or 0)>=60 and (h89s or 100)<58 else "Selection×Execution共同驗證"
    if leader_exception: conflict="族群退潮但個股/橫截面領先｜逆勢領先待確認"
    elif sector_bad and (strength or 0)>=80: conflict="高強度個股 vs 弱/鈍化族群｜須證明逆勢領先"
    elif sector_bad: conflict="個股與族群環境衝突"
    else: conflict="無明顯衝突"
    exhaustion="BLOCK" if hard_exhaust else "HIGH" if overheat else "LOW"
    risk_floor="PASS" if (risk is None or risk>=40) and (h89e is None or h89e>=40) else "CAUTION" if not hard_floor else "BLOCK"

    if hard_exhaust: learn="Late Momentum / Exhaustion"
    elif sector_bad: learn="Sector-Stock Conflict"
    elif (h89s or 0)>=60 and (h89e or 100)<45: learn="Selection vs Execution"
    elif formal_plan=="是" and (h89e or 0)>=60: learn="Entry Efficiency / Opportunity Cost"
    else: learn="Selection×Execution×Regime"
    t1="T+1驗證方向/Alpha；若Entry未觸發，另記Opportunity Cost；若觸發，記MFE/MAE與收盤相對Entry。"
    if news_missing: t1 += " 新聞層標記Missing，不把50分當有效中性證據。"

    raw_adj=(score-50)/50*4.0
    adj=max(-4.0,min(4.0,raw_adj))
    summary=f"Alpha品質{score:.1f}｜{level}｜共識風險{len(consensus)}｜族群:{sector_regime}｜{sep}；只治理Research/Waiting，不取得Formal權限。"
    return {
        "H94版本":VERSION,"H94Alpha品質分":round(score,2),"H94研究層級":level,"H94研究資格":eligibility,
        "H94研究排序加減分":round(adj,2),"H94共識風險數":len(consensus),"H94共識風險":"；".join(consensus) or "無重大共識警示",
        "H94耗竭風險":exhaustion,"H94族群衝突":conflict,"H94族群狀態":sector_regime,"H94新聞證據狀態":news_status,
        "H94風控底線":risk_floor,"H94選股/執行分離":sep,"H94主題群組":theme,"H94集中度治理":"待研究池選擇後確認",
        "H94升級理由":"；".join(boosts) or "無額外升級證據","H94降級理由":"；".join(penalties) or "無額外降級因素",
        "H94學習標籤建議":learn,"H94T1驗證計畫":t1,
        "H94Formal權限":"LOCKED｜H94不得建立/放寬Formal；Formal仍由H64/H68＋資料/流動性/RR/停損治理。",
        "H94決策摘要":summary,
    }


def apply_overlay(frame: pd.DataFrame | None, *, sector_df: pd.DataFrame | None=None, settings: dict[str,Any] | None=None) -> pd.DataFrame:
    if frame is None: return pd.DataFrame()
    if not isinstance(frame,pd.DataFrame): frame=pd.DataFrame(frame)
    out=frame.copy(deep=True)
    if out.empty:
        for c in H94_COLUMNS:
            if c not in out.columns: out[c]=pd.Series(dtype="object")
        return out
    lookup=_sector_map(sector_df); cfg=_settings(settings)
    add=pd.DataFrame([analyze_candidate(r,sector_lookup=lookup,settings=cfg) for r in out.to_dict("records")],index=out.index)
    for c in H94_COLUMNS: out[c]=add[c] if c in add.columns else None
    front=[c for c in MANAGER_FRONT if c in out.columns]; rest=[c for c in out.columns if c not in front]
    return out.loc[:,front+rest].copy()


def _dedupe_code(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "股票代號" not in frame.columns: return frame
    x=frame.copy(); x["__h94_code"]=x["股票代號"].astype(str).str.replace(r"\.0$","",regex=True).str.strip()
    x=x.loc[~x["__h94_code"].duplicated(keep="first")].drop(columns=["__h94_code"])
    return x


def _rebuild_research_waiting(research: pd.DataFrame, waiting: pd.DataFrame, cfg: dict[str,Any]) -> tuple[pd.DataFrame,pd.DataFrame,dict[str,int]]:
    r=research.copy(); w=waiting.copy()
    if not r.empty: r["__h94_origin"]="research"
    if not w.empty: w["__h94_origin"]="waiting"
    combined=pd.concat([r,w],ignore_index=True,sort=False) if (not r.empty or not w.empty) else pd.DataFrame()
    combined=_dedupe_code(combined)
    if combined.empty: return r.drop(columns=["__h94_origin"],errors="ignore"),w.drop(columns=["__h94_origin"],errors="ignore"),{"selected":0,"promoted":0,"demoted":0,"concentration_blocked":0}
    for c in ["H94Alpha品質分","H93七維總分","H79自適應機會分"]:
        if c in combined.columns: combined[c]=pd.to_numeric(combined[c],errors="coerce")
    combined=combined.sort_values([c for c in ["H94Alpha品質分","H93七維總分","H79自適應機會分"] if c in combined.columns],ascending=False,na_position="last").reset_index(drop=True)
    max_rows=int(cfg["portfolio"]["max_research_rows"]); max_cat=int(cfg["portfolio"]["max_per_category"]); max_theme=int(cfg["portfolio"]["max_per_broad_theme"])
    selected=[]; cat_count={}; theme_count={}; blocked=0
    for i,row in combined.iterrows():
        if _text(row.get("H94研究資格"))!="KEEP/PROMOTE": continue
        cat=_text(row.get("類別")) or "未分類"; theme=_text(row.get("H94主題群組")) or _broad_theme(cat)
        if cat_count.get(cat,0)>=max_cat or theme_count.get(theme,0)>=max_theme:
            combined.at[i,"H94集中度治理"]="WAIT｜研究池集中度上限"
            combined.at[i,"H94降級理由"]=( _text(row.get("H94降級理由"))+"；研究池集中度上限" ).strip("；")
            blocked+=1; continue
        selected.append(i); cat_count[cat]=cat_count.get(cat,0)+1; theme_count[theme]=theme_count.get(theme,0)+1
        combined.at[i,"H94集中度治理"]="PASS"
        if len(selected)>=max_rows: break
    sel=set(selected)
    research_out=combined.loc[list(selected)].copy() if selected else combined.iloc[0:0].copy()
    waiting_out=combined.loc[[i for i in combined.index if i not in sel]].copy()
    research_out=research_out.sort_values("H94Alpha品質分",ascending=False,na_position="last") if not research_out.empty else research_out
    waiting_out=waiting_out.sort_values("H94Alpha品質分",ascending=False,na_position="last") if not waiting_out.empty else waiting_out
    promoted=int((research_out.get("__h94_origin",pd.Series(dtype=str)).astype(str)=="waiting").sum()) if not research_out.empty else 0
    demoted=int((waiting_out.get("__h94_origin",pd.Series(dtype=str)).astype(str)=="research").sum()) if not waiting_out.empty else 0
    research_out=research_out.drop(columns=["__h94_origin"],errors="ignore").reset_index(drop=True)
    waiting_out=waiting_out.drop(columns=["__h94_origin"],errors="ignore").reset_index(drop=True)
    return research_out,waiting_out,{"selected":len(research_out),"promoted":promoted,"demoted":demoted,"concentration_blocked":blocked}


def decorate_decision_tables(tables: dict[str,Any] | None, *, candidate_df: pd.DataFrame | None=None, scan_report: dict[str,Any] | None=None, sector_df: pd.DataFrame | None=None, settings: dict[str,Any] | None=None) -> dict[str,pd.DataFrame]:
    tables=tables if isinstance(tables,dict) else {}
    # Always retain H93 as the preceding seven-direction evidence layer.
    try:
        from godpick_h93_evolution_engine import decorate_decision_tables as decorate_h93
        base=decorate_h93(tables,candidate_df=candidate_df,scan_report=scan_report,settings=None)
    except Exception:
        base={k:(v.copy() if isinstance(v,pd.DataFrame) else pd.DataFrame(v) if v is not None else pd.DataFrame()) for k,v in tables.items()}
    cfg=_settings(settings)
    out={}
    for name in ["actionable","research","recommendations","waiting","emerging_watch","data_repairs","audit","health"]:
        frame=base.get(name,pd.DataFrame())
        if name!="health" and isinstance(frame,pd.DataFrame) and not frame.empty:
            frame=apply_overlay(frame,sector_df=sector_df,settings=cfg)
        out[name]=frame if isinstance(frame,pd.DataFrame) else pd.DataFrame()
    if bool(cfg.get("enabled",True)):
        new_r,new_w,diag=_rebuild_research_waiting(out.get("research",pd.DataFrame()),out.get("waiting",pd.DataFrame()),cfg)
        out["research"]=new_r; out["waiting"]=new_w
    else:
        diag={"selected":len(out.get("research",pd.DataFrame())),"promoted":0,"demoted":0,"concentration_blocked":0}
    health=out.get("health",pd.DataFrame()).copy()
    if not health.empty and "項目" in health.columns:
        health=health.loc[~health["項目"].astype(str).str.startswith("H94")].copy()
    rows=[
        {"項目":"H94版本","數值":VERSION},
        {"項目":"H94治理範圍","數值":"Research/Waiting重排；Formal完全鎖定"},
        {"項目":"H94研究輸出列","數值":diag["selected"]},
        {"項目":"H94由Waiting升級","數值":diag["promoted"]},
        {"項目":"H94由Research降級","數值":diag["demoted"]},
        {"項目":"H94集中度擋下","數值":diag["concentration_blocked"]},
        {"項目":"H94Anti-Exhaustion","數值":"ENABLED" if cfg["anti_exhaustion"].get("enabled",True) else "OFF"},
        {"項目":"H94新聞Missing治理","數值":"Missing≠Neutral Evidence"},
        {"項目":"H94Formal權限","數值":"LOCKED"},
    ]
    out["health"]=pd.concat([health,pd.DataFrame(rows)],ignore_index=True,sort=False)

    # H96: high-conviction discovery expands the research universe from the old
    # Research+Waiting survivors to the bounded 120-row audit pool.  This is
    # intentionally downstream of H94 so H94 evidence remains visible, while
    # sheet 02 becomes a high-reference list instead of a quota-filled list.
    try:
        from godpick_h96_high_conviction_engine import apply_h96_decision_tables
        out = apply_h96_decision_tables(
            out, sector_df=sector_df, candidate_df=candidate_df, settings=None
        )
    except Exception as _h96_exc:
        _health = out.get("health", pd.DataFrame()).copy()
        _health = pd.concat([_health, pd.DataFrame([
            {"項目":"H96核心研究引擎","數值":f"ERROR｜{type(_h96_exc).__name__}: {_h96_exc}"},
            {"項目":"H96Formal權限","數值":"LOCKED｜H96失敗不影響既有Formal治理"},
        ])], ignore_index=True, sort=False)
        out["health"] = _health

    # H99: reconcile execution truth after H96 has finished selecting the
    # manager-facing pools.  This is intentionally last-mile and idempotent so
    # restored compact snapshots/Excel exports cannot re-introduce an old H81
    # RR narrative that conflicts with the newer H89 price plan.
    try:
        from godpick_h99_execution_truth import decorate_decision_tables as decorate_h99_execution_truth
        out = decorate_h99_execution_truth(out)
    except Exception as _h99_exc:
        _health = out.get("health", pd.DataFrame()).copy()
        _health = pd.concat([_health, pd.DataFrame([
            {"項目":"H99執行真相引擎","數值":f"ERROR｜{type(_h99_exc).__name__}: {_h99_exc}"},
            {"項目":"H99Formal權限","數值":"LOCKED｜H99失敗不影響既有Formal治理"},
        ])], ignore_index=True, sort=False)
        out["health"] = _health
    return out


def export_contract_summary(tables: dict[str,pd.DataFrame] | None) -> dict[str,Any]:
    tables=tables if isinstance(tables,dict) else {}
    result={"version":VERSION,"ok":True,"sheets":{}}
    for name in ["actionable","research","waiting","audit","health","emerging_watch"]:
        df=tables.get(name,pd.DataFrame()); ok=isinstance(df,pd.DataFrame)
        cols=[c for c in H94_COLUMNS if ok and c in df.columns]
        if name!="health" and ok and not df.empty and not cols: result["ok"]=False
        result["sheets"][name]={"rows":int(len(df)) if ok else 0,"h94_columns":len(cols),"ok":ok}
    return result
