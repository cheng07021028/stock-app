# -*- coding: utf-8 -*-
"""V191-H96 High-Conviction Opportunity Discovery.

Why this layer exists
---------------------
H94 fixed risk/sector conflicts but still only re-ranked the old Research +
Waiting pool.  On 2026-09-23 that allowed six low-reference C-tier names to stay
visible while a true market leader (2368) was present in the 120-row audit pool.

H96 therefore:
* discovers from the broader audit pool (bounded, no full-market rescan);
* makes sheet 02 a *high-conviction reference list*, not a quota-filled list;
* requires current leadership evidence, not just safe execution/fundamentals;
* allows bad-sector names only as explicit leader exceptions;
* labels leaders that are extended / not executable as LEADER-NO-CHASE;
* keeps Formal/A-/R1 authority fully locked to existing governance.
"""
from __future__ import annotations

from typing import Any, Iterable
import math
import warnings
import pandas as pd

VERSION = "v191_h96_high_conviction_opportunity_discovery_20260923"

H96_COLUMNS = [
    "H96版本","H96核心機會分","H96核心研究等級","H96參考價值","H96核心資格",
    "H96領先證據數","H96領先證據","H96領先例外","H96市場族群一致性","H96執行狀態",
    "H96研究池來源","H96不追價","H96核心理由","H96排除理由","H96Formal權限","H96決策摘要",
]

MANAGER_FRONT = [
    "股票代號","股票名稱","市場別","類別",
    "H96核心研究等級","H96核心機會分","H96參考價值","H96核心資格","H96領先證據數",
    "H96領先證據","H96領先例外","H96市場族群一致性","H96執行狀態","H96研究池來源",
    "H96不追價","H96核心理由","H96排除理由","H96Formal權限","H96決策摘要",
    "H94研究層級","H94Alpha品質分","H94共識風險數","H94族群狀態","H94風控底線",
    "H93研究優先級","H93七維總分","H81專業研究總分","H81技術多週期分","H81風險管理分",
    "H82自適應加減分","H89選股方向分","H89執行品質分","H89交易型態","H89主進場",
    "H89防守停損","H89第一目標","H89成本後RR1","H89Formal價格計畫合格",
    "H79自適應機會分","H79強度百分位%","H79族群百分位%",
]


def _text(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def _num(v: Any, default: float | None = None) -> float | None:
    if v is None or isinstance(v, bool):
        return default
    if isinstance(v, str):
        v = v.replace(",", "").replace("％", "%").replace("%", "").replace("+", "").strip()
        if not v or v.lower() in {"nan","none","null","--","-","<na>"}:
            return default
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _clip(v: float | None, lo: float=0.0, hi: float=100.0, default: float=50.0) -> float:
    x = default if v is None else float(v)
    return max(lo, min(hi, x))


def _first_num(row: dict[str, Any], names: Iterable[str]) -> float | None:
    for n in names:
        x = _num(row.get(n), None)
        if x is not None:
            return x
    return None


def _first_text(row: dict[str, Any], names: Iterable[str]) -> str:
    for n in names:
        s = _text(row.get(n))
        if s:
            return s
    return ""


def _settings(settings: dict[str, Any] | None=None) -> dict[str, Any]:
    if isinstance(settings, dict):
        try:
            from godpick_h96_high_conviction_settings import normalize_settings
            return normalize_settings(settings)
        except Exception:
            return settings
    from godpick_h96_high_conviction_settings import load_settings_safe
    return load_settings_safe()


def _broad_theme(category: str) -> str:
    s = _text(category)
    if any(k in s for k in ["半導體","晶圓","封測","IC設計","記憶體","矽晶圓","測試介面","PCB","載板"]):
        return "半導體/AI電子鏈"
    if any(k in s for k in ["被動元件","連接器","散熱","機殼","電源供應","光通訊","網通","電子"]):
        return "電子供應鏈"
    if any(k in s for k in ["金融","銀行","金控","保險","證券"]): return "金融"
    if any(k in s for k in ["生技","醫療","製藥"]): return "生技醫療"
    if any(k in s for k in ["航運","航空","觀光"]): return "運輸觀光"
    return s or "未分類"


def _sector_lookup(sector_df: pd.DataFrame | None) -> dict[str, dict[str, Any]]:
    if sector_df is None or not isinstance(sector_df, pd.DataFrame) or sector_df.empty or "類別" not in sector_df.columns:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for r in sector_df.to_dict("records"):
        cat = _text(r.get("類別"))
        if cat:
            out[cat] = r
    return out


def _sector_state(row: dict[str, Any], lookup: dict[str, dict[str, Any]]) -> tuple[str, float | None, bool, bool]:
    cat = _text(row.get("類別"))
    sr = lookup.get(cat, {})
    regime = _first_text(sr, ["族群輪動狀態","強勢族群等級"]) or _first_text(row,["H94族群狀態"])
    flow = _first_num(sr, ["族群資金流分數","類股熱度分數"])
    bad = any(k in regime for k in ["資金退潮","高檔鈍化","弱勢","降溫","退潮"])
    good = any(k in regime for k in ["主升","加速","資金流入","攻擊","強勢"])
    if not regime:
        regime = "待確認"
    return regime, flow, bad, good


def analyze_candidate(row: dict[str, Any] | pd.Series, *, sector_lookup: dict[str, dict[str, Any]] | None=None, settings: dict[str, Any] | None=None, source: str="audit") -> dict[str, Any]:
    raw = row.to_dict() if isinstance(row, pd.Series) else dict(row or {})
    cfg = _settings(settings)
    lookup = sector_lookup or {}
    d = cfg["discovery"]

    strength = _first_num(raw,["H79強度百分位%","H47個股相對強度分"])
    sector_pct = _first_num(raw,["H79族群百分位%","H53族群共振分"])
    h89s = _first_num(raw,["H89選股方向分"])
    h89e = _first_num(raw,["H89執行品質分"])
    tech = _first_num(raw,["H81技術多週期分"])
    market = _first_num(raw,["H81市場定價理解分"])
    risk = _first_num(raw,["H81風險管理分"])
    growth = _first_num(raw,["H93成長機會分","H93七維總分"])
    h82 = _first_num(raw,["H82自適應加減分"])
    h79 = _first_num(raw,["H79自適應機會分"])
    h94_level = _first_text(raw,["H94研究層級"])
    h94_exhaust = _first_text(raw,["H94耗竭風險"])
    formal_plan = _first_text(raw,["H89Formal價格計畫合格"])
    trade_type = _first_text(raw,["H89交易型態"])
    regime, flow, sector_bad, sector_good = _sector_state(raw, lookup)

    comps = {
        "strength_pct": strength,
        "sector_pct": sector_pct,
        "h89_selection": h89s,
        "h81_technical": tech,
        "h81_market_pricing": market,
        "h81_risk": risk,
        "h89_execution": h89e,
        "h93_growth": growth,
    }
    num=0.0; den=0.0
    for k,v in comps.items():
        if v is None: continue
        w=float(cfg["weights"].get(k,0) or 0)
        num += _clip(v) * w; den += w
    score = num/den if den else 50.0

    evidence=[]
    if (strength or 0) >= 75: evidence.append("個股強度Top25%")
    if (sector_pct or 0) >= 75: evidence.append("族群橫截面Top25%")
    if (h89s or 0) >= 60: evidence.append("Selection>=60")
    if (tech or 0) >= 60: evidence.append("技術多週期>=60")
    if (h79 or 0) >= 60: evidence.append("H79機會分>=60")
    if (risk or 0) >= 65 and (h89e or 0) >= 60: evidence.append("風控+執行共振")

    leader_exception = bool(
        (strength or 0) >= float(d["leader_strength_pct_min"])
        and (sector_pct or 0) >= float(d["leader_sector_pct_min"])
        and (h89s or 0) >= float(d["leader_selection_min"])
        and (h89e or 0) >= float(d["leader_execution_min"])
        and (risk or 0) >= float(d["leader_risk_min"])
    )

    reasons=[]; excludes=[]
    if sector_good:
        score += float(cfg["bonuses"]["sector_good"]); reasons.append("族群環境正向")
    elif sector_bad:
        if leader_exception:
            score += float(cfg["bonuses"]["leader_exception"]); reasons.append("弱族群中的明確領先例外")
        else:
            score -= float(cfg["penalties"]["sector_bad"]); excludes.append("族群退潮且未達領先例外")

    if h82 is not None:
        if h82 <= -0.70:
            score -= float(cfg["penalties"]["h82_strong_negative"]); excludes.append(f"H82成熟學習{h82:+.2f}")
        elif h82 <= -0.30:
            score -= float(cfg["penalties"]["h82_mild_negative"]); excludes.append(f"H82成熟學習{h82:+.2f}")
    if h94_exhaust in {"HIGH","BLOCK"} or "過熱" in _first_text(raw,["H81三大風險"]):
        score -= float(cfg["penalties"]["overheat"]); excludes.append("延伸/耗竭，不追價")
    if (h89s or 0) < 55:
        score -= float(cfg["penalties"]["selection_below_55"]); excludes.append("Selection強度不足")
    if (strength or 0) < 50:
        score -= float(cfg["penalties"]["strength_below_50"]); excludes.append("個股橫截面強度不足")
    if risk is not None and risk >= 65 and h89e is not None and h89e >= 65:
        score += float(cfg["bonuses"]["risk_execution_alignment"]); reasons.append("風控與執行品質一致")
    if formal_plan == "是":
        score += float(cfg["bonuses"]["formal_price_plan"]); reasons.append("價格計畫結構可用")
    news_state = _first_text(raw,["H94新聞證據狀態"])
    if "MISSING" in news_state:
        score -= float(cfg["penalties"]["news_missing"]); excludes.append("新聞證據缺口")

    score = _clip(score)

    standard_core = bool(
        score >= float(d["core_score_min"])
        and (strength or 0) >= float(d["core_strength_pct_min"])
        and (sector_pct or 0) >= float(d["core_sector_pct_min"])
        and (h89s or 0) >= float(d["core_selection_min"])
        and (h89e or 0) >= float(d["core_execution_min"])
        and (risk or 0) >= float(d["core_risk_min"])
        and (not sector_bad or not bool(d.get("allow_bad_sector_only_for_leader", True)))
    )
    leader_core = bool(
        leader_exception and score >= float(d["leader_score_min"])
    )
    eligible = standard_core or leader_core

    extended = h94_exhaust in {"HIGH","BLOCK"} or formal_plan != "是"
    if eligible and leader_core and extended:
        level = "LEADER｜領先股研究・不追價"
        execution = "LEADER-NO-CHASE｜只研究領先，不把延伸股當買點"
    elif eligible and leader_core:
        level = "LEADER｜逆勢領先核心"
        execution = "WAIT-ENTRY｜依H89價格計畫，不追價"
    elif eligible:
        level = "CORE｜核心研究"
        execution = "WAIT-ENTRY｜依H89價格計畫"
    else:
        level = "WATCH｜非核心候選"
        execution = "WATCH"

    ref_value = "HIGH" if eligible and score >= 70 else "MEDIUM-HIGH" if eligible else "LOW"
    alignment = f"{regime}｜{'領先例外' if leader_exception else '一般'}"
    reason = "；".join(reasons) or "未形成高信心升級證據"
    excluded = "；".join(dict.fromkeys(excludes)) or "無重大排除因素"
    summary = (
        f"核心機會{score:.1f}｜{level}｜領先證據{len(evidence)}｜{alignment}｜{execution}；"
        "H96只決定02是否值得參考，不建立Formal。"
    )
    return {
        "H96版本": VERSION,
        "H96核心機會分": round(score,2),
        "H96核心研究等級": level,
        "H96參考價值": ref_value,
        "H96核心資格": "CORE" if eligible else "WATCH",
        "H96領先證據數": len(evidence),
        "H96領先證據": "；".join(evidence) or "不足",
        "H96領先例外": "是" if leader_exception else "否",
        "H96市場族群一致性": alignment,
        "H96執行狀態": execution,
        "H96研究池來源": source,
        "H96不追價": "是" if extended else "否",
        "H96核心理由": reason,
        "H96排除理由": excluded,
        "H96Formal權限": "LOCKED｜仍須H64/H68＋新鮮度＋流動性＋成本後RR＋停損治理。",
        "H96決策摘要": summary,
    }


def apply_overlay(frame: pd.DataFrame | None, *, sector_df: pd.DataFrame | None=None, settings: dict[str, Any] | None=None, source: str="audit") -> pd.DataFrame:
    if frame is None:
        return pd.DataFrame()
    if not isinstance(frame,pd.DataFrame):
        frame=pd.DataFrame(frame)
    out=frame.copy(deep=True)
    if out.empty:
        for c in H96_COLUMNS:
            if c not in out.columns: out[c]=pd.Series(dtype="object")
        return out
    lookup=_sector_lookup(sector_df); cfg=_settings(settings)
    add=pd.DataFrame([analyze_candidate(r,sector_lookup=lookup,settings=cfg,source=source) for r in out.to_dict("records")],index=out.index)
    for c in H96_COLUMNS:
        out[c]=add[c] if c in add.columns else None
    front=[c for c in MANAGER_FRONT if c in out.columns]
    rest=[c for c in out.columns if c not in front]
    return out.loc[:,front+rest].copy()


def _code(v: Any) -> str:
    s=_text(v)
    return s[:-2] if s.endswith(".0") else s


def _dedupe(frames: list[pd.DataFrame]) -> pd.DataFrame:
    valid=[]
    for f in frames:
        if isinstance(f,pd.DataFrame) and not f.empty:
            valid.append(f.copy())
    if not valid:
        return pd.DataFrame()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        x=pd.concat(valid,ignore_index=True,sort=False)
    if "股票代號" not in x.columns:
        return x
    x["__h96_code"]=x["股票代號"].map(_code)
    x=x.loc[x["__h96_code"].ne("")]
    # Earlier frames have priority.  Audit is passed first so its latest broad-pool truth wins.
    x=x.loc[~x["__h96_code"].duplicated(keep="first")].drop(columns=["__h96_code"])
    return x.reset_index(drop=True)


def apply_h96_decision_tables(tables: dict[str, Any] | None, *, sector_df: pd.DataFrame | None=None, candidate_df: pd.DataFrame | None=None, settings: dict[str, Any] | None=None) -> dict[str,pd.DataFrame]:
    tables=tables if isinstance(tables,dict) else {}
    cfg=_settings(settings)
    out={k:(v.copy() if isinstance(v,pd.DataFrame) else pd.DataFrame(v) if v is not None else pd.DataFrame()) for k,v in tables.items()}

    # Decorate the broad audit pool first.  This is the critical H96 fix: we do not
    # restrict discovery to yesterday's Research+Waiting survivors.
    audit=apply_overlay(out.get("audit",pd.DataFrame()),sector_df=sector_df,settings=cfg,source="AUDIT-120")
    old_r=apply_overlay(out.get("research",pd.DataFrame()),sector_df=sector_df,settings=cfg,source="H94-RESEARCH")
    old_w=apply_overlay(out.get("waiting",pd.DataFrame()),sector_df=sector_df,settings=cfg,source="H94-WAITING")
    out["audit"]=audit

    limit=int(cfg["discovery"]["audit_pool_limit"])
    pool=_dedupe([audit.head(limit),old_r,old_w])
    if not pool.empty:
        pool["H96核心機會分"]=pd.to_numeric(pool.get("H96核心機會分"),errors="coerce")
        pool=pool.sort_values(["H96核心機會分","H94Alpha品質分"],ascending=False,na_position="last").reset_index(drop=True)

    max_rows=int(cfg["discovery"]["max_core_rows"])
    max_cat=int(cfg["discovery"]["max_per_category"])
    max_theme=int(cfg["discovery"]["max_per_broad_theme"])
    selected=[]; cat_count={}; theme_count={}; concentration_blocked=0
    for i,row in pool.iterrows() if not pool.empty else []:
        if _text(row.get("H96核心資格"))!="CORE":
            continue
        cat=_text(row.get("類別")) or "未分類"; theme=_broad_theme(cat)
        if cat_count.get(cat,0)>=max_cat or theme_count.get(theme,0)>=max_theme:
            pool.at[i,"H96排除理由"]=( _text(row.get("H96排除理由"))+"；核心研究集中度上限" ).strip("；")
            pool.at[i,"H96核心資格"]="WATCH"; concentration_blocked+=1; continue
        selected.append(i); cat_count[cat]=cat_count.get(cat,0)+1; theme_count[theme]=theme_count.get(theme,0)+1
        if len(selected)>=max_rows:
            break

    research=pool.loc[selected].copy().reset_index(drop=True) if selected else pool.iloc[0:0].copy()
    selected_codes=set(research["股票代號"].map(_code)) if not research.empty and "股票代號" in research.columns else set()

    # Waiting remains readable: only original Research/Waiting plus demoted old research.
    waiting_base=_dedupe([old_r,old_w])
    if not waiting_base.empty and "股票代號" in waiting_base.columns:
        waiting_base=waiting_base.loc[~waiting_base["股票代號"].map(_code).isin(selected_codes)].copy()
        waiting_base["H96核心機會分"]=pd.to_numeric(waiting_base.get("H96核心機會分"),errors="coerce")
        waiting_base=waiting_base.sort_values("H96核心機會分",ascending=False,na_position="last").reset_index(drop=True)

    # Count origins for diagnostics.
    old_research_codes=set(old_r["股票代號"].map(_code)) if not old_r.empty and "股票代號" in old_r.columns else set()
    audit_promoted=sum(1 for c in selected_codes if c not in old_research_codes)
    old_research_demoted=sum(1 for c in old_research_codes if c not in selected_codes)

    out["research"]=research
    out["waiting"]=waiting_base

    # H96 export clarity: a one-row conclusion/status placeholder is not a Formal
    # stock.  Keep the conclusion, but do not decorate it into a fake 100-column
    # actionable record or count it as an executable recommendation.
    formal_count = 0
    if "actionable" in out and isinstance(out["actionable"],pd.DataFrame) and not out["actionable"].empty:
        _act = out["actionable"].copy()
        if "股票代號" in _act.columns:
            _mask = _act["股票代號"].map(_code).ne("")
            formal_count = int(_mask.sum())
            if formal_count > 0:
                out["actionable"] = apply_overlay(_act.loc[_mask].copy(),sector_df=sector_df,settings=cfg,source="FORMAL")
            else:
                conclusion = _first_text(_act.iloc[0].to_dict(), ["結論","狀態","說明"]) or "本輪沒有正式可執行股票；研究股不得冒充買進。"
                out["actionable"] = pd.DataFrame({"結論":[conclusion]})
        else:
            conclusion = _first_text(_act.iloc[0].to_dict(), ["結論","狀態","說明"]) or "本輪沒有正式可執行股票；研究股不得冒充買進。"
            out["actionable"] = pd.DataFrame({"結論":[conclusion]})
    if "emerging_watch" in out and isinstance(out["emerging_watch"],pd.DataFrame) and not out["emerging_watch"].empty:
        out["emerging_watch"]=apply_overlay(out["emerging_watch"],sector_df=sector_df,settings=cfg,source="EMERGING")

    health=out.get("health",pd.DataFrame()).copy()
    if not health.empty and "項目" in health.columns:
        health=health.loc[~health["項目"].astype(str).str.startswith("H96")].copy()
    rows=[
        {"項目":"H96版本","數值":VERSION},
        {"項目":"H96主管閱讀順序","數值":"01 Formal有資料先看01；Formal=0時，02只保留High-Conviction核心研究；04看市場/族群；05看完整證據。"},
        {"項目":"H96正式可執行檔數","數值":int(formal_count)},
        {"項目":"H96核心研究輸出列","數值":int(len(research))},
        {"項目":"H96Audit廣域發現池","數值":int(min(len(audit),limit))},
        {"項目":"H96由Audit新發現升級","數值":int(audit_promoted)},
        {"項目":"H96舊Research降級","數值":int(old_research_demoted)},
        {"項目":"H96集中度擋下","數值":int(concentration_blocked)},
        {"項目":"H96Research語意","數值":"02=真正值得參考的核心研究；C/條件式/僅安全但不領先者移至03 Waiting。"},
        {"項目":"H96Formal權限","數值":"LOCKED"},
    ]
    out["health"]=pd.concat([health,pd.DataFrame(rows)],ignore_index=True,sort=False)
    return out


def export_contract_summary(tables: dict[str,pd.DataFrame] | None) -> dict[str,Any]:
    tables=tables if isinstance(tables,dict) else {}
    result={"version":VERSION,"ok":True,"sheets":{}}
    for name in ["actionable","research","waiting","audit","health","emerging_watch"]:
        df=tables.get(name,pd.DataFrame()); ok=isinstance(df,pd.DataFrame)
        cols=[c for c in H96_COLUMNS if ok and c in df.columns]
        if name in {"research","waiting","audit"} and ok and not df.empty and not cols:
            result["ok"]=False
        result["sheets"][name]={"rows":int(len(df)) if ok else 0,"h96_columns":len(cols),"ok":ok}
    return result
