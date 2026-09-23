# -*- coding: utf-8 -*-
"""V191-H93 Seven-Direction GodPick Evolution Engine.

User-defined evolution directions
---------------------------------
1. 深入研究      Deep research / pricing / technical / news / evidence synthesis
2. 學習計畫      Mature-sample learning plan and attribution readiness
3. 顧問模式      Conditional, explainable advisor mode (not a buy command)
4. 優化系統方案  Bottleneck detection and system improvement plan
5. 成長機會      Growth/catalyst/sector opportunity evidence
6. 持續優化      Closed-loop monitoring / post-trade feedback plan
7. 提高效率      Data readiness, cache reuse and bounded-computation readiness

Governance
----------
* H93 NEVER creates Formal/A-/R1 authority.
* H93 NEVER moves stops or fabricates a price target.
* H93 can only apply a bounded research-order adjustment.
* Unknown/missing evidence stays neutral; it is never guessed.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
import json
import math

import pandas as pd

VERSION = "v191_h93_seven_direction_godpick_evolution_20260923"
STATE_FILE = "godpick_h93_evolution_state.json"
STATE_DOC = "godpick_h93_evolution_state"
RECORDS_FILE = "godpick_records.json"
BASE_DIR = Path(__file__).resolve().parent

H93_COLUMNS = [
    "H93版本", "H93七維總分", "H93研究優先級", "H93研究排序加減分", "H93資料覆蓋%",
    "H93深入研究分", "H93深入研究摘要", "H93市場已反映", "H93可能未反映",
    "H93學習計畫分", "H93學習成熟度", "H93學習計畫",
    "H93顧問模式分", "H93顧問模式", "H93顧問結論", "H93成立條件", "H93失效條件",
    "H93系統優化分", "H93系統瓶頸", "H93系統優化方案",
    "H93成長機會分", "H93三大成長催化", "H93三大風險", "H93下一關鍵發展",
    "H93持續優化分", "H93持續優化計畫", "H93回測/檢討口徑",
    "H93效率分", "H93效率狀態", "H93效率建議",
    "H93Formal權限", "H93七維決策摘要",
]


MANAGER_FRONT_COLUMNS = [
    "股票代號","股票名稱","市場別","類別","正式推薦分區","V188交易許可",
    "H93研究優先級","H93七維總分","H93顧問模式","H93顧問結論",
    "H89交易型態","H89主進場","H89防守停損","H89第一目標","H89第二目標","H89成本後RR1","H89Formal價格計畫合格",
    "H93成長機會分","H93三大成長催化","H93三大風險","H93下一關鍵發展",
    "H93深入研究分","H93市場已反映","H93可能未反映",
    "H93學習成熟度","H93學習計畫","H93系統瓶頸","H93系統優化方案",
    "H93持續優化計畫","H93回測/檢討口徑","H93效率狀態","H93效率建議",
    "H93Formal權限","H93七維決策摘要",
]

def _reorder_manager_columns(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or not isinstance(frame,pd.DataFrame) or frame.empty:
        return pd.DataFrame() if frame is None else frame
    front=[c for c in MANAGER_FRONT_COLUMNS if c in frame.columns]
    rest=[c for c in frame.columns if c not in front]
    return frame.loc[:,front+rest].copy()


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


def _clip(v: float | None, lo: float = 0.0, hi: float = 100.0, default: float = 50.0) -> float:
    x = default if v is None else float(v)
    return max(lo, min(hi, x))


def _first_num(row: dict[str, Any], names: Iterable[str]) -> float | None:
    for name in names:
        x = _num(row.get(name), None)
        if x is not None:
            return x
    return None


def _first_text(row: dict[str, Any], names: Iterable[str]) -> str:
    for name in names:
        s = _text(row.get(name))
        if s:
            return s
    return ""


def _uniq(parts: Iterable[str], n: int = 3) -> list[str]:
    out: list[str] = []
    for raw in parts:
        s = _text(raw)
        if s and s not in out:
            out.append(s)
        if len(out) >= n:
            break
    return out


def _settings(settings: dict[str, Any] | None = None) -> dict[str, Any]:
    if isinstance(settings, dict):
        try:
            from godpick_h93_evolution_settings import normalize_settings
            return normalize_settings(settings)
        except Exception:
            return settings
    from godpick_h93_evolution_settings import load_settings_safe
    return load_settings_safe()


def _safe_mean(values: Iterable[tuple[float | None, float]]) -> tuple[float, float]:
    total = 0.0; weight = 0.0; used = 0.0; possible = 0.0
    for value, w in values:
        possible += max(0.0, w)
        if value is None:
            continue
        ww = max(0.0, w)
        total += _clip(value) * ww
        weight += ww
        used += ww
    return ((total / weight) if weight else 50.0, (used / possible) if possible else 0.0)


def _growth_score(row: dict[str, Any]) -> tuple[float, list[str], list[str], list[str]]:
    revenue = _first_num(row, ["月營收YoY%","累計營收YoY%","營收成長率%"])
    eps = _first_num(row, ["EPS成長率%","EPS成長%","EPS成長分數","基本面成長分數"])
    sector = _first_num(row, ["H53族群共振分","H74主流資金分","族群資金流分數","類股熱度分數"])
    strength = _first_num(row, ["H47個股相對強度分","H72風險調整分","強勢動能分"])
    h81 = _first_num(row, ["H81市場定價理解分","H81專業研究總分"])
    inst = _first_num(row, ["法人籌碼官方分數","法人籌碼分數","大戶鎖碼分數"])

    def growth(v: float | None) -> float | None:
        if v is None: return None
        if 0 <= v <= 100 and abs(v) > 40: return _clip(v)
        if v >= 30: return 92
        if v >= 15: return 80
        if v >= 5: return 68
        if v >= -5: return 52
        if v >= -15: return 38
        return 22

    score, _ = _safe_mean([
        (growth(revenue), .22), (growth(eps), .18), (sector, .18),
        (strength, .16), (h81, .16), (inst, .10),
    ])
    catalysts: list[str] = []
    risks: list[str] = []
    watch: list[str] = []
    h81_cat = _first_text(row, ["H81三大利多催化"])
    h81_risk = _first_text(row, ["H81三大風險"])
    h81_watch = _first_text(row, ["H81下一步關注"])
    if h81_cat: catalysts.extend([x for x in h81_cat.split("；") if x])
    if h81_risk: risks.extend([x for x in h81_risk.split("；") if x])
    if h81_watch: watch.extend([x for x in h81_watch.split("；") if x])
    if revenue is not None:
        (catalysts if revenue >= 10 else risks if revenue < -5 else watch).append(f"營收YoY {revenue:.1f}%")
    if sector is not None:
        (catalysts if sector >= 70 else risks if sector < 40 else watch).append(f"族群共振 {sector:.0f}")
    if strength is not None:
        (catalysts if strength >= 70 else risks if strength < 40 else watch).append(f"相對強度 {strength:.0f}")
    return _clip(score), _uniq(catalysts,3), _uniq(risks,3), _uniq(watch,3)


def _deep_research(row: dict[str, Any], growth_score: float) -> tuple[float, float, str, str, str]:
    h81_total = _first_num(row, ["H81專業研究總分"])
    h81_cov = _first_num(row, ["H81資料覆蓋%"])
    h89_sel = _first_num(row, ["H89選股方向分"])
    h72 = _first_num(row, ["H72風險調整分"])
    official = _first_num(row, ["官方因子總分","官方有效因子分"])
    tech = _first_num(row, ["H81技術多週期分","技術結構分數","H47個股相對強度分"])
    news = _first_num(row, ["H81新聞事件影響分"])
    score, coverage = _safe_mean([
        (h81_total,.28),(h89_sel,.18),(h72,.14),(official,.12),
        (tech,.12),(news,.08),(growth_score,.08),
    ])
    if h81_cov is not None:
        coverage = min(1.0, coverage * .65 + _clip(h81_cov)/100*.35)

    priced = []
    gap = []
    ret5 = _first_num(row,["近5日漲幅%","5日漲幅%"])
    pricing = _first_num(row,["H81市場定價理解分"])
    if ret5 is not None and ret5 >= 12:
        priced.append(f"近5日已上漲{ret5:.1f}%，部分題材可能已反映")
    if pricing is not None and pricing >= 70 and (ret5 is None or ret5 < 8):
        gap.append("基本面/籌碼研究偏正向，但短線價格未明顯過熱")
    if growth_score >= 72:
        gap.append("成長與催化證據仍具研究價值")
    if not priced:
        priced.append("未發現足夠證據判定市場已完全反映；維持條件式判讀")
    if not gap:
        gap.append("尚未形成高信心Expectation Gap，等待新的可驗證催化")
    summary = f"深入研究分{score:.1f}｜覆蓋{coverage*100:.0f}%｜以H81市場定價/技術/新聞、H89選股品質與官方因子交叉驗證。"
    return _clip(score), coverage, summary, "；".join(_uniq(priced,2)), "；".join(_uniq(gap,2))


def _learning_plan(row: dict[str, Any], cfg: dict[str, Any], state: dict[str, Any]) -> tuple[float, str, str]:
    mature = _first_num(row,["H82成熟樣本數","績效樣本數","H80績效樣本數"])
    confidence = _first_num(row,["H82學習信心%"])
    clean = _first_num(row,["H89學習乾淨樣本"])
    h82_status = _first_text(row,["H82學習狀態"])
    global_mature = _num(state.get("mature_samples"),0.0) or 0.0
    n = max([x for x in [mature, clean, global_mature] if x is not None] or [0.0])
    min_n = float(cfg["learning"].get("minimum_mature_samples",12))
    high_n = float(cfg["learning"].get("high_confidence_samples",40))
    n_score = 30 + min(50, max(0.0, (n-min_n)/max(high_n-min_n,1)*50)) if n >= min_n else 20 + n/max(min_n,1)*20
    conf_score = _clip(confidence, default=50)
    status_penalty = 18 if any(x in h82_status for x in ["凍結","FROZEN","不足"]) else 0
    score = _clip(n_score*.55 + conf_score*.45 - status_penalty)
    maturity = "成熟" if n >= high_n and conf_score >= 65 else "成長中" if n >= min_n else "暖機"
    plan = (
        f"{maturity}｜成熟/乾淨樣本約{int(n)}；"
        "分開追蹤Selection與Execution，T+1/T+5/T+10成熟後才調整研究排序；"
        "不得以未成熟/代理樣本放寬Formal。"
    )
    return score, maturity, plan


def _advisor(row: dict[str, Any]) -> tuple[float, str, str, str, str]:
    formal = _first_text(row,["是否正式推薦","V188正式推薦資格","正式推薦分區"])
    h89_exec = _first_num(row,["H89執行品質分"])
    h89_sel = _first_num(row,["H89選股方向分"])
    rr = _first_num(row,["H89成本後RR1","H79成本後RR","風險報酬比"])
    risk = _first_num(row,["Risk風控安全分","H81風險管理分"])
    plan_ok = _first_text(row,["H89Formal價格計畫合格","H79計畫狀態"])
    score, _ = _safe_mean([(h89_exec,.35),(h89_sel,.25),(risk,.20),(_clip((rr or 0)*35) if rr is not None else None,.20)])
    if any(x in formal for x in ["是","正式下週主推薦","A-"]) and ("是" in plan_ok or "PASS" in plan_ok):
        mode = "執行顧問"
        conclusion = "研究與交易結構同時具備；仍須依實際價格、停損與倉位紀律執行。"
    elif (h89_sel or 0) >= 70 and (h89_exec or 0) < 65:
        mode = "等待買點顧問"
        conclusion = "方向值得研究，但目前Entry/Stop/Target/RR尚未形成高品質執行條件。"
    elif _first_num(row,["H93成長機會分"]) and (_first_num(row,["H93成長機會分"]) or 0) >= 70:
        mode = "成長研究顧問"
        conclusion = "成長/催化有研究價值；先等技術與交易條件確認，不把題材直接視為買進。"
    else:
        mode = "條件式研究顧問"
        conclusion = "目前以條件式研究為主；只有證據與價格結構同時改善才提高行動層級。"
    成立 = _first_text(row,["H81多頭情境","盤中確認條件","等待條件"])
    失效 = _first_text(row,["失效條件","H81空頭情境","轉弱條件"])
    if not 成立: 成立 = "主流/基本面/技術證據持續改善，且價格結構與成本後RR同時成立。"
    if not 失效: 失效 = "關鍵支撐失守、量價轉弱、負面事件擴大或RR惡化時降級。"
    return score, mode, conclusion, 成立, 失效


def _system_optimization(row: dict[str, Any]) -> tuple[float, str, str]:
    bottlenecks: list[str] = []
    fixes: list[str] = []
    data_status = _first_text(row,["股神資料總新鮮度","官方因子資料狀態","H83正式資料可用"])
    coverage = _first_num(row,["H81資料覆蓋%","資料完整度","官方資料完整度"])
    h82 = _first_text(row,["H82學習狀態"])
    h89 = "｜".join([_text(row.get("H89價格計畫來源")), _text(row.get("H89Formal價格計畫合格"))])
    if any(x in data_status for x in ["NOT","失敗","過期","STALE","否"]):
        bottlenecks.append("資料新鮮度/官方因子")
        fixes.append("先修資料權威與業務日期，不用降低推薦門檻")
    if coverage is not None and coverage < 60:
        bottlenecks.append(f"資料覆蓋偏低({coverage:.0f}%)")
        fixes.append("補足基本面/新聞/技術證據後再提高研究信心")
    if any(x in h82 for x in ["凍結","不足","FROZEN"]):
        bottlenecks.append("成熟學習樣本不足/治理凍結")
        fixes.append("累積乾淨成熟樣本並排除代理績效")
    if "否" in h89 or "缺" in h89:
        bottlenecks.append("交易價格結構")
        fixes.append("改善Entry/Stop/Target真實結構，不以合成目標升格Formal")
    if not bottlenecks:
        bottlenecks.append("無重大硬故障；目前瓶頸轉為排序與執行品質")
        fixes.append("維持門檻，持續用成熟績效做小幅有界校正")
    score = _clip(90 - max(0,len(bottlenecks)-1)*14 - (20 if coverage is not None and coverage < 45 else 0))
    return score, "；".join(_uniq(bottlenecks,3)), "；".join(_uniq(fixes,3))


def _continuous_improvement(row: dict[str, Any], learning_score: float) -> tuple[float, str, str]:
    opp = _first_num(row,["H89錯失機會率%"])
    chase = _first_num(row,["H89追價失敗率%"])
    journal = _first_num(row,["H81交易日誌回饋分"])
    bt = _first_num(row,["H81歷史回測可信分"])
    score, _ = _safe_mean([(learning_score,.35),(journal,.25),(bt,.25),(_clip(100-(opp or 0)-(chase or 0)*.5) if opp is not None or chase is not None else None,.15)])
    plan = "每日保存決策快照；T+1/T+5/T+10分離檢討Selection、Entry、Stop、Target與市場Regime；只用成熟樣本更新。"
    metric = "勝率＋平均獲利/虧損＋Profit Factor＋最大回撤＋Selection Alpha＋MFE/MAE＋Opportunity Cost，禁止只看勝率。"
    return score, plan, metric


def _efficiency(row: dict[str, Any], coverage: float) -> tuple[float, str, str]:
    freshness = _first_text(row,["股神資料總新鮮度","H83正式資料可用"])
    missing_penalty = 0
    for key in ["股票代號","最新價","H81專業研究總分","H89選股方向分","H89執行品質分"]:
        if _text(row.get(key)) == "": missing_penalty += 8
    score = _clip(55 + coverage*35 - missing_penalty + (10 if any(x in freshness for x in ["READY","可用","PASS"]) else 0))
    if score >= 80:
        status = "FAST-READY"
        advice = "主要證據可由已保存快照直接重用；避免重跑全市場與重複外部抓取。"
    elif score >= 60:
        status = "BOUNDED"
        advice = "使用本機快照＋有限補算；只更新缺漏/過期資料。"
    else:
        status = "REPAIR-FIRST"
        advice = "先修缺漏資料與權威日期，避免用更多模型重算掩蓋資料問題。"
    return score, status, advice


def load_state_safe() -> dict[str, Any]:
    path = BASE_DIR / STATE_FILE
    try:
        if path.exists():
            raw = json.loads(path.read_text(encoding="utf-8-sig"))
            return raw if isinstance(raw, dict) else {}
    except Exception:
        pass
    return {}


def refresh_evolution_state(records: list[dict[str, Any]] | None = None, *, persist: bool = True) -> dict[str, Any]:
    cfg = _settings()
    rows = records
    if rows is None:
        try:
            raw = json.loads((BASE_DIR / RECORDS_FILE).read_text(encoding="utf-8-sig"))
            rows = raw if isinstance(raw, list) else []
        except Exception:
            rows = []
    max_rows = int(cfg["learning"].get("max_history_rows",2500))
    rows = [r for r in (rows or []) if isinstance(r,dict)][-max_rows:]
    mature = 0; wins = 0; missed = 0; chase_fail = 0; sel_err = 0; exec_err = 0
    for r in rows:
        age = _first_num(r,["持有天數","績效成熟天數","H82成熟天數"])
        r5 = _first_num(r,["推薦後5日%","實際報酬%","實際報酬"])
        if (age is not None and age >= 5) or r5 is not None:
            mature += 1
            if r5 is not None and r5 > 0: wins += 1
        label = _first_text(r,["H89錯誤歸因","績效錯誤歸因","檢討標籤"])
        if "Entry Too Conservative" in label or "錯失" in label: missed += 1
        if "Chase" in label or "追價" in label: chase_fail += 1
        if "Selection" in label and any(x in label for x in ["Wrong","失敗"]): sel_err += 1
        if "Execution" in label or any(x in label for x in ["Entry","Stop","Target"]): exec_err += 1
    state = {
        "version": VERSION,
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "history_rows": len(rows),
        "mature_samples": mature,
        "mature_win_rate_pct": round(wins/mature*100,2) if mature else None,
        "missed_opportunity_count": missed,
        "chase_failure_count": chase_fail,
        "selection_error_count": sel_err,
        "execution_error_count": exec_err,
        "learning_status": "MATURE" if mature >= cfg["learning"]["high_confidence_samples"] else "GROWING" if mature >= cfg["learning"]["minimum_mature_samples"] else "WARMUP",
    }
    if persist:
        try:
            from godpick_durability_service import persist_json_async
            persist_json_async(STATE_FILE,state,firestore_doc=STATE_DOC,reason="H93 seven-direction evolution state")
        except Exception:
            try:
                tmp=(BASE_DIR/STATE_FILE).with_suffix(".json.tmp_h93")
                tmp.write_text(json.dumps(state,ensure_ascii=False,indent=2),encoding="utf-8")
                tmp.replace(BASE_DIR/STATE_FILE)
            except Exception:
                pass
    return state


def analyze_candidate(row: dict[str, Any] | pd.Series, settings: dict[str, Any] | None = None, state: dict[str, Any] | None = None) -> dict[str, Any]:
    raw = row.to_dict() if isinstance(row,pd.Series) else dict(row or {})
    cfg = _settings(settings)
    state = state if isinstance(state,dict) else load_state_safe()
    growth, catalysts, risks, watch = _growth_score(raw)
    deep, coverage, deep_summary, priced, unpriced = _deep_research(raw,growth)
    learning, maturity, learning_plan = _learning_plan(raw,cfg,state)

    # Seed growth before advisor mode can refer to it.
    raw["H93成長機會分"] = growth
    advisor, advisor_mode, advisor_conclusion,成立,失效 = _advisor(raw)
    system, bottleneck, optimize = _system_optimization(raw)
    cont, cont_plan, bt_metric = _continuous_improvement(raw,learning)
    efficiency, efficiency_status, efficiency_advice = _efficiency(raw,coverage)

    weights = cfg.get("weights",{})
    dims = {
        "deep_research": deep,
        "learning_plan": learning,
        "advisor_mode": advisor,
        "system_optimization": system,
        "growth_opportunity": growth,
        "continuous_improvement": cont,
        "efficiency": efficiency,
    }
    total_w = sum(float(weights.get(k,0) or 0) for k in dims) or 100.0
    total = sum(dims[k]*float(weights.get(k,0) or 0) for k in dims)/total_w
    thresholds = cfg["research_overlay"].get("priority_thresholds",{})
    priority = "A｜優先研究" if total >= float(thresholds.get("A",75)) else "B｜重點追蹤" if total >= float(thresholds.get("B",65)) else "C｜等待催化" if total >= float(thresholds.get("C",55)) else "D｜低優先觀察"
    max_pts = float(cfg["research_overlay"].get("max_abs_points",4.0))
    if not bool(cfg.get("enabled",True)) or not bool(cfg["research_overlay"].get("enabled",True)) or coverage*100 < float(cfg["research_overlay"].get("min_data_coverage_pct",45)):
        adj = 0.0
    else:
        adj = max(-max_pts,min(max_pts,(total-50)/50*max_pts))

    next_dev = "；".join(_uniq(watch,3)) or "等待下一個可驗證的財報、營收、法說、訂單、價格突破或產業催化。"
    formal_lock = "LOCKED｜H93不得建立Formal；仍須H64/H68＋新鮮度＋流動性＋成本後RR＋停損治理。"
    summary = f"七維{total:.1f}｜{priority}｜{advisor_mode}｜研究排序{adj:+.2f}；只影響研究優先，不改Formal權限。"
    return {
        "H93版本":VERSION,
        "H93七維總分":round(_clip(total),2),
        "H93研究優先級":priority,
        "H93研究排序加減分":round(adj,2),
        "H93資料覆蓋%":round(coverage*100,2),
        "H93深入研究分":round(deep,2), "H93深入研究摘要":deep_summary,
        "H93市場已反映":priced, "H93可能未反映":unpriced,
        "H93學習計畫分":round(learning,2), "H93學習成熟度":maturity, "H93學習計畫":learning_plan,
        "H93顧問模式分":round(advisor,2), "H93顧問模式":advisor_mode, "H93顧問結論":advisor_conclusion,
        "H93成立條件":成立, "H93失效條件":失效,
        "H93系統優化分":round(system,2), "H93系統瓶頸":bottleneck, "H93系統優化方案":optimize,
        "H93成長機會分":round(growth,2), "H93三大成長催化":"；".join(catalysts) or "目前沒有足夠證據形成額外催化假設",
        "H93三大風險":"；".join(risks) or "目前沒有足夠證據形成額外風險假設",
        "H93下一關鍵發展":next_dev,
        "H93持續優化分":round(cont,2), "H93持續優化計畫":cont_plan, "H93回測/檢討口徑":bt_metric,
        "H93效率分":round(efficiency,2), "H93效率狀態":efficiency_status, "H93效率建議":efficiency_advice,
        "H93Formal權限":formal_lock,
        "H93七維決策摘要":summary,
    }


def apply_seven_direction_overlay(frame: pd.DataFrame | None, settings: dict[str, Any] | None = None, state: dict[str, Any] | None = None) -> pd.DataFrame:
    if frame is None:
        return pd.DataFrame()
    if not isinstance(frame,pd.DataFrame):
        frame = pd.DataFrame(frame)
    out = frame.copy(deep=True)
    if out.empty:
        for col in H93_COLUMNS:
            if col not in out.columns: out[col]=pd.Series(dtype="object")
        return out
    cfg = _settings(settings)
    state = state if isinstance(state,dict) else load_state_safe()
    rows = [analyze_candidate(row,cfg,state) for row in out.to_dict("records")]
    overlay = pd.DataFrame(rows,index=out.index)
    for col in H93_COLUMNS:
        out[col] = overlay[col] if col in overlay.columns else None
    return out


def _sort_research(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or not isinstance(frame,pd.DataFrame) or frame.empty:
        return pd.DataFrame() if frame is None else frame
    work = frame.copy()
    keys=[]; asc=[]
    for col in ["H93七維總分","H89雙軌研究排序分","H82自適應研究排序分","H81專業研究總分","H79研究推薦分"]:
        if col in work.columns:
            work[col]=pd.to_numeric(work[col],errors="coerce")
            keys.append(col); asc.append(False)
    if keys:
        work=work.sort_values(keys,ascending=asc,na_position="last")
    return work.reset_index(drop=True)


def build_health_rows(candidate_df: pd.DataFrame | None = None, scan_report: dict[str,Any] | None = None, state: dict[str,Any] | None = None) -> pd.DataFrame:
    state = state if isinstance(state,dict) else load_state_safe()
    df = candidate_df if isinstance(candidate_df,pd.DataFrame) else pd.DataFrame()
    rows = [
        {"項目":"H93版本","數值":VERSION},
        {"項目":"H93七維方向","數值":"深入研究｜學習計畫｜顧問模式｜優化系統方案｜成長機會｜持續優化｜提高效率"},
        {"項目":"H93Formal治理","數值":"LOCKED｜H93只調研究順位，不建立Formal"},
        {"項目":"H93候選列數","數值":int(len(df))},
        {"項目":"H93成熟學習樣本","數值":int(_num(state.get('mature_samples'),0) or 0)},
        {"項目":"H93學習狀態","數值":_text(state.get('learning_status')) or 'WARMUP'},
        {"項目":"H93歷史成熟勝率","數值":state.get('mature_win_rate_pct')},
        {"項目":"H93錯失機會計數","數值":int(_num(state.get('missed_opportunity_count'),0) or 0)},
        {"項目":"H93追價失敗計數","數值":int(_num(state.get('chase_failure_count'),0) or 0)},
    ]
    if not df.empty and "H93七維總分" in df.columns:
        vals=pd.to_numeric(df["H93七維總分"],errors="coerce")
        rows.extend([
            {"項目":"H93平均七維分","數值":round(float(vals.mean()),2) if vals.notna().any() else None},
            {"項目":"H93 A級研究數","數值":int(df.get("H93研究優先級",pd.Series('',index=df.index)).astype(str).str.startswith('A').sum())},
            {"項目":"H93 B級研究數","數值":int(df.get("H93研究優先級",pd.Series('',index=df.index)).astype(str).str.startswith('B').sum())},
        ])
    if isinstance(scan_report,dict):
        rows.append({"項目":"H93掃描品質","數值":_text(scan_report.get("掃描品質狀態") or scan_report.get("掃描品質等級"))})
        rows.append({"項目":"H93正式資料可用","數值":scan_report.get("正式推薦可用")})
    return pd.DataFrame(rows)


def decorate_decision_tables(tables: dict[str,Any] | None, candidate_df: pd.DataFrame | None = None, scan_report: dict[str,Any] | None = None, settings: dict[str,Any] | None = None, state: dict[str,Any] | None = None) -> dict[str,pd.DataFrame]:
    out: dict[str,pd.DataFrame]={}
    tables=tables if isinstance(tables,dict) else {}
    state=state if isinstance(state,dict) else load_state_safe()
    for name in ["actionable","research","recommendations","waiting","emerging_watch","data_repairs","audit","health"]:
        raw=tables.get(name)
        if isinstance(raw,pd.DataFrame): frame=raw.copy()
        elif raw is None: frame=pd.DataFrame()
        else:
            try: frame=pd.DataFrame(raw)
            except Exception: frame=pd.DataFrame()
        if name != "health" and not frame.empty:
            frame=apply_seven_direction_overlay(frame,settings=settings,state=state)
        if name in {"research","waiting","emerging_watch"}:
            frame=_sort_research(frame)
        if name in {"actionable","research","recommendations","waiting","emerging_watch","audit"}:
            frame=_reorder_manager_columns(frame)
        out[name]=frame
    health=out.get("health",pd.DataFrame())
    # Idempotent decoration: export/render may pass through H93 more than once.
    # Remove only prior H93 health rows, never other engines' diagnostics.
    if isinstance(health,pd.DataFrame) and not health.empty and "項目" in health.columns:
        try:
            health = health.loc[~health["項目"].astype(str).str.startswith("H93")].copy()
        except Exception:
            pass
    health_rows=build_health_rows(candidate_df,scan_report,state)
    out["health"]=pd.concat([health,health_rows],ignore_index=True,sort=False)
    return out


def export_contract_summary(tables: dict[str,pd.DataFrame] | None) -> dict[str,Any]:
    tables=tables if isinstance(tables,dict) else {}
    required=["actionable","research","waiting","audit","health","emerging_watch"]
    result={"version":VERSION,"ok":True,"sheets":{}}
    for name in required:
        df=tables.get(name,pd.DataFrame())
        ok=isinstance(df,pd.DataFrame)
        h93_cols=[c for c in H93_COLUMNS if ok and c in df.columns]
        if name not in {"health"} and ok and not df.empty and not h93_cols:
            result["ok"]=False
        result["sheets"][name]={"rows":int(len(df)) if ok else 0,"h93_columns":len(h93_cols),"ok":ok}
    return result
