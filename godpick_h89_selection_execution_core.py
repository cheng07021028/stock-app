# -*- coding: utf-8 -*-
"""H89 Selection-Execution Dual Learning & Dynamic Trade Plan Core.

This layer addresses two different questions separately:
1) Was the stock/industry direction worth selecting?
2) Was the proposed entry/stop/target actually executable and efficient?

Governance:
- H89 never creates H64/H68 Formal authority.
- A model-generated target is research-only and can never grant Formal.
- Stops are never moved closer merely to force RR.
- Suspicious proxy outcomes are excluded from H89 learning instead of freezing
  the whole learning system.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
import json
import math
import os
import hashlib

import pandas as pd

VERSION = "v191_h89_selection_execution_dual_learning_20260922"
STATE_FILE = "godpick_h89_selection_execution_state.json"
STATE_DOC = "godpick_h89_selection_execution_state"
RECORDS_FILE = "godpick_records.json"
BASE_DIR = Path(__file__).resolve().parent

H89_COLUMNS = [
    "H89版本", "H89選股方向分", "H89執行品質分", "H89雙軌研究排序分", "H89研究排序加減分",
    "H89交易型態", "H89進場區下緣", "H89進場區上緣", "H89主進場", "H89防守停損",
    "H89第一目標", "H89第二目標", "H89成本後RR1", "H89成本後RR2", "H89停損距離%",
    "H89價格計畫來源", "H89Formal價格計畫合格", "H89模型目標僅研究", "H89所需拉回%",
    "H89機會成本監控", "H89學習乾淨樣本", "H89錯失機會率%", "H89追價失敗率%",
    "H89進場積極度調整%", "H89選股與執行摘要",
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
        if not v or v.lower() in {"none", "nan", "null", "--", "-", "<na>"}:
            return default
    try:
        x = float(v)
    except Exception:
        return default
    return x if math.isfinite(x) else default


def _bool(v: Any) -> bool:
    return _text(v).lower() in {"true", "1", "yes", "y", "是", "已買進", "已觸發", "達標", "已達"}


def _clip(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(v)))


def _first_num(row: dict[str, Any], names: Iterable[str]) -> float | None:
    for name in names:
        x = _num(row.get(name), None)
        if x is not None and x > 0:
            return x
    return None


def _first_any_num(row: dict[str, Any], names: Iterable[str]) -> float | None:
    for name in names:
        x = _num(row.get(name), None)
        if x is not None:
            return x
    return None


def _settings(settings: dict[str, Any] | None = None) -> dict[str, Any]:
    if isinstance(settings, dict):
        try:
            from godpick_h89_execution_settings import normalize_settings
            return normalize_settings(settings)
        except Exception:
            return settings
    from godpick_h89_execution_settings import load_settings_safe
    return load_settings_safe()


def _net_rr(entry: float, stop: float, target: float, *, commission: float = .001425, tax: float = .003, slippage: float = .001) -> float | None:
    if not (stop < entry < target):
        return None
    buy = entry * (1 + slippage) * (1 + commission)
    sell_factor = (1 - slippage) * (1 - commission - tax)
    risk = buy - stop * sell_factor
    reward = target * sell_factor - buy
    if risk <= 0:
        return None
    return reward / risk


def _entry_ceiling_for_rr(stop: float, target: float, rr: float, *, commission: float = .001425, tax: float = .003, slippage: float = .001) -> float | None:
    if not (stop > 0 and target > stop and rr > 0):
        return None
    sell = (1 - slippage) * (1 - commission - tax)
    buy_factor = (1 + slippage) * (1 + commission)
    ceiling = sell * (target + rr * stop) / ((1 + rr) * buy_factor)
    if stop < ceiling < target:
        return ceiling
    return None


def _target_for_rr(entry: float, stop: float, rr: float, *, commission: float = .001425, tax: float = .003, slippage: float = .001) -> float | None:
    if not (stop < entry and rr > 0):
        return None
    buy = entry * (1 + slippage) * (1 + commission)
    sell = (1 - slippage) * (1 - commission - tax)
    risk = buy - stop * sell
    if risk <= 0 or sell <= 0:
        return None
    return (buy + rr * risk) / sell


def _reference_price(row: dict[str, Any]) -> float | None:
    return _first_num(row, [
        "目前價", "最新價", "收盤價", "推薦價格", "推薦日價格", "成交價",
        "主要進場參考價", "實戰觸發價",
    ])


def _structural_stop(row: dict[str, Any]) -> tuple[float | None, str]:
    for key in ["停損參考", "H79結構停損", "SuperAI動態停損價", "實戰停損參考", "停損價", "結構停損"]:
        x = _num(row.get(key), None)
        if x is not None and x > 0:
            return x, key
    # Research-only fallback: support / moving average, never Formal by itself.
    candidates = []
    for key in ["主要支撐價", "第一支撐價", "MA20", "20日均線", "MA10", "10日均線", "近20日低點"]:
        x = _num(row.get(key), None)
        if x is not None and x > 0:
            candidates.append((x, key))
    if candidates:
        return max(candidates, key=lambda t: t[0])
    return None, ""


def _structural_targets(row: dict[str, Any], entry: float | None) -> list[tuple[float, str]]:
    targets: list[tuple[float, str]] = []
    for key in [
        "第一壓力價", "SuperAI第一減碼價", "第二壓力價", "SuperAI第二減碼價",
        "近20日高點", "近60日高點", "前高", "布林上軌", "壓力價",
    ]:
        x = _num(row.get(key), None)
        if x is not None and x > 0 and (entry is None or x > entry * 1.002):
            targets.append((x, key))
    # unique by rounded price, ascending.
    seen = set(); out = []
    for value, key in sorted(targets, key=lambda t: t[0]):
        mark = round(value, 6)
        if mark not in seen:
            seen.add(mark); out.append((value, key))
    return out


def _selection_score(row: dict[str, Any]) -> float:
    parts: list[tuple[float, float]] = []
    for names, weight in [
        (["H79自適應機會分", "V188股神作戰優先分", "股神推薦優先分"], .35),
        (["H79強度百分位%", "H47個股相對強度分", "相對強度分"], .22),
        (["H79族群百分位%", "H53族群共振分", "族群共振分"], .18),
        (["H81專業研究總分"], .15),
        (["H77驗證增量分", "H74決策總分", "H72風險調整分"], .10),
    ]:
        value = _first_any_num(row, names)
        if value is not None:
            parts.append((_clip(value, 0, 100), weight))
    if not parts:
        return 50.0
    total = sum(w for _, w in parts)
    return _clip(sum(v*w for v,w in parts)/total, 0, 100)


def _plan_one(row: dict[str, Any], cfg: dict[str, Any], learning_state: dict[str, Any] | None = None) -> dict[str, Any]:
    tcfg = cfg["trade_plan"]
    lstate = learning_state or {}
    entry0 = _first_num(row, ["主要進場參考價", "實戰觸發價", "H79計畫進場"])
    ref = _reference_price(row) or entry0
    stop, stop_source = _structural_stop(row)
    targets = _structural_targets(row, entry0)
    target0 = _first_num(row, ["第一壓力價", "SuperAI第一減碼價", "H79第一目標"])
    if target0 is not None and entry0 is not None and target0 > entry0 * 1.002 and all(abs(target0-x[0]) > 1e-6 for x in targets):
        targets.insert(0, (target0, "原始第一目標"))

    selection = _selection_score(row)
    entry_bias = _num((lstate.get("execution_policy") or {}).get("entry_bias_pct"), 0.0) or 0.0
    entry_bias = _clip(entry_bias, -float(cfg["learning"]["max_entry_bias_pct"]), float(cfg["learning"]["max_entry_bias_pct"]))

    result: dict[str, Any] = {
        "H89版本": VERSION,
        "H89選股方向分": round(selection, 2),
        "H89執行品質分": 20.0,
        "H89交易型態": "等待價格結構",
        "H89進場區下緣": None, "H89進場區上緣": None, "H89主進場": entry0,
        "H89防守停損": stop, "H89第一目標": target0, "H89第二目標": None,
        "H89成本後RR1": None, "H89成本後RR2": None, "H89停損距離%": None,
        "H89價格計畫來源": "缺完整價格結構", "H89Formal價格計畫合格": "否",
        "H89模型目標僅研究": "否", "H89所需拉回%": None,
        "H89機會成本監控": "是" if selection >= 70 else "否",
        "H89進場積極度調整%": round(entry_bias, 3),
    }
    if entry0 is None or stop is None or stop <= 0:
        result["H89選股與執行摘要"] = "選股方向與價格執行分離：目前缺進場/停損，僅保留研究方向，不建立買進許可。"
        return result

    # Existing valid structural plan wins. Never alter a valid plan merely for a higher score.
    structural_target = targets[0] if targets else None
    if structural_target is not None:
        t1, t1_source = structural_target
        rr0 = _net_rr(entry0, stop, t1)
        dist0 = (entry0-stop)/entry0*100 if entry0 > stop else None
        if rr0 is not None and rr0 >= float(tcfg["formal_min_net_rr"]) and dist0 is not None and dist0 <= float(tcfg["max_stop_distance_pct"]):
            width = _clip(abs(entry_bias) + float(tcfg["min_entry_zone_width_pct"]), float(tcfg["min_entry_zone_width_pct"]), float(tcfg["max_entry_zone_width_pct"]))
            result.update({
                "H89交易型態": "原始結構有效",
                "H89進場區下緣": round(entry0*(1-width/100), 4),
                "H89進場區上緣": round(entry0*(1+width/100), 4),
                "H89主進場": round(entry0, 4), "H89第一目標": round(t1, 4),
                "H89第二目標": round(targets[1][0],4) if len(targets)>1 else None,
                "H89成本後RR1": round(rr0, 4),
                "H89成本後RR2": round(_net_rr(entry0, stop, targets[1][0]),4) if len(targets)>1 and _net_rr(entry0,stop,targets[1][0]) is not None else None,
                "H89停損距離%": round(dist0, 4),
                "H89價格計畫來源": f"原始結構｜停損:{stop_source}｜目標:{t1_source}",
                "H89Formal價格計畫合格": "是",
                "H89執行品質分": round(_clip(68 + min(22, (rr0-float(tcfg['formal_min_net_rr']))*12) - max(0, dist0-5)*2, 0, 100), 2),
            })
            result["H89選股與執行摘要"] = f"選股方向{selection:.1f}；原始結構RR={rr0:.2f}，Formal價格計畫可沿用，但仍須H64/H68。"
            return result

    # Try a pullback entry against a real structural target. This solves target==entry / low-RR plans without moving the stop.
    if structural_target is not None:
        t1, t1_source = structural_target
        ceiling = _entry_ceiling_for_rr(stop, t1, float(tcfg["formal_min_net_rr"]))
        if ceiling is not None:
            # Positive bias learned from missed opportunities makes entry slightly more aggressive,
            # but it may never exceed the formal RR ceiling.
            adjusted = ceiling * (1 + max(0.0, entry_bias)/100)
            adjusted = min(adjusted, ceiling)
            pullback = ((ref-adjusted)/ref*100) if ref and ref > 0 else None
            if pullback is None or pullback <= float(tcfg["max_pullback_from_reference_pct"]):
                rr = _net_rr(adjusted, stop, t1)
                dist = (adjusted-stop)/adjusted*100 if adjusted > stop else None
                if rr is not None and dist is not None:
                    width = _clip(float(tcfg["min_entry_zone_width_pct"]) + max(0.0,-entry_bias)*.3, float(tcfg["min_entry_zone_width_pct"]), float(tcfg["max_entry_zone_width_pct"]))
                    result.update({
                        "H89交易型態": "RR反推拉回",
                        "H89進場區下緣": round(adjusted*(1-width/100),4),
                        "H89進場區上緣": round(adjusted*(1+width/100),4),
                        "H89主進場": round(adjusted,4), "H89第一目標": round(t1,4),
                        "H89第二目標": round(targets[1][0],4) if len(targets)>1 else None,
                        "H89成本後RR1": round(rr,4),
                        "H89成本後RR2": round(_net_rr(adjusted,stop,targets[1][0]),4) if len(targets)>1 and _net_rr(adjusted,stop,targets[1][0]) is not None else None,
                        "H89停損距離%": round(dist,4), "H89所需拉回%": round(max(0.0,pullback or 0.0),3),
                        "H89價格計畫來源": f"RR反推進場｜停損:{stop_source}｜結構目標:{t1_source}",
                        "H89Formal價格計畫合格": "是" if dist <= float(tcfg["max_stop_distance_pct"]) else "否",
                        "H89執行品質分": round(_clip(72 - max(0.0,(pullback or 0)-5)*2.3 - max(0.0,dist-float(tcfg['max_stop_distance_pct']))*4,0,100),2),
                    })
                    result["H89選股與執行摘要"] = f"原進場RR不足；不移動停損，以真實結構目標反推進場。所需拉回約{max(0.0,pullback or 0):.1f}%。"
                    return result

    # Research-only continuation model. Never grants Formal because the target is synthetic.
    if bool(tcfg.get("allow_research_model_target", True)) and ref and stop < ref and selection >= 68:
        research_rr = float(tcfg["research_min_net_rr"])
        model_target = _target_for_rr(ref, stop, research_rr)
        upside = (model_target/ref-1)*100 if model_target and ref else None
        if model_target is not None and upside is not None and 0 < upside <= float(tcfg["max_model_target_upside_pct"]):
            width = _clip(float(tcfg["min_entry_zone_width_pct"]) + abs(entry_bias)*.25, float(tcfg["min_entry_zone_width_pct"]), float(tcfg["max_entry_zone_width_pct"]))
            dist = (ref-stop)/ref*100
            rr = _net_rr(ref, stop, model_target)
            result.update({
                "H89交易型態": "研究型動能續航",
                "H89進場區下緣": round(ref*(1-width/100),4), "H89進場區上緣": round(ref*(1+width/100),4),
                "H89主進場": round(ref,4), "H89第一目標": round(model_target,4),
                "H89成本後RR1": round(rr,4) if rr is not None else None, "H89停損距離%": round(dist,4),
                "H89價格計畫來源": "研究模型RR目標｜非結構壓力價",
                "H89Formal價格計畫合格": "否", "H89模型目標僅研究": "是",
                "H89執行品質分": round(_clip(52 + (selection-68)*.35 - max(0,dist-float(tcfg['max_stop_distance_pct']))*3,0,75),2),
            })
            result["H89選股與執行摘要"] = "方向強但缺可驗證下一結構目標；建立研究型續航情境，不得升格Formal。"
            return result

    result["H89選股與執行摘要"] = "選股方向仍可研究，但目前沒有同時滿足停損、目標與成本後RR的可執行價格計畫。"
    return result


def _suspicious_proxy(row: dict[str, Any]) -> bool:
    source = _text(row.get("績效資料來源")).lower()
    if any(k in source for k in ["proxy", "代理", "目前", "最新價", "即時"]):
        return True
    vals = []
    for key in ["推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%"]:
        v = _num(row.get(key), None)
        if v is not None:
            vals.append(round(v, 8))
    return len(vals) >= 3 and max(vals)-min(vals) < 1e-9


def _horizon_return(row: dict[str, Any], horizon: int) -> float | None:
    actual = _num(row.get("實際報酬%"), None)
    if (_bool(row.get("是否已實際買進")) or _bool(row.get("是否已買進"))) and actual is not None:
        return actual
    for key in [f"可執行交易{horizon}日%", f"推薦後{horizon}日%", f"{horizon}日績效%", f"{horizon}日報酬%"]:
        v = _num(row.get(key), None)
        if v is not None and -100 <= v <= 300:
            return v
    return None


def _triggered(row: dict[str, Any]) -> bool | None:
    keys = ["是否觸發進場", "是否達進場", "是否已實際買進", "是否已買進", "進場已觸發"]
    for k in keys:
        if k in row and _text(row.get(k)):
            return _bool(row.get(k))
    if _text(row.get("實際進場日期")) or _text(row.get("進場日期")) or _num(row.get("實際進場價"), None) is not None:
        return True
    return None


def _sector(row: dict[str, Any]) -> str:
    for key in ["正式產業別", "類別", "產業", "族群", "主題類別"]:
        s = _text(row.get(key))
        if s:
            return s
    return "未分類"


def _style(row: dict[str, Any]) -> str:
    s = "｜".join(_text(row.get(k)) for k in ["H89交易型態", "H79推薦狀態", "推薦分層", "建議動作"])
    if "等待拉回" in s or "RR反推拉回" in s:
        return "等待拉回"
    if "續航" in s or "突破" in s or "主升" in s:
        return "動能續航"
    if "重建進場" in s or "等待有效進場" in s:
        return "價格重建"
    if "正式" in s or "條件可執行" in s:
        return "正式執行"
    return "其他"


def build_learning_state(records: pd.DataFrame | list[dict[str, Any]] | None, settings: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = _settings(settings)
    lcfg = cfg["learning"]
    df = records.copy() if isinstance(records, pd.DataFrame) else pd.DataFrame(records or [])
    if df.empty:
        return {"version": VERSION, "available": False, "generated_at": datetime.now().isoformat(timespec="seconds"), "clean_samples": 0, "reason": "沒有推薦績效紀錄", "execution_policy": {"entry_bias_pct": 0.0}}
    horizon = int(lcfg["primary_horizon"])
    rows = []
    suspicious = 0
    for _, s in df.loc[:, ~df.columns.duplicated()].iterrows():
        row = s.to_dict()
        ret = _horizon_return(row, horizon)
        if ret is None:
            continue
        if _suspicious_proxy(row):
            suspicious += 1
            if bool(lcfg.get("exclude_suspicious_proxy_instead_of_freezing_all", True)):
                continue
        trig = _triggered(row)
        selection_success = ret >= float(lcfg["selection_success_return_pct"])
        selection_failure = ret <= float(lcfg["selection_failure_return_pct"])
        opportunity = (trig is False) and ret >= float(lcfg["opportunity_cost_return_pct"])
        exec_success = bool(trig is True and (ret > 0 or _bool(row.get("是否達目標1"))))
        exec_failure = bool(trig is True and (ret < 0 or _bool(row.get("是否達停損"))))
        rows.append({
            "return": float(ret), "triggered": trig, "selection_success": selection_success,
            "selection_failure": selection_failure, "opportunity": opportunity,
            "execution_success": exec_success, "execution_failure": exec_failure,
            "sector": _sector(row), "style": _style(row),
        })
    clean = pd.DataFrame(rows)
    n = len(clean)
    min_n = int(lcfg["minimum_clean_samples"])
    if clean.empty:
        return {"version": VERSION, "available": False, "generated_at": datetime.now().isoformat(timespec="seconds"), "clean_samples": 0, "excluded_suspicious_proxy": suspicious, "reason": "沒有乾淨成熟樣本", "execution_policy": {"entry_bias_pct": 0.0}}

    selection_win = float(clean["selection_success"].mean())
    selection_fail = float(clean["selection_failure"].mean())
    miss = float(clean["opportunity"].mean())
    triggered = clean[clean["triggered"] == True]
    chase_fail = float(triggered["execution_failure"].mean()) if len(triggered) else 0.0
    exec_win = float(triggered["execution_success"].mean()) if len(triggered) else 0.0
    # If missed opportunities dominate failed triggers, future research entry may be slightly more aggressive.
    raw_bias = (miss - chase_fail) * 5.0
    entry_bias = _clip(raw_bias, -float(lcfg["max_entry_bias_pct"]), float(lcfg["max_entry_bias_pct"])) if n >= min_n else 0.0

    sectors: dict[str, Any] = {}
    min_seg = int(lcfg["minimum_segment_samples"])
    for name, g in clean.groupby("sector"):
        if len(g) < min_seg:
            continue
        sectors[str(name)] = {
            "sample": int(len(g)),
            "avg_return": round(float(g["return"].mean()), 4),
            "selection_win_rate": round(float(g["selection_success"].mean()), 4),
        }
    styles: dict[str, Any] = {}
    for name, g in clean.groupby("style"):
        if len(g) < min_seg:
            continue
        gt = g[g["triggered"] == True]
        styles[str(name)] = {
            "sample": int(len(g)),
            "opportunity_cost_rate": round(float(g["opportunity"].mean()), 4),
            "triggered_sample": int(len(gt)),
            "execution_failure_rate": round(float(gt["execution_failure"].mean()), 4) if len(gt) else None,
        }
    return {
        "version": VERSION,
        "available": bool(n >= min_n and cfg.get("enabled", True) and lcfg.get("enabled", True)),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "primary_horizon": horizon,
        "clean_samples": n,
        "excluded_suspicious_proxy": suspicious,
        "selection": {
            "success_rate": round(selection_win, 4), "failure_rate": round(selection_fail, 4),
            "avg_return": round(float(clean["return"].mean()), 4),
        },
        "execution": {
            "triggered_samples": int(len(triggered)), "success_rate": round(exec_win,4),
            "failure_rate": round(chase_fail,4), "opportunity_cost_rate": round(miss,4),
        },
        "execution_policy": {"entry_bias_pct": round(entry_bias,4)},
        "sector_profiles": sectors, "style_profiles": styles,
        "governance": "乾淨樣本學習；可疑代理直接排除，不用代理資料凍結整套選股；只調研究排序/研究進場積極度，不建立Formal。",
    }


def _read_records(path: str = RECORDS_FILE) -> list[dict[str, Any]]:
    p = BASE_DIR / path
    try:
        raw = json.loads(p.read_text(encoding="utf-8-sig"))
        return raw if isinstance(raw, list) else list(raw.get("records", [])) if isinstance(raw, dict) else []
    except Exception:
        return []


def _write_state(state: dict[str, Any], persist_remote: bool = False) -> tuple[bool, str]:
    p = BASE_DIR / STATE_FILE
    try:
        tmp = p.with_suffix(".json.tmp_h89")
        tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        tmp.replace(p)
    except Exception as exc:
        return False, f"H89本機狀態保存失敗：{exc}"
    if persist_remote:
        try:
            from godpick_durability_service import persist_json_async
            ok, msg = persist_json_async(STATE_FILE, state, firestore_doc=STATE_DOC, reason="H89 selection/execution learning state")
            return True, f"本機完成｜遠端背景：{msg}"
        except Exception as exc:
            return True, f"本機完成｜遠端背景例外：{exc}"
    return True, "H89狀態本機完成"


def refresh_learning_state(*, records: pd.DataFrame | list[dict[str, Any]] | None = None, settings: dict[str, Any] | None = None, persist_remote: bool = False) -> tuple[dict[str, Any], list[str]]:
    raw = records if records is not None else _read_records()
    state = build_learning_state(raw, settings)
    ok, msg = _write_state(state, persist_remote=persist_remote)
    return state, [msg if ok else "H89狀態保存失敗：" + msg]


def load_learning_state() -> dict[str, Any]:
    try:
        raw = json.loads((BASE_DIR / STATE_FILE).read_text(encoding="utf-8-sig"))
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def _learning_adjustment(row: dict[str, Any], state: dict[str, Any], cfg: dict[str, Any]) -> float:
    if not state.get("available"):
        return 0.0
    cap = float(cfg["learning"]["max_rank_adjustment_points"])
    adj = 0.0
    sector = _sector(row)
    sp = (state.get("sector_profiles") or {}).get(sector) or {}
    n = int(sp.get("sample", 0) or 0)
    if n >= int(cfg["learning"]["minimum_segment_samples"]):
        win = _num(sp.get("selection_win_rate"), .5) or .5
        avg = _num(sp.get("avg_return"), 0.0) or 0.0
        adj += _clip((win-.5)*2.0 + avg/8.0, -.8, .8)
    # Execution learning has smaller influence than selection quality.
    miss = _num((state.get("execution") or {}).get("opportunity_cost_rate"), 0.0) or 0.0
    fail = _num((state.get("execution") or {}).get("failure_rate"), 0.0) or 0.0
    selection = _selection_score(row)
    if selection >= 75:
        adj += _clip((miss-fail)*1.0, -.35, .35)
    return _clip(adj, -cap, cap)


def apply_execution_plan_overlay(frame: pd.DataFrame | None, settings: dict[str, Any] | None = None, state: dict[str, Any] | None = None) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
    cfg = _settings(settings)
    if not cfg.get("enabled", True):
        return frame.copy()
    learning = state if isinstance(state, dict) else load_learning_state()
    rows = []
    for raw in frame.to_dict("records"):
        plan = _plan_one(raw, cfg, learning)
        out = dict(raw); out.update(plan)
        # Feed a valid H89 plan into the canonical H79 price-plan validator.
        # A synthetic/model target remains marked research-only and is blocked
        # from Formal later by H89Formal價格計畫合格.
        if _num(plan.get("H89主進場"), None) is not None:
            out["主要進場參考價"] = plan["H89主進場"]
        if _num(plan.get("H89防守停損"), None) is not None:
            out["停損參考"] = plan["H89防守停損"]
        if _num(plan.get("H89第一目標"), None) is not None:
            out["第一壓力價"] = plan["H89第一目標"]
        rows.append(out)
    return pd.DataFrame(rows, index=frame.index)


def apply_learning_overlay(frame: pd.DataFrame | None, settings: dict[str, Any] | None = None, state: dict[str, Any] | None = None) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
    cfg = _settings(settings)
    learning = state if isinstance(state, dict) else load_learning_state()
    out = frame.copy()
    adjs=[]; ranks=[]; clean=[]; miss=[]; fail=[]; summaries=[]
    clean_n = int(learning.get("clean_samples",0) or 0)
    miss_rate = (_num((learning.get("execution") or {}).get("opportunity_cost_rate"),0) or 0)*100
    fail_rate = (_num((learning.get("execution") or {}).get("failure_rate"),0) or 0)*100
    for raw in out.to_dict("records"):
        adj = _learning_adjustment(raw, learning, cfg)
        base = _first_any_num(raw, ["H82自適應研究排序分", "H81研究排序分", "H79自適應機會分"]) or 50.0
        adjs.append(round(adj,4)); ranks.append(round(_clip(base+adj,0,100),2)); clean.append(clean_n); miss.append(round(miss_rate,2)); fail.append(round(fail_rate,2))
        summaries.append(
            f"Selection/Execution分離：選股方向={_selection_score(raw):.1f}；執行品質={_num(raw.get('H89執行品質分'),20):.1f}；"
            f"乾淨成熟樣本={clean_n}；研究排序調整={adj:+.2f}。H89不建立Formal權限。"
        )
    out["H89學習乾淨樣本"] = clean
    out["H89錯失機會率%"] = miss
    out["H89追價失敗率%"] = fail
    out["H89研究排序加減分"] = adjs
    out["H89雙軌研究排序分"] = ranks
    # Preserve the more detailed price-plan summary when present.
    old = out.get("H89選股與執行摘要", pd.Series("", index=out.index)).fillna("").astype(str)
    out["H89選股與執行摘要"] = [((a + "｜" + b).strip("｜")) for a,b in zip(old.tolist(), summaries)]
    return out


def apply_record_snapshot(frame: pd.DataFrame | None, settings: dict[str, Any] | None = None) -> pd.DataFrame:
    """Attach current H89 plan/learning evidence to Page08 records without granting Formal."""
    work = apply_execution_plan_overlay(frame, settings=settings)
    return apply_learning_overlay(work, settings=settings)


def state_fingerprint(state: dict[str, Any]) -> str:
    try:
        return hashlib.sha256(json.dumps(state, ensure_ascii=False, sort_keys=True, default=str).encode()).hexdigest()[:20]
    except Exception:
        return ""
