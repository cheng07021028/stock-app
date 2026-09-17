# -*- coding: utf-8 -*-
"""V191-H75 executive decision/export layer.

Purpose
-------
H74 improved daily research ranking, but the exported workbook still exposed 20+
worksheets with nearly equal visual priority. H75 does not weaken Formal rules and
never manufactures a BUY. It creates one executive decision surface and four compact
supporting views, while keeping technical sheets available as hidden diagnostics.

Authority boundaries
--------------------
* Ranking/research: H74.
* Formal identity: H64/H63.
* Next-session execution veto: H68.
* H75 'readiness' is diagnostic only. It cannot promote A-/Radar to Formal.
"""
from __future__ import annotations

from typing import Any
import math
import pandas as pd

VERSION = "v191_h75_executive_decision_export_20260917"

_BLANK = {"", "none", "nan", "nat", "null", "--", "-", "<na>"}


def _s(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    t = str(v).strip()
    return "" if t.lower() in _BLANK else t


def _num_series(df: pd.DataFrame, col: str, default: float = 50.0) -> pd.Series:
    if col not in df.columns:
        return pd.Series([default] * len(df), index=df.index, dtype=float)
    return pd.to_numeric(df[col], errors="coerce").fillna(default).astype(float)


def _ensure_h74(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
    try:
        from godpick_h74_fresh_mainstream_capital_engine import VERSION as H74V, apply_h74_fresh_mainstream_capital
        v = frame.get("H74版本", pd.Series([""] * len(frame), index=frame.index)).fillna("").astype(str)
        if not v.eq(H74V).all():
            return apply_h74_fresh_mainstream_capital(frame)
    except Exception:
        pass
    return frame.copy()


def apply_h75_executive_decision(frame: pd.DataFrame) -> pd.DataFrame:
    work = _ensure_h74(frame)
    if work.empty:
        return work
    work = work.copy().reset_index(drop=True)

    h74 = _num_series(work, "H74決策總分", 50.0).clip(0, 100)
    core = _num_series(work, "H64核心共振分", 50.0).clip(0, 100)
    rrq = _num_series(work, "H61RR品質分", 50.0).clip(0, 100)
    incremental = _num_series(work, "H62增量機會分", 50.0).clip(0, 100)
    inst = _num_series(work, "H74法人資金加速度分", 50.0).clip(0, 100)
    capital = _num_series(work, "H74成交資金加速度分", 50.0).clip(0, 100)
    fresh = _num_series(work, "H74訊號新鮮分", 50.0).clip(0, 100)
    strength = _num_series(work, "H74強勢加速度分", 50.0).clip(0, 100)
    mainstream = _num_series(work, "H74主流新鮮度分", 50.0).clip(0, 100)

    auth = work.get("H64有效權威", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)
    gate = work.get("H64品質閘門", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)
    h68 = work.get("H68次日執行狀態", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)
    holder = work.get("H74大戶鎖碼狀態", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)
    tier = work.get("H74研究層級", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)

    # Diagnostic readiness only. Authority is not changed by this score.
    readiness = (
        h74 * 0.38 + core * 0.19 + rrq * 0.10 + incremental * 0.08 +
        inst * 0.08 + capital * 0.07 + fresh * 0.04 + strength * 0.03 + mainstream * 0.03
    ).clip(0, 100)
    # If current Formal identity exists, keep it visibly above research rows.
    readiness = (readiness + auth.eq("EFFECTIVE-FORMAL").astype(float) * 12.0).clip(0, 100)

    labels, gaps, one_liners, action_levels = [], [], [], []
    for i in work.index:
        a = _s(auth.iat[i]).upper()
        g = _s(gate.iat[i])
        e = _s(h68.iat[i]).upper()
        ht = _s(holder.iat[i])
        t = _s(tier.iat[i])
        s = float(readiness.iat[i])

        exec_block = any(k in e for k in ["BLOCK", "NO-FORMAL", "HOLD", "禁止", "不可"])
        if a == "EFFECTIVE-FORMAL":
            if exec_block:
                label = "正式推薦｜待H68執行放行"
                action = "F0｜Formal待執行"
            else:
                label = "正式推薦｜仍需盤前價格/觸發重驗"
                action = "F0｜Formal"
        elif t.startswith("F1"):
            label = "第一觀察｜新鮮主流資金核心（非Formal）"
            action = "W1｜第一觀察"
        elif t.startswith("F2"):
            label = "優先觀察｜資金加速（非Formal）"
            action = "W2｜優先觀察"
        elif t.startswith("F3"):
            label = "輪動候選｜等待再確認（非Formal）"
            action = "W3｜候選"
        else:
            label = "一般研究｜不列主要盯盤"
            action = "R0｜一般研究"

        gap_parts = []
        if g.startswith("BLOCK-STRENGTH"):
            gap_parts.append("缺當前強勢/相對強度")
        elif g.startswith("BLOCK-MAINSTREAM"):
            gap_parts.append("缺當前主流/族群共振")
        elif g.startswith("BLOCK-EXEC"):
            gap_parts.append("缺RR/耗竭/增量空間")
        elif g.startswith("WAIT-LOCK"):
            gap_parts.append("TDCC缺前期比較/鎖碼確認")
        elif g.startswith("PASS") and a != "EFFECTIVE-FORMAL":
            gap_parts.append("核心品質已過，但上游仍非Formal")
        elif not g:
            gap_parts.append("Formal品質閘門狀態不足")
        if ht.startswith("UNCONFIRMED") and not any("TDCC" in x for x in gap_parts):
            gap_parts.append("TDCC趨勢未確認")
        if a not in {"EFFECTIVE-FORMAL", "FORMAL-QUALITY-HOLD", "FORMAL-HOLD"}:
            gap_parts.append(f"上游權威={a or 'UNKNOWN'}")
        if exec_block:
            gap_parts.append("H68次日執行未放行")
        gaps.append("；".join(dict.fromkeys(gap_parts)) or "無明顯缺口；仍需盤前重驗")

        advantage = _s(work.at[i, "H74主要優勢"]) if "H74主要優勢" in work.columns else ""
        warning = _s(work.at[i, "H74主要警示"]) if "H74主要警示" in work.columns else ""
        if a == "EFFECTIVE-FORMAL":
            one = f"Formal身分成立；接近度{s:.0f}。{advantage or '等待盤前重驗'}"
        else:
            one = f"{label}；接近度{s:.0f}。{advantage or '新鮮證據仍不足'}"
        if warning and warning != "無重大H74警示":
            one += f"｜警示：{warning}"
        one_liners.append(one)
        labels.append(label)
        action_levels.append(action)

    work["H75主管決策層級"] = action_levels
    work["H75今日結論"] = labels
    work["H75Formal接近度"] = readiness.round(2)
    work["H75Formal主要缺口"] = gaps
    work["H75主管一句話"] = one_liners
    work["H75權威邊界"] = "H75只整併主管決策與Excel輸出；不把A-/Radar/F1/F2/F3升格Formal，不解除H68執行否決。"
    work["H75版本"] = VERSION
    return work


def _decision_priority(s: str) -> int:
    s = _s(s)
    if s.startswith("F0"): return 50
    if s.startswith("W1"): return 40
    if s.startswith("W2"): return 30
    if s.startswith("W3"): return 20
    return 10


def build_h75_today_ai_table(frame: pd.DataFrame, max_rows: int = 12, max_per_sector: int = 3) -> pd.DataFrame:
    w = apply_h75_executive_decision(frame)
    if w.empty:
        return pd.DataFrame({"今日結論": ["本輪沒有可用候選；不為推薦而推薦。"]})
    w["_h75p"] = w["H75主管決策層級"].map(_decision_priority)
    w = w.sort_values(["_h75p", "H75Formal接近度", "H74決策總分", "H74法人資金加速度分", "H74成交資金加速度分"], ascending=False, kind="mergesort")
    sec = next((c for c in ["族群名稱", "類別", "產業別"] if c in w.columns), None)
    picks, counts = [], {}
    for idx, row in w.iterrows():
        group = _s(row.get(sec)) if sec else "未分類"
        if max_per_sector and counts.get(group, 0) >= max_per_sector and not _s(row.get("H75主管決策層級")).startswith("F0"):
            continue
        picks.append(idx); counts[group] = counts.get(group, 0) + 1
        if len(picks) >= max_rows:
            break
    w = w.loc[picks].copy()
    w.insert(0, "今日順位", range(1, len(w) + 1))
    w["是否正式推薦"] = w.get("H64有效權威", pd.Series([""] * len(w), index=w.index)).fillna("").astype(str).eq("EFFECTIVE-FORMAL").map({True:"是｜仍需H68/盤前重驗", False:"否｜觀察/研究"})
    cols = [c for c in [
        "今日順位", "股票代號", "股票名稱", "市場別", "族群名稱", "H75主管決策層級", "H75今日結論", "是否正式推薦",
        "H75Formal接近度", "H74決策總分", "H74強勢加速度分", "H74主流新鮮度分", "H74法人資金加速度分", "H74成交資金加速度分",
        "H74大戶鎖碼狀態", "H74訊號新鮮分", "H75Formal主要缺口", "H68次日執行狀態", "H75主管一句話"
    ] if c in w.columns]
    return w[cols].reset_index(drop=True)


def build_h75_sector_summary(sector_df: pd.DataFrame, max_rows: int = 10) -> pd.DataFrame:
    if sector_df is None or not isinstance(sector_df, pd.DataFrame) or sector_df.empty:
        return pd.DataFrame({"狀態": ["目前沒有可用主流族群資料。"]})
    w = sector_df.copy().head(max_rows)
    score_col = next((c for c in ["H60族群複利機會分", "H60族群三因子分", "H53族群決策分"] if c in w.columns), None)
    interpretations = []
    for _, r in w.iterrows():
        score = float(pd.to_numeric(pd.Series([r.get(score_col)]), errors="coerce").fillna(50).iat[0]) if score_col else 50.0
        resonance = float(pd.to_numeric(pd.Series([r.get("H53族群共振分")]), errors="coerce").fillna(50).iat[0]) if "H53族群共振分" in w.columns else 50.0
        breadth = float(pd.to_numeric(pd.Series([r.get("H53族群廣度分")]), errors="coerce").fillna(50).iat[0]) if "H53族群廣度分" in w.columns else 50.0
        cash = float(pd.to_numeric(pd.Series([r.get("H57族群前三資金加速")]), errors="coerce").fillna(50).iat[0]) if "H57族群前三資金加速" in w.columns else 50.0
        if score >= 67 and resonance >= 60 and cash >= 60:
            interpretations.append("主流優先｜共振與資金同步")
        elif score >= 63 and (resonance >= 58 or cash >= 65):
            interpretations.append("輪動偏強｜持續觀察延續")
        elif breadth < 45:
            interpretations.append("集中/廣度不足｜避免只看單一強股")
        else:
            interpretations.append("一般輪動｜需個股再確認")
    w.insert(min(2, len(w.columns)), "今日族群解讀", interpretations)
    cols = [c for c in [
        "H60族群排名", "類別", "今日族群解讀", "H60族群複利機會分", "H60族群三因子分", "H53族群共振分", "H53族群廣度分",
        "H53族群攻擊分", "H53族群資金分", "H57族群前三資金加速", "H57族群點火廣度分", "H60族群主升分", "H60族群鎖碼分", "H60族群真實鎖碼覆蓋率%"
    ] if c in w.columns]
    return w[cols].reset_index(drop=True)


def build_h75_evidence_table(frame: pd.DataFrame, max_rows: int = 20) -> pd.DataFrame:
    w = apply_h75_executive_decision(frame)
    if w.empty:
        return pd.DataFrame({"狀態": ["目前沒有推薦證據。"]})
    w["_h75p"] = w["H75主管決策層級"].map(_decision_priority)
    w = w.sort_values(["_h75p", "H75Formal接近度", "H74決策總分"], ascending=False, kind="mergesort").head(max_rows).copy()
    w.insert(0, "證據順位", range(1, len(w) + 1))
    cols = [c for c in [
        "證據順位", "股票代號", "股票名稱", "族群名稱", "H75主管決策層級", "H75Formal接近度", "H75Formal主要缺口",
        "H74決策總分", "H74強勢加速度分", "H74主流新鮮度分", "H74法人資金加速度分", "H74成交資金加速度分", "H74大戶鎖碼真相分",
        "H74大戶鎖碼狀態", "H74訊號新鮮分", "H74熟面孔慣性扣分", "H74陳舊品質扣分", "H64核心共振分", "H64品質閘門",
        "H61RR品質分", "H62增量機會分", "H68次日執行狀態", "H74主要優勢", "H74主要警示", "H74研究建議"
    ] if c in w.columns]
    return w[cols].reset_index(drop=True)


def build_h75_performance_summary(perf_df: pd.DataFrame) -> pd.DataFrame:
    if perf_df is None or not isinstance(perf_df, pd.DataFrame) or perf_df.empty:
        return pd.DataFrame({"狀態": ["目前沒有成熟績效資料。"]})
    if not {"績效指標", "目前數值"}.issubset(set(perf_df.columns)):
        return perf_df.head(30).copy()
    preferred = [
        "H74學習快照成熟樣本", "H74學習啟用狀態", "H74_F1成熟樣本", "H74_F1正報酬率%", "H74_F1平均1日報酬%", "H74_F1平均SelectionAlpha%",
        "H74_F2成熟樣本", "H74_F2正報酬率%", "H74_F2平均1日報酬%", "H74_F2平均SelectionAlpha%",
        "H74_F3成熟樣本", "H74_F3正報酬率%", "H74_F3平均1日報酬%", "H74_F3平均SelectionAlpha%", "H74排名成熟交易日", "H74平均RankIC", "H74平均NDCG@10",
        "H73排名成熟交易日", "H73平均RankIC", "H73平均NDCG@10", "brier_score", "brier_skill_vs_base_rate_pct"
    ]
    order = {k:i for i,k in enumerate(preferred)}
    w = perf_df.loc[perf_df["績效指標"].astype(str).isin(preferred)].copy()
    if w.empty:
        w = perf_df.head(30).copy()
    else:
        w["_o"] = w["績效指標"].map(order).fillna(999)
        w = w.sort_values("_o").drop(columns=["_o"])
    interpretations=[]
    for _, r in w.iterrows():
        k=_s(r.get("績效指標")); v=r.get("目前數值")
        if "成熟樣本" in k or "成熟交易日" in k:
            try:
                n=float(v); interpretations.append("樣本仍少，暫不自動調權" if n < 30 else "樣本達治理門檻，可供滾動校正")
            except Exception: interpretations.append("樣本狀態待確認")
        elif "正報酬率" in k:
            interpretations.append("越高越好；需搭配樣本數與Alpha")
        elif "SelectionAlpha" in k or "平均1日報酬" in k:
            interpretations.append("正值較佳；避免只看命中率")
        elif "RankIC" in k or "NDCG" in k:
            interpretations.append("排序品質指標；看滾動趨勢，不看單日")
        else:
            interpretations.append("模型校準/績效治理指標")
    w["主管解讀"] = interpretations
    return w.reset_index(drop=True)


def build_h75_health_summary(health_df: pd.DataFrame, max_rows: int = 30) -> pd.DataFrame:
    if health_df is None or not isinstance(health_df, pd.DataFrame) or health_df.empty:
        return pd.DataFrame({"狀態": ["目前沒有系統健康資料。"]})
    w = health_df.copy()
    text_cols = [c for c in ["項目", "類型", "狀態"] if c in w.columns]
    if "項目" in w.columns:
        keys = ["掃描", "覆蓋", "TDCC", "官方", "K線", "資料", "版本一致", "正式推薦可用", "新鮮", "H74", "H75", "錯誤", "失敗"]
        mask = w["項目"].astype(str).map(lambda x: any(k in x for k in keys))
        f = w.loc[mask].copy()
        if not f.empty:
            w = f
    w = w.head(max_rows).copy()
    if "項目" in w.columns:
        def _health_read(r):
            item=_s(r.get("項目")); val=_s(r.get("數值"))
            low=(item+" "+val).lower()
            if any(k in low for k in ["fail", "失敗", "不足", "false", "不可用"]): return "注意｜先修資料再解讀推薦"
            if "tdcc" in low and any(k in low for k in ["缺前期", "unconfirmed"]): return "注意｜有持股不等於鎖碼趨勢"
            return "正常/資訊｜配合推薦資料時間閱讀"
        w["主管解讀"] = w.apply(_health_read, axis=1)
    return w.reset_index(drop=True)


__all__ = [
    "VERSION", "apply_h75_executive_decision", "build_h75_today_ai_table", "build_h75_sector_summary",
    "build_h75_evidence_table", "build_h75_performance_summary", "build_h75_health_summary",
]
