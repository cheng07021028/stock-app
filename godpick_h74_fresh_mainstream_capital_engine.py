# -*- coding: utf-8 -*-
"""V191-H74 Fresh Mainstream / Capital Rotation truth overlay.

H74 fixes a structural weakness observed in H73 exports: steady, historically high-
quality names can remain near the front even when today's leadership, sector rotation,
institutional demand and traded-capital acceleration have moved elsewhere.

Design rules
------------
* Decision-time only. No future return, T+1 outcome or later market data is used.
* Cross-sectional every run: raw institutional flows and turnover acceleration are
  compared with the *current* candidate universe instead of carrying stale absolute
  levels forward.
* TDCC truth: a current large-holder percentage without a previous-period comparison
  is NOT called "locking". It is explicitly UNCONFIRMED and receives only neutral
  support. A real weekly delta is required to confirm accumulation/distribution.
* Familiar-name governance: repeat appearances are penalized only when new evidence
  has not renewed. A genuinely re-accelerating leader is allowed to remain near front.
* H74 is research/ranking authority only. It never creates Formal and never bypasses
  H64/H63 Formal truth or H68 execution veto.
"""
from __future__ import annotations

from typing import Any, Iterable
import math
import pandas as pd

VERSION = "v191_h74_fresh_mainstream_capital_rotation_truth_20260917"

H74_COLUMNS = [
    "H74強勢加速度分", "H74主流新鮮度分", "H74法人資金加速度分", "H74成交資金加速度分",
    "H74大戶鎖碼真相分", "H74大戶鎖碼狀態", "H74訊號新鮮分", "H74熟面孔慣性扣分",
    "H74陳舊品質扣分", "H74資金共振加分", "H74決策總分", "H74全市場百分位%", "H74全市場順位",
    "H74研究層級", "H74相對H73順位變化", "H74主要優勢", "H74主要警示", "H74研究建議",
    "H74學習快照狀態", "H74權威邊界", "H74版本",
]
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


def _f(v: Any, default: float | None = None) -> float | None:
    try:
        t = str(v).strip().replace(",", "").replace("％", "%")
        if t.endswith("%"):
            t = t[:-1].strip()
        if not t or t.lower() in _BLANK:
            return default
        x = float(t)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _clip(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, float(x)))


def _series_num(work: pd.DataFrame, col: str, default: float = 50.0, *, clip: bool = True) -> pd.Series:
    if col not in work.columns:
        return pd.Series([default] * len(work), index=work.index, dtype=float)
    s = pd.to_numeric(work[col], errors="coerce").fillna(default).astype(float)
    return s.clip(0, 100) if clip else s


def _first_num_series(work: pd.DataFrame, names: Iterable[str], default: float = 50.0, *, clip: bool = True) -> pd.Series:
    for c in names:
        if c in work.columns:
            s = pd.to_numeric(work[c], errors="coerce")
            if s.notna().any():
                s = s.fillna(default).astype(float)
                return s.clip(0, 100) if clip else s
    return pd.Series([default] * len(work), index=work.index, dtype=float)


def _percentile_score(raw: pd.Series, neutral: float = 50.0) -> pd.Series:
    s = pd.to_numeric(raw, errors="coerce")
    good = s.notna() & s.map(lambda x: math.isfinite(float(x)) if pd.notna(x) else False)
    if int(good.sum()) < 3:
        return pd.Series([neutral] * len(s), index=s.index, dtype=float)
    out = pd.Series([neutral] * len(s), index=s.index, dtype=float)
    out.loc[good] = s.loc[good].rank(method="average", pct=True).mul(100.0)
    return out.clip(0, 100)


def _signed_flow_score(raw: pd.Series) -> pd.Series:
    """Relative flow score with sign discipline.

    Percentile controls market-cap scale. Sign stops a merely "less negative" stock
    from looking like strong institutional accumulation.
    """
    s = pd.to_numeric(raw, errors="coerce")
    pct = _percentile_score(s, 50.0)
    score = 50.0 + (pct - 50.0) * 0.72
    score = score.where(~(s > 0), score + 8.0)
    score = score.where(~(s < 0), score - 10.0)
    score = score.where(s.notna(), 50.0)
    return score.clip(0, 100)


def _raw_accel_score(raw: pd.Series) -> pd.Series:
    s = pd.to_numeric(raw, errors="coerce")
    pct = _percentile_score(s, 50.0)
    # tanh bounds very large percentage spikes while preserving sign and direction.
    absolute = s.fillna(0.0).map(lambda x: 50.0 + 42.0 * math.tanh(float(x) / 65.0))
    out = pct * 0.45 + absolute * 0.55
    out = out.where(s.notna(), 50.0)
    return out.clip(0, 100)


def _institutional_score(work: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    h66 = _series_num(work, "H66法人加速度分")
    h72 = _series_num(work, "H72法人需求模型分")
    proxy = _first_num_series(work, ["法人連買代理分數", "法人連買分數"], 50.0)

    flow_parts = []
    flow_weights = []
    for col, wt in [
        ("外資近1日買賣超", 0.20), ("外資近3日買賣超", 0.13), ("外資近5日買賣超", 0.16),
        ("投信近1日買賣超", 0.20), ("投信近3日買賣超", 0.13), ("投信近5日買賣超", 0.18),
    ]:
        if col in work.columns and pd.to_numeric(work[col], errors="coerce").notna().any():
            flow_parts.append(_signed_flow_score(work[col]))
            flow_weights.append(wt)
    if flow_parts:
        total = sum(flow_weights)
        flow = sum(s * (w / total) for s, w in zip(flow_parts, flow_weights))
        coverage = pd.Series([min(100.0, len(flow_parts) / 6.0 * 100.0)] * len(work), index=work.index)
    else:
        flow = pd.Series([50.0] * len(work), index=work.index)
        coverage = pd.Series([0.0] * len(work), index=work.index)

    score = (h66 * 0.31 + h72 * 0.24 + proxy * 0.15 + flow * 0.30).clip(0, 100)
    return score, coverage


def _capital_acceleration_score(work: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    parts = []
    weights = []
    for col, wt in [
        ("成交額3日加速度%", 0.29), ("成交額5日加速度%", 0.23),
        ("成交量3日加速度%", 0.22), ("成交量5日加速度%", 0.16),
    ]:
        if col in work.columns and pd.to_numeric(work[col], errors="coerce").notna().any():
            parts.append(_raw_accel_score(work[col])); weights.append(wt)
    if "當日量比" in work.columns and pd.to_numeric(work["當日量比"], errors="coerce").notna().any():
        ratio = pd.to_numeric(work["當日量比"], errors="coerce")
        parts.append((_percentile_score(ratio) * 0.6 + ratio.fillna(1.0).map(lambda x: _clip(50 + (float(x)-1.0)*26)) * 0.4).clip(0,100))
        weights.append(0.10)
    if not parts:
        return pd.Series([50.0] * len(work), index=work.index), pd.Series([0.0] * len(work), index=work.index)
    total = sum(weights)
    score = sum(s * (w / total) for s, w in zip(parts, weights)).clip(0, 100)
    coverage = pd.Series([min(100.0, len(parts) / 5.0 * 100.0)] * len(work), index=work.index)
    return score, coverage


def _holder_truth(work: pd.DataFrame) -> tuple[pd.Series, pd.Series, pd.Series]:
    holder_base = _series_num(work, "H72大戶鎖碼模型分")
    delta_col = "H60千張大戶週變化pp"
    delta = pd.to_numeric(work[delta_col], errors="coerce") if delta_col in work.columns else pd.Series([float("nan")]*len(work), index=work.index)
    current = pd.to_numeric(work.get("H60千張大戶持股比%", pd.Series([float("nan")]*len(work), index=work.index)), errors="coerce")
    scores=[]; states=[]; data_quality=[]
    for i in work.index:
        d = delta.loc[i] if i in delta.index else float("nan")
        b = float(holder_base.loc[i])
        cur = current.loc[i] if i in current.index else float("nan")
        if pd.notna(d) and math.isfinite(float(d)):
            d=float(d)
            dscore = _clip(50 + 50 * math.tanh(d / 0.65))
            score = _clip(b * 0.38 + dscore * 0.62)
            if d >= 0.30: state="LOCKING_CONFIRMED｜週增持"
            elif d >= 0.05: state="ACCUMULATING｜小幅增持"
            elif d > -0.05: state="FLAT｜持股近乎持平"
            else: state="DISTRIBUTING｜週減持"
            dq=100.0
        elif pd.notna(cur):
            # Critical truth correction: current ownership level != locking trend.
            score = min(52.0, 50.0 + (b - 50.0) * 0.12)
            state = "UNCONFIRMED_NO_PRIOR｜有當期持股但缺前期比較"
            dq=50.0
        else:
            score = 45.0
            state = "NO_TDCC_TRUTH｜缺真實大戶持股"
            dq=0.0
        scores.append(score); states.append(state); data_quality.append(dq)
    return pd.Series(scores,index=work.index), pd.Series(states,index=work.index), pd.Series(data_quality,index=work.index)


def apply_h74_fresh_mainstream_capital(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
    work = frame.copy().reset_index(drop=True)
    try:
        from godpick_h73_leadership_breadth_engine import VERSION as H73V, apply_h73_leadership_breadth
        hv = work.get("H73版本", pd.Series([""] * len(work))).fillna("").astype(str)
        if not hv.eq(H73V).all():
            work = apply_h73_leadership_breadth(work)
    except Exception:
        pass

    # 1) What is accelerating now? Avoid historical-quality dominance.
    strength = (
        _series_num(work,"H64真強勢分") * 0.25 +
        _series_num(work,"H72動能相對強度模型分") * 0.20 +
        _series_num(work,"H72突破時機模型分") * 0.16 +
        _series_num(work,"H66短線動能分") * 0.14 +
        _first_num_series(work,["H47個股相對強度分","H51個股領漲品質分"],50) * 0.13 +
        _series_num(work,"H73領先加速分") * 0.12
    ).clip(0,100)

    # 2) Is the *sector/mainstream* fresh today rather than merely high-quality?
    mainstream = (
        _series_num(work,"H64主流真相分") * 0.28 +
        _series_num(work,"H50族群新鮮度分") * 0.20 +
        _series_num(work,"H73族群廣度分") * 0.19 +
        _first_num_series(work,["H57主流形成前兆分","H57主流形成分"],50) * 0.17 +
        _series_num(work,"H66主流點火分") * 0.16
    ).clip(0,100)

    institutional, inst_cov = _institutional_score(work)
    capital, capital_cov = _capital_acceleration_score(work)
    holder, holder_state, holder_dq = _holder_truth(work)
    freshness = (
        _series_num(work,"今日訊號新鮮分") * 0.52 +
        _series_num(work,"H50族群新鮮度分") * 0.28 +
        _first_num_series(work,["H61新鮮機會分","H57提前視窗分"],50) * 0.20
    ).clip(0,100)

    repeat_base = _first_num_series(work,["H61重複慣性扣分","重複推薦慣性扣分"],0.0)
    near5 = _first_num_series(work,["近5次入榜次數","H61近5次入榜次數"],0.0,clip=False).clip(lower=0)
    renewed = (freshness >= 68) & (strength >= 66) & ((institutional >= 64) | (capital >= 68))
    repeat_pen = (repeat_base + (near5 - 2).clip(lower=0) * 1.7).clip(0,24)
    repeat_pen = repeat_pen.where(~renewed, repeat_pen * 0.42)

    # High historic quality with no fresh capital/mainstream evidence should not occupy the front.
    quality = _series_num(work,"H72品質獲利模型分")
    stale_mask = (quality >= 68) & (mainstream < 52) & (capital < 52) & (institutional < 57) & (freshness < 60)
    stale_pen = pd.Series([0.0]*len(work),index=work.index)
    stale_pen.loc[stale_mask] = ((quality.loc[stale_mask]-68)*0.20 + (52-mainstream.loc[stale_mask])*0.18 + (60-freshness.loc[stale_mask])*0.12).clip(0,12)

    # Explicit contradiction: distribution or weak institutions versus price spike.
    contradiction = pd.Series([0.0]*len(work),index=work.index)
    contradiction += ((strength>=70) & (institutional<42)).astype(float)*5.0
    contradiction += ((strength>=70) & (capital<42)).astype(float)*3.0
    contradiction += holder_state.astype(str).str.startswith("DISTRIBUTING").astype(float)*6.0

    # Fresh-money resonance is additive but bounded; it cannot manufacture Formal.
    resonance_count = ((strength>=66).astype(int)+(mainstream>=58).astype(int)+(institutional>=62).astype(int)+(capital>=62).astype(int)+(freshness>=60).astype(int))
    resonance = pd.Series([0.0]*len(work),index=work.index)
    resonance += (resonance_count>=4).astype(float)*3.0
    resonance += (resonance_count>=5).astype(float)*2.0

    structural = _series_num(work,"H72風險調整分")
    score = (
        strength*0.22 + mainstream*0.20 + institutional*0.20 + capital*0.16 +
        freshness*0.10 + holder*0.05 + structural*0.07 + resonance - repeat_pen - stale_pen - contradiction
    ).clip(0,100)
    pct = score.rank(method="average",pct=True).mul(100)
    rank = score.rank(method="first",ascending=False).astype("Int64")
    h73rank = pd.to_numeric(work.get("H73全市場順位",pd.Series([None]*len(work))),errors="coerce")
    rank_delta = h73rank - pd.to_numeric(rank,errors="coerce")

    tiers=[]; strengths=[]; warnings=[]; advice=[]
    for i,row in work.iterrows():
        sc=float(score.iat[i]); pv=float(pct.iat[i]); st=float(strength.iat[i]); ms=float(mainstream.iat[i]); ins=float(institutional.iat[i]); cap=float(capital.iat[i]); fr=float(freshness.iat[i]); rp=float(repeat_pen.iat[i]); sp=float(stale_pen.iat[i]); hs=_s(holder_state.iat[i])
        # F1/F2 are intentionally strict: current leadership + fresh money are mandatory.
        if sc>=72 and pv>=98 and st>=66 and ms>=56 and max(ins,cap)>=66 and fr>=55 and rp<=12 and not hs.startswith("DISTRIBUTING"):
            tier="F1｜新鮮主流資金核心"; adv="第一優先研究｜盤前重驗價格/主流延續；Formal仍看H64/H68"
        elif sc>=66 and pv>=93 and st>=61 and (ms>=53 or ins>=64 or cap>=68) and fr>=48:
            tier="F2｜資金加速優先"; adv="優先觀察｜確認法人/成交資金延續，不盲追"
        elif sc>=60 and pv>=80:
            tier="F3｜輪動候選"; adv="候選追蹤｜等待主流/資金再確認"
        else:
            tier="R0｜一般研究"; adv="一般研究｜不得視為正式推薦"
        good=[]
        if st>=70: good.append(f"真強勢加速{st:.0f}")
        if ms>=62: good.append(f"主流新鮮{ms:.0f}")
        if ins>=68: good.append(f"法人加速{ins:.0f}")
        if cap>=68: good.append(f"成交資金加速{cap:.0f}")
        if fr>=70: good.append(f"訊號新鮮{fr:.0f}")
        if hs.startswith(("LOCKING","ACCUMULATING")): good.append(hs.split("｜")[0])
        bad=[]
        if hs.startswith("UNCONFIRMED"): bad.append("TDCC缺前期，鎖碼未確認")
        elif hs.startswith("NO_TDCC"): bad.append("TDCC真相不足")
        elif hs.startswith("DISTRIBUTING"): bad.append("TDCC週減持")
        if rp>=8: bad.append(f"熟面孔慣性扣{rp:.1f}")
        if sp>=3: bad.append(f"陳舊品質扣{sp:.1f}")
        if ins<45 and st>=68: bad.append("價格強但法人未共振")
        if cap<45 and st>=68: bad.append("價格強但成交資金未加速")
        if ms<48: bad.append("非當日主流新鮮優先")
        tiers.append(tier); advice.append(adv); strengths.append("；".join(good) or "尚無足夠新鮮資金共振"); warnings.append("；".join(bad) or "無重大H74警示")

    work["H74強勢加速度分"] = strength.round(2)
    work["H74主流新鮮度分"] = mainstream.round(2)
    work["H74法人資金加速度分"] = institutional.round(2)
    work["H74成交資金加速度分"] = capital.round(2)
    work["H74大戶鎖碼真相分"] = holder.round(2)
    work["H74大戶鎖碼狀態"] = holder_state
    work["H74訊號新鮮分"] = freshness.round(2)
    work["H74熟面孔慣性扣分"] = repeat_pen.round(2)
    work["H74陳舊品質扣分"] = stale_pen.round(2)
    work["H74資金共振加分"] = resonance.round(2)
    work["H74決策總分"] = score.round(2)
    work["H74全市場百分位%"] = pct.round(2)
    work["H74全市場順位"] = rank
    work["H74研究層級"] = tiers
    work["H74相對H73順位變化"] = rank_delta.round(0).astype("Int64")
    work["H74主要優勢"] = strengths
    work["H74主要警示"] = warnings
    work["H74研究建議"] = advice
    work["H74學習快照狀態"] = "SNAPSHOT-READY"
    work["H74權威邊界"] = "H74只重排研究注意力；不得建立Formal，不得解除H64/H63/H68權威與執行否決。"
    work["H74版本"] = VERSION
    # Governance telemetry retained for summary, not promoted to user score columns.
    work["_H74法人原始流覆蓋%"] = inst_cov.round(2)
    work["_H74成交加速覆蓋%"] = capital_cov.round(2)
    work["_H74TDCC趨勢資料品質%"] = holder_dq.round(2)
    work["_H74新證據續命"] = renewed.map(lambda x: "YES｜新證據續命" if bool(x) else "NO｜依一般重複治理")
    return work


def build_h74_research_table(frame: pd.DataFrame, max_rows: int = 30, max_per_sector: int | None = 4) -> pd.DataFrame:
    w=apply_h74_fresh_mainstream_capital(frame)
    if w.empty:return w
    pri=w["H74研究層級"].fillna("").astype(str).map(lambda x:40 if x.startswith("F1") else 30 if x.startswith("F2") else 20 if x.startswith("F3") else 10)
    w=w.assign(_p=pri).sort_values(["_p","H74決策總分","H74全市場百分位%","H74法人資金加速度分","H74成交資金加速度分"],ascending=False,kind="mergesort")
    sec=next((c for c in ["族群名稱","類別","產業別"] if c in w.columns),None)
    if sec and max_per_sector:
        pick=[];cnt={}
        for idx,r in w.iterrows():
            s=_s(r.get(sec)) or "未分類"
            if cnt.get(s,0)>=max_per_sector: continue
            pick.append(idx);cnt[s]=cnt.get(s,0)+1
            if len(pick)>=max_rows: break
        w=w.loc[pick]
    else:w=w.head(max_rows)
    cols=[c for c in [
        "股票代號","股票名稱","市場別","族群名稱","類別","H74研究層級","H74決策總分","H74全市場百分位%","H74全市場順位",
        "H74強勢加速度分","H74主流新鮮度分","H74法人資金加速度分","H74成交資金加速度分","H74大戶鎖碼真相分","H74大戶鎖碼狀態","H74訊號新鮮分",
        "H74熟面孔慣性扣分","H74陳舊品質扣分","H74資金共振加分","H74相對H73順位變化","H74主要優勢","H74主要警示","H74研究建議",
        "H64有效權威","H68次日執行狀態","H73研究層級","H73研究排序分","H74權威邊界","H74版本"] if c in w.columns]
    return w[cols].reset_index(drop=True)


def build_h74_decision_overview(frame: pd.DataFrame, max_rows: int = 15) -> pd.DataFrame:
    """Excel/UI first-read table. One compact answer before detailed evidence sheets."""
    w=apply_h74_fresh_mainstream_capital(frame)
    if w.empty:return w
    pri=w["H74研究層級"].fillna("").astype(str).map(lambda x:40 if x.startswith("F1") else 30 if x.startswith("F2") else 20 if x.startswith("F3") else 10)
    w=w.assign(_p=pri).sort_values(["_p","H74決策總分","H74法人資金加速度分","H74成交資金加速度分"],ascending=False,kind="mergesort").head(max_rows).copy()
    w.insert(0,"AI決策順位",range(1,len(w)+1))
    authority = w.get("H64有效權威",pd.Series([""]*len(w),index=w.index)).fillna("").astype(str)
    w["是否正式推薦"] = authority.eq("EFFECTIVE-FORMAL").map({True:"是｜仍需H68可執行",False:"否｜研究排序"})
    cols=[c for c in [
        "AI決策順位","股票代號","股票名稱","市場別","族群名稱","類別","H74研究層級","H74決策總分",
        "H74強勢加速度分","H74主流新鮮度分","H74法人資金加速度分","H74成交資金加速度分","H74大戶鎖碼真相分","H74大戶鎖碼狀態",
        "H74訊號新鮮分","H74熟面孔慣性扣分","H74陳舊品質扣分","H74主要優勢","H74主要警示","H74研究建議",
        "H64有效權威","H68次日執行狀態","是否正式推薦"] if c in w.columns]
    return w[cols].reset_index(drop=True)


def build_h74_excel_guide() -> pd.DataFrame:
    return pd.DataFrame([
        {"優先序":1,"活頁":"AI決策總覽","你要看什麼":"每天先看這張：真正強勢＋主流新鮮＋法人/成交資金＋TDCC真相＋重複慣性後的單一研究順位","是否買進清單":"否｜先看是否正式推薦/H68執行狀態"},
        {"優先序":2,"活頁":"正式推薦作戰","你要看什麼":"唯一真正Formal作戰名單；沒有就明確空白，不為推薦而推薦","是否買進清單":"是，但仍需盤前價格/H68執行許可"},
        {"優先序":3,"活頁":"H74新鮮主流資金","你要看什麼":"AI決策總覽的完整Top30證據；適合檢查為何某股升/降順位","是否買進清單":"否｜F1/F2/F3是研究層"},
        {"優先序":4,"活頁":"主流族群","你要看什麼":"今天資金正在往哪些族群集中，確認個股不是單兵作戰","是否買進清單":"否｜族群證據"},
        {"優先序":5,"活頁":"系統健康與資料品質","你要看什麼":"官方因子、K線、TDCC與掃描覆蓋是否可信；資料不健康時不要解讀排名","是否買進清單":"否｜可信度稽核"},
        {"優先序":6,"活頁":"股神推薦總排名","你要看什麼":"需要追查所有欄位時才看；不是日常第一張","是否買進清單":"否｜完整研究證據庫"},
        {"優先序":7,"活頁":"H73/H72/H70/H67/H66/H65明細","你要看什麼":"模型拆解、回歸與稽核；平常不需要逐張看","是否買進清單":"否｜模型佐證"},
    ])


def build_h74_governance_summary(frame: pd.DataFrame) -> pd.DataFrame:
    w=apply_h74_fresh_mainstream_capital(frame)
    if w.empty:return pd.DataFrame({"治理項目":["H74"],"目前狀態":["無資料"],"治理原則":["不建立Formal"]})
    tier=w["H74研究層級"].fillna("").astype(str)
    hs=w["H74大戶鎖碼狀態"].fillna("").astype(str)
    return pd.DataFrame([
        {"治理項目":"F1新鮮主流資金核心","目前狀態":int(tier.str.startswith("F1").sum()),"治理原則":"真強勢＋主流新鮮＋法人/成交資金至少一項強共振；仍非Formal"},
        {"治理項目":"F2資金加速優先","目前狀態":int(tier.str.startswith("F2").sum()),"治理原則":"優先研究，盤前仍重驗"},
        {"治理項目":"TDCC鎖碼已確認","目前狀態":int(hs.str.startswith(("LOCKING","ACCUMULATING")).sum()),"治理原則":"必須有前期比較值，單一當期持股比例不算鎖碼"},
        {"治理項目":"TDCC缺前期比較","目前狀態":int(hs.str.startswith("UNCONFIRMED").sum()),"治理原則":"明確標示未確認，靜態大戶持股不得冒充趨勢"},
        {"治理項目":"熟面孔慣性扣分中位","目前狀態":round(float(pd.to_numeric(w["H74熟面孔慣性扣分"],errors="coerce").median()),2),"治理原則":"只有新鮮資金重新加速才可減免，不是永久封殺舊強股"},
        {"治理項目":"陳舊品質扣分檔數","目前狀態":int((pd.to_numeric(w["H74陳舊品質扣分"],errors="coerce").fillna(0)>0).sum()),"治理原則":"品質高但主流/資金/新鮮度弱時不得長期霸榜"},
        {"治理項目":"Formal/Execution權威","目前狀態":"UNCHANGED","治理原則":"H64/H63 Formal＋H68執行否決仍唯一權威；H74只重排研究注意力"},
    ])


__all__=["VERSION","H74_COLUMNS","apply_h74_fresh_mainstream_capital","build_h74_research_table","build_h74_decision_overview","build_h74_excel_guide","build_h74_governance_summary"]
