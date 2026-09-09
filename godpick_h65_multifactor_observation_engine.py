# -*- coding: utf-8 -*-
"""V191-H65 full-market multi-factor observation radar.

H65 does NOT weaken or replace H64 Formal authority.  Its job is different:
when the strict Formal gate legitimately returns zero names, the system should
still rank the best *research/observation* opportunities in the whole analysed
universe, explain why they are interesting, and state what is still missing
before they may become executable.

Design principles
-----------------
* Formal authority remains owned by H64/H63/H56/V188/Entry/RR.
* Ten independent positive pillars avoid one hot factor dominating the result.
* Missing data is neutral, never silently treated as strong; coverage is scored.
* Cross-sectional percentiles are used for raw flow/growth/liquidity metrics so
  the radar adapts to the current market rather than relying only on fixed cuts.
* Risk is an explicit penalty and can cap the research tier.
* The radar may always show relative candidates, but R0 is clearly labelled as
  research-only when no stock is good enough for W1/W2/W3.
"""
from __future__ import annotations

from typing import Any, Iterable
import math
import pandas as pd

VERSION = "v191_h65_multifactor_observation_radar_20260909"

PILLAR_WEIGHTS = {
    "主流": 13.0,
    "趨勢": 14.0,
    "法人": 14.0,
    "大戶": 10.0,
    "營收": 10.0,
    "獲利EPS": 9.0,
    "估值": 7.0,
    "技術": 10.0,
    "量能流動性": 6.0,
    "催化加速度": 7.0,
}

H65_COLUMNS = [
    "H65主流分", "H65趨勢分", "H65法人分", "H65大戶分", "H65營收分",
    "H65獲利EPS分", "H65估值分", "H65技術分", "H65量能流動性分", "H65催化加速度分",
    "H65風險扣分", "H65資料覆蓋%", "H65有效支柱數", "H65多因子觀察分",
    "H65全市場觀察百分位%", "H65觀察層級", "H65觀察推薦", "H65升級缺口",
    "H65觀察理由", "H65權威邊界", "H65版本",
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


def _first_col(frame: pd.DataFrame, names: Iterable[str]) -> str | None:
    for c in names:
        if c in frame.columns:
            return c
    return None


def _row_num(row: pd.Series, names: Iterable[str], default: float | None = None) -> float | None:
    for c in names:
        if c in row.index:
            x = _f(row.get(c), None)
            if x is not None:
                return x
    return default


def _row_txt(row: pd.Series, names: Iterable[str], default: str = "") -> str:
    for c in names:
        if c in row.index:
            t = _s(row.get(c))
            if t:
                return t
    return default


def _has_any(row: pd.Series, names: Iterable[str]) -> bool:
    for c in names:
        if c in row.index and _s(row.get(c)):
            return True
    return False


def _mean(values: Iterable[float | None], default: float = 50.0) -> float:
    xs = [float(x) for x in values if x is not None and math.isfinite(float(x))]
    return _clip(sum(xs) / len(xs)) if xs else float(default)


def _weighted(values: list[tuple[float | None, float]], default: float = 50.0) -> float:
    valid = [(float(v), float(w)) for v, w in values if v is not None and math.isfinite(float(v)) and w > 0]
    if not valid:
        return float(default)
    den = sum(w for _, w in valid)
    return _clip(sum(v * w for v, w in valid) / den)


def _score_from_pct(pct: float | None) -> float | None:
    return _clip(float(pct)) if pct is not None else None


def _numeric_percentile(frame: pd.DataFrame, names: Iterable[str], invert: bool = False) -> pd.Series:
    col = _first_col(frame, names)
    if not col:
        return pd.Series([float("nan")] * len(frame), index=frame.index, dtype="float64")
    s = pd.to_numeric(frame[col].astype(str).str.replace(",", "", regex=False).str.replace("%", "", regex=False), errors="coerce")
    out = s.rank(method="average", pct=True).mul(100.0)
    if invert:
        out = 100.0 - out
    return out


def _pe_score(v: float | None) -> float | None:
    if v is None:
        return None
    if v <= 0:
        return 28.0
    if 8 <= v <= 22:
        return 88.0
    if 22 < v <= 30:
        return 76.0
    if 5 <= v < 8:
        return 70.0
    if 30 < v <= 40:
        return 58.0
    if 40 < v <= 55:
        return 42.0
    return 30.0


def _pbr_score(v: float | None) -> float | None:
    if v is None or v <= 0:
        return None
    if 0.7 <= v <= 2.5:
        return 82.0
    if 2.5 < v <= 4.0:
        return 68.0
    if 0.3 <= v < 0.7:
        return 65.0
    if 4.0 < v <= 6.0:
        return 50.0
    return 35.0


def _rsi_score(v: float | None) -> float | None:
    if v is None:
        return None
    if 50 <= v <= 68:
        return 86.0
    if 42 <= v < 50:
        return 70.0
    if 68 < v <= 76:
        return 66.0
    if 35 <= v < 42:
        return 55.0
    if v > 82:
        return 25.0
    return 42.0


def _volume_ratio_score(v: float | None) -> float | None:
    if v is None:
        return None
    if 1.2 <= v <= 2.8:
        return 86.0
    if 0.85 <= v < 1.2:
        return 66.0
    if 2.8 < v <= 4.5:
        return 68.0
    if v > 6.0:
        return 42.0
    return 50.0


def _tier_priority(text: str) -> int:
    t = _s(text)
    if t.startswith("F1"):
        return 50
    if t.startswith("W1"):
        return 40
    if t.startswith("W2"):
        return 30
    if t.startswith("W3"):
        return 20
    return 10


def apply_h65_multifactor_observation(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()

    work = frame.copy().reset_index(drop=True)
    if "H64版本" not in work.columns:
        try:
            from godpick_h64_core_truth_engine import apply_h64_core_truth
            work = apply_h64_core_truth(work)
        except Exception:
            pass

    # Cross-sectional raw-factor ranks.  They are intentionally calculated once
    # on the full candidate universe, before any top-N filtering.
    pct = {
        "foreign": _numeric_percentile(work, ["外資近5日買賣超", "外資近5日買超", "外資近1日買賣超"]),
        "trust": _numeric_percentile(work, ["投信近5日買賣超", "投信近5日買超", "投信近1日買賣超"]),
        "inst": _numeric_percentile(work, ["三大法人近5日合計", "三大法人近5日買賣超", "三大法人近1日合計"]),
        "rev_yoy": _numeric_percentile(work, ["月營收YoY%", "單月營收YoY%", "營收年增率%"]),
        "rev_mom": _numeric_percentile(work, ["月營收MoM%", "單月營收MoM%", "營收月增率%"]),
        "cum_rev": _numeric_percentile(work, ["累計營收YoY%", "累計營收年增率%"]),
        "eps": _numeric_percentile(work, ["估算EPS", "EPS", "每股盈餘"]),
        "eps_growth": _numeric_percentile(work, ["EPS成長率%", "EPS年增率%", "EPS成長分數"]),
        "roe": _numeric_percentile(work, ["ROE%", "股東權益報酬率%"]),
        "amount": _numeric_percentile(work, ["流動性參考成交額百萬", "成交額百萬", "平均成交額百萬"]),
        "ret5": _numeric_percentile(work, ["近5日漲幅%", "5日績效%", "近5日報酬%"]),
        "ret20": _numeric_percentile(work, ["近20日漲幅%", "20日績效%", "近20日報酬%"]),
    }

    rows: list[dict[str, Any]] = []
    pillar_scores: list[dict[str, float]] = []
    meta_rows: list[dict[str, Any]] = []

    for idx, row in work.iterrows():
        # 1. Mainstream / sector leadership
        mainstream_inputs = [
            (_row_num(row, ["H64主流真相分"]), 0.26),
            (_row_num(row, ["H51族群主線分"]), 0.22),
            (_row_num(row, ["H53族群共振分"]), 0.18),
            (_row_num(row, ["H53族群廣度分"]), 0.10),
            (_row_num(row, ["H57主流形成前兆分"]), 0.12),
            (_row_num(row, ["H60主升段分"]), 0.12),
        ]
        main_score = _weighted(mainstream_inputs)
        main_avail = _has_any(row, ["H64主流真相分", "H51族群主線分", "H53族群共振分", "H57主流形成前兆分", "H60主升段分"])

        # 2. Price trend / relative strength
        trend_inputs = [
            (_row_num(row, ["H64真強勢分"]), 0.24),
            (_row_num(row, ["H47個股相對強度分"]), 0.19),
            (_row_num(row, ["H51個股領漲品質分"]), 0.17),
            (_row_num(row, ["強勢動能分", "技術結構分數"]), 0.14),
            (_score_from_pct(pct["ret5"].get(idx) if idx in pct["ret5"].index and pd.notna(pct["ret5"].get(idx)) else None), 0.13),
            (_score_from_pct(pct["ret20"].get(idx) if idx in pct["ret20"].index and pd.notna(pct["ret20"].get(idx)) else None), 0.13),
        ]
        trend_score = _weighted(trend_inputs)
        trend_avail = _has_any(row, ["H64真強勢分", "H47個股相對強度分", "H51個股領漲品質分", "強勢動能分", "技術結構分數", "近5日漲幅%", "5日績效%"])

        # 3. Institutional / foreign / trust flows
        inst_score = _weighted([
            (_row_num(row, ["法人籌碼官方分數", "法人籌碼分數"]), 0.28),
            (_score_from_pct(pct["foreign"].get(idx) if pd.notna(pct["foreign"].get(idx, float("nan"))) else None), 0.22),
            (_score_from_pct(pct["trust"].get(idx) if pd.notna(pct["trust"].get(idx, float("nan"))) else None), 0.18),
            (_score_from_pct(pct["inst"].get(idx) if pd.notna(pct["inst"].get(idx, float("nan"))) else None), 0.18),
            (_clip((_row_num(row, ["法人連買天數"], 0.0) or 0.0) * 12.0 + 34.0) if _has_any(row, ["法人連買天數"]) else None, 0.14),
        ])
        inst_avail = _has_any(row, ["法人籌碼官方分數", "法人籌碼分數", "外資近5日買賣超", "外資近1日買賣超", "投信近5日買賣超", "投信近1日買賣超", "三大法人近5日合計", "三大法人近1日合計", "法人連買天數"])

        # 4. Major-holder / TDCC lock trend
        lock_state = _row_txt(row, ["H64鎖碼趨勢狀態", "H60大戶鎖碼層級"])
        holder_score = _weighted([
            (_row_num(row, ["H64鎖碼確認分"]), 0.55),
            (_row_num(row, ["H60大戶鎖碼真相分", "大戶鎖碼分數"]), 0.30),
            (_clip(50.0 + (_row_num(row, ["H60千張大戶週變化pp"], 0.0) or 0.0) * 18.0) if _has_any(row, ["H60千張大戶週變化pp"]) else None, 0.15),
        ])
        if lock_state.startswith("LC"):
            holder_score = max(holder_score, 78.0)
        elif lock_state.startswith("LD"):
            holder_score = min(holder_score, 38.0)
        holder_avail = _has_any(row, ["H64鎖碼確認分", "H60大戶鎖碼真相分", "H60千張大戶持股比%", "H60千張大戶週變化pp", "大戶鎖碼分數"])

        # 5. Monthly/cumulative revenue growth
        revenue_score = _weighted([
            (_row_num(row, ["營收成長官方分數", "營收成長分數"]), 0.40),
            (_score_from_pct(pct["rev_yoy"].get(idx) if pd.notna(pct["rev_yoy"].get(idx, float("nan"))) else None), 0.28),
            (_score_from_pct(pct["rev_mom"].get(idx) if pd.notna(pct["rev_mom"].get(idx, float("nan"))) else None), 0.12),
            (_score_from_pct(pct["cum_rev"].get(idx) if pd.notna(pct["cum_rev"].get(idx, float("nan"))) else None), 0.20),
        ])
        revenue_avail = _has_any(row, ["營收成長官方分數", "營收成長分數", "月營收YoY%", "月營收MoM%", "累計營收YoY%"])

        # 6. Profitability / EPS quality
        profit_score = _weighted([
            (_row_num(row, ["官方基本面成長分數", "基本面成長分數"]), 0.32),
            (_row_num(row, ["EPS成長分數"]), 0.24),
            (_score_from_pct(pct["eps_growth"].get(idx) if pd.notna(pct["eps_growth"].get(idx, float("nan"))) else None), 0.18),
            (_score_from_pct(pct["roe"].get(idx) if pd.notna(pct["roe"].get(idx, float("nan"))) else None), 0.14),
            (_score_from_pct(pct["eps"].get(idx) if pd.notna(pct["eps"].get(idx, float("nan"))) else None), 0.12),
        ])
        profit_avail = _has_any(row, ["官方基本面成長分數", "基本面成長分數", "EPS成長分數", "EPS成長率%", "估算EPS", "EPS", "ROE%"])

        # 7. Valuation.  A field explicitly named 'risk' is inverted.
        valuation_risk = _row_num(row, ["官方估值風險分數", "估值風險分數"])
        valuation_score = _weighted([
            (100.0 - _clip(valuation_risk) if valuation_risk is not None else None, 0.38),
            (_pe_score(_row_num(row, ["PER本益比", "本益比", "PE"])), 0.34),
            (_pbr_score(_row_num(row, ["PBR股價淨值比", "股價淨值比"])), 0.16),
            (_clip(45.0 + (_row_num(row, ["股利殖利率%"], 0.0) or 0.0) * 7.0) if _has_any(row, ["股利殖利率%"]) else None, 0.12),
        ])
        valuation_avail = _has_any(row, ["官方估值風險分數", "估值風險分數", "PER本益比", "本益比", "PBR股價淨值比", "股利殖利率%"])

        # 8. Technical timing / entry quality
        technical_score = _weighted([
            (_row_num(row, ["Entry進場買點分", "進場可執行分", "實戰買點分數"]), 0.28),
            (_row_num(row, ["技術結構分數", "H51Pivot起漲分"]), 0.24),
            (_row_num(row, ["H51量價確認分", "起漲前兆分數"]), 0.18),
            (_rsi_score(_row_num(row, ["RSI14", "RSI"])), 0.12),
            (_row_num(row, ["強勢前兆分", "H57相對強度轉折分"]), 0.18),
        ])
        technical_avail = _has_any(row, ["Entry進場買點分", "進場可執行分", "實戰買點分數", "技術結構分數", "H51Pivot起漲分", "H51量價確認分", "RSI14", "RSI", "強勢前兆分", "H57相對強度轉折分"])

        # 9. Volume / liquidity.  Extreme volume ratio is not automatically good.
        liquidity_score = _weighted([
            (_row_num(row, ["H51流動性分"]), 0.38),
            (_score_from_pct(pct["amount"].get(idx) if pd.notna(pct["amount"].get(idx, float("nan"))) else None), 0.36),
            (_volume_ratio_score(_row_num(row, ["量比", "成交量比"])), 0.26),
        ])
        liquidity_avail = _has_any(row, ["H51流動性分", "流動性參考成交額百萬", "成交額百萬", "平均成交額百萬", "量比", "成交量比"])

        # 10. Catalysts / acceleration / emerging-leader evidence
        catalyst_score = _weighted([
            (_row_num(row, ["H57資金加速度分"]), 0.22),
            (_row_num(row, ["H57飆股發動前兆分", "H51發動潛力分"]), 0.20),
            (_row_num(row, ["H62新領漲分"]), 0.18),
            (_row_num(row, ["H62增量上漲空間分"]), 0.14),
            (_row_num(row, ["H55催化代理分"]), 0.12),
            (_row_num(row, ["今日訊號新鮮分"]), 0.14),
        ])
        catalyst_avail = _has_any(row, ["H57資金加速度分", "H57飆股發動前兆分", "H51發動潛力分", "H62新領漲分", "H62增量上漲空間分", "H55催化代理分", "今日訊號新鮮分"])

        scores = {
            "主流": main_score, "趨勢": trend_score, "法人": inst_score, "大戶": holder_score,
            "營收": revenue_score, "獲利EPS": profit_score, "估值": valuation_score, "技術": technical_score,
            "量能流動性": liquidity_score, "催化加速度": catalyst_score,
        }
        avail = {
            "主流": main_avail, "趨勢": trend_avail, "法人": inst_avail, "大戶": holder_avail,
            "營收": revenue_avail, "獲利EPS": profit_avail, "估值": valuation_avail, "技術": technical_avail,
            "量能流動性": liquidity_avail, "催化加速度": catalyst_avail,
        }
        avail_weight = sum(PILLAR_WEIGHTS[k] for k, ok in avail.items() if ok)
        coverage = _clip(avail_weight)  # weights intentionally sum to 100
        active_pillars = sum(1 for ok in avail.values() if ok)
        weighted_sum = sum(scores[k] * PILLAR_WEIGHTS[k] for k, ok in avail.items() if ok)
        normalized = weighted_sum / avail_weight if avail_weight > 0 else 50.0
        coverage_adjusted = normalized * (0.76 + 0.24 * coverage / 100.0)

        # Explicit risk penalty.  Positive Risk風控安全分 reduces the penalty;
        # fields named risk/exhaustion/chase increase it.
        exhaust = _row_num(row, ["H54耗竭風險分", "隔日耗竭風險分"], 50.0) or 50.0
        chase = _row_num(row, ["追價風險分"], 50.0) or 50.0
        risk_safe = _row_num(row, ["Risk風控安全分"], 50.0) or 50.0
        risk_penalty = max(0.0, exhaust - 68.0) * 0.16 + max(0.0, chase - 68.0) * 0.12 + max(0.0, 52.0 - risk_safe) * 0.10
        risk_text = "｜".join([
            _row_txt(row, ["高分禁買旗標"]), _row_txt(row, ["正式推薦排除原因"]),
            _row_txt(row, ["K線資料新鮮度"]), _row_txt(row, ["股神資料總新鮮度"]),
            _row_txt(row, ["V188交易許可"]),
        ])
        stale = any(k in risk_text for k in ["過期", "落後", "待更新"])
        hard_block = any(k in risk_text for k in ["禁買", "BLOCK", "正式排除"])
        if stale:
            risk_penalty += 8.0
        if hard_block:
            risk_penalty += 7.0
        if liquidity_avail and liquidity_score < 35:
            risk_penalty += 4.0
        risk_penalty = min(22.0, risk_penalty)

        final_score = _clip(coverage_adjusted - risk_penalty)
        pillar_scores.append(scores)
        meta_rows.append({
            "coverage": coverage, "active_pillars": active_pillars, "risk_penalty": risk_penalty,
            "final_score": final_score, "stale": stale, "hard_block": hard_block,
            "avail": avail, "lock_state": lock_state,
        })

    final_series = pd.Series([m["final_score"] for m in meta_rows], index=work.index, dtype="float64")
    pct_final = final_series.rank(method="average", pct=True).mul(100.0)

    for pos, idx in enumerate(work.index):
        row = work.loc[idx]
        scores = pillar_scores[pos]
        m = meta_rows[pos]
        percentile = float(pct_final.loc[idx]) if idx in pct_final.index else 0.0
        coverage = float(m["coverage"])
        active = int(m["active_pillars"])
        score = float(m["final_score"])
        risk_penalty = float(m["risk_penalty"])
        eff = _row_txt(row, ["H64有效權威"])
        h64_tier = _row_txt(row, ["H64研究層級"])

        if eff == "EFFECTIVE-FORMAL":
            tier = "F1｜H64正式推薦同步"
            rec = "正式推薦以H64作戰規則執行"
        elif score >= 70 and percentile >= 85 and coverage >= 55 and active >= 6 and not m["stale"] and not m["hard_block"]:
            tier = "W1｜重點觀察推薦"
            rec = "是｜重點觀察；等待Formal/Entry/RR升級"
        elif score >= 62 and percentile >= 70 and coverage >= 45 and active >= 5 and not m["stale"]:
            tier = "W2｜提前卡位觀察"
            rec = "是｜提前卡位觀察；不可冒充正式推薦"
        elif score >= 55 and percentile >= 55 and coverage >= 35 and active >= 4 and not m["stale"]:
            tier = "W3｜候選追蹤"
            rec = "是｜候選追蹤；等關鍵因子轉強"
        else:
            tier = "R0｜相對優先研究"
            rec = "否｜僅相對排名，不構成觀察推薦"

        # Risk caps: a stock may remain visible for research, but high-risk or a
        # formal block cannot retain a top observation label.
        if tier.startswith("W1") and (risk_penalty >= 12 or h64_tier.startswith("D0")):
            tier = "W2｜提前卡位觀察"
            rec = "是｜提前卡位觀察；風險/H64核心尚未完成"
        if tier.startswith(("W1", "W2")) and (risk_penalty >= 16 or m["hard_block"]):
            tier = "W3｜候選追蹤"
            rec = "是｜候選追蹤；風險解除前不可升級"
        if m["stale"]:
            tier = "R0｜資料待更新研究"
            rec = "否｜資料待更新，不構成觀察推薦"

        # Explain top strengths and concrete upgrade gaps.
        top_strengths = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)[:3]
        weak = sorted(scores.items(), key=lambda kv: kv[1])
        gaps: list[str] = []
        missing = [k for k, ok in m["avail"].items() if not ok]
        if coverage < 55:
            gaps.append(f"資料覆蓋僅{coverage:.0f}%")
        for k, v in weak:
            if v < 58 and k not in missing and len(gaps) < 4:
                gaps.append(f"{k}{v:.0f}偏弱")
        if m["lock_state"].startswith(("LU", "LP", "L0", "LW", "LD")) and len(gaps) < 4:
            gaps.append("TDCC鎖碼趨勢未確認")
        if eff != "EFFECTIVE-FORMAL" and len(gaps) < 4:
            gaps.append("H64尚未取得有效Formal")
        if missing and len(gaps) < 4:
            gaps.append("缺" + "/".join(missing[:3]) + "資料")
        if risk_penalty >= 8 and len(gaps) < 4:
            gaps.append(f"風險扣分{risk_penalty:.1f}")
        if not gaps:
            gaps.append("等待H56盤前、Entry觸發、守價與RR確認")

        strength_text = "、".join(f"{k}{v:.0f}" for k, v in top_strengths)
        reason = (
            f"多因子{score:.1f}/全市場P{percentile:.1f}/覆蓋{coverage:.0f}%/{active}支柱；"
            f"優勢={strength_text}；風險扣分={risk_penalty:.1f}；H64={eff or h64_tier or '未授權'}。"
        )

        rows.append({
            "H65主流分": round(scores["主流"], 2),
            "H65趨勢分": round(scores["趨勢"], 2),
            "H65法人分": round(scores["法人"], 2),
            "H65大戶分": round(scores["大戶"], 2),
            "H65營收分": round(scores["營收"], 2),
            "H65獲利EPS分": round(scores["獲利EPS"], 2),
            "H65估值分": round(scores["估值"], 2),
            "H65技術分": round(scores["技術"], 2),
            "H65量能流動性分": round(scores["量能流動性"], 2),
            "H65催化加速度分": round(scores["催化加速度"], 2),
            "H65風險扣分": round(risk_penalty, 2),
            "H65資料覆蓋%": round(coverage, 2),
            "H65有效支柱數": active,
            "H65多因子觀察分": round(score, 2),
            "H65全市場觀察百分位%": round(percentile, 2),
            "H65觀察層級": tier,
            "H65觀察推薦": rec,
            "H65升級缺口": "；".join(gaps[:4]),
            "H65觀察理由": reason,
            "H65權威邊界": "H65只做研究/觀察排序；正式推薦仍須H64有效Formal＋H56盤前＋Entry/守價＋RR。",
            "H65版本": VERSION,
        })

    addon = pd.DataFrame(rows, index=work.index)
    for c in H65_COLUMNS:
        work[c] = addon[c]
    return work


def build_h65_observation_radar_table(frame: pd.DataFrame, max_rows: int = 20, max_per_sector: int = 3) -> pd.DataFrame:
    """Return a diversified observation radar.

    It never returns an empty table solely because Formal is empty.  If no name
    meets W1/W2/W3, the best relative R0 rows remain visible and are explicitly
    marked research-only.
    """
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return pd.DataFrame({
            "H65觀察層級": ["R0｜目前沒有可排序候選"],
            "H65觀察推薦": ["否"],
            "H65升級缺口": ["請先完成全市場掃描與資料更新"],
        })
    work = frame if ("H65版本" in frame.columns and frame["H65版本"].astype(str).eq(VERSION).all()) else apply_h65_multifactor_observation(frame)
    out = work.copy()
    out["_H65tier"] = out.get("H65觀察層級", pd.Series([""] * len(out), index=out.index)).astype(str).map(_tier_priority)
    _sort_cols = ["_H65tier", "H65多因子觀察分", "H65全市場觀察百分位%", "H65資料覆蓋%", "H64核心共振分"]
    for _c in _sort_cols:
        if _c not in out.columns:
            out[_c] = 0.0
    out.sort_values(_sort_cols, ascending=False, kind="mergesort", inplace=True)

    target = max(1, int(max_rows or 20))
    cap = max(1, int(max_per_sector or 3))
    chosen: list[pd.Series] = []
    seen: set[str] = set()
    counts: dict[str, int] = {}
    for _, r in out.iterrows():
        code = _row_txt(r, ["股票代號", "代號"])
        if not code or code in seen:
            continue
        sector = _row_txt(r, ["類別", "族群名稱", "產業"], "未分類")
        if counts.get(sector, 0) >= cap:
            continue
        chosen.append(r); seen.add(code); counts[sector] = counts.get(sector, 0) + 1
        if len(chosen) >= target:
            break
    if len(chosen) < target:
        for _, r in out.iterrows():
            code = _row_txt(r, ["股票代號", "代號"])
            if not code or code in seen:
                continue
            chosen.append(r); seen.add(code)
            if len(chosen) >= target:
                break
    res = pd.DataFrame(chosen).reset_index(drop=True) if chosen else out.head(target).copy().reset_index(drop=True)
    res["H65觀察順位"] = range(1, len(res) + 1)
    res.drop(columns=["_H65tier"], errors="ignore", inplace=True)
    front = [c for c in [
        "H65觀察順位", "股票代號", "股票名稱", "類別", "H65觀察層級", "H65觀察推薦",
        "H65多因子觀察分", "H65全市場觀察百分位%", "H65資料覆蓋%", "H65有效支柱數",
        "H65主流分", "H65趨勢分", "H65法人分", "H65大戶分", "H65營收分", "H65獲利EPS分",
        "H65估值分", "H65技術分", "H65量能流動性分", "H65催化加速度分", "H65風險扣分",
        "H65升級缺口", "H65觀察理由", "H64研究層級", "H64有效權威", "H64品質閘門",
        "法人連買天數", "外資近5日買賣超", "投信近5日買賣超", "三大法人近5日合計",
        "月營收YoY%", "月營收MoM%", "累計營收YoY%", "估算EPS", "PER本益比", "PBR股價淨值比",
        "H60千張大戶持股比%", "H60千張大戶週變化pp", "H65權威邊界",
    ] if c in res.columns]
    rest = [c for c in res.columns if c not in front]
    return res[front + rest]


_INDICATOR_CATALOG = [
    ("市場環境", "大盤/櫃買趨勢、漲跌家數、創高創低、成交額、波動、期貨、外資期貨、美元台幣、SOX/Nasdaq/VIX/利率", ["大盤資料新鮮度", "市場環境分數", "大盤橋接分數", "NASDAQ漲跌%", "費半漲跌%", "台指夜盤漲跌"]),
    ("主流族群", "族群1/5/20日相對強弱、資金占比、廣度、攻擊度、領漲集群、生命週期、主流共振", ["H51族群主線分", "H53族群共振分", "H53族群廣度分", "H53族群攻擊分", "H57主流形成前兆分"]),
    ("價格趨勢", "MA5/10/20/60/120/240位置與斜率、相對強度、5/20/60日報酬、52週高低、突破/回測", ["H64真強勢分", "H47個股相對強度分", "H51個股領漲品質分", "近5日漲幅%", "技術結構分數"]),
    ("量能流動性", "量比、均量、成交額、週轉率、OBV、價量背離、縮量回檔、放量突破", ["H51流動性分", "量比", "成交額百萬", "流動性參考成交額百萬", "H51量價確認分"]),
    ("法人籌碼", "外資/投信/自營商1/3/5/10/20日買賣超、連買天數、買超占量比、三大法人共振", ["法人籌碼官方分數", "法人連買天數", "外資近5日買賣超", "投信近5日買賣超", "三大法人近5日合計"]),
    ("大戶TDCC", "400/1000張級距持股、週變化、集中度、股東人數變化、內部人持股、董監質押", ["H60千張大戶持股比%", "H60千張大戶週變化pp", "H60大戶鎖碼真相分", "H64鎖碼確認分"]),
    ("融資融券借券", "融資餘額/增減、融券、券資比、借券賣出、當沖、軋空/回補風險", ["融資增減", "融券增減", "券資比%", "借券賣出餘額"]),
    ("營收成長", "月營收MoM/YoY、累計YoY、3/6/12月趨勢、歷史新高、成長加速度", ["營收成長官方分數", "月營收YoY%", "月營收MoM%", "累計營收YoY%"]),
    ("獲利EPS", "季/TTM EPS、QoQ/YoY、毛利/營益/淨利率、ROE/ROA、營業現金流、自由現金流、應收/存貨品質", ["官方基本面成長分數", "基本面成長分數", "EPS成長分數", "估算EPS", "ROE%"]),
    ("估值", "PE(TTM/Forward)、PEG、PBR、PS、EV/EBITDA、FCF殖利率、股息率、歷史/同業估值百分位", ["官方估值風險分數", "PER本益比", "PBR股價淨值比", "股利殖利率%"]),
    ("財務體質", "負債比、淨現金/淨負債、流動/速動比、利息保障倍數、現金轉換週期", ["負債比%", "流動比率%", "速動比率%", "利息保障倍數"]),
    ("技術買點", "RSI、MACD、KD、ADX、布林、ATR、VWAP、支撐壓力、Pivot、型態、相對量", ["RSI14", "Entry進場買點分", "H51Pivot起漲分", "H57相對強度轉折分", "強勢前兆分"]),
    ("事件催化", "財報驚喜/展望、訂單、擴產、法說、庫藏股、內部人買進、指數/ETF納入、產品/產能/產業事件", ["H57資金加速度分", "H57飆股發動前兆分", "H55催化代理分", "今日訊號新鮮分"]),
    ("風險治理", "過熱、跳空、長上影、假突破、ATR/波動、Beta、低流動性、異常交易、法規/事件/地緣風險", ["H54耗竭風險分", "追價風險分", "Risk風控安全分", "高分禁買旗標", "正式推薦排除原因"]),
    ("AI績效學習", "Selection Alpha、T+1命中、MFE/MAE、滑價、RR、重複推薦疲勞、Brier/校準、資料新鮮度", ["H61近期SelectionAlpha%", "H61近期成熟樣本", "SuperAI執行風報比", "重複推薦校正分", "股神資料總新鮮度"]),
]


def build_h65_indicator_coverage_table(frame: pd.DataFrame) -> pd.DataFrame:
    """Self-audit of the broader indicator universe H65 should observe."""
    work = frame if isinstance(frame, pd.DataFrame) else pd.DataFrame()
    rows = []
    for category, target, aliases in _INDICATOR_CATALOG:
        available = [c for c in aliases if c in work.columns]
        if work.empty or not available:
            coverage = 0.0
        else:
            masks = []
            for c in available:
                masks.append(work[c].map(lambda v: bool(_s(v))))
            combined = masks[0].copy()
            for m in masks[1:]:
                combined = combined | m
            coverage = float(combined.mean() * 100.0) if len(combined) else 0.0
        rows.append({
            "指標群": category,
            "應觀察指標": target,
            "目前可用欄位": "、".join(available) if available else "尚未接入/欄位未命中",
            "候選資料覆蓋%": round(coverage, 1),
            "H65處理方式": "直接納入多因子支柱" if category in {"主流族群", "價格趨勢", "量能流動性", "法人籌碼", "大戶TDCC", "營收成長", "獲利EPS", "估值", "技術買點", "事件催化"} else "治理/擴充觀察層",
        })
    return pd.DataFrame(rows)


__all__ = [
    "VERSION", "PILLAR_WEIGHTS", "H65_COLUMNS", "apply_h65_multifactor_observation",
    "build_h65_observation_radar_table", "build_h65_indicator_coverage_table",
]
