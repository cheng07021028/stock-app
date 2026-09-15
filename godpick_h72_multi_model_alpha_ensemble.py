# -*- coding: utf-8 -*-
"""V191-H72 Multi-Model Alpha Ensemble.

Research concepts adapted into one explainable ensemble (not copied proprietary rules):
- Growth / earnings acceleration: CAN SLIM-style current + annual growth discipline.
- Quality: AQR QMJ / Piotroski / Novy-Marx-inspired profitability, stability and balance-sheet quality proxies.
- Institutional demand: TWSE/TPEx foreign, investment-trust and dealer accumulation.
- Holder concentration: Taiwan-specific TDCC large-holder concentration / lock-up trend.
- Value-growth efficiency: earnings yield, P/B, dividend yield and growth-vs-valuation balance.
- Momentum / relative strength: medium-term winner persistence and cross-sectional relative strength.
- Breakout timing: price/volume, close quality, ignition and anti-chase controls.
- Risk / regime fit: market breadth/regime, sector flow, signal consensus and counter-regime survival.

Authority boundary
------------------
H72 is a research-ranking ensemble.  It MUST NOT create Formal authority or bypass
H68 execution veto.  H64/H63 remain Formal truth; H68 remains next-session execution truth.
H72 snapshots are persisted for forward-only learning; no historical backfill may invent ranks.
"""
from __future__ import annotations

from typing import Any, Iterable
import math
import statistics
import pandas as pd

VERSION = "v191_h72_multi_model_alpha_ensemble_20260915"

MODEL_KEYS = ["成長動能", "品質獲利", "法人需求", "大戶鎖碼", "估值效率", "動能相對強度", "突破時機", "風險Regime"]

WEIGHTS = {
    "ATTACK": {"成長動能":15, "品質獲利":12, "法人需求":17, "大戶鎖碼":9, "估值效率":7, "動能相對強度":17, "突破時機":15, "風險Regime":8},
    "NEUTRAL": {"成長動能":16, "品質獲利":15, "法人需求":16, "大戶鎖碼":10, "估值效率":10, "動能相對強度":14, "突破時機":11, "風險Regime":8},
    "DEFENSIVE": {"成長動能":16, "品質獲利":18, "法人需求":16, "大戶鎖碼":10, "估值效率":12, "動能相對強度":9, "突破時機":7, "風險Regime":12},
}

H72_COLUMNS = [
    "H72成長動能模型分", "H72品質獲利模型分", "H72法人需求模型分", "H72大戶鎖碼模型分",
    "H72估值效率模型分", "H72動能相對強度模型分", "H72突破時機模型分", "H72風險Regime模型分",
    "H72模型覆蓋%", "H72共振模型數", "H72模型分歧度", "H72市場模式", "H72動態權重",
    "H72原始共識分", "H72風險調整分", "H72全市場百分位%", "H72全市場順位",
    "H72研究層級", "H72研究建議", "H72主要優勢", "H72主要缺口", "H72學習快照狀態",
    "H72權威邊界", "H72版本",
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


def _clip(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, float(v)))


def _num(row: pd.Series | dict[str, Any], names: Iterable[str], default: float | None = None) -> float | None:
    idx = row.index if isinstance(row, pd.Series) else row.keys()
    for c in names:
        if c in idx:
            x = _f(row.get(c), None)
            if x is not None:
                return x
    return default


def _txt(row: pd.Series | dict[str, Any], names: Iterable[str], default: str = "") -> str:
    idx = row.index if isinstance(row, pd.Series) else row.keys()
    for c in names:
        if c in idx:
            t = _s(row.get(c))
            if t:
                return t
    return default


def _weighted(items: list[tuple[float | None, float]], default: float | None = None) -> float | None:
    valid = [(float(v), float(w)) for v, w in items if v is not None and math.isfinite(float(v)) and w > 0]
    if not valid:
        return default
    den = sum(w for _, w in valid)
    return _clip(sum(v * w for v, w in valid) / den)


def _first_col(frame: pd.DataFrame, names: Iterable[str]) -> str | None:
    for c in names:
        if c in frame.columns:
            return c
    return None


def _percentile(frame: pd.DataFrame, names: Iterable[str], invert: bool = False) -> pd.Series:
    c = _first_col(frame, names)
    if not c:
        return pd.Series([float("nan")] * len(frame), index=frame.index, dtype="float64")
    s = pd.to_numeric(frame[c].astype(str).str.replace(",", "", regex=False).str.replace("%", "", regex=False), errors="coerce")
    p = s.rank(method="average", pct=True).mul(100.0)
    return 100.0 - p if invert else p


def _pe_score(pe: float | None) -> float | None:
    if pe is None:
        return None
    if pe <= 0: return 20.0
    if pe <= 12: return 90.0
    if pe <= 20: return 82.0
    if pe <= 30: return 70.0
    if pe <= 45: return 56.0
    if pe <= 70: return 42.0
    if pe <= 100: return 30.0
    return 20.0


def _pbr_score(pb: float | None) -> float | None:
    if pb is None or pb <= 0: return None
    if pb <= 1.2: return 90.0
    if pb <= 2.5: return 80.0
    if pb <= 4.0: return 65.0
    if pb <= 6.0: return 50.0
    return 35.0


def _yield_score(y: float | None) -> float | None:
    if y is None or y < 0: return None
    return _clip(42.0 + min(y, 8.0) * 6.0)


def _peg_lite_score(growth: float | None, pe: float | None) -> float | None:
    if growth is None or pe is None or pe <= 0:
        return None
    if growth <= 0:
        return 30.0
    ratio = growth / pe
    if ratio >= 2.0: return 92.0
    if ratio >= 1.2: return 82.0
    if ratio >= 0.7: return 70.0
    if ratio >= 0.4: return 58.0
    return 42.0


def _volume_ratio_score(v: float | None) -> float | None:
    if v is None: return None
    if 1.2 <= v <= 2.8: return 88.0
    if 0.9 <= v < 1.2: return 66.0
    if 2.8 < v <= 4.5: return 70.0
    if 4.5 < v <= 6.0: return 55.0
    if v > 6.0: return 38.0
    return 48.0


def _regime(row: pd.Series) -> str:
    adj = _num(row, ["H67市場Regime調整"], None)
    bridge = _num(row, ["大盤橋接分數", "市場環境分數"], None)
    risk = _txt(row, ["大盤風險等級", "AI市場狀態", "市場環境"])
    attack = _txt(row, ["大盤攻擊模式", "大盤策略模式"])
    if (adj is not None and adj <= -7) or (bridge is not None and bridge < 42) or any(x in risk for x in ["高", "空", "防守", "Risk-Off"]):
        return "DEFENSIVE"
    if (adj is not None and adj >= 4) or (bridge is not None and bridge >= 65) or any(x in attack for x in ["攻擊", "多頭", "Risk-On"]):
        return "ATTACK"
    return "NEUTRAL"


def _quality_optional(row: pd.Series) -> list[tuple[float | None, float]]:
    # Future-proof hooks: if the official-factor service later adds real accounting fields,
    # H72 consumes them automatically without changing the authority boundary.
    roe = _num(row, ["ROE%", "股東權益報酬率%"], None)
    gm = _num(row, ["毛利率%", "毛利率"], None)
    opm = _num(row, ["營業利益率%", "營益率%", "營益率"], None)
    debt = _num(row, ["負債比%", "負債比率%"], None)
    cfo = _num(row, ["營業現金流", "營運現金流"], None)
    return [
        (_clip(35 + roe * 3) if roe is not None else None, 0.10),
        (_clip(35 + gm * 1.6) if gm is not None else None, 0.08),
        (_clip(35 + opm * 2.2) if opm is not None else None, 0.08),
        (_clip(100 - debt) if debt is not None else None, 0.08),
        (80.0 if cfo is not None and cfo > 0 else 35.0 if cfo is not None else None, 0.08),
    ]


def apply_h72_multi_model_alpha_ensemble(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
    work = frame.copy().reset_index(drop=True)
    try:
        from godpick_h70_counter_regime_session_truth import VERSION as H70V, apply_h70_counter_regime_session_truth
        hv = work.get("H70版本", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)
        if not hv.eq(H70V).all():
            work = apply_h70_counter_regime_session_truth(work)
    except Exception:
        pass

    pct = {
        "eps": _percentile(work, ["估算EPS", "EPS", "每股盈餘"]),
        "rev_yoy": _percentile(work, ["月營收YoY%", "單月營收YoY%", "營收年增率%"]),
        "cum_rev": _percentile(work, ["累計營收YoY%", "累計營收年增率%"]),
        "foreign1": _percentile(work, ["外資近1日買賣超"]),
        "foreign5": _percentile(work, ["外資近5日買賣超"]),
        "trust5": _percentile(work, ["投信近5日買賣超"]),
        "inst5": _percentile(work, ["三大法人近5日合計", "三大法人近5日買賣超"]),
        "inst_streak": _percentile(work, ["法人連買天數"]),
        "inst_volume_share": _percentile(work, ["法人買超占量比%"]),
        "holder": _percentile(work, ["TDCC千張大戶持股比%", "H60千張大戶持股比%"]),
        "ret5": _percentile(work, ["近5日漲幅%"]),
        "ret20": _percentile(work, ["近20日漲幅%", "20日績效%"]),
        "ret60": _percentile(work, ["近60日漲幅%", "60日績效%"]),
        "vol20_inv": _percentile(work, ["20日波動率%"], invert=True),
    }

    out_rows: list[dict[str, Any]] = []
    for i, row in work.iterrows():
        # 1 Growth / earnings acceleration
        growth = _weighted([
            (_num(row, ["H65營收分"], None), .32),
            (_num(row, ["H65獲利EPS分"], None), .23),
            (_num(row, ["營收成長官方分數", "營收成長分數"], None), .15),
            (pct["rev_yoy"].get(i) if pd.notna(pct["rev_yoy"].get(i, float('nan'))) else None, .12),
            (pct["cum_rev"].get(i) if pd.notna(pct["cum_rev"].get(i, float('nan'))) else None, .08),
            (_num(row, ["EPS成長分數"], None), .10),
        ], None)

        # 2 Quality / profitability proxy, with hooks for actual ROE/margins/CFO when available.
        quality_items = [
            (_num(row, ["H65獲利EPS分"], None), .30),
            (_num(row, ["H66結構品質分"], None), .22),
            (_num(row, ["官方基本面成長分數", "基本面成長分數"], None), .16),
            (pct["eps"].get(i) if pd.notna(pct["eps"].get(i, float('nan'))) else None, .10),
            (pct["vol20_inv"].get(i) if pd.notna(pct["vol20_inv"].get(i, float('nan'))) else None, .08),
            (_clip(100 - (_num(row, ["H65風險扣分"], 0) or 0) * 4), .06),
        ] + _quality_optional(row)
        quality = _weighted(quality_items, None)

        # 3 Institutional demand
        inst = _weighted([
            (_num(row, ["H65法人分"], None), .22),
            (_num(row, ["H66法人加速度分"], None), .22),
            (_num(row, ["法人籌碼官方分數", "法人籌碼分數"], None), .14),
            (pct["foreign1"].get(i) if pd.notna(pct["foreign1"].get(i, float('nan'))) else None, .07),
            (pct["foreign5"].get(i) if pd.notna(pct["foreign5"].get(i, float('nan'))) else None, .08),
            (pct["trust5"].get(i) if pd.notna(pct["trust5"].get(i, float('nan'))) else None, .07),
            (pct["inst5"].get(i) if pd.notna(pct["inst5"].get(i, float('nan'))) else None, .08),
            (pct["inst_streak"].get(i) if pd.notna(pct["inst_streak"].get(i, float('nan'))) else None, .06),
            (pct["inst_volume_share"].get(i) if pd.notna(pct["inst_volume_share"].get(i, float('nan'))) else None, .06),
        ], None)

        # 4 TDCC / major holder lock-up
        tdcc_delta = _num(row, ["TDCC千張大戶週變化pp", "H60千張大戶週變化pp"], None)
        holder = _weighted([
            (_num(row, ["H65大戶分"], None), .38),
            (_num(row, ["H60大戶鎖碼真相分", "H64鎖碼確認分"], None), .27),
            (pct["holder"].get(i) if pd.notna(pct["holder"].get(i, float('nan'))) else None, .20),
            (_clip(50 + tdcc_delta * 20) if tdcc_delta is not None else None, .15),
        ], None)

        # 5 Value-growth efficiency
        pe = _num(row, ["PER本益比", "本益比"], None)
        pb = _num(row, ["PBR股價淨值比", "股價淨值比"], None)
        dy = _num(row, ["股利殖利率%", "殖利率%"], None)
        rev_yoy = _num(row, ["月營收YoY%", "營收年增率%"], None)
        value = _weighted([
            (_num(row, ["H65估值分"], None), .35),
            (_pe_score(pe), .23),
            (_pbr_score(pb), .13),
            (_yield_score(dy), .09),
            (_peg_lite_score(rev_yoy, pe), .20),
        ], None)

        # 6 Medium-term momentum / relative strength
        ret20 = _num(row, ["近20日漲幅%"], None)
        ret5 = _num(row, ["近5日漲幅%"], None)
        momentum = _weighted([
            (_num(row, ["H65趨勢分"], None), .26),
            (_num(row, ["H47個股相對強度分", "H57相對強度轉折分"], None), .16),
            (_num(row, ["H66短線動能分"], None), .12),
            (pct["ret5"].get(i) if pd.notna(pct["ret5"].get(i, float('nan'))) else None, .10),
            (pct["ret20"].get(i) if pd.notna(pct["ret20"].get(i, float('nan'))) else None, .20),
            (pct["ret60"].get(i) if pd.notna(pct["ret60"].get(i, float('nan'))) else None, .16),
        ], None)
        # Momentum literature supports persistence, but the system must not mistake a
        # late-stage parabolic extension for a fresh winner.  Penalize only extreme
        # 5/20-day extensions; ordinary positive momentum remains rewarded.
        if momentum is not None:
            exhaust = 0.0
            if ret5 is not None and ret5 > 22:
                exhaust += min(8.0, (ret5 - 22.0) * 0.35)
            if ret20 is not None and ret20 > 55:
                exhaust += min(10.0, (ret20 - 55.0) * 0.22)
            momentum = max(0.0, momentum - exhaust)

        # 7 Breakout / timing quality with anti-chase and sell-the-news penalties
        vol_ratio = _num(row, ["當日量比", "均量比"], None)
        close_pos = _num(row, ["當日收盤位置%"], None)
        breakout = _weighted([
            (_num(row, ["H65技術分"], None), .19),
            (_num(row, ["H66收盤品質分"], None), .20),
            (_num(row, ["H66技術買點分"], None), .19),
            (_num(row, ["H66量能流動性分"], None), .11),
            (_num(row, ["H66主流點火分"], None), .11),
            (_volume_ratio_score(vol_ratio), .08),
            (close_pos, .12),
        ], None)
        if breakout is not None:
            breakout = _clip(
                breakout
                - (_num(row, ["H66矛盾訊號扣分"], 0) or 0) * .45
                - (_num(row, ["H66利多不漲扣分"], 0) or 0) * .35
                - (_num(row, ["H67追價耗竭扣分"], 0) or 0) * .30
            )

        # 8 Risk / market-regime fit.  Counter-regime X1/X2 improves research resilience only.
        regime = _regime(row)
        market_adj = _num(row, ["H67市場Regime調整"], 0) or 0
        sector_adj = _num(row, ["H67族群資金調整"], 0) or 0
        h70_tier = _txt(row, ["H70逆勢研究層級"])
        counter_bonus = 12.0 if h70_tier.startswith("X1") else 6.0 if h70_tier.startswith("X2") else 0.0
        official_risk = _txt(row, ["H68官方資料風險"])
        official_score = 35.0 if any(x in official_risk.upper() for x in ["BLOCK", "FAIL", "STALE", "HIGH"]) else 82.0
        risk_model = _weighted([
            (_num(row, ["H67關鍵訊號一致性分"], None), .24),
            (_clip(65 + market_adj * 2.0), .16),
            (_clip(65 + sector_adj * 2.2), .13),
            (_clip(100 - (_num(row, ["H65風險扣分"], 0) or 0) * 4.0), .13),
            (_clip(100 - (_num(row, ["H66矛盾訊號扣分"], 0) or 0) * 2.2), .10),
            (_clip(100 - (_num(row, ["H66利多不漲扣分"], 0) or 0) * 3.0), .08),
            (_clip(100 - (_num(row, ["H67追價耗竭扣分"], 0) or 0) * 3.2), .08),
            (official_score, .08),
        ], None)
        if risk_model is not None and regime == "DEFENSIVE":
            risk_model = _clip(risk_model + counter_bonus)

        scores = {
            "成長動能": growth, "品質獲利": quality, "法人需求": inst, "大戶鎖碼": holder,
            "估值效率": value, "動能相對強度": momentum, "突破時機": breakout, "風險Regime": risk_model,
        }
        valid = {k: v for k, v in scores.items() if v is not None}
        coverage = len(valid) / len(MODEL_KEYS) * 100.0
        weights = WEIGHTS[regime]
        den = sum(weights[k] for k in valid)
        raw = sum(float(valid[k]) * weights[k] for k in valid) / den if den > 0 else 50.0
        divergence = statistics.pstdev(valid.values()) if len(valid) >= 2 else 0.0
        resonance = sum(1 for v in valid.values() if v >= 60.0)
        strong = sum(1 for v in valid.values() if v >= 70.0)

        coverage_pen = max(0.0, 75.0 - coverage) * 0.12
        divergence_pen = max(0.0, divergence - 16.0) * 0.30
        low_model_pen = max(0, 5 - resonance) * 1.4
        adjusted = _clip(raw - coverage_pen - divergence_pen - low_model_pen)

        # Explainable strengths / gaps
        ordered = sorted(valid.items(), key=lambda kv: kv[1], reverse=True)
        strengths = [f"{k}{v:.0f}" for k, v in ordered[:3] if v >= 60]
        gaps = [f"{k}{v:.0f}" for k, v in sorted(valid.items(), key=lambda kv: kv[1])[:3] if v < 55]
        if coverage < 75: gaps.append(f"模型覆蓋{coverage:.0f}%")
        if divergence > 20: gaps.append(f"模型分歧{divergence:.1f}")
        if regime == "DEFENSIVE" and not h70_tier.startswith(("X1", "X2")): gaps.append("弱市無逆勢確認")

        snap_ready = bool(
            _txt(row, ["H65觀察層級"]) and _txt(row, ["H66T1層級"]) and
            _txt(row, ["H67研究優先層級"]) and _txt(row, ["H68學習快照狀態"]) and
            _txt(row, ["H70學習快照狀態"])
        )
        out_rows.append({
            "H72成長動能模型分": round(growth, 2) if growth is not None else None,
            "H72品質獲利模型分": round(quality, 2) if quality is not None else None,
            "H72法人需求模型分": round(inst, 2) if inst is not None else None,
            "H72大戶鎖碼模型分": round(holder, 2) if holder is not None else None,
            "H72估值效率模型分": round(value, 2) if value is not None else None,
            "H72動能相對強度模型分": round(momentum, 2) if momentum is not None else None,
            "H72突破時機模型分": round(breakout, 2) if breakout is not None else None,
            "H72風險Regime模型分": round(risk_model, 2) if risk_model is not None else None,
            "H72模型覆蓋%": round(coverage, 1),
            "H72共振模型數": int(resonance),
            "H72模型分歧度": round(divergence, 2),
            "H72市場模式": regime,
            "H72動態權重": "｜".join(f"{k}{weights[k]}%" for k in MODEL_KEYS),
            "H72原始共識分": round(raw, 2),
            "H72風險調整分": round(adjusted, 2),
            "H72全市場百分位%": None,
            "H72全市場順位": None,
            "H72研究層級": "",
            "H72研究建議": "",
            "H72主要優勢": "；".join(strengths) or "尚無明顯多模型優勢",
            "H72主要缺口": "；".join(gaps) or "無重大模型缺口",
            "H72學習快照狀態": "SNAPSHOT-READY" if snap_ready and coverage >= 62.5 else "SNAPSHOT-INCOMPLETE",
            "H72權威邊界": "H72只做多模型研究排序；不得建立Formal，不得解除H68執行否決。",
            "H72版本": VERSION,
            "_h72_strong_models": strong,
            "_h72_precise_score": float(adjusted),
        })

    extra = pd.DataFrame(out_rows, index=work.index)
    # Full-universe percentile/rank before any top-N filtering.
    # Rank on the unrounded internal score so the 2-decimal display does not create artificial ties.
    # method="first" is the final deterministic tie-breaker and preserves the incoming full-market order.
    score_s = pd.to_numeric(extra["_h72_precise_score"], errors="coerce")
    pct_s = score_s.rank(method="average", pct=True).mul(100.0)
    rank_s = score_s.rank(method="first", ascending=False).astype("Int64")
    extra["H72全市場百分位%"] = pct_s.round(2)
    extra["H72全市場順位"] = rank_s

    tiers=[]; adv=[]
    for _, r in extra.iterrows():
        score=float(r["H72風險調整分"]); pctv=float(r["H72全市場百分位%"]); cov=float(r["H72模型覆蓋%"])
        res=int(r["H72共振模型數"]); div=float(r["H72模型分歧度"]); riskv=_f(r["H72風險Regime模型分"], 0) or 0
        strong=int(r["_h72_strong_models"])
        if score >= 72 and pctv >= 98 and cov >= 75 and res >= 6 and strong >= 3 and div <= 22 and riskv >= 50:
            tier="E1｜多模型共振核心"; rec="多模型重點研究｜等待H64/H68獨立確認"
        elif score >= 66 and pctv >= 93 and cov >= 62.5 and res >= 5 and div <= 25 and riskv >= 42:
            tier="E2｜多模型優先觀察"; rec="優先觀察｜模型共識已形成，仍非Formal"
        elif score >= 60 and pctv >= 80 and cov >= 62.5 and res >= 4:
            tier="E3｜多模型候選追蹤"; rec="候選追蹤｜等待弱模型改善或價格確認"
        else:
            tier="R0｜模型分歧研究"; rec="僅研究｜不得視為正式推薦"
        tiers.append(tier); adv.append(rec)
    extra["H72研究層級"] = tiers
    extra["H72研究建議"] = adv
    extra.drop(columns=["_h72_strong_models", "_h72_precise_score"], inplace=True)
    for c in H72_COLUMNS:
        work[c] = extra[c]
    return work


def build_h72_ensemble_table(frame: pd.DataFrame, max_rows: int = 30, max_per_sector: int = 3) -> pd.DataFrame:
    work = apply_h72_multi_model_alpha_ensemble(frame)
    if work.empty:
        return work
    priority = work["H72研究層級"].fillna("").astype(str).map(lambda x: 40 if x.startswith("E1") else 30 if x.startswith("E2") else 20 if x.startswith("E3") else 10)
    work = work.assign(_h72_priority=priority)
    work.sort_values(["_h72_priority", "H72風險調整分", "H72全市場百分位%"], ascending=False, kind="mergesort", inplace=True)
    sector_col = _first_col(work, ["族群名稱", "類別", "產業別"])
    if sector_col and max_per_sector and max_per_sector > 0:
        selected=[]; counts={}
        for idx,row in work.iterrows():
            sec=_s(row.get(sector_col)) or "未分類"
            if counts.get(sec,0) >= int(max_per_sector): continue
            selected.append(idx); counts[sec]=counts.get(sec,0)+1
            if len(selected)>=max(1,int(max_rows)): break
        work=work.loc[selected]
    else:
        work=work.head(max(1,int(max_rows)))
    cols=[c for c in [
        "股票代號","股票名稱","市場別","族群名稱","H72研究層級","H72風險調整分","H72全市場百分位%","H72全市場順位",
        "H72共振模型數","H72模型覆蓋%","H72模型分歧度","H72市場模式",
        "H72成長動能模型分","H72品質獲利模型分","H72法人需求模型分","H72大戶鎖碼模型分",
        "H72估值效率模型分","H72動能相對強度模型分","H72突破時機模型分","H72風險Regime模型分",
        "H72主要優勢","H72主要缺口","H72研究建議","H65觀察層級","H66T1層級","H67研究優先層級",
        "H68次日執行狀態","H70逆勢研究層級","H72權威邊界","H72版本"
    ] if c in work.columns]
    return work[cols].reset_index(drop=True)


def build_h72_governance_summary(frame: pd.DataFrame) -> pd.DataFrame:
    work=apply_h72_multi_model_alpha_ensemble(frame)
    if work.empty:
        return pd.DataFrame({"治理項目":["H72"],"目前狀態":["無資料"]})
    tier=work["H72研究層級"].fillna("").astype(str)
    regime=_s(work.iloc[0].get("H72市場模式"))
    return pd.DataFrame([
        {"治理項目":"E1多模型共振核心","目前狀態":int(tier.str.startswith("E1").sum()),"治理原則":"至少6/8模型共振＋高百分位＋低分歧；仍非Formal"},
        {"治理項目":"E2多模型優先觀察","目前狀態":int(tier.str.startswith("E2").sum()),"治理原則":"至少5/8模型共振；仍需H64/H68獨立確認"},
        {"治理項目":"E3多模型候選追蹤","目前狀態":int(tier.str.startswith("E3").sum()),"治理原則":"至少4/8模型共振；不得直接交易"},
        {"治理項目":"目前市場模式","目前狀態":regime,"治理原則":_s(work.iloc[0].get("H72動態權重"))},
        {"治理項目":"平均模型覆蓋%","目前狀態":round(pd.to_numeric(work["H72模型覆蓋%"],errors="coerce").mean(),1),"治理原則":"缺資料不當成強訊號，僅對可用模型重新正規化權重"},
        {"治理項目":"Formal/Execution權威","目前狀態":"UNCHANGED","治理原則":"H64/H63 Formal＋H68執行否決仍是唯一權威"},
    ])


__all__=["VERSION","MODEL_KEYS","WEIGHTS","H72_COLUMNS","apply_h72_multi_model_alpha_ensemble","build_h72_ensemble_table","build_h72_governance_summary"]
