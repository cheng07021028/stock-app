# -*- coding: utf-8 -*-
"""V191-H70 Counter-Regime Alpha Survivor × Session-Time Truth.

H70 fixes two blind spots found in the 2026-09-13 snapshot -> 2026-09-14 review:

1) H67 correctly suppresses broad A1 inflation in a weak market, but an across-the-board
   market penalty can hide a rare stock whose own multi-factor structure keeps strengthening.
   H70 therefore adds a *research-only* counter-regime survivor lane (X1/X2). It never
   creates Formal authority or execution permission.
2) A report generated on a weekend/holiday must not look like a same-day market signal.
   H70 records generation date vs. market-data anchor date and marks non-trading snapshots
   explicitly so T+1 learning is aligned to the next session rather than the calendar day.

Authority boundary: H64/H63 own Formal truth. H68 owns next-session execution veto.
H70 only improves research recall and timing auditability.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Iterable
from zoneinfo import ZoneInfo
import math
import re
import pandas as pd

VERSION = "v191_h70_counter_regime_alpha_session_truth_20260914"

H70_COLUMNS = [
    "H70逆勢Alpha分", "H70逆勢研究層級", "H70逆勢研究建議", "H70逆勢成立條件",
    "H70逆勢阻擋原因", "H70市場資料錨定日", "H70報告產生日", "H70快照時序狀態",
    "H70預期T1交易日", "H70學習快照狀態", "H70權威邊界", "H70版本",
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


def _parse_date(v: Any) -> date | None:
    t = _s(v)
    if not t:
        return None
    if re.fullmatch(r"\d{8}", t):
        try:
            return datetime.strptime(t, "%Y%m%d").date()
        except Exception:
            return None
    if re.fullmatch(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}.*", t):
        head = re.split(r"[ T]", t, 1)[0].replace("/", "-")
        try:
            return datetime.strptime(head, "%Y-%m-%d").date()
        except Exception:
            return None
    return None


def _frame_market_date(frame: pd.DataFrame) -> date | None:
    # Priority is important: do not let weekly TDCC dates override daily market/K-line dates.
    groups = [
        ["官方因子資料日期", "每日因子資料日期", "行情資料日期", "行情日期", "最新行情日期", "最新交易日"],
        ["K線資料日期", "個股K線資料日期", "股價日期", "資料日期"],
    ]
    for cols in groups:
        vals: list[date] = []
        for c in cols:
            if c not in frame.columns:
                continue
            for v in frame[c].tolist():
                d = _parse_date(v)
                if d:
                    vals.append(d)
        if vals:
            return max(vals)
    return None


def _next_weekday(d: date | None) -> date | None:
    if d is None:
        return None
    x = d + timedelta(days=1)
    while x.weekday() >= 5:
        x += timedelta(days=1)
    return x


def _generated_date(generated_at: datetime | date | None) -> date:
    if isinstance(generated_at, datetime):
        return generated_at.date()
    if isinstance(generated_at, date):
        return generated_at
    return datetime.now(ZoneInfo("Asia/Taipei")).date()


def _counter_regime(row: pd.Series) -> tuple[float, str, str, list[str], list[str]]:
    h65_tier = _txt(row, ["H65觀察層級"])
    h65 = _num(row, ["H65多因子觀察分"], 50.0) or 50.0
    h65_pct = _num(row, ["H65全市場觀察百分位%"], 0.0) or 0.0
    coverage = _num(row, ["H65資料覆蓋%"], 0.0) or 0.0
    h65_risk = _num(row, ["H65風險扣分"], 0.0) or 0.0

    h66 = _num(row, ["H66T1自適應排序分"], 50.0) or 50.0
    close = _num(row, ["H66收盤品質分"], 50.0) or 50.0
    inst = _num(row, ["H66法人加速度分"], 50.0) or 50.0
    ignition = _num(row, ["H66主流點火分"], 50.0) or 50.0
    tech = _num(row, ["H66技術買點分"], 50.0) or 50.0
    liquidity = _num(row, ["H66量能流動性分"], 50.0) or 50.0
    momentum = _num(row, ["H66短線動能分"], 50.0) or 50.0
    structure = _num(row, ["H66結構品質分"], 50.0) or 50.0
    contradiction = _num(row, ["H66矛盾訊號扣分"], 0.0) or 0.0
    sellnews = _num(row, ["H66利多不漲扣分"], 0.0) or 0.0

    market_adj = _num(row, ["H67市場Regime調整"], 0.0) or 0.0
    sector_adj = _num(row, ["H67族群資金調整"], 0.0) or 0.0
    consensus = _num(row, ["H67關鍵訊號一致性分"], 50.0) or 50.0
    chase = _num(row, ["H67追價耗竭扣分"], 0.0) or 0.0

    weighted = (
        h65 * 0.22 + h66 * 0.18 + close * 0.11 + ignition * 0.12 + tech * 0.11
        + liquidity * 0.10 + momentum * 0.06 + structure * 0.06 + max(inst, 40.0) * 0.04
    )
    score = _clip(weighted - contradiction * 0.8 - sellnews * 0.45 - max(0.0, chase - 4.0) * 0.5)

    conditions: list[str] = []
    blocks: list[str] = []
    if market_adj <= -7:
        conditions.append(f"弱市逆勢檢查({market_adj:+.0f})")
    else:
        blocks.append("非弱市，不使用逆勢例外通道")
    if sector_adj > -6:
        conditions.append(f"族群未明顯退潮({sector_adj:+.0f})")
    else:
        blocks.append(f"族群資金仍明顯退潮({sector_adj:+.0f})")
    if h65_tier.startswith("W1"):
        conditions.append("H65=W1")
    elif h65_tier.startswith("W2") and h65 >= 69 and h65_pct >= 99:
        conditions.append("H65高分W2")
    else:
        blocks.append(f"H65結構不足({h65_tier or 'NA'}/{h65:.1f})")
    checks = [
        (coverage >= 85, f"覆蓋{coverage:.0f}%", "資料覆蓋不足"),
        (h65_risk <= 6, f"H65風險{h65_risk:.1f}", "H65風險過高"),
        (h66 >= 66, f"H66={h66:.1f}", "H66時機不足"),
        (close >= 70, f"收盤{close:.0f}", "收盤品質不足"),
        (ignition >= 65, f"點火{ignition:.0f}", "主流點火不足"),
        (tech >= 65, f"技術{tech:.0f}", "技術買點不足"),
        (liquidity >= 70, f"量能{liquidity:.0f}", "量能不足"),
        (structure >= 60, f"結構{structure:.0f}", "結構品質不足"),
        (consensus >= 65, f"一致性{consensus:.0f}", "關鍵訊號一致性不足"),
        (contradiction < 8, f"矛盾-{contradiction:.1f}", "矛盾訊號過高"),
        (sellnews < 7, f"利多不漲-{sellnews:.1f}", "利多不漲風險過高"),
        (chase < 8, f"追價-{chase:.1f}", "追價耗竭過高"),
    ]
    for ok, yes, no in checks:
        (conditions if ok else blocks).append(yes if ok else no)

    if not blocks and score >= 71 and h65_tier.startswith("W1") and h65_pct >= 98:
        tier = "X1｜逆勢Alpha重點觀察"
        rec = "逆勢重點研究｜市場弱仍保留；非Formal、非執行許可"
    elif len(blocks) <= 1 and market_adj <= -7 and sector_adj > -6 and score >= 67 and h65_pct >= 98:
        tier = "X2｜逆勢韌性追蹤"
        rec = "逆勢韌性追蹤｜等待盤前/價格再次確認；非Formal"
    else:
        tier = "R0｜無逆勢例外"
        rec = "沿用H67/H68防守治理"
    return round(score, 2), tier, rec, conditions, blocks


def apply_h70_counter_regime_session_truth(
    frame: pd.DataFrame,
    *,
    generated_at: datetime | date | None = None,
) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
    work = frame.copy().reset_index(drop=True)
    try:
        from godpick_h68_execution_learning_truth import VERSION as H68_VERSION, apply_h68_execution_learning_truth
        hv = work.get("H68版本", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)
        if not hv.eq(H68_VERSION).all():
            work = apply_h68_execution_learning_truth(work)
    except Exception:
        pass

    gen = _generated_date(generated_at)
    market_d = _frame_market_date(work)
    target = _next_weekday(market_d)
    if gen.weekday() >= 5:
        timing = "NON-TRADING-GENERATION｜非交易日產生，錨定最近交易日"
    elif market_d is None:
        timing = "UNKNOWN-MARKET-DATE｜需確認市場資料日期"
    elif market_d == gen:
        timing = "SAME-SESSION｜市場資料日與報告日一致"
    elif market_d < gen:
        timing = "PRIOR-SESSION｜沿用最近已完成交易日，次日需重驗"
    else:
        timing = "DATE-CHECK｜市場資料日期晚於報告日"

    rows = []
    for _, row in work.iterrows():
        score, tier, rec, conditions, blocks = _counter_regime(row)
        snap_ready = bool(
            _s(row.get("H65觀察層級")) and _s(row.get("H66T1層級"))
            and _s(row.get("H67研究優先層級")) and _s(row.get("H68學習快照狀態"))
        )
        rows.append({
            "H70逆勢Alpha分": score,
            "H70逆勢研究層級": tier,
            "H70逆勢研究建議": rec,
            "H70逆勢成立條件": "；".join(conditions[:8]) or "無",
            "H70逆勢阻擋原因": "；".join(blocks[:6]) or "無",
            "H70市場資料錨定日": market_d.isoformat() if market_d else "",
            "H70報告產生日": gen.isoformat(),
            "H70快照時序狀態": timing,
            "H70預期T1交易日": target.isoformat() if target else "",
            "H70學習快照狀態": "SNAPSHOT-READY" if snap_ready else "SNAPSHOT-INCOMPLETE",
            "H70權威邊界": "X1/X2只修正弱市研究召回；不得建立Formal，也不得繞過H68次日執行否決。",
            "H70版本": VERSION,
        })
    extra = pd.DataFrame(rows, index=work.index)
    for c in H70_COLUMNS:
        work[c] = extra[c]
    return work


def build_h70_counter_regime_table(frame: pd.DataFrame, max_rows: int = 20) -> pd.DataFrame:
    work = apply_h70_counter_regime_session_truth(frame)
    if work.empty:
        return work
    order = work.get("H70逆勢研究層級", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str).map(
        lambda x: 30 if x.startswith("X1") else 20 if x.startswith("X2") else 10
    )
    work = work.assign(_h70_order=order)
    work.sort_values(["_h70_order", "H70逆勢Alpha分", "H65全市場觀察百分位%"], ascending=False, kind="mergesort", inplace=True)
    cols = [c for c in [
        "股票代號", "股票名稱", "類別", "H70逆勢研究層級", "H70逆勢Alpha分", "H70逆勢研究建議",
        "H70逆勢成立條件", "H70逆勢阻擋原因", "H70市場資料錨定日", "H70報告產生日", "H70快照時序狀態",
        "H70預期T1交易日", "H65觀察層級", "H65多因子觀察分", "H65全市場觀察百分位%",
        "H66T1層級", "H66T1自適應排序分", "H66收盤品質分", "H66法人加速度分", "H66主流點火分",
        "H66技術買點分", "H66量能流動性分", "H67市場Regime調整", "H67族群資金調整",
        "H67關鍵訊號一致性分", "H67追價耗竭扣分", "H67研究優先層級", "H68次日執行狀態",
        "H64有效權威", "H70權威邊界", "H70版本",
    ] if c in work.columns]
    return work.head(max(1, int(max_rows or 1)))[cols].reset_index(drop=True)


def build_h70_governance_summary(frame: pd.DataFrame, generated_at: datetime | date | None = None) -> pd.DataFrame:
    work = apply_h70_counter_regime_session_truth(frame, generated_at=generated_at)
    if work.empty:
        return pd.DataFrame({"治理項目": ["H70"], "目前狀態": ["無資料"]})
    tiers = work["H70逆勢研究層級"].fillna("").astype(str)
    first = work.iloc[0]
    return pd.DataFrame([
        {"治理項目": "X1逆勢Alpha重點", "目前狀態": int(tiers.str.startswith("X1").sum()), "治理原則": "弱市仍允許極少數個股被研究召回；非Formal"},
        {"治理項目": "X2逆勢韌性", "目前狀態": int(tiers.str.startswith("X2").sum()), "治理原則": "等待盤前/價格確認；非Formal"},
        {"治理項目": "市場資料錨定日", "目前狀態": _s(first.get("H70市場資料錨定日")), "治理原則": "週末報告不得冒充當日交易資料"},
        {"治理項目": "報告產生日", "目前狀態": _s(first.get("H70報告產生日")), "治理原則": _s(first.get("H70快照時序狀態"))},
        {"治理項目": "Formal/Execution權威", "目前狀態": "UNCHANGED", "治理原則": "H64/H63 Formal + H68執行否決仍是唯一權威"},
    ])


__all__ = [
    "VERSION", "H70_COLUMNS", "apply_h70_counter_regime_session_truth",
    "build_h70_counter_regime_table", "build_h70_governance_summary",
]
