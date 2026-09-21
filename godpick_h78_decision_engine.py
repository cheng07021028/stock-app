"""H80: H79 opportunity governance + bounded Page08 performance feedback.

The module name remains ``godpick_h78_decision_engine`` so an H78 installation
can be upgraded by copying files in-place. H79 fixes four production defects:

* weekend report dates are normalised to the preceding market weekday;
* listed/OTC stocks are ranked separately from emerging-market research;
* optional TDCC history never turns an otherwise valid row into data repair;
* absolute factor scales are combined with cross-sectional ranks, preventing a
  whole market from failing solely because one upstream score has a compressed
  range.

Research ranking and formal trading permission remain separate. H79 never
creates H64/H68 authority and never fabricates a price plan.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone, timedelta
import hashlib
import json
import math
import re

import pandas as pd


VERSION = "v191_h80_record_performance_feedback_loop_20260921"


@dataclass(frozen=True)
class Policy:
    min_score: float = 60
    min_strength_floor: float = 55
    min_sector_floor: float = 35
    min_breadth_floor: float = 15
    min_rank_percentile: float = 60
    min_core_coverage: float = .70
    min_amount_million: float = 30
    min_net_rr: float = 1.5
    max_stop_pct: float = 8
    max_quote_lag_sessions: int = 1
    max_official_lag_sessions: int = 3
    commission: float = .001425
    sell_tax: float = .003
    slippage: float = .001
    max_rows: int = 8
    max_per_sector: int = 2

    def __post_init__(self):
        for key, value in vars(self).items():
            if isinstance(value, bool) or not math.isfinite(float(value)) or value < 0:
                raise ValueError("invalid policy: " + key)
        integer_fields = (self.max_quote_lag_sessions, self.max_official_lag_sessions,
                          self.max_rows, self.max_per_sector)
        if any(int(value) != value for value in integer_fields):
            raise ValueError("session and row limits must be integers")
        if self.max_rows < 1 or self.max_per_sector < 1 or self.min_net_rr <= 0:
            raise ValueError("positive limits required")
        if not 0 < self.min_core_coverage <= 1:
            raise ValueError("core coverage must be in (0, 1]")
        if any(x >= .1 for x in (self.commission, self.sell_tax, self.slippage)):
            raise ValueError("cost rates must be decimal fractions")


def num(value):
    try:
        if isinstance(value, bool):
            return None
        parsed = float(str(value).replace(",", "").rstrip("%％"))
        return parsed if math.isfinite(parsed) else None
    except (TypeError, ValueError):
        return None


def first(row, *keys):
    for key in keys:
        value = num(row.get(key))
        if value is not None:
            return value
    return None


def text(value):
    if value is None or str(value).lower() in {"nan", "none", "<na>", "nat"}:
        return ""
    return str(value).strip()


def day(value):
    value = text(value)
    if re.fullmatch(r"\d{8}", value):
        value = value[:4] + "-" + value[4:6] + "-" + value[6:]
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def clip(value):
    return max(0., min(100., value)) if value is not None else None


def _previous_weekday(value: date | None) -> date | None:
    if value is None:
        return None
    while value.weekday() >= 5:
        value -= timedelta(days=1)
    return value


def _session_lag(later: date | None, earlier: date | None) -> int | None:
    """Weekday-session distance. Holidays need an upstream market anchor."""
    if later is None or earlier is None:
        return None
    if earlier > later:
        reverse = _session_lag(earlier, later)
        return -reverse if reverse is not None else None
    count, cursor = 0, earlier
    while cursor < later:
        cursor += timedelta(days=1)
        if cursor.weekday() < 5:
            count += 1
    return count


def _market_bucket(value) -> str:
    raw = text(value).upper().replace(" ", "")
    if "興櫃" in raw or "EMERGING" in raw:
        return "興櫃"
    if "上櫃" in raw or raw in {"OTC", "TPEX"}:
        return "上櫃"
    if "上市" in raw or raw == "TWSE":
        return "上市"
    return "市場別未確認"


def _rank_pct(series: pd.Series, scope: pd.Series) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    out = pd.Series(float("nan"), index=series.index, dtype=float)
    eligible = scope.fillna(False).astype(bool) & values.notna()
    if eligible.any():
        out.loc[eligible] = values.loc[eligible].rank(method="average", pct=True) * 100
    return out


def price_plan(row, policy=Policy()):
    """Validate the supplied structure; never move a stop/target to force RR."""
    entry = first(row, "主要進場參考價", "實戰觸發價")
    stop = first(row, "停損參考", "SuperAI動態停損價")
    target = first(row, "第一壓力價", "SuperAI第一減碼價")
    result = {
        "H79計畫進場": entry,
        "H79結構停損": stop,
        "H79第一目標": target,
        "H79成本後RR": None,
        "H79停損距離%": None,
        "H79價格上限試算": None,
        "H79計畫狀態": "缺價格結構",
    }
    if any(value is None or value <= 0 for value in (entry, stop, target)):
        return result
    sell = (1 - policy.slippage) * (1 - policy.commission - policy.sell_tax)
    ceiling = sell * (target + policy.min_net_rr * stop) / (
        (1 + policy.min_net_rr) * (1 + policy.slippage) * (1 + policy.commission)
    )
    if stop < ceiling < target:
        result["H79價格上限試算"] = round(ceiling, 4)
    if not stop < entry < target:
        result["H79計畫狀態"] = "價格結構不成立：須停損＜進場＜第一目標"
        return result
    buy = entry * (1 + policy.slippage) * (1 + policy.commission)
    risk = buy - stop * sell
    reward = target * sell - buy
    if risk <= 0:
        result["H79計畫狀態"] = "價格結構不成立：成本後風險非正值"
        return result
    rr = reward / risk
    distance = (entry - stop) / entry * 100
    result.update({
        "H79成本後RR": round(rr, 4),
        "H79停損距離%": round(distance, 4),
        "H79計畫狀態": "PASS" if rr >= policy.min_net_rr and distance <= policy.max_stop_pct
        else "等待合理買點：成本後RR或停損距離不合格",
    })
    return result


def _prepare(frame: pd.DataFrame, policy: Policy) -> pd.DataFrame:
    prepared = frame.copy(deep=True)
    prepared["__code"] = prepared.get("股票代號", pd.Series("", index=prepared.index)).map(text).str.replace(r"\.0$", "", regex=True)
    market_raw = prepared.get("市場別", prepared.get("market", pd.Series("", index=prepared.index)))
    prepared["__market"] = market_raw.map(_market_bucket)
    prepared["__major"] = prepared["__market"].isin(["上市", "上櫃"])

    records = prepared.to_dict("records")
    prepared["__strength"] = [clip(first(row, "H47個股相對強度分")) for row in records]
    prepared["__sector"] = [clip(first(row, "H53族群共振分")) for row in records]
    prepared["__breadth"] = [clip(first(row, "H53族群廣度分")) for row in records]
    prepared["__liquidity"] = [first(row, "20日均成交額百萬") for row in records]
    prepared["__vol"] = [first(row, "當日量比") for row in records]
    prepared["__capital"] = [clip(50 + (value - 1) * 20) if value is not None else None for value in prepared["__vol"]]

    institution = []
    for row in records:
        i1, i3 = first(row, "三大法人近1日合計"), first(row, "三大法人近3日合計")
        score = None
        if i1 is not None and i3 is not None:
            prior_daily = (i3 - i1) / 2
            score = 90. if i1 > 0 and i3 > 0 and i1 > prior_daily else 75. if i1 > 0 and i3 > 0 else 25.
        institution.append(score)
    prepared["__inst"] = institution

    factors = ["__strength", "__sector", "__breadth", "__capital", "__inst"]
    weights = pd.Series({"__strength": .30, "__sector": .25, "__breadth": .15, "__capital": .15, "__inst": .15})
    numeric = prepared[factors].apply(pd.to_numeric, errors="coerce")
    covered = numeric.notna().mul(weights).sum(axis=1)
    prepared["__coverage"] = covered
    numerator = numeric.mul(weights).sum(axis=1)
    prepared["__absolute"] = (numerator / covered.where(covered.gt(0))).fillna(0.)

    rank_scope = prepared["__major"] & prepared["__code"].ne("")
    percentiles = pd.DataFrame(index=prepared.index)
    for column in factors:
        percentiles[column] = _rank_pct(prepared[column], rank_scope)
        prepared[column + "_pct"] = percentiles[column]
    pct_covered = percentiles.notna().mul(weights).sum(axis=1)
    pct_numerator = percentiles.mul(weights).sum(axis=1)
    prepared["__relative"] = (pct_numerator / pct_covered.where(pct_covered.gt(0))).fillna(0.)
    prepared["__sector_signal"] = prepared["__sector"] * .625 + prepared["__breadth"] * .375
    prepared["__sector_pct"] = _rank_pct(prepared["__sector_signal"], rank_scope)

    def series_or_nan(name):
        if name in prepared.columns:
            return pd.to_numeric(prepared[name], errors="coerce")
        return pd.Series(float("nan"), index=prepared.index)

    confirmation = pd.DataFrame({
        "h72": series_or_nan("H72風險調整分"),
        "h74": series_or_nan("H74決策總分"),
        "h77": series_or_nan("H77驗證增量分"),
    }, index=prepared.index)
    prepared["__confirmation"] = confirmation.mean(axis=1, skipna=True).clip(0, 100)
    has_confirmation = confirmation.notna().sum(axis=1).ge(2)
    prepared["__score"] = prepared["__absolute"] * .55 + prepared["__relative"] * .45
    prepared.loc[has_confirmation, "__score"] = (
        prepared.loc[has_confirmation, "__absolute"] * .45
        + prepared.loc[has_confirmation, "__relative"] * .40
        + prepared.loc[has_confirmation, "__confirmation"] * .15
    )

    # H80: Page08 matured performance may refine the *research ranking* only.
    # It never creates H64/H68 authority and cannot bypass price-plan, heat,
    # liquidity, freshness or risk gates.  Require a meaningful segment sample
    # size and cap the contribution to +/-2 points to avoid overfitting.
    feedback_corr = series_or_nan("績效校正分")
    feedback_corr = feedback_corr.where(feedback_corr.notna(), series_or_nan("Feedback績效校正分"))
    feedback_samples = series_or_nan("績效樣本數").fillna(0)
    feedback_adj = feedback_corr.fillna(0).clip(-8, 8).mul(0.25)
    feedback_adj = feedback_adj.where(feedback_samples.ge(8), 0.0).clip(-2.0, 2.0)
    prepared["__feedback_corr"] = feedback_corr
    prepared["__feedback_samples"] = feedback_samples
    prepared["__feedback_adj"] = feedback_adj
    prepared["__score"] = (prepared["__score"] + feedback_adj).clip(0, 100)
    return prepared


def evaluate(frame, *, as_of=None, policy=Policy()):
    """Evaluate the complete supplied universe before any top-N selection."""
    if frame is None or frame.empty:
        return pd.DataFrame()
    if frame.columns.duplicated().any():
        raise ValueError("duplicate input columns")
    now = day(as_of) if as_of is not None else datetime.now(timezone(timedelta(hours=8))).date()
    if now is None:
        raise ValueError("invalid as_of")
    evaluation_anchor = _previous_weekday(now)
    prepared = _prepare(frame, policy)
    rows = []

    for prepared_row in prepared.to_dict("records"):
        raw = {key: value for key, value in prepared_row.items() if not key.startswith("__")}
        code = text(prepared_row.get("__code"))
        if not code:
            continue
        market = text(prepared_row.get("__market"))
        major_market = bool(prepared_row.get("__major"))
        hard_issues, missing, evidence, reasons, date_notes = [], [], [], [], []

        quote_raw = day(raw.get("K線最後交易日"))
        anchor_raw = day(raw.get("本輪市場最新交易日"))
        official_raw = day(raw.get("官方因子資料日期"))
        quote, anchor, official = map(_previous_weekday, (quote_raw, anchor_raw, official_raw))
        if quote_raw is not None and quote != quote_raw:
            date_notes.append(f"K線{quote_raw.isoformat()}→{quote.isoformat()}")
        if anchor_raw is not None and anchor != anchor_raw:
            date_notes.append(f"市場錨定{anchor_raw.isoformat()}→{anchor.isoformat()}")
        if official_raw is not None and official != official_raw:
            date_notes.append(f"官方{official_raw.isoformat()}→{official.isoformat()}")

        quote_lag = _session_lag(evaluation_anchor, quote)
        anchor_gap = _session_lag(anchor, quote)
        if quote is None or anchor is None:
            hard_issues.append("缺K線/市場日期")
        elif anchor_gap is None or anchor_gap < 0 or anchor_gap > policy.max_quote_lag_sessions:
            hard_issues.append("K線未對齊市場錨定日")
        elif quote_lag is None or quote_lag < 0 or quote_lag > policy.max_quote_lag_sessions:
            hard_issues.append("K線超過允許交易日落後")
        official_lag = _session_lag(quote, official)
        if official is None or official_lag is None or official_lag < 0 or official_lag > policy.max_official_lag_sessions:
            hard_issues.append("官方日期缺失、過期或未來")
        freshness = text(raw.get("股神資料總新鮮度"))
        if not freshness.startswith("READY"):
            hard_issues.append("上游資料新鮮度未READY")
        price = first(raw, "最新價")
        if price is None or price <= 0:
            hard_issues.append("缺有效現價")

        liquidity = num(prepared_row.get("__liquidity"))
        if liquidity is None:
            hard_issues.append("缺流動性依據")
        elif liquidity < policy.min_amount_million:
            reasons.append("流動性不足")

        strength = num(prepared_row.get("__strength"))
        sector = num(prepared_row.get("__sector"))
        breadth = num(prepared_row.get("__breadth"))
        capital = num(prepared_row.get("__capital"))
        institution = num(prepared_row.get("__inst"))
        strength_pct = num(prepared_row.get("__strength_pct"))
        sector_pct = num(prepared_row.get("__sector_pct"))
        score = num(prepared_row.get("__score")) or 0.
        absolute = num(prepared_row.get("__absolute")) or 0.
        relative = num(prepared_row.get("__relative")) or 0.
        coverage = num(prepared_row.get("__coverage")) or 0.
        confirmation = num(prepared_row.get("__confirmation"))
        feedback_corr = num(prepared_row.get("__feedback_corr"))
        feedback_samples = num(prepared_row.get("__feedback_samples")) or 0.
        feedback_adj = num(prepared_row.get("__feedback_adj")) or 0.
        if feedback_samples >= 8 and abs(feedback_adj) >= .01:
            evidence.append(f"歷史績效校正{feedback_adj:+.2f}")

        momentum = first(raw, "3日動能加速度百分點")
        acceleration = first(raw, "成交額3日加速度%", "成交量3日加速度%")
        holder = first(raw, "TDCC千張大戶週變化pp")
        holder_date = _previous_weekday(day(raw.get("TDCC大戶資料日期")))
        holder_prior = _previous_weekday(day(raw.get("TDCC大戶前期日期")))
        holder_lag = _session_lag(quote, holder_date)
        holder_ok = (holder is not None and holder_date is not None and holder_prior is not None
                     and holder_prior < holder_date and holder_lag is not None and 0 <= holder_lag <= 10)
        if momentum is None:
            missing.append("3日動能增量")
        elif momentum >= .5:
            evidence.append("動能加速")
        if acceleration is None:
            missing.append("3日量額增量")
        elif acceleration >= 8:
            evidence.append("量額加速")
        if not holder_ok:
            missing.append("TDCC前後期增量（選配）")
        elif holder > 0:
            evidence.append("TDCC增持")
        if institution is not None and institution >= 90:
            evidence.append("法人流速改善")

        factor_values = {
            "相對強度": strength,
            "族群共振": sector,
            "族群廣度": breadth,
            "量能參與": capital,
            "法人方向": institution,
        }
        for key, value in factor_values.items():
            if value is None:
                missing.append(key)

        r1, r5, gap = first(raw, "今日漲幅%"), first(raw, "近5日漲幅%"), first(raw, "收盤距MA20%")
        if r1 is None or r5 is None:
            hard_issues.append("缺短線漲幅")
        if gap is None:
            missing.append("MA20乖離")
        hot = (r1 is not None and r1 >= 7) or (r5 is not None and r5 >= 12) or (gap is not None and gap >= 12)
        extreme = (r1 is not None and r1 >= 9.5) or (r5 is not None and r5 >= 18) or (gap is not None and gap >= 20)

        strong = strength is not None and strength >= policy.min_strength_floor and strength_pct is not None and strength_pct >= policy.min_rank_percentile
        mainstream = (sector is not None and sector >= policy.min_sector_floor
                      and breadth is not None and breadth >= policy.min_breadth_floor
                      and sector_pct is not None and sector_pct >= policy.min_rank_percentile)
        flow = (capital is not None and capital >= 55) or (institution is not None and institution >= 75)
        liquid = liquidity is not None and liquidity >= policy.min_amount_million
        selected = (major_market and not hard_issues and liquid and coverage >= policy.min_core_coverage
                    and strong and mainstream and flow and score >= policy.min_score and not extreme)

        if not major_market:
            reasons.append("非上市櫃主市場")
        if not strong:
            reasons.append("相對強度或橫截面排名未達標")
        if not mainstream:
            reasons.append("族群共振/廣度相對排名未達標")
        if not flow:
            reasons.append("量能/法人方向未確認")
        if coverage < policy.min_core_coverage:
            reasons.append("核心證據覆蓋不足")
        if score < policy.min_score:
            reasons.append("自適應機會分不足")
        if extreme:
            reasons.append("短線極端過熱")

        plan = price_plan(raw, policy)
        legacy = (text(raw.get("H64有效權威")) == "EFFECTIVE-FORMAL"
                  and text(raw.get("H68次日執行狀態")).startswith("READY-COND"))
        executable = selected and legacy and plan["H79計畫狀態"] == "PASS" and not hot and gap is not None

        if hard_issues:
            tier = "資料待修復"
            status = "資料待修復"
            execution = "禁止：先修復必要資料"
        elif market == "興櫃":
            tier = "興櫃隔離"
            status = "興櫃研究隔離"
            execution = "禁止：不列入上市櫃正式推薦"
        elif market == "市場別未確認":
            tier = "市場別待確認"
            status = "市場別待確認"
            execution = "禁止：先確認交易市場"
        elif executable:
            tier = "正式條件可執行"
            status = "正式推薦｜條件可執行"
            execution = "條件可執行：盤前重驗、觸發及守價後"
        elif selected:
            tier = "研究推薦"
            if hot:
                status = "研究推薦｜等待拉回"
                execution = "研究推薦：短線偏熱，等待拉回重算"
            elif plan["H79計畫狀態"] != "PASS":
                status = "研究推薦｜重建進場價格"
                execution = "研究推薦：等待有效進場/停損/目標"
            elif gap is None:
                status = "研究推薦｜補齊MA20乖離"
                execution = "研究推薦：補齊MA20乖離後重驗"
            elif not legacy:
                status = "研究推薦｜等待正式授權"
                execution = "研究推薦：未取得H64/H68正式授權"
            else:
                status = "研究推薦｜條件追蹤"
                execution = "研究推薦：盤前重新驗證"
        else:
            tier = "高熱等待" if extreme else "觀察等待"
            status = tier
            execution = "禁止：未通過本輪研究政策"

        reason_values = list(dict.fromkeys(hard_issues + reasons))
        out = dict(raw)
        out.update({
            "股票代號": code,
            "市場別": market,
            "H79版本": VERSION,
            "H79決策層級": tier,
            "H79推薦狀態": status,
            "H79自適應機會分": round(score, 2),
            "H79絕對品質分": round(absolute, 2),
            "H79橫截面排名分": round(relative, 2),
            "H79確認模型分": round(confirmation, 2) if confirmation is not None else None,
            "H80績效樣本數": int(feedback_samples),
            "H80績效校正原始分": round(feedback_corr, 2) if feedback_corr is not None else None,
            "H80績效校正加減分": round(feedback_adj, 2),
            "H79強度百分位%": round(strength_pct, 2) if strength_pct is not None else None,
            "H79族群百分位%": round(sector_pct, 2) if sector_pct is not None else None,
            "H79核心覆蓋%": round(coverage * 100, 2),
            "H79有效增量數": len(evidence),
            "H79有效增量": "；".join(dict.fromkeys(evidence)) or "無已驗證增量",
            "H79缺資料": "；".join(dict.fromkeys(missing)),
            "H79未入選原因": "；".join(reason_values),
            "H79推薦理由": "；".join(
                f"{key}={value:.1f}" if value is not None else f"{key}=缺資料"
                for key, value in factor_values.items()
            ),
            "H79交易狀態": execution,
            "H79原始K線日": quote_raw.isoformat() if quote_raw else "",
            "H79市場錨定日": anchor.isoformat() if anchor else "",
            "H79資料基準日": quote.isoformat() if quote else "",
            "H79評估日": now.isoformat(),
            "H79日期正規化": "；".join(date_notes) or "無",
            "H79選股政策": "絕對品質＋上市櫃橫截面百分位；Formal權威與研究排名分離",
            "H79範圍": "本次完整輸入池；興櫃隔離；不代表獲利保證",
            **plan,
        })
        payload = {key: text(value) for key, value in out.items()
                   if (key.startswith("H79") and key != "H79決策指紋") or key == "股票代號"}
        payload["policy"] = vars(policy)
        out["H79決策指紋"] = hashlib.sha256(
            json.dumps(payload, ensure_ascii=False, sort_keys=True).encode()
        ).hexdigest()[:20]
        rows.append(out)

    if not rows:
        return pd.DataFrame()
    result = pd.DataFrame(rows)
    duplicates = result["股票代號"].duplicated(keep=False)
    result.loc[duplicates, "H79決策層級"] = "資料待修復"
    result.loc[duplicates, "H79推薦狀態"] = "資料待修復"
    result.loc[duplicates, "H79交易狀態"] = "禁止：重複股票代號"
    result.loc[duplicates, "H79未入選原因"] = "重複股票代號"
    for index in result.index[duplicates]:
        result.at[index, "H79決策指紋"] = hashlib.sha256(
            (result.at[index, "H79決策指紋"] + "|duplicate").encode()
        ).hexdigest()[:20]
    return result


DISPLAY = [
    "股票代號", "股票名稱", "市場別", "類別", "H79決策層級", "H79推薦狀態",
    "H79自適應機會分", "H79絕對品質分", "H79橫截面排名分", "H79確認模型分",
    "H80績效樣本數", "H80績效校正原始分", "H80績效校正加減分",
    "H79強度百分位%", "H79族群百分位%", "H79推薦理由", "H79交易狀態",
    "H79計畫進場", "H79結構停損", "H79第一目標", "H79成本後RR", "H79停損距離%",
    "H79價格上限試算", "H79有效增量", "H79缺資料", "H79未入選原因",
    "H79原始K線日", "H79資料基準日", "H79日期正規化", "H79決策指紋",
]


def _message(message: str) -> pd.DataFrame:
    return pd.DataFrame({"結論": [message]})


def build_tables(frame, *, as_of=None, policy=Policy()):
    work = evaluate(frame, as_of=as_of, policy=policy)
    if work.empty:
        empty = _message("沒有輸入候選資料；請重新掃描。")
        return {key: empty.copy() for key in (
            "actionable", "research", "recommendations", "waiting", "emerging_watch",
            "data_repairs", "audit", "health",
        )}

    work = work.copy()
    work["__executable"] = work["H79交易狀態"].str.startswith("條件可執行", na=False)
    ranked = work.sort_values(
        ["__executable", "H79自適應機會分", "股票代號"],
        ascending=[False, False, True], kind="mergesort",
    )
    pool = ranked[ranked["H79決策層級"].isin(["正式條件可執行", "研究推薦"])]
    chosen, sector_counts = [], {}
    for index, row in pool.iterrows():
        sector = text(row.get("類別")) or "未分類"
        if sector_counts.get(sector, 0) >= policy.max_per_sector:
            continue
        chosen.append(index)
        sector_counts[sector] = sector_counts.get(sector, 0) + 1
        if len(chosen) >= policy.max_rows:
            break

    cols = [column for column in DISPLAY if column in work.columns]
    chosen_rows = ranked.loc[ranked.index.isin(chosen), cols]
    actionable = chosen_rows[chosen_rows["H79交易狀態"].str.startswith("條件可執行", na=False)].reset_index(drop=True)
    research = chosen_rows[~chosen_rows["H79交易狀態"].str.startswith("條件可執行", na=False)].reset_index(drop=True)
    if actionable.empty:
        actionable = _message("本輪沒有通過H64/H68與成本後價格計畫的正式可執行股票；不以研究股冒充買進。")
    if research.empty:
        research = _message("本輪沒有上市櫃股票通過研究推薦政策；請查看等待原因與資料健康。")

    waiting_mask = (
        ranked["市場別"].isin(["上市", "上櫃"])
        & ~ranked.index.isin(chosen)
        & ~ranked["H79決策層級"].eq("資料待修復")
    )
    waiting = ranked.loc[waiting_mask, cols].head(20).reset_index(drop=True)
    if waiting.empty:
        waiting = _message("本輪沒有可列入上市櫃等待區的候選。")
    emerging = ranked.loc[ranked["H79決策層級"].eq("興櫃隔離"), cols].head(20).reset_index(drop=True)
    if emerging.empty:
        emerging = _message("本輪沒有興櫃隔離研究股。")
    repairs = ranked.loc[ranked["H79決策層級"].eq("資料待修復"), cols].head(50).reset_index(drop=True)
    if repairs.empty:
        repairs = _message("本輪沒有必要資料待修復股票。")

    chosen_executable_count = int(work.loc[chosen, "__executable"].sum()) if chosen else 0
    health_rows = [
        {"項目": "輸入候選數", "數值": len(work)},
        {"項目": "上市櫃主市場數", "數值": int(work["市場別"].isin(["上市", "上櫃"]).sum())},
        {"項目": "興櫃隔離數", "數值": int(work["H79決策層級"].eq("興櫃隔離").sum())},
        {"項目": "通過研究政策", "數值": len(pool)},
        {"項目": "分散後研究推薦", "數值": len(chosen) - chosen_executable_count},
        {"項目": "正式條件可執行", "數值": chosen_executable_count},
        {"項目": "必要資料待修復", "數值": int(work["H79決策層級"].eq("資料待修復").sum())},
        {"項目": "週末日期正規化", "數值": int(work["H79日期正規化"].ne("無").sum())},
        {"項目": "學習狀態", "數值": "固定H79政策；需逐日全市場快照及成熟標籤做外樣本驗證"},
        {"項目": "排名口徑", "數值": "上市櫃絕對品質55%＋橫截面45%；有H72/H74/H77時改為45%＋40%＋確認15%"},
        {"項目": "缺資料處理", "數值": "TDCC為選配證據；缺TDCC不觸發資料待修復，必要日期/價格/流動性缺失才修復"},
        {"項目": "市場分流", "數值": "上市/上櫃可進主推薦；興櫃只進隔離研究，不占主榜"},
        {"項目": "成本假設", "數值": f"單邊手續費{policy.commission:.4%}、賣出稅{policy.sell_tax:.2%}、單邊滑價{policy.slippage:.2%}"},
        {"項目": "執行規則", "數值": "研究推薦不等於買進；正式可執行仍須H64/H68及成本後價格計畫"},
    ]
    for key in ["3日動能加速度百分點", "成交額3日加速度%", "TDCC千張大戶週變化pp"]:
        values = work.get(key, pd.Series(dtype=object))
        count = sum(num(value) is not None for value in values)
        health_rows.append({"項目": key + "覆蓋", "數值": f"{count}/{len(work)}"})

    audit = ranked[cols].reset_index(drop=True)
    health = pd.DataFrame(health_rows)
    return {
        "actionable": actionable,
        "research": research,
        "recommendations": research.copy(),
        "waiting": waiting,
        "emerging_watch": emerging,
        "data_repairs": repairs,
        "audit": audit,
        "health": health,
    }
