# -*- coding: utf-8 -*-
"""V191-H99 execution-truth reconciliation and next-session governance.

H99 solves a concrete contradiction observed in the 2026-09-24 manager export:
H89 had rebuilt a valid pullback plan with cost-after-fee RR=1.50, while H81's
human-readable intraday text still quoted the older H79 RR=0.73.  The same
workbook therefore exposed two different execution truths for one candidate.

Governance
----------
* H89 is the newest price-plan authority when it has a complete valid plan.
* H79 remains the fallback price plan and retains its original Formal gates.
* H99 never creates Formal/A-/R1 authority.  It only reconciles presentation,
  research risk scoring, and next-session revalidation semantics.
* A post-close recommendation separated from the next tradable session by a
  weekend/holiday information window must be revalidated before execution.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable
import json
import math

import pandas as pd

VERSION = "v191_h99_execution_truth_reconciliation_20260924"

# Official TWSE market closure dates for 2026.  Weekends are handled
# independently.  An optional godpick_twse_holidays.json may add/override
# closures without a code release; accepted shapes are a list of YYYY-MM-DD or
# {"closed_dates": [...]}.
TWSE_CLOSED_2026 = {
    "2026-01-01",
    "2026-02-12", "2026-02-13", "2026-02-16", "2026-02-17", "2026-02-18", "2026-02-19", "2026-02-20",
    "2026-02-27",
    "2026-04-03", "2026-04-06",
    "2026-05-01",
    "2026-06-19",
    "2026-09-25", "2026-09-28",
    "2026-10-09", "2026-10-26",
    "2026-12-25",
}

H99_COLUMNS = [
    "H99版本", "H99執行真相來源", "H99主進場", "H99防守停損", "H99第一目標",
    "H99成本後RR", "H99RR重算值", "H99RR一致性", "H99舊RR差異",
    "H99價格計畫狀態", "H99市場資料日", "H99目標交易日", "H99資訊空窗日數",
    "H99盤前重驗", "H99執行狀態", "H99Formal權限", "H99決策摘要",
]

FRONT = [
    "股票代號", "股票名稱", "市場別", "類別",
    "H99執行真相來源", "H99價格計畫狀態", "H99主進場", "H99防守停損", "H99第一目標",
    "H99成本後RR", "H99RR一致性", "H99目標交易日", "H99資訊空窗日數", "H99盤前重驗",
    "H99執行狀態", "H99Formal權限", "H99決策摘要",
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


def _date_value(v: Any) -> date | None:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    s = _text(v)
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            return datetime.strptime(s[:10] if fmt != "%Y%m%d" else s[:8], fmt).date()
        except Exception:
            pass
    try:
        ts = pd.to_datetime(s, errors="coerce")
        if pd.notna(ts):
            return ts.date()
    except Exception:
        pass
    return None


def _extra_closed_dates() -> set[str]:
    out: set[str] = set()
    for p in (Path("godpick_twse_holidays.json"), Path(__file__).resolve().parent / "godpick_twse_holidays.json"):
        try:
            if not p.exists():
                continue
            data = json.loads(p.read_text(encoding="utf-8-sig"))
            values = data.get("closed_dates", []) if isinstance(data, dict) else data
            if isinstance(values, list):
                out.update(str(x)[:10] for x in values if _date_value(x))
            break
        except Exception:
            continue
    return out


def closed_dates_for_year(year: int) -> set[str]:
    base = set(TWSE_CLOSED_2026) if year == 2026 else set()
    return base | _extra_closed_dates()


def is_twse_trading_day(day_value: date) -> bool:
    if day_value.weekday() >= 5:
        return False
    return day_value.isoformat() not in closed_dates_for_year(day_value.year)


def next_twse_trading_day(anchor: Any) -> date | None:
    d = _date_value(anchor)
    if d is None:
        return None
    probe = d + timedelta(days=1)
    for _ in range(20):
        if is_twse_trading_day(probe):
            return probe
        probe += timedelta(days=1)
    return None


def _net_rr(entry: float | None, stop: float | None, target: float | None,
            *, commission: float = .001425, tax: float = .003, slippage: float = .001) -> float | None:
    if entry is None or stop is None or target is None or not (0 < stop < entry < target):
        return None
    buy = entry * (1 + slippage) * (1 + commission)
    sell = (1 - slippage) * (1 - commission - tax)
    risk = buy - stop * sell
    reward = target * sell - buy
    if risk <= 0:
        return None
    return reward / risk


def resolve_execution_truth(row: dict[str, Any] | pd.Series) -> dict[str, Any]:
    raw = row.to_dict() if isinstance(row, pd.Series) else dict(row or {})
    h89_entry = _first_num(raw, ["H89主進場"])
    h89_stop = _first_num(raw, ["H89防守停損"])
    h89_target = _first_num(raw, ["H89第一目標"])
    h89_rr = _first_num(raw, ["H89成本後RR1"])
    h79_entry = _first_num(raw, ["H79計畫進場", "主要進場參考價", "實戰觸發價"])
    h79_stop = _first_num(raw, ["H79結構停損", "停損參考", "SuperAI動態停損價"])
    h79_target = _first_num(raw, ["H79第一目標", "第一壓力價", "SuperAI第一減碼價"])
    h79_rr = _first_num(raw, ["H79成本後RR", "風險報酬比_決策", "風險報酬比"])

    h89_complete = all(x is not None and x > 0 for x in [h89_entry, h89_stop, h89_target]) and bool(h89_stop < h89_entry < h89_target)
    h79_complete = all(x is not None and x > 0 for x in [h79_entry, h79_stop, h79_target]) and bool(h79_stop < h79_entry < h79_target)

    if h89_complete:
        source = "H89｜最新執行價格計畫"
        entry, stop, target, rr = h89_entry, h89_stop, h89_target, h89_rr
        plan_source = _first_text(raw, ["H89價格計畫來源"]) or "H89"
        price_plan_ok = _first_text(raw, ["H89Formal價格計畫合格"])
        model_only = _first_text(raw, ["H89模型目標僅研究"])
    elif h79_complete:
        source = "H79｜原始價格計畫"
        entry, stop, target, rr = h79_entry, h79_stop, h79_target, h79_rr
        plan_source = "H79原始價格計畫"
        price_plan_ok = "是" if (_first_text(raw, ["H79計畫狀態"]).startswith("PASS")) else "待確認"
        model_only = "否"
    else:
        source = "INCOMPLETE｜價格計畫不完整"
        entry = h89_entry if h89_entry is not None else h79_entry
        stop = h89_stop if h89_stop is not None else h79_stop
        target = h89_target if h89_target is not None else h79_target
        rr = h89_rr if h89_rr is not None else h79_rr
        plan_source = _first_text(raw, ["H89價格計畫來源", "H79計畫狀態"])
        price_plan_ok = "否"
        model_only = _first_text(raw, ["H89模型目標僅研究"])

    rr_calc = _net_rr(entry, stop, target)
    # Prefer the plan-native H89/H79 RR if available; the recalculation is an
    # independent consistency guard using the shared default cost assumptions.
    rr_truth = rr if rr is not None else rr_calc
    rr_native_ok = rr_truth is None or rr_calc is None or abs(rr_truth - rr_calc) <= 0.03
    cross_delta = None
    if h89_rr is not None and h79_rr is not None:
        cross_delta = h89_rr - h79_rr
    cross_ok = cross_delta is None or abs(cross_delta) <= 0.05

    market_day = _date_value(_first_text(raw, [
        "H83市場資料日期", "H79資料基準日", "本輪市場最新交易日", "K線最後交易日", "行情資料日期", "價格資料日期"
    ]))
    target_day = next_twse_trading_day(market_day)
    gap_days = (target_day - market_day).days if (market_day and target_day) else None
    long_gap = bool(gap_days is not None and gap_days >= 3)

    formal_state = _first_text(raw, ["H79決策層級", "H79推薦狀態", "正式推薦資格", "操作許可"])
    is_formal = "正式條件可執行" in formal_state or formal_state.startswith("FORMAL")
    h96_state = _first_text(raw, ["H96執行狀態"])
    research_only = (not is_formal) or (model_only == "是") or h96_state.startswith(("WAIT", "LEADER-NO-CHASE", "WATCH"))

    if long_gap:
        preopen = f"REQUIRED｜{target_day.isoformat()}盤前重驗；距資料日{gap_days}個日曆日，須更新隔夜市場/重大事件/法人與開盤跳空後再決定。"
    elif target_day:
        preopen = f"REQUIRED｜{target_day.isoformat()}盤前重驗資料新鮮度、隔夜事件與開盤跳空。"
    else:
        preopen = "REQUIRED｜下一交易日盤前重驗；目標交易日待市場行事曆確認。"

    if not h89_complete and not h79_complete:
        exec_state = "BLOCK｜價格計畫不完整"
    elif research_only:
        exec_state = "WAIT-PREOPEN｜研究候選，盤前重驗後仍須原Formal治理"
    elif long_gap:
        exec_state = "REVALIDATE｜長假資訊空窗，原Formal條件不得直接沿用"
    else:
        exec_state = "PRICE-PLAN-READY｜仍須原Formal治理"

    rr_state_parts = []
    if not cross_ok:
        rr_state_parts.append(f"H79 {h79_rr:.2f} → H89 {h89_rr:.2f}，已以H89最新價格計畫統一")
    if not rr_native_ok and rr_truth is not None and rr_calc is not None:
        rr_state_parts.append(f"計畫RR {rr_truth:.2f}／重算 {rr_calc:.2f} 不一致")
    rr_consistency = "PASS｜單一執行真相" if not rr_state_parts else "RECONCILED｜" + "；".join(rr_state_parts)

    return {
        "source": source,
        "entry": entry,
        "stop": stop,
        "target": target,
        "rr": rr_truth,
        "rr_calc": rr_calc,
        "h79_rr": h79_rr,
        "h89_rr": h89_rr,
        "rr_delta": cross_delta,
        "rr_consistency": rr_consistency,
        "plan_source": plan_source,
        "price_plan_ok": price_plan_ok,
        "model_only": model_only,
        "market_day": market_day,
        "target_day": target_day,
        "gap_days": gap_days,
        "preopen": preopen,
        "exec_state": exec_state,
        "research_only": research_only,
    }


def analyze_candidate(row: dict[str, Any] | pd.Series) -> dict[str, Any]:
    truth = resolve_execution_truth(row)
    entry, stop, target, rr = truth["entry"], truth["stop"], truth["target"], truth["rr"]
    price_state = (
        f"{truth['price_plan_ok']}｜{truth['plan_source']}"
        + ("｜模型目標僅研究" if truth["model_only"] == "是" else "")
    )
    market_day = truth["market_day"].isoformat() if truth["market_day"] else ""
    target_day = truth["target_day"].isoformat() if truth["target_day"] else ""
    delta_text = ""
    if truth["rr_delta"] is not None and abs(truth["rr_delta"]) > 0.05:
        delta_text = f"H89-H79={truth['rr_delta']:+.2f}"
    summary = (
        f"執行真相={truth['source']}｜Entry={entry if entry is not None else 'NA'}｜Stop={stop if stop is not None else 'NA'}｜"
        f"Target={target if target is not None else 'NA'}｜NetRR={round(rr,2) if rr is not None else 'NA'}｜"
        f"目標交易日={target_day or '待確認'}｜{truth['exec_state']}。H99不建立Formal權限。"
    )
    return {
        "H99版本": VERSION,
        "H99執行真相來源": truth["source"],
        "H99主進場": round(entry, 4) if entry is not None else None,
        "H99防守停損": round(stop, 4) if stop is not None else None,
        "H99第一目標": round(target, 4) if target is not None else None,
        "H99成本後RR": round(rr, 4) if rr is not None else None,
        "H99RR重算值": round(truth["rr_calc"], 4) if truth["rr_calc"] is not None else None,
        "H99RR一致性": truth["rr_consistency"],
        "H99舊RR差異": delta_text,
        "H99價格計畫狀態": price_state,
        "H99市場資料日": market_day,
        "H99目標交易日": target_day,
        "H99資訊空窗日數": truth["gap_days"],
        "H99盤前重驗": truth["preopen"],
        "H99執行狀態": truth["exec_state"],
        "H99Formal權限": "LOCKED｜H99只統一執行真相與下一交易日重驗；Formal仍由H64/H68＋新鮮度＋流動性＋成本後RR＋停損治理。",
        "H99決策摘要": summary,
    }


def _rewrite_h81_plan(raw: dict[str, Any], truth: dict[str, Any]) -> dict[str, str]:
    entry, stop, target, rr = truth["entry"], truth["stop"], truth["target"], truth["rr"]
    target_day = truth["target_day"].isoformat() if truth["target_day"] else "下一交易日"
    source = truth["source"]
    rr_text = f"{rr:.2f}" if rr is not None else "未建立"
    e_text = f"{entry:.4f}" if entry is not None else "未建立"
    s_text = f"{stop:.4f}" if stop is not None else "未建立"
    t_text = f"{target:.4f}" if target is not None else "未建立"
    return {
        "H81盤前檢查": f"08:30~08:55｜{target_day}盤前重驗；{truth['preopen']}｜只有資料READY且原研究/正式條件未失效才保留。",
        "H81開盤策略": f"09:00~09:30｜執行真相={source}；觀察是否接近計畫進場 {e_text}，避免跳空追價。｜價格/量能符合原計畫且未觸發過熱或失效條件才續留。",
        "H81盤中調整": f"09:30~13:20｜{source}；停損 {s_text}、第一目標 {t_text}、成本後RR {rr_text}。｜只有原始風險報酬仍成立才維持；失效即取消，不移動停損硬湊RR。",
        "H81收盤檢討": "13:30後｜記錄實際觸發、最高/最低、收盤、量能、是否達停損/目標及市場情境。｜同步第8頁推薦紀錄，供T+1/T+3/T+5績效與Selection/Execution分離檢討。",
    }


def apply_execution_truth_overlay(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None:
        return pd.DataFrame()
    if not isinstance(frame, pd.DataFrame):
        frame = pd.DataFrame(frame)
    out = frame.copy(deep=True)
    if out.empty:
        for c in H99_COLUMNS:
            if c not in out.columns:
                out[c] = pd.Series(dtype="object")
        return out
    rows = out.to_dict("records")
    add = pd.DataFrame([analyze_candidate(r) for r in rows], index=out.index)
    for c in H99_COLUMNS:
        out[c] = add[c] if c in add.columns else None
    # Last-mile reconciliation: even a compact/stale H81 narrative is rewritten
    # from the authoritative H89/H79 execution truth before UI/Excel export.
    for idx, raw in zip(out.index, rows):
        truth = resolve_execution_truth(raw)
        for col, value in _rewrite_h81_plan(raw, truth).items():
            if col in out.columns:
                out.at[idx, col] = value
    front = [c for c in FRONT if c in out.columns]
    rest = [c for c in out.columns if c not in front]
    return out.loc[:, front + rest].copy()


def decorate_decision_tables(tables: dict[str, Any] | None) -> dict[str, pd.DataFrame]:
    src = tables if isinstance(tables, dict) else {}
    out: dict[str, pd.DataFrame] = {}
    for name, value in src.items():
        frame = value.copy() if isinstance(value, pd.DataFrame) else pd.DataFrame(value) if value is not None else pd.DataFrame()
        if name != "health" and isinstance(frame, pd.DataFrame) and not frame.empty:
            frame = apply_execution_truth_overlay(frame)
        out[name] = frame
    health = out.get("health", pd.DataFrame()).copy()
    if not health.empty and "項目" in health.columns:
        health = health.loc[~health["項目"].astype(str).str.startswith("H99")].copy()
    audit = out.get("audit", pd.DataFrame())
    mismatches = 0
    target_dates: list[str] = []
    long_gap = 0
    if isinstance(audit, pd.DataFrame) and not audit.empty:
        if "H99RR一致性" in audit.columns:
            mismatches = int(audit["H99RR一致性"].astype(str).str.startswith("RECONCILED").sum())
        if "H99目標交易日" in audit.columns:
            target_dates = [x for x in audit["H99目標交易日"].astype(str).tolist() if x and x != "nan"]
        if "H99資訊空窗日數" in audit.columns:
            g = pd.to_numeric(audit["H99資訊空窗日數"], errors="coerce")
            long_gap = int(g.ge(3).sum())
    target_mode = max(set(target_dates), key=target_dates.count) if target_dates else "待確認"
    rows = [
        {"項目": "H99版本", "數值": VERSION},
        {"項目": "H99執行真相", "數值": "H89完整價格計畫優先；H79為fallback；UI/Excel/H81敘述統一同一RR/Entry/Stop/Target"},
        {"項目": "H99RR不一致已調和", "數值": mismatches},
        {"項目": "H99目標交易日", "數值": target_mode},
        {"項目": "H99長假/週末重驗列", "數值": long_gap},
        {"項目": "H99盤前治理", "數值": "跨週末/休市資訊空窗必須在下一交易日盤前重驗；研究候選不得把前一收盤計畫直接當買進指令"},
        {"項目": "H99Formal權限", "數值": "LOCKED"},
    ]
    out["health"] = pd.concat([health, pd.DataFrame(rows)], ignore_index=True, sort=False)
    return out
