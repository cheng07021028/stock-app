# -*- coding: utf-8 -*-
"""V191-H40 authoritative market-date bridge for Page07 candidates.

H40 fixes a decision-integrity bug where the candidate K-line date was current but
``大盤資料日期`` became blank before Formal/V188/H34.  The bridge now uses one
verified market business date across the whole decision frame, derives per-row
market/K-line alignment, and records an explicit audit trail.  Missing authority
remains fail-closed; the module never manufactures a market date from a stock date.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any
import json
import pandas as pd

VERSION = "v191_h40_market_date_authority_consistency_20260818"
MARKET_FILE = "market_snapshot.json"
LEGACY_MARKET_FILE = "macro_mode_bridge.json"


def _text(value: Any) -> str:
    try:
        if value is None or pd.isna(value):
            return ""
    except Exception:
        pass
    try:
        return str(value).strip()
    except Exception:
        return ""


def _date_text(value: Any) -> str:
    text = _text(value).replace("/", "-")
    if not text:
        return ""
    if text.isdigit() and len(text) == 8:
        text = f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    try:
        return pd.to_datetime(text, errors="raise").strftime("%Y-%m-%d")
    except Exception:
        return ""


def _nested(payload: dict[str, Any], key: str) -> Any:
    required = payload.get("required_by_godpick")
    if isinstance(required, dict) and required.get(key) not in (None, "", []):
        return required.get(key)
    return None


def market_business_date(snapshot: dict[str, Any] | None) -> str:
    payload = snapshot if isinstance(snapshot, dict) else {}
    for key in (
        "twse_data_date", "market_date", "data_date", "otc_data_date",
        "futures_data_date", "_market_data_date", "資料日期", "大盤資料日期",
    ):
        d = _date_text(payload.get(key))
        if d:
            return d
    for key in ("market_date", "data_date", "twse_data_date", "otc_data_date", "futures_data_date"):
        d = _date_text(_nested(payload, key))
        if d:
            return d
    return ""


def _read_json(root: Path, name: str) -> dict[str, Any]:
    try:
        path = root / name
        if not path.exists():
            return {}
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _merge_context(primary: dict[str, Any], secondary: dict[str, Any], source: str) -> dict[str, Any]:
    out = dict(secondary or {})
    for k, v in (primary or {}).items():
        if v not in (None, "", []):
            out[k] = v
    out["_h40_source"] = source
    return out


def load_authoritative_market_snapshot(base_dir: str | Path | None = None) -> dict[str, Any]:
    """Load the freshest local runtime macro context, with runtime-data restore only when needed."""
    root = Path(base_dir or Path(__file__).resolve().parent)
    snapshot = _read_json(root, MARKET_FILE)
    bridge = _read_json(root, LEGACY_MARKET_FILE)

    snap_date = market_business_date(snapshot)
    bridge_date = market_business_date(bridge)
    if not snap_date and not bridge_date:
        # A fresh Streamlit process may start from the packaged checkout. H22 can
        # restore the runtime-data authority; call it only when local authority is
        # missing so normal Page07 reruns do not pay a network cost here.
        try:
            from macro_runtime_authority import ensure_macro_runtime_authority_current
            ensure_macro_runtime_authority_current()
            snapshot = _read_json(root, MARKET_FILE)
            bridge = _read_json(root, LEGACY_MARKET_FILE)
            snap_date = market_business_date(snapshot)
            bridge_date = market_business_date(bridge)
        except Exception:
            pass

    if snap_date and (not bridge_date or snap_date >= bridge_date):
        return _merge_context(snapshot, bridge, MARKET_FILE)
    if bridge_date:
        return _merge_context(bridge, snapshot, LEGACY_MARKET_FILE)
    return _merge_context(snapshot, bridge, "local-no-date") if (snapshot or bridge) else {}


def _stock_date(row: pd.Series) -> str:
    for col in (
        "本輪市場最新交易日", "K線最後交易日", "行情資料日期", "價格資料日期",
        "資料日期", "最後交易日", "最新交易日",
    ):
        if col in row.index:
            d = _date_text(row.get(col))
            if d:
                return d
    return ""


def _business_days_between(older: str, newer: str) -> int:
    if not older or not newer:
        return 999
    try:
        old = pd.Timestamp(older)
        new = pd.Timestamp(newer)
        if new <= old:
            return 0
        return int(len(pd.bdate_range(start=old + pd.Timedelta(days=1), end=new)))
    except Exception:
        return 999


def _alignment(market_date: str, stock_date: str) -> tuple[int, str, str, bool]:
    if not market_date:
        return 999, "日期未驗證｜正式推薦待同步", f"WAIT｜大盤未知／K線{stock_date or '未知'}", False
    if not stock_date:
        return 999, "K線日期未驗證｜正式推薦待同步", f"WAIT｜大盤{market_date}／K線未知", False
    if market_date == stock_date:
        return 0, "最新/對齊", "PASS｜大盤與K線同交易日", True
    if market_date < stock_date:
        lag = _business_days_between(market_date, stock_date)
        if lag == 1:
            return 1, "落後1日｜正式推薦待同步", f"WAIT｜大盤{market_date}落後K線{stock_date} 1交易日", False
        return lag, f"過期｜落後{lag}交易日", f"BLOCK｜大盤{market_date}落後K線{stock_date} {lag}交易日", False
    ahead = _business_days_between(stock_date, market_date)
    return -ahead, f"大盤較K線新{ahead}交易日｜等待K線", f"WAIT｜大盤{market_date}較K線{stock_date}新{ahead}交易日", False


def _risk_light(snapshot: dict[str, Any]) -> str:
    gate = _text(snapshot.get("risk_gate")).lower()
    risk = _text(snapshot.get("market_risk_level"))
    if not gate and not risk:
        return ""
    if gate in {"lockdown", "red", "severe", "panic"} or any(k in risk for k in ("極高", "嚴重", "恐慌")):
        return "紅燈"
    if gate in {"defensive", "yellow", "caution", "watch"} or any(k in risk for k in ("偏高", "中高")):
        return "黃燈"
    if gate in {"normal", "green", "selective"} or risk:
        return "綠燈"
    return ""


def apply_authoritative_market_context(
    df: pd.DataFrame | None,
    *,
    snapshot: dict[str, Any] | None = None,
    base_dir: str | Path | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Stamp one authoritative market date and derive row-level K-line alignment."""
    if df is None or not isinstance(df, pd.DataFrame):
        return pd.DataFrame(), {"version": VERSION, "ok": False, "message": "candidate frame invalid"}
    out = df.copy()
    if out.empty:
        return out, {"version": VERSION, "ok": True, "rows": 0, "market_date": ""}

    payload = dict(snapshot) if isinstance(snapshot, dict) else load_authoritative_market_snapshot(base_dir)
    market_date = market_business_date(payload)
    source = _text(payload.get("_h40_source") or payload.get("twse_source") or payload.get("source") or MARKET_FILE)

    previous = out.get("大盤資料日期", pd.Series([""] * len(out), index=out.index)).map(_date_text)
    stock_dates = out.apply(_stock_date, axis=1)

    if market_date:
        out["大盤資料日期"] = market_date
    elif "大盤資料日期" not in out.columns:
        out["大盤資料日期"] = previous

    lags, freshness, statuses, aligned_flags = [], [], [], []
    for idx in out.index:
        row_market = market_date or _date_text(out.at[idx, "大盤資料日期"] if "大盤資料日期" in out.columns else "")
        lag, fresh, status, aligned = _alignment(row_market, stock_dates.loc[idx])
        lags.append(lag); freshness.append(fresh); statuses.append(status); aligned_flags.append(aligned)

    out["大盤資料落後交易日"] = lags
    out["大盤資料新鮮度"] = freshness
    out["大盤與K線對齊狀態"] = statuses
    out["H40大盤權威日期"] = market_date
    out["H40大盤權威來源"] = source
    out["H40市場日期修正前"] = previous
    out["H40市場日期一致性"] = [
        "PASS｜權威大盤日與K線一致" if ok else status
        for ok, status in zip(aligned_flags, statuses)
    ]
    out["V191_H40大盤權威橋接"] = "是" if market_date else "否"
    out["V191_H40大盤權威版本"] = VERSION

    def fill_blank(col: str, value: Any) -> None:
        if col not in out.columns:
            out[col] = value
            return
        s = out[col]
        try:
            blank = s.isna() | s.astype("string").fillna("").str.strip().eq("")
            out.loc[blank, col] = value
        except Exception:
            pass

    # Missing macro metrics are unknown, not bearish zeroes.  Writing 0 into an
    # absent market_score makes H34 treat an otherwise valid selective market as
    # LOCKDOWN (score < 42).  Only stamp fields when authority actually provided them.
    _market_score = payload.get("market_score")
    if _text(_market_score):
        fill_blank("大盤橋接分數", _market_score)
    _strategy = payload.get("macro_mode") or payload.get("market_trend") or payload.get("market_state") or ""
    if _text(_strategy):
        fill_blank("大盤策略模式", _strategy)
    _light = _risk_light(payload)
    if _light:
        fill_blank("大盤風險燈號", _light)
    _risk = payload.get("market_risk_level") or ""
    if _text(_risk):
        fill_blank("大盤風險等級", _risk)
    if source:
        fill_blank("大盤資料來源", source)

    aligned_count = int(sum(bool(x) for x in aligned_flags))
    diag = {
        "version": VERSION,
        "ok": bool(market_date and aligned_count == len(out)),
        "rows": int(len(out)),
        "aligned_rows": aligned_count,
        "stamped_rows": int(out["大盤資料日期"].astype(str).str.strip().ne("").sum()) if "大盤資料日期" in out.columns else 0,
        "market_date": market_date,
        "source": source,
        "missing_authority": not bool(market_date),
        "message": (
            f"H40 market authority {market_date} aligned {aligned_count}/{len(out)} rows"
            if market_date else "H40 market authority date missing; decision remains fail-closed"
        ),
    }
    return out, diag


__all__ = ["VERSION", "market_business_date", "load_authoritative_market_snapshot", "apply_authoritative_market_context"]
