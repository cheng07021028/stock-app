# -*- coding: utf-8 -*-
"""V191-H25 authoritative market context bridge for Page07 candidates.

The formal engine intentionally requires candidate K-line date and market date to
be aligned.  Page07 previously carried ``本輪市場最新交易日`` but did not copy the
actual ``market_snapshot.json`` business date into the recognised market-date
columns.  That made ``market_ready`` false for every stock even when the macro
snapshot itself was current.

H25 is an orchestration bridge only.  It does not alter Entry/Risk/RR/liquidity
thresholds and it does not make stale K-lines tradable.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any
import json
import pandas as pd

VERSION = "v191_h25_authoritative_market_candidate_bridge_20260815"
MARKET_FILE = "market_snapshot.json"


def _text(value: Any) -> str:
    try:
        return "" if value is None else str(value).strip()
    except Exception:
        return ""


def _date_text(value: Any) -> str:
    text = _text(value).replace("/", "-")
    if not text:
        return ""
    try:
        return pd.to_datetime(text, errors="raise").strftime("%Y-%m-%d")
    except Exception:
        return ""


def load_authoritative_market_snapshot(base_dir: str | Path | None = None) -> dict[str, Any]:
    root = Path(base_dir or Path(__file__).resolve().parent)
    path = root / MARKET_FILE
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def market_business_date(snapshot: dict[str, Any] | None) -> str:
    payload = snapshot if isinstance(snapshot, dict) else {}
    for key in ("twse_data_date", "market_date", "data_date"):
        d = _date_text(payload.get(key))
        if d:
            return d
    return ""


def _risk_light(snapshot: dict[str, Any]) -> str:
    gate = _text(snapshot.get("risk_gate")).lower()
    risk = _text(snapshot.get("market_risk_level"))
    if gate in {"lockdown", "red", "severe", "panic"} or any(k in risk for k in ("極高", "嚴重", "恐慌")):
        return "紅燈"
    if gate in {"defensive", "yellow", "caution", "watch"} or any(k in risk for k in ("偏高", "中高")):
        return "黃燈"
    return "綠燈"


def apply_authoritative_market_context(
    df: pd.DataFrame | None,
    *,
    snapshot: dict[str, Any] | None = None,
    base_dir: str | Path | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Stamp Page07 candidates with the authoritative macro business date.

    The authoritative date may move forward but never backward.  Other market
    fields are filled from the same snapshot only when missing/blank so this
    bridge cannot overwrite richer per-run diagnostics produced elsewhere.
    """
    if df is None or not isinstance(df, pd.DataFrame):
        return pd.DataFrame(), {"version": VERSION, "ok": False, "message": "candidate frame invalid"}
    out = df.copy()
    if out.empty:
        return out, {"version": VERSION, "ok": True, "rows": 0, "market_date": ""}

    payload = snapshot if isinstance(snapshot, dict) else load_authoritative_market_snapshot(base_dir)
    market_date = market_business_date(payload)
    if not market_date:
        return out, {
            "version": VERSION, "ok": False, "rows": len(out), "market_date": "",
            "message": "authoritative market_snapshot business date missing; formal gate remains fail-closed",
        }

    existing = out.get("大盤資料日期", pd.Series([""] * len(out), index=out.index)).map(_date_text)
    # Never downgrade a row if another trusted layer somehow has a later date.
    chosen = existing.where(existing.astype(str).gt(market_date), market_date)
    out["大盤資料日期"] = chosen
    out["大盤資料新鮮度"] = chosen.map(lambda x: "最新/權威橋接" if x == market_date else "較新資料")

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

    fill_blank("大盤橋接分數", payload.get("market_score", 0))
    fill_blank("大盤策略模式", payload.get("macro_mode") or payload.get("market_trend") or "")
    fill_blank("大盤風險燈號", _risk_light(payload))
    fill_blank("大盤風險等級", payload.get("market_risk_level") or "")
    fill_blank("大盤資料來源", payload.get("twse_source") or "market_snapshot.json")
    out["V191_H25大盤權威橋接"] = "是"
    out["V191_H25大盤權威版本"] = VERSION

    stamped = int(out["大盤資料日期"].astype(str).str.strip().ne("").sum())
    return out, {
        "version": VERSION,
        "ok": stamped == len(out),
        "rows": len(out),
        "stamped_rows": stamped,
        "market_date": market_date,
        "risk_light": _risk_light(payload),
        "message": f"H25 market authority {market_date} stamped {stamped}/{len(out)} rows",
    }


__all__ = ["VERSION", "market_business_date", "load_authoritative_market_snapshot", "apply_authoritative_market_context"]
