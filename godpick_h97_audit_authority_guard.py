# -*- coding: utf-8 -*-
"""V191-H97 Page07 audit-authority / scan-run binding guard.

H97 fixes a cross-run authority bug discovered from the 2026-09-23 manager
workbook: the current scan reported 1,645 successful candidates while the
compact H79/H94/H96 core still carried an empty Audit table from an older
session snapshot.  H92 previously considered any compact core with a
``created_at`` field reusable, even when it belonged to a different scan run.

This module does not rank stocks and has no Formal authority.  It only:
* fingerprints the current candidate universe;
* stamps a compact decision core with the scan-run/candidate identity;
* rejects stale, unbound, or audit-empty compact cores for a newly completed
  scan;
* exposes small diagnostics that Page07 can persist/export.
"""
from __future__ import annotations

from hashlib import sha256
from typing import Any, Iterable

import pandas as pd

VERSION = "v191_h97_audit_authority_recovery_20260924"
META_KEY = "h97_audit_authority"

_DATE_COLUMNS = (
    "本輪市場最新交易日",
    "K線最後交易日",
    "行情資料日期",
    "價格資料日期",
    "資料基準日",
    "H79資料基準日",
)


def _text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def _code(value: Any) -> str:
    value = _text(value)
    if value.endswith(".0"):
        value = value[:-2]
    return value


def _records(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, pd.DataFrame):
        if value.empty:
            return []
        try:
            return value.to_dict(orient="records")
        except Exception:
            return []
    if isinstance(value, list):
        return [row for row in value if isinstance(row, dict)]
    return []


def _latest_date(frame: pd.DataFrame | None) -> str:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return ""
    for column in _DATE_COLUMNS:
        if column not in frame.columns:
            continue
        try:
            values = pd.to_datetime(frame[column], errors="coerce")
            if values.notna().any():
                return pd.Timestamp(values.max()).strftime("%Y-%m-%d")
        except Exception:
            continue
    return ""


def candidate_signature(frame: pd.DataFrame | None) -> str:
    """Return a deterministic, cheap identity for the current scan universe.

    The signature is intentionally independent of row order.  It binds the core
    to candidate count, stock-code set and latest business date without
    serialising the multi-MB full candidate frame.
    """
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return ""
    codes: list[str] = []
    if "股票代號" in frame.columns:
        try:
            codes = sorted({_code(v) for v in frame["股票代號"].tolist() if _code(v)})
        except Exception:
            codes = []
    if not codes:
        # Fallback still prevents accidental reuse across very different scans.
        codes = [f"rows:{len(frame)}"]
    material = f"rows={len(frame)}|date={_latest_date(frame)}|codes={'/'.join(codes)}"
    return sha256(material.encode("utf-8", errors="ignore")).hexdigest()


def audit_row_count(core: dict[str, Any] | None) -> int:
    if not isinstance(core, dict):
        return 0
    rows = _records(core.get("audit"))
    if not rows:
        return 0
    # A one-row conclusion/status placeholder is not a discovery audit row.
    valid = 0
    for row in rows:
        if _code(row.get("股票代號")):
            valid += 1
    return valid


def stamp_core(
    core: dict[str, Any] | None,
    candidate_df: pd.DataFrame | None,
    *,
    run_id: str = "",
    run_date: str = "",
    source: str = "",
) -> dict[str, Any]:
    out = dict(core or {})
    signature = candidate_signature(candidate_df)
    meta = {
        "version": VERSION,
        "run_id": _text(run_id),
        "run_date": _text(run_date)[:10],
        "candidate_count": int(len(candidate_df)) if isinstance(candidate_df, pd.DataFrame) else 0,
        "candidate_signature": signature,
        "business_date": _latest_date(candidate_df),
        "audit_rows": audit_row_count(out),
        "source": _text(source),
        "formal_authority": "LOCKED",
    }
    out[META_KEY] = meta
    out["h97_version"] = VERSION
    out["h97_run_id"] = meta["run_id"]
    out["h97_candidate_signature"] = signature
    out["h97_audit_rows"] = meta["audit_rows"]
    return out


def validate_core_for_scan(
    core: dict[str, Any] | None,
    candidate_df: pd.DataFrame | None,
    *,
    run_id: str = "",
    require_audit: bool = True,
) -> tuple[bool, str]:
    """Validate whether a compact core is safe to reuse for *this* scan.

    Old H85/H88/H92 cores are deliberately rejected when a current scan exists,
    because they lack H97 binding metadata.  During a pure reboot restore with
    no current candidate frame/run id, callers can still load old snapshots for
    display compatibility.
    """
    if not isinstance(core, dict) or not core:
        return False, "CORE_EMPTY"

    current_has_scan = bool(_text(run_id)) or (
        isinstance(candidate_df, pd.DataFrame) and not candidate_df.empty
    )
    meta = core.get(META_KEY)
    if not isinstance(meta, dict):
        if current_has_scan:
            return False, "UNBOUND_LEGACY_CORE"
        return True, "LEGACY_REBOOT_COMPAT"

    expected_run = _text(run_id)
    actual_run = _text(meta.get("run_id"))
    if expected_run and actual_run != expected_run:
        return False, f"RUN_ID_MISMATCH:{actual_run or 'EMPTY'}"

    expected_sig = candidate_signature(candidate_df)
    actual_sig = _text(meta.get("candidate_signature"))
    if expected_sig and actual_sig != expected_sig:
        return False, "CANDIDATE_SIGNATURE_MISMATCH"

    if require_audit and audit_row_count(core) <= 0:
        return False, "AUDIT_EMPTY"

    return True, "MATCH"


def health_rows(core: dict[str, Any] | None, *, decision: str = "", reason: str = "") -> pd.DataFrame:
    meta = core.get(META_KEY, {}) if isinstance(core, dict) else {}
    if not isinstance(meta, dict):
        meta = {}
    return pd.DataFrame([
        {"項目": "H97版本", "數值": VERSION},
        {"項目": "H97掃描核心綁定", "數值": decision or "UNKNOWN"},
        {"項目": "H97核心判定原因", "數值": reason or ""},
        {"項目": "H97核心候選數", "數值": int(meta.get("candidate_count") or 0)},
        {"項目": "H97Audit有效列", "數值": int(meta.get("audit_rows") or audit_row_count(core))},
        {"項目": "H97市場資料日", "數值": _text(meta.get("business_date"))},
        {"項目": "H97Formal權限", "數值": "LOCKED｜H97只保證本輪Audit/Research資料權威，不建立買進權限"},
    ])
