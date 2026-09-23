# -*- coding: utf-8 -*-
"""H95 Page18 fast-entry local-first service.

Purpose
-------
Keep the H81 professional research center responsive on Streamlit Cloud.
Opening Page18 must never require GitHub/Firestore reads, Page08 history restores,
or a full-market H81 recomputation.  Remote authority sync and heavy research are
explicit, user-triggered actions.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable
import json
import math

import pandas as pd

VERSION = "v191_h95_page18_local_first_lazy_research_20260923"
BASE_DIR = Path(__file__).resolve().parent

LOCAL_SETTINGS = "godpick_h81_professional_settings.json"
LOCAL_ANCHOR = "godpick_latest_run_anchor.json"
LOCAL_RECOMMENDATIONS = "godpick_latest_recommendations.json"
LOCAL_RECORDS = "godpick_records.json"


def _read_json(path: Path, default: Any) -> Any:
    try:
        if not path.exists() or not path.is_file():
            return default
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return default


def load_h81_settings_local(default_settings: dict[str, Any], normalize_func=None, *, base_dir: Path | None = None) -> tuple[dict[str, Any], list[str]]:
    base = Path(base_dir or BASE_DIR)
    path = base / LOCAL_SETTINGS
    payload = _read_json(path, None)
    details: list[str] = []
    if not isinstance(payload, dict):
        payload = dict(default_settings or {})
        details.append("H95：本機尚無H81設定快照，先使用程式預設值；不在進頁時連線遠端。")
    else:
        details.append(f"H95：H81設定使用本機快照 {path.name}。")
    if callable(normalize_func):
        try:
            payload = normalize_func(payload)
        except Exception as exc:
            details.append(f"H95：設定正規化失敗，維持安全預設：{type(exc).__name__}: {exc}")
            payload = normalize_func(dict(default_settings or {}))
    return payload, details


def _table_rows(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [dict(x) for x in value if isinstance(x, dict)]
    if isinstance(value, dict):
        # pandas orient='split'
        cols = value.get("columns")
        data = value.get("data")
        if isinstance(cols, list) and isinstance(data, list):
            out = []
            for row in data:
                if isinstance(row, list):
                    out.append({str(c): (row[i] if i < len(row) else None) for i, c in enumerate(cols)})
            if out:
                return out
        for key in ["records", "rows", "items", "data", "recommendations", "full_rows"]:
            vv = value.get(key)
            if isinstance(vv, list):
                rows = [dict(x) for x in vv if isinstance(x, dict)]
                if rows:
                    return rows
    return []


def _extract_candidate_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return _table_rows(payload)
    if not isinstance(payload, dict):
        return []

    buckets: list[list[dict[str, Any]]] = []
    core = payload.get("h79_core_tables")
    if isinstance(core, dict):
        for key in ["actionable", "research", "waiting", "emerging_watch"]:
            rows = _table_rows(core.get(key))
            if rows:
                buckets.append(rows)
    for key in ["actionable", "research", "recommendations", "waiting", "candidate_diagnosis", "full_rows", "rows", "records"]:
        rows = _table_rows(payload.get(key))
        if rows:
            buckets.append(rows)

    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for rows in buckets:
        for row in rows:
            code = str(row.get("股票代號") or row.get("stock_code") or "").strip().replace(".0", "")
            key = code or json.dumps(row, ensure_ascii=False, sort_keys=True, default=str)[:240]
            if key in seen:
                continue
            seen.add(key)
            out.append(row)
    return out


def load_latest_candidates_local(*, base_dir: Path | None = None, max_rows: int = 400) -> tuple[pd.DataFrame, list[str], str]:
    base = Path(base_dir or BASE_DIR)
    diagnostics: list[str] = []
    for filename, label in [(LOCAL_ANCHOR, "H92/H94永久小型錨點本機鏡像"), (LOCAL_RECOMMENDATIONS, "完整推薦本機鏡像")]:
        path = base / filename
        payload = _read_json(path, None)
        rows = _extract_candidate_rows(payload)
        if rows:
            frame = pd.DataFrame(rows)
            if "股票代號" in frame.columns:
                codes = frame["股票代號"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
                frame = frame.loc[codes.ne("")].copy()
                frame["股票代號"] = codes.loc[frame.index]
                frame = frame.loc[~frame["股票代號"].duplicated(keep="first")].copy()
            frame = frame.head(max(20, int(max_rows))).reset_index(drop=True)
            diagnostics.append(f"H95：候選使用{label} {filename}，共{len(frame)}檔；進頁不連GitHub/Firestore。")
            return frame, diagnostics, filename
        if path.exists():
            diagnostics.append(f"H95：{filename}存在但沒有可解析候選列。")
    diagnostics.append("H95：本機尚無可用推薦快照；頁面仍可立即開啟，請按『同步永久權威』或先至第7頁完成推薦。")
    return pd.DataFrame(), diagnostics, ""


def load_records_local(*, base_dir: Path | None = None, max_rows: int = 5000) -> tuple[pd.DataFrame, list[str]]:
    base = Path(base_dir or BASE_DIR)
    path = base / LOCAL_RECORDS
    payload = _read_json(path, None)
    rows: list[dict[str, Any]] = []
    if isinstance(payload, list):
        rows = [dict(x) for x in payload if isinstance(x, dict)]
    elif isinstance(payload, dict):
        for key in ["records", "rows", "items", "data"]:
            vv = payload.get(key)
            if isinstance(vv, list):
                rows = [dict(x) for x in vv if isinstance(x, dict)]
                break
    if not rows:
        return pd.DataFrame(), ["H95：本機沒有第8頁推薦紀錄；只有在回測/交易日誌頁需要時才建議同步永久權威。"]
    frame = pd.DataFrame(rows)
    if len(frame) > int(max_rows):
        frame = frame.tail(int(max_rows)).copy()
    return frame.reset_index(drop=True), [f"H95：第8頁紀錄使用本機 {path.name}，{len(frame)}筆。"]


def candidate_options(frame: pd.DataFrame | None) -> list[tuple[str, str]]:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty or "股票代號" not in frame.columns:
        return []
    names = frame.get("股票名稱", pd.Series([""] * len(frame), index=frame.index)).fillna("").astype(str)
    codes = frame["股票代號"].fillna("").astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    result: list[tuple[str, str]] = []
    seen: set[str] = set()
    for code, name in zip(codes, names):
        if code and code not in seen:
            seen.add(code)
            result.append((code, f"{code} {name}".strip()))
    return result


def fast_existing_h81_preview(frame: pd.DataFrame | None, *, top_n: int = 5) -> pd.DataFrame:
    """Use already-persisted H81 evidence. Never recompute the whole market."""
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return pd.DataFrame()
    if "H81專業研究總分" not in frame.columns:
        return pd.DataFrame()
    out = frame.copy()
    out["H81專業研究總分"] = pd.to_numeric(out["H81專業研究總分"], errors="coerce")
    if "H81資料覆蓋%" in out.columns:
        out["H81資料覆蓋%"] = pd.to_numeric(out["H81資料覆蓋%"], errors="coerce")
        sort_cols = ["H81專業研究總分", "H81資料覆蓋%"]
    else:
        sort_cols = ["H81專業研究總分"]
    out = out.sort_values(sort_cols, ascending=False, na_position="last").head(max(1, int(top_n)))
    cols = [c for c in ["股票代號", "股票名稱", "H94研究層級", "H94Alpha品質分", "H81專業研究總分", "H81資料覆蓋%", "H81排名加減分", "H81三大利多催化", "H81三大風險"] if c in out.columns]
    return out[cols].reset_index(drop=True)


def bounded_research_preview(frame: pd.DataFrame | None, settings: dict[str, Any], *, max_eval: int = 80, top_n: int = 5) -> pd.DataFrame:
    """Explicit heavy action: evaluate only a bounded pre-ranked subset."""
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return pd.DataFrame()
    work = frame.copy()
    score_col = next((c for c in ["H94Alpha品質分", "H93七維總分", "H79自適應機會分", "V188股神作戰優先分", "股神推薦優先分"] if c in work.columns), None)
    if score_col:
        work[score_col] = pd.to_numeric(work[score_col], errors="coerce")
        work = work.sort_values(score_col, ascending=False, na_position="last")
    work = work.head(max(10, int(max_eval))).copy()
    from godpick_h81_professional_ai import apply_professional_research_overlay
    ranked = apply_professional_research_overlay(work, settings)
    if "H81專業研究總分" in ranked.columns:
        ranked["H81專業研究總分"] = pd.to_numeric(ranked["H81專業研究總分"], errors="coerce")
        ranked = ranked.sort_values("H81專業研究總分", ascending=False, na_position="last")
    return ranked.head(max(1, int(top_n))).reset_index(drop=True)
