"""H82 runtime bridge for Streamlit hot-reload and adaptive-learning compatibility.

Streamlit can keep the already-imported H78 module in ``sys.modules`` after a
copy-over deployment.  The H79 page then calls the old ``build_tables`` and
receives the H78 schema, which does not contain ``actionable``.  This bridge
checks the engine version and returned contract, reloads once from disk, and
always returns a complete H79 table mapping instead of leaking ``KeyError`` to
the user interface or Excel export.
"""

from __future__ import annotations

import importlib
from types import ModuleType
from typing import Any

import pandas as pd


VERSION = "v191_h91_duplicate_column_contract_guard_20260922"
EXPECTED_ENGINE_VERSION = "v191_h91_duplicate_column_truth_recovery_20260922"
REQUIRED_TABLES = (
    "actionable",
    "research",
    "recommendations",
    "waiting",
    "emerging_watch",
    "data_repairs",
    "audit",
    "health",
)


def _failure_tables(message: str) -> dict[str, pd.DataFrame]:
    frame = pd.DataFrame({"狀態": [message]})
    return {key: frame.copy() for key in REQUIRED_TABLES}


def _contract_ok(value: Any) -> bool:
    return isinstance(value, dict) and all(key in value for key in REQUIRED_TABLES)


def _normalise(value: dict[str, Any]) -> dict[str, pd.DataFrame]:
    result: dict[str, pd.DataFrame] = {}
    for key in REQUIRED_TABLES:
        table = value.get(key)
        if isinstance(table, pd.DataFrame):
            result[key] = table
        elif table is None:
            result[key] = pd.DataFrame()
        else:
            try:
                result[key] = pd.DataFrame(table)
            except Exception:
                result[key] = pd.DataFrame({"狀態": [f"H89 無法解析 {key} 表格。"]})
    return result


def _load_engine(module: ModuleType | None = None, *, force_reload: bool = False) -> ModuleType:
    if module is None:
        module = importlib.import_module("godpick_h78_decision_engine")
    if force_reload or getattr(module, "VERSION", "") != EXPECTED_ENGINE_VERSION:
        importlib.invalidate_caches()
        module = importlib.reload(module)
    return module


def _coalesce_duplicate_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Bridge-level defense: use the same newest-nonblank truth for duplicate labels."""
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return pd.DataFrame() if frame is None else frame
    work = frame.copy(deep=True)
    labels = [str(c) for c in work.columns]
    work.columns = labels
    if work.columns.is_unique:
        return work
    ordered: list[str] = []
    pos: dict[str, list[int]] = {}
    for i, name in enumerate(labels):
        if name not in pos:
            ordered.append(name); pos[name] = []
        pos[name].append(i)
    clean = pd.DataFrame(index=work.index)
    for name in ordered:
        picks = pos[name]
        chosen = work.iloc[:, picks[-1]].copy()
        for j in reversed(picks[:-1]):
            earlier = work.iloc[:, j]
            try:
                mask = chosen.isna() | chosen.astype("string").fillna("").str.strip().eq("")
            except Exception:
                mask = chosen.isna()
            if bool(mask.any()):
                chosen = chosen.where(~mask, earlier)
        clean[name] = chosen
    return clean


def build_tables_guarded(frame: pd.DataFrame, *, engine_module: ModuleType | None = None, **kwargs: Any) -> dict[str, pd.DataFrame]:
    """Build H79 tables after validating both code version and output schema.

    ``engine_module`` exists for deterministic testing of stale in-memory
    modules.  Production callers leave it unset.
    """

    try:
        module = _load_engine(engine_module)
    except Exception as exc:
        return _failure_tables(f"H89 引擎載入失敗：{type(exc).__name__}: {exc}")

    if getattr(module, "VERSION", "") != EXPECTED_ENGINE_VERSION:
        return _failure_tables(
            "H89 已阻止舊版決策引擎：磁碟上的 godpick_h78_decision_engine.py "
            f"版本為 {getattr(module, 'VERSION', '未知')}，請用本修正版 ZIP 完整覆蓋後重新啟動。"
        )

    try:
        builder = getattr(module, "build_tables")
        frame = _coalesce_duplicate_columns(frame)
        tables = builder(frame, **kwargs)
    except Exception as exc:
        return _failure_tables(f"H89 決策建立失敗：{type(exc).__name__}: {exc}")

    if not _contract_ok(tables):
        # The version constant and function object can be out of sync during a
        # Streamlit hot reload.  Reload once more and rebuild from disk.
        try:
            module = _load_engine(module, force_reload=True)
            tables = getattr(module, "build_tables")(frame, **kwargs)
        except Exception as exc:
            return _failure_tables(f"H89 引擎重新載入失敗：{type(exc).__name__}: {exc}")

    if not _contract_ok(tables):
        missing = [key for key in REQUIRED_TABLES if not isinstance(tables, dict) or key not in tables]
        return _failure_tables(
            "H89 已攔截舊版表格契約；缺少：" + "、".join(missing) + "。請完整覆蓋 ZIP 後重新啟動。"
        )
    return _normalise(tables)

