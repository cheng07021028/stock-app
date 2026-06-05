# -*- coding: utf-8 -*-
"""股神推薦共用執行快取 / Phase 4。

目的：
- 把市場、族群、籌碼、飆股獵人等 DataFrame 衍生計算集中快取。
- 避免同一輪推薦在畫面、匯出、紀錄串接時重複 groupby / apply。
- 不讀寫 JSON、不改任何正式資料，只保存本次 Python process 內的短暫計算結果。
"""
from __future__ import annotations

from typing import Any, Callable
import hashlib
import json
import time

import pandas as pd

RUNTIME_CACHE_VERSION = "phase4_runtime_cache_20260605"
_CACHE: dict[str, tuple[float, Any]] = {}
_MAX_ITEMS = 32
_DEFAULT_TTL = 900.0


def _safe_text(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def dataframe_fingerprint(df: pd.DataFrame | None, *, cols: list[str] | None = None, max_rows: int = 5000) -> str:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return "empty"
    use_cols = cols or [
        "股票代號", "股票名稱", "類別", "推薦總分", "股神實戰總分", "最新價",
        "近5日漲幅%", "近20日漲幅%", "成交額百萬", "最新成交量_張",
        "外資近1日買賣超", "投信近1日買賣超", "自營商近1日買賣超",
    ]
    use_cols = [c for c in use_cols if c in df.columns]
    if not use_cols:
        use_cols = list(df.columns[:20])
    sample = df.loc[:, use_cols].head(max_rows).copy()
    payload = {
        "version": RUNTIME_CACHE_VERSION,
        "rows": int(len(df)),
        "cols": use_cols,
        "data": sample.fillna("").astype(str).to_dict(orient="split"),
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()


def cache_key(namespace: str, df: pd.DataFrame | None, *, extra: str = "") -> str:
    return f"{namespace}:{dataframe_fingerprint(df)}:{_safe_text(extra)}"


def get_or_compute(key: str, factory: Callable[[], Any], *, ttl: float = _DEFAULT_TTL) -> Any:
    now = time.time()
    hit = _CACHE.get(key)
    if hit is not None:
        ts, value = hit
        if now - ts <= ttl:
            try:
                if isinstance(value, pd.DataFrame):
                    return value.copy()
            except Exception:
                pass
            return value
    value = factory()
    if len(_CACHE) >= _MAX_ITEMS:
        oldest = sorted(_CACHE.items(), key=lambda kv: kv[1][0])[: max(1, len(_CACHE) - _MAX_ITEMS + 1)]
        for k, _ in oldest:
            _CACHE.pop(k, None)
    if isinstance(value, pd.DataFrame):
        _CACHE[key] = (now, value.copy())
        return value.copy()
    _CACHE[key] = (now, value)
    return value


def clear_runtime_cache() -> None:
    _CACHE.clear()
