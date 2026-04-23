# -*- coding: utf-8 -*-
"""
watchlist_runtime_sync.py

用途：
讓 4_自選股中心.py 在 watchlist_version 改變時，強制重載 watchlist.json，
避免 8_股神推薦紀錄 / 7_股神推薦 匯入後，4_自選股中心畫面還停留舊資料。

使用方式（貼到 4_自選股中心.py）：

from watchlist_runtime_sync import ensure_watchlist_runtime_fresh

watchlist_data = ensure_watchlist_runtime_fresh(
    load_func=get_normalized_watchlist,
    namespace="watchlist_center",
)

之後頁面都用 watchlist_data，不要直接只讀舊的 session_state。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Callable
import copy


def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    try:
        return str(v).strip()
    except Exception:
        return ""


def _normalize_code(v: Any) -> str:
    s = _safe_str(v)
    if not s:
        return ""
    if s.isdigit():
        return s
    digits = "".join(ch for ch in s if ch.isdigit())
    if 4 <= len(digits) <= 6:
        return digits
    return s


def _normalize_watchlist_payload(data: Any) -> dict[str, list[dict[str, str]]]:
    payload: dict[str, list[dict[str, str]]] = {}
    if not isinstance(data, dict):
        return payload

    for group_name, items in data.items():
        g = _safe_str(group_name)
        if not g:
            continue

        rows: list[dict[str, str]] = []
        seen: set[str] = set()

        if isinstance(items, list):
            for item in items:
                if isinstance(item, dict):
                    code = _normalize_code(item.get("code"))
                    name = _safe_str(item.get("name")) or code
                    market = _safe_str(item.get("market")) or "上市"
                    category = _safe_str(item.get("category"))
                else:
                    code = _normalize_code(item)
                    name = code
                    market = "上市"
                    category = ""

                if not code or code in seen:
                    continue
                seen.add(code)

                row = {"code": code, "name": name, "market": market}
                if category:
                    row["category"] = category
                rows.append(row)

        payload[g] = sorted(rows, key=lambda x: (_normalize_code(x.get("code")), _safe_str(x.get("name"))))

    return payload


def ensure_watchlist_runtime_fresh(load_func: Callable[[], Any], namespace: str = "watchlist_center") -> dict[str, list[dict[str, str]]]:
    """
    依據 session_state 中的 watchlist_version 強制刷新 watchlist。
    需要在 Streamlit 頁面內呼叫。

    參數
    ----
    load_func:
        例如 utils.get_normalized_watchlist
    namespace:
        每個頁面給不同名稱，避免 서로覆蓋
    """
    import streamlit as st

    ver_key = f"{namespace}_seen_watchlist_version"
    data_key = f"{namespace}_watchlist_data"
    reload_key = f"{namespace}_last_reload_at"

    global_version = int(st.session_state.get("watchlist_version", 0) or 0)
    seen_version = int(st.session_state.get(ver_key, -1) or -1)

    need_reload = False

    if data_key not in st.session_state:
        need_reload = True
    elif global_version != seen_version:
        need_reload = True

    if need_reload:
        fresh = {}
        try:
            fresh = load_func() if callable(load_func) else {}
        except Exception:
            fresh = {}

        fresh = _normalize_watchlist_payload(fresh)
        st.session_state[data_key] = copy.deepcopy(fresh)
        st.session_state[ver_key] = global_version
        st.session_state[reload_key] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return fresh

    cached = st.session_state.get(data_key, {})
    return _normalize_watchlist_payload(cached)


def build_watchlist_group_options(watchlist_data: dict[str, list[dict[str, str]]]) -> list[str]:
    if not isinstance(watchlist_data, dict):
        return []
    return [g for g in watchlist_data.keys() if _safe_str(g)]


def flatten_watchlist_items(watchlist_data: dict[str, list[dict[str, str]]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    if not isinstance(watchlist_data, dict):
        return rows

    for group_name, items in watchlist_data.items():
        for item in items or []:
            if not isinstance(item, dict):
                continue
            rows.append(
                {
                    "群組": _safe_str(group_name),
                    "股票代號": _normalize_code(item.get("code")),
                    "股票名稱": _safe_str(item.get("name")),
                    "市場別": _safe_str(item.get("market")) or "上市",
                    "類別": _safe_str(item.get("category")),
                }
            )
    return rows
