
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Callable
import copy
import hashlib
import json

def safe_str(v: Any) -> str:
    if v is None:
        return ""
    try:
        return str(v).strip()
    except Exception:
        return ""

def normalize_code(v: Any) -> str:
    s = safe_str(v)
    if not s:
        return ""
    if s.isdigit():
        return s
    digits = "".join(ch for ch in s if ch.isdigit())
    if 4 <= len(digits) <= 6:
        return digits
    return s

def make_signature(payload: Any) -> str:
    try:
        text = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    except Exception:
        text = repr(payload)
    return hashlib.md5(text.encode("utf-8")).hexdigest()

def dedupe_stock_rows(rows: list[dict[str, Any]], key_fields: tuple[str, ...] = ("股票代號", "市場別")) -> list[dict[str, Any]]:
    out = []
    seen = set()
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        norm = dict(row)
        for k in list(norm.keys()):
            if "代號" in k or k == "code":
                norm[k] = normalize_code(norm.get(k))
            elif isinstance(norm.get(k), str):
                norm[k] = safe_str(norm.get(k))
        key_parts = []
        for field in key_fields:
            if field in {"股票代號", "code"}:
                key_parts.append(normalize_code(norm.get(field)))
            else:
                key_parts.append(safe_str(norm.get(field)))
        key = tuple(key_parts)
        if not key_parts or not key_parts[0]:
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(norm)
    return out

def session_cached_compute(st, cache_name: str, signature: str, compute_fn: Callable[[], Any]):
    sig_key = f"perf_sig_{cache_name}"
    val_key = f"perf_val_{cache_name}"
    if st.session_state.get(sig_key) == signature and val_key in st.session_state:
        return copy.deepcopy(st.session_state[val_key]), True
    value = compute_fn()
    st.session_state[sig_key] = signature
    st.session_state[val_key] = copy.deepcopy(value)
    return value, False
