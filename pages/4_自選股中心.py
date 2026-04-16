# pages/4_自選股中心.py
from __future__ import annotations

from datetime import datetime
from typing import Any
import json
import os

import pandas as pd
import streamlit as st

from utils import (
    get_all_code_name_map,
    get_normalized_watchlist,
    inject_pro_theme,
    render_pro_hero,
    render_pro_info_card,
    render_pro_kpi_row,
    render_pro_section,
    save_watchlist,
)

PAGE_TITLE = "自選股中心"
PFX = "watch_"


# =========================================================
# 基礎工具
# =========================================================
def _k(key: str) -> str:
    return f"{PFX}{key}"


def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _normalize_code(code: Any) -> str:
    text = _safe_str(code)
    if not text:
        return ""
    if text.isdigit():
        return text
    digits = "".join(ch for ch in text if ch.isdigit())
    if 4 <= len(digits) <= 6:
        return digits
    return text


# =========================================================
# 讀取 / 儲存
# =========================================================
def _load_watchlist_data() -> dict[str, list[dict[str, str]]]:
    raw = get_normalized_watchlist()
    result: dict[str, list[dict[str, str]]] = {}

    if isinstance(raw, dict):
        for group_name, items in raw.items():
            g = _safe_str(group_name) or "未分組"
            result[g] = []

            if isinstance(items, list):
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    code = _normalize_code(item.get("code"))
                    name = _safe_str(item.get("name")) or code
                    market = _safe_str(item.get("market")) or "上市"
                    if code:
                        result[g].append(
                            {
                                "code": code,
                                "name": name,
                                "market": market,
                            }
                        )

    return result


def _normalize_watchlist_payload(data: dict[str, list[dict[str, str]]]) -> dict[str, list[dict[str, str]]]:
    payload: dict[str, list[dict[str, str]]] = {}

    for group_name, items in data.items():
        g = _safe_str(group_name)
        if not g:
            continue

        payload[g] = []
        seen = set()

        for item in items:
            if not isinstance(item, dict):
                continue

            code = _normalize_code(item.get("code"))
            name = _safe_str(item.get("name")) or code
            market = _safe_str(item.get("market")) or "上市"

            if not code:
                continue

            key = (g, code)
            if key in seen:
                continue
            seen.add(key)

            payload[g].append(
                {
                    "code": code,
                    "name": name,
                    "market": market,
                }
            )

    return payload


def _save_watchlist_data(data: dict[str, list[dict[str, str]]]) -> bool:
    payload = _normalize_watchlist_payload(data)

    candidate_paths = [
        "watchlist.json",
        "data/watchlist.json",
    ]

    success_count = 0

    for path in candidate_paths:
        try:
            folder = os.path.dirname(path)
            if folder:
                os.makedirs(folder, exist_ok=True)

            with open(path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)

            success_count += 1
        except Exception:
            pass

    try:
        if save_watchlist(payload, filepath="watchlist.json"):
            success_count = max(success_count, 1)
    except Exception:
        pass

    if success_count > 0:
        st.session_state[_k("watchlist")] = payload
        return True

    return False


def _persist_watchlist(success_msg: str, fail_msg: str = "儲存失敗，請檢查檔案權限。") -> bool:
    ok = _save_watchlist_data(st.session_state[_k("watchlist")])
    _set_status(
        f"{success_msg}｜{_now_text()}" if ok else fail_msg,
        "success" if ok else "error",
    )
    return ok


def _load_stock_master() -> pd.DataFrame:
    df = get_all_code_name_map("")
    if not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame(columns=["code", "name", "market"])

    out = df.copy()
    for col in ["code", "name", "market"]:
        if col not in out.columns:
            out[col] = ""

    out["code"] = out["code"].map(_normalize_code)
    out["name"] = out["name"].map(_safe_str)
    out["market"] = out["market"].map(_safe_str).replace("", "上市")
    out = out[out["code"] != ""].drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)
    return out


# =========================================================
# Session State
# =========================================================
def _init_state():
    if _k("watchlist") not in st.session_state:
        st.session_state[_k("watchlist")] = _load_watchlist_data()

    if _k("master_df") not in st.session_state:
        st.session_state[_k("master_df")] = _load_stock_master()

    if _k("selected_group") not in st.session_state:
        groups = list(st.session_state[_k("watchlist")].keys())
        st.session_state[_k("selected_group")] = groups[0] if groups else ""

    if _k("new_group_name") not in st.session_state:
        st.session_state[_k("new_group_name")] = ""

    if _k("add_code") not in st.session_state:
        st.session_state[_k("add_code")] = ""

    if _k("add_name") not in st.session_state:
        st.session_state[_k("add_name")] = ""

    if _k("add_market") not in st.session_state:
        st.session_state[_k("add_market")] = "上市"

    if _k("bulk_text") not in st.session_state:
        st.session_state[_k("bulk_text")] = ""

    if _k("search_text") not in st.session_state:
        st.session_state[_k("search_text")] = ""

    if _k("status_msg") not in st.session_state:
        st.session_state[_k("status_msg")] = ""

    if _k("status_type") not in st.session_state:
        st.session_state[_k("status_type")] = "info"

    if _k("add_code_next") in st.session_state:
        st.session_state[_k("add_code")] = st.session_state.pop(_k("add_code_next"))

    if _k("add_name_next") in st.session_state:
        st.session_state[_k("add_name")] = st.session_state.pop(_k("add_name_next"))

    if _k("add_market_next") in st.session_state:
        st.session_state[_k("add_market")] = st.session_state.pop(_k("add_market_next"))

    if _k("bulk_text_next") in st.session_state:
        st.session_state[_k("bulk_text")] = st.session_state.pop(_k("bulk_text_next"))

    _repair_selected_group()


def _repair_selected_group():
    watchlist = st.session_state[_k("watchlist")]
    groups = list(watchlist.keys())
    current = _safe_str(st.session_state.get(_k("selected_group"), ""))

    if groups:
        if current not in groups:
            st.session_state[_k("selected_group")] = groups[0]
    else:
        st.session_state[_k("selected_group")] = ""


def _set_status(msg: str, level: str = "info"):
    st.session_state[_k("status_msg")] = msg
    st.session_state[_k("status_type")] = level


# =========================================================
# 核心操作
# =========================================================
def _find_stock_name_market(code: str) -> tuple[str, str]:
    code = _normalize_code(code)
    master_df = st.session_state[_k("master_df")]
    if isinstance(master_df, pd.DataFrame) and not master_df.empty:
        matched = master_df[master_df["code"].astype(str) == code]
        if not matched.empty:
            row = matched.iloc[0]
            return _safe_str(row.get("name")) or code, _safe_str(row.get("market")) or "上市"
    return code, "上市"


def _find_stock_by_code_or_name(keyword: str) -> tuple[str, str, str]:
    text = _safe_str(keyword)
    if not text:
        return "", "", ""

    master_df = st.session_state[_k("master_df")]
    if not isinstance(master_df, pd.DataFrame) or master_df.empty:
        return "", "", ""

    parts = text.replace("，", " ").replace(",", " ").split()
    first = _normalize_code(parts[0]) if parts else ""

    if first and first.isdigit():
        matched = master_df[master_df["code"].astype(str) == first]
        if not matched.empty:
            row = matched.iloc[0]
            return (
                _normalize_code(row.get("code")),
                _safe_str(row.get("name")) or first,
                _safe_str(row.get("market")) or "上市",
            )

    matched = master_df[master_df["name"].astype(str).str.strip() == text]
    if not matched.empty:
        row = matched.iloc[0]
        return (
            _normalize_code(row.get("code")),
            _safe_str(row.get("name")) or text,
            _safe_str(row.get("market")) or "上市",
        )

    matched = master_df[
        master_df["name"].astype(str).str.contains(text, case=False, na=False)
        | master_df["code"].astype(str).str.contains(text, case=False, na=False)
    ]
    if not matched.empty:
        row = matched.iloc[0]
        return (
            _normalize_code(row.get("code")),
            _safe_str(row.get("name")) or text,
            _safe_str(row.get("market")) or "上市",
        )

    return "", "", ""


def _ensure_group(group_name: str):
    g = _safe_str(group_name)
    if not g:
        return
    watchlist = st.session_state[_k("watchlist")]
    if g not in watchlist:
        watchlist[g] = []
    st.session_state[_k("watchlist")] = watchlist
    st.session_state[_k("selected_group")] = g


def _add_stock(group_name: str, code: str, name: str = "", market: str = "") -> tuple[bool, str]:
    g = _safe_str(group_name)
    code = _normalize_code(code)
    name = _safe_str(name)
    market = _safe_str(market) or "上市"

    if not g:
        return False, "請先選擇群組。"

    if not code and name:
        found_code, found_name, found_market = _find_stock_by_code_or_name(name)
        code = found_code
        name = found_name or name
        market = found_market or market

    if code and not code.isdigit():
        found_code, found_name, found_market = _find_stock_by_code_or_name(code)
        code = found_code
        name = name or found_name
        market = found_market or market

    if not code:
        return False, "請輸入股票代碼或股票名稱。"

    if not name:
        name, market_from_master = _find_stock_name_market(code)
        market = market or market_from_master

    watchlist = st.session_state[_k("watchlist")]
    if g not in watchlist:
        watchlist[g] = []

    exists = any(_normalize_code(x.get("code")) == code for x in watchlist[g])
    if exists:
        return False, f"{g} 已存在 {code}。"

    watchlist[g].append(
        {
            "code": code,
            "name": name or code,
            "market": market or "上市",
        }
    )
    watchlist[g] = sorted(watchlist[g], key=lambda x: (_normalize_code(x.get("code")), _safe_str(x.get("name"))))
    st.session_state[_k("watchlist")] = watchlist
    return True, f"已加入 {g}：{code} {name or code}"


def _delete_stock(group_name: str, code: str) -> tuple[bool, str]:
    g = _safe_str(group_name)
    code = _normalize_code(code)
    watchlist = st.session_state[_k("watchlist")]

    if g not in watchlist:
        return False, "群組不存在。"

    before = len(watchlist[g])
    watchlist[g] = [x for x in watchlist[g] if _normalize_code(x.get("code")) != code]
    after = len(watchlist[g])
    st.session_state[_k("watchlist")] = watchlist

    if before == after:
        return False, f"{g} 沒有 {code}。"
    return True, f"已刪除 {g}：{code}"


def _delete_group(group_name: str) -> tuple[bool, str]:
    g = _safe_str(group_name)
    watchlist = st.session_state[_k("watchlist")]

    if not g or g not in watchlist:
        return False, "群組不存在。"

    del watchlist[g]
    st.session_state[_k("watchlist")] = watchlist
    _repair_selected_group()
    return True, f"已刪除群組：{g}"


def _parse_bulk_lines(text: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    raw_lines = [line.strip() for line in _safe_str(text).splitlines() if line.strip()]

    for line in raw_lines:
        normalized = line.replace("，", ",").replace("\t", ",").replace(" ", ",")
        parts = [p.strip() for p in normalized.split(",") if p.strip()]

        code = ""
        name = ""
        market = ""

        if len(parts) >= 1:
            code = _normalize_code(parts[0])
        if len(parts) >= 2:
            name = _safe_str(parts[1])
        if len(parts) >= 3:
            market = _safe_str(parts[2])

        if code:
            if not name:
                name, master_market = _find_stock_name_market(code)
                market = market or master_market
            rows.append(
                {
                    "code": code,
                    "name": name or code,
                    "market": market or "上市",
                }
            )

    return rows


def _apply_bulk_add(group_name: str, text: str) -> tuple[int, list[str]]:
    rows = _parse_bulk_lines(text)
    ok_count = 0
    messages = []

    for row in rows:
        ok, msg = _add_stock(group_name, row["code"], row["name"], row["market"])
        if ok:
            ok_count += 1
        messages.append(msg)

    return ok_count, messages


# =========================================================
# 畫面資料
# =========================================================
def _build_overview_df(watchlist: dict[str, list[dict[str, str]]]) -> pd.DataFrame:
    rows = []
    for group_name, items in watchlist.items():
        for item in items:
            rows.append(
                {
                    "群組": group_name,
                    "股票代號": _normalize_code(item.get("code")),
                    "股票名稱": _safe_str(item.get("name")),
                    "市場別": _safe_str(item.get("market")) or "上市",
                    "股票": f"{_normalize_code(item.get('code'))} {_safe_str(item.get('name'))}",
                }
            )
    if not rows:
        return pd.DataFrame(columns=["群組", "股票代號", "股票名稱", "市場別", "股票"])
    return pd.DataFrame(rows).sort_values(["群組", "股票代號"]).reset_index(drop=True)


def _build_group_summary_df(watchlist: dict[str, list[dict[str, str]]]) -> pd.DataFrame:
    rows = []
    for group_name, items in watchlist.items():
        rows.append(
            {
                "群組": group_name,
                "股票數": len(items),
                "市場別組成": " / ".join(sorted(set([_safe_str(x.get("market")) or "上市" for x in items]))) if items else "—",
            }
        )
    if not rows:
        return pd.DataFrame(columns=["群組", "股票數", "市場別組成"])
    return pd.DataFrame(rows).sort_values(["股票數", "群組"], ascending=[False, True]).reset_index(drop=True)


def _filter_master_df(df: pd.DataFrame, keyword: str) -> pd.DataFrame:
    q = _safe_str(keyword).lower()
    if df is None or df.empty:
        return pd.DataFrame(columns=["code", "name", "market"])

    if not q:
        return df.head(100).copy()

    mask = (
        df["code"].astype(str).str.lower().str.contains(q, na=False)
        | df["name"].astype(str).str.lower().str.contains(q, na=False)
        | df["market"].astype(str).str.lower().str.contains(q, na=False)
    )
    return df[mask].head(100).copy()


# =========================================================
# 主畫面
# =========================================================
def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    inject_pro_theme()
    _init_state()

    watchlist = st.session_state[_k("watchlist")]
    master_df = st.session_state[_k("master_df")]
    _repair_selected_group()
    selected_group = _safe_str(st.session_state.get(_k("selected_group"), ""))

    render_pro_hero(
        title="自選股中心｜股神版",
        subtitle="群組管理、單筆新增、批次貼上、快速搜尋、直接輸入股票名稱新增，且每次新增刪除都自動記錄。",
    )

    overview_df = _build_overview_df(watchlist)
    group_summary_df = _build_group_summary_df(watchlist)

    total_groups = len(watchlist)
    total_stocks = len(overview_df)
    listed_count = int((overview_df["市場別"] == "上市").sum()) if not overview_df.empty else 0
    otc_count = int((overview_df["市場別"] == "上櫃").sum()) if not overview_df.empty else 0

    render_pro_kpi_row(
        [
            {"label": "群組數", "value": total_groups, "delta": "自選股群組", "delta_class": "pro-kpi-delta-flat"},
            {"label": "股票總數", "value": total_stocks, "delta": "自選股總計", "delta_class": "pro-kpi-delta-flat"},
            {"label": "上市檔數", "value": listed_count, "delta": "市場別統計", "delta_class": "pro-kpi-delta-flat"},
            {"label": "上櫃檔數", "value": otc_count, "delta": "市場別統計", "delta_class": "pro-kpi-delta-flat"},
        ]
    )

    status_msg = _safe_str(st.session_state.get(_k("status_msg"), ""))
    status_type = _safe_str(st.session_state.get(_k("status_type"), "info"))
    if status_msg:
        if status_type == "success":
            st.success(status_msg)
        elif status_type == "warning":
            st.warning(status_msg)
        elif status_type == "error":
            st.error(status_msg)
        else:
            st.info(status_msg)

    render_pro_section("群組管理")

    c1, c2, c3 = st.columns([3, 2, 2])

    with c1:
        group_options = list(watchlist.keys()) if watchlist else []
        st.selectbox("目前群組", options=group_options if group_options else [""], key=_k("selected_group"))

    with c2:
        st.text_input("新增群組名稱", key=_k("new_group_name"), placeholder="例如：AI / 半導體 / 高股息")

    with c3:
        if st.button("新增群組", use_container_width=True, type="primary"):
            new_group = _safe_str(st.session_state.get(_k("new_group_name"), ""))
            if not new_group:
                _set_status("請輸入群組名稱。", "warning")
            elif new_group in watchlist:
                _set_status(f"群組已存在：{new_group}", "warning")
            else:
                _ensure_group(new_group)
                _persist_watchlist(f"已新增群組：{new_group}")
                st.session_state[_k("new_group_name")] = ""
                st.rerun()

    d1, d2 = st.columns([2, 2])
    with d1:
        if st.button("刪除目前群組", use_container_width=True):
            if not selected_group:
                _set_status("目前沒有可刪除的群組。", "warning")
            else:
                ok, msg = _delete_group(selected_group)
                if ok:
                    _persist_watchlist(msg)
                else:
                    _set_status(msg, "warning")
                st.rerun()

    with d2:
        if st.button("手動儲存自選股", use_container_width=True):
            _persist_watchlist("已手動儲存 watchlist.json")
            st.rerun()

    render_pro_section("單筆新增股票")

    a1, a2, a3, a4 = st.columns([2, 2, 2, 2])

    with a1:
        add_group = st.selectbox("加入到群組", options=list(watchlist.keys()) if watchlist else [""], key=_k("add_group_select"))

    with a2:
        st.text_input("股票代碼（可直接打名稱）", key=_k("add_code"), placeholder="例如：2330 或 台積電")

    with a3:
        st.text_input("股票名稱（也可直接打名稱新增）", key=_k("add_name"), placeholder="例如：台積電")

    with a4:
        st.selectbox("市場別", ["上市", "上櫃"], key=_k("add_market"))

    b1, b2 = st.columns([2, 2])
    with b1:
        if st.button("新增這檔股票", use_container_width=True, type="primary"):
            ok, msg = _add_stock(
                add_group,
                st.session_state.get(_k("add_code"), ""),
                st.session_state.get(_k("add_name"), ""),
                st.session_state.get(_k("add_market"), "上市"),
            )

            if ok:
                _persist_watchlist(msg)
                st.session_state[_k("add_code_next")] = ""
                st.session_state[_k("add_name_next")] = ""
                st.session_state[_k("add_market_next")] = "上市"
            else:
                _set_status(msg, "warning")

            st.rerun()

    with b2:
        if st.button("自動帶入資料", use_container_width=True):
            raw_code = _safe_str(st.session_state.get(_k("add_code"), ""))
            raw_name = _safe_str(st.session_state.get(_k("add_name"), ""))

            keyword = raw_code or raw_name
            if not keyword:
                _set_status("請先輸入股票代碼或股票名稱。", "warning")
            else:
                code, name, market = _find_stock_by_code_or_name(keyword)
                if not code:
                    _set_status("找不到對應股票，請確認名稱或代碼。", "warning")
                else:
                    st.session_state[_k("add_code_next")] = code
                    st.session_state[_k("add_name_next")] = name
                    st.session_state[_k("add_market_next")] = market
                    _set_status(f"已帶入：{code} {name} / {market}", "success")
                st.rerun()

    render_pro_section("批次新增")

    st.text_area(
        "每行一筆：股票代碼,股票名稱,市場別。股票名稱 / 市場別可省略。",
        key=_k("bulk_text"),
        height=160,
        placeholder="2330,台積電,上市\n2454,聯發科\n3017",
    )

    e1, e2 = st.columns([2, 2])
    with e1:
        bulk_group = st.selectbox("批次加入到群組", options=list(watchlist.keys()) if watchlist else [""], key=_k("bulk_group_select"))
    with e2:
        if st.button("批次加入", use_container_width=True, type="primary"):
            ok_count, messages = _apply_bulk_add(bulk_group, st.session_state.get(_k("bulk_text"), ""))
            if ok_count > 0:
                _persist_watchlist(f"批次加入完成：成功 {ok_count} 筆")
                st.session_state[_k("bulk_text_next")] = ""
            else:
                _set_status("批次加入失敗，請確認格式或避免重複股票。", "warning")
            st.rerun()

    left, right = st.columns([1, 1])

    with left:
        render_pro_section("群組總覽")
        if group_summary_df.empty:
            st.info("目前沒有任何群組。")
        else:
            st.dataframe(group_summary_df, use_container_width=True, hide_index=True)

        render_pro_info_card(
            "管理提醒",
            [
                ("自動記錄", "新增 / 刪除 / 批次加入 / 群組異動後會立即寫回 watchlist.json 與 data/watchlist.json。", ""),
                ("直接新增", "股票代碼欄可直接輸入 2330 或 台積電。", ""),
                ("建議作法", "先建立群組，再用單筆或批次方式加入股票。", ""),
                ("批次格式", "每行：代碼,名稱,市場別；名稱與市場別可省略。", ""),
            ],
        )

    with right:
        render_pro_section("股票資料庫搜尋")
        st.text_input("搜尋股票代碼 / 名稱 / 市場別", key=_k("search_text"), placeholder="例如：2330 / 台積電 / 上櫃")
        search_df = _filter_master_df(master_df, st.session_state.get(_k("search_text"), ""))
        if search_df.empty:
            st.info("查無符合資料。")
        else:
            display_df = search_df.rename(columns={"code": "股票代號", "name": "股票名稱", "market": "市場別"})
            display_df["股票"] = display_df["股票代號"].astype(str) + " " + display_df["股票名稱"].astype(str)
            st.dataframe(display_df[["股票代號", "股票名稱", "市場別", "股票"]], use_container_width=True, hide_index=True)

    render_pro_section("目前群組明細")

    current_items = watchlist.get(selected_group, []) if selected_group else []
    current_df = pd.DataFrame(
        [
            {
                "群組": selected_group,
                "股票代號": _normalize_code(x.get("code")),
                "股票名稱": _safe_str(x.get("name")),
                "市場別": _safe_str(x.get("market")) or "上市",
                "股票": f"{_normalize_code(x.get('code'))} {_safe_str(x.get('name'))}",
            }
            for x in current_items
        ]
    )

    if current_df.empty:
        st.info("目前群組沒有股票。")
    else:
        st.dataframe(current_df, use_container_width=True, hide_index=True)

        remove_code = st.selectbox(
            "刪除目前群組中的股票",
            options=current_df["股票代號"].astype(str).tolist(),
            format_func=lambda code: current_df[current_df["股票代號"].astype(str) == str(code)]["股票"].iloc[0],
            key=_k("remove_code_select"),
        )

        if st.button("刪除這檔股票", use_container_width=True):
            ok, msg = _delete_stock(selected_group, remove_code)
            if ok:
                _persist_watchlist(msg)
            else:
                _set_status(msg, "warning")
            st.rerun()

    render_pro_section("全部自選股總覽")
    if overview_df.empty:
        st.info("目前沒有任何自選股。")
    else:
        st.dataframe(overview_df, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
