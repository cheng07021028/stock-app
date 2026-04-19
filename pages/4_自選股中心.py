from __future__ import annotations

from datetime import datetime
from typing import Any
import copy
import hashlib
import json
import base64

import pandas as pd
import requests
import streamlit as st

from utils import (
    get_all_code_name_map,
    get_normalized_watchlist,
    inject_pro_theme,
    render_pro_hero,
    render_pro_info_card,
    render_pro_kpi_row,
    render_pro_section,
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


def _payload_hash(payload: dict[str, list[dict[str, str]]]) -> str:
    try:
        text = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        return hashlib.md5(text.encode("utf-8")).hexdigest()
    except Exception:
        return ""


def _set_status(msg: str, level: str = "info"):
    st.session_state[_k("status_msg")] = msg
    st.session_state[_k("status_type")] = level


# =========================================================
# GitHub API 設定
# =========================================================
def _github_config() -> dict[str, str]:
    return {
        "token": _safe_str(st.secrets.get("GITHUB_TOKEN", "")),
        "owner": _safe_str(st.secrets.get("GITHUB_REPO_OWNER", "cheng07021028")),
        "repo": _safe_str(st.secrets.get("GITHUB_REPO_NAME", "stock-app")),
        "branch": _safe_str(st.secrets.get("GITHUB_REPO_BRANCH", "main")) or "main",
        "path": _safe_str(st.secrets.get("WATCHLIST_GITHUB_PATH", "watchlist.json")) or "watchlist.json",
    }


def _github_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _github_contents_url(owner: str, repo: str, path: str) -> str:
    return f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"


def _get_repo_watchlist_sha(cfg: dict[str, str]) -> tuple[str, str]:
    token = cfg["token"]
    owner = cfg["owner"]
    repo = cfg["repo"]
    branch = cfg["branch"]
    path = cfg["path"]

    if not token:
        return "", "缺少 GITHUB_TOKEN"

    url = _github_contents_url(owner, repo, path)
    try:
        resp = requests.get(
            url,
            headers=_github_headers(token),
            params={"ref": branch},
            timeout=20,
        )
        if resp.status_code == 200:
            data = resp.json()
            return _safe_str(data.get("sha")), ""
        if resp.status_code == 404:
            return "", ""
        return "", f"讀取 GitHub 檔案失敗：{resp.status_code} / {resp.text[:300]}"
    except Exception as e:
        return "", f"讀取 GitHub 檔案例外：{e}"


def _push_watchlist_to_github(payload: dict[str, list[dict[str, str]]]) -> tuple[bool, str]:
    cfg = _github_config()
    token = cfg["token"]
    owner = cfg["owner"]
    repo = cfg["repo"]
    branch = cfg["branch"]
    path = cfg["path"]

    if not token:
        return False, "未設定 GITHUB_TOKEN，無法回寫 GitHub。"

    sha, sha_err = _get_repo_watchlist_sha(cfg)
    if sha_err:
        return False, sha_err

    content_text = json.dumps(payload, ensure_ascii=False, indent=2)
    encoded_content = base64.b64encode(content_text.encode("utf-8")).decode("utf-8")

    body: dict[str, Any] = {
        "message": f"Update {path} from Streamlit @ {_now_text()}",
        "content": encoded_content,
        "branch": branch,
    }
    if sha:
        body["sha"] = sha

    url = _github_contents_url(owner, repo, path)

    try:
        resp = requests.put(
            url,
            headers=_github_headers(token),
            json=body,
            timeout=30,
        )
        if resp.status_code in (200, 201):
            return True, f"已回寫 GitHub：{owner}/{repo}@{branch}:{path}"
        return False, f"GitHub API 寫入失敗：{resp.status_code} / {resp.text[:500]}"
    except Exception as e:
        return False, f"GitHub API 寫入例外：{e}"


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

        seen = set()
        normalized_items = []

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

            normalized_items.append(
                {
                    "code": code,
                    "name": name,
                    "market": market,
                }
            )

        payload[g] = sorted(
            normalized_items,
            key=lambda x: (_normalize_code(x.get("code")), _safe_str(x.get("name"))),
        )

    return payload


def _force_write_watchlist_github(data: dict[str, list[dict[str, str]]]) -> bool:
    payload = _normalize_watchlist_payload(data)
    ok, msg = _push_watchlist_to_github(payload)

    version = int(st.session_state.get(_k("version"), 0)) + 1
    saved_at = _now_text()
    payload_md5 = _payload_hash(payload)

    st.session_state[_k("watchlist")] = copy.deepcopy(payload)
    st.session_state[_k("version")] = version
    st.session_state[_k("last_saved_at")] = saved_at
    st.session_state[_k("payload_hash")] = payload_md5
    st.session_state[_k("last_github_msg")] = msg

    st.session_state["watchlist_data"] = copy.deepcopy(payload)
    st.session_state["watchlist_version"] = version
    st.session_state["watchlist_last_saved_at"] = saved_at
    st.session_state["watchlist_last_saved_hash"] = payload_md5

    if ok:
        _set_status(f"{msg}｜版本 v{version}｜{saved_at}", "success")
    else:
        _set_status(msg, "error")

    return ok


def _persist_watchlist(success_msg: str) -> bool:
    ok = _force_write_watchlist_github(st.session_state[_k("watchlist")])
    if ok:
        base_msg = _safe_str(st.session_state.get(_k("last_github_msg"), ""))
        _set_status(f"{success_msg}｜{base_msg}", "success")
    return ok


@st.cache_data(ttl=1800, show_spinner=False)
def _load_stock_master() -> pd.DataFrame:
    dfs = []

    for market_arg in ["", "上市", "上櫃", "興櫃"]:
        try:
            df = get_all_code_name_map(market_arg)
            if isinstance(df, pd.DataFrame) and not df.empty:
                temp = df.copy()

                for col in ["code", "name", "market"]:
                    if col not in temp.columns:
                        temp[col] = ""

                temp["code"] = temp["code"].map(_normalize_code)
                temp["name"] = temp["name"].map(_safe_str)
                temp["market"] = temp["market"].map(_safe_str)

                if market_arg in ["上市", "上櫃", "興櫃"]:
                    temp["market"] = temp["market"].replace("", market_arg)

                dfs.append(temp[["code", "name", "market"]])
        except Exception:
            pass

    if not dfs:
        return pd.DataFrame(columns=["code", "name", "market"])

    out = pd.concat(dfs, ignore_index=True)
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

    defaults = {
        "new_group_name": "",
        "rename_group_name": "",
        "add_code": "",
        "add_name": "",
        "add_market": "上市",
        "bulk_text": "",
        "search_text": "",
        "status_msg": "",
        "status_type": "info",
        "last_saved_at": "",
        "last_github_msg": "",
        "version": int(st.session_state.get("watchlist_version", 0) or 0),
        "payload_hash": "",
        "batch_delete_codes": [],
        "clear_group_confirm": False,
    }
    for k, v in defaults.items():
        if _k(k) not in st.session_state:
            st.session_state[_k(k)] = v

    for name in [
        "batch_delete_codes",
        "clear_group_confirm",
        "add_code",
        "add_name",
        "add_market",
        "bulk_text",
        "selected_group",
        "new_group_name",
        "rename_group_name",
    ]:
        next_key = _k(f"{name}_next")
        real_key = _k(name)
        if next_key in st.session_state:
            st.session_state[real_key] = st.session_state.pop(next_key)

    _repair_selected_group()

    st.session_state["watchlist_data"] = copy.deepcopy(st.session_state[_k("watchlist")])
    st.session_state["watchlist_version"] = st.session_state.get(_k("version"), 0)
    st.session_state["watchlist_last_saved_at"] = st.session_state.get(_k("last_saved_at"), "")
    st.session_state["watchlist_last_saved_hash"] = st.session_state.get(_k("payload_hash"), "")


def _repair_selected_group():
    watchlist = st.session_state[_k("watchlist")]
    groups = list(watchlist.keys())
    current = _safe_str(st.session_state.get(_k("selected_group"), ""))

    if groups:
        if current not in groups:
            st.session_state[_k("selected_group")] = groups[0]
    else:
        st.session_state[_k("selected_group")] = ""


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


def _create_group(group_name: str) -> tuple[bool, str]:
    g = _safe_str(group_name)
    if not g:
        return False, "請輸入群組名稱。"

    watchlist = copy.deepcopy(st.session_state[_k("watchlist")])
    if g in watchlist:
        return False, f"群組已存在：{g}"

    watchlist[g] = []
    st.session_state[_k("watchlist")] = watchlist
    st.session_state[_k("selected_group_next")] = g
    st.session_state[_k("rename_group_name_next")] = g
    return True, f"已新增群組：{g}"


def _rename_group(old_name: str, new_name: str) -> tuple[bool, str]:
    old_g = _safe_str(old_name)
    new_g = _safe_str(new_name)

    if not old_g:
        return False, "請先選擇群組。"
    if not new_g:
        return False, "請輸入新的群組名稱。"
    if old_g == new_g:
        return False, "新舊群組名稱相同。"

    watchlist = copy.deepcopy(st.session_state[_k("watchlist")])

    if old_g not in watchlist:
        return False, "原群組不存在。"
    if new_g in watchlist:
        return False, f"新群組名稱已存在：{new_g}"

    ordered = {}
    for g, items in watchlist.items():
        if g == old_g:
            ordered[new_g] = items
        else:
            ordered[g] = items

    st.session_state[_k("watchlist")] = ordered
    st.session_state[_k("selected_group_next")] = new_g
    st.session_state[_k("rename_group_name_next")] = new_g
    return True, f"已將群組 {old_g} 更名為 {new_g}"


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

    watchlist = copy.deepcopy(st.session_state[_k("watchlist")])
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
    watchlist = copy.deepcopy(st.session_state[_k("watchlist")])

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
    watchlist = copy.deepcopy(st.session_state[_k("watchlist")])

    if not g:
        return False, "請先選擇群組。"
    if g not in watchlist:
        return False, "群組不存在。"

    del watchlist[g]
    st.session_state[_k("watchlist")] = watchlist

    groups = list(watchlist.keys())
    st.session_state[_k("selected_group_next")] = groups[0] if groups else ""
    st.session_state[_k("rename_group_name_next")] = groups[0] if groups else ""
    st.session_state[_k("batch_delete_codes_next")] = []
    st.session_state[_k("clear_group_confirm_next")] = False

    return True, f"已刪除群組：{g}"


def _delete_multiple_stocks(group_name: str, codes: list[str]) -> tuple[int, str]:
    g = _safe_str(group_name)
    clean_codes = [_normalize_code(x) for x in codes if _normalize_code(x)]

    if not g:
        return 0, "請先選擇群組。"
    if not clean_codes:
        return 0, "請先勾選要刪除的股票。"

    watchlist = copy.deepcopy(st.session_state[_k("watchlist")])
    if g not in watchlist:
        return 0, "群組不存在。"

    code_set = set(clean_codes)
    before = len(watchlist[g])
    watchlist[g] = [x for x in watchlist[g] if _normalize_code(x.get("code")) not in code_set]
    removed = before - len(watchlist[g])

    st.session_state[_k("watchlist")] = watchlist
    st.session_state[_k("batch_delete_codes_next")] = []

    if removed <= 0:
        return 0, "沒有可刪除的股票。"
    return removed, f"已批次刪除 {g}：{removed} 檔"


def _clear_group(group_name: str) -> tuple[int, str]:
    g = _safe_str(group_name)
    watchlist = copy.deepcopy(st.session_state[_k("watchlist")])

    if not g:
        return 0, "請先選擇群組。"
    if g not in watchlist:
        return 0, "群組不存在。"

    removed = len(watchlist[g])
    watchlist[g] = []
    st.session_state[_k("watchlist")] = watchlist
    st.session_state[_k("batch_delete_codes_next")] = []
    st.session_state[_k("clear_group_confirm_next")] = False

    return removed, f"已清空群組：{g}（{removed} 檔）"


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

    work = df.copy()
    work["code"] = work["code"].astype(str)
    work["name"] = work["name"].astype(str)
    work["market"] = work["market"].astype(str)

    if not q:
        return work.head(100).copy()

    exact = work[(work["code"].str.lower() == q) | (work["name"].str.lower() == q)].copy()
    if not exact.empty:
        return exact.head(100).copy()

    prefix = work[
        work["code"].str.lower().str.startswith(q, na=False)
        | work["name"].str.lower().str.startswith(q, na=False)
    ].copy()
    if not prefix.empty:
        return prefix.head(100).copy()

    contain = work[
        work["code"].str.lower().str.contains(q, na=False)
        | work["name"].str.lower().str.contains(q, na=False)
        | work["market"].str.lower().str.contains(q, na=False)
    ].copy()

    return contain.head(100).copy()


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

    current_group = _safe_str(st.session_state.get(_k("selected_group"), ""))
    if not _safe_str(st.session_state.get(_k("rename_group_name"), "")) and current_group:
        st.session_state[_k("rename_group_name")] = current_group

    render_pro_hero(
        title="自選股中心｜GitHub API 強制回寫版",
        subtitle="只要新增、刪除、改名、批次操作，都直接 commit 回 GitHub repo 的 watchlist.json。",
    )

    overview_df = _build_overview_df(watchlist)
    group_summary_df = _build_group_summary_df(watchlist)
    github_cfg = _github_config()

    render_pro_kpi_row(
        [
            {"label": "群組數", "value": len(watchlist), "delta": "自選股群組", "delta_class": "pro-kpi-delta-flat"},
            {"label": "股票總數", "value": len(overview_df), "delta": "自選股總計", "delta_class": "pro-kpi-delta-flat"},
            {"label": "同步版本", "value": st.session_state.get(_k("version"), 0), "delta": "GitHub commit", "delta_class": "pro-kpi-delta-flat"},
            {"label": "最後儲存", "value": st.session_state.get(_k("last_saved_at"), "—") or "—", "delta": github_cfg["path"], "delta_class": "pro-kpi-delta-flat"},
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

    render_pro_section("GitHub 回寫設定")
    render_pro_info_card(
        "目前目標",
        [
            ("Owner", github_cfg["owner"], ""),
            ("Repo", github_cfg["repo"], ""),
            ("Branch", github_cfg["branch"], ""),
            ("Path", github_cfg["path"], ""),
            ("Token 狀態", "已設定" if github_cfg["token"] else "未設定", ""),
        ],
    )

    render_pro_section("群組管理")

    c1, c2, c3 = st.columns([3, 2, 2])

    with c1:
        group_options = list(watchlist.keys()) if watchlist else [""]
        st.selectbox("目前群組", options=group_options, key=_k("selected_group"))

    with c2:
        st.text_input("新增群組名稱", key=_k("new_group_name"))

    with c3:
        if st.button("新增群組", use_container_width=True, type="primary"):
            ok, msg = _create_group(_safe_str(st.session_state.get(_k("new_group_name"), "")))
            if ok:
                _persist_watchlist(msg)
                st.session_state[_k("new_group_name_next")] = ""
            else:
                _set_status(msg, "warning")
            st.rerun()

    r1, r2, r3 = st.columns([3, 2, 2])

    with r1:
        st.text_input("目前群組改名為", key=_k("rename_group_name"))

    with r2:
        if st.button("套用群組改名", use_container_width=True):
            ok, msg = _rename_group(
                _safe_str(st.session_state.get(_k("selected_group"), "")),
                _safe_str(st.session_state.get(_k("rename_group_name"), "")),
            )
            if ok:
                _persist_watchlist(msg)
            else:
                _set_status(msg, "warning")
            st.rerun()

    with r3:
        if st.button("刪除目前群組", use_container_width=True):
            ok, msg = _delete_group(_safe_str(st.session_state.get(_k("selected_group"), "")))
            if ok:
                _persist_watchlist(msg)
            else:
                _set_status(msg, "warning")
            st.rerun()

    render_pro_section("單筆新增股票")

    add_group_options = list(st.session_state[_k("watchlist")].keys()) if st.session_state[_k("watchlist")] else [""]
    if _safe_str(st.session_state.get(_k("add_group_select"), "")) not in add_group_options:
        st.session_state[_k("add_group_select")] = add_group_options[0] if add_group_options else ""

    a1, a2, a3, a4 = st.columns(4)
    with a1:
        add_group = st.selectbox("加入到群組", options=add_group_options, key=_k("add_group_select"))
    with a2:
        st.text_input("股票代碼（可直接打名稱）", key=_k("add_code"))
    with a3:
        st.text_input("股票名稱", key=_k("add_name"))
    with a4:
        st.selectbox("市場別", ["上市", "上櫃"], key=_k("add_market"))

    b1, b2 = st.columns(2)
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
            keyword = _safe_str(st.session_state.get(_k("add_code"), "")) or _safe_str(st.session_state.get(_k("add_name"), ""))
            if not keyword:
                _set_status("請先輸入股票代碼或股票名稱。", "warning")
            else:
                code, name, market = _find_stock_by_code_or_name(keyword)
                if not code:
                    _set_status("找不到對應股票。", "warning")
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
        height=150,
        placeholder="2330,台積電,上市\n2454,聯發科\n3548,兆利,上櫃",
    )

    bulk_group_options = list(st.session_state[_k("watchlist")].keys()) if st.session_state[_k("watchlist")] else [""]
    if _safe_str(st.session_state.get(_k("bulk_group_select"), "")) not in bulk_group_options:
        st.session_state[_k("bulk_group_select")] = bulk_group_options[0] if bulk_group_options else ""

    e1, e2 = st.columns(2)
    with e1:
        bulk_group = st.selectbox("批次加入到群組", options=bulk_group_options, key=_k("bulk_group_select"))
    with e2:
        if st.button("批次加入", use_container_width=True, type="primary"):
            ok_count, _ = _apply_bulk_add(bulk_group, st.session_state.get(_k("bulk_text"), ""))
            if ok_count > 0:
                _persist_watchlist(f"批次加入完成：成功 {ok_count} 筆")
                st.session_state[_k("bulk_text_next")] = ""
            else:
                _set_status("批次加入失敗，請確認格式或避免重複。", "warning")
            st.rerun()

    left, right = st.columns(2)

    with left:
        render_pro_section("群組總覽")
        if group_summary_df.empty:
            st.info("目前沒有任何群組。")
        else:
            st.dataframe(group_summary_df, use_container_width=True, hide_index=True)

        render_pro_info_card(
            "回寫狀態",
            [
                ("模式", "GitHub API 強制回寫", ""),
                ("版本", f"v{st.session_state.get(_k('version'), 0)}", ""),
                ("最後儲存", st.session_state.get(_k("last_saved_at"), "—") or "—", ""),
                ("最後訊息", st.session_state.get(_k("last_github_msg"), "—") or "—", ""),
            ],
        )

    with right:
        render_pro_section("股票資料庫搜尋")
        st.text_input("搜尋股票代碼 / 名稱 / 市場別", key=_k("search_text"))
        search_df = _filter_master_df(master_df, st.session_state.get(_k("search_text"), ""))
        if search_df.empty:
            st.info("查無符合資料。")
        else:
            display_df = search_df.rename(columns={"code": "股票代號", "name": "股票名稱", "market": "市場別"})
            display_df["股票"] = display_df["股票代號"].astype(str) + " " + display_df["股票名稱"].astype(str)
            st.dataframe(display_df[["股票代號", "股票名稱", "市場別", "股票"]], use_container_width=True, hide_index=True)

    render_pro_section("目前群組明細")

    current_group_name = _safe_str(st.session_state.get(_k("selected_group"), ""))
    current_items = st.session_state[_k("watchlist")].get(current_group_name, [])

    current_df = pd.DataFrame(
        [
            {
                "群組": current_group_name,
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

        m1, m2 = st.columns(2)

        with m1:
            remove_code = st.selectbox(
                "單筆刪除目前群組中的股票",
                options=current_df["股票代號"].astype(str).tolist(),
                format_func=lambda code: current_df[current_df["股票代號"].astype(str) == str(code)]["股票"].iloc[0],
                key=_k("remove_code_select"),
            )

            if st.button("刪除這檔股票", use_container_width=True):
                ok, msg = _delete_stock(current_group_name, remove_code)
                if ok:
                    _persist_watchlist(msg)
                    st.session_state[_k("batch_delete_codes_next")] = []
                else:
                    _set_status(msg, "warning")
                st.rerun()

        with m2:
            code_to_label = {str(r["股票代號"]): str(r["股票"]) for _, r in current_df.iterrows()}
            all_codes = current_df["股票代號"].astype(str).tolist()

            st.multiselect(
                "批次刪除勾選股票",
                options=all_codes,
                default=st.session_state.get(_k("batch_delete_codes"), []),
                format_func=lambda x: code_to_label.get(str(x), str(x)),
                key=_k("batch_delete_codes"),
            )

            x1, x2 = st.columns(2)
            with x1:
                if st.button("批次刪除勾選", use_container_width=True):
                    removed, msg = _delete_multiple_stocks(
                        current_group_name,
                        st.session_state.get(_k("batch_delete_codes"), []),
                    )
                    if removed > 0:
                        _persist_watchlist(msg)
                    else:
                        _set_status(msg, "warning")
                    st.rerun()

            with x2:
                if st.button("全選目前群組", use_container_width=True):
                    st.session_state[_k("batch_delete_codes_next")] = all_codes
                    _set_status(f"已全選 {len(all_codes)} 檔，可直接批次刪除。", "info")
                    st.rerun()

        st.checkbox("確認清空目前群組全部股票", key=_k("clear_group_confirm"))

        if st.button("清空目前群組", use_container_width=True):
            if not st.session_state.get(_k("clear_group_confirm"), False):
                _set_status("請先勾選確認清空。", "warning")
            else:
                removed, msg = _clear_group(current_group_name)
                if removed >= 0:
                    _persist_watchlist(msg)
            st.rerun()

    render_pro_section("全部自選股總覽")
    final_overview_df = _build_overview_df(st.session_state[_k("watchlist")])
    if final_overview_df.empty:
        st.info("目前沒有任何自選股。")
    else:
        st.dataframe(final_overview_df, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
