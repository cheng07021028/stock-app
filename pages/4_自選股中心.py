from __future__ import annotations

from datetime import datetime
from typing import Any
import copy
import hashlib
import json
import base64
import os
import tempfile

import pandas as pd
import requests
import streamlit as st

from utils import (
    get_normalized_watchlist,
    inject_pro_theme,
    render_pro_hero,
    render_pro_info_card,
    render_pro_kpi_row,
    render_pro_section,
)

from stock_master_service import load_stock_master, search_stock_master

PAGE_TITLE = "自選股中心｜升級完整版"
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


def _local_watchlist_path() -> str:
    """自選股本機 JSON 固定儲存位置。

    GitHub 只是雲端備份 / 部署回寫，本機 watchlist.json 才是 Streamlit
    各頁面同步讀取的最低保證來源。若 GitHub token 未設定或 API 失敗，仍必須
    成功寫入本機，避免新增自選股後換頁消失。
    """
    try:
        cfg_path = _safe_str(st.secrets.get("WATCHLIST_LOCAL_PATH", ""))
    except Exception:
        cfg_path = ""
    return cfg_path or "watchlist.json"


def _safe_write_json_local(path: str, payload: Any) -> tuple[bool, str]:
    """安全寫入 JSON：先寫暫存檔，再原子替換，降低寫壞 watchlist.json 的風險。"""
    try:
        folder = os.path.dirname(path)
        if folder:
            os.makedirs(folder, exist_ok=True)

        target_dir = folder or "."
        fd, tmp_path = tempfile.mkstemp(prefix="watchlist_", suffix=".tmp", dir=target_dir)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, path)
        return True, f"已寫入本機 JSON：{path}"
    except Exception as e:
        return False, f"本機 JSON 寫入失敗：{e}"

def _safe_read_json_local(path: str, default: Any = None) -> tuple[Any, str]:
    """安全讀取 JSON；讀不到時不讓頁面中斷。"""
    if default is None:
        default = {}
    try:
        if not os.path.exists(path):
            return default, f"檔案不存在：{path}"
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f), f"已讀取：{path}"
    except Exception as e:
        return default, f"讀取失敗：{e}"


def _verify_local_watchlist(path: str, payload: dict[str, list[dict[str, str]]]) -> tuple[bool, str]:
    """寫入後重新讀取驗證，避免畫面顯示已儲存但實際 JSON 沒更新。"""
    disk_payload, msg = _safe_read_json_local(path, {})
    disk_norm = _normalize_watchlist_payload(disk_payload if isinstance(disk_payload, dict) else {})
    disk_hash = _payload_hash(disk_norm)
    target_hash = _payload_hash(_normalize_watchlist_payload(payload))
    if disk_hash and disk_hash == target_hash:
        return True, f"本機 JSON 回讀驗證成功｜{disk_hash[:8]}"
    return False, f"本機 JSON 回讀驗證失敗｜目標 {target_hash[:8]} / 實際 {disk_hash[:8]}｜{msg}"


def _write_watchlist_bridge_files(payload: dict[str, list[dict[str, str]]]) -> tuple[bool, str]:
    """寫入跨頁橋接檔，讓 7/8/10/首頁不需要等快取自然過期。"""
    bridge_payload = {
        "updated_at": _now_text(),
        "version": int(st.session_state.get(_k("version"), 0) or 0),
        "payload_hash": _payload_hash(payload),
        "groups": list(payload.keys()),
        "group_count": len(payload),
        "stock_count": sum(len(v) for v in payload.values()),
        "watchlist": payload,
    }
    ok1, msg1 = _safe_write_json_local("watchlist_runtime_snapshot.json", bridge_payload)
    ok2, msg2 = _safe_write_json_local("watchlist_normalized.json", payload)
    return bool(ok1 and ok2), f"{msg1}｜{msg2}"


def _sync_runtime_watchlist_state(payload: dict[str, list[dict[str, str]]], version: int, saved_at: str, local_msg: str, github_msg: str, verify_msg: str):
    """統一同步所有跨頁 session_state key，避免換頁或重新整理後又回舊資料。"""
    payload = _normalize_watchlist_payload(payload)
    payload_hash = _payload_hash(payload)

    st.session_state[_k("watchlist")] = copy.deepcopy(payload)
    st.session_state[_k("version")] = version
    st.session_state[_k("last_saved_at")] = saved_at
    st.session_state[_k("payload_hash")] = payload_hash
    st.session_state[_k("last_local_msg")] = local_msg
    st.session_state[_k("last_github_msg")] = github_msg
    st.session_state[_k("last_verify_msg")] = verify_msg

    # 給其他頁面使用的全域 key
    st.session_state["watchlist_data"] = copy.deepcopy(payload)
    st.session_state["watchlist_version"] = version
    st.session_state["watchlist_last_saved_at"] = saved_at
    st.session_state["watchlist_last_saved_hash"] = payload_hash
    st.session_state["watchlist_last_verify_msg"] = verify_msg
    st.session_state["watchlist_force_reload_token"] = f"{version}_{payload_hash[:8]}"


def _clear_watchlist_cache_safely():
    """清除自選股快取，讓其他頁面下一次讀取立即吃到最新 watchlist.json。"""
    try:
        if hasattr(get_normalized_watchlist, "clear"):
            get_normalized_watchlist.clear()
    except Exception:
        pass


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

def _godpick_records_github_config() -> dict[str, str]:
    return {
        "token": _safe_str(st.secrets.get("GITHUB_TOKEN", "")),
        "owner": _safe_str(st.secrets.get("GITHUB_REPO_OWNER", "cheng07021028")),
        "repo": _safe_str(st.secrets.get("GITHUB_REPO_NAME", "stock-app")),
        "branch": _safe_str(st.secrets.get("GITHUB_REPO_BRANCH", "main")) or "main",
        "path": _safe_str(st.secrets.get("GODPICK_RECORDS_GITHUB_PATH", "godpick_records.json")) or "godpick_records.json",
    }


@st.cache_data(ttl=900, show_spinner=False)
def _load_godpick_records_df() -> pd.DataFrame:
    cfg = _godpick_records_github_config()
    token = cfg["token"]
    cols = [
        "record_id", "股票代號", "股票名稱", "市場別", "類別", "推薦模式", "推薦等級", "推薦總分",
        "買點分級", "型態名稱", "爆發等級", "推薦日期", "推薦時間", "更新時間", "目前狀態",
        "是否已實際買進", "推薦標籤"
    ]
    if not token:
        return pd.DataFrame(columns=cols)
    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            params={"ref": cfg["branch"]},
            timeout=20,
        )
        if resp.status_code != 200:
            return pd.DataFrame(columns=cols)
        content = resp.json().get("content", "")
        if not content:
            return pd.DataFrame(columns=cols)
        payload = json.loads(base64.b64decode(content).decode("utf-8"))
        df = pd.DataFrame(payload if isinstance(payload, list) else [])
        for c in cols:
            if c not in df.columns:
                df[c] = None
        df["股票代號"] = df["股票代號"].map(_normalize_code)
        df["推薦總分"] = pd.to_numeric(df["推薦總分"], errors="coerce")
        df["推薦日期"] = df["推薦日期"].fillna("").astype(str)
        df["推薦時間"] = df["推薦時間"].fillna("").astype(str)
        df["更新時間"] = df["更新時間"].fillna("").astype(str)
        return df[cols].copy()
    except Exception:
        return pd.DataFrame(columns=cols)


def _build_latest_rec_map(rec_df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    if rec_df is None or rec_df.empty:
        return {}
    work = rec_df.copy()
    work["_sort_dt"] = pd.to_datetime(
        work["推薦日期"].fillna("").astype(str) + " " + work["推薦時間"].fillna("").astype(str),
        errors="coerce"
    )
    work["_upd_dt"] = pd.to_datetime(work["更新時間"], errors="coerce")
    work = work.sort_values(["股票代號", "_sort_dt", "_upd_dt"], ascending=[True, False, False], na_position="last")
    work = work.drop_duplicates(subset=["股票代號"], keep="first")
    out = {}
    for _, r in work.iterrows():
        code = _normalize_code(r.get("股票代號"))
        if code:
            out[code] = {
                "最近推薦模式": _safe_str(r.get("推薦模式")),
                "最近推薦總分": r.get("推薦總分"),
                "買點分級": _safe_str(r.get("買點分級")),
                "型態名稱": _safe_str(r.get("型態名稱")),
                "爆發等級": _safe_str(r.get("爆發等級")),
                "最近推薦日期": _safe_str(r.get("推薦日期")),
                "最近推薦時間": _safe_str(r.get("推薦時間")),
                "最近推薦狀態": _safe_str(r.get("目前狀態")),
                "是否已寫入推薦紀錄": "是",
            }
    return out


def _enrich_watchlist_rows(rows_df: pd.DataFrame, rec_map: dict[str, dict[str, Any]]) -> pd.DataFrame:
    if rows_df is None or rows_df.empty:
        return rows_df
    x = rows_df.copy()
    x["股票代號"] = x["股票代號"].map(_normalize_code)
    x["是否曾被股神推薦"] = x["股票代號"].map(lambda c: "是" if c in rec_map else "否")
    x["最近推薦總分"] = x["股票代號"].map(lambda c: rec_map.get(c, {}).get("最近推薦總分"))
    x["買點分級"] = x["股票代號"].map(lambda c: rec_map.get(c, {}).get("買點分級", ""))
    x["最近推薦模式"] = x["股票代號"].map(lambda c: rec_map.get(c, {}).get("最近推薦模式", ""))
    x["型態名稱"] = x["股票代號"].map(lambda c: rec_map.get(c, {}).get("型態名稱", ""))
    x["爆發等級"] = x["股票代號"].map(lambda c: rec_map.get(c, {}).get("爆發等級", ""))
    x["最近推薦時間"] = x["股票代號"].map(
        lambda c: ((rec_map.get(c, {}).get("最近推薦日期", "") + " " + rec_map.get(c, {}).get("最近推薦時間", "")).strip())
    )
    x["是否已寫入推薦紀錄"] = x["股票代號"].map(lambda c: rec_map.get(c, {}).get("是否已寫入推薦紀錄", "否"))
    return x


def _send_group_to_other_pages(group_name: str, codes: list[str]):
    st.session_state["godpick_last_watch_group"] = _safe_str(group_name)
    st.session_state["godpick_last_watch_codes"] = [str(_normalize_code(x)) for x in codes if _normalize_code(x)]
    st.session_state["godpick_last_watch_sent_at"] = _now_text()
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
    """強制儲存自選股。

    v3 修正重點：
    1. 先寫本機 watchlist.json，確保換頁 / 重新整理 / 其他模組一定讀得到。
    2. GitHub API 只當遠端同步；未設定 token 或 API 失敗時，不視為整體失敗。
    3. 寫入後同步 session_state 並清除 get_normalized_watchlist cache。
    """
    payload = _normalize_watchlist_payload(data)

    local_path = _local_watchlist_path()
    local_ok, local_msg = _safe_write_json_local(local_path, payload)

    verify_ok, verify_msg = _verify_local_watchlist(local_path, payload) if local_ok else (False, "本機未寫入，略過回讀驗證")
    bridge_ok, bridge_msg = _write_watchlist_bridge_files(payload) if local_ok and verify_ok else (False, "本機驗證未通過，略過橋接檔")

    _clear_watchlist_cache_safely()

    github_ok, github_msg = _push_watchlist_to_github(payload)

    version = int(st.session_state.get(_k("version"), 0)) + 1
    saved_at = _now_text()
    _sync_runtime_watchlist_state(payload, version, saved_at, local_msg, github_msg, verify_msg)
    st.session_state[_k("last_bridge_msg")] = bridge_msg

    if local_ok and verify_ok and bridge_ok and github_ok:
        _set_status(f"{local_msg}｜{verify_msg}｜{bridge_msg}｜{github_msg}｜版本 v{version}｜{saved_at}", "success")
    elif local_ok and verify_ok and bridge_ok and not github_ok:
        _set_status(f"本機已完成：{local_msg}｜{verify_msg}｜{bridge_msg}｜GitHub 未同步：{github_msg}｜版本 v{version}｜{saved_at}", "warning")
    elif local_ok and verify_ok and not bridge_ok:
        _set_status(f"本機已完成但橋接檔未完成：{local_msg}｜{verify_msg}｜{bridge_msg}｜版本 v{version}", "warning")
    else:
        _set_status(f"本機儲存異常：{local_msg}｜{verify_msg}｜GitHub：{github_msg}", "error")

    return bool(local_ok and verify_ok)

def _persist_watchlist(success_msg: str) -> bool:
    ok = _force_write_watchlist_github(st.session_state[_k("watchlist")])
    if ok:
        base_msg = _safe_str(st.session_state.get(_k("last_github_msg"), ""))
        _set_status(f"{success_msg}｜{base_msg}", "success")
    return ok



@st.cache_data(ttl=1800, show_spinner=False)
def _load_stock_master() -> pd.DataFrame:
    """
    自選股中心統一改由 stock_master_service.py 讀取股票主檔，
    避免與股神推薦 / 股票主檔更新頁的資料來源不一致。
    """
    try:
        return load_stock_master()
    except Exception:
        return pd.DataFrame(columns=["code", "name", "market", "category", "official_industry", "theme_category"])





def _sync_watchlist_from_shared_or_source(force_reload: bool = False) -> tuple[bool, str]:
    """同步自選股中心資料。

    問題來源：
    - 7_股神推薦 / 8_推薦紀錄 / 儀表板可能會更新 st.session_state["watchlist_data"] 或 watchlist.json。
    - 但本頁原本只看 watch_watchlist，導致「全部自選股總覽」不會更新。

    修正策略：
    1. force_reload=True：清除 get_normalized_watchlist cache，重新讀 watchlist.json。
    2. force_reload=False：若全域 watchlist_data 比本頁資料不同，立即同步。
    """
    current = st.session_state.get(_k("watchlist"), {})
    current_hash = _payload_hash(_normalize_watchlist_payload(current)) if isinstance(current, dict) else ""

    if force_reload:
        try:
            if hasattr(get_normalized_watchlist, "clear"):
                get_normalized_watchlist.clear()
        except Exception:
            pass

        fresh = _load_watchlist_data()
        fresh_hash = _payload_hash(_normalize_watchlist_payload(fresh))

        st.session_state[_k("watchlist")] = copy.deepcopy(fresh)
        st.session_state[_k("payload_hash")] = fresh_hash
        st.session_state[_k("version")] = int(st.session_state.get(_k("version"), 0) or 0) + 1
        st.session_state[_k("last_saved_at")] = _now_text()

        st.session_state["watchlist_data"] = copy.deepcopy(fresh)
        st.session_state["watchlist_version"] = st.session_state[_k("version")]
        st.session_state["watchlist_last_saved_at"] = st.session_state[_k("last_saved_at")]
        st.session_state["watchlist_last_saved_hash"] = fresh_hash

        _repair_selected_group()
        return True, f"已強制重新讀取 watchlist.json｜版本 v{st.session_state[_k('version')]}"

    shared = st.session_state.get("watchlist_data")
    if isinstance(shared, dict) and shared:
        shared_norm = _normalize_watchlist_payload(shared)
        shared_hash = _payload_hash(shared_norm)

        if shared_hash and shared_hash != current_hash:
            st.session_state[_k("watchlist")] = copy.deepcopy(shared_norm)
            st.session_state[_k("payload_hash")] = shared_hash
            st.session_state[_k("version")] = int(st.session_state.get("watchlist_version", st.session_state.get(_k("version"), 0)) or 0)
            st.session_state[_k("last_saved_at")] = _safe_str(
                st.session_state.get("watchlist_last_saved_at")
                or st.session_state.get(_k("last_saved_at"))
                or _now_text()
            )

            st.session_state["watchlist_data"] = copy.deepcopy(shared_norm)
            st.session_state["watchlist_last_saved_hash"] = shared_hash
            _repair_selected_group()
            return True, "已同步其他頁面的最新自選股資料"

    return False, "目前自選股資料已是最新"



# =========================================================
# 重新帶入 / 加速
# =========================================================
def _clear_data_caches():
    """清除本頁與共用資料快取；不改變功能，只讓使用者需要時手動重新抓資料。"""
    for func in [_load_godpick_records_df, _load_stock_master]:
        try:
            func.clear()
        except Exception:
            pass

    try:
        if hasattr(get_normalized_watchlist, "clear"):
            get_normalized_watchlist.clear()
    except Exception:
        pass


def _reload_watchlist_master_records():
    """
    重新帶入最新 watchlist / 股票主檔 / 股神推薦紀錄。
    用按鈕觸發，避免每次頁面 rerun 都重抓造成卡頓。
    """
    _clear_data_caches()

    # 強制重新讀取 watchlist.json，避免「全部自選股總覽」停在舊 session_state。
    try:
        if hasattr(get_normalized_watchlist, "clear"):
            get_normalized_watchlist.clear()
    except Exception:
        pass

    fresh_watchlist = _load_watchlist_data()
    fresh_master = _load_stock_master()
    fresh_rec_df = _load_godpick_records_df()

    st.session_state[_k("watchlist")] = copy.deepcopy(fresh_watchlist)
    st.session_state[_k("master_df")] = fresh_master.copy() if isinstance(fresh_master, pd.DataFrame) else pd.DataFrame()

    groups = list(fresh_watchlist.keys())
    current = _safe_str(st.session_state.get(_k("selected_group"), ""))
    if current not in groups:
        st.session_state[_k("selected_group")] = groups[0] if groups else ""
        st.session_state[_k("rename_group_name")] = groups[0] if groups else ""

    version = int(st.session_state.get(_k("version"), 0) or 0) + 1
    payload_hash = _payload_hash(_normalize_watchlist_payload(fresh_watchlist))

    st.session_state[_k("version")] = version
    st.session_state[_k("last_saved_at")] = _now_text()
    st.session_state[_k("payload_hash")] = payload_hash

    st.session_state["watchlist_data"] = copy.deepcopy(fresh_watchlist)
    st.session_state["watchlist_version"] = version
    st.session_state["watchlist_last_saved_at"] = st.session_state[_k("last_saved_at")]
    st.session_state["watchlist_last_saved_hash"] = payload_hash

    rec_count = len(fresh_rec_df) if isinstance(fresh_rec_df, pd.DataFrame) else 0
    stock_count = sum(len(v) for v in fresh_watchlist.values()) if isinstance(fresh_watchlist, dict) else 0
    master_count = len(fresh_master) if isinstance(fresh_master, pd.DataFrame) else 0

    _set_status(
        f"已重新帶入資料｜自選股 {stock_count} 檔｜股票主檔 {master_count} 筆｜股神推薦紀錄 {rec_count} 筆｜版本 v{version}",
        "success",
    )


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
        "last_local_msg": "",
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

    # 自動同步其他頁面更新過的全域 watchlist_data。
    # 例如：7_股神推薦匯入自選股後，回到本頁不需手動清 cache 才會看到。
    try:
        _sync_watchlist_from_shared_or_source(force_reload=False)
    except Exception:
        pass

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
def _build_overview_df(watchlist: dict[str, list[dict[str, str]]], rec_map: dict[str, dict[str, Any]] | None = None) -> pd.DataFrame:
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
    out = pd.DataFrame(rows).sort_values(["群組", "股票代號"]).reset_index(drop=True)
    return _enrich_watchlist_rows(out, rec_map or {})


def _build_group_summary_df(watchlist: dict[str, list[dict[str, str]]], rec_map: dict[str, dict[str, Any]] | None = None) -> pd.DataFrame:
    rec_map = rec_map or {}
    rows = []
    for group_name, items in watchlist.items():
        codes = [_normalize_code(x.get("code")) for x in items if _normalize_code(x.get("code"))]
        rec_codes = [c for c in codes if c in rec_map]
        rows.append(
            {
                "群組": group_name,
                "股票數": len(items),
                "已被推薦數": len(rec_codes),
                "推薦覆蓋率%": round((len(rec_codes) / len(codes) * 100), 2) if codes else 0.0,
                "平均最近推薦分數": round(pd.to_numeric(pd.Series([rec_map.get(c, {}).get("最近推薦總分") for c in rec_codes]), errors="coerce").dropna().mean(), 2) if rec_codes else None,
                "市場別組成": " / ".join(sorted(set([_safe_str(x.get("market")) or "上市" for x in items]))) if items else "—",
            }
        )
    if not rows:
        return pd.DataFrame(columns=["群組", "股票數", "已被推薦數", "推薦覆蓋率%", "平均最近推薦分數", "市場別組成"])
    return pd.DataFrame(rows).sort_values(["股票數", "群組"], ascending=[False, True]).reset_index(drop=True)



def _filter_master_df(df: pd.DataFrame, keyword: str) -> pd.DataFrame:
    q = _safe_str(keyword)
    if df is None or df.empty:
        return pd.DataFrame(columns=["code", "name", "market", "category", "official_industry", "theme_category"])

    try:
        if not q:
            work = df.copy()
            show_cols = [c for c in ["code", "name", "market", "category", "official_industry", "theme_category"] if c in work.columns]
            return work[show_cols].head(100).copy()

        work = search_stock_master(
            df,
            keyword=q,
            market_filter="全部",
            category_filter="全部",
        )
        show_cols = [c for c in ["code", "name", "market", "category", "official_industry", "theme_category"] if c in work.columns]
        return work[show_cols].head(100).copy()
    except Exception:
        work = df.copy()
        if not q:
            return work.head(100).copy()
        for col in ["code", "name", "market"]:
            if col not in work.columns:
                work[col] = ""
        ql = q.lower()
        work = work[
            work["code"].astype(str).str.lower().str.contains(ql, na=False)
            | work["name"].astype(str).str.lower().str.contains(ql, na=False)
            | work["market"].astype(str).str.lower().str.contains(ql, na=False)
        ].copy()
        return work.head(100).copy()


# =========================================================
# 主畫面
# =========================================================
def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    inject_pro_theme()
    _init_state()

    # 進入頁面時先同步一次 shared watchlist_data，避免總覽顯示舊資料。
    try:
        _sync_watchlist_from_shared_or_source(force_reload=False)
    except Exception:
        pass

    watchlist = st.session_state[_k("watchlist")]
    master_df = st.session_state[_k("master_df")]
    rec_df = _load_godpick_records_df()
    rec_map = _build_latest_rec_map(rec_df)
    _repair_selected_group()

    current_group = _safe_str(st.session_state.get(_k("selected_group"), ""))
    if not _safe_str(st.session_state.get(_k("rename_group_name"), "")) and current_group:
        st.session_state[_k("rename_group_name")] = current_group

    render_pro_hero(
        title="自選股中心｜升級完整版",
        subtitle="保留 GitHub 強制回寫，並串接股神推薦紀錄，顯示最近推薦分數 / 買點分級 / 推薦模式 / 推薦時間。",
    )
    st.caption("股票主檔來源已統一改由 stock_master_service.py 提供；自選股變更會先寫入本機 watchlist.json，再嘗試同步 GitHub。")

    overview_df = _build_overview_df(watchlist, rec_map)
    group_summary_df = _build_group_summary_df(watchlist, rec_map)
    github_cfg = _github_config()

    render_pro_kpi_row(
        [
            {"label": "群組數", "value": len(watchlist), "delta": "自選股群組", "delta_class": "pro-kpi-delta-flat"},
            {"label": "股票總數", "value": len(overview_df), "delta": "自選股總計", "delta_class": "pro-kpi-delta-flat"},
            {"label": "已被股神推薦", "value": int((overview_df["是否曾被股神推薦"] == "是").sum()) if not overview_df.empty else 0, "delta": "推薦覆蓋", "delta_class": "pro-kpi-delta-flat"},
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

    render_pro_section("資料重新帶入 / 加速")
    reload_cols = st.columns([1.2, 1.2, 2.6])
    with reload_cols[0]:
        if st.button("🔄 重新同步自選股 / 推薦紀錄", use_container_width=True, type="primary"):
            _reload_watchlist_master_records()
            st.rerun()
    with reload_cols[1]:
        if st.button("🧹 清除快取", use_container_width=True):
            _clear_data_caches()
            _set_status("快取已清除；可按『重新帶入資料』抓最新資料。", "success")
            st.rerun()
    with reload_cols[2]:
        st.caption("重新同步會強制清除 watchlist 快取，重新讀取 watchlist.json、股票主檔與股神推薦紀錄，確保『全部自選股總覽』更新。")

    render_pro_section("自選股儲存 / GitHub 回寫設定")
    render_pro_info_card(
        "目前目標",
        [
            ("Owner", github_cfg["owner"], ""),
            ("Repo", github_cfg["repo"], ""),
            ("Branch", github_cfg["branch"], ""),
            ("GitHub Path", github_cfg["path"], ""),
            ("本機 JSON", _local_watchlist_path(), ""),
            ("Token 狀態", "已設定" if github_cfg["token"] else "未設定（仍會寫入本機 JSON）", ""),
        ],
    )

    st.markdown("#### v49 watchlist 寫回驗證")
    v49_cols = st.columns([1.2, 1.2, 1.6])
    with v49_cols[0]:
        if st.button("🧪 驗證本機 watchlist", use_container_width=True):
            payload = _normalize_watchlist_payload(st.session_state.get(_k("watchlist"), {}))
            ok, msg = _verify_local_watchlist(_local_watchlist_path(), payload)
            _set_status(msg, "success" if ok else "error")
            st.rerun()
    with v49_cols[1]:
        if st.button("🛠 重寫並驗證 watchlist", use_container_width=True, type="primary"):
            payload = _normalize_watchlist_payload(st.session_state.get(_k("watchlist"), {}))
            if _force_write_watchlist_github(payload):
                _set_status("v49 已重寫 watchlist.json、橋接檔並完成回讀驗證。", "success")
            st.rerun()
    with v49_cols[2]:
        st.caption(
            f"回讀驗證：{st.session_state.get(_k('last_verify_msg'), '尚未驗證')}｜"
            f"橋接檔：{st.session_state.get(_k('last_bridge_msg'), '尚未寫入')}"
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
        st.caption("已串接 7_股神推薦 / 8_股神推薦紀錄，顯示每個群組被推薦覆蓋率與平均最近推薦分數。")
        if group_summary_df.empty:
            st.info("目前沒有任何群組。")
        else:
            st.dataframe(group_summary_df, use_container_width=True, hide_index=True)

        render_pro_info_card(
            "回寫狀態",
            [
                ("模式", "本機 JSON 必寫 + GitHub 選配同步", ""),
                ("版本", f"v{st.session_state.get(_k('version'), 0)}", ""),
                ("最後儲存", st.session_state.get(_k("last_saved_at"), "—") or "—", ""),
                ("本機訊息", st.session_state.get(_k("last_local_msg"), "—") or "—", ""),
                ("GitHub訊息", st.session_state.get(_k("last_github_msg"), "—") or "—", ""),
            ],
        )

    with right:
        render_pro_section("股票資料庫搜尋")
        st.text_input("搜尋股票代碼 / 名稱 / 市場別", key=_k("search_text"))
        search_df = _filter_master_df(master_df, st.session_state.get(_k("search_text"), ""))
        if search_df.empty:
            st.info("查無符合資料。")
        else:
            display_df = search_df.rename(columns={
                "code": "股票代號",
                "name": "股票名稱",
                "market": "市場別",
                "category": "類別",
                "official_industry": "正式產業別",
                "theme_category": "主題類別",
            })
            display_df["股票"] = display_df["股票代號"].astype(str) + " " + display_df["股票名稱"].astype(str)
            show_cols = [c for c in ["股票代號", "股票名稱", "市場別", "正式產業別", "主題類別", "類別", "股票"] if c in display_df.columns]
            st.dataframe(display_df[show_cols], use_container_width=True, hide_index=True)


    with st.expander("股神推薦串接說明", expanded=False):
        st.write("1. 本頁會讀取 godpick_records.json，顯示每檔自選股最近一次股神推薦分數。")
        st.write("2. 可看見買點分級、最近推薦模式、型態名稱、爆發等級、最近推薦時間。")
        st.write("3. 按下『將目前群組送到7_股神推薦』後，會把群組代號記到 session_state，方便其他頁承接。")

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
        ],
        columns=["群組", "股票代號", "股票名稱", "市場別", "股票"],
    )
    current_df = _enrich_watchlist_rows(current_df, rec_map)

    if current_df.empty:
        st.info("目前群組沒有股票。")
    else:
        st.dataframe(current_df, use_container_width=True, hide_index=True)

        action_top = st.columns(3)
        with action_top[0]:
            if st.button("🚀 將目前群組送到7_股神推薦", use_container_width=True):
                _send_group_to_other_pages(current_group_name, current_df["股票代號"].astype(str).tolist())
                _set_status(f"已記錄群組 {current_group_name}，可到 7_股神推薦.py 接續使用。", "success")
                st.rerun()
        with action_top[1]:
            if st.button("📘 將第一檔送到3_歷史K線分析", use_container_width=True):
                if not current_df.empty:
                    st.session_state["kline_focus_stock_code"] = _safe_str(current_df.iloc[0]["股票代號"])
                    st.session_state["kline_focus_stock_name"] = _safe_str(current_df.iloc[0]["股票名稱"])
                    _set_status(f"已記錄 {_safe_str(current_df.iloc[0]['股票代號'])}，可到 3_歷史K線分析.py 接續查看。", "success")
                    st.rerun()
        with action_top[2]:
            st.caption(f"最近送出：{_safe_str(st.session_state.get('godpick_last_watch_sent_at', '未送出'))}")

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
    st.caption(
        f"自選股同步版本：v{st.session_state.get(_k('version'), 0)}｜"
        f"最後同步 / 儲存：{st.session_state.get(_k('last_saved_at'), '—') or '—'}｜"
        f"資料Hash：{st.session_state.get(_k('payload_hash'), '')[:8]}"
    )
    final_overview_df = _build_overview_df(st.session_state[_k("watchlist")], rec_map)
    if final_overview_df.empty:
        st.info("目前沒有任何自選股。")
    else:
        show_cols = [c for c in ["群組", "股票代號", "股票名稱", "市場別", "是否曾被股神推薦", "最近推薦總分", "買點分級", "最近推薦模式", "型態名稱", "爆發等級", "最近推薦時間", "是否已寫入推薦紀錄"] if c in final_overview_df.columns]
        st.dataframe(final_overview_df[show_cols], use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
