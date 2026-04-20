from __future__ import annotations

from datetime import date, timedelta, datetime
from typing import Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import copy
import json
import base64

import pandas as pd
import requests
import streamlit as st
import firebase_admin
from firebase_admin import credentials, firestore

from utils import (
    compute_radar_scores,
    compute_signal_snapshot,
    compute_support_resistance_snapshot,
    format_number,
    get_all_code_name_map,
    get_history_data,
    get_normalized_watchlist,
    inject_pro_theme,
    render_pro_hero,
    render_pro_info_card,
    render_pro_kpi_row,
    render_pro_section,
)

PAGE_TITLE = "股神推薦"
PFX = "godpick_"


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


def _safe_float(v: Any, default=None):
    try:
        if pd.isna(v):
            return default
    except Exception:
        pass
    try:
        return float(v)
    except Exception:
        return default


def _normalize_code(v: Any) -> str:
    text = _safe_str(v)
    if not text:
        return ""
    if text.isdigit():
        return text
    digits = "".join(ch for ch in text if ch.isdigit())
    if 4 <= len(digits) <= 6:
        return digits
    return text


def _normalize_category(v: Any) -> str:
    text = _safe_str(v)
    if not text:
        return ""
    return text.replace("　", " ").strip()


def _score_clip(v: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, v))


def _avg_safe(values: list[float | None], default: float = 0.0) -> float:
    clean = [float(x) for x in values if x is not None]
    if not clean:
        return default
    return sum(clean) / len(clean)


def _fmt_num(v: Any, d: int = 2) -> str:
    return format_number(v, d) if pd.notna(v) else ""


def _fmt_seconds(sec: float) -> str:
    try:
        sec = max(0, int(sec))
    except Exception:
        sec = 0

    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60

    if h > 0:
        return f"{h}小時 {m}分 {s}秒"
    if m > 0:
        return f"{m}分 {s}秒"
    return f"{s}秒"


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _set_status(msg: str, level: str = "info"):
    st.session_state[_k("status_msg")] = msg
    st.session_state[_k("status_type")] = level


# =========================================================
# 類型推論：更細分
# =========================================================
def _infer_category_from_name(name: str) -> str:
    n = _safe_str(name)
    if not n:
        return "其他"

    s = n.lower()

    category_rules = [
        ("晶圓代工", ["台積", "聯電", "力積電", "世界先進", "世界", "umc", "tsmc"]),
        ("IC設計", ["聯發科", "瑞昱", "聯詠", "群聯", "創意", "世芯", "智原", "敦泰", "原相", "晶心科", "矽力", "力旺"]),
        ("封測", ["日月光", "矽品", "京元電", "頎邦", "封測", "測試"]),
        ("記憶體", ["南亞科", "華邦電", "旺宏", "記憶體", "dram", "nand"]),
        ("矽晶圓", ["環球晶", "中美晶", "合晶", "嘉晶", "矽晶圓"]),
        ("半導體設備材料", ["帆宣", "漢唐", "家登", "辛耘", "中砂", "崇越", "設備", "材料"]),
        ("IP矽智財", ["力旺", "晶心科", "智原", "創意", "世芯", "ip", "矽智財"]),
        ("AI伺服器", ["伺服器", "server", "緯穎", "廣達", "英業達", "緯創", "鴻海", "技嘉"]),
        ("散熱", ["雙鴻", "奇鋐", "散熱", "風扇", "熱導管"]),
        ("機殼", ["勤誠", "晟銘電", "機殼"]),
        ("電源供應", ["台達電", "光寶科", "群電", "電源", "供應器"]),
        ("高速傳輸", ["高速", "傳輸", "祥碩", "譜瑞", "創惟", "usb4", "pcie"]),
        ("網通交換器", ["智邦", "明泰", "中磊", "網通", "交換器", "switch"]),
        ("光通訊", ["光通訊", "波若威", "華星光", "聯鈞", "上詮", "cpo"]),
        ("PCB載板", ["欣興", "南電", "景碩", "金像電", "載板", "pcb"]),
        ("EMS代工", ["鴻海", "和碩", "廣達", "仁寶", "英業達", "緯創", "組裝"]),
        ("消費電子", ["大立光", "玉晶光", "耳機", "鏡頭", "聲學", "消費電子"]),
        ("面板", ["友達", "群創", "彩晶", "面板"]),
        ("光學鏡頭", ["大立光", "玉晶光", "亞光", "鏡頭", "光學"]),
        ("被動元件", ["國巨", "華新科", "禾伸堂", "被動元件", "電容", "電阻"]),
        ("連接器", ["貿聯", "嘉澤", "連接器", "端子"]),
        ("電池材料", ["康普", "美琪瑪", "立凱", "長園科", "電池", "材料"]),
        ("金控", ["金控"]),
        ("銀行", ["銀行"]),
        ("保險", ["保險"]),
        ("證券", ["證券"]),
        ("航運", ["長榮", "陽明", "萬海", "航運", "海運", "貨櫃"]),
        ("航空觀光", ["華航", "長榮航", "航空", "觀光", "旅遊", "飯店"]),
        ("鋼鐵", ["中鋼", "大成鋼", "鋼", "鋼鐵"]),
        ("塑化", ["台塑", "南亞", "台化", "台塑化", "塑化", "化工"]),
        ("生技醫療", ["保瑞", "藥華藥", "美時", "生技", "醫療", "製藥", "藥"]),
        ("車用電子", ["和大", "貿聯", "車用", "車電", "汽車"]),
        ("綠能儲能", ["中興電", "華城", "儲能", "綠能", "太陽能", "風電"]),
        ("營建資產", ["營建", "建設", "資產"]),
        ("食品民生", ["統一", "食品", "餐飲"]),
        ("紡織製鞋", ["紡織", "成衣", "製鞋"]),
        ("電機機械", ["上銀", "亞德客", "機械", "工具機", "自動化"]),
        ("其他電子", ["電子", "電腦", "光電"]),
    ]

    for cat, keywords in category_rules:
        for kw in keywords:
            if kw.lower() in s:
                return cat

    return "其他"


# =========================================================
# GitHub API
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
# Firestore
# =========================================================
def _firebase_config() -> dict[str, str]:
    return {
        "project_id": _safe_str(st.secrets.get("FIREBASE_PROJECT_ID", "")),
        "client_email": _safe_str(st.secrets.get("FIREBASE_CLIENT_EMAIL", "")),
        "private_key": _safe_str(st.secrets.get("FIREBASE_PRIVATE_KEY", "")),
    }


def _init_firebase_app():
    try:
        return firebase_admin.get_app()
    except ValueError:
        pass

    cfg = _firebase_config()
    project_id = cfg["project_id"]
    client_email = cfg["client_email"]
    private_key = cfg["private_key"].replace("\\n", "\n")

    if not project_id or not client_email or not private_key:
        raise ValueError("Firebase secrets 未設定完整，請確認 FIREBASE_PROJECT_ID / FIREBASE_CLIENT_EMAIL / FIREBASE_PRIVATE_KEY")

    cred_dict = {
        "type": "service_account",
        "project_id": project_id,
        "private_key": private_key,
        "client_email": client_email,
        "token_uri": "https://oauth2.googleapis.com/token",
    }

    cred = credentials.Certificate(cred_dict)
    return firebase_admin.initialize_app(cred, {"projectId": project_id})


def _push_watchlist_to_firestore(payload: dict[str, list[dict[str, str]]]) -> tuple[bool, str]:
    """
    Firestore 結構：
    - system/watchlist_summary
    - watchlists/{group_name}
    - watchlists/{group_name}/stocks/{code}
    """
    try:
        _init_firebase_app()
        db = firestore.client()
        batch = db.batch()
        now = firestore.SERVER_TIMESTAMP

        summary_ref = db.collection("system").document("watchlist_summary")
        batch.set(
            summary_ref,
            {
                "group_count": len(payload),
                "updated_at": now,
                "source": "streamlit_dual_write",
            },
            merge=True,
        )

        for group_name, items in payload.items():
            group_name = _safe_str(group_name)
            if not group_name:
                continue

            group_ref = db.collection("watchlists").document(group_name)
            batch.set(
                group_ref,
                {
                    "group_name": group_name,
                    "count": len(items),
                    "items": items,
                    "updated_at": now,
                    "source": "streamlit_dual_write",
                },
                merge=True,
            )

            for item in items:
                code = _normalize_code(item.get("code"))
                if not code:
                    continue

                stock_ref = group_ref.collection("stocks").document(code)
                batch.set(
                    stock_ref,
                    {
                        "code": code,
                        "name": _safe_str(item.get("name")) or code,
                        "market": _safe_str(item.get("market")) or "上市",
                        "category": _normalize_category(item.get("category")),
                        "group_name": group_name,
                        "updated_at": now,
                    },
                    merge=True,
                )

        batch.commit()
        return True, "已同步寫入 Firestore"
    except Exception as e:
        return False, f"Firestore 寫入失敗：{e}"


# =========================================================
# watchlist payload / 雙寫
# =========================================================
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
            category = _normalize_category(item.get("category"))

            if not code:
                continue

            key = (g, code)
            if key in seen:
                continue
            seen.add(key)

            row = {
                "code": code,
                "name": name,
                "market": market,
            }
            if category:
                row["category"] = category

            normalized_items.append(row)

        payload[g] = sorted(
            normalized_items,
            key=lambda x: (_normalize_code(x.get("code")), _safe_str(x.get("name"))),
        )

    return payload


def _force_write_watchlist_dual(data: dict[str, list[dict[str, str]]]) -> bool:
    payload = _normalize_watchlist_payload(data)

    ok_github, msg_github = _push_watchlist_to_github(payload)
    ok_firestore, msg_firestore = _push_watchlist_to_firestore(payload)

    saved_at = _now_text()
    st.session_state["watchlist_data"] = copy.deepcopy(payload)
    st.session_state["watchlist_version"] = int(st.session_state.get("watchlist_version", 0)) + 1
    st.session_state["watchlist_last_saved_at"] = saved_at

    if ok_github and ok_firestore:
        _set_status(f"GitHub + Firestore 同步成功｜{saved_at}", "success")
        return True

    if ok_github and not ok_firestore:
        _set_status(f"GitHub 成功，但 Firestore 失敗｜{msg_firestore}", "warning")
        return True

    if (not ok_github) and ok_firestore:
        _set_status(f"Firestore 成功，但 GitHub 失敗｜{msg_github}", "warning")
        return True

    _set_status(f"GitHub / Firestore 都失敗｜{msg_github}｜{msg_firestore}", "error")
    return False


# =========================================================
# watchlist / 主檔
# =========================================================
def _load_watchlist_map() -> dict[str, list[dict[str, str]]]:
    raw = st.session_state.get("watchlist_data")
    if not isinstance(raw, dict) or not raw:
        raw = get_normalized_watchlist()

    result: dict[str, list[dict[str, str]]] = {}

    if isinstance(raw, dict):
        for group_name, items in raw.items():
            g = _safe_str(group_name)
            if not g:
                continue

            rows = []
            seen = set()

            if isinstance(items, list):
                for item in items:
                    if not isinstance(item, dict):
                        continue

                    code = _normalize_code(item.get("code"))
                    name = _safe_str(item.get("name")) or code
                    market = _safe_str(item.get("market")) or "上市"
                    category = _normalize_category(item.get("category")) or _infer_category_from_name(name)

                    if not code or code in seen:
                        continue
                    seen.add(code)

                    rows.append(
                        {
                            "code": code,
                            "name": name,
                            "market": market,
                            "category": category,
                            "label": f"{code} {name}",
                        }
                    )
            result[g] = rows

    return result


@st.cache_data(ttl=1800, show_spinner=False)
def _load_master_df() -> pd.DataFrame:
    dfs = []
    category_candidates = [
        "category", "industry", "sector", "theme",
        "類別", "產業別", "產業", "主題",
    ]

    for market_arg in ["", "上市", "上櫃", "興櫃"]:
        try:
            df = get_all_code_name_map(market_arg)
            if isinstance(df, pd.DataFrame) and not df.empty:
                temp = df.copy()

                mapping = {
                    "證券代號": "code",
                    "證券名稱": "name",
                    "市場別": "market",
                    "code": "code",
                    "name": "name",
                    "market": "market",
                }
                temp = temp.rename(columns=mapping)

                found_category_col = None
                for col in temp.columns:
                    if str(col).strip() in category_candidates:
                        found_category_col = col
                        break
                if found_category_col:
                    temp = temp.rename(columns={found_category_col: "category"})

                for col in ["code", "name", "market"]:
                    if col not in temp.columns:
                        temp[col] = ""
                if "category" not in temp.columns:
                    temp["category"] = ""

                temp["code"] = temp["code"].map(_normalize_code)
                temp["name"] = temp["name"].map(_safe_str)
                temp["market"] = temp["market"].map(_safe_str)
                temp["category"] = temp["category"].map(_normalize_category)

                if market_arg in ["上市", "上櫃", "興櫃"]:
                    temp["market"] = temp["market"].replace("", market_arg)

                temp["category"] = temp.apply(
                    lambda r: _normalize_category(r.get("category")) or _infer_category_from_name(r.get("name")),
                    axis=1,
                )

                dfs.append(temp[["code", "name", "market", "category"]])
        except Exception:
            pass

    if not dfs:
        return pd.DataFrame(columns=["code", "name", "market", "category"])

    out = pd.concat(dfs, ignore_index=True)
    out["code"] = out["code"].map(_normalize_code)
    out["name"] = out["name"].map(_safe_str)
    out["market"] = out["market"].map(_safe_str).replace("", "上市")
    out["category"] = out["category"].map(_normalize_category)
    out = out[out["code"] != ""].drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)
    return out


# =========================================================
# 股票主檔搜尋 / 更新中心
# =========================================================
def _search_master_df(
    master_df: pd.DataFrame,
    keyword: str,
    market_filter: str = "全部",
    category_filter: str = "全部",
    limit: int = 100,
) -> pd.DataFrame:
    if master_df is None or master_df.empty:
        return pd.DataFrame(columns=["code", "name", "market", "category"])

    kw = _safe_str(keyword)
    work = master_df.copy()

    for c in ["code", "name", "market", "category"]:
        if c not in work.columns:
            work[c] = ""
        work[c] = work[c].astype(str)

    if _safe_str(market_filter) not in ["", "全部"]:
        work = work[work["market"] == _safe_str(market_filter)].copy()

    if _safe_str(category_filter) not in ["", "全部"]:
        work = work[work["category"] == _safe_str(category_filter)].copy()

    if not kw:
        return work.head(limit).reset_index(drop=True)

    exact_code = work[work["code"] == kw].copy()
    exact_name = work[work["name"] == kw].copy()

    fuzzy = work[
        work["code"].str.contains(kw, case=False, na=False)
        | work["name"].str.contains(kw, case=False, na=False)
        | work["category"].str.contains(kw, case=False, na=False)
        | work["market"].str.contains(kw, case=False, na=False)
    ].copy()

    if not fuzzy.empty:
        fuzzy["sort_score"] = 0
        fuzzy.loc[fuzzy["code"] == kw, "sort_score"] += 100
        fuzzy.loc[fuzzy["name"] == kw, "sort_score"] += 90
        fuzzy.loc[fuzzy["code"].str.startswith(kw, na=False), "sort_score"] += 50
        fuzzy.loc[fuzzy["name"].str.startswith(kw, na=False), "sort_score"] += 40
        fuzzy.loc[fuzzy["category"].str.contains(kw, case=False, na=False), "sort_score"] += 15
        fuzzy = fuzzy.sort_values(["sort_score", "code"], ascending=[False, True])

    out = pd.concat([exact_code, exact_name, fuzzy], ignore_index=True)
    out = out.drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)
    return out[["code", "name", "market", "category"]].head(limit)


def _refresh_master_df_now() -> pd.DataFrame:
    try:
        _load_master_df.clear()
    except Exception:
        pass
    return _load_master_df()


def _update_master_category_in_memory(master_df: pd.DataFrame, code: str, new_category: str) -> pd.DataFrame:
    if master_df is None or master_df.empty:
        return master_df

    code = _normalize_code(code)
    new_category = _normalize_category(new_category)
    if not code or not new_category:
        return master_df

    temp = master_df.copy()
    mask = temp["code"].astype(str) == code
    if mask.any():
        temp.loc[mask, "category"] = new_category
    return temp


def _market_options_from_master(master_df: pd.DataFrame) -> list[str]:
    if master_df is None or master_df.empty:
        return ["全部", "上市", "上櫃", "興櫃"]
    vals = ["全部"] + sorted([x for x in master_df["market"].dropna().astype(str).unique().tolist() if x.strip()])
    return vals


def _category_options_from_master(master_df: pd.DataFrame) -> list[str]:
    if master_df is None or master_df.empty:
        return ["全部"]
    vals = ["全部"] + sorted([x for x in master_df["category"].dropna().astype(str).unique().tolist() if x.strip()])
    return vals


def _append_stock_to_watchlist(
    group_name: str,
    code: str,
    name: str,
    market: str,
    category: str,
):
    group_name = _safe_str(group_name)
    code = _normalize_code(code)
    name = _safe_str(name) or code
    market = _safe_str(market) or "上市"
    category = _normalize_category(category) or _infer_category_from_name(name)

    if not group_name:
        return False, "群組不可空白"
    if not code:
        return False, "股票代號不可空白"

    raw = st.session_state.get("watchlist_data")
    if not isinstance(raw, dict) or not raw:
        raw = get_normalized_watchlist()

    if group_name not in raw or not isinstance(raw[group_name], list):
        raw[group_name] = []

    for item in raw[group_name]:
        if _normalize_code(item.get("code")) == code:
            return False, f"{code} 已存在於 {group_name}"

    row = {
        "code": code,
        "name": name,
        "market": market,
    }
    if category:
        row["category"] = category

    raw[group_name].append(row)

    ok = _force_write_watchlist_dual(raw)
    if ok:
        return True, f"已加入 {group_name}：{code} {name}"
    return False, _safe_str(st.session_state.get(_k("status_msg"), "寫入失敗"))


def _append_multiple_stocks_to_watchlist(
    group_name: str,
    rows: list[dict[str, str]],
) -> tuple[int, list[str]]:
    group_name = _safe_str(group_name)
    if not group_name:
        return 0, ["請先選擇群組。"]

    raw = st.session_state.get("watchlist_data")
    if not isinstance(raw, dict) or not raw:
        raw = get_normalized_watchlist()

    if group_name not in raw or not isinstance(raw[group_name], list):
        raw[group_name] = []

    existing_codes = {_normalize_code(x.get("code")) for x in raw[group_name] if isinstance(x, dict)}
    added = 0
    messages = []

    for row in rows:
        code = _normalize_code(row.get("code"))
        name = _safe_str(row.get("name")) or code
        market = _safe_str(row.get("market")) or "上市"
        category = _normalize_category(row.get("category")) or _infer_category_from_name(name)

        if not code:
            continue

        if code in existing_codes:
            messages.append(f"{code} 已存在於 {group_name}")
            continue

        item = {
            "code": code,
            "name": name,
            "market": market,
        }
        if category:
            item["category"] = category

        raw[group_name].append(item)
        existing_codes.add(code)
        added += 1
        messages.append(f"已加入 {group_name}：{code} {name}")

    if added > 0:
        ok = _force_write_watchlist_dual(raw)
        if not ok:
            return 0, [_safe_str(st.session_state.get(_k("status_msg"), "GitHub / Firestore 寫入失敗"))]

    return added, messages


def _remove_stock_from_watchlist(group_name: str, code: str) -> tuple[bool, str]:
    group_name = _safe_str(group_name)
    code = _normalize_code(code)

    raw = st.session_state.get("watchlist_data")
    if not isinstance(raw, dict) or not raw:
        raw = get_normalized_watchlist()

    if group_name not in raw:
        return False, f"找不到群組：{group_name}"

    before_count = len(raw[group_name])
    raw[group_name] = [x for x in raw[group_name] if _normalize_code(x.get("code")) != code]
    after_count = len(raw[group_name])

    if before_count == after_count:
        return False, f"{code} 不在 {group_name}"

    ok = _force_write_watchlist_dual(raw)
    if ok:
        return True, f"已從 {group_name} 刪除 {code}"
    return False, _safe_str(st.session_state.get(_k("status_msg"), "刪除同步失敗"))


# =========================================================
# 股票資訊查找
# =========================================================
def _find_name_market_category(
    code: str,
    manual_name: str,
    manual_market: str,
    manual_category: str,
    master_df: pd.DataFrame,
) -> tuple[str, str, str]:
    code = _normalize_code(code)
    manual_name = _safe_str(manual_name)
    manual_market = _safe_str(manual_market)
    manual_category = _normalize_category(manual_category)

    if isinstance(master_df, pd.DataFrame) and not master_df.empty:
        matched = master_df[master_df["code"].astype(str) == code]
        if not matched.empty:
            row = matched.iloc[0]
            final_name = _safe_str(row.get("name")) or manual_name or code
            final_market = _safe_str(row.get("market")) or manual_market or "上市"
            final_category = _normalize_category(row.get("category")) or manual_category or _infer_category_from_name(final_name)
            return final_name, final_market, final_category

    final_name = manual_name or code
    final_market = manual_market or "上市"
    final_category = manual_category or _infer_category_from_name(final_name)
    return final_name, final_market, final_category


def _parse_manual_codes(text: str, master_df: pd.DataFrame) -> list[dict[str, str]]:
    rows = []
    seen = set()
    raw_lines = [x.strip() for x in _safe_str(text).replace("，", "\n").replace(",", "\n").splitlines() if x.strip()]

    for raw in raw_lines:
        txt = _safe_str(raw)
        code = _normalize_code(txt)
        name = ""
        market = "上市"
        category = ""

        if not code and isinstance(master_df, pd.DataFrame) and not master_df.empty:
            matched = master_df[master_df["name"].astype(str).str.contains(txt, case=False, na=False)]
            if not matched.empty:
                row = matched.iloc[0]
                code = _normalize_code(row.get("code"))
                name = _safe_str(row.get("name"))
                market = _safe_str(row.get("market")) or "上市"
                category = _normalize_category(row.get("category"))

        if code and not name:
            name, market, category = _find_name_market_category(code, "", market, category, master_df)

        if code and code not in seen:
            seen.add(code)
            rows.append(
                {
                    "code": code,
                    "name": name or code,
                    "market": market or "上市",
                    "category": category,
                    "label": f"{code} {name or code}",
                }
            )
    return rows


def _build_universe_from_market(master_df: pd.DataFrame, market_mode: str, limit_count: Any, selected_categories: list[str]) -> list[dict[str, str]]:
    if master_df is None or master_df.empty:
        return []

    work = master_df.copy()
    market_mode = _safe_str(market_mode)

    if market_mode == "上市":
        work = work[work["market"].astype(str) == "上市"].copy()
    elif market_mode == "上櫃":
        work = work[work["market"].astype(str) == "上櫃"].copy()
    elif market_mode == "興櫃":
        work = work[work["market"].astype(str) == "興櫃"].copy()

    clean_categories = [_normalize_category(x) for x in selected_categories if _normalize_category(x) and x != "全部"]
    if clean_categories:
        work = work[work["category"].astype(str).isin(clean_categories)].copy()

    if _safe_str(limit_count) != "全部":
        try:
            limit_n = int(limit_count)
            if limit_n > 0:
                work = work.head(limit_n).copy()
        except Exception:
            pass

    rows = []
    for _, row in work.iterrows():
        code = _normalize_code(row.get("code"))
        name = _safe_str(row.get("name")) or code
        market = _safe_str(row.get("market")) or "上市"
        category = _normalize_category(row.get("category")) or _infer_category_from_name(name)
        if code:
            rows.append(
                {
                    "code": code,
                    "name": name,
                    "market": market,
                    "category": category,
                    "label": f"{code} {name}",
                }
            )
    return rows


def _collect_all_categories(master_df: pd.DataFrame, watchlist_map: dict[str, list[dict[str, str]]]) -> list[str]:
    cats = set()

    if isinstance(master_df, pd.DataFrame) and not master_df.empty:
        for _, row in master_df.iterrows():
            name = _safe_str(row.get("name"))
            cat = _normalize_category(row.get("category")) or _infer_category_from_name(name)
            if cat:
                cats.add(cat)

    if isinstance(watchlist_map, dict):
        for _, items in watchlist_map.items():
            for item in items:
                name = _safe_str(item.get("name"))
                cat = _normalize_category(item.get("category")) or _infer_category_from_name(name)
                if cat:
                    cats.add(cat)

    return sorted(list(cats))


# =========================================================
# 歷史資料
# =========================================================
def _prepare_history_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    temp = df.copy()
    if "日期" not in temp.columns:
        return pd.DataFrame()

    temp["日期"] = pd.to_datetime(temp["日期"], errors="coerce")
    temp = temp.dropna(subset=["日期"]).sort_values("日期").reset_index(drop=True)

    for col in ["成交股數", "成交金額", "開盤價", "最高價", "最低價", "收盤價", "成交筆數"]:
        if col in temp.columns:
            temp[col] = pd.to_numeric(temp[col], errors="coerce")

    if "收盤價" not in temp.columns:
        return pd.DataFrame()

    temp = temp.dropna(subset=["收盤價"]).copy()
    if temp.empty:
        return pd.DataFrame()

    close = temp["收盤價"]
    high = temp["最高價"] if "最高價" in temp.columns else close
    low = temp["最低價"] if "最低價" in temp.columns else close
    vol = pd.to_numeric(temp["成交股數"], errors="coerce") if "成交股數" in temp.columns else pd.Series(index=temp.index, dtype=float)

    for n in [5, 10, 20, 60, 120, 240]:
        temp[f"MA{n}"] = close.rolling(n).mean()

    prev_close = close.shift(1)
    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    temp["ATR14"] = tr.rolling(14).mean()

    temp["VOL5"] = vol.rolling(5).mean()
    temp["VOL20"] = vol.rolling(20).mean()

    temp["RET5"] = close.pct_change(5) * 100
    temp["RET20"] = close.pct_change(20) * 100
    temp["RET60"] = close.pct_change(60) * 100

    temp["UP_DAY"] = (close > close.shift(1)).astype(float)

    return temp


@st.cache_data(ttl=3600, show_spinner=False)
def _get_history_smart(stock_no: str, stock_name: str, market_type: str, start_date: date, end_date: date) -> tuple[pd.DataFrame, str]:
    primary = _safe_str(market_type)
    tried = []

    if primary:
        tried.append(primary)

    fallback_map = {
        "上市": ["上櫃", "興櫃", ""],
        "上櫃": ["上市", "興櫃", ""],
        "興櫃": ["上市", "上櫃", ""],
        "": ["上市", "上櫃", "興櫃"],
    }

    for mk in fallback_map.get(primary, ["上市", "上櫃", "興櫃", ""]):
        if mk not in tried:
            tried.append(mk)

    for mk in tried:
        try:
            df = get_history_data(
                stock_no=stock_no,
                stock_name=stock_name,
                market_type=mk,
                start_date=start_date,
                end_date=end_date,
            )
        except TypeError:
            try:
                df = get_history_data(
                    stock_no=stock_no,
                    stock_name=stock_name,
                    market_type=mk,
                    start_dt=start_date,
                    end_dt=end_date,
                )
            except Exception:
                df = pd.DataFrame()
        except Exception:
            df = pd.DataFrame()

        df = _prepare_history_df(df)
        if not df.empty:
            return df, (mk or market_type or "未知")

    return pd.DataFrame(), (_safe_str(market_type) or "未知")


# =========================================================
# 單股分析
# =========================================================
def _build_auto_factor_scores(df: pd.DataFrame, signal_snapshot: dict, sr_snapshot: dict, radar: dict) -> dict[str, Any]:
    last = df.iloc[-1]

    close_now = _safe_float(last.get("收盤價"))
    ma20 = _safe_float(last.get("MA20"))
    ma60 = _safe_float(last.get("MA60"))
    ma120 = _safe_float(last.get("MA120"))
    atr14 = _safe_float(last.get("ATR14"))
    vol5 = _safe_float(last.get("VOL5"))
    vol20 = _safe_float(last.get("VOL20"))
    ret20 = _safe_float(last.get("RET20"))
    ret60 = _safe_float(last.get("RET60"))

    signal_score = _safe_float(signal_snapshot.get("score"), 0) or 0
    radar_trend = _safe_float(radar.get("trend"), 50) or 50
    radar_momentum = _safe_float(radar.get("momentum"), 50) or 50
    radar_volume = _safe_float(radar.get("volume"), 50) or 50
    radar_structure = _safe_float(radar.get("structure"), 50) or 50
    sup20 = _safe_float(sr_snapshot.get("sup_20"))

    eps_proxy = 50.0
    if close_now not in [None, 0]:
        trend_bonus = 0.0
        if ma120 is not None and close_now > ma120:
            trend_bonus += 18
        if ma60 is not None and close_now > ma60:
            trend_bonus += 12
        if ma20 is not None and close_now > ma20:
            trend_bonus += 8

        vol_penalty = 0.0
        if atr14 is not None:
            atr_pct = atr14 / close_now * 100
            if atr_pct <= 2.5:
                vol_penalty = 0
            elif atr_pct <= 5:
                vol_penalty = 6
            else:
                vol_penalty = 12

        eps_proxy = _score_clip(30 + trend_bonus + radar_structure * 0.25 + radar_trend * 0.20 - vol_penalty)

    revenue_proxy = _score_clip(
        25
        + (_safe_float(ret20, 0) or 0) * 0.9
        + (_safe_float(ret60, 0) or 0) * 0.35
        + radar_momentum * 0.30
        + radar_volume * 0.20
    )

    profit_proxy = _score_clip(
        30
        + signal_score * 6
        + radar_trend * 0.28
        + radar_structure * 0.22
        + (_safe_float(ret60, 0) or 0) * 0.35
    )

    lock_proxy = 45.0
    if close_now not in [None, 0]:
        vol_ratio = None
        if vol5 not in [None, 0] and vol20 not in [None, 0]:
            vol_ratio = vol5 / vol20

        atr_pct = None
        if atr14 is not None:
            atr_pct = atr14 / close_now * 100

        lock_bonus = 0.0
        if ma20 is not None and close_now >= ma20:
            lock_bonus += 12
        if sup20 is not None and close_now >= sup20:
            lock_bonus += 10
        if vol_ratio is not None:
            if 0.7 <= vol_ratio <= 1.15:
                lock_bonus += 12
            elif vol_ratio < 0.7:
                lock_bonus += 8
        if atr_pct is not None:
            if atr_pct <= 2.5:
                lock_bonus += 14
            elif atr_pct <= 4:
                lock_bonus += 8

        lock_proxy = _score_clip(20 + lock_bonus + radar_structure * 0.24)

    recent = df.tail(5).copy()
    up_days_5 = int(recent["UP_DAY"].sum()) if "UP_DAY" in recent.columns else 0
    inst_proxy = _score_clip(
        20
        + up_days_5 * 10
        + signal_score * 5
        + radar_momentum * 0.25
        + radar_volume * 0.20
    )

    factor_summary = (
        f"EPS代理 {format_number(eps_proxy,1)} / "
        f"營收動能代理 {format_number(revenue_proxy,1)} / "
        f"獲利代理 {format_number(profit_proxy,1)} / "
        f"大戶鎖碼代理 {format_number(lock_proxy,1)} / "
        f"法人連買代理 {format_number(inst_proxy,1)}"
    )

    return {
        "auto_factor_total": _avg_safe([eps_proxy, revenue_proxy, profit_proxy, lock_proxy, inst_proxy], 0),
        "eps_proxy": eps_proxy,
        "revenue_proxy": revenue_proxy,
        "profit_proxy": profit_proxy,
        "lock_proxy": lock_proxy,
        "inst_proxy": inst_proxy,
        "factor_summary": factor_summary,
    }


def _build_trade_plan(df: pd.DataFrame, sr_snapshot: dict, signal_snapshot: dict) -> dict[str, Any]:
    last = df.iloc[-1]
    close_now = _safe_float(last.get("收盤價"), 0) or 0
    atr14 = _safe_float(last.get("ATR14"), 0) or max(close_now * 0.03, 1.0)
    ma20 = _safe_float(last.get("MA20"))
    res20 = _safe_float(sr_snapshot.get("res_20"))
    sup20 = _safe_float(sr_snapshot.get("sup_20"))
    res60 = _safe_float(sr_snapshot.get("res_60"))
    score = _safe_float(signal_snapshot.get("score"), 0) or 0

    breakout_buy = res20 if res20 is not None else close_now
    pullback_buy = ma20 if ma20 is not None else (sup20 if sup20 is not None else close_now)
    stop_price = sup20 if sup20 is not None else max(close_now - atr14, 0)
    sell_target_1 = res20 if res20 is not None and res20 > close_now else close_now + atr14 * 1.5
    sell_target_2 = res60 if res60 is not None and res60 > sell_target_1 else sell_target_1 + atr14 * 1.2

    if score >= 4:
        launch_tag = "強勢起漲候選"
    elif score >= 2:
        launch_tag = "偏多轉強候選"
    elif score <= -2:
        launch_tag = "不建議追價"
    else:
        launch_tag = "等待表態"

    def _rr(entry: float, stop: float, target: float) -> str:
        risk = entry - stop
        reward = target - entry
        if risk <= 0:
            return "—"
        return f"1 : {reward / risk:.2f}"

    rr1 = _rr(pullback_buy, stop_price, sell_target_1) if pullback_buy and stop_price is not None and sell_target_1 else "—"
    rr2 = _rr(breakout_buy, stop_price, sell_target_2) if breakout_buy and stop_price is not None and sell_target_2 else "—"

    return {
        "launch_tag": launch_tag,
        "breakout_buy": breakout_buy,
        "pullback_buy": pullback_buy,
        "stop_price": stop_price,
        "sell_target_1": sell_target_1,
        "sell_target_2": sell_target_2,
        "rr1": rr1,
        "rr2": rr2,
    }


@st.cache_data(ttl=3600, show_spinner=False)
def _analyze_stock_bundle(stock_no: str, stock_name: str, market_type: str, start_dt: date, end_dt: date) -> dict[str, Any]:
    hist_df, used_market = _get_history_smart(
        stock_no=stock_no,
        stock_name=stock_name,
        market_type=market_type,
        start_date=start_dt,
        end_date=end_dt,
    )
    if hist_df.empty:
        return {}

    signal_snapshot = compute_signal_snapshot(hist_df)
    sr_snapshot = compute_support_resistance_snapshot(hist_df)
    radar = compute_radar_scores(hist_df)
    auto_factor = _build_auto_factor_scores(hist_df, signal_snapshot, sr_snapshot, radar)
    trade_plan = _build_trade_plan(hist_df, sr_snapshot, signal_snapshot)

    last = hist_df.iloc[-1]
    first = hist_df.iloc[0]

    close_now = _safe_float(last.get("收盤價"))
    close_first = _safe_float(first.get("收盤價"))
    period_pct = None
    if close_now is not None and close_first not in [None, 0]:
        period_pct = ((close_now / close_first) - 1) * 100

    res20 = _safe_float(sr_snapshot.get("res_20"))
    sup20 = _safe_float(sr_snapshot.get("sup_20"))
    pressure_dist = None
    support_dist = None
    if close_now is not None and res20 not in [None, 0]:
        pressure_dist = ((res20 - close_now) / res20) * 100
    if close_now is not None and sup20 not in [None, 0]:
        support_dist = ((close_now - sup20) / sup20) * 100

    radar_avg = _avg_safe(
        [
            _safe_float(radar.get("trend")),
            _safe_float(radar.get("momentum")),
            _safe_float(radar.get("volume")),
            _safe_float(radar.get("position")),
            _safe_float(radar.get("structure")),
        ],
        50.0,
    )

    return {
        "used_market": used_market,
        "signal_snapshot": signal_snapshot,
        "sr_snapshot": sr_snapshot,
        "radar": radar,
        "auto_factor": auto_factor,
        "trade_plan": trade_plan,
        "close_now": close_now,
        "period_pct": period_pct,
        "pressure_dist": pressure_dist,
        "support_dist": support_dist,
        "radar_avg": radar_avg,
    }


def _analyze_one_stock_for_recommend(
    item: dict[str, str],
    master_df: pd.DataFrame,
    start_dt: date,
    end_dt: date,
    min_signal_score: float,
    clean_categories: list[str],
):
    code = _normalize_code(item.get("code"))
    manual_name = _safe_str(item.get("name"))
    manual_market = _safe_str(item.get("market"))
    manual_category = _normalize_category(item.get("category"))

    if not code:
        return None

    stock_name, market_type, category = _find_name_market_category(
        code, manual_name, manual_market, manual_category, master_df
    )

    if clean_categories and category not in clean_categories:
        return None

    bundle = _analyze_stock_bundle(
        stock_no=code,
        stock_name=stock_name,
        market_type=market_type,
        start_dt=start_dt,
        end_dt=end_dt,
    )
    if not bundle:
        return None

    signal_score = _safe_float(bundle["signal_snapshot"].get("score"), 0) or 0
    if signal_score < min_signal_score:
        return None

    auto_factor_total = _safe_float(bundle["auto_factor"].get("auto_factor_total"), 0) or 0
    technical_score = _score_clip(signal_score * 12 + (_safe_float(bundle["radar_avg"], 50) or 50) * 0.45)

    position_bonus = 0.0
    if bundle["pressure_dist"] is not None and 0 <= bundle["pressure_dist"] <= 8:
        position_bonus += 8.0
    if bundle["support_dist"] is not None and 0 <= bundle["support_dist"] <= 6:
        position_bonus += 6.0

    base_composite = (
        technical_score * 0.44
        + auto_factor_total * 0.40
        + position_bonus
        + (_safe_float(bundle["period_pct"], 0) * 0.10 if bundle["period_pct"] is not None else 0)
    )
    base_composite = _score_clip(base_composite)

    return {
        "股票代號": code,
        "股票名稱": stock_name,
        "市場別": bundle["used_market"],
        "類別": category or _infer_category_from_name(stock_name),
        "最新價": bundle["close_now"],
        "區間漲跌幅%": bundle["period_pct"],
        "訊號分數": signal_score,
        "雷達均分": bundle["radar_avg"],
        "自動因子總分": auto_factor_total,
        "EPS代理分數": bundle["auto_factor"]["eps_proxy"],
        "營收動能代理分數": bundle["auto_factor"]["revenue_proxy"],
        "獲利代理分數": bundle["auto_factor"]["profit_proxy"],
        "大戶鎖碼代理分數": bundle["auto_factor"]["lock_proxy"],
        "法人連買代理分數": bundle["auto_factor"]["inst_proxy"],
        "20日壓力距離%": bundle["pressure_dist"],
        "20日支撐距離%": bundle["support_dist"],
        "個股原始總分": base_composite,
        "起漲判斷": bundle["trade_plan"]["launch_tag"],
        "推薦買點_突破": bundle["trade_plan"]["breakout_buy"],
        "推薦買點_拉回": bundle["trade_plan"]["pullback_buy"],
        "停損價": bundle["trade_plan"]["stop_price"],
        "賣出目標1": bundle["trade_plan"]["sell_target_1"],
        "賣出目標2": bundle["trade_plan"]["sell_target_2"],
        "風險報酬_拉回": bundle["trade_plan"]["rr1"],
        "風險報酬_突破": bundle["trade_plan"]["rr2"],
        "自動因子摘要": bundle["auto_factor"]["factor_summary"],
        "雷達摘要": _safe_str(bundle["radar"].get("summary")) or "—",
    }


# =========================================================
# 類股強度
# =========================================================
def _compute_category_strength(base_df: pd.DataFrame) -> pd.DataFrame:
    if base_df is None or base_df.empty:
        return pd.DataFrame(columns=["類別", "類股平均總分", "類股平均訊號", "類股平均漲幅", "類股熱度分數"])

    grp = (
        base_df.groupby("類別", dropna=False)
        .agg(
            股票數=("股票代號", "count"),
            類股平均總分=("個股原始總分", "mean"),
            類股平均訊號=("訊號分數", "mean"),
            類股平均漲幅=("區間漲跌幅%", "mean"),
            類股平均雷達=("雷達均分", "mean"),
            類股平均自動因子=("自動因子總分", "mean"),
        )
        .reset_index()
    )

    grp["類股熱度分數"] = (
        grp["類股平均總分"] * 0.38
        + grp["類股平均訊號"] * 6.5
        + grp["類股平均漲幅"].fillna(0) * 0.45
        + grp["類股平均雷達"] * 0.22
        + grp["類股平均自動因子"] * 0.15
    ).apply(lambda x: _score_clip(x))

    grp = grp.sort_values(["類股熱度分數", "類股平均總分"], ascending=[False, False]).reset_index(drop=True)
    return grp


# =========================================================
# 推薦表
# =========================================================
def _build_recommend_df(
    universe_items: list[dict[str, str]],
    master_df: pd.DataFrame,
    start_dt: date,
    end_dt: date,
    min_total_score: float,
    min_signal_score: float,
    selected_categories: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    clean_categories = [_normalize_category(x) for x in selected_categories if _normalize_category(x) and x != "全部"]

    if not universe_items:
        return pd.DataFrame(), pd.DataFrame()

    total_count = len(universe_items)
    worker_count = min(12, max(4, total_count // 8 if total_count >= 8 else 4))

    progress_wrap = st.container()
    progress_bar = progress_wrap.progress(0, text="準備開始推薦...")
    progress_text = progress_wrap.empty()

    start_ts = time.time()
    done_count = 0
    base_rows = []

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [
            executor.submit(
                _analyze_one_stock_for_recommend,
                item,
                master_df,
                start_dt,
                end_dt,
                min_signal_score,
                clean_categories,
            )
            for item in universe_items
        ]

        for future in as_completed(futures):
            done_count += 1

            try:
                row = future.result()
                if row:
                    base_rows.append(row)
            except Exception:
                pass

            elapsed = time.time() - start_ts
            avg_per_stock = elapsed / done_count if done_count > 0 else 0
            remain_count = max(total_count - done_count, 0)
            eta_sec = avg_per_stock * remain_count
            ratio = done_count / total_count if total_count > 0 else 0

            progress_bar.progress(
                min(max(ratio, 0.0), 1.0),
                text=f"推薦計算中... {done_count}/{total_count} ({ratio*100:.1f}%)"
            )
            progress_text.caption(
                f"已完成 {done_count}/{total_count}｜"
                f"已花時間：{_fmt_seconds(elapsed)}｜"
                f"預估剩餘：{_fmt_seconds(eta_sec)}｜"
                f"平均每檔：約 {_fmt_seconds(avg_per_stock)}"
            )

    progress_bar.progress(1.0, text=f"推薦完成，共處理 {total_count} 檔")
    total_elapsed = time.time() - start_ts
    progress_text.caption(f"推薦完成｜總耗時：{_fmt_seconds(total_elapsed)}")

    base_df = pd.DataFrame(base_rows)
    if base_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    category_strength_df = _compute_category_strength(base_df)
    if category_strength_df.empty:
        base_df["類股平均總分"] = None
        base_df["類股平均訊號"] = None
        base_df["類股平均漲幅"] = None
        base_df["類股熱度分數"] = None
    else:
        base_df = base_df.merge(
            category_strength_df[["類別", "類股平均總分", "類股平均訊號", "類股平均漲幅", "類股熱度分數"]],
            on="類別",
            how="left",
        )

    base_df["是否領先同類股"] = (
        base_df["個股原始總分"] >= base_df["類股平均總分"].fillna(0)
    ).map({True: "是", False: "否"})

    base_df["推薦總分"] = (
        base_df["個股原始總分"] * 0.78
        + base_df["類股熱度分數"].fillna(0) * 0.22
    ).apply(lambda x: _score_clip(x))

    def _recommend(score: float) -> str:
        if score >= 84:
            return "強烈關注"
        if score >= 72:
            return "優先觀察"
        if score >= 60:
            return "可列追蹤"
        return "觀察"

    base_df["推薦等級"] = base_df["推薦總分"].apply(_recommend)

    base_df["推薦理由摘要"] = base_df.apply(
        lambda r: (
            f"{_safe_str(r['類別'])}熱度 {_fmt_num(r['類股熱度分數'],1)}，"
            f"個股分數 {_fmt_num(r['個股原始總分'],1)}，"
            f"{'領先同類股' if _safe_str(r['是否領先同類股']) == '是' else '未明顯領先同類股'}，"
            f"{_safe_str(r['起漲判斷'])}"
        ),
        axis=1,
    )

    final_df = base_df[base_df["推薦總分"] >= min_total_score].copy()
    final_df = final_df.sort_values(
        ["推薦總分", "訊號分數", "區間漲跌幅%"],
        ascending=[False, False, False]
    ).reset_index(drop=True)

    return final_df, category_strength_df


def _format_df(df: pd.DataFrame) -> pd.DataFrame:
    show = df.copy()
    price_cols = ["最新價", "推薦買點_突破", "推薦買點_拉回", "停損價", "賣出目標1", "賣出目標2"]
    pct_cols = ["區間漲跌幅%", "20日壓力距離%", "20日支撐距離%", "類股平均漲幅"]
    score_cols = [
        "訊號分數", "雷達均分", "自動因子總分",
        "EPS代理分數", "營收動能代理分數", "獲利代理分數",
        "大戶鎖碼代理分數", "法人連買代理分數",
        "個股原始總分", "類股平均總分", "類股平均訊號", "類股熱度分數", "推薦總分"
    ]

    for c in price_cols:
        if c in show.columns:
            show[c] = show[c].apply(lambda x: format_number(x, 2) if pd.notna(x) else "")
    for c in pct_cols:
        if c in show.columns:
            show[c] = show[c].apply(lambda x: f"{x:,.2f}%" if pd.notna(x) else "")
    for c in score_cols:
        if c in show.columns:
            show[c] = show[c].apply(lambda x: format_number(x, 1) if pd.notna(x) else "")

    return show


def _save_recommend_result_to_state(rec_df: pd.DataFrame, category_strength_df: pd.DataFrame):
    st.session_state[_k("rec_df_store")] = rec_df.copy()
    st.session_state[_k("category_strength_store")] = category_strength_df.copy()
    st.session_state[_k("result_saved_at")] = _now_text()


def _load_recommend_result_from_state() -> tuple[pd.DataFrame, pd.DataFrame]:
    rec_df = st.session_state.get(_k("rec_df_store"))
    cat_df = st.session_state.get(_k("category_strength_store"))

    if isinstance(rec_df, pd.DataFrame) and isinstance(cat_df, pd.DataFrame):
        return rec_df.copy(), cat_df.copy()

    return pd.DataFrame(), pd.DataFrame()


# =========================================================
# Main
# =========================================================
def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    inject_pro_theme()

    watchlist_map = _load_watchlist_map()
    master_df = _load_master_df()
    today = date.today()

    defaults = {
        "universe_mode": "自選群組",
        "group": list(watchlist_map.keys())[0] if watchlist_map else "",
        "days": 120,
        "top_n": 20,
        "manual_codes": "",
        "scan_limit": 1000,
        "selected_categories": ["全部"],
        "min_total_score": 55.0,
        "min_signal_score": -2.0,
        "submitted_once": False,
        "focus_code": "",
        "status_msg": "",
        "status_type": "info",
        "rec_pick_group": list(watchlist_map.keys())[0] if watchlist_map else "",
        "rec_pick_codes": [],
        "result_saved_at": "",
    }
    for name, value in defaults.items():
        if _k(name) not in st.session_state:
            st.session_state[_k(name)] = value

    next_pick_key = _k("rec_pick_codes_next")
    real_pick_key = _k("rec_pick_codes")
    if next_pick_key in st.session_state:
        st.session_state[real_pick_key] = st.session_state.pop(next_pick_key)

    render_pro_hero(
        title="股神推薦｜類股強度版",
        subtitle="按下按鈕才開始推薦，支援 GitHub + Firestore 雙寫、自選股同步、主檔搜尋更新、加速與 ETA。",
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

    if st.session_state.get("watchlist_version"):
        st.caption(
            f"自選股同步狀態：watchlist_version = {st.session_state.get('watchlist_version', 0)}"
            + (
                f" / 最後更新：{_safe_str(st.session_state.get('watchlist_last_saved_at', ''))}"
                if _safe_str(st.session_state.get("watchlist_last_saved_at", ""))
                else ""
            )
        )

    if _k("master_search_keyword") not in st.session_state:
        st.session_state[_k("master_search_keyword")] = ""
    if _k("master_search_market") not in st.session_state:
        st.session_state[_k("master_search_market")] = "全部"
    if _k("master_search_category") not in st.session_state:
        st.session_state[_k("master_search_category")] = "全部"
    if _k("master_search_result") not in st.session_state:
        st.session_state[_k("master_search_result")] = pd.DataFrame()
    if _k("master_selected_code") not in st.session_state:
        st.session_state[_k("master_selected_code")] = ""
    if _k("master_manual_category") not in st.session_state:
        st.session_state[_k("master_manual_category")] = ""

    with st.expander("股票主檔搜尋 / 更新中心", expanded=False):
        render_pro_info_card(
            "功能說明",
            [
                ("搜尋股票", "可用代號、名稱、分類搜尋。", ""),
                ("主檔更新", "可強制重新抓最新股票主檔。", ""),
                ("快速加入", "找到股票後可直接加入自選群組。", ""),
                ("分類修正", "可手動修正單檔分類，供本次頁面使用。", ""),
                ("雙寫同步", "新增/刪除/批次加入時，同步寫 GitHub + Firestore。", ""),
            ],
            chips=["功能不變", "進階版", "搜尋更新", "雙寫同步"],
        )

        s1, s2, s3, s4 = st.columns([2, 1, 1, 1])

        market_opts = _market_options_from_master(master_df)
        category_opts = _category_options_from_master(master_df)

        with s1:
            search_kw = st.text_input(
                "搜尋股票 / 類別",
                value=st.session_state.get(_k("master_search_keyword"), ""),
                placeholder="例如：2330、台積電、AI伺服器、IC設計",
                key=_k("master_search_keyword_input"),
            )
        with s2:
            saved_market = st.session_state.get(_k("master_search_market"), "全部")
            search_market = st.selectbox(
                "市場篩選",
                market_opts,
                index=market_opts.index(saved_market) if saved_market in market_opts else 0,
                key=_k("master_search_market_input"),
            )
        with s3:
            saved_category = st.session_state.get(_k("master_search_category"), "全部")
            search_category = st.selectbox(
                "分類篩選",
                category_opts,
                index=category_opts.index(saved_category) if saved_category in category_opts else 0,
                key=_k("master_search_category_input"),
            )
        with s4:
            st.write("")
            st.write("")
            do_refresh_master = st.button("更新股票主檔", use_container_width=True, key=_k("refresh_master_btn"))

        if do_refresh_master:
            master_df = _refresh_master_df_now()
            st.success(f"股票主檔已重新更新，共 {len(master_df):,} 筆。")

        b1, b2 = st.columns([1, 1])
        with b1:
            do_search_master = st.button("搜尋股票", use_container_width=True, key=_k("search_master_btn"))
        with b2:
            do_clear_master = st.button("清空搜尋", use_container_width=True, key=_k("clear_master_btn"))

        if do_clear_master:
            st.session_state[_k("master_search_keyword")] = ""
            st.session_state[_k("master_search_market")] = "全部"
            st.session_state[_k("master_search_category")] = "全部"
            st.session_state[_k("master_search_result")] = pd.DataFrame()
            st.session_state[_k("master_selected_code")] = ""
            st.session_state[_k("master_manual_category")] = ""
            st.rerun()

        if do_search_master:
            st.session_state[_k("master_search_keyword")] = search_kw
            st.session_state[_k("master_search_market")] = search_market
            st.session_state[_k("master_search_category")] = search_category

            result_df = _search_master_df(
                master_df=master_df,
                keyword=search_kw,
                market_filter=search_market,
                category_filter=search_category,
                limit=100,
            )
            st.session_state[_k("master_search_result")] = result_df

        result_df = st.session_state.get(_k("master_search_result"), pd.DataFrame())

        if isinstance(result_df, pd.DataFrame) and not result_df.empty:
            st.dataframe(
                result_df.rename(
                    columns={
                        "code": "股票代號",
                        "name": "股票名稱",
                        "market": "市場別",
                        "category": "分類",
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )

            code_options = result_df["code"].astype(str).tolist()
            if code_options and st.session_state.get(_k("master_selected_code"), "") not in code_options:
                st.session_state[_k("master_selected_code")] = code_options[0]

            c1, c2, c3 = st.columns([2, 2, 2])

            with c1:
                selected_code = st.selectbox(
                    "選擇股票",
                    code_options,
                    format_func=lambda x: (
                        f"{x} "
                        f"{result_df[result_df['code'].astype(str) == str(x)]['name'].iloc[0]}"
                    ),
                    key=_k("master_selected_code"),
                )

            selected_row = result_df[result_df["code"].astype(str) == str(selected_code)].iloc[0]

            with c2:
                watchlist_groups = list(watchlist_map.keys()) if watchlist_map else []
                add_group = st.selectbox(
                    "加入自選群組",
                    watchlist_groups if watchlist_groups else [""],
                    key=_k("master_add_group"),
                )

            with c3:
                st.write("")
                st.write("")
                add_to_watchlist = st.button("加入自選股", use_container_width=True, key=_k("add_watchlist_btn"))

            if add_to_watchlist:
                ok, msg = _append_stock_to_watchlist(
                    group_name=add_group,
                    code=selected_row["code"],
                    name=selected_row["name"],
                    market=selected_row["market"],
                    category=selected_row["category"],
                )
                if ok:
                    st.success(msg)
                    watchlist_map = _load_watchlist_map()
                else:
                    st.warning(msg)

            d1, d2 = st.columns([2, 1])
            with d1:
                manual_category = st.text_input(
                    "手動修正分類（本次執行立即生效）",
                    value=selected_row["category"],
                    key=_k("master_manual_category"),
                )
            with d2:
                st.write("")
                st.write("")
                save_manual_category = st.button("套用分類修正", use_container_width=True, key=_k("save_manual_category_btn"))

            if save_manual_category:
                master_df = _update_master_category_in_memory(master_df, selected_row["code"], manual_category)
                st.success(f"{selected_row['code']} {selected_row['name']} 分類已改為：{manual_category}")
        else:
            st.caption("尚未搜尋，或目前沒有符合的股票。")

    all_categories = _collect_all_categories(master_df, watchlist_map)
    category_options = ["全部"] + all_categories if all_categories else ["全部"]

    saved_categories = st.session_state.get(_k("selected_categories"), ["全部"])
    saved_categories = [x for x in saved_categories if x in category_options] or ["全部"]

    render_pro_section("掃描設定")

    with st.form(key=_k("recommend_form"), clear_on_submit=False):
        c1, c2, c3, c4 = st.columns([2, 2, 2, 2])

        with c1:
            universe_options = ["自選群組", "手動輸入", "全市場", "上市", "上櫃", "興櫃"]
            saved_universe = st.session_state.get(_k("universe_mode"), "自選群組")
            if saved_universe not in universe_options:
                saved_universe = "自選群組"
            form_universe_mode = st.selectbox(
                "掃描範圍",
                universe_options,
                index=universe_options.index(saved_universe),
            )

        with c2:
            group_options = list(watchlist_map.keys()) if watchlist_map else [""]
            saved_group = st.session_state.get(_k("group"), "")
            if saved_group not in group_options:
                saved_group = group_options[0] if group_options else ""
            form_group = st.selectbox(
                "自選群組",
                group_options,
                index=group_options.index(saved_group) if saved_group in group_options else 0,
            )

        with c3:
            day_options = [60, 90, 120, 180, 240]
            saved_days = int(st.session_state.get(_k("days"), 120))
            if saved_days not in day_options:
                saved_days = 120
            form_days = st.selectbox("觀察天數", day_options, index=day_options.index(saved_days))

        with c4:
            topn_options = [10, 20, 30, 50]
            saved_topn = int(st.session_state.get(_k("top_n"), 20))
            if saved_topn not in topn_options:
                saved_topn = 20
            form_top_n = st.selectbox("輸出 Top N", topn_options, index=topn_options.index(saved_topn))

        d1, d2 = st.columns([2, 2])

        with d1:
            limit_options = [100, 200, 300, 500, 1000, 1500, 2000, "全部"]
            saved_limit = st.session_state.get(_k("scan_limit"), 1000)
            if saved_limit not in limit_options:
                saved_limit = 1000

            form_scan_limit = st.selectbox(
                "掃描上限筆數",
                limit_options,
                index=limit_options.index(saved_limit),
                help="選『全部』時，會把目前市場範圍內的股票全部納入掃描，不做截斷。",
            )

        with d2:
            form_manual_codes = st.text_area(
                "手動輸入股票（可代碼 / 名稱，一行一檔）",
                value=st.session_state.get(_k("manual_codes"), ""),
                height=110,
                placeholder="2330\n2454\n3548\n台積電",
            )

        render_pro_section("類型篩選")
        form_selected_categories = st.multiselect(
            "選擇類型（可多選）",
            options=category_options,
            default=saved_categories,
            help="已細分為 IC設計、晶圓代工、封測、AI伺服器、散熱、金控、銀行等。",
        )

        render_pro_section("推薦門檻")
        f1, f2 = st.columns(2)
        with f1:
            form_min_total_score = st.number_input(
                "推薦總分下限",
                value=float(st.session_state.get(_k("min_total_score"), 55.0)),
                step=1.0,
            )
        with f2:
            form_min_signal_score = st.number_input(
                "訊號分數下限",
                value=float(st.session_state.get(_k("min_signal_score"), -2.0)),
                step=1.0,
            )

        btn1, btn2, btn3 = st.columns([2, 2, 2])
        with btn1:
            submit_recommend = st.form_submit_button("開始推薦", use_container_width=True, type="primary")
        with btn2:
            submit_refresh = st.form_submit_button("重新推薦", use_container_width=True)
        with btn3:
            submit_clear = st.form_submit_button("清空條件", use_container_width=True)

    ccache1, ccache2 = st.columns([1, 1])
    with ccache1:
        clear_cache_btn = st.button("清除推薦快取", use_container_width=True)
    with ccache2:
        st.caption("資料異常或想強制重算時再按")

    if clear_cache_btn:
        try:
            _get_history_smart.clear()
        except Exception:
            pass
        try:
            _analyze_stock_bundle.clear()
        except Exception:
            pass
        try:
            _load_master_df.clear()
        except Exception:
            pass
        st.success("推薦快取已清除")

    if submit_clear:
        st.session_state[_k("universe_mode")] = "自選群組"
        st.session_state[_k("group")] = list(watchlist_map.keys())[0] if watchlist_map else ""
        st.session_state[_k("days")] = 120
        st.session_state[_k("top_n")] = 20
        st.session_state[_k("manual_codes")] = ""
        st.session_state[_k("scan_limit")] = 1000
        st.session_state[_k("selected_categories")] = ["全部"]
        st.session_state[_k("min_total_score")] = 55.0
        st.session_state[_k("min_signal_score")] = -2.0
        st.session_state[_k("submitted_once")] = False
        st.session_state[_k("focus_code")] = ""
        st.session_state[_k("rec_df_store")] = pd.DataFrame()
        st.session_state[_k("category_strength_store")] = pd.DataFrame()
        st.session_state[_k("rec_pick_codes_next")] = []
        st.rerun()

    if submit_recommend or submit_refresh:
        st.session_state[_k("universe_mode")] = form_universe_mode
        st.session_state[_k("group")] = form_group
        st.session_state[_k("days")] = form_days
        st.session_state[_k("top_n")] = form_top_n
        st.session_state[_k("manual_codes")] = form_manual_codes
        st.session_state[_k("scan_limit")] = form_scan_limit
        st.session_state[_k("selected_categories")] = form_selected_categories if form_selected_categories else ["全部"]
        st.session_state[_k("min_total_score")] = float(form_min_total_score)
        st.session_state[_k("min_signal_score")] = float(form_min_signal_score)
        st.session_state[_k("submitted_once")] = True

    render_pro_info_card(
        "類股強度邏輯",
        [
            ("類型細分", "半導體 / AI / 電子 / 金融已再細分成更小分類。", ""),
            ("類股熱度", "用同類股平均總分、平均訊號、平均漲幅計算。", ""),
            ("個股領先", "若個股原始總分高於同類股平均，視為領先股。", ""),
            ("最終推薦", "個股原始總分 + 類股熱度分數一起決定。", ""),
        ],
        chips=["類型更細", "類股強度", "股神版"],
    )

    if not st.session_state.get(_k("submitted_once"), False):
        st.info("請先設定條件，再按「開始推薦」。")
        return

    selected_categories = st.session_state.get(_k("selected_categories"), ["全部"])
    universe_mode = _safe_str(st.session_state.get(_k("universe_mode"), ""))

    if universe_mode == "自選群組":
        universe_items = watchlist_map.get(_safe_str(st.session_state.get(_k("group"), "")), [])
    elif universe_mode == "手動輸入":
        universe_items = _parse_manual_codes(st.session_state.get(_k("manual_codes"), ""), master_df)
    else:
        universe_items = _build_universe_from_market(
            master_df=master_df,
            market_mode=universe_mode,
            limit_count=st.session_state.get(_k("scan_limit"), 1000),
            selected_categories=selected_categories,
        )

    if not universe_items:
        st.warning("目前掃描池沒有股票。")
        return

    start_dt = today - timedelta(days=int(st.session_state.get(_k("days"), 120)))
    end_dt = today

    rec_df = pd.DataFrame()
    category_strength_df = pd.DataFrame()

    if submit_recommend or submit_refresh:
        rec_df, category_strength_df = _build_recommend_df(
            universe_items=universe_items,
            master_df=master_df,
            start_dt=start_dt,
            end_dt=end_dt,
            min_total_score=float(st.session_state.get(_k("min_total_score"), 55.0)),
            min_signal_score=float(st.session_state.get(_k("min_signal_score"), -2.0)),
            selected_categories=selected_categories,
        )
        _save_recommend_result_to_state(rec_df, category_strength_df)
    else:
        rec_df, category_strength_df = _load_recommend_result_from_state()

    if rec_df.empty:
        st.error("目前沒有已保存的推薦結果，請先按一次「開始推薦」。")
        return

    saved_at = _safe_str(st.session_state.get(_k("result_saved_at"), ""))
    if saved_at:
        st.caption(f"目前顯示的是已保存推薦結果｜保存時間：{saved_at}")

    top_n = int(st.session_state.get(_k("top_n"), 20))
    top_df = rec_df.head(top_n).copy()

    strong_count = int((rec_df["推薦等級"] == "強烈關注").sum())
    avg_score = _avg_safe([_safe_float(x) for x in rec_df["推薦總分"].tolist()], 0)
    leader_count = int((rec_df["是否領先同類股"] == "是").sum())

    render_pro_kpi_row(
        [
            {"label": "掃描股票數", "value": len(rec_df), "delta": universe_mode, "delta_class": "pro-kpi-delta-flat"},
            {"label": "強烈關注", "value": strong_count, "delta": "最高等級", "delta_class": "pro-kpi-delta-flat"},
            {"label": "領先同類股", "value": leader_count, "delta": "類股相對強勢", "delta_class": "pro-kpi-delta-flat"},
            {"label": "平均總分", "value": format_number(avg_score, 1), "delta": "含類股熱度", "delta_class": "pro-kpi-delta-flat"},
        ]
    )

    render_pro_section("推薦股票加入自選股中心")

    watchlist_map = _load_watchlist_map()
    rec_group_options = list(watchlist_map.keys()) if watchlist_map else [""]
    saved_pick_group = st.session_state.get(_k("rec_pick_group"), "")
    if saved_pick_group not in rec_group_options:
        saved_pick_group = rec_group_options[0] if rec_group_options else ""
        st.session_state[_k("rec_pick_group")] = saved_pick_group

    rec_code_to_label = {
        str(r["股票代號"]): f"{r['股票代號']} {r['股票名稱']}｜{r['推薦等級']}｜{format_number(r['推薦總分'],1)}"
        for _, r in rec_df.iterrows()
    }
    rec_all_codes = rec_df["股票代號"].astype(str).tolist()

    p1, p2, p3 = st.columns([2, 4, 2])
    with p1:
        pick_group = st.selectbox(
            "加入群組",
            options=rec_group_options,
            index=rec_group_options.index(saved_pick_group) if saved_pick_group in rec_group_options else 0,
            key=_k("rec_pick_group"),
        )
    with p2:
        current_pick_codes = [
            x for x in st.session_state.get(_k("rec_pick_codes"), [])
            if x in rec_all_codes
        ]

        st.multiselect(
            "勾選推薦股",
            options=rec_all_codes,
            default=current_pick_codes,
            format_func=lambda x: rec_code_to_label.get(str(x), str(x)),
            key=_k("rec_pick_codes"),
        )
    with p3:
        st.write("")
        st.write("")
        add_selected_btn = st.button("加入勾選股票到自選股中心", use_container_width=True, type="primary")

    q1, q2 = st.columns([1, 1])
    with q1:
        if st.button("全選本輪推薦", use_container_width=True):
            st.session_state[_k("rec_pick_codes_next")] = rec_all_codes
            st.rerun()

    with q2:
        if st.button("清空勾選", use_container_width=True):
            st.session_state[_k("rec_pick_codes_next")] = []
            st.rerun()

    if add_selected_btn:
        selected_codes = [_normalize_code(x) for x in st.session_state.get(_k("rec_pick_codes"), []) if _normalize_code(x)]
        if not selected_codes:
            st.warning("請先勾選推薦股票。")
        else:
            picked_rows = []
            work = rec_df[rec_df["股票代號"].astype(str).isin(selected_codes)].copy()
            for _, r in work.iterrows():
                picked_rows.append(
                    {
                        "code": _normalize_code(r.get("股票代號")),
                        "name": _safe_str(r.get("股票名稱")),
                        "market": _safe_str(r.get("市場別")) or "上市",
                        "category": _normalize_category(r.get("類別")),
                    }
                )

            added, messages = _append_multiple_stocks_to_watchlist(pick_group, picked_rows)
            if added > 0:
                st.success(f"已加入 {added} 檔到 {pick_group}")
                watchlist_map = _load_watchlist_map()
            else:
                st.warning("沒有新增成功。")

            if messages:
                with st.expander("加入結果明細", expanded=False):
                    for msg in messages:
                        st.write(f"- {msg}")

    render_pro_section("本輪精華推薦")
    st.dataframe(
        _format_df(
            top_df[
                [
                    "股票代號",
                    "股票名稱",
                    "市場別",
                    "類別",
                    "推薦等級",
                    "推薦總分",
                    "類股熱度分數",
                    "是否領先同類股",
                    "起漲判斷",
                    "最新價",
                    "推薦買點_拉回",
                    "推薦買點_突破",
                    "停損價",
                    "賣出目標1",
                    "賣出目標2",
                    "推薦理由摘要",
                ]
            ]
        ),
        use_container_width=True,
        hide_index=True,
    )

    pick_options = top_df["股票代號"].astype(str).tolist()
    if pick_options and st.session_state.get(_k("focus_code"), "") not in pick_options:
        st.session_state[_k("focus_code")] = pick_options[0]

    code_to_row = {str(r["股票代號"]): r for _, r in rec_df.iterrows()}

    render_pro_section("單股股神劇本")
    selected_code = st.selectbox(
        "選擇推薦股",
        options=pick_options,
        format_func=lambda x: f"{x} {code_to_row.get(str(x), {}).get('股票名稱', '')}",
        key=_k("focus_code"),
    )

    focus_row = code_to_row.get(str(selected_code))
    if focus_row is not None:
        render_pro_info_card(
            "股神推薦結論",
            [
                ("股票", f"{_safe_str(focus_row.get('股票代號'))} {_safe_str(focus_row.get('股票名稱'))}", ""),
                ("類別", _safe_str(focus_row.get("類別")), ""),
                ("推薦等級", _safe_str(focus_row.get("推薦等級")), ""),
                ("推薦總分", format_number(focus_row.get("推薦總分"), 1), ""),
                ("類股熱度分數", format_number(focus_row.get("類股熱度分數"), 1), ""),
                ("是否領先同類股", _safe_str(focus_row.get("是否領先同類股")), ""),
                ("起漲判斷", _safe_str(focus_row.get("起漲判斷")), ""),
                ("推薦買點（拉回）", format_number(focus_row.get("推薦買點_拉回"), 2), ""),
                ("推薦買點（突破）", format_number(focus_row.get("推薦買點_突破"), 2), ""),
                ("停損價", format_number(focus_row.get("停損價"), 2), ""),
                ("賣出目標1", format_number(focus_row.get("賣出目標1"), 2), ""),
                ("賣出目標2", format_number(focus_row.get("賣出目標2"), 2), ""),
                ("風險報酬（拉回）", _safe_str(focus_row.get("風險報酬_拉回")), ""),
                ("風險報酬（突破）", _safe_str(focus_row.get("風險報酬_突破")), ""),
                ("推薦理由摘要", _safe_str(focus_row.get("推薦理由摘要")), ""),
            ],
            chips=[
                _safe_str(focus_row.get("推薦等級")),
                _safe_str(focus_row.get("類別")),
                _safe_str(focus_row.get("是否領先同類股")),
            ],
        )

    tabs = st.tabs(["完整推薦表", "類股強度榜", "同類股領先榜", "自動因子榜", "操作說明"])

    with tabs[0]:
        st.dataframe(_format_df(rec_df), use_container_width=True, hide_index=True)

    with tabs[1]:
        category_show = category_strength_df.copy()
        for c in ["類股平均總分", "類股平均訊號", "類股平均漲幅", "類股平均雷達", "類股平均自動因子", "類股熱度分數"]:
            if c in category_show.columns:
                if c == "類股平均漲幅":
                    category_show[c] = category_show[c].apply(lambda x: f"{x:,.2f}%" if pd.notna(x) else "")
                else:
                    category_show[c] = category_show[c].apply(lambda x: format_number(x, 1) if pd.notna(x) else "")
        st.dataframe(category_show, use_container_width=True, hide_index=True)

    with tabs[2]:
        leader_df = rec_df.sort_values(["是否領先同類股", "推薦總分", "類股熱度分數"], ascending=[False, False, False]).copy()
        st.dataframe(
            _format_df(
                leader_df[
                    [
                        "股票代號",
                        "股票名稱",
                        "類別",
                        "是否領先同類股",
                        "個股原始總分",
                        "類股平均總分",
                        "類股熱度分數",
                        "推薦總分",
                        "推薦理由摘要",
                    ]
                ].head(top_n)
            ),
            use_container_width=True,
            hide_index=True,
        )

    with tabs[3]:
        factor_rank = rec_df.sort_values(
            ["自動因子總分", "EPS代理分數", "營收動能代理分數", "獲利代理分數"],
            ascending=[False, False, False, False]
        ).reset_index(drop=True)
        st.dataframe(
            _format_df(
                factor_rank[
                    [
                        "股票代號",
                        "股票名稱",
                        "類別",
                        "自動因子總分",
                        "EPS代理分數",
                        "營收動能代理分數",
                        "獲利代理分數",
                        "大戶鎖碼代理分數",
                        "法人連買代理分數",
                        "自動因子摘要",
                    ]
                ].head(top_n)
            ),
            use_container_width=True,
            hide_index=True,
        )

    with tabs[4]:
        render_pro_info_card(
            "模組邏輯",
            [
                ("按鈕觸發", "調整條件不會自動重算，按下開始推薦才會跑。", ""),
                ("類型更細分", "已由大類擴充成 IC設計、晶圓代工、封測、AI伺服器、散熱、金控、銀行等。", ""),
                ("類股強度", "每個類別都會算平均總分、平均訊號、平均漲幅與類股熱度分數。", ""),
                ("個股領先", "若個股原始總分高於同類股平均，視為領先股。", ""),
                ("推薦總分", "個股原始總分 78% + 類股熱度 22%。", ""),
                ("股票搜尋更新中心", "新增搜尋主檔、更新主檔、加入自選股、分類修正。", ""),
                ("加速與 ETA", "歷史資料與單股分析保留快取，整批推薦改成併發並顯示剩餘時間。", ""),
                ("推薦加入自選股", "可直接勾選推薦結果並批次加入指定群組。", ""),
                ("雙寫同步", "自選股新增/刪除/批次加入時，同步寫回 GitHub watchlist.json + Firestore。", ""),
                ("推薦結果保留", "推薦結果會存到 session_state，切頁後回來不會立刻消失。", ""),
                ("掃描上限", "已支援 1000 / 1500 / 2000 / 全部掃描。", ""),
            ],
            chips=["按鈕觸發", "類股強度版", "股神版", "進階搜尋更新", "加速+ETA", "GitHub+Firestore雙寫", "全部掃描"],
        )


if __name__ == "__main__":
    main()
