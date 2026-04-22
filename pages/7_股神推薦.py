# pages/7_股神推薦.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import date, timedelta, datetime
from typing import Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import copy
import json
import base64
import io
import hashlib
import html

import pandas as pd
import requests
import streamlit as st
import firebase_admin
from firebase_admin import credentials, firestore

from stock_master_service import (
    load_stock_master,
    refresh_stock_master,
    search_stock_master,
    get_stock_master_categories,
    get_stock_master_diagnostics,
)

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

GODPICK_RECORD_COLUMNS = [
    "record_id",
    "股票代號",
    "股票名稱",
    "市場別",
    "類別",
    "推薦模式",
    "推薦等級",
    "推薦總分",
    "技術結構分數",
    "起漲前兆分數",
    "交易可行分數",
    "類股熱度分數",
    "同類股領先幅度",
    "是否領先同類股",
    "推薦標籤",
    "推薦理由摘要",
    "推薦價格",
    "停損價",
    "賣出目標1",
    "賣出目標2",
    "推薦日期",
    "推薦時間",
    "建立時間",
    "更新時間",
    "目前狀態",
    "是否已實際買進",
    "實際買進價",
    "實際賣出價",
    "實際報酬%",
    "最新價",
    "最新更新時間",
    "損益金額",
    "損益幅%",
    "是否達停損",
    "是否達目標1",
    "是否達目標2",
    "持有天數",
    "模式績效標籤",
    "備註",
]


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


def _now_date_text() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _now_time_text() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _create_record_id(code: str, rec_date: str, rec_time: str, mode: str) -> str:
    raw = f"{_safe_str(code)}|{_safe_str(rec_date)}|{_safe_str(rec_time)}|{_safe_str(mode)}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _set_status(msg: str, level: str = "info"):
    st.session_state[_k("status_msg")] = msg
    st.session_state[_k("status_type")] = level


def _ensure_godpick_record_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS)

    x = df.copy()

    if "record_id" not in x.columns and "rec_id" in x.columns:
        x["record_id"] = x["rec_id"]

    for c in GODPICK_RECORD_COLUMNS:
        if c not in x.columns:
            x[c] = None

    numeric_cols = [
        "推薦總分", "技術結構分數", "起漲前兆分數", "交易可行分數", "類股熱度分數",
        "同類股領先幅度", "推薦價格", "停損價", "賣出目標1", "賣出目標2",
        "實際買進價", "實際賣出價", "實際報酬%", "最新價", "損益金額", "損益幅%", "持有天數"
    ]
    for c in numeric_cols:
        x[c] = pd.to_numeric(x[c], errors="coerce")

    bool_cols = ["是否領先同類股", "是否已實際買進", "是否達停損", "是否達目標1", "是否達目標2"]
    for c in bool_cols:
        x[c] = x[c].fillna(False).map(lambda v: str(v).strip().lower() in {"true", "1", "yes", "y", "是"})

    x["目前狀態"] = x["目前狀態"].fillna("觀察").replace("", "觀察")
    x["推薦日期"] = x["推薦日期"].fillna("").astype(str).replace("", _now_date_text())
    x["推薦時間"] = x["推薦時間"].fillna("").astype(str).replace("", _now_time_text())
    x["建立時間"] = x["建立時間"].fillna("").astype(str).replace("", _now_text())
    x["更新時間"] = x["更新時間"].fillna("").astype(str).replace("", _now_text())
    x["最新更新時間"] = x["最新更新時間"].fillna("").astype(str)
    x["模式績效標籤"] = x["模式績效標籤"].fillna("").astype(str)
    x["備註"] = x["備註"].fillna("").astype(str)

    need_id = x["record_id"].isna() | (x["record_id"].astype(str).str.strip() == "")
    if need_id.any():
        for idx in x[need_id].index:
            rec_date = _safe_str(x.at[idx, "推薦日期"]) or _now_date_text()
            rec_time = _safe_str(x.at[idx, "推薦時間"]) or _now_time_text()
            x.at[idx, "record_id"] = _create_record_id(
                _safe_str(x.at[idx, "股票代號"]),
                rec_date,
                rec_time,
                _safe_str(x.at[idx, "推薦模式"]),
            )

    return x[GODPICK_RECORD_COLUMNS].copy()


def _append_records_dedup_by_business_key(base_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    base_df = _ensure_godpick_record_columns(base_df)
    new_df = _ensure_godpick_record_columns(new_df)

    if new_df.empty:
        return base_df.copy()

    merged = pd.concat([base_df, new_df], ignore_index=True)
    merged["_biz_key"] = (
        merged["股票代號"].fillna("").astype(str) + "|"
        + merged["推薦日期"].fillna("").astype(str) + "|"
        + merged["推薦時間"].fillna("").astype(str) + "|"
        + merged["推薦模式"].fillna("").astype(str)
    )
    merged["_upd"] = pd.to_datetime(merged["更新時間"], errors="coerce")
    merged = merged.sort_values(["_biz_key", "_upd"], ascending=[True, False], na_position="last")
    merged = merged.drop_duplicates(subset=["_biz_key"], keep="first")
    return _ensure_godpick_record_columns(merged.drop(columns=["_biz_key", "_upd"], errors="ignore"))



# =========================================================
# 類別推論
# =========================================================
CATEGORY_KEYWORD_RULES: list[tuple[str, list[str]]] = [
    ("晶圓代工", ["台積", "聯電", "力積電", "世界先進", "umc", "tsmc", "晶圓代工"]),
    ("IC設計", ["聯發科", "瑞昱", "聯詠", "群聯", "創意", "世芯", "智原", "敦泰", "原相", "晶心科", "矽力", "力旺", "天鈺", "義隆", "祥碩", "譜瑞", "聯陽", "瑞鼎", "義傳", "ic設計"]),
    ("封測", ["日月光", "矽品", "京元電", "頎邦", "欣銓", "矽格", "封測", "測試"]),
    ("記憶體", ["南亞科", "華邦電", "旺宏", "宇瞻", "十銓", "記憶體", "dram", "nand"]),
    ("矽晶圓", ["環球晶", "中美晶", "合晶", "嘉晶", "矽晶圓"]),
    ("半導體設備材料", ["帆宣", "漢唐", "家登", "辛耘", "中砂", "崇越", "萬潤", "均豪", "弘塑", "設備", "材料"]),
    ("IP矽智財", ["力旺", "晶心科", "智原", "創意", "世芯", "ip", "矽智財"]),
    ("AI伺服器", ["伺服器", "server", "緯穎", "廣達", "英業達", "緯創", "鴻海", "技嘉", "微星", "華碩"]),
    ("散熱", ["雙鴻", "奇鋐", "建準", "散熱", "風扇", "熱導管"]),
    ("機殼", ["勤誠", "晟銘電", "迎廣", "機殼"]),
    ("電源供應", ["台達電", "光寶科", "群電", "全漢", "康舒", "電源", "供應器"]),
    ("高速傳輸", ["高速", "傳輸", "祥碩", "譜瑞", "創惟", "威鋒", "usb4", "pcie"]),
    ("網通交換器", ["智邦", "明泰", "中磊", "智易", "啟碁", "網通", "交換器", "switch"]),
    ("光通訊", ["光通訊", "波若威", "華星光", "聯鈞", "上詮", "眾達", "聯亞", "光聖", "cpo"]),
    ("PCB載板", ["欣興", "南電", "景碩", "金像電", "健鼎", "台燿", "華通", "載板", "pcb", "銅箔基板"]),
    ("EMS代工", ["鴻海", "和碩", "廣達", "仁寶", "英業達", "緯創", "組裝"]),
    ("面板", ["友達", "群創", "彩晶", "凌巨", "面板"]),
    ("光學鏡頭", ["大立光", "玉晶光", "亞光", "今國光", "鏡頭", "光學"]),
    ("被動元件", ["國巨", "華新科", "禾伸堂", "凱美", "立隆電", "被動元件", "電容", "電阻"]),
    ("連接器", ["貿聯", "嘉澤", "信邦", "良維", "胡連", "連接器", "端子", "連接線"]),
    ("電池材料", ["康普", "美琪瑪", "立凱", "長園科", "電池", "材料", "鋰"]),
    ("金控", ["金控"]),
    ("銀行", ["銀行"]),
    ("保險", ["保險"]),
    ("證券", ["證券"]),
    ("航運", ["長榮", "陽明", "萬海", "裕民", "慧洋", "航運", "海運", "貨櫃", "散裝"]),
    ("航空觀光", ["華航", "長榮航", "星宇", "航空", "觀光", "旅遊", "飯店"]),
    ("鋼鐵", ["中鋼", "大成鋼", "東和鋼鐵", "鋼鐵", "鋼"]),
    ("塑化", ["台塑", "南亞", "台化", "台塑化", "台聚", "塑化", "化工"]),
    ("生技醫療", ["保瑞", "藥華藥", "美時", "生技", "醫療", "製藥", "藥", "醫材"]),
    ("車用電子", ["和大", "貿聯", "堤維西", "東陽", "車用", "車電", "汽車"]),
    ("綠能儲能", ["中興電", "華城", "士電", "儲能", "綠能", "太陽能", "風電"]),
    ("營建資產", ["營建", "建設", "資產"]),
    ("食品民生", ["統一", "大成", "食品", "餐飲", "飲料"]),
    ("紡織製鞋", ["儒鴻", "聚陽", "志強", "豐泰", "寶成", "紡織", "成衣", "製鞋"]),
    ("電機機械", ["上銀", "亞德客", "直得", "全球傳動", "機械", "工具機", "自動化"]),
    ("其他電子", ["電子", "電腦", "光電"]),
]

CANONICAL_CATEGORY_ALIAS = {
    "半導體": "半導體設備材料",
    "半導體設備": "半導體設備材料",
    "設備材料": "半導體設備材料",
    "半導體材料": "半導體設備材料",
    "伺服器": "AI伺服器",
    "server": "AI伺服器",
    "網通": "網通交換器",
    "交換器": "網通交換器",
    "光通訊/cpo": "光通訊",
    "載板": "PCB載板",
    "pcb": "PCB載板",
    "ems": "EMS代工",
    "鏡頭": "光學鏡頭",
    "光學": "光學鏡頭",
    "被動": "被動元件",
    "電池": "電池材料",
    "生技": "生技醫療",
    "醫療": "生技醫療",
    "車電": "車用電子",
    "綠能": "綠能儲能",
    "建材營造": "營建資產",
    "營建": "營建資產",
    "機械": "電機機械",
}

def _canonical_category(v: Any) -> str:
    text = _normalize_category(v)
    if not text:
        return ""
    key = text.lower()
    for alias, target in CANONICAL_CATEGORY_ALIAS.items():
        if key == alias.lower():
            return target
    return text

def _infer_category_from_name(name: str) -> str:
    n = _safe_str(name)
    if not n:
        return "其他"

    s = n.lower()
    for cat, keywords in CATEGORY_KEYWORD_RULES:
        for kw in keywords:
            if kw.lower() in s:
                return cat
    return "其他"

def _infer_category_from_record(name: str, raw_category: Any) -> str:
    raw_cat = _canonical_category(raw_category)
    if raw_cat:
        if raw_cat in {x[0] for x in CATEGORY_KEYWORD_RULES}:
            return raw_cat
        by_name = _infer_category_from_name(raw_cat)
        if by_name != "其他":
            return by_name
        return raw_cat
    return _infer_category_from_name(name)


# =========================================================
# GitHub / Firestore
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
    if not token:
        return "", "缺少 GITHUB_TOKEN"

    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            params={"ref": cfg["branch"]},
            timeout=20,
        )
        if resp.status_code == 200:
            return _safe_str(resp.json().get("sha")), ""
        if resp.status_code == 404:
            return "", ""
        return "", f"讀取 GitHub 檔案失敗：{resp.status_code} / {resp.text[:300]}"
    except Exception as e:
        return "", f"讀取 GitHub 檔案例外：{e}"


def _push_watchlist_to_github(payload: dict[str, list[dict[str, str]]]) -> tuple[bool, str]:
    cfg = _github_config()
    token = cfg["token"]
    if not token:
        return False, "未設定 GITHUB_TOKEN"

    sha, err = _get_repo_watchlist_sha(cfg)
    if err:
        return False, err

    content_text = json.dumps(payload, ensure_ascii=False, indent=2)
    encoded_content = base64.b64encode(content_text.encode("utf-8")).decode("utf-8")

    body: dict[str, Any] = {
        "message": f"update watchlist from streamlit at {_now_text()}",
        "content": encoded_content,
        "branch": cfg["branch"],
    }
    if sha:
        body["sha"] = sha

    try:
        resp = requests.put(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            json=body,
            timeout=30,
        )
        if resp.status_code in (200, 201):
            return True, f"已回寫 GitHub：{cfg['path']}"
        return False, f"GitHub API 寫入失敗：{resp.status_code} / {resp.text[:500]}"
    except Exception as e:
        return False, f"GitHub API 寫入例外：{e}"


def _firebase_config() -> dict[str, str]:
    return {
        "project_id": _safe_str(st.secrets.get("FIREBASE_PROJECT_ID", "")),
        "client_email": _safe_str(st.secrets.get("FIREBASE_CLIENT_EMAIL", "")),
        "private_key": _safe_str(st.secrets.get("FIREBASE_PRIVATE_KEY", "")),
    }


def _clean_private_key(raw_key: str) -> str:
    private_key = _safe_str(raw_key)
    private_key = private_key.replace("\\n", "\n").strip()
    if private_key.startswith("\ufeff"):
        private_key = private_key.lstrip("\ufeff")
    return private_key


def _init_firebase_app():
    try:
        return firebase_admin.get_app()
    except ValueError:
        pass

    cfg = _firebase_config()
    project_id = _safe_str(cfg["project_id"]).strip()
    client_email = _safe_str(cfg["client_email"]).strip()
    private_key = _clean_private_key(cfg["private_key"])

    if not project_id:
        raise ValueError("缺少 FIREBASE_PROJECT_ID")
    if not client_email:
        raise ValueError("缺少 FIREBASE_CLIENT_EMAIL")
    if not private_key:
        raise ValueError("缺少 FIREBASE_PRIVATE_KEY")
    if "BEGIN PRIVATE KEY" not in private_key or "END PRIVATE KEY" not in private_key:
        raise ValueError("FIREBASE_PRIVATE_KEY 不是有效 PEM 格式")

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
    try:
        _init_firebase_app()
        db = firestore.client()
        batch = db.batch()
        now = firestore.SERVER_TIMESTAMP

        summary_ref = db.collection("system").document("watchlist_summary")
        batch.set(
            summary_ref,
            {"group_count": len(payload), "updated_at": now, "source": "streamlit_dual_write"},
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

            new_codes = set()
            for item in items:
                code = _normalize_code(item.get("code"))
                if not code:
                    continue
                new_codes.add(code)
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

            existing_docs = list(group_ref.collection("stocks").stream())
            for doc in existing_docs:
                if doc.id not in new_codes:
                    batch.delete(doc.reference)

        batch.commit()
        return True, "已同步寫入 Firestore"
    except Exception as e:
        return False, f"Firestore 寫入失敗：{e}"


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

            row = {"code": code, "name": name, "market": market}
            if category:
                row["category"] = category
            normalized_items.append(row)

        payload[g] = sorted(normalized_items, key=lambda x: (_normalize_code(x.get("code")), _safe_str(x.get("name"))))
    return payload


def _force_write_watchlist_dual(data: dict[str, list[dict[str, str]]]) -> bool:
    payload = _normalize_watchlist_payload(data)
    ok_github, msg_github = _push_watchlist_to_github(payload)
    ok_firestore, msg_firestore = _push_watchlist_to_firestore(payload)

    st.session_state["watchlist_data"] = copy.deepcopy(payload)
    st.session_state["watchlist_version"] = int(st.session_state.get("watchlist_version", 0)) + 1
    st.session_state["watchlist_last_saved_at"] = _now_text()

    st.session_state[_k("last_dual_write_detail")] = [
        f"GitHub: {'成功' if ok_github else '失敗'} | {msg_github}",
        f"Firestore: {'成功' if ok_firestore else '失敗'} | {msg_firestore}",
    ]

    if ok_github and ok_firestore:
        _set_status("GitHub + Firestore 同步成功", "success")
        return True
    if ok_github or ok_firestore:
        _set_status("部分同步成功", "warning")
        return True
    _set_status("GitHub / Firestore 都失敗", "error")
    return False


# =========================================================
# 8 頁推薦紀錄 寫入
# =========================================================
def _godpick_records_config() -> dict[str, str]:
    return {
        "token": _safe_str(st.secrets.get("GITHUB_TOKEN", "")),
        "owner": _safe_str(st.secrets.get("GITHUB_REPO_OWNER", "cheng07021028")),
        "repo": _safe_str(st.secrets.get("GITHUB_REPO_NAME", "stock-app")),
        "branch": _safe_str(st.secrets.get("GITHUB_REPO_BRANCH", "main")) or "main",
        "path": _safe_str(st.secrets.get("GODPICK_RECORDS_GITHUB_PATH", "godpick_records.json")) or "godpick_records.json",
    }


def _read_godpick_records_from_github() -> tuple[list[dict[str, Any]], str]:
    cfg = _godpick_records_config()
    token = cfg["token"]
    if not token:
        return [], "未設定 GITHUB_TOKEN"

    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            params={"ref": cfg["branch"]},
            timeout=20,
        )
        if resp.status_code == 404:
            return [], ""
        if resp.status_code != 200:
            return [], f"讀取推薦紀錄失敗：{resp.status_code} / {resp.text[:300]}"

        data = resp.json()
        content = data.get("content", "")
        if not content:
            return [], ""

        decoded = base64.b64decode(content).decode("utf-8")
        payload = json.loads(decoded)
        if isinstance(payload, list):
            return payload, ""
        return [], ""
    except Exception as e:
        return [], f"讀取推薦紀錄例外：{e}"


def _get_godpick_records_sha() -> tuple[str, str]:
    cfg = _godpick_records_config()
    token = cfg["token"]
    if not token:
        return "", "缺少 GITHUB_TOKEN"

    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            params={"ref": cfg["branch"]},
            timeout=20,
        )
        if resp.status_code == 200:
            return _safe_str(resp.json().get("sha")), ""
        if resp.status_code == 404:
            return "", ""
        return "", f"讀取推薦紀錄 SHA 失敗：{resp.status_code} / {resp.text[:300]}"
    except Exception as e:
        return "", f"讀取推薦紀錄 SHA 例外：{e}"


def _write_godpick_records_to_github(records: list[dict[str, Any]]) -> tuple[bool, str]:
    cfg = _godpick_records_config()
    token = cfg["token"]
    if not token:
        return False, "未設定 GITHUB_TOKEN"

    sha, err = _get_godpick_records_sha()
    if err:
        return False, err

    content_text = json.dumps(records, ensure_ascii=False, indent=2)
    encoded_content = base64.b64encode(content_text.encode("utf-8")).decode("utf-8")

    body: dict[str, Any] = {
        "message": f"update godpick records at {_now_text()}",
        "content": encoded_content,
        "branch": cfg["branch"],
    }
    if sha:
        body["sha"] = sha

    try:
        resp = requests.put(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            json=body,
            timeout=30,
        )
        if resp.status_code in (200, 201):
            return True, f"已回寫 GitHub：{cfg['path']}"
        return False, f"推薦紀錄 GitHub 寫入失敗：{resp.status_code} / {resp.text[:500]}"
    except Exception as e:
        return False, f"推薦紀錄 GitHub 寫入例外：{e}"


def _write_godpick_records_to_firestore(records: list[dict[str, Any]]) -> tuple[bool, str]:
    try:
        _init_firebase_app()
        db = firestore.client()
        batch = db.batch()
        now = firestore.SERVER_TIMESTAMP

        summary_ref = db.collection("system").document("godpick_records_summary")
        batch.set(summary_ref, {"count": len(records), "updated_at": now, "source": "streamlit_godpick_records"}, merge=True)

        records_ref = db.collection("godpick_records")
        existing_docs = list(records_ref.stream())
        existing_ids = {doc.id for doc in existing_docs}
        new_ids = set()

        for row in records:
            rec_id = _safe_str(row.get("record_id"))
            if not rec_id:
                rec_id = _create_record_id(
                    _normalize_code(row.get("股票代號")),
                    _safe_str(row.get("推薦日期")) or _now_date_text(),
                    _safe_str(row.get("推薦時間")) or _now_time_text(),
                    _safe_str(row.get("推薦模式")),
                )
                row["record_id"] = rec_id

            new_ids.add(rec_id)
            doc_ref = records_ref.document(rec_id)
            doc_data = dict(row)
            doc_data["updated_at"] = now
            batch.set(doc_ref, doc_data, merge=True)

        for old_id in existing_ids - new_ids:
            batch.delete(records_ref.document(old_id))

        batch.commit()
        return True, "已同步寫入 Firestore"
    except Exception as e:
        return False, f"推薦紀錄 Firestore 寫入失敗：{e}"


def _normalize_godpick_record(row: dict[str, Any]) -> dict[str, Any]:
    rec_price = _safe_float(row.get("推薦價格"))
    latest_price = _safe_float(row.get("最新價"))
    stop_price = _safe_float(row.get("停損價"))
    target1 = _safe_float(row.get("賣出目標1"))
    target2 = _safe_float(row.get("賣出目標2"))

    pnl_amt = None
    pnl_pct = None
    if rec_price not in [None, 0] and latest_price is not None:
        pnl_amt = latest_price - rec_price
        pnl_pct = (pnl_amt / rec_price) * 100

    hit_stop = False
    if stop_price is not None and latest_price is not None and latest_price <= stop_price:
        hit_stop = True

    hit_target1 = False
    if target1 is not None and latest_price is not None and latest_price >= target1:
        hit_target1 = True

    hit_target2 = False
    if target2 is not None and latest_price is not None and latest_price >= target2:
        hit_target2 = True

    rec_date = _safe_str(row.get("推薦日期")) or _now_date_text()
    rec_time = _safe_str(row.get("推薦時間")) or _now_time_text()
    mode = _safe_str(row.get("推薦模式"))

    norm = {
        "record_id": _safe_str(row.get("record_id")) or _safe_str(row.get("rec_id")) or _create_record_id(
            _normalize_code(row.get("股票代號")), rec_date, rec_time, mode
        ),
        "股票代號": _normalize_code(row.get("股票代號")),
        "股票名稱": _safe_str(row.get("股票名稱")),
        "市場別": _safe_str(row.get("市場別")) or "上市",
        "類別": _normalize_category(row.get("類別")),
        "推薦模式": mode,
        "推薦等級": _safe_str(row.get("推薦等級")),
        "推薦總分": _safe_float(row.get("推薦總分")),
        "技術結構分數": _safe_float(row.get("技術結構分數")),
        "起漲前兆分數": _safe_float(row.get("起漲前兆分數")),
        "交易可行分數": _safe_float(row.get("交易可行分數")),
        "類股熱度分數": _safe_float(row.get("類股熱度分數")),
        "同類股領先幅度": _safe_float(row.get("同類股領先幅度")),
        "是否領先同類股": _safe_str(row.get("是否領先同類股")) in {"是", "True", "true", "1"},
        "推薦標籤": _safe_str(row.get("推薦標籤")),
        "推薦理由摘要": _safe_str(row.get("推薦理由摘要")),
        "推薦價格": rec_price,
        "停損價": stop_price,
        "賣出目標1": target1,
        "賣出目標2": target2,
        "推薦日期": rec_date,
        "推薦時間": rec_time,
        "建立時間": _safe_str(row.get("建立時間")) or _now_text(),
        "更新時間": _now_text(),
        "目前狀態": _safe_str(row.get("目前狀態")) or "觀察",
        "是否已實際買進": _safe_str(row.get("是否已實際買進")) in {"是", "True", "true", "1"},
        "實際買進價": _safe_float(row.get("實際買進價")),
        "實際賣出價": _safe_float(row.get("實際賣出價")),
        "實際報酬%": _safe_float(row.get("實際報酬%")),
        "最新價": latest_price,
        "最新更新時間": _safe_str(row.get("最新更新時間")),
        "損益金額": pnl_amt,
        "損益幅%": pnl_pct,
        "是否達停損": hit_stop if row.get("是否達停損") is None else (_safe_str(row.get("是否達停損")) in {"是", "True", "true", "1"}),
        "是否達目標1": hit_target1 if row.get("是否達目標1") is None else (_safe_str(row.get("是否達目標1")) in {"是", "True", "true", "1"}),
        "是否達目標2": hit_target2 if row.get("是否達目標2") is None else (_safe_str(row.get("是否達目標2")) in {"是", "True", "true", "1"}),
        "持有天數": _safe_float(row.get("持有天數")),
        "模式績效標籤": _safe_str(row.get("模式績效標籤")),
        "備註": _safe_str(row.get("備註")),
    }
    return _ensure_godpick_record_columns(pd.DataFrame([norm])).iloc[0].to_dict()


def _build_record_rows_from_rec_df(rec_df: pd.DataFrame, selected_codes: list[str]) -> list[dict[str, Any]]:
    if rec_df is None or rec_df.empty:
        return []

    work = rec_df[rec_df["股票代號"].astype(str).isin([str(x) for x in selected_codes])].copy()
    rows = []

    rec_date = _now_date_text()
    rec_time = _now_time_text()
    build_time = _now_text()

    for _, r in work.iterrows():
        code = _normalize_code(r.get("股票代號"))
        mode = _safe_str(r.get("推薦模式"))
        rows.append(
            {
                "record_id": _create_record_id(code, rec_date, rec_time, mode),
                "股票代號": code,
                "股票名稱": _safe_str(r.get("股票名稱")),
                "市場別": _safe_str(r.get("市場別")) or "上市",
                "類別": _normalize_category(r.get("類別")),
                "推薦模式": mode,
                "推薦等級": _safe_str(r.get("推薦等級")),
                "推薦總分": _safe_float(r.get("推薦總分")),
                "技術結構分數": _safe_float(r.get("技術結構分數")),
                "起漲前兆分數": _safe_float(r.get("起漲前兆分數")),
                "交易可行分數": _safe_float(r.get("交易可行分數")),
                "類股熱度分數": _safe_float(r.get("類股熱度分數")),
                "同類股領先幅度": _safe_float(r.get("同類股領先幅度")),
                "是否領先同類股": _safe_str(r.get("是否領先同類股")) in {"是", "True", "true", "1"},
                "推薦標籤": _safe_str(r.get("推薦標籤")),
                "推薦理由摘要": _safe_str(r.get("推薦理由摘要")),
                "推薦價格": _safe_float(r.get("最新價") if pd.notna(r.get("最新價")) else r.get("推薦買點_拉回")),
                "停損價": _safe_float(r.get("停損價")),
                "賣出目標1": _safe_float(r.get("賣出目標1")),
                "賣出目標2": _safe_float(r.get("賣出目標2")),
                "推薦日期": rec_date,
                "推薦時間": rec_time,
                "建立時間": build_time,
                "更新時間": build_time,
                "目前狀態": "觀察",
                "是否已實際買進": False,
                "實際買進價": None,
                "實際賣出價": None,
                "實際報酬%": None,
                "最新價": _safe_float(r.get("最新價")),
                "最新更新時間": "",
                "損益金額": None,
                "損益幅%": None,
                "是否達停損": False,
                "是否達目標1": False,
                "是否達目標2": False,
                "持有天數": None,
                "模式績效標籤": "",
                "備註": "",
            }
        )
    return rows



# =========================================================

# =========================================================
OFFICIAL_INDUSTRY_CODE_MAP = {
    "01": "水泥工業",
    "02": "食品工業",
    "03": "塑膠工業",
    "04": "紡織纖維",
    "05": "電機機械",
    "06": "電器電纜",
    "08": "玻璃陶瓷",
    "09": "造紙工業",
    "10": "鋼鐵工業",
    "11": "橡膠工業",
    "12": "汽車工業",
    "14": "建材營造",
    "15": "航運業",
    "16": "觀光餐旅",
    "17": "金融保險",
    "18": "貿易百貨",
    "19": "綜合",
    "20": "其他",
    "21": "化學工業",
    "22": "生技醫療",
    "23": "油電燃氣",
    "24": "半導體業",
    "25": "電腦及週邊設備業",
    "26": "光電業",
    "27": "通信網路業",
    "28": "電子零組件業",
    "29": "電子通路業",
    "30": "資訊服務業",
    "31": "其他電子業",
    "32": "文化創意業",
    "33": "農業科技業",
    "34": "綠能環保",
    "35": "數位雲端",
    "36": "運動休閒",
    "37": "居家生活",
}

# 股票主檔 / 分類修正持久化
# =========================================================
import re
import html as _html


def _master_cols() -> list[str]:
    return [
        "code", "name", "market",
        "official_industry_raw", "official_industry_raw_col", "official_industry",
        "theme_category", "category",
        "source", "source_api", "source_rank", "待修原因",
    ]


def _empty_master_df() -> pd.DataFrame:
    return pd.DataFrame(columns=_master_cols())


YAHOO_CATEGORY_ALIAS = {
    "半導體": "半導體業",
    "電腦週邊": "電腦及週邊設備業",
    "電腦周邊": "電腦及週邊設備業",
    "光電": "光電業",
    "通訊網路": "通信網路業",
    "電子零組件": "電子零組件業",
    "電子通路": "電子通路業",
    "資訊服務": "資訊服務業",
    "其他電子": "其他電子業",
    "生技醫療": "生技醫療",
    "油電燃氣": "油電燃氣",
    "建材營造": "建材營造",
    "航運": "航運業",
    "觀光餐旅": "觀光餐旅",
    "綠能環保": "綠能環保",
    "數位雲端": "數位雲端",
    "運動休閒": "運動休閒",
    "居家生活": "居家生活",
    "水泥": "水泥工業",
    "食品": "食品工業",
    "塑膠": "塑膠工業",
    "紡織纖維": "紡織纖維",
    "電機機械": "電機機械",
    "電器電纜": "電器電纜",
    "玻璃陶瓷": "玻璃陶瓷",
    "造紙": "造紙工業",
    "鋼鐵": "鋼鐵工業",
    "橡膠": "橡膠工業",
    "汽車": "汽車工業",
    "貿易百貨": "貿易百貨",
    "化學": "化學工業",
}

STOCK_CATEGORY_NAME_WHITELIST = {
    "1101": {"official": "水泥工業", "theme": "水泥工業"},
    "1102": {"official": "水泥工業", "theme": "水泥工業"},
    "1103": {"official": "水泥工業", "theme": "水泥工業"},
    "1104": {"official": "水泥工業", "theme": "水泥工業"},
    "1108": {"official": "水泥工業", "theme": "水泥工業"},
    "1109": {"official": "水泥工業", "theme": "水泥工業"},
    "1216": {"official": "食品工業", "theme": "食品民生"},
    "1301": {"official": "塑膠工業", "theme": "塑化"},
    "1303": {"official": "塑膠工業", "theme": "塑化"},
    "1326": {"official": "塑膠工業", "theme": "塑化"},
    "1402": {"official": "紡織纖維", "theme": "紡織製鞋"},
    "1409": {"official": "紡織纖維", "theme": "紡織製鞋"},
    "1410": {"official": "紡織纖維", "theme": "紡織製鞋"},
    "1414": {"official": "紡織纖維", "theme": "紡織製鞋"},
    "1434": {"official": "紡織纖維", "theme": "紡織製鞋"},
    "1605": {"official": "電器電纜", "theme": "電器電纜"},
    "1707": {"official": "化學工業", "theme": "化學工業"},
    "1710": {"official": "化學工業", "theme": "化學工業"},
    "1711": {"official": "化學工業", "theme": "化學工業"},
    "1712": {"official": "化學工業", "theme": "化學工業"},
    "1722": {"official": "生技醫療", "theme": "生技醫療"},
    "1802": {"official": "電機機械", "theme": "電機機械"},
    "1907": {"official": "造紙工業", "theme": "造紙工業"},
    "2002": {"official": "鋼鐵工業", "theme": "鋼鐵"},
    "2101": {"official": "橡膠工業", "theme": "橡膠工業"},
    "2201": {"official": "汽車工業", "theme": "汽車"},
    "2204": {"official": "汽車工業", "theme": "汽車"},
    "2603": {"official": "航運業", "theme": "航運"},
    "2609": {"official": "航運業", "theme": "航運"},
    "2615": {"official": "航運業", "theme": "航運"},
    "2801": {"official": "金融保險", "theme": "金融保險"},
    "2809": {"official": "金融保險", "theme": "金融保險"},
    "2812": {"official": "金融保險", "theme": "金融保險"},
    "2834": {"official": "金融保險", "theme": "金融保險"},
    "2880": {"official": "金融保險", "theme": "金控"},
    "2881": {"official": "金融保險", "theme": "金控"},
    "2882": {"official": "金融保險", "theme": "金控"},
    "2883": {"official": "金融保險", "theme": "金控"},
    "2884": {"official": "金融保險", "theme": "金控"},
    "2885": {"official": "金融保險", "theme": "金控"},
    "2886": {"official": "金融保險", "theme": "金控"},
    "2887": {"official": "金融保險", "theme": "金控"},
    "2888": {"official": "金融保險", "theme": "金控"},
    "2889": {"official": "金融保險", "theme": "金控"},
    "2890": {"official": "金融保險", "theme": "金控"},
    "2891": {"official": "金融保險", "theme": "金控"},
    "2892": {"official": "金融保險", "theme": "第一金控"},
    "5871": {"official": "金融保險", "theme": "金控"},
    "6005": {"official": "貿易百貨", "theme": "貿易百貨"},
}

NAME_THEME_HINTS = [
    ("水泥", "水泥工業"),
    ("紡織", "紡織製鞋"),
    ("成衣", "紡織製鞋"),
    ("製鞋", "紡織製鞋"),
    ("百貨", "貿易百貨"),
    ("航運", "航運"),
    ("海運", "航運"),
    ("金控", "金控"),
    ("銀行", "銀行"),
    ("保險", "保險"),
    ("證券", "證券"),
    ("水泥", "水泥工業"),
]

def _secondary_refine_theme(code: Any, name: Any, official: Any, current: Any) -> tuple[str, str, str]:
    code = _normalize_code(code)
    name = _safe_str(name)
    official = _official_industry_name(official)
    current = _safe_str(current)

    white = STOCK_CATEGORY_NAME_WHITELIST.get(code, {})
    if white:
        final_official = _safe_str(white.get("official")) or official
        final_theme = _safe_str(white.get("theme")) or current or _infer_category_from_name(name)
        if final_theme:
            return final_official or official, final_theme, "whitelist"

    if current and "其他" not in current:
        return official, current, ""

    if official in {"水泥工業", "食品工業", "塑膠工業", "紡織纖維", "電機機械", "電器電纜", "玻璃陶瓷", "造紙工業", "鋼鐵工業", "橡膠工業", "汽車工業", "建材營造", "航運業", "觀光餐旅", "金融保險", "貿易百貨", "化學工業", "生技醫療", "油電燃氣", "半導體業", "電腦及週邊設備業", "光電業", "通信網路業", "電子零組件業", "電子通路業", "資訊服務業", "其他電子業", "文化創意業", "農業科技業", "綠能環保", "數位雲端", "運動休閒", "居家生活"}:
        theme = _yahoo_industry_to_theme(official, name)
        if theme and "其他" not in theme:
            return official, theme, "official_refine"

    by_name = _infer_category_from_name(name)
    if by_name and "其他" not in by_name:
        return official, by_name, "name_rule"

    for kw, theme in NAME_THEME_HINTS:
        if kw in name:
            return official, theme, "name_hint"

    return official, current or "其他", ""



def _stock_master_config() -> dict[str, str]:
    return {
        "token": _safe_str(st.secrets.get("GITHUB_TOKEN", "")),
        "owner": _safe_str(st.secrets.get("GITHUB_REPO_OWNER", "cheng07021028")),
        "repo": _safe_str(st.secrets.get("GITHUB_REPO_NAME", "stock-app")),
        "branch": _safe_str(st.secrets.get("GITHUB_REPO_BRANCH", "main")) or "main",
        "master_path": _safe_str(st.secrets.get("STOCK_MASTER_GITHUB_PATH", "stock_master_cache.json")) or "stock_master_cache.json",
        "override_path": _safe_str(st.secrets.get("STOCK_CATEGORY_OVERRIDE_GITHUB_PATH", "stock_category_overrides.json")) or "stock_category_overrides.json",
    }


def _read_json_from_github(path: str) -> tuple[Any, str]:
    cfg = _stock_master_config()
    token = cfg["token"]
    if not token:
        return None, "未設定 GITHUB_TOKEN"
    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], path),
            headers=_github_headers(token),
            params={"ref": cfg["branch"]},
            timeout=20,
        )
        if resp.status_code == 404:
            return None, ""
        if resp.status_code != 200:
            return None, f"讀取 GitHub JSON 失敗：{resp.status_code} / {resp.text[:300]}"
        data = resp.json()
        content = data.get("content", "")
        if not content:
            return None, ""
        decoded = base64.b64decode(content).decode("utf-8")
        return json.loads(decoded), ""
    except Exception as e:
        return None, f"讀取 GitHub JSON 例外：{e}"


def _get_github_sha_by_path(path: str) -> tuple[str, str]:
    cfg = _stock_master_config()
    token = cfg["token"]
    if not token:
        return "", "缺少 GITHUB_TOKEN"
    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], path),
            headers=_github_headers(token),
            params={"ref": cfg["branch"]},
            timeout=20,
        )
        if resp.status_code == 200:
            return _safe_str(resp.json().get("sha")), ""
        if resp.status_code == 404:
            return "", ""
        return "", f"讀取 SHA 失敗：{resp.status_code} / {resp.text[:300]}"
    except Exception as e:
        return "", f"讀取 SHA 例外：{e}"


def _write_json_to_github(path: str, payload: Any, commit_message: str) -> tuple[bool, str]:
    cfg = _stock_master_config()
    token = cfg["token"]
    if not token:
        return False, "未設定 GITHUB_TOKEN"

    sha, err = _get_github_sha_by_path(path)
    if err:
        return False, err

    content_text = json.dumps(payload, ensure_ascii=False, indent=2)
    encoded_content = base64.b64encode(content_text.encode("utf-8")).decode("utf-8")
    body: dict[str, Any] = {
        "message": commit_message,
        "content": encoded_content,
        "branch": cfg["branch"],
    }
    if sha:
        body["sha"] = sha

    try:
        resp = requests.put(
            _github_contents_url(cfg["owner"], cfg["repo"], path),
            headers=_github_headers(token),
            json=body,
            timeout=30,
        )
        if resp.status_code in (200, 201):
            return True, f"已回寫 GitHub：{path}"
        return False, f"GitHub 寫入失敗：{resp.status_code} / {resp.text[:500]}"
    except Exception as e:
        return False, f"GitHub 寫入例外：{e}"


def _official_industry_name(raw_value: Any) -> str:
    raw = _safe_str(raw_value)
    if not raw:
        return ""

    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) == 1:
        digits = digits.zfill(2)

    code_map = {
        "01": "水泥工業",
        "02": "食品工業",
        "03": "塑膠工業",
        "04": "紡織纖維",
        "05": "電機機械",
        "06": "電器電纜",
        "08": "玻璃陶瓷",
        "09": "造紙工業",
        "10": "鋼鐵工業",
        "11": "橡膠工業",
        "12": "汽車工業",
        "14": "建材營造",
        "15": "航運業",
        "16": "觀光餐旅",
        "17": "金融保險",
        "18": "貿易百貨",
        "19": "綜合",
        "20": "其他",
        "21": "化學工業",
        "22": "生技醫療",
        "23": "油電燃氣",
        "24": "半導體業",
        "25": "電腦及週邊設備業",
        "26": "光電業",
        "27": "通信網路業",
        "28": "電子零組件業",
        "29": "電子通路業",
        "30": "資訊服務業",
        "31": "其他電子業",
        "32": "文化創意業",
        "33": "農業科技業",
        "34": "綠能環保",
        "35": "數位雲端",
        "36": "運動休閒",
        "37": "居家生活",
    }

    if digits and digits in code_map:
        return code_map[digits]

    raw = raw.replace("業別", "").strip()
    return raw


def _yahoo_industry_to_theme(industry: Any, name: Any) -> str:
    raw = _safe_str(industry)
    name = _safe_str(name)
    if not raw:
        return _infer_category_from_name(name)
    if "金融" in raw:
        if "金控" in name:
            return "金控"
        if "保險" in name or ("產險" in name) or ("人壽" in name):
            return "保險"
        if ("銀行" in name) or name.endswith("銀") or ("商銀" in name):
            return "銀行"
        if ("證" in name) or ("期" in name):
            return "證券"
        return "金融保險"
    cat = _infer_category_from_record(name, raw)
    return cat or raw or "其他_官方未知"


def _normalize_master_df(df: pd.DataFrame) -> pd.DataFrame:
    cols = _master_cols()
    if df is None or df.empty:
        return _empty_master_df()
    x = df.copy()
    for c in cols:
        if c not in x.columns:
            x[c] = ""
    x["code"] = x["code"].map(_normalize_code)
    x["name"] = x["name"].map(_safe_str)
    x["market"] = x["market"].map(_safe_str)
    x["official_industry_raw"] = x["official_industry_raw"].map(_safe_str)
    x["official_industry_raw_col"] = x["official_industry_raw_col"].map(_safe_str)
    x["official_industry"] = x["official_industry"].map(_official_industry_name)
    x["theme_category"] = x.apply(lambda r: _yahoo_industry_to_theme(r.get("official_industry"), r.get("name")), axis=1)
    x["category"] = x["theme_category"]
    x["source"] = x["source"].map(_safe_str)
    x["source_api"] = x["source_api"].map(_safe_str)
    x["source_rank"] = pd.to_numeric(x["source_rank"], errors="coerce").fillna(999).astype(int)
    x["待修原因"] = x["待修原因"].map(_safe_str)
    x = x[x["code"].astype(str).str.fullmatch(r"\d{4}")].copy()
    x = x.drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)

    refine_count = 0
    refine_sources = []
    for idx in x.index:
        final_official, final_theme, refine_src = _secondary_refine_theme(
            x.at[idx, "code"], x.at[idx, "name"], x.at[idx, "official_industry"], x.at[idx, "theme_category"]
        )
        if final_official and not _safe_str(x.at[idx, "official_industry"]):
            x.at[idx, "official_industry"] = final_official
        if final_theme:
            if _safe_str(x.at[idx, "theme_category"]) != final_theme or _safe_str(x.at[idx, "category"]) != final_theme:
                x.at[idx, "theme_category"] = final_theme
                x.at[idx, "category"] = final_theme
                refine_count += 1
            if refine_src and _safe_str(x.at[idx, "source"]) in {"yahoo_profile_primary", "twse_isin_base", "tpex_上櫃", "tpex_興櫃"}:
                refine_sources.append(refine_src)
        if _safe_str(x.at[idx, "official_industry"]) or (_safe_str(x.at[idx, "category"]) and "其他" not in _safe_str(x.at[idx, "category"])):
            x.at[idx, "待修原因"] = ""
        else:
            x.at[idx, "待修原因"] = _safe_str(x.at[idx, "待修原因"]) or "Yahoo / 官方產業待補"

    x.attrs["refine_count"] = refine_count
    x.attrs["refine_sources"] = refine_sources
    return x[cols].copy()


def _http_get_text(url: str, timeout: int = 30, verify: bool | None = None, headers: dict[str, str] | None = None) -> str:
    headers = headers or {"User-Agent": "Mozilla/5.0"}
    kwargs = {"timeout": timeout, "headers": headers}
    if verify is not None:
        kwargs["verify"] = verify
    try:
        resp = requests.get(url, **kwargs)
        resp.raise_for_status()
        resp.encoding = resp.encoding or "utf-8"
        return resp.text
    except Exception:
        if verify is not False:
            try:
                requests.packages.urllib3.disable_warnings()  # type: ignore[attr-defined]
            except Exception:
                pass
            resp = requests.get(url, timeout=timeout, headers=headers, verify=False)
            resp.raise_for_status()
            resp.encoding = resp.encoding or "utf-8"
            return resp.text
        raise


def _find_col(cols: list[str], keywords: list[str]) -> str:
    for c in cols:
        s = _safe_str(c)
        if all(k in s for k in keywords):
            return c
    for c in cols:
        s = _safe_str(c)
        if any(k in s for k in keywords):
            return c
    return ""


def _split_code_name(raw: Any) -> tuple[str, str]:
    txt = _safe_str(raw)
    if not txt:
        return "", ""
    m = re.match(r"^(\d{4,6})\s+(.+)$", txt)
    if m:
        return _normalize_code(m.group(1)), _safe_str(m.group(2))
    m = re.match(r"^(\d{4,6})(.+)$", txt)
    if m:
        return _normalize_code(m.group(1)), _safe_str(m.group(2))
    return _normalize_code(txt), ""


@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_twse_isin_base() -> tuple[pd.DataFrame, dict[str, Any]]:
    url = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2"
    diag = {"rows": 0, "official_hit": 0, "raw_cols": [], "source_api": "twse_isin_html", "error": ""}

    def _clean_cell(cell_html: str) -> str:
        txt = re.sub(r"<br\s*/?>", " ", cell_html, flags=re.I)
        txt = re.sub(r"<[^>]+>", "", txt)
        txt = html.unescape(txt)
        txt = txt.replace("\u3000", " ").replace("&nbsp;", " ")
        txt = re.sub(r"\s+", " ", txt).strip()
        return txt

    try:
        html_text = _http_get_text(url, timeout=40)
    except Exception as e:
        diag["error"] = f"{type(e).__name__}: {e}"
        return _empty_master_df(), diag

    rows = []
    tr_blocks = re.findall(r"<tr[^>]*>(.*?)</tr>", html_text, flags=re.I | re.S)
    if not tr_blocks:
        diag["error"] = "無法解析 TWSE ISIN HTML 列資料"
        return _empty_master_df(), diag

    diag["raw_cols"] = ["有價證券代號及名稱", "ISIN", "上市日", "市場別", "產業別", "CFI", "備註"]

    for tr in tr_blocks:
        cells_html = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, flags=re.I | re.S)
        cells = [_clean_cell(x) for x in cells_html]
        if len(cells) < 5:
            continue

        code, name = _split_code_name(cells[0])
        code = _normalize_code(code)
        if not re.fullmatch(r"\d{4}", code or ""):
            continue

        market = ""
        market_idx = -1
        for i, c in enumerate(cells):
            if "上市" in c or "上櫃" in c or "興櫃" in c:
                market = c
                market_idx = i
                break
        if market and "上市" not in market:
            continue
        if not market:
            market = "上市"

        official_raw = ""
        if market_idx >= 0 and market_idx + 1 < len(cells):
            official_raw = _safe_str(cells[market_idx + 1])

        if not official_raw:
            for c in cells[1:]:
                c2 = _safe_str(c)
                if any(k in c2 for k in ["工業", "業", "保險", "銀行", "證券", "其他電子", "半導體", "光電", "通信網路", "電子零組件", "電腦及週邊設備", "資訊服務", "貿易百貨", "生技醫療", "油電燃氣", "建材營造", "觀光餐旅", "航運"]):
                    official_raw = c2
                    break

        official = _official_industry_name(official_raw)
        theme = _yahoo_industry_to_theme(official, name)

        rows.append({
            "code": code,
            "name": name or code,
            "market": "上市",
            "official_industry_raw": official_raw,
            "official_industry_raw_col": "產業別",
            "official_industry": official,
            "theme_category": theme,
            "category": theme,
            "source": "twse_isin_base",
            "source_api": "twse_isin_html",
            "source_rank": 3,
            "待修原因": "" if official else "Yahoo / 官方產業待補",
        })

    out = _normalize_master_df(pd.DataFrame(rows))
    diag["rows"] = len(out)
    diag["official_hit"] = int(out["official_industry"].fillna("").astype(str).str.strip().ne("").sum()) if not out.empty else 0
    if diag["rows"] == 0 and not diag["error"]:
        diag["error"] = "TWSE ISIN HTML 已抓到，但未解析出上市 4 碼股票"
    return out, diag


@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_tpex_base(mode: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    url = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O" if mode == "上櫃" else "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_R"
    diag = {"rows": 0, "official_hit": 0, "raw_cols": [], "source_api": f"tpex_{mode}", "error": ""}
    try:
        text = _http_get_text(url, timeout=30)
        payload = json.loads(text)
        df = pd.DataFrame(payload)
    except Exception as e:
        diag["error"] = f"{type(e).__name__}: {e}"
        return _empty_master_df(), diag

    rows = []
    for _, r in df.iterrows():
        code = _normalize_code(r.get("SecuritiesCompanyCode"))
        name = _safe_str(r.get("CompanyAbbreviation"))
        if not re.fullmatch(r"\d{4}", code or ""):
            continue
        official_raw = _safe_str(r.get("SecuritiesIndustryCode"))
        official = _official_industry_name(official_raw)
        rows.append({
            "code": code,
            "name": name or code,
            "market": mode,
            "official_industry_raw": official_raw,
            "official_industry_raw_col": "SecuritiesIndustryCode",
            "official_industry": official,
            "theme_category": _yahoo_industry_to_theme(official, name),
            "category": _yahoo_industry_to_theme(official, name),
            "source": f"tpex_{mode}_base",
            "source_api": f"tpex_{mode}",
            "source_rank": 3,
            "待修原因": "" if official else "Yahoo / 官方產業待補",
        })
    out = _normalize_master_df(pd.DataFrame(rows))
    diag["rows"] = len(out)
    diag["official_hit"] = int(out["official_industry"].fillna("").astype(str).str.strip().ne("").sum()) if not out.empty else 0
    diag["raw_cols"] = list(df.columns)
    return out, diag



def _build_utils_name_aux() -> pd.DataFrame:
    dfs = []
    for market_arg in ["上市", "上櫃", "興櫃"]:
        try:
            df = get_all_code_name_map(market_arg)
        except Exception:
            df = pd.DataFrame()
        if df is None or df.empty:
            continue
        temp = df.copy().rename(columns={"證券代號": "code", "證券名稱": "name", "市場別": "market"})
        for c in ["code", "name", "market"]:
            if c not in temp.columns:
                temp[c] = ""
        temp["code"] = temp["code"].map(_normalize_code)
        temp["name"] = temp["name"].map(_safe_str)
        temp["market"] = temp["market"].map(_safe_str).replace("", market_arg)
        temp = temp[temp["code"].astype(str).str.fullmatch(r"\d{4}")].copy()
        dfs.append(temp[["code", "name", "market"]])
    if not dfs:
        return pd.DataFrame(columns=["code", "name_aux", "market_aux"])
    out = pd.concat(dfs, ignore_index=True).drop_duplicates("code", keep="first").reset_index(drop=True)
    out = out.rename(columns={"name": "name_aux", "market": "market_aux"})
    return out


def _apply_aux_name_market(master_df: pd.DataFrame) -> pd.DataFrame:
    if master_df is None or master_df.empty:
        return _empty_master_df()
    aux = _build_utils_name_aux()
    if aux.empty:
        return master_df
    work = master_df.merge(aux, on="code", how="left")
    for idx in work.index:
        if not _safe_str(work.at[idx, "name"]) and _safe_str(work.at[idx, "name_aux"]):
            work.at[idx, "name"] = _safe_str(work.at[idx, "name_aux"])
        if _safe_str(work.at[idx, "market"]) not in {"上市", "上櫃", "興櫃"} and _safe_str(work.at[idx, "market_aux"]) in {"上市", "上櫃", "興櫃"}:
            work.at[idx, "market"] = _safe_str(work.at[idx, "market_aux"])
    return _normalize_master_df(work.drop(columns=["name_aux", "market_aux"], errors="ignore"))

def _build_formal_base_master() -> tuple[pd.DataFrame, dict[str, Any]]:
    twse_df, twse_info = _fetch_twse_isin_base()
    tpex_o_df, tpex_o_info = _fetch_tpex_base("上櫃")
    tpex_r_df, tpex_r_info = _fetch_tpex_base("興櫃")
    base = pd.concat([twse_df, tpex_o_df, tpex_r_df], ignore_index=True) if any(not x.empty for x in [twse_df, tpex_o_df, tpex_r_df]) else _empty_master_df()
    base = _normalize_master_df(base).sort_values(["code"]).drop_duplicates("code", keep="first").reset_index(drop=True)
    info = {
        "rows": len(base),
        "twse_info": twse_info,
        "tpex_o_info": tpex_o_info,
        "tpex_r_info": tpex_r_info,
    }
    return base, info


def _extract_text_lines_from_html(html_text: str) -> list[str]:
    text = re.sub(r"(?is)<script.*?>.*?</script>", " ", html_text)
    text = re.sub(r"(?is)<style.*?>.*?</style>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", "\n", text)
    text = _html.unescape(text)
    lines = []
    for line in text.splitlines():
        line = re.sub(r"\s+", " ", line).strip()
        if line:
            lines.append(line)
    return lines


def _pick_after(lines: list[str], labels: list[str]) -> str:
    for i, line in enumerate(lines[:-1]):
        if line in labels:
            return _safe_str(lines[i + 1])
    return ""


@st.cache_data(ttl=86400, show_spinner=False)
def _fetch_yahoo_profile_fill(code: str, market: str) -> dict[str, str]:
    code = _normalize_code(code)
    market = _safe_str(market)
    if not re.fullmatch(r"\d{4}", code or ""):
        return {}

    suffixes = []
    if market == "上市":
        suffixes = ["TW", "TWO"]
    elif market in {"上櫃", "興櫃"}:
        suffixes = ["TWO", "TW"]
    else:
        suffixes = ["TW", "TWO"]

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
        "Referer": "https://tw.stock.yahoo.com/",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

    def _regex_pick(text: str, label: str) -> str:
        patterns = [
            rf"{label}\s*[：:]?\s*([\u4e00-\u9fffA-Za-z0-9\-（）()、／/．\. ]{{1,80}})",
            rf'\"{label}\"\s*[:：]\s*\"([^\"]{{1,80}})\"',
        ]
        for pat in patterns:
            m = re.search(pat, text, flags=re.S)
            if m:
                val = _safe_str(m.group(1))
                if val and val != label:
                    return val
        return ""

    for sfx in suffixes:
        url = f"https://tw.stock.yahoo.com/quote/{code}.{sfx}/profile"
        try:
            html_text = _http_get_text(url, timeout=20, headers=headers)
            lines = _extract_text_lines_from_html(html_text)

            industry = _pick_after(lines, ["產業類別"]) or _regex_pick(html_text, "產業類別")
            market_found = _pick_after(lines, ["市場別"]) or _regex_pick(html_text, "市場別")
            name = _pick_after(lines, ["公司名稱"]) or _regex_pick(html_text, "公司名稱")

            if not name:
                m = re.search(r"#\s*([^\n#]+)", html_text)
                if m:
                    cand = _safe_str(m.group(1))
                    if cand and "(" not in cand and "Yahoo" not in cand:
                        name = cand

            if industry or market_found or name:
                return {
                    "code": code,
                    "name": name,
                    "market": market_found,
                    "industry": industry,
                    "source_api": f"yahoo_profile_{sfx}",
                }
        except Exception:
            continue
    return {}

def _apply_yahoo_primary_categories(base_df: pd.DataFrame, workers: int = 12) -> tuple[pd.DataFrame, dict[str, Any]]:
    if base_df is None or base_df.empty:
        return _empty_master_df(), {"rows": 0, "hit": 0, "error": "", "processed": 0, "secondary_refine": 0}

    work = _normalize_master_df(base_df)
    target_df = work[
        work["official_industry"].fillna("").astype(str).str.strip().eq("")
        | work["category"].fillna("").astype(str).str.contains("其他", na=False)
    ].copy()
    rows = target_df.to_dict(orient="records")
    results: dict[str, dict[str, str]] = {}
    errors = []

    if rows:
        with ThreadPoolExecutor(max_workers=max(4, min(workers, 16))) as ex:
            fut_map = {ex.submit(_fetch_yahoo_profile_fill, r["code"], r["market"]): r["code"] for r in rows}
            for fut in as_completed(fut_map):
                code = fut_map[fut]
                try:
                    info = fut.result()
                    if info:
                        results[code] = info
                except Exception as e:
                    errors.append(f"{code}:{type(e).__name__}")

    hit = 0
    secondary_refine = 0
    for idx in work.index:
        code = _normalize_code(work.at[idx, "code"])
        info = results.get(code, {})
        yahoo_industry = _safe_str(info.get("industry"))
        yahoo_market = _safe_str(info.get("market"))
        yahoo_name = _safe_str(info.get("name"))

        if yahoo_name:
            work.at[idx, "name"] = yahoo_name
        if yahoo_market in {"上市", "上櫃", "興櫃"}:
            work.at[idx, "market"] = yahoo_market

        if yahoo_industry:
            official = _official_industry_name(yahoo_industry)
            theme = _yahoo_industry_to_theme(official or yahoo_industry, work.at[idx, "name"])
            final_official, final_theme, refine_src = _secondary_refine_theme(code, work.at[idx, "name"], official or yahoo_industry, theme)
            work.at[idx, "official_industry_raw"] = yahoo_industry
            work.at[idx, "official_industry_raw_col"] = "Yahoo_產業類別"
            work.at[idx, "official_industry"] = final_official or official or yahoo_industry
            work.at[idx, "theme_category"] = final_theme or theme
            work.at[idx, "category"] = final_theme or theme
            work.at[idx, "source"] = "yahoo_profile_primary"
            work.at[idx, "source_api"] = _safe_str(info.get("source_api")) or "yahoo_profile"
            work.at[idx, "source_rank"] = 1
            work.at[idx, "待修原因"] = ""
            hit += 1
            if refine_src:
                secondary_refine += 1

    # pending second pass
    for idx in work.index:
        if _safe_str(work.at[idx, "待修原因"]) or "其他" in _safe_str(work.at[idx, "category"]):
            final_official, final_theme, refine_src = _secondary_refine_theme(
                work.at[idx, "code"], work.at[idx, "name"], work.at[idx, "official_industry"], work.at[idx, "category"]
            )
            if final_official and not _safe_str(work.at[idx, "official_industry"]):
                work.at[idx, "official_industry"] = final_official
            if final_theme and "其他" not in final_theme:
                work.at[idx, "theme_category"] = final_theme
                work.at[idx, "category"] = final_theme
                if _safe_str(work.at[idx, "source"]) in {"twse_isin_base", "tpex_上櫃", "tpex_興櫃", ""}:
                    work.at[idx, "source"] = "secondary_refine"
                    work.at[idx, "source_api"] = refine_src or "secondary_refine"
                    work.at[idx, "source_rank"] = 2
                work.at[idx, "待修原因"] = ""
                secondary_refine += 1

    work = _normalize_master_df(work)
    info = {
        "rows": hit,
        "hit": hit,
        "processed": len(rows),
        "secondary_refine": secondary_refine,
        "error": "；".join(errors[:20]),
    }
    return work, info



@st.cache_data(ttl=900, show_spinner=False)
def _load_stock_master_cache_from_repo() -> pd.DataFrame:
    cfg = _stock_master_config()
    payload, _ = _read_json_from_github(cfg["master_path"])
    if not isinstance(payload, list):
        return _empty_master_df()
    return _normalize_master_df(pd.DataFrame(payload))


@st.cache_data(ttl=300, show_spinner=False)
def _load_stock_category_override_map() -> dict[str, dict[str, str]]:
    cfg = _stock_master_config()
    payload, _ = _read_json_from_github(cfg["override_path"])
    if not isinstance(payload, dict):
        return {}
    out = {}
    for code, item in payload.items():
        norm_code = _normalize_code(code)
        if not norm_code:
            continue
        if not isinstance(item, dict):
            item = {"category": item}
        out[norm_code] = {
            "code": norm_code,
            "name": _safe_str(item.get("name")),
            "market": _safe_str(item.get("market")),
            "category": _canonical_category(item.get("category")),
            "updated_at": _safe_str(item.get("updated_at")),
        }
    return out


def _apply_master_overrides(master_df: pd.DataFrame) -> pd.DataFrame:
    work = _normalize_master_df(master_df)
    override_map = _load_stock_category_override_map()
    if not override_map:
        return work
    for code, item in override_map.items():
        matched = work["code"].astype(str) == str(code)
        if matched.any():
            idx = work[matched].index[0]
            if _safe_str(item.get("name")):
                work.at[idx, "name"] = _safe_str(item.get("name"))
            if _safe_str(item.get("market")) in {"上市", "上櫃", "興櫃"}:
                work.at[idx, "market"] = _safe_str(item.get("market"))
            if _safe_str(item.get("category")):
                cat = _canonical_category(item.get("category"))
                work.at[idx, "theme_category"] = cat
                work.at[idx, "category"] = cat
                work.at[idx, "source"] = "override"
                work.at[idx, "source_api"] = "github_override"
                work.at[idx, "source_rank"] = 0
                work.at[idx, "待修原因"] = ""
    return _normalize_master_df(work)


def _save_master_cache_to_repo(master_df: pd.DataFrame) -> tuple[bool, str]:
    cfg = _stock_master_config()
    work = _normalize_master_df(master_df)
    payload = work.sort_values(["market", "code"]).to_dict(orient="records")
    return _write_json_to_github(cfg["master_path"], payload, f"refresh stock master cache at {_now_text()}")


def _save_category_override(code: str, name: str, market: str, category: str) -> tuple[bool, str]:
    cfg = _stock_master_config()
    code = _normalize_code(code)
    if not code:
        return False, "股票代號不可空白"
    payload, _ = _read_json_from_github(cfg["override_path"])
    if not isinstance(payload, dict):
        payload = {}
    payload[code] = {
        "code": code,
        "name": _safe_str(name),
        "market": _safe_str(market) or "上市",
        "category": _canonical_category(category) or _infer_category_from_record(name, category),
        "updated_at": _now_text(),
    }
    ok, msg = _write_json_to_github(cfg["override_path"], payload, f"update stock category override {code} at {_now_text()}")
    if ok:
        try:
            _load_stock_category_override_map.clear()
        except Exception:
            pass
    return ok, msg

def _build_master_diagnostics(base_info=None, yahoo_info=None, merged=None) -> list[str]:
    base_info = base_info if isinstance(base_info, dict) else {}
    twse_info = base_info.get("twse_info", {}) if isinstance(base_info.get("twse_info", {}), dict) else {}
    tpex_o_info = base_info.get("tpex_o_info", {}) if isinstance(base_info.get("tpex_o_info", {}), dict) else {}
    tpex_r_info = base_info.get("tpex_r_info", {}) if isinstance(base_info.get("tpex_r_info", {}), dict) else {}
    yahoo_info = yahoo_info if isinstance(yahoo_info, dict) else {}
    merged_df = merged if isinstance(merged, pd.DataFrame) else _empty_master_df()

    def _n(v, default=0):
        try:
            return int(v)
        except Exception:
            return default

    logs = []
    logs.append(f"正式底座(TWSE ISIN + TPEX)：{_n(base_info.get('rows'))} 筆")
    logs.append(f"TWSE ISIN：{_n(twse_info.get('rows'))} 筆 / 正式產業有值 {_n(twse_info.get('official_hit'))} 筆 / API: {_safe_str(twse_info.get('source_api')) or '-'}")
    if _safe_str(twse_info.get("error")):
        logs.append(f"TWSE ISIN 錯誤：{_safe_str(twse_info.get('error'))}")
    logs.append(f"TPEX-上櫃：{_n(tpex_o_info.get('rows'))} 筆 / 正式產業有值 {_n(tpex_o_info.get('official_hit'))} 筆 / API: {_safe_str(tpex_o_info.get('source_api')) or '-'}")
    if _safe_str(tpex_o_info.get("error")):
        logs.append(f"TPEX-上櫃 錯誤：{_safe_str(tpex_o_info.get('error'))}")
    logs.append(f"TPEX-興櫃：{_n(tpex_r_info.get('rows'))} 筆 / 正式產業有值 {_n(tpex_r_info.get('official_hit'))} 筆 / API: {_safe_str(tpex_r_info.get('source_api')) or '-'}")
    if _safe_str(tpex_r_info.get("error")):
        logs.append(f"TPEX-興櫃 錯誤：{_safe_str(tpex_r_info.get('error'))}")
    logs.append(f"Yahoo 主來源補值：{_n(yahoo_info.get('rows'))} 筆 / 處理 {_n(yahoo_info.get('processed'))} 筆 / 二次細分 {_n(yahoo_info.get('secondary_refine'))} 筆")
    if _safe_str(yahoo_info.get("error")):
        logs.append(f"Yahoo 補值錯誤：{_safe_str(yahoo_info.get('error'))}")
    if not merged_df.empty:
        hit = int(merged_df["official_industry"].fillna("").astype(str).str.strip().ne("").sum()) if "official_industry" in merged_df.columns else 0
        pending = int(merged_df["待修原因"].fillna("").astype(str).str.strip().ne("").sum()) if "待修原因" in merged_df.columns else 0
        logs.append(f"合併後：{len(merged_df)} 筆 / 正式產業有值 {hit} 筆 / 待修 {pending} 筆")
        if "source" in merged_df.columns:
            vc = merged_df["source"].fillna("").astype(str).value_counts()
            if not vc.empty:
                src_line = "來源統計：" + " / ".join([f"{k}:{int(v)}" for k, v in vc.items()])
                logs.append(src_line)
    else:
        logs.append("合併後：0 筆 / 正式產業有值 0 筆 / 待修 0 筆")
    return logs


def _build_live_master_df() -> tuple[pd.DataFrame, list[str], dict[str, Any], dict[str, Any]]:
    base_df, base_info = _build_formal_base_master()
    base_df = _apply_aux_name_market(base_df)
    yahoo_df, yahoo_info = _apply_yahoo_primary_categories(base_df)
    merged = _apply_master_overrides(yahoo_df)
    logs = _build_master_diagnostics(base_info, yahoo_info, merged)
    return merged, logs, base_info, yahoo_info


def _refresh_stock_master_now() -> tuple[pd.DataFrame, list[str]]:
    try:
        _fetch_yahoo_profile_fill.clear()
    except Exception:
        pass
    try:
        _build_live_master_df.clear()
    except Exception:
        pass
    try:
        _load_master_df.clear()
    except Exception:
        pass

    fresh_df, logs, base_info, yahoo_info = _build_live_master_df()
    if fresh_df.empty:
        return fresh_df, logs + ["主檔更新失敗：正式股票清單為空。"]
    ok, msg = _save_master_cache_to_repo(fresh_df)
    logs.append(msg)
    if ok:
        try:
            _load_stock_master_cache_from_repo.clear()
        except Exception:
            pass
    pending = int(fresh_df["待修原因"].fillna("").astype(str).str.strip().ne("").sum()) if "待修原因" in fresh_df.columns else 0
    if pending > 0:
        logs.append(f"待修清單仍有 {pending} 檔，建議優先在主檔搜尋中心檢查 source / 官方產業 / 類別。")
    st.session_state[_k("master_diag_logs")] = logs
    return fresh_df, logs


def _search_master_df(master_df: pd.DataFrame, keyword: str, market_filter: str, category_filter: str) -> pd.DataFrame:
    cols = _master_cols()
    if master_df is None or master_df.empty:
        return pd.DataFrame(columns=cols)
    work = _normalize_master_df(master_df)
    kw = _safe_str(keyword)
    market_filter = _safe_str(market_filter)
    category_filter = _safe_str(category_filter)
    if market_filter and market_filter != "全部":
        work = work[work["market"].astype(str) == market_filter].copy()
    if category_filter and category_filter != "全部":
        work = work[(work["category"].astype(str) == category_filter) | (work["official_industry"].astype(str) == category_filter)].copy()
    if kw:
        work = work[
            work["code"].astype(str).str.contains(kw, case=False, na=False)
            | work["name"].astype(str).str.contains(kw, case=False, na=False)
            | work["official_industry"].astype(str).str.contains(kw, case=False, na=False)
            | work["theme_category"].astype(str).str.contains(kw, case=False, na=False)
            | work["category"].astype(str).str.contains(kw, case=False, na=False)
        ].copy()
    return work.sort_values(["market", "source_rank", "code"]).reset_index(drop=True)


def _render_stock_master_center(master_df: pd.DataFrame, watchlist_map: dict[str, list[dict[str, str]]], all_categories: list[str]) -> pd.DataFrame:
    render_pro_section("股票主檔搜尋 / 更新中心")
    with st.expander("展開股票主檔搜尋 / 更新中心", expanded=True):
        c1, c2, c3, c4 = st.columns([3, 2, 2, 2])

        with c1:
            master_kw = st.text_input("搜尋股票代號 / 名稱 / 正式產業別 / 主題類別", key=_k("master_kw"))
        with c2:
            master_market = st.selectbox("市場別篩選", ["全部", "上市", "上櫃", "興櫃"], key=_k("master_market"))
        with c3:
            cat_opts = ["全部"] + sorted({x for x in all_categories if _safe_str(x)})
            master_cat = st.selectbox("類別 / 產業篩選", cat_opts, key=_k("master_cat"))
        with c4:
            st.write("")
            st.write("")
            refresh_master_btn = st.button("更新股票主檔（獨立模組）", key=_k("refresh_master_btn"), use_container_width=True, type="primary")

        if refresh_master_btn:
            with st.spinner("更新股票主檔中（stock_master_service）..."):
                new_master_df, logs = refresh_stock_master()
                if not new_master_df.empty:
                    master_df = new_master_df.copy()
                    st.success("股票主檔已更新，已由 stock_master_service 套用主檔更新。")
                else:
                    st.error("股票主檔更新失敗，仍保留目前版本。")
                for line in logs:
                    st.caption(line)

        logs = get_stock_master_diagnostics(master_df)
        if logs:
            st.caption("主檔來源：stock_master_service.py")
        with st.expander("官方主檔診斷訊息", expanded=False):
            for line in logs:
                st.write(f"- {line}")

        total_count = len(master_df) if isinstance(master_df, pd.DataFrame) else 0
        official_hit = int(master_df["official_industry"].fillna("").astype(str).str.strip().ne("").sum()) if isinstance(master_df, pd.DataFrame) and not master_df.empty else 0
        theme_hit = int(master_df["theme_category"].fillna("").astype(str).str.strip().ne("").sum()) if isinstance(master_df, pd.DataFrame) and not master_df.empty else 0
        need_fix = int(master_df["待修原因"].fillna("").astype(str).str.strip().ne("").sum()) if isinstance(master_df, pd.DataFrame) and not master_df.empty else 0

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("主檔總筆數", f"{total_count:,}")
        k2.metric("正式產業有值", f"{official_hit:,}")
        k3.metric("主題有值", f"{theme_hit:,}")
        k4.metric("需作修正主檔欄位", f"{need_fix:,}")
        st.caption(f"目前顯示 {total_count:,} / {total_count:,} 筆")

        found_df = search_stock_master(
            master_df,
            keyword=master_kw,
            market_filter=master_market,
            category_filter=master_cat,
        )

        show_cols = [
            "code","name","market","official_industry_raw","official_industry_raw_col",
            "official_industry","theme_category","category","source","source_api","source_rank","待修原因"
        ]
        show_cols = [c for c in show_cols if c in found_df.columns]
        st.dataframe(
            found_df[show_cols] if not found_df.empty else pd.DataFrame(columns=show_cols),
            use_container_width=True,
            height=520,
        )

        if not found_df.empty:
            selected_display = st.selectbox(
                "選擇股票進行修正 / 加入自選股",
                options=[
                    f"{r['code']} {r['name']} | {r['market']} | {r['category']}"
                    for _, r in found_df.head(500).iterrows()
                ],
                key=_k("master_selected_display"),
            )
            selected_code = _normalize_code(selected_display.split(" ")[0]) if selected_display else ""
            row = found_df[found_df["code"].astype(str) == selected_code].head(1)
            if not row.empty:
                r = row.iloc[0]
                st.markdown("### 主檔修正 / 加入自選股")
                c5, c6, c7, c8 = st.columns([1.1, 1.4, 1.2, 1.2])
                with c5:
                    st.text_input("股票代號", value=_safe_str(r.get("code")), disabled=True, key=_k("master_fix_code"))
                with c6:
                    st.text_input("股票名稱", value=_safe_str(r.get("name")), disabled=True, key=_k("master_fix_name"))
                with c7:
                    fix_market = st.selectbox("修正市場別", ["上市", "上櫃", "興櫃"], index=max(0, ["上市","上櫃","興櫃"].index(_safe_str(r.get("market"))) if _safe_str(r.get("market")) in ["上市","上櫃","興櫃"] else 0), key=_k("master_fix_market"))
                with c8:
                    fix_category = st.text_input("修正操作類別", value=_safe_str(r.get("category")), key=_k("master_fix_category"))

                c9, c10 = st.columns(2)
                with c9:
                    group_name = st.selectbox("加入群組", options=list(watchlist_map.keys()) if watchlist_map else ["自選"], key=_k("master_fix_group"))
                    if st.button("加入自選股", key=_k("master_add_watchlist"), use_container_width=True):
                        ok, msg = _append_stock_to_watchlist(group_name, _safe_str(r.get("code")), _safe_str(r.get("name")), fix_market)
                        if ok:
                            st.success(msg)
                        else:
                            st.error(msg)
                with c10:
                    if st.button("儲存主檔修正", key=_k("master_save_override"), use_container_width=True):
                        ok, msg = _save_category_override(_safe_str(r.get("code")), _safe_str(r.get("name")), fix_market, fix_category)
                        if ok:
                            st.success(msg)
                        else:
                            st.error(msg)

    return master_df

