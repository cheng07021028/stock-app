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
from stock_master_service import load_stock_master

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
# 股票主檔 / 分類修正持久化
# =========================================================

# 官方產業代碼映射（TWSE / TPEX 常用）
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
    if digits in OFFICIAL_INDUSTRY_CODE_MAP:
        return OFFICIAL_INDUSTRY_CODE_MAP[digits]
    return raw.replace("業別", "").replace("工業", "工業").strip()


def _theme_from_official(official_industry: Any, name: Any) -> str:
    official = _official_industry_name(official_industry)
    by_name = _infer_category_from_name(_safe_str(name))
    if by_name != "其他":
        return by_name
    if not official:
        return "其他_官方未知"
    mapping = {
        "水泥工業": "水泥工業",
        "食品工業": "食品民生",
        "塑膠工業": "塑化",
        "紡織纖維": "紡織製鞋",
        "電機機械": "電機機械",
        "電器電纜": "電器電纜",
        "玻璃陶瓷": "玻璃陶瓷",
        "造紙工業": "造紙工業",
        "鋼鐵工業": "鋼鐵",
        "橡膠工業": "橡膠工業",
        "汽車工業": "汽車",
        "建材營造": "營建資產",
        "航運業": "航運",
        "觀光餐旅": "航空觀光",
        "金融保險": "金融保險",
        "貿易百貨": "貿易百貨",
        "綜合": "綜合",
        "其他": "其他_主題未映射",
        "化學工業": "塑化",
        "生技醫療": "生技醫療",
        "油電燃氣": "油電燃氣",
        "半導體業": "半導體業",
        "電腦及週邊設備業": "電腦及週邊設備業",
        "光電業": "光電業",
        "通信網路業": "通信網路業",
        "電子零組件業": "電子零組件業",
        "電子通路業": "電子通路業",
        "資訊服務業": "資訊服務業",
        "其他電子業": "其他電子業",
        "文化創意業": "文化創意業",
        "農業科技業": "農業科技業",
        "綠能環保": "綠能環保",
        "數位雲端": "數位雲端",
        "運動休閒": "運動休閒",
        "居家生活": "居家生活",
    }
    return mapping.get(official, official)


def _normalize_master_columns(df: pd.DataFrame, market_label: str, code_col: str, name_col: str, industry_col: str, source_api: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    if df is None or df.empty:
        empty = pd.DataFrame(columns=["code","name","market","official_industry_raw","official_industry_raw_col","official_industry","theme_category","category","source","source_api","source_rank","待修原因"])
        return empty, {"rows": 0, "official_hit": 0, "raw_cols": [], "source_api": source_api}
    work = df.copy()
    for c in [code_col, name_col, industry_col]:
        if c not in work.columns:
            work[c] = ""
    work = work.rename(columns={code_col: "code", name_col: "name", industry_col: "official_industry_raw"})
    work["code"] = work["code"].map(_normalize_code)
    work["name"] = work["name"].map(_safe_str)
    work["market"] = market_label
    work["official_industry_raw_col"] = industry_col
    work["official_industry"] = work["official_industry_raw"].map(_official_industry_name)
    work["theme_category"] = work.apply(lambda r: _theme_from_official(r.get("official_industry"), r.get("name")), axis=1)
    work["category"] = work["theme_category"]
    work["source"] = f"official_{market_label}"
    work["source_api"] = source_api
    work["source_rank"] = 1
    work["待修原因"] = work["official_industry"].map(lambda x: "" if _safe_str(x) else "官方產業未抓到")
    work = work[work["code"] != ""].drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)
    info = {
        "rows": len(work),
        "official_hit": int(work["official_industry"].fillna("").astype(str).str.strip().ne("").sum()),
        "raw_cols": list(df.columns),
        "source_api": source_api,
    }
    return work[["code","name","market","official_industry_raw","official_industry_raw_col","official_industry","theme_category","category","source","source_api","source_rank","待修原因"]].copy(), info


@st.cache_data(ttl=1800, show_spinner=False)
def _fetch_twse_master() -> tuple[pd.DataFrame, dict[str, Any]]:
    url = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        df = pd.DataFrame(payload)
        return _normalize_master_columns(df, "上市", "公司代號", "公司簡稱", "產業別", "twse_openapi")
    except Exception:
        empty = pd.DataFrame(columns=["code","name","market","official_industry_raw","official_industry_raw_col","official_industry","theme_category","category","source","source_api","source_rank","待修原因"])
        return empty, {"rows": 0, "official_hit": 0, "raw_cols": [], "source_api": "twse_openapi"}


@st.cache_data(ttl=1800, show_spinner=False)
def _fetch_tpex_master(mode: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    if mode == "上櫃":
        url = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"
    else:
        url = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_R"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        payload = resp.json()
        df = pd.DataFrame(payload)
        return _normalize_master_columns(df, mode, "SecuritiesCompanyCode", "CompanyAbbreviation", "SecuritiesIndustryCode", f"tpex_{mode}")
    except Exception:
        empty = pd.DataFrame(columns=["code","name","market","official_industry_raw","official_industry_raw_col","official_industry","theme_category","category","source","source_api","source_rank","待修原因"])
        return empty, {"rows": 0, "official_hit": 0, "raw_cols": [], "source_api": f"tpex_{mode}"}


@st.cache_data(ttl=1800, show_spinner=False)
def _fetch_twse_isin_fill_map() -> dict[str, str]:
    url = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2"
    out: dict[str, str] = {}
    try:
        tables = pd.read_html(url)
    except Exception:
        return out
    for tb in tables:
        tmp = tb.copy()
        tmp.columns = [str(c) for c in tmp.columns]
        cols = set(tmp.columns)
        if not ({"有價證券代號", "產業別"} <= cols):
            continue
        for _, r in tmp.iterrows():
            code = _normalize_code(r.get("有價證券代號"))
            industry = _official_industry_name(r.get("產業別"))
            if code and industry:
                out[code] = industry
    return out


def _build_utils_master_fallback() -> tuple[pd.DataFrame, dict[str, Any]]:
    dfs = []
    for market_arg in ["上市", "上櫃", "興櫃"]:
        try:
            df = get_all_code_name_map(market_arg)
        except Exception:
            df = pd.DataFrame()
        if df is None or df.empty:
            continue
        temp = df.copy().rename(columns={"證券代號":"code", "證券名稱":"name", "市場別":"market"})
        for c in ["code","name","market"]:
            if c not in temp.columns:
                temp[c] = ""
        temp["code"] = temp["code"].map(_normalize_code)
        temp["name"] = temp["name"].map(_safe_str)
        temp["market"] = temp["market"].map(_safe_str).replace("", market_arg)
        temp["official_industry_raw"] = ""
        temp["official_industry_raw_col"] = ""
        temp["official_industry"] = ""
        temp["theme_category"] = temp["name"].map(_infer_category_from_name).replace("其他", "其他_官方未知")
        temp["category"] = temp["theme_category"]
        temp["source"] = "utils_fallback"
        temp["source_api"] = "utils_all"
        temp["source_rank"] = 9
        temp["待修原因"] = "官方產業未抓到"
        dfs.append(temp[["code","name","market","official_industry_raw","official_industry_raw_col","official_industry","theme_category","category","source","source_api","source_rank","待修原因"]])
    if not dfs:
        empty = pd.DataFrame(columns=["code","name","market","official_industry_raw","official_industry_raw_col","official_industry","theme_category","category","source","source_api","source_rank","待修原因"])
        return empty, {"rows": 0, "official_hit": 0, "raw_cols": [], "source_api": "utils_all"}
    out = pd.concat(dfs, ignore_index=True).drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)
    return out, {"rows": len(out), "official_hit": 0, "raw_cols": list(out.columns), "source_api": "utils_all"}


@st.cache_data(ttl=900, show_spinner=False)
def _load_stock_master_cache_from_repo() -> pd.DataFrame:
    cfg = _stock_master_config()
    payload, _ = _read_json_from_github(cfg["master_path"])
    cols = ["code","name","market","official_industry_raw","official_industry_raw_col","official_industry","theme_category","category","source","source_api","source_rank","待修原因"]
    if not isinstance(payload, list):
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(payload)
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    df["code"] = df["code"].map(_normalize_code)
    df["name"] = df["name"].map(_safe_str)
    df["market"] = df["market"].map(_safe_str).replace("", "上市")
    df["official_industry"] = df["official_industry"].map(_official_industry_name)
    df["theme_category"] = df.apply(lambda r: _theme_from_official(r.get("official_industry"), r.get("name")), axis=1)
    df["category"] = df["theme_category"]
    return df[df["code"] != ""].drop_duplicates(subset=["code"], keep="first")[cols].reset_index(drop=True)


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


def _merge_master_sources(*dfs: pd.DataFrame) -> pd.DataFrame:
    cols = ["code","name","market","official_industry_raw","official_industry_raw_col","official_industry","theme_category","category","source","source_api","source_rank","待修原因"]
    items = []
    for df in dfs:
        if isinstance(df, pd.DataFrame) and not df.empty:
            tmp = df.copy()
            for c in cols:
                if c not in tmp.columns:
                    tmp[c] = ""
            items.append(tmp[cols])
    if not items:
        return pd.DataFrame(columns=cols)
    merged = pd.concat(items, ignore_index=True)
    merged["source_rank"] = pd.to_numeric(merged["source_rank"], errors="coerce").fillna(999)
    merged["official_hit"] = merged["official_industry"].fillna("").astype(str).str.strip().ne("").astype(int)
    merged = merged.sort_values(["code", "official_hit", "source_rank"], ascending=[True, False, True])
    merged = merged.drop_duplicates(subset=["code"], keep="first").drop(columns=["official_hit"]).reset_index(drop=True)
    return merged


def _apply_twse_isin_fill(master_df: pd.DataFrame) -> pd.DataFrame:
    if master_df is None or master_df.empty:
        return master_df
    fill_map = _fetch_twse_isin_fill_map()
    if not fill_map:
        return master_df
    work = master_df.copy()
    mask = (work["market"].astype(str) == "上市") & (work["official_industry"].fillna("").astype(str).str.strip() == "")
    for idx in work[mask].index:
        code = _normalize_code(work.at[idx, "code"])
        fill = fill_map.get(code, "")
        if fill:
            work.at[idx, "official_industry_raw"] = fill
            work.at[idx, "official_industry_raw_col"] = "TWSE_ISIN_產業別"
            work.at[idx, "official_industry"] = fill
            work.at[idx, "theme_category"] = _theme_from_official(fill, work.at[idx, "name"])
            work.at[idx, "category"] = work.at[idx, "theme_category"]
            work.at[idx, "source"] = "twse_isin_fill"
            work.at[idx, "source_api"] = "twse_isin"
            work.at[idx, "source_rank"] = 2
            work.at[idx, "待修原因"] = ""
    return work


def _apply_master_overrides(master_df: pd.DataFrame) -> pd.DataFrame:
    if master_df is None or master_df.empty:
        master_df = pd.DataFrame(columns=["code","name","market","official_industry_raw","official_industry_raw_col","official_industry","theme_category","category","source","source_api","source_rank","待修原因"])
    work = master_df.copy()
    repo_df = _load_stock_master_cache_from_repo()
    work = _merge_master_sources(work, repo_df)
    override_map = _load_stock_category_override_map()
    if override_map:
        for code, item in override_map.items():
            matched = work["code"].astype(str) == str(code)
            if matched.any():
                idx = work[matched].index[0]
                if _safe_str(item.get("name")):
                    work.at[idx, "name"] = _safe_str(item.get("name"))
                if _safe_str(item.get("market")):
                    work.at[idx, "market"] = _safe_str(item.get("market"))
                if _safe_str(item.get("category")):
                    work.at[idx, "theme_category"] = _canonical_category(item.get("category"))
                    work.at[idx, "category"] = _canonical_category(item.get("category"))
                    work.at[idx, "source"] = "override"
                    work.at[idx, "source_api"] = "github_override"
                    work.at[idx, "source_rank"] = 0
                    work.at[idx, "待修原因"] = ""
    work = work[work["code"] != ""].drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)
    return work


def _save_master_cache_to_repo(master_df: pd.DataFrame) -> tuple[bool, str]:
    cfg = _stock_master_config()
    cols = ["code","name","market","official_industry_raw","official_industry_raw_col","official_industry","theme_category","category","source","source_api","source_rank","待修原因"]
    work = master_df.copy() if isinstance(master_df, pd.DataFrame) else pd.DataFrame(columns=cols)
    for c in cols:
        if c not in work.columns:
            work[c] = ""
    work = work[work["code"].map(_normalize_code) != ""].copy()
    work["code"] = work["code"].map(_normalize_code)
    work["name"] = work["name"].map(_safe_str)
    work["market"] = work["market"].map(_safe_str)
    payload = work[cols].drop_duplicates(subset=["code"], keep="first").sort_values(["code"]).to_dict(orient="records")
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
        "category": _canonical_category(category) or _infer_category_from_name(_safe_str(name)),
        "updated_at": _now_text(),
    }
    ok, msg = _write_json_to_github(cfg["override_path"], payload, f"update stock category override {code} at {_now_text()}")
    if ok:
        try:
            _load_stock_category_override_map.clear()
        except Exception:
            pass
    return ok, msg


def _build_master_diagnostics(twse_info=None, tpex_o_info=None, tpex_r_info=None, utils_info=None, merged=None) -> list[str]:
    twse_info = twse_info if isinstance(twse_info, dict) else {}
    tpex_o_info = tpex_o_info if isinstance(tpex_o_info, dict) else {}
    tpex_r_info = tpex_r_info if isinstance(tpex_r_info, dict) else {}
    utils_info = utils_info if isinstance(utils_info, dict) else {}
    merged_df = merged if isinstance(merged, pd.DataFrame) else pd.DataFrame()

    def _n(v, default=0):
        try:
            return int(v)
        except Exception:
            return default

    logs = []
    logs.append(f"TWSE：{_n(twse_info.get('rows'))} 筆 / 正式產業有值 {_n(twse_info.get('official_hit'))} 筆 / API: {_safe_str(twse_info.get('source_api')) or '-'}")
    if twse_info.get("raw_cols"):
        logs.append("TWSE 欄位：" + ", ".join([str(x) for x in list(twse_info.get("raw_cols", []))[:20]]))
    logs.append(f"TPEX-上櫃：{_n(tpex_o_info.get('rows'))} 筆 / 正式產業有值 {_n(tpex_o_info.get('official_hit'))} 筆 / API: {_safe_str(tpex_o_info.get('source_api')) or '-'}")
    logs.append(f"TPEX-興櫃：{_n(tpex_r_info.get('rows'))} 筆 / 正式產業有值 {_n(tpex_r_info.get('official_hit'))} 筆 / API: {_safe_str(tpex_r_info.get('source_api')) or '-'}")
    logs.append(f"utils fallback：{_n(utils_info.get('rows'))} 筆 / API: {_safe_str(utils_info.get('source_api')) or '-'}")
    if not merged_df.empty and "official_industry" in merged_df.columns:
        hit = int(merged_df["official_industry"].fillna("").astype(str).str.strip().ne("").sum())
        logs.append(f"合併後：{len(merged_df)} 筆 / 正式產業有值 {hit} 筆")
    else:
        logs.append("合併後：0 筆 / 正式產業有值 0 筆")
    return logs


def _refresh_stock_master_now() -> tuple[pd.DataFrame, list[str]]:
    try:
        _load_master_df.clear()
    except Exception:
        pass
    fresh_df = _load_master_df()
    logs = list(st.session_state.get(_k("master_diag_logs"), []))
    if fresh_df.empty:
        return fresh_df, logs + ["主檔更新失敗：官方主檔與 fallback 皆無資料"]
    ok, msg = _save_master_cache_to_repo(fresh_df)
    logs.append(msg)
    if ok:
        try:
            _load_stock_master_cache_from_repo.clear()
        except Exception:
            pass
    return fresh_df, logs


def _search_master_df(master_df: pd.DataFrame, keyword: str, market_filter: str, category_filter: str) -> pd.DataFrame:
    cols = ["code","name","market","official_industry_raw","official_industry_raw_col","official_industry","theme_category","category","source","source_api","source_rank","待修原因"]
    if master_df is None or master_df.empty:
        return pd.DataFrame(columns=cols)
    work = master_df.copy()
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
    return work.sort_values(["market","source_rank","code"]).reset_index(drop=True)


def _render_stock_master_center(
    master_df: pd.DataFrame,
    watchlist_map: dict[str, list[dict[str, str]]],
    all_categories: list[str],
) -> pd.DataFrame:
    return master_df


@st.cache_data(ttl=1800, show_spinner=False)
def _load_master_df() -> pd.DataFrame:
    twse_df, twse_info = _fetch_twse_master()
    tpex_o_df, tpex_o_info = _fetch_tpex_master("上櫃")
    tpex_r_df, tpex_r_info = _fetch_tpex_master("興櫃")
    utils_df, utils_info = _build_utils_master_fallback()
    merged = _merge_master_sources(twse_df, tpex_o_df, tpex_r_df, utils_df)
    merged = _apply_twse_isin_fill(merged)
    merged = _apply_master_overrides(merged)
    st.session_state[_k("master_diag_logs")] = _build_master_diagnostics(twse_info, tpex_o_info, tpex_r_info, utils_info, merged)
    return merged

# =========================================================
# 主檔 / universe helpers
# =========================================================

# =========================================================
# 主檔 / universe helpers
# =========================================================
def _load_watchlist_map() -> dict[str, list[dict[str, str]]]:
    raw = st.session_state.get("watchlist_data")
    if not isinstance(raw, dict) or not raw:
        try:
            raw = get_normalized_watchlist()
        except Exception:
            raw = {}

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
                    if isinstance(item, dict):
                        code = _normalize_code(item.get("code"))
                        name = _safe_str(item.get("name")) or code
                        market = _safe_str(item.get("market")) or "上市"
                        category = _infer_category_from_record(name, item.get("category"))
                    else:
                        code = _normalize_code(item)
                        name = code
                        market = "上市"
                        category = ""

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


@st.cache_data(ttl=300, show_spinner=False)
def _load_master_df_fallback_only() -> pd.DataFrame:
    try:
        repo_df = load_stock_master()
    except Exception:
        repo_df = pd.DataFrame()

    if repo_df is None or repo_df.empty:
        repo_df = _load_stock_master_cache_from_repo()

    if repo_df is None or repo_df.empty:
        return pd.DataFrame(columns=["code", "name", "market", "category"])

    work = repo_df.copy()

    if "code" not in work.columns:
        work["code"] = ""
    if "name" not in work.columns:
        work["name"] = ""
    if "market" not in work.columns:
        work["market"] = "上市"
    if "category" not in work.columns:
        if "theme_category" in work.columns:
            work["category"] = work["theme_category"]
        else:
            work["category"] = ""

    work["code"] = work["code"].map(_normalize_code)
    work["name"] = work["name"].map(_safe_str)
    work["market"] = work["market"].map(_safe_str).replace("", "上市")
    work["category"] = work.apply(
        lambda r: _infer_category_from_record(r.get("name"), r.get("category")),
        axis=1,
    )

    work = _apply_master_overrides(work)

    return (
        work[work["code"] != ""]
        .drop_duplicates(subset=["code"], keep="first")
        .reset_index(drop=True)
    )

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
            final_category = _normalize_category(row.get("category")) or manual_category or _infer_category_from_record(final_name, manual_category)
            return final_name, final_market, final_category

    final_name = manual_name or code
    final_market = manual_market or "上市"
    final_category = manual_category or _infer_category_from_record(final_name, manual_category)
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


def _build_universe_from_market(
    master_df: pd.DataFrame,
    market_mode: str,
    limit_count: Any,
    selected_categories: list[str],
) -> list[dict[str, str]]:
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
                cat = _infer_category_from_record(name, item.get("category"))
                if cat:
                    cats.add(cat)

    return sorted(list(cats))


def _append_stock_to_watchlist(group_name: str, code: str, name: str, market: str, category: str):
    group_name = _safe_str(group_name)
    code = _normalize_code(code)
    name = _safe_str(name) or code
    market = _safe_str(market) or "上市"
    category = _canonical_category(category) or _infer_category_from_record(name, category)

    if not group_name:
        return False, "群組不可空白"
    if not code:
        return False, "股票代號不可空白"

    raw = st.session_state.get("watchlist_data")
    if not isinstance(raw, dict) or not raw:
        try:
            raw = get_normalized_watchlist()
        except Exception:
            raw = {}

    if group_name not in raw or not isinstance(raw[group_name], list):
        raw[group_name] = []

    for item in raw[group_name]:
        if isinstance(item, dict) and _normalize_code(item.get("code")) == code:
            return False, f"{code} 已存在於 {group_name}"

    row = {"code": code, "name": name, "market": market}
    if category:
        row["category"] = category

    raw[group_name].append(row)
    ok = _force_write_watchlist_dual(raw)
    if ok:
        return True, f"已加入 {group_name}：{code} {name}"
    return False, _safe_str(st.session_state.get(_k("status_msg"), "寫入失敗"))


def _append_multiple_stocks_to_watchlist(group_name: str, rows: list[dict[str, str]]) -> tuple[int, list[str]]:
    group_name = _safe_str(group_name)
    if not group_name:
        return 0, ["請先選擇群組。"]

    raw = st.session_state.get("watchlist_data")
    if not isinstance(raw, dict) or not raw:
        try:
            raw = get_normalized_watchlist()
        except Exception:
            raw = {}

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

        item = {"code": code, "name": name, "market": market}
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


def _create_watchlist_group(group_name: str) -> tuple[bool, str]:
    group_name = _safe_str(group_name)
    if not group_name:
        return False, "群組名稱不可空白"

    raw = st.session_state.get("watchlist_data")
    if not isinstance(raw, dict) or raw is None:
        try:
            raw = get_normalized_watchlist()
        except Exception:
            raw = {}

    if not isinstance(raw, dict):
        raw = {}

    if group_name in raw:
        return False, f"群組已存在：{group_name}"

    raw[group_name] = []
    ok = _force_write_watchlist_dual(raw)
    if ok:
        return True, f"已新增群組：{group_name}"
    return False, _safe_str(st.session_state.get(_k("status_msg"), "新增群組失敗"))


def _append_godpick_records(record_rows: list[dict[str, Any]]) -> tuple[int, list[str]]:
    if not record_rows:
        return 0, ["沒有可寫入的推薦紀錄。"]

    try:
        old_records, read_msg = _read_godpick_records_from_github()
        if read_msg:
            old_records = []

        old_df = _ensure_godpick_record_columns(pd.DataFrame(old_records))
        new_df = _ensure_godpick_record_columns(pd.DataFrame([_normalize_godpick_record(x) for x in record_rows]))

        before_count = len(old_df)
        merged_df = _append_records_dedup_by_business_key(old_df, new_df)
        after_count = len(merged_df)
        added_count = max(after_count - before_count, 0)

        merged_records = merged_df.to_dict(orient="records")

        ok_github, msg_github = _write_godpick_records_to_github(merged_records)
        ok_firestore, msg_firestore = _write_godpick_records_to_firestore(merged_records)

        st.session_state[_k("last_record_write_detail")] = [
            f"GitHub: {'成功' if ok_github else '失敗'} | {msg_github}",
            f"Firestore: {'成功' if ok_firestore else '失敗'} | {msg_firestore}",
            f"本次寫入筆數: {added_count}",
            f"合併後總筆數: {after_count}",
        ]

        msgs = []
        msgs.append(msg_github if ok_github else f"GitHub 失敗：{msg_github}")
        msgs.append(msg_firestore if ok_firestore else f"Firestore 失敗：{msg_firestore}")

        if ok_github or ok_firestore:
            return added_count, msgs

        return 0, msgs

    except Exception as e:
        st.session_state[_k("last_record_write_detail")] = [f"例外：{e}"]
        return 0, [f"寫入 8_股神推薦紀錄失敗：{e}"]


# =========================================================
# 歷史資料 / 指標
# =========================================================
def _prepare_history_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    temp = df.copy()
    if "日期" not in temp.columns:
        possible_date = [c for c in temp.columns if str(c).lower() in {"date", "日期"}]
        if possible_date:
            temp = temp.rename(columns={possible_date[0]: "日期"})
        else:
            return pd.DataFrame()

    temp["日期"] = pd.to_datetime(temp["日期"], errors="coerce")
    temp = temp.dropna(subset=["日期"]).sort_values("日期").reset_index(drop=True)

    rename_map = {}
    for c in temp.columns:
        cs = str(c).lower()
        if cs == "open":
            rename_map[c] = "開盤價"
        elif cs == "high":
            rename_map[c] = "最高價"
        elif cs == "low":
            rename_map[c] = "最低價"
        elif cs == "close":
            rename_map[c] = "收盤價"
        elif cs == "volume":
            rename_map[c] = "成交股數"
    temp = temp.rename(columns=rename_map)

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

    low_9 = low.rolling(9).min()
    high_9 = high.rolling(9).max()
    rsv = (close - low_9) / (high_9 - low_9).replace(0, pd.NA) * 100
    temp["K"] = rsv.ewm(alpha=1 / 3, adjust=False).mean()
    temp["D"] = temp["K"].ewm(alpha=1 / 3, adjust=False).mean()
    temp["J"] = 3 * temp["K"] - 2 * temp["D"]

    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    temp["DIF"] = ema12 - ema26
    temp["DEA"] = temp["DIF"].ewm(span=9, adjust=False).mean()
    temp["MACD_HIST"] = temp["DIF"] - temp["DEA"]

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
    temp["RET120"] = close.pct_change(120) * 100
    temp["UP_DAY"] = (close > close.shift(1)).astype(float)
    temp["MA20_SLOPE"] = temp["MA20"].diff(3)
    temp["MA60_SLOPE"] = temp["MA60"].diff(3)

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
                try:
                    df = get_history_data(code=stock_no, start_date=start_date, end_date=end_date)
                except Exception:
                    df = pd.DataFrame()
        except Exception:
            df = pd.DataFrame()

        df = _prepare_history_df(df)
        if not df.empty:
            return df, (mk or market_type or "未知")

    return pd.DataFrame(), (_safe_str(market_type) or "未知")


# =========================================================
# 計分
# =========================================================
def _build_prelaunch_scores(df: pd.DataFrame, signal_snapshot: dict, sr_snapshot: dict, radar: dict) -> dict[str, Any]:
    if df is None or df.empty:
        return {
            "起漲前兆分數": 0.0,
            "均線轉強分": 0.0,
            "量能啟動分": 0.0,
            "突破準備分": 0.0,
            "動能翻多分": 0.0,
            "支撐防守分": 0.0,
        }

    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) >= 2 else last

    close_now = _safe_float(last.get("收盤價"))
    ma5 = _safe_float(last.get("MA5"))
    ma10 = _safe_float(last.get("MA10"))
    ma20 = _safe_float(last.get("MA20"))
    ma60 = _safe_float(last.get("MA60"))
    ma20_slope = _safe_float(last.get("MA20_SLOPE"), 0) or 0
    vol5 = _safe_float(last.get("VOL5"))
    vol20 = _safe_float(last.get("VOL20"))
    ret5 = _safe_float(last.get("RET5"), 0) or 0
    k_now = _safe_float(last.get("K"))
    d_now = _safe_float(last.get("D"))
    k_prev = _safe_float(prev.get("K"))
    d_prev = _safe_float(prev.get("D"))
    hist_now = _safe_float(last.get("MACD_HIST"))
    hist_prev = _safe_float(prev.get("MACD_HIST"))
    res20 = _safe_float(sr_snapshot.get("res_20"))
    sup20 = _safe_float(sr_snapshot.get("sup_20"))

    trend_score = 0.0
    if close_now is not None and ma20 is not None and close_now >= ma20:
        trend_score += 25
    if close_now is not None and ma60 is not None and close_now >= ma60:
        trend_score += 18
    if ma5 is not None and ma10 is not None and ma5 >= ma10:
        trend_score += 18
    if ma20_slope > 0:
        trend_score += 22
    trend_score = _score_clip(trend_score)

    volume_score = 0.0
    if vol5 not in [None, 0] and vol20 not in [None, 0]:
        ratio = vol5 / vol20
        if ratio >= 1.8:
            volume_score = 85
        elif ratio >= 1.4:
            volume_score = 72
        elif ratio >= 1.1:
            volume_score = 60
        elif ratio >= 0.9:
            volume_score = 45
        else:
            volume_score = 25

    breakout_score = 0.0
    if close_now is not None and res20 not in [None, 0]:
        dist = ((res20 - close_now) / res20) * 100
        if 0 <= dist <= 2:
            breakout_score = 90
        elif 2 < dist <= 5:
            breakout_score = 72
        elif 5 < dist <= 8:
            breakout_score = 55
        elif dist < 0:
            breakout_score = 60
        else:
            breakout_score = 30

    momentum_score = 0.0
    if k_prev is not None and d_prev is not None and k_now is not None and d_now is not None:
        if k_prev <= d_prev and k_now > d_now:
            momentum_score += 45
        elif k_now > d_now:
            momentum_score += 28
    if hist_now is not None:
        if hist_prev is not None and hist_prev <= 0 < hist_now:
            momentum_score += 35
        elif hist_now > 0:
            momentum_score += 20
    radar_m = _safe_float(radar.get("momentum"), 50) or 50
    momentum_score += radar_m * 0.2
    momentum_score = _score_clip(momentum_score)

    support_score = 0.0
    if close_now is not None and sup20 not in [None, 0]:
        dist_sup = ((close_now - sup20) / sup20) * 100
        if 0 <= dist_sup <= 2:
            support_score = 85
        elif 2 < dist_sup <= 5:
            support_score = 70
        elif 5 < dist_sup <= 8:
            support_score = 55
        elif dist_sup < 0:
            support_score = 20
        else:
            support_score = 40

    if ret5 > 12:
        breakout_score -= 15
    if ret5 > 20:
        breakout_score -= 25

    total = _avg_safe([trend_score, volume_score, breakout_score, momentum_score, support_score], 0)
    return {
        "起漲前兆分數": _score_clip(total),
        "均線轉強分": _score_clip(trend_score),
        "量能啟動分": _score_clip(volume_score),
        "突破準備分": _score_clip(breakout_score),
        "動能翻多分": _score_clip(momentum_score),
        "支撐防守分": _score_clip(support_score),
    }


def _build_risk_filter(df: pd.DataFrame, signal_snapshot: dict, sr_snapshot: dict, strictness: str) -> dict[str, Any]:
    if df is None or df.empty:
        return {"是否通過風險過濾": False, "風險分數": 0.0, "淘汰原因": "無歷史資料"}

    last = df.iloc[-1]
    close_now = _safe_float(last.get("收盤價"))
    ma20 = _safe_float(last.get("MA20"))
    ma60 = _safe_float(last.get("MA60"))
    atr14 = _safe_float(last.get("ATR14"))
    vol20 = _safe_float(last.get("VOL20"))
    ret20 = _safe_float(last.get("RET20"), 0) or 0
    pressure_dist = None
    res20 = _safe_float(sr_snapshot.get("res_20"))
    if close_now not in [None] and res20 not in [None, 0]:
        pressure_dist = ((res20 - close_now) / res20) * 100

    rules = {
        "寬鬆": {"min_days": 60, "min_vol20": 300000, "max_atr_pct": 11.0, "max_ret20": 35.0},
        "標準": {"min_days": 90, "min_vol20": 800000, "max_atr_pct": 8.5, "max_ret20": 28.0},
        "嚴格": {"min_days": 120, "min_vol20": 1200000, "max_atr_pct": 6.5, "max_ret20": 22.0},
    }
    cfg = rules.get(_safe_str(strictness), rules["標準"])

    reasons = []
    risk_score = 100.0

    if len(df) < cfg["min_days"]:
        reasons.append(f"歷史資料不足{cfg['min_days']}天")
        risk_score -= 30
    if vol20 not in [None] and vol20 < cfg["min_vol20"]:
        reasons.append("量能不足")
        risk_score -= 22
    if close_now not in [None] and atr14 not in [None]:
        atr_pct = atr14 / close_now * 100 if close_now != 0 else 999
        if atr_pct > cfg["max_atr_pct"]:
            reasons.append("波動過大")
            risk_score -= 18
    if close_now not in [None] and ma20 not in [None] and ma60 not in [None]:
        if close_now < ma20 and close_now < ma60:
            reasons.append("中期結構偏弱")
            risk_score -= 20
    if ret20 > cfg["max_ret20"]:
        reasons.append("近20日漲幅過大")
        risk_score -= 16

    if pressure_dist is not None and pressure_dist < 0:
        risk_score -= 4
    elif pressure_dist is not None and pressure_dist > 10:
        risk_score -= 8

    signal_score = _safe_float(signal_snapshot.get("score"), 0) or 0
    risk_score += max(min(signal_score * 1.8, 12), -12)
    risk_score = _score_clip(risk_score)

    passed = len(reasons) == 0 or risk_score >= 55
    return {
        "是否通過風險過濾": passed,
        "風險分數": risk_score,
        "淘汰原因": "；".join(reasons) if reasons else "",
    }


def _build_trade_feasibility(df: pd.DataFrame, sr_snapshot: dict, signal_snapshot: dict) -> dict[str, Any]:
    if df is None or df.empty:
        return {
            "交易可行分數": 0.0,
            "追價風險分數": 0.0,
            "拉回買點分數": 0.0,
            "突破買點分數": 0.0,
            "風險報酬評級": "—",
        }

    last = df.iloc[-1]
    close_now = _safe_float(last.get("收盤價"), 0) or 0
    atr14 = _safe_float(last.get("ATR14"), 0) or max(close_now * 0.03, 1.0)
    ma20 = _safe_float(last.get("MA20"))
    res20 = _safe_float(sr_snapshot.get("res_20"))
    sup20 = _safe_float(sr_snapshot.get("sup_20"))

    pullback_buy = ma20 if ma20 is not None else (sup20 if sup20 is not None else close_now)
    breakout_buy = res20 if res20 is not None else close_now
    stop_price = sup20 if sup20 is not None else max(close_now - atr14, 0)
    target_1 = res20 if res20 is not None and res20 > close_now else close_now + atr14 * 1.5
    target_2 = target_1 + atr14 * 1.2

    def _rr(entry: float, stop: float, target: float) -> float:
        risk = entry - stop
        reward = target - entry
        if risk <= 0:
            return 0.0
        return reward / risk

    rr_pullback = _rr(pullback_buy, stop_price, target_1) if pullback_buy and stop_price is not None and target_1 else 0.0
    rr_breakout = _rr(breakout_buy, stop_price, target_2) if breakout_buy and stop_price is not None and target_2 else 0.0

    pullback_score = 25 + min(rr_pullback * 28, 45)
    breakout_score = 25 + min(rr_breakout * 22, 40)

    chase_risk = 0.0
    if ma20 not in [None, 0] and close_now not in [None]:
        bias = ((close_now - ma20) / ma20) * 100
        if bias >= 12:
            chase_risk = 88
        elif bias >= 8:
            chase_risk = 72
        elif bias >= 5:
            chase_risk = 58
        else:
            chase_risk = 35

    signal_score = _safe_float(signal_snapshot.get("score"), 0) or 0
    feasibility = _avg_safe(
        [_score_clip(pullback_score), _score_clip(breakout_score), _score_clip(100 - chase_risk), 50 + signal_score * 5],
        0,
    )

    if feasibility >= 80:
        rr_grade = "A"
    elif feasibility >= 68:
        rr_grade = "B"
    elif feasibility >= 55:
        rr_grade = "C"
    else:
        rr_grade = "D"

    return {
        "交易可行分數": _score_clip(feasibility),
        "追價風險分數": _score_clip(chase_risk),
        "拉回買點分數": _score_clip(pullback_score),
        "突破買點分數": _score_clip(breakout_score),
        "風險報酬評級": rr_grade,
    }


def _build_mode_score(
    mode: str,
    technical_score: float,
    prelaunch_score: float,
    category_heat_score: float,
    factor_score: float,
    trade_score: float,
    leader_advantage: float,
) -> tuple[float, str]:
    mode = _safe_str(mode)

    if mode == "飆股模式":
        total = prelaunch_score * 0.35 + technical_score * 0.25 + category_heat_score * 0.20 + factor_score * 0.10 + trade_score * 0.10
        tag = "突破前夜 / 起漲優先"
    elif mode == "波段模式":
        total = technical_score * 0.30 + category_heat_score * 0.25 + factor_score * 0.20 + trade_score * 0.15 + prelaunch_score * 0.10
        tag = "趨勢延續 / 波段優先"
    elif mode == "領頭羊模式":
        total = leader_advantage * 0.30 + category_heat_score * 0.25 + technical_score * 0.20 + prelaunch_score * 0.15 + factor_score * 0.10
        tag = "類股領先 / 龍頭優先"
    else:
        total = technical_score * 0.30 + prelaunch_score * 0.20 + category_heat_score * 0.20 + factor_score * 0.15 + trade_score * 0.15
        tag = "綜合推薦"

    return _score_clip(total), tag


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

    revenue_proxy = _score_clip(25 + (_safe_float(ret20, 0) or 0) * 0.9 + (_safe_float(ret60, 0) or 0) * 0.35 + radar_momentum * 0.30 + radar_volume * 0.20)
    profit_proxy = _score_clip(30 + signal_score * 6 + radar_trend * 0.28 + radar_structure * 0.22 + (_safe_float(ret60, 0) or 0) * 0.35)

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
    inst_proxy = _score_clip(20 + up_days_5 * 10 + signal_score * 5 + radar_momentum * 0.25 + radar_volume * 0.20)

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
def _analyze_stock_bundle(stock_no: str, stock_name: str, market_type: str, start_dt: date, end_dt: date, risk_strictness: str) -> dict[str, Any]:
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
    prelaunch = _build_prelaunch_scores(hist_df, signal_snapshot, sr_snapshot, radar)
    risk_filter = _build_risk_filter(hist_df, signal_snapshot, sr_snapshot, risk_strictness)
    trade_feasibility = _build_trade_feasibility(hist_df, sr_snapshot, signal_snapshot)

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

    technical_score = _score_clip(
        (radar_avg * 0.55)
        + ((_safe_float(signal_snapshot.get("score"), 0) or 0) * 7.5)
        + ((_safe_float(period_pct, 0) or 0) * 0.18)
    )

    return {
        "used_market": used_market,
        "signal_snapshot": signal_snapshot,
        "sr_snapshot": sr_snapshot,
        "radar": radar,
        "auto_factor": auto_factor,
        "trade_plan": trade_plan,
        "prelaunch": prelaunch,
        "risk_filter": risk_filter,
        "trade_feasibility": trade_feasibility,
        "close_now": close_now,
        "period_pct": period_pct,
        "pressure_dist": pressure_dist,
        "support_dist": support_dist,
        "radar_avg": radar_avg,
        "technical_score": technical_score,
    }


def _analyze_one_stock_for_recommend(
    item: dict[str, str],
    master_df: pd.DataFrame,
    start_dt: date,
    end_dt: date,
    min_signal_score: float,
    clean_categories: list[str],
    mode: str,
    risk_strictness: str,
    min_prelaunch_score: float,
    min_trade_score: float,
):
    code = _normalize_code(item.get("code"))
    manual_name = _safe_str(item.get("name"))
    manual_market = _safe_str(item.get("market"))
    manual_category = _normalize_category(item.get("category"))

    if not code:
        return None

    stock_name, market_type, category = _find_name_market_category(code, manual_name, manual_market, manual_category, master_df)

    if clean_categories and category not in clean_categories:
        return None

    bundle = _analyze_stock_bundle(
        stock_no=code,
        stock_name=stock_name,
        market_type=market_type,
        start_dt=start_dt,
        end_dt=end_dt,
        risk_strictness=risk_strictness,
    )
    if not bundle:
        return None

    signal_score = _safe_float(bundle["signal_snapshot"].get("score"), 0) or 0
    if signal_score < min_signal_score:
        return None

    risk_pass = bool(bundle["risk_filter"].get("是否通過風險過濾", False))
    if not risk_pass:
        return None

    prelaunch_score = _safe_float(bundle["prelaunch"].get("起漲前兆分數"), 0) or 0
    if prelaunch_score < min_prelaunch_score:
        return None

    trade_score = _safe_float(bundle["trade_feasibility"].get("交易可行分數"), 0) or 0
    if trade_score < min_trade_score:
        return None

    auto_factor_total = _safe_float(bundle["auto_factor"].get("auto_factor_total"), 0) or 0
    technical_score = _safe_float(bundle.get("technical_score"), 0) or 0

    base_composite = _score_clip(technical_score * 0.40 + auto_factor_total * 0.32 + prelaunch_score * 0.18 + trade_score * 0.10)

    return {
        "股票代號": code,
        "股票名稱": stock_name,
        "市場別": bundle["used_market"],
        "類別": category or _infer_category_from_record(stock_name, category),
        "最新價": bundle["close_now"],
        "區間漲跌幅%": bundle["period_pct"],
        "訊號分數": signal_score,
        "雷達均分": bundle["radar_avg"],
        "技術結構分數": technical_score,
        "起漲前兆分數": prelaunch_score,
        "交易可行分數": trade_score,
        "追價風險分數": _safe_float(bundle["trade_feasibility"].get("追價風險分數"), 0) or 0,
        "拉回買點分數": _safe_float(bundle["trade_feasibility"].get("拉回買點分數"), 0) or 0,
        "突破買點分數": _safe_float(bundle["trade_feasibility"].get("突破買點分數"), 0) or 0,
        "風險報酬評級": _safe_str(bundle["trade_feasibility"].get("風險報酬評級")),
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
        "風險分數": _safe_float(bundle["risk_filter"].get("風險分數"), 0) or 0,
        "淘汰原因": _safe_str(bundle["risk_filter"].get("淘汰原因")),
        "均線轉強分": _safe_float(bundle["prelaunch"].get("均線轉強分"), 0) or 0,
        "量能啟動分": _safe_float(bundle["prelaunch"].get("量能啟動分"), 0) or 0,
        "突破準備分": _safe_float(bundle["prelaunch"].get("突破準備分"), 0) or 0,
        "動能翻多分": _safe_float(bundle["prelaunch"].get("動能翻多分"), 0) or 0,
        "支撐防守分": _safe_float(bundle["prelaunch"].get("支撐防守分"), 0) or 0,
        "推薦模式": mode,
    }


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
            類股平均起漲前兆=("起漲前兆分數", "mean"),
            類股平均交易可行=("交易可行分數", "mean"),
        )
        .reset_index()
    )

    grp["類股熱度分數"] = (
        grp["類股平均總分"] * 0.28
        + grp["類股平均訊號"] * 5.5
        + grp["類股平均漲幅"].fillna(0) * 0.32
        + grp["類股平均雷達"] * 0.16
        + grp["類股平均自動因子"] * 0.12
        + grp["類股平均起漲前兆"] * 0.12
    ).apply(lambda x: _score_clip(x))

    grp["類股加速度"] = (
        grp["類股平均起漲前兆"] * 0.45
        + grp["類股平均交易可行"] * 0.20
        + grp["類股平均訊號"] * 4.0
        + grp["類股平均漲幅"].fillna(0) * 0.18
    ).apply(lambda x: _score_clip(x))

    grp = grp.sort_values(["類股熱度分數", "類股平均總分"], ascending=[False, False]).reset_index(drop=True)
    grp["類股熱度排名"] = range(1, len(grp) + 1)
    return grp


def _build_recommend_df(
    universe_items: list[dict[str, str]],
    master_df: pd.DataFrame,
    start_dt: date,
    end_dt: date,
    min_total_score: float,
    min_signal_score: float,
    selected_categories: list[str],
    mode: str,
    risk_strictness: str,
    min_prelaunch_score: float,
    min_trade_score: float,
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
                mode,
                risk_strictness,
                min_prelaunch_score,
                min_trade_score,
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

            progress_bar.progress(min(max(ratio, 0.0), 1.0), text=f"推薦計算中... {done_count}/{total_count} ({ratio*100:.1f}%)")
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
        base_df["類股熱度排名"] = None
        base_df["類股加速度"] = None
    else:
        base_df = base_df.merge(
            category_strength_df[
                ["類別", "類股平均總分", "類股平均訊號", "類股平均漲幅", "類股熱度分數", "類股熱度排名", "類股加速度"]
            ],
            on="類別",
            how="left",
        )

    base_df["同類股領先幅度"] = (base_df["個股原始總分"] - base_df["類股平均總分"].fillna(0)).apply(lambda x: _score_clip(50 + x))
    base_df["是否領先同類股"] = (base_df["個股原始總分"] >= base_df["類股平均總分"].fillna(0)).map({True: "是", False: "否"})
    base_df["類股內排名"] = base_df.groupby("類別")["個股原始總分"].rank(method="dense", ascending=False).astype(int)
    base_df["類股前3強"] = base_df["類股內排名"].apply(lambda x: "是" if pd.notna(x) and int(x) <= 3 else "否")

    mode_scores = base_df.apply(
        lambda r: _build_mode_score(
            mode=_safe_str(mode),
            technical_score=_safe_float(r.get("技術結構分數"), 0) or 0,
            prelaunch_score=_safe_float(r.get("起漲前兆分數"), 0) or 0,
            category_heat_score=_safe_float(r.get("類股熱度分數"), 0) or 0,
            factor_score=_safe_float(r.get("自動因子總分"), 0) or 0,
            trade_score=_safe_float(r.get("交易可行分數"), 0) or 0,
            leader_advantage=_safe_float(r.get("同類股領先幅度"), 0) or 0,
        ),
        axis=1,
    )
    base_df["推薦總分"] = [x[0] for x in mode_scores]
    base_df["推薦標籤"] = [x[1] for x in mode_scores]

    def _recommend(score: float) -> str:
        if score >= 90:
            return "股神級"
        if score >= 84:
            return "強烈關注"
        if score >= 72:
            return "優先觀察"
        if score >= 60:
            return "可列追蹤"
        return "觀察"

    base_df["推薦等級"] = base_df["推薦總分"].apply(_recommend)

    def _reason_builder(r):
        reason_parts = []
        if _safe_float(r.get("均線轉強分"), 0) >= 70:
            reason_parts.append("均線結構轉強")
        if _safe_float(r.get("量能啟動分"), 0) >= 65:
            reason_parts.append("量能明顯放大")
        if _safe_float(r.get("突破準備分"), 0) >= 70:
            reason_parts.append("接近壓力突破位")
        if _safe_float(r.get("動能翻多分"), 0) >= 65:
            reason_parts.append("動能翻多")
        if _safe_float(r.get("支撐防守分"), 0) >= 65:
            reason_parts.append("支撐防守佳")
        if _safe_str(r.get("是否領先同類股")) == "是":
            reason_parts.append("領先同類股")
        if _safe_str(r.get("類股前3強")) == "是":
            reason_parts.append("類股前3強")
        if _safe_float(r.get("類股熱度分數"), 0) >= 75:
            reason_parts.append("所屬類股熱度高")
        if _safe_float(r.get("交易可行分數"), 0) >= 70:
            reason_parts.append("風險報酬佳")
        if not reason_parts:
            reason_parts.append("結構偏多，列入觀察")
        return "、".join(reason_parts[:6])

    base_df["推薦理由摘要"] = base_df.apply(_reason_builder, axis=1)

    for c in ["3日績效%", "5日績效%", "10日績效%", "20日績效%"]:
        if c not in base_df.columns:
            base_df[c] = pd.NA

    final_df = base_df[base_df["推薦總分"] >= min_total_score].copy()
    final_df = final_df.sort_values(["推薦總分", "起漲前兆分數", "訊號分數", "區間漲跌幅%"], ascending=[False, False, False, False]).reset_index(drop=True)

    if "勾選" not in final_df.columns:
        final_df.insert(0, "勾選", False)

    return final_df, category_strength_df


def _format_df(df: pd.DataFrame) -> pd.DataFrame:
    show = df.copy()
    price_cols = ["最新價", "推薦買點_突破", "推薦買點_拉回", "停損價", "賣出目標1", "賣出目標2"]
    pct_cols = ["區間漲跌幅%", "20日壓力距離%", "20日支撐距離%", "類股平均漲幅", "3日績效%", "5日績效%", "10日績效%", "20日績效%"]
    score_cols = [
        "訊號分數", "雷達均分", "技術結構分數", "起漲前兆分數", "交易可行分數",
        "追價風險分數", "拉回買點分數", "突破買點分數",
        "自動因子總分", "EPS代理分數", "營收動能代理分數", "獲利代理分數",
        "大戶鎖碼代理分數", "法人連買代理分數",
        "個股原始總分", "類股平均總分", "類股平均訊號", "類股熱度分數",
        "類股加速度", "同類股領先幅度", "推薦總分", "風險分數",
        "均線轉強分", "量能啟動分", "突破準備分", "動能翻多分", "支撐防守分"
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
# Excel 匯出
# =========================================================
@st.cache_data(ttl=300, show_spinner=False)
def _build_export_views(rec_df: pd.DataFrame, category_strength_df: pd.DataFrame, top_n: int):
    if rec_df is None or rec_df.empty:
        empty = pd.DataFrame()
        return empty, empty, empty, empty

    rec_export = rec_df.copy()
    leader_df = rec_df.sort_values(["是否領先同類股", "推薦總分", "類股熱度分數"], ascending=[False, False, False]).reset_index(drop=True)
    factor_rank = rec_df.sort_values(["自動因子總分", "EPS代理分數", "營收動能代理分數", "獲利代理分數"], ascending=[False, False, False, False]).reset_index(drop=True)
    cat_export = category_strength_df.copy() if isinstance(category_strength_df, pd.DataFrame) else pd.DataFrame()

    leader_export = leader_df[
        ["股票代號", "股票名稱", "類別", "類股內排名", "類股前3強", "是否領先同類股", "同類股領先幅度", "個股原始總分", "類股平均總分", "類股熱度分數", "推薦總分", "推薦理由摘要"]
    ].head(top_n).copy() if not leader_df.empty else pd.DataFrame()

    factor_export = factor_rank[
        ["股票代號", "股票名稱", "類別", "自動因子總分", "EPS代理分數", "營收動能代理分數", "獲利代理分數", "大戶鎖碼代理分數", "法人連買代理分數", "自動因子摘要"]
    ].head(top_n).copy() if not factor_rank.empty else pd.DataFrame()

    return rec_export, cat_export, leader_export, factor_export


@st.cache_data(ttl=300, show_spinner=False)
def _build_excel_bytes(
    rec_export: pd.DataFrame,
    cat_export: pd.DataFrame,
    leader_export: pd.DataFrame,
    factor_export: pd.DataFrame,
) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        if rec_export is not None:
            rec_export.to_excel(writer, sheet_name="完整推薦表", index=False)
        if cat_export is not None:
            cat_export.to_excel(writer, sheet_name="類股強度榜", index=False)
        if leader_export is not None:
            leader_export.to_excel(writer, sheet_name="同類股領先榜", index=False)
        if factor_export is not None:
            factor_export.to_excel(writer, sheet_name="自動因子榜", index=False)

        try:
            for ws in writer.book.worksheets:
                ws.freeze_panes = "A2"
                for col_cells in ws.columns:
                    max_len = 0
                    col_letter = col_cells[0].column_letter
                    for cell in col_cells:
                        cell_val = "" if cell.value is None else str(cell.value)
                        if len(cell_val) > max_len:
                            max_len = len(cell_val)
                    ws.column_dimensions[col_letter].width = min(max(max_len + 2, 10), 40)
        except Exception:
            pass

    output.seek(0)
    return output.getvalue()


def _render_export_block(rec_df: pd.DataFrame, category_strength_df: pd.DataFrame, top_n: int):
    if rec_df is None or rec_df.empty:
        return

    rec_export, cat_export, leader_export, factor_export = _build_export_views(rec_df, category_strength_df, top_n)
    excel_bytes = _build_excel_bytes(rec_export, cat_export, leader_export, factor_export)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"股神推薦_V2_{ts}.xlsx"

    render_pro_section("Excel 匯出")
    c1, c2 = st.columns([2, 4])
    with c1:
        st.download_button(
            label="匯出推薦結果 Excel",
            data=excel_bytes,
            file_name=file_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
    with c2:
        st.caption("匯出內容：完整推薦表、類股強度榜、同類股領先榜、自動因子榜。")


def _render_selected_export_block():
    selected_df = st.session_state.get(_k("selected_rec_snapshot"))
    if not isinstance(selected_df, pd.DataFrame) or selected_df.empty:
        return

    export_df = selected_df.copy()
    want_cols = [
        "股票代號", "股票名稱", "市場別", "類別",
        "類股內排名", "類股前3強",
        "推薦模式", "推薦等級", "推薦總分",
        "技術結構分數", "起漲前兆分數", "交易可行分數", "類股熱度分數",
        "同類股領先幅度", "是否領先同類股",
        "最新價", "推薦買點_拉回", "推薦買點_突破",
        "停損價", "賣出目標1", "賣出目標2",
        "3日績效%", "5日績效%", "10日績效%", "20日績效%",
        "推薦標籤", "推薦理由摘要",
    ]
    export_df = export_df[[c for c in want_cols if c in export_df.columns]].copy()

    selected_bytes = _build_excel_bytes(
        rec_export=export_df,
        cat_export=pd.DataFrame(),
        leader_export=pd.DataFrame(),
        factor_export=pd.DataFrame(),
    )

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    st.download_button(
        label="匯出勾選推薦股 Excel",
        data=selected_bytes,
        file_name=f"股神推薦_勾選結果_{ts}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )



def _build_record_export_bytes(record_rows: list[dict[str, Any]]) -> bytes:
    df = _ensure_godpick_record_columns(pd.DataFrame(record_rows))
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="股神推薦紀錄匯入", index=False)
        try:
            ws = writer.book["股神推薦紀錄匯入"]
            ws.freeze_panes = "A2"
            for col_cells in ws.columns:
                max_len = 0
                col_letter = col_cells[0].column_letter
                for cell in col_cells:
                    cell_val = "" if cell.value is None else str(cell.value)
                    max_len = max(max_len, len(cell_val))
                ws.column_dimensions[col_letter].width = min(max(max_len + 2, 10), 42)
        except Exception:
            pass
    output.seek(0)
    return output.getvalue()




def _render_recommendation_scoring_guide():
    st.markdown(
        """
        <style>
        .gp-guide-wrap{
            background:#f8fafc;
            border:1px solid rgba(99,102,241,.14);
            border-radius:18px;
            padding:18px 18px 14px 18px;
            margin:18px 0 10px 0;
        }
        .gp-guide-title{
            font-size:1.65rem;
            font-weight:800;
            color:#0f172a;
            margin-bottom:16px;
        }
        .gp-guide-grid{
            display:grid;
            grid-template-columns:repeat(4,minmax(0,1fr));
            gap:14px;
        }
        .gp-guide-card{
            background:#ffffff;
            border:1px solid rgba(99,102,241,.14);
            border-radius:18px;
            padding:18px 18px 14px 18px;
            box-shadow:0 1px 3px rgba(15,23,42,.04);
            height:100%;
        }
        .gp-guide-card h4{
            margin:0 0 10px 0;
            font-size:1.28rem;
            font-weight:800;
            color:#111827;
        }
        .gp-guide-card p{
            margin:0 0 10px 0;
            color:#334155;
            line-height:1.75;
            font-size:1rem;
        }
        .gp-guide-card ul{
            margin:0;
            padding-left:1.2rem;
        }
        .gp-guide-card li{
            margin:0 0 8px 0;
            color:#334155;
            line-height:1.75;
            font-size:1rem;
        }
        .gp-score-list{display:flex;flex-direction:column;gap:10px;}
        .gp-score-row{display:flex;align-items:flex-start;gap:10px;line-height:1.6;}
        .gp-badge{
            display:inline-block;
            min-width:92px;
            text-align:center;
            padding:6px 10px;
            border-radius:10px;
            font-size:.95rem;
            font-weight:800;
            border:1px solid transparent;
            white-space:nowrap;
        }
        .gp-badge.green{background:#e8f7ee;color:#15803d;border-color:#b7e4c7;}
        .gp-badge.green2{background:#eefbf3;color:#166534;border-color:#ccefd7;}
        .gp-badge.yellow{background:#fff7db;color:#b45309;border-color:#f7d98a;}
        .gp-badge.orange{background:#fff1e6;color:#c2410c;border-color:#fdc9a6;}
        .gp-badge.red{background:#feecec;color:#b91c1c;border-color:#f5b5b5;}
        .gp-guide-foot{
            margin-top:14px;
            padding-top:10px;
            border-top:1px solid rgba(99,102,241,.12);
            color:#475569;
            font-size:.98rem;
            font-weight:600;
        }
        @media (max-width: 1200px){
            .gp-guide-grid{grid-template-columns:repeat(2,minmax(0,1fr));}
        }
        @media (max-width: 760px){
            .gp-guide-grid{grid-template-columns:1fr;}
        }
        </style>
        <div class="gp-guide-wrap">
            <div class="gp-guide-title">推薦條件說明 / 分數解讀</div>
            <div class="gp-guide-grid">
                <div class="gp-guide-card">
                    <h4>評分是怎麼算的？</h4>
                    <p>系統依多個面向加總評分，分數越高，代表技術面、趨勢面、量價面與風險報酬條件越完整。</p>
                    <ul>
                        <li><b>趨勢強度：</b>均線多頭、突破型態、是否站穩關鍵價位</li>
                        <li><b>量價結構：</b>量能放大、價量配合、是否有主力進場跡象</li>
                        <li><b>風險控管：</b>回檔風險、追高風險、破線風險、波動風險</li>
                        <li><b>交易可行：</b>進場點清楚、停損點明確、風險報酬比合理</li>
                        <li><b>類股動能：</b>所屬類股熱度、資金輪動、族群帶動性</li>
                    </ul>
                </div>
                <div class="gp-guide-card">
                    <h4>分數代表什麼？</h4>
                    <div class="gp-score-list">
                        <div class="gp-score-row"><span class="gp-badge green">90 分以上</span><div><b>強勢買進區：</b>條件完整，可優先關注</div></div>
                        <div class="gp-score-row"><span class="gp-badge green2">80–89 分</span><div><b>偏多觀察區：</b>適合逢回找買點</div></div>
                        <div class="gp-score-row"><span class="gp-badge yellow">70–79 分</span><div><b>觀察等待區：</b>條件尚可，需搭配突破或量能確認</div></div>
                        <div class="gp-score-row"><span class="gp-badge orange">60–69 分</span><div><b>保守區：</b>有題材但訊號不足，先觀察</div></div>
                        <div class="gp-score-row"><span class="gp-badge red">60 分以下</span><div><b>不建議進場：</b>風險較高，勝率不足</div></div>
                    </div>
                </div>
                <div class="gp-guide-card">
                    <h4>何時適合買入？</h4>
                    <ul>
                        <li>建議 <b>80 分以上</b> 再優先考慮進場</li>
                        <li>若達 <b>90 分以上</b>，且量價配合、風險報酬佳，可列為高優先名單</li>
                        <li><b>70–79 分</b> 可列入觀察名單，等待突破、放量或回測支撐成功</li>
                        <li>低於 <b>70 分</b> 原則上不追價</li>
                    </ul>
                </div>
                <div class="gp-guide-card">
                    <h4>使用提醒</h4>
                    <ul>
                        <li>本分數為輔助判斷，不等於保證獲利</li>
                        <li>建議搭配停損、部位控管與大盤方向一起判讀</li>
                        <li>短線、波段、領頭羊模式的標準會略有不同</li>
                    </ul>
                </div>
            </div>
            <div class="gp-guide-foot">提醒：市場隨時變化，請搭配最新資訊與自身交易策略，謹慎評估風險。</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_record_export_block(rec_df: pd.DataFrame):
    selected_df = st.session_state.get(_k("selected_rec_snapshot"))
    if not isinstance(selected_df, pd.DataFrame) or selected_df.empty:
        return

    selected_codes = [_normalize_code(x) for x in selected_df["股票代號"].astype(str).tolist() if _normalize_code(x)]
    if not selected_codes:
        return

    record_rows = _build_record_rows_from_rec_df(rec_df, selected_codes)
    if not record_rows:
        return

    record_bytes = _build_record_export_bytes(record_rows)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    render_pro_section("匯出到股神推薦紀錄")
    st.caption("這裡只做匯出，不直接串接 8_股神推薦紀錄。你可以下載後自行備份或匯入。")
    st.download_button(
        label="匯出股神推薦紀錄 Excel",
        data=record_bytes,
        file_name=f"股神推薦紀錄匯入檔_{ts}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

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
        "rec_record_codes": [],
        "result_saved_at": "",
        "recommend_mode": "飆股模式",
        "risk_strictness": "標準",
        "min_prelaunch_score": 45.0,
        "min_trade_score": 45.0,
    }
    for name, value in defaults.items():
        if _k(name) not in st.session_state:
            st.session_state[_k(name)] = value

    if _k("selected_rec_snapshot") not in st.session_state:
        st.session_state[_k("selected_rec_snapshot")] = pd.DataFrame()

    next_pick_key = _k("rec_pick_codes_next")
    real_pick_key = _k("rec_pick_codes")
    if next_pick_key in st.session_state:
        st.session_state[real_pick_key] = st.session_state.pop(next_pick_key)

    next_record_key = _k("rec_record_codes_next")
    real_record_key = _k("rec_record_codes")
    if next_record_key in st.session_state:
        st.session_state[real_record_key] = st.session_state.pop(next_record_key)

    render_pro_hero(
        title="股神推薦｜V2 升級版",
        subtitle="保留原功能 + 起漲前兆分數 + 風險淘汰 + 三模式推薦 + Excel 匯出 + 寫入 8_股神推薦紀錄。",
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

    all_categories = _collect_all_categories(master_df, watchlist_map)
    category_options = ["全部"] + all_categories if all_categories else ["全部"]

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
            form_universe_mode = st.selectbox("掃描範圍", universe_options, index=universe_options.index(saved_universe))

        with c2:
            group_options = list(watchlist_map.keys()) if watchlist_map else [""]
            saved_group = st.session_state.get(_k("group"), "")
            if saved_group not in group_options:
                saved_group = group_options[0] if group_options else ""
            form_group = st.selectbox("自選群組", group_options, index=group_options.index(saved_group) if saved_group in group_options else 0)

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

        render_pro_section("模式 / 類型篩選")
        m1, m2 = st.columns([2, 2])
        with m1:
            form_recommend_mode = st.selectbox(
                "推薦模式",
                ["飆股模式", "波段模式", "領頭羊模式", "綜合模式"],
                index=["飆股模式", "波段模式", "領頭羊模式", "綜合模式"].index(st.session_state.get(_k("recommend_mode"), "飆股模式")),
            )
        with m2:
            form_risk_strictness = st.selectbox(
                "風險過濾強度",
                ["寬鬆", "標準", "嚴格"],
                index=["寬鬆", "標準", "嚴格"].index(st.session_state.get(_k("risk_strictness"), "標準")),
            )

        form_selected_categories = st.multiselect(
            "選擇類型（可多選）",
            options=category_options,
            default=saved_categories,
            help="已細分為 IC設計、晶圓代工、封測、AI伺服器、散熱、金控、銀行等。",
        )

        render_pro_section("推薦門檻")
        f1, f2, f3, f4 = st.columns(4)
        with f1:
            form_min_total_score = st.number_input("推薦總分下限", value=float(st.session_state.get(_k("min_total_score"), 55.0)), step=1.0)
        with f2:
            form_min_signal_score = st.number_input("訊號分數下限", value=float(st.session_state.get(_k("min_signal_score"), -2.0)), step=1.0)
        with f3:
            form_min_prelaunch_score = st.number_input("起漲前兆分數下限", value=float(st.session_state.get(_k("min_prelaunch_score"), 45.0)), step=1.0)
        with f4:
            form_min_trade_score = st.number_input("交易可行分數下限", value=float(st.session_state.get(_k("min_trade_score"), 45.0)), step=1.0)

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
        try:
            _build_excel_bytes.clear()
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
        st.session_state[_k("min_prelaunch_score")] = 45.0
        st.session_state[_k("min_trade_score")] = 45.0
        st.session_state[_k("recommend_mode")] = "飆股模式"
        st.session_state[_k("risk_strictness")] = "標準"
        st.session_state[_k("submitted_once")] = False
        st.session_state[_k("focus_code")] = ""
        st.session_state[_k("rec_df_store")] = pd.DataFrame()
        st.session_state[_k("category_strength_store")] = pd.DataFrame()
        st.session_state[_k("rec_pick_codes_next")] = []
        st.session_state[_k("rec_pick_codes")] = []
        st.session_state[_k("rec_record_codes")] = []
        st.session_state[_k("selected_rec_snapshot")] = pd.DataFrame()
        st.session_state["godpick_rec_selected_df"] = pd.DataFrame()
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
        st.session_state[_k("min_prelaunch_score")] = float(form_min_prelaunch_score)
        st.session_state[_k("min_trade_score")] = float(form_min_trade_score)
        st.session_state[_k("recommend_mode")] = form_recommend_mode
        st.session_state[_k("risk_strictness")] = form_risk_strictness
        st.session_state[_k("submitted_once")] = True

    render_pro_info_card(
        "V2 選股邏輯",
        [
            ("推薦模式", "新增 飆股模式 / 波段模式 / 領頭羊模式 / 綜合模式。", ""),
            ("起漲前兆", "新增均線轉強、量能啟動、突破準備、動能翻多、支撐防守。", ""),
            ("風險淘汰", "新增風險過濾強度：寬鬆 / 標準 / 嚴格。", ""),
            ("交易可行", "新增交易可行分數、追價風險、拉回買點、突破買點、風險報酬評級。", ""),
            ("類股強度", "保留類股熱度，新增類股加速度與熱度排名。", ""),
            ("匯出", "新增 Excel 匯出，不重算目前結果。", ""),
            ("推薦紀錄", "新增可勾選後直接寫入 8_股神推薦紀錄。", ""),
            ("勾選快照", "本輪精華推薦表可直接勾選，並同步到自選股/推薦紀錄/勾選匯出。", ""),
        ],
        chips=["V2", "功能不刪", "顯示加速", "精準度升級", "Excel匯出", "推薦紀錄串接"],
    )

    _render_recommendation_scoring_guide()

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
            mode=_safe_str(st.session_state.get(_k("recommend_mode"), "飆股模式")),
            risk_strictness=_safe_str(st.session_state.get(_k("risk_strictness"), "標準")),
            min_prelaunch_score=float(st.session_state.get(_k("min_prelaunch_score"), 45.0)),
            min_trade_score=float(st.session_state.get(_k("min_trade_score"), 45.0)),
        )
        _save_recommend_result_to_state(rec_df, category_strength_df)
    else:
        rec_df, category_strength_df = _load_recommend_result_from_state()

    if rec_df.empty:
        if submit_recommend or submit_refresh:
            st.warning("本輪條件篩選後為 0 檔，代表推薦流程有執行，但目前門檻、風險過濾或掃描池條件過嚴，沒有股票通過。")
            st.info("建議先改成：風險過濾=寬鬆、起漲前兆下限=30、交易可行下限=30、訊號分數下限=-3，再重新推薦。")
        else:
            st.error("目前沒有已保存的推薦結果，請先按一次「開始推薦」。")
        return

    saved_at = _safe_str(st.session_state.get(_k("result_saved_at"), ""))
    if saved_at:
        st.caption(f"目前顯示的是已保存推薦結果｜保存時間：{saved_at}")

    top_n = int(st.session_state.get(_k("top_n"), 20))
    top_df = rec_df.iloc[:top_n].copy()

    strong_count = int((rec_df["推薦等級"].isin(["股神級", "強烈關注"])).sum())
    avg_score = _avg_safe([_safe_float(x) for x in rec_df["推薦總分"].tolist()], 0)
    leader_count = int((rec_df["是否領先同類股"] == "是").sum())

    render_pro_kpi_row(
        [
            {"label": "掃描股票數", "value": len(rec_df), "delta": universe_mode, "delta_class": "pro-kpi-delta-flat"},
            {"label": "強勢推薦", "value": strong_count, "delta": "最高等級群", "delta_class": "pro-kpi-delta-flat"},
            {"label": "領先同類股", "value": leader_count, "delta": "類股相對強勢", "delta_class": "pro-kpi-delta-flat"},
            {"label": "平均總分", "value": format_number(avg_score, 1), "delta": _safe_str(st.session_state.get(_k("recommend_mode"), "")), "delta_class": "pro-kpi-delta-flat"},
        ]
    )

    render_pro_section("推薦股票加入自選股中心")
    watchlist_map = _load_watchlist_map()

    g1, g2, g3 = st.columns([3, 2, 1])
    with g1:
        new_group_name = st.text_input("新增群組名稱", key=_k("new_group_name"), placeholder="例如：0422股神推薦")
    with g2:
        st.write("")
        st.write("")
        create_group_btn = st.button("新增群組", key=_k("create_group_btn"), use_container_width=True)
    with g3:
        st.write("")
        st.write("")
        refresh_group_btn = st.button("重新載入群組", key=_k("refresh_group_btn"), use_container_width=True)

    if create_group_btn:
        ok, msg = _create_watchlist_group(new_group_name)
        if ok:
            st.success(msg)
            watchlist_map = _load_watchlist_map()
            st.session_state[_k("rec_pick_group")] = _safe_str(new_group_name)
            st.rerun()
        else:
            st.warning(msg)

    if refresh_group_btn:
        watchlist_map = _load_watchlist_map()
        st.rerun()

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
        if rec_group_options and rec_group_options != [""]:
            pick_group = st.selectbox(
                "加入群組",
                options=rec_group_options,
                index=rec_group_options.index(saved_pick_group) if saved_pick_group in rec_group_options else 0,
                key=_k("rec_pick_group"),
            )
        else:
            pick_group = ""
            st.info("目前尚無群組，請先新增群組名稱。")
    with p2:
        current_pick_codes = [x for x in st.session_state.get(_k("rec_pick_codes"), []) if x in rec_all_codes]
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

    detail_lines = st.session_state.get(_k("last_dual_write_detail"), [])
    if detail_lines:
        with st.expander("雙寫狀態明細", expanded=False):
            for line in detail_lines:
                st.write(f"- {line}")

    render_pro_section("寫入 8_股神推薦紀錄")
    record_code_to_label = {
        str(r["股票代號"]): f"{r['股票代號']} {r['股票名稱']}｜{r['推薦等級']}｜{format_number(r['推薦總分'],1)}"
        for _, r in rec_df.iterrows()
    }
    record_all_codes = rec_df["股票代號"].astype(str).tolist()

    rr1, rr2 = st.columns([4, 2])
    with rr1:
        current_record_codes = [x for x in st.session_state.get(_k("rec_record_codes"), []) if x in record_all_codes]
        st.multiselect(
            "勾選要記錄到 8_股神推薦紀錄 的股票",
            options=record_all_codes,
            default=current_record_codes,
            format_func=lambda x: record_code_to_label.get(str(x), str(x)),
            key=_k("rec_record_codes"),
        )

    with rr2:
        st.write("")
        st.write("")
        record_to_log_btn = st.button("記錄到 8_股神推薦紀錄", use_container_width=True, type="primary")

    rr3, rr4 = st.columns([1, 1])
    with rr3:
        if st.button("全選本輪推薦做紀錄", use_container_width=True):
            st.session_state[_k("rec_record_codes_next")] = record_all_codes
            st.rerun()
    with rr4:
        if st.button("清空紀錄勾選", use_container_width=True):
            st.session_state[_k("rec_record_codes_next")] = []
            st.rerun()

    selected_snapshot_df = rec_df[
        rec_df["股票代號"].astype(str).isin([_normalize_code(x) for x in st.session_state.get(_k("rec_record_codes"), []) if _normalize_code(x)])
    ].copy()
    st.session_state[_k("selected_rec_snapshot")] = selected_snapshot_df
    st.session_state["godpick_rec_selected_df"] = selected_snapshot_df

    if record_to_log_btn:
        selected_record_codes = [_normalize_code(x) for x in st.session_state.get(_k("rec_record_codes"), []) if _normalize_code(x)]
        if not selected_record_codes:
            st.warning("請先勾選要記錄的推薦股票。")
        else:
            record_rows = _build_record_rows_from_rec_df(rec_df, selected_record_codes)
            added_count, record_msgs = _append_godpick_records(record_rows)
            if added_count > 0:
                st.success(f"已寫入 {added_count} 筆到 8_股神推薦紀錄")
            else:
                st.warning("沒有新增任何推薦紀錄。")
            if record_msgs:
                with st.expander("推薦紀錄寫入明細", expanded=False):
                    for msg in record_msgs:
                        st.write(f"- {msg}")

    record_detail_lines = st.session_state.get(_k("last_record_write_detail"), [])
    if record_detail_lines:
        with st.expander("8_股神推薦紀錄 同步明細", expanded=False):
            for line in record_detail_lines:
                st.write(f"- {line}")

    _render_export_block(rec_df=rec_df, category_strength_df=category_strength_df, top_n=top_n)
    _render_selected_export_block()
    _render_record_export_block(rec_df)

    render_pro_section("本輪精華推薦")

    top_show_df = top_df[
        [
            "勾選",
            "股票代號",
            "股票名稱",
            "市場別",
            "類別",
            "類股內排名",
            "類股前3強",
            "推薦模式",
            "推薦等級",
            "推薦總分",
            "起漲前兆分數",
            "交易可行分數",
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
    ].copy()

    edited_top_df = st.data_editor(
        _format_df(top_show_df),
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        key=_k("top_pick_editor"),
        column_config={
            "勾選": st.column_config.CheckboxColumn("勾選"),
            "推薦理由摘要": st.column_config.TextColumn("推薦理由摘要", width="large"),
        }
    )

    picked_codes_from_top = []
    for _, row in edited_top_df.iterrows():
        picked_val = row.get("勾選", False)
        if isinstance(picked_val, bool):
            is_checked = picked_val
        else:
            is_checked = str(picked_val).strip().lower() in {"true", "1", "yes", "y", "是"}

        if is_checked:
            code = _normalize_code(row.get("股票代號"))
            if code:
                picked_codes_from_top.append(code)

    current_pick_codes = st.session_state.get(_k("rec_pick_codes"), [])
    current_record_codes = st.session_state.get(_k("rec_record_codes"), [])

    if picked_codes_from_top != current_pick_codes:
        st.session_state[_k("rec_pick_codes_next")] = picked_codes_from_top
    if picked_codes_from_top != current_record_codes:
        st.session_state[_k("rec_record_codes_next")] = picked_codes_from_top

    selected_snapshot_top = rec_df[rec_df["股票代號"].astype(str).isin([str(x) for x in picked_codes_from_top])].copy()
    st.session_state[_k("selected_rec_snapshot")] = selected_snapshot_top
    st.session_state["godpick_rec_selected_df"] = selected_snapshot_top

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
                ("類股內排名", _safe_str(focus_row.get("類股內排名")), ""),
                ("類股前3強", _safe_str(focus_row.get("類股前3強")), ""),
                ("推薦模式", _safe_str(focus_row.get("推薦模式")), ""),
                ("推薦等級", _safe_str(focus_row.get("推薦等級")), ""),
                ("推薦總分", format_number(focus_row.get("推薦總分"), 1), ""),
                ("起漲前兆分數", format_number(focus_row.get("起漲前兆分數"), 1), ""),
                ("交易可行分數", format_number(focus_row.get("交易可行分數"), 1), ""),
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
            chips=[_safe_str(focus_row.get("推薦等級")), _safe_str(focus_row.get("類別")), _safe_str(focus_row.get("推薦標籤"))],
        )

    leader_df = rec_df.sort_values(["是否領先同類股", "推薦總分", "類股熱度分數"], ascending=[False, False, False]).reset_index(drop=True)
    factor_rank = rec_df.sort_values(["自動因子總分", "EPS代理分數", "營收動能代理分數", "獲利代理分數"], ascending=[False, False, False, False]).reset_index(drop=True)

    tabs = st.tabs(["完整推薦表", "類股強度榜", "同類股領先榜", "自動因子榜", "操作說明"])

    with tabs[0]:
        st.dataframe(_format_df(rec_df), use_container_width=True, hide_index=True)

    with tabs[1]:
        category_show = category_strength_df.copy()
        for c in ["類股平均總分", "類股平均訊號", "類股平均漲幅", "類股平均雷達", "類股平均自動因子", "類股平均起漲前兆", "類股平均交易可行", "類股熱度分數", "類股加速度"]:
            if c in category_show.columns:
                if c == "類股平均漲幅":
                    category_show[c] = category_show[c].apply(lambda x: f"{x:,.2f}%" if pd.notna(x) else "")
                else:
                    category_show[c] = category_show[c].apply(lambda x: format_number(x, 1) if pd.notna(x) else "")
        st.dataframe(category_show, use_container_width=True, hide_index=True)

    with tabs[2]:
        st.dataframe(
            _format_df(
                leader_df[
                    [
                        "股票代號", "股票名稱", "類別", "類股內排名", "類股前3強",
                        "是否領先同類股", "同類股領先幅度", "個股原始總分",
                        "類股平均總分", "類股熱度分數", "推薦總分", "推薦理由摘要",
                    ]
                ].head(top_n)
            ),
            use_container_width=True,
            hide_index=True,
        )

    with tabs[3]:
        st.dataframe(
            _format_df(
                factor_rank[
                    [
                        "股票代號", "股票名稱", "類別", "自動因子總分", "EPS代理分數",
                        "營收動能代理分數", "獲利代理分數", "大戶鎖碼代理分數",
                        "法人連買代理分數", "自動因子摘要",
                    ]
                ].head(top_n)
            ),
            use_container_width=True,
            hide_index=True,
        )

    with tabs[4]:
        render_pro_info_card(
            "V2 模組邏輯",
            [
                ("按鈕觸發", "調整條件不會自動重算，按下開始推薦才會跑。", ""),
                ("類型更細分", "已由大類擴充成 IC設計、晶圓代工、封測、AI伺服器、散熱、金控、銀行等。", ""),
                ("推薦模式", "新增 飆股模式 / 波段模式 / 領頭羊模式 / 綜合模式。", ""),
                ("風險過濾", "新增 寬鬆 / 標準 / 嚴格，先淘汰不合格股票。", ""),
                ("起漲前兆", "新增均線轉強、量能啟動、突破準備、動能翻多、支撐防守。", ""),
                ("交易可行", "新增交易可行分數、追價風險、拉回買點、突破買點、風險報酬評級。", ""),
                ("類股強度", "每個類別都會算平均總分、平均訊號、平均漲幅、類股熱度與類股加速度。", ""),
                ("個股領先", "若個股原始總分高於同類股平均，視為領先股。", ""),
                ("推薦表勾選", "本輪精華推薦表可直接勾選，會同步到加入自選股與寫入 8 頁用的勾選清單。", ""),
                ("類股內排名", "新增每個類別內部排名，快速找該族群最強個股。", ""),
                ("類股前3強", "若個股在該類別內排名 1~3，會標記為類股前3強。", ""),
                ("理由升級", "推薦理由已改成更偏交易決策語言，不只是分數描述。", ""),
                ("績效預留", "已預留 3日 / 5日 / 10日 / 20日績效欄位，供下一版自動回填。", ""),
                ("推薦加入自選股", "可直接勾選推薦結果並批次加入指定群組。", ""),
                ("寫入推薦紀錄", "可直接勾選推薦結果並批次寫入 8_股神推薦紀錄。", ""),
                ("雙寫同步", "自選股新增/刪除/批次加入時，同步寫回 GitHub watchlist.json + Firestore。", ""),
                ("Excel 匯出", "可匯出完整推薦表、類股強度榜、同類股領先榜、自動因子榜。", ""),
                ("加速與 ETA", "歷史資料與單股分析保留快取，整批推薦改成併發並顯示剩餘時間。", ""),
                ("推薦結果保留", "推薦結果會存到 session_state，切頁後回來不會立刻消失。", ""),
                ("掃描上限", "已支援 1000 / 1500 / 2000 / 全部掃描。", ""),
                ("7/8 對齊", "record_id、推薦日期、推薦時間、推薦欄位已正式對齊 8 頁。", ""),
            ],
            chips=["V2", "功能不刪", "顯示加速", "三模式", "起漲前兆", "風險過濾", "Excel匯出", "推薦紀錄串接"],
        )


if __name__ == "__main__":
    main()
