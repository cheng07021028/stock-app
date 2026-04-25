# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any
import base64
import io
import json

import pandas as pd
import requests
import streamlit as st

try:
    import firebase_admin
    from firebase_admin import credentials, firestore
except Exception:
    firebase_admin = None
    credentials = None
    firestore = None

try:
    from utils import inject_pro_theme, render_pro_hero, render_pro_section, render_pro_info_card, format_number
except Exception:
    def inject_pro_theme():
        return None
    def render_pro_hero(title: str, subtitle: str = "", chips=None):
        st.title(title)
        if subtitle:
            st.caption(subtitle)
    def render_pro_section(title: str):
        st.subheader(title)
    def render_pro_info_card(title: str, value: str, desc: str = ""):
        st.metric(title, value, desc)
    def format_number(v, digits=2):
        try:
            return f"{float(v):,.{digits}f}"
        except Exception:
            return ""

PAGE_TITLE = "推薦清單"
PFX = "godpick_list_"
PRELAUNCH_78910_VERSION = "recommend_list_prelaunch_78910_v1_20260425"

GODPICK_RECOMMEND_LIST_FILE = "godpick_recommend_list.json"

GODPICK_RECORD_COLUMNS = [
    "record_id",
    "股票代號",
    "股票名稱",
    "市場別",
    "類別",
    "推薦模式",
    "推薦等級",
    "推薦總分",
    "買點分級",
    "風險說明",
    "股神推論邏輯",
    "權重設定",
    "推薦分桶",
    "飆股起漲分數",
    "起漲等級",
    "起漲摘要",
    "信心等級",
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
    s = _safe_str(v)
    if not s:
        return ""
    if s.isdigit():
        return s
    digits = "".join(ch for ch in s if ch.isdigit())
    if 4 <= len(digits) <= 6:
        return digits
    return s


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _github_config() -> dict[str, str]:
    return {
        "token": _safe_str(st.secrets.get("GITHUB_TOKEN", "")),
        "owner": _safe_str(st.secrets.get("GITHUB_REPO_OWNER", "cheng07021028")),
        "repo": _safe_str(st.secrets.get("GITHUB_REPO_NAME", "stock-app")),
        "branch": _safe_str(st.secrets.get("GITHUB_REPO_BRANCH", "main")) or "main",
        "path": _safe_str(st.secrets.get("GODPICK_RECORDS_GITHUB_PATH", "godpick_records.json")) or "godpick_records.json",
    }


def _github_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _github_contents_url(owner: str, repo: str, path: str) -> str:
    return f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"



def _read_json_file_from_github(path_name: str, default):
    cfg = _github_config()
    token = cfg["token"]
    if not token:
        return default, "未設定 GITHUB_TOKEN"

    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], path_name),
            headers=_github_headers(token),
            params={"ref": cfg["branch"]},
            timeout=20,
        )
        if resp.status_code == 404:
            return default, f"{path_name} 尚未建立"
        if resp.status_code != 200:
            return default, f"讀取 {path_name} 失敗：{resp.status_code}"

        content = resp.json().get("content", "")
        if not content:
            return default, f"{path_name} 內容空白"
        payload = json.loads(base64.b64decode(content).decode("utf-8"))
        return payload, f"已讀取 {path_name}"
    except Exception as e:
        return default, f"讀取 {path_name} 例外：{e}"


def _read_recommend_list_from_latest() -> tuple[pd.DataFrame, str]:
    payload, msg = _read_json_file_from_github(GODPICK_RECOMMEND_LIST_FILE, [])
    if not isinstance(payload, list) or not payload:
        try:
            with open(GODPICK_RECOMMEND_LIST_FILE, "r", encoding="utf-8") as f:
                payload = json.load(f)
            msg = f"已讀取本機 {GODPICK_RECOMMEND_LIST_FILE}"
        except Exception:
            payload = []
    if not isinstance(payload, list):
        payload = []
    return _ensure_record_columns(pd.DataFrame(payload)), msg




def _derive_list_prelaunch_summary(row: pd.Series) -> str:
    existing = _safe_str(row.get("起漲摘要"))
    if existing:
        return existing
    s = _safe_float(row.get("飆股起漲分數"), row.get("起漲前兆分數")) or 0
    parts = []
    if s >= 90:
        parts.append("接近漲停")
    elif s >= 78:
        parts.append("強漲")
    elif s >= 68:
        parts.append("明顯上漲")
    elif s >= 55:
        parts.append("小漲轉強")
    else:
        parts.append("未見明顯起漲訊號")
    if _safe_float(row.get("爆發力分數"), 0) and _safe_float(row.get("爆發力分數"), 0) >= 70:
        parts.append("量能放大")
    if _safe_float(row.get("型態突破分數"), 0) and _safe_float(row.get("型態突破分數"), 0) >= 70:
        parts.append("突破結構")
    return "、".join(parts)


def _derive_list_prelaunch_grade(row: pd.Series) -> str:
    pre = _safe_float(row.get("起漲前兆分數"), 0) or 0
    burst = _safe_float(row.get("爆發力分數"), 0) or 0
    pattern = _safe_float(row.get("型態突破分數"), 0) or 0
    mix = pre * 0.6 + burst * 0.25 + pattern * 0.15
    if mix >= 88:
        return "S｜強烈起漲"
    if mix >= 78:
        return "A｜起漲優先"
    if mix >= 68:
        return "B｜轉強確認"
    if mix >= 55:
        return "C｜初步轉強"
    return "D｜尚未起漲"


def _derive_list_buy_grade(row: pd.Series) -> str:
    score = _safe_float(row.get("推薦總分"), 0) or 0
    pre = _safe_float(row.get("起漲前兆分數"), 0) or 0
    trade = _safe_float(row.get("交易可行分數"), 0) or 0
    if score >= 88 and pre >= 75 and trade >= 70:
        return "A+｜可積極觀察"
    if score >= 80 and trade >= 65:
        return "A｜優先觀察"
    if score >= 72:
        return "B｜等確認"
    if score >= 60:
        return "C｜僅觀察"
    return "D｜暫不追價"


def _derive_list_risk(row: pd.Series) -> str:
    stop_loss = row.get("停損價")
    target1 = row.get("賣出目標1")
    parts = []
    if pd.notna(stop_loss):
        parts.append(f"停損 {format_number(stop_loss, 2)}")
    if pd.notna(target1):
        parts.append(f"目標1 {format_number(target1, 2)}")
    if _safe_float(row.get("交易可行分數"), 0) < 55:
        parts.append("交易可行偏低")
    return "｜".join(parts) if parts else "依原推薦風控"


def _derive_list_logic(row: pd.Series) -> str:
    parts = []
    if _safe_str(row.get("類別")):
        parts.append(_safe_str(row.get("類別")))
    if _safe_float(row.get("起漲前兆分數"), 0) >= 75:
        parts.append("起漲前兆強")
    if _safe_float(row.get("類股熱度分數"), 0) >= 75:
        parts.append("類股熱度高")
    if _safe_str(row.get("是否領先同類股")).lower() in ["true", "1", "是"]:
        parts.append("領先同類股")
    if _safe_float(row.get("交易可行分數"), 0) >= 70:
        parts.append("進出場清楚")
    return "、".join(parts) if parts else _safe_str(row.get("推薦理由摘要")) or "觀察名單"



def _ensure_record_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS)
    x = df.copy()
    if "record_id" not in x.columns and "rec_id" in x.columns:
        x["record_id"] = x["rec_id"]
    for c in GODPICK_RECORD_COLUMNS:
        if c not in x.columns:
            x[c] = None


    if "起漲等級" in x.columns:
        x["起漲等級"] = x["起漲等級"].fillna("").astype(str)
        mask = x["起漲等級"].str.strip() == ""
        if mask.any():
            x.loc[mask, "起漲等級"] = x.loc[mask].apply(_derive_list_prelaunch_grade, axis=1)


    # 7/8/9/10 起漲欄位串聯補齊：舊資料沒有新欄位時自動補。
    if "飆股起漲分數" in x.columns:
        x["飆股起漲分數"] = pd.to_numeric(x["飆股起漲分數"], errors="coerce")
        if "起漲前兆分數" in x.columns:
            x["飆股起漲分數"] = x["飆股起漲分數"].fillna(pd.to_numeric(x["起漲前兆分數"], errors="coerce"))
    if "起漲等級" in x.columns:
        x["起漲等級"] = x["起漲等級"].fillna("").astype(str)
        mask = x["起漲等級"].str.strip() == ""
        if mask.any():
            x.loc[mask, "起漲等級"] = x.loc[mask].apply(_derive_list_prelaunch_grade, axis=1)
    if "起漲摘要" in x.columns:
        x["起漲摘要"] = x["起漲摘要"].fillna("").astype(str)
        mask = x["起漲摘要"].str.strip() == ""
        if mask.any():
            x.loc[mask, "起漲摘要"] = x.loc[mask].apply(_derive_list_prelaunch_summary, axis=1)

    if "買點分級" in x.columns:
        x["買點分級"] = x["買點分級"].fillna("").astype(str)
        mask = x["買點分級"].str.strip() == ""
        if mask.any():
            x.loc[mask, "買點分級"] = x.loc[mask].apply(_derive_list_buy_grade, axis=1)

    if "風險說明" in x.columns:
        x["風險說明"] = x["風險說明"].fillna("").astype(str)
        mask = x["風險說明"].str.strip() == ""
        if mask.any():
            x.loc[mask, "風險說明"] = x.loc[mask].apply(_derive_list_risk, axis=1)

    if "股神推論邏輯" in x.columns:
        x["股神推論邏輯"] = x["股神推論邏輯"].fillna("").astype(str)
        mask = x["股神推論邏輯"].str.strip() == ""
        if mask.any():
            x.loc[mask, "股神推論邏輯"] = x.loc[mask].apply(_derive_list_logic, axis=1)
    num_cols = [
        "推薦總分", "技術結構分數", "起漲前兆分數", "交易可行分數", "類股熱度分數",
        "同類股領先幅度", "推薦價格", "停損價", "賣出目標1", "賣出目標2",
        "實際買進價", "實際賣出價", "實際報酬%", "最新價", "損益金額", "損益幅%", "持有天數"
    ]
    for c in num_cols:
        x[c] = pd.to_numeric(x[c], errors="coerce")
    bool_cols = ["是否領先同類股", "是否已實際買進", "是否達停損", "是否達目標1", "是否達目標2"]
    for c in bool_cols:
        x[c] = x[c].fillna(False).map(lambda v: str(v).strip().lower() in {"true", "1", "yes", "y", "是"})
    for c in ["推薦日期", "推薦時間", "建立時間", "更新時間", "最新更新時間", "目前狀態", "模式績效標籤", "備註"]:
        x[c] = x[c].fillna("").astype(str)
    x["股票代號"] = x["股票代號"].map(_normalize_code)
    x["股票名稱"] = x["股票名稱"].fillna("").astype(str)
    return x[GODPICK_RECORD_COLUMNS].copy()


def _read_records_from_github() -> tuple[pd.DataFrame, str]:
    cfg = _github_config()
    token = cfg["token"]
    if not token:
        return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS), "未設定 GITHUB_TOKEN"
    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            params={"ref": cfg["branch"]},
            timeout=25,
        )
        if resp.status_code == 404:
            return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS), "尚未建立 godpick_records.json"
        if resp.status_code != 200:
            return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS), f"讀取推薦清單失敗：{resp.status_code} / {resp.text[:300]}"
        data = resp.json()
        content = data.get("content", "")
        if not content:
            return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS), "推薦清單為空"
        payload = json.loads(base64.b64decode(content).decode("utf-8"))
        if not isinstance(payload, list):
            return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS), "推薦清單格式不是 list"
        return _ensure_record_columns(pd.DataFrame(payload)), ""
    except Exception as e:
        return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS), f"讀取推薦清單例外：{e}"


def _get_records_sha() -> tuple[str, str]:
    cfg = _github_config()
    token = cfg["token"]
    if not token:
        return "", "未設定 GITHUB_TOKEN"
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
        return "", f"讀取 SHA 失敗：{resp.status_code} / {resp.text[:300]}"
    except Exception as e:
        return "", f"讀取 SHA 例外：{e}"


def _firebase_ready() -> tuple[bool, str]:
    if firebase_admin is None or credentials is None or firestore is None:
        return False, "firebase_admin 未安裝或不可用"
    return True, ""


def _clean_private_key(raw_key: str) -> str:
    private_key = _safe_str(raw_key).replace("\\n", "\n").strip()
    if private_key.startswith("\ufeff"):
        private_key = private_key.lstrip("\ufeff")
    return private_key


def _init_firebase_app():
    ok, msg = _firebase_ready()
    if not ok:
        raise ValueError(msg)
    try:
        return firebase_admin.get_app()
    except Exception:
        pass
    project_id = _safe_str(st.secrets.get("FIREBASE_PROJECT_ID", ""))
    client_email = _safe_str(st.secrets.get("FIREBASE_CLIENT_EMAIL", ""))
    private_key = _clean_private_key(_safe_str(st.secrets.get("FIREBASE_PRIVATE_KEY", "")))
    if not project_id or not client_email or not private_key:
        raise ValueError("Firebase secrets 不完整")
    cred = credentials.Certificate({
        "type": "service_account",
        "project_id": project_id,
        "private_key": private_key,
        "client_email": client_email,
        "token_uri": "https://oauth2.googleapis.com/token",
    })
    return firebase_admin.initialize_app(cred, {"projectId": project_id})


def _write_records_to_firestore(records: list[dict[str, Any]]) -> tuple[bool, str]:
    try:
        _init_firebase_app()
        db = firestore.client()
        batch = db.batch()
        now = firestore.SERVER_TIMESTAMP
        summary_ref = db.collection("system").document("godpick_records_summary")
        batch.set(summary_ref, {"count": len(records), "updated_at": now, "source": "streamlit_godpick_list"}, merge=True)
        records_ref = db.collection("godpick_records")
        existing = list(records_ref.stream())
        existing_ids = {doc.id for doc in existing}
        new_ids = set()
        for row in records:
            rec_id = _safe_str(row.get("record_id"))
            if not rec_id:
                continue
            new_ids.add(rec_id)
            doc_ref = records_ref.document(rec_id)
            payload = dict(row)
            payload["updated_at"] = now
            batch.set(doc_ref, payload, merge=True)
        for old_id in existing_ids - new_ids:
            batch.delete(records_ref.document(old_id))
        batch.commit()
        return True, "已同步寫入 Firestore"
    except Exception as e:
        return False, f"Firestore 同步失敗：{e}"


def _write_records_to_github(df: pd.DataFrame) -> tuple[bool, str]:
    cfg = _github_config()
    token = cfg["token"]
    if not token:
        return False, "未設定 GITHUB_TOKEN"
    sha, err = _get_records_sha()
    if err:
        return False, err
    work = _ensure_record_columns(df)
    content_text = json.dumps(work.to_dict(orient="records"), ensure_ascii=False, indent=2)
    encoded = base64.b64encode(content_text.encode("utf-8")).decode("utf-8")
    body: dict[str, Any] = {
        "message": f"update godpick records from 推薦清單 at {_now_text()}",
        "content": encoded,
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
        return False, f"GitHub 寫入失敗：{resp.status_code} / {resp.text[:500]}"
    except Exception as e:
        return False, f"GitHub 寫入例外：{e}"


def _sync_records(df: pd.DataFrame) -> tuple[bool, list[str]]:
    github_ok, github_msg = _write_records_to_github(df)
    fs_ok, fs_msg = _write_records_to_firestore(_ensure_record_columns(df).to_dict(orient="records"))
    st.session_state[_k("last_sync_msgs")] = [
        f"GitHub: {'成功' if github_ok else '失敗'} | {github_msg}",
        f"Firestore: {'成功' if fs_ok else '失敗'} | {fs_msg}",
    ]
    return (github_ok or fs_ok), st.session_state[_k("last_sync_msgs")]


def _load_records_cached(force: bool = False) -> pd.DataFrame:
    if force or _k("records_df") not in st.session_state:
        df, msg = _read_records_from_github()
        latest_df, latest_msg = _read_recommend_list_from_latest()

        frames = []
        if isinstance(df, pd.DataFrame) and not df.empty:
            df = df.copy()
            df["資料來源"] = "股神推薦紀錄"
            frames.append(df)
        if isinstance(latest_df, pd.DataFrame) and not latest_df.empty:
            latest_df = latest_df.copy()
            latest_df["資料來源"] = "本輪推薦清單"
            frames.append(latest_df)

        if frames:
            merged = pd.concat(frames, ignore_index=True)
            if "record_id" in merged.columns:
                merged = merged.drop_duplicates(subset=["record_id"], keep="last")
            else:
                merged = merged.drop_duplicates(subset=["股票代號", "推薦日期", "推薦時間", "推薦模式"], keep="last")
        else:
            merged = pd.DataFrame(columns=GODPICK_RECORD_COLUMNS)

        st.session_state[_k("records_df")] = _ensure_record_columns(merged).copy()
        st.session_state[_k("load_msg")] = f"{msg}｜{latest_msg}"
        st.session_state[_k("loaded_at")] = _now_text()
    rec = st.session_state.get(_k("records_df"), pd.DataFrame(columns=GODPICK_RECORD_COLUMNS))
    return _ensure_record_columns(rec)


def _filter_df(df: pd.DataFrame, start_date: date, end_date: date, mode: str, status: str, kw: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS)
    work = df.copy()
    work["推薦日期_dt"] = pd.to_datetime(work["推薦日期"], errors="coerce").dt.date
    if start_date:
        work = work[work["推薦日期_dt"] >= start_date]
    if end_date:
        work = work[work["推薦日期_dt"] <= end_date]
    if mode and mode != "全部":
        work = work[work["推薦模式"].astype(str) == mode]
    if status and status != "全部":
        work = work[work["目前狀態"].astype(str) == status]
    kw = _safe_str(kw)
    if kw:
        work = work[
            work["股票代號"].astype(str).str.contains(kw, case=False, na=False)
            | work["股票名稱"].astype(str).str.contains(kw, case=False, na=False)
            | work["推薦理由摘要"].astype(str).str.contains(kw, case=False, na=False)
            | work["類別"].astype(str).str.contains(kw, case=False, na=False)
        ]
    return work.sort_values(["推薦日期", "推薦時間", "推薦總分"], ascending=[False, False, False]).drop(columns=["推薦日期_dt"], errors="ignore").reset_index(drop=True)


def _format_show_df(df: pd.DataFrame) -> pd.DataFrame:
    show = df.copy()
    show = show.drop(columns=[c for c in ["record_id"] if c in show.columns])
    num1_cols = ["推薦總分", "技術結構分數", "起漲前兆分數", "交易可行分數", "類股熱度分數", "同類股領先幅度", "實際報酬%", "損益幅%"]
    price_cols = ["推薦價格", "停損價", "賣出目標1", "賣出目標2", "實際買進價", "實際賣出價", "最新價", "損益金額"]
    for c in num1_cols:
        if c in show.columns:
            show[c] = show[c].apply(lambda x: format_number(x, 1) if pd.notna(x) else "")
    for c in price_cols:
        if c in show.columns:
            show[c] = show[c].apply(lambda x: format_number(x, 2) if pd.notna(x) else "")
    return show


def _to_excel_bytes(df: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        _ensure_record_columns(df).to_excel(writer, sheet_name="推薦清單", index=False)
    return output.getvalue()


def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    inject_pro_theme()

    render_pro_hero(
        title="推薦清單",
        subtitle="集中查看股神推薦紀錄，支援日期篩選、批次刪除、匯出備份與 GitHub/Firestore 同步。",
        chips=["日期篩選", "批次刪除", "推薦分數", "GitHub 同步"],
    )

    if _k("last_sync_msgs") not in st.session_state:
        st.session_state[_k("last_sync_msgs")] = []

    with st.sidebar:
        st.subheader("操作區")
        reload_btn = st.button("重新讀取推薦清單", use_container_width=True, type="primary")
        if reload_btn:
            _load_records_cached(force=True)
        df = _load_records_cached(force=False)
        load_msg = _safe_str(st.session_state.get(_k("load_msg"), ""))
        if load_msg:
            st.caption(load_msg)
        st.caption(f"最近載入時間：{_safe_str(st.session_state.get(_k('loaded_at'), ''))}")

    df = _load_records_cached(force=False)

    if df.empty:
        render_pro_section("推薦清單資料")
        st.warning("目前沒有推薦紀錄。先到 7_股神推薦頁面把勾選結果寫入推薦紀錄。")
        return

    mode_options = ["全部"] + sorted([x for x in df["推薦模式"].dropna().astype(str).unique().tolist() if x])
    status_options = ["全部"] + sorted([x for x in df["目前狀態"].dropna().astype(str).unique().tolist() if x])
    rec_dates = pd.to_datetime(df["推薦日期"], errors="coerce").dropna()
    min_d = rec_dates.min().date() if not rec_dates.empty else (date.today() - timedelta(days=30))
    max_d = rec_dates.max().date() if not rec_dates.empty else date.today()

    c1, c2, c3, c4, c5 = st.columns([1.1, 1.1, 1.1, 1.1, 1.4])
    with c1:
        start_date = st.date_input("開始日期", value=min_d, key=_k("start_date"))
    with c2:
        end_date = st.date_input("結束日期", value=max_d, key=_k("end_date"))
    with c3:
        mode = st.selectbox("推薦模式", mode_options, key=_k("mode_filter"))
    with c4:
        status = st.selectbox("目前狀態", status_options, key=_k("status_filter"))
    with c5:
        kw = st.text_input("搜尋代號 / 名稱 / 類別 / 理由", key=_k("kw"))

    filtered_df = _filter_df(df, start_date, end_date, mode, status, kw)

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.metric("目前筆數", len(filtered_df))
    with k2:
        st.metric("平均推薦總分", format_number(filtered_df["推薦總分"].mean(), 1) if not filtered_df.empty else "0")
    with k3:
        st.metric("股神級 / 強烈關注", int(filtered_df["推薦等級"].isin(["股神級", "強烈關注"]).sum()) if not filtered_df.empty else 0)
    with k4:
        st.metric("達停損筆數", int(filtered_df["是否達停損"].fillna(False).sum()) if not filtered_df.empty else 0)

    render_pro_section("推薦清單明細")
    show_cols = [
        "資料來源", "推薦日期", "推薦時間", "股票代號", "股票名稱", "推薦模式", "推薦等級", "推薦總分", "飆股起漲分數", "起漲等級", "起漲摘要",
        "買點分級", "技術結構分數", "起漲前兆分數", "交易可行分數", "類股熱度分數",
        "推薦價格", "停損價", "賣出目標1", "賣出目標2", "最新價", "目前狀態",
        "股神推論邏輯", "風險說明", "推薦理由摘要", "備註"
    ]
    existing_cols = [c for c in show_cols if c in filtered_df.columns]
    st.dataframe(_format_show_df(filtered_df[existing_cols]), use_container_width=True, height=620)

    ex1, ex2 = st.columns(2)
    with ex1:
        st.download_button(
            label="下載目前篩選結果 Excel",
            data=_to_excel_bytes(filtered_df),
            file_name=f"推薦清單_{_now_text().replace(':','-').replace(' ','_')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
    with ex2:
        st.download_button(
            label="下載目前篩選結果 CSV",
            data=filtered_df.to_csv(index=False, encoding="utf-8-sig"),
            file_name=f"推薦清單_{_now_text().replace(':','-').replace(' ','_')}.csv",
            mime="text/csv",
            use_container_width=True,
        )

    render_pro_section("批次刪除")
    st.caption("這裡會依你上面目前的篩選條件，一次刪除符合條件的紀錄，不需要一筆一筆點。")
    d1, d2 = st.columns([1.4, 1])
    with d1:
        st.info(f"目前將刪除 {len(filtered_df)} 筆：日期 {start_date} ~ {end_date}，模式 {mode}，狀態 {status}，關鍵字 {_safe_str(kw) or '無'}")
    with d2:
        confirm_delete = st.checkbox("我確認要刪除目前篩選結果", key=_k("confirm_delete"))

    if st.button("批次刪除目前篩選結果", use_container_width=True, type="primary"):
        if filtered_df.empty:
            st.warning("目前沒有符合篩選條件的資料可刪除。")
        elif not confirm_delete:
            st.error("請先勾選確認刪除。")
        else:
            remain_df = df[~df["record_id"].astype(str).isin(filtered_df["record_id"].astype(str))].copy()
            ok, msgs = _sync_records(remain_df)
            if ok:
                st.session_state[_k("records_df")] = _ensure_record_columns(remain_df)
                st.success(f"已刪除 {len(filtered_df)} 筆推薦紀錄。")
            else:
                st.error("批次刪除失敗。")
            with st.expander("同步明細", expanded=False):
                for m in msgs:
                    st.write(f"- {m}")

    if st.session_state.get(_k("last_sync_msgs")):
        with st.expander("最近一次同步明細", expanded=False):
            for m in st.session_state[_k("last_sync_msgs")]:
                st.write(f"- {m}")


if __name__ == "__main__":
    main()
