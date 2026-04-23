# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, date, timedelta
from typing import Any
import base64
import hashlib
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


# ------------------------------
# 基本工具
# ------------------------------
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


def _safe_bool(v: Any) -> bool:
    s = _safe_str(v).lower()
    return s in {"true", "1", "yes", "y", "是"}


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


def _today_text() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _time_text() -> str:
    return datetime.now().strftime("%H:%M:%S")


# ------------------------------
# GitHub / Firestore
# ------------------------------
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


# ------------------------------
# 資料整形
# ------------------------------
def ensure_record_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=GODPICK_RECORD_COLUMNS)
    x = df.copy()
    if "record_id" not in x.columns and "rec_id" in x.columns:
        x["record_id"] = x["rec_id"]
    for c in GODPICK_RECORD_COLUMNS:
        if c not in x.columns:
            x[c] = None
    num_cols = [
        "推薦總分", "技術結構分數", "起漲前兆分數", "交易可行分數", "類股熱度分數",
        "同類股領先幅度", "推薦價格", "停損價", "賣出目標1", "賣出目標2",
        "實際買進價", "實際賣出價", "實際報酬%", "最新價", "損益金額", "損益幅%", "持有天數"
    ]
    for c in num_cols:
        x[c] = pd.to_numeric(x[c], errors="coerce")
    bool_cols = ["是否領先同類股", "是否已實際買進", "是否達停損", "是否達目標1", "是否達目標2"]
    for c in bool_cols:
        x[c] = x[c].fillna(False).map(_safe_bool)
    for c in ["推薦日期", "推薦時間", "建立時間", "更新時間", "最新更新時間", "目前狀態", "模式績效標籤", "備註"]:
        x[c] = x[c].fillna("").astype(str)
    x["股票代號"] = x["股票代號"].map(_normalize_code)
    x["股票名稱"] = x["股票名稱"].fillna("").astype(str)
    return x[GODPICK_RECORD_COLUMNS].copy()


def _pick_first(row: pd.Series, names: list[str], default=""):
    for n in names:
        if n in row.index:
            v = row.get(n)
            try:
                if pd.notna(v) and str(v).strip() != "":
                    return v
            except Exception:
                if v is not None and str(v).strip() != "":
                    return v
    return default


def _build_level_by_score(score: float | None) -> str:
    if score is None:
        return ""
    if score >= 90:
        return "股神級"
    if score >= 80:
        return "強烈關注"
    if score >= 70:
        return "可追蹤"
    return "觀察"


def build_record_from_row(row: pd.Series, mode_name: str = "股神推薦") -> dict[str, Any]:
    code = _normalize_code(_pick_first(row, ["股票代號", "代號", "code", "stock_id"]))
    name = _safe_str(_pick_first(row, ["股票名稱", "名稱", "name", "stock_name"]))
    market = _safe_str(_pick_first(row, ["市場別", "市場", "market"]))
    category = _safe_str(_pick_first(row, ["類別", "產業", "產業別", "category"]))

    total_score = _safe_float(_pick_first(row, ["推薦總分", "總分", "score", "final_score"]), None)
    tech_score = _safe_float(_pick_first(row, ["技術結構分數", "技術分數", "technical_score"]), None)
    surge_score = _safe_float(_pick_first(row, ["起漲前兆分數", "起漲分數", "surge_score"]), None)
    trade_score = _safe_float(_pick_first(row, ["交易可行分數", "交易分數", "trade_score"]), None)
    heat_score = _safe_float(_pick_first(row, ["類股熱度分數", "熱度分數", "heat_score"]), None)

    price = _safe_float(_pick_first(row, ["推薦價格", "現價", "最新價", "close", "price"]), None)
    stop_loss = _safe_float(_pick_first(row, ["停損價", "stop_loss"]), None)
    target1 = _safe_float(_pick_first(row, ["賣出目標1", "目標1", "target1"]), None)
    target2 = _safe_float(_pick_first(row, ["賣出目標2", "目標2", "target2"]), None)

    level = _safe_str(_pick_first(row, ["推薦等級", "等級", "level"])) or _build_level_by_score(total_score)
    tags = _safe_str(_pick_first(row, ["推薦標籤", "標籤", "tags"]))
    reason = _safe_str(_pick_first(row, ["推薦理由摘要", "推薦理由", "理由摘要", "reason"]))
    leader_gap = _safe_float(_pick_first(row, ["同類股領先幅度", "領先幅度"]), None)
    is_leader = _safe_bool(_pick_first(row, ["是否領先同類股", "領先同類股"]))

    today = _today_text()
    now_time = _time_text()
    now_text = _now_text()
    rec_key = f"{today}|{mode_name}|{code}|{name}"
    rec_id = hashlib.md5(rec_key.encode("utf-8")).hexdigest()

    return {
        "record_id": rec_id,
        "股票代號": code,
        "股票名稱": name,
        "市場別": market,
        "類別": category,
        "推薦模式": mode_name,
        "推薦等級": level,
        "推薦總分": total_score,
        "技術結構分數": tech_score,
        "起漲前兆分數": surge_score,
        "交易可行分數": trade_score,
        "類股熱度分數": heat_score,
        "同類股領先幅度": leader_gap,
        "是否領先同類股": is_leader,
        "推薦標籤": tags,
        "推薦理由摘要": reason,
        "推薦價格": price,
        "停損價": stop_loss,
        "賣出目標1": target1,
        "賣出目標2": target2,
        "推薦日期": today,
        "推薦時間": now_time,
        "建立時間": now_text,
        "更新時間": now_text,
        "目前狀態": "新推薦",
        "是否已實際買進": False,
        "實際買進價": None,
        "實際賣出價": None,
        "實際報酬%": None,
        "最新價": price,
        "最新更新時間": now_text,
        "損益金額": None,
        "損益幅%": None,
        "是否達停損": False,
        "是否達目標1": False,
        "是否達目標2": False,
        "持有天數": 0,
        "模式績效標籤": "",
        "備註": "",
    }


# ------------------------------
# 讀寫推薦清單
# ------------------------------
def read_records_from_github() -> tuple[pd.DataFrame, str]:
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
        return ensure_record_columns(pd.DataFrame(payload)), ""
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


def _write_records_to_github(df: pd.DataFrame) -> tuple[bool, str]:
    cfg = _github_config()
    token = cfg["token"]
    if not token:
        return False, "未設定 GITHUB_TOKEN"
    sha, err = _get_records_sha()
    if err:
        return False, err
    work = ensure_record_columns(df)
    content_text = json.dumps(work.to_dict(orient="records"), ensure_ascii=False, indent=2)
    encoded = base64.b64encode(content_text.encode("utf-8")).decode("utf-8")
    body: dict[str, Any] = {
        "message": f"update godpick records from 股神推薦 at {_now_text()}",
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


def _write_records_to_firestore(records: list[dict[str, Any]]) -> tuple[bool, str]:
    try:
        _init_firebase_app()
        db = firestore.client()
        batch = db.batch()
        now = firestore.SERVER_TIMESTAMP
        summary_ref = db.collection("system").document("godpick_records_summary")
        batch.set(summary_ref, {"count": len(records), "updated_at": now, "source": "godpick_record_service"}, merge=True)
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


def sync_records(df: pd.DataFrame) -> tuple[bool, list[str]]:
    github_ok, github_msg = _write_records_to_github(df)
    fs_ok, fs_msg = _write_records_to_firestore(ensure_record_columns(df).to_dict(orient="records"))
    msgs = [
        f"GitHub: {'成功' if github_ok else '失敗'} | {github_msg}",
        f"Firestore: {'成功' if fs_ok else '失敗'} | {fs_msg}",
    ]
    return (github_ok or fs_ok), msgs


# ------------------------------
# 對外主函式
# ------------------------------
def save_recommendations_to_list(
    result_df: pd.DataFrame,
    mode_name: str = "股神推薦",
    only_selected: bool = False,
    selected_codes: list[str] | None = None,
    keep_latest_same_day: bool = True,
) -> tuple[bool, str, int, list[str]]:
    if result_df is None or result_df.empty:
        return False, "沒有可寫入的推薦結果", 0, []

    work = result_df.copy()

    code_col = None
    for c in ["股票代號", "代號", "code", "stock_id"]:
        if c in work.columns:
            code_col = c
            break

    if code_col is None:
        return False, "推薦結果缺少股票代號欄位，無法寫入推薦清單", 0, []

    if only_selected:
        selected_codes = [_normalize_code(x) for x in (selected_codes or []) if _normalize_code(x)]
        if not selected_codes:
            return False, "你目前沒有勾選任何股票", 0, []
        work = work[work[code_col].astype(str).map(_normalize_code).isin(selected_codes)].copy()

    if work.empty:
        return False, "目前沒有符合條件的股票可寫入推薦清單", 0, []

    old_df, load_msg = read_records_from_github()
    if load_msg and ("失敗" in load_msg or "例外" in load_msg):
        return False, load_msg, 0, []

    new_records: list[dict[str, Any]] = []
    for _, row in work.iterrows():
        rec = build_record_from_row(row, mode_name=mode_name)
        if rec["股票代號"]:
            new_records.append(rec)

    if not new_records:
        return False, "沒有成功轉換成推薦紀錄的資料", 0, []

    new_df = ensure_record_columns(pd.DataFrame(new_records))
    all_df = pd.concat([old_df, new_df], ignore_index=True)
    all_df = ensure_record_columns(all_df)

    if keep_latest_same_day:
        all_df["__keep_key__"] = (
            all_df["推薦日期"].astype(str) + "|" +
            all_df["推薦模式"].astype(str) + "|" +
            all_df["股票代號"].astype(str)
        )
        all_df = all_df.sort_values(["更新時間", "推薦時間"], ascending=[True, True])
        all_df = all_df.drop_duplicates(subset=["__keep_key__"], keep="last").drop(columns=["__keep_key__"])

    ok, msgs = sync_records(all_df)
    if ok:
        return True, "已寫入推薦清單", len(new_df), msgs
    return False, "寫入推薦清單失敗", 0, msgs


def render_save_to_list_block(
    result_df: pd.DataFrame,
    mode_name: str = "股神推薦",
    editor_key_prefix: str = "godpick_record",
    default_auto_save: bool = True,
) -> None:
    st.markdown("### 推薦清單紀錄")

    if result_df is None or result_df.empty:
        st.info("目前沒有推薦結果可記錄。")
        return

    code_col = None
    for c in ["股票代號", "代號", "code", "stock_id"]:
        if c in result_df.columns:
            code_col = c
            break

    if code_col is None:
        st.warning("推薦結果缺少股票代號欄位，無法啟用推薦清單紀錄。")
        return

    radio_key = f"{editor_key_prefix}_save_mode"
    auto_key = f"{editor_key_prefix}_auto_save"
    editor_key = f"{editor_key_prefix}_editor"
    btn_key = f"{editor_key_prefix}_save_btn"

    save_mode = st.radio(
        "寫入方式",
        ["本次全部推薦直接寫入推薦清單", "只寫入我勾選的股票"],
        horizontal=True,
        key=radio_key,
    )

    preview_df = result_df.copy()
    if "寫入推薦清單" not in preview_df.columns:
        preview_df.insert(0, "寫入推薦清單", False)

    edited_df = st.data_editor(
        preview_df,
        use_container_width=True,
        hide_index=True,
        key=editor_key,
        height=360,
    )

    selected_codes = (
        edited_df.loc[edited_df["寫入推薦清單"] == True, code_col]
        .astype(str)
        .map(_normalize_code)
        .tolist()
    )

    c1, c2 = st.columns([1, 1])
    with c1:
        st.checkbox("推薦完成後自動寫入推薦清單", value=default_auto_save, key=auto_key)
    with c2:
        if st.button("立即寫入推薦清單", use_container_width=True, type="primary", key=btn_key):
            ok, msg, cnt, msgs = save_recommendations_to_list(
                result_df=result_df,
                mode_name=mode_name,
                only_selected=(save_mode == "只寫入我勾選的股票"),
                selected_codes=selected_codes,
            )
            if ok:
                st.success(f"已寫入推薦清單：{cnt} 筆｜{msg}")
            else:
                st.error(msg)
            with st.expander("同步明細", expanded=False):
                for m in msgs:
                    st.write(f"- {m}")


def auto_save_after_recommend(
    result_df: pd.DataFrame,
    mode_name: str = "股神推薦",
    editor_key_prefix: str = "godpick_record",
    selected_codes: list[str] | None = None,
) -> tuple[bool, str, int, list[str]]:
    auto_key = f"{editor_key_prefix}_auto_save"
    radio_key = f"{editor_key_prefix}_save_mode"
    auto_save = bool(st.session_state.get(auto_key, True))
    save_mode = st.session_state.get(radio_key, "本次全部推薦直接寫入推薦清單")
    if not auto_save:
        return False, "已關閉自動寫入", 0, []

    return save_recommendations_to_list(
        result_df=result_df,
        mode_name=mode_name,
        only_selected=(save_mode == "只寫入我勾選的股票"),
        selected_codes=selected_codes,
    )


INTEGRATION_EXAMPLE = r'''
# 1) 在 7_股神推薦.py import
from godpick_record_service import render_save_to_list_block, auto_save_after_recommend

# 2) 你的推薦結果 DataFrame 假設叫 result_df
#    在顯示推薦結果表格後，直接放這段
render_save_to_list_block(result_df, mode_name="股神推薦", editor_key_prefix="page7")

# 3) 如果你要按下推薦後就自動記錄，
#    在 result_df 產生後加這段：
ok, msg, cnt, msgs = auto_save_after_recommend(result_df, mode_name="股神推薦", editor_key_prefix="page7")
if ok:
    st.success(f"本次推薦已自動記錄到推薦清單：{cnt} 筆")
elif msg not in {"已關閉自動寫入", "你目前沒有勾選任何股票"}:
    st.warning(msg)
'''
