from __future__ import annotations

from datetime import datetime, date
from typing import Any
import io
import json
import base64
import uuid

import pandas as pd
import requests
import streamlit as st
import firebase_admin
from firebase_admin import credentials, firestore

from utils import (
    format_number,
    get_realtime_stock_info,
    inject_pro_theme,
    render_pro_hero,
    render_pro_info_card,
    render_pro_kpi_row,
    render_pro_section,
)

PAGE_TITLE = "股神推薦紀錄"
PFX = "godlog_"


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


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _today_text() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _new_record_id() -> str:
    return uuid.uuid4().hex[:16]


def _set_status(msg: str, level: str = "info"):
    st.session_state[_k("status_msg")] = msg
    st.session_state[_k("status_type")] = level


# =========================================================
# GitHub 儲存
# =========================================================
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


def _read_records_from_github() -> tuple[list[dict[str, Any]], str]:
    cfg = _github_config()
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
            return [], f"讀取 GitHub 紀錄失敗：{resp.status_code} / {resp.text[:300]}"

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
        return [], f"讀取 GitHub 紀錄例外：{e}"


def _get_records_sha() -> tuple[str, str]:
    cfg = _github_config()
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
        return "", f"讀取 GitHub SHA 失敗：{resp.status_code} / {resp.text[:300]}"
    except Exception as e:
        return "", f"讀取 GitHub SHA 例外：{e}"


def _write_records_to_github(records: list[dict[str, Any]]) -> tuple[bool, str]:
    cfg = _github_config()
    token = cfg["token"]
    if not token:
        return False, "未設定 GITHUB_TOKEN"

    sha, err = _get_records_sha()
    if err:
        return False, err

    content_text = json.dumps(records, ensure_ascii=False, indent=2)
    encoded_content = base64.b64encode(content_text.encode("utf-8")).decode("utf-8")

    body = {
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
        return False, f"GitHub 寫入失敗：{resp.status_code} / {resp.text[:500]}"
    except Exception as e:
        return False, f"GitHub 寫入例外：{e}"


# =========================================================
# Firestore 儲存
# =========================================================
def _firebase_config() -> dict[str, str]:
    return {
        "project_id": _safe_str(st.secrets.get("FIREBASE_PROJECT_ID", "")),
        "client_email": _safe_str(st.secrets.get("FIREBASE_CLIENT_EMAIL", "")),
        "private_key": _safe_str(st.secrets.get("FIREBASE_PRIVATE_KEY", "")),
    }


def _clean_private_key(raw_key: str) -> str:
    private_key = _safe_str(raw_key).replace("\\n", "\n").strip()
    if private_key.startswith("\ufeff"):
        private_key = private_key.lstrip("\ufeff")
    return private_key


def _init_firebase_app():
    try:
        return firebase_admin.get_app()
    except ValueError:
        pass

    cfg = _firebase_config()
    project_id = _safe_str(cfg["project_id"])
    client_email = _safe_str(cfg["client_email"])
    private_key = _clean_private_key(cfg["private_key"])

    if not project_id:
        raise ValueError("缺少 FIREBASE_PROJECT_ID")
    if not client_email:
        raise ValueError("缺少 FIREBASE_CLIENT_EMAIL")
    if not private_key:
        raise ValueError("缺少 FIREBASE_PRIVATE_KEY")
    if "BEGIN PRIVATE KEY" not in private_key or "END PRIVATE KEY" not in private_key:
        raise ValueError("FIREBASE_PRIVATE_KEY 格式不正確")

    cred_dict = {
        "type": "service_account",
        "project_id": project_id,
        "private_key": private_key,
        "client_email": client_email,
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    cred = credentials.Certificate(cred_dict)
    return firebase_admin.initialize_app(cred, {"projectId": project_id})


def _write_records_to_firestore(records: list[dict[str, Any]]) -> tuple[bool, str]:
    try:
        _init_firebase_app()
        db = firestore.client()
        batch = db.batch()
        now = firestore.SERVER_TIMESTAMP

        summary_ref = db.collection("system").document("godpick_records_summary")
        batch.set(
            summary_ref,
            {
                "count": len(records),
                "updated_at": now,
                "source": "streamlit_godpick_records",
            },
            merge=True,
        )

        records_ref = db.collection("godpick_records")
        existing_docs = list(records_ref.stream())
        existing_ids = {doc.id for doc in existing_docs}
        new_ids = set()

        for row in records:
            rec_id = _safe_str(row.get("rec_id"))
            if not rec_id:
                continue
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
        return False, f"Firestore 寫入失敗：{e}"


# =========================================================
# 資料讀寫
# =========================================================
def _normalize_record(row: dict[str, Any]) -> dict[str, Any]:
    code = _normalize_code(row.get("股票代號"))
    name = _safe_str(row.get("股票名稱")) or code
    market = _safe_str(row.get("市場別")) or "上市"
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

    hit_stop = "否"
    if stop_price is not None and latest_price is not None and latest_price <= stop_price:
        hit_stop = "是"

    hit_target1 = "否"
    if target1 is not None and latest_price is not None and latest_price >= target1:
        hit_target1 = "是"

    hit_target2 = "否"
    if target2 is not None and latest_price is not None and latest_price >= target2:
        hit_target2 = "是"

    return {
        "rec_id": _safe_str(row.get("rec_id")) or _new_record_id(),
        "推薦日期": _safe_str(row.get("推薦日期")) or _today_text(),
        "推薦時間": _safe_str(row.get("推薦時間")) or _now_text(),
        "股票代號": code,
        "股票名稱": name,
        "市場別": market,
        "類別": _safe_str(row.get("類別")),
        "推薦模式": _safe_str(row.get("推薦模式")),
        "推薦等級": _safe_str(row.get("推薦等級")),
        "推薦總分": _safe_float(row.get("推薦總分")),
        "起漲前兆分數": _safe_float(row.get("起漲前兆分數")),
        "交易可行分數": _safe_float(row.get("交易可行分數")),
        "類股熱度分數": _safe_float(row.get("類股熱度分數")),
        "是否領先同類股": _safe_str(row.get("是否領先同類股")),
        "推薦價格": rec_price,
        "最新價": latest_price,
        "損益金額": pnl_amt,
        "損益幅%": pnl_pct,
        "停損價": stop_price,
        "賣出目標1": target1,
        "賣出目標2": target2,
        "是否達停損": hit_stop,
        "是否達目標1": hit_target1,
        "是否達目標2": hit_target2,
        "目前狀態": _safe_str(row.get("目前狀態")) or "觀察",
        "是否已實際買進": _safe_str(row.get("是否已實際買進")) or "否",
        "實際買進價": _safe_float(row.get("實際買進價")),
        "實際賣出價": _safe_float(row.get("實際賣出價")),
        "實際報酬%": _safe_float(row.get("實際報酬%")),
        "推薦理由摘要": _safe_str(row.get("推薦理由摘要")),
        "備註": _safe_str(row.get("備註")),
        "最後更新時間": _safe_str(row.get("最後更新時間")) or _now_text(),
    }


def _load_records() -> list[dict[str, Any]]:
    data = st.session_state.get(_k("records_data"))
    if isinstance(data, list):
        return copy.deepcopy(data)

    rows, err = _read_records_from_github()
    if err:
        _set_status(err, "warning")

    normalized = [_normalize_record(x) for x in rows if isinstance(x, dict)]
    st.session_state[_k("records_data")] = copy.deepcopy(normalized)
    return normalized


def _save_records(records: list[dict[str, Any]]) -> bool:
    normalized = [_normalize_record(x) for x in records if isinstance(x, dict)]

    ok_github, msg_github = _write_records_to_github(normalized)
    ok_firestore, msg_firestore = _write_records_to_firestore(normalized)

    st.session_state[_k("records_data")] = copy.deepcopy(normalized)
    st.session_state[_k("last_save_at")] = _now_text()
    st.session_state[_k("last_save_detail")] = [
        f"GitHub: {'成功' if ok_github else '失敗'} | {msg_github}",
        f"Firestore: {'成功' if ok_firestore else '失敗'} | {msg_firestore}",
    ]

    if ok_github and ok_firestore:
        _set_status("推薦紀錄已同步到 GitHub + Firestore", "success")
        return True
    if ok_github or ok_firestore:
        _set_status("推薦紀錄部分同步成功", "warning")
        return True

    _set_status("推薦紀錄同步失敗", "error")
    return False


# =========================================================
# 更新最新價格
# =========================================================
def _update_records_market_price(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    updated = []

    progress = st.progress(0, text="準備更新價格...")
    total = len(records)

    for idx, row in enumerate(records, start=1):
        temp = dict(row)
        code = _normalize_code(temp.get("股票代號"))
        name = _safe_str(temp.get("股票名稱"))
        market = _safe_str(temp.get("市場別")) or "上市"

        if code:
            try:
                info = get_realtime_stock_info(code, name, market)
                latest_price = _safe_float(info.get("price"))
                if latest_price is not None:
                    temp["最新價"] = latest_price
            except Exception:
                pass

        temp = _normalize_record(temp)
        temp["最後更新時間"] = _now_text()
        updated.append(temp)

        ratio = idx / total if total > 0 else 1
        progress.progress(ratio, text=f"更新中... {idx}/{total}")

    progress.progress(1.0, text="價格更新完成")
    return updated


# =========================================================
# 分析表
# =========================================================
def _build_analysis_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    total = len(df)
    profit_cnt = int((pd.to_numeric(df["損益幅%"], errors="coerce") > 0).sum())
    loss_cnt = int((pd.to_numeric(df["損益幅%"], errors="coerce") < 0).sum())
    avg_pnl = pd.to_numeric(df["損益幅%"], errors="coerce").mean()
    avg_score = pd.to_numeric(df["推薦總分"], errors="coerce").mean()

    rows = [
        {"指標": "總筆數", "數值": total},
        {"指標": "獲利筆數", "數值": profit_cnt},
        {"指標": "虧損筆數", "數值": loss_cnt},
        {"指標": "勝率(%)", "數值": round((profit_cnt / total) * 100, 2) if total > 0 else 0},
        {"指標": "平均損益幅(%)", "數值": round(avg_pnl, 2) if pd.notna(avg_pnl) else None},
        {"指標": "平均推薦總分", "數值": round(avg_score, 2) if pd.notna(avg_score) else None},
    ]
    return pd.DataFrame(rows)


def _build_group_analysis(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    if df.empty or group_col not in df.columns:
        return pd.DataFrame()

    work = df.copy()
    work["損益幅%"] = pd.to_numeric(work["損益幅%"], errors="coerce")
    work["推薦總分"] = pd.to_numeric(work["推薦總分"], errors="coerce")

    out = (
        work.groupby(group_col, dropna=False)
        .agg(
            筆數=("rec_id", "count"),
            平均推薦總分=("推薦總分", "mean"),
            平均損益幅=("損益幅%", "mean"),
            最大獲利=("損益幅%", "max"),
            最大虧損=("損益幅%", "min"),
        )
        .reset_index()
    )

    win_rate = (
        work.assign(獲利=work["損益幅%"] > 0)
        .groupby(group_col)["獲利"]
        .mean()
        .reset_index(name="勝率")
    )

    out = out.merge(win_rate, on=group_col, how="left")
    out["勝率"] = out["勝率"] * 100
    out = out.sort_values(["平均損益幅", "勝率"], ascending=[False, False]).reset_index(drop=True)
    return out


# =========================================================
# Excel 匯出
# =========================================================
@st.cache_data(ttl=120, show_spinner=False)
def _build_excel_bytes(records_df: pd.DataFrame, analysis_df: pd.DataFrame, category_df: pd.DataFrame, mode_df: pd.DataFrame) -> bytes:
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        records_df.to_excel(writer, sheet_name="推薦紀錄", index=False)
        analysis_df.to_excel(writer, sheet_name="績效分析", index=False)
        category_df.to_excel(writer, sheet_name="類別分析", index=False)
        mode_df.to_excel(writer, sheet_name="模式分析", index=False)

        try:
            for ws in writer.book.worksheets:
                ws.freeze_panes = "A2"
                for col_cells in ws.columns:
                    max_len = 0
                    col_letter = col_cells[0].column_letter
                    for cell in col_cells:
                        cell_val = "" if cell.value is None else str(cell.value)
                        max_len = max(max_len, len(cell_val))
                    ws.column_dimensions[col_letter].width = min(max(max_len + 2, 10), 40)
        except Exception:
            pass

    output.seek(0)
    return output.getvalue()


# =========================================================
# 主頁
# =========================================================
def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    inject_pro_theme()

    if _k("status_msg") not in st.session_state:
        st.session_state[_k("status_msg")] = ""
    if _k("status_type") not in st.session_state:
        st.session_state[_k("status_type")] = "info"

    records = _load_records()
    df = pd.DataFrame(records)

    render_pro_hero(
        title="股神推薦紀錄",
        subtitle="記錄 7_股神推薦 的推薦結果、每日更新損益、分析勝率與匯出 Excel。",
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

    if _safe_str(st.session_state.get(_k("last_save_at"), "")):
        st.caption(f"最後同步時間：{st.session_state.get(_k('last_save_at'))}")

    render_pro_section("快捷操作")
    c1, c2, c3 = st.columns(3)

    with c1:
        if st.button("更新全部最新價格", use_container_width=True, type="primary"):
            updated = _update_records_market_price(records)
            if _save_records(updated):
                df = pd.DataFrame(updated)
                st.rerun()

    with c2:
        if st.button("重新讀取紀錄", use_container_width=True):
            st.session_state.pop(_k("records_data"), None)
            st.rerun()

    with c3:
        st.caption("下一步再接 7_股神推薦 勾選後直接寫入這裡")

    render_pro_section("手動新增推薦紀錄")
    with st.form(_k("manual_add_form"), clear_on_submit=False):
        a1, a2, a3, a4 = st.columns(4)
        with a1:
            stock_code = st.text_input("股票代號")
        with a2:
            stock_name = st.text_input("股票名稱")
        with a3:
            market = st.selectbox("市場別", ["上市", "上櫃", "興櫃"])
        with a4:
            category = st.text_input("類別")

        b1, b2, b3, b4 = st.columns(4)
        with b1:
            rec_mode = st.selectbox("推薦模式", ["飆股模式", "波段模式", "領頭羊模式", "綜合模式"])
        with b2:
            rec_level = st.selectbox("推薦等級", ["強烈關注", "優先觀察", "可列追蹤", "觀察"])
        with b3:
            rec_score = st.number_input("推薦總分", value=70.0, step=1.0)
        with b4:
            rec_price = st.number_input("推薦價格", value=0.0, step=0.1)

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            prelaunch_score = st.number_input("起漲前兆分數", value=0.0, step=1.0)
        with c2:
            trade_score = st.number_input("交易可行分數", value=0.0, step=1.0)
        with c3:
            category_heat = st.number_input("類股熱度分數", value=0.0, step=1.0)
        with c4:
            leader = st.selectbox("是否領先同類股", ["是", "否"])

        d1, d2, d3 = st.columns(3)
        with d1:
            stop_price = st.number_input("停損價", value=0.0, step=0.1)
        with d2:
            target1 = st.number_input("賣出目標1", value=0.0, step=0.1)
        with d3:
            target2 = st.number_input("賣出目標2", value=0.0, step=0.1)

        reason = st.text_area("推薦理由摘要", height=80)
        note = st.text_area("備註", height=80)

        submit_add = st.form_submit_button("新增推薦紀錄", use_container_width=True, type="primary")

    if submit_add:
        row = _normalize_record(
            {
                "rec_id": _new_record_id(),
                "推薦日期": _today_text(),
                "推薦時間": _now_text(),
                "股票代號": stock_code,
                "股票名稱": stock_name,
                "市場別": market,
                "類別": category,
                "推薦模式": rec_mode,
                "推薦等級": rec_level,
                "推薦總分": rec_score,
                "起漲前兆分數": prelaunch_score,
                "交易可行分數": trade_score,
                "類股熱度分數": category_heat,
                "是否領先同類股": leader,
                "推薦價格": rec_price,
                "最新價": rec_price,
                "停損價": stop_price if stop_price > 0 else None,
                "賣出目標1": target1 if target1 > 0 else None,
                "賣出目標2": target2 if target2 > 0 else None,
                "目前狀態": "觀察",
                "推薦理由摘要": reason,
                "備註": note,
            }
        )
        records.append(row)
        if _save_records(records):
            st.rerun()

    if df.empty:
        st.info("目前沒有任何推薦紀錄。")
        return

    df["損益幅%"] = pd.to_numeric(df["損益幅%"], errors="coerce")
    df["推薦總分"] = pd.to_numeric(df["推薦總分"], errors="coerce")

    total_count = len(df)
    profit_count = int((df["損益幅%"] > 0).sum())
    loss_count = int((df["損益幅%"] < 0).sum())
    avg_pnl = df["損益幅%"].mean()

    render_pro_kpi_row(
        [
            {
                "label": "推薦總筆數",
                "value": total_count,
                "delta": "全部紀錄",
                "delta_class": "pro-kpi-delta-flat",
            },
            {
                "label": "獲利筆數",
                "value": profit_count,
                "delta": f"勝率 {((profit_count/total_count)*100):.2f}%" if total_count > 0 else "0%",
                "delta_class": "pro-kpi-delta-flat",
            },
            {
                "label": "虧損筆數",
                "value": loss_count,
                "delta": "目前更新結果",
                "delta_class": "pro-kpi-delta-flat",
            },
            {
                "label": "平均損益幅",
                "value": f"{avg_pnl:+.2f}%" if pd.notna(avg_pnl) else "—",
                "delta": "每日更新",
                "delta_class": "pro-kpi-delta-flat",
            },
        ]
    )

    analysis_df = _build_analysis_df(df)
    category_df = _build_group_analysis(df, "類別")
    mode_df = _build_group_analysis(df, "推薦模式")

    render_pro_section("Excel 匯出")
    excel_bytes = _build_excel_bytes(df, analysis_df, category_df, mode_df)
    st.download_button(
        label="匯出推薦紀錄 Excel",
        data=excel_bytes,
        file_name=f"股神推薦紀錄_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

    tabs = st.tabs(["推薦紀錄", "績效分析", "類別分析", "模式分析", "紀錄建議"])

    with tabs[0]:
        st.dataframe(df, use_container_width=True, hide_index=True)

    with tabs[1]:
        st.dataframe(analysis_df, use_container_width=True, hide_index=True)

    with tabs[2]:
        st.dataframe(category_df, use_container_width=True, hide_index=True)

    with tabs[3]:
        st.dataframe(mode_df, use_container_width=True, hide_index=True)

    with tabs[4]:
        render_pro_info_card(
            "建議你再增加的紀錄欄位",
            [
                ("是否已實際買進", "區分系統推薦和你真正下單。", ""),
                ("實際買進價", "分析是系統準還是買點偏掉。", ""),
                ("實際賣出價", "之後可回算真實績效。", ""),
                ("實際報酬%", "與系統理論績效分開看。", ""),
                ("持有天數", "可以判斷短打和波段哪種最有效。", ""),
                ("是否先達停損", "看風控規則是否合理。", ""),
                ("是否先達目標價", "驗證目標價設計是否有效。", ""),
                ("手動備註", "記錄為什麼沒買、為什麼提早賣。", ""),
            ],
            chips=["績效追蹤", "股神進化", "下一版建議"],
        )

    details = st.session_state.get(_k("last_save_detail"), [])
    if details:
        with st.expander("同步明細", expanded=False):
            for line in details:
                st.write(f"- {line}")


if __name__ == "__main__":
    main()
