# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
import importlib
import json
import os
import sys
import traceback

import pandas as pd
import streamlit as st

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
PFX = "diag_"


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


def _fmt_dt(ts: float | None) -> str:
    if not ts:
        return ""
    try:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""


def _read_json_safe(path: Path) -> tuple[bool, Any, str]:
    try:
        if not path.exists():
            return False, None, "檔案不存在"
        txt = path.read_text(encoding="utf-8-sig")
        if not txt.strip():
            return False, None, "檔案空白"
        return True, json.loads(txt), ""
    except Exception as e:
        return False, None, str(e)


def _json_count(data: Any) -> str:
    if isinstance(data, list):
        return f"list：{len(data)} 筆"
    if isinstance(data, dict):
        return f"dict：{len(data)} 個鍵"
    return type(data).__name__


def _file_row(name: str, path: Path, must_exist: bool = True) -> dict[str, Any]:
    exists = path.exists()
    size = round(path.stat().st_size / 1024, 2) if exists and path.is_file() else ""
    status = "正常" if exists or not must_exist else "缺少"
    return {
        "項目": name,
        "狀態": status,
        "是否存在": exists,
        "大小KB": size,
        "最後修改": _fmt_dt(path.stat().st_mtime) if exists else "",
        "路徑": str(path),
    }


def _try_import(module_name: str):
    try:
        if str(BASE_DIR) not in sys.path:
            sys.path.insert(0, str(BASE_DIR))
        return importlib.import_module(module_name), ""
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"


def _clear_streamlit_caches() -> list[str]:
    msgs = []
    try:
        st.cache_data.clear()
        msgs.append("st.cache_data 已清除")
    except Exception as e:
        msgs.append(f"st.cache_data 清除失敗：{e}")
    try:
        st.cache_resource.clear()
        msgs.append("st.cache_resource 已清除")
    except Exception as e:
        msgs.append(f"st.cache_resource 清除失敗：{e}")
    return msgs


def _render_metric_card(title: str, value: str, note: str = "", tone: str = "info"):
    colors = {
        "ok": ("#ecfdf5", "#10b981", "#065f46"),
        "warn": ("#fffbeb", "#f59e0b", "#92400e"),
        "bad": ("#fef2f2", "#ef4444", "#991b1b"),
        "info": ("#eff6ff", "#3b82f6", "#1e3a8a"),
    }
    bg, border, text = colors.get(tone, colors["info"])
    st.markdown(
        f"""
        <div style="background:{bg};border:1px solid {border};border-radius:18px;padding:14px 16px;margin-bottom:10px;">
          <div style="font-size:13px;color:{text};font-weight:800;">{title}</div>
          <div style="font-size:24px;color:#0f172a;font-weight:900;line-height:1.35;">{value}</div>
          <div style="font-size:12px;color:#475569;">{note}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


try:
    from utils import inject_pro_theme, render_pro_hero
except Exception:
    def inject_pro_theme():
        pass

    def render_pro_hero(title: str, subtitle: str = ""):
        st.title(title)
        if subtitle:
            st.caption(subtitle)


st.set_page_config(page_title="資料診斷", layout="wide")
inject_pro_theme()
render_pro_hero("資料診斷｜v55 全系統健康檢查", "檢查 v45 大盤、v47 資料源、v48 推薦速度、v49 自選股同步、v50-v53 推薦績效與 JSON 串聯狀態；v55 可一鍵產生 runtime 診斷檔。")

c1, c2, c3, c4 = st.columns(4)
with c1:
    _render_metric_card("專案根目錄", "存在" if BASE_DIR.exists() else "缺少", str(BASE_DIR), "ok" if BASE_DIR.exists() else "bad")
with c2:
    _render_metric_card("pages 目錄", "存在" if (BASE_DIR / "pages").exists() else "缺少", str(BASE_DIR / "pages"), "ok" if (BASE_DIR / "pages").exists() else "bad")
with c3:
    _render_metric_card("Python", sys.version.split()[0], sys.executable, "info")
with c4:
    _render_metric_card("Streamlit Session", f"{len(st.session_state)} keys", "用於檢查換頁後狀態是否保留", "info")

st.markdown("---")

with st.sidebar:
    st.subheader("診斷操作")
    if st.button("清除 Streamlit 快取", use_container_width=True):
        st.session_state[_k("cache_clear_msgs")] = _clear_streamlit_caches()
        st.success("已執行快取清除")
    if _k("cache_clear_msgs") in st.session_state:
        for msg in st.session_state[_k("cache_clear_msgs")]:
            st.caption(msg)

    st.markdown("---")
    st.subheader("測試股票")
    test_code = st.text_input("股票代號", value=st.session_state.get(_k("test_code"), "3548"), key=_k("test_code"))
    test_name = st.text_input("股票名稱", value=st.session_state.get(_k("test_name"), "兆利"), key=_k("test_name"))
    test_market = st.selectbox("市場別", ["上市", "上櫃", "興櫃"], index=1, key=_k("test_market"))
    test_days = st.slider("歷史資料測試天數", min_value=30, max_value=730, value=180, step=30, key=_k("test_days"))

json_files = [
    ("stock_master_cache.json", BASE_DIR / "stock_master_cache.json", True),
    ("data/stock_master_cache.json", DATA_DIR / "stock_master_cache.json", False),
    ("watchlist.json", BASE_DIR / "watchlist.json", True),
    ("godpick_user_settings.json", BASE_DIR / "godpick_user_settings.json", False),
    ("godpick_records.json", BASE_DIR / "godpick_records.json", False),
    ("godpick_record_ui_config.json", BASE_DIR / "godpick_record_ui_config.json", False),
    ("godpick_recommend_list.json", BASE_DIR / "godpick_recommend_list.json", False),
    ("godpick_latest_recommendations.json", BASE_DIR / "godpick_latest_recommendations.json", False),
    ("macro_trend_records.json", BASE_DIR / "macro_trend_records.json", False),
    ("data_source_diagnostics.json", BASE_DIR / "data_source_diagnostics.json", False),
    ("watchlist_runtime_snapshot.json", BASE_DIR / "watchlist_runtime_snapshot.json", False),
    ("watchlist_normalized.json", BASE_DIR / "watchlist_normalized.json", False),
    ("last_query_state.json", BASE_DIR / "last_query_state.json", False),
]

st.subheader("1. 關鍵檔案存在狀態")
file_rows = [
    _file_row("utils.py", BASE_DIR / "utils.py"),
    _file_row("stock_master_service.py", BASE_DIR / "stock_master_service.py"),
    _file_row("streamlit_app.py", BASE_DIR / "streamlit_app.py"),
    _file_row("requirements.txt", BASE_DIR / "requirements.txt"),
]
file_rows += [_file_row(name, path, must) for name, path, must in json_files]
st.dataframe(pd.DataFrame(file_rows), use_container_width=True, hide_index=True)

st.subheader("2. JSON 讀取檢查")
json_rows = []
json_data_map: dict[str, Any] = {}
for name, path, _must in json_files:
    ok, data, err = _read_json_safe(path)
    if ok:
        json_data_map[name] = data
    json_rows.append({
        "檔案": name,
        "狀態": "OK" if ok else "錯誤 / 不存在",
        "型態與筆數": _json_count(data) if ok else "",
        "錯誤訊息": err,
        "路徑": str(path),
    })
st.dataframe(pd.DataFrame(json_rows), use_container_width=True, hide_index=True)

bad_json = [r for r in json_rows if r["狀態"] != "OK" and r["檔案"] in {"stock_master_cache.json", "watchlist.json"}]
if bad_json:
    st.error("主檔或自選股 JSON 有異常，可能造成行情、歷史K線、股神推薦、推薦清單串聯失敗。")
else:
    st.success("主檔與自選股 JSON 基本讀取正常。")

st.subheader("3. 頁面檔案與重複頁檢查")
pages_dir = BASE_DIR / "pages"
page_rows = []
if pages_dir.exists():
    for p in sorted(pages_dir.glob("*.py")):
        page_rows.append({
            "檔名": p.name,
            "大小KB": round(p.stat().st_size / 1024, 2),
            "最後修改": _fmt_dt(p.stat().st_mtime),
            "路徑": str(p),
        })
page_df = pd.DataFrame(page_rows)
st.dataframe(page_df, use_container_width=True, hide_index=True)

if not page_df.empty:
    duplicated_no = page_df["檔名"].str.extract(r"^(\d+)_")[0].dropna().value_counts()
    duplicated_no = duplicated_no[duplicated_no > 1]
    if not duplicated_no.empty:
        st.warning("偵測到相同頁碼可能重複，Streamlit 側邊欄可能出現兩個相近頁面，請確認是否同時存在中文檔名與 #U 編碼檔名。")
        st.dataframe(duplicated_no.rename("重複數").reset_index().rename(columns={"index": "頁碼"}), hide_index=True)

st.subheader("4. 共用模組匯入檢查")
module_rows = []
utils_mod, utils_err = _try_import("utils")
stock_mod, stock_err = _try_import("stock_master_service")
for name, mod, err in [("utils", utils_mod, utils_err), ("stock_master_service", stock_mod, stock_err)]:
    module_rows.append({
        "模組": name,
        "狀態": "OK" if mod is not None else "匯入失敗",
        "錯誤": err,
        "檔案": getattr(mod, "__file__", "") if mod is not None else "",
    })
st.dataframe(pd.DataFrame(module_rows), use_container_width=True, hide_index=True)

expected_utils_funcs = [
    "safe_read_json", "safe_write_json", "load_watchlist", "save_watchlist", "get_normalized_watchlist",
    "get_history_data", "get_realtime_stock_info", "get_realtime_quotes", "inject_pro_theme",
]
expected_master_funcs = [
    "load_stock_master", "refresh_stock_master", "search_stock_master", "get_stock_master_categories", "get_stock_master_diagnostics",
]

func_rows = []
if utils_mod is not None:
    for fn in expected_utils_funcs:
        func_rows.append({"模組": "utils", "函式": fn, "是否存在": hasattr(utils_mod, fn)})
if stock_mod is not None:
    for fn in expected_master_funcs:
        func_rows.append({"模組": "stock_master_service", "函式": fn, "是否存在": hasattr(stock_mod, fn)})
if func_rows:
    st.dataframe(pd.DataFrame(func_rows), use_container_width=True, hide_index=True)

st.subheader("5. 股票主檔診斷")
if stock_mod is None:
    st.error(f"stock_master_service 匯入失敗：{stock_err}")
else:
    try:
        master = stock_mod.load_stock_master()
        if master is None:
            master = pd.DataFrame()
        st.write(f"主檔筆數：**{len(master):,}**")
        if isinstance(master, pd.DataFrame) and not master.empty:
            cols = list(master.columns)
            c1, c2, c3 = st.columns(3)
            with c1:
                st.caption("欄位")
                st.write("、".join(cols[:20]))
            with c2:
                if "市場別" in master.columns:
                    st.caption("市場別分布")
                    st.dataframe(master["市場別"].astype(str).value_counts().reset_index().rename(columns={"index": "市場別", "市場別": "筆數"}), hide_index=True, use_container_width=True)
            with c3:
                cat_col = "主題類別" if "主題類別" in master.columns else ("產業別" if "產業別" in master.columns else "")
                if cat_col:
                    st.caption(f"{cat_col} 前 15")
                    st.dataframe(master[cat_col].astype(str).value_counts().head(15).reset_index().rename(columns={"index": cat_col, cat_col: "筆數"}), hide_index=True, use_container_width=True)
            st.dataframe(master.head(80), use_container_width=True, hide_index=True)
        else:
            st.warning("主檔為空，請先到 9_股票主檔更新 重新建立。")
    except Exception as e:
        st.error(f"load_stock_master() 執行失敗：{e}")
        st.code(traceback.format_exc())

st.subheader("6. 自選股診斷")
watch_data = json_data_map.get("watchlist.json")
if watch_data is None:
    st.warning("watchlist.json 無法讀取。")
else:
    try:
        rows = []
        if isinstance(watch_data, dict):
            for group, items in watch_data.items():
                if isinstance(items, list):
                    rows.append({"群組": group, "股票數": len(items), "內容預覽": ", ".join([_safe_str(x) for x in items[:8]])})
                else:
                    rows.append({"群組": group, "股票數": "格式異常", "內容預覽": _safe_str(items)[:120]})
        elif isinstance(watch_data, list):
            rows.append({"群組": "list", "股票數": len(watch_data), "內容預覽": ", ".join([_safe_str(x) for x in watch_data[:8]])})
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"自選股解析失敗：{e}")

    if utils_mod is not None and hasattr(utils_mod, "get_normalized_watchlist"):
        try:
            normalized = utils_mod.get_normalized_watchlist()
            st.caption("get_normalized_watchlist() 回傳")
            if isinstance(normalized, dict):
                norm_rows = [{"群組": k, "股票數": len(v) if isinstance(v, list) else "非 list"} for k, v in normalized.items()]
                st.dataframe(pd.DataFrame(norm_rows), hide_index=True, use_container_width=True)
            else:
                st.write(type(normalized).__name__)
                st.write(normalized)
        except Exception as e:
            st.error(f"get_normalized_watchlist() 執行失敗：{e}")

st.subheader("7. 即時資料 / 歷史資料實測")
run_realtime = st.button("測試即時資料", type="primary", use_container_width=True)
run_history = st.button("測試歷史資料", use_container_width=True)

if run_realtime:
    if utils_mod is None or not hasattr(utils_mod, "get_realtime_stock_info"):
        st.error("utils.get_realtime_stock_info 不存在。")
    else:
        try:
            info = utils_mod.get_realtime_stock_info(test_code, test_name, test_market, refresh_token="diagnostics")
            st.success("即時資料函式已回應。")
            st.json(info if isinstance(info, (dict, list)) else {"value": _safe_str(info)})
            if isinstance(info, dict):
                price_keys = ["price", "current_price", "現價", "最新價", "close"]
                vals = {k: info.get(k) for k in price_keys if k in info}
                if vals and all(_safe_str(v) in {"", "0", "0.0", "None"} for v in vals.values()):
                    st.warning("即時價格欄位疑似為 0 或空白，請檢查資料來源 fallback。")
        except Exception as e:
            st.error(f"即時資料測試失敗：{e}")
            st.code(traceback.format_exc())

if run_history:
    if utils_mod is None or not hasattr(utils_mod, "get_history_data"):
        st.error("utils.get_history_data 不存在。")
    else:
        end_dt = date.today()
        start_dt = end_dt - timedelta(days=int(test_days))
        try:
            try:
                hist = utils_mod.get_history_data(stock_no=test_code, stock_name=test_name, market_type=test_market, start_date=start_dt, end_date=end_dt)
            except TypeError:
                try:
                    hist = utils_mod.get_history_data(test_code, test_name, test_market, start_dt, end_dt)
                except TypeError:
                    hist = utils_mod.get_history_data(code=test_code, start_date=start_dt, end_date=end_dt)
            if hist is None:
                hist = pd.DataFrame()
            st.write(f"歷史資料筆數：**{len(hist):,}**")
            if isinstance(hist, pd.DataFrame) and not hist.empty:
                st.success("歷史資料函式已回應且有資料。")
                st.caption(f"欄位：{', '.join([str(c) for c in hist.columns])}")
                st.dataframe(hist.tail(80), use_container_width=True, hide_index=True)
                if "成交股數" in hist.columns:
                    vol = pd.to_numeric(hist["成交股數"], errors="coerce")
                    if vol.max(skipna=True) is not None and vol.max(skipna=True) < 10000:
                        st.warning("成交量數值偏小，可能仍是張數而非股數，會影響量能分數。")
            else:
                st.warning("歷史資料為空。若測試的是上櫃股，請確認 utils.py 已有 TPEx fallback。")
        except Exception as e:
            st.error(f"歷史資料測試失敗：{e}")
            st.code(traceback.format_exc())

st.subheader("8. 推薦與紀錄 JSON 串聯檢查")
record_files = ["godpick_records.json", "godpick_recommend_list.json", "godpick_latest_recommendations.json", "godpick_user_settings.json", "godpick_record_ui_config.json"]
rec_rows = []
for name in record_files:
    data = json_data_map.get(name)
    path = BASE_DIR / name
    ok, data2, err = _read_json_safe(path)
    data = data if data is not None else data2
    cols = ""
    sample_id = ""
    if isinstance(data, list) and data:
        if isinstance(data[0], dict):
            cols = ", ".join(list(data[0].keys())[:20])
            sample_id = _safe_str(data[0].get("record_id") or data[0].get("id"))
    elif isinstance(data, dict):
        cols = ", ".join(list(data.keys())[:20])
    rec_rows.append({
        "檔案": name,
        "狀態": "OK" if ok else "錯誤 / 不存在",
        "型態與筆數": _json_count(data) if ok else "",
        "樣本ID": sample_id,
        "欄位預覽": cols,
        "錯誤": err,
    })
st.dataframe(pd.DataFrame(rec_rows), use_container_width=True, hide_index=True)

with st.expander("查看 JSON 內容預覽", expanded=False):
    pick = st.selectbox("選擇 JSON", [name for name, _, _ in json_files], key=_k("json_preview_pick"))
    path = dict((name, path) for name, path, _ in json_files).get(pick)
    if path:
        ok, data, err = _read_json_safe(path)
        if ok:
            st.json(data)
        else:
            st.error(err)
            try:
                st.code(path.read_text(encoding="utf-8-sig")[:5000])
            except Exception:
                pass

st.info("建議流程：先確認本頁主檔、自選股、歷史資料、即時資料都 OK，再回到 7_股神推薦.py 測試推薦與匯入 8 / 10。")

# ============================================================
# v54 全系統串聯驗證：0 -> 7 -> 8 / 10 -> 首頁 / 儀表板
# ============================================================

st.markdown('---')
st.subheader('9. v54 全系統串聯驗證與欄位修復')
st.caption('檢查 0_大盤趨勢、7_股神推薦、8_股神推薦紀錄、10_推薦清單、首頁 / 儀表板之間的 JSON 串聯。此區塊只讀本機檔案，不重新抓網路資料；v54 可補齊舊推薦紀錄缺少的大盤欄位。')

try:
    from system_integration_health import run_full_integration_check, ensure_missing_json_files, repair_recommendation_market_fields, repair_v54_missing_fields, backup_json_files, initialize_v55_runtime_diagnostics
    _v41_report = run_full_integration_check(BASE_DIR)
    _v41_summary = _v41_report.get('summary', {})

    cc1, cc2, cc3, cc4, cc5 = st.columns(5)
    with cc1:
        _render_metric_card('整體狀態', str(_v41_summary.get('整體狀態', '')), str(_v41_summary.get('檢查時間', '')), 'ok' if _v41_summary.get('整體狀態') == 'OK' else ('warn' if _v41_summary.get('整體狀態') == '注意' else 'bad'))
    with cc2:
        _render_metric_card('正常', str(_v41_summary.get('正常', 0)), '串聯檢查正常項目', 'ok')
    with cc3:
        _render_metric_card('注意', str(_v41_summary.get('注意', 0)), '尚無樣本或可接受提醒', 'warn')
    with cc4:
        _render_metric_card('異常', str(_v41_summary.get('異常', 0)), '缺檔 / 缺欄位 / 格式異常', 'bad' if int(_v41_summary.get('異常', 0) or 0) else 'ok')
    with cc5:
        _render_metric_card('總項目', str(_v41_summary.get('總項目', 0)), 'v54 串聯檢查項目數', 'info')

    with st.expander('v54 橋接檔檢查：market_snapshot / macro bridge', expanded=True):
        st.dataframe(pd.DataFrame(_v41_report.get('bridge_rows', [])), use_container_width=True, hide_index=True)

    with st.expander('v54 大盤快照欄位檢查：0 大盤趨勢 -> 7 股神推薦', expanded=True):
        st.dataframe(pd.DataFrame(_v41_report.get('market_rows', [])), use_container_width=True, hide_index=True)
        _snap = _v41_report.get('market_snapshot', {}) or {}
        if isinstance(_snap, dict) and _snap:
            s1, s2, s3, s4 = st.columns(4)
            with s1:
                st.metric('market_score', _safe_str(_snap.get('market_score') or '—'))
            with s2:
                st.metric('market_trend', _safe_str(_snap.get('market_trend') or '—'))
            with s3:
                st.metric('risk_gate', _safe_str(_snap.get('risk_gate') or '—'))
            with s4:
                st.metric('market_session', _safe_str(_snap.get('market_session_label') or _snap.get('market_session') or '—'))
            with st.expander('market_snapshot.json 完整內容', expanded=False):
                st.json(_snap)

    with st.expander('v54 推薦結果大盤欄位檢查：7 -> 8 / 10', expanded=True):
        st.dataframe(pd.DataFrame(_v41_report.get('recommendation_rows', [])), use_container_width=True, hide_index=True)

    with st.expander('v54 關鍵 JSON 檔案矩陣', expanded=False):
        st.dataframe(pd.DataFrame(_v41_report.get('file_rows', [])), use_container_width=True, hide_index=True)

    with st.expander('v54 頁面檔案檢查', expanded=False):
        st.dataframe(pd.DataFrame(_v41_report.get('page_rows', [])), use_container_width=True, hide_index=True)

    with st.expander('v54 大盤功能管理中心檢查：v45 欄位', expanded=True):
        st.dataframe(pd.DataFrame(_v41_report.get('v45_rows', [])), use_container_width=True, hide_index=True)

    with st.expander('v54 資料源診斷檢查：utils.py v47', expanded=True):
        st.dataframe(pd.DataFrame(_v41_report.get('v47_rows', [])), use_container_width=True, hide_index=True)

    with st.expander('v54 推薦速度監控檢查：7_股神推薦 v48', expanded=False):
        st.dataframe(pd.DataFrame(_v41_report.get('v48_rows', [])), use_container_width=True, hide_index=True)

    with st.expander('v54 自選股同步檢查：4_自選股中心 v49', expanded=False):
        st.dataframe(pd.DataFrame(_v41_report.get('v49_rows', [])), use_container_width=True, hide_index=True)

    with st.expander('v54 推薦後績效欄位檢查：8 / 10 v50-v53', expanded=True):
        st.dataframe(pd.DataFrame(_v41_report.get('performance_rows', [])), use_container_width=True, hide_index=True)

    with st.expander('v54 全部檢查明細', expanded=False):
        st.dataframe(pd.DataFrame(_v41_report.get('all_rows', [])), use_container_width=True, hide_index=True)

    st.markdown('#### v54 修復工具')
    st.caption('缺檔修復只會建立不存在的空白 JSON；欄位修復會把 market_snapshot.json 的大盤欄位補到舊推薦結果 / 紀錄 / 推薦清單，不刪除既有資料、不覆蓋已有非空值。')
    if st.button('建立缺少的空白 JSON（不覆蓋既有檔）', use_container_width=True):
        _created = ensure_missing_json_files(BASE_DIR)
        st.dataframe(pd.DataFrame(_created), use_container_width=True, hide_index=True)
        st.success('已完成缺檔建立檢查；請重新整理本頁再次驗證。')


    st.markdown('##### v54 舊推薦資料大盤欄位補齊')
    st.caption('用途：修正畫面上「7 -> 8 / 10」出現缺欄位造成異常數增加。此功能只補缺少或空白欄位，不刪除任何推薦紀錄。')
    if st.button('v54 一鍵補齊舊推薦資料的大盤欄位', use_container_width=True, type='primary'):
        _repair = repair_recommendation_market_fields(BASE_DIR)
        if _repair.get('ok'):
            st.success(_repair.get('message', '已完成'))
        else:
            st.error(_repair.get('message', '修復失敗'))
        st.dataframe(pd.DataFrame(_repair.get('rows', [])), use_container_width=True, hide_index=True)
        with st.expander('本次補入欄位預設值', expanded=False):
            st.json(_repair.get('defaults', {}))
        st.info('請重新整理本頁，再看異常數是否下降。')

    st.markdown('##### v54 推薦後績效欄位補齊')
    st.caption('用途：修正 8 / 10 舊資料缺少 v50-v53 推薦後績效欄位造成 KeyError 或健康檢查異常。此功能只補缺欄位，不刪資料。')
    if st.button('v54 一鍵補齊 8 / 10 推薦後績效欄位', use_container_width=True):
        _perf_repair = repair_v54_missing_fields(BASE_DIR)
        if _perf_repair.get('ok'):
            st.success(_perf_repair.get('message', '已完成'))
        else:
            st.error(_perf_repair.get('message', '修復失敗'))
        st.dataframe(pd.DataFrame(_perf_repair.get('rows', [])), use_container_width=True, hide_index=True)
        st.info('請重新整理本頁，再看 v50-v53 推薦後績效欄位檢查是否下降。')

    st.markdown('##### v55 runtime 診斷檔初始化')
    st.caption('用途：你截圖中的 data_source_diagnostics.json、watchlist_runtime_snapshot.json、watchlist_normalized.json 是 runtime 產物；尚未執行 7_股神推薦或 4_自選股中心前可能不存在。此工具會依現有 watchlist.json 產生安全快照，並建立資料源診斷占位檔，不覆蓋既有資料。')
    if st.button('v55 一鍵產生缺少的資料源 / 自選股 runtime 診斷檔', use_container_width=True, type='primary'):
        _v55_init = initialize_v55_runtime_diagnostics(BASE_DIR)
        if _v55_init.get('ok'):
            st.success(_v55_init.get('message', '已完成'))
        else:
            st.error(_v55_init.get('message', '初始化失敗'))
        st.dataframe(pd.DataFrame(_v55_init.get('rows', [])), use_container_width=True, hide_index=True)
        st.info('請重新整理本頁，再看 v47 / v49 檢查是否轉為 OK。')

    st.markdown('##### v54 JSON 備份工具')
    st.caption('會把關鍵 JSON 備份到 backups/v54_health_backup_時間戳，不覆蓋原檔。')
    if st.button('v54 一鍵備份關鍵 JSON', use_container_width=True):
        _backup_rows = backup_json_files(BASE_DIR)
        st.dataframe(pd.DataFrame(_backup_rows), use_container_width=True, hide_index=True)
        st.success('已完成備份檢查。')

except Exception as _v41_e:
    st.error(f'v54 全系統串聯驗證載入失敗：{_v41_e}')
    st.code(traceback.format_exc())
