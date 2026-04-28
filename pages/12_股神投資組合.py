# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, date
from typing import Any, Dict, List, Tuple

import pandas as pd
import streamlit as st

try:
    from utils import inject_pro_theme, render_pro_hero, render_pro_section, render_pro_info_card
except Exception:
    inject_pro_theme = None
    render_pro_hero = None
    render_pro_section = None
    render_pro_info_card = None

PAGE_TITLE = "股神投資組合｜v18"
BASE_DIR = Path(__file__).resolve().parents[1]
RECOMMEND_FILES = [
    BASE_DIR / "godpick_recommend_list.json",
    BASE_DIR / "recommend_list.json",
]
RECORD_FILES = [
    BASE_DIR / "godpick_records.json",
    BASE_DIR / "god_recommend_records.json",
]

CORE_COLUMNS = [
    "推薦日期", "股票代號", "股票名稱", "市場別", "類別", "產業", "推薦分數", "推薦型態", "機會型態",
    "進場時機", "建議動作", "追高風險等級", "單檔風險等級", "建議倉位%", "動態建議倉位%",
    "建議投入等級", "分批策略", "停損策略", "停利策略", "族群集中警示", "組合配置建議",
    "大盤策略模式", "大盤策略建議", "強勢族群等級", "族群輪動狀態", "命中結果",
]


def _safe_json_load(path: Path) -> Any:
    if not path.exists():
        return []
    try:
        text = path.read_text(encoding="utf-8")
        if not text.strip():
            return []
        return json.loads(text)
    except Exception:
        return []


def _extract_rows(obj: Any) -> List[Dict[str, Any]]:
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if isinstance(obj, dict):
        for key in ["records", "data", "items", "recommendations", "list"]:
            val = obj.get(key)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
        # 單筆 dict 也轉成一筆，避免格式異常時整頁空白
        if any(k in obj for k in ["股票代號", "code", "stock_code"]):
            return [obj]
    return []


def _load_many(paths: List[Path]) -> Tuple[pd.DataFrame, List[str]]:
    rows: List[Dict[str, Any]] = []
    sources: List[str] = []
    for path in paths:
        obj = _safe_json_load(path)
        part = _extract_rows(obj)
        if part:
            for r in part:
                rr = dict(r)
                rr.setdefault("資料來源檔", path.name)
                rows.append(rr)
            sources.append(f"{path.name}：{len(part)} 筆")
        else:
            sources.append(f"{path.name}：0 筆或不存在")
    if not rows:
        return pd.DataFrame(), sources
    df = pd.DataFrame(rows)
    # 標準化常見欄位別名
    alias = {
        "code": "股票代號", "stock_code": "股票代號", "symbol": "股票代號",
        "name": "股票名稱", "stock_name": "股票名稱",
        "date": "推薦日期", "created_at": "推薦日期", "recommend_date": "推薦日期",
        "score": "推薦分數", "total_score": "推薦分數",
        "category": "類別", "sector": "產業",
    }
    for src, dst in alias.items():
        if src in df.columns and dst not in df.columns:
            df[dst] = df[src]
    if "股票代號" in df.columns:
        df["股票代號"] = df["股票代號"].astype(str).str.strip()
        df = df[df["股票代號"].ne("")]
    # 去重：同股票代號保留最新一筆；日期無法解析時仍保留原序最後一筆
    if "推薦日期" in df.columns:
        df["_dt_sort"] = pd.to_datetime(df["推薦日期"], errors="coerce")
    else:
        df["_dt_sort"] = pd.NaT
    df["_row_order"] = range(len(df))
    if "股票代號" in df.columns:
        df = df.sort_values(["股票代號", "_dt_sort", "_row_order"]).drop_duplicates("股票代號", keep="last")
    df = df.drop(columns=[c for c in ["_dt_sort", "_row_order"] if c in df.columns])
    return df.reset_index(drop=True), sources


def _to_num(series: pd.Series, default: float = 0.0) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.replace("%", "", regex=False).str.replace(",", "", regex=False), errors="coerce").fillna(default)


def _ensure_numeric(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ["推薦分數", "建議倉位%", "動態建議倉位%", "追高風險分數_決策", "大盤多空分數", "族群資金流分數"]:
        if col in df.columns:
            df[col] = _to_num(df[col])
    if "動態建議倉位%" not in df.columns and "建議倉位%" in df.columns:
        df["動態建議倉位%"] = df["建議倉位%"]
    if "建議倉位%" not in df.columns and "動態建議倉位%" in df.columns:
        df["建議倉位%"] = df["動態建議倉位%"]
    return df


def _risk_rank(value: Any) -> int:
    s = str(value or "")
    if any(x in s for x in ["極高", "高風險", "偏高"]):
        return 4
    if any(x in s for x in ["中高", "中等", "中風險"]):
        return 3
    if any(x in s for x in ["低", "偏低", "保守"]):
        return 1
    return 2


def _allocation_action(row: pd.Series) -> str:
    score = float(row.get("推薦分數", 0) or 0)
    risk = _risk_rank(row.get("單檔風險等級", row.get("追高風險等級", "")))
    timing = str(row.get("進場時機", ""))
    chase = str(row.get("追高風險等級", ""))
    market = str(row.get("大盤策略模式", ""))
    alloc = float(row.get("動態建議倉位%", row.get("建議倉位%", 0)) or 0)

    if risk >= 4 or "不建議" in chase or "空頭" in market:
        return "減碼 / 僅觀察"
    if score >= 80 and risk <= 2 and alloc >= 15 and any(x in timing for x in ["可", "成熟", "分批", "接近"]):
        return "可分批建立"
    if score >= 70 and risk <= 3:
        return "觀察等確認"
    return "暫不加碼"


def _portfolio_warnings(df: pd.DataFrame) -> List[str]:
    warnings: List[str] = []
    if df.empty:
        return warnings
    sector_col = "類別" if "類別" in df.columns else ("產業" if "產業" in df.columns else None)
    alloc_col = "動態建議倉位%" if "動態建議倉位%" in df.columns else ("建議倉位%" if "建議倉位%" in df.columns else None)
    if alloc_col:
        total_alloc = float(_to_num(df[alloc_col]).sum())
        if total_alloc > 100:
            warnings.append(f"建議倉位合計 {total_alloc:.1f}% 已超過 100%，請優先挑選低風險與高分標的，不要全部進場。")
        elif total_alloc > 70:
            warnings.append(f"建議倉位合計 {total_alloc:.1f}% 偏高，建議分批執行並保留現金。")
    if sector_col:
        top_sector = df[sector_col].fillna("未分類").astype(str).value_counts(normalize=True).head(1)
        if not top_sector.empty and float(top_sector.iloc[0]) >= 0.4:
            warnings.append(f"族群集中度偏高：{top_sector.index[0]} 佔 {float(top_sector.iloc[0])*100:.1f}%，需注意同族群同步回檔。")
    risk_col = "單檔風險等級" if "單檔風險等級" in df.columns else ("追高風險等級" if "追高風險等級" in df.columns else None)
    if risk_col:
        high_risk_ratio = df[risk_col].map(lambda x: _risk_rank(x) >= 4).mean()
        if high_risk_ratio >= 0.25:
            warnings.append(f"高風險標的比例 {high_risk_ratio*100:.1f}% 偏高，建議降低強勢追價股權重。")
    return warnings


def _render_kpis(df: pd.DataFrame) -> None:
    alloc_col = "動態建議倉位%" if "動態建議倉位%" in df.columns else ("建議倉位%" if "建議倉位%" in df.columns else None)
    total = len(df)
    avg_score = _to_num(df["推薦分數"]).mean() if "推薦分數" in df.columns and total else 0
    total_alloc = _to_num(df[alloc_col]).sum() if alloc_col and total else 0
    high_risk = 0
    if "單檔風險等級" in df.columns:
        high_risk = df["單檔風險等級"].map(lambda x: _risk_rank(x) >= 4).sum()
    elif "追高風險等級" in df.columns:
        high_risk = df["追高風險等級"].map(lambda x: _risk_rank(x) >= 4).sum()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("追蹤標的", f"{total} 檔")
    c2.metric("平均推薦分數", f"{avg_score:.1f}")
    c3.metric("建議倉位合計", f"{total_alloc:.1f}%")
    c4.metric("高風險標的", f"{int(high_risk)} 檔")


def _select_columns(df: pd.DataFrame) -> List[str]:
    cols = [c for c in CORE_COLUMNS if c in df.columns]
    extras = [c for c in df.columns if c not in cols and not c.startswith("_")]
    return cols + extras[:20]


def main() -> None:
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    if inject_pro_theme:
        try:
            inject_pro_theme()
        except Exception:
            pass
    if render_pro_hero:
        try:
            render_pro_hero("股神投資組合", "v18｜推薦清單整體配置、族群集中、風險倉位與操作優先順序")
        except Exception:
            st.title("股神投資組合｜v18")
    else:
        st.title("股神投資組合｜v18")

    st.caption("讀取 10_推薦清單 / 8_股神推薦紀錄資料，提供整體持股配置與風控建議。不寫入 JSON，不影響原推薦邏輯。")

    rec_df, rec_sources = _load_many(RECOMMEND_FILES)
    hist_df, hist_sources = _load_many(RECORD_FILES)

    with st.expander("資料來源狀態", expanded=False):
        st.write("推薦清單：")
        st.code("\n".join(rec_sources))
        st.write("推薦紀錄：")
        st.code("\n".join(hist_sources))

    source_option = st.radio("分析資料來源", ["推薦清單 / 目前追蹤", "股神推薦紀錄 / 歷史全部"], horizontal=True)
    df = rec_df if source_option.startswith("推薦清單") else hist_df
    if df.empty:
        st.warning("目前沒有可分析資料。請先在 7_股神推薦 勾選股票匯入 10_推薦清單，或在 8_股神推薦紀錄建立紀錄。")
        return
    df = _ensure_numeric(df)

    with st.sidebar:
        st.header("篩選")
        if "推薦日期" in df.columns:
            dts = pd.to_datetime(df["推薦日期"], errors="coerce").dt.date.dropna()
            if not dts.empty:
                min_d, max_d = dts.min(), dts.max()
                start_d, end_d = st.date_input("推薦日期區間", value=(min_d, max_d), min_value=min_d, max_value=max_d)
                if isinstance(start_d, date) and isinstance(end_d, date):
                    mask = pd.to_datetime(df["推薦日期"], errors="coerce").dt.date.between(start_d, end_d)
                    df = df[mask.fillna(False)]
        for col in ["類別", "產業", "推薦型態", "進場時機", "單檔風險等級", "追高風險等級", "大盤策略模式"]:
            if col in df.columns:
                values = sorted([x for x in df[col].dropna().astype(str).unique().tolist() if x.strip()])
                if values:
                    chosen = st.multiselect(col, values, default=[])
                    if chosen:
                        df = df[df[col].astype(str).isin(chosen)]
        min_score = st.slider("最低推薦分數", 0, 100, 0)
        if "推薦分數" in df.columns:
            df = df[_to_num(df["推薦分數"]) >= min_score]

    if df.empty:
        st.warning("篩選後沒有資料。")
        return

    _render_kpis(df)

    warnings = _portfolio_warnings(df)
    if warnings:
        st.subheader("組合風險提醒")
        for w in warnings:
            st.warning(w)
    else:
        st.success("目前組合未偵測到明顯過度集中或高風險倉位問題。")

    show_df = df.copy()
    show_df["v18操作優先順序"] = show_df.apply(_allocation_action, axis=1)

    st.subheader("操作優先順序")
    action_counts = show_df["v18操作優先順序"].value_counts().reset_index()
    action_counts.columns = ["建議", "檔數"]
    st.dataframe(action_counts, use_container_width=True, hide_index=True)

    c1, c2 = st.columns(2)
    with c1:
        sector_col = "類別" if "類別" in show_df.columns else ("產業" if "產業" in show_df.columns else None)
        if sector_col:
            st.subheader("族群集中度")
            st.dataframe(show_df[sector_col].fillna("未分類").astype(str).value_counts().rename_axis(sector_col).reset_index(name="檔數"), use_container_width=True, hide_index=True)
    with c2:
        type_col = "推薦型態" if "推薦型態" in show_df.columns else ("機會型態" if "機會型態" in show_df.columns else None)
        if type_col:
            st.subheader("推薦型態配置")
            st.dataframe(show_df[type_col].fillna("未分類").astype(str).value_counts().rename_axis(type_col).reset_index(name="檔數"), use_container_width=True, hide_index=True)

    st.subheader("投資組合明細")
    sort_cols = []
    if "v18操作優先順序" in show_df.columns:
        sort_cols.append("v18操作優先順序")
    if "推薦分數" in show_df.columns:
        sort_cols.append("推薦分數")
    if sort_cols:
        ascending = [True] * len(sort_cols)
        if "推薦分數" in sort_cols:
            ascending[sort_cols.index("推薦分數")] = False
        show_df = show_df.sort_values(sort_cols, ascending=ascending)

    display_cols = ["v18操作優先順序"] + _select_columns(show_df)
    display_cols = [c for i, c in enumerate(display_cols) if c in show_df.columns and c not in display_cols[:i]]
    st.dataframe(show_df[display_cols], use_container_width=True, hide_index=True, height=560)

    st.download_button(
        "下載投資組合分析 CSV",
        show_df[display_cols].to_csv(index=False).encode("utf-8-sig"),
        file_name=f"godpick_portfolio_v18_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        use_container_width=True,
    )

    st.info("v18 不會自動下單，也不會自動修改推薦權重；它用來檢查目前推薦清單的資金配置、風險集中與操作優先順序。")


if __name__ == "__main__":
    main()
