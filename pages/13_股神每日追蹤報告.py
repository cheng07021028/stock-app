# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, date, timedelta
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

PAGE_TITLE = "股神每日追蹤報告｜v19"
BASE_DIR = Path(__file__).resolve().parents[1]

RECOMMEND_FILES = [
    BASE_DIR / "godpick_recommend_list.json",
    BASE_DIR / "recommend_list.json",
]
RECORD_FILES = [
    BASE_DIR / "godpick_records.json",
    BASE_DIR / "god_recommend_records.json",
]

DEFAULT_COLUMNS = [
    "追蹤分級", "今日操作建議", "股票代號", "股票名稱", "市場別", "類別", "產業",
    "推薦日期", "推薦分數", "推薦型態", "機會型態", "進場時機", "建議動作",
    "建議倉位%", "動態建議倉位%", "建議投入等級", "追高風險等級", "單檔風險等級",
    "近端支撐", "近端壓力", "停損參考", "停利策略", "停損策略",
    "大盤策略模式", "大盤策略建議", "族群集中警示", "強勢族群等級", "族群輪動狀態",
    "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%",
    "推薦後最大漲幅%", "推薦後最大回撤%", "命中結果", "績效評語", "狀態", "資料來源檔",
]


def _safe_load_json(path: Path) -> Any:
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
        for key in ["records", "data", "items", "recommendations", "list", "rows"]:
            val = obj.get(key)
            if isinstance(val, list):
                return [x for x in val if isinstance(x, dict)]
        if any(k in obj for k in ["股票代號", "code", "stock_code", "symbol"]):
            return [obj]
    return []


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    alias = {
        "code": "股票代號", "stock_code": "股票代號", "symbol": "股票代號",
        "name": "股票名稱", "stock_name": "股票名稱",
        "date": "推薦日期", "recommend_date": "推薦日期", "created_at": "推薦日期", "建立時間": "推薦日期",
        "score": "推薦分數", "total_score": "推薦分數",
        "category": "類別", "sector": "產業",
        "status": "狀態",
    }
    for src, dst in alias.items():
        if src in df.columns and dst not in df.columns:
            df[dst] = df[src]
    if "股票代號" in df.columns:
        df["股票代號"] = df["股票代號"].astype(str).str.strip()
        df = df[df["股票代號"].ne("")]
    if "推薦日期" in df.columns:
        df["推薦日期"] = df["推薦日期"].astype(str).replace({"NaT": "", "nan": "", "None": ""})
    return df.reset_index(drop=True)


def _load_records(paths: List[Path]) -> Tuple[pd.DataFrame, List[str]]:
    rows: List[Dict[str, Any]] = []
    source_notes: List[str] = []
    for path in paths:
        obj = _safe_load_json(path)
        part = _extract_rows(obj)
        source_notes.append(f"{path.name}：{len(part)} 筆" if part else f"{path.name}：0 筆或不存在")
        for r in part:
            rr = dict(r)
            rr.setdefault("資料來源檔", path.name)
            rows.append(rr)
    if not rows:
        return pd.DataFrame(), source_notes
    df = _normalize_df(pd.DataFrame(rows))
    if "股票代號" in df.columns:
        if "推薦日期" in df.columns:
            df["_dt"] = pd.to_datetime(df["推薦日期"], errors="coerce")
        else:
            df["_dt"] = pd.NaT
        df["_seq"] = range(len(df))
        df = df.sort_values(["股票代號", "_dt", "_seq"]).drop_duplicates("股票代號", keep="last")
        df = df.drop(columns=["_dt", "_seq"], errors="ignore")
    return df.reset_index(drop=True), source_notes


def _num(val: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(val):
            return default
        s = str(val).strip().replace("%", "").replace(",", "")
        if s in ["", "-", "None", "nan", "NaT"]:
            return default
        return float(s)
    except Exception:
        return default


def _to_num_col(df: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
    if col not in df.columns:
        return pd.Series([default] * len(df), index=df.index)
    return df[col].map(lambda x: _num(x, default))


def _make_tracking_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    score = _to_num_col(df, "推薦分數", 0)
    risk = df.get("追高風險等級", pd.Series([""] * len(df), index=df.index)).astype(str)
    single_risk = df.get("單檔風險等級", pd.Series([""] * len(df), index=df.index)).astype(str)
    timing = df.get("進場時機", pd.Series([""] * len(df), index=df.index)).astype(str)
    action = df.get("建議動作", pd.Series([""] * len(df), index=df.index)).astype(str)
    result = df.get("命中結果", pd.Series([""] * len(df), index=df.index)).astype(str)
    max_dd = _to_num_col(df, "推薦後最大回撤%", 0)
    max_up = _to_num_col(df, "推薦後最大漲幅%", 0)

    grades: List[str] = []
    actions: List[str] = []
    for i in df.index:
        risk_text = f"{risk.loc[i]} {single_risk.loc[i]}"
        timing_text = timing.loc[i]
        action_text = action.loc[i]
        result_text = result.loc[i]
        s = float(score.loc[i])
        dd = float(max_dd.loc[i])
        up = float(max_up.loc[i])

        if "停損" in result_text or "失敗" in result_text or dd <= -8:
            grades.append("D｜風險處理")
            actions.append("檢查停損與是否移出追蹤")
        elif "達標" in result_text or up >= 8:
            grades.append("A｜達標追蹤")
            actions.append("可分批停利，保留強勢續抱觀察")
        elif any(k in risk_text for k in ["高", "過熱", "追高"]):
            grades.append("C｜避免追高")
            actions.append("不建議追價，等待拉回或量價再確認")
        elif any(k in timing_text + action_text for k in ["可分批", "可觀察", "支撐不破", "低檔", "拉回"]):
            grades.append("A｜優先觀察")
            actions.append("依支撐與停損分批，確認量價後再加碼")
        elif s >= 80:
            grades.append("B｜強勢追蹤")
            actions.append("訊號強，但須確認是否偏離支撐過遠")
        elif s >= 65:
            grades.append("B｜一般追蹤")
            actions.append("維持觀察，等突破或回測不破")
        else:
            grades.append("C｜低優先")
            actions.append("暫不加碼，等待訊號轉強")

    df["追蹤分級"] = grades
    df["今日操作建議"] = actions
    return df


def _filter_by_date(df: pd.DataFrame, days: int) -> pd.DataFrame:
    if df.empty or "推薦日期" not in df.columns or days <= 0:
        return df
    dt = pd.to_datetime(df["推薦日期"], errors="coerce")
    cutoff = pd.Timestamp(datetime.now() - timedelta(days=days))
    mask = dt.isna() | (dt >= cutoff)
    return df.loc[mask].reset_index(drop=True)


def _display_kpis(df: pd.DataFrame) -> None:
    if df.empty:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("追蹤股票", 0)
        c2.metric("優先觀察", 0)
        c3.metric("高風險 / 避免追高", 0)
        c4.metric("建議倉位合計", "0%")
        return
    grade = df.get("追蹤分級", pd.Series([""] * len(df), index=df.index)).astype(str)
    high_priority = int(grade.str.contains("A｜", na=False).sum())
    risk_count = int(grade.str.contains("C｜避免追高|D｜", regex=True, na=False).sum())
    weight_col = "動態建議倉位%" if "動態建議倉位%" in df.columns else "建議倉位%"
    total_weight = float(_to_num_col(df, weight_col, 0).sum()) if weight_col in df.columns else 0.0
    hit = df.get("命中結果", pd.Series([""] * len(df), index=df.index)).astype(str)
    hit_count = int(hit.str.contains("達標|成功|命中", regex=True, na=False).sum())

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("追蹤股票", len(df))
    c2.metric("優先觀察", high_priority)
    c3.metric("高風險 / 避免追高", risk_count)
    c4.metric("建議倉位合計", f"{total_weight:.1f}%")
    c5.metric("已達標 / 命中", hit_count)


def _column_order(df: pd.DataFrame) -> List[str]:
    cols = [c for c in DEFAULT_COLUMNS if c in df.columns]
    rest = [c for c in df.columns if c not in cols]
    return cols + rest


def _render_section(title: str) -> None:
    if render_pro_section:
        try:
            render_pro_section(title)
            return
        except Exception:
            pass
    st.subheader(title)


def main() -> None:
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    if inject_pro_theme:
        try:
            inject_pro_theme()
        except Exception:
            pass

    if render_pro_hero:
        try:
            render_pro_hero("股神每日追蹤報告", "v19｜推薦清單 × 紀錄 × 回測績效 × 今日操作重點")
        except Exception:
            st.title(PAGE_TITLE)
    else:
        st.title(PAGE_TITLE)

    st.caption("此頁不重新掃描股票，只讀取 7/8/10 既有 JSON 結果，整理成每日追蹤與操作檢查清單。")

    rec_df, rec_notes = _load_records(RECOMMEND_FILES)
    hist_df, hist_notes = _load_records(RECORD_FILES)

    source = st.radio("資料來源", ["推薦清單 / 目前追蹤", "股神推薦紀錄 / 歷史紀錄", "合併檢視"], horizontal=True)
    if source.startswith("推薦清單"):
        df = rec_df.copy()
        notes = rec_notes
    elif source.startswith("股神推薦紀錄"):
        df = hist_df.copy()
        notes = hist_notes
    else:
        df = pd.concat([rec_df, hist_df], ignore_index=True, sort=False)
        df = _normalize_df(df)
        if not df.empty and "股票代號" in df.columns:
            if "推薦日期" in df.columns:
                df["_dt"] = pd.to_datetime(df["推薦日期"], errors="coerce")
            else:
                df["_dt"] = pd.NaT
            df["_seq"] = range(len(df))
            df = df.sort_values(["股票代號", "_dt", "_seq"]).drop_duplicates("股票代號", keep="last")
            df = df.drop(columns=["_dt", "_seq"], errors="ignore").reset_index(drop=True)
        notes = rec_notes + hist_notes

    with st.expander("資料來源狀態", expanded=False):
        for n in notes:
            st.write("- " + n)

    if df.empty:
        st.warning("目前沒有可追蹤資料。請先到 7_股神推薦 匯入推薦清單，或到 8_股神推薦紀錄 建立紀錄。")
        return

    df = _make_tracking_columns(df)

    with st.sidebar:
        st.header("追蹤篩選")
        days = st.selectbox("推薦日期範圍", [0, 3, 7, 14, 30, 60, 90], index=4, format_func=lambda x: "全部" if x == 0 else f"近 {x} 天")
        df = _filter_by_date(df, int(days))
        keyword = st.text_input("搜尋代號 / 名稱 / 類別 / 型態", "")
        if keyword.strip():
            kw = keyword.strip()
            mask = pd.Series(False, index=df.index)
            for col in ["股票代號", "股票名稱", "類別", "產業", "推薦型態", "機會型態", "追蹤分級"]:
                if col in df.columns:
                    mask = mask | df[col].astype(str).str.contains(kw, case=False, na=False)
            df = df.loc[mask].reset_index(drop=True)
        grade_options = ["全部"] + sorted([x for x in df.get("追蹤分級", pd.Series(dtype=str)).dropna().astype(str).unique().tolist() if x])
        grade_sel = st.selectbox("追蹤分級", grade_options)
        if grade_sel != "全部" and "追蹤分級" in df.columns:
            df = df[df["追蹤分級"].astype(str).eq(grade_sel)].reset_index(drop=True)
        risk_options = ["全部"] + sorted([x for x in df.get("追高風險等級", pd.Series(dtype=str)).dropna().astype(str).unique().tolist() if x])
        risk_sel = st.selectbox("追高風險", risk_options)
        if risk_sel != "全部" and "追高風險等級" in df.columns:
            df = df[df["追高風險等級"].astype(str).eq(risk_sel)].reset_index(drop=True)

    _render_section("一、今日股神追蹤總覽")
    _display_kpis(df)

    _render_section("二、今日操作重點")
    if df.empty:
        st.info("篩選後沒有資料。")
    else:
        work = df.copy()
        if "推薦分數" in work.columns:
            work["_score"] = _to_num_col(work, "推薦分數", 0)
        else:
            work["_score"] = 0
        priority_rank = {"A｜優先觀察": 1, "A｜達標追蹤": 2, "B｜強勢追蹤": 3, "B｜一般追蹤": 4, "C｜避免追高": 5, "C｜低優先": 6, "D｜風險處理": 7}
        work["_rank"] = work.get("追蹤分級", pd.Series([""] * len(work), index=work.index)).map(priority_rank).fillna(9)
        work = work.sort_values(["_rank", "_score"], ascending=[True, False]).drop(columns=["_rank", "_score"], errors="ignore")
        show_cols = _column_order(work)
        st.dataframe(work[show_cols], use_container_width=True, height=520)

        csv = work[show_cols].to_csv(index=False).encode("utf-8-sig")
        st.download_button("下載今日追蹤報告 CSV", data=csv, file_name=f"godpick_daily_report_{date.today().isoformat()}.csv", mime="text/csv")

    _render_section("三、分組檢查")
    c1, c2 = st.columns(2)
    with c1:
        if "追蹤分級" in df.columns:
            st.write("追蹤分級分布")
            st.dataframe(df["追蹤分級"].value_counts().rename_axis("追蹤分級").reset_index(name="數量"), use_container_width=True)
        if "推薦型態" in df.columns:
            st.write("推薦型態分布")
            st.dataframe(df["推薦型態"].astype(str).value_counts().rename_axis("推薦型態").reset_index(name="數量"), use_container_width=True)
    with c2:
        group_col = "類別" if "類別" in df.columns else ("產業" if "產業" in df.columns else None)
        if group_col:
            st.write(f"{group_col} 集中度")
            st.dataframe(df[group_col].astype(str).value_counts().head(20).rename_axis(group_col).reset_index(name="數量"), use_container_width=True)
        if "追高風險等級" in df.columns:
            st.write("追高風險分布")
            st.dataframe(df["追高風險等級"].astype(str).value_counts().rename_axis("追高風險等級").reset_index(name="數量"), use_container_width=True)

    _render_section("四、使用建議")
    st.info(
        "建議每日開盤前先看『避免追高 / 風險處理』，盤中看『優先觀察』，收盤後回到 8_股神推薦紀錄更新績效，讓 v15 權重回饋校正有足夠樣本。"
    )


if __name__ == "__main__":
    main()
