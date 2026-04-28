# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd
import streamlit as st

try:
    from utils import inject_pro_theme, render_pro_hero
except Exception:
    inject_pro_theme = None
    render_pro_hero = None

PAGE_TITLE = "股神管理中心｜v21.1"
BASE_DIR = Path(__file__).resolve().parents[1]

RECOMMEND_FILES = [
    BASE_DIR / "godpick_recommend_list.json",
    BASE_DIR / "recommend_list.json",
]
RECORD_FILES = [
    BASE_DIR / "godpick_records.json",
    BASE_DIR / "god_recommend_records.json",
]
ALL_DATA_FILES = RECORD_FILES + RECOMMEND_FILES

PORTFOLIO_COLUMNS = [
    "v21操作優先順序", "股票代號", "股票名稱", "市場別", "類別", "產業", "推薦日期", "推薦分數",
    "推薦模式", "推薦型態", "機會型態", "進場時機", "建議動作", "等待條件",
    "建議倉位%", "動態建議倉位%", "建議投入等級", "第一筆進場%", "分批策略", "第二筆加碼條件",
    "追高風險等級", "單檔風險等級", "最大風險%", "近端支撐", "近端壓力", "停損參考",
    "停利策略", "停損策略", "族群集中警示", "組合配置建議", "大盤策略模式", "大盤策略建議",
    "強勢族群等級", "族群輪動狀態", "族群資金流分數", "命中結果", "狀態", "資料來源檔",
]

DAILY_COLUMNS = [
    "追蹤分級", "今日操作建議", "股票代號", "股票名稱", "市場別", "類別", "產業", "推薦日期", "推薦分數",
    "推薦型態", "機會型態", "進場時機", "建議動作", "建議倉位%", "動態建議倉位%",
    "追高風險等級", "單檔風險等級", "近端支撐", "近端壓力", "停損參考", "停利策略", "停損策略",
    "大盤策略模式", "大盤策略建議", "族群集中警示", "強勢族群等級", "族群輪動狀態",
    "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%",
    "推薦後最大漲幅%", "推薦後最大回撤%", "命中結果", "績效評語", "狀態", "資料來源檔",
]

QUALITY_COLUMNS = [
    "品質分級", "品質建議", "股票代號", "股票名稱", "市場別", "類別", "產業", "推薦日期", "推薦模式",
    "推薦型態", "機會型態", "進場時機", "建議動作", "推薦分數", "買點分級", "追高風險等級", "單檔風險等級",
    "建議倉位%", "動態建議倉位%", "大盤策略模式", "強勢族群等級", "族群輪動狀態",
    "推薦後1日%", "推薦後3日%", "推薦後5日%", "推薦後10日%", "推薦後20日%",
    "推薦後最大漲幅%", "推薦後最大回撤%", "命中結果", "績效評語", "狀態", "資料來源檔",
]

GROUP_FIELDS = [
    "推薦模式", "推薦型態", "機會型態", "進場時機", "追高風險等級", "單檔風險等級",
    "大盤策略模式", "強勢族群等級", "族群輪動狀態", "類別", "產業",
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



def _file_status_rows(paths: List[Path]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for path in paths:
        exists = path.exists()
        size = path.stat().st_size if exists else 0
        mtime = datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S") if exists else ""
        rows.append({
            "資料檔": path.name,
            "是否存在": "是" if exists else "否",
            "檔案大小KB": round(size / 1024, 2) if exists else 0,
            "最後修改時間": mtime,
            "路徑": str(path),
        })
    return pd.DataFrame(rows)


def _refresh_management_data() -> None:
    try:
        st.cache_data.clear()
    except Exception:
        pass
    st.session_state["v21_management_refresh_seq"] = int(st.session_state.get("v21_management_refresh_seq", 0)) + 1
    st.session_state["v21_management_last_refresh"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _extract_rows(obj: Any) -> List[Dict[str, Any]]:
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if isinstance(obj, dict):
        for key in ["records", "data", "items", "rows", "recommendations", "list", "history"]:
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
        "code": "股票代號", "stock_code": "股票代號", "symbol": "股票代號", "股票": "股票代號",
        "name": "股票名稱", "stock_name": "股票名稱", "market": "市場別",
        "industry": "產業", "category": "類別", "sector": "產業",
        "date": "推薦日期", "recommend_date": "推薦日期", "created_at": "推薦日期", "建立時間": "推薦日期",
        "score": "推薦分數", "total_score": "推薦分數", "final_score": "推薦分數",
        "status": "狀態", "result": "命中結果", "opportunity_type": "機會型態", "entry_timing": "進場時機",
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


def _load_many(paths: List[Path], dedupe_latest: bool = False) -> Tuple[pd.DataFrame, List[str]]:
    rows: List[Dict[str, Any]] = []
    notes: List[str] = []
    for path in paths:
        obj = _safe_load_json(path)
        part = _extract_rows(obj)
        notes.append(f"{path.name}：{len(part)} 筆" if part else f"{path.name}：0 筆或不存在")
        for r in part:
            rr = dict(r)
            rr.setdefault("資料來源檔", path.name)
            rows.append(rr)
    if not rows:
        return pd.DataFrame(), notes
    df = _normalize_df(pd.DataFrame(rows))
    if dedupe_latest and "股票代號" in df.columns:
        df["_dt"] = pd.to_datetime(df.get("推薦日期", pd.Series([None] * len(df))), errors="coerce")
        df["_seq"] = range(len(df))
        df = df.sort_values(["股票代號", "_dt", "_seq"]).drop_duplicates("股票代號", keep="last")
        df = df.drop(columns=["_dt", "_seq"], errors="ignore")
    return df.reset_index(drop=True), notes


def _num(val: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(val):
            return default
        s = str(val).strip().replace("%", "").replace(",", "")
        if not s or s.lower() in ["nan", "none", "null", "--", "-"]:
            return default
        return float(s)
    except Exception:
        return default


def _to_num(series: pd.Series, default: float = 0.0) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.replace("%", "", regex=False).str.replace(",", "", regex=False), errors="coerce").fillna(default)


def _risk_rank(value: Any) -> int:
    s = str(value or "")
    if any(x in s for x in ["極高", "高風險", "偏高", "不建議"]):
        return 4
    if any(x in s for x in ["中高", "中等", "中風險"]):
        return 3
    if any(x in s for x in ["低", "偏低", "保守"]):
        return 1
    return 2


def _hit_flag(row: pd.Series) -> bool:
    result = str(row.get("命中結果", "")) + str(row.get("是否達標_回測", "")) + str(row.get("績效評語", ""))
    if any(x in result for x in ["達標", "命中", "成功", "有效", "偏強"]):
        return True
    perf = _num(row.get("推薦後5日%", row.get("推薦後10日%", row.get("推薦後20日%", 0))))
    max_gain = _num(row.get("推薦後最大漲幅%", 0))
    max_dd = _num(row.get("推薦後最大回撤%", 0))
    return perf >= 3 or max_gain >= 6 or (perf > 0 and max_dd > -4)


def _fail_flag(row: pd.Series) -> bool:
    result = str(row.get("命中結果", "")) + str(row.get("是否停損_回測", "")) + str(row.get("績效評語", ""))
    if any(x in result for x in ["停損", "失敗", "回撤過大", "不佳"]):
        return True
    perf = _num(row.get("推薦後5日%", row.get("推薦後10日%", row.get("推薦後20日%", 0))))
    max_dd = _num(row.get("推薦後最大回撤%", 0))
    return perf <= -3 or max_dd <= -6


def _display_cols(df: pd.DataFrame, preferred: List[str], limit_extra: int = 25) -> List[str]:
    cols = [c for c in preferred if c in df.columns]
    for c in df.columns:
        if c not in cols and not c.startswith("_"):
            cols.append(c)
        if len(cols) >= len(preferred) + limit_extra:
            break
    return cols


def _kpi_row(items: List[Tuple[str, str, Optional[str]]]) -> None:
    cols = st.columns(len(items))
    for col, (label, value, delta) in zip(cols, items):
        col.metric(label, value, delta=delta)


def _filter_df(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    with st.expander("篩選條件", expanded=False):
        c1, c2, c3 = st.columns(3)
        with c1:
            if "推薦日期" in out.columns:
                dts = pd.to_datetime(out["推薦日期"], errors="coerce").dt.date.dropna()
                if not dts.empty:
                    min_d, max_d = dts.min(), dts.max()
                    val = st.date_input("推薦日期區間", value=(min_d, max_d), min_value=min_d, max_value=max_d, key=f"{prefix}_date")
                    if isinstance(val, tuple) and len(val) == 2:
                        start_d, end_d = val
                        mask = pd.to_datetime(out["推薦日期"], errors="coerce").dt.date.between(start_d, end_d)
                        out = out[mask.fillna(False)]
        with c2:
            if "推薦分數" in out.columns:
                min_score = st.slider("最低推薦分數", 0, 100, 0, key=f"{prefix}_score")
                out = out[_to_num(out["推薦分數"]) >= min_score]
        with c3:
            keyword = st.text_input("股票代號 / 名稱關鍵字", key=f"{prefix}_kw")
            if keyword:
                mask = pd.Series(False, index=out.index)
                for col in ["股票代號", "股票名稱"]:
                    if col in out.columns:
                        mask = mask | out[col].astype(str).str.contains(keyword, case=False, na=False)
                out = out[mask]
        filter_cols = ["類別", "產業", "推薦型態", "機會型態", "進場時機", "單檔風險等級", "追高風險等級", "大盤策略模式"]
        cols = st.columns(4)
        for i, col_name in enumerate(filter_cols):
            if col_name not in out.columns:
                continue
            values = sorted([x for x in out[col_name].dropna().astype(str).unique().tolist() if x.strip()])
            if not values:
                continue
            chosen = cols[i % 4].multiselect(col_name, values, default=[], key=f"{prefix}_{col_name}")
            if chosen:
                out = out[out[col_name].astype(str).isin(chosen)]
    return out.reset_index(drop=True)


def _allocation_action(row: pd.Series) -> str:
    score = _num(row.get("推薦分數", 0))
    risk = _risk_rank(row.get("單檔風險等級", row.get("追高風險等級", "")))
    timing = str(row.get("進場時機", ""))
    chase = str(row.get("追高風險等級", ""))
    market = str(row.get("大盤策略模式", ""))
    alloc = _num(row.get("動態建議倉位%", row.get("建議倉位%", 0)))
    if risk >= 4 or "不建議" in chase or "空頭" in market:
        return "減碼 / 僅觀察"
    if score >= 80 and risk <= 2 and alloc >= 15 and any(x in timing for x in ["可", "成熟", "分批", "接近"]):
        return "可分批建立"
    if score >= 70 and risk <= 3:
        return "觀察等確認"
    return "暫不加碼"


def _daily_action(row: pd.Series) -> str:
    risk = _risk_rank(row.get("單檔風險等級", row.get("追高風險等級", "")))
    status = str(row.get("狀態", ""))
    result = str(row.get("命中結果", ""))
    action = str(row.get("建議動作", ""))
    if any(x in status + result for x in ["停損", "失敗", "賣出"]):
        return "風險處理 / 檢查停損"
    if risk >= 4:
        return "避免追高 / 僅觀察"
    if any(x in action for x in ["分批", "觀察", "等待", "確認"]):
        return action[:40]
    return "優先觀察 / 等確認訊號"


def _tracking_grade(row: pd.Series) -> str:
    score = _num(row.get("推薦分數", 0))
    risk = _risk_rank(row.get("單檔風險等級", row.get("追高風險等級", "")))
    if risk >= 4:
        return "C｜高風險"
    if score >= 80 and risk <= 2:
        return "A｜優先追蹤"
    if score >= 70:
        return "B｜觀察確認"
    return "D｜低優先"


def _quality_grade(row: pd.Series) -> str:
    if _hit_flag(row):
        return "A｜有效"
    if _fail_flag(row):
        return "D｜失敗"
    perf = _num(row.get("推薦後5日%", row.get("推薦後10日%", row.get("推薦後20日%", 0))))
    if perf > 0:
        return "B｜偏正向"
    return "C｜待觀察"


def _quality_advice(row: pd.Series) -> str:
    if _fail_flag(row):
        risk = str(row.get("追高風險等級", ""))
        market = str(row.get("大盤策略模式", ""))
        if "高" in risk or "不建議" in risk:
            return "失敗偏向追高風險，後續同類型應降低權重或降倉位。"
        if "空" in market:
            return "失敗可能與大盤偏弱有關，空頭階段需提高風控。"
        return "建議回看K線與支撐是否失守，納入錯誤案例檢討。"
    if _hit_flag(row):
        return "型態表現有效，可納入後續權重正向參考。"
    return "樣本尚未明確，持續追蹤。"


def _group_quality(df: pd.DataFrame, field: str) -> pd.DataFrame:
    if df.empty or field not in df.columns:
        return pd.DataFrame()
    work = df.copy()
    work[field] = work[field].fillna("未分類").astype(str)
    work["_hit"] = work.apply(_hit_flag, axis=1)
    work["_fail"] = work.apply(_fail_flag, axis=1)
    work["_perf5"] = work.apply(lambda r: _num(r.get("推薦後5日%", r.get("推薦後10日%", r.get("推薦後20日%", 0)))), axis=1)
    work["_gain"] = work.apply(lambda r: _num(r.get("推薦後最大漲幅%", 0)), axis=1)
    work["_dd"] = work.apply(lambda r: _num(r.get("推薦後最大回撤%", 0)), axis=1)
    g = work.groupby(field, dropna=False).agg(
        樣本數=(field, "size"),
        命中率=("_hit", "mean"),
        失敗率=("_fail", "mean"),
        平均績效=("_perf5", "mean"),
        平均最大漲幅=("_gain", "mean"),
        平均最大回撤=("_dd", "mean"),
    ).reset_index()
    g["命中率%"] = (g["命中率"] * 100).round(1)
    g["失敗率%"] = (g["失敗率"] * 100).round(1)
    g["平均績效%"] = g["平均績效"].round(2)
    g["平均最大漲幅%"] = g["平均最大漲幅"].round(2)
    g["平均最大回撤%"] = g["平均最大回撤"].round(2)
    g["校正建議"] = g.apply(lambda r: _tune_suggestion(r), axis=1)
    return g[[field, "樣本數", "命中率%", "失敗率%", "平均績效%", "平均最大漲幅%", "平均最大回撤%", "校正建議"]].sort_values(["樣本數", "命中率%", "平均績效%"], ascending=[False, False, False])


def _tune_suggestion(row: pd.Series) -> str:
    n = int(row.get("樣本數", 0) or 0)
    hit = _num(row.get("命中率%", 0))
    fail = _num(row.get("失敗率%", 0))
    perf = _num(row.get("平均績效%", 0))
    dd = _num(row.get("平均最大回撤%", 0))
    if n < 5:
        return "樣本不足，暫不調權"
    if hit >= 60 and perf > 2 and dd > -5:
        return "建議提高權重"
    if fail >= 45 or perf < -1.5 or dd <= -7:
        return "建議降低權重"
    return "建議維持觀察"


def _portfolio_warnings(df: pd.DataFrame) -> List[str]:
    warnings: List[str] = []
    if df.empty:
        return warnings
    alloc_col = "動態建議倉位%" if "動態建議倉位%" in df.columns else ("建議倉位%" if "建議倉位%" in df.columns else None)
    sector_col = "類別" if "類別" in df.columns else ("產業" if "產業" in df.columns else None)
    if alloc_col:
        total_alloc = float(_to_num(df[alloc_col]).sum())
        if total_alloc > 100:
            warnings.append(f"建議倉位合計 {total_alloc:.1f}% 已超過 100%，請優先挑選低風險與高分標的，不要全部進場。")
        elif total_alloc > 70:
            warnings.append(f"建議倉位合計 {total_alloc:.1f}% 偏高，建議分批執行並保留現金。")
    if sector_col:
        top = df[sector_col].fillna("未分類").astype(str).value_counts(normalize=True).head(1)
        if not top.empty and float(top.iloc[0]) >= 0.4:
            warnings.append(f"族群集中度偏高：{top.index[0]} 佔 {float(top.iloc[0]) * 100:.1f}%，需注意同族群同步回檔。")
    risk_col = "單檔風險等級" if "單檔風險等級" in df.columns else ("追高風險等級" if "追高風險等級" in df.columns else None)
    if risk_col:
        high_risk_ratio = df[risk_col].map(lambda x: _risk_rank(x) >= 4).mean()
        if high_risk_ratio >= 0.25:
            warnings.append(f"高風險標的比例 {high_risk_ratio * 100:.1f}% 偏高，建議降低強勢追價股權重。")
    return warnings


def _render_source_status(notes: List[str]) -> None:
    with st.expander("資料來源狀態", expanded=False):
        st.code("\n".join(notes) if notes else "無資料來源")


def render_portfolio_tab(rec_df: pd.DataFrame, hist_df: pd.DataFrame, notes: List[str]) -> None:
    st.subheader("投資組合與資金配置")
    st.caption("整合原 v18 投資組合功能：檢查建議倉位、族群集中、風險標的與操作優先順序。")
    _render_source_status(notes)
    source = st.radio("分析資料來源", ["推薦清單 / 目前追蹤", "股神推薦紀錄 / 歷史全部"], horizontal=True, key="v21_port_src")
    df = rec_df.copy() if source.startswith("推薦清單") else hist_df.copy()
    if df.empty:
        st.warning("目前沒有可分析資料。請先在 7_股神推薦 匯入 10_推薦清單，或在 8_股神推薦紀錄建立紀錄。")
        return
    df = _filter_df(df, "v21_port")
    if df.empty:
        st.warning("篩選後沒有資料。")
        return
    df["v21操作優先順序"] = df.apply(_allocation_action, axis=1)
    alloc_col = "動態建議倉位%" if "動態建議倉位%" in df.columns else ("建議倉位%" if "建議倉位%" in df.columns else None)
    avg_score = _to_num(df["推薦分數"]).mean() if "推薦分數" in df.columns else 0
    total_alloc = _to_num(df[alloc_col]).sum() if alloc_col else 0
    risk_col = "單檔風險等級" if "單檔風險等級" in df.columns else ("追高風險等級" if "追高風險等級" in df.columns else None)
    high_risk = int(df[risk_col].map(lambda x: _risk_rank(x) >= 4).sum()) if risk_col else 0
    _kpi_row([
        ("追蹤標的", f"{len(df)} 檔", None),
        ("平均推薦分數", f"{avg_score:.1f}", None),
        ("建議倉位合計", f"{total_alloc:.1f}%", None),
        ("高風險標的", f"{high_risk} 檔", None),
    ])
    warnings = _portfolio_warnings(df)
    if warnings:
        for w in warnings:
            st.warning(w)
    else:
        st.success("目前組合未偵測到明顯過度集中或高風險倉位問題。")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("#### 操作優先順序")
        st.dataframe(df["v21操作優先順序"].value_counts().rename_axis("建議").reset_index(name="檔數"), use_container_width=True, hide_index=True)
    with c2:
        sector_col = "類別" if "類別" in df.columns else ("產業" if "產業" in df.columns else None)
        st.markdown("#### 族群集中")
        if sector_col:
            st.dataframe(df[sector_col].fillna("未分類").astype(str).value_counts().rename_axis(sector_col).reset_index(name="檔數"), use_container_width=True, hide_index=True)
        else:
            st.info("缺少類別 / 產業欄位。")
    with c3:
        type_col = "推薦型態" if "推薦型態" in df.columns else ("機會型態" if "機會型態" in df.columns else None)
        st.markdown("#### 推薦型態")
        if type_col:
            st.dataframe(df[type_col].fillna("未分類").astype(str).value_counts().rename_axis(type_col).reset_index(name="檔數"), use_container_width=True, hide_index=True)
        else:
            st.info("缺少推薦型態欄位。")
    display_cols = _display_cols(df, PORTFOLIO_COLUMNS)
    st.markdown("#### 投資組合明細")
    st.dataframe(df[display_cols], use_container_width=True, hide_index=True, height=520)
    st.download_button("下載投資組合分析 CSV", df[display_cols].to_csv(index=False).encode("utf-8-sig"), file_name=f"godpick_management_portfolio_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", mime="text/csv", use_container_width=True)


def render_daily_tab(rec_df: pd.DataFrame, hist_df: pd.DataFrame, notes: List[str]) -> None:
    st.subheader("每日追蹤報告")
    st.caption("整合原 v19 每日追蹤功能：今日操作重點、優先觀察、避免追高、風險處理與追蹤清單匯出。")
    _render_source_status(notes)
    df = rec_df.copy()
    if df.empty:
        df = hist_df.copy()
    if df.empty:
        st.warning("目前沒有推薦清單或推薦紀錄可追蹤。")
        return
    df = _filter_df(df, "v21_daily")
    if df.empty:
        st.warning("篩選後沒有資料。")
        return
    df["今日操作建議"] = df.apply(_daily_action, axis=1)
    df["追蹤分級"] = df.apply(_tracking_grade, axis=1)
    high_risk = df["今日操作建議"].astype(str).str.contains("避免追高|風險處理|停損", na=False).sum()
    priority = df["追蹤分級"].astype(str).str.startswith("A").sum()
    alloc_col = "動態建議倉位%" if "動態建議倉位%" in df.columns else ("建議倉位%" if "建議倉位%" in df.columns else None)
    alloc = _to_num(df[alloc_col]).sum() if alloc_col else 0
    _kpi_row([
        ("今日追蹤檔數", f"{len(df)} 檔", None),
        ("A級優先追蹤", f"{int(priority)} 檔", None),
        ("需風險處理 / 避免追高", f"{int(high_risk)} 檔", None),
        ("建議倉位合計", f"{alloc:.1f}%", None),
    ])
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### 今日操作重點")
        st.dataframe(df["今日操作建議"].value_counts().rename_axis("操作建議").reset_index(name="檔數"), use_container_width=True, hide_index=True)
    with c2:
        st.markdown("#### 追蹤分級")
        st.dataframe(df["追蹤分級"].value_counts().rename_axis("追蹤分級").reset_index(name="檔數"), use_container_width=True, hide_index=True)
    st.markdown("#### 今日追蹤明細")
    sort_cols = [c for c in ["追蹤分級", "推薦分數"] if c in df.columns]
    if sort_cols:
        ascending = [True if c == "追蹤分級" else False for c in sort_cols]
        df = df.sort_values(sort_cols, ascending=ascending)
    display_cols = _display_cols(df, DAILY_COLUMNS)
    st.dataframe(df[display_cols], use_container_width=True, hide_index=True, height=560)
    st.download_button("下載每日追蹤報告 CSV", df[display_cols].to_csv(index=False).encode("utf-8-sig"), file_name=f"godpick_daily_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", mime="text/csv", use_container_width=True)


def render_quality_tab(all_df: pd.DataFrame, notes: List[str]) -> None:
    st.subheader("推薦品質儀表板")
    st.caption("整合原 v20 推薦品質功能：命中率、失敗率、型態品質、追高風險與校正建議。")
    _render_source_status(notes)
    if all_df.empty:
        st.warning("目前沒有推薦紀錄或推薦清單可分析。請先建立紀錄，並在 8_股神推薦紀錄執行推薦後績效更新。")
        return
    df = _filter_df(all_df.copy(), "v21_quality")
    if df.empty:
        st.warning("篩選後沒有資料。")
        return
    df["品質分級"] = df.apply(_quality_grade, axis=1)
    df["品質建議"] = df.apply(_quality_advice, axis=1)
    hits = int(df.apply(_hit_flag, axis=1).sum())
    fails = int(df.apply(_fail_flag, axis=1).sum())
    perf_col = "推薦後5日%" if "推薦後5日%" in df.columns else ("推薦後10日%" if "推薦後10日%" in df.columns else ("推薦後20日%" if "推薦後20日%" in df.columns else None))
    avg_perf = _to_num(df[perf_col]).mean() if perf_col else 0
    avg_gain = _to_num(df["推薦後最大漲幅%"]).mean() if "推薦後最大漲幅%" in df.columns else 0
    avg_dd = _to_num(df["推薦後最大回撤%"]).mean() if "推薦後最大回撤%" in df.columns else 0
    hit_rate = hits / len(df) * 100 if len(df) else 0
    fail_rate = fails / len(df) * 100 if len(df) else 0
    _kpi_row([
        ("分析樣本", f"{len(df)} 筆", None),
        ("命中率", f"{hit_rate:.1f}%", None),
        ("失敗率", f"{fail_rate:.1f}%", None),
        ("平均績效", f"{avg_perf:.2f}%", None),
        ("平均最大漲幅", f"{avg_gain:.2f}%", None),
        ("平均最大回撤", f"{avg_dd:.2f}%", None),
    ])
    c1, c2 = st.columns(2)
    with c1:
        field = st.selectbox("品質分組欄位", [f for f in GROUP_FIELDS if f in df.columns] or ["推薦型態"], key="v21_quality_group")
    with c2:
        min_samples = st.slider("最少樣本數", 1, 50, 3, key="v21_quality_min_samples")
    group_df = _group_quality(df, field) if field in df.columns else pd.DataFrame()
    if not group_df.empty:
        group_df = group_df[group_df["樣本數"] >= min_samples]
        st.markdown("#### 分組品質與校正建議")
        st.dataframe(group_df, use_container_width=True, hide_index=True, height=360)
    st.markdown("#### 失敗案例檢討")
    fail_df = df[df.apply(_fail_flag, axis=1)].copy()
    if fail_df.empty:
        st.success("目前沒有明確失敗案例。")
    else:
        st.dataframe(fail_df[_display_cols(fail_df, QUALITY_COLUMNS)], use_container_width=True, hide_index=True, height=300)
    st.markdown("#### 品質明細")
    display_cols = _display_cols(df, QUALITY_COLUMNS)
    st.dataframe(df[display_cols], use_container_width=True, hide_index=True, height=520)
    st.download_button("下載品質分析 CSV", df[display_cols].to_csv(index=False).encode("utf-8-sig"), file_name=f"godpick_quality_dashboard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", mime="text/csv", use_container_width=True)
    if not group_df.empty:
        st.download_button("下載分組校正建議 CSV", group_df.to_csv(index=False).encode("utf-8-sig"), file_name=f"godpick_quality_group_tune_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", mime="text/csv", use_container_width=True)


def render_cleanup_tab() -> None:
    st.subheader("側邊欄整理建議")
    st.caption("v21 已把 12、13、14 的功能整合到同一個管理中心。若要側邊欄乾淨，可以移除舊獨立頁。")
    st.markdown("#### 建議保留")
    st.code("""7_股神推薦.py
8_股神推薦紀錄.py
10_推薦清單.py
12_股神管理中心.py""")
    st.markdown("#### 若已確認 v21 正常，可刪除或改副檔名 .bak")
    st.code("""pages/12_股神投資組合.py
pages/13_股神每日追蹤報告.py
pages/14_股神推薦品質儀表板.py

或 #U 編碼檔名：
pages/12_#U80a1#U795e#U6295#U8cc7#U7d44#U5408.py
pages/13_#U80a1#U795e#U6bcf#U65e5#U8ffd#U8e64#U5831#U544a.py
pages/14_#U80a1#U795e#U63a8#U85a6#U54c1#U8cea#U5100#U8868#U677f.py""")
    st.warning("不要刪除 7、8、10，也不要刪除任何 JSON。13、14 只是獨立分析頁，功能已整合進 v21。")
    st.info("如果你不想刪檔，也可以先把舊頁副檔名改成 .bak，確認一週沒問題後再刪除。")


def main() -> None:
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    if inject_pro_theme:
        try:
            inject_pro_theme()
        except Exception:
            pass
    if render_pro_hero:
        try:
            render_pro_hero("股神管理中心", "v21.1｜新增資訊重整帶入｜投資組合、每日追蹤、推薦品質整合入口")
        except Exception:
            st.title(PAGE_TITLE)
    else:
        st.title(PAGE_TITLE)
    st.caption("本頁整合 v18 投資組合、v19 每日追蹤、v20 推薦品質儀表板；不修改推薦邏輯、不寫入 JSON、不影響掃描速度。")

    c_refresh, c_status = st.columns([1.2, 4])
    with c_refresh:
        if st.button("🔄 資訊重整帶入", type="primary", use_container_width=True, help="重新讀取推薦清單、推薦紀錄與品質分析資料，並清除 Streamlit 快取。"):
            _refresh_management_data()
            st.success("已重新讀取資料來源並清除快取。")
    with c_status:
        last_refresh = st.session_state.get("v21_management_last_refresh", "尚未手動重整")
        st.info(f"目前資料讀取狀態：每次進頁會自動讀取 JSON；手動重整時間：{last_refresh}")

    with st.expander("資料檔案更新狀態", expanded=False):
        st.dataframe(_file_status_rows(ALL_DATA_FILES), use_container_width=True, hide_index=True)

    rec_df, rec_notes = _load_many(RECOMMEND_FILES, dedupe_latest=True)
    hist_df, hist_notes = _load_many(RECORD_FILES, dedupe_latest=False)
    all_df, all_notes = _load_many(ALL_DATA_FILES, dedupe_latest=False)
    notes = ["推薦清單："] + rec_notes + ["推薦紀錄："] + hist_notes

    tabs = st.tabs(["投資組合", "每日追蹤", "推薦品質", "側邊欄整理"])
    with tabs[0]:
        render_portfolio_tab(rec_df, hist_df, notes)
    with tabs[1]:
        render_daily_tab(rec_df, hist_df, notes)
    with tabs[2]:
        render_quality_tab(all_df, all_notes)
    with tabs[3]:
        render_cleanup_tab()


if __name__ == "__main__":
    main()
