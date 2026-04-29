from datetime import date
import json
from pathlib import Path
import time
import pandas as pd
import streamlit as st

from utils import (
    get_normalized_watchlist,
    get_realtime_watchlist_df,
    apply_font_scale,
    get_font_scale,
    inject_pro_theme,
    render_pro_hero,
    render_pro_section,
    render_pro_kpi_row,
    format_number,
)


# ============================================================
# 儀表板強制市場別修正
# ============================================================
def _dashboard_force_fix_watchlist_data(data):
    """修正 watchlist 舊資料造成的市場別錯誤。
    目前防呆：3548 兆利必須為上櫃。
    """
    if not isinstance(data, dict):
        return data

    fixed = {}
    for group_name, items in data.items():
        if not isinstance(items, list):
            fixed[group_name] = items
            continue

        new_items = []
        for item in items:
            if not isinstance(item, dict):
                new_items.append(item)
                continue

            row = dict(item)
            code = str(row.get("code") or row.get("股票代號") or row.get("代號") or "").strip()
            if code.endswith(".0"):
                code = code[:-2]

            if code == "3548":
                row["code"] = "3548"
                row["股票代號"] = "3548"
                row["name"] = "兆利"
                row["股票名稱"] = "兆利"
                row["market"] = "上櫃"
                row["市場別"] = "上櫃"
                row["category"] = row.get("category") or "光學鏡頭"
                row["類別"] = row.get("類別") or "光學鏡頭"

            new_items.append(row)

        fixed[group_name] = new_items

    return fixed


PAGE_TITLE = "儀表板"
PFX = "dash_"


def _k(key: str) -> str:
    return f"{PFX}{key}"


def _safe_float(v, default=None):
    try:
        if pd.isna(v):
            return default
    except Exception:
        pass
    try:
        return float(v)
    except Exception:
        return default



# ============================================================
# v39 儀表板大盤快照串接
# 僅讀取 0_大盤趨勢輸出的 market_snapshot.json，不在儀表板重抓網路。
# ============================================================
def _read_market_snapshot_dash_v39() -> dict:
    for p in [Path("market_snapshot.json"), Path("macro_mode_bridge.json")]:
        try:
            if not p.exists():
                continue
            with p.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                data["_snapshot_file"] = str(p)
                return data
        except Exception:
            continue
    return {}


def _dash_pick(snapshot: dict, key: str, default="—"):
    if not isinstance(snapshot, dict):
        return default
    v = snapshot.get(key, default)
    if v is None or v == "":
        return default
    return v


def _dash_num(v, digits=2, signed=False, suffix=""):
    try:
        if v is None or pd.isna(v):
            return "—"
    except Exception:
        pass
    try:
        n = float(str(v).replace(",", "").replace("%", ""))
        sign = "+" if signed and n > 0 else ""
        return f"{sign}{n:,.{digits}f}{suffix}"
    except Exception:
        return str(v) if str(v).strip() else "—"


def render_market_risk_dashboard_v39():
    snapshot = _read_market_snapshot_dash_v39()
    render_pro_section("大盤風控快照", "讀取 0_大盤趨勢輸出的 market_snapshot.json，作為自選股監控的市場背景。")

    if not snapshot:
        st.info("尚未讀到 market_snapshot.json。請先到 0_大盤趨勢更新並寫入股神橋接檔。")
        return

    score = _dash_pick(snapshot, "market_score", None)
    try:
        score_float = float(score)
    except Exception:
        score_float = None

    trend = _dash_pick(snapshot, "market_trend")
    risk = _dash_pick(snapshot, "market_risk_level")
    gate = _dash_pick(snapshot, "risk_gate", _dash_pick(snapshot, "risk_gate_mode"))
    session_label = _dash_pick(snapshot, "market_session_label", _dash_pick(snapshot, "market_session"))
    quality = _dash_pick(snapshot, "data_quality")
    updated_at = _dash_pick(snapshot, "updated_at")

    render_pro_kpi_row([
        {
            "label": "大盤分數",
            "value": _dash_num(score, 1),
            "delta": f"{trend}｜風險 {risk}",
            "delta_class": "pro-kpi-delta-up" if (score_float or 0) >= 60 else "pro-kpi-delta-down" if (score_float or 0) < 45 else "pro-kpi-delta-flat",
        },
        {
            "label": "風控閘門",
            "value": gate,
            "delta": f"交易時段：{session_label}",
            "delta_class": "pro-kpi-delta-flat",
        },
        {
            "label": "資料品質",
            "value": quality,
            "delta": f"更新：{updated_at}",
            "delta_class": "pro-kpi-delta-flat",
        },
    ])

    effect = snapshot.get("godpick_market_effect", {}) if isinstance(snapshot.get("godpick_market_effect"), dict) else {}
    effect_desc = effect.get("effect_summary") or effect.get("description") or snapshot.get("trend_comment") or "—"
    st.caption(f"股神推薦影響：{effect_desc}")
@st.cache_data(ttl=5, show_spinner=False)
def _prepare_dashboard_metrics(df: pd.DataFrame) -> dict:
    if df is None or df.empty:
        return {
            "total_count": 0,
            "success_count": 0,
            "fail_count": 0,
            "up_count": 0,
            "down_count": 0,
            "flat_count": 0,
            "avg_change_pct": None,
            "total_volume": None,
        }

    work = df.copy()

    if "漲跌" in work.columns:
        work["漲跌"] = pd.to_numeric(work["漲跌"], errors="coerce")
    if "漲跌幅(%)" in work.columns:
        work["漲跌幅(%)"] = pd.to_numeric(work["漲跌幅(%)"], errors="coerce")
    if "總量" in work.columns:
        work["總量"] = pd.to_numeric(work["總量"], errors="coerce")

    total_count = len(work)
    success_count = int(work["現價"].notna().sum()) if "現價" in work.columns else 0
    fail_count = max(0, total_count - success_count)
    up_count = int((work["漲跌"] > 0).sum()) if "漲跌" in work.columns else 0
    down_count = int((work["漲跌"] < 0).sum()) if "漲跌" in work.columns else 0
    flat_count = max(0, success_count - up_count - down_count)

    avg_change_pct = work["漲跌幅(%)"].dropna().mean() if "漲跌幅(%)" in work.columns else None
    total_volume = work["總量"].dropna().sum() if "總量" in work.columns else None

    return {
        "total_count": total_count,
        "success_count": success_count,
        "fail_count": fail_count,
        "up_count": up_count,
        "down_count": down_count,
        "flat_count": flat_count,
        "avg_change_pct": avg_change_pct,
        "total_volume": total_volume,
    }


@st.cache_data(ttl=5, show_spinner=False)
def _prepare_dashboard_table(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    show_cols = [
        "群組", "股票代號", "股票名稱", "市場別",
        "現價", "昨收", "漲跌", "漲跌幅(%)",
        "開盤", "最高", "最低", "總量",
        "價格來源", "漲跌來源", "更新時間", "是否成功", "訊息"
    ]
    show_cols = [c for c in show_cols if c in df.columns]

    display_df = df[show_cols].copy()

    numeric_cols = ["現價", "昨收", "漲跌", "漲跌幅(%)", "開盤", "最高", "最低", "總量"]
    exist_numeric_cols = [c for c in numeric_cols if c in display_df.columns]
    if exist_numeric_cols:
        display_df[exist_numeric_cols] = display_df[exist_numeric_cols].apply(pd.to_numeric, errors="coerce")

    if "群組" in display_df.columns and "漲跌幅(%)" in display_df.columns:
        display_df = display_df.sort_values(
            by=["群組", "漲跌幅(%)"],
            ascending=[True, False],
            na_position="last"
        ).reset_index(drop=True)

    return display_df


def render_dashboard_summary(df: pd.DataFrame):
    metrics = _prepare_dashboard_metrics(df)

    avg_change_pct = metrics["avg_change_pct"]
    avg_text = f"{avg_change_pct:+.2f}%" if avg_change_pct is not None and pd.notna(avg_change_pct) else "—"

    if avg_change_pct is not None and pd.notna(avg_change_pct):
        if avg_change_pct > 0:
            avg_class = "pro-kpi-delta-up"
        elif avg_change_pct < 0:
            avg_class = "pro-kpi-delta-down"
        else:
            avg_class = "pro-kpi-delta-flat"
    else:
        avg_class = "pro-kpi-delta-flat"

    render_pro_kpi_row([
        {
            "label": "監控股票數",
            "value": f"{metrics['total_count']:,}",
            "delta": f"成功 {metrics['success_count']}｜失敗 {metrics['fail_count']}｜上漲 {metrics['up_count']}｜下跌 {metrics['down_count']}",
            "delta_class": "pro-kpi-delta-flat"
        },
        {
            "label": "平均漲跌幅",
            "value": avg_text,
            "delta": "Portfolio Breadth",
            "delta_class": avg_class
        },
        {
            "label": "合計成交量",
            "value": format_number(metrics["total_volume"], 0),
            "delta": "Total Session Volume",
            "delta_class": "pro-kpi-delta-flat"
        },
    ])


def render_dashboard_table(df: pd.DataFrame, height: int = 720):
    display_df = _prepare_dashboard_table(df)

    if display_df.empty:
        st.info("目前沒有可顯示的即時資料。")
        return

    format_dict = {}
    for col in ["現價", "昨收", "漲跌", "漲跌幅(%)", "開盤", "最高", "最低"]:
        if col in display_df.columns:
            format_dict[col] = "{:,.2f}"
    if "總量" in display_df.columns:
        format_dict["總量"] = "{:,.0f}"

    def color_change(val):
        if pd.isna(val):
            return ""
        try:
            v = float(val)
        except Exception:
            return ""
        if v > 0:
            return "color: #dc2626; font-weight: 800;"
        elif v < 0:
            return "color: #059669; font-weight: 800;"
        return "color: #64748b; font-weight: 700;"

    styler = display_df.style.format(format_dict, na_rep="—")

    if "漲跌" in display_df.columns:
        styler = styler.map(color_change, subset=["漲跌"])
    if "漲跌幅(%)" in display_df.columns:
        styler = styler.map(color_change, subset=["漲跌幅(%)"])

    st.dataframe(styler, use_container_width=True, hide_index=True, height=height)


def _init_page_state():
    if "font_scale" not in st.session_state:
        st.session_state.font_scale = get_font_scale()

    if _k("last_refresh_date") not in st.session_state:
        st.session_state[_k("last_refresh_date")] = ""

    if _k("force_refresh") not in st.session_state:
        st.session_state[_k("force_refresh")] = False

    if _k("refresh_token") not in st.session_state:
        st.session_state[_k("refresh_token")] = "init"


def main():
    st.set_page_config(page_title=PAGE_TITLE, page_icon="📊", layout="wide")
    _init_page_state()

    apply_font_scale(st.session_state.font_scale)
    inject_pro_theme()

    render_pro_hero(
        "盤面儀表板",
        "專業監控視圖｜集中觀察自選股強弱、即時波動與成交量分布"
    )
    render_market_risk_dashboard_v39()


    watchlist_dict = get_normalized_watchlist()
    if not watchlist_dict:
        st.warning("目前沒有自選股群組。")
        st.stop()

    query_date = date.today().strftime("%Y%m%d")

    top1, top2, top3 = st.columns([1.2, 3, 2])
    with top1:
        refresh_clicked = st.button("更新即時資料", type="primary", use_container_width=True)
    with top2:
        st.caption(f"查詢日期：{query_date}")
    with top3:
        st.caption(f"刷新識別：{st.session_state[_k('refresh_token')]}")

    if refresh_clicked:
        new_token = str(int(time.time() * 1000))
        st.session_state[_k("force_refresh")] = True
        st.session_state[_k("refresh_token")] = new_token
        st.session_state[_k("last_refresh_date")] = query_date

        # 清掉即時資料與表格快取；若 utils 的自選股快取存在，也一併清除。
        try:
            get_realtime_watchlist_df.clear()
        except Exception:
            pass
        try:
            get_normalized_watchlist.clear()
        except Exception:
            pass
        try:
            _prepare_dashboard_metrics.clear()
        except Exception:
            pass
        try:
            _prepare_dashboard_table.clear()
        except Exception:
            pass

    refresh_token = st.session_state[_k("refresh_token")]

    realtime_error = ""
    with st.spinner("正在讀取即時資料..."):
        # DASHBOARD_FORCE_FIX_SAFE_V6
        try:
            watchlist_dict = _dashboard_force_fix_watchlist_data(watchlist_dict)
        except Exception:
            pass

        try:
            realtime_df = get_realtime_watchlist_df(
                watchlist_dict=watchlist_dict,
                query_date=query_date,
                refresh_token=refresh_token,
            )
        except Exception as e:
            realtime_df = pd.DataFrame()
            realtime_error = str(e)

    st.session_state[_k("force_refresh")] = False

    if realtime_df is None or realtime_df.empty:
        st.warning("目前即時資料暫時沒有回傳價格，但自選股清單已正常讀取。")
        fallback_rows = []
        for group_name, items in (watchlist_dict or {}).items():
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, dict):
                    continue
                fallback_rows.append(
                    {
                        "群組": group_name,
                        "股票代號": str(item.get("code", "")).strip(),
                        "股票名稱": str(item.get("name", "")).strip(),
                        "市場別": str(item.get("market", "")).strip(),
                        "狀態": "即時資料待更新",
                    }
                )
        fallback_df = pd.DataFrame(fallback_rows)
        render_pro_kpi_row(
            [
                {"label": "自選股群組", "value": len(watchlist_dict or {}), "delta": "watchlist 正常", "delta_class": "pro-kpi-delta-flat"},
                {"label": "自選股總數", "value": len(fallback_df), "delta": "即時資料未回傳", "delta_class": "pro-kpi-delta-down"},
                {"label": "查詢日期", "value": query_date, "delta": "可按更新重試", "delta_class": "pro-kpi-delta-flat"},
            ]
        )
        if not fallback_df.empty:
            st.dataframe(fallback_df, use_container_width=True, hide_index=True)
        with st.expander("即時資料診斷", expanded=True):
            if realtime_error:
                st.error(realtime_error)
            st.caption("這裡不再把抓不到的現價偽裝成 0。請先確認 11_資料診斷 的即時資料測試；若單股可抓到，通常是快取或批次 API 暫時異常。")
        return

    render_dashboard_summary(realtime_df)
    render_pro_section("即時盤面清單", "可快速掃描各群組成員的價格、漲跌幅與成交量變化")
    fail_df = realtime_df[realtime_df["現價"].isna()] if "現價" in realtime_df.columns else pd.DataFrame()
    if not fail_df.empty:
        with st.expander(f"資料來源診斷｜仍有 {len(fail_df)} 檔無價格", expanded=False):
            diag_cols = [c for c in ["群組", "股票代號", "股票名稱", "市場別", "價格來源", "漲跌來源", "訊息"] if c in fail_df.columns]
            st.dataframe(fail_df[diag_cols], use_container_width=True, hide_index=True)
            st.caption("常見原因：TWSE/TPEX MIS 即時 API 暫時無回應、Yahoo 也暫時無資料、股票市場別判斷錯誤、或該股票近期無交易資料。")

    render_dashboard_table(realtime_df, height=760)


if __name__ == "__main__":
    main()
