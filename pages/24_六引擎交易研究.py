# -*- coding: utf-8 -*-
"""H130 六引擎交易研究中心：研究工具，絕非自動下單。"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import io
import json

import pandas as pd
import streamlit as st

try:
    from app_auth import require_login
    require_login()
except Exception as exc:
    st.error(f"登入系統載入失敗：{exc}")
    st.stop()

from godpick_h130_six_engine import (
    VERSION, trade_ideas, technical_analysis, news_impacts, backtest_sma,
    portfolio_risk, journal_review, make_template,
)

st.set_page_config(page_title="六引擎交易研究中心｜H130", layout="wide")
st.title("六引擎交易研究中心｜" + VERSION)
st.caption("以證據為基礎的研究工作台｜不送出委託、不變更權威推薦／設定／持股／交易紀錄；不保證獲利。")
asof = st.date_input("研究資料截止日（台灣市場交易日；請自行核對）", value=date.today(), max_value=date.today(), key="h130_asof")


def load_uploaded(upload) -> pd.DataFrame:
    if upload is None:
        return pd.DataFrame()
    if getattr(upload, "size", 0) > 20 * 1024 * 1024:
        st.error("檔案超過20 MB上限，請先篩選最近期間及必要欄位。")
        return pd.DataFrame()
    try:
        if upload.name.lower().endswith(".xlsx"):
            return pd.read_excel(upload, sheet_name=0, dtype={"股票代號": str})
        return pd.read_csv(upload, encoding="utf-8-sig", dtype={"股票代號": str})
    except Exception as exc:
        st.error(f"資料讀取失敗：{type(exc).__name__}，請檢查格式與欄名。")
        return pd.DataFrame()


def latest_local_research() -> tuple[pd.DataFrame, str]:
    path = Path("godpick_latest_recommendations.json")
    try:
        with path.open("r", encoding="utf-8") as stream:
            data = json.load(stream)
        if not isinstance(data, dict):
            return pd.DataFrame(), "未發現推薦權威快照"
        rows = data.get("recommendations", [])
        return pd.DataFrame(rows if isinstance(rows, list) else []), str(data.get("saved_at") or "UNKNOWN")
    except Exception:
        return pd.DataFrame(), "沒有本機快照（可上傳本期主管表）"


def _safe_csv(data: pd.DataFrame) -> bytes:
    """Avoid spreadsheet formula execution when imported headlines start with =/+/@."""
    safe = data.copy()
    for column in safe.select_dtypes(include="object").columns:
        safe[column] = safe[column].map(
            lambda v: "'" + v if isinstance(v, str) and v.lstrip().startswith(("=", "+", "@")) else v
        )
    return safe.to_csv(index=False).encode("utf-8-sig")


def table_result(title: str, data: pd.DataFrame, key: str):
    st.markdown("**" + title + "**")
    if not isinstance(data, pd.DataFrame) or data.empty:
        st.info("尚無資料；請提供必要的輸入。")
        return
    st.dataframe(data, use_container_width=True, hide_index=True)
    st.download_button("下載本模組 CSV", data=_safe_csv(data),
                       file_name=f"H130_{key}.csv", mime="text/csv", key=f"h130_dl_{key}")


with st.expander("共同研究資料｜實際資料來源與截止日期", expanded=True):
    st.warning("資料日期、來源及股票證據必須查證。舊版推薦快照不是當日行情；研究股不等於正式買進授權。")
    stocks_file = st.file_uploader("候選名單：上傳當期 H126 主管決策 CSV／Excel（第一張工作表）", type=["csv", "xlsx"], key="h130_candidates")
    stock_input = load_uploaded(stocks_file)
    source_note = "已上傳候選檔（仍需確認來源與日期）"
    if stock_input.empty and stocks_file is None:
        transferred = st.session_state.get("h130_h126_current_authority")
        if isinstance(transferred, pd.DataFrame) and not transferred.empty:
            stock_input = transferred.copy()
            built_at = st.session_state.get("h130_h126_manager_built_at", "UNKNOWN")
            source_note = (f"第7頁本工作階段的 H126 主管決策（建立時間 {built_at}）；"
                           "時間不代表行情交易日，仍依每檔研究截止日驗證")
        else:
            stock_input, saved = latest_local_research()
            source_note = f"本機舊研究快照；保存時間：{saved}（不可當成今日推薦）"
    st.caption(source_note)
    prices_file = st.file_uploader("K 線：OHLCV 歷史行情 CSV／Excel（可包含多檔，用於組合相關性）", type=["csv", "xlsx"], key="h130_prices")
    all_prices = load_uploaded(prices_file)
    st.caption("K線欄位：股票代號、日期、開盤價、最高價、最低價、收盤價、成交量；必須使用可追溯來源。")
    st.download_button("下載 K 線格式範例", data=make_template("行情").encode("utf-8-sig"), file_name="H130_Prices_Template.csv", mime="text/csv")

one, two, three, four, five, six = st.tabs([
    "01 市場機會", "02 技術分析", "03 新聞事件", "04 樣本外回測", "05 組合風控", "06 交易日誌",
])

with one:
    st.markdown("#### 市場機會｜正式權威先行")
    st.caption("優先 H126 認證狀態與 H109 風控；沒有當日官方證據就列待驗證，不以 H102 舊分數批准買進。")
    ideas = trade_ideas(stock_input, asof=asof, limit=5)
    table_result("優先研究標的（最多五檔，不等於買進）", ideas, "Ideas")

with two:
    st.markdown("#### 技術分析｜日線與動能指標")
    ticker = st.text_input("分析股票代號（若 K 線含多檔則用此代號篩選）", value="", key="h130_ticker", max_chars=6).strip()
    tech_prices = all_prices
    if ticker and "股票代號" in tech_prices:
        tech_prices = tech_prices[tech_prices["股票代號"].astype(str) == ticker]
    auto_code = ticker
    if st.button("從現有行情服務按需取得日K（僅查詢，不改權威資料）", key="h130_fetch", disabled=not ticker):
        try:
            from utils import get_history_data
            hist = get_history_data(ticker, start_date=asof - timedelta(days=400), end_date=asof)
            if hist is None or hist.empty:
                st.warning("當前資料服務沒有可用日K；不使用舊價格偽裝。")
            else:
                hist = hist.copy()
                hist["股票代號"] = ticker
                st.session_state["h130_history"] = hist
                st.session_state["h130_history_code"] = ticker
                st.session_state["h130_history_asof"] = asof
        except Exception as exc:
            st.error(f"行情取得失敗：{type(exc).__name__}（不影響原推薦）")
    if tech_prices.empty and st.session_state.get("h130_history_code") == ticker and st.session_state.get("h130_history_asof") == asof:
        tech_prices = st.session_state.get("h130_history", pd.DataFrame())
    tech_result = technical_analysis(tech_prices, asof=asof)
    table_result("支撐、壓力、均線與 RSI 技術參考", tech_result, "Technical")

with three:
    st.markdown("#### 新聞轉策略｜只分析真實來源事件")
    event_file = st.file_uploader("上傳新聞／公告事件表（必須有日期、標題、來源網址）", type=["csv", "xlsx"], key="h130_event_file")
    st.download_button("下載事件格式範例", data=make_template("新聞").encode("utf-8-sig"), file_name="H130_News_Template.csv", mime="text/csv")
    event_result = news_impacts(load_uploaded(event_file), asof=asof)
    table_result("事件驗證、可能情境與失效條件", event_result, "News")

with four:
    st.markdown("#### 交易策略回測｜固定規則＋樣本外＋交易成本")
    st.caption("SMA20 上穿 SMA60，於次交易日開盤進場、持有固定交易日後於開盤出場；以較早70%訓練、後30%樣本外評估。")
    price_ok = st.checkbox("我已確認行情完成除權息及拆併股調整，且來源日期可信", value=False, key="h130_adjusted")
    days = st.slider("固定持有交易日", min_value=2, max_value=20, value=5, key="h130_hold")
    test_price = tech_prices if isinstance(tech_prices, pd.DataFrame) and not tech_prices.empty else all_prices
    backtest_result, trades = backtest_sma(test_price, asof=asof, adjusted_confirmed=price_ok, hold_days=days)
    table_result("樣本外統計（樣本不足時不計勝率）", backtest_result, "Backtest")
    if not trades.empty:
        table_result("逐筆回測紀錄（開盤價／手續費／證交稅／滑價）", trades, "Backtest_Trades")

with five:
    st.markdown("#### 投資組合風險管理｜集中度與 -20% 情境")
    st.caption("未輸入實際權重不可推定資產配置；風險情境僅作研究，不是實際未來跌幅。")
    holdings_file = st.file_uploader("持股 CSV／Excel：股票代號、產業、權重%", type=["csv", "xlsx"], key="h130_holdings")
    st.download_button("下載持股格式範例", data=make_template("持股").encode("utf-8-sig"), file_name="H130_Holdings_Template.csv", mime="text/csv")
    risk, correlation = portfolio_risk(load_uploaded(holdings_file), all_prices)
    table_result("曝險、集中度及市場下跌20%壓力測試", risk, "Risk")
    if not correlation.empty:
        table_result("持股間日報酬相關性（至少60共同交易日）", correlation, "Correlation")

with six:
    st.markdown("#### 實際交易日誌｜不把推薦紀錄當成交紀錄")
    st.caption("上傳最近20筆有完整進出場資料的實際成交；若手續費與交易稅缺漏，僅計毛報酬。")
    journal_file = st.file_uploader("交易日誌 CSV／Excel", type=["csv", "xlsx"], key="h130_journal")
    st.download_button("下載交易日誌格式範例", data=make_template("交易").encode("utf-8-sig"), file_name="H130_Journal_Template.csv", mime="text/csv")
    log_summary, log_rows = journal_review(load_uploaded(journal_file))
    table_result("交易重複問題與三條執行規則", log_summary, "Journal")
    if not log_rows.empty:
        table_result("最近20筆實際交易核對", log_rows, "Journal_Trades")

st.divider()
st.caption("H130 為研究用六引擎；所有結果僅存在此次 Streamlit 工作階段／上傳檔，沒有寫入 GitHub 或 Firestore，也不提供下單指令。正式進場仍需 H126 與現有第7頁正式交易閘門的獨立核准。")
