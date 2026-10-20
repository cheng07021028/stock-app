# -*- coding: utf-8 -*-
"""H133 六引擎研究中心：歷史黑馬樣本外統計＋H132公告＋H131資料橋接。"""
from __future__ import annotations

from datetime import date, timedelta, datetime
from zoneinfo import ZoneInfo
from pathlib import Path
import io
import json
import hashlib

import pandas as pd
import streamlit as st

try:
    from app_auth import require_login
    require_login()
except Exception as exc:
    st.error(f"登入系統載入失敗：{exc}")
    st.stop()

from godpick_h131_data_bridge import (
    VERSION as BRIDGE_VERSION, read_h126_workbook, verified_calendar,
    candidate_evidence_audit, batch_history, historical_forward_observations, MAX_BATCH,
)

from godpick_h132_official_events import (
    VERSION as EVENT_VERSION, SOURCE_URL as TWSE_MATERIAL_API,
    fetch_twse_disclosures, candidate_event_watch, MAX_CODES as MAX_NEWS_CODES,
)

from godpick_h130_six_engine import (
    VERSION, trade_ideas, technical_analysis, news_impacts, backtest_sma,
    portfolio_risk, journal_review, make_template,
)

from godpick_h133_blackhorse_validation import (
    VERSION as H133_VERSION, evaluate_blackhorse,
)

st.set_page_config(page_title="六引擎交易研究中心｜H133", layout="wide")
st.title("六引擎交易研究中心｜" + H133_VERSION)
st.caption("以證據為基礎的研究工作台｜不送出委託、不變更權威推薦／設定／持股／交易紀錄；不保證獲利。")
st.page_link("pages/25_主管智慧決策戰情.py", label="開啟 H134｜主管智慧決策戰情與遠端封存", icon="📊")
st.page_link("pages/26_訊號生命週期與策略審查.py", label="開啟 H135｜訊號生命週期與策略驗證", icon="📈")
taipei_today = datetime.now(ZoneInfo("Asia/Taipei")).date()
asof = st.date_input("研究資料截止日（如盤中尚無正式收盤，必須看H117核驗情況）", value=taipei_today, max_value=taipei_today, key="h130_asof")

@st.cache_data(ttl=300, show_spinner=False)
def h131_calendar(day: date) -> dict:
    return verified_calendar(Path("."), asof=day)

calendar = h131_calendar(asof)
st.caption(f"H117三市場交易日核驗：{calendar['status']}｜最後經驗證日：{calendar['official_market_day']}。此資訊不能單獨認證個股行情、法人或TDCC。")


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
            lambda v: "'" + v if isinstance(v, str) and v.lstrip().startswith(("=", "+", "-", "@")) else v
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
    stock_input = pd.DataFrame()
    source_note = "等待本期H126決策名單"
    if stocks_file is not None:
        stock_input, source_note = read_h126_workbook(stocks_file.getvalue(), stocks_file.name)
        if stock_input.empty:
            st.error(f"H131無法確認H126主管權威：{source_note}。不以第一張舊排名表冒充本期推薦。")
    if stocks_file is None:
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
    st.markdown("**H131｜按需批次串接既有行情服務（不自動掃描、不寫雲端）**")
    default_codes = []
    if "股票代號" in stock_input:
        default_codes = [str(v) for v in stock_input["股票代號"].dropna().astype(str).unique() if str(v).isdigit()][:MAX_BATCH]
    code_text = st.text_input("最多5檔台股代號（逗號分隔），預設帶入H126研究順位", value=",".join(default_codes), key="h131_codes")
    st.caption("僅按下按鈕才呼叫現有 get_history_data，每次最多5檔；資料可能來自Yahoo、官方或本機快取，不會冒稱官方認證。")
    if st.button("按需擷取最多5檔歷史日K並核對截止日", key="h131_batch"):
        codes = [v.strip() for v in code_text.replace("，", ",").split(",") if v.strip()]
        try:
            from utils import get_history_data
            market_map = {}
            if {"股票代號", "市場別"}.issubset(stock_input.columns):
                market_map = dict(zip(stock_input["股票代號"].astype(str), stock_input["市場別"].astype(str)))
            bars, audit = batch_history(codes, asof=asof, fetcher=get_history_data,
                                        market_by_code=market_map,
                                        expected_session=calendar["official_market_day"])
            st.session_state["h131_batch_bars"] = bars
            st.session_state["h131_batch_audit"] = audit
            st.session_state["h131_batch_day"] = asof
        except Exception as exc:
            st.error(f"H131批次讀取未完成：{type(exc).__name__}；請檢查代號及網路服務，未更動正式推薦。")
    batch_audit = st.session_state.get("h131_batch_audit", pd.DataFrame()) if st.session_state.get("h131_batch_day") == asof else pd.DataFrame()
    batch_bars = st.session_state.get("h131_batch_bars", pd.DataFrame()) if st.session_state.get("h131_batch_day") == asof else pd.DataFrame()
    if isinstance(batch_audit, pd.DataFrame) and not batch_audit.empty:
        st.dataframe(batch_audit, use_container_width=True, hide_index=True)
    if isinstance(batch_bars, pd.DataFrame) and not batch_bars.empty:
        if all_prices.empty:
            all_prices = batch_bars
            st.caption("六引擎K線來源：H131本工作階段按需批次取得。")
        else:
            st.caption("本輪以使用者上傳K線為主要研究來源；H131擷取結果不混入使用者自行調整過的行情。")

one, two, three, four, five, six, seven = st.tabs([
    "01 市場機會", "02 技術分析", "03 新聞事件", "04 樣本外回測", "05 組合風控", "06 交易日誌", "07 H133黑馬驗證",
])

with one:
    st.markdown("#### 市場機會｜正式權威先行")
    st.caption("優先 H126 認證狀態與 H109 風控；沒有當日官方證據就列待驗證，不以 H102 舊分數批准買進。")
    ideas = trade_ideas(stock_input, asof=asof, limit=5)
    table_result("優先研究標的（最多五檔，不等於買進）", ideas, "Ideas")
    evidence = candidate_evidence_audit(stock_input, asof=asof)
    table_result("H131法人／TDCC證據追溯待驗證（不改變H126決策）", evidence.head(120), "Evidence")
    st.caption("表內H123/H124為原列聲稱的狀態，並非H131重新向官方核實；未取得原始逐日／逐週證據前不予認證。")

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
    st.markdown("#### H132｜上市公司重大訊息 × 事件時效稽核")
    st.caption("按需查詢TWSE正式OpenAPI『上市公司每日重大訊息』。僅可證明收到當期官方端點的回應，沒有外部簽章；不含上櫃、一般新聞及真正歷史快照。")
    st.markdown(f"公告資料固定來源：[證交所上市公司每日重大訊息 OpenAPI]({TWSE_MATERIAL_API})")
    relevant_codes = []
    if "股票代號" in stock_input:
        relevant_codes = [str(x) for x in stock_input["股票代號"].dropna().astype(str).unique() if str(x).isdigit()][:MAX_NEWS_CODES]
    news_codes_text = st.text_input("查核上市公司代號（最多30檔、逗號分隔）", value=",".join(relevant_codes[:10]), key="h132_news_codes")
    st.warning("只在按下查核按鈕時連線。歷史研究日不會把今日的公告快照偽裝成當時已知資訊；盤中公告也不得假定已被股價充分反映。")
    if st.button("H132｜查詢證交所當期重大訊息（上市）", key="h132_fetch_official"):
        codes = [x.strip() for x in news_codes_text.replace("，", ",").split(",") if x.strip()]
        news, receipt = fetch_twse_disclosures(asof=asof, codes=codes)
        st.session_state["h132_official_events"] = news
        st.session_state["h132_official_receipt"] = receipt
        st.session_state["h132_official_day"] = asof
        st.session_state["h132_official_codes"] = tuple(codes)
    selected_codes = tuple(x.strip() for x in news_codes_text.replace("，", ",").split(",") if x.strip())
    is_same_query = (st.session_state.get("h132_official_day") == asof and
                     st.session_state.get("h132_official_codes") == selected_codes)
    if is_same_query:
        official_news = st.session_state.get("h132_official_events", pd.DataFrame())
        official_receipt = st.session_state.get("h132_official_receipt", pd.DataFrame())
        if isinstance(official_receipt, pd.DataFrame) and not official_receipt.empty:
            st.dataframe(official_receipt, use_container_width=True, hide_index=True)
        if isinstance(official_news, pd.DataFrame) and not official_news.empty:
            table_result("上市公司重大訊息原始證據（公告日非事實發生日）", official_news, "H132_TWSE_Material")
            watch = candidate_event_watch(stock_input, official_news)
            table_result("H126研究名單 × 公告事件（只供追蹤，H109/H126決策不變）", watch, "H132_Catalyst_Watch")
        else:
            st.info("尚無匹配且通過時間驗證的公告；或官方服務尚未回應。請看上方資料狀態，不代表該股票沒有其他公開資訊。")
    else:
        st.caption("本研究日和代號組合尚未查詢；不使用其他研究日／其他股票的舊回應。")
    st.markdown("#### 其他新聞／上櫃事件｜人工提供可追溯來源")
    event_file = st.file_uploader("上傳新聞／公告事件表（必須有日期、標題、來源網址）", type=["csv", "xlsx"], key="h130_event_file")
    st.download_button("下載事件格式範例", data=make_template("新聞").encode("utf-8-sig"), file_name="H130_News_Template.csv", mime="text/csv")
    event_result = news_impacts(load_uploaded(event_file), asof=asof)
    table_result("人工輸入事件的時效與情境（URL字串不能證明原文真偽）", event_result, "News")

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
    st.markdown("#### H131｜歷史研究訊號的T+1／T+3／T+5收盤觀測")
    st.caption("僅比較歷史研究日及後續實際K線收盤，非開盤成交、不含手續費、非下單績效。訊號日缺K線或沒有未來交易日則留空；不使用未來資料反推當天的推薦。")
    forward = historical_forward_observations(stock_input, all_prices, asof=asof)
    table_result("研究閉環觀測（與正式交易日誌完全分離）", forward, "Forward_Observation")

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

with seven:
    st.markdown("#### H133｜黑馬因子樣本外研究驗證（研究用途）")
    st.warning("歷史訊號 Excel 即使有日期，也不能證明當時真的產生該分數；本模組無法獨立核驗不可變PIT封存。研究收盤報酬不是可成交績效，不更新正式推薦或策略權重。")
    st.caption("至少提供**多個不同交易日**的 H126 A/B 研究訊號（非舊H102排行榜）、對應個股調整後日K、同期大盤指數日收盤。所有日期均依研究截止日限制。")
    left, right = st.columns(2)
    with left:
        signals_upload = st.file_uploader("歷史研究訊號CSV／Excel：股票代號、訊號研究日、H126權威層別、H126最後研究分、H109決策權威", type=["csv", "xlsx"], key="h133_signals")
        price_upload = st.file_uploader("個股歷史日K CSV／Excel：股票代號、日期、收盤價", type=["csv", "xlsx"], key="h133_stock_bars")
        use_existing = st.checkbox("若未單獨上傳個股歷史K線，沿用本頁上方已讀取的日K", value=False, key="h133_use_common_bars")
    with right:
        benchmark_upload = st.file_uploader("大盤指數歷史日收盤 CSV／Excel：日期、收盤價", type=["csv", "xlsx"], key="h133_index_bars")
        adjusted = st.checkbox("我已核對個股行情的除權息／拆併股調整口徑與歷史來源（需另行獨立審核）", value=False, key="h133_adjust_confirm")
        k_choice = st.slider("每日研究標的 Top N（僅採當時H126分數）", min_value=1, max_value=10, value=5, key="h133_topk")
        min_days = st.slider("最少有效樣本外研究交易日", min_value=20, max_value=60, value=20, step=5, key="h133_min_oos")
    st.download_button("下載 H133 歷史研究訊號欄位範本", data="股票代號,訊號研究日,H126權威層別,H126最後研究分,H109決策權威,H123法人實證狀態,H124大戶實證狀態,訊號產生時間\n".encode("utf-8-sig"), file_name="H133_Signals_Template.csv", mime="text/csv", key="h133_template")
    st.caption("時間切分：前70%／後30%；T+5 市場交易日禁運；以每個研究日為樣本單位；缺少任何Top N個股報價的日子不列為有效績效。Bootstrap 為研究誤差範圍，並非未來報酬保證。")
    def h133_fingerprint():
        digest = hashlib.sha256(str((asof, adjusted, k_choice, min_days, use_existing)).encode())
        for upload in (signals_upload, price_upload, benchmark_upload):
            digest.update(upload.getvalue() if upload else b"NO_UPLOAD")
        if use_existing and price_upload is None and not all_prices.empty:
            digest.update(pd.util.hash_pandas_object(all_prices, index=True).values.tobytes())
        return digest.hexdigest()
    fingerprint = h133_fingerprint()
    if st.button("H133｜執行黑馬歷史樣本外研究（不寫入正式系統）", key="h133_run"):
        if signals_upload is None or benchmark_upload is None or (price_upload is None and not use_existing):
            st.error("缺少歷史訊號、大盤日K或個股日K；無法計算有效樣本外比較。")
            st.session_state.pop("h133_audit_report", None)
        else:
            with st.spinner("正在逐日查核研究資格、行情對齊與績效完整性…"):
                sig = load_uploaded(signals_upload)
                historical_stock = load_uploaded(price_upload) if price_upload is not None else all_prices
                index_bars = load_uploaded(benchmark_upload)
                result = evaluate_blackhorse(sig, historical_stock, index_bars, asof=asof,
                                             adjusted_confirmed=adjusted, top_k=k_choice,
                                             min_oos_days=min_days)
                st.session_state["h133_audit_report"] = (fingerprint, result)
    stored = st.session_state.get("h133_audit_report")
    if stored and stored[0] == fingerprint:
        result = stored[1]
        st.markdown(f"**分析狀態：{result['狀態']}**")
        st.caption(result["說明"])
        for title, key, label in (
            ("歷史訊號稽核（拒絕原因）", "訊號稽核", "H133_Audit"),
            ("時間切割／PIT可信度", "分割摘要", "H133_Split"),
            ("T+1／T+3／T+5 样本外研究比較", "樣本外績效", "H133_Holdout"),
            ("法人與TDCC因子分組（僅描述相關性）", "因子分組", "H133_Factors"),
            ("逐筆研究收盤觀測（不是成交績效）", "逐筆觀測", "H133_Observations"),
        ):
            table_result(title, result.get(key, pd.DataFrame()), label)
    elif stored:
        st.info("輸入或設定已變更。舊的研究結果不再顯示，請重新執行H133。")

st.divider()
st.caption("H133 為多日期研究訊號與個股／大盤K線的回溯描述及樣本外比較；原始訊號的PIT封存與企業行動尚需獨立核對。H132上市公告、H131按需行情保持不變。所有新功能不自動交易、不調正式H126權限／權重、不寫GitHub／Firestore。")
