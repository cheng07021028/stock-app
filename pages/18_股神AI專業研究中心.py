# -*- coding: utf-8 -*-
from __future__ import annotations

try:
    from app_auth import require_login
    require_login()
except Exception as _auth_e:
    import streamlit as st
    st.error(f"登入系統載入失敗：{_auth_e}")
    st.stop()

from datetime import date, timedelta
import re
from typing import Any

import pandas as pd
import streamlit as st

st.set_page_config(page_title="18_股神AI專業研究中心｜H95 Fast", layout="wide")

try:
    from utils import inject_pro_theme, get_history_data
    inject_pro_theme()
except Exception:
    get_history_data = None

from godpick_h81_professional_settings import DEFAULT_SETTINGS, normalize_settings, save_settings
from godpick_h81_professional_ai import (
    VERSION as H81_VERSION,
    analyze_candidate,
    analyze_price_history,
    fetch_latest_news,
    news_impact_analysis,
    backtest_trade_records,
    backtest_price_strategy,
    portfolio_stress_test,
    portfolio_correlation_analysis,
    news_price_scenario,
    analyze_trade_journal,
    build_daily_trading_plan,
)
from godpick_h95_page18_fast_service import (
    VERSION as H95_VERSION,
    load_h81_settings_local,
    load_latest_candidates_local,
    load_records_local,
    candidate_options,
    fast_existing_h81_preview,
    bounded_research_preview,
)

st.title("18_股神AI專業研究中心｜H95")
st.caption(
    "H95改為 Local-First + Lazy Load：進頁不讀GitHub/Firestore、不載入第8頁完整紀錄、不重跑全市場H81。"
    "只有你主動按下分析/同步按鈕，才執行對應重工作業。"
)


def re_split(line: str) -> list[str]:
    return re.split(r"[｜|]", line)


@st.cache_data(ttl=300, show_spinner=False)
def _h95_local_settings_cached():
    return load_h81_settings_local(DEFAULT_SETTINGS, normalize_settings)


@st.cache_data(ttl=120, show_spinner=False)
def _h95_local_candidates_cached():
    return load_latest_candidates_local(max_rows=400)


@st.cache_data(ttl=120, show_spinner=False)
def _h95_local_records_cached():
    return load_records_local(max_rows=5000)


settings, setting_details = _h95_local_settings_cached()
latest_df, latest_details, latest_source = _h95_local_candidates_cached()

# A user-triggered remote sync is held in this session only.  It never blocks first paint.
if isinstance(st.session_state.get("h95_remote_latest_df"), pd.DataFrame) and not st.session_state["h95_remote_latest_df"].empty:
    latest_df = st.session_state["h95_remote_latest_df"].copy()
    latest_source = "本次Session永久權威同步"

st.success(f"H95快速入口已啟用｜候選 {len(latest_df)} 檔｜來源：{latest_source or '本機暫無快照'}")
st.caption("進頁主執行緒：不做遠端權威選舉、不做全市場研究重算、不讀完整績效資料庫。")

with st.sidebar:
    st.markdown("### H95 快速研究導覽")
    section = st.radio(
        "功能",
        ["市場正在定價什麼", "日/週線技術", "新聞→市場影響", "策略回測", "投組壓力測試", "交易日誌分析", "全日交易計畫", "永久設定"],
        key="h95_section",
    )
    if st.button("重新讀取本機快照", use_container_width=True):
        _h95_local_candidates_cached.clear()
        _h95_local_settings_cached.clear()
        st.session_state.pop("h95_remote_latest_df", None)
        st.rerun()
    if st.button("同步最新推薦永久權威（按需）", use_container_width=True):
        with st.spinner("只在本次按鈕操作讀取永久權威..."):
            try:
                from godpick_persistence_service import load_named_json_permanent
                payload, details = load_named_json_permanent(
                    "godpick_latest_recommendations.json", {}, firestore_doc="godpick_latest_recommendations"
                )
                from godpick_h95_page18_fast_service import _extract_candidate_rows
                rows = _extract_candidate_rows(payload)
                remote_df = pd.DataFrame(rows).head(400) if rows else pd.DataFrame()
                if not remote_df.empty:
                    st.session_state["h95_remote_latest_df"] = remote_df
                    st.success(f"永久權威同步完成：{len(remote_df)}檔")
                    st.rerun()
                else:
                    st.warning("永久權威讀取完成，但沒有可解析候選列。")
                if details:
                    st.caption("｜".join(map(str, details[-3:])))
            except Exception as exc:
                st.error(f"永久權威同步失敗：{type(exc).__name__}: {exc}")
    with st.expander("快速入口診斷", expanded=False):
        st.code("\n".join((setting_details or []) + (latest_details or [])))

opts = candidate_options(latest_df)
selected_code = None
selected_row = None
if opts:
    labels = [label for _, label in opts]
    selected_label = st.sidebar.selectbox("分析標的", labels, key="h95_target")
    selected_code = next(code for code, label in opts if label == selected_label)
    if "股票代號" in latest_df.columns:
        codes = latest_df["股票代號"].astype(str).str.replace(r"\.0$", "", regex=True)
        matched = latest_df.loc[codes.eq(selected_code)]
        if not matched.empty:
            selected_row = matched.iloc[0]
else:
    st.sidebar.warning("本機目前沒有候選快照；頁面仍可正常使用設定功能。")


def _load_records_for_active_section() -> tuple[pd.DataFrame, list[str]]:
    if isinstance(st.session_state.get("h95_remote_records_df"), pd.DataFrame) and not st.session_state["h95_remote_records_df"].empty:
        return st.session_state["h95_remote_records_df"].copy(), ["H95：本次Session使用永久權威紀錄。"]
    return _h95_local_records_cached()


def _remote_records_button() -> None:
    if st.button("同步第8頁永久紀錄（按需）", use_container_width=True):
        with st.spinner("讀取第8頁永久權威；只在本次按鈕操作執行..."):
            try:
                from godpick_persistence_service import load_records_permanent
                rows, details = load_records_permanent()
                frame = pd.DataFrame(rows or [])
                if not frame.empty:
                    st.session_state["h95_remote_records_df"] = frame
                    st.success(f"永久紀錄同步完成：{len(frame)}筆")
                    st.rerun()
                st.warning("永久權威目前沒有可用推薦紀錄。")
                if details:
                    st.caption("｜".join(map(str, details[-3:])))
            except Exception as exc:
                st.error(f"永久紀錄同步失敗：{type(exc).__name__}: {exc}")


if section == "市場正在定價什麼":
    st.subheader("市場正在定價什麼｜基本面、估值、催化、風險")
    preview = fast_existing_h81_preview(latest_df, top_n=5)
    if not preview.empty:
        st.markdown("#### 已保存H81研究前5｜Zero Recompute")
        st.dataframe(preview, use_container_width=True, hide_index=True)
        st.caption("這裡直接讀第7頁已保存H81證據，不重新分析整個市場。")
    elif not latest_df.empty:
        st.info("這份本機快照尚未含H81研究欄。若需要排行榜，請按下方按鈕；最多只分析80檔，不重跑全市場。")
        if st.button("建立H81研究Top5（最多80檔）", type="primary", use_container_width=True):
            with st.spinner("H95 bounded research：最多80檔..."):
                ranked = bounded_research_preview(latest_df, settings, max_eval=80, top_n=5)
            st.session_state["h95_bounded_preview"] = ranked
        ranked = st.session_state.get("h95_bounded_preview")
        if isinstance(ranked, pd.DataFrame) and not ranked.empty:
            cols = [c for c in ["股票代號", "股票名稱", "H81專業研究總分", "H81資料覆蓋%", "H81排名加減分", "H81三大利多催化", "H81三大風險"] if c in ranked.columns]
            st.dataframe(ranked[cols], use_container_width=True, hide_index=True)
    if selected_row is None:
        st.warning("目前沒有可分析標的。可先同步永久權威，或至第7頁完成推薦。")
    else:
        result = analyze_candidate(selected_row, settings)
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("H81專業研究分", result["H81專業研究總分"])
        k2.metric("資料覆蓋", f"{result['H81資料覆蓋%']:.0f}%")
        k3.metric("研究排序調整", f"{result['H81排名加減分']:+.2f}")
        k4.metric("市場定價理解", result["H81市場定價理解分"])
        st.write("**3個主要利多/催化：**", result["H81三大利多催化"])
        st.write("**3個主要風險：**", result["H81三大風險"])
        st.write("**接下來關注：**", result["H81下一步關注"])
        st.write("**多頭情境：**", result["H81多頭情境"])
        st.write("**中性情境：**", result["H81中性情境"])
        st.write("**空頭情境：**", result["H81空頭情境"])
        st.caption("H81只提供條件式研究，不把任何情境視為必然結果。")

elif section == "日/週線技術":
    st.subheader("日線＋週期技術分析")
    if selected_row is None or not callable(get_history_data):
        st.warning("目前無法取得標的或歷史K線服務。")
    else:
        market = str(selected_row.get("市場別", "上市") or "上市")
        name = str(selected_row.get("股票名稱", "") or "")
        days = st.slider("歷史日曆天數", 90, 730, 240, 30)
        if st.button("分析日/週期K線", type="primary"):
            with st.spinner("讀取歷史K線並計算支撐/壓力、均線、RSI、MACD與量能..."):
                hist = get_history_data(selected_code, name, market, date.today() - timedelta(days=days), date.today())
                analysis = analyze_price_history(hist)
            if not analysis.get("available"):
                st.error(analysis.get("message"))
            else:
                st.json(analysis)
                if isinstance(hist, pd.DataFrame) and not hist.empty:
                    close_col = next((c for c in ["收盤價", "Close", "close"] if c in hist.columns), None)
                    date_col = next((c for c in ["日期", "Date", "date"] if c in hist.columns), None)
                    if close_col:
                        chart_df = hist[[date_col, close_col]].copy() if date_col else hist[[close_col]].copy()
                        if date_col:
                            chart_df[date_col] = pd.to_datetime(chart_df[date_col], errors="coerce")
                            chart_df = chart_df.dropna(subset=[date_col]).set_index(date_col)
                        st.line_chart(pd.to_numeric(chart_df[close_col], errors="coerce"))

elif section == "新聞→市場影響":
    st.subheader("把新聞轉換為市場影響")
    st.caption("外部新聞只有按下按鈕才連線；進頁不抓新聞。")
    if selected_row is not None:
        q_name = str(selected_row.get("股票名稱", "") or "")
        q_sector = str(selected_row.get("類別", "") or "")
        news_query = st.text_input("最新新聞查詢", value=f"{q_name} {q_sector}".strip())
        if st.button("抓取最新公司/產業新聞"):
            fetched, fetch_msg = fetch_latest_news(news_query, max_items=12, timeout=5.0)
            if fetched.empty:
                st.warning(fetch_msg)
            else:
                st.success(fetch_msg)
                impact = news_impact_analysis(fetched)
                st.dataframe(impact, use_container_width=True, hide_index=True)
                scenario = news_price_scenario(selected_row, impact, horizon_days=int(settings["news"]["short_term_days"]))
                st.json(scenario)
    raw_news = st.text_area("貼入新聞資料", height=220, placeholder="2026-09-21｜公司公告｜月營收創高")
    if st.button("解析新聞影響"):
        rows = []
        for line in raw_news.splitlines():
            line = line.strip()
            if not line:
                continue
            parts = [x.strip() for x in re_split(line)]
            rows.append({"日期": parts[0], "來源": parts[1], "標題": "｜".join(parts[2:])} if len(parts) >= 3 else {"標題": line})
        impact = news_impact_analysis(rows)
        st.dataframe(impact, use_container_width=True, hide_index=True)
        if selected_row is not None:
            st.json(news_price_scenario(selected_row, impact, horizon_days=int(settings["news"]["short_term_days"])))

elif section == "策略回測":
    st.subheader("策略回測｜勝率、獲利因子、最大回撤、外樣本")
    records_df, record_details = _load_records_for_active_section()
    st.caption("｜".join(record_details))
    _remote_records_button()
    bt_mode = st.radio("回測來源", ["第8頁實際/推薦績效", "標的K線策略"], horizontal=True)
    if bt_mode == "第8頁實際/推薦績效":
        if records_df.empty:
            st.warning("第8頁本機紀錄尚無可用資料。")
        else:
            return_candidates = [c for c in ["實際報酬%", "可執行交易5日%", "推薦後5日%", "推薦後3日%", "推薦後1日%", "損益幅%"] if c in records_df.columns]
            selected_return = st.selectbox("回測報酬欄位", return_candidates) if return_candidates else None
            if st.button("執行績效回測檢討"):
                st.json(backtest_trade_records(records_df, return_col=selected_return, train_ratio=float(settings["backtest"]["train_ratio"])))
    else:
        if selected_row is None or not callable(get_history_data):
            st.warning("請先選擇標的，且歷史K線服務需可用。")
        else:
            s1, s2, s3, s4 = st.columns(4)
            with s1: strategy = st.selectbox("策略", ["MA_CROSS", "RSI_REVERSAL"], format_func=lambda x: "均線交叉" if x == "MA_CROSS" else "RSI低檔轉強")
            with s2: short_ma = st.number_input("短均線", 2, 60, 5, 1)
            with s3: long_ma = st.number_input("長均線", 5, 240, 20, 1)
            with s4: cost_bps = st.number_input("每次部位切換成本(bp)", 0.0, 200.0, 30.0, 5.0)
            q1, q2 = st.columns(2)
            with q1: rsi_entry = st.number_input("RSI進場門檻", 10.0, 50.0, 35.0, 1.0)
            with q2: rsi_exit = st.number_input("RSI退出門檻", 40.0, 90.0, 55.0, 1.0)
            if st.button("執行K線策略回測"):
                market = str(selected_row.get("市場別", "上市") or "上市")
                name = str(selected_row.get("股票名稱", "") or "")
                with st.spinner("讀取K線並執行無look-ahead外樣本回測..."):
                    hist = get_history_data(selected_code, name, market, date.today() - timedelta(days=730), date.today())
                    result = backtest_price_strategy(hist, strategy=strategy, short_ma=int(short_ma), long_ma=int(long_ma), rsi_entry=float(rsi_entry), rsi_exit=float(rsi_exit), cost_bps=float(cost_bps), train_ratio=float(settings["backtest"]["train_ratio"]))
                st.json(result)

elif section == "投組壓力測試":
    st.subheader("投資組合壓力測試")
    template = pd.DataFrame([
        {"股票代號": "", "配置比例%": 25.0, "類別": "", "地區": "台灣", "市場別": "上市", "Beta": 1.0, "利率敏感度": None},
        {"股票代號": "", "配置比例%": 25.0, "類別": "", "地區": "台灣", "市場別": "上市", "Beta": 1.0, "利率敏感度": None},
        {"股票代號": "", "配置比例%": 25.0, "類別": "", "地區": "台灣", "市場別": "上市", "Beta": 1.0, "利率敏感度": None},
        {"股票代號": "", "配置比例%": 25.0, "類別": "", "地區": "台灣", "市場別": "上市", "Beta": 1.0, "利率敏感度": None},
    ])
    portfolio_df = st.data_editor(template, num_rows="dynamic", use_container_width=True, key="h95_portfolio")
    if st.button("執行壓力測試"):
        st.json(portfolio_stress_test(portfolio_df, settings))
    if st.button("分析隱藏相關性（讀取近一年K線）"):
        if not callable(get_history_data):
            st.error("歷史K線服務不可用。")
        else:
            histories = {}
            valid = portfolio_df.copy()
            valid["股票代號"] = valid["股票代號"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
            valid = valid[valid["股票代號"].ne("")].head(12)
            with st.spinner("讀取投組K線並計算日報酬相關性..."):
                for _, prow in valid.iterrows():
                    code = str(prow.get("股票代號") or "").strip()
                    if code:
                        histories[code] = get_history_data(code, "", str(prow.get("市場別") or "上市"), date.today() - timedelta(days=420), date.today())
            st.json(portfolio_correlation_analysis(histories, threshold=float(settings["portfolio_risk"]["high_correlation_threshold"])))

elif section == "交易日誌分析":
    st.subheader("最近交易日誌分析")
    records_df, record_details = _load_records_for_active_section()
    st.caption("｜".join(record_details))
    _remote_records_button()
    if records_df.empty:
        st.warning("第8頁沒有可分析的本機紀錄。")
    else:
        recent_n = st.slider("檢視最近交易/成熟推薦", 5, 100, int(settings["trade_journal"]["recent_trades"]), 5)
        if st.button("分析最近交易"):
            result = analyze_trade_journal(records_df, recent_n=recent_n)
            st.json(result)
            if result.get("available"):
                for i, rule in enumerate(result.get("personalized_rules", []), 1):
                    st.write(f"{i}. {rule}")

elif section == "全日交易計畫":
    st.subheader("盤前→開盤→盤中→收盤交易檢查表")
    if selected_row is None:
        st.warning("沒有可建立計畫的標的。")
    else:
        st.dataframe(build_daily_trading_plan(selected_row, settings), use_container_width=True, hide_index=True)
        st.caption("這是條件式執行清單，不會送單，也不把研究推薦當成買進許可。")

elif section == "永久設定":
    st.subheader("H81永久設定｜H95進頁使用本機值，儲存時才同步永久層")
    cfg = normalize_settings(settings)
    enabled = st.toggle("啟用H81專業研究層", value=bool(cfg.get("enabled", True)))
    labels = {
        "market_pricing": "市場定價/基本面估值", "technical": "日週線技術", "news": "新聞事件",
        "backtest": "策略回測", "portfolio_risk": "風險管理", "journal": "交易日誌", "execution_plan": "交易計畫",
    }
    edited_features = {}
    edited_weights = {}
    cols = st.columns(4)
    for i, (key, label) in enumerate(labels.items()):
        with cols[i % 4]:
            edited_features[key] = st.toggle(label, value=bool(cfg.get("features", {}).get(key, True)), key=f"h95_feature_{key}")
            edited_weights[key] = st.number_input(f"{label}權重", 0, 100, int(cfg["ranking_overlay"]["weights"].get(key, 0)), 1, key=f"h95_w_{key}")
    c1, c2 = st.columns(2)
    with c1: max_points = st.number_input("研究排序最大加減分", 0.0, 5.0, float(cfg["ranking_overlay"]["max_abs_points"]), 0.1)
    with c2: min_cov = st.slider("最低資料覆蓋率", 0.20, 0.90, float(cfg["ranking_overlay"]["min_data_coverage"]), 0.05)
    draft = normalize_settings({**cfg, "enabled": enabled, "features": edited_features, "ranking_overlay": {**cfg["ranking_overlay"], "max_abs_points": max_points, "min_data_coverage": min_cov, "weights": edited_weights}})
    if st.button("永久保存H81設定", type="primary", use_container_width=True):
        with st.spinner("設定永久化與回讀驗證..."):
            ok, msg, saved = save_settings(draft)
        (st.success if ok else st.error)(msg)
        if ok:
            _h95_local_settings_cached.clear()
            st.json(saved)

st.caption(f"H95 Fast Entry {H95_VERSION}｜H81 {H81_VERSION}｜Heavy work only on explicit action")
