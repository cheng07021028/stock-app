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
import io
import json
import re
from typing import Any

import pandas as pd
import streamlit as st

st.set_page_config(page_title="18_股神AI專業研究中心｜H81", layout="wide")

try:
    from utils import inject_pro_theme, get_history_data
    inject_pro_theme()
except Exception:
    get_history_data = None

from godpick_h81_professional_settings import (
    VERSION as SETTINGS_VERSION,
    DEFAULT_SETTINGS,
    load_settings,
    normalize_settings,
    save_settings,
)
from godpick_h81_professional_ai import (
    VERSION as H81_VERSION,
    analyze_candidate,
    apply_professional_research_overlay,
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

try:
    from godpick_persistence_service import load_named_json_permanent, load_records_permanent
except Exception:
    load_named_json_permanent = None
    load_records_permanent = None


st.title("18_股神AI專業研究中心｜H81")
st.caption(
    "把『市場正在定價什麼、日/週線、新聞影響、策略回測、投組壓力、交易日誌、每日交易計畫』"
    "整合成可驗證研究層。H81只影響研究排序；不會自行取得Formal買進授權。"
)


def re_split(line: str) -> list[str]:
    return re.split(r"[｜|]", line)


def _rows_from_payload(payload: Any) -> pd.DataFrame:
    if isinstance(payload, pd.DataFrame):
        return payload.copy()
    if isinstance(payload, list):
        return pd.DataFrame([x for x in payload if isinstance(x, dict)])
    if isinstance(payload, dict):
        for key in ["records", "rows", "items", "data", "recommendations", "full_rows"]:
            value = payload.get(key)
            if isinstance(value, list):
                return pd.DataFrame([x for x in value if isinstance(x, dict)])
        # Some snapshots use code -> dict mapping.
        values = [x for x in payload.values() if isinstance(x, dict)]
        if values:
            return pd.DataFrame(values)
    return pd.DataFrame()


@st.cache_data(ttl=60, show_spinner=False)
def _load_latest_candidates() -> tuple[pd.DataFrame, list[str]]:
    if not callable(load_named_json_permanent):
        return pd.DataFrame(), ["永久快照服務不可用"]
    try:
        payload, details = load_named_json_permanent(
            "godpick_latest_recommendations.json", {}, firestore_doc="godpick_latest_recommendations"
        )
        return _rows_from_payload(payload), list(details or [])
    except Exception as exc:
        return pd.DataFrame(), [f"最新推薦快照讀取失敗：{exc}"]


@st.cache_data(ttl=60, show_spinner=False)
def _load_records() -> tuple[pd.DataFrame, list[str]]:
    if not callable(load_records_permanent):
        return pd.DataFrame(), ["推薦紀錄永久權威不可用"]
    try:
        rows, details = load_records_permanent()
        return pd.DataFrame(rows or []), list(details or [])
    except Exception as exc:
        return pd.DataFrame(), [f"推薦紀錄讀取失敗：{exc}"]


settings, setting_details = load_settings()
latest_df, latest_details = _load_latest_candidates()
records_df, record_details = _load_records()

_tab_setting, _tab_candidate, _tab_chart, _tab_news, _tab_backtest, _tab_portfolio, _tab_journal, _tab_plan = st.tabs([
    "永久設定", "市場正在定價什麼", "日/週線技術", "新聞→市場影響",
    "策略回測", "投組壓力測試", "交易日誌分析", "全日交易計畫",
])

with _tab_setting:
    st.subheader("H81永久設定")
    st.info("儲存時使用既有 GodPick 永久權威（本機原子寫入＋GitHub/Firestore）；寫入後會回讀驗證版本序號。")
    cfg = normalize_settings(settings)
    enabled = st.toggle("啟用H81專業研究層", value=bool(cfg.get("enabled", True)))
    st.markdown("#### 七大能力開關")
    _feature_labels = {
        "market_pricing": "市場正在定價什麼", "technical": "日/週線技術", "news": "新聞→市場影響",
        "backtest": "策略回測", "portfolio_risk": "投組風險", "journal": "交易日誌", "execution_plan": "全日交易計畫",
    }
    _fcols = st.columns(4)
    edited_features = {}
    for _i, (_key, _label) in enumerate(_feature_labels.items()):
        with _fcols[_i % 4]:
            edited_features[_key] = st.toggle(_label, value=bool(cfg.get("features", {}).get(_key, True)), key=f"h81_feature_{_key}")
    overlay_enabled = st.toggle("允許H81影響研究排序", value=bool(cfg["ranking_overlay"].get("enabled", True)))
    c1, c2 = st.columns(2)
    with c1:
        max_points = st.number_input("研究排序最大加減分", 0.0, 5.0, float(cfg["ranking_overlay"]["max_abs_points"]), 0.1)
    with c2:
        min_cov = st.slider("最低資料覆蓋率", 0.20, 0.90, float(cfg["ranking_overlay"]["min_data_coverage"]), 0.05)
    st.markdown("#### 七大能力權重（保存時自動正規化為100%）")
    labels = {
        "market_pricing": "市場定價/基本面估值", "technical": "日週線技術", "news": "新聞事件",
        "backtest": "策略回測", "portfolio_risk": "風險管理", "journal": "交易日誌", "execution_plan": "交易計畫",
    }
    edited_weights = {}
    cols = st.columns(4)
    for i, (key, label) in enumerate(labels.items()):
        with cols[i % 4]:
            edited_weights[key] = st.number_input(label, 0, 100, int(cfg["ranking_overlay"]["weights"].get(key, 0)), 1, key=f"w_{key}")

    st.markdown("#### 回測防過擬合")
    b1, b2, b3 = st.columns(3)
    with b1:
        min_trades = st.number_input("最低成熟交易筆數", 5, 500, int(cfg["backtest"]["minimum_trades"]), 1)
    with b2:
        min_oos = st.number_input("最低外樣本筆數", 3, 200, int(cfg["backtest"]["minimum_out_of_sample_trades"]), 1)
    with b3:
        train_ratio = st.slider("訓練區間比例", 0.50, 0.85, float(cfg["backtest"]["train_ratio"]), 0.05)

    st.markdown("#### 投資組合壓力設定")
    r1, r2, r3, r4 = st.columns(4)
    with r1:
        max_single = st.number_input("單一股票上限%", 5.0, 100.0, float(cfg["portfolio_risk"]["max_single_stock_weight_pct"]), 1.0)
    with r2:
        max_sector = st.number_input("單一產業上限%", 10.0, 100.0, float(cfg["portfolio_risk"]["max_sector_weight_pct"]), 1.0)
    with r3:
        market_shock = st.number_input("市場修正情境%", -50.0, 0.0, float(cfg["portfolio_risk"]["stress_market_correction_pct"]), 1.0)
    with r4:
        bear_shock = st.number_input("熊市情境%", -60.0, 0.0, float(cfg["portfolio_risk"]["stress_bear_market_pct"]), 1.0)
    r5, r6, r7 = st.columns(3)
    with r5:
        recession_shock = st.number_input("衰退情境%", -70.0, 0.0, float(cfg["portfolio_risk"]["stress_recession_pct"]), 1.0)
    with r6:
        rate_shock = st.number_input("利率上升情境(bp)", 25, 500, int(cfg["portfolio_risk"]["stress_rate_shock_bp"]), 25)
    with r7:
        corr_threshold = st.slider("高相關門檻", 0.30, 0.99, float(cfg["portfolio_risk"]["high_correlation_threshold"]), 0.01)

    st.markdown("#### 交易日誌 / 新聞 / 全日計畫")
    x1, x2, x3, x4 = st.columns(4)
    with x1:
        journal_n = st.number_input("預設檢視最近交易數", 5, 200, int(cfg["trade_journal"]["recent_trades"]), 5)
    with x2:
        news_short = st.number_input("新聞短期天數", 1, 20, int(cfg["news"]["short_term_days"]), 1)
    with x3:
        news_mid = st.number_input("新聞中期天數", 10, 120, int(cfg["news"]["medium_term_days"]), 5)
    with x4:
        news_long = st.number_input("新聞長期天數", 60, 730, int(cfg["news"]["long_term_days"]), 30)
    p1, p2, p3, p4 = st.columns(4)
    with p1:
        preopen_on = st.toggle("盤前計畫", value=bool(cfg["daily_plan"]["preopen_enabled"]))
    with p2:
        open_on = st.toggle("開盤計畫", value=bool(cfg["daily_plan"]["open_enabled"]))
    with p3:
        intraday_on = st.toggle("盤中調整", value=bool(cfg["daily_plan"]["intraday_enabled"]))
    with p4:
        close_on = st.toggle("收盤檢討", value=bool(cfg["daily_plan"]["close_enabled"]))

    draft = normalize_settings({
        **cfg,
        "enabled": enabled,
        "features": edited_features,
        "ranking_overlay": {**cfg["ranking_overlay"], "enabled": overlay_enabled, "max_abs_points": max_points, "min_data_coverage": min_cov, "weights": edited_weights},
        "backtest": {**cfg["backtest"], "minimum_trades": min_trades, "minimum_out_of_sample_trades": min_oos, "train_ratio": train_ratio},
        "portfolio_risk": {**cfg["portfolio_risk"], "max_single_stock_weight_pct": max_single, "max_sector_weight_pct": max_sector, "stress_market_correction_pct": market_shock, "stress_bear_market_pct": bear_shock, "stress_recession_pct": recession_shock, "stress_rate_shock_bp": rate_shock, "high_correlation_threshold": corr_threshold},
        "trade_journal": {**cfg["trade_journal"], "recent_trades": journal_n},
        "news": {**cfg["news"], "short_term_days": news_short, "medium_term_days": news_mid, "long_term_days": news_long},
        "daily_plan": {**cfg["daily_plan"], "preopen_enabled": preopen_on, "open_enabled": open_on, "intraday_enabled": intraday_on, "close_enabled": close_on},
    })
    if st.button("永久保存H81設定", type="primary", use_container_width=True):
        ok, msg, saved = save_settings(draft)
        if ok:
            st.success(msg)
            st.json(saved)
            st.cache_data.clear()
        else:
            st.error(msg)
    with st.expander("目前永久權威來源/診斷"):
        st.code("\n".join(setting_details or ["無診斷訊息"]))


def _candidate_options(df: pd.DataFrame) -> list[tuple[str, str]]:
    if df.empty or "股票代號" not in df.columns:
        return []
    name_series = df.get("股票名稱", pd.Series([""] * len(df), index=df.index)).fillna("").astype(str)
    code_series = df["股票代號"].fillna("").astype(str).str.replace(r"\.0$", "", regex=True)
    result = []
    for code, name in zip(code_series, name_series):
        if code:
            result.append((code, f"{code} {name}".strip()))
    return list(dict.fromkeys(result))


candidate_options = _candidate_options(latest_df)
selected_code = None
selected_row = None
if candidate_options:
    selected_label = st.sidebar.selectbox("H81分析標的", [label for _, label in candidate_options])
    selected_code = next(code for code, label in candidate_options if label == selected_label)
    selected_matches = latest_df[latest_df["股票代號"].astype(str).str.replace(r"\.0$", "", regex=True).eq(selected_code)]
    if not selected_matches.empty:
        selected_row = selected_matches.iloc[0]
else:
    st.sidebar.warning("目前沒有可讀取的最新推薦快照。")

with _tab_candidate:
    st.subheader("市場正在定價什麼｜基本面、估值、催化、風險")
    if not latest_df.empty:
        _ranked = apply_professional_research_overlay(latest_df, settings)
        if "H81專業研究總分" in _ranked.columns:
            _show_cols = [c for c in ["股票代號", "股票名稱", "H81專業研究總分", "H81資料覆蓋%", "H81排名加減分", "H81三大利多催化", "H81三大風險"] if c in _ranked.columns]
            st.markdown("#### 今日H81研究順位前5（研究，不等於Formal買進）")
            st.dataframe(_ranked.sort_values(["H81專業研究總分", "H81資料覆蓋%"], ascending=False).head(5)[_show_cols], use_container_width=True, hide_index=True)
    if selected_row is None:
        st.warning("請先完成第7頁推薦，使最新推薦永久快照有資料。")
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
        st.caption("H81不把任何情境視為必然結果；只有可驗證證據才影響研究排序。")

with _tab_chart:
    st.subheader("日線＋週期技術分析")
    if selected_row is None or not callable(get_history_data):
        st.warning("目前無法取得標的或歷史K線服務。")
    else:
        market = str(selected_row.get("市場別", "上市") or "上市")
        name = str(selected_row.get("股票名稱", "") or "")
        days = st.slider("歷史日曆天數", 90, 730, 240, 30)
        if st.button("分析日/週期K線", key="h81_chart_run"):
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

with _tab_news:
    st.subheader("把新聞轉換為市場影響")
    st.caption("可手動抓取Google News RSS，也可直接貼入公司/產業新聞。外部來源只在按鈕觸發時連線，失敗不阻塞推薦；未知新聞維持中性。")
    if selected_row is not None:
        q_name = str(selected_row.get("股票名稱", "") or "")
        q_sector = str(selected_row.get("類別", "") or "")
        news_query = st.text_input("最新新聞查詢", value=(f"{q_name} {q_sector}".strip()))
        if st.button("抓取最新公司/產業新聞", key="h81_news_fetch"):
            fetched, fetch_msg = fetch_latest_news(news_query, max_items=12, timeout=5.0)
            if fetched.empty:
                st.warning(fetch_msg)
            else:
                st.success(fetch_msg)
                _impact = news_impact_analysis(fetched)
                st.dataframe(_impact, use_container_width=True, hide_index=True)
                _scenario = news_price_scenario(selected_row, _impact, horizon_days=int(settings["news"]["short_term_days"]))
                if _scenario.get("available"):
                    st.markdown("#### 新聞價格波動情境（不是預測）")
                    st.json(_scenario)
                else:
                    st.caption(_scenario.get("message", "缺資料，未建立價格情境"))
    raw_news = st.text_area("貼入新聞資料", height=220, placeholder="2026-09-21｜公司公告｜月營收創高\n2026-09-21｜媒體｜新產品通過認證")
    if st.button("解析新聞影響"):
        rows = []
        for line in raw_news.splitlines():
            line = line.strip()
            if not line:
                continue
            parts = [x.strip() for x in re_split(line)]
            if len(parts) >= 3:
                rows.append({"日期": parts[0], "來源": parts[1], "標題": "｜".join(parts[2:])})
            else:
                rows.append({"標題": line})
        result = news_impact_analysis(rows)
        st.dataframe(result, use_container_width=True, hide_index=True)
        if selected_row is not None:
            _scenario = news_price_scenario(selected_row, result, horizon_days=int(settings["news"]["short_term_days"]))
            if _scenario.get("available"):
                st.json(_scenario)

with _tab_backtest:
    st.subheader("策略回測｜勝率、獲利因子、最大回撤、外樣本")
    bt_mode = st.radio("回測來源", ["第8頁實際/推薦績效", "標的K線策略"], horizontal=True)
    if bt_mode == "第8頁實際/推薦績效":
        if records_df.empty:
            st.warning("第8頁推薦紀錄尚無可用資料。")
        else:
            return_candidates = [c for c in ["實際報酬%", "可執行交易5日%", "推薦後5日%", "推薦後3日%", "推薦後1日%", "損益幅%"] if c in records_df.columns]
            selected_return = st.selectbox("回測報酬欄位", return_candidates) if return_candidates else None
            if st.button("執行績效回測檢討"):
                result = backtest_trade_records(records_df, return_col=selected_return, train_ratio=float(settings["backtest"]["train_ratio"]))
                st.json(result)
                if result.get("available"):
                    st.caption("外樣本轉弱時不會自動放寬規則；新規則先以影子/低權重驗證。")
    else:
        if selected_row is None or not callable(get_history_data):
            st.warning("請先選擇標的，且歷史K線服務需可用。")
        else:
            s1, s2, s3, s4 = st.columns(4)
            with s1:
                strategy = st.selectbox("策略", ["MA_CROSS", "RSI_REVERSAL"], format_func=lambda x: "均線交叉" if x == "MA_CROSS" else "RSI低檔轉強")
            with s2:
                short_ma = st.number_input("短均線", 2, 60, 5, 1)
            with s3:
                long_ma = st.number_input("長均線", 5, 240, 20, 1)
            with s4:
                cost_bps = st.number_input("每次部位切換成本(bp)", 0.0, 200.0, 30.0, 5.0)
            q1, q2 = st.columns(2)
            with q1:
                rsi_entry = st.number_input("RSI進場門檻", 10.0, 50.0, 35.0, 1.0)
            with q2:
                rsi_exit = st.number_input("RSI退出門檻", 40.0, 90.0, 55.0, 1.0)
            if st.button("執行K線策略回測"):
                market = str(selected_row.get("市場別", "上市") or "上市")
                name = str(selected_row.get("股票名稱", "") or "")
                with st.spinner("讀取K線並執行無look-ahead外樣本回測..."):
                    hist = get_history_data(selected_code, name, market, date.today() - timedelta(days=730), date.today())
                    result = backtest_price_strategy(hist, strategy=strategy, short_ma=int(short_ma), long_ma=int(long_ma), rsi_entry=float(rsi_entry), rsi_exit=float(rsi_exit), cost_bps=float(cost_bps), train_ratio=float(settings["backtest"]["train_ratio"]))
                st.json(result)
                st.caption("回測只用於研究；任何參數改善先看外樣本，不把最佳歷史參數直接當未來規則。")

with _tab_portfolio:
    st.subheader("投資組合壓力測試")
    st.caption("分析集中度、產業/地區曝險、重複押注與可選的歷史相關性。持倉內容只留在本次畫面，不會自動永久保存。")
    template = pd.DataFrame([
        {"股票代號": "", "配置比例%": 25.0, "類別": "", "地區": "台灣", "市場別": "上市", "Beta": 1.0, "利率敏感度": None},
        {"股票代號": "", "配置比例%": 25.0, "類別": "", "地區": "台灣", "市場別": "上市", "Beta": 1.0, "利率敏感度": None},
        {"股票代號": "", "配置比例%": 25.0, "類別": "", "地區": "台灣", "市場別": "上市", "Beta": 1.0, "利率敏感度": None},
        {"股票代號": "", "配置比例%": 25.0, "類別": "", "地區": "台灣", "市場別": "上市", "Beta": 1.0, "利率敏感度": None},
    ])
    portfolio_df = st.data_editor(template, num_rows="dynamic", use_container_width=True, key="h81_portfolio")
    if st.button("執行壓力測試"):
        result = portfolio_stress_test(portfolio_df, settings)
        st.json(result)
    if st.button("分析隱藏相關性（讀取近一年K線）"):
        if not callable(get_history_data):
            st.error("歷史K線服務不可用。")
        else:
            histories = {}
            valid_rows = portfolio_df.copy()
            valid_rows["股票代號"] = valid_rows["股票代號"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
            valid_rows = valid_rows[valid_rows["股票代號"].ne("")].head(12)
            with st.spinner("讀取投組K線並計算日報酬相關性..."):
                for _, prow in valid_rows.iterrows():
                    code = str(prow.get("股票代號") or "").strip()
                    market = str(prow.get("市場別") or "上市")
                    if code:
                        histories[code] = get_history_data(code, "", market, date.today() - timedelta(days=420), date.today())
            corr = portfolio_correlation_analysis(histories, threshold=float(settings["portfolio_risk"]["high_correlation_threshold"]))
            st.json(corr)
            st.caption("高相關代表可能重複押注；壓力市場中相關性可能進一步上升。")

with _tab_journal:
    st.subheader("最近交易日誌分析")
    if records_df.empty:
        st.warning("第8頁沒有可分析紀錄。")
    else:
        recent_n = st.slider("檢視最近交易/成熟推薦", 5, 100, int(settings["trade_journal"]["recent_trades"]), 5)
        if st.button("分析最近交易"):
            result = analyze_trade_journal(records_df, recent_n=recent_n)
            st.json(result)
            if result.get("available"):
                st.markdown("#### 三條一致性規則")
                for i, rule in enumerate(result.get("personalized_rules", []), 1):
                    st.write(f"{i}. {rule}")

with _tab_plan:
    st.subheader("盤前→開盤→盤中→收盤交易檢查表")
    if selected_row is None:
        st.warning("沒有可建立計畫的標的。")
    else:
        plan = build_daily_trading_plan(selected_row, settings)
        st.dataframe(plan, use_container_width=True, hide_index=True)
        st.caption("這是條件式執行清單，不會向券商送單，也不把研究推薦當成買進許可。")

