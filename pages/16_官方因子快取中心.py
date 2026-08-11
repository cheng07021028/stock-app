# -*- coding: utf-8 -*-
from __future__ import annotations

try:
    from app_auth import require_login
    require_login()
except Exception as _auth_e:
    import streamlit as st
    st.error(f"登入系統載入失敗：{_auth_e}")
    st.stop()

import pandas as pd
import streamlit as st

try:
    from utils import inject_pro_theme
except Exception:
    def inject_pro_theme() -> None:  # type: ignore
        return None

from official_factor_service import (
    FACTOR_COLUMNS,
    build_official_factor_cache,
    cache_status,
    export_cache_csv_bytes,
    finmind_config_status,
    get_factor_authority_status,
    load_factor_frame,
    load_stock_universe,
    load_update_logs,
    push_cache_to_github,
    read_cache_from_github,
)

st.set_page_config(page_title="16_官方因子快取中心", layout="wide")
inject_pro_theme()

st.title("16_官方因子快取中心")
st.caption("V186｜官方因子業務日期永久權威＋Reboot防回退｜TWSE/TPEx current OpenAPI 優先＋FinMind 缺值備援")


def _fmt(v):
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return v


def _display_status() -> None:
    s = cache_status()
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    records = int(s.get("record_count", 0) or 0)
    complete = int(s.get("complete_count", 0) or 0)
    eligible = int(s.get("eligible_count", 0) or 0)
    eligible_complete = int(s.get("eligible_complete_count", 0) or 0)
    eligible_coverage = float(s.get("eligible_coverage", 0.0) or 0.0)
    c1.metric("快取筆數", records)
    c2.metric("上市+上櫃母體", eligible)
    c3.metric("上市+上櫃完整>=60", eligible_complete)
    c4.metric("主要市場有效覆蓋率", f"{eligible_coverage:.1f}%")
    c5.metric("檔案大小 KB", s.get("size_kb", 0.0))
    c6.metric("更新時間", s.get("updated_at") or "未更新")
    authority = get_factor_authority_status()
    authority_state = authority.get("state") if isinstance(authority.get("state"), dict) else {}
    data_date = authority.get("data_date") or "未驗證"
    remote_ok = bool(authority_state.get("remote_permanent_confirmed"))
    st.caption(
        f"V186永久權威｜資料日期 {data_date}｜恢復來源 {authority.get('restore_source') or 'local'}｜"
        f"遠端永久化 {'✅ 已確認' if remote_ok else '⚠️ 尚未確認'}"
    )
    if not remote_ok:
        st.warning("目前官方因子尚未證實已寫入遠端永久層；若此時 Reboot，可能只能從較舊遠端版本恢復。請先執行更新或『同步快取到 GitHub』直到顯示已確認。")
    if eligible and eligible_complete == 0:
        st.warning("目前上市＋上櫃官方因子完整度>=60 為 0；代表主要市場來源仍未抓成功或資料不足，暫不建議接進 07 推薦分數。")
    elif eligible_complete > 0:
        st.success(f"主要市場官方因子已有可用完整資料：{eligible_complete}/{eligible} 筆（{eligible_coverage:.1f}%）。")
    market_stats = s.get("market_stats", {}) or {}
    if market_stats:
        parts = []
        for market in ["上市", "上櫃", "興櫃"]:
            item = market_stats.get(market) or {}
            if item:
                parts.append(f"{market} {int(item.get('complete', 0) or 0)}/{int(item.get('rows', 0) or 0)}")
        if parts:
            st.caption("各市場完整度>=60：" + "｜".join(parts) + "。興櫃目前屬延伸覆蓋，不納入上市＋上櫃主覆蓋率分母。")
    df = load_factor_frame()
    if df is not None and not df.empty:
        fallback_rows = 0
        official_only_rows = 0
        if "因子備援來源" in df.columns:
            fallback_rows = int(df["因子備援來源"].astype(str).str.strip().ne("").sum())
        official_only_rows = max(0, len(df) - fallback_rows)
        a, b, c = st.columns(3)
        a.metric("純官方資料列", official_only_rows)
        b.metric("含備援/舊快取補值", fallback_rows)
        if "因子來源可信度" in df.columns:
            trust = pd.to_numeric(df["因子來源可信度"], errors="coerce").dropna()
            c.metric("平均來源可信度", f"{trust.mean():.1f}" if not trust.empty else "-")
        else:
            c.metric("平均來源可信度", "-")
    with st.expander("快取狀態 / 診斷", expanded=False):
        st.write(f"路徑：`{s.get('path', '')}`")
        diagnostics = s.get("diagnostics", []) or []
        if diagnostics:
            for msg in diagnostics[-25:]:
                st.write(f"- {msg}")
        else:
            st.write("目前沒有診斷訊息。")


with st.sidebar:
    st.header("V182 更新設定")
    market_filter = st.selectbox("更新市場", ["全部", "上市", "上櫃"], index=0)
    scan_limit = st.selectbox("測試/更新筆數", [0, 50, 200, 500, 1000, 1500, 2000], index=0, help="0 = 使用股票主檔全部股票。")
    include_institutional = st.checkbox("更新法人買賣超", value=True)
    include_revenue = st.checkbox("更新月營收", value=True)
    include_valuation = st.checkbox("更新 PER / PBR / 估算 EPS", value=True)
    fm_status = finmind_config_status()
    enable_finmind = st.checkbox("官方缺值時啟用 FinMind 備援", value=True, disabled=not fm_status.get("token_configured"))
    finmind_max_stocks = st.selectbox("FinMind 本輪最多補值股票", [50, 100, 120, 200], index=2, disabled=not fm_status.get("token_configured"), help="避免超過 API 每小時限額；每次更新會增量補值。")
    update_mode = st.selectbox(
        "更新模式",
        ["快速安全（90秒，官方 OpenAPI 優先）", "完整增量（240秒，FinMind只補缺值）"],
        index=0,
        help="快速模式只跑 TWSE/TPEx current OpenAPI 與既有快取；完整模式才對仍缺值股票做有限 FinMind 逐檔補值。兩種模式都有硬性時間/請求上限。",
    )
    st.caption("FinMind Token：" + ("已設定" if fm_status.get("token_configured") else "未設定（請在 Streamlit Secrets 加入 FINMIND_TOKEN）"))
    st.divider()
    do_update = st.button("更新官方因子快取", type="primary", use_container_width=True)
    do_pull = st.button("從 GitHub 讀取快取", use_container_width=True)
    do_push = st.button("同步快取到 GitHub", use_container_width=True)

st.info(
    "V182 來源階梯：① TWSE current OpenAPI/T86 ＋ TPEx current OpenAPI；"
    "② 已知 404 的舊 TPEx/MOPS 路徑預設停用，不再重複浪費請求；③ 完整增量模式才用 FinMind 對仍缺值股票逐檔補值；"
    "④ 最後保留前次有效快取。官方值永遠優先，來源、資料日期、可信度與補值欄位數都會保留。"
    "第 07 頁只讀快取，不會在推薦時即時大量呼叫外部 API。"
)
st.warning(
    "安全修正：V182 已停止把 FINMIND_TOKEN 放在 URL query string，也會遮蔽診斷中的 token。"
    "如果舊版錯誤畫面/截圖曾顯示完整 token，建議在 FinMind 重新產生 token，並只更新 Streamlit Secrets。"
)
if not finmind_config_status().get("token_configured"):
    st.warning('FinMind 備援尚未啟用。請在 Streamlit Cloud → App settings → Secrets 加入 `FINMIND_TOKEN = \"你的token\"`，重新啟動後再更新。不要把 token 寫進程式或 GitHub。')

if do_pull:
    ok, msg = read_cache_from_github()
    (st.success if ok else st.warning)(msg)

if do_update:
    limit = int(scan_limit) if int(scan_limit) > 0 else None
    with st.spinner("正在更新官方因子快取；官方網站若較慢，請稍候..."):
        df, meta = build_official_factor_cache(
            limit=limit,
            market_filter=market_filter,
            include_institutional=include_institutional,
            include_revenue=include_revenue,
            include_valuation=include_valuation,
            save=True,
            enable_finmind_fallback=bool(enable_finmind),
            finmind_max_stocks=int(finmind_max_stocks),
            quick_mode=str(update_mode).startswith("快速"),
            max_runtime_seconds=90 if str(update_mode).startswith("快速") else 240,
            max_requests=60 if str(update_mode).startswith("快速") else 180,
            finmind_bulk_only=str(update_mode).startswith("快速"),
        )
    if meta.get("ok"):
        if meta.get("preserved_old_cache"):
            st.warning(f"本次抓取完成 {len(df)} 筆，但完整度偏低，已保留舊有效快取。")
        else:
            suffix = "（已達安全上限並保存目前成果）" if meta.get("timed_out") else ""
            update_text = (
                f"官方因子快取已更新：{len(df)} 筆；"
                f"上市+上櫃完整>=60：{meta.get('eligible_complete_count', 0)}/{meta.get('eligible_count', 0)} "
                f"（{float(meta.get('eligible_coverage', 0) or 0):.1f}%）。{suffix}"
            )
            if meta.get("permanent_ok"):
                st.success(update_text + "｜V186遠端永久化已確認")
            else:
                st.warning(update_text + "｜⚠️ 僅本機更新，遠端永久化尚未確認；請勿在完成同步前Reboot")
        st.caption(f"本輪耗時 {meta.get('elapsed_seconds', 0)} 秒｜網路請求 {meta.get('request_count', 0)}/{meta.get('request_budget', 0)}")
    else:
        st.error("官方因子快取更新失敗，請查看診斷訊息。")
    for msg in meta.get("diagnostics", [])[-20:]:
        st.write(f"- {msg}")

if do_push:
    ok, msg = push_cache_to_github()
    (st.success if ok else st.warning)(msg)

_display_status()

st.subheader("資料預覽")
df = load_factor_frame()
if df.empty:
    st.warning("尚無官方因子快取。請先按左側「更新官方因子快取」。")
    universe = load_stock_universe(limit=20)
    if not universe.empty:
        with st.expander("股票主檔前 20 筆檢查", expanded=False):
            st.dataframe(universe, use_container_width=True, hide_index=True)
else:
    search = st.text_input("搜尋股票代號 / 名稱 / 產業", "")
    show_df = df.copy()
    if search.strip():
        kw = search.strip()
        mask = pd.Series(False, index=show_df.index)
        for col in ["股票代號", "股票名稱", "正式產業別", "市場別"]:
            if col in show_df.columns:
                mask = mask | show_df[col].astype(str).str.contains(kw, case=False, na=False)
        show_df = show_df[mask]

    priority_cols = [
        "股票代號", "股票名稱", "市場別", "正式產業別",
        "官方因子總分", "官方資料完整度", "官方因子資料狀態",
        "法人籌碼官方分數", "外資近5日買賣超", "投信近5日買賣超", "三大法人近5日合計", "法人連買天數",
        "營收成長官方分數", "月營收YoY%", "月營收MoM%", "累計營收YoY%", "營收年月",
        "官方估值風險分數", "PER本益比", "估算EPS", "PBR股價淨值比", "股利殖利率%",
        "官方因子更新時間", "官方因子資料源",
    ]
    cols = [c for c in priority_cols if c in show_df.columns] + [c for c in show_df.columns if c not in priority_cols]
    st.dataframe(show_df[cols].map(_fmt) if hasattr(show_df, "map") else show_df[cols].applymap(_fmt), use_container_width=True, hide_index=True)
    st.download_button(
        "下載 official_factors_cache.csv",
        data=export_cache_csv_bytes(),
        file_name="official_factors_cache.csv",
        mime="text/csv",
        use_container_width=True,
    )

st.subheader("更新紀錄")
logs = load_update_logs()
if logs:
    st.dataframe(pd.DataFrame(logs[:50]), use_container_width=True, hide_index=True)
else:
    st.caption("尚無更新紀錄。")

with st.expander("V182 說明", expanded=False):
    st.markdown(
        """
- 本頁是官方因子資料層，不會取代 07 股神推薦；07 只讀快取，不在推薦流程大量打外部 API。
- V182 將上櫃估值改為 TPEx `tpex_mainboard_peratio_analysis`、上櫃法人改為 `tpex_3insti_daily_trading`、上櫃營收改為 `mopsfin_t187ap05_O` current OpenAPI。
- 上市估值維持 TWSE `BWIBBU_ALL`；上市營收使用 TWSE OpenAPI；上市法人維持 T86。
- 舊 TPEx `/www/zh-tw/afterTrading/...` 與 MOPS `/nas/t21/...` 已知有 404 風險，V182 預設完全停用；只有明確設定 `OFFICIAL_FACTOR_ENABLE_LEGACY_ENDPOINTS=1` 才會作緊急回退測試。
- TPEx 法人 current OpenAPI 若只提供最新日，V182 會保存每日官方快照，逐日累積成 3/5 日法人合計；不會以單日數值冒充 5 日。
- FinMind 不再先做容易 400 的全市場猜測型查詢；快速模式完全不逐檔打 FinMind，完整增量才對「仍缺值」股票有限逐檔補值。
- FinMind 月營收 MoM/YoY 已改為由真正營收值計算，不再把 `revenue_month` / `revenue_year` 誤當百分比。
- V182 不再把 FINMIND_TOKEN 放入 URL，診斷訊息也會遮蔽 token。
- 完整度計分中，PER 為空但 PBR/殖利率有效的虧損公司，仍可取得估值資料完整度，不再被誤判成整個估值缺失。
- 首頁主覆蓋率以「上市＋上櫃」計算；興櫃保留延伸資料，但不再用大量尚無同等因子來源的興櫃股票稀釋主要市場覆蓋率。
- 若本次來源異常、完整度大幅低於舊快取，仍保留舊有效快取，不會用壞資料覆蓋。
        """
    )
