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
    load_factor_frame,
    load_stock_universe,
    load_update_logs,
    push_cache_to_github,
    read_cache_from_github,
)

st.set_page_config(page_title="16_官方因子快取中心", layout="wide")
inject_pro_theme()

st.title("16_官方因子快取中心")
st.caption("V109｜官方優先＋FinMind可信備援｜法人 / 月營收 / EPS / PER 快取中心｜供 07/08/10/14 讀取")


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
    coverage = (complete / records * 100.0) if records else 0.0
    c1.metric("快取筆數", records)
    c2.metric("完整度 >= 60", complete)
    c3.metric("快取有效覆蓋率", f"{coverage:.1f}%")
    c4.metric("檔案大小 KB", s.get("size_kb", 0.0))
    c5.metric("快取存在", "是" if s.get("exists") else "否")
    c6.metric("更新時間", s.get("updated_at") or "未更新")
    if records and complete == 0:
        st.warning("目前官方因子完整度>=60 為 0；代表官方資料尚未抓成功或仍不足，暫不建議接進 07 推薦分數。")
    elif complete > 0:
        st.success(f"官方因子已有可用完整資料：{complete} 筆。")
    df = load_factor_frame()
    if df is not None and not df.empty:
        fallback_rows = 0
        official_only_rows = 0
        if "因子備援來源" in df.columns:
            fallback_rows = int(df["因子備援來源"].astype(str).str.strip().ne("").sum())
        official_only_rows = max(0, len(df) - fallback_rows)
        a, b, c = st.columns(3)
        a.metric("純官方資料列", official_only_rows)
        b.metric("含 FinMind/快取補值", fallback_rows)
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
    st.header("V108C 更新設定")
    market_filter = st.selectbox("更新市場", ["全部", "上市", "上櫃"], index=0)
    scan_limit = st.selectbox("測試/更新筆數", [0, 50, 200, 500, 1000, 1500, 2000], index=0, help="0 = 使用股票主檔全部股票。")
    include_institutional = st.checkbox("更新法人買賣超", value=True)
    include_revenue = st.checkbox("更新月營收", value=True)
    include_valuation = st.checkbox("更新 PER / PBR / 估算 EPS", value=True)
    fm_status = finmind_config_status()
    enable_finmind = st.checkbox("官方缺值時啟用 FinMind 備援", value=True, disabled=not fm_status.get("token_configured"))
    finmind_max_stocks = st.selectbox("FinMind 本輪最多補值股票", [50, 100, 120, 200], index=2, disabled=not fm_status.get("token_configured"), help="避免超過 API 每小時限額；每次更新會增量補值。")
    st.caption("FinMind Token：" + ("已設定" if fm_status.get("token_configured") else "未設定（請在 Streamlit Secrets 加入 FINMIND_TOKEN）"))
    st.divider()
    do_update = st.button("更新官方因子快取", type="primary", use_container_width=True)
    do_pull = st.button("從 GitHub 讀取快取", use_container_width=True)
    do_push = st.button("同步快取到 GitHub", use_container_width=True)

st.info(
    "建議流程：先用 TWSE／TPEx／MOPS 更新；只有官方缺值時才由 FinMind 補值。"
    "FinMind 不會覆蓋較新的官方值，每筆都會保存來源、可信度與補值欄位數。"
    "第 07 頁只讀快取，不會在推薦時即時大量呼叫外部 API。"
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
        )
    if meta.get("ok"):
        if meta.get("preserved_old_cache"):
            st.warning(f"本次抓取完成 {len(df)} 筆，但完整度偏低，已保留舊有效快取。")
        else:
            st.success(f"官方因子快取已更新：{len(df)} 筆；完整度>=60：{meta.get('complete_count', 0)} 筆。")
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

with st.expander("V109 說明", expanded=False):
    st.markdown(
        """
- 本頁是官方因子資料層，不會取代 07 股神推薦。
- 慢的官方資料更新集中在本頁；07 後續只讀 `official_factors_cache.json`。
- 官方來源失敗時會保留診斷訊息，不會讓 07、10、8、14 主線中斷。
- V108A 修正 Streamlit Cloud 連 TWSE/TPEX 時可能發生的 SSL 憑證驗證失敗。
- V108B 修正 SSL 備援成功但內容為空/HTML/非 JSON 時誤判成功的問題。
- V108C 修正推薦表已存在空白/0值欄位時，真實快取被寫進 `_官方` 暫存欄卻未回填，導致覆蓋率永遠0%的根因。
- V108C 新增櫃買中心上櫃法人與估值 best-effort 來源，月營收則針對單一市場失敗時個別啟用 MOPS HTML 備援。
- 若本次抓取完整度低於舊快取，會保留舊有效快取，不會用壞資料覆蓋。
- V109 新增 FinMind 可信備援：僅補空值，官方值永遠優先，並保存來源可信度與補值數量。
- 未設定 FINMIND_TOKEN 時不會匿名大量呼叫，避免耗盡額度。
- 每次補值有安全請求上限，未完成股票會留待下次增量更新。
        """
    )
