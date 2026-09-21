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
    def inject_pro_theme() -> None:
        return None

from godpick_h82_learning_settings import (
    VERSION as H82_SETTINGS_VERSION,
    DEFAULT_SETTINGS,
    load_settings,
    normalize_settings,
    save_settings,
)
from godpick_h82_adaptive_learning import (
    VERSION as H82_CORE_VERSION,
    RECORDS_FILE,
    STATE_FILE,
    apply_adaptive_learning_overlay,
    build_learning_health_tables,
    load_learning_state,
    refresh_learning_state,
)

st.set_page_config(page_title="19_Adaptive GodPick Learning Core", layout="wide")
inject_pro_theme()

st.title("19_Adaptive GodPick Learning Core｜H82 自適應股神學習核心")
st.caption(
    "H82 = 成熟績效 → 錯誤歸因 → 市場Regime/產業/決策狀態/H81分桶 → 收縮與信心門檻 → "
    "小幅研究排序校正。H82永遠不能建立H64/H68 Formal權威，也不能繞過RR、停損、流動性或資料新鮮度。"
)

cfg, cfg_details = load_settings()
state = load_learning_state(settings=cfg)

tab_cfg, tab_health, tab_errors, tab_segments, tab_sim = st.tabs([
    "永久設定", "學習健康", "錯誤歸因", "分群證據", "候選影子模擬"
])

with tab_cfg:
    st.subheader("H82 永久設定")
    st.info("設定由 godpick_adaptive_learning_settings.json 的本機＋GitHub/Firestore權威保存；不是 session_state。")
    c1, c2, c3 = st.columns(3)
    with c1:
        enabled = st.checkbox("啟用H82", value=bool(cfg.get("enabled", True)))
        mode_labels = {
            "active_bounded": "Active Bounded｜成熟後小幅套用",
            "shadow_only": "Shadow Only｜只計算不影響排序",
            "frozen": "Frozen｜完全凍結",
        }
        mode_rev = {v: k for k, v in mode_labels.items()}
        current_mode = cfg.get("mode") if cfg.get("mode") in mode_labels else "active_bounded"
        mode_label = st.selectbox("學習模式", list(mode_labels.values()), index=list(mode_labels.keys()).index(current_mode))
        primary_horizon = st.selectbox("主要成熟績效週期", [1, 3, 5, 10, 20], index=[1, 3, 5, 10, 20].index(int(cfg.get("primary_horizon", 5))))
        min_global = st.number_input("全域最低成熟樣本", 8, 5000, int(cfg.get("minimum_global_mature_samples", 30)), step=1)
        min_segment = st.number_input("分群最低成熟樣本", 5, 1000, int(cfg.get("minimum_segment_mature_samples", 12)), step=1)
    with c2:
        min_eff = st.number_input("分群最低有效樣本權重", 2.0, 1000.0, float(cfg.get("minimum_effective_segment_samples", 6.0)), step=0.5)
        half_life = st.number_input("時間衰減半衰期（日）", 20, 730, int(cfg.get("half_life_days", 90)), step=5)
        max_rank = st.number_input("H82最大研究排序影響（±分）", 0.0, 3.0, float(cfg.get("max_abs_rank_points", 2.0)), step=0.1)
        max_component = st.number_input("單一分群最大影響（±分）", 0.0, 1.5, float(cfg.get("max_component_abs_points", 0.9)), step=0.1)
        confidence_floor = st.slider("分群最低信心", 0.30, 0.90, float(cfg.get("confidence_floor", 0.55)), 0.01)
    with c3:
        shrinkage = st.number_input("收縮先驗樣本", 5.0, 200.0, float(cfg.get("shrinkage_prior_samples", 20.0)), step=1.0)
        winsor = st.number_input("績效Winsorize上限（±%）", 5.0, 100.0, float(cfg.get("winsorize_return_pct", 20.0)), step=1.0)
        suspicious = st.slider("可疑績效代理最大比例", 0.00, 0.50, float(cfg.get("max_suspicious_proxy_ratio", 0.10)), 0.01)
        feat = cfg.get("features") or {}
        time_decay = st.checkbox("啟用時間衰減", value=bool(feat.get("time_decay", True)))
        freeze_quality = st.checkbox("資料品質異常自動凍結", value=bool(feat.get("freeze_on_data_quality", True)))

    st.markdown("#### 四大自適應分群權重")
    weights = dict(cfg.get("segment_weights") or {})
    w1, w2, w3, w4 = st.columns(4)
    with w1:
        wr = st.number_input("市場Regime", 0, 100, int(weights.get("market_regime", 40)), step=1)
    with w2:
        ws = st.number_input("產業", 0, 100, int(weights.get("sector", 25)), step=1)
    with w3:
        wd = st.number_input("決策狀態", 0, 100, int(weights.get("decision_status", 15)), step=1)
    with w4:
        wh = st.number_input("H81研究分桶", 0, 100, int(weights.get("h81_bucket", 20)), step=1)

    candidate = normalize_settings({
        **cfg,
        "enabled": enabled,
        "mode": mode_rev[mode_label],
        "primary_horizon": primary_horizon,
        "minimum_global_mature_samples": min_global,
        "minimum_segment_mature_samples": min_segment,
        "minimum_effective_segment_samples": min_eff,
        "half_life_days": half_life,
        "max_abs_rank_points": max_rank,
        "max_component_abs_points": max_component,
        "confidence_floor": confidence_floor,
        "shrinkage_prior_samples": shrinkage,
        "winsorize_return_pct": winsor,
        "max_suspicious_proxy_ratio": suspicious,
        "segment_weights": {"market_regime": wr, "sector": ws, "decision_status": wd, "h81_bucket": wh},
        "features": {**feat, "time_decay": time_decay, "freeze_on_data_quality": freeze_quality},
    })

    b1, b2 = st.columns(2)
    with b1:
        if st.button("永久保存 H82 設定", type="primary", use_container_width=True):
            ok, msg, saved = save_settings(candidate)
            if ok:
                st.success(msg)
                st.caption(f"保存版本：{saved.get('version')}｜seq={saved.get('update_seq')}")
            else:
                st.error(msg)
    with b2:
        if st.button("重建並永久保存 H82 學習狀態", use_container_width=True):
            new_state, notes = refresh_learning_state(settings=candidate, persist_remote=True)
            if new_state.get("available"):
                st.success("H82學習狀態已重建；永久化結果如下。")
            else:
                st.warning("H82學習狀態已重建，但因成熟樣本或資料品質規則目前保持凍結。")
            for note in notes:
                st.caption(note)

    with st.expander("永久設定權威讀取明細", expanded=False):
        st.write(f"Settings version: {H82_SETTINGS_VERSION}")
        st.write(f"Core version: {H82_CORE_VERSION}")
        st.json(cfg)
        for line in cfg_details:
            st.caption(line)

with tab_health:
    tables = build_learning_health_tables(state)
    base = state.get("baseline") or {}
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("H82狀態", "ACTIVE" if state.get("available") else "FROZEN")
    c2.metric("成熟樣本", int(base.get("sample", 0) or 0))
    c3.metric("有效樣本權重", f"{float(base.get('effective_sample', 0) or 0):.1f}")
    c4.metric("成熟勝率", f"{float(base.get('win_rate', 0) or 0)*100:.1f}%")
    c5.metric("平均報酬", f"{float(base.get('avg_return', 0) or 0):+.2f}%")
    if state.get("freeze_reasons"):
        st.warning("目前自適應凍結原因：" + "；".join(state.get("freeze_reasons") or []))
    st.dataframe(tables["summary"], use_container_width=True, hide_index=True)
    st.markdown("#### H82目前學習指令")
    st.dataframe(tables["directives"], use_container_width=True, hide_index=True)
    st.caption(f"Records authority: {RECORDS_FILE}｜Derived learning state: {STATE_FILE}")

with tab_errors:
    tables = build_learning_health_tables(state)
    st.subheader("交易/推薦錯誤歸因")
    st.caption("錯誤標籤不是事後替市場找理由；只有成熟績效且欄位可驗證時才建立。")
    if tables["errors"].empty:
        st.info("目前沒有足夠成熟錯誤樣本。")
    else:
        st.dataframe(tables["errors"].sort_values("rate", ascending=False), use_container_width=True, hide_index=True)

with tab_segments:
    tables = build_learning_health_tables(state)
    st.subheader("市場環境 / 產業 / 決策狀態 / H81分桶")
    if tables["segments"].empty:
        st.info("目前沒有達成熟門檻的分群統計。")
    else:
        show = tables["segments"].copy()
        show = show.sort_values(["分群", "ready", "effective_sample"], ascending=[True, False, False])
        st.dataframe(show, use_container_width=True, hide_index=True)
    st.caption("ready=False 的分群即使短期績效很好，也不會影響研究排序；這是防止小樣本過擬合。")

with tab_sim:
    st.subheader("候選影子模擬")
    st.caption("可直接貼一列候選欄位做H82影子分析；這裡不送單、不建立Formal權威。")
    sector = st.text_input("產業/類別", "半導體業")
    market_mode = st.selectbox("市場環境", ["多頭/進攻", "盤整/中性", "空頭/防禦", "高波動/事件", "市場環境未確認"])
    h81_score = st.number_input("H81專業研究總分", 0.0, 100.0, 70.0, step=1.0)
    chase = st.number_input("追價風險分", 0.0, 100.0, 55.0, step=1.0)
    rr = st.number_input("H79成本後RR", 0.0, 10.0, 1.8, step=0.1)
    h79_status = st.selectbox("H79推薦狀態", ["研究推薦｜條件追蹤", "研究推薦｜等待拉回", "研究推薦｜重建進場價格", "研究推薦｜等待正式授權"])
    mock = pd.DataFrame([{
        "股票代號": "SIM", "類別": sector, "H72市場模式": market_mode,
        "H81專業研究總分": h81_score, "追價風險分": chase,
        "H79成本後RR": rr, "H79推薦狀態": h79_status, "H79自適應機會分": 65.0,
    }])
    result = apply_adaptive_learning_overlay(mock, state=state, settings=cfg)
    cols = [c for c in result.columns if c.startswith("H82")]
    st.dataframe(result[cols], use_container_width=True, hide_index=True)
