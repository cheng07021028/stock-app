# -*- coding: utf-8 -*-
"""
godpick_bi_theme.py
v135｜股神版 Streamlit BI 圖表視覺核心

這個檔案不改資料來源、不改推薦演算法，只統一 Streamlit / Plotly 的視覺呈現。
"""
from __future__ import annotations
from typing import Any
import streamlit as st

BI_VERSION = "v135 BI Pro"


def inject_bi_css() -> None:
    if st.session_state.get("_godpick_bi_css_loaded_v135"):
        return
    st.session_state["_godpick_bi_css_loaded_v135"] = True
    st.markdown(
        """
        <style>
        .block-container{padding-top:1.1rem;}
        div[data-testid="stMetric"]{
          background:linear-gradient(180deg,#ffffff 0%,#f8fafc 100%);
          border:1px solid #e2e8f0;
          border-radius:18px;
          padding:14px 16px;
          box-shadow:0 8px 22px rgba(15,23,42,0.06);
        }
        div[data-testid="stMetric"] label{color:#64748b!important;font-weight:800!important;}
        div[data-testid="stMetric"] [data-testid="stMetricValue"]{font-weight:900!important;color:#0f172a!important;}
        div[data-testid="stMetric"] [data-testid="stMetricDelta"]{font-weight:800!important;}
        div[data-testid="stDataFrame"], div[data-testid="stDataEditor"]{
          border-radius:16px!important;overflow:hidden!important;border:1px solid #e2e8f0!important;
          box-shadow:0 8px 22px rgba(15,23,42,0.04)!important;
        }
        .godpick-bi-note{
          display:inline-flex;align-items:center;gap:8px;margin:4px 0 12px 0;
          background:#eff6ff;border:1px solid #bfdbfe;color:#1e3a8a;
          border-radius:999px;padding:6px 12px;font-weight:850;font-size:12px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _style_plotly_figure(fig: Any, page_key: str = "") -> Any:
    if fig is None or not hasattr(fig, "update_layout"):
        return fig
    try:
        fig.update_layout(
            paper_bgcolor="white",
            plot_bgcolor="white",
            font=dict(family="Arial, Microsoft JhengHei, sans-serif", color="#0f172a", size=13),
            hovermode="x unified",
            hoverlabel=dict(
                bgcolor="rgba(255,255,255,0.97)",
                bordercolor="rgba(37,99,235,0.30)",
                font=dict(size=13, color="#0f172a"),
                namelength=-1,
            ),
            legend=dict(
                bgcolor="rgba(255,255,255,0.88)",
                bordercolor="rgba(226,232,240,0.95)",
                borderwidth=1,
                font=dict(size=12, color="#334155"),
            ),
            transition=dict(duration=180),
        )
        fig.update_xaxes(
            showgrid=False,
            showline=True,
            linecolor="rgba(148,163,184,0.35)",
            tickfont=dict(color="#64748b", size=11),
            title_font=dict(color="#475569", size=12),
            zeroline=False,
        )
        fig.update_yaxes(
            showgrid=True,
            gridcolor="rgba(148,163,184,0.16)",
            showline=True,
            linecolor="rgba(148,163,184,0.35)",
            tickfont=dict(color="#64748b", size=11),
            title_font=dict(color="#475569", size=12),
            zeroline=False,
        )
    except Exception:
        return fig
    return fig


def install_bi_dashboard_style(page_key: str = "") -> None:
    """安裝 BI CSS 與 Plotly 自動美化。必須在 st.set_page_config 後呼叫。"""
    inject_bi_css()
    patch_key = f"_godpick_bi_plotly_patch_v135_{page_key or 'global'}"
    if st.session_state.get(patch_key):
        return
    original_plotly_chart = st.plotly_chart

    def _patched_plotly_chart(fig_or_data=None, *args, **kwargs):
        try:
            fig_or_data = _style_plotly_figure(fig_or_data, page_key=page_key)
        except Exception:
            pass
        return original_plotly_chart(fig_or_data, *args, **kwargs)

    st.plotly_chart = _patched_plotly_chart  # type: ignore[assignment]
    st.session_state[patch_key] = True
    st.markdown('<div class="godpick-bi-note">📊 BI Pro 圖表視覺已啟用｜v135</div>', unsafe_allow_html=True)


__all__ = ["BI_VERSION", "install_bi_dashboard_style", "inject_bi_css"]
