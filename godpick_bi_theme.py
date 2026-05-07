# -*- coding: utf-8 -*-
"""GodPick BI Theme v133

共用 BI 視覺規範：只注入 CSS / Plotly 預設樣式，不改資料來源、不改推薦邏輯。
"""
from __future__ import annotations

from typing import Any, Iterable
import streamlit as st

BI_VERSION = "v133 BI Core"

BI_COLORS = {
    "bg": "#f8fafc",
    "panel": "rgba(255,255,255,0.94)",
    "ink": "#0f172a",
    "muted": "#64748b",
    "line": "rgba(148,163,184,0.28)",
    "blue": "#2563eb",
    "cyan": "#0891b2",
    "green": "#16a34a",
    "red": "#dc2626",
    "amber": "#d97706",
    "purple": "#7c3aed",
}


def configure_plotly_bi_theme() -> None:
    """設定 Plotly 全域 template，讓三個核心模組圖表視覺一致。"""
    try:
        import plotly.io as pio
        import plotly.graph_objects as go

        template = go.layout.Template(
            layout=go.Layout(
                font=dict(family="Noto Sans TC, Microsoft JhengHei, Arial, sans-serif", color=BI_COLORS["ink"], size=13),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="#ffffff",
                colorway=["#2563eb", "#ef4444", "#16a34a", "#f97316", "#14b8a6", "#8b5cf6", "#64748b"],
                hoverlabel=dict(
                    bgcolor="rgba(255,255,255,0.96)",
                    bordercolor="rgba(37,99,235,0.28)",
                    font=dict(color=BI_COLORS["ink"], size=13),
                    namelength=-1,
                ),
                legend=dict(
                    bgcolor="rgba(255,255,255,0.72)",
                    bordercolor="rgba(148,163,184,0.24)",
                    borderwidth=1,
                    font=dict(size=12),
                ),
                xaxis=dict(
                    showgrid=False,
                    linecolor="rgba(148,163,184,0.35)",
                    tickfont=dict(color="#64748b", size=12),
                    titlefont=dict(color="#475569", size=12),
                    zeroline=False,
                ),
                yaxis=dict(
                    gridcolor="rgba(148,163,184,0.16)",
                    linecolor="rgba(148,163,184,0.35)",
                    tickfont=dict(color="#64748b", size=12),
                    titlefont=dict(color="#475569", size=12),
                    zeroline=False,
                ),
                margin=dict(l=28, r=28, t=58, b=34),
            )
        )
        pio.templates["godpick_bi_v133"] = template
        pio.templates.default = "godpick_bi_v133"
    except Exception:
        pass


def install_bi_plotly_patch() -> None:
    """輕量包裝 st.plotly_chart：只套用視覺 template，不改資料。"""
    try:
        if getattr(st, "_godpick_bi_plotly_patched", False):
            return
        _orig_plotly_chart = st.plotly_chart

        def _wrapped_plotly_chart(fig_or_data=None, *args, **kwargs):
            try:
                if fig_or_data is not None and hasattr(fig_or_data, "update_layout"):
                    fig_or_data = enhance_plotly_figure(fig_or_data)
            except Exception:
                pass
            return _orig_plotly_chart(fig_or_data, *args, **kwargs)

        st.plotly_chart = _wrapped_plotly_chart
        st._godpick_bi_plotly_patched = True
    except Exception:
        pass


def inject_bi_theme() -> None:
    configure_plotly_bi_theme()
    install_bi_plotly_patch()
    st.markdown(
        """
        <style>
        :root{
          --bi-bg:#f8fafc; --bi-panel:#ffffff; --bi-ink:#0f172a; --bi-muted:#64748b;
          --bi-line:rgba(148,163,184,.28); --bi-blue:#2563eb; --bi-green:#16a34a;
          --bi-red:#dc2626; --bi-amber:#d97706; --bi-purple:#7c3aed;
        }
        .block-container{padding-top:1.25rem !important; max-width:1540px !important;}
        .bi-hero{
          background:linear-gradient(135deg,#0f172a 0%,#1e293b 48%,#0f766e 100%);
          border:1px solid rgba(148,163,184,.25); border-radius:22px; padding:20px 24px;
          color:white; box-shadow:0 18px 42px rgba(15,23,42,.18); margin:8px 0 18px 0;
        }
        .bi-hero-title{font-size:22px;font-weight:900;letter-spacing:.02em;margin-bottom:5px;}
        .bi-hero-sub{font-size:13px;color:rgba(255,255,255,.78);line-height:1.6;}
        .bi-chip-row{display:flex;gap:8px;flex-wrap:wrap;margin-top:13px;}
        .bi-chip{font-size:12px;font-weight:800;color:#dbeafe;background:rgba(37,99,235,.22);border:1px solid rgba(147,197,253,.28);padding:5px 10px;border-radius:999px;}
        .bi-panel{
          background:rgba(255,255,255,.94); border:1px solid var(--bi-line); border-radius:18px;
          box-shadow:0 10px 28px rgba(15,23,42,.06); padding:14px 16px; margin:10px 0;
        }
        .bi-section-title{font-size:18px;font-weight:900;color:var(--bi-ink);margin:8px 0 10px 0;}
        div[data-testid="stMetric"]{
          background:linear-gradient(180deg,#ffffff 0%,#f8fafc 100%); border:1px solid rgba(148,163,184,.22);
          border-radius:18px; padding:12px 14px; box-shadow:0 8px 20px rgba(15,23,42,.045);
        }
        div[data-testid="stMetric"] label{color:#475569 !important;font-weight:800 !important;}
        div[data-testid="stMetricValue"]{color:#0f172a !important;font-weight:900 !important;}
        div[data-testid="stMetricDelta"]{font-weight:800 !important;}
        div[data-testid="stDataFrame"], div[data-testid="stTable"]{
          border:1px solid rgba(148,163,184,.22); border-radius:16px; overflow:hidden; box-shadow:0 8px 22px rgba(15,23,42,.04);
        }
        .stPlotlyChart{
          background:rgba(255,255,255,.94); border:1px solid rgba(148,163,184,.20); border-radius:18px;
          padding:8px; box-shadow:0 10px 26px rgba(15,23,42,.055);
        }
        .stTabs [data-baseweb="tab-list"]{gap:8px;border-bottom:1px solid rgba(148,163,184,.22);}
        .stTabs [data-baseweb="tab"]{border-radius:999px 999px 0 0;font-weight:800;}
        .stButton > button{border-radius:14px !important;font-weight:850 !important;}
        .stDownloadButton > button{border-radius:14px !important;font-weight:850 !important;}
        div[data-testid="stExpander"]{border-radius:16px !important;border-color:rgba(148,163,184,.25) !important;}
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_bi_banner(title: str, subtitle: str = "", chips: Iterable[str] | None = None) -> None:
    chips_html = "".join([f'<span class="bi-chip">{c}</span>' for c in (chips or [])])
    st.markdown(
        f"""
        <div class="bi-hero">
          <div class="bi-hero-title">{title}</div>
          <div class="bi-hero-sub">{subtitle}</div>
          <div class="bi-chip-row">{chips_html}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_bi_note(title: str, body: str, tone: str = "blue") -> None:
    color = {
        "blue": "#2563eb", "green": "#16a34a", "amber": "#d97706", "red": "#dc2626", "purple": "#7c3aed"
    }.get(tone, "#2563eb")
    st.markdown(
        f"""
        <div class="bi-panel" style="border-left:5px solid {color};">
          <div style="font-size:15px;font-weight:900;color:#0f172a;margin-bottom:4px;">{title}</div>
          <div style="font-size:13px;color:#475569;line-height:1.65;">{body}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def enhance_plotly_figure(fig: Any, *, height: int | None = None, title: str | None = None) -> Any:
    """對現有 Plotly fig 做安全美化；失敗時回傳原圖。"""
    try:
        if title:
            fig.update_layout(title=dict(text=title, x=0.01, xanchor="left", font=dict(size=18, color="#0f172a")))
        fig.update_layout(
            template="godpick_bi_v133",
            hovermode="x unified",
            hoverlabel=dict(bgcolor="rgba(255,255,255,0.96)", bordercolor="rgba(37,99,235,0.28)", font=dict(color="#0f172a", size=13), namelength=-1),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="#ffffff",
            legend=dict(bgcolor="rgba(255,255,255,0.74)", bordercolor="rgba(148,163,184,0.22)", borderwidth=1),
        )
        if height:
            fig.update_layout(height=height)
        fig.update_xaxes(showgrid=False, linecolor="rgba(148,163,184,0.35)")
        fig.update_yaxes(gridcolor="rgba(148,163,184,0.16)", linecolor="rgba(148,163,184,0.35)")
    except Exception:
        pass
    return fig
