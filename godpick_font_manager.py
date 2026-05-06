# -*- coding: utf-8 -*-
from __future__ import annotations

import base64
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Tuple

import requests
import streamlit as st

FONT_SETTINGS_FILE = Path(__file__).resolve().parent / "godpick_ui_font_settings.json"
FONT_SETTINGS_VERSION = "v117_global_font_size_home_control"


def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    try:
        return str(v).strip()
    except Exception:
        return ""


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _clamp_int(v: Any, low: int, high: int, default: int) -> int:
    try:
        x = int(float(v))
    except Exception:
        x = default
    return max(low, min(high, x))


def default_font_settings() -> Dict[str, Any]:
    return {
        "version": FONT_SETTINGS_VERSION,
        "updated_at": _now_text(),
        "enabled": True,
        "font_scale_pct": 110,
        "table_font_scale_pct": 105,
        "sidebar_font_scale_pct": 105,
        "button_font_scale_pct": 105,
        "input_font_scale_pct": 105,
        "line_height_pct": 115,
        "compact_table": False,
    }


def normalize_font_settings(data: Any) -> Dict[str, Any]:
    base = default_font_settings()
    if isinstance(data, dict):
        base.update(data)
    base["enabled"] = bool(base.get("enabled", True))
    base["font_scale_pct"] = _clamp_int(base.get("font_scale_pct"), 85, 150, 110)
    base["table_font_scale_pct"] = _clamp_int(base.get("table_font_scale_pct"), 85, 150, 105)
    base["sidebar_font_scale_pct"] = _clamp_int(base.get("sidebar_font_scale_pct"), 85, 150, 105)
    base["button_font_scale_pct"] = _clamp_int(base.get("button_font_scale_pct"), 85, 150, 105)
    base["input_font_scale_pct"] = _clamp_int(base.get("input_font_scale_pct"), 85, 150, 105)
    base["line_height_pct"] = _clamp_int(base.get("line_height_pct"), 100, 170, 115)
    base["compact_table"] = bool(base.get("compact_table", False))
    base["version"] = FONT_SETTINGS_VERSION
    if not _safe_str(base.get("updated_at")):
        base["updated_at"] = _now_text()
    return base


def _github_cfg() -> Dict[str, str]:
    return {
        "token": _safe_str(st.secrets.get("GITHUB_TOKEN", "")),
        "owner": _safe_str(st.secrets.get("GITHUB_REPO_OWNER", "cheng07021028")) or "cheng07021028",
        "repo": _safe_str(st.secrets.get("GITHUB_REPO_NAME", "stock-app")) or "stock-app",
        "branch": _safe_str(st.secrets.get("GITHUB_REPO_BRANCH", "main")) or "main",
        "path": _safe_str(st.secrets.get("GODPICK_FONT_SETTINGS_GITHUB_PATH", "godpick_ui_font_settings.json")) or "godpick_ui_font_settings.json",
    }


def _github_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _github_contents_url(owner: str, repo: str, path: str) -> str:
    return f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"


def _read_github_settings() -> Tuple[Dict[str, Any] | None, str, str]:
    cfg = _github_cfg()
    token = cfg["token"]
    if not token:
        return None, "", "未設定 GITHUB_TOKEN，字體設定只會暫存在本機。"
    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            params={"ref": cfg["branch"]},
            timeout=12,
        )
        if resp.status_code == 404:
            return None, "", f"GitHub 尚未建立 {cfg['path']}，套用後會自動建立。"
        if resp.status_code != 200:
            return None, "", f"讀取 GitHub 字體設定失敗：{resp.status_code}"
        raw = resp.json()
        content = _safe_str(raw.get("content"))
        sha = _safe_str(raw.get("sha"))
        if not content:
            return None, sha, "GitHub 字體設定內容空白。"
        parsed = json.loads(base64.b64decode(content).decode("utf-8"))
        if not isinstance(parsed, dict):
            return None, sha, "GitHub 字體設定格式不是 JSON 物件。"
        return normalize_font_settings(parsed), sha, f"已讀取 GitHub 字體設定：{cfg['path']}"
    except Exception as e:
        return None, "", f"讀取 GitHub 字體設定例外：{e}"


def _write_github_settings(settings: Dict[str, Any]) -> Tuple[bool, str]:
    cfg = _github_cfg()
    token = cfg["token"]
    if not token:
        return False, "未設定 GITHUB_TOKEN，無法永久回寫 GitHub。"
    _, sha, _ = _read_github_settings()
    payload = normalize_font_settings(settings)
    payload["updated_at"] = _now_text()
    content_text = json.dumps(payload, ensure_ascii=False, indent=2)
    body: Dict[str, Any] = {
        "message": f"Update godpick_ui_font_settings.json @ {_now_text()}",
        "content": base64.b64encode(content_text.encode("utf-8")).decode("utf-8"),
        "branch": cfg["branch"],
    }
    if sha:
        body["sha"] = sha
    try:
        resp = requests.put(
            _github_contents_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_github_headers(token),
            json=body,
            timeout=20,
        )
        if resp.status_code in (200, 201):
            return True, f"已永久回寫 GitHub：{cfg['path']}"
        return False, f"GitHub 字體設定寫入失敗：{resp.status_code} / {resp.text[:300]}"
    except Exception as e:
        return False, f"GitHub 字體設定寫入例外：{e}"


def _read_local_settings() -> Tuple[Dict[str, Any] | None, str]:
    try:
        if FONT_SETTINGS_FILE.exists():
            parsed = json.loads(FONT_SETTINGS_FILE.read_text(encoding="utf-8"))
            return normalize_font_settings(parsed), f"已讀取本機 {FONT_SETTINGS_FILE.name}"
    except Exception as e:
        return None, f"讀取本機字體設定失敗：{e}"
    return None, "本機字體設定不存在。"


def _write_local_settings(settings: Dict[str, Any]) -> Tuple[bool, str]:
    try:
        payload = normalize_font_settings(settings)
        payload["updated_at"] = _now_text()
        FONT_SETTINGS_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return True, f"已寫入本機 {FONT_SETTINGS_FILE.name}"
    except Exception as e:
        return False, f"本機字體設定寫入失敗：{e}"


@st.cache_data(ttl=60, show_spinner=False)
def load_font_settings() -> Dict[str, Any]:
    gh_settings, _sha, gh_msg = _read_github_settings()
    if gh_settings is not None:
        st.session_state["godpick_font_settings_source_v117"] = "GitHub"
        st.session_state["godpick_font_settings_message_v117"] = gh_msg
        return gh_settings
    local_settings, local_msg = _read_local_settings()
    if local_settings is not None:
        st.session_state["godpick_font_settings_source_v117"] = "local"
        st.session_state["godpick_font_settings_message_v117"] = f"{gh_msg}；{local_msg}"
        return local_settings
    st.session_state["godpick_font_settings_source_v117"] = "default"
    st.session_state["godpick_font_settings_message_v117"] = f"{gh_msg}；{local_msg}；使用預設字體設定。"
    return default_font_settings()


def save_font_settings(settings: Dict[str, Any]) -> Tuple[bool, str]:
    payload = normalize_font_settings(settings)
    payload["updated_at"] = _now_text()
    local_ok, local_msg = _write_local_settings(payload)
    gh_ok, gh_msg = _write_github_settings(payload)
    try:
        load_font_settings.clear()
    except Exception:
        pass
    if gh_ok:
        return True, f"{gh_msg}；{local_msg}"
    if local_ok:
        return False, f"已暫存本機，但尚未永久同步 GitHub：{gh_msg}；{local_msg}"
    return False, f"儲存失敗：{gh_msg}；{local_msg}"


def inject_global_font_css(settings: Dict[str, Any] | None = None) -> None:
    """所有頁面共用字體放大 CSS；由 app_auth.require_login 每頁自動注入。"""
    try:
        cfg = normalize_font_settings(settings or load_font_settings())
        if not cfg.get("enabled", True):
            return
        main = cfg["font_scale_pct"] / 100.0
        table = cfg["table_font_scale_pct"] / 100.0
        side = cfg["sidebar_font_scale_pct"] / 100.0
        btn = cfg["button_font_scale_pct"] / 100.0
        inp = cfg["input_font_scale_pct"] / 100.0
        line = cfg["line_height_pct"] / 100.0
        table_pad = "0.30rem 0.42rem" if cfg.get("compact_table") else "0.45rem 0.55rem"
        st.markdown(
            f"""
            <style id="godpick-global-font-size-v117">
            :root {{
                --godpick-main-font: {main:.3f};
                --godpick-table-font: {table:.3f};
                --godpick-sidebar-font: {side:.3f};
                --godpick-button-font: {btn:.3f};
                --godpick-input-font: {inp:.3f};
                --godpick-line-height: {line:.3f};
            }}
            html, body, .stApp, .main, [data-testid="stAppViewContainer"] {{
                font-size: calc(16px * var(--godpick-main-font)) !important;
                line-height: var(--godpick-line-height) !important;
            }}
            .stApp p, .stApp li, .stMarkdown, .stText, .stCaption, .stAlert,
            [data-testid="stMarkdownContainer"], [data-testid="stMetricLabel"], [data-testid="stMetricValue"],
            [data-testid="stMetricDelta"], label, legend {{
                font-size: calc(1rem * var(--godpick-main-font)) !important;
                line-height: var(--godpick-line-height) !important;
            }}
            h1, .stMarkdown h1 {{ font-size: calc(2.05rem * var(--godpick-main-font)) !important; line-height: 1.18 !important; }}
            h2, .stMarkdown h2 {{ font-size: calc(1.65rem * var(--godpick-main-font)) !important; line-height: 1.20 !important; }}
            h3, .stMarkdown h3 {{ font-size: calc(1.32rem * var(--godpick-main-font)) !important; line-height: 1.22 !important; }}
            div[data-testid="stSidebar"], div[data-testid="stSidebar"] * {{
                font-size: calc(1rem * var(--godpick-sidebar-font)) !important;
                line-height: var(--godpick-line-height) !important;
            }}
            .stButton button, .stDownloadButton button, button[kind], button, .stTabs button, [role="button"] {{
                font-size: calc(1rem * var(--godpick-button-font)) !important;
                line-height: 1.25 !important;
            }}
            input, textarea, select, .stTextInput input, .stNumberInput input,
            .stDateInput input, .stTimeInput input, .stSelectbox div, .stMultiSelect div {{
                font-size: calc(1rem * var(--godpick-input-font)) !important;
                line-height: 1.35 !important;
            }}
            div[data-testid="stDataFrame"] *, div[data-testid="stDataEditor"] *,
            .stDataFrame *, .stDataEditor *, div[data-testid="stTable"] *, .stTable * {{
                font-size: calc(14px * var(--godpick-table-font)) !important;
                line-height: 1.30 !important;
            }}
            div[data-testid="stDataFrame"] div[role="gridcell"],
            div[data-testid="stDataEditor"] div[role="gridcell"],
            div[data-testid="stDataFrame"] div[role="columnheader"],
            div[data-testid="stDataEditor"] div[role="columnheader"] {{
                padding: {table_pad} !important;
            }}
            .pro-card, .pro-card *, .pro-hero, .pro-hero *, .pro-section-title, .pro-chip {{
                line-height: var(--godpick-line-height) !important;
            }}
            </style>
            """,
            unsafe_allow_html=True,
        )
    except Exception:
        # 字體設定不可影響主功能
        pass


def _init_font_form_state() -> Dict[str, Any]:
    current = load_font_settings()
    for k, v in current.items():
        st.session_state.setdefault(f"font_v117_draft_{k}", v)
    return current


def render_home_font_size_manager() -> None:
    """首頁專用：全系統字體大小管理。採 form，只有按套用才永久生效。"""
    current = _init_font_form_state()
    st.markdown("### 🔠 全系統字體大小")
    st.caption("此設定會套用到所有模組；調整時不立即重整主資料，只有按『套用並永久記錄』才寫入 GitHub / 本機。")

    with st.expander("🔠 字體大小管理｜v117 全模組永久套用", expanded=False):
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("目前總字體", f"{current.get('font_scale_pct', 110)}%")
        c2.metric("表格字體", f"{current.get('table_font_scale_pct', 105)}%")
        c3.metric("側邊欄字體", f"{current.get('sidebar_font_scale_pct', 105)}%")
        c4.metric("啟用狀態", "啟用" if current.get("enabled", True) else "關閉")

        with st.form("godpick_font_size_form_v117"):
            enabled = st.checkbox("啟用全系統字體放大", value=bool(current.get("enabled", True)))
            a1, a2 = st.columns(2)
            with a1:
                font_scale = st.slider("全系統主要字體 %", 85, 150, int(current.get("font_scale_pct", 110)), 1)
                sidebar_scale = st.slider("側邊欄字體 %", 85, 150, int(current.get("sidebar_font_scale_pct", 105)), 1)
                button_scale = st.slider("按鈕字體 %", 85, 150, int(current.get("button_font_scale_pct", 105)), 1)
            with a2:
                table_scale = st.slider("表格字體 %", 85, 150, int(current.get("table_font_scale_pct", 105)), 1)
                input_scale = st.slider("輸入框 / 下拉選單字體 %", 85, 150, int(current.get("input_font_scale_pct", 105)), 1)
                line_height = st.slider("行距 %", 100, 170, int(current.get("line_height_pct", 115)), 1)
            compact_table = st.checkbox("表格緊湊模式", value=bool(current.get("compact_table", False)))

            b1, b2 = st.columns([1, 1])
            apply_btn = b1.form_submit_button("✅ 套用並永久記錄", use_container_width=True, type="primary")
            reset_btn = b2.form_submit_button("↩️ 恢復預設字體", use_container_width=True)

        if apply_btn:
            payload = normalize_font_settings({
                "enabled": enabled,
                "font_scale_pct": font_scale,
                "table_font_scale_pct": table_scale,
                "sidebar_font_scale_pct": sidebar_scale,
                "button_font_scale_pct": button_scale,
                "input_font_scale_pct": input_scale,
                "line_height_pct": line_height,
                "compact_table": compact_table,
            })
            ok, msg = save_font_settings(payload)
            inject_global_font_css(payload)
            if ok:
                st.success(f"字體設定已永久套用：{msg}")
            else:
                st.warning(msg)
            st.rerun()

        if reset_btn:
            payload = default_font_settings()
            ok, msg = save_font_settings(payload)
            inject_global_font_css(payload)
            if ok:
                st.success(f"已恢復預設字體並永久記錄：{msg}")
            else:
                st.warning(msg)
            st.rerun()

        st.info(
            "建議值：主要字體 110～120%、表格 105～115%、側邊欄 105～115%。"
            "此功能由 app_auth 每頁注入 CSS，因此換到 0～15 各模組都會生效。"
        )
        msg = _safe_str(st.session_state.get("godpick_font_settings_message_v117"))
        if msg:
            st.caption(msg)
