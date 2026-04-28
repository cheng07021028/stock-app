# -*- coding: utf-8 -*-
from __future__ import annotations

"""
macro_mode_bridge.py
用途：
1. 讀取 0_大盤走勢.py 產生的 macro_trend_records.json
2. 找出最近最準 / 最近一次最佳的大盤模式
3. 回傳給 7_股神推薦.py 使用，讓個股推薦分數可動態跟隨大盤模式
"""

from datetime import datetime, timedelta
from typing import Any
import base64
import json
from pathlib import Path

import pandas as pd
import requests
import streamlit as st


MACRO_RECORD_COLUMNS = [
    "record_id", "推估日期", "建立時間", "更新時間", "模式名稱", "市場情境", "推估方向", "方向強度",
    "是否適合進場", "是否適合續抱", "是否適合減碼", "是否適合出場", "建議動作", "股神模式分數", "股神信心度",
    "預估漲跌點", "預估高點", "預估低點", "預估區間寬度", "風險等級", "國際新聞風險分", "美股因子分", "夜盤因子分",
    "技術面因子分", "籌碼面因子分", "事件因子分", "結構面因子分", "風險面因子分", "大盤基準點", "加權收盤",
    "加權漲跌%", "成交量估分", "VIX", "美元台幣", "NASDAQ漲跌%", "SOX漲跌%", "SP500漲跌%", "台積電ADR漲跌%",
    "ES夜盤漲跌%", "NQ夜盤漲跌%", "外資買賣超估分", "期貨選擇權估分", "類股輪動估分", "外資買賣超(億)",
    "三大法人合計(億)", "外資期貨淨單", "PCR", "融資增減(億)", "融券增減張", "強勢族群", "弱勢族群",
    "重大事件清單", "因子來源狀態", "加權資料日期", "美股資料日期", "夜盤資料日期", "法人資料日期",
    "期權資料日期", "融資券資料日期", "新聞資料區間", "股神推論邏輯", "進場確認條件", "出場警訊", "主要風險",
    "建議倉位", "實際方向", "實際漲跌點", "實際高點", "實際低點", "方向是否命中", "區間是否命中", "點數誤差",
    "建議動作是否合適", "進場建議績效分", "出場建議績效分", "整體檢討分", "誤判主因類別", "誤判主因",
    "收盤檢討", "備註",
]

DEFAULT_MODE_WEIGHTS = {
    "股神平衡版": {"tech": 0.24, "chip": 0.20, "trend": 0.20, "value": 0.10, "event": 0.10, "risk": 0.08, "momentum": 0.08},
    "技術面優先": {"tech": 0.34, "chip": 0.16, "trend": 0.18, "value": 0.08, "event": 0.08, "risk": 0.06, "momentum": 0.10},
    "美夜盤優先": {"tech": 0.16, "chip": 0.18, "trend": 0.28, "value": 0.08, "event": 0.08, "risk": 0.08, "momentum": 0.14},
    "新聞風險優先": {"tech": 0.14, "chip": 0.16, "trend": 0.16, "value": 0.08, "event": 0.20, "risk": 0.18, "momentum": 0.08},
    "籌碼事件優先": {"tech": 0.16, "chip": 0.28, "trend": 0.16, "value": 0.08, "event": 0.16, "risk": 0.08, "momentum": 0.08},
}

def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()

def _safe_float(v: Any, default: float | None = None) -> float | None:
    try:
        if pd.isna(v):
            return default
    except Exception:
        pass
    try:
        return float(v)
    except Exception:
        return default

def _ensure_macro_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=MACRO_RECORD_COLUMNS)
    out = df.copy()
    for c in MACRO_RECORD_COLUMNS:
        if c not in out.columns:
            out[c] = None
    for c in ["股神模式分數", "股神信心度", "預估漲跌點", "國際新聞風險分", "美股因子分", "夜盤因子分",
              "技術面因子分", "籌碼面因子分", "事件因子分", "結構面因子分", "風險面因子分", "實際漲跌點", "點數誤差",
              "進場建議績效分", "出場建議績效分", "整體檢討分"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    for c in ["方向是否命中", "區間是否命中", "建議動作是否合適"]:
        out[c] = out[c].fillna(False).map(lambda x: str(x).lower() in {"1", "true", "yes", "y", "是"})
    return out[MACRO_RECORD_COLUMNS].copy()

def _github_cfg() -> dict[str, str]:
    return {
        "token": _safe_str(st.secrets.get("GITHUB_TOKEN", "")),
        "owner": _safe_str(st.secrets.get("GITHUB_REPO_OWNER", "cheng07021028")),
        "repo": _safe_str(st.secrets.get("GITHUB_REPO_NAME", "stock-app")),
        "branch": _safe_str(st.secrets.get("GITHUB_REPO_BRANCH", "main")) or "main",
        "path": _safe_str(st.secrets.get("MACRO_TREND_RECORDS_GITHUB_PATH", "macro_trend_records.json")) or "macro_trend_records.json",
    }

def _github_url(owner: str, repo: str, path: str) -> str:
    return f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"

def _headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _local_macro_path() -> Path:
    """回傳 macro_trend_records.json 的本機路徑，優先使用專案根目錄。"""
    try:
        return Path(__file__).resolve().parent / "macro_trend_records.json"
    except Exception:
        return Path("macro_trend_records.json")


def _load_macro_records_local() -> pd.DataFrame:
    """讀取本機大盤紀錄。GitHub 未設定或失敗時不可讓股神推薦失去大盤參考。"""
    path = _local_macro_path()
    if not path.exists():
        return pd.DataFrame(columns=MACRO_RECORD_COLUMNS)

    try:
        text = path.read_text(encoding="utf-8-sig").strip()
        if not text:
            return pd.DataFrame(columns=MACRO_RECORD_COLUMNS)

        payload = json.loads(text)
        if isinstance(payload, dict):
            # 相容 {"records": [...]} 或 {"data": [...]} 格式
            for key in ("records", "data", "items"):
                if isinstance(payload.get(key), list):
                    payload = payload[key]
                    break

        if not isinstance(payload, list):
            return pd.DataFrame(columns=MACRO_RECORD_COLUMNS)

        return _ensure_macro_columns(pd.DataFrame(payload))
    except Exception:
        return pd.DataFrame(columns=MACRO_RECORD_COLUMNS)


def _load_macro_records_github() -> pd.DataFrame:
    """讀取 GitHub 大盤紀錄；失敗只回空表，不拋錯拖垮頁面。"""
    cfg = _github_cfg()
    token = cfg["token"]
    if not token:
        return pd.DataFrame(columns=MACRO_RECORD_COLUMNS)

    try:
        r = requests.get(
            _github_url(cfg["owner"], cfg["repo"], cfg["path"]),
            headers=_headers(token),
            params={"ref": cfg["branch"]},
            timeout=12,
        )
        if r.status_code != 200:
            return pd.DataFrame(columns=MACRO_RECORD_COLUMNS)

        content = ((r.json() or {}).get("content") or "").strip()
        if not content:
            return pd.DataFrame(columns=MACRO_RECORD_COLUMNS)

        payload = json.loads(base64.b64decode(content).decode("utf-8"))
        if isinstance(payload, dict):
            for key in ("records", "data", "items"):
                if isinstance(payload.get(key), list):
                    payload = payload[key]
                    break

        if not isinstance(payload, list):
            return pd.DataFrame(columns=MACRO_RECORD_COLUMNS)

        return _ensure_macro_columns(pd.DataFrame(payload))
    except Exception:
        return pd.DataFrame(columns=MACRO_RECORD_COLUMNS)


def _merge_macro_record_sources(local_df: pd.DataFrame, github_df: pd.DataFrame) -> pd.DataFrame:
    """合併本機與 GitHub 大盤紀錄，以 record_id 去重；本機最新資料優先保留。"""
    frames = []
    if github_df is not None and not github_df.empty:
        frames.append(_ensure_macro_columns(github_df))
    if local_df is not None and not local_df.empty:
        frames.append(_ensure_macro_columns(local_df))

    if not frames:
        return pd.DataFrame(columns=MACRO_RECORD_COLUMNS)

    out = pd.concat(frames, ignore_index=True)
    if "record_id" in out.columns:
        out["_rid"] = out["record_id"].map(lambda x: _safe_str(x))
        no_id = out["_rid"].eq("")
        if no_id.any():
            out.loc[no_id, "_rid"] = (
                out.loc[no_id, "推估日期"].astype(str) + "|" +
                out.loc[no_id, "模式名稱"].astype(str) + "|" +
                out.loc[no_id, "建立時間"].astype(str)
            )
        out = out.drop_duplicates("_rid", keep="last").drop(columns=["_rid"], errors="ignore")

    return _ensure_macro_columns(out)


@st.cache_data(ttl=300, show_spinner=False)
def load_macro_records() -> pd.DataFrame:
    """
    讀取大盤模式紀錄。

    v5 修正重點：
    1. 不再強制依賴 GitHub Token。
    2. GitHub 未設定、API 逾時或失敗時，會讀取本機 macro_trend_records.json。
    3. 同時有 GitHub 與本機時會合併，避免舊雲端資料蓋掉本機最新資料。
    4. 回傳空表但不拋錯，避免 7_股神推薦.py 因大盤模組失敗而整頁卡死。
    """
    local_df = _load_macro_records_local()
    github_df = _load_macro_records_github()
    return _merge_macro_record_sources(local_df, github_df)


def _build_mode_scoreboard(df: pd.DataFrame) -> pd.DataFrame:
    x = _ensure_macro_columns(df)
    if x.empty:
        return pd.DataFrame(columns=["模式名稱", "樣本數", "方向命中率", "區間命中率", "平均點數誤差", "平均檢討分", "綜合分"])
    g = x.groupby("模式名稱", dropna=False).agg(
        樣本數=("record_id", "count"),
        方向命中率=("方向是否命中", lambda s: float(pd.Series(s).fillna(False).mean() * 100)),
        區間命中率=("區間是否命中", lambda s: float(pd.Series(s).fillna(False).mean() * 100)),
        平均點數誤差=("點數誤差", "mean"),
        平均檢討分=("整體檢討分", "mean"),
    ).reset_index()
    g["平均點數誤差"] = pd.to_numeric(g["平均點數誤差"], errors="coerce")
    g["平均檢討分"] = pd.to_numeric(g["平均檢討分"], errors="coerce")
    g["綜合分"] = (
        g["方向命中率"].fillna(0) * 0.42
        + g["區間命中率"].fillna(0) * 0.18
        + g["平均檢討分"].fillna(0) * 0.25
        + (120 - g["平均點數誤差"].fillna(120).clip(upper=120)) * 0.15
    )
    return g.sort_values(["綜合分", "方向命中率"], ascending=[False, False]).reset_index(drop=True)

def get_macro_best_mode(lookback_days: int = 30) -> dict[str, Any]:
    df = load_macro_records()
    if df.empty:
        return {
            "mode_name": "股神平衡版",
            "scoreboard": pd.DataFrame(),
            "reason": "無大盤歷史紀錄，使用預設模式",
            "source": "default",
        }
    df["推估日期_dt"] = pd.to_datetime(df["推估日期"], errors="coerce")
    cutoff = pd.Timestamp.today().normalize() - pd.Timedelta(days=max(3, int(lookback_days)))
    recent = df[df["推估日期_dt"] >= cutoff].copy()
    eval_df = recent if len(recent) >= 10 else df.copy()
    scoreboard = _build_mode_scoreboard(eval_df)
    if scoreboard.empty:
        return {
            "mode_name": "股神平衡版",
            "scoreboard": pd.DataFrame(),
            "reason": "大盤紀錄不足，使用預設模式",
            "source": "default",
        }
    best = scoreboard.iloc[0]
    return {
        "mode_name": _safe_str(best["模式名稱"]) or "股神平衡版",
        "scoreboard": scoreboard,
        "reason": f"近{lookback_days}日以綜合分最高模式為主",
        "source": "macro_trend_records",
        "hit_rate": _safe_float(best["方向命中率"], 0.0) or 0.0,
        "point_error": _safe_float(best["平均點數誤差"], 999.0) or 999.0,
        "review_score": _safe_float(best["平均檢討分"], 0.0) or 0.0,
    }

def get_latest_macro_pick() -> dict[str, Any]:
    df = load_macro_records()
    if df.empty:
        return {}
    x = df.copy()
    x["推估日期_dt"] = pd.to_datetime(x["推估日期"], errors="coerce")
    x["更新時間_dt"] = pd.to_datetime(x["更新時間"], errors="coerce")
    x = x.sort_values(["推估日期_dt", "更新時間_dt", "股神模式分數", "股神信心度"], ascending=[False, False, False, False])
    latest_date = x["推估日期_dt"].dropna().max()
    if pd.isna(latest_date):
        return {}
    day_df = x[x["推估日期_dt"] == latest_date].copy()
    if day_df.empty:
        return {}
    top = day_df.sort_values(["股神模式分數", "股神信心度"], ascending=[False, False]).iloc[0]
    return top.to_dict()

def get_macro_dynamic_weights() -> dict[str, float]:
    best = get_macro_best_mode()
    mode_name = _safe_str(best.get("mode_name")) or "股神平衡版"
    return DEFAULT_MODE_WEIGHTS.get(mode_name, DEFAULT_MODE_WEIGHTS["股神平衡版"]).copy()

def get_macro_bias_adjustment(lookback_days: int = 45) -> dict[str, float]:
    df = load_macro_records()
    if df.empty:
        return {"scale": 1.0, "bias": 0.0, "confidence_bonus": 0.0}
    df["推估日期_dt"] = pd.to_datetime(df["推估日期"], errors="coerce")
    cutoff = pd.Timestamp.today().normalize() - pd.Timedelta(days=max(5, int(lookback_days)))
    recent = df[df["推估日期_dt"] >= cutoff].copy()
    use_df = recent if len(recent) >= 12 else df.copy()
    use_df = use_df.dropna(subset=["預估漲跌點", "實際漲跌點"]).copy()
    if use_df.empty:
        return {"scale": 1.0, "bias": 0.0, "confidence_bonus": 0.0}
    pred_abs = pd.to_numeric(use_df["預估漲跌點"], errors="coerce").abs().replace(0, pd.NA)
    act_abs = pd.to_numeric(use_df["實際漲跌點"], errors="coerce").abs()
    ratio = (act_abs / pred_abs).dropna()
    scale = float(ratio.median()) if not ratio.empty else 1.0
    scale = max(0.55, min(1.25, scale))
    signed_gap = pd.to_numeric(use_df["實際漲跌點"], errors="coerce") - pd.to_numeric(use_df["預估漲跌點"], errors="coerce")
    bias = float(signed_gap.median()) if not signed_gap.dropna().empty else 0.0
    bias = max(-80.0, min(80.0, bias))
    direction_hit = float(pd.Series(use_df["方向是否命中"]).fillna(False).mean() * 100)
    confidence_bonus = 0.0
    if direction_hit >= 68:
        confidence_bonus = 6.0
    elif direction_hit >= 58:
        confidence_bonus = 3.0
    elif direction_hit <= 42:
        confidence_bonus = -5.0
    return {"scale": round(scale, 4), "bias": round(bias, 2), "confidence_bonus": round(confidence_bonus, 2)}

def apply_macro_mode_to_stock_score(
    base_score: float,
    factor_scores: dict[str, float],
    lookback_days: int = 30,
) -> dict[str, Any]:
    """
    factor_scores 預期鍵值：
    tech / chip / trend / value / event / risk / momentum
    """
    weights = get_macro_dynamic_weights()
    adj = get_macro_bias_adjustment()
    weighted = 0.0
    details = {}
    for k, w in weights.items():
        v = _safe_float(factor_scores.get(k), 0.0) or 0.0
        part = v * w
        weighted += part
        details[k] = {"score": round(v, 4), "weight": round(w, 4), "contrib": round(part, 4)}
    final_score = base_score * 0.35 + weighted * 100 * 0.65
    final_score = final_score * adj["scale"] + adj["bias"] * 0.12
    best = get_macro_best_mode(lookback_days=lookback_days)
    return {
        "best_mode_name": best.get("mode_name", "股神平衡版"),
        "best_mode_reason": best.get("reason", ""),
        "macro_adjustment": adj,
        "factor_details": details,
        "macro_weighted_score": round(weighted * 100, 2),
        "final_score": round(final_score, 2),
    }

def render_macro_mode_hint():
    best = get_macro_best_mode()
    latest = get_latest_macro_pick()
    st.caption(
        f"大盤最準模式：{_safe_str(best.get('mode_name')) or '股神平衡版'}"
        f"｜命中率 {(_safe_float(best.get('hit_rate'), 0.0) or 0.0):.1f}%"
        f"｜最近最佳方向：{_safe_str(latest.get('推估方向')) or '-'}"
        f"｜動作：{_safe_str(latest.get('建議動作')) or '-'}"
    )
