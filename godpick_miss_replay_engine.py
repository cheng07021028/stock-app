# -*- coding: utf-8 -*-
"""VNext Phase 6.2 漲停股回放校正 / 漏選原因診斷引擎。

設計目的：
- 不連網、不讀寫 JSON、不覆蓋推薦紀錄；只針對既有推薦 DataFrame 補上回放診斷欄位。
- 把「為什麼強勢/漲停股沒有被股神推薦抓到」拆成可追蹤原因：候選池、族群、流動性、風控、Entry/RR、分類與大盤模式。
- 與 Phase 6 的領漲回補 / 飆股雷達並存；不把高風險股直接升成買進，只放入回放校正與盤中重掃清單。
"""
from __future__ import annotations

from typing import Any, Iterable
import math
import re

import pandas as pd

MISS_REPLAY_VERSION = "phase6_2_miss_replay_20260613"

MISS_REPLAY_COLUMNS = [
    "漲停回放分",
    "強勢股漏選風險分",
    "候選池覆蓋診斷",
    "漲停漏選原因",
    "漏選原因分類",
    "漏選修正動作",
    "回放校正角色",
    "回放校正分區",
    "回放校正版本",
]

NUMERIC_MISS_REPLAY_COLUMNS = {
    "漲停回放分",
    "強勢股漏選風險分",
}

_BLANKS = {"", "nan", "none", "null", "nat", "--", "-", "<na>"}

# 只放族群關鍵字，不硬背個股，避免下次換族群時變成固定答案。
_HOT_THEME_PATTERNS: dict[str, list[str]] = {
    "記憶體/半導體": ["記憶體", "dram", "nand", "flash", "半導體", "ic設計", "晶圓", "矽晶圓", "封測", "測試", "探針", "設備", "材料", "晶片", "先進封裝"],
    "PCB/載板/電子零組件": ["pcb", "載板", "軟板", "銅箔", "電子零組件", "被動元件", "電容", "電阻", "連接器", "導線架", "hdI"],
    "光電/面板/光通訊": ["光電", "面板", "光學", "光通訊", "光纖", "光模組", "矽光子", "cpo"],
    "AI/伺服器/散熱": ["ai", "伺服器", "散熱", "水冷", "機殼", "電源", "高速傳輸"],
    "重電/電纜/材料": ["重電", "電纜", "電器電纜", "變壓器", "電力", "材料", "化工", "塑化"],
    "綠能/太陽能": ["綠能", "太陽能", "電池", "儲能", "再生能源"],
}


def _blank(v: Any) -> bool:
    try:
        if v is None or pd.isna(v):
            return True
    except Exception:
        pass
    return str(v).strip().lower() in _BLANKS


def _safe_str(v: Any) -> str:
    return "" if _blank(v) else str(v).strip()


def _num(df: pd.DataFrame, names: Iterable[str], default: float = 0.0) -> pd.Series:
    out = pd.Series([float("nan")] * len(df), index=df.index, dtype="float64")
    for name in names:
        if name not in df.columns:
            continue
        s = pd.to_numeric(df[name], errors="coerce")
        mask = out.isna() & s.notna()
        if mask.any():
            out.loc[mask] = s.loc[mask]
    return out.fillna(default).astype(float)


def _text_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([""] * len(df), index=df.index, dtype="object")
    return df[col].fillna("").astype(str).str.strip()


def _text_blob(df: pd.DataFrame) -> pd.Series:
    cols = [
        "股票名稱", "類別", "產業", "族群名稱", "資金流熱門族群", "推薦型態", "機會型態", "型態名稱",
        "族群攻擊說明", "資金攻擊摘要", "攻擊候選原因", "飆股雷達原因", "錯失原因診斷", "今日大盤結論",
        "股神同步說明", "推薦理由", "推薦原因", "推薦理由摘要", "股神推論", "股神推論邏輯",
    ]
    s = pd.Series([""] * len(df), index=df.index, dtype="object")
    for c in cols:
        if c in df.columns:
            s = s + "｜" + df[c].fillna("").astype(str)
    return s.str.lower()


def _theme_hit_score(blob: pd.Series) -> tuple[pd.Series, pd.Series]:
    score = pd.Series([34.0] * len(blob), index=blob.index, dtype="float64")
    label = pd.Series([""] * len(blob), index=blob.index, dtype="object")
    for theme, words in _HOT_THEME_PATTERNS.items():
        pat = "|".join(re.escape(w.lower()) for w in words)
        hit = blob.str.contains(pat, na=False, regex=True)
        score.loc[hit] = score.loc[hit].clip(lower=78)
        label.loc[hit & label.eq("")] = theme
        multi = hit & label.ne("") & ~label.str.contains(re.escape(theme), na=False, regex=True)
        label.loc[multi] = label.loc[multi] + "+" + theme
    return score.clip(0, 100), label.replace("", "未命中主流強勢族群")


def _liquidity_score(amount_m: pd.Series, volume_k: pd.Series) -> pd.Series:
    amount_m = pd.to_numeric(amount_m, errors="coerce").fillna(0.0)
    volume_k = pd.to_numeric(volume_k, errors="coerce").fillna(0.0)
    s = pd.Series([35.0] * len(amount_m), index=amount_m.index, dtype="float64")
    s = s.mask(amount_m >= 80, 48)
    s = s.mask(amount_m >= 200, 60)
    s = s.mask(amount_m >= 500, 72)
    s = s.mask(amount_m >= 1000, 82)
    s = s.mask(amount_m >= 3000, 92)
    # 成交額缺漏時，用成交量當次要參考，避免整列都被當成無量。
    s = s.mask((amount_m <= 0) & (volume_k >= 1000), 55)
    s = s.mask((amount_m <= 0) & (volume_k >= 3000), 67)
    return s.clip(0, 100)


def _gate_flags(out: pd.DataFrame) -> dict[str, pd.Series]:
    role = _text_series(out, "推薦角色")
    radar_role = _text_series(out, "飆股雷達角色")
    leader_role = _text_series(out, "領漲回補角色")
    sync_bucket = _text_series(out, "股神同步分區")
    main_bucket = _text_series(out, "主流作戰分區")
    deny_text = (
        _text_series(out, "硬否決原因") + "｜" + _text_series(out, "真禁買原因") + "｜" + _text_series(out, "主推薦降級原因") + "｜" +
        _text_series(out, "冷門股警示") + "｜" + _text_series(out, "限制原因")
    )
    denied = deny_text.str.contains("停損|風險報酬|RR|追價|追高|過熱|禁買|硬否決", na=False)
    liquidity_block = deny_text.str.contains("低流動性|冷門|成交額不足|成交量不足", na=False) | main_bucket.str.contains("低流動性|冷門", na=False)
    weak_or_blocked = role.str.contains(r"C-|D｜|禁買|弱勢", na=False) | sync_bucket.str.contains("弱勢|禁止|低流動性", na=False)
    already_radar = radar_role.str.contains(r"S\+｜|S｜|B\+｜|R｜", na=False) | leader_role.str.contains(r"L\+｜|L｜|T｜", na=False)
    return {
        "denied": denied,
        "liquidity_block": liquidity_block,
        "weak_or_blocked": weak_or_blocked,
        "already_radar": already_radar,
    }


def apply_godpick_miss_replay_engine(df: pd.DataFrame | None, strong_df: pd.DataFrame | None = None) -> pd.DataFrame:
    """補上漏選回放診斷欄位。

    strong_df 保留給未來由排行榜/漲停榜傳入真實強勢股清單時做覆蓋率比較；目前不強制需要。
    """
    if df is None:
        return pd.DataFrame(columns=MISS_REPLAY_COLUMNS)
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df)
    out = out.loc[:, ~pd.Index(out.columns).duplicated()].copy()
    if out.empty:
        for c in MISS_REPLAY_COLUMNS:
            out[c] = pd.Series(dtype="float64" if c in NUMERIC_MISS_REPLAY_COLUMNS else "object")
        return out

    blob = _text_blob(out)
    theme_s, theme_label = _theme_hit_score(blob)
    amount = _num(out, ["成交額百萬", "今日成交額百萬", "成交金額百萬", "20日均成交額百萬"], 0)
    volume_k = _num(out, ["最新成交量_張", "最新成交量張", "成交量", "成交量_張"], 0)
    liquidity_s = _liquidity_score(amount, volume_k)
    sector_s = _num(out, ["族群攻擊強度", "族群輪動分", "族群續航力", "族群資金流分數", "類股熱度分數"], 50).clip(0, 100)
    radar_s = _num(out, ["爆發雷達分", "飆股攻擊分", "隔日爆發分", "隔日大漲機率分"], 50).clip(0, 100)
    leader_s = _num(out, ["主流領漲回補分", "市場領漲相似分", "漲停族群相似度"], 50).clip(0, 100)
    money_s = _num(out, ["主流資金分", "資金攻擊有效分", "籌碼續航分", "主力點火分", "法人攻擊分"], 50).clip(0, 100)
    entry_s = _num(out, ["Entry進場買點分", "進場買點分", "買進分數", "交易可行分數"], 50).clip(0, 100)
    risk_s = _num(out, ["Risk風控安全分", "風控安全分", "交易可行分數"], 50).clip(0, 100)
    rr = _num(out, ["風險報酬比", "RR"], 0)
    stop_pct = _num(out, ["停損距離%", "最大風險%", "單檔風險%"], 0)
    ret5 = _num(out, ["近5日漲幅%", "5日漲幅%", "RET5"], 0)

    flags = _gate_flags(out)
    # 漲停回放分：不是買進分，而是「如果隔日真正強勢榜出現，這列是否應被回頭檢查」。
    replay_score = (
        theme_s * 0.20
        + sector_s * 0.18
        + radar_s * 0.18
        + leader_s * 0.16
        + liquidity_s * 0.12
        + money_s * 0.10
        + entry_s * 0.03
        + risk_s * 0.03
    ).clip(0, 100)
    # 過熱且完全無族群/成交額者降低，避免又回到亂抓冷門股。
    replay_score = replay_score.where(~((ret5 > 18) & (theme_s < 65) & (liquidity_s < 60)), replay_score - 12).clip(0, 100)
    miss_risk = replay_score.copy()
    miss_risk = miss_risk + flags["weak_or_blocked"].astype(float) * 8 + flags["denied"].astype(float) * 6
    miss_risk = miss_risk + ((entry_s < 55) | (risk_s < 55) | ((rr > 0) & (rr < 0.7)) | (stop_pct > 12)).astype(float) * 5
    miss_risk = miss_risk.clip(0, 100)

    # 如果 strong_df 有傳入，補候選池覆蓋狀態；否則只做候選內自我診斷。
    strong_codes: set[str] = set()
    if isinstance(strong_df, pd.DataFrame) and not strong_df.empty:
        for c in ["股票代號", "code", "symbol", "股票"]:
            if c in strong_df.columns:
                strong_codes = set(strong_df[c].astype(str).str.replace(".0", "", regex=False).str.strip())
                break
    codes = _text_series(out, "股票代號").str.replace(".0", "", regex=False)

    coverage: list[str] = []
    reasons: list[str] = []
    classes: list[str] = []
    actions: list[str] = []
    roles: list[str] = []
    buckets: list[str] = []
    for idx in out.index:
        sc = float(replay_score.loc[idx]); mr = float(miss_risk.loc[idx]); th = float(theme_s.loc[idx]); liq = float(liquidity_s.loc[idx])
        sec = float(sector_s.loc[idx]); rad = float(radar_s.loc[idx]); lead = float(leader_s.loc[idx]); ent = float(entry_s.loc[idx]); risk = float(risk_s.loc[idx])
        rr_i = float(rr.loc[idx]); stop_i = float(stop_pct.loc[idx]); code = str(codes.loc[idx])
        in_strong = bool(strong_codes and code in strong_codes)
        already = bool(flags["already_radar"].loc[idx])
        denied = bool(flags["denied"].loc[idx])
        low_liq = bool(flags["liquidity_block"].loc[idx]) or liq < 48
        weak = bool(flags["weak_or_blocked"].loc[idx])

        if in_strong:
            coverage.append("已在真實強勢/漲停榜，需檢查前一日是否進候選池。")
        elif strong_codes:
            coverage.append("未在本次真實強勢榜。")
        else:
            coverage.append("未傳入真實強勢股清單；依候選內分數做漏選風險診斷。")

        reason_parts: list[str] = []
        class_i = "一般回放"
        if th >= 76 and sec >= 60:
            reason_parts.append(f"主流題材/族群同步：{theme_label.loc[idx]}，族群{sec:.1f}")
        elif th >= 65:
            reason_parts.append(f"題材轉強但族群同步不足：{theme_label.loc[idx]}，族群{sec:.1f}")
        else:
            reason_parts.append(f"未明顯命中主流強勢族群：{theme_label.loc[idx]}")
        if low_liq:
            reason_parts.append(f"流動性不足或冷門股，成交額分{liq:.1f}")
            class_i = "流動性錯殺/隔離"
        if denied:
            reason_parts.append("被硬否決/風控條件降級")
            class_i = "風控錯殺風險" if class_i == "一般回放" else class_i
        if ent < 55 or risk < 55 or ((rr_i > 0) and rr_i < 0.7) or stop_i > 12:
            reason_parts.append(f"Entry/Risk/RR 不足：Entry{ent:.1f}/Risk{risk:.1f}/RR{rr_i:.2f}/停損{stop_i:.1f}%")
            if class_i == "一般回放":
                class_i = "Entry/RR過早刪除"
        if rad >= 72 or lead >= 72:
            reason_parts.append(f"雷達/領漲回補仍偏強：雷達{rad:.1f}/回補{lead:.1f}")
            if class_i == "一般回放":
                class_i = "雷達已抓到但未升級"
        if weak and sc >= 68:
            reason_parts.append("候選被歸為弱勢/禁買，但具回放分，需盤前重掃")

        if sc >= 82 and th >= 72 and liq >= 60:
            role_i = "M+｜漲停漏選回放"
            bucket_i = "漏選回放校正"
            action_i = "下次同型態不可直接刪除；盤前重掃族群、成交額與突破價，必要時升為 S/B+/L。"
        elif sc >= 72 and (th >= 65 or rad >= 72 or lead >= 72):
            role_i = "M｜強勢漏選追蹤"
            bucket_i = "漏選原因診斷"
            action_i = "保留在回放追蹤池；若隔日同族群漲停家數增加或放量突破，升級到飆股雷達。"
        elif already:
            role_i = "K｜已納入雷達"
            bucket_i = "已覆蓋雷達"
            action_i = "已被 Phase 5/6 雷達覆蓋，後續用績效檢查是否真強或假強。"
        elif low_liq and sc < 78:
            role_i = "Q｜冷門低流動回放"
            bucket_i = "低流動性回放"
            action_i = "可觀察但不得進主流攻擊；除非成交額放大到主流門檻。"
        else:
            role_i = "N｜一般回放觀察"
            bucket_i = "一般回放"
            action_i = "目前不需升級；等待實際強勢榜或績效資料驗證。"

        reasons.append("；".join(reason_parts))
        classes.append(class_i)
        actions.append(action_i)
        roles.append(role_i)
        buckets.append(bucket_i)

    out["漲停回放分"] = replay_score.round(1)
    out["強勢股漏選風險分"] = miss_risk.round(1)
    out["候選池覆蓋診斷"] = coverage
    out["漲停漏選原因"] = reasons
    out["漏選原因分類"] = classes
    out["漏選修正動作"] = actions
    out["回放校正角色"] = roles
    out["回放校正分區"] = buckets
    out["回放校正版本"] = MISS_REPLAY_VERSION
    return out


def build_miss_replay_summary(df: pd.DataFrame | None) -> dict[str, Any]:
    work = apply_godpick_miss_replay_engine(df)
    if work.empty:
        return {"version": MISS_REPLAY_VERSION, "rows": 0, "bucket_counts": {}, "class_counts": {}, "top": []}
    bucket_counts = work.get("回放校正分區", pd.Series(dtype="object")).astype(str).value_counts().to_dict()
    class_counts = work.get("漏選原因分類", pd.Series(dtype="object")).astype(str).value_counts().to_dict()
    score = pd.to_numeric(work.get("漲停回放分"), errors="coerce").fillna(0)
    top_df = work.assign(_score=score).sort_values("_score", ascending=False).head(10)
    top_cols = [c for c in ["股票代號", "股票名稱", "類別", "漲停回放分", "強勢股漏選風險分", "回放校正角色", "漏選原因分類"] if c in top_df.columns]
    return {
        "version": MISS_REPLAY_VERSION,
        "rows": int(len(work)),
        "bucket_counts": {str(k): int(v) for k, v in bucket_counts.items()},
        "class_counts": {str(k): int(v) for k, v in class_counts.items()},
        "top": top_df[top_cols].to_dict("records") if top_cols else [],
    }
