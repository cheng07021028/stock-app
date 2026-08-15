# -*- coding: utf-8 -*-
"""V191-H31 concise SuperAI guide for Page07 Excel exports.

H31 fixes two H30 design errors:
1) the guide must refine the *official ranked universe* (股神推薦總排名), not start
   a second ranking from the 1,700+ candidate-diagnosis universe;
2) prohibited / excluded / high-risk rows can never be relabeled as 條件候選.

The guide is therefore a concise reading layer, not a competing recommendation
engine.  Formal/A- authority always stays with the official partition + permit.
"""
from __future__ import annotations

from typing import Any
import pandas as pd

VERSION = "v191_h31_super_ai_excel_guide_rank_alignment_20260815"

_BLANK = {"", "none", "nan", "nat", "null", "--", "-", "<na>"}
_FORBIDDEN_KEYS = (
    "禁止買進", "禁止新倉", "禁止碰", "不可直接買", "正式排除", "排除清單",
    "高風險雷達", "禁止操作", "BLOCK", "LOCKDOWN",
)
_WAIT_KEYS = ("WAIT", "待確認", "未對齊", "待同步", "只觀察", "觀察")


def _blank(v: Any) -> bool:
    try:
        if v is None or pd.isna(v):
            return True
    except Exception:
        pass
    return str(v).strip().lower() in _BLANK


def _num_series(df: pd.DataFrame, names: list[str], default: float = 0.0) -> pd.Series:
    out = pd.Series([float("nan")] * len(df), index=df.index, dtype="float64")
    for name in names:
        if name not in df.columns:
            continue
        obj = df[name]
        frames = [obj.iloc[:, j] for j in range(obj.shape[1] - 1, -1, -1)] if isinstance(obj, pd.DataFrame) else [obj]
        for raw in frames:
            s = pd.to_numeric(raw, errors="coerce")
            mask = out.isna() & s.notna()
            if mask.any():
                out.loc[mask] = s.loc[mask]
    return out.fillna(float(default)).astype(float)


def _text_series(df: pd.DataFrame, names: list[str], default: str = "") -> pd.Series:
    out = pd.Series([default] * len(df), index=df.index, dtype="object")
    for name in names:
        if name not in df.columns:
            continue
        obj = df[name]
        frames = [obj.iloc[:, j] for j in range(obj.shape[1] - 1, -1, -1)] if isinstance(obj, pd.DataFrame) else [obj]
        for raw in frames:
            s = raw.fillna("").astype(str).str.strip()
            mask = out.map(_blank) & s.map(lambda x: not _blank(x))
            if mask.any():
                out.loc[mask] = s.loc[mask]
    return out


def _contains_any(text: pd.Series, keys: tuple[str, ...]) -> pd.Series:
    s = text.fillna("").astype(str)
    mask = pd.Series(False, index=s.index)
    for key in keys:
        mask |= s.str.contains(key, regex=False)
    return mask


def _safe_ratio_score(rr: pd.Series) -> pd.Series:
    return (rr.clip(lower=0, upper=3) / 3.0 * 100.0).clip(0, 100)


def _official_rank(df: pd.DataFrame) -> pd.Series:
    rank = _num_series(df, ["股神推薦總排名", "攻略順位"], 9999.0)
    # If a source has no official rank column, it is not the H31 authoritative
    # universe.  Keep a stable fallback order but mark it clearly downstream.
    if (rank >= 9999).all():
        rank = pd.Series(range(1, len(df) + 1), index=df.index, dtype="float64")
    return rank


def build_super_ai_excel_guide(df: pd.DataFrame | None, *, max_rows: int = 20) -> pd.DataFrame:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame({"狀態": ["本輪沒有可建立超級AI精選攻略的正式排名資料。"], "攻略版本": [VERSION]})

    work = df.copy()
    code = _text_series(work, ["股票代號", "代號"])
    name = _text_series(work, ["股票名稱", "名稱"])
    market = _text_series(work, ["市場別"])
    category = _text_series(work, ["類別", "產業類別"])
    partition = _text_series(work, ["正式推薦分區", "正式推薦等級"])
    permit = _text_series(work, ["操作許可", "V188交易許可", "建議動作"])
    v188_permit = _text_series(work, ["V188交易許可", "操作許可"])
    is_formal_text = _text_series(work, ["是否正式推薦", "V188正式推薦資格"])
    official_rank = _official_rank(work)
    official_priority = _num_series(work, ["V188股神作戰優先分", "股神推薦優先分"], 0.0)

    entry = _num_series(work, ["Entry進場買點分", "Entry進場分", "進場時機分數", "買進分數"])
    risk = _num_series(work, ["Risk風控安全分", "Risk風控分", "風控安全分"])
    rr = _num_series(work, ["路徑風險報酬比", "SuperAI執行風報比", "實戰風險報酬比", "風險報酬比", "風險報酬比_決策"])
    executable = _num_series(work, ["進場可執行分", "交易可行分數", "交易可行分"])
    distance = _num_series(work, ["距最近可執行買點%", "觸發距離%"], 99.0)
    chase = _num_series(work, ["追價風險分", "追高風險分數_決策", "追價風險分數"], 55.0)
    trade = _num_series(work, ["SuperAI Trade分", "SuperAI交易分"])
    final_ai = _num_series(work, ["SuperAI 最終決策分", "SuperAI最終決策分", "V188股神作戰優先分"])
    next_up = _num_series(work, ["H32隔日上漲機率%", "SuperAI校準後隔日上漲機率%", "模型隔日上漲機率%", "隔日上漲機率%", "上漲機率%"], 50.0)
    h32_t1 = _num_series(work, ["H32隔日預估漲跌幅%"], 0.0)
    h32_t1_low = _num_series(work, ["H32隔日90%區間下緣%"], 0.0)
    h32_t1_high = _num_series(work, ["H32隔日90%區間上緣%"], 0.0)
    h32_swing = _num_series(work, ["H32後續波段預估漲幅%", "H32_10日預估報酬%"], 0.0)
    h32_10_low = _num_series(work, ["H32_10日90%區間下緣%"], 0.0)
    h32_10_high = _num_series(work, ["H32_10日90%區間上緣%"], 0.0)
    h32_validation = _text_series(work, ["H32預測驗證狀態"], "未驗證｜不得宣稱90%準確")
    sector = _num_series(work, ["族群攻擊強度", "族群攻擊分", "族群輪動分", "類股熱度分數"])
    mainstream = _num_series(work, ["主流資金分"])

    kline = _text_series(work, ["K線資料新鮮度", "K線最後交易日"])
    official = _text_series(work, ["官方因子新鮮度", "官方因子資料狀態", "官方資料日期"])
    market_state = _text_series(work, ["大盤資料新鮮度", "大盤風控層級", "大盤橋接狀態", "V188市場對齊治理"])
    block = _text_series(work, ["正式與A近門檻說明", "正式推薦排除原因", "進場阻擋原因", "推薦漏斗阻擋主因", "V188市場對齊治理"])
    action = _text_series(work, ["正式推薦動作", "建議動作", "進場路徑", "主要進場路徑", "最終操作結論"])

    rr_score = _safe_ratio_score(rr)
    distance_penalty = pd.Series(0.0, index=work.index)
    distance_penalty.loc[distance > 8] = 10.0
    distance_penalty.loc[(distance > 6) & (distance <= 8)] = 5.0
    guide_score = (
        entry * 0.15 + risk * 0.15 + rr_score * 0.15 + executable * 0.15
        + trade * 0.10 + final_ai * 0.10 + next_up * 0.05 + sector * 0.05
        + mainstream * 0.05 + (100.0 - chase.clip(0, 100)) * 0.05
        - distance_penalty
    ).clip(0, 100).round(1)

    combined = (partition + "｜" + permit + "｜" + v188_permit + "｜" + is_formal_text).fillna("")
    forbidden = _contains_any(combined, _FORBIDDEN_KEYS)
    wait_only = _contains_any(combined, _WAIT_KEYS)
    formal = (~forbidden) & (
        partition.str.contains("正式下週主推薦", regex=False)
        | is_formal_text.str.contains("是", regex=False)
    )
    a_minus = (~forbidden) & (~formal) & partition.str.contains("A-", regex=False)
    executable_permit = (~forbidden) & (~wait_only) & permit.str.contains("可操作|條件進場|允許|小量試單", regex=True)

    # H31: role is descriptive only.  Forbidden rows can never become 條件候選.
    role = pd.Series("觀察雷達", index=work.index, dtype="object")
    role.loc[forbidden] = "禁止/排除"
    role.loc[executable_permit] = "條件候選"
    role.loc[a_minus] = "A-準主推薦"
    role.loc[formal] = "正式推薦"
    role.loc[(~forbidden) & (~formal) & (~a_minus) & wait_only] = "等待確認/雷達"

    role_order = {"正式推薦": 0, "A-準主推薦": 1, "條件候選": 2, "等待確認/雷達": 3, "觀察雷達": 4, "禁止/排除": 9}
    tier = role.map(role_order).fillna(8).astype(int)

    selected = pd.DataFrame(index=work.index)
    selected["_tier"] = tier
    selected["_official_rank"] = official_rank
    selected["攻略順位"] = 0
    selected["超級AI定位"] = role
    selected["原股神總排名"] = official_rank.astype(int)
    selected["V188作戰優先分"] = official_priority.round(1)
    selected["超級AI攻略分"] = guide_score
    selected["股票代號"] = code
    selected["股票名稱"] = name
    selected["市場別"] = market
    selected["類別"] = category
    selected["正式推薦分區"] = partition.where(partition.ne(""), "未升級")
    selected["操作許可"] = permit.where(permit.ne(""), "等待條件")
    selected["V188交易許可"] = v188_permit.where(v188_permit.ne(""), "等待確認")
    selected["Entry"] = entry.round(1)
    selected["Risk"] = risk.round(1)
    selected["RR"] = rr.round(2)
    selected["進場可執行分"] = executable.round(1)
    selected["距最近買點%"] = distance.where(distance < 90, float("nan")).round(2)
    selected["追價風險"] = chase.round(1)
    selected["SuperAI Trade"] = trade.round(1)
    selected["SuperAI決策"] = final_ai.round(1)
    selected["隔日上漲機率%"] = next_up.round(1)
    selected["隔日預估漲跌幅%"] = h32_t1.round(2)
    selected["隔日90%區間"] = [f"{lo:+.2f}% ~ {hi:+.2f}%" for lo, hi in zip(h32_t1_low, h32_t1_high)]
    selected["後續波段預估漲幅%"] = h32_swing.round(2)
    selected["10日90%區間"] = [f"{lo:+.2f}% ~ {hi:+.2f}%" for lo, hi in zip(h32_10_low, h32_10_high)]
    selected["報酬預測驗證"] = h32_validation
    selected["族群攻擊"] = sector.round(1)
    selected["主流資金"] = mainstream.round(1)
    selected["K線/資料狀態"] = kline
    selected["官方因子狀態"] = official
    selected["大盤/V188狀態"] = market_state.where(market_state.ne(""), v188_permit)
    selected["主要阻擋/近門檻"] = block.where(block.ne(""), "—")
    selected["操作原則"] = action.where(action.ne(""), "未觸發前不買；只在正式操作許可成立後操作")

    selected = selected.loc[code.ne("")].copy()
    # 精選攻略不應把正式禁止/排除股票放進前20；它們留在原始總排名/排除表查閱。
    eligible = selected.loc[selected["超級AI定位"] != "禁止/排除"].copy()
    if eligible.empty:
        return pd.DataFrame({
            "狀態": ["本輪正式排名內沒有可列入精選攻略的非禁止候選；請以正式分區/操作許可為準。"],
            "攻略版本": [VERSION],
        })

    # H31 alignment rule: official status first, then preserve original GodPick rank.
    # 攻略分只提供解讀，不另起一套排序模型與總排名競爭。
    eligible = eligible.sort_values(["_tier", "_official_rank"], ascending=[True, True], kind="mergesort").head(max(1, int(max_rows)))
    eligible["攻略順位"] = range(1, len(eligible) + 1)
    eligible["攻略版本"] = VERSION
    return eligible.drop(columns=["_tier", "_official_rank"]).reset_index(drop=True)


__all__ = ["VERSION", "build_super_ai_excel_guide"]
