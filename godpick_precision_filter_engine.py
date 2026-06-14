# -*- coding: utf-8 -*-
"""Phase 6.4 precision output filter.

目的：讓使用者在 7_股神推薦設定的「推薦總分下限」與「輸出 Top N」
真正套用到最後畫面、匯出與寫入流程，避免完整推薦表一次塞入大量
弱勢觀察、正式排除、低流動性或回放診斷股票，被誤解成正式推薦。

本模組只處理 DataFrame 欄位，不讀寫 JSON、不連網、不覆蓋既有欄位。
"""
from __future__ import annotations

from typing import Any
import pandas as pd

PRECISION_FILTER_VERSION = "vnext_phase6_4_precision_topn_20260614"

PRECISION_FILTER_COLUMNS = [
    "精選TopN排名",
    "精選輸出分區",
    "精選輸出資格",
    "精選輸出原因",
    "使用者推薦總分下限",
    "使用者輸出TopN",
    "精選輸出版本",
]

NUMERIC_PRECISION_FILTER_COLUMNS = {
    "精選TopN排名",
    "使用者推薦總分下限",
    "使用者輸出TopN",
}

_BLANK_TEXTS = {"", "none", "nan", "nat", "null", "--", "-", "<na>"}


def _safe_str(v: Any) -> str:
    try:
        if v is None:
            return ""
        if pd.isna(v):
            return ""
    except Exception:
        pass
    s = str(v).strip()
    return "" if s.lower() in _BLANK_TEXTS else s


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return float(default)
        x = pd.to_numeric(v, errors="coerce")
        if pd.isna(x):
            return float(default)
        return float(x)
    except Exception:
        return float(default)


def _num(row: pd.Series, col: str, default: float = 0.0) -> float:
    return _safe_float(row.get(col), default)


def _text_blob(row: pd.Series, cols: list[str]) -> str:
    return "｜".join(_safe_str(row.get(c)) for c in cols if c in row.index)


def _contains_any(text: str, keys: list[str]) -> bool:
    return any(k in text for k in keys)


def _primary_score(row: pd.Series) -> float:
    """使用者設定的「推薦總分下限」應優先對推薦總分生效。"""
    if "推薦總分" in row.index:
        return _num(row, "推薦總分", 0.0)
    # 舊資料缺推薦總分時才使用備援欄位。
    return max(
        _num(row, "候選強度分", 0.0),
        _num(row, "推薦分數", 0.0),
        _num(row, "股神決策分數", 0.0),
        _num(row, "股神實戰總分", 0.0),
    )


def _is_blocked(row: pd.Series) -> tuple[bool, str]:
    bucket = _text_blob(row, ["正式推薦分區", "主流作戰分區", "飆股雷達分區", "回放校正分區", "下週作戰分區"])
    role = _text_blob(row, ["推薦角色", "穩健推薦角色", "飆股雷達角色", "領漲回補角色", "主流資金角色"])
    qual = _text_blob(row, ["正式推薦資格", "實戰過濾狀態", "今日決策結論"])
    formal_exclude = _text_blob(row, ["正式推薦排除原因", "真禁買原因"])
    cold_liq = _text_blob(row, ["冷門股警示", "主流作戰分區", "成交額等級"])
    blob = "｜".join([bucket, role, qual, formal_exclude, cold_liq])

    # 真正排除類型：正式排除、低流動性、假強、D 禁買。
    # 注意：盤中雷達常會保留 RR/停損距離不足等警示，這些是「不可直接買」原因，
    # 不應該把它從精選雷達中完全刪除。
    if _contains_any(bucket, ["正式排除清單", "低流動性排除", "禁止買進排除", "假強排除", "X｜假強排除", "冷門禁追"]):
        return True, "排除/低流動性/假強類型不進精選TopN"
    if _contains_any(role, ["D｜過熱禁買", "過熱禁買", "X｜假強排除"]):
        return True, "過熱禁買或假強排除不進精選TopN"
    if _contains_any(qual, ["BLOCK｜不列推薦", "BLOCK"]):
        return True, "正式資格為BLOCK不進精選TopN"
    if _contains_any(cold_liq, ["低流動性排除", "冷門禁追", "成交額不足"]):
        return True, "低流動性或成交額不足不進精選TopN"
    if _contains_any(formal_exclude, ["禁買", "假強", "低流動性", "成交額不足"]):
        return True, "存在正式禁買/假強/低流動性原因"
    return False, ""


def _tier(row: pd.Series, include_risk_radar: bool) -> tuple[int, str, str]:
    bucket = _text_blob(row, ["正式推薦分區", "主流作戰分區", "下週作戰分區"])
    role = _text_blob(row, ["推薦角色", "飆股雷達角色", "領漲回補角色", "回放校正角色", "主流資金角色"])
    radar_bucket = _safe_str(row.get("飆股雷達分區"))
    leader_bucket = _safe_str(row.get("領漲回補分區"))
    if "正式下週主推薦" in bucket:
        return 0, "正式下週主推薦", "通過正式推薦分區"
    if "主流攻擊候選" in bucket:
        return 1, "主流攻擊候選", "主流資金與攻擊分區優先"
    # 若正式推薦淨化已經判成高風險，尊重正式分區，不再因底層 B+/S 雷達角色升回盤中追蹤。
    if include_risk_radar and ("高風險雷達觀察" in bucket or "高風險爆發觀察" in radar_bucket):
        return 4, "高風險雷達觀察", "只保留為雷達，不可直接買"
    if "盤中雷達追蹤" in bucket or _contains_any(role, ["B+｜盤中點火追蹤", "S｜飆股攻擊候選", "S+｜漲停雷達"]):
        return 2, "盤中雷達追蹤", "盤中觸發前不可買，放量突破才進攻"
    if "主流突破追蹤" in bucket or _contains_any(role, ["B｜等突破確認", "L｜主流強勢回補", "L+｜領漲回補雷達"]):
        return 3, "主流突破追蹤", "主流/領漲回補追蹤，等待突破確認"
    if include_risk_radar and _contains_any(role, ["R｜高風險爆發觀察", "T｜題材轉強追蹤", "M｜強勢漏選追蹤"]):
        return 4, "高風險雷達觀察", "只保留為雷達，不可直接買"
    if _contains_any(role, ["C+｜早期潛伏", "早期潛伏"]):
        return 5, "早期潛伏觀察", "未達正式推薦，只能小量觀察"
    if "題材轉強追蹤" in leader_bucket:
        return 5, "題材轉強追蹤", "題材轉強但仍需買點修復"
    return 99, "非精選", "不屬於正式/雷達精選分區"


def _sort_frame(work: pd.DataFrame) -> pd.DataFrame:
    if work.empty:
        return work
    sort_specs = [
        ("_precision_tier", True),
        ("正式推薦排序分", False),
        ("可操作分", False),
        ("爆發雷達分", False),
        ("隔日爆發分", False),
        ("主流領漲回補分", False),
        ("漲停回放分", False),
        ("主流資金分", False),
        ("族群攻擊強度", False),
        ("成交額百萬", False),
        ("推薦總分", False),
    ]
    for col, _ in sort_specs:
        if col not in work.columns:
            work[col] = 0 if col != "_precision_tier" else 99
        if col != "_precision_tier":
            work[col] = pd.to_numeric(work[col], errors="coerce").fillna(0)
    sort_cols = [c for c, _ in sort_specs]
    ascending = [a for _, a in sort_specs]
    return work.sort_values(sort_cols, ascending=ascending, kind="mergesort").reset_index(drop=True)


def apply_precision_topn_filter(
    df: pd.DataFrame | None,
    min_total_score: float = 0.0,
    top_n: int = 10,
    include_risk_radar: bool = True,
) -> pd.DataFrame:
    """套用使用者門檻與輸出 Top N。

    - 推薦總分低於使用者設定者不輸出。
    - 正式排除、低流動性、過熱禁買、假強排除不輸出。
    - Top N 是最多 N 檔，不硬湊滿 N 檔。
    """
    if df is None:
        return pd.DataFrame(columns=PRECISION_FILTER_COLUMNS)
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df)
    if out.empty:
        for c in PRECISION_FILTER_COLUMNS:
            if c not in out.columns:
                out[c] = pd.Series(dtype="float64" if c in NUMERIC_PRECISION_FILTER_COLUMNS else "object")
        return out

    try:
        n = int(top_n)
    except Exception:
        n = 10
    n = max(1, min(n, 50))
    score_floor = float(_safe_float(min_total_score, 0.0))

    rows = []
    keep_index = []
    for idx, row in out.iterrows():
        score = _primary_score(row)
        blocked, blocked_reason = _is_blocked(row)
        tier, bucket, tier_reason = _tier(row, include_risk_radar=include_risk_radar)
        reasons = []
        if score < score_floor:
            reasons.append(f"推薦總分{score:.1f}低於使用者下限{score_floor:.1f}")
        if blocked:
            reasons.append(blocked_reason)
        if tier >= 90:
            reasons.append(tier_reason)
        eligible = not reasons
        rows.append({
            "_precision_tier": tier,
            "_precision_primary_score": score,
            "精選輸出分區": bucket if eligible else "未納入精選TopN",
            "精選輸出資格": "PASS｜納入精選" if eligible else "DROP｜不輸出",
            "精選輸出原因": tier_reason if eligible else "、".join(reasons),
            "使用者推薦總分下限": score_floor,
            "使用者輸出TopN": n,
            "精選輸出版本": PRECISION_FILTER_VERSION,
        })
        if eligible:
            keep_index.append(idx)
    meta = pd.DataFrame(rows, index=out.index)
    for c in meta.columns:
        out[c] = meta[c]

    selected = out.loc[keep_index].copy()
    if selected.empty:
        return selected.reset_index(drop=True)
    selected = _sort_frame(selected).head(n).copy()
    selected["精選TopN排名"] = range(1, len(selected) + 1)
    # 清掉內部排序欄位，避免顯示/匯出污染。
    selected = selected.drop(columns=[c for c in selected.columns if str(c).startswith("_precision_")], errors="ignore")
    return selected.reset_index(drop=True)


def annotate_precision_filter_status(
    df: pd.DataFrame | None,
    min_total_score: float = 0.0,
    top_n: int = 10,
    include_risk_radar: bool = True,
) -> pd.DataFrame:
    """只標註狀態，不截斷；供診斷頁需要時使用。"""
    if df is None:
        return pd.DataFrame(columns=PRECISION_FILTER_COLUMNS)
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df)
    if out.empty:
        return out
    # 重用主流程後，把沒選到的列也標註成 DROP。
    filtered = apply_precision_topn_filter(out, min_total_score, top_n, include_risk_radar)
    selected_codes = set(filtered.get("股票代號", pd.Series(dtype="object")).astype(str).tolist()) if not filtered.empty else set()
    annotated = out.copy()
    try:
        full_status = []
        for _, row in annotated.iterrows():
            code = _safe_str(row.get("股票代號"))
            if code in selected_codes:
                full_status.append("PASS｜納入精選")
            else:
                score = _primary_score(row)
                blocked, blocked_reason = _is_blocked(row)
                tier, _, tier_reason = _tier(row, include_risk_radar)
                reasons = []
                if score < float(_safe_float(min_total_score, 0.0)):
                    reasons.append("未達推薦總分下限")
                if blocked:
                    reasons.append(blocked_reason)
                if tier >= 90:
                    reasons.append(tier_reason)
                full_status.append("DROP｜" + ("、".join(reasons) if reasons else "超出TopN排序"))
        annotated["精選輸出資格"] = full_status
        annotated["使用者推薦總分下限"] = float(_safe_float(min_total_score, 0.0))
        annotated["使用者輸出TopN"] = int(max(1, min(int(top_n or 10), 50)))
        annotated["精選輸出版本"] = PRECISION_FILTER_VERSION
    except Exception:
        pass
    return annotated
