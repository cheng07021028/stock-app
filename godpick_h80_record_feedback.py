"""H80 recommendation-record feedback loop helpers.

Pure dataframe helpers only.  This module deliberately does not decide whether a
stock is a Formal buy.  It converts H79 *research recommendation* output into a
separate Page08 learning sample so the ranking model can be evaluated without
polluting Formal/A- trading performance.
"""
from __future__ import annotations

from typing import Iterable, Mapping, Any

import pandas as pd

VERSION = "v191_h82_record_feedback_adaptive_evidence_20260921"
RESEARCH_MODE = "股神校正研究"
RESEARCH_LEVEL = "H79研究推薦"
RESEARCH_SAMPLE_TYPE = "B｜H79研究推薦校正研究樣本"
RESEARCH_SAMPLE_WEIGHT = 0.45

# These columns are deliberately persisted in godpick_records.json.  Page08 can
# keep showing its existing columns, while the authority record retains enough
# evidence for later performance review and model calibration.
H80_RECORD_FEEDBACK_COLUMNS = [
    "紀錄來源", "自動記錄", "紀錄層級",
    "校正樣本類型", "校正樣本用途", "校正樣本權重",
    "是否納入正式推薦績效", "是否納入權重校正", "個股資料品質",
    "樣本可信度", "校正樣本建立版本",
    "H79決策層級", "H79推薦狀態", "H79自適應機會分", "H79絕對品質分",
    "H79橫截面排名分", "H79確認模型分", "H79強度百分位%", "H79族群百分位%",
    "H79推薦理由", "H79交易狀態", "H79計畫進場", "H79結構停損",
    "H79第一目標", "H79成本後RR", "H79停損距離%", "H79價格上限試算",
    "H79有效增量", "H79缺資料", "H79未入選原因", "H79原始K線日",
    "H79資料基準日", "H79日期正規化", "H79決策指紋",
    "H81版本", "H81市場定價理解分", "H81技術多週期分", "H81新聞事件影響分",
    "H81歷史回測可信分", "H81風險管理分", "H81交易日誌回饋分", "H81交易計畫完整分",
    "H81專業研究總分", "H81資料覆蓋%", "H81排名加減分", "H81研究排序分", "H81三大利多催化",
    "H81三大風險", "H81下一步關注", "H81多頭情境", "H81中性情境", "H81空頭情境",
    "H81研究摘要", "H81盤前檢查", "H81開盤策略", "H81盤中調整", "H81收盤檢討", "H81設定版本",
    "H82版本", "H82市場環境", "H82成熟樣本數", "H82有效樣本權重", "H82學習信心%",
    "H82市場環境加減分", "H82產業加減分", "H82決策狀態加減分", "H82H81分桶加減分",
    "H82錯誤治理加減分", "H82影子建議加減分", "H82自適應加減分", "H82自適應研究排序分",
    "H82主要學習依據", "H82主要錯誤風險", "H82學習摘要", "H82學習狀態", "H82設定版本",
]


def _code(value: Any) -> str:
    text = "" if value is None else str(value).strip()
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]
    return text.zfill(4) if text.isdigit() and len(text) < 4 else text


def _text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def build_research_tracking_frame(
    source_df: pd.DataFrame | None,
    research_df: pd.DataFrame | None,
    excluded_codes: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Build full-fidelity Page08 research-learning rows from H79 output.

    H79 research tables are intentionally compact.  We therefore start from the
    full decision-source row (when available) and overlay H79 evidence onto it.
    Formal/actionable codes are excluded to avoid double-learning the same signal.
    """
    research = research_df.copy() if isinstance(research_df, pd.DataFrame) else pd.DataFrame()
    if research.empty or "股票代號" not in research.columns:
        return pd.DataFrame()

    research["股票代號"] = research["股票代號"].map(_code)
    research = research[research["股票代號"].ne("")].copy()
    if "H79決策層級" in research.columns:
        research = research[research["H79決策層級"].fillna("").astype(str).eq("研究推薦")].copy()
    if research.empty:
        return pd.DataFrame()

    excluded = {_code(x) for x in (excluded_codes or []) if _code(x)}
    if excluded:
        research = research[~research["股票代號"].isin(excluded)].copy()
    research = research.drop_duplicates(subset=["股票代號"], keep="first")
    if research.empty:
        return pd.DataFrame()

    source = source_df.copy() if isinstance(source_df, pd.DataFrame) else pd.DataFrame()
    source_by_code: dict[str, dict[str, Any]] = {}
    if not source.empty and "股票代號" in source.columns:
        source["__h80_code"] = source["股票代號"].map(_code)
        for _, row in source[source["__h80_code"].ne("")].drop_duplicates("__h80_code", keep="first").iterrows():
            payload = row.drop(labels=["__h80_code"], errors="ignore").to_dict()
            source_by_code[_code(payload.get("股票代號"))] = payload

    rows: list[dict[str, Any]] = []
    for _, h79_row in research.iterrows():
        code = _code(h79_row.get("股票代號"))
        raw = dict(source_by_code.get(code, {}))
        raw.update(h79_row.to_dict())
        raw["股票代號"] = code

        # Page08 research samples have their own mode/business-key and never
        # masquerade as Formal/A- executable recommendations.
        raw["推薦模式"] = RESEARCH_MODE
        raw["推薦用途"] = "H79研究推薦績效追蹤（非買進許可）"
        raw["紀錄來源"] = "07_股神推薦｜H79研究推薦自動同步"
        raw["自動記錄"] = "是"
        raw["紀錄層級"] = RESEARCH_LEVEL
        raw["目前狀態"] = "研究推薦追蹤"
        raw["是否可直接買進"] = "否"
        raw["建議動作"] = "研究追蹤；等待H79交易狀態改善後重算，不視為買進許可。"
        raw["校正樣本類型"] = RESEARCH_SAMPLE_TYPE
        raw["校正樣本用途"] = "研究排序、T+1/T+3/T+5績效與失效條件校正；不得計入正式交易勝率"
        raw["校正樣本權重"] = RESEARCH_SAMPLE_WEIGHT
        raw["是否納入正式推薦績效"] = "否"
        raw["是否納入權重校正"] = "是"
        raw["個股資料品質"] = _text(raw.get("個股資料品質")) or "可追蹤"
        raw["樣本可信度"] = _text(raw.get("樣本可信度")) or "中"
        raw["校正樣本建立版本"] = VERSION

        # Keep the signal-time benchmark price trackable even if the compact
        # H79 table is used without its original full candidate row.
        if not _text(raw.get("推薦價格")):
            raw["推薦價格"] = raw.get("最新價") if _text(raw.get("最新價")) else raw.get("H79計畫進場")
        if not _text(raw.get("最新價")):
            raw["最新價"] = raw.get("推薦價格")
        if not _text(raw.get("停損價")):
            raw["停損價"] = raw.get("H79結構停損")
        if not _text(raw.get("賣出目標1")):
            raw["賣出目標1"] = raw.get("H79第一目標")

        note = _text(raw.get("備註"))
        governance_note = "H82研究學習樣本：保留H79/H80/H81/H82證據，只做成熟績效/排序校正，不代表正式買進許可。"
        raw["備註"] = f"{note}；{governance_note}" if note and governance_note not in note else (note or governance_note)
        rows.append(raw)

    out = pd.DataFrame(rows)
    if not out.empty:
        out["__h80_order"] = out["股票代號"].map({c: i for i, c in enumerate(research["股票代號"].tolist())})
        out = out.sort_values("__h80_order", kind="stable").drop(columns=["__h80_order"], errors="ignore")
    return out.reset_index(drop=True)


def mark_research_record_rows(rows: Iterable[Mapping[str, Any]] | None) -> list[dict[str, Any]]:
    """Re-assert research governance after Page07's generic record builder."""
    result: list[dict[str, Any]] = []
    for row in rows or []:
        raw = dict(row)
        raw["推薦模式"] = RESEARCH_MODE
        raw["紀錄來源"] = "07_股神推薦｜H79研究推薦自動同步"
        raw["自動記錄"] = "是"
        raw["紀錄層級"] = RESEARCH_LEVEL
        raw["目前狀態"] = "研究推薦追蹤"
        raw["是否可直接買進"] = "否"
        raw["校正樣本類型"] = RESEARCH_SAMPLE_TYPE
        raw["校正樣本用途"] = "研究排序、T+1/T+3/T+5績效與失效條件校正；不得計入正式交易勝率"
        raw["校正樣本權重"] = RESEARCH_SAMPLE_WEIGHT
        raw["是否納入正式推薦績效"] = "否"
        raw["是否納入權重校正"] = "是"
        raw["個股資料品質"] = _text(raw.get("個股資料品質")) or "可追蹤"
        raw["樣本可信度"] = _text(raw.get("樣本可信度")) or "中"
        raw["校正樣本建立版本"] = VERSION
        note = _text(raw.get("備註"))
        governance_note = "H82研究學習樣本：保留H79/H80/H81/H82證據，只做成熟績效/排序校正，不代表正式買進許可。"
        raw["備註"] = f"{note}；{governance_note}" if note and governance_note not in note else (note or governance_note)
        result.append(raw)
    return result
