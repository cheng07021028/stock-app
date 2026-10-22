# -*- coding: utf-8 -*-
"""H142 sector-fade provenance review. Research-only, fail closed.

An H108 lifecycle string is not evidence of independently verified sector
outflows. Audit exactly which contemporaneous numbers/previous observations
corroborate H109 FADE_VETO. Never changes the original risk/trade authority.
"""
from __future__ import annotations
from datetime import date
from typing import Any, Mapping
import math
import re
import pandas as pd

VERSION = "V191-H142.0"


def _text(v: Any) -> str:
    if v is None: return ""
    s = str(v).strip()
    return "" if s.lower() in ("nan", "nat", "none", "null", "<na>") else s


def _float(v: Any) -> float | None:
    try:
        if isinstance(v, bool): return None
        x = float(_text(v).replace(",", ""))
        return x if math.isfinite(x) else None
    except (TypeError, ValueError): return None


def _day(v: Any) -> date | None:
    try: return date.fromisoformat(_text(v)[:10])
    except (TypeError, ValueError): return None


def _code(v: Any) -> str:
    s = _text(v)
    if s.endswith(".0") and s[:-2].isdigit(): s = s[:-2]
    return s if re.fullmatch(r"[0-9]{4}", s) else ""


def assess_sector_fade(row: Mapping[str, Any]) -> dict[str, Any]:
    """No measured sector data is ever interpreted as a corroborated decline.

    Internal two-day deltas are never labelled official/PIT verified. The
    original H109 hard veto remains in force even with missing corroboration.
    """
    auth = _text(row.get("H109決策權威"))
    phase = _text(row.get("H108族群生命週期"))
    fade = auth == "FADE_VETO" or ("FADE" in phase.upper() or "退潮" in phase)
    flow = _float(row.get("H108族群資金流變化"))
    breadth = _float(row.get("H108族群廣度變化"))
    prev = _day(row.get("H108族群前次資料日"))
    cutoff = _day(row.get("H110研究資訊截止日"))
    has_prior = prev is not None and cutoff is not None and prev < cutoff
    signals = []
    if has_prior:
        if flow is not None and flow <= -10: signals.append("資金流變化≤-10")
        if breadth is not None and breadth <= -8: signals.append("族群廣度變化≤-8")
    if not fade:
        level, why = "NOT_FADE", "本輪無退潮標記；不代表買進合格"
    elif len(signals) == 2:
        level, why = "TWO_INTERNAL_METRICS", "前後日量化訊號支持退潮；尚未獨立驗證來源/交易日"
    elif signals:
        level, why = "SINGLE_INTERNAL_METRIC", "僅一項前後日量化支持退潮；不可當完整退潮實證"
    else:
        level, why = "UNVERIFIED_FADE_LABEL", "僅有H108退潮標籤，缺跨期有效資金流及族群廣度雙證據"
    data_state = _text(row.get("H110證據狀態"))
    if fade and data_state.startswith(("PARTIAL", "UNKNOWN", "CONFLICT")):
        why += "；H110來源狀態=" + data_state[:60]
    return {
        "H142退潮證據級別":level,
        "H142證據說明":why,
        "H142前次觀測日期":prev.isoformat() if prev else "UNKNOWN",
        "H142研究截止日期":cutoff.isoformat() if cutoff else "UNKNOWN",
        "H142資金流變化":flow,
        "H142族群廣度變化":breadth,
        "H142保留風控":"H109原權威維持；H142不得解除禁止新倉或證據門檻",
    }


def build_h142_review(tables: Mapping[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return manager summary, source audit, sector concentration (no IO)."""
    audit = tables.get("audit") if isinstance(tables, Mapping) else None
    authority = tables.get("h126_authority") if isinstance(tables, Mapping) else None
    audit = audit if isinstance(audit, pd.DataFrame) else pd.DataFrame()
    authority = authority if isinstance(authority, pd.DataFrame) else pd.DataFrame()
    selected = {}
    # Repeated pools do not count as distinct independent market observations.
    for rec in audit.to_dict("records"):
        code = _code(rec.get("股票代號"))
        if not code: continue
        if code not in selected: selected[code] = rec
        elif _text(rec.get("H109決策權威")) == "FADE_VETO":selected[code] = rec
    data=[]
    for code, rec in selected.items():
        status = assess_sector_fade(rec)
        data.append({
            "股票代號":code,"股票名稱":_text(rec.get("股票名稱")),
            "產業類別":_text(rec.get("類別")) or "UNKNOWN",
            "H109原風控":_text(rec.get("H109決策權威")) or "UNKNOWN",
            "H108原始生命週期":_text(rec.get("H108族群生命週期")) or "未提供",
            "H108族群確認分":_float(rec.get("H108族群確認分")),
            "H108觀測狀態":_text(rec.get("H108族群觀測狀態")) or "未提供",
            "H108內部樣本可信度":_float(rec.get("H108內部樣本可信度")),
            "H110證據狀態":_text(rec.get("H110證據狀態")) or "UNKNOWN",
            "H123法人實證":_text(rec.get("H123法人實證狀態")) or "UNKNOWN",
            "H124大戶實證":_text(rec.get("H124大戶實證狀態")) or "UNKNOWN",
            **status,
        })
    details = pd.DataFrame(data)
    if details.empty:
        details = pd.DataFrame(columns=["股票代號", "H109原風控", "H142退潮證據級別", "H142保留風控"])
    count=len(details)
    fade=details[details["H109原風控"]=="FADE_VETO"]
    unverified=int((fade["H142退潮證據級別"]=="UNVERIFIED_FADE_LABEL").sum())
    corroborated=int((fade["H142退潮證據級別"]=="TWO_INTERNAL_METRICS").sum())
    sectors=int(fade["產業類別"].nunique()) if not fade.empty else 0
    ratio=round(len(fade)*100/count, 2) if count else None
    mass = count>=20 and len(fade)/count>=0.9
    b_count=int(authority.get("H126權威層別",pd.Series([],dtype=str)).astype(str).str.startswith("B｜").sum())
    official_count=int((authority.get("H126認證資格",pd.Series([],dtype=str)).astype(str)=="PASS").sum())
    source_pending=sum(int(details.get(k,pd.Series([],dtype=str)).astype(str).ne(v).sum()) for k,v in [
        ("H123法人實證","PROVEN_FLOW"),("H124大戶實證","VERIFIED_RISING")])
    headline = "STOP｜H109退潮集中且資料證據需複核，不授權新倉" if mass else "REVIEW｜維持全部原始風控，不授權買進"
    overview=pd.DataFrame([
        ("版本",VERSION),("主管品質結論",headline),
        ("H113稽核去重檔數",count),("H109 FADE_VETO檔數",len(fade)),
        ("FADE佔比%",ratio),("FADE涉及產業數",sectors),
        ("退潮僅標籤、無雙量化證據檔數",unverified),
        ("退潮有雙內部指標支援檔數",corroborated),
        ("H123/H124缺實證欄位數",source_pending),
        ("H126 B級待驗證研究檔數",b_count),
        ("H126 A級認證研究檔數",official_count),
        ("價格及交易權限", "H142無買進授權；00 H126決策及01正式交易計畫仍為最終門檻"),
        ("注意", "內部數值同向不等於官方發行時點已認證；歷史回放不可算當日LIVE"),
        ("修復方向", "先確認H108原始族群時序、來源日期、廣度與資金流，不要一律把FADE改成可買進"),
    ], columns=["項目","數值"])
    if data:
        by_sector=(details.groupby("產業類別",dropna=False)
            .agg(候選檔數=("股票代號","count"),FADE檔數=("H109原風控",lambda x:int((x=="FADE_VETO").sum())),
                 無雙量化證據=("H142退潮證據級別",lambda x:int((x=="UNVERIFIED_FADE_LABEL").sum())))
            .reset_index().sort_values(["FADE檔數","候選檔數","產業類別"],ascending=[False,False,True]))
    else:
        by_sector=pd.DataFrame(columns=["產業類別","候選檔數","FADE檔數","無雙量化證據"])
    return overview,details,by_sector
