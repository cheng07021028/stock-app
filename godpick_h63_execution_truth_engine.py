from __future__ import annotations

from typing import Any
import pandas as pd

VERSION = "v191_h63_formal_execution_identity_truth_20260907"

H63_COLUMNS = [
    "H63角色", "H63是否正式推薦", "H63是否今日精選", "H63正式作戰資格",
    "H63是否可直接買", "H63用途", "H63決策理由", "H63版本",
]


def _s(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def _series(frame: pd.DataFrame, name: str, default: str = "") -> pd.Series:
    if name in frame.columns:
        return frame[name].fillna("").astype(str)
    return pd.Series([default] * len(frame), index=frame.index, dtype="object")


def apply_h63_execution_truth(frame: pd.DataFrame) -> pd.DataFrame:
    """Annotate every row with one unambiguous human-facing identity.

    H63 never upgrades authority.  EFFECTIVE-FORMAL is the only role that may be
    called a formal recommendation. FORMAL-HOLD, A-, Radar and research rows stay
    visible for audit/research but cannot masquerade as recommendations.
    """
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
    out = frame.copy()

    eff = _series(out, "H62有效權威")
    raw = _series(out, "H62原始權威")
    if not raw.ne("").any():
        raw = _series(out, "H56上游權威層級")
    bucket = _series(out, "正式推薦分區")
    explicit = _series(out, "是否正式推薦").str.lower()
    daily = _series(out, "H34每日精選").eq("是")

    roles=[]; is_formal=[]; is_daily=[]; battle=[]; direct=[]; uses=[]; reasons=[]
    for idx in out.index:
        e = _s(eff.loc[idx]).upper()
        r = _s(raw.loc[idx]).upper()
        b = _s(bucket.loc[idx])
        ex = _s(explicit.loc[idx]).lower()
        d = bool(daily.loc[idx])

        # H62 current-run authority is decisive. Only fall back to old Formal
        # evidence when H62 fields are absent, never when H62 says FORMAL-HOLD.
        if e == "EFFECTIVE-FORMAL":
            formal = True
            role = "F1｜正式推薦＋每日精選" if d else "F0｜正式推薦"
            usage = "本輪真正正式推薦；仍須依盤前、觸發、守價、停損與RR執行。"
            reason = "H62有效權威=EFFECTIVE-FORMAL"
        elif e == "FORMAL-HOLD":
            formal = False
            role = "FH｜原始Formal暫停"
            usage = "保留原始Formal稽核，但本輪不列正式作戰推薦。"
            reason = "H62有效權威=FORMAL-HOLD"
        elif e in {"A-MINUS", "RADAR", "RESTRICTED", "UNKNOWN"}:
            formal = False
            if e == "A-MINUS" or "A-" in b or "準主推薦" in b:
                role = "A-｜準主推薦條件候選"
                usage = "非正式推薦；僅盤中觸發且守價後小量試單。"
            elif e == "RADAR" or "雷達" in b:
                role = "R｜盤中雷達研究"
                usage = "非正式推薦；只供盤中研究/追蹤。"
            elif e == "RESTRICTED":
                role = "X｜權威限制/禁止"
                usage = "非正式推薦；本輪禁止建立新倉。"
            else:
                role = "W｜一般研究"
                usage = "非正式推薦；只供研究。"
            reason = f"H62有效權威={e or 'UNKNOWN'}"
        else:
            # Legacy compatibility only when H62 evidence genuinely does not exist.
            legacy_formal = (r == "FORMAL") or b == "正式下週主推薦" or ex.startswith(("是", "true", "1"))
            if legacy_formal:
                formal = True
                role = "F1｜正式推薦＋每日精選" if d else "F0｜正式推薦"
                usage = "舊資料Formal相容顯示；部署H62/H63後應以有效Formal為準。"
                reason = "Legacy Formal fallback（H62有效權威缺失）"
            elif r == "A-MINUS" or "A-" in b or "準主推薦" in b:
                formal = False; role = "A-｜準主推薦條件候選"; usage = "非正式推薦；條件候選。"; reason = "A-MINUS"
            elif r == "RADAR" or "雷達" in b:
                formal = False; role = "R｜盤中雷達研究"; usage = "非正式推薦；研究雷達。"; reason = "RADAR"
            else:
                formal = False; role = "W｜一般研究"; usage = "非正式推薦；只供研究。"; reason = "No Formal authority"

        roles.append(role)
        is_formal.append("是｜正式推薦" if formal else "否｜非正式推薦")
        is_daily.append("是｜每日精選" if d else "否")
        battle.append("是｜本輪正式作戰" if formal else "否")
        # Even a formal recommendation is never a market-order instruction.
        direct.append("否｜仍需盤前/觸發/守價/RR")
        uses.append(usage)
        reasons.append(reason)

    out["H63角色"] = roles
    out["H63是否正式推薦"] = is_formal
    out["H63是否今日精選"] = is_daily
    out["H63正式作戰資格"] = battle
    out["H63是否可直接買"] = direct
    out["H63用途"] = uses
    out["H63決策理由"] = reasons
    out["H63版本"] = VERSION
    return out


def build_h63_formal_execution_table(frame: pd.DataFrame, max_rows: int = 30) -> pd.DataFrame:
    """The only H63 formal recommendation/execution sheet.

    Contains EFFECTIVE-FORMAL only. H34 daily-selection status is shown as a
    priority flag, not used to hide an otherwise valid effective Formal.
    """
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return pd.DataFrame({
            "H63正式推薦": ["NONE｜本輪沒有有效正式推薦"],
            "H63是否可直接買": ["否"],
            "現在該做什麼": ["等待新機會；A-/Radar不冒充正式推薦。"],
        })
    work = apply_h63_execution_truth(frame)
    mask = work["H63正式作戰資格"].astype(str).str.startswith("是")
    out = work.loc[mask].copy()
    if out.empty:
        return pd.DataFrame({
            "H63正式推薦": ["NONE｜本輪沒有有效正式推薦"],
            "H63是否可直接買": ["否"],
            "現在該做什麼": ["等待新機會；A-/Radar不冒充正式推薦。"],
        })

    daily = out.get("H34每日精選", pd.Series([""] * len(out), index=out.index)).fillna("").astype(str).eq("是")
    rank = pd.to_numeric(out.get("H34每日精選排名", pd.Series([9999] * len(out), index=out.index)), errors="coerce").fillna(9999)
    h62score = pd.to_numeric(out.get("H62增量機會分", pd.Series([0] * len(out), index=out.index)), errors="coerce").fillna(0)
    out["_H63daily"] = daily.astype(int)
    out["_H63rank"] = rank
    out["_H63score"] = h62score
    out.sort_values(["_H63daily", "_H63rank", "_H63score"], ascending=[False, True, False], inplace=True, kind="mergesort")
    out = out.head(max(1, int(max_rows))).copy()
    out["H63正式推薦順位"] = range(1, len(out) + 1)
    out["H63正式推薦"] = out["H63角色"]
    out["現在該做什麼"] = out.get("正式推薦動作", out.get("操作許可", pd.Series(["依觸發/守價/RR執行"] * len(out), index=out.index))).fillna("").astype(str)

    front = [c for c in [
        "H63正式推薦順位", "股票代號", "股票名稱", "類別", "H63正式推薦", "H63是否今日精選",
        "H63是否可直接買", "現在該做什麼", "H56盤前狀態", "H62有效權威",
        "正式推薦分區", "正式推薦等級", "操作許可", "主要進場路徑", "主要進場參考價",
        "回測承接參考價", "實戰觸發價", "觸發後守價", "實戰停損參考", "路徑風險報酬比",
        "H34安全精選分", "H61機會價值分", "H62增量機會分", "H63決策理由",
    ] if c in out.columns]
    rest = [c for c in out.columns if c not in front and not c.startswith("_H63")]
    return out.loc[:, front + rest]


def build_h63_authority_audit_table(frame: pd.DataFrame, max_rows: int = 50) -> pd.DataFrame:
    """Human-facing audit table; explicitly not a recommendation list."""
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return pd.DataFrame()
    work = apply_h63_execution_truth(frame)
    role = work["H63角色"].fillna("").astype(str)
    order = role.map(lambda x: 10 if x.startswith("F1") else 11 if x.startswith("F0") else 20 if x.startswith("FH") else 30 if x.startswith("A-") else 40 if x.startswith("R") else 50 if x.startswith("X") else 60)
    work["_H63role_order"] = order
    score = pd.to_numeric(work.get("H62增量機會分", pd.Series([0]*len(work), index=work.index)), errors="coerce").fillna(0)
    work["_H63score"] = score
    work.sort_values(["_H63role_order", "_H63score"], ascending=[True, False], inplace=True, kind="mergesort")
    work = work.head(max(1, int(max_rows))).copy()
    cols = [c for c in [
        "H63角色", "H63是否正式推薦", "H63用途", "股票代號", "股票名稱", "類別",
        "H62有效權威", "正式推薦分區", "操作許可", "正式推薦等級", "H34每日精選",
        "H61機會層級", "H62機會層級", "H62增量機會分", "路徑風險報酬比", "H63決策理由",
    ] if c in work.columns]
    return work.loc[:, cols]


__all__ = [
    "VERSION", "H63_COLUMNS", "apply_h63_execution_truth",
    "build_h63_formal_execution_table", "build_h63_authority_audit_table",
]
