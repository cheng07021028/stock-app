# -*- coding: utf-8 -*-
"""V191-H68 execution truth + learning snapshot authority.

H68 fixes two structural problems exposed by the 2026-09-10 -> 2026-09-11 T+1 review:
1) end-of-day research priority (H67 P1/P2/C1) must never be mistaken for next-session
   execution permission; overnight/pre-open shock, data freshness and H56/Entry/RR still govern.
2) H65/H66/H67 ranking features must be persisted with the recommendation record at the
   decision time, otherwise adaptive learning remains permanently at zero mature cohorts.

H68 never creates Formal authority. The only Formal truth remains H64 EFFECTIVE-FORMAL,
then H63 execution identity. H68 only reconciles labels, persists immutable learning features,
and supplies an execution-veto layer.
"""
from __future__ import annotations

from typing import Any, Iterable
import math
import pandas as pd

VERSION = "v191_h68_execution_learning_authority_20260911"

H68_COLUMNS = [
    "H68學習快照狀態", "H68學習快照完整率%", "H68最終Formal真相",
    "H68官方資料風險", "H68隔夜衝擊分", "H68次日執行狀態", "H68執行否決原因",
    "H68權威邊界", "H68版本",
]

_BLANK = {"", "none", "nan", "nat", "null", "--", "-", "<na>"}


def _s(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    t = str(v).strip()
    return "" if t.lower() in _BLANK else t


def _f(v: Any, default: float | None = None) -> float | None:
    try:
        t = str(v).strip().replace(",", "").replace("％", "%")
        if t.endswith("%"):
            t = t[:-1].strip()
        if not t or t.lower() in _BLANK:
            return default
        x = float(t)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _num(row: pd.Series | dict[str, Any], names: Iterable[str], default: float | None = None) -> float | None:
    idx = row.index if isinstance(row, pd.Series) else row.keys()
    for c in names:
        if c in idx:
            x = _f(row.get(c), None)
            if x is not None:
                return x
    return default


def _txt(row: pd.Series | dict[str, Any], names: Iterable[str], default: str = "") -> str:
    idx = row.index if isinstance(row, pd.Series) else row.keys()
    for c in names:
        if c in idx:
            t = _s(row.get(c))
            if t:
                return t
    return default


def _ensure_h67(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
    work = frame.copy()
    try:
        from godpick_h67_regime_consensus_engine import VERSION as H67_VERSION, apply_h67_regime_consensus
        v = work.get("H67版本", pd.Series([""] * len(work), index=work.index)).fillna("").astype(str)
        if not v.eq(H67_VERSION).all():
            work = apply_h67_regime_consensus(work)
    except Exception:
        pass
    return work


def _snapshot_completeness(row: pd.Series) -> tuple[float, bool]:
    # Rank/percentile fields are essential: without them the model cannot learn whether
    # rank #1 actually beat rank #10. Scores alone are insufficient.
    required = [
        "H65觀察層級", "H65多因子觀察分", "H65全市場觀察百分位%",
        "H66T1層級", "H66T1自適應排序分", "H66T1全市場順位",
        "H67研究優先層級", "H67T1治理分", "H67全市場順位",
    ]
    present = 0
    for c in required:
        if c in row.index and _s(row.get(c)):
            present += 1
    pct = round(present / len(required) * 100.0, 2)
    return pct, present == len(required)


def _official_data_risk(row: pd.Series) -> tuple[str, bool, list[str]]:
    reasons: list[str] = []
    hard = False
    lag = _num(row, ["官方因子落後交易日"], None)
    kline_lag = _num(row, ["K線落後交易日", "個股K線落後交易日"], None)
    freshness = "｜".join([
        _txt(row, ["官方因子新鮮度"]), _txt(row, ["股神資料總新鮮度"]),
        _txt(row, ["K線資料新鮮度"]), _txt(row, ["大盤資料新鮮度"]),
    ])
    if lag is not None and lag >= 2:
        hard = True; reasons.append(f"官方因子落後{lag:.0f}日")
    elif lag is not None and lag >= 1:
        reasons.append("官方因子T-1")
    if kline_lag is not None and kline_lag >= 1:
        hard = True; reasons.append(f"K線落後{kline_lag:.0f}日")
    if any(k in freshness for k in ["逾時", "過期", "STALE", "不可信", "缺失"]):
        hard = True; reasons.append("關鍵資料新鮮度不足")
    status = "BLOCK｜資料過期" if hard else ("T-1｜需盤前重驗" if reasons else "READY｜資料新鮮")
    return status, hard, reasons


def _overnight_shock(row: pd.Series) -> tuple[float, bool, list[str]]:
    """Use pre-open/current-session fields only when actually present.

    H68 deliberately does not invent tomorrow's market state at EOD. If the deployment
    supplies futures/night-session/pre-open breadth on the next session, those fields
    become a hard veto. Otherwise the state remains RECHECK rather than false CONFIRMED.
    """
    candidates = [
        ("台指期夜盤", _num(row, ["台指期夜盤漲跌%", "台指夜盤漲跌幅%", "夜盤期貨漲跌%"], None)),
        ("盤前大盤", _num(row, ["盤前大盤漲跌幅%", "加權指數盤前漲跌%", "盤前指數漲跌%"], None)),
        ("費半", _num(row, ["費半漲跌%", "SOX漲跌%"], None)),
        ("NASDAQ", _num(row, ["NASDAQ漲跌%", "那斯達克漲跌%"], None)),
    ]
    vals = [(name, v) for name, v in candidates if v is not None]
    breadth = _num(row, ["盤前市場廣度%", "市場廣度%", "上漲家數占比%"], None)
    reasons: list[str] = []
    block = False
    score = 50.0
    if vals:
        worst_name, worst = min(vals, key=lambda x: x[1])
        score += max(-30.0, min(20.0, worst * 10.0))
        if worst <= -2.0:
            block = True; reasons.append(f"{worst_name}{worst:.2f}%急跌")
        elif worst <= -1.2:
            reasons.append(f"{worst_name}{worst:.2f}%偏弱")
    if breadth is not None:
        score = score * 0.7 + max(0.0, min(100.0, breadth)) * 0.3
        if breadth < 30:
            block = True; reasons.append(f"市場廣度僅{breadth:.1f}%")
        elif breadth < 40:
            reasons.append(f"市場廣度偏弱{breadth:.1f}%")
    return max(0.0, min(100.0, score)), block, reasons


def apply_h68_execution_learning_truth(frame: pd.DataFrame) -> pd.DataFrame:
    if frame is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
    work = _ensure_h67(frame).reset_index(drop=True)
    rows = []
    for _, row in work.iterrows():
        snap_pct, snap_ready = _snapshot_completeness(row)
        h64 = _txt(row, ["H64有效權威", "H63有效權威", "H62有效權威"])
        final_formal = "EFFECTIVE-FORMAL" if h64 == "EFFECTIVE-FORMAL" else "NO-FORMAL"
        data_status, data_block, data_reasons = _official_data_risk(row)
        shock_score, shock_block, shock_reasons = _overnight_shock(row)
        preopen = _txt(row, ["H67盤前再確認狀態", "H56最終參考層級", "H56盤前狀態"])
        reasons: list[str] = [*data_reasons, *shock_reasons]

        if final_formal != "EFFECTIVE-FORMAL":
            execution = "NO-FORMAL｜僅研究觀察"
            reasons.insert(0, "H64非EFFECTIVE-FORMAL")
        elif data_block or shock_block or any(k in preopen.upper() for k in ["BLOCK", "LOCKDOWN"]):
            execution = "BLOCK｜次日不得執行"
            if any(k in preopen.upper() for k in ["BLOCK", "LOCKDOWN"]):
                reasons.append("H56/H67盤前封鎖")
        elif preopen.startswith("CONFIRMED"):
            execution = "READY-COND｜仍須Entry/守價/RR"
        else:
            execution = "RECHECK｜次日盤前重新驗證"
            reasons.append("收盤後順位不可直接沿用成隔日執行資格")

        rows.append({
            "H68學習快照狀態": "SNAPSHOT-READY" if snap_ready else "SNAPSHOT-INCOMPLETE",
            "H68學習快照完整率%": snap_pct,
            "H68最終Formal真相": final_formal,
            "H68官方資料風險": data_status,
            "H68隔夜衝擊分": round(shock_score, 2),
            "H68次日執行狀態": execution,
            "H68執行否決原因": "；".join(dict.fromkeys([r for r in reasons if r])) or "無",
            "H68權威邊界": "H68不建立Formal；H64 EFFECTIVE-FORMAL→H63身分仍是唯一正式推薦權威",
            "H68版本": VERSION,
        })
    extra = pd.DataFrame(rows, index=work.index)
    for c in H68_COLUMNS:
        work[c] = extra[c]
    return work


def reconcile_h68_formal_summary(
    summary_df: pd.DataFrame,
    authority_frame: pd.DataFrame,
    scan_report: dict[str, Any] | None = None,
    formal_execution_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Reconcile legacy upstream Formal counts with H64/H63 single truth."""
    if summary_df is None or not isinstance(summary_df, pd.DataFrame) or summary_df.empty:
        return summary_df
    out = summary_df.copy()
    row0 = out.iloc[0]
    upstream = int(_f(row0.get("正式推薦檔數"), 0) or 0)
    frame = authority_frame if isinstance(authority_frame, pd.DataFrame) else pd.DataFrame()
    h64_n = 0
    if not frame.empty and "H64有效權威" in frame.columns:
        h64_n = int(frame["H64有效權威"].fillna("").astype(str).eq("EFFECTIVE-FORMAL").sum())
    h63_n = None
    if isinstance(formal_execution_df, pd.DataFrame) and "H63正式推薦順位" in formal_execution_df.columns:
        h63_n = int(len(formal_execution_df))
    final_n = h64_n if h63_n is None else h63_n
    report = scan_report if isinstance(scan_report, dict) else {}
    data_usable = bool(report.get("正式推薦可用", False))

    out.loc[out.index[0], "上游Formal檔數"] = upstream
    out.loc[out.index[0], "H64有效Formal檔數"] = h64_n
    out.loc[out.index[0], "H63正式作戰檔數"] = h63_n if h63_n is not None else h64_n
    out.loc[out.index[0], "正式推薦檔數"] = final_n
    out.loc[out.index[0], "正式推薦資料可用"] = "是" if data_usable else "否"
    out.loc[out.index[0], "正式推薦可用"] = "是" if (data_usable and final_n > 0) else "否"
    out.loc[out.index[0], "H68正式推薦真相完整性"] = "PASS" if (h63_n is None or h63_n == h64_n) else f"CHECK｜H64={h64_n}/H63={h63_n}"
    out.loc[out.index[0], "H68版本"] = VERSION
    if not data_usable:
        out.loc[out.index[0], "本輪結論"] = "資料品質不足｜禁止正式推薦"
    elif final_n > 0:
        out.loc[out.index[0], "本輪結論"] = f"有正式推薦｜H64/H63有效 {final_n} 檔"
        out.loc[out.index[0], "操作說明"] = f"本輪H64/H63最終有效Formal {final_n} 檔；仍須盤前、Entry/守價與RR成立後才可執行。"
    elif upstream > 0:
        out.loc[out.index[0], "本輪結論"] = "無正式推薦｜上游Formal已被H64/H63品質治理暫停"
        out.loc[out.index[0], "操作說明"] = f"上游曾有 {upstream} 檔Formal，但H64/H63最終有效Formal為0；不得對外宣稱有正式推薦。"
        out.loc[out.index[0], "空白/封鎖原因"] = f"上游Formal {upstream} 檔已被H64/H63收斂為0；空手/等待是正式決策。"
    else:
        out.loc[out.index[0], "本輪結論"] = "無正式推薦｜H64/H63最終真相"
        out.loc[out.index[0], "操作說明"] = "本輪沒有H64/H63有效Formal；H65/H66/H67/H68僅供研究與次日重驗。"
    return out


def build_h68_execution_learning_table(frame: pd.DataFrame, max_rows: int = 30) -> pd.DataFrame:
    work = apply_h68_execution_learning_truth(frame)
    if work.empty:
        return work
    sort_cols = [c for c in ["H68最終Formal真相", "H67全市場順位", "H67T1治理分"] if c in work.columns]
    if "H67全市場順位" in sort_cols:
        work["_h68_rank"] = pd.to_numeric(work["H67全市場順位"], errors="coerce").fillna(999999)
        work = work.sort_values(["_h68_rank"], ascending=True, kind="mergesort")
    cols = [c for c in [
        "股票代號", "股票名稱", "類別", "H64有效權威", "H67研究優先層級", "H67T1治理分", "H67全市場順位",
        "H68最終Formal真相", "H68次日執行狀態", "H68執行否決原因", "H68官方資料風險", "H68隔夜衝擊分",
        "H68學習快照狀態", "H68學習快照完整率%", "H68權威邊界", "H68版本",
    ] if c in work.columns]
    return work.head(max(1, int(max_rows or 1)))[cols].reset_index(drop=True)


__all__ = [
    "VERSION", "H68_COLUMNS", "apply_h68_execution_learning_truth",
    "reconcile_h68_formal_summary", "build_h68_execution_learning_table",
]
