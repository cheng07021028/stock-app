# -*- coding: utf-8 -*-
"""V191-H102 Sector Rotation Shock × T+1 Closed Loop.

Research/ranking evolution layer only.

What H102 fixes
----------------
* Detects a fast sector ignition even when the previous H94 label still says
  ``資金退潮`` / ``弱勢``.
* Re-ranks Research/Waiting with a dynamic score while preserving H101 as the
  stable baseline score.
* May promote a tightly governed Waiting row into *dynamic Research* when the
  sector shock + execution/risk/RR evidence is strong.  It never grants Formal,
  A- or R1 buy authority.
* Separates Pullback Entry A from Momentum Continuation Entry B.  B is a
  re-validation state, not a copied price/stop from A.
* Builds a manager T+1 review table from the local executable-truth authority so
  missed executable opportunities can become explicit learning samples.
* Provides bounded shadow tracking rows for the strongest dynamic Waiting names
  without polluting formal performance.

The module is deterministic/dataframe-only during recommendation ranking.  It
performs no network requests, so it cannot slow the 1,600+ stock scan.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable
import json
import math

import pandas as pd

VERSION = "v191_h102_sector_rotation_shock_t1_closed_loop_20261001"
BASE_DIR = Path(__file__).resolve().parent
_T1_CACHE: dict[str, Any] = {"mtime_ns": None, "stats": {}}

H102_COLUMNS = [
    "H102版本",
    "H102同層順位",
    "H102動態優先分",
    "H102動態層級",
    "H102來源層別",
    "H102族群衝擊分",
    "H102族群動態狀態",
    "H102Catalyst分",
    "H102研究升級",
    "H102研究升級理由",
    "H102Pullback Entry A",
    "H102Momentum Entry B",
    "H102Momentum重驗",
    "H102T1學習調整",
    "H102建議動作",
    "H102Formal權限",
    "H102決策摘要",
]

OVERVIEW_COLUMNS = [
    "H102閱讀順位", "H102推薦層別", "H102同層順位", "股票代號", "股票名稱", "類別",
    "H102動態優先分", "H102動態層級", "H102族群衝擊分", "H102族群動態狀態",
    "H101推薦優先分", "H96核心研究等級",
    "H99主進場", "H99防守停損", "H99第一目標", "H99成本後RR", "H99目標交易日",
    "H102Momentum Entry B", "H102建議動作", "H102研究升級理由",
]


def _text(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    s = str(v).strip()
    return "" if s.lower() in {"", "nan", "none", "null", "<na>", "--"} else s


def _num(v: Any, default: float | None = None) -> float | None:
    if v is None or isinstance(v, bool):
        return default
    if isinstance(v, str):
        v = v.replace(",", "").replace("％", "%").replace("%", "").replace("+", "").strip()
        if not v or v.lower() in {"nan", "none", "null", "<na>", "--", "-"}:
            return default
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _first_num(row: dict[str, Any], names: Iterable[str], default: float | None = None) -> float | None:
    for name in names:
        x = _num(row.get(name), None)
        if x is not None:
            return x
    return default


def _first_text(row: dict[str, Any], names: Iterable[str]) -> str:
    for name in names:
        s = _text(row.get(name))
        if s:
            return s
    return ""


def _clip(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, float(v)))


def _code(v: Any) -> str:
    s = _text(v)
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    return s.zfill(4) if s.isdigit() and len(s) < 4 else s


def _rr_score(rr: float | None) -> float:
    if rr is None or rr <= 0:
        return 0.0
    return _clip(45.0 + (rr - 1.0) * 60.0)


def _sector_map(sector_df: pd.DataFrame | None) -> dict[str, dict[str, Any]]:
    if not isinstance(sector_df, pd.DataFrame) or sector_df.empty or "類別" not in sector_df.columns:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for _, row in sector_df.iterrows():
        key = _text(row.get("類別"))
        if key and key not in out:
            out[key] = row.to_dict()
    return out


def score_sector_shock(sector_row: dict[str, Any] | None) -> tuple[float, str, str]:
    """Return (0-100 score, state, evidence).

    The score intentionally allows a sector to be labelled IGNITION even when
    an older categorical field still says 資金退潮.  Cross-sectional rank and
    realised group price strength have priority in that contradiction.
    """
    r = dict(sector_row or {})
    if not r:
        return 0.0, "NO-SECTOR-DATA", "缺少即時族群表"
    rank = _num(r.get("類股熱度排名"), 99.0) or 99.0
    avg_gain = _num(r.get("類股平均漲幅"), 0.0) or 0.0
    accel = _num(r.get("類股加速度"), 0.0) or 0.0
    funds = _num(r.get("族群資金流分數"), 0.0) or 0.0
    strong = _num(r.get("同族群強勢比例"), 0.0) or 0.0
    volume = _num(r.get("同族群平均量能分"), 0.0) or 0.0
    heat = _num(r.get("類股熱度分數"), 0.0) or 0.0
    legacy_state = _text(r.get("族群輪動狀態") or r.get("強勢族群等級"))

    if rank <= 3:
        rank_pts = 32.0
    elif rank <= 5:
        rank_pts = 26.0
    elif rank <= 10:
        rank_pts = 18.0
    elif rank <= 15:
        rank_pts = 10.0
    else:
        rank_pts = 0.0
    gain_pts = _clip(avg_gain * 5.0, 0.0, 28.0)
    accel_pts = _clip((accel - 25.0) * 0.50, 0.0, 10.0)
    fund_pts = _clip((funds - 35.0) * 0.50, 0.0, 8.0)
    strong_pts = _clip((strong - 25.0) * 0.40, 0.0, 6.0)
    volume_pts = _clip((volume - 40.0) * 0.35, 0.0, 6.0)
    heat_pts = _clip((heat - 45.0) * 0.18, 0.0, 4.0)
    score = _clip(rank_pts + gain_pts + accel_pts + fund_pts + strong_pts + volume_pts + heat_pts)

    stale_weak = any(x in legacy_state for x in ["退潮", "弱勢", "資金不足", "降溫"])
    if score >= 75 and stale_weak:
        state = "IGNITION｜退潮反轉點火"
    elif score >= 75:
        state = "IGNITION｜族群加速"
    elif score >= 65 and stale_weak:
        state = "REVERSAL-WATCH｜舊弱勢正在反轉"
    elif score >= 65:
        state = "ROTATION-WATCH｜輪動升溫"
    elif score >= 50:
        state = "NEUTRAL｜觀察"
    else:
        state = "COOL｜未點火"
    evidence = (
        f"熱度#{int(rank) if rank < 99 else 99}｜均漲{avg_gain:.2f}%｜"
        f"加速度{accel:.1f}｜資金{funds:.1f}｜強勢比{strong:.1f}%｜量能{volume:.1f}｜舊標籤:{legacy_state or 'NA'}"
    )
    return round(score, 2), state, evidence


def _catalyst_score(row: dict[str, Any]) -> float:
    news = _first_num(row, ["H81新聞事件影響分"], 50.0) or 50.0
    state = _first_text(row, ["H94新聞證據狀態"])
    text = "｜".join([
        _first_text(row, ["H81三大利多催化"]),
        _first_text(row, ["H93三大成長催化"]),
    ])
    score = news
    if "VERIFIED" in state or "AVAILABLE" in state:
        score += 5.0
    if "MISSING" in state:
        score -= 10.0
    # Text presence is only a completeness bonus, not a claim that the event is new.
    if len(text) >= 12:
        score += 3.0
    return round(_clip(score), 2)


def _local_t1_category_stats() -> dict[str, dict[str, Any]]:
    """Load/aggregate local T+1 authority at most once per file version."""
    path = BASE_DIR / "godpick_t1_trade_truth.json"
    if not path.exists():
        return {}
    try:
        mtime_ns = path.stat().st_mtime_ns
    except Exception:
        mtime_ns = None
    if _T1_CACHE.get("mtime_ns") == mtime_ns and isinstance(_T1_CACHE.get("stats"), dict):
        return _T1_CACHE["stats"]
    stats: dict[str, list[float]] = {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        rows = payload.get("records", []) if isinstance(payload, dict) else payload if isinstance(payload, list) else []
        for r in rows:
            if not isinstance(r, dict) or not bool(r.get("T1成熟")):
                continue
            cat = _text(r.get("類別"))
            if not cat:
                continue
            alpha = _num(r.get("Selection Alpha%"), None)
            ret = _num(r.get("隔日候選漲跌%"), None)
            if alpha is None and ret is None:
                continue
            stats.setdefault(cat, []).append(alpha if alpha is not None else ret)
    except Exception:
        stats = {}
    compact: dict[str, dict[str, Any]] = {}
    for cat, vals in stats.items():
        tail = vals[-40:]
        compact[cat] = {"n": len(vals), "mean": (sum(tail) / len(tail)) if tail else 0.0}
    _T1_CACHE["mtime_ns"] = mtime_ns
    _T1_CACHE["stats"] = compact
    return compact


def _local_t1_learning_adjustment(row: dict[str, Any]) -> tuple[float, str]:
    """Bounded local-only feedback with no network and one cached file read."""
    cat = _text(row.get("類別"))
    if not cat:
        return 0.0, "無類別"
    stat = _local_t1_category_stats().get(cat)
    if not stat:
        return 0.0, f"{cat}尚無成熟T+1樣本"
    n = int(stat.get("n", 0) or 0)
    mean = float(stat.get("mean", 0.0) or 0.0)
    if n < 5:
        return 0.0, f"{cat}成熟樣本{n}<5，不調權"
    adj = max(-2.5, min(2.5, mean * 0.35))
    return round(adj, 2), f"{cat}成熟樣本{n}｜平均Alpha/報酬{mean:+.2f}%｜調整{adj:+.2f}"


def analyze_candidate(row: dict[str, Any] | pd.Series, *, pool: str, sector_map: dict[str, dict[str, Any]]) -> dict[str, Any]:
    raw = row.to_dict() if isinstance(row, pd.Series) else dict(row or {})
    cat = _text(raw.get("類別"))
    sector_score, sector_state, sector_evidence = score_sector_shock(sector_map.get(cat))
    h101 = _first_num(raw, ["H101推薦優先分"], 0.0) or 0.0
    execution = _first_num(raw, ["H89執行品質分"], 0.0) or 0.0
    risk = _first_num(raw, ["H81風險管理分"], 0.0) or 0.0
    sector_pct = _first_num(raw, ["H79族群百分位%", "H53族群共振分"], 0.0) or 0.0
    rr = _first_num(raw, ["H99成本後RR", "H89成本後RR1", "H79成本後RR"], None)
    catalyst = _catalyst_score(raw)
    t1_adj, t1_note = _local_t1_learning_adjustment(raw)

    score = (
        h101 * 0.45
        + sector_score * 0.25
        + execution * 0.10
        + risk * 0.08
        + sector_pct * 0.07
        + _rr_score(rr) * 0.05
    ) + t1_adj

    no_chase = _first_text(raw, ["H96不追價"])
    h96_exec = _first_text(raw, ["H96執行狀態"])
    h99_exec = _first_text(raw, ["H99執行狀態"])
    exhaust = _first_text(raw, ["H94耗竭風險"])
    if sector_state.startswith("IGNITION"):
        score += 7.0
    elif sector_state.startswith("REVERSAL-WATCH"):
        score += 3.0
    if no_chase == "是" or h96_exec.startswith("LEADER-NO-CHASE"):
        score -= 8.0
    if exhaust in {"HIGH", "BLOCK"}:
        score -= 5.0
    if h99_exec.startswith("BLOCK"):
        score -= 8.0
    score = round(_clip(score), 2)

    promotable = (
        pool == "waiting"
        and sector_state.startswith("IGNITION")
        and score >= 72.0
        and execution >= 65.0
        and risk >= 60.0
        and sector_pct >= 65.0
        and (rr is not None and rr >= 1.40)
        and no_chase != "是"
        and exhaust not in {"HIGH", "BLOCK"}
        and not h99_exec.startswith("BLOCK")
    )
    if promotable:
        dynamic_tier = "D1｜輪動核心研究"
        action = "動態升級為Research；優先盤前/盤中重驗。仍非Formal，未重新取得正式授權不可直接買進。"
    elif sector_state.startswith("IGNITION") and score >= 62:
        dynamic_tier = "D2｜輪動優先觀察"
        action = "族群已點火但個股條件仍不足；列前段Waiting，等待價格/風控/不追價條件改善。"
    elif pool == "research":
        dynamic_tier = "R｜核心研究"
        action = "維持Research，依H102動態順位追蹤；盤前重驗後仍須原Formal治理。"
    elif pool == "actionable":
        dynamic_tier = "F｜正式池動態排序"
        action = "只重排Formal池閱讀順序；原Formal授權與風控條件必須仍然有效。"
    else:
        dynamic_tier = "W｜等待"
        action = "等待，不因H102排名而取得買進許可。"

    entry_a = _first_num(raw, ["H99主進場", "H89主進場", "H79計畫進場"], None)
    momentum_b = "RECALC_REQUIRED｜突破/回測成立後獨立重建Entry/Stop/RR"
    momentum_ready = (
        "ARMED｜不得沿用Pullback A停損；需突破量價＋守價/回測＋NetRR重新驗證"
        if sector_state.startswith("IGNITION") and execution >= 65 and risk >= 60 and no_chase != "是"
        else "STANDBY｜尚未達Momentum續強重驗條件"
    )
    upgrade_reason = (
        f"{sector_state}｜動態分{score:.1f}｜Execution{execution:.1f}｜Risk{risk:.1f}｜"
        f"族群百分位{sector_pct:.1f}｜NetRR{rr:.2f}｜{sector_evidence}"
        if promotable and rr is not None
        else f"{sector_state}｜{sector_evidence}"
    )
    summary = (
        f"動態分{score:.1f}｜族群衝擊{sector_score:.1f}｜Catalyst{catalyst:.1f}｜{dynamic_tier}；"
        "H102只做族群輪動/研究升級/T+1閉環，Formal永久LOCKED。"
    )
    return {
        "H102版本": VERSION,
        "H102同層順位": None,
        "H102動態優先分": score,
        "H102動態層級": dynamic_tier,
        "H102來源層別": pool.upper(),
        "H102族群衝擊分": sector_score,
        "H102族群動態狀態": sector_state,
        "H102Catalyst分": catalyst,
        "H102研究升級": "是" if promotable else "否",
        "H102研究升級理由": upgrade_reason,
        "H102Pullback Entry A": entry_a,
        "H102Momentum Entry B": momentum_b,
        "H102Momentum重驗": momentum_ready,
        "H102T1學習調整": t1_note,
        "H102建議動作": action,
        "H102Formal權限": "LOCKED｜不得建立或放寬Formal/A-/R1；正式執行仍由既有治理鏈決定。",
        "H102決策摘要": summary,
    }


def _apply_overlay(frame: pd.DataFrame | None, *, pool: str, sector_map: dict[str, dict[str, Any]]) -> pd.DataFrame:
    if frame is None:
        return pd.DataFrame()
    out = frame.copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame(frame)
    for c in H102_COLUMNS:
        if c not in out.columns:
            out[c] = None
    if out.empty or "股票代號" not in out.columns:
        return out
    valid = out["股票代號"].map(_code).ne("")
    if valid.any():
        add = pd.DataFrame([analyze_candidate(r, pool=pool, sector_map=sector_map) for r in out.loc[valid].to_dict("records")], index=out.index[valid])
        for c in H102_COLUMNS:
            if c in add.columns:
                out.loc[add.index, c] = add[c]
        rank_df = out.loc[valid].copy()
        rank_df["__h102_score"] = pd.to_numeric(rank_df["H102動態優先分"], errors="coerce")
        order = rank_df.sort_values(["__h102_score", "H101推薦優先分"], ascending=[False, False], na_position="last").index.tolist()
        for rank, idx in enumerate(order, start=1):
            out.at[idx, "H102同層順位"] = rank
        ranked = out.loc[order].copy()
        other = out.loc[~out.index.isin(order)].copy()
        out = pd.concat([ranked, other], ignore_index=True, sort=False)
    front = [c for c in [
        "H102同層順位", "H102動態優先分", "H102動態層級", "H102研究升級",
        "H102族群衝擊分", "H102族群動態狀態", "H102Catalyst分",
        "H101同層順位", "H101推薦優先分", "H101優先層級", "H101推薦層別",
        "股票代號", "股票名稱", "市場別", "類別", "H102建議動作", "H102研究升級理由",
        "H102Pullback Entry A", "H102Momentum Entry B", "H102Momentum重驗",
        "H99目標交易日", "H99主進場", "H99防守停損", "H99第一目標", "H99成本後RR",
    ] if c in out.columns]
    return out.loc[:, front + [c for c in out.columns if c not in front]].copy()


def _build_overview(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for pool_order, name in enumerate(("actionable", "research")):
        df = tables.get(name, pd.DataFrame())
        if not isinstance(df, pd.DataFrame) or df.empty or "股票代號" not in df.columns:
            continue
        work = df[df["股票代號"].map(_code).ne("")].copy()
        if work.empty:
            continue
        work["__pool_order"] = pool_order
        work["__dyn_rank"] = pd.to_numeric(work.get("H102同層順位"), errors="coerce").fillna(9999)
        work["H102推薦層別"] = "FORMAL｜正式可執行" if name == "actionable" else "RESEARCH｜核心/動態研究（非買進）"
        frames.append(work)
    if not frames:
        return pd.DataFrame(columns=OVERVIEW_COLUMNS)
    out = pd.concat(frames, ignore_index=True, sort=False)
    out = out.sort_values(["__pool_order", "__dyn_rank", "H102動態優先分"], ascending=[True, True, False], na_position="last").reset_index(drop=True)
    out["H102閱讀順位"] = range(1, len(out) + 1)
    for c in OVERVIEW_COLUMNS:
        if c not in out.columns:
            out[c] = None
    return out.loc[:, OVERVIEW_COLUMNS].copy()


def decorate_decision_tables(tables: dict[str, Any] | None, *, sector_df: pd.DataFrame | None = None) -> dict[str, pd.DataFrame]:
    src = tables if isinstance(tables, dict) else {}
    out: dict[str, pd.DataFrame] = {
        k: (v.copy() if isinstance(v, pd.DataFrame) else pd.DataFrame(v) if v is not None else pd.DataFrame())
        for k, v in src.items()
    }
    smap = _sector_map(sector_df)

    # First pass: discover dynamic promotion candidates in Waiting.
    waiting = _apply_overlay(out.get("waiting", pd.DataFrame()), pool="waiting", sector_map=smap)
    research = _apply_overlay(out.get("research", pd.DataFrame()), pool="research", sector_map=smap)
    promotions = pd.DataFrame()
    if not waiting.empty and "H102研究升級" in waiting.columns:
        cand = waiting[waiting["H102研究升級"].astype(str).eq("是")].copy()
        if not cand.empty:
            # Diversification: max three promotions, max one per sector.
            cand["__score"] = pd.to_numeric(cand["H102動態優先分"], errors="coerce")
            cand = cand.sort_values("__score", ascending=False, na_position="last")
            cand = cand.drop_duplicates(subset=["類別"], keep="first") if "類別" in cand.columns else cand
            promotions = cand.head(3).drop(columns=["__score"], errors="ignore").copy()
            if not promotions.empty:
                promotions["H102來源層別"] = "WAITING→RESEARCH"
                promo_codes = set(promotions["股票代號"].map(_code))
                waiting = waiting[~waiting["股票代號"].map(_code).isin(promo_codes)].copy()
                research = pd.concat([research, promotions], ignore_index=True, sort=False)
                if "股票代號" in research.columns:
                    research = research.drop_duplicates(subset=["股票代號"], keep="first")

    # Re-align H101 pool semantics after H102's research-only promotion.
    try:
        from godpick_h101_priority_ranking import apply_priority_overlay
        research = apply_priority_overlay(research, pool="research")
        waiting = apply_priority_overlay(waiting, pool="waiting")
    except Exception:
        pass
    research = _apply_overlay(research, pool="research", sector_map=smap)
    waiting = _apply_overlay(waiting, pool="waiting", sector_map=smap)
    # Preserve the promotion provenance after the second overlay.
    if not promotions.empty and "股票代號" in research.columns:
        promo_codes = set(promotions["股票代號"].map(_code))
        mask = research["股票代號"].map(_code).isin(promo_codes)
        research.loc[mask, "H102來源層別"] = "WAITING→RESEARCH"
        research.loc[mask, "H102研究升級"] = "是"
        research.loc[mask, "H102動態層級"] = "D1｜輪動核心研究"
        research.loc[mask, "H102建議動作"] = "H102由Waiting動態升級Research；優先盤前/盤中重驗，但仍非Formal。"

    out["research"] = research
    out["waiting"] = waiting
    for name in ("actionable", "audit", "emerging_watch"):
        out[name] = _apply_overlay(out.get(name, pd.DataFrame()), pool=name, sector_map=smap)
    out["priority_overview"] = _build_overview(out)

    health = out.get("health", pd.DataFrame()).copy()
    if not health.empty and "項目" in health.columns:
        health = health.loc[~health["項目"].astype(str).str.startswith("H102")].copy()
    rows = [
        {"項目": "H102版本", "數值": VERSION},
        {"項目": "H102動態Research升級", "數值": int(len(promotions))},
        {"項目": "H102族群衝擊治理", "數值": "熱度排名＋均漲＋加速度＋資金＋強勢廣度＋量能；舊『退潮』標籤可被新IGNITION證據覆寫於研究排序層"},
        {"項目": "H102雙進場", "數值": "A=H99 Pullback原計畫；B=Momentum Continuation必須獨立重建Entry/Stop/RR，禁止沿用A停損追價"},
        {"項目": "H102T1閉環", "數值": "可執行觸發/MFE/MAE/Selection Alpha寫入檢討；只用成熟歷史樣本做±2.5分內調整"},
        {"項目": "H102Formal權限", "數值": "LOCKED"},
    ]
    out["health"] = pd.concat([health, pd.DataFrame(rows)], ignore_index=True, sort=False)
    return out


def build_t1_review_table_local(limit: int = 80) -> pd.DataFrame:
    """Build manager-facing T+1 review from local truth only (zero network)."""
    path = BASE_DIR / "godpick_t1_trade_truth.json"
    if not path.exists():
        return pd.DataFrame({"狀態": ["本機尚無成熟T+1真相；背景更新完成後，本表會顯示Entry觸發、MFE、MAE與排名檢討。"]})
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        rows = payload.get("records", []) if isinstance(payload, dict) else payload if isinstance(payload, list) else []
    except Exception as exc:
        return pd.DataFrame({"狀態": [f"T+1本機真相讀取失敗：{type(exc).__name__}: {exc}"]})
    rows = [dict(r) for r in rows if isinstance(r, dict) and bool(r.get("T1成熟"))]
    # One economic outcome per recommendation date + stock.
    best: dict[str, dict[str, Any]] = {}
    for r in rows:
        key = f"{_text(r.get('推薦日期'))}|{_code(r.get('股票代號'))}"
        if not key.strip("|"):
            continue
        cur = best.get(key)
        if cur is None or _text(r.get("updated_at")) > _text(cur.get("updated_at")):
            best[key] = r
    rows = sorted(best.values(), key=lambda r: (_text(r.get("推薦日期")), _code(r.get("股票代號"))), reverse=True)[: max(1, int(limit))]
    out_rows: list[dict[str, Any]] = []
    for r in rows:
        mfe = _num(r.get("MFE%"), None)
        mae = _num(r.get("MAE%"), None)
        ret = _num(r.get("隔日候選漲跌%"), None)
        alpha = _num(r.get("Selection Alpha%"), None)
        trig = _text(r.get("進場觸發狀態"))
        entry_result = _text(r.get("Entry結果"))
        executable = bool(r.get("是否納入可執行績效"))
        rank = _first_num(r, ["H102同層順位", "H101同層順位"], None)
        if executable and mfe is not None and mfe >= 5:
            review = "EXECUTABLE-WIN｜Entry已觸發且MFE>=5%"
        elif (not executable or "未觸發" in trig or "NO-TRADE" in entry_result) and ret is not None and ret >= 5:
            review = "SELECTION-RIGHT-NO-ENTRY｜方向正確但未成交"
        elif executable and mae is not None and mae <= -3:
            review = "RISK-REVIEW｜已觸發但MAE偏大"
        elif alpha is not None and alpha < -2:
            review = "SELECTION-REVIEW｜相對市場落後"
        else:
            review = "NORMAL｜持續累積"
        if rank is not None and rank >= 3 and ((mfe is not None and mfe >= 5) or (ret is not None and ret >= 5)):
            rank_review = "RANK-UNDERRATED｜後順位卻出現強T+1，納入H102檢討"
        elif rank is not None and rank == 1 and ret is not None and ret < 0:
            rank_review = "RANK1-REVIEW｜第一順位T+1為負，需檢討權重"
        else:
            rank_review = "RANK-OK/WAIT"
        out_rows.append({
            "推薦日期": _text(r.get("推薦日期")),
            "隔日日期": _text(r.get("隔日日期")),
            "股票代號": _code(r.get("股票代號")),
            "股票名稱": _text(r.get("股票名稱")),
            "類別": _text(r.get("類別")),
            "H102順位": _num(r.get("H102同層順位"), None),
            "H101順位": _num(r.get("H101同層順位"), None),
            "H101優先分": _num(r.get("H101推薦優先分"), None),
            "進場觸發狀態": trig,
            "Entry結果": entry_result,
            "MFE%": mfe,
            "MAE%": mae,
            "隔日候選漲跌%": ret,
            "Selection Alpha%": alpha,
            "是否納入可執行績效": "是" if executable else "否",
            "H102T1檢討": review,
            "H102排名檢討": rank_review,
        })
    return pd.DataFrame(out_rows) if out_rows else pd.DataFrame({"狀態": ["目前沒有成熟T+1樣本。"]})


def build_shadow_tracking_frame(
    source_df: pd.DataFrame | None,
    waiting_df: pd.DataFrame | None,
    excluded_codes: Iterable[str] | None = None,
    *,
    max_rows: int = 6,
) -> pd.DataFrame:
    """Track top dynamic Waiting rows as low-weight learning samples.

    These rows are explicitly non-buy research shadows, enabling T+1 analysis of
    missed executable opportunities without inflating formal performance.
    """
    wait = waiting_df.copy() if isinstance(waiting_df, pd.DataFrame) else pd.DataFrame()
    if wait.empty or "股票代號" not in wait.columns:
        return pd.DataFrame()
    wait["股票代號"] = wait["股票代號"].map(_code)
    wait = wait[wait["股票代號"].ne("")].copy()
    excluded = {_code(x) for x in (excluded_codes or []) if _code(x)}
    if excluded:
        wait = wait[~wait["股票代號"].isin(excluded)].copy()
    if wait.empty:
        return pd.DataFrame()
    score = pd.to_numeric(wait.get("H102動態優先分"), errors="coerce")
    shock = pd.to_numeric(wait.get("H102族群衝擊分"), errors="coerce")
    mask = score.ge(62) | (shock.ge(65) & score.ge(50))
    wait = wait.loc[mask].copy()
    if wait.empty:
        return pd.DataFrame()
    wait["__score"] = pd.to_numeric(wait.get("H102動態優先分"), errors="coerce")
    wait = wait.sort_values("__score", ascending=False, na_position="last").head(max(1, int(max_rows)))

    source = source_df.copy() if isinstance(source_df, pd.DataFrame) else pd.DataFrame()
    source_map: dict[str, dict[str, Any]] = {}
    if not source.empty and "股票代號" in source.columns:
        for _, r in source.iterrows():
            c = _code(r.get("股票代號"))
            if c and c not in source_map:
                source_map[c] = r.to_dict()
    rows = []
    for _, wr in wait.iterrows():
        c = _code(wr.get("股票代號"))
        raw = dict(source_map.get(c, {}))
        raw.update(wr.drop(labels=["__score"], errors="ignore").to_dict())
        raw.update({
            "股票代號": c,
            "推薦模式": "H102動態輪動影子研究",
            "推薦用途": "T+1漏選/可執行機會閉環（非買進）",
            "紀錄來源": "07_股神推薦｜H102等待候選影子追蹤",
            "自動記錄": "是",
            "紀錄層級": "H102等待候選影子追蹤",
            "目前狀態": "H102動態Waiting追蹤",
            "是否可直接買進": "否",
            "校正樣本類型": "C｜H102 Waiting動態輪動影子樣本",
            "校正樣本用途": "T+1 Entry觸發/MFE/MAE/排名漏選檢討；不得計入正式交易勝率",
            "校正樣本權重": 0.25,
            "是否納入正式推薦績效": "否",
            "是否納入權重校正": "是",
            "樣本可信度": "中低｜影子研究",
            "校正樣本建立版本": VERSION,
        })
        if not _text(raw.get("推薦價格")):
            raw["推薦價格"] = raw.get("H99主進場") or raw.get("H89主進場")
        if not _text(raw.get("停損價")):
            raw["停損價"] = raw.get("H99防守停損") or raw.get("H89防守停損")
        if not _text(raw.get("賣出目標1")):
            raw["賣出目標1"] = raw.get("H99第一目標") or raw.get("H89第一目標")
        rows.append(raw)
    return pd.DataFrame(rows).reset_index(drop=True)


def mark_shadow_record_rows(rows: Iterable[dict[str, Any]] | None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in rows or []:
        x = dict(r)
        x["推薦模式"] = "H102動態輪動影子研究"
        x["紀錄來源"] = "07_股神推薦｜H102等待候選影子追蹤"
        x["自動記錄"] = "是"
        x["紀錄層級"] = "H102等待候選影子追蹤"
        x["目前狀態"] = "H102動態Waiting追蹤"
        x["是否可直接買進"] = "否"
        x["校正樣本類型"] = "C｜H102 Waiting動態輪動影子樣本"
        x["校正樣本用途"] = "T+1 Entry觸發/MFE/MAE/排名漏選檢討；不得計入正式交易勝率"
        x["校正樣本權重"] = 0.25
        x["是否納入正式推薦績效"] = "否"
        x["是否納入權重校正"] = "是"
        x["校正樣本建立版本"] = VERSION
        out.append(x)
    return out


def export_contract_summary(tables: dict[str, pd.DataFrame] | None) -> dict[str, Any]:
    t = tables if isinstance(tables, dict) else {}
    overview = t.get("priority_overview", pd.DataFrame())
    research = t.get("research", pd.DataFrame())
    waiting = t.get("waiting", pd.DataFrame())
    promoted = 0
    if isinstance(research, pd.DataFrame) and "H102研究升級" in research.columns:
        promoted = int(research["H102研究升級"].astype(str).eq("是").sum())
    ok = all(isinstance(x, pd.DataFrame) for x in [overview, research, waiting])
    return {
        "version": VERSION,
        "ok": bool(ok),
        "overview_rows": int(len(overview)) if isinstance(overview, pd.DataFrame) else 0,
        "research_rows": int(len(research)) if isinstance(research, pd.DataFrame) else 0,
        "waiting_rows": int(len(waiting)) if isinstance(waiting, pd.DataFrame) else 0,
        "dynamic_promotions": promoted,
    }


__all__ = [
    "VERSION", "H102_COLUMNS", "score_sector_shock", "analyze_candidate",
    "decorate_decision_tables", "build_t1_review_table_local",
    "build_shadow_tracking_frame", "mark_shadow_record_rows", "export_contract_summary",
]
