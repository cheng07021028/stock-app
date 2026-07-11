# -*- coding: utf-8 -*-
"""Phase 6.1 股神訊號中樞。

目的：
- 把大盤、族群、主流資金、飆股雷達、領漲回補等共用訊號集中在同一個純函式模組。
- 頁面只讀共用欄位與分區，不再各自複製一套分流判斷，降低重複計算與顯示變慢。
- 不連網、不寫 JSON；只讀取既有 DataFrame 或本機既有 JSON 供儀表板顯示。
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable
import json
import math

import pandas as pd

PHASE61_SIGNAL_VERSION = "phase6_2_signal_hub_miss_replay_20260613"

PHASE61_SIGNAL_COLUMNS = [
    "股神同步分區",
    "股神同步優先序",
    "股神同步說明",
    "股神同步版本",
]

ROLE_PRIORITY = {
    "漏選回放校正": 5,
    "漏選原因診斷": 8,
    "已覆蓋雷達": 9,
    "領漲回補雷達": 10,
    "題材轉強追蹤": 20,
    "飆股雷達": 30,
    "高風險爆發觀察": 40,
    "主流攻擊候選": 50,
    "主流突破追蹤": 60,
    "早期潛伏觀察": 70,
    "冷門潛伏觀察": 80,
    "低流動性排除": 90,
    "弱勢觀察": 100,
    "禁止買進": 110,
    "一般觀察": 120,
}

SOURCE_FILES = [
    "godpick_latest_recommendations.json",
    "godpick_recommend_list.json",
    "godpick_records.json",
]


def _blank(v: Any) -> bool:
    try:
        if v is None or pd.isna(v):
            return True
    except Exception:
        pass
    return str(v).strip().lower() in {"", "nan", "none", "null", "--", "-", "<na>", "nat"}


def _text(v: Any) -> str:
    return "" if _blank(v) else str(v).strip()


def _num(v: Any, default: float = 0.0) -> float:
    if _blank(v):
        return default
    try:
        if isinstance(v, str):
            v = v.replace(",", "").replace("%", "").strip()
        x = float(v)
        if math.isnan(x) or math.isinf(x):
            return default
        return x
    except Exception:
        return default


def _series_text(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([""] * len(df), index=df.index, dtype="object")
    return df[col].fillna("").astype(str).str.strip()


def _series_num(df: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
    if col not in df.columns:
        return pd.Series([default] * len(df), index=df.index, dtype="float64")
    return pd.to_numeric(df[col], errors="coerce").fillna(default).astype(float)


def _has_useful_column(df: pd.DataFrame, cols: Iterable[str]) -> bool:
    for c in cols:
        if c in df.columns:
            try:
                if int(df[c].map(lambda x: not _blank(x)).sum()) > 0:
                    return True
            except Exception:
                continue
    return False


def add_phase61_signal_columns(df: pd.DataFrame | None) -> pd.DataFrame:
    """只用既有欄位建立統一分區，不重跑任何引擎。"""
    if df is None:
        return pd.DataFrame(columns=PHASE61_SIGNAL_COLUMNS)
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df)
    if out.empty:
        for c in PHASE61_SIGNAL_COLUMNS:
            if c not in out.columns:
                out[c] = pd.Series(dtype="object")
        return out

    radar_role = _series_text(out, "飆股雷達角色")
    radar_bucket = _series_text(out, "飆股雷達分區")
    leader_role = _series_text(out, "領漲回補角色")
    leader_bucket = _series_text(out, "領漲回補分區")
    main_bucket = _series_text(out, "主流作戰分區")
    week_bucket = _series_text(out, "下週作戰分區")
    rec_role = _series_text(out, "推薦角色")
    cold_warning = _series_text(out, "冷門股警示")
    mainstream_role = _series_text(out, "主流資金角色")
    leader_score = _series_num(out, "主流領漲回補分", 0)
    radar_score = _series_num(out, "爆發雷達分", 0)
    attack_score = _series_num(out, "飆股攻擊分", 0)
    miss_role = _series_text(out, "回放校正角色")
    miss_bucket = _series_text(out, "回放校正分區")
    miss_score = _series_num(out, "漲停回放分", 0)
    miss_risk = _series_num(out, "強勢股漏選風險分", 0)

    buckets: list[str] = []
    priorities: list[int] = []
    notes: list[str] = []
    for idx in out.index:
        lr = str(leader_role.loc[idx]); lb = str(leader_bucket.loc[idx])
        rr = str(radar_role.loc[idx]); rb = str(radar_bucket.loc[idx])
        mb = str(main_bucket.loc[idx]); wb = str(week_bucket.loc[idx]); role = str(rec_role.loc[idx])
        cold = str(cold_warning.loc[idx]); mrole = str(mainstream_role.loc[idx])
        mr_role = str(miss_role.loc[idx]); mr_bucket = str(miss_bucket.loc[idx])
        ls = float(leader_score.loc[idx]); rs = float(radar_score.loc[idx]); atk = float(attack_score.loc[idx])
        ms = float(miss_score.loc[idx]); mrisk = float(miss_risk.loc[idx])

        if "M+｜漲停漏選回放" in mr_role or mr_bucket == "漏選回放校正" or (ms >= 82 and mrisk >= 78):
            b = "漏選回放校正"
            n = f"漲停回放 {ms:.1f} / 漏選風險 {mrisk:.1f}；需檢查候選池、風控、族群與盤前重掃，不等同直接買進。"
        elif "M｜強勢漏選追蹤" in mr_role or mr_bucket == "漏選原因診斷" or (ms >= 72 and mrisk >= 70):
            b = "漏選原因診斷"
            n = f"強勢漏選風險 {mrisk:.1f}；保留於回放診斷，避免下次同型態被早刪。"
        elif "K｜已納入雷達" in mr_role or mr_bucket == "已覆蓋雷達":
            b = "已覆蓋雷達"
            n = f"已由飆股/領漲雷達覆蓋；回放分 {ms:.1f}，後續檢查真強或假強。"
        elif "L+｜領漲回補雷達" in lr or "L｜主流強勢回補" in lr or lb == "領漲回補雷達" or ls >= 78:
            b = "領漲回補雷達"
            n = f"主流領漲回補 {ls:.1f}；盤前/盤中需重掃，不因穩健風控提前刪除。"
        elif "T｜題材轉強追蹤" in lr or lb == "題材轉強追蹤" or (ls >= 66 and "N｜非領漲回補" not in lr):
            b = "題材轉強追蹤"
            n = f"題材轉強回補 {ls:.1f}；等族群與量能確認。"
        elif "S+｜漲停雷達" in rr or "S｜飆股攻擊候選" in rr or "B+｜盤中點火追蹤" in rr or rb == "飆股雷達" or rs >= 78:
            b = "飆股雷達"
            n = f"爆發雷達 {rs:.1f} / 飆股攻擊 {atk:.1f}；只做盤中觸發追蹤，不直接等同買進。"
        elif "R｜高風險爆發觀察" in rr or rb == "高風險爆發觀察":
            b = "高風險爆發觀察"
            n = f"具爆發但風險高；爆發雷達 {rs:.1f}，需小心假突破。"
        elif mb in {"主流攻擊候選", "主流突破追蹤", "早期潛伏觀察", "冷門潛伏觀察", "低流動性排除", "弱勢觀察"}:
            b = mb
            n = f"主流資金分區：{mb}。"
        elif wb in {"下週可進攻名單", "盤中突破追蹤名單", "早期潛伏觀察名單", "弱勢觀察清單", "禁止買進排除名單"}:
            mapping = {
                "下週可進攻名單": "主流攻擊候選",
                "盤中突破追蹤名單": "主流突破追蹤",
                "早期潛伏觀察名單": "早期潛伏觀察",
                "弱勢觀察清單": "弱勢觀察",
                "禁止買進排除名單": "禁止買進",
            }
            b = mapping.get(wb, "一般觀察")
            n = f"下週作戰分區：{wb}。"
        elif "D｜" in role or "禁買" in role or "BLOCK" in role.upper():
            b = "禁止買進"
            n = "推薦角色屬禁買/排除。"
        elif "C-" in role:
            b = "弱勢觀察"
            n = "僅弱勢觀察，不列買進清單。"
        elif cold or "冷門" in mrole or "低流動性" in mrole:
            b = "冷門潛伏觀察"
            n = cold or "冷門或低流動性，需與主流資金股分開。"
        else:
            b = "一般觀察"
            n = "尚未命中 Phase 6.1 主流/飆股/回補分區。"
        buckets.append(b)
        priorities.append(ROLE_PRIORITY.get(b, 999))
        notes.append(n)

    out["股神同步分區"] = buckets
    out["股神同步優先序"] = priorities
    out["股神同步說明"] = notes
    out["股神同步版本"] = PHASE61_SIGNAL_VERSION
    return out


def apply_phase61_signal_hub(df: pd.DataFrame | None, *, compute_missing: bool = True) -> pd.DataFrame:
    """Phase 6.1 共用訊號入口。

    compute_missing=True 時才會套用既有引擎；頁面 8/12 讀歷史資料時可改 False，避免重算。
    """
    if df is None:
        return pd.DataFrame(columns=PHASE61_SIGNAL_COLUMNS)
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df)
    if out.empty:
        return add_phase61_signal_columns(out)

    if compute_missing:
        # 先回補市場領漲，再補大盤/族群/資金/主流/飆股；每個引擎都只處理 DataFrame，不連網不寫檔。
        engine_calls = []
        try:
            from market_leader_replay_engine import apply_market_leader_replay_engine
            engine_calls.append(("市場領漲檢討版本", apply_market_leader_replay_engine))
        except Exception:
            pass
        try:
            from market_regime_engine import apply_market_regime_engine
            engine_calls.append(("大盤情境版本", apply_market_regime_engine))
        except Exception:
            pass
        try:
            from sector_rotation_engine import apply_sector_rotation_engine
            engine_calls.append(("族群輪動版本", apply_sector_rotation_engine))
        except Exception:
            pass
        try:
            from smart_money_engine import apply_smart_money_engine
            engine_calls.append(("智慧資金版本", apply_smart_money_engine))
        except Exception:
            pass
        try:
            from mainstream_money_engine import apply_mainstream_money_engine
            engine_calls.append(("主流資金版本", apply_mainstream_money_engine))
        except Exception:
            pass
        try:
            from limitup_hunter_engine import apply_limitup_hunter_engine
            engine_calls.append(("漲停獵人版本", apply_limitup_hunter_engine))
        except Exception:
            pass
        try:
            from explosive_radar_engine import apply_explosive_radar_engine
            engine_calls.append(("飆股雷達版本", apply_explosive_radar_engine))
        except Exception:
            pass
        try:
            from godpick_miss_replay_engine import apply_godpick_miss_replay_engine
            engine_calls.append(("回放校正版本", apply_godpick_miss_replay_engine))
        except Exception:
            pass

        for version_col, func in engine_calls:
            # 若版本欄與核心輸出已有資料，視為同一資料流已處理，避免 7/匯出/管理中心重複套算。
            try:
                if version_col in out.columns and _has_useful_column(out, [version_col]):
                    continue
                out = func(out)
            except Exception:
                continue

    return add_phase61_signal_columns(out)


def split_phase61_signal_views(df: pd.DataFrame | None) -> dict[str, pd.DataFrame]:
    work = add_phase61_signal_columns(df)
    views: dict[str, pd.DataFrame] = {}
    order = [
        "漏選回放校正", "漏選原因診斷", "已覆蓋雷達",
        "領漲回補雷達", "題材轉強追蹤", "飆股雷達", "高風險爆發觀察", "主流攻擊候選", "主流突破追蹤",
        "早期潛伏觀察", "冷門潛伏觀察", "低流動性排除", "弱勢觀察", "禁止買進", "一般觀察",
    ]
    for name in order:
        if "股神同步分區" not in work.columns:
            views[name] = pd.DataFrame()
        else:
            part = work[work["股神同步分區"].astype(str).eq(name)].copy()
            sort_cols = [c for c in ["股神同步優先序", "主流領漲回補分", "爆發雷達分", "飆股攻擊分", "主流資金分", "推薦總分", "成交額百萬"] if c in part.columns]
            if sort_cols and not part.empty:
                ascending = [True] + [False] * (len(sort_cols) - 1)
                try:
                    part = part.sort_values(sort_cols, ascending=ascending, kind="mergesort")
                except Exception:
                    pass
            views[name] = part.reset_index(drop=True)
    return views


def build_phase61_summary(df: pd.DataFrame | None) -> dict[str, Any]:
    work = add_phase61_signal_columns(df)
    if work.empty:
        return {"version": PHASE61_SIGNAL_VERSION, "rows": 0, "bucket_counts": {}, "top_sectors": [], "market_mode": "無資料"}
    bucket_counts = work.get("股神同步分區", pd.Series(dtype="object")).astype(str).value_counts().to_dict()
    market_mode = _text(work.get("大盤攻擊模式", pd.Series([""])).iloc[0] if "大盤攻擊模式" in work.columns and len(work) else "") or "未同步"
    sector_col = "類別" if "類別" in work.columns else ("產業" if "產業" in work.columns else "")
    top_sectors: list[dict[str, Any]] = []
    if sector_col:
        tmp = work.copy()
        score = _series_num(tmp, "族群攻擊強度", 50)
        radar = _series_num(tmp, "爆發雷達分", 0)
        tmp["_score"] = (score * 0.6 + radar * 0.4).round(1)
        g = tmp.groupby(sector_col, dropna=False).agg(檔數=(sector_col, "size"), 平均強度=("_score", "mean"))
        g = g.sort_values(["平均強度", "檔數"], ascending=[False, False]).head(8)
        top_sectors = [{"族群": str(idx), "檔數": int(row["檔數"]), "平均強度": round(float(row["平均強度"]), 1)} for idx, row in g.iterrows()]
    return {
        "version": PHASE61_SIGNAL_VERSION,
        "rows": int(len(work)),
        "bucket_counts": {str(k): int(v) for k, v in bucket_counts.items()},
        "top_sectors": top_sectors,
        "market_mode": market_mode,
    }


def _extract_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ["records", "items", "data", "rows", "recommendations", "latest"]:
            rows = payload.get(key)
            if isinstance(rows, list):
                return [x for x in rows if isinstance(x, dict)]
        # 有些檔案以股票代號當 key。
        if payload and all(isinstance(v, dict) for v in payload.values()):
            return [dict(v) for v in payload.values()]
    return []


def load_latest_godpick_frame(base_dir: str | Path = ".", *, max_rows: int = 500) -> pd.DataFrame:
    """讀取既有推薦 JSON 做 0/5/12 的輔助顯示；不寫回任何資料。"""
    base = Path(base_dir)
    frames: list[pd.DataFrame] = []
    for name in SOURCE_FILES:
        p = base / name
        if not p.exists():
            continue
        try:
            payload = json.loads(p.read_text(encoding="utf-8"))
            rows = _extract_rows(payload)
            if rows:
                part = pd.DataFrame(rows)
                part["資料來源檔"] = name
                frames.append(part)
        except Exception:
            continue
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True, sort=False)
    if "股票代號" in out.columns:
        out["股票代號"] = out["股票代號"].astype(str).str.strip().str.replace(".0", "", regex=False)
        out = out[out["股票代號"].ne("")]
        # 優先保留越新的資料列。
        sort_cols = [c for c in ["推薦日期", "推薦時間", "追蹤更新時間", "最新更新時間"] if c in out.columns]
        if sort_cols:
            try:
                out = out.sort_values(sort_cols, ascending=[False] * len(sort_cols), kind="mergesort")
            except Exception:
                pass
        out = out.drop_duplicates(subset=["股票代號"], keep="first")
    return out.head(max_rows).reset_index(drop=True)


def compact_signal_table(df: pd.DataFrame | None, *, max_rows: int = 30) -> pd.DataFrame:
    work = add_phase61_signal_columns(df)
    if work.empty:
        return work
    cols = [
        "股神同步分區", "股票代號", "股票名稱", "類別", "產業", "推薦角色", "飆股雷達角色", "領漲回補角色", "回放校正角色",
        "主流作戰分區", "推薦總分", "股神實戰總分", "爆發雷達分", "主流領漲回補分", "漲停回放分", "強勢股漏選風險分", "族群攻擊強度", "主流資金分", "成交額百萬", "盤中轉強觸發價", "股神同步說明",
    ]
    cols = [c for c in cols if c in work.columns]
    sort_cols = [c for c in ["股神同步優先序", "漲停回放分", "強勢股漏選風險分", "主流領漲回補分", "爆發雷達分", "推薦總分"] if c in work.columns]
    if sort_cols:
        try:
            work = work.sort_values(sort_cols, ascending=[True] + [False] * (len(sort_cols) - 1), kind="mergesort")
        except Exception:
            pass
    return work[cols].head(max_rows).reset_index(drop=True)
