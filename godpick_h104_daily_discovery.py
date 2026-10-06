# -*- coding: utf-8 -*-
"""Bounded candidate discovery and persistent recommendation continuity.

H107 update
-----------
The original H104 continuity logic depended almost entirely on ``godpick_records.json``.
When recent recommendation rows were not persisted there, every run became
``待確認｜缺少可比較歷史`` and the system could not know that the same stock or
same sector had already been shown repeatedly.

This revision keeps the original interfaces but adds a small, independent,
append-only daily discovery authority: ``godpick_discovery_history.json``.
It is created at runtime and is never shipped inside the patch ZIP.

The history is research governance only.  It never creates buy authority.
"""
from __future__ import annotations

from pathlib import Path
from datetime import date, timedelta
from typing import Any
import json
import os
import tempfile
import pandas as pd

VERSION = "v191_h107_persistent_discovery_continuity_20261006"
DISCOVERY_HISTORY_FILE = "godpick_discovery_history.json"
MAX_HISTORY_ROWS = 12000
KEEP_HISTORY_DAYS = 120

COLUMNS = [
    "H104版本",
    "H104推薦性質",
    "H104先前入選日數",
    "H104前次資料日",
    "H104前次優先分",
    "H104優先分變化",
    "H104前次黑馬分",
    "H104黑馬分變化",
    "H104近期族群入選日數",
    "H104延續說明",
    "H104歷史範圍",
]

SCORES = [
    "H79研究推薦分",
    "H89雙軌研究排序分",
    "V188股神作戰優先分",
    "股神推薦優先分",
    "候選強度分",
    "推薦總分",
]

_BLANK = {"", "nan", "none", "null", "nat", "--", "-", "<na>"}


def text(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    s = str(v).strip()
    return "" if s.lower() in _BLANK else s


def number(v: Any, default: float | None = None) -> float | None:
    if v is None or isinstance(v, bool):
        return default
    try:
        if isinstance(v, str):
            v = v.replace(",", "").replace("％", "%").replace("%", "").replace("+", "").strip()
            if not v or v.lower() in _BLANK:
                return default
        x = float(v)
        return x if pd.notna(x) else default
    except Exception:
        return default


def _code(v: Any) -> str:
    s = text(v)
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    return s.zfill(4) if s.isdigit() and len(s) < 4 else s


def select_discovery_input(frame, limit: int = 240):
    """60% global leaders + 40% category round-robin to widen future discovery.

    H107 intentionally gives more capacity to sectors that are not yet dominating the
    legacy ranking. Every selected row still faces all downstream risk/evidence gates.
    """
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return pd.DataFrame()
    out = frame.copy()
    if "股票代號" not in out:
        return out.head(limit)
    out["股票代號"] = out["股票代號"].map(lambda x: text(x).removesuffix(".0"))
    out = out.loc[out["股票代號"].ne("")].copy()
    keys = []
    for i, c in enumerate(SCORES):
        if c in out:
            k = f"__h104_score{i}"
            out[k] = pd.to_numeric(out[c], errors="coerce")
            keys.append(k)
    out = out.sort_values(
        keys + ["股票代號"],
        ascending=[False] * len(keys) + [True],
        kind="stable",
        na_position="last",
    ).drop_duplicates("股票代號")
    if len(out) > limit and "類別" in out:
        main = out.head(max(1, int(limit * .60)))
        remaining = out.loc[~out["股票代號"].isin(main["股票代號"])].copy()
        remaining["__h104_sector_rank"] = remaining.groupby(remaining["類別"].fillna("未分類")).cumcount()
        remaining = remaining.sort_values(
            ["__h104_sector_rank"] + keys + ["股票代號"],
            ascending=[True] + [False] * len(keys) + [True],
            kind="stable",
            na_position="last",
        )
        out = pd.concat([main, remaining.head(limit - len(main))], ignore_index=True)
    return out.head(limit).drop(columns=[c for c in out if c.startswith("__h104_")]).reset_index(drop=True)


def _day(row: dict[str, Any]) -> str:
    # Market observation date is distinct from run/export time.
    for key in (
        "H106資料日", "H105資料日", "H99市場資料日", "市場資料日期", "最新K線日期",
        "推薦日期", "資料日", "market_date",
    ):
        s = text(row.get(key))[:10]
        try:
            return date.fromisoformat(s).isoformat()
        except ValueError:
            pass
    ts = text(row.get("推薦批次時間"))[:10]
    try:
        return date.fromisoformat(ts).isoformat()
    except ValueError:
        return ""


def _is_relevant_record(r: dict[str, Any]) -> bool:
    if not isinstance(r, dict) or not _code(r.get("股票代號")) or not _day(r):
        return False
    # New discovery snapshots have explicit marker and always qualify.
    if text(r.get("H104歷史來源")) == "DISCOVERY_SNAPSHOT":
        return True
    layer = " ".join([
        text(r.get("H103最終層別")), text(r.get("紀錄層級")), text(r.get("候選性質")),
        text(r.get("正式推薦分區")), text(r.get("H105推薦性質")), text(r.get("H104推薦性質")),
        text(r.get("推薦模式")), text(r.get("推薦型態")),
    ])
    if "影子" in layer:
        return False
    positive_tokens = ("ACTIONABLE", "RESEARCH", "研究", "正式", "推薦", "黑馬", "雷達", "WAITING")
    return any(tok in layer.upper() if tok.isascii() else tok in layer for tok in positive_tokens)


def _read_json_rows(path: Path) -> list[dict[str, Any]]:
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError, AttributeError, TypeError):
        return []
    rows = data if isinstance(data, list) else data.get("records", []) if isinstance(data, dict) else []
    return [r for r in rows if isinstance(r, dict)]


def load_history(base_dir):
    """Load recent decision history from both legacy records and H107 snapshot authority."""
    base = Path(base_dir)
    combined: list[dict[str, Any]] = []
    for name in ("godpick_records.json", DISCOVERY_HISTORY_FILE):
        combined.extend(_read_json_rows(base / name))
    relevant = [r for r in combined if _is_relevant_record(r)]
    # De-duplicate identical date/code/pool snapshots, preferring the later source row.
    dedup: dict[tuple[str, str, str], dict[str, Any]] = {}
    for r in relevant:
        key = (_day(r), _code(r.get("股票代號")), text(r.get("H104歷史池")) or text(r.get("H103最終層別")) or "")
        dedup[key] = r
    return sorted(dedup.values(), key=lambda r: (_day(r), _code(r.get("股票代號"))))


def continuity(row, history):
    day = _day(row)
    code = _code(row.get("股票代號"))
    category = text(row.get("類別"))
    cutoff = (date.fromisoformat(day) - timedelta(days=14)).isoformat() if day else ""
    past = [r for r in history if cutoff <= _day(r) < day and _day(r)] if day else []
    matches = [r for r in past if _code(r.get("股票代號")) == code]
    matches.sort(key=lambda r: (_day(r), text(r.get("推薦批次時間"))), reverse=True)
    prev = matches[0] if matches else {}

    old = number(prev.get("H102動態優先分"))
    cur = number(row.get("H102動態優先分"))
    delta = round(cur - old, 2) if cur is not None and old is not None else None

    old_bh = number(prev.get("H105黑馬預發動分"))
    cur_bh = number(row.get("H105黑馬預發動分"))
    bh_delta = round(cur_bh - old_bh, 2) if cur_bh is not None and old_bh is not None else None

    sector_days = len({_day(r) for r in past if category and text(r.get("類別")) == category})
    known_days = len({_day(r) for r in past})
    repeat_days = len({_day(r) for r in matches})

    if not past:
        kind = "歷史權威尚未建立｜本輪建立基準"
        note = "前14日尚無可比較的已保存每日發現快照；本輪會建立H107 discovery history，下一交易日起可判斷重複推薦。"
    elif not matches:
        kind = "近期首次觀察｜已知歷史未見"
        note = "在已保存的前14日每日發現快照中未見此股票；仍不等於全市場首次。"
    else:
        kind = "延續追蹤｜非首次推薦"
        note = "前次已入選；延續標記不授予研究或買進資格。"
        if bh_delta is not None and bh_delta >= 5:
            kind = "延續追蹤｜黑馬分顯著改善"
            note += " 黑馬預發動分較前次提高至少5分，可保留但不得冒充全新機會。"
        elif delta is not None and delta >= 3:
            kind = "延續追蹤｜排序指標改善"
            note += " 優先分提高至少3分，僅為模型指標改善。"
        elif (bh_delta is not None and bh_delta <= -4) or (delta is not None and delta <= -3):
            note += " 指標較前次轉弱，應降低重複曝光。"
        else:
            note += " 尚無足夠改善證據，應受重複推薦懲罰。"

    values = [
        VERSION, kind, repeat_days, _day(prev), old, delta, old_bh, bh_delta, sector_days,
        note, f"{cutoff} ≤ 資料日 < {day}；已知{known_days}個日期",
    ]
    return dict(zip(COLUMNS, values))


def _snapshot_rows(tables: dict[str, Any], market_day: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    pools = ("blackhorse_overview", "research", "waiting", "actionable", "early_radar")
    for pool in pools:
        frame = tables.get(pool)
        if not isinstance(frame, pd.DataFrame) or frame.empty or "股票代號" not in frame.columns:
            continue
        limit = 40 if pool in ("blackhorse_overview", "early_radar") else 20
        for rank, r in enumerate(frame.head(limit).to_dict("records"), 1):
            code = _code(r.get("股票代號"))
            if not code:
                continue
            row = {
                "H104歷史來源": "DISCOVERY_SNAPSHOT",
                "H104歷史池": pool,
                "H104歷史順位": rank,
                "股票代號": code,
                "股票名稱": text(r.get("股票名稱")),
                "類別": text(r.get("類別")),
                "市場別": text(r.get("市場別")),
                "推薦日期": market_day,
                "H99市場資料日": market_day,
                "H102動態優先分": number(r.get("H102動態優先分")),
                "H105黑馬預發動分": number(r.get("H105黑馬預發動分")),
                "H105推薦性質": text(r.get("H105推薦性質")),
                "H105發動階段": text(r.get("H105發動階段")),
                "H105今日強勢排除": text(r.get("H105今日強勢排除")),
            }
            rows.append(row)
    return rows


def save_discovery_snapshot(base_dir, tables: dict[str, Any]) -> dict[str, Any]:
    """Persist a small daily snapshot atomically for repeat/sector continuity.

    Safe properties:
    * no session state authority;
    * append + de-duplicate only;
    * does not touch ``godpick_records.json``;
    * runtime data file is intentionally excluded from patch ZIPs.
    """
    base = Path(base_dir)
    market_day = ""
    for pool in ("blackhorse_overview", "research", "waiting", "actionable"):
        frame = tables.get(pool)
        if isinstance(frame, pd.DataFrame) and not frame.empty:
            for r in frame.to_dict("records")[:5]:
                market_day = _day(r)
                if market_day:
                    break
        if market_day:
            break
    if not market_day:
        market_day = date.today().isoformat()

    new_rows = _snapshot_rows(tables, market_day)
    if not new_rows:
        return {"ok": True, "written": 0, "market_day": market_day, "path": str(base / DISCOVERY_HISTORY_FILE)}

    path = base / DISCOVERY_HISTORY_FILE
    existing = _read_json_rows(path)
    merged = existing + new_rows

    # Keep only a bounded rolling history.
    try:
        cutoff = (date.fromisoformat(market_day) - timedelta(days=KEEP_HISTORY_DAYS)).isoformat()
        merged = [r for r in merged if not _day(r) or _day(r) >= cutoff]
    except ValueError:
        pass

    dedup: dict[tuple[str, str, str], dict[str, Any]] = {}
    for r in merged:
        if not isinstance(r, dict):
            continue
        key = (_day(r), _code(r.get("股票代號")), text(r.get("H104歷史池")))
        if key[0] and key[1]:
            dedup[key] = r
    final_rows = sorted(dedup.values(), key=lambda r: (_day(r), text(r.get("H104歷史池")), _code(r.get("股票代號"))))[-MAX_HISTORY_ROWS:]

    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(final_rows, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, path)
    finally:
        try:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
        except OSError:
            pass
    return {"ok": True, "written": len(new_rows), "rows": len(final_rows), "market_day": market_day, "path": str(path)}


__all__ = [
    "VERSION", "COLUMNS", "DISCOVERY_HISTORY_FILE", "select_discovery_input", "continuity",
    "load_history", "save_discovery_snapshot", "text", "number",
]
