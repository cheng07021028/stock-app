# -*- coding: utf-8 -*-
"""God Pick recommendation rotation / repeat-quality guard.

This module keeps a lightweight history of the ranked recommendation view and
uses it to distinguish two very different situations:

1. A real market leader remains strong and deserves to stay visible.
2. A stock stays near the top only because structural/mainstream/liquidity
   factors are sticky, while no new price/volume/trigger evidence has appeared.

The guard never turns a weak stock into a buy recommendation.  It only applies
small ranking adjustments, and repeated names with fresh evidence remain
eligible.  The history file is intentionally small and is not a replacement for
``godpick_records.json`` (the performance authority file).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable
import hashlib
import json
import math
import os
import tempfile

import pandas as pd

ROTATION_GUARD_VERSION = "phase108_rotation_event_memory_v1_20260807"
ROTATION_HISTORY_FILE = "godpick_rotation_history.json"
ROTATION_MAX_DATES = 30
ROTATION_SNAPSHOT_TOP_N = 30

ROTATION_COLUMNS = [
    "今日訊號新鮮分",
    "近5次入榜次數",
    "連續入榜次數",
    "前次推薦排名",
    "前次推薦優先分",
    "本次分數變化",
    "重複推薦校正分",
    "推薦輪動狀態",
    "今日新進榜",
    "重複推薦說明",
    "股神推薦優先分_輪動前",
    "推薦輪動版本",
    "推薦事件類型",
    "是否新增推薦事件",
    "本日新增證據",
    "類股集中校正分",
    "類股集中說明",
]

_NUMERIC_ROTATION_COLUMNS = {
    "今日訊號新鮮分",
    "近5次入榜次數",
    "連續入榜次數",
    "前次推薦排名",
    "前次推薦優先分",
    "本次分數變化",
    "重複推薦校正分",
    "股神推薦優先分_輪動前",
    "類股集中校正分",
}

_BLANKS = {"", "none", "nan", "nat", "null", "<na>", "--", "-"}


def _safe_str(value: Any) -> str:
    try:
        if value is None:
            return ""
        text = str(value).strip()
        return "" if text.lower() in _BLANKS else text
    except Exception:
        return ""


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
        if not math.isfinite(result):
            return float(default)
        return result
    except Exception:
        return float(default)


def _code(value: Any) -> str:
    text = _safe_str(value)
    if not text:
        return ""
    if text.endswith(".0"):
        text = text[:-2]
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.zfill(4) if digits else text


def _date_text(value: Any) -> str:
    text = _safe_str(value)[:10]
    if len(text) == 10 and text[4:5] == "-" and text[7:8] == "-":
        return text
    return ""


def _project_root(base_dir: str | Path | None = None) -> Path:
    if base_dir:
        return Path(base_dir).resolve()
    return Path(__file__).resolve().parent


def _history_path(base_dir: str | Path | None = None) -> Path:
    return _project_root(base_dir) / ROTATION_HISTORY_FILE


def _file_signature(path: Path) -> str:
    try:
        stat = path.stat()
        return f"{path}:{stat.st_mtime_ns}:{stat.st_size}"
    except Exception:
        return f"{path}:missing"


def _normalise_history(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        rows = payload.get("snapshots") or payload.get("history") or payload.get("data") or []
    else:
        rows = payload
    if not isinstance(rows, list):
        return []
    out: list[dict[str, Any]] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        scan_date = _date_text(item.get("date") or item.get("scan_date") or item.get("交易日"))
        entries = item.get("rows") or item.get("entries") or item.get("ranked") or []
        if not scan_date or not isinstance(entries, list):
            continue
        clean_entries: list[dict[str, Any]] = []
        seen: set[str] = set()
        for row in entries:
            if not isinstance(row, dict):
                continue
            code = _code(row.get("股票代號") or row.get("code"))
            if not code or code in seen:
                continue
            seen.add(code)
            clean_entries.append({
                "股票代號": code,
                "股票名稱": _safe_str(row.get("股票名稱") or row.get("name")),
                "股神推薦總排名": int(_safe_float(row.get("股神推薦總排名") or row.get("rank"), 0)),
                "股神推薦優先分": round(_safe_float(row.get("股神推薦優先分") or row.get("score"), 0), 1),
                "正式推薦分區": _safe_str(row.get("正式推薦分區") or row.get("bucket")),
                "股神推薦用途": _safe_str(row.get("股神推薦用途") or row.get("usage")),
                "類別": _safe_str(row.get("類別") or row.get("category")),
                "主流主升優先分": round(_safe_float(row.get("主流主升優先分"), 0), 1),
                "今日訊號新鮮分": round(_safe_float(row.get("今日訊號新鮮分"), 0), 1),
                "最新價": _safe_float(row.get("最新價"), 0),
                "實戰觸發價": _safe_float(row.get("實戰觸發價"), 0),
                "AI新證據分": round(_safe_float(row.get("AI新證據分"), 0), 1),
                "AI跨市場新強股分": round(_safe_float(row.get("AI跨市場新強股分"), 0), 1),
                "型態突破分數": round(_safe_float(row.get("型態突破分數"), 0), 1),
                "當日量比": round(_safe_float(row.get("當日量比"), 0), 2),
                "今日漲幅%": round(_safe_float(row.get("今日漲幅%"), 0), 2),
                "正式推薦資格": _safe_str(row.get("正式推薦資格")),
                "推薦事件類型": _safe_str(row.get("推薦事件類型")),
            })
        out.append({"date": scan_date, "saved_at": _safe_str(item.get("saved_at")), "rows": clean_entries})
    dedup: dict[str, dict[str, Any]] = {}
    for item in out:
        dedup[item["date"]] = item
    return [dedup[d] for d in sorted(dedup)]


@lru_cache(maxsize=16)
def _load_history_cached(path_text: str, signature: str) -> tuple[dict[str, Any], ...]:
    del signature
    path = Path(path_text)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return tuple()
    return tuple(_normalise_history(payload))




def _bootstrap_history_from_records(base_dir: str | Path | None = None) -> list[dict[str, Any]]:
    """Create a compact initial history from the existing authority records.

    Newer records may already contain ``股神推薦總排名``. Older records do
    not, so the bootstrap ranks each date by the best available recommendation
    score and keeps only the top 50. This runs only when the compact rotation
    file does not exist, then future scans overwrite/add exact daily snapshots.
    """
    root = _project_root(base_dir)
    records_path = root / "godpick_records.json"
    try:
        payload = json.loads(records_path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(payload, dict):
        records = payload.get("records") or payload.get("data") or payload.get("items") or []
    else:
        records = payload
    if not isinstance(records, list):
        return []

    by_date: dict[str, list[dict[str, Any]]] = {}
    for raw in records:
        if not isinstance(raw, dict):
            continue
        rec_date = _date_text(raw.get("推薦日期") or raw.get("K線最後交易日") or raw.get("建立時間"))
        code = _code(raw.get("股票代號"))
        if not rec_date or not code:
            continue
        score = 0.0
        for col in ("股神推薦優先分", "股神實戰總分", "推薦總分", "推薦分數", "候補排序分"):
            score = _safe_float(raw.get(col), 0)
            if score > 0:
                break
        rank = int(_safe_float(raw.get("股神推薦總排名"), 0))
        by_date.setdefault(rec_date, []).append({
            "股票代號": code,
            "股票名稱": _safe_str(raw.get("股票名稱")),
            "股神推薦總排名": rank,
            "股神推薦優先分": round(score, 1),
            "正式推薦分區": _safe_str(raw.get("正式推薦分區")),
            "股神推薦用途": _safe_str(raw.get("股神推薦用途") or raw.get("推薦用途")),
            "類別": _safe_str(raw.get("類別")),
            "主流主升優先分": round(_safe_float(raw.get("主流主升優先分"), 0), 1),
            "今日訊號新鮮分": round(_safe_float(raw.get("今日訊號新鮮分"), 0), 1),
            "最新價": _safe_float(raw.get("最新價") or raw.get("推薦價格"), 0),
            "實戰觸發價": _safe_float(raw.get("實戰觸發價"), 0),
            "AI新證據分": round(_safe_float(raw.get("AI新證據分"), 0), 1),
            "AI跨市場新強股分": round(_safe_float(raw.get("AI跨市場新強股分"), 0), 1),
            "型態突破分數": round(_safe_float(raw.get("型態突破分數"), 0), 1),
            "當日量比": round(_safe_float(raw.get("當日量比"), 0), 2),
            "今日漲幅%": round(_safe_float(raw.get("今日漲幅%"), 0), 2),
            "正式推薦資格": _safe_str(raw.get("正式推薦資格")),
            "推薦事件類型": _safe_str(raw.get("推薦事件類型")),
        })

    snapshots: list[dict[str, Any]] = []
    for rec_date in sorted(by_date)[-ROTATION_MAX_DATES:]:
        dedup: dict[str, dict[str, Any]] = {}
        for item in by_date[rec_date]:
            code = item["股票代號"]
            old = dedup.get(code)
            if old is None or item["股神推薦優先分"] > old["股神推薦優先分"]:
                dedup[code] = item
        rows = list(dedup.values())
        rows.sort(key=lambda item: (
            item["股神推薦總排名"] if item["股神推薦總排名"] > 0 else 9999,
            -item["股神推薦優先分"],
        ))
        rows = rows[:ROTATION_SNAPSHOT_TOP_N]
        for pos, item in enumerate(rows, start=1):
            if int(item.get("股神推薦總排名") or 0) <= 0:
                item["股神推薦總排名"] = pos
        if rows:
            snapshots.append({"date": rec_date, "saved_at": "authority-bootstrap", "rows": rows})
    return snapshots


def load_rotation_history(base_dir: str | Path | None = None) -> list[dict[str, Any]]:
    path = _history_path(base_dir)
    history = [dict(item) for item in _load_history_cached(str(path), _file_signature(path))]
    if history:
        return history

    # V177：本機快照遺失/重啟時先讀永久層，不再把近期推薦全部誤判為 NEW。
    # 只在本機無有效 history 時做遠端讀取，避免正常頁面每次 rerun 都打網路。
    try:
        from godpick_persistence_service import load_named_json_permanent
        payload, _messages = load_named_json_permanent(
            ROTATION_HISTORY_FILE, {}, firestore_doc="godpick_rotation_history"
        )
        remote_history = _normalise_history(payload)
        if remote_history:
            _atomic_write_json(path, {
                "version": ROTATION_GUARD_VERSION,
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "restored_from_permanent": True,
                "snapshots": remote_history,
            })
            _load_history_cached.cache_clear()
            return remote_history
    except Exception:
        pass

    history = _bootstrap_history_from_records(base_dir)
    if history:
        try:
            _atomic_write_json(path, {
                "version": ROTATION_GUARD_VERSION,
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "bootstrap_source": "godpick_records.json",
                "snapshots": history,
            })
            _load_history_cached.cache_clear()
        except Exception:
            pass
    return history


def _atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2, default=str)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp_name, path)
    finally:
        try:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)
        except Exception:
            pass


def _current_scan_date(df: pd.DataFrame) -> str:
    for col in ["K線最後交易日", "本輪市場最新交易日", "大盤資料日期", "推薦日期"]:
        if col not in df.columns:
            continue
        dates = [_date_text(v) for v in df[col].tolist()]
        dates = [d for d in dates if d]
        if dates:
            return max(dates)
    return date.today().isoformat()


def _contains(text: Any, words: Iterable[str]) -> bool:
    source = _safe_str(text)
    return any(word in source for word in words)


def _signal_freshness_score(row: pd.Series) -> float:
    """Current-day evidence score; structural leadership alone is insufficient."""
    momentum = _safe_str(row.get("強勢動能判定"))
    prebreak = _safe_str(row.get("強勢前兆判定"))
    trigger_quality = _safe_float(row.get("隔日觸發品質分"), 0)
    mainrise = _safe_float(row.get("主流主升優先分"), 0)
    close_pos = _safe_float(row.get("當日收盤位置%"), 50)
    upper = _safe_float(row.get("上影線比例%"), 0)
    ret1 = _safe_float(row.get("今日漲幅%"), 0)
    volume_score = _safe_float(row.get("人氣量能分"), _safe_float(row.get("量能確認分"), 50))

    score = 30.0
    if _contains(momentum, ["PASS", "READY", "M｜", "M+"]):
        score += 24.0
    elif _contains(momentum, ["BLOCK"]):
        score -= 5.0
    if _contains(prebreak, ["PASS", "READY"]):
        score += 20.0
    elif _contains(prebreak, ["MARKET-WAIT-P"]):
        score += 11.0
    elif _contains(prebreak, ["BLOCK-P"]):
        score -= 4.0

    if 0.8 <= ret1 <= 5.5:
        score += 13.0
    elif 0.0 <= ret1 < 0.8:
        score += 5.0
    elif ret1 < -2.5:
        score -= 12.0
    elif ret1 > 7.5:
        score -= 8.0

    if close_pos >= 80:
        score += 13.0
    elif close_pos >= 65:
        score += 7.0
    elif close_pos < 40:
        score -= 10.0
    if upper > 45:
        score -= 10.0
    elif upper > 28:
        score -= 5.0

    if trigger_quality >= 70:
        score += 13.0
    elif trigger_quality >= 60:
        score += 8.0
    elif trigger_quality >= 55:
        score += 3.0
    else:
        score -= 5.0
    if volume_score >= 75:
        score += 8.0
    elif volume_score < 35:
        score -= 5.0
    if mainrise >= 80:
        score += 5.0
    return round(max(0.0, min(100.0, score)), 1)


def _history_metrics(history: list[dict[str, Any]], current_date: str) -> tuple[list[str], dict[str, list[dict[str, Any]]]]:
    prior = [item for item in history if item.get("date") and item["date"] < current_date]
    prior = sorted(prior, key=lambda item: item["date"])[-5:]
    dates = [item["date"] for item in prior]
    by_code: dict[str, list[dict[str, Any]]] = {}
    for item in prior:
        for row in item.get("rows", []):
            code = _code(row.get("股票代號"))
            if not code:
                continue
            copy_row = dict(row)
            copy_row["date"] = item["date"]
            by_code.setdefault(code, []).append(copy_row)
    return dates, by_code


def _repeat_adjustment(row: pd.Series, appearances: list[dict[str, Any]], prior_dates: list[str]) -> dict[str, Any]:
    current_score = _safe_float(row.get("股神推薦優先分"), 0)
    signal = _signal_freshness_score(row)
    present_dates = {item.get("date") for item in appearances}
    count = sum(1 for d in prior_dates if d in present_dates)
    consecutive = 0
    for d in reversed(prior_dates):
        if d in present_dates:
            consecutive += 1
        else:
            break

    last = sorted(appearances, key=lambda x: x.get("date", ""))[-1] if appearances else {}
    last_rank = int(_safe_float(last.get("股神推薦總排名"), 0))
    last_score = _safe_float(last.get("股神推薦優先分"), 0)
    delta = current_score - last_score if last_score > 0 else 0.0

    # V177 Recommendation Event：同一檔股票「仍然很強」不等於今天又是一筆新推薦。
    # 只有出現可量化的新證據（訊號、新強股分、突破/量價、分數躍升）才視為再確認事件。
    last_signal = _safe_float(last.get("今日訊號新鮮分"), 0)
    ai_fresh = _safe_float(row.get("AI新證據分"), signal)
    last_ai_fresh = _safe_float(last.get("AI新證據分"), 0)
    cross = _safe_float(row.get("AI跨市場新強股分"), 0)
    last_cross = _safe_float(last.get("AI跨市場新強股分"), 0)
    breakout = _safe_float(row.get("型態突破分數"), 0)
    last_breakout = _safe_float(last.get("型態突破分數"), 0)
    vol_ratio = _safe_float(row.get("當日量比"), 0)
    last_price = _safe_float(last.get("最新價"), 0)
    current_price = _safe_float(row.get("最新價"), 0)
    price_delta_pct = ((current_price / last_price - 1.0) * 100.0) if last_price > 0 and current_price > 0 else 0.0

    evidence: list[str] = []
    if signal >= 62 and signal >= last_signal + 10:
        evidence.append(f"訊號新鮮度+{signal-last_signal:.0f}")
    if ai_fresh >= 68 and ai_fresh >= last_ai_fresh + 8:
        evidence.append(f"AI新證據+{ai_fresh-last_ai_fresh:.0f}")
    if cross >= 78 and cross >= last_cross + 7:
        evidence.append(f"跨市場強度+{cross-last_cross:.0f}")
    if breakout >= 72 and breakout >= last_breakout + 8:
        evidence.append("突破結構升級")
    if delta >= 4.0:
        evidence.append(f"推薦優先分+{delta:.1f}")
    if 1.5 <= price_delta_pct <= 7.5 and vol_ratio >= 1.15:
        evidence.append(f"價量續強{price_delta_pct:+.1f}%")

    is_new_event = (count == 0) or bool(evidence)

    penalty = 0.0
    if count == 3:
        penalty += 1.5
    elif count == 4:
        penalty += 3.5
    elif count >= 5:
        penalty += 5.5
    if consecutive >= 3:
        penalty += min(3.0, float(consecutive - 2))
    if count >= 3 and signal < 50:
        penalty += 3.0
    if count >= 1 and not is_new_event:
        penalty += 2.5 if count < 3 else 4.0
    if delta <= -4 and count >= 2:
        penalty += 2.0
    elif delta >= 4:
        penalty *= 0.5

    mainrise = _safe_float(row.get("主流主升優先分"), 0)
    mainstream = _safe_float(row.get("主流資金分"), 0)
    sector = _safe_float(row.get("族群攻擊強度"), _safe_float(row.get("族群熱度分數"), 0))
    amount = _safe_float(row.get("成交額百萬"), _safe_float(row.get("流動性參考成交額百萬"), 0))
    true_leader = mainrise >= 78 and mainstream >= 76 and sector >= 72 and amount >= 500 and signal >= 58
    if true_leader and is_new_event:
        penalty = min(penalty, 2.5)
    elif true_leader and not is_new_event:
        penalty = max(penalty, 3.5)

    structure_only = mainrise >= 75 and signal < 50
    if structure_only:
        penalty = max(penalty, 4.0)

    bonus = 0.0
    if count == 0 and signal >= 65:
        bonus = 2.5
    elif count <= 1 and signal >= 72:
        bonus = 1.0

    adjustment = round(bonus - min(10.0, penalty), 1)
    if count == 0:
        status = "NEW｜今日新進榜" if signal >= 60 else "NEW-WAIT｜新進但訊號待確認"
        event_type = "NEW｜新發現"
    elif is_new_event and true_leader:
        status = "LEADER-RECONFIRM｜領漲股有新證據"
        event_type = "RECONFIRM｜新證據再確認"
    elif is_new_event:
        status = "RECONFIRM｜重複股但今日有新證據"
        event_type = "RECONFIRM｜新證據再確認"
    elif count >= 3:
        status = "STICKY-BLOCK｜重複且缺少新證據"
        event_type = "TRACK｜既有邏輯續追"
    else:
        status = "TRACK-ONLY｜既有邏輯續追，非新推薦"
        event_type = "TRACK｜既有邏輯續追"

    reason = (
        f"近5次入榜{count}次、連續{consecutive}次、今日新訊號{signal:.1f}、"
        f"前次排名{last_rank or '-'}、分數變化{delta:+.1f}、輪動校正{adjustment:+.1f}"
    )
    return {
        "今日訊號新鮮分": signal,
        "近5次入榜次數": count,
        "連續入榜次數": consecutive,
        "前次推薦排名": last_rank,
        "前次推薦優先分": round(last_score, 1),
        "本次分數變化": round(delta, 1),
        "重複推薦校正分": adjustment,
        "推薦輪動狀態": status,
        "今日新進榜": "是" if count == 0 else "否",
        "重複推薦說明": reason,
        "推薦輪動版本": ROTATION_GUARD_VERSION,
        "推薦事件類型": event_type,
        "是否新增推薦事件": "是" if is_new_event else "否",
        "本日新增證據": "、".join(evidence) if evidence else ("首次進榜" if count == 0 else "無足夠新增證據"),
        "類股集中校正分": 0.0,
        "類股集中說明": "",
    }


def apply_recommendation_rotation_guard(df: pd.DataFrame | None, base_dir: str | Path | None = None) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame(columns=ROTATION_COLUMNS)
    out = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df)
    if out.empty or "股票代號" not in out.columns or "股神推薦優先分" not in out.columns:
        for col in ROTATION_COLUMNS:
            if col not in out.columns:
                out[col] = 0.0 if col in _NUMERIC_ROTATION_COLUMNS else ""
        return out

    history = load_rotation_history(base_dir)
    current_date = _current_scan_date(out)
    prior_dates, by_code = _history_metrics(history, current_date)
    profiles = []
    for _, row in out.iterrows():
        code = _code(row.get("股票代號"))
        profiles.append(_repeat_adjustment(row, by_code.get(code, []), prior_dates))
    profile_df = pd.DataFrame(profiles, index=out.index)
    for col in profile_df.columns:
        out[col] = profile_df[col]

    base_score = pd.to_numeric(out["股神推薦優先分"], errors="coerce").fillna(0.0)
    out["股神推薦優先分_輪動前"] = base_score.round(1)
    adjust = pd.to_numeric(out["重複推薦校正分"], errors="coerce").fillna(0.0)

    # V177 類股相似度懲罰：不是硬性分散，而是第3檔起需有更強的新證據。
    # 若同族群真的壓倒性領先且有新證據，只做極小校正；不會硬塞弱股。
    prelim = pd.DataFrame({"score": base_score + adjust}, index=out.index).sort_values("score", ascending=False, kind="mergesort")
    sector_seen: dict[str, int] = {}
    concentration = pd.Series([0.0] * len(out), index=out.index, dtype="float64")
    concentration_reason = pd.Series([""] * len(out), index=out.index, dtype="object")
    for pos, idx in enumerate(prelim.index[:40], start=1):
        sector = _safe_str(out.at[idx, "類別"] if "類別" in out.columns else "") or "未分類"
        seen = sector_seen.get(sector, 0)
        if seen >= 2:
            signal_v = _safe_float(out.at[idx, "今日訊號新鮮分"] if "今日訊號新鮮分" in out.columns else 0)
            is_event_v = _safe_str(out.at[idx, "是否新增推薦事件"] if "是否新增推薦事件" in out.columns else "否") == "是"
            raw_penalty = min(4.5, 1.5 * (seen - 1))
            if signal_v >= 75 and is_event_v:
                raw_penalty *= 0.30
            elif is_event_v:
                raw_penalty *= 0.55
            concentration.at[idx] = -round(raw_penalty, 1)
            concentration_reason.at[idx] = f"{sector}已先有{seen}檔｜{'有新證據，輕度校正' if is_event_v else '無新證據，提高相似度懲罰'}"
        sector_seen[sector] = seen + 1
    out["類股集中校正分"] = concentration
    out["類股集中說明"] = concentration_reason
    out["股神推薦優先分"] = (base_score + adjust + concentration).clip(lower=0, upper=100).round(1)

    # A repeated structure-only name must not retain formal/A- status without a
    # fresh trigger.  It remains visible as a radar candidate instead of being
    # deleted from the candidate pool.
    sticky_block = out["推薦輪動狀態"].astype(str).str.startswith("STICKY-BLOCK")
    repeat_track = (out.get("是否新增推薦事件", pd.Series(["是"] * len(out), index=out.index)).astype(str).eq("否")) & (pd.to_numeric(out.get("近5次入榜次數", 0), errors="coerce").fillna(0) >= 2)
    if sticky_block.any() or repeat_track.any():
        bucket = out.get("正式推薦分區", pd.Series([""] * len(out), index=out.index)).fillna("").astype(str)
        downgrade = (sticky_block | repeat_track) & bucket.isin(["正式下週主推薦", "A-｜準主推薦小量試單"])
        if downgrade.any():
            out.loc[downgrade, "正式推薦分區"] = "盤中雷達追蹤"
            out.loc[downgrade, "是否正式推薦"] = "否"
            out.loc[downgrade, "操作許可"] = "既有持續追蹤｜無新增證據前不新增新倉"
            out.loc[downgrade, "最終操作結論"] = "TRACK-ONLY｜既有邏輯仍成立，但今天不是新的推薦事件"

    if "股神推薦分數說明" in out.columns:
        out["股神推薦分數說明"] = (
            out["股神推薦分數說明"].fillna("").astype(str)
            + "｜"
            + out["重複推薦說明"].fillna("").astype(str)
        ).str.strip("｜")

    # Re-rank after the small rotation adjustment. Formal exclusions remain out.
    bucket = out.get("正式推薦分區", pd.Series([""] * len(out), index=out.index)).fillna("").astype(str)
    score = pd.to_numeric(out["股神推薦優先分"], errors="coerce").fillna(0.0)
    miss = pd.to_numeric(out.get("強勢股漏選風險分", pd.Series([0] * len(out), index=out.index)), errors="coerce").fillna(0.0)
    eligible = ~bucket.eq("正式排除清單")
    eligible &= score.ge(50) | (score.ge(45) & miss.ge(75) & bucket.isin(["高風險雷達觀察", "不可直接買觀察"]))
    ranked = out.loc[eligible].copy()
    if not ranked.empty:
        sort_cols = [
            "股神推薦優先分", "今日訊號新鮮分", "隔日觸發品質分", "主流主升優先分",
            "實戰操作品質分", "強勢動能分", "強勢前兆分", "主流資金分", "成交額百萬",
        ]
        for col in sort_cols:
            if col not in ranked.columns:
                ranked[col] = 0.0
            ranked[col] = pd.to_numeric(ranked[col], errors="coerce").fillna(0.0)
        ranked = ranked.sort_values(sort_cols, ascending=[False] * len(sort_cols), kind="mergesort")

        # 前10名不再被「反覆入榜但缺乏新證據」完全占滿。只有在有合格新訊號候選時，
        # 最多保留7檔近5次入榜>=3次的舊名單，至少讓3個新/低重複訊號參與競爭。
        if len(ranked) >= 10 and "近5次入榜次數" in ranked.columns and "今日訊號新鮮分" in ranked.columns:
            repeat_n = pd.to_numeric(ranked["近5次入榜次數"], errors="coerce").fillna(0)
            signal_n = pd.to_numeric(ranked["今日訊號新鮮分"], errors="coerce").fillna(0)
            repeat_pool = ranked.loc[repeat_n.ge(3)].copy()
            fresh_pool = ranked.loc[repeat_n.lt(3) & signal_n.ge(50)].copy()
            if len(fresh_pool) >= 3 and len(repeat_pool) >= 7:
                selected_top = []
                repeat_used = 0
                fresh_used = 0
                for idx in ranked.index:
                    is_repeat = bool(repeat_n.loc[idx] >= 3)
                    is_fresh = bool(repeat_n.loc[idx] < 3 and signal_n.loc[idx] >= 50)
                    if is_repeat and repeat_used >= 7 and fresh_used < 3:
                        continue
                    selected_top.append(idx)
                    repeat_used += int(is_repeat)
                    fresh_used += int(is_fresh)
                    if len(selected_top) == 10:
                        break
                for idx in fresh_pool.index:
                    if len(selected_top) >= 10:
                        break
                    if idx not in selected_top:
                        selected_top.append(idx)
                remaining = [idx for idx in ranked.index if idx not in selected_top]
                ranked = ranked.loc[selected_top + remaining]

        out["股神推薦總排名"] = 0
        rank_map = {idx: pos for pos, idx in enumerate(ranked.index, start=1)}
        out.loc[list(rank_map.keys()), "股神推薦總排名"] = [rank_map[idx] for idx in rank_map]
    out["股神推薦總排名"] = pd.to_numeric(out.get("股神推薦總排名", 0), errors="coerce").fillna(0).astype(int)
    return out


def save_rotation_snapshot(
    df: pd.DataFrame | None,
    base_dir: str | Path | None = None,
    *,
    top_n: int = ROTATION_SNAPSHOT_TOP_N,
    persist_remote: bool = True,
) -> tuple[bool, str]:
    if df is None:
        return False, "沒有推薦資料，未建立輪動快照"
    work = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df)
    if work.empty or "股票代號" not in work.columns:
        return False, "推薦資料為空，未建立輪動快照"
    scan_date = _current_scan_date(work)
    if not scan_date:
        return False, "無法取得K線交易日，未建立輪動快照"

    rank = pd.to_numeric(work.get("股神推薦總排名", 0), errors="coerce").fillna(0)
    score = pd.to_numeric(work.get("股神推薦優先分", 0), errors="coerce").fillna(0)
    selected = work.loc[(rank > 0) & (score >= 45)].copy()
    if selected.empty:
        selected = work.loc[score >= 45].copy()
    if selected.empty:
        return False, "沒有可保存的排名資料"
    selected["_rank"] = pd.to_numeric(selected.get("股神推薦總排名", 9999), errors="coerce").fillna(9999)
    selected = selected.sort_values(["_rank", "股神推薦優先分"], ascending=[True, False], kind="mergesort").head(max(1, int(top_n)))

    entries = []
    for _, row in selected.iterrows():
        code = _code(row.get("股票代號"))
        if not code:
            continue
        entries.append({
            "股票代號": code,
            "股票名稱": _safe_str(row.get("股票名稱")),
            "股神推薦總排名": int(_safe_float(row.get("股神推薦總排名"), 0)),
            "股神推薦優先分": round(_safe_float(row.get("股神推薦優先分"), 0), 1),
            "正式推薦分區": _safe_str(row.get("正式推薦分區")),
            "股神推薦用途": _safe_str(row.get("股神推薦用途")),
            "類別": _safe_str(row.get("類別")),
            "主流主升優先分": round(_safe_float(row.get("主流主升優先分"), 0), 1),
            "今日訊號新鮮分": round(_safe_float(row.get("今日訊號新鮮分"), 0), 1),
            "最新價": _safe_float(row.get("最新價"), 0),
            "實戰觸發價": _safe_float(row.get("實戰觸發價"), 0),
            "AI新證據分": round(_safe_float(row.get("AI新證據分"), 0), 1),
            "AI跨市場新強股分": round(_safe_float(row.get("AI跨市場新強股分"), 0), 1),
            "型態突破分數": round(_safe_float(row.get("型態突破分數"), 0), 1),
            "當日量比": round(_safe_float(row.get("當日量比"), 0), 2),
            "今日漲幅%": round(_safe_float(row.get("今日漲幅%"), 0), 2),
            "正式推薦資格": _safe_str(row.get("正式推薦資格")),
            "推薦事件類型": _safe_str(row.get("推薦事件類型")),
        })

    history = load_rotation_history(base_dir)
    snapshot = {
        "date": scan_date,
        "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "version": ROTATION_GUARD_VERSION,
        "rows": entries,
    }
    by_date = {item.get("date"): item for item in history if item.get("date")}
    by_date[scan_date] = snapshot
    dates = sorted(by_date)[-ROTATION_MAX_DATES:]
    payload = {
        "version": ROTATION_GUARD_VERSION,
        "updated_at": snapshot["saved_at"],
        "snapshots": [by_date[d] for d in dates],
    }
    path = _history_path(base_dir)
    _atomic_write_json(path, payload)
    _load_history_cached.cache_clear()
    msg = f"已更新推薦輪動快照：{scan_date}｜{len(entries)} 檔｜{path.name}"
    if persist_remote:
        try:
            from godpick_persistence_service import save_named_json_permanent
            report = save_named_json_permanent(
                ROTATION_HISTORY_FILE, payload, firestore_doc="godpick_rotation_history"
            )
            if getattr(report, "permanent_ok", False):
                msg += "｜永久層已驗證"
            else:
                msg += "｜本機已保存；遠端永久層待確認"
        except Exception as exc:
            msg += f"｜本機已保存；遠端同步略過：{exc}"
    return True, msg


def rotation_diagnostics(df: pd.DataFrame | None) -> dict[str, Any]:
    if df is None:
        return {"available": False}
    work = df if isinstance(df, pd.DataFrame) else pd.DataFrame(df)
    if work.empty or "近5次入榜次數" not in work.columns:
        return {"available": False}
    rank = pd.to_numeric(work.get("股神推薦總排名", 0), errors="coerce").fillna(0)
    top = work.loc[(rank > 0) & (rank <= 10)].copy()
    if top.empty:
        return {"available": False}
    repeat = pd.to_numeric(top.get("近5次入榜次數", 0), errors="coerce").fillna(0)
    signal = pd.to_numeric(top.get("今日訊號新鮮分", 0), errors="coerce").fillna(0)
    new_count = int((repeat == 0).sum())
    sticky_count = int(((repeat >= 3) & (signal < 50)).sum())
    return {
        "available": True,
        "top10_count": int(len(top)),
        "top10_new_count": new_count,
        "top10_repeat3_count": int((repeat >= 3).sum()),
        "top10_sticky_without_signal": sticky_count,
        "top10_average_signal_freshness": round(float(signal.mean()), 1),
        "top10_new_event_count": int(top.get("是否新增推薦事件", pd.Series(["否"] * len(top), index=top.index)).astype(str).eq("是").sum()),
        "top10_track_only_count": int(top.get("是否新增推薦事件", pd.Series(["否"] * len(top), index=top.index)).astype(str).eq("否").sum()),
        "warning": sticky_count >= 3 or (new_count == 0 and int((repeat >= 3).sum()) >= 5),
    }
