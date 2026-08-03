# -*- coding: utf-8 -*-
"""
macro_startup_service.py
v109：專案開啟即更新 0/1 大盤走勢橋接資料。

用途：
- 不必點進「大盤走勢」頁，也會建立 / 更新：
  market_snapshot.json、macro_mode_bridge.json、macro_trend_records.json
- 讓 1_儀表板、7_股神推薦、11_資料診斷、14_權重校正等模組不會因為缺大盤檔案而串接失敗。
- 預設不重跑大盤走勢整頁，不建立大量 Streamlit widget。

設計原則：
- 有可用快照：背景更新，不拖慢進頁。
- 完全沒有快照：做一次快速同步補底，避免其他頁讀不到檔案。
- 更新失敗：保留舊資料，寫入 macro_startup_status.json 顯示原因。
"""
from __future__ import annotations

import base64
import json
import math
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Tuple

import requests

try:
    import streamlit as st
except Exception:  # pragma: no cover
    st = None

BASE_DIR = Path(__file__).resolve().parent
MARKET_SNAPSHOT_FILE = BASE_DIR / "market_snapshot.json"
MACRO_BRIDGE_FILE = BASE_DIR / "macro_mode_bridge.json"
MACRO_RECORDS_FILE = BASE_DIR / "macro_trend_records.json"
STATUS_FILE = BASE_DIR / "macro_startup_status.json"
LOCK_FILE = BASE_DIR / "macro_startup_update.lock"

VERSION = "v109_2_startup_macro_github_sync_hard_off"
DEFAULT_TTL_SECONDS = 30 * 60
LOCK_TTL_SECONDS = 8 * 60
TAIPEI_TZ = timezone(timedelta(hours=8))


def _tw_now() -> datetime:
    """Return a timezone-aware Taiwan datetime.

    The previous implementation added eight hours to a UTC-aware datetime but
    kept the UTC tzinfo. That made every saved Taiwan timestamp appear roughly
    eight hours old when freshness was checked, so each Streamlit restart
    started another update and committed runtime JSON files back to GitHub.
    Those commits triggered another Streamlit Cloud redeploy and formed a loop.
    """
    return datetime.now(TAIPEI_TZ)


def _startup_github_sync_enabled() -> bool:
    """Never commit startup/runtime snapshots to the deployment branch.

    Streamlit Community Cloud redeploys whenever ``main`` changes. Runtime JSON
    commits therefore disconnect every active session with HTTP 503 and can form
    a redeploy loop. Startup refresh remains local; persistent runtime data must
    use Firestore or a separate non-deployment data branch.
    """
    return False


def _now_text() -> str:
    return _tw_now().strftime("%Y-%m-%d %H:%M:%S")


def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    try:
        return str(v).strip()
    except Exception:
        return ""


def _safe_float(v: Any, default: float | None = None) -> float | None:
    try:
        if v is None:
            return default
        if isinstance(v, str):
            v = v.replace(",", "").replace("+", "").replace("%", "").strip()
            if not v or v in {"-", "--", "None", "nan"}:
                return default
        x = float(v)
        if math.isnan(x) or math.isinf(x):
            return default
        return x
    except Exception:
        return default


def _read_json(path: Path, default: Any) -> Any:
    try:
        if not path.exists():
            return default
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if data is not None else default
    except Exception:
        return default


def _write_json(path: Path, data: Any) -> Tuple[bool, str]:
    try:
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        tmp.replace(path)
        return True, str(path.name)
    except Exception as e:
        return False, str(e)


def _parse_time_text(v: Any) -> datetime | None:
    s = _safe_str(v)
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            dt = datetime.strptime(s[:19], fmt)
            return dt.replace(tzinfo=timezone(timedelta(hours=8)))
        except Exception:
            continue
    return None


def _is_snapshot_fresh(ttl_seconds: int = DEFAULT_TTL_SECONDS) -> bool:
    data = _read_json(MARKET_SNAPSHOT_FILE, {})
    if not isinstance(data, dict) or not data:
        return False
    ts = _parse_time_text(data.get("updated_at") or data.get("snapshot_time") or data.get("time"))
    if not ts:
        return False
    try:
        return (_tw_now() - ts).total_seconds() < ttl_seconds
    except Exception:
        return False


def _has_usable_snapshot() -> bool:
    data = _read_json(MARKET_SNAPSHOT_FILE, {})
    if not isinstance(data, dict) or not data:
        return False
    return any(k in data for k in ["market_score", "market_trend", "risk_gate", "twse_change_pct", "market_risk_level"])


def _lock_is_recent() -> bool:
    if not LOCK_FILE.exists():
        return False
    try:
        data = json.loads(LOCK_FILE.read_text(encoding="utf-8"))
        ts = _parse_time_text(data.get("started_at"))
        if not ts:
            return False
        return (_tw_now() - ts).total_seconds() < LOCK_TTL_SECONDS
    except Exception:
        return False


def _set_lock() -> None:
    _write_json(LOCK_FILE, {"version": VERSION, "started_at": _now_text()})


def _clear_lock() -> None:
    try:
        LOCK_FILE.unlink(missing_ok=True)
    except Exception:
        pass


def _write_status(ok: bool, message: str, **extra: Any) -> Dict[str, Any]:
    status = {
        "version": VERSION,
        "ok": bool(ok),
        "message": _safe_str(message),
        "short_message": _safe_str(message)[:80],
        "updated_at": _now_text(),
        **extra,
    }
    _write_json(STATUS_FILE, status)
    return status


def _github_config() -> Dict[str, str]:
    if st is None:
        return {"token": "", "owner": "", "repo": "", "branch": "main"}
    try:
        return {
            "token": _safe_str(st.secrets.get("GITHUB_TOKEN", "")),
            "owner": _safe_str(st.secrets.get("GITHUB_REPO_OWNER", "cheng07021028")) or "cheng07021028",
            "repo": _safe_str(st.secrets.get("GITHUB_REPO_NAME", "stock-app")) or "stock-app",
            "branch": _safe_str(st.secrets.get("GITHUB_REPO_BRANCH", "main")) or "main",
        }
    except Exception:
        return {"token": "", "owner": "", "repo": "", "branch": "main"}


def _github_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _github_url(owner: str, repo: str, path: str) -> str:
    return f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"


def _write_github_json(path_name: str, payload: Any) -> Tuple[bool, str]:
    cfg = _github_config()
    token = cfg.get("token", "")
    if not token:
        return False, "未設定 GITHUB_TOKEN"
    owner, repo, branch = cfg["owner"], cfg["repo"], cfg["branch"]
    sha = ""
    try:
        r = requests.get(_github_url(owner, repo, path_name), headers=_github_headers(token), params={"ref": branch}, timeout=8)
        if r.status_code == 200:
            sha = _safe_str((r.json() or {}).get("sha"))
    except Exception:
        pass
    body = {
        "message": f"Update {path_name} by {VERSION} @ {_now_text()}",
        "content": base64.b64encode(json.dumps(payload, ensure_ascii=False, indent=2, default=str).encode("utf-8")).decode("utf-8"),
        "branch": branch,
    }
    if sha:
        body["sha"] = sha
    try:
        r = requests.put(_github_url(owner, repo, path_name), headers=_github_headers(token), json=body, timeout=12)
        if r.status_code in (200, 201):
            return True, f"GitHub已寫入 {path_name}"
        return False, f"GitHub寫入失敗 {path_name}: {r.status_code}"
    except Exception as e:
        return False, f"GitHub寫入例外 {path_name}: {e}"


def _fetch_yahoo_chart(symbol: str, days: int = 90, timeout: float = 4.0) -> Dict[str, Any]:
    now_utc = datetime.now(timezone.utc)
    p2 = int(now_utc.timestamp())
    p1 = int((now_utc - timedelta(days=days)).timestamp())
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{requests.utils.quote(symbol, safe='^=')}"
    try:
        r = requests.get(
            url,
            params={"period1": p1, "period2": p2, "interval": "1d", "includePrePost": "false"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=timeout,
        )
        if r.status_code != 200:
            return {"ok": False, "symbol": symbol, "error": f"HTTP {r.status_code}"}
        result = (((r.json() or {}).get("chart") or {}).get("result") or [])
        if not result:
            return {"ok": False, "symbol": symbol, "error": "empty"}
        item = result[0]
        ts = item.get("timestamp") or []
        q = ((item.get("indicators") or {}).get("quote") or [{}])[0]
        close = q.get("close") or []
        high = q.get("high") or []
        low = q.get("low") or []
        vol = q.get("volume") or []
        rows = []
        for i, c in enumerate(close):
            c = _safe_float(c)
            if c is None:
                continue
            rows.append({
                "date": datetime.fromtimestamp(ts[i], tz=timezone.utc).astimezone(timezone(timedelta(hours=8))).strftime("%Y-%m-%d") if i < len(ts) else "",
                "close": c,
                "high": _safe_float(high[i] if i < len(high) else None),
                "low": _safe_float(low[i] if i < len(low) else None),
                "volume": _safe_float(vol[i] if i < len(vol) else None),
            })
        if len(rows) < 2:
            return {"ok": False, "symbol": symbol, "error": "not enough rows"}
        last = rows[-1]
        prev = rows[-2]
        chg = (last["close"] or 0) - (prev["close"] or 0)
        chg_pct = (chg / prev["close"] * 100) if prev.get("close") else 0.0
        ma5 = sum([x["close"] for x in rows[-5:]]) / min(len(rows), 5)
        ma20 = sum([x["close"] for x in rows[-20:]]) / min(len(rows), 20)
        return {
            "ok": True,
            "symbol": symbol,
            "date": last.get("date"),
            "close": round(last["close"], 2),
            "prev_close": round(prev["close"], 2),
            "change": round(chg, 2),
            "change_pct": round(chg_pct, 4),
            "ma5": round(ma5, 2),
            "ma20": round(ma20, 2),
            "source": "Yahoo chart API",
            "rows": len(rows),
        }
    except Exception as e:
        return {"ok": False, "symbol": symbol, "error": str(e)[:200]}


def _market_score_from_inputs(tw: Dict[str, Any], otc: Dict[str, Any], nasdaq: Dict[str, Any], sox: Dict[str, Any]) -> float:
    score = 50.0
    tw_pct = _safe_float(tw.get("change_pct"), 0) or 0
    otc_pct = _safe_float(otc.get("change_pct"), 0) or 0
    nas_pct = _safe_float(nasdaq.get("change_pct"), 0) or 0
    sox_pct = _safe_float(sox.get("change_pct"), 0) or 0
    score += max(min(tw_pct * 4.5, 18), -18)
    score += max(min(otc_pct * 3.0, 12), -12)
    score += max(min(nas_pct * 2.0, 8), -8)
    score += max(min(sox_pct * 2.0, 8), -8)
    if _safe_float(tw.get("close"), 0) and _safe_float(tw.get("ma20"), 0):
        score += 6 if tw["close"] >= tw["ma20"] else -6
    if _safe_float(tw.get("ma5"), 0) and _safe_float(tw.get("ma20"), 0):
        score += 4 if tw["ma5"] >= tw["ma20"] else -4
    return round(max(0, min(100, score)), 1)


def _trend_from_score(score: float) -> str:
    if score >= 72:
        return "多頭"
    if score >= 58:
        return "偏多"
    if score <= 32:
        return "空頭"
    if score <= 45:
        return "偏空"
    return "盤整"


def _risk_from_score(score: float) -> Tuple[str, str, str]:
    if score >= 70:
        return "低", "可進場", "大盤偏多，股神推薦可正常放行。"
    if score >= 55:
        return "中低", "選股進場", "大盤中性偏多，建議搭配個股強度。"
    if score >= 40:
        return "中", "保守選股", "大盤震盪，降低追高比重。"
    return "高", "保守觀望", "大盤風險偏高，建議降低部位。"


def _build_snapshot(results: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    tw = results.get("twse") or {}
    otc = results.get("otc") or {}
    nas = results.get("nasdaq") or {}
    sox = results.get("sox") or {}
    futures = results.get("futures") or {}
    score = _market_score_from_inputs(tw, otc, nas, sox)
    trend = _trend_from_score(score)
    risk, gate, comment = _risk_from_score(score)
    data_date = _safe_str(tw.get("date") or otc.get("date") or _tw_now().strftime("%Y-%m-%d"))
    snapshot = {
        "version": VERSION,
        "updated_at": _now_text(),
        "data_date": data_date,
        "source": "startup Yahoo batch + cache fallback",
        "startup_auto_update": True,
        "market_score": score,
        "market_trend": trend,
        "market_risk_level": risk,
        "risk_gate": gate,
        "position_hint": gate,
        "market_comment": comment,
        "twse_index": tw.get("close"),
        "twse_change": tw.get("change"),
        "twse_change_pct": tw.get("change_pct"),
        "twse_ma5": tw.get("ma5"),
        "twse_ma20": tw.get("ma20"),
        "otc_index": otc.get("close"),
        "otc_change": otc.get("change"),
        "otc_change_pct": otc.get("change_pct"),
        "nasdaq_change_pct": nas.get("change_pct"),
        "sox_change_pct": sox.get("change_pct"),
        "futures_index": futures.get("close"),
        "futures_change": futures.get("change"),
        "futures_change_pct": futures.get("change_pct"),
        "overnight_score": score,
        "overnight_risk_level": risk,
        "overnight_bias": trend,
        "overnight_comment": f"NASDAQ {nas.get('change_pct', '—')}% / SOX {sox.get('change_pct', '—')}% / 台指期 {futures.get('change_pct', '—')}%",
        "event_factor": "startup_auto",
        "macro_mode_estimate": trend,
        "feature_center_version": VERSION,
        "required_by_godpick": True,
        "recommendation_bias": {
            "bias": trend,
            "risk_level": risk,
            "position_hint": gate,
            "score": score,
        },
        "source_status": {k: {"ok": bool(v.get("ok")), "source": v.get("source"), "error": v.get("error", "")} for k, v in results.items()},
    }
    # v80：啟動快抓完成後同步產生隔日大盤預測，避免登入背景更新覆蓋 01 頁已寫入的預測欄位。
    try:
        from market_nextday_forecast_engine import build_nextday_market_forecast, flatten_forecast_for_bridge
        forecast = build_nextday_market_forecast(snapshot, None)
        snapshot.update(flatten_forecast_for_bridge(forecast))
        req = snapshot.get("required_by_godpick") if isinstance(snapshot.get("required_by_godpick"), dict) else {}
        for key in [
            "next_day_forecast_date", "next_day_market_direction", "next_day_market_score",
            "next_day_confidence", "next_day_confidence_score", "next_day_up_probability_pct",
            "next_day_flat_probability_pct", "next_day_down_probability_pct",
            "next_day_expected_return_pct", "next_day_godpick_score_delta",
            "next_day_market_weight_delta", "next_day_position_cap_pct",
            "next_day_preferred_style", "next_day_avoid_style", "next_day_effect_mode",
        ]:
            req[key] = snapshot.get(key)
        snapshot["required_by_godpick"] = req
    except Exception as e:
        snapshot["next_day_forecast_warning"] = str(e)
    return snapshot


def _build_bridge(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    bridge = dict(snapshot)
    bridge.update({
        "version": VERSION,
        "bridge_source": "market_snapshot.json",
        "_source": "startup macro auto update",
        "required_by_godpick": True,
    })
    return bridge


def _append_record(snapshot: Dict[str, Any]) -> list:
    records = _read_json(MACRO_RECORDS_FILE, [])
    if not isinstance(records, list):
        records = []
    item = {
        "version": VERSION,
        "updated_at": snapshot.get("updated_at"),
        "data_date": snapshot.get("data_date"),
        "market_score": snapshot.get("market_score"),
        "market_trend": snapshot.get("market_trend"),
        "market_risk_level": snapshot.get("market_risk_level"),
        "risk_gate": snapshot.get("risk_gate"),
        "twse_index": snapshot.get("twse_index"),
        "twse_change_pct": snapshot.get("twse_change_pct"),
        "otc_change_pct": snapshot.get("otc_change_pct"),
        "source": snapshot.get("source"),
        "next_day_forecast_date": snapshot.get("next_day_forecast_date"),
        "next_day_market_direction": snapshot.get("next_day_market_direction"),
        "next_day_market_score": snapshot.get("next_day_market_score"),
        "next_day_confidence": snapshot.get("next_day_confidence"),
        "next_day_up_probability_pct": snapshot.get("next_day_up_probability_pct"),
        "next_day_down_probability_pct": snapshot.get("next_day_down_probability_pct"),
        "next_day_expected_return_pct": snapshot.get("next_day_expected_return_pct"),
    }
    if not records or _safe_str(records[-1].get("updated_at"))[:16] != _safe_str(item.get("updated_at"))[:16]:
        records.append(item)
    return records[-500:]


def _run_fast_update(sync_github: bool = True) -> Dict[str, Any]:
    symbols = {
        "twse": "^TWII",
        "otc": "^TWOII",
        "nasdaq": "^IXIC",
        "sox": "^SOX",
        "futures": "TX=F",
    }
    results: Dict[str, Dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=5) as ex:
        fut_map = {key: ex.submit(_fetch_yahoo_chart, sym) for key, sym in symbols.items()}
        for key, fut in fut_map.items():
            try:
                results[key] = fut.result(timeout=6)
            except Exception as e:
                results[key] = {"ok": False, "symbol": symbols[key], "error": str(e)[:120] or "timeout"}
    for key, sym in symbols.items():
        results.setdefault(key, {"ok": False, "symbol": sym, "error": "timeout"})

    # V110：台股兩項必須有至少2筆歷史資料；Yahoo失敗或rows不足時改走多來源服務。
    try:
        from market_index_history_service import fetch_market_index_history
        for key in ("twse", "otc"):
            current = results.get(key) or {}
            if not current.get("ok") or int(current.get("rows") or 0) < 2:
                fixed = fetch_market_index_history(key, days=90, timeout=5.0)
                if fixed.get("ok") and int(fixed.get("rows") or 0) >= 2:
                    results[key] = {
                        "ok": True, "symbol": symbols[key], "date": fixed.get("date"),
                        "close": fixed.get("close"), "prev_close": fixed.get("prev_close"),
                        "change": fixed.get("change"), "change_pct": fixed.get("change_pct"),
                        "ma5": round(sum(x["close"] for x in fixed["history"][-5:]) / min(5, len(fixed["history"])), 2),
                        "ma20": round(sum(x["close"] for x in fixed["history"][-20:]) / min(20, len(fixed["history"])), 2),
                        "source": fixed.get("source"), "rows": fixed.get("rows"),
                        "history": fixed.get("history"), "diagnostics": fixed.get("diagnostics"),
                    }
                else:
                    results[key] = fixed
    except Exception as e:
        results.setdefault("index_history_service", {"ok": False, "error": str(e)})

    if not (results.get("twse") or {}).get("ok"):
        old = _read_json(MARKET_SNAPSHOT_FILE, {})
        if isinstance(old, dict) and old:
            return _write_status(False, "台股大盤快抓失敗，已保留舊 market_snapshot", results=results, kept_old=True)
        return _write_status(False, "台股大盤快抓失敗，尚無舊快照可保留", results=results, kept_old=False)

    snapshot = _build_snapshot(results)
    bridge = _build_bridge(snapshot)
    records = _append_record(snapshot)

    s_ok, s_msg = _write_json(MARKET_SNAPSHOT_FILE, snapshot)
    b_ok, b_msg = _write_json(MACRO_BRIDGE_FILE, bridge)
    r_ok, r_msg = _write_json(MACRO_RECORDS_FILE, records)

    gh_msgs = []
    if sync_github:
        for name, data in [
            ("market_snapshot.json", snapshot),
            ("macro_mode_bridge.json", bridge),
            ("macro_trend_records.json", records),
        ]:
            ok, msg = _write_github_json(name, data)
            gh_msgs.append({"file": name, "ok": ok, "message": msg})

    ok = bool(s_ok and b_ok and r_ok)
    return _write_status(
        ok,
        f"大盤啟動更新完成：{snapshot.get('market_trend')} / {snapshot.get('market_score')}",
        snapshot_ok=s_ok,
        bridge_ok=b_ok,
        records_ok=r_ok,
        local_messages=[s_msg, b_msg, r_msg],
        github=gh_msgs,
        results=results,
    )


def _background_worker() -> None:
    try:
        _set_lock()
        _run_fast_update(sync_github=_startup_github_sync_enabled())
    except Exception as e:
        _write_status(False, f"背景大盤啟動更新例外：{e}")
    finally:
        _clear_lock()


def ensure_macro_startup_update(ttl_seconds: int = DEFAULT_TTL_SECONDS) -> Dict[str, Any]:
    """登入後呼叫。必要時自動更新大盤橋接檔。"""
    status = _read_json(STATUS_FILE, {})

    if _is_snapshot_fresh(ttl_seconds):
        return {
            "version": VERSION,
            "ok": True,
            "message": "快照仍新鮮，略過啟動更新",
            "short_message": "快照新鮮",
            "mode": "skip_fresh",
            "status": status,
        }

    if _lock_is_recent():
        return {
            "version": VERSION,
            "ok": True,
            "message": "大盤背景更新中",
            "short_message": "背景更新中",
            "mode": "running",
            "status": status,
        }

    if st is not None:
        try:
            if st.session_state.get("_macro_startup_update_started_v109"):
                return {
                    "version": VERSION,
                    "ok": True,
                    "message": "本次 session 已啟動過大盤更新",
                    "short_message": "本次已啟動",
                    "mode": "session_started",
                    "status": status,
                }
            st.session_state["_macro_startup_update_started_v109"] = True
        except Exception:
            pass

    if not _has_usable_snapshot():
        try:
            _set_lock()
            return _run_fast_update(sync_github=_startup_github_sync_enabled())
        finally:
            _clear_lock()

    try:
        _set_lock()
        t = threading.Thread(target=_background_worker, name="macro_startup_update_v109", daemon=True)
        t.start()
        return {
            "version": VERSION,
            "ok": True,
            "message": "已啟動大盤背景更新，其他模組先沿用舊快照",
            "short_message": "背景更新已啟動",
            "mode": "background_started",
        }
    except Exception as e:
        _clear_lock()
        return _write_status(False, f"啟動大盤背景更新失敗：{e}")


def get_macro_startup_status() -> Dict[str, Any]:
    data = _read_json(STATUS_FILE, {})
    return data if isinstance(data, dict) else {}
