# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Any
import json

import pandas as pd
import requests
import urllib3
import streamlit as st

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    from utils import inject_pro_theme, render_pro_hero, render_pro_kpi_row, render_pro_info_card
except Exception:
    def inject_pro_theme():
        pass

    def render_pro_hero(title: str, subtitle: str = ""):
        st.title(title)
        if subtitle:
            st.caption(subtitle)

    def render_pro_kpi_row(items):
        cols = st.columns(len(items) if items else 1)
        for col, item in zip(cols, items):
            with col:
                st.metric(item.get("label", ""), item.get("value", ""), item.get("delta", ""))

    def render_pro_info_card(title: str, pairs, chips=None):
        st.subheader(title)
        for a, b, *_ in pairs:
            st.write(f"**{a}**：{b}")

PAGE_TITLE = "大盤走勢"
PFX = "macro_safe_"
CACHE_FILE = "macro_market_close_cache.json"
INST_CACHE_FILE = "macro_institutional_cache.json"


def _k(key: str) -> str:
    return f"{PFX}{key}"


def _tw_now() -> datetime:
    # Streamlit Cloud 常是 UTC，這裡轉台灣時間。
    return datetime.utcnow() + timedelta(hours=8)


def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def _safe_float(v: Any, default: float | None = None):
    try:
        if v is None:
            return default
        if isinstance(v, str):
            v = v.replace(",", "").replace("+", "").replace("%", "").strip()
            if not v or v in {"-", "--", "nan", "None"}:
                return default
        return float(v)
    except Exception:
        return default


def _read_cache() -> dict[str, Any]:
    p = Path(CACHE_FILE)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_cache(cache: dict[str, Any]) -> None:
    try:
        Path(CACHE_FILE).write_text(json.dumps(cache, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    except Exception:
        pass


def _read_inst_cache() -> dict[str, Any]:
    p = Path(INST_CACHE_FILE)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_inst_cache(cache: dict[str, Any]) -> None:
    try:
        Path(INST_CACHE_FILE).write_text(json.dumps(cache, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    except Exception:
        pass


def _extract_money_100m(v: Any):
    """TWSE 金額通常為元，轉成億元。"""
    s = _safe_str(v).replace(",", "").replace("+", "").replace(" ", "")
    if not s or s in {"-", "--", "None", "nan"}:
        return None
    try:
        return float(s) / 100000000
    except Exception:
        return None


def _fetch_twse_institutional_manual(target_date: date, timeout: float = 3.0) -> dict[str, Any]:
    """
    v27.6：手動更新三大法人買賣超。
    只有按按鈕才執行，不進頁自動抓，避免卡住。
    單位：億元。
    """
    ymd = pd.to_datetime(target_date).strftime("%Y%m%d")
    urls = [
        f"https://www.twse.com.tw/rwd/zh/fund/BFI82U?dayDate={ymd}&type=day&response=json",
        f"https://www.twse.com.tw/fund/BFI82U?dayDate={ymd}&type=day&response=json",
    ]

    for url in urls:
        try:
            r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout, verify=False)
            if r.status_code != 200:
                continue
            data = r.json()
            rows = data.get("data") or []
            if not isinstance(rows, list) or not rows:
                continue

            foreign = None
            invest = None
            dealer = None
            total = None
            raw_rows = []

            for row in rows:
                if not isinstance(row, list) or len(row) < 2:
                    continue
                label = _safe_str(row[0])
                nums = [_extract_money_100m(x) for x in row[1:]]
                nums = [x for x in nums if x is not None]
                if not nums:
                    continue
                # BFI82U 常見最後一欄為買賣超金額，取最後有效值。
                val = nums[-1]
                raw_rows.append({"項目": label, "買賣超億元": val})
                if "外資" in label:
                    foreign = val
                elif "投信" in label:
                    invest = val
                elif "自營商" in label:
                    dealer = val
                elif "合計" in label or "總計" in label:
                    total = val

            calc_total = sum([x for x in [foreign, invest, dealer] if x is not None]) if any(x is not None for x in [foreign, invest, dealer]) else total
            if total is None:
                total = calc_total

            if any(x is not None for x in [foreign, invest, dealer, total]):
                return {
                    "ok": True,
                    "date": pd.to_datetime(target_date).strftime("%Y-%m-%d"),
                    "source": "TWSE 三大法人",
                    "foreign_100m": foreign,
                    "investment_trust_100m": invest,
                    "dealer_100m": dealer,
                    "total_100m": total,
                    "raw_rows": raw_rows,
                    "updated_at": _tw_now().strftime("%Y-%m-%d %H:%M:%S"),
                }
        except Exception:
            continue

    return {
        "ok": False,
        "date": pd.to_datetime(target_date).strftime("%Y-%m-%d"),
        "source": "TWSE 三大法人",
        "error": "法人資料尚未取得、非交易日或連線失敗",
    }


def _save_inst_row(row: dict[str, Any]) -> None:
    if not isinstance(row, dict) or not row.get("ok"):
        return
    dt = pd.to_datetime(row.get("date") or _tw_now().date(), errors="coerce")
    if pd.isna(dt):
        dt = pd.Timestamp(_tw_now().date())
    ymd = dt.strftime("%Y%m%d")
    cache = _read_inst_cache()
    cache[ymd] = row
    _write_inst_cache(cache)


def _default_inst_row(target_date: date) -> dict[str, Any]:
    cache = _read_inst_cache()
    ymd = pd.to_datetime(target_date).strftime("%Y%m%d")
    if isinstance(cache.get(ymd), dict):
        return cache[ymd]
    if cache:
        keys = sorted([k for k in cache.keys() if isinstance(cache.get(k), dict)], reverse=True)
        if keys:
            row = dict(cache[keys[0]])
            row["source"] = _safe_str(row.get("source")) + "｜最近快取"
            return row
    return {"ok": False, "date": pd.to_datetime(target_date).strftime("%Y-%m-%d"), "source": "尚未更新", "total_100m": None}


def _institutional_score(inst: dict[str, Any]) -> dict[str, Any]:
    total = _safe_float(inst.get("total_100m"), 0) or 0
    foreign = _safe_float(inst.get("foreign_100m"), 0) or 0
    invest = _safe_float(inst.get("investment_trust_100m"), 0) or 0
    score = 50
    score += max(min(total * 0.25, 18), -18)
    score += max(min(foreign * 0.18, 12), -12)
    score += max(min(invest * 0.35, 10), -10)
    score = max(0, min(100, score))

    if score >= 70:
        label = "法人偏多"
        advice = "可提高順勢與回測承接權重。"
    elif score >= 55:
        label = "法人中性偏多"
        advice = "可正常觀察，但仍看技術確認。"
    elif score >= 45:
        label = "法人中性"
        advice = "法人沒有明顯方向，回到個股條件。"
    elif score >= 30:
        label = "法人偏空"
        advice = "降低追價，優先防守。"
    else:
        label = "法人明顯偏空"
        advice = "建議縮小部位或觀望。"

    return {"法人分數": round(score, 1), "法人狀態": label, "法人建議": advice}


def _inst_cache_to_df() -> pd.DataFrame:
    cache = _read_inst_cache()
    rows = []
    if isinstance(cache, dict):
        for k, v in cache.items():
            if not isinstance(v, dict):
                continue
            rows.append({
                "日期": v.get("date") or k,
                "外資億元": v.get("foreign_100m"),
                "投信億元": v.get("investment_trust_100m"),
                "自營商億元": v.get("dealer_100m"),
                "三大法人合計億元": v.get("total_100m"),
                "來源": v.get("source"),
                "更新時間": v.get("updated_at"),
            })
    if not rows:
        return pd.DataFrame(columns=["日期", "外資億元", "投信億元", "自營商億元", "三大法人合計億元", "來源", "更新時間"])
    df = pd.DataFrame(rows)
    df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
    return df.dropna(subset=["日期"]).sort_values("日期").reset_index(drop=True)


def _num_tw(v: Any):
    s = _safe_str(v).replace(",", "").replace("+", "").replace("%", "")
    if not s or s in {"-", "--", "None", "nan"}:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _fetch_twse_realtime(timeout: float = 1.8) -> dict[str, Any]:
    """手動按鈕才會呼叫。避免進頁卡住。"""
    url = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp"
    params = {
        "ex_ch": "tse_t00.tw",
        "json": "1",
        "delay": "0",
        "_": int(_tw_now().timestamp() * 1000),
    }
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://mis.twse.com.tw/stock/fibest.jsp?stock=t00",
    }
    try:
        r = requests.get(url, params=params, headers=headers, timeout=timeout, verify=False)
        if r.status_code != 200:
            return {"ok": False, "source": "TWSE MIS", "error": f"HTTP {r.status_code}"}
        data = r.json()
        arr = data.get("msgArray") or []
        if not arr:
            return {"ok": False, "source": "TWSE MIS", "error": "msgArray empty"}
        row = arr[0]
        current = _num_tw(row.get("z"))
        prev = _num_tw(row.get("y"))
        open_v = _num_tw(row.get("o"))
        high_v = _num_tw(row.get("h"))
        low_v = _num_tw(row.get("l"))
        if current is None:
            return {"ok": False, "source": "TWSE MIS", "error": "目前非盤中或即時值尚未提供", "prev_close": prev}
        pct = ((current - prev) / prev * 100) if prev not in [None, 0] else None
        d = _safe_str(row.get("d"))
        used_date = f"{d[:4]}-{d[4:6]}-{d[6:8]}" if len(d) == 8 else _tw_now().strftime("%Y-%m-%d")
        return {
            "ok": True,
            "source": "TWSE MIS 盤中即時",
            "date": used_date,
            "used_date": used_date,
            "time": _safe_str(row.get("t")),
            "close": current,
            "prev_close": prev,
            "open": open_v,
            "high": high_v,
            "low": low_v,
            "pct": pct,
            "is_realtime": True,
        }
    except Exception as e:
        return {"ok": False, "source": "TWSE MIS", "error": "連線失敗或 SSL 憑證被雲端環境攔截，請改按收盤紀錄或稍後再試。"}


def _fetch_twse_close(target_date: date, timeout: float = 2.5) -> dict[str, Any]:
    """手動按鈕才會呼叫。收盤後 / 晚上抓收盤資料。"""
    ymd = pd.to_datetime(target_date).strftime("%Y%m%d")
    urls = [
        f"https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?date={ymd}&type=IND&response=json",
        f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={ymd}&type=IND",
    ]

    def walk_rows(obj):
        if isinstance(obj, list):
            if obj and all(not isinstance(x, (list, dict)) for x in obj):
                yield obj
            for x in obj:
                yield from walk_rows(x)
        elif isinstance(obj, dict):
            for v in obj.values():
                yield from walk_rows(v)

    for url in urls:
        try:
            r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout, verify=False)
            if r.status_code != 200:
                continue
            data = r.json()
            for row in walk_rows(data):
                joined = " ".join(_safe_str(x) for x in row)
                if "發行量加權股價指數" not in joined and "TAIEX" not in joined.upper():
                    continue
                nums = [_num_tw(x) for x in row]
                nums = [x for x in nums if x is not None]
                if not nums:
                    continue
                close = nums[0]
                pct = None
                if len(nums) >= 3 and abs(nums[-1]) < 20:
                    pct = nums[-1]
                return {
                    "ok": True,
                    "source": "TWSE 收盤紀錄",
                    "date": pd.to_datetime(target_date).strftime("%Y-%m-%d"),
                    "used_date": pd.to_datetime(target_date).strftime("%Y-%m-%d"),
                    "close": close,
                    "pct": pct,
                    "is_realtime": False,
                }
        except Exception:
            continue
    return {"ok": False, "source": "TWSE 收盤紀錄", "error": "收盤資料尚未取得或交易所未發布"}


def _default_market_row(target_date: date) -> dict[str, Any]:
    cache = _read_cache()
    ymd = pd.to_datetime(target_date).strftime("%Y%m%d")
    if isinstance(cache.get(ymd), dict):
        return cache[ymd]

    # 找最近一筆快取
    if cache:
        keys = sorted([k for k in cache.keys() if isinstance(cache.get(k), dict)], reverse=True)
        if keys:
            row = dict(cache[keys[0]])
            row["source"] = _safe_str(row.get("source")) + "｜最近快取"
            return row

    return {
        "ok": False,
        "source": "尚未更新",
        "date": pd.to_datetime(target_date).strftime("%Y-%m-%d"),
        "used_date": pd.to_datetime(target_date).strftime("%Y-%m-%d"),
        "close": None,
        "pct": None,
        "is_realtime": False,
        "error": "尚未按更新，目前不呼叫外部資料源",
    }


def _save_market_row(row: dict[str, Any]) -> None:
    if not isinstance(row, dict) or row.get("close") is None:
        return
    dt = pd.to_datetime(row.get("date") or row.get("used_date") or _tw_now().date(), errors="coerce")
    if pd.isna(dt):
        dt = pd.Timestamp(_tw_now().date())
    ymd = dt.strftime("%Y%m%d")
    cache = _read_cache()
    cache[ymd] = row
    _write_cache(cache)


def _recent_business_dates(end_date: date, days: int = 20) -> list[date]:
    out = []
    cur = pd.to_datetime(end_date).date()
    guard = 0
    while len(out) < int(days) and guard < 60:
        if cur.weekday() < 5:
            out.append(cur)
        cur = cur - timedelta(days=1)
        guard += 1
    return list(reversed(out))


def _batch_fetch_close_cache(end_date: date, days: int = 20) -> tuple[int, list[str]]:
    """
    v27.1：手動補抓近 N 個交易日收盤紀錄。
    僅在使用者按鈕觸發，不會進頁自動跑，避免卡住。
    """
    cache = _read_cache()
    added = 0
    messages = []
    for d in _recent_business_dates(end_date, days):
        ymd = pd.to_datetime(d).strftime("%Y%m%d")
        if isinstance(cache.get(ymd), dict) and cache[ymd].get("close") is not None:
            messages.append(f"{ymd} 已有快取，略過")
            continue
        row = _fetch_twse_close(d, timeout=2.0)
        if row.get("ok"):
            cache[ymd] = row
            added += 1
            messages.append(f"{ymd} 收盤 {row.get('close')} 已加入")
        else:
            messages.append(f"{ymd} 未取得：{row.get('error')}")
    _write_cache(cache)
    return added, messages


def _cache_download_csv_bytes() -> bytes:
    df = _cache_to_market_df() if "_cache_to_market_df" in globals() else pd.DataFrame()
    if df is None or df.empty:
        return b""
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")


def _render_market_cache_chart():
    df = _cache_to_market_df()
    if df is None or df.empty or len(df) < 2:
        st.info("大盤快取資料不足，補抓近20日收盤後，這裡會顯示收盤趨勢。")
        return
    st.markdown("### 大盤快取趨勢")
    chart_df = df[["日期", "收盤"]].copy()
    chart_df["日期"] = pd.to_datetime(chart_df["日期"]).dt.strftime("%Y-%m-%d")
    st.line_chart(chart_df.set_index("日期"), use_container_width=True)


BRIDGE_FILE = "macro_mode_bridge.json"


def _build_macro_bridge_payload(row: dict[str, Any]) -> dict[str, Any]:
    factors = _calc_stable_market_factors(row)
    inst = _default_inst_row(pd.to_datetime(row.get("date") or row.get("used_date") or date.today(), errors="coerce").date() if pd.notna(pd.to_datetime(row.get("date") or row.get("used_date") or date.today(), errors="coerce")) else date.today())
    inst_score = _institutional_score(inst)
    close_val = _safe_float(row.get("close"))
    pct_val = _safe_float(row.get("pct"))
    payload = {
        "version": "v27.5_macro_bridge",
        "updated_at": _tw_now().strftime("%Y-%m-%d %H:%M:%S"),
        "market_date": _safe_str(row.get("used_date") or row.get("date")),
        "source": _safe_str(row.get("source")),
        "is_realtime": bool(row.get("is_realtime")),
        "close": close_val,
        "pct": pct_val,
        "market_score": _safe_float(factors.get("大盤穩定分"), 50),
        "market_state": _safe_str(factors.get("大盤狀態")),
        "godpick_weight_advice": _safe_str(factors.get("股神推薦加權")),
        "strategy": _safe_str(factors.get("今日策略")),
        "ma5": _safe_float(factors.get("MA5")),
        "ma20": _safe_float(factors.get("MA20")),
        "dist_ma5_pct": _safe_float(factors.get("距MA5%")),
        "dist_ma20_pct": _safe_float(factors.get("距MA20%")),
        "position_20d_pct": _safe_float(factors.get("20日位置%")),
        "cache_count": int(factors.get("快取筆數") or 0),
        "institutional_date": _safe_str(inst.get("date")),
        "institutional_score": _safe_float(inst_score.get("法人分數"), 50),
        "institutional_state": _safe_str(inst_score.get("法人狀態")),
        "institutional_advice": _safe_str(inst_score.get("法人建議")),
        "foreign_100m": _safe_float(inst.get("foreign_100m")),
        "investment_trust_100m": _safe_float(inst.get("investment_trust_100m")),
        "dealer_100m": _safe_float(inst.get("dealer_100m")),
        "institutional_total_100m": _safe_float(inst.get("total_100m")),
        "recommendation_bias": _macro_bias_from_score((_safe_float(factors.get("大盤穩定分"), 50) * 0.75) + (_safe_float(inst_score.get("法人分數"), 50) * 0.25)),
    }
    return payload


def _macro_bias_from_score(score: float) -> dict[str, Any]:
    score = _safe_float(score, 50) or 50
    if score >= 75:
        return {
            "risk_filter": "放寬",
            "preferred_modes": ["強勢突破", "低檔轉強", "拉回承接"],
            "avoid_modes": ["純題材追高"],
            "position_advice": "可正常至偏積極，但仍須控單檔風險。",
        }
    if score >= 60:
        return {
            "risk_filter": "正常",
            "preferred_modes": ["低檔轉強", "拉回承接", "回測支撐"],
            "avoid_modes": ["高位爆量追高"],
            "position_advice": "可正常操作，優先挑選有支撐與族群轉強的標的。",
        }
    if score >= 45:
        return {
            "risk_filter": "中性",
            "preferred_modes": ["回測支撐", "低檔止跌"],
            "avoid_modes": ["追高突破", "無量反彈"],
            "position_advice": "降低追價，採分批與小部位。",
        }
    if score >= 30:
        return {
            "risk_filter": "偏嚴",
            "preferred_modes": ["低檔止跌", "防守型回測"],
            "avoid_modes": ["強勢追價", "高波動題材"],
            "position_advice": "保守操作，等待確認再進場。",
        }
    return {
        "risk_filter": "嚴格",
        "preferred_modes": ["觀望", "極低檔止跌"],
        "avoid_modes": ["追高", "突破", "高槓桿題材"],
        "position_advice": "以觀望與現金比重為主。",
    }


def _write_macro_bridge(row: dict[str, Any]) -> tuple[bool, str]:
    payload = _build_macro_bridge_payload(row)
    try:
        Path(BRIDGE_FILE).write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        return True, f"已寫入 {BRIDGE_FILE}，股神推薦可讀取大盤穩定因子。"
    except Exception as e:
        return False, f"寫入 {BRIDGE_FILE} 失敗：{e}"


def _read_macro_bridge() -> dict[str, Any]:
    p = Path(BRIDGE_FILE)
    if not p.exists():
        return {}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _render_macro_bridge_block(row: dict[str, Any]):
    st.markdown("### 股神推薦串聯")
    payload = _build_macro_bridge_payload(row)
    c1, c2, c3, c4 = st.columns([1.2, 1.2, 1.2, 2.4])
    with c1:
        if st.button("寫入股神大盤參考", use_container_width=True, type="primary"):
            ok, msg = _write_macro_bridge(row)
            if ok:
                st.success(msg)
            else:
                st.warning(msg)
    with c2:
        st.metric("推薦風控", payload.get("recommendation_bias", {}).get("risk_filter", "中性"))
    with c3:
        st.metric("建議加權", payload.get("godpick_weight_advice", "0%"))
    with c4:
        st.caption("v27.5：將大盤穩定分、策略、風控建議寫入 macro_mode_bridge.json，後續可讓 7_股神推薦讀取。")

    with st.expander("股神大盤橋接檔內容", expanded=False):
        st.json(payload)
        old_bridge = _read_macro_bridge()
        if old_bridge:
            st.write("目前已存在橋接檔：")
            st.json(old_bridge)


def _macro_feature_status_df() -> pd.DataFrame:
    rows = [
        {
            "功能": "頁面先顯示",
            "目前狀態": "已恢復",
            "是否自動執行": "是",
            "說明": "進頁後直接顯示，不再等待外部API。",
        },
        {
            "功能": "盤中即時大盤",
            "目前狀態": "已恢復",
            "是否自動執行": "否，手動按鈕",
            "說明": "按「更新盤中即時大盤」才抓 TWSE MIS，避免卡住。",
        },
        {
            "功能": "晚上/收盤紀錄",
            "目前狀態": "已恢復",
            "是否自動執行": "否，手動按鈕",
            "說明": "按「更新收盤紀錄」或「補抓近20日收盤」。",
        },
        {
            "功能": "大盤穩定因子",
            "目前狀態": "已恢復",
            "是否自動執行": "是，讀本機快取",
            "說明": "使用 macro_market_close_cache.json 計算 MA5、MA20、20日位置與大盤穩定分。",
        },
        {
            "功能": "股神推薦橋接",
            "目前狀態": "已恢復",
            "是否自動執行": "否，手動寫入",
            "說明": "按「寫入股神大盤參考」產生 macro_mode_bridge.json。",
        },
        {
            "功能": "大盤趨勢圖",
            "目前狀態": "已恢復",
            "是否自動執行": "是，讀本機快取",
            "說明": "用本機快取畫收盤趨勢，不抓外部資料。",
        },
        {
            "功能": "大盤快取下載",
            "目前狀態": "已恢復",
            "是否自動執行": "否，下載按鈕",
            "說明": "可下載 macro_market_close_cache.csv。",
        },
        {
            "功能": "Google News 事件因子",
            "目前狀態": "暫停",
            "是否自動執行": "否",
            "說明": "先暫停，這是之前最容易造成頁面等待的來源之一。",
        },
        {
            "功能": "TAIFEX 期權因子",
            "目前狀態": "暫停",
            "是否自動執行": "否",
            "說明": "先暫停，之後可改成手動更新模式。",
        },
        {
            "功能": "完整法人籌碼",
            "目前狀態": "已恢復",
            "是否自動執行": "否，手動按鈕",
            "說明": "v27.6 已改為手動更新三大法人並寫入本機快取。",
        },
        {
            "功能": "Yahoo / Stooq 外盤模型",
            "目前狀態": "暫停",
            "是否自動執行": "否",
            "說明": "先暫停，避免外部站台延遲造成頁面卡住。",
        },
        {
            "功能": "完整大盤模式預估",
            "目前狀態": "暫停",
            "是否自動執行": "否",
            "說明": "舊版會自動跑完整模型，已改為先穩定顯示，再逐項回補。",
        },
    ]
    return pd.DataFrame(rows)


def _render_institutional_block(target_date: date):
    st.markdown("### 法人籌碼手動回補")
    inst = _default_inst_row(target_date)
    score = _institutional_score(inst)

    c1, c2, c3, c4, c5 = st.columns([1.25, 1.1, 1.1, 1.1, 1.5])
    with c1:
        update_inst = st.button("更新三大法人", use_container_width=True)
    with c2:
        st.metric("法人分數", f"{_safe_float(score.get('法人分數'), 50):.1f}")
    with c3:
        st.metric("法人狀態", _safe_str(score.get("法人狀態")))
    with c4:
        st.metric("合計億元", f"{_safe_float(inst.get('total_100m'), 0):+.2f}")
    with c5:
        st.caption("只在按下時抓 TWSE 法人資料，不會進頁自動執行。")

    if update_inst:
        with st.spinner("正在手動更新 TWSE 三大法人，最多等待約 3 秒..."):
            row = _fetch_twse_institutional_manual(target_date)
        if row.get("ok"):
            _save_inst_row(row)
            st.success(f"三大法人更新成功：合計 { _safe_float(row.get('total_100m'), 0):+.2f} 億元")
            st.rerun()
        else:
            st.warning(f"三大法人更新失敗：{row.get('error')}")

    detail = [
        ("資料日期", _safe_str(inst.get("date")) or "尚未更新"),
        ("外資", f"{_safe_float(inst.get('foreign_100m'), 0):+.2f} 億"),
        ("投信", f"{_safe_float(inst.get('investment_trust_100m'), 0):+.2f} 億"),
        ("自營商", f"{_safe_float(inst.get('dealer_100m'), 0):+.2f} 億"),
        ("法人建議", _safe_str(score.get("法人建議"))),
    ]
    cols = st.columns(len(detail))
    for col, (title, value) in zip(cols, detail):
        with col:
            st.markdown(
                f"""
                <div style="border:1px solid #e2e8f0;border-radius:14px;padding:12px;background:#ffffff;min-height:84px;">
                    <div style="font-size:13px;color:#64748b;font-weight:800;">{title}</div>
                    <div style="font-size:15px;color:#0f172a;font-weight:900;margin-top:8px;">{value}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    with st.expander("法人籌碼快取明細", expanded=False):
        df = _inst_cache_to_df()
        st.dataframe(df, use_container_width=True, hide_index=True)
        if not df.empty:
            st.download_button(
                "下載法人快取CSV",
                data=df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"),
                file_name="macro_institutional_cache.csv",
                mime="text/csv",
                use_container_width=True,
            )


def _render_macro_feature_center():
    st.markdown("### 大盤功能管理中心")
    st.caption("v27.5：把 0_大盤趨勢的功能狀態攤開顯示，避免不知道哪些功能已恢復、哪些先暫停。")
    df = _macro_feature_status_df()

    active_count = int((df["目前狀態"] == "已恢復").sum())
    paused_count = int((df["目前狀態"] == "暫停").sum())

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("已恢復功能", active_count)
    with c2:
        st.metric("暫停功能", paused_count)
    with c3:
        st.metric("卡頓風險", "低")
    with c4:
        st.metric("外部API自動執行", "0")

    st.dataframe(df, use_container_width=True, hide_index=True)

    with st.expander("後續建議回補順序", expanded=False):
        st.write("1. 先回補：法人買賣超，但改成手動按鈕 + 本機快取。")
        st.write("2. 再回補：外盤 NASDAQ / SOX，但只抓收盤資料，不自動等待。")
        st.write("3. 再回補：TAIFEX 期權，同樣改成手動更新。")
        st.write("4. 最後才回補：Google News，因為最容易慢。")
        st.warning("重點：不要再讓任何外部資料源進頁自動執行，否則大盤頁會再次卡住。")


def _score_context(row: dict[str, Any]) -> dict[str, str]:
    pct = _safe_float(row.get("pct"), 0) or 0
    if pct >= 1.0:
        mood = "強多"
        advice = "可偏積極，但避免追高。"
    elif pct >= 0.3:
        mood = "偏多"
        advice = "可順勢觀察強勢族群。"
    elif pct <= -1.0:
        mood = "偏空"
        advice = "保守控倉，優先等止跌。"
    elif pct <= -0.3:
        mood = "偏弱"
        advice = "降低追價，等待支撐確認。"
    else:
        mood = "震盪"
        advice = "以低檔拉回、支撐回測為主。"
    return {"mood": mood, "advice": advice}


def _cache_to_market_df() -> pd.DataFrame:
    cache = _read_cache()
    rows = []
    if isinstance(cache, dict):
        for k, v in cache.items():
            if not isinstance(v, dict):
                continue
            dt = pd.to_datetime(v.get("date") or v.get("used_date") or k, errors="coerce")
            close = _safe_float(v.get("close"))
            pct = _safe_float(v.get("pct"))
            if pd.isna(dt) or close is None:
                continue
            rows.append(
                {
                    "日期": dt,
                    "收盤": close,
                    "漲跌幅%": pct,
                    "來源": _safe_str(v.get("source")),
                    "即時": bool(v.get("is_realtime")),
                }
            )
    if not rows:
        return pd.DataFrame(columns=["日期", "收盤", "漲跌幅%", "來源", "即時"])
    df = pd.DataFrame(rows).drop_duplicates(subset=["日期"], keep="last")
    return df.sort_values("日期").reset_index(drop=True)


def _calc_stable_market_factors(row: dict[str, Any]) -> dict[str, Any]:
    df = _cache_to_market_df()
    close = _safe_float(row.get("close"))
    pct = _safe_float(row.get("pct"), 0) or 0
    if close is None and not df.empty:
        close = _safe_float(df["收盤"].iloc[-1])
    if close is None:
        close = 0.0

    if not df.empty:
        closes = pd.to_numeric(df["收盤"], errors="coerce").dropna()
    else:
        closes = pd.Series(dtype=float)

    ma5 = float(closes.tail(5).mean()) if len(closes) >= 2 else close
    ma20 = float(closes.tail(20).mean()) if len(closes) >= 5 else close
    high20 = float(closes.tail(20).max()) if len(closes) >= 2 else close
    low20 = float(closes.tail(20).min()) if len(closes) >= 2 else close

    dist_ma5 = ((close / ma5 - 1) * 100) if ma5 not in [None, 0] else 0
    dist_ma20 = ((close / ma20 - 1) * 100) if ma20 not in [None, 0] else 0
    pos20 = ((close - low20) / (high20 - low20) * 100) if high20 != low20 else 50

    score = 50
    score += max(min(pct * 8, 12), -12)
    score += 8 if close >= ma5 else -8
    score += 10 if close >= ma20 else -10
    if pos20 >= 75:
        score += 6
    elif pos20 <= 25:
        score -= 6
    score = max(0, min(100, score))

    if score >= 75:
        market_state = "偏多可操作"
        godpick_weight = "+10%"
        strategy = "追強與回測支撐並行"
    elif score >= 60:
        market_state = "中性偏多"
        godpick_weight = "+5%"
        strategy = "優先挑選低檔轉強與拉回承接"
    elif score >= 45:
        market_state = "震盪觀望"
        godpick_weight = "0%"
        strategy = "降低追價，等支撐回測"
    elif score >= 30:
        market_state = "偏弱保守"
        godpick_weight = "-10%"
        strategy = "只做高勝率低檔止跌，不追高"
    else:
        market_state = "風險偏高"
        godpick_weight = "-20%"
        strategy = "以觀望與資金控管為主"

    return {
        "大盤穩定分": round(score, 1),
        "大盤狀態": market_state,
        "股神推薦加權": godpick_weight,
        "今日策略": strategy,
        "MA5": ma5,
        "MA20": ma20,
        "距MA5%": dist_ma5,
        "距MA20%": dist_ma20,
        "20日位置%": pos20,
        "快取筆數": len(df),
    }


def _render_stable_factor_block(row: dict[str, Any]):
    factors = _calc_stable_market_factors(row)
    st.markdown("### 大盤穩定因子")
    render_pro_kpi_row([
        {
            "label": "大盤穩定分",
            "value": f"{_safe_float(factors.get('大盤穩定分'), 0):.1f}",
            "delta": _safe_str(factors.get("大盤狀態")),
            "delta_class": "pro-kpi-delta-flat",
        },
        {
            "label": "股神推薦加權",
            "value": _safe_str(factors.get("股神推薦加權")),
            "delta": "僅作推薦頁參考",
            "delta_class": "pro-kpi-delta-flat",
        },
        {
            "label": "今日策略",
            "value": _safe_str(factors.get("今日策略")),
            "delta": f"快取 {factors.get('快取筆數')} 筆",
            "delta_class": "pro-kpi-delta-flat",
        },
        {
            "label": "20日位置",
            "value": f"{_safe_float(factors.get('20日位置%'), 0):.1f}%",
            "delta": f"距MA20 {_safe_float(factors.get('距MA20%'), 0):+.2f}%",
            "delta_class": "pro-kpi-delta-flat",
        },
    ])

    with st.expander("大盤穩定因子明細", expanded=False):
        st.dataframe(_cache_to_market_df(), use_container_width=True, hide_index=True)
        st.json(factors)


def main():
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    inject_pro_theme()

    render_pro_hero(
        title="大盤走勢｜緊急穩定版",
        subtitle="本頁預設完全不呼叫外部 API，先顯示畫面；按下更新才抓盤中即時或收盤紀錄。",
    )

    st.warning("目前使用 v26.9 緊急穩定版：不會自動跑外部資料與完整模型，避免頁面一直轉圈。")

    c1, c2, c3, c4, c5 = st.columns([1.25, 1.25, 1.35, 1.2, 2.1])
    with c1:
        update_realtime = st.button("更新盤中即時大盤", use_container_width=True, type="primary")
    with c2:
        update_close = st.button("更新收盤紀錄", use_container_width=True)
    with c3:
        batch_close = st.button("補抓近20日收盤", use_container_width=True)
    with c4:
        clear_cache = st.button("清除大盤快取", use_container_width=True)
    with c5:
        target_date = st.date_input("大盤日期", value=date.today(), key=_k("target_date"))

    if clear_cache:
        _write_cache({})
        st.success("已清除 macro_market_close_cache.json")
        st.rerun()

    status_msg = ""
    if update_realtime:
        with st.spinner("正在抓取 TWSE 盤中即時大盤，最多等待約 2 秒..."):
            row = _fetch_twse_realtime()
        if row.get("ok"):
            _save_market_row(row)
            status_msg = f"盤中即時更新成功：{row.get('close')}"
            st.success(status_msg)
        else:
            st.warning(f"盤中即時更新失敗：{row.get('error')}")

    if update_close:
        with st.spinner("正在抓取 TWSE 收盤紀錄，最多等待約 3 秒..."):
            row = _fetch_twse_close(target_date)
        if row.get("ok"):
            _save_market_row(row)
            status_msg = f"收盤紀錄更新成功：{row.get('close')}"
            st.success(status_msg)
        else:
            st.warning(f"收盤紀錄更新失敗：{row.get('error')}")

    if batch_close:
        with st.spinner("正在手動補抓近20日收盤資料；只在按下時執行，不會自動卡住頁面..."):
            added, msgs = _batch_fetch_close_cache(target_date, days=20)
        if added > 0:
            st.success(f"近20日收盤補抓完成，新增 {added} 筆。")
        else:
            st.warning("近20日收盤沒有新增資料，可能都已存在或交易所尚未提供。")
        with st.expander("補抓明細", expanded=False):
            for msg in msgs:
                st.write(f"- {msg}")

    row = _default_market_row(target_date)
    ctx = _score_context(row)

    close_val = _safe_float(row.get("close"))
    pct_val = _safe_float(row.get("pct"))

    render_pro_kpi_row([
        {
            "label": "目前大盤",
            "value": f"{close_val:,.2f}" if close_val is not None else "尚未更新",
            "delta": f"{pct_val:+.2f}%" if pct_val is not None else "無漲跌幅",
            "delta_class": "pro-kpi-delta-up" if (pct_val or 0) >= 0 else "pro-kpi-delta-down",
        },
        {
            "label": "資料型態",
            "value": "盤中即時" if row.get("is_realtime") else "收盤/快取",
            "delta": _safe_str(row.get("source")),
            "delta_class": "pro-kpi-delta-flat",
        },
        {
            "label": "資料日期",
            "value": _safe_str(row.get("used_date") or row.get("date")),
            "delta": _safe_str(row.get("time")),
            "delta_class": "pro-kpi-delta-flat",
        },
        {
            "label": "大盤狀態",
            "value": ctx["mood"],
            "delta": ctx["advice"],
            "delta_class": "pro-kpi-delta-flat",
        },
    ])

    _render_stable_factor_block(row)
    _render_institutional_block(target_date)
    _render_market_cache_chart()
    _render_macro_bridge_block(row)
    _render_macro_feature_center()

    dl_cols = st.columns([1.2, 4])
    with dl_cols[0]:
        st.download_button(
            "下載大盤快取CSV",
            data=_cache_download_csv_bytes(),
            file_name="macro_market_close_cache.csv",
            mime="text/csv",
            use_container_width=True,
            disabled=_cache_to_market_df().empty,
        )
    with dl_cols[1]:
        st.caption("v27.1：快取資料越完整，大盤穩定分、MA5/MA20、20日位置越有參考性。")

    # v26.10：改用原生 markdown 區塊，避免部分 theme card 在此頁輸出殘留 </div>。
    st.markdown("### 大盤操作參考")
    ref_cols = st.columns(4)
    ref_items = [
        ("目前狀態", ctx["mood"]),
        ("操作建議", ctx["advice"]),
        ("資料來源", _safe_str(row.get("source"))),
        ("重要說明", "先確保頁面不再卡住；完整法人/期權/新聞模型暫時停用，等大盤頁穩定後再逐項加回。"),
    ]
    for _col, (_title, _value) in zip(ref_cols, ref_items):
        with _col:
            st.markdown(
                f"""
                <div style="border:1px solid #e2e8f0;border-radius:14px;padding:14px;background:#ffffff;min-height:92px;">
                    <div style="font-size:13px;color:#64748b;font-weight:800;">{_title}</div>
                    <div style="font-size:16px;color:#0f172a;font-weight:900;margin-top:8px;">{_value}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    with st.expander("大盤快取 / 除錯資料", expanded=False):
        st.json(_read_cache())
        st.write("目前列：")
        st.json(row)


if __name__ == "__main__":
    main()
