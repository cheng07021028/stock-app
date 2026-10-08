# -*- coding: utf-8 -*-
"""V191-H109 cross-layer veto, event/freshness and as-of-safe revenue evidence.

Research-only authority: never grants a trade. Optional TWSE revenue fetch is
latest-only; historical/backtest dates NEVER fetch current observations.
"""
from __future__ import annotations
import datetime as dt
import json
import math
import os
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

VERSION = "v191_h109_cross_layer_authority_20261008"
REVENUE_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap05_L"
CACHE_NAME = "godpick_h109_official_revenue_cache.json"


def _runtime_dir(base_dir: Path) -> Path:
    """Honor a mounted/persistent data directory without creating it implicitly."""
    override = t(os.getenv("GODPICK_H109_DATA_DIR"))
    p = Path(override).expanduser() if override else Path(base_dir)
    return p if p.is_dir() else Path(base_dir)


def t(v: Any) -> str:
    if v is None: return ""
    try:
        if isinstance(v, float) and math.isnan(v): return ""
    except (TypeError, ValueError): pass
    s = str(v).strip()
    return "" if s.lower() in {"nan", "none", "nat", "null", "--", "<na>"} else s


def f(v: Any) -> float | None:
    if isinstance(v, bool) or v is None: return None
    try:
        x = float(str(v).replace(",", "").replace("％", "").replace("%", "").replace("+", ""))
        return x if math.isfinite(x) else None
    except (TypeError, ValueError): return None


def n(row: dict, names: tuple[str, ...]) -> float | None:
    for k in names:
        value = f(row.get(k))
        if value is not None: return value
    return None


def label(row: dict, names: tuple[str, ...]) -> str:
    for k in names:
        value = t(row.get(k))
        if value: return value
    return ""


def parse_day(value: Any) -> dt.date | None:
    s = t(value)[:10].replace("/", "-")
    if s.isdigit() and len(s) == 8: s = f"{s[:4]}-{s[4:6]}-{s[6:]}"
    try: return dt.date.fromisoformat(s)
    except ValueError: return None


def parse_month(value: Any) -> tuple[int, int] | None:
    s = t(value).replace("/", "").replace("-", "").replace(" ", "")
    if not s.isdigit(): return None
    if len(s) == 8: s=s[:6]
    if len(s) == 7: s=s[:5]
    if len(s) == 5: year, month = int(s[:3])+1911, int(s[3:])
    elif len(s) == 6: year, month = int(s[:4]), int(s[4:])
    else: return None
    return (year, month) if 2000 <= year <= 2100 and 1 <= month <= 12 else None


def _month_gap(a: tuple[int,int], b: tuple[int,int]) -> int:
    return (a[0]-b[0])*12 + a[1]-b[1]


def revenue_freshness(row: dict, asof: dt.date | None) -> tuple[str, str]:
    period = parse_month(label(row, ("H109營收資料年月", "營收資料日期", "營收資料年月", "月營收資料日期", "資料年月")))
    if period is None: return "UNKNOWN｜未取得可驗證營收年月", ""
    if asof is None: return "UNKNOWN｜推薦資料日期未定", f"{period[0]}{period[1]:02d}"
    ref = (asof.year, asof.month)
    gap = _month_gap(ref, period)
    # Monthly statements are normally due by day 10 of following month, subject
    # to defined exceptions. Before the deadline do NOT falsely call old data stale.
    if gap < 0: return "FUTURE｜資料晚於推薦日，禁止使用", f"{period[0]}{period[1]:02d}"
    if gap == 0: return "CURRENT｜本月已公告", f"{period[0]}{period[1]:02d}"
    if gap == 1: return "FRESH｜上月已公告", f"{period[0]}{period[1]:02d}"
    insurance = any(k in label(row,("產業別", "類別", "產業類別")) for k in ("保險", "壽險", "產險"))
    # 2026 insurance-industry special extension to the 15th (when applicable).
    deadline = 15 if insurance and asof.year >= 2026 else 10
    if gap == 2 and asof.day <= deadline: return "PENDING｜上月仍在申報期間，需查最新", f"{period[0]}{period[1]:02d}"
    return "STALE｜資料期落後，停止當成新催化", f"{period[0]}{period[1]:02d}"


def official_snapshot(base_dir: Path, asof: dt.date | None) -> dict[str, dict]:
    """Fast cached optional official feed. Does not query future data in backtests.

    A server with external network disabled will degrade to MISSING; it will NOT
    silently use outdated revenue as current. Network attempts are capped at 3s
    and negative cached to avoid repeated rerun stalls.
    """
    if os.getenv("GODPICK_H109_OFFICIAL_FETCH", "1").strip() == "0": return {}
    now = dt.datetime.now(ZoneInfo("Asia/Taipei"))
    if asof != now.date() or now.hour < 15: return {}
    path = _runtime_dir(base_dir) / CACHE_NAME
    obj: dict = {}
    try:
        if path.is_file():
            obj = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(obj, dict): obj = {}
    except (OSError, ValueError): obj = {}
    last = parse_day(obj.get("fetched_date"))
    try: last_ts = dt.datetime.fromisoformat(t(obj.get("fetched_at")))
    except ValueError: last_ts = None
    fresh = last == now.date() and last_ts is not None and (now-last_ts).total_seconds() < (21600 if obj.get("status")=="OK" else 3600)
    if fresh: return obj.get("stocks", {}) if obj.get("status")=="OK" else {}
    stocks: dict[str,dict] = {}
    status = "FAILED"
    try:
        req = Request(REVENUE_URL, headers={"User-Agent":"SPT-Stock-App-H109/1.0", "Accept":"application/json"})
        with urlopen(req, timeout=3) as res:
            data = json.load(res)
        if not isinstance(data, list) or len(data) < 50: raise ValueError("unexpected payload")
        for rec in data:
            if not isinstance(rec, dict): continue
            code = t(rec.get("公司代號"))
            period = parse_month(rec.get("資料年月"))
            if not code or not period: continue
            # Never accept a report period after its as-of date.
            if _month_gap((asof.year, asof.month), period) < 0: continue
            stocks[code] = {"H109營收資料年月": f"{period[0]}{period[1]:02d}",
                            "H109官方營收YoY%": f(rec.get("營業收入-去年同月增減(%)")),
                            "H109官方營收MoM%": f(rec.get("營業收入-上月比較增減(%)")),
                            "H109營收來源": REVENUE_URL}
        if stocks: status = "OK"
    except (OSError, ValueError, TimeoutError, TypeError):
        status = "FAILED"
    payload = {"fetched_at":now.isoformat(), "fetched_date":str(now.date()), "status":status, "stocks":stocks}
    try:
        import tempfile
        fd, temp = tempfile.mkstemp(prefix=".h109_revenue_", suffix=".tmp", dir=path.parent)
        try:
            with os.fdopen(fd,"w",encoding="utf-8") as fp: json.dump(payload, fp, ensure_ascii=False)
            os.replace(temp,path)
        finally:
            if os.path.exists(temp): os.unlink(temp)
    except OSError: pass
    return stocks



def _roc_day(v: Any) -> dt.date | None:
    s = t(v).replace("/", "").replace("-", "")
    if len(s)==7 and s.isdigit():
        try: return dt.date(int(s[:3])+1911, int(s[3:5]), int(s[5:7]))
        except ValueError: return None
    return parse_day(v)


def official_announcements(base_dir: Path, asof: dt.date | None) -> dict[str,dict]:
    """As-of-safe TWSE listed official disclosures. Titles are evidence, NOT sentiment.

    Publication date/time, not event/fact date, is used to prevent leakage.
    """
    if os.getenv("GODPICK_H109_OFFICIAL_FETCH", "1").strip()=="0": return {}
    now=dt.datetime.now(ZoneInfo("Asia/Taipei"))
    if asof != now.date() or now.hour < 15: return {}
    path = _runtime_dir(base_dir)/"godpick_h109_official_announcements_cache.json"
    obj={}
    try:
        if path.is_file(): obj=json.loads(path.read_text(encoding="utf-8"))
    except (ValueError,OSError): pass
    try: last_ts=dt.datetime.fromisoformat(t(obj.get("fetched_at")))
    except ValueError: last_ts=None
    if last_ts is not None and (now-last_ts).total_seconds() < (21600 if obj.get("status")=="OK" else 3600):
        return obj.get("stocks",{}) if obj.get("status")=="OK" else {}
    data_map={}; status="FAILED"
    try:
        url="https://openapi.twse.com.tw/v1/opendata/t187ap04_L"
        with urlopen(Request(url,headers={"User-Agent":"SPT-Stock-App-H109/1.0","Accept":"application/json"}), timeout=3) as res:
            data=json.load(res)
        if not isinstance(data,list): raise ValueError("unexpected announcement payload")
        for entry in data:
            if not isinstance(entry,dict): continue
            normalized={str(k).strip():v for k,v in entry.items()}
            date=_roc_day(normalized.get("發言日期"))
            if date is None or date>asof or (asof-date).days>3: continue
            code=t(normalized.get("公司代號")); subject=t(normalized.get("主旨"))
            if not code or not subject: continue
            info={"H109重大訊息標題":subject[:180], "H109重大訊息發布日":str(date),
                  "H109重大訊息來源":url, "H109重大訊息狀態":"已取得公告｜正負需核對全文"}
            if code not in data_map or date>_roc_day(data_map[code]["H109重大訊息發布日"]):
                data_map[code]=info
        status="OK" if data else "EMPTY"
    except (ValueError,OSError,TimeoutError,TypeError): pass
    payload={"fetched_at":now.isoformat(),"status":status,"stocks":data_map}
    try:
        import tempfile
        fd,tmp=tempfile.mkstemp(prefix=".h109_news_",suffix=".tmp",dir=path.parent)
        try:
            with os.fdopen(fd,"w",encoding="utf-8") as fp: json.dump(payload,fp,ensure_ascii=False)
            os.replace(tmp,path)
        finally:
            if os.path.exists(tmp): os.unlink(tmp)
    except OSError: pass
    return data_map

def join_official(row: dict, official: dict | None) -> dict:
    r = dict(row)
    if not official: return r
    old = parse_month(label(r,("營收資料日期", "營收資料年月", "H109營收資料年月")))
    new = parse_month(official.get("H109營收資料年月"))
    if new and (old is None or _month_gap(new,old)>0):
        r.update(official)
        if official.get("H109官方營收YoY%") is not None: r["月營收YoY%_官方"] = official["H109官方營收YoY%"]
        if official.get("H109官方營收MoM%") is not None: r["月營收MoM%_官方"] = official["H109官方營收MoM%"]
    return r


def govern(row: dict, *, asof: dt.date | None = None) -> dict[str, Any]:
    """Final decision veto. Independent from novelty/rank/sector score."""
    day = asof or parse_day(label(row,("推薦日期", "資料日期", "交易日期", "行情日期", "H99目標交易日")))
    d1 = n(row,("今日漲幅%", "當日漲跌幅%", "今日漲跌幅%", "單日漲跌幅%"))
    h102 = n(row,("H102族群衝擊分",))
    phase = label(row,("H108族群生命週期",)).upper()
    confirm = n(row,("H108族群確認分",)) or 0
    momentum = label(row,("主流主升判定", "近期強勢狀態", "飆股雷達角色"))
    exec_status = label(row,("進場可執行判定", "H99進場狀態", "H51執行狀態", "H51買點狀態", "交易可執行狀態"))
    risk_message = " ".join(label(row,(x,)) for x in ("H51狀態", "H51建議操作", "進場可執行判定", "假強排除原因", "失效條件", "風險警示"))
    negative = label(row,("H109官方事件方向", "重大訊息方向", "新聞事件方向"))
    fresh, period = revenue_freshness(row,day)
    yoy = n(row,("H109官方營收YoY%", "月營收YoY%_官方", "月營收YoY%"))
    mom = n(row,("H109官方營收MoM%", "月營收MoM%_官方", "月營收MoM%"))
    if any(fresh.startswith(s) for s in ("STALE", "FUTURE", "UNKNOWN")):
        yoy = mom = None  # old or unverified numbers must not create new catalyst
    event_dir = "UNKNOWN｜未驗證"
    verified_event = bool(label(row,("H109事件來源", "重大訊息來源", "H109官方事件來源")))
    if verified_event and any(x in negative.upper() for x in ("NEGATIVE", "BEARISH", "負面", "重大利空")):
        event_dir = "NEGATIVE｜有來源的負面事件"
    elif verified_event and any(x in negative.upper() for x in ("POSITIVE", "BULLISH", "正面", "重大利多")):
        event_dir = "POSITIVE｜有來源的正面事件"
    elif negative:
        event_dir = "UNVERIFIED｜事件方向尚無來源證明"
    elif yoy is not None and mom is not None:
        if yoy <= 0 and mom <= -25: event_dir = "NEGATIVE｜營收雙轉弱"
        elif mom <= -25: event_dir = "CAUTION｜營收月減大，須檢驗季節性"
        elif yoy >= 20 and mom >= 0: event_dir = "POSITIVE｜營收雙增"
        else: event_dir = "NEUTRAL｜營收未有明確催化"
    elif mom is not None and mom <= -25:
        event_dir = "CAUTION｜營收月減大，須檢驗季節性"
    crash = d1 is not None and d1 <= -7.0
    breakdown = (any(s in momentum.upper() for s in ("假強排除", "FAKE-BREAK")) and
                 any(s in (exec_status+" "+risk_message).upper() for s in ("BLOCK", "WAIT-RECLAIM", "失守", "跌破")))
    shock = crash or breakdown
    sector_fade = "FADE" in phase or "退潮" in phase
    fast_conflict = sector_fade and h102 is not None and h102 >= 85
    sector_pending = (not sector_fade and h102 is not None and h102 >= 90 and
                      not any(s in phase for s in ("IGNITION", "EXPANSION")))
    veto_reason = []
    if crash: veto_reason.append("單日急跌≤-7%，需事件隔離及重新站穩")
    if breakdown: veto_reason.append("假強/結構失守且進場封鎖，不得新鮮補位")
    if sector_fade: veto_reason.append("H108族群FADE，禁止H105/H107補回主榜")
    if fast_conflict: veto_reason.append("H102高速點火與H108退潮衝突，先留Waiting複查")
    if event_dir.startswith("NEGATIVE") and d1 is not None and d1 <= -4:
        shock = True; veto_reason.append("負面事件與價格跌幅共振")
    if fresh.startswith("FUTURE"):
        authority = "FUTURE_DATA_VETO"; veto_reason.append("營收資料晚於推薦日，防止未來資料洩漏")
    elif shock: authority = "SHOCK_QUARANTINE"
    elif sector_fade: authority = "FADE_VETO"
    elif sector_pending: authority = "FAST_SHOCK_RECHECK"
    else: authority = "RESEARCH_ELIGIBLE"
    # Numeric score may help ordering *within* eligible pool; it never overrides veto.
    prio = n(row,("H108研究優先分", "H107新鮮機會分", "H105黑馬預發動分"))
    if prio is None: prio = 0.0
    if authority != "RESEARCH_ELIGIBLE": prio = min(prio, 49.0)
    if fresh.startswith(("STALE","FUTURE")): prio = max(0,prio-4)
    if event_dir.startswith("CAUTION"): prio = max(0,prio-5)
    if event_dir.startswith("POSITIVE") and fresh.startswith(("CURRENT", "FRESH")):
        prio = min(100,prio+3)
    window = label(row,("H105未來發動窗口",))
    if authority in ("SHOCK_QUARANTINE", "FADE_VETO", "FUTURE_DATA_VETO"): window = "隔離/退潮｜無預測買點"
    elif authority == "FAST_SHOCK_RECHECK": window = "高速族群衝擊｜待生命週期確認"
    elif not window or window == "未定": window = "NO-TIMING-CONFIDENCE｜時機未確認"
    reasons = "；".join(veto_reason) if veto_reason else ("高速衝擊待補證" if sector_pending else "H109無跨層否決")
    return {"H109版本":VERSION, "H109決策權威":authority, "H109否決原因":reasons,
            "H109官方營收新鮮度":fresh, "H109營收資料年月":period or label(row,("H109營收資料年月",)),
            "H109催化方向":event_dir,
            "H109重大訊息狀態":label(row,("H109重大訊息狀態",)) or "未接入/查無當日公告",
            "H109重大訊息標題":label(row,("H109重大訊息標題",)),
            "H109重大訊息發布日":label(row,("H109重大訊息發布日",)),
            "H109重大訊息來源":label(row,("H109重大訊息來源",)),
            "H109最終研究優先分":round(prio,2),
            "H109時機窗口":window, "H109跨層衝突": "H102_IGNITION_vs_H108_FADE" if fast_conflict else ("FAST_SHOCK_UNCONFIRMED" if sector_pending else "無"),
            "H109正式買進權限":"LOCKED｜Research，不產生Formal交易許可"}


def apply_quarantine_continuity(tables: dict, base_dir: Path, asof: dt.date | None, *, persist: bool = True) -> dict[str,int]:
    """Carry previous SHOCK quarantines for 3 distinct *observed* market days.

    A stock is not automatically rehabilitated by H107 novelty or a one-day bounce.
    The history is append/snapshot-style and never writes data during historical
    replay. It is a local runtime journal, NOT a trade authority.
    """
    stats={"cooldown":0,"fresh_shock":0,"history":0}
    if asof is None or os.getenv("GODPICK_H109_RISK_HISTORY","1")=="0": return stats
    path=_runtime_dir(base_dir)/"godpick_h109_quarantine_history.json"
    stored={}
    try:
        if path.is_file(): stored=json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(stored,dict): stored={}
    except (OSError,ValueError): stored={}
    today=str(asof)
    valid={}
    for code,record in stored.items():
        if not isinstance(record,dict): continue
        d=parse_day(record.get("first_day"))
        if d and d<=asof and (asof-d).days<=45: valid[code]=record
    changed=False
    # Eval both pools on the same date; no duplicate day increment on Streamlit reruns.
    for name in ("research","waiting"):
        df=tables.get(name)
        if df is None or getattr(df,"empty",True) or "股票代號" not in df.columns: continue
        for i,r in df.iterrows():
            code=t(r.get("股票代號"))
            if code.endswith(".0") and code[:-2].isdigit(): code=code[:-2]
            auth=t(r.get("H109決策權威"))
            if auth=="SHOCK_QUARANTINE":
                record=valid.get(code, {})
                dates=record.get("observed_days",[])
                if not isinstance(dates,list): dates=[]
                dates=[d for d in dates if parse_day(d) and parse_day(d)<=asof]
                if today not in dates: dates.append(today)
                valid[code]={"first_day":record.get("first_day") or today, "observed_days":sorted(set(dates)),
                             "last_shock_day":today, "last_reason":t(r.get("H109否決原因"))}
                stats["fresh_shock"]+=1; changed=True
            elif code in valid:
                record=valid[code]
                dates=[d for d in record.get("observed_days",[]) if parse_day(d) and parse_day(d)<=asof]
                if today not in dates: dates.append(today); changed=True
                dates=sorted(set(dates))
                record["observed_days"]=dates
                if len(dates)<=3:
                    df.at[i,"H109決策權威"]="COOLDOWN_QUARANTINE"
                    df.at[i,"H109否決原因"]="H109急跌後冷卻期｜需跨3個不同交易日重新驗證"
                    df.at[i,"H109最終研究優先分"]=min(f(r.get("H109最終研究優先分")) or 0,49)
                    df.at[i,"H109時機窗口"]="冷卻追蹤｜不是進場訊號"
                    stats["cooldown"]+=1
                else:
                    # Stays eligible only if all today's hard and sector vetoes passed.
                    valid.pop(code,None); changed=True
            df.at[i,"H109隔離觀察日數"]=len(valid.get(code,{}).get("observed_days",[]))
    stats["history"]=len(valid)
    # Only persist live after close, no writing during historical replay/backtests.
    now=dt.datetime.now(ZoneInfo("Asia/Taipei"))
    if not persist or asof!=now.date() or now.hour < 15 or not changed: return stats
    try:
        import tempfile
        fd,tmp=tempfile.mkstemp(prefix=".h109_risk_",suffix=".tmp",dir=path.parent)
        try:
            with os.fdopen(fd,"w",encoding="utf-8") as fp: json.dump(valid,fp,ensure_ascii=False,indent=2)
            os.replace(tmp,path)
        finally:
            if os.path.exists(tmp): os.unlink(tmp)
    except OSError: pass
    return stats
