
from __future__ import annotations

import base64
import concurrent.futures
import datetime as dt
import hashlib
import html
import json
import re
from typing import Any, Callable

import pandas as pd
import requests
import streamlit as st

try:
    from utils import get_all_code_name_map
except Exception:
    def get_all_code_name_map(market: str = "") -> pd.DataFrame:  # type: ignore
        return pd.DataFrame()


# =========================================================
# Core helpers
# =========================================================

def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v).strip()


def _normalize_code(v: Any) -> str:
    s = _safe_str(v)
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) == 4:
        return digits
    return ""


def _now_text() -> str:
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _master_cols() -> list[str]:
    return [
        "code",
        "name",
        "market",
        "official_industry_raw",
        "official_industry_raw_col",
        "official_industry",
        "theme_category",
        "category",
        "source",
        "source_api",
        "source_rank",
        "待修原因",
    ]


def empty_master_df() -> pd.DataFrame:
    return pd.DataFrame(columns=_master_cols())


OFFICIAL_INDUSTRY_CODE_MAP = {
    "01": "水泥工業",
    "02": "食品工業",
    "03": "塑膠工業",
    "04": "紡織纖維",
    "05": "電機機械",
    "06": "電器電纜",
    "08": "玻璃陶瓷",
    "09": "造紙工業",
    "10": "鋼鐵工業",
    "11": "橡膠工業",
    "12": "汽車工業",
    "14": "建材營造",
    "15": "航運業",
    "16": "觀光餐旅",
    "17": "金融保險",
    "18": "貿易百貨",
    "19": "綜合",
    "20": "其他",
    "21": "化學工業",
    "22": "生技醫療",
    "23": "油電燃氣",
    "24": "半導體業",
    "25": "電腦及週邊設備業",
    "26": "光電業",
    "27": "通信網路業",
    "28": "電子零組件業",
    "29": "電子通路業",
    "30": "資訊服務業",
    "31": "其他電子業",
    "32": "文化創意業",
    "33": "農業科技業",
    "34": "綠能環保",
    "35": "數位雲端",
    "36": "運動休閒",
    "37": "居家生活",
}

YAHOO_CATEGORY_ALIAS = {
    "半導體": "半導體業",
    "電腦週邊": "電腦及週邊設備業",
    "電腦周邊": "電腦及週邊設備業",
    "光電": "光電業",
    "通訊網路": "通信網路業",
    "電子零組件": "電子零組件業",
    "電子通路": "電子通路業",
    "資訊服務": "資訊服務業",
    "其他電子": "其他電子業",
    "生技醫療": "生技醫療",
    "油電燃氣": "油電燃氣",
    "建材營造": "建材營造",
    "航運": "航運業",
    "觀光餐旅": "觀光餐旅",
    "綠能環保": "綠能環保",
    "數位雲端": "數位雲端",
    "運動休閒": "運動休閒",
    "居家生活": "居家生活",
    "水泥": "水泥工業",
    "食品": "食品工業",
    "塑膠": "塑膠工業",
    "紡織纖維": "紡織纖維",
    "電機機械": "電機機械",
    "電器電纜": "電器電纜",
    "玻璃陶瓷": "玻璃陶瓷",
    "造紙": "造紙工業",
    "鋼鐵": "鋼鐵工業",
    "橡膠": "橡膠工業",
    "汽車": "汽車工業",
    "貿易百貨": "貿易百貨",
    "化學": "化學工業",
}

STOCK_CATEGORY_NAME_WHITELIST = {
    "1101": {"official": "水泥工業", "theme": "水泥工業"},
    "1102": {"official": "水泥工業", "theme": "水泥工業"},
    "1103": {"official": "水泥工業", "theme": "水泥工業"},
    "1104": {"official": "水泥工業", "theme": "水泥工業"},
    "1108": {"official": "水泥工業", "theme": "水泥工業"},
    "1109": {"official": "水泥工業", "theme": "水泥工業"},
    "1216": {"official": "食品工業", "theme": "食品民生"},
    "1301": {"official": "塑膠工業", "theme": "塑化"},
    "1303": {"official": "塑膠工業", "theme": "塑化"},
    "1326": {"official": "塑膠工業", "theme": "塑化"},
    "1402": {"official": "紡織纖維", "theme": "紡織製鞋"},
    "1409": {"official": "紡織纖維", "theme": "紡織製鞋"},
    "1410": {"official": "紡織纖維", "theme": "紡織製鞋"},
    "1414": {"official": "紡織纖維", "theme": "紡織製鞋"},
    "1434": {"official": "紡織纖維", "theme": "紡織製鞋"},
    "1605": {"official": "電器電纜", "theme": "電器電纜"},
    "1707": {"official": "化學工業", "theme": "化學工業"},
    "1710": {"official": "化學工業", "theme": "化學工業"},
    "1711": {"official": "化學工業", "theme": "化學工業"},
    "1712": {"official": "化學工業", "theme": "化學工業"},
    "1722": {"official": "生技醫療", "theme": "生技醫療"},
    "1802": {"official": "電機機械", "theme": "電機機械"},
    "1907": {"official": "造紙工業", "theme": "造紙工業"},
    "2002": {"official": "鋼鐵工業", "theme": "鋼鐵"},
    "2101": {"official": "橡膠工業", "theme": "橡膠工業"},
    "2201": {"official": "汽車工業", "theme": "汽車"},
    "2204": {"official": "汽車工業", "theme": "汽車"},
    "2603": {"official": "航運業", "theme": "航運"},
    "2609": {"official": "航運業", "theme": "航運"},
    "2615": {"official": "航運業", "theme": "航運"},
    "2801": {"official": "金融保險", "theme": "金融保險"},
    "2809": {"official": "金融保險", "theme": "金融保險"},
    "2812": {"official": "金融保險", "theme": "金融保險"},
    "2834": {"official": "金融保險", "theme": "金融保險"},
    "2880": {"official": "金融保險", "theme": "金控"},
    "2881": {"official": "金融保險", "theme": "金控"},
    "2882": {"official": "金融保險", "theme": "金控"},
    "2883": {"official": "金融保險", "theme": "金控"},
    "2884": {"official": "金融保險", "theme": "金控"},
    "2885": {"official": "金融保險", "theme": "金控"},
    "2886": {"official": "金融保險", "theme": "金控"},
    "2887": {"official": "金融保險", "theme": "金控"},
    "2888": {"official": "金融保險", "theme": "金控"},
    "2889": {"official": "金融保險", "theme": "金控"},
    "2890": {"official": "金融保險", "theme": "金控"},
    "2891": {"official": "金融保險", "theme": "金控"},
    "2892": {"official": "金融保險", "theme": "第一金控"},
    "5871": {"official": "金融保險", "theme": "金控"},
    "6005": {"official": "貿易百貨", "theme": "貿易百貨"},
}

NAME_THEME_HINTS = [
    ("水泥", "水泥工業"),
    ("紡織", "紡織製鞋"),
    ("成衣", "紡織製鞋"),
    ("製鞋", "紡織製鞋"),
    ("百貨", "貿易百貨"),
    ("航運", "航運"),
    ("海運", "航運"),
    ("金控", "金控"),
    ("銀行", "銀行"),
    ("保險", "保險"),
    ("證券", "證券"),
]


def _canonical_category(v: Any) -> str:
    s = _safe_str(v)
    return YAHOO_CATEGORY_ALIAS.get(s, s)


def _official_industry_name(raw_value: Any) -> str:
    raw = _safe_str(raw_value)
    if not raw:
        return ""
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) == 1:
        digits = digits.zfill(2)
    if digits and digits in OFFICIAL_INDUSTRY_CODE_MAP:
        return OFFICIAL_INDUSTRY_CODE_MAP[digits]
    raw = raw.replace("業別", "").strip()
    return YAHOO_CATEGORY_ALIAS.get(raw, raw)


def _infer_category_from_name(name: Any) -> str:
    name = _safe_str(name)
    if not name:
        return "其他"
    rules = [
        (["金控"], "金控"),
        (["銀行", "商銀", "票券"], "銀行"),
        (["產險", "人壽", "保險"], "保險"),
        (["證券", "投顧", "期貨"], "證券"),
        (["水泥"], "水泥工業"),
        (["塑膠"], "塑化"),
        (["紡織", "成衣", "製鞋"], "紡織製鞋"),
        (["汽車", "車電"], "汽車"),
        (["航運", "海運", "航空"], "航運"),
        (["百貨", "零售", "超商"], "貿易百貨"),
        (["半導體", "晶圓", "IC"], "半導體"),
        (["面板", "光電", "鏡頭"], "光電"),
        (["網通", "通訊"], "通信網路"),
        (["主機板", "筆電", "電腦"], "電腦及週邊設備"),
        (["PCB", "連接器", "被動元件"], "電子零組件"),
        (["資訊", "軟體", "雲端"], "資訊服務"),
    ]
    for kws, cat in rules:
        if any(k in name for k in kws):
            return cat
    return "其他"


def _infer_category_from_record(name: Any, raw_cat: Any) -> str:
    raw = _safe_str(raw_cat)
    if raw:
        raw2 = _canonical_category(raw)
        if raw2 and "其他" not in raw2:
            return raw2
    return _infer_category_from_name(name)


def _yahoo_industry_to_theme(industry: Any, name: Any) -> str:
    raw = _safe_str(industry)
    name = _safe_str(name)
    if not raw:
        return _infer_category_from_name(name)
    if "金融" in raw:
        if "金控" in name:
            return "金控"
        if "保險" in name or ("產險" in name) or ("人壽" in name):
            return "保險"
        if ("銀行" in name) or name.endswith("銀") or ("商銀" in name):
            return "銀行"
        if ("證" in name) or ("期" in name):
            return "證券"
        return "金融保險"
    return _infer_category_from_record(name, raw) or raw or "其他"


def _secondary_refine_theme(code: Any, name: Any, official: Any, current: Any) -> tuple[str, str, str]:
    code = _normalize_code(code)
    name = _safe_str(name)
    official = _official_industry_name(official)
    current = _safe_str(current)

    white = STOCK_CATEGORY_NAME_WHITELIST.get(code, {})
    if white:
        final_official = _safe_str(white.get("official")) or official
        final_theme = _safe_str(white.get("theme")) or current or _infer_category_from_name(name)
        if final_theme:
            return final_official or official, final_theme, "whitelist"

    if current and "其他" not in current:
        return official, current, ""

    if official:
        theme = _yahoo_industry_to_theme(official, name)
        if theme and "其他" not in theme:
            return official, theme, "official_refine"

    by_name = _infer_category_from_name(name)
    if by_name and "其他" not in by_name:
        return official, by_name, "name_rule"

    for kw, theme in NAME_THEME_HINTS:
        if kw in name:
            return official, theme, "name_hint"

    return official, current or "其他", ""


def _normalize_master_df(df: pd.DataFrame) -> pd.DataFrame:
    cols = _master_cols()
    if df is None or df.empty:
        return empty_master_df()
    x = df.copy()
    for c in cols:
        if c not in x.columns:
            x[c] = ""
    x["code"] = x["code"].map(_normalize_code)
    x["name"] = x["name"].map(_safe_str)
    x["market"] = x["market"].map(_safe_str)
    x["official_industry_raw"] = x["official_industry_raw"].map(_safe_str)
    x["official_industry_raw_col"] = x["official_industry_raw_col"].map(_safe_str)
    x["official_industry"] = x["official_industry"].map(_official_industry_name)
    x["theme_category"] = x.apply(lambda r: _yahoo_industry_to_theme(r.get("official_industry"), r.get("name")), axis=1)
    x["category"] = x["theme_category"]
    x["source"] = x["source"].map(_safe_str)
    x["source_api"] = x["source_api"].map(_safe_str)
    x["source_rank"] = pd.to_numeric(x["source_rank"], errors="coerce").fillna(999).astype(int)
    x["待修原因"] = x["待修原因"].map(_safe_str)
    x = x[x["code"].astype(str).str.fullmatch(r"\d{4}")].copy()
    x = x.drop_duplicates(subset=["code"], keep="first").reset_index(drop=True)

    for idx in x.index:
        final_official, final_theme, _ = _secondary_refine_theme(
            x.at[idx, "code"], x.at[idx, "name"], x.at[idx, "official_industry"], x.at[idx, "theme_category"]
        )
        if final_official and not _safe_str(x.at[idx, "official_industry"]):
            x.at[idx, "official_industry"] = final_official
        if final_theme:
            x.at[idx, "theme_category"] = final_theme
            x.at[idx, "category"] = final_theme
        if _safe_str(x.at[idx, "official_industry"]) or (_safe_str(x.at[idx, "category"]) and "其他" not in _safe_str(x.at[idx, "category"])):
            x.at[idx, "待修原因"] = ""
        else:
            x.at[idx, "待修原因"] = _safe_str(x.at[idx, "待修原因"]) or "Yahoo / 官方產業待補"
    return x[cols].copy()


# =========================================================
# HTTP / GitHub
# =========================================================

def _http_get_text(url: str, timeout: int = 30, verify: bool | None = None, headers: dict[str, str] | None = None) -> str:
    headers = headers or {"User-Agent": "Mozilla/5.0"}
    kwargs = {"timeout": timeout, "headers": headers}
    if verify is not None:
        kwargs["verify"] = verify
    try:
        resp = requests.get(url, **kwargs)
        resp.raise_for_status()
        resp.encoding = resp.encoding or "utf-8"
        return resp.text
    except Exception:
        if verify is not False:
            try:
                requests.packages.urllib3.disable_warnings()  # type: ignore[attr-defined]
            except Exception:
                pass
            resp = requests.get(url, timeout=timeout, headers=headers, verify=False)
            resp.raise_for_status()
            resp.encoding = resp.encoding or "utf-8"
            return resp.text
        raise


def _stock_master_config() -> dict[str, str]:
    return {
        "token": _safe_str(st.secrets.get("GITHUB_TOKEN", "")),
        "owner": _safe_str(st.secrets.get("GITHUB_REPO_OWNER", "cheng07021028")),
        "repo": _safe_str(st.secrets.get("GITHUB_REPO_NAME", "stock-app")),
        "branch": _safe_str(st.secrets.get("GITHUB_REPO_BRANCH", "main")) or "main",
        "master_path": _safe_str(st.secrets.get("STOCK_MASTER_GITHUB_PATH", "stock_master_cache.json")) or "stock_master_cache.json",
        "override_path": _safe_str(st.secrets.get("STOCK_CATEGORY_OVERRIDE_GITHUB_PATH", "stock_category_overrides.json")) or "stock_category_overrides.json",
    }


def _github_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }


def _github_contents_url(owner: str, repo: str, path: str) -> str:
    return f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"


def _read_json_from_github(path: str) -> tuple[Any, str]:
    cfg = _stock_master_config()
    token = cfg["token"]
    if not token:
        return None, "未設定 GITHUB_TOKEN"
    try:
        resp = requests.get(
            _github_contents_url(cfg["owner"], cfg["repo"], path),
            headers=_github_headers(token),
            params={"ref": cfg["branch"]},
            timeout=20,
        )
        if resp.status_code == 404:
            return None, ""
        if resp.status_code != 200:
            return None, f"讀取 GitHub JSON 失敗：{resp.status_code} / {resp.text[:300]}"
        data = resp.json()
        content = data.get("content", "")
        if not content:
            return None, ""
        decoded = base64.b64decode(content).decode("utf-8")
        return json.loads(decoded), ""
    except Exception as e:
        return None, f"讀取 GitHub JSON 例外：{e}"


def _write_json_to_github(path: str, payload: Any, message: str) -> tuple[bool, str]:
    cfg = _stock_master_config()
    token = cfg["token"]
    if not token:
        return False, "未設定 GITHUB_TOKEN"

    url = _github_contents_url(cfg["owner"], cfg["repo"], path)
    headers = _github_headers(token)
    sha = ""

    try:
        cur = requests.get(url, headers=headers, params={"ref": cfg["branch"]}, timeout=20)
        if cur.status_code == 200:
            sha = _safe_str(cur.json().get("sha"))
        elif cur.status_code not in (200, 404):
            return False, f"讀取 GitHub 檔案失敗：{cur.status_code}"
        body = {
            "message": message,
            "content": base64.b64encode(json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")).decode("utf-8"),
            "branch": cfg["branch"],
        }
        if sha:
            body["sha"] = sha
        resp = requests.put(url, headers=headers, json=body, timeout=30)
        if resp.status_code not in (200, 201):
            return False, f"寫入 GitHub 失敗：{resp.status_code} / {resp.text[:300]}"
        return True, f"已回寫 GitHub：{path}"
    except Exception as e:
        return False, f"寫入 GitHub 例外：{e}"


# =========================================================
# Base master sources
# =========================================================

def _split_code_name(text: str) -> tuple[str, str]:
    text = _safe_str(text)
    m = re.match(r"^(\d{4})\s+(.+)$", text)
    if m:
        return m.group(1), _safe_str(m.group(2))
    code = _normalize_code(text)
    return code, _safe_str(text.replace(code, "", 1))


def _extract_text_lines_from_html(html_text: str) -> list[str]:
    text = re.sub(r"(?is)<script.*?>.*?</script>", " ", html_text)
    text = re.sub(r"(?is)<style.*?>.*?</style>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", "\n", text)
    text = html.unescape(text)
    lines = []
    for line in text.splitlines():
        line = re.sub(r"\s+", " ", line).strip()
        if line:
            lines.append(line)
    return lines


def _pick_after(lines: list[str], labels: list[str]) -> str:
    for i, line in enumerate(lines[:-1]):
        if line in labels:
            return _safe_str(lines[i + 1])
    return ""


@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_twse_isin_base() -> tuple[pd.DataFrame, dict[str, Any]]:
    url = "https://isin.twse.com.tw/isin/C_public.jsp?strMode=2"
    diag = {"rows": 0, "official_hit": 0, "raw_cols": [], "source_api": "twse_isin_html", "error": ""}

    def _clean_cell(cell_html: str) -> str:
        txt = re.sub(r"<br\s*/?>", " ", cell_html, flags=re.I)
        txt = re.sub(r"<[^>]+>", "", txt)
        txt = html.unescape(txt)
        txt = txt.replace("\u3000", " ").replace("&nbsp;", " ")
        txt = re.sub(r"\s+", " ", txt).strip()
        return txt

    try:
        html_text = _http_get_text(url, timeout=40)
    except Exception as e:
        diag["error"] = f"{type(e).__name__}: {e}"
        return empty_master_df(), diag

    rows = []
    tr_blocks = re.findall(r"<tr[^>]*>(.*?)</tr>", html_text, flags=re.I | re.S)
    diag["raw_cols"] = ["有價證券代號及名稱", "ISIN", "上市日", "市場別", "產業別", "CFI", "備註"]

    for tr in tr_blocks:
        cells_html = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, flags=re.I | re.S)
        cells = [_clean_cell(x) for x in cells_html]
        if len(cells) < 5:
            continue

        code, name = _split_code_name(cells[0])
        code = _normalize_code(code)
        if not re.fullmatch(r"\d{4}", code or ""):
            continue

        market = ""
        market_idx = -1
        for i, c in enumerate(cells):
            if "上市" in c or "上櫃" in c or "興櫃" in c:
                market = c
                market_idx = i
                break
        if market and "上市" not in market:
            continue
        if not market:
            market = "上市"

        official_raw = ""
        if market_idx >= 0 and market_idx + 1 < len(cells):
            official_raw = _safe_str(cells[market_idx + 1])
        if not official_raw:
            for c in cells[1:]:
                c2 = _safe_str(c)
                if any(k in c2 for k in ["工業", "業", "保險", "銀行", "證券", "其他電子", "半導體", "光電", "通信網路", "電子零組件", "電腦及週邊設備", "資訊服務", "貿易百貨", "生技醫療", "油電燃氣", "建材營造", "觀光餐旅", "航運"]):
                    official_raw = c2
                    break

        official = _official_industry_name(official_raw)
        theme = _yahoo_industry_to_theme(official, name)

        rows.append({
            "code": code,
            "name": name or code,
            "market": "上市",
            "official_industry_raw": official_raw,
            "official_industry_raw_col": "產業別",
            "official_industry": official,
            "theme_category": theme,
            "category": theme,
            "source": "twse_isin_base",
            "source_api": "twse_isin_html",
            "source_rank": 3,
            "待修原因": "" if official else "Yahoo / 官方產業待補",
        })

    out = _normalize_master_df(pd.DataFrame(rows))
    diag["rows"] = len(out)
    diag["official_hit"] = int(out["official_industry"].fillna("").astype(str).str.strip().ne("").sum()) if not out.empty else 0
    return out, diag


@st.cache_data(ttl=3600, show_spinner=False)
def _fetch_tpex_base(mode: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    url = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O" if mode == "上櫃" else "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_R"
    diag = {"rows": 0, "official_hit": 0, "raw_cols": [], "source_api": f"tpex_{mode}", "error": ""}
    try:
        raw = json.loads(_http_get_text(url, timeout=30))
        df = pd.DataFrame(raw)
    except Exception as e:
        diag["error"] = f"{type(e).__name__}: {e}"
        return empty_master_df(), diag

    rows = []
    diag["raw_cols"] = list(df.columns)
    for _, r in df.iterrows():
        code = _normalize_code(r.get("SecuritiesCompanyCode"))
        if not code:
            continue
        name = _safe_str(r.get("CompanyAbbreviation"))
        official_raw = _safe_str(r.get("SecuritiesIndustryCode"))
        official = _official_industry_name(official_raw)
        theme = _yahoo_industry_to_theme(official, name)
        rows.append({
            "code": code,
            "name": name or code,
            "market": mode,
            "official_industry_raw": official_raw,
            "official_industry_raw_col": "SecuritiesIndustryCode",
            "official_industry": official,
            "theme_category": theme,
            "category": theme,
            "source": f"tpex_{mode}_base",
            "source_api": f"tpex_{mode}",
            "source_rank": 3,
            "待修原因": "" if official else "Yahoo / 官方產業待補",
        })
    out = _normalize_master_df(pd.DataFrame(rows))
    diag["rows"] = len(out)
    diag["official_hit"] = int(out["official_industry"].fillna("").astype(str).str.strip().ne("").sum()) if not out.empty else 0
    return out, diag


def _build_utils_name_aux() -> pd.DataFrame:
    dfs = []
    for market_arg in ["上市", "上櫃", "興櫃"]:
        try:
            df = get_all_code_name_map(market_arg)
        except Exception:
            df = pd.DataFrame()
        if df is None or df.empty:
            continue
        temp = df.copy().rename(columns={"證券代號": "code", "證券名稱": "name", "市場別": "market"})
        for c in ["code", "name", "market"]:
            if c not in temp.columns:
                temp[c] = ""
        temp["code"] = temp["code"].map(_normalize_code)
        temp["name"] = temp["name"].map(_safe_str)
        temp["market"] = temp["market"].map(_safe_str).replace("", market_arg)
        temp = temp[temp["code"].astype(str).str.fullmatch(r"\d{4}")].copy()
        dfs.append(temp[["code", "name", "market"]])
    if not dfs:
        return pd.DataFrame(columns=["code", "name_aux", "market_aux"])
    out = pd.concat(dfs, ignore_index=True).drop_duplicates("code", keep="first").reset_index(drop=True)
    out = out.rename(columns={"name": "name_aux", "market": "market_aux"})
    return out


def _apply_aux_name_market(master_df: pd.DataFrame) -> pd.DataFrame:
    if master_df is None or master_df.empty:
        return empty_master_df()
    aux = _build_utils_name_aux()
    if aux.empty:
        return master_df
    work = master_df.merge(aux, on="code", how="left")
    for idx in work.index:
        if not _safe_str(work.at[idx, "name"]) and _safe_str(work.at[idx, "name_aux"]):
            work.at[idx, "name"] = _safe_str(work.at[idx, "name_aux"])
        if _safe_str(work.at[idx, "market"]) not in {"上市", "上櫃", "興櫃"} and _safe_str(work.at[idx, "market_aux"]) in {"上市", "上櫃", "興櫃"}:
            work.at[idx, "market"] = _safe_str(work.at[idx, "market_aux"])
    return _normalize_master_df(work.drop(columns=["name_aux", "market_aux"], errors="ignore"))


def _build_formal_base_master() -> tuple[pd.DataFrame, dict[str, Any]]:
    twse_df, twse_info = _fetch_twse_isin_base()
    tpex_o_df, tpex_o_info = _fetch_tpex_base("上櫃")
    tpex_r_df, tpex_r_info = _fetch_tpex_base("興櫃")
    base = pd.concat([twse_df, tpex_o_df, tpex_r_df], ignore_index=True) if any(not x.empty for x in [twse_df, tpex_o_df, tpex_r_df]) else empty_master_df()
    base = _normalize_master_df(base).sort_values(["code"]).drop_duplicates("code", keep="first").reset_index(drop=True)
    info = {"rows": len(base), "twse_info": twse_info, "tpex_o_info": tpex_o_info, "tpex_r_info": tpex_r_info}
    return base, info


# =========================================================
# Yahoo fill / cache
# =========================================================

@st.cache_data(ttl=86400, show_spinner=False)
def _fetch_yahoo_profile_fill(code: str, market: str) -> dict[str, str]:
    code = _normalize_code(code)
    market = _safe_str(market)
    if not re.fullmatch(r"\d{4}", code or ""):
        return {}

    if market == "上市":
        suffixes = ["TW", "TWO"]
    elif market in {"上櫃", "興櫃"}:
        suffixes = ["TWO", "TW"]
    else:
        suffixes = ["TW", "TWO"]

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
        "Referer": "https://tw.stock.yahoo.com/",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

    def _regex_pick(text: str, label: str) -> str:
        patterns = [
            rf"{label}\s*[：:]?\s*([\u4e00-\u9fffA-Za-z0-9\-（）()、／/．\. ]{{1,80}})",
            rf'\"{label}\"\s*[:：]\s*\"([^\"]{{1,80}})\"',
        ]
        for pat in patterns:
            m = re.search(pat, text, flags=re.S)
            if m:
                val = _safe_str(m.group(1))
                if val and val != label:
                    return val
        return ""

    for sfx in suffixes:
        url = f"https://tw.stock.yahoo.com/quote/{code}.{sfx}/profile"
        try:
            html_text = _http_get_text(url, timeout=20, headers=headers)
            lines = _extract_text_lines_from_html(html_text)
            industry = _pick_after(lines, ["產業類別"]) or _regex_pick(html_text, "產業類別")
            market_found = _pick_after(lines, ["市場別"]) or _regex_pick(html_text, "市場別")
            name = _pick_after(lines, ["公司名稱"]) or _regex_pick(html_text, "公司名稱")
            if industry or market_found or name:
                return {"code": code, "name": name, "market": market_found, "industry": industry, "source_api": f"yahoo_profile_{sfx}"}
        except Exception:
            continue
    return {}


def _overlay_repo_seed(base_df: pd.DataFrame, repo_df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    if base_df is None or base_df.empty:
        return empty_master_df(), 0
    if repo_df is None or repo_df.empty:
        return _normalize_master_df(base_df), 0
    base = _normalize_master_df(base_df)
    repo = _normalize_master_df(repo_df)
    repo_map = repo.set_index("code").to_dict(orient="index")
    reused = 0
    for idx in base.index:
        code = _normalize_code(base.at[idx, "code"])
        old = repo_map.get(code)
        if not old:
            continue
        old_official = _safe_str(old.get("official_industry"))
        old_category = _safe_str(old.get("category"))
        old_source = _safe_str(old.get("source"))
        old_pending = _safe_str(old.get("待修原因"))
        if old_official and old_category and not old_pending and old_source in {"yahoo_profile_primary", "secondary_refine", "override", "twse_isin_base", "tpex_上櫃_base", "tpex_興櫃_base"}:
            for col in ["name", "market", "official_industry_raw", "official_industry_raw_col", "official_industry", "theme_category", "category", "source", "source_api", "source_rank", "待修原因"]:
                if col in base.columns and col in old:
                    base.at[idx, col] = old.get(col, base.at[idx, col])
            reused += 1
    return _normalize_master_df(base), reused


def _apply_yahoo_primary_categories(base_df: pd.DataFrame, workers: int = 8) -> tuple[pd.DataFrame, dict[str, Any]]:
    if base_df is None or base_df.empty:
        return empty_master_df(), {"rows": 0, "hit": 0, "error": "", "processed": 0, "secondary_refine": 0}

    work = _normalize_master_df(base_df)
    target_df = work[
        work["official_industry"].fillna("").astype(str).str.strip().eq("")
        | work["category"].fillna("").astype(str).str.contains("其他", na=False)
        | work["待修原因"].fillna("").astype(str).str.strip().ne("")
    ].copy()
    rows = target_df.to_dict(orient="records")
    results: dict[str, dict[str, str]] = {}
    errors = []

    if rows:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(4, min(workers, 8))) as ex:
            fut_map = {ex.submit(_fetch_yahoo_profile_fill, r["code"], r["market"]): r["code"] for r in rows}
            for fut in concurrent.futures.as_completed(fut_map):
                code = fut_map[fut]
                try:
                    info = fut.result()
                    if info:
                        results[code] = info
                except Exception as e:
                    errors.append(f"{code}:{type(e).__name__}")

    hit = 0
    secondary_refine = 0
    for idx in work.index:
        code = _normalize_code(work.at[idx, "code"])
        info = results.get(code, {})
        yahoo_industry = _safe_str(info.get("industry"))
        yahoo_market = _safe_str(info.get("market"))
        yahoo_name = _safe_str(info.get("name"))

        if yahoo_name:
            work.at[idx, "name"] = yahoo_name
        if yahoo_market in {"上市", "上櫃", "興櫃"}:
            work.at[idx, "market"] = yahoo_market

        if yahoo_industry:
            official = _official_industry_name(yahoo_industry)
            theme = _yahoo_industry_to_theme(official or yahoo_industry, work.at[idx, "name"])
            final_official, final_theme, refine_src = _secondary_refine_theme(code, work.at[idx, "name"], official or yahoo_industry, theme)
            work.at[idx, "official_industry_raw"] = yahoo_industry
            work.at[idx, "official_industry_raw_col"] = "Yahoo_產業類別"
            work.at[idx, "official_industry"] = final_official or official or yahoo_industry
            work.at[idx, "theme_category"] = final_theme or theme
            work.at[idx, "category"] = final_theme or theme
            work.at[idx, "source"] = "yahoo_profile_primary"
            work.at[idx, "source_api"] = _safe_str(info.get("source_api")) or "yahoo_profile"
            work.at[idx, "source_rank"] = 1
            work.at[idx, "待修原因"] = ""
            hit += 1
            if refine_src:
                secondary_refine += 1

    for idx in work.index:
        if _safe_str(work.at[idx, "待修原因"]) or "其他" in _safe_str(work.at[idx, "category"]):
            final_official, final_theme, refine_src = _secondary_refine_theme(
                work.at[idx, "code"], work.at[idx, "name"], work.at[idx, "official_industry"], work.at[idx, "category"]
            )
            if final_official and not _safe_str(work.at[idx, "official_industry"]):
                work.at[idx, "official_industry"] = final_official
            if final_theme and "其他" not in final_theme:
                work.at[idx, "theme_category"] = final_theme
                work.at[idx, "category"] = final_theme
                if _safe_str(work.at[idx, "source"]) in {"twse_isin_base", "tpex_上櫃_base", "tpex_興櫃_base", ""}:
                    work.at[idx, "source"] = "secondary_refine"
                    work.at[idx, "source_api"] = refine_src or "secondary_refine"
                    work.at[idx, "source_rank"] = 2
                work.at[idx, "待修原因"] = ""
                secondary_refine += 1

    work = _normalize_master_df(work)
    return work, {
        "rows": hit,
        "hit": hit,
        "processed": len(rows),
        "secondary_refine": secondary_refine,
        "error": "；".join(errors[:20]),
    }


@st.cache_data(ttl=900, show_spinner=False)
def _load_stock_master_cache_from_repo() -> pd.DataFrame:
    cfg = _stock_master_config()
    payload, _ = _read_json_from_github(cfg["master_path"])
    if not isinstance(payload, list):
        return empty_master_df()
    return _normalize_master_df(pd.DataFrame(payload))


@st.cache_data(ttl=300, show_spinner=False)
def _load_stock_category_override_map() -> dict[str, dict[str, str]]:
    cfg = _stock_master_config()
    payload, _ = _read_json_from_github(cfg["override_path"])
    if not isinstance(payload, dict):
        return {}
    out: dict[str, dict[str, str]] = {}
    for code, item in payload.items():
        norm_code = _normalize_code(code)
        if not norm_code:
            continue
        if not isinstance(item, dict):
            item = {"category": item}
        out[norm_code] = {
            "code": norm_code,
            "name": _safe_str(item.get("name")),
            "market": _safe_str(item.get("market")),
            "category": _canonical_category(item.get("category")),
            "updated_at": _safe_str(item.get("updated_at")),
        }
    return out


def _apply_master_overrides(master_df: pd.DataFrame) -> pd.DataFrame:
    work = _normalize_master_df(master_df)
    override_map = _load_stock_category_override_map()
    if not override_map:
        return work
    for code, item in override_map.items():
        matched = work["code"].astype(str) == str(code)
        if matched.any():
            idx = work[matched].index[0]
            if _safe_str(item.get("name")):
                work.at[idx, "name"] = _safe_str(item.get("name"))
            if _safe_str(item.get("market")) in {"上市", "上櫃", "興櫃"}:
                work.at[idx, "market"] = _safe_str(item.get("market"))
            if _safe_str(item.get("category")):
                cat = _canonical_category(item.get("category"))
                work.at[idx, "theme_category"] = cat
                work.at[idx, "category"] = cat
                work.at[idx, "source"] = "override"
                work.at[idx, "source_api"] = "github_override"
                work.at[idx, "source_rank"] = 0
                work.at[idx, "待修原因"] = ""
    return _normalize_master_df(work)


def _save_master_cache_to_repo(master_df: pd.DataFrame) -> tuple[bool, str]:
    cfg = _stock_master_config()
    work = _normalize_master_df(master_df)
    payload = work.sort_values(["market", "code"]).to_dict(orient="records")
    return _write_json_to_github(cfg["master_path"], payload, f"refresh stock master cache at {_now_text()}")


def _save_category_override(code: str, name: str, market: str, category: str) -> tuple[bool, str]:
    cfg = _stock_master_config()
    code = _normalize_code(code)
    if not code:
        return False, "股票代號不可空白"
    payload, _ = _read_json_from_github(cfg["override_path"])
    if not isinstance(payload, dict):
        payload = {}
    payload[code] = {
        "code": code,
        "name": _safe_str(name),
        "market": _safe_str(market) or "上市",
        "category": _canonical_category(category) or _infer_category_from_record(name, category),
        "updated_at": _now_text(),
    }
    ok, msg = _write_json_to_github(cfg["override_path"], payload, f"update stock category override {code} at {_now_text()}")
    if ok:
        try:
            _load_stock_category_override_map.clear()
        except Exception:
            pass
    return ok, msg


# =========================================================
# Public service API
# =========================================================

def _build_master_diagnostics(base_info=None, yahoo_info=None, merged=None) -> list[str]:
    base_info = base_info if isinstance(base_info, dict) else {}
    twse_info = base_info.get("twse_info", {}) if isinstance(base_info.get("twse_info", {}), dict) else {}
    tpex_o_info = base_info.get("tpex_o_info", {}) if isinstance(base_info.get("tpex_o_info", {}), dict) else {}
    tpex_r_info = base_info.get("tpex_r_info", {}) if isinstance(base_info.get("tpex_r_info", {}), dict) else {}
    yahoo_info = yahoo_info if isinstance(yahoo_info, dict) else {}
    merged_df = merged if isinstance(merged, pd.DataFrame) else empty_master_df()

    def _n(v, default=0):
        try:
            return int(v)
        except Exception:
            return default

    logs = []
    logs.append(f"正式底座(TWSE ISIN + TPEX)：{_n(base_info.get('rows'))} 筆")
    logs.append(f"TWSE ISIN：{_n(twse_info.get('rows'))} 筆 / 正式產業有值 {_n(twse_info.get('official_hit'))} 筆 / API: {_safe_str(twse_info.get('source_api')) or '-'}")
    logs.append(f"TPEX-上櫃：{_n(tpex_o_info.get('rows'))} 筆 / 正式產業有值 {_n(tpex_o_info.get('official_hit'))} 筆 / API: {_safe_str(tpex_o_info.get('source_api')) or '-'}")
    logs.append(f"TPEX-興櫃：{_n(tpex_r_info.get('rows'))} 筆 / 正式產業有值 {_n(tpex_r_info.get('official_hit'))} 筆 / API: {_safe_str(tpex_r_info.get('source_api')) or '-'}")
    logs.append(f"Yahoo 主來源補值：{_n(yahoo_info.get('rows'))} 筆 / 處理 {_n(yahoo_info.get('processed'))} 筆 / 二次細分 {_n(yahoo_info.get('secondary_refine'))} 筆")
    if not merged_df.empty:
        hit = int(merged_df["official_industry"].fillna("").astype(str).str.strip().ne("").sum())
        pending = int(merged_df["待修原因"].fillna("").astype(str).str.strip().ne("").sum())
        logs.append(f"合併後：{len(merged_df)} 筆 / 正式產業有值 {hit} 筆 / 待修 {pending} 筆")
        vc = merged_df["source"].fillna("").astype(str).value_counts()
        if not vc.empty:
            logs.append("來源統計：" + " / ".join([f"{k}:{int(v)}" for k, v in vc.items()]))
    return logs


def _build_live_master_df() -> tuple[pd.DataFrame, list[str], dict[str, Any], dict[str, Any]]:
    base_df, base_info = _build_formal_base_master()
    base_df = _apply_aux_name_market(base_df)
    try:
        repo_seed_df = _load_stock_master_cache_from_repo()
    except Exception:
        repo_seed_df = empty_master_df()

    base_df, reused_count = _overlay_repo_seed(base_df, repo_seed_df)
    yahoo_df, yahoo_info = _apply_yahoo_primary_categories(base_df, workers=8)
    merged = _apply_master_overrides(yahoo_df)
    logs = _build_master_diagnostics(base_info, yahoo_info, merged)
    if reused_count > 0:
        logs.append(f"增量沿用 cache 已完成資料：{reused_count} 筆")
    return merged, logs, base_info, yahoo_info


def refresh_stock_master() -> tuple[pd.DataFrame, list[str]]:
    try:
        _fetch_yahoo_profile_fill.clear()
    except Exception:
        pass
    try:
        _build_live_master_df.clear()
    except Exception:
        pass
    try:
        load_stock_master.clear()
    except Exception:
        pass
    fresh_df, logs, _, yahoo_info = _build_live_master_df()
    if fresh_df.empty:
        return fresh_df, logs + ["主檔更新失敗：正式股票清單為空。"]
    ok, msg = _save_master_cache_to_repo(fresh_df)
    logs.append(msg)
    if ok:
        try:
            _load_stock_master_cache_from_repo.clear()
        except Exception:
            pass
    logs.append(f"本次實際待補 Yahoo 範圍：{int(yahoo_info.get('processed', 0))} 筆")
    return fresh_df, logs


@st.cache_data(ttl=300, show_spinner=False)
def load_stock_master() -> pd.DataFrame:
    repo_df = _load_stock_master_cache_from_repo()
    repo_df = _normalize_master_df(repo_df)

    need_live = repo_df.empty
    if not need_live:
        official_hit = int(repo_df["official_industry"].fillna("").astype(str).str.strip().ne("").sum())
        yahoo_hit = int((repo_df["source"].fillna("").astype(str) == "yahoo_profile_primary").sum())
        pending = int(repo_df["待修原因"].fillna("").astype(str).str.strip().ne("").sum())
        need_live = (official_hit < 1500) or (yahoo_hit < 200) or (pending > 20)

    if not need_live:
        return _apply_master_overrides(repo_df)

    live_df, _, _, _ = _build_live_master_df()
    return _apply_master_overrides(live_df)


def search_stock_master(master_df: pd.DataFrame, keyword: str = "", market_filter: str = "全部", category_filter: str = "全部") -> pd.DataFrame:
    cols = _master_cols()
    if master_df is None or master_df.empty:
        return pd.DataFrame(columns=cols)
    work = master_df.copy()
    kw = _safe_str(keyword)
    market_filter = _safe_str(market_filter)
    category_filter = _safe_str(category_filter)

    if market_filter and market_filter != "全部":
        work = work[work["market"].astype(str) == market_filter].copy()
    if category_filter and category_filter != "全部":
        work = work[(work["category"].astype(str) == category_filter) | (work["official_industry"].astype(str) == category_filter)].copy()
    if kw:
        work = work[
            work["code"].astype(str).str.contains(kw, case=False, na=False)
            | work["name"].astype(str).str.contains(kw, case=False, na=False)
            | work["official_industry"].astype(str).str.contains(kw, case=False, na=False)
            | work["theme_category"].astype(str).str.contains(kw, case=False, na=False)
            | work["category"].astype(str).str.contains(kw, case=False, na=False)
        ].copy()
    return work.sort_values(["market", "source_rank", "code"]).reset_index(drop=True)


def get_stock_master_categories(master_df: pd.DataFrame) -> list[str]:
    if master_df is None or master_df.empty:
        return []
    s1 = master_df["category"].fillna("").astype(str).tolist() if "category" in master_df.columns else []
    s2 = master_df["official_industry"].fillna("").astype(str).tolist() if "official_industry" in master_df.columns else []
    return sorted({x for x in (s1 + s2) if _safe_str(x)})


def get_stock_master_diagnostics(master_df: pd.DataFrame | None = None) -> list[str]:
    if master_df is None or master_df.empty:
        master_df = load_stock_master()
    if master_df is None or master_df.empty:
        return ["主檔為空"]
    return [
        f"主檔總筆數：{len(master_df)}",
        f"正式產業有值：{int(master_df['official_industry'].fillna('').astype(str).str.strip().ne('').sum())}",
        f"主題有值：{int(master_df['theme_category'].fillna('').astype(str).str.strip().ne('').sum())}",
        f"待修：{int(master_df['待修原因'].fillna('').astype(str).str.strip().ne('').sum())}",
    ]
