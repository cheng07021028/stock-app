def get_twse_code_name_map(query_date: str) -> pd.DataFrame:
    url = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={query_date}&type=ALL"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*"
    }

    try:
        r = requests.get(url, headers=headers, timeout=30, verify=False)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return pd.DataFrame(columns=["證券代號", "證券名稱", "市場別"])

    tables = data.get("tables", [])
    all_rows = []

    for table in tables:
        fields = table.get("fields", [])
        rows = table.get("data", [])

        if "證券代號" in fields and "證券名稱" in fields:
            for row in rows:
                if len(row) != len(fields):
                    continue
                record = dict(zip(fields, row))
                stock_no = str(record.get("證券代號", "")).strip()
                stock_name = str(record.get("證券名稱", "")).strip()
                if stock_no and stock_name:
                    all_rows.append({
                        "證券代號": stock_no,
                        "證券名稱": stock_name,
                        "市場別": "上市"
                    })

    if not all_rows:
        return pd.DataFrame(columns=["證券代號", "證券名稱", "市場別"])

    return pd.DataFrame(all_rows).drop_duplicates(subset=["證券代號"]).reset_index(drop=True)


def get_tpex_code_name_map() -> pd.DataFrame:
    url = "https://www.tpex.org.tw/openapi/v1/mkt/sii_and_otc_company_info"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*"
    }

    try:
        r = requests.get(url, headers=headers, timeout=30, verify=False)
        r.raise_for_status()

        if not r.text or not r.text.strip():
            return pd.DataFrame(columns=["證券代號", "證券名稱", "市場別"])

        data = r.json()
        if not data:
            return pd.DataFrame(columns=["證券代號", "證券名稱", "市場別"])

        df = pd.DataFrame(data)
        if df.empty:
            return pd.DataFrame(columns=["證券代號", "證券名稱", "市場別"])

        code_col = None
        name_col = None

        for c in df.columns:
            cs = str(c).strip()
            if cs in ["SecuritiesCompanyCode", "公司代號", "股票代號", "代號"]:
                code_col = c
            elif cs in ["CompanyName", "公司名稱", "股票名稱", "名稱"]:
                name_col = c

        if code_col is None:
            for c in df.columns:
                if "代號" in str(c):
                    code_col = c
                    break

        if name_col is None:
            for c in df.columns:
                if "名稱" in str(c):
                    name_col = c
                    break

        if code_col is None or name_col is None:
            return pd.DataFrame(columns=["證券代號", "證券名稱", "市場別"])

        result = pd.DataFrame()
        result["證券代號"] = df[code_col].astype(str).str.strip()
        result["證券名稱"] = df[name_col].astype(str).str.strip()
        result["市場別"] = "上櫃"

        result = result[
            (result["證券代號"] != "") &
            (result["證券名稱"] != "")
        ].drop_duplicates(subset=["證券代號"]).reset_index(drop=True)

        return result

    except Exception:
        return pd.DataFrame(columns=["證券代號", "證券名稱", "市場別"])


def get_all_code_name_map(query_date: str) -> pd.DataFrame:
    twse_df = get_twse_code_name_map(query_date)
    tpex_df = get_tpex_code_name_map()
    all_df = pd.concat([twse_df, tpex_df], ignore_index=True)

    if all_df.empty:
        return pd.DataFrame(columns=["證券代號", "證券名稱", "市場別"])

    return all_df.drop_duplicates(subset=["證券代號"]).reset_index(drop=True)
