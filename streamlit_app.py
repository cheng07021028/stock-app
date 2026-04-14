import streamlit as st
import requests
import pandas as pd
import urllib3
import re
import html
from io import BytesIO

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

st.set_page_config(page_title="台股每日資料查詢", layout="wide")

st.title("台股每日資料查詢")
st.write("輸入股票代號與日期，查詢證交所每日收盤資料")


def get_twse_data(date_str):
    url = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&date={date_str}&type=ALL"
    r = requests.get(url, timeout=30, verify=False)
    r.raise_for_status()
    return r.json()


def clean_html_text(value):
    if value is None:
        return ""

    text = str(value)
    text = html.unescape(text)
    text = re.sub(r"<.*?>", "", text)
    return text.strip()


def find_stock_row(data, stock_no):
    tables = data.get("tables", [])

    for table in tables:
        title = table.get("title", "")
        fields = table.get("fields", [])
        rows = table.get("data", [])

        for row in rows:
            if len(row) > 0 and str(row[0]).strip() == stock_no:
                return title, fields, row

    return None, None, None


def build_result_dict(fields, row):
    result = {}
    sign_value = ""
    diff_value = ""

    for i in range(len(fields)):
        if i >= len(row):
            continue

        field_name = str(fields[i]).strip()
        value = row[i]

        if field_name == "漲跌(+/-)":
            sign_value = clean_html_text(value)
        elif field_name == "漲跌價差":
            diff_value = str(value).strip()
        else:
            result[field_name] = value

    result["漲跌"] = f"{sign_value}{diff_value}".strip()
    return result


def pick_main_columns(result_dict):
    wanted_cols = [
        "證券代號",
        "證券名稱",
        "成交股數",
        "成交金額",
        "開盤價",
        "最高價",
        "最低價",
        "收盤價",
        "漲跌",
        "本益比"
    ]

    final_dict = {}
    for col in wanted_cols:
        if col in result_dict:
            final_dict[col] = result_dict[col]

    return final_dict


def color_rise_fall(val):
    text = str(val).strip()
    if text.startswith("+"):
        return "color: red; font-weight: bold;"
    elif text.startswith("-"):
        return "color: green; font-weight: bold;"
    return ""


def to_excel_bytes(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="查詢結果")
    output.seek(0)
    return output.getvalue()


col1, col2 = st.columns(2)

with col1:
    stock_no = st.text_input("股票代號", value="2330")

with col2:
    date_str = st.text_input("日期", value="20260413")

if st.button("查詢"):
    if not stock_no.strip():
        st.error("請輸入股票代號")
    elif not date_str.strip():
        st.error("請輸入日期，例如 20260413")
    elif len(date_str.strip()) != 8 or not date_str.strip().isdigit():
        st.error("日期格式錯誤，請輸入 8 碼，例如 20260413")
    else:
        try:
            data = get_twse_data(date_str.strip())
            title, fields, row = find_stock_row(data, stock_no.strip())

            if row:
                full_result = build_result_dict(fields, row)
                main_result = pick_main_columns(full_result)

                df = pd.DataFrame([main_result])

                st.success("查詢成功")
                st.subheader(title)

                styled_df = df.style.map(
                    color_rise_fall,
                    subset=["漲跌"] if "漲跌" in df.columns else None
                )

                st.dataframe(df, use_container_width=True)

                if "漲跌" in df.columns:
                    st.markdown("**漲跌顏色說明：紅色 = 上漲，綠色 = 下跌**")

                excel_data = to_excel_bytes(df)
                file_name = f"stock_{stock_no.strip()}_{date_str.strip()}.xlsx"

                st.download_button(
                    label="下載 Excel",
                    data=excel_data,
                    file_name=file_name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

                with st.expander("查看完整原始欄位"):
                    full_df = pd.DataFrame([full_result])
                    st.dataframe(full_df, use_container_width=True)

            else:
                st.warning("查不到這支股票，可能當天無資料、日期錯誤，或該股票不在此清單中。")

        except Exception as e:
            st.error(f"發生錯誤：{e}")