import streamlit as st

from utils import (
    get_normalized_watchlist,
    save_watchlist,
    apply_font_scale,
    get_font_scale,
)


def dedup_group_items(items):
    seen = set()
    result = []

    for item in items:
        code = str(item.get("code", "")).strip()
        market = str(item.get("market", "")).strip() or "上市"
        key = (code, market)

        if not code or key in seen:
            continue

        seen.add(key)
        result.append({
            "code": code,
            "name": str(item.get("name", "")).strip() or code,
            "market": market,
        })

    return result


st.set_page_config(page_title="自選股中心", page_icon="⭐", layout="wide")

if "font_scale" not in st.session_state:
    st.session_state.font_scale = get_font_scale()

apply_font_scale(st.session_state.font_scale)

st.title("⭐ 自選股中心")
st.caption("正式整合版｜管理群組與自選股票")

watchlist_dict = get_normalized_watchlist()
group_names = list(watchlist_dict.keys())

st.markdown("---")
st.subheader("新增群組")

c1, c2 = st.columns([3, 1])
with c1:
    new_group_name = st.text_input("新群組名稱", placeholder="例如：AI概念股", key="new_group_name")
with c2:
    add_group_btn = st.button("新增群組", use_container_width=True)

if add_group_btn:
    group_name = str(new_group_name).strip()
    if not group_name:
        st.warning("請輸入群組名稱。")
    elif group_name in watchlist_dict:
        st.warning("群組已存在。")
    else:
        watchlist_dict[group_name] = []
        if save_watchlist(watchlist_dict):
            st.success(f"已新增群組：{group_name}")
            st.rerun()
        else:
            st.error("寫入 watchlist.json 失敗。")

st.markdown("---")
st.subheader("新增股票")

if not group_names:
    st.info("目前沒有群組，請先新增群組。")
else:
    c1, c2, c3, c4 = st.columns([2, 2, 2, 1])

    with c1:
        target_group = st.selectbox("選擇群組", group_names, key="target_group")
    with c2:
        stock_code = st.text_input("股票代號", placeholder="例如：2330", key="stock_code")
    with c3:
        stock_name = st.text_input("股票名稱（可選填）", placeholder="例如：台積電", key="stock_name")
    with c4:
        market_type = st.selectbox("市場別", ["上市", "上櫃"], key="market_type")

    add_stock_btn = st.button("加入自選股", type="primary", use_container_width=True)

    if add_stock_btn:
        code = str(stock_code).strip()
        name = str(stock_name).strip() or code
        market = str(market_type).strip() or "上市"

        if not code:
            st.warning("請輸入股票代號。")
        else:
            if target_group not in watchlist_dict:
                watchlist_dict[target_group] = []

            watchlist_dict[target_group].append({
                "code": code,
                "name": name,
                "market": market,
            })
            watchlist_dict[target_group] = dedup_group_items(watchlist_dict[target_group])

            if save_watchlist(watchlist_dict):
                st.success(f"已加入：{name}（{code}）[{market}]")
                st.rerun()
            else:
                st.error("寫入 watchlist.json 失敗。")

st.markdown("---")
st.subheader("目前自選股清單")

watchlist_dict = get_normalized_watchlist()
group_names = list(watchlist_dict.keys())

if not group_names:
    st.info("目前沒有任何群組。")
else:
    for group_name in group_names:
        with st.expander(f"{group_name}（{len(watchlist_dict.get(group_name, []))} 檔）", expanded=True):
            items = watchlist_dict.get(group_name, [])

            if not items:
                st.info("此群組目前沒有股票。")
            else:
                rows = []
                for idx, item in enumerate(items):
                    rows.append({
                        "序號": idx + 1,
                        "股票代號": item.get("code", ""),
                        "股票名稱": item.get("name", ""),
                        "市場別": item.get("market", ""),
                    })

                st.dataframe(rows, use_container_width=True, hide_index=True)

                st.markdown("#### 刪除股票")
                stock_labels = [
                    f"{item.get('name', item.get('code', ''))} ({item.get('code', '')}) [{item.get('market', '')}]"
                    for item in items
                ]

                c1, c2 = st.columns([4, 1])
                with c1:
                    delete_stock_label = st.selectbox(
                        f"選擇要刪除的股票｜{group_name}",
                        stock_labels,
                        key=f"delete_stock_{group_name}"
                    )
                with c2:
                    delete_stock_btn = st.button("刪除股票", key=f"delete_stock_btn_{group_name}", use_container_width=True)

                if delete_stock_btn:
                    delete_index = stock_labels.index(delete_stock_label)
                    del watchlist_dict[group_name][delete_index]

                    if save_watchlist(watchlist_dict):
                        st.success(f"已刪除股票：{delete_stock_label}")
                        st.rerun()
                    else:
                        st.error("寫入 watchlist.json 失敗。")

            st.markdown("#### 刪除群組")
            if st.button(f"刪除群組：{group_name}", key=f"delete_group_{group_name}", use_container_width=True):
                watchlist_dict.pop(group_name, None)

                if save_watchlist(watchlist_dict):
                    st.success(f"已刪除群組：{group_name}")
                    st.rerun()
                else:
                    st.error("寫入 watchlist.json 失敗。")
