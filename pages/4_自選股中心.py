from datetime import date
import streamlit as st

from utils import (
    get_normalized_watchlist,
    save_watchlist,
    apply_font_scale,
    get_font_scale,
    search_stock_candidates,
    build_stock_candidate_labels,
)

from watchlist_ui_state import (
    load_watchlist_ui_state,
    save_watchlist_ui_state,
)

st.set_page_config(page_title="自選股中心", page_icon="⭐", layout="wide")

if "font_scale" not in st.session_state:
    st.session_state.font_scale = get_font_scale()

apply_font_scale(st.session_state.font_scale)

if "watchlist_ui_loaded" not in st.session_state:
    ui_state = load_watchlist_ui_state()
    st.session_state.wl_selected_group = ui_state.get("selected_group", "")
    st.session_state.wl_stock_keyword = ui_state.get("stock_keyword", "")
    st.session_state.wl_selected_candidate_label = ui_state.get("selected_candidate_label", "")
    st.session_state.wl_manual_code = ui_state.get("manual_code", "")
    st.session_state.wl_manual_name = ui_state.get("manual_name", "")
    st.session_state.watchlist_ui_loaded = True

st.title("⭐ 自選股中心")
st.caption("可管理自選股群組、手動新增股票代號與名稱、刪除股票與群組")

watchlist_dict = get_normalized_watchlist()

# =========================
# 新增群組
# =========================
st.markdown("---")
st.subheader("新增群組")

with st.form("add_group_form", clear_on_submit=False):
    c1, c2 = st.columns([3, 1])

    with c1:
        new_group_name = st.text_input(
            "新群組名稱",
            placeholder="例如：AI概念股"
        )

    with c2:
        st.write("")
        st.write("")
        add_group_btn = st.form_submit_button("新增群組", use_container_width=True)

if add_group_btn:
    group_name = str(new_group_name).strip()

    if not group_name:
        st.warning("請輸入群組名稱。")
    elif group_name in watchlist_dict:
        st.warning(f"群組「{group_name}」已存在。")
    else:
        watchlist_dict[group_name] = []
        save_watchlist(watchlist_dict)
        st.session_state.wl_selected_group = group_name
        st.success(f"已新增群組：{group_name}")
        st.rerun()

# =========================
# 新增股票
# =========================
st.markdown("---")
st.subheader("新增股票")

watchlist_dict = get_normalized_watchlist()
group_names = list(watchlist_dict.keys())

if not group_names:
    st.warning("請先建立群組。")
else:
    today_dt = date.today()
    lookup_date = today_dt.strftime("%Y%m%d")

    saved_group = st.session_state.get("wl_selected_group", "")
    group_index = group_names.index(saved_group) if saved_group in group_names else 0

    with st.form("watchlist_add_stock_form", clear_on_submit=False):
        c1, c2 = st.columns(2)

        with c1:
            selected_group = st.selectbox(
                "選擇群組",
                group_names,
                index=group_index
            )

        with c2:
            stock_keyword = st.text_input(
                "輸入股票名稱或代號",
                value=st.session_state.get("wl_stock_keyword", ""),
                placeholder="例如：台積電 / 東元 / 2330 / 1504"
            )

        candidate_df = search_stock_candidates(stock_keyword, lookup_date, top_n=20) if stock_keyword.strip() else None
        candidate_labels = build_stock_candidate_labels(candidate_df) if candidate_df is not None else []

        selected_candidate_label = ""
        selected_code = ""
        selected_name = ""

        if candidate_labels:
            saved_candidate = st.session_state.get("wl_selected_candidate_label", "")
            candidate_index = candidate_labels.index(saved_candidate) if saved_candidate in candidate_labels else 0

            selected_candidate_label = st.selectbox(
                "查詢結果",
                candidate_labels,
                index=candidate_index
            )

            selected_row = candidate_df.iloc[candidate_labels.index(selected_candidate_label)]
            selected_code = str(selected_row["證券代號"]).strip()
            selected_name = str(selected_row["證券名稱"]).strip()
        elif stock_keyword.strip():
            st.info("查無符合股票，仍可手動輸入代號與名稱新增。")

        d1, d2 = st.columns(2)

        with d1:
            stock_code = st.text_input(
                "股票代號",
                value=selected_code if selected_code else st.session_state.get("wl_manual_code", ""),
                placeholder="例如：2330"
            )

        with d2:
            stock_name = st.text_input(
                "股票名稱",
                value=selected_name if selected_name else st.session_state.get("wl_manual_name", ""),
                placeholder="例如：台積電"
            )

        add_stock_btn = st.form_submit_button("加入自選股", type="primary", use_container_width=True)

    save_watchlist_ui_state(
        selected_group=selected_group,
        stock_keyword=stock_keyword,
        selected_candidate_label=selected_candidate_label,
        manual_code=stock_code,
        manual_name=stock_name,
    )

    if add_stock_btn:
        stock_code = str(stock_code).strip()
        stock_name = str(stock_name).strip()

        if not stock_code:
            st.warning("請輸入或選擇股票。")
        else:
            current_items = watchlist_dict.get(selected_group, [])
            exists = any(str(item.get("code", "")).strip() == stock_code for item in current_items)

            if exists:
                st.warning(f"{stock_code} 已存在於群組「{selected_group}」。")
            else:
                current_items.append({
                    "code": stock_code,
                    "name": stock_name
                })
                watchlist_dict[selected_group] = current_items
                save_watchlist(watchlist_dict)

                st.session_state.wl_selected_group = selected_group
                st.session_state.wl_stock_keyword = stock_keyword
                st.session_state.wl_selected_candidate_label = selected_candidate_label
                st.session_state.wl_manual_code = stock_code
                st.session_state.wl_manual_name = stock_name

                st.success(f"已加入 {stock_name or stock_code} 到群組「{selected_group}」")
                st.rerun()

# =========================
# 群組內容管理
# =========================
st.markdown("---")
st.subheader("群組內容管理")

watchlist_dict = get_normalized_watchlist()
group_names = list(watchlist_dict.keys())

if not group_names:
    st.info("目前沒有群組。")
else:
    manage_group = st.selectbox("查看群組", group_names, key="manage_group_selectbox")
    group_items = watchlist_dict.get(manage_group, [])

    if not group_items:
        st.info(f"群組「{manage_group}」目前沒有股票。")
    else:
        st.markdown(f"### 群組：{manage_group}")

        for idx, item in enumerate(group_items):
            code = str(item.get("code", "")).strip()
            name = str(item.get("name", "")).strip()

            c1, c2, c3 = st.columns([2, 3, 1])
            with c1:
                st.write(code)
            with c2:
                st.write(name if name else "—")
            with c3:
                if st.button("刪除", key=f"delete_stock_{manage_group}_{idx}", use_container_width=True):
                    group_items.pop(idx)
                    watchlist_dict[manage_group] = group_items
                    save_watchlist(watchlist_dict)
                    st.success(f"已刪除 {code}")
                    st.rerun()

# =========================
# 刪除群組
# =========================
st.markdown("---")
st.subheader("刪除群組")

watchlist_dict = get_normalized_watchlist()
group_names = list(watchlist_dict.keys())

if not group_names:
    st.info("目前沒有可刪除群組。")
else:
    with st.form("delete_group_form", clear_on_submit=False):
        delete_group_name = st.selectbox("選擇要刪除的群組", group_names)
        confirm_delete = st.checkbox("我確認要刪除此群組（包含群組內所有股票）")
        delete_group_btn = st.form_submit_button("刪除群組", use_container_width=True)

    if delete_group_btn:
        if not confirm_delete:
            st.warning("請先勾選確認刪除。")
        else:
            if delete_group_name in watchlist_dict:
                del watchlist_dict[delete_group_name]
                save_watchlist(watchlist_dict)

                if st.session_state.get("wl_selected_group", "") == delete_group_name:
                    st.session_state.wl_selected_group = ""

                st.success(f"已刪除群組：{delete_group_name}")
                st.rerun()
