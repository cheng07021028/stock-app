def _render_login_page():
    _render_sidebar_user_block()

    render_pro_hero(
        title="台股分析系統｜股神版首頁",
        subtitle="Firebase 登入版｜請先登入再進入首頁與各分析頁面",
    )

    c1, c2, c3 = st.columns([1.2, 1.6, 1.2])

    with c2:
        st.markdown("## 🔐 系統登入")

        login_email = st.text_input(
            "Email",
            value=st.session_state.get("login_email", ""),
            placeholder="請輸入 Email",
        )
        login_password = st.text_input(
            "Password",
            type="password",
            placeholder="請輸入密碼",
        )

        err = st.session_state.get("login_error", "")
        if err:
            st.error(err)

            # 暫時除錯：確認 Streamlit Cloud 目前讀到的 Firebase API Key
            try:
                api_key = str(st.secrets.get("FIREBASE_API_KEY", ""))
                st.caption(f"DEBUG API KEY 前8碼：{api_key[:8]} | 長度：{len(api_key)}")
            except Exception as e:
                st.caption(f"DEBUG 讀取 API KEY 失敗：{e}")

        col_a, col_b = st.columns(2)

        with col_a:
            if st.button("登入系統", use_container_width=True, type="primary"):
                if not login_email.strip() or not login_password.strip():
                    st.warning("請輸入帳號與密碼")
                else:
                    _do_login(login_email.strip(), login_password)

        with col_b:
            if st.button("清除", use_container_width=True):
                st.session_state["login_error"] = ""
                st.session_state["login_email"] = ""
                st.rerun()

        st.markdown("---")
        st.caption("登入成功後才會載入首頁內容與自選股總覽。")
        st.caption("首次登入成功後，若 users/{uid} 不存在，系統會自動建立預設資料。")
