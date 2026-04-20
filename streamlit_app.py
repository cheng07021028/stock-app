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
