with tabs[5]:
    real_df = df.copy()
    real_df["實際報酬%"] = pd.to_numeric(real_df["實際報酬%"], errors="coerce")
    traded_df = real_df[real_df["是否已實際買進"].astype(str) == "是"].copy()

    if traded_df.empty:
        st.info("目前沒有已實際買進的紀錄。")
    else:
        trade_total = len(traded_df)
        trade_win = int((traded_df["實際報酬%"] > 0).sum())
        trade_avg = traded_df["實際報酬%"].mean()

        render_pro_kpi_row(
            [
                {
                    "label": "實際交易筆數",
                    "value": trade_total,
                    "delta": "已買進紀錄",
                    "delta_class": "pro-kpi-delta-flat",
                },
                {
                    "label": "實際勝率",
                    "value": f"{(trade_win / trade_total * 100):.2f}%" if trade_total > 0 else "0%",
                    "delta": f"獲利 {trade_win} 筆",
                    "delta_class": "pro-kpi-delta-flat",
                },
                {
                    "label": "平均實際報酬",
                    "value": f"{trade_avg:+.2f}%" if pd.notna(trade_avg) else "—",
                    "delta": "已買進樣本",
                    "delta_class": "pro-kpi-delta-flat",
                },
            ]
        )

        st.dataframe(traded_df, use_container_width=True, hide_index=True)
