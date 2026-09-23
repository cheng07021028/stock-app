# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd

st.set_page_config(page_title="23. 研究選股智慧中心｜H94", layout="wide")
st.title("23. 研究選股智慧中心｜H94")
st.caption("H94 Research Selection Intelligence：把強勢、風控、成熟學習、交易執行、族群Regime與集中度真正整合到研究池選擇。Formal權限永久鎖定。")

try:
    from godpick_h94_research_selection_settings import load_settings_safe
    from godpick_h94_research_selection_intelligence import VERSION
    cfg=load_settings_safe()
except Exception as exc:
    st.error(f"H94載入失敗：{type(exc).__name__}: {exc}")
    st.stop()

c1,c2,c3,c4=st.columns(4)
c1.metric("版本", VERSION.replace("v191_",""))
c2.metric("研究最低Alpha品質", f"{cfg['quality']['min_research_score']:.0f}")
c3.metric("研究池上限", int(cfg['portfolio']['max_research_rows']))
c4.metric("Formal權限", "LOCKED")

st.subheader("H94五個核心治理")
st.dataframe(pd.DataFrame([
    ["Anti-Exhaustion","過熱/高檔鈍化/追價風險與低風控、低執行同時出現時，直接降研究層級。"],
    ["Consensus Downgrade","H81風控、H82成熟學習、H89執行、回測、族群與耗竭多模型一致反對時，真正降級。"],
    ["Sector–Stock Conflict","個股強但族群退潮時，要求證明逆勢領先；不能只靠個股百分位進前排。"],
    ["Portfolio Concentration","研究池限制同類別/大主題集中度，避免8檔幾乎都押同一條半導體鏈。"],
    ["Missing≠Neutral","新聞抓不到時標記Missing，不再把50分誤當成有效中性證據。"],
],columns=["治理","說明"]),use_container_width=True,hide_index=True)

st.subheader("學習口徑")
st.info("T+1/T+5/T+10分別紀錄 Selection Alpha、Entry是否觸發、MFE/MAE、Opportunity Cost、Sector Regime與Exhaustion。選股錯與進場錯分開學，不把全部模型一起扣分。")
st.warning("H94不建立Formal、不放寬H64/H68、不移動停損、不用合成目標硬湊RR。正式交易權限仍由既有治理鏈決定。")
