V191-H165｜交易員模式：實檔稽核、急跌隔離、主升共振與抗跌基準修正
===================================================================
程式基準：GitHub main，開始核對時 HEAD 9cfa039042c9ed72e85ed61d8f4ca1c9546c849e
            以下三個已修改檔案以 GitHub main 當前位元組內容為原始基準：
            godpick_h153_growth_setup_rank.py：f7b3378b98bb2cfe3f620d271c3df6ec28b01702
            godpick_h157_trader_audit.py：02b828961955827a21ac9181144e2029d8134db7
          原 H164 godpick_h164_trader_focus.py 無變動。
實檔稽核來源：StockPick_H1521_Potential_20261116_103819.xlsx。
研究日期 2026-11-16，原本14張工作表；上市/上櫃最新市場背景查核日 2026-11-17。

必要修改檔（ZIP相對路徑覆蓋 GitHub main；其它程式不覆蓋）：
- godpick_h153_growth_setup_rank.py
  H165：5日下跌 <= -15% 或單日跌 <= -8% 的技術研究股不再取得預發動優先名次。
  此檔股票仍留在診斷/稽核/漏網與原始研究線索，不刪除任何紀錄。
  新欄位「H165主升起漲研究核查」：三率兩季同比、已公告毛利/營益五季突破、
  營收年增/近3月年增、黃金交叉或箱型突破、同日量比、85根OHLCV與MACD都要有
  可用證據。通過亦僅表示研究共振，不得成為H126買進授權或無校準勝率。
- godpick_h157_trader_audit.py
  H165：相對抗跌遇「上櫃或市場未證」不再假冒已核對市場指數。
  保留 H157 的11/12兩表以及 H164 的既有導航，在Sheet11結尾新增H165稽核。
- godpick_h165_evidence_gate.py（新增）
  純證據分析與H165導覽；清楚分隔原始研究、W核查、極端下跌、官方財報與技術。
- tests/test_h165_trader_evidence_gate.py（離線測試，非必要生產檔）
- H165_README.txt

實檔結果（使用者11/16檔；不是11/17即時個股排名）：
診斷1196、H113稽核120、H153研究B 5、W核查8、U待核11、H126正式0。
H115官方原始PE/EPS帳本0、前五檔三率+獲利雙認0、有效3/5日抗跌0。
3595山太士：Excel 近五日-28.804%，即為 H165 急跌隔離針對案例。
H145 supplied complete/MATCHED 不等於公司資料全部完整，也不等於有可交易股票。
H153原研究排序與H154調整排序分不可混為一談，H126不能從B等級推出。

另附：StockPick_V191_H165_Audit_20261117.xlsx
以artifact_tool從使用者11/16實檔匯入新增第15張「13_H165交易員稽核」。
原有14表及前五內容未改；新增24股逐列證據/防追高/官方缺口/11/18市場三情境。
來源：
https://www.cna.com.tw/news/afe/202611170136.aspx
https://tw.stock.yahoo.com/news/%E6%AB%83%E8%B2%B7%E7%9B%A4%E5%BE%8C-%E8%B7%8C12-51%E9%BB%9E-054554666.html
https://www.fidelity.com/learning-center/trading-investing/technical-analysis
https://www.twse.com.tw/ / https://www.tpex.org.tw/ / https://mops.twse.com.tw/

驗證：64項相關離線測試通過（H165+H157+H164+H153+H153.2+H153.4+H153.9+H154）。
已測 Python compile、ZIP CRC/路徑、中文檔名、工作表匯出回讀。
未登入正式 Streamlit/未部署、未執行11/17最新全市場K線及官方估值/季度資料更新；
11/18預測只提供行情已知下的條件情境，絕不輸出勝率。H126原權威不受本包更動。

下一步驗收：
1 GitHub main 同路徑覆蓋三個生產檔；保留 H164 及其他既有檔，不動 runtime-data/Secrets。
2 Streamlit重新啟動，第7頁用完整已更新同輪母體重建Excel。
3 檢查02統計新增「H165極端跌勢隔離／待止跌認證」及 01「H165主升起漲研究核查」欄。
4 第11頁H164摘要後可看H165逐股證據稽核；13-15張表變化本包只會擴充11頁，
  程式自動匯出仍保持14張原表；獨立稽核版Excel另加第15張。
5 仍須處理官方H115 PE/EPS帳本0筆、風控與工作排程／H145上游來源覆蓋問題；
  不可把測試成功當作實盤或行情更新成功。
