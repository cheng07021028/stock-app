# -*- coding: utf-8 -*-
from godpick_t1_trade_truth import build_h57_h60_learning_summary, build_h70_learning_summary

def main():
    rows=[]
    for i in range(35):
        rows.append({
            "T1成熟":True,"H70學習快照狀態":"SNAPSHOT-READY",
            "H70逆勢研究層級":"X1｜逆勢Alpha重點觀察" if i<20 else "X2｜逆勢韌性追蹤",
            "隔日候選漲跌%": 2.0 if i%3 else -1.0,
            "Selection Alpha%": 1.5 if i%3 else -0.5,
        })
    s=build_h70_learning_summary(rows)
    assert s["H70學習快照成熟樣本"] == 35
    assert s["H70學習啟用狀態"] == "ACTIVE"
    assert s["H70_X1成熟樣本"] == 20 and s["H70_X2成熟樣本"] == 15
    c=build_h57_h60_learning_summary(rows)
    assert c["H70_X1成熟樣本"] == 20 and c["H70_X2成熟樣本"] == 15
    print("PASS H70 T1 learning")

if __name__ == "__main__": main()
