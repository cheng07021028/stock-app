# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path
import sys
import pandas as pd
ROOT=Path(__file__).resolve().parents[1]; sys.path.insert(0,str(ROOT))
from godpick_execution_governance import build_scan_quality_report, apply_scan_quality_to_frame

def main():
    n=120
    frame=pd.DataFrame({
        '成交額百萬':[500]*n,'20日均成交額百萬':[450]*n,
        '官方資料完整度':[80]*n,'官方因子資料狀態':['完整']*n,
        '官方因子資料日期':['2026-07-31']*n,'K線最後交易日':['2026-08-10']*n,
        '因子來源可信度':[90]*n,'建議倉位上限%':[5]*n,
    })
    rep=build_scan_quality_report({'total_count':n,'history_ok':n,'analyzed_ok':n}, universe_size=n,candidate_count=n,final_count=5,candidate_frame=frame)
    assert '正式推薦暫停' in rep['掃描品質狀態']
    assert rep['正式推薦可用'] is False, rep
    assert rep['倉位折減係數']==0.0
    assert rep['A-資料受限研究可用'] is True
    out=apply_scan_quality_to_frame(frame.head(2),rep)
    assert float(out['建議倉位上限%'].max())==0.0
    print('PASS V183 governance consistency｜formal pause cannot coexist with 正式推薦可用=True')
if __name__=='__main__': main()
