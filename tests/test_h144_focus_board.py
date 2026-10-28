import pandas as pd
from pathlib import Path
from godpick_h144_focus_board import build_focus_board


def _tabs(audit):
    return {'audit': pd.DataFrame(audit),
            'h126_authority': pd.DataFrame([{'股票代號':'2344','H126能否買進':'否'}]),
            'h126_health': pd.DataFrame([{'項目':'H126授權進場檔數','數值':0}]),
            'legacy_priority_overview':pd.DataFrame([{'股票代號':'9999','H102動態優先分':99}])}


def test_oct_28_only_known_stock_not_market_top1():
    state={'scope_incomplete':True,'status':'EXPORT_SCOPE_INCOMPLETE',
        'scan_count':1718,'diagnosis_count':1665,'reported_final':8,'export_count':1}
    out,meta=build_focus_board(_tabs([{
        '股票代號':'2344','股票名稱':'華邦電','類別':'記憶體','H110最終研究優先分':49.75,
        'H105發動階段':'IG2｜ALREADY-IGNITED','H109決策權威':'FADE_VETO',
        'H123法人實證狀態':'PENDING_EVIDENCE',
        'H124大戶實證狀態':'PENDING_EVIDENCE'}]),state)
    assert len(out)==1 and out.loc[0,'股票代號']=='2344'
    assert out.loc[0,'H110研究分']==49.75 and out.loc[0,'觀察順位']==1
    assert out.loc[0,'買進資格']=='禁止依本榜買進'
    assert '局部' in meta['warning'] or '母體' in meta['warning']
    assert meta['h126_buyable']==0


def test_rank_only_comparable_h110_scores_no_legacy():
    t=_tabs([{'股票代號':'2317','H110最終研究優先分':75},
             {'股票代號':'2344','H110最終研究優先分':49.75},
             {'股票代號':'2330','H110最終研究優先分':None},
             {'股票代號':'2399','H110最終研究優先分':float('nan')}])
    r,m=build_focus_board(t,{'status':'SCOPE_RECONCILED_FOR_DISPLAY'})
    assert list(r['股票代號'])==['2317','2344','2330','2399']
    assert r.loc[2,'H110研究分']!=r.loc[2,'H110研究分'] # NaN
    assert '9999' not in r['股票代號'].tolist()


def test_rejects_nonsense_ids_and_score_scale():
    t=_tabs([{'股票代號':'ABC','H110最終研究優先分':98},
             {'股票代號':'2317','H110最終研究優先分':120}])
    r,m=build_focus_board(t, {'scope_incomplete':True})
    assert list(r['股票代號'])==['2317']
    assert pd.isna(r.loc[0,'H110研究分'])


def test_duplicate_stock_keeps_valid_score():
    t=_tabs([{'股票代號':'2344','H110最終研究優先分':None},
             {'股票代號':'2344','H110最終研究優先分':49}])
    r,m=build_focus_board(t,{})
    assert len(r)==1 and r.loc[0,'H110研究分']==49


def test_page_default_has_just_two_sheets():
    s=(Path(__file__).resolve().parents[1]/'pages/7_股神推薦.py').read_text('utf-8')
    assert '"00_主管重點觀察榜", h144_focus' in s
    assert '"00_H126主管決策", tables' in s
    block=s[s.index('    sheets = [\n',s.index('# H144: verify scan->H97')):]
    assert block.split('    for name, frame, fallback in sheets:',1)[0].count('("00_')==2
    assert '⬇️ 下載主管精簡觀察榜（僅2張）' in s


def test_no_authority_data_no_fake_stock_ranking():
    r,m=build_focus_board(_tabs([]),{'scope_incomplete':True})
    assert r.empty and m['ranked']==0
    assert m['h126_buyable']==0
