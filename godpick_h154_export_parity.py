# -*- coding: utf-8 -*-
"""V191-H154: current-run Excel vs H85/H126 export truth reconciliation.

No network, no persistence mutation, no invented recommendation, no H109/H126
changes. A research watchlist is not the official system recommendation.
"""
from __future__ import annotations
from collections.abc import Mapping
from typing import Any
import pandas as pd

VERSION='V191-H154-EXPORT-PARITY'

def _code(x:Any)->str:
    if x is None:return ''
    s=str(x).strip()
    if s.endswith('.0') and s[:-2].isdigit():s=s[:-2]
    return s.zfill(4) if s.isdigit() and len(s)<4 else s

def _rows(obj):
    return obj.to_dict('records') if isinstance(obj,pd.DataFrame) else [dict(x) for x in obj if isinstance(x,Mapping)] if isinstance(obj,list) else []

def _codes(obj):
    return [c for r in _rows(obj) if (c:=_code(r.get('股票代號'))) and len(c)==4 and c.isdigit()]

def _uniq(codes):return list(dict.fromkeys(codes))

def _named(rows):
    """Preserve original same-run stock names for manual H85/Excel reconciliation."""
    out=[];seen=set()
    for row in _rows(rows):
        code=_code(row.get('股票代號'))
        if len(code)!=4 or not code.isdigit() or code in seen:continue
        seen.add(code)
        name=str(row.get('股票名稱') or '').strip()
        out.append((code,name))
    return out

def reconcile_run(*, tables:Mapping, ranked, watch, research, plans, six_summary,
                  h145_status:str, run_id='', scan_run_id='', scan_report=None):
    """Current in-memory tables from ONE export, not a different historical cache."""
    tables=tables if isinstance(tables,Mapping) else {}
    scanned=scan_report if isinstance(scan_report,Mapping) else {}
    h126=[r for r in _rows(tables.get('h126_authority'))
          if _code(r.get('股票代號')) and str(r.get('H126能否買進','')).strip() in ('可以買進','允許買進','是')]
    research_rows=[r for r in _rows(ranked) if str(r.get('排名等級','')).startswith(('A','B'))]
    research_codes=_uniq(_codes(research_rows))
    watch_codes=[c for c in _uniq(_codes(watch)) if c not in research_codes]
    waiting=_uniq(_codes(tables.get('waiting')))
    emerging=_uniq(_codes(tables.get('emerging_watch')))
    system_research=_uniq(_codes(tables.get('research')))
    plans_codes=_uniq(_codes(plans))
    mismatch=bool(str(h145_status).startswith('BLOCKED_'))
    waiting_named=_named(tables.get('waiting'))
    emerging_named=_named(tables.get('emerging_watch'))
    identity='MATCHED' if run_id and scan_run_id and run_id==scan_run_id else (
        'MISMATCH' if run_id and scan_run_id else 'UNVERIFIED')
    state='RUN_CONFLICT' if identity=='MISMATCH' else ('FORMAL_BLOCKED_H145' if mismatch else 'AUDIT_ONLY')
    return {'run_id':str(run_id or ''),'scan_run_id':str(scan_run_id or ''),
        'run_identity':identity,'h145_status':str(h145_status or ''),'status':state,
        'h126_buyable':len(_uniq(_codes(h126))),'h85_waiting':len(waiting),
        'h85_waiting_codes':waiting,'h85_waiting_named':waiting_named,'h85_emerging':len(emerging),'h85_emerging_codes':emerging,'h85_emerging_named':emerging_named,
        'h85_research':len(system_research),'ranked':len(research_codes),
        'ranked_codes':research_codes,'watch':len(watch_codes),'watch_codes':watch_codes,
        'plans_codes':plans_codes,'all_research_codes':_uniq(_codes(research)),
        'six_stage_rows':len(_rows(six_summary)),
        'scan_report':{k:scanned.get(k) for k in ('預計掃描數','成功分析數','完整候選診斷數','最終作戰候選數')},
        'message':('H145輸出完整性未通過，H126正式0檔不可被研究池替代' if mismatch else
                   '各研究池和正式推薦為不同口徑；不得相加或互相冒充')}

def append_audit_and_overview(wb, audit:dict):
    """Write explicitly labeled sources without changing any H126 authority cell."""
    ws=wb['00_主管重點觀察榜']
    old=str(ws['B4'].value or '')
    new=(f"｜H154同輪H85等待/雷達 {audit['h85_waiting']} 檔、"
         f"新興觀察 {audit['h85_emerging']} 檔（與研究榜為不同口徑）")
    ws['B4']=old+new if new not in old else old
    ws['A5']='H126正式交易資格（不得與研究混算）'
    old5=str(ws['B5'].value or '')
    ws['B5']=old5+f"｜H154比對：系統正式 {audit['h126_buyable']} 檔；{audit['status']}"
    if audit['run_id']:
        ws['B1']=str(ws['B1'].value or '')+f"｜本輪ID：{audit['run_id']}"
    if audit['h85_waiting_codes']:
        ws['B6']=str(ws['B6'].value or '') + ('｜H154 系統等待池前五（非正式推薦）：'+
                                              '、'.join(audit['h85_waiting_codes'][:5]))
    ws['B4'].alignment=ws['B4'].alignment.copy(wrap_text=True)
    ws.row_dimensions[4].height=max(ws.row_dimensions[4].height or 30,46)
    return True

def append_audit_summary(summary,audit:dict):
    summary=summary.copy() if isinstance(summary,pd.DataFrame) else pd.DataFrame(columns=['檢查項目','結果','說明'])
    records=[
        ('H154同輪執行ID',audit['run_id'] or '未驗證','與系統最新快照需比對相同run_id；沒有身分不可宣稱對齊'),
        ('H154同輪身分比對',audit['run_identity'],f"掃描ID：{audit['scan_run_id'] or '未驗證'}"),
        ('H154正式推薦唯一來源H126',audit['h126_buyable'],'正式權威股票，非H153研究/非H85等待'),
        ('H154系統H85等待/雷達股',audit['h85_waiting'],'本輪H85 waiting來源，並非正式買進，不能假稱H153研究榜與其相同'),
        ('H154系統H85新興觀察股',audit['h85_emerging'],'同輪H85 emerging_watch，非正式買進'),
        ('H154系統H85研究股',audit['h85_research'],'與H153高成長技術研究流程不同'),
        ('H154系統H85等待前25代號',','.join(audit['h85_waiting_codes'][:25]) or '無','僅已執行同輪的非交易等待/雷達池'),
        ('H154系統H85等待具名清單','、'.join(f'{c} {n}'.strip() for c,n in audit.get('h85_waiting_named',[])[:25]) or '無','每檔股票代號／名稱來自本輪H85 waiting，不得視為H153排名'),
        ('H154系統H85新興前25代號',','.join(audit['h85_emerging_codes'][:25]) or '無','僅本輪觀察池'),
        ('H154系統H85新興具名清單','、'.join(f'{c} {n}'.strip() for c,n in audit.get('h85_emerging_named',[])[:25]) or '無','每檔股票代號／名稱來自本輪H85 emerging_watch'),
        ('H154 H153具名研究股',audit['ranked'],'01表有名次的A/B研究候選；不得視為H126買進'),
        ('H154 W級非交易核查股',audit['watch'],'00與01的獨立核查，且需先去重'),
        ('H154 H153研究前五代號',','.join(audit['ranked_codes'][:5]) or '無','03五檔研究計畫來源；非正式推薦'),
        ('H154正式完整性狀態',audit['status'],audit['message']+'｜'+audit['h145_status']),
        ('H154六維研究階段差異','H149階段≠H153名次','04六維摘要取上游研究母體，候選數不得當H153最終具名研究或H126筆數'),
        ('H154原分與排序分','H153原始分/ H154排序分','重複快照無新證據時的研究優先調整公開顯示；同名次可以並列'),
    ]
    return pd.concat([summary,pd.DataFrame([{'檢查項目':a,'結果':b,'說明':c} for a,b,c in records])],ignore_index=True)

def tag_six_pillar_stage(frame):
    value=frame.copy() if isinstance(frame,pd.DataFrame) else pd.DataFrame(columns=['研究面向','本輪事實','主要盲點／反證','下一步'])
    if not value.empty and not (value.get('研究面向',pd.Series(dtype=str)).astype(str).str.contains('H154階段').any()):
        row={'研究面向':'H154階段口徑說明',
             '本輪事實':'本表屬H149/H152.1上游六維研究階段；不是H153最終具名研究/系統H85等待清單',
             '主要盲點／反證':'本表13檔等數字不可直接與H153的17檔、H85等待25檔、H126的0檔比較',
             '下一步':'請到00主管與02 H154同輪口徑稽核，核對同輪執行ID與H145完整性'}
        value=pd.concat([value,pd.DataFrame([row])],ignore_index=True)
    return value

def verify_final_xlsx(wb,audit:dict):
    """Strict content-level gate: compare visible stock codes/ranks/scores."""
    manager=wb['00_主管重點觀察榜'];research=wb['01_具名強勢研究'];plan=wb['03_五檔研究計畫'];summary=wb['02_研究召回統計']
    from openpyxl.utils.cell import range_boundaries
    codes=[];ranks=[];scores=[]
    if 'H1537ManagerResearch' in manager.tables:
        left,top,right,bottom=range_boundaries(manager.tables['H1537ManagerResearch'].ref)
        for r in range(top+1,bottom+1):
            codes.append(_code(manager.cell(r,2).value))
            ranks.append(int(manager.cell(r,1).value))
            scores.append(str(manager.cell(r,5).value or ''))
    expected=audit['ranked_codes']
    if codes!=expected:
        # H160: first eight can agree while a tie farther down was reordered.
        # Preserve the strict same-run fail-closed gate, but locate the exact
        # discrepancy so the operator can distinguish sorting from missing rows.
        position=next((i for i,(left,right) in enumerate(zip(codes,expected))
                       if left!=right),min(len(codes),len(expected)))
        left=codes[position] if position<len(codes) else '缺項'
        right=expected[position] if position<len(expected) else '缺項'
        raise ValueError(
            f'H154主管研究與實際本輪排名不同：首次差異第{position+1}檔 '
            f'00={left} 來源={right}；00共{len(codes)}檔／來源共{len(expected)}檔。'
            ' 請檢查同名次內H154排序分／輸出時是否重排，不得跳過對帳。')
    headers=[c.value for c in research[1]]
    for field in ('研究排名','股票代號','研究證據分'):
        if field not in headers:raise ValueError('H154研究表缺少：'+field)
    positions={field:headers.index(field)+1 for field in headers}
    rs=[]
    for row in range(2,research.max_row+1):
        grade=str(research.cell(row,positions.get('排名等級',2)).value or '')
        code=_code(research.cell(row,positions['股票代號']).value)
        if grade.startswith(('A','B')) and code:
            rs.append((code,int(research.cell(row,positions['研究排名']).value)))
    if rs!=list(zip(codes,ranks)):
        raise ValueError('H154主管00與01研究名次/代號不一致')
    plan_head=[c.value for c in plan[1]]
    if '股票代號' in plan_head:
        i=plan_head.index('股票代號')+1
        pc=[_code(plan.cell(r,i).value) for r in range(2,plan.max_row+1)]
        pc=[c for c in pc if c and c.isdigit() and len(c)==4]
        if pc!=expected[:5]:raise ValueError('H154五檔計畫與01前五名排序不一致')
    if expected and any('排序' in s for s in scores) and any('H154研究排序分' not in headers for _ in [0]):
        raise ValueError('H154主管顯示排序分、01卻沒有排序分來源')
    audits={str(summary.cell(i,1).value) for i in range(2,summary.max_row+1)}
    if 'H154正式推薦唯一來源H126' not in audits:
        raise ValueError('H154研究報表缺系統正式權威對帳明細')
    if audit['h126_buyable']==0 and '禁止買進' not in str(wb['00_H126主管決策']['G2'].value or ''):
        raise ValueError('H154正式0檔卻缺H126禁止買進')
    return True
