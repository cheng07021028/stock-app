# -*- coding: utf-8 -*-
"""H165 evidence gate: no overclaiming about anti-fall / earnings breakout / next-day gain.

Pure auditable checks for H153/H157 workbooks. Does not change H126, scan pool,
ranking weights, original rows, network snapshots, settings, or historical data.
"""
from __future__ import annotations
from collections import Counter
from datetime import date, datetime
from math import isfinite
from typing import Any, Mapping

VERSION = 'V191-H165-TRADER-EVIDENCE-GATE'
CRASH_5D_PCT = -15.0


def _t(v: Any) -> str:
    return str(v).strip() if v is not None else ''


def _v(v: Any) -> float | None:
    if isinstance(v, bool) or v is None: return None
    try:
        x=float(_t(v).replace(',','').replace('%',''))
        return x if isfinite(x) else None
    except (ValueError, TypeError, OverflowError): return None


def _date(v: Any) -> str:
    if isinstance(v, datetime): return v.date().isoformat()
    if isinstance(v, date):return v.isoformat()
    try: return date.fromisoformat(_t(v)[:10]).isoformat()
    except ValueError: return ''


def severe_fall_reason(*, five_day: Any, today: Any = None) -> str:
    """Screen falls, not historical removal. Missing return never implies pass."""
    r5=_v(five_day);d1=_v(today)
    if r5 is not None and r5<=CRASH_5D_PCT:
        return f'5日跌幅{r5:+.2f}%≤{CRASH_5D_PCT:g}%：急跌觀察，不得當作預發動'
    if d1 is not None and d1<=-8:
        return f'單日急跌{d1:+.2f}%≤-8%：須先驗證止跌，不當作預發動'
    return ''


def leader_start_gate(*, yoy_margin_two: bool, profit_breakout: bool,
                      revenue_verified: bool, revenue_yoy: Any,
                      revenue_3m_yoy: Any, price_cross: bool,
                      volume_ratio: Any, price_bars: Any,
                      macd_status: str, five_day: Any) -> str:
    """Necessary hypotheses only; research permission and H126 are separate.

    '主升起漲' is never established by a past or model-projected revenue high.
    """
    blockers=[]
    if not (yoy_margin_two and profit_breakout):blockers.append('需已公告連兩季三率同比改善及五季毛利/營益突破')
    yy=_v(revenue_yoy);mm=_v(revenue_3m_yoy)
    if not (revenue_verified and yy is not None and mm is not None and yy>=15 and mm>=12):
        blockers.append('需有公告日期的營收YoY≥15%與近三月YoY≥12%')
    if not price_cross:blockers.append('需同日確認黃金交叉或箱型突破')
    ratio=_v(volume_ratio)
    if ratio is None or ratio<1.5:blockers.append('需同日量比≥1.5')
    bars=_v(price_bars)
    if bars is None or bars<85 or not _t(macd_status) or _t(macd_status)=='未驗證':
        blockers.append('需85根同股有效OHLCV與MACD來源證據')
    risk=severe_fall_reason(five_day=five_day)
    if risk:blockers.append('急跌隔離：'+risk)
    return ('研究共振條件已具備｜非主升確定、非H126買進授權；需T+1/5樣本外驗證'
            if not blockers else '不可認定主升起漲｜'+'；'.join(blockers))


def _read_table(ws: Any) -> list[dict[str,Any]]:
    if ws is None:return []
    head=[_t(c.value) for c in ws[1]]
    records=[]
    for row in ws.iter_rows(min_row=2,values_only=True):
        d={key:row[i] for i,key in enumerate(head) if key}
        if any(v is not None and _t(v) for v in d.values()):records.append(d)
    return records


def _lookup(wb: Any, tab: str) -> list[dict[str,Any]]:
    return _read_table(wb[tab]) if tab in wb.sheetnames else []


def build_evidence_review(rows: list[dict], summary: Mapping[str,Any], *, research_day: Any,
                          h126_buyable: int=0, h145_status: str='') -> dict:
    """No stock re-ranking: expose risk/coverage for each original research row."""
    rd=_date(research_day)
    checked=[];counts=Counter();seen=set()
    for rec in rows:
        code=_t(rec.get('股票代號'))
        if not code or code in seen:continue
        seen.add(code)
        tier=_t(rec.get('排名等級'))
        ranked = tier.startswith(('A','B','C')) and _v(rec.get('研究排名')) is not None
        ret5=rec.get('近5日漲幅%')
        if ret5 is None:ret5=rec.get('當前5日漲幅%')
        shock=severe_fall_reason(five_day=ret5,today=rec.get('當日漲跌幅%'))
        raw=_t(rec.get('原始PE_EPS狀態'))
        source=_t(rec.get('基本面來源狀態'))
        pe=_v(rec.get('本益比'));eps=_v(rec.get('近四季EPS'))
        yoy=_v(rec.get('營收年增率%'))
        valuation=bool(pe is not None and pe>0 and eps is not None and eps>0 and raw.startswith('官方'))
        growth=bool(valuation and yoy is not None and yoy>=15 and '營收:VERIFIED_ASOF' in source)
        margin=_t(rec.get('H1534三率三升狀態'))
        margin_break=_t(rec.get('H1534毛利營益突破'))
        margin_ok=margin.startswith('連兩季三率年增') and margin_break=='近五季新高'
        tdate=_date(rec.get('行情證據日') or rec.get('H114行情日期'))
        same=bool(rd and tdate==rd)
        stock=_v(rec.get('個股同日漲跌%'));mkt=_v(rec.get('大盤同日漲跌%'))
        rel=_v(rec.get('相對強度pp'))
        bench=_t(rec.get('對照指數'))
        mismatch=_t(rec.get('多日抗跌來源狀態'))
        valid_market=(same and stock is not None and mkt is not None and rel is not None
                      and abs(stock-mkt-rel)<=0.06 and bench not in ('','同日基準未證或不適用')
                      and '上櫃或市場未證' not in mismatch)
        anti_fall = (valid_market and mkt<=-1.5 and stock>=-0.5 and rel>=2)
        macd=_t(rec.get('H157_MACD'))
        rsi=_v(rec.get('H157_RSI14'))
        technical=bool(same and rsi is not None and macd and macd!='未驗證')
        grade='已核研究但非交易' if tier.startswith('A') else ('風控隔離' if tier.startswith('C') else
              '研究待補證' if tier.startswith('B') else ('W非交易核查' if tier.startswith('W') else '原研究待核'))
        if shock and ranked:grade='急跌隔離｜須先確認止跌（非預發動）'
        missing=[]
        if not valuation:missing.append('官方原始PE/EPS')
        if not growth:missing.append('營收公告與成長三率/估值交叉')
        if not margin_ok:missing.append('連兩季三率同比＋獲利突破')
        if not valid_market:missing.append('同日個股與正確市場指數')
        if not technical:missing.append('85根OHLCV＋RSI/MACD')
        if shock:missing.append(shock)
        flags=[]
        if growth: flags.append('官方成長初篩')
        if margin_ok:flags.append('三率×獲利突破')
        if anti_fall:flags.append('同日抗跌待續守')
        if technical:flags.append('RSI/MACD已計算')
        checked.append({'股票代號':code,'股票名稱':_t(rec.get('股票名稱')),
            '原研究排名':rec.get('研究排名'),'原研究分':rec.get('研究證據分'),
            'H154調整分':rec.get('H154研究排序分'), '來源等級':tier,
            '交易員證據分池':grade,'近5日漲跌%':_v(ret5),
            '同日指數與個股核實': '是' if valid_market else '否',
            '抗跌認證': '是｜仍非買進' if anti_fall else '否／資料待補',
            'PE/EPS＋公告營收': '已核初篩' if growth else '未雙核',
            '三率三升×獲利突破': '已核' if margin_ok else '未核',
            '技術性證據': '已核RSI/MACD' if technical else '未完整',
            '主升起漲': '不得認定｜缺多重共振' if not (growth and margin_ok and technical and not shock) else
                        '僅具研究共振，不代表已主升／須H126',
            '交易授權': '以H126逐股正式權限為準｜研究表不可下單',
            '證據缺口／反證': '；'.join(missing) or '完整性仍需H126與T+1驗證',
        })
        counts[grade]+=1
    total_rank=sum(_v(r.get('原研究排名')) is not None for r in checked)
    crash=[r['股票代號'] for r in checked if r['交易員證據分池'].startswith('急跌隔離')]
    warnings=[]
    if _v(summary.get('H153.1 H115原始PE/EPS帳本實際列數')) in (None,0):
        warnings.append('官方PE/EPS帳本0筆或未驗證；不得顯示任何低PE/高EPS已核榜')
    if not sum(r['同日指數與個股核實']=='是' for r in checked):
        warnings.append('本輪沒有通過正確市場基準的同日抗跌證據')
    if crash:warnings.append('發現急跌但原研究仍具名：'+','.join(crash)+'；建議上游預發動極端跌勢隔離')
    if int(h126_buyable or 0)==0:warnings.append('H126可買進0檔；研究表不可下單')
    if _t(h145_status)=='SUPPLIED_FULL_SCAN_MATCHED':
        warnings.append('H145 supplied 名單一致僅為局部形式對帳，不等於全市場與官方因子認證')
    return {'study_day':rd,'rows':checked,'ranked':total_rank,'checked':len(checked),
        'risk_crash_codes':crash,'counts':dict(counts),'warnings':warnings,
        'h126_buyable':int(h126_buyable or 0),
        'verified_growth':sum(r['PE/EPS＋公告營收']=='已核初篩' for r in checked),
        'verified_margin':sum(r['三率三升×獲利突破']=='已核' for r in checked),
        'verified_antifall':sum(r['抗跌認證'].startswith('是') for r in checked),
        'summary':dict(summary)}


def append_h165_review(wb: Any, *, research_day: Any, h145_status: str='', h126_buyable: int=0) -> dict:
    """Append ONLY below H164 existing navigation; preserve original 14 sheets."""
    from openpyxl.styles import Alignment, Font, PatternFill
    name='11_交易員重點導航'
    if name not in wb.sheetnames:raise ValueError('H165缺11導航表；不可覆蓋原表')
    ws=wb[name]
    if any('H165' in _t(ws.cell(r,1).value) for r in range(1,ws.max_row+1)):
        raise ValueError('H165已寫入；禁止重複追加')
    if '01_具名強勢研究' not in wb.sheetnames or '02_研究召回統計' not in wb.sheetnames:
        raise ValueError('H165缺具名研究或召回稽核表；禁止假造報表')
    original=_lookup(wb,'01_具名強勢研究')
    summ={_t(r.get('檢查項目')):r.get('結果') for r in _lookup(wb,'02_研究召回統計')}
    # H113 pool size lives in the independent 09 quality ledger, not in 02.
    # Do not fill missing counts from estimates or by adding different pools.
    for r in _lookup(wb,'09_資料來源品質'):
        key=_t(r.get('檢查項目'))
        if key and key not in summ:summ[key]=r.get('結果')
    review=build_evidence_review(original,summ,research_day=research_day,
        h126_buyable=h126_buyable,h145_status=h145_status)
    start=ws.max_row+3
    ws.cell(start,1,'H165｜證據正確性與主升起漲反向驗證（不改原排名）')
    ws.cell(start,2,'研究日'+review['study_day']+'｜H126 '+str(review['h126_buyable'])+'檔｜'
            '極端跌勢隔離'+str(len(review['risk_crash_codes']))+'檔｜不得用證據分當隔日機率')
    ws.cell(start,1).fill=PatternFill('solid',fgColor='17324D')
    ws.cell(start,1).font=Font(name='Microsoft JhengHei',bold=True,color='FFFFFF')
    ws.cell(start,2).font=Font(name='Microsoft JhengHei',bold=True,color='B91C1C')
    ws.row_dimensions[start].height=33
    lines=[
        ('市場與研究池','診斷'+str(summ.get('診斷母體原始列','?'))+'｜H113稽核'+
         str(summ.get('H113稽核原始列數','?'))+'｜有名次'+str(review['ranked'])+
         '｜本表逐股'+str(review['checked'])+'；口徑不得互相加總'),
        ('官方PE/EPS帳本','本輪'+str(summ.get('H153.1 H115原始PE/EPS帳本實際列數','未驗證'))+
         '筆；無估值/公告時點不得自稱低本益比、高EPS'),
        ('H153三率與營收','正式初篩成長'+str(review['verified_growth'])+'檔；三率獲利雙認'+str(review['verified_margin'])+
         '檔；季度單季同比、財報公告日和近五季必備'),
        ('市場抗跌','同日且正確指數已核'+str(review['verified_antifall'])+
         '檔；上市／上櫃不可互代，不能以大盤昨日跌幅認證今日股票'),
        ('急跌隔離',','.join(review['risk_crash_codes']) or '無；但缺5日行情者仍需補證'),
        ('H145真正意義',_t(h145_status)+'：名單與供給一致不代表交易權威、H115補證或全市場覆蓋'),
        ('隔日方向與主升','只保留續守／假突破／再破低三情境；RSI/MACD/ADX/量能與基本面要同日核實；無校準不報百分比'),
        ('技術/基本面來源','https://www.fidelity.com/learning-center/trading-investing/technical-analysis｜'
         'https://www.twse.com.tw/｜https://www.tpex.org.tw/｜https://mops.twse.com.tw/'),
    ]
    for i,(label,msg) in enumerate(lines,start+1):
        ws.cell(i,1,label);ws.cell(i,2,msg)
        ws.cell(i,1).font=Font(name='Microsoft JhengHei',bold=True,color='0F3654')
        ws.cell(i,2).alignment=Alignment(wrap_text=True,vertical='center')
        ws.row_dimensions[i].height=45 if i==start+len(lines) else 32
    hstart=start+len(lines)+2
    keys=list(review['rows'][0]) if review['rows'] else ['股票代號','交易員證據分池']
    for c,k in enumerate(keys,1):
        ws.cell(hstart,c,k)
        ws.cell(hstart,c).fill=PatternFill('solid',fgColor='0B6E75')
        ws.cell(hstart,c).font=Font(name='Microsoft JhengHei',bold=True,color='FFFFFF')
    ws.row_dimensions[hstart].height=32
    for i,rec in enumerate(review['rows'],hstart+1):
        for j,k in enumerate(keys,1):
            c=ws.cell(i,j,rec.get(k));c.alignment=Alignment(wrap_text=j>=6,vertical='center')
            if i%2==0:c.fill=PatternFill('solid',fgColor='F1F7F9')
            if k=='交易員證據分池' and _t(rec.get(k)).startswith('急跌隔離'):
                c.fill=PatternFill('solid',fgColor='FEE2E2')
                c.font=Font(name='Microsoft JhengHei',color='991B1B',bold=True)
        ws.row_dimensions[i].height=42
    # Preserve existing Sheet11 columns A/B for H157 and H164; limit appended widths.
    for j in range(10,len(keys)+1):
        from openpyxl.utils import get_column_letter
        ws.column_dimensions[get_column_letter(j)].width=min(45,29 if j<16 else 45)
    return {'start_row':start,'table_row':hstart,'checked':review['checked'],
            'ranked':review['ranked'],'warnings':review['warnings'],
            'risk_crash_codes':review['risk_crash_codes']}

__all__=['VERSION','CRASH_5D_PCT','severe_fall_reason','leader_start_gate',
         'build_evidence_review','append_h165_review']
