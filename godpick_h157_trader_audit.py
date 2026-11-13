# -*- coding: utf-8 -*-
"""V191-H157: evidence-first trading RESEARCH overview for an existing H154 Excel.

No network, side effects, recommendations, H109/H126 permission changes, price
fabrication, or hidden scores. All conclusions are as-of the same workbook's
study date. Each research pool remains separately countable and auditable.
"""
from __future__ import annotations
from collections import Counter
from datetime import date, datetime, timedelta
from math import isfinite
from typing import Any

VERSION = 'V191-H157-TRADER-AUDIT'
OVERVIEW = '11_交易員重點導航'
DETAIL = '12_逐股分類與反證'


def _s(x: Any) -> str:
    return str(x or '').strip()


def _n(x: Any) -> float | None:
    if x is None or isinstance(x, bool):return None
    try:
        v=float(str(x).strip().replace('%','').replace(',',''))
        return v if isfinite(v) else None
    except (ValueError, TypeError, OverflowError):return None


def _day(x: Any) -> date | None:
    if isinstance(x,datetime):return x.date()
    if isinstance(x,date):return x
    try:return date.fromisoformat(str(x).strip()[:10])
    except (TypeError,ValueError,OverflowError):return None


def _workday(d: date | None) -> str:
    if not d:return '無有效研究日期'
    nxt=d+timedelta(days=1)
    while nxt.weekday()>=5:nxt+=timedelta(days=1)
    return f'{nxt.isoformat()}（僅跳過週末，假日依交易所日曆）'


def _code(x: Any) -> str:
    if x is None or isinstance(x,bool):return ''
    s=str(x).strip().replace('.0','') if str(x).strip().endswith('.0') else str(x).strip()
    return s.zfill(4) if s.isdigit() and len(s)<=4 else ''


def _sheet_records(wb: Any, name: str) -> list[dict]:
    if name not in wb.sheetnames:return []
    ws=wb[name]
    heads=[_s(x.value) for x in ws[1]]
    records=[]
    for cells in ws.iter_rows(min_row=2,values_only=True):
        obj={key:cells[i] for i,key in enumerate(heads) if key}
        if _code(obj.get('股票代號')):records.append(obj)
    return records


def analyze_research_rows(rows: list[dict], *, research_day: Any,
                          h145_status: str = '', h126_buyable: int=0,
                          authorized_codes: Any=None) -> dict:
    """Deterministic current-snapshot fact ledger, never a trading optimizer."""
    day=_day(research_day)
    authorized={_code(v) for v in (authorized_codes or ())}-{''}
    groups=Counter();seen=set();records=[];excluded=0
    for r in rows:
        code=_code(r.get('股票代號'))
        if not code:continue
        if code in seen:
            excluded+=1
            continue  # do not choose a more favorable duplicate observation
        seen.add(code)
        tier=_s(r.get('排名等級'))
        if tier.startswith('A'):group='A｜已核研究'
        elif tier.startswith('B'):group='B｜待補證研究'
        elif tier.startswith('C'):group='C｜風控隔離'
        elif tier.startswith('W'):group='W｜非交易核查'
        else:group='原研究｜待核'
        groups[group]+=1
        price_day=_day(r.get('H114行情日期') or r.get('行情證據日') or r.get('研究交易日'))
        same_day=bool(day and price_day==day)
        market=_n(r.get('大盤同日漲跌%')); stock=_n(r.get('個股同日漲跌%'))
        relative=_n(r.get('相對強度pp'))
        market_label=_s(r.get('對照指數'))
        valid_relative=(same_day and market is not None and stock is not None
            and relative is not None and abs((stock-market)-relative)<=0.06
            and market_label not in {'','同日基準未證或不適用'})
        if valid_relative and market<=-1.5 and stock>=-0.5 and relative>=2:
            if stock>=8:resilience='逆勢大漲｜高位追價警示'
            elif stock>0:resilience='逆勢收紅｜需確認續強'
            else:resilience='相對抗跌｜尚未翻紅'
        elif valid_relative:resilience='尚未符合跌市抗跌條件'
        else:resilience='同日大盤／個股／比較值未充分核實'
        pe=_n(r.get('本益比'));eps=_n(r.get('近四季EPS'));yoy=_n(r.get('營收年增率%'))
        raw_status=_s(r.get('原始PE_EPS狀態'))
        pe_eps_ok=bool(pe is not None and pe>0 and eps is not None and eps>0
            and raw_status.startswith('官方'))
        revenue_ok=bool(yoy is not None and '營收:VERIFIED_ASOF' in _s(r.get('基本面來源狀態')))
        record=_s(r.get('營收創高'))
        record_ok=revenue_ok and record=='近12月新高'
        if pe_eps_ok and revenue_ok:
            growth=('已核PE/EPS＋已公告營收；'+
                ('PE≤15、EPS≥5、營收YoY≥15%之初篩' if pe<=15 and eps>=5 and yoy>=15
                  else '未同時符合PE≤15／EPS≥5／營收YoY≥15%初篩'))
        else:growth='官方PE/EPS或逐月營收證據不足｜不可列為已認證高成長'
        if record_ok:growth+='｜已公告近12月營收新高'
        if _s(r.get('營收預估創高判定')).startswith('模型情境可能創高'):
            growth+='｜模型可能創高（非已公告／不可加買進分）'
        three=_s(r.get('H1534三率三升狀態'))
        breakout=_s(r.get('H1534毛利營益突破'))
        three_ok=three.startswith('連兩季三率年增') and breakout=='近五季新高'
        three_label=('已核三率連兩季同比改善＋毛利／營益突破（非股價突破）'
                     if three_ok else '三率×獲利突破尚未同時認證')
        golden=_s(r.get('黃金交叉'));box=_s(r.get('箱型型態'))
        tech=[]
        if golden.startswith('確認'):tech.append('MA黃金交叉（內部訊號）')
        if box.startswith('確認突破'):tech.append('箱型突破（需量價及後續站穩）')
        if box.startswith('接近箱頂'):tech.append('接近箱頂（尚未突破）')
        if _s(r.get('H1532布林收斂')).startswith('是｜'):tech.append('布林帶收斂（方向未定）')
        if _s(r.get('H1532DMI方向')).startswith('空向'):tech.append('DMI空向反證')
        if _n(r.get('H157_RSI14')) is not None:tech.append('RSI14已計算（需看數值）')
        if _s(r.get('H157_MACD')) not in ('','未驗證'):tech.append('MACD已計算（非獨立買進）')
        tech_label='；'.join(tech) if same_day and tech else ('技術訊號未驗證／缺同日來源' if not same_day else '尚無確認型態')
        if same_day and stock is not None and stock>=8:
            warning='單日漲幅≥8%：即使抗跌也不可當作未大漲；防追高／隔日開高走低'
        elif not same_day:warning='研究日與股價資料不一致／來源未驗證；不得據此推斷下一交易日'
        else:warning='抗跌不保證續漲；需次日量價與風險失效點'
        if valid_relative and stock is not None and stock>=8:
            direction='下一交易日：強勢延續或沖高回落雙情境｜不宜追價'
        elif valid_relative and resilience.startswith(('逆勢','相對')):
            direction='下一交易日：觀察是否守住前低與放量續強｜方向未校準'
        elif same_day and (golden.startswith('確認') or box.startswith('確認突破')):
            direction='下一交易日：突破續守／假突破雙情境｜方向未校準'
        else:direction='下一交易日：缺足夠方向優勢，觀望補證｜方向未校準'
        if _s(h145_status).startswith('BLOCKED_'):
            trade_status='H145完整性阻擋｜研究表不可下單'
        elif code in authorized:
            trade_status='H126本輪已授權（仍須查看正式H126逐股條件）'
        elif int(h126_buyable or 0)>0:
            trade_status='未出現在本輪H126允許名單｜不可依研究表下單'
        else:trade_status='未授權／禁止以研究表下單'
        records.append({
            '股票代號':code,'股票名稱':_s(r.get('股票名稱')),'來源股票池':group,
            'H153研究排名':r.get('研究排名'),'原研究證據分':r.get('研究證據分'),
            '研究資料日':price_day.isoformat() if price_day else '未驗證',
            '同日價量': '內部同日K標記（非獨立交易所認證）' if same_day else '缺同日可用K',
            '大盤漲跌%':market if valid_relative else None,
            '個股漲跌%':stock if valid_relative else None,
            '相對強度pp':relative if valid_relative else None,
            '跌市抗跌':resilience,'成長價值證據':growth,'三率×獲利突破':three_label,
            '技術型態與反證':tech_label,
            'RSI14':_n(r.get('H157_RSI14')),'MACD狀態':_s(r.get('H157_MACD')) or '未驗證',
            'ATR14波動%':_n(r.get('H157_ATR14_PCT')),
            '量價突破':_s(r.get('H157_VOLUME_BREAKOUT')) or '未驗證',
            '下一交易日觀察情境':direction,'風險／盲點':warning,
            'H126正式交易資格':trade_status,
            '證據來源提醒':'以同輪研究表為準；需再核證交所/櫃買/MOPS，無來源不得擅自補值',
        })
    return {'rows':records,'groups':dict(groups),'duplicated':excluded,
            'study_day':day.isoformat() if day else '未驗證',
            'next_workday':_workday(day),'h126_buyable':max(0,int(h126_buyable or 0)),
            'h145_status':_s(h145_status),'h126_authorized_codes':len(authorized),
            'anti_fall':sum('逆勢' in r['跌市抗跌'] or r['跌市抗跌'].startswith('相對抗跌') for r in records),
            'chase_risk':sum('高位追價警示' in r['跌市抗跌'] for r in records),
            'growth_verified':sum(r['成長價值證據'].startswith('已核PE/EPS') for r in records),
            'margin_verified':sum(r['三率×獲利突破'].startswith('已核三率') for r in records),
            'valid_comparison':sum(r['大盤漲跌%'] is not None for r in records)}


def render_trader_audit(wb: Any, *, research_day: Any, h145_status: str='',
                        h126_buyable: int=0, run_id: str='') -> dict:
    """Append two evidence-only tabs. Does not reorder or modify 12 legacy tabs."""
    from openpyxl.styles import Font, PatternFill, Alignment
    from openpyxl.utils import get_column_letter
    for name in (OVERVIEW,DETAIL):
        if name in wb.sheetnames: raise ValueError('H157禁止覆蓋已有工作表：'+name)
    records=_sheet_records(wb,'01_具名強勢研究')
    # H126 is the ONLY source of formal permissions; do not infer from research
    # grade. The caller's aggregate is merely an independent audit count.
    auth_rows=_sheet_records(wb,'00_H126主管決策')
    authorized={_code(r.get('股票代號')) for r in auth_rows
        if _s(r.get('H126能否買進')) in ('可以買進','允許買進','是')}
    authorized.discard('')
    result=analyze_research_rows(records,research_day=research_day,
        h145_status=h145_status,h126_buyable=h126_buyable,
        authorized_codes=authorized)
    o=wb.create_sheet(OVERVIEW)
    matrix=[
        ('H157｜交易員十項審核 × 真實資料門檻','本表僅研究／分池稽核；非交易授權'),
        ('研究交易日',result['study_day']),('下一交易工作日（暫估）',result['next_workday']),
        ('同輪識別',_s(run_id) or '未獨立核驗；禁止當成另一輪結果'),
        ('H145完整性',result['h145_status'] or '缺檢查結論；禁止認定完整'),
        ('正式授權H126',f"{result['h126_buyable']} 檔；仍須以00_H126主管決策為唯一權威"),
        ('H126實際允許代號',f"{len(authorized)} 檔，與報表申報數{'一致' if len(authorized)==result['h126_buyable'] else '不一致，需人工稽核'}；若H145阻擋仍不可下單"),
        ('01具名唯一股票',len(result['rows'])),
        ('A已核研究',result['groups'].get('A｜已核研究',0)),
        ('B待補證研究',result['groups'].get('B｜待補證研究',0)),
        ('W非交易核查',result['groups'].get('W｜非交易核查',0)),
        ('原始研究待核',result['groups'].get('原研究｜待核',0)),
        ('C風控隔離',result['groups'].get('C｜風控隔離',0)),
        ('同代號重複已隔離',result['duplicated']),
        ('可核對同日指數差額',result['valid_comparison']),
        ('跌市抗跌研究線索',result['anti_fall']),
        ('其中單日強漲≥8%追價警示',result['chase_risk']),
        ('官方PE/EPS與營收雙來源已核',result['growth_verified']),
        ('三率同比連升＋獲利突破雙認證',result['margin_verified']),
        ('推薦資料是否可直接用來買進','不可：本表不取代H126；H145阻擋時須先修輸出完整性'),
        ('研究分數不是上漲機率','沒有樣本外校準、真實交易成本及T+1結果，不能聲稱隔日勝率'),
        ('低本益比／高EPS判讀','需官方PE/EPS同日；PE≤15、EPS≥5為篩選假設，必須再做產業比較'),
        ('營收創高判讀','需有公告日及12～24個月可比較月營收；預測創高只可標示模型情境'),
        ('三率三升判讀','官方單季毛利／營益／淨利率，本季與前季各高於去年同季；另看五季突破'),
        ('抗跌但高漲幅','當日守住不等於後續安全；≥8%股價優先檢查隔日追價與假突破'),
        ('技術指標新增建議','RSI14、MACD(12,26,9)、ATR14、20日放量突破；只在85根同股有效OHLCV下運算'),
        ('前視偏差防護','不得使用研究日之後公告的營收、財報、法人、股價或隔日報酬重算當日排名'),
        ('來源核查連結','https://www.twse.com.tw/；https://www.tpex.org.tw/；https://mops.twse.com.tw/'),
        ('技術指標參考','https://www.fidelity.com/learning-center/trading-investing/technical-analysis'),
        ('讀表順序','11重點→12逐股→01原始研究→02稽核→00_H126正式資格'),
    ]
    for row in matrix:o.append(row)
    d=wb.create_sheet(DETAIL)
    keys=list(result['rows'][0]) if result['rows'] else ['股票代號','股票名稱','來源股票池','H126正式交易資格']
    d.append(keys)
    for row in result['rows']:d.append([row.get(k) for k in keys])
    d.freeze_panes='D2';o.freeze_panes='B2'
    if len(result['rows'])>0:d.auto_filter.ref=d.dimensions
    for ws in (o,d):
        ws.sheet_view.showGridLines=False
        for cell in ws[1]:
            cell.font=Font(name='Microsoft JhengHei',bold=True,color='FFFFFF',size=11)
            cell.fill=PatternFill('solid',fgColor='14345A')
            cell.alignment=Alignment(vertical='center',wrap_text=True)
        ws.row_dimensions[1].height=34
    o.column_dimensions['A'].width=38;o.column_dimensions['B'].width=94
    for i in range(2,len(matrix)+1):
        o.cell(i,1).font=Font(name='Microsoft JhengHei',bold=True,color='14345A')
        o.cell(i,2).alignment=Alignment(wrap_text=True,vertical='center')
        o.row_dimensions[i].height=30 if i<20 else 38
        if i in (5,6,19):o.cell(i,2).fill=PatternFill('solid',fgColor='FCE7EA')
    for col,k in enumerate(keys,1):
        width=15 if col<=7 else 20
        if k in ('跌市抗跌','三率×獲利突破','技術型態與反證','下一交易日觀察情境'):width=36
        if k in ('成長價值證據','風險／盲點','證據來源提醒'):width=58
        d.column_dimensions[get_column_letter(col)].width=width
    for row in d.iter_rows(min_row=2):
        d.row_dimensions[row[0].row].height=44
        for cell in row:
            cell.alignment=Alignment(vertical='center',wrap_text=True)
        if '高位追價警示' in str(row[keys.index('跌市抗跌')].value or ''):
            row[keys.index('跌市抗跌')].fill=PatternFill('solid',fgColor='FCE7EA')
    return result


__all__=['VERSION','OVERVIEW','DETAIL','analyze_research_rows','render_trader_audit']
