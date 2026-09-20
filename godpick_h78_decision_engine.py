"""H78: opportunity selection and executable price plans, with explicit evidence.

No trained-model or probability claim. Fixed initial policy, auditable inputs.
Missing optional TDCC history does not veto a price/sector/flow opportunity.
Execution still needs legacy authority and a valid cost-adjusted price plan.
"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import date, datetime, timezone, timedelta
import hashlib
import json
import math
import re
import pandas as pd

VERSION = 'v191_h78_evidence_contract_decision_rebuild_20260920'

@dataclass(frozen=True)
class Policy:
    min_score: float = 65
    min_strength: float = 60
    min_sector: float = 55
    min_amount_million: float = 30
    min_net_rr: float = 1.5
    max_stop_pct: float = 8
    commission: float = .001425  # configurable assumption; actual broker rate varies
    sell_tax: float = .003       # ordinary Taiwan stock, non-day-trade
    slippage: float = .001       # assumption per side, NOT observed liquidity
    max_rows: int = 8
    max_per_sector: int = 2

    def __post_init__(self):
        for key, value in vars(self).items():
            if isinstance(value, bool) or not math.isfinite(float(value)) or value < 0:
                raise ValueError('invalid policy: ' + key)
        if self.max_rows < 1 or self.max_per_sector < 1 or self.min_net_rr <= 0:
            raise ValueError('positive limits required')
        if int(self.max_rows) != self.max_rows or int(self.max_per_sector) != self.max_per_sector:
            raise ValueError('integer row limits required')
        if any(x >= .1 for x in (self.commission, self.sell_tax, self.slippage)):
            raise ValueError('cost rates must be decimal fractions')

def num(value):
    try:
        if isinstance(value, bool):
            return None
        v = float(str(value).replace(',', '').rstrip('%％'))
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None

def first(row, *keys):
    for key in keys:
        v = num(row.get(key))
        if v is not None:
            return v
    return None

def text(value):
    if value is None or str(value).lower() in {'nan', 'none', '<na>', 'nat'}:
        return ''
    return str(value).strip()

def day(value):
    t = text(value)
    if re.fullmatch(r'\d{8}', t):
        t = t[:4] + '-' + t[4:6] + '-' + t[6:]
    try:
        return date.fromisoformat(t[:10])
    except ValueError:
        return None

def clip(v):
    return max(0., min(100., v)) if v is not None else None

def price_plan(row, policy=Policy()):
    # Structural stop only. Never substitute the tighter holding/trigger level
    # solely to increase RR; never increase targets to manufacture a trade.
    entry = first(row, '主要進場參考價', '實戰觸發價')
    stop = first(row, '停損參考', 'SuperAI動態停損價')
    target = first(row, '第一壓力價', 'SuperAI第一減碼價')
    result = {'H78計畫進場': entry, 'H78結構停損': stop, 'H78第一目標': target,
              'H78成本後RR': None, 'H78停損距離%': None, 'H78價格上限試算': None,
              'H78計畫狀態': '缺價格結構'}
    if any(v is None or v <= 0 for v in (entry, stop, target)):
        return result
    # Algebraic ceiling is NOT support, a new limit order or a forecast.
    # Below-stop ceilings imply the whole structure is invalid.
    sell = (1 - policy.slippage) * (1 - policy.commission - policy.sell_tax)
    ceiling = sell * (target + policy.min_net_rr * stop) / ((1 + policy.min_net_rr) * (1 + policy.slippage) * (1 + policy.commission))
    if stop < ceiling < target:
        result['H78價格上限試算'] = round(ceiling, 4)
    if not stop < entry < target:
        result['H78計畫狀態'] = '價格結構不成立：須停損＜進場＜第一目標'
        return result
    buy = entry * (1 + policy.slippage) * (1 + policy.commission)
    risk = buy - stop * sell
    reward = target * sell - buy
    rr = reward / risk
    distance = (entry - stop) / entry * 100
    result.update({'H78成本後RR': round(rr, 4), 'H78停損距離%': round(distance, 4),
                   'H78計畫狀態': 'PASS' if rr >= policy.min_net_rr and distance <= policy.max_stop_pct
                   else '等待合理買點：成本後RR或停損距離不合格'})
    return result

def evaluate(frame, *, as_of=None, policy=Policy()):
    """Pure evaluation over the full supplied pool, before top-N selection.

    as_of is an explicit historical replay date or today's Taiwan calendar date.
    A <=4 calendar-day window accommodates weekends, but also requires the row's
    quote date to equal the upstream common market date. This is not an exchange
    calendar and does not authorize long-holiday live execution.
    """
    if frame is None or frame.empty:
        return pd.DataFrame()
    if frame.columns.duplicated().any():
        raise ValueError('duplicate input columns')
    now = day(as_of) if as_of is not None else datetime.now(timezone(timedelta(hours=8))).date()
    if now is None:
        raise ValueError('invalid as_of')
    rows = []
    for raw in frame.to_dict('records'):
        code = text(raw.get('股票代號'))
        if re.fullmatch(r'\d+\.0', code):
            code = code[:-2]
        if not code:
            continue
        missing, data_issues, evidence = [], [], []
        quote = day(raw.get('K線最後交易日'))
        anchor = day(raw.get('本輪市場最新交易日'))
        official = day(raw.get('官方因子資料日期'))
        if quote is None or anchor is None:
            data_issues.append('缺K線/市場日期')
        elif quote != anchor or not 0 <= (now - quote).days <= 4:
            data_issues.append('K線過期、未對齊或未來日期')
        if official is None or quote is None or not 0 <= (quote - official).days <= 4:
            data_issues.append('官方日期缺失、過期或未來')
        fresh = text(raw.get('股神資料總新鮮度'))
        if not fresh.startswith('READY'):
            data_issues.append('上游資料新鮮度未READY')
        price = first(raw, '最新價')
        if price is None or price <= 0:
            data_issues.append('缺有效現價')
        liquidity = first(raw, '20日均成交額百萬')
        if liquidity is None:
            missing.append('20日均成交額')
        elif liquidity < policy.min_amount_million:
            data_issues.append('流動性不足')
        strength = clip(first(raw, 'H47個股相對強度分'))
        sector = clip(first(raw, 'H53族群共振分'))
        breadth = clip(first(raw, 'H53族群廣度分'))
        vol = first(raw, '當日量比')
        i1 = first(raw, '三大法人近1日合計')
        i3 = first(raw, '三大法人近3日合計')
        momentum = first(raw, '3日動能加速度百分點')
        acceleration = first(raw, '成交額3日加速度%')
        if acceleration is None:
            acceleration = first(raw, '成交量3日加速度%')
        holder = first(raw, 'TDCC千張大戶週變化pp')
        hd, hp = day(raw.get('TDCC大戶資料日期')), day(raw.get('TDCC大戶前期日期'))
        holder_ok = (holder is not None and hd is not None and hp is not None
                     and quote is not None and hp < hd <= quote and (quote - hd).days <= 14)
        if momentum is None:
            missing.append('3日動能增量')
        elif momentum >= .5:
            evidence.append('動能加速')
        if acceleration is None:
            missing.append('3日量額增量')
        elif acceleration >= 8:
            evidence.append('量額加速')
        if not holder_ok:
            missing.append('TDCC前後期增量')
        elif holder > 0:
            evidence.append('TDCC增持')
        inst = None
        if i1 is not None and i3 is not None:
            prior_daily = (i3 - i1) / 2  # exclude today from comparison
            inst = 75. if i1 > 0 and i3 > 0 else 25.
            if i1 > 0 and i1 > prior_daily:
                inst = 90.
                evidence.append('法人流速改善')
        capital = clip(50 + (vol - 1) * 20) if vol is not None else None
        factors = {'相對強度': (strength, .30), '族群共振': (sector, .25),
                   '族群廣度': (breadth, .15), '量能參與': (capital, .15), '法人方向': (inst, .15)}
        covered = sum(weight for value, weight in factors.values() if value is not None)
        score = sum(value * weight for value, weight in factors.values() if value is not None) / covered if covered else 0.
        for key, (v, _) in factors.items():
            if v is None:
                missing.append(key)
        r1, r5, gap = first(raw, '今日漲幅%'), first(raw, '近5日漲幅%'), first(raw, '收盤距MA20%')
        if r1 is None or r5 is None:
            data_issues.append('缺短線漲幅')
        if gap is None:
            missing.append('MA20乖離')
        hot = (r1 is not None and r1 >= 7) or (r5 is not None and r5 >= 12) or (gap is not None and gap >= 12)
        extreme = (r1 is not None and r1 >= 9.5) or (r5 is not None and r5 >= 18) or (gap is not None and gap >= 20)
        strong = strength is not None and strength >= policy.min_strength
        mainstream = sector is not None and sector >= policy.min_sector and breadth is not None and breadth >= 50
        flow = (capital is not None and capital >= 55) or (inst is not None and inst >= 75)
        selected = (not data_issues and liquidity is not None and covered >= .85 and strong and mainstream and flow
                    and score >= policy.min_score and not extreme)
        if data_issues:
            status = '資料待修復'
        elif selected:
            status = '推薦研究｜等待拉回' if hot else '推薦研究｜條件追蹤'
        else:
            status = '高熱等待' if extreme else '未入選'
        reasons = list(data_issues)
        if not strong: reasons.append('相對強度未達標')
        if not mainstream: reasons.append('族群共振/廣度未達標')
        if not flow: reasons.append('量能/法人方向未確認')
        if covered < .85: reasons.append('核心證據覆蓋不足')
        if liquidity is None: reasons.append('缺流動性依據')
        if score < policy.min_score: reasons.append('綜合機會分不足')
        if extreme: reasons.append('短線過熱')
        plan = price_plan(raw, policy)
        legacy = text(raw.get('H64有效權威')) == 'EFFECTIVE-FORMAL' and text(raw.get('H68次日執行狀態')).startswith('READY-COND')
        execution = '等待上游交易授權'
        if data_issues: execution = '禁止：先修復資料'
        elif not selected: execution = '禁止：未入選'
        elif plan['H78計畫狀態'] != 'PASS': execution = '等待：' + plan['H78計畫狀態']
        elif hot: execution = '等待：拉回重算計畫'
        elif gap is None: execution = '等待：補齊MA20乖離'
        elif legacy: execution = '條件可執行：盤前重驗、觸發及守價後'
        out = dict(raw)
        out.update({'股票代號': code, 'H78版本': VERSION, 'H78推薦狀態': status,
                    'H78機會分': round(score, 2), 'H78核心覆蓋%': round(covered * 100, 2),
                    'H78有效增量數': len(evidence), 'H78有效增量': '；'.join(evidence) or '無已驗證增量',
                    'H78缺資料': '；'.join(missing), 'H78未入選原因': '；'.join(reasons),
                    'H78推薦理由': '；'.join(f'{k}={v:.1f}' if v is not None else f'{k}=缺資料' for k,(v,_) in factors.items()),
                    'H78交易狀態': execution, 'H78資料基準日': quote.isoformat() if quote else '',
                    'H78評估日': now.isoformat(), 'H78選股政策': '固定初始政策，未經歷史外樣本驗證',
                    'H78範圍': '僅本次輸入候選池，非全市場績效證明', **plan})
        payload = {k: text(v) for k,v in out.items() if (k.startswith('H78') and k != 'H78決策指紋') or k == '股票代號'}
        payload['policy'] = vars(policy)
        out['H78決策指紋'] = hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode()).hexdigest()[:20]
        rows.append(out)
    if not rows:
        return pd.DataFrame()
    result = pd.DataFrame(rows)
    # Duplicate symbols are a source error; don't silently choose a bullish row.
    duplicates = result['股票代號'].duplicated(keep=False)
    result.loc[duplicates, 'H78推薦狀態'] = '資料待修復'
    result.loc[duplicates, 'H78交易狀態'] = '禁止：重複股票代號'
    result.loc[duplicates, 'H78未入選原因'] = '重複股票代號'
    for i in result.index[duplicates]:
        result.at[i, 'H78決策指紋'] = hashlib.sha256((result.at[i, 'H78決策指紋'] + '|duplicate').encode()).hexdigest()[:20]
    return result

DISPLAY = ['股票代號','股票名稱','類別','H78推薦狀態','H78機會分','H78推薦理由','H78交易狀態',
           'H78計畫進場','H78結構停損','H78第一目標','H78成本後RR','H78停損距離%','H78價格上限試算',
           'H78有效增量','H78缺資料','H78未入選原因','H78資料基準日','H78決策指紋']

def build_tables(frame, *, as_of=None, policy=Policy()):
    work = evaluate(frame, as_of=as_of, policy=policy)
    if work.empty:
        empty = pd.DataFrame({'結論':['沒有輸入候選資料；請重新掃描。']})
        return {'recommendations':empty, 'waiting':empty, 'audit':empty, 'health':empty}
    ranked = work.sort_values(['H78機會分','股票代號'], ascending=[False,True],kind='mergesort')
    pool = ranked[ranked['H78推薦狀態'].str.startswith('推薦研究')]
    chosen, counts = [], {}
    for idx, row in pool.iterrows():
        sector = text(row.get('類別')) or '未分類'
        if counts.get(sector, 0) >= policy.max_per_sector: continue
        chosen.append(idx); counts[sector] = counts.get(sector,0) + 1
        if len(chosen) >= policy.max_rows: break
    cols = [c for c in DISPLAY if c in work]
    recommended = ranked.loc[chosen, cols].reset_index(drop=True)
    if recommended.empty:
        recommended = pd.DataFrame({'結論':['沒有候選通過本輪選股政策；詳見完整淘汰原因及資料健康。']})
    waiting = ranked.loc[~ranked.index.isin(chosen),cols].head(20).reset_index(drop=True)
    health = [{'項目':'輸入候選數','數值':len(work)}, {'項目':'符合選股政策','數值':len(pool)},
              {'項目':'分散後推薦研究','數值':len(chosen)},
              {'項目':'條件可執行','數值':int(work.loc[chosen,'H78交易狀態'].str.startswith('條件可執行').sum())},
              {'項目':'資料待修復','數值':int(work['H78推薦狀態'].eq('資料待修復').sum())},
              {'項目':'學習狀態','數值':'固定政策未訓練；需逐日全市場快照及成熟標籤做外樣本檢驗'},
              {'項目':'成本假設','數值':f'單邊手續費{policy.commission:.4%}、賣出稅{policy.sell_tax:.2%}、單邊滑價{policy.slippage:.2%}'},
              {'項目':'缺資料處理','數值':'缺值不補0/50；TDCC/增量非所有策略的共同硬門檻'},
              {'項目':'價格上限試算','數值':'固定原停損/第一目標反解RR；不是支撐價，不是掛單價，須有實際止穩證據'},
              {'項目':'執行規則','數值':'推薦研究不等於買進；H64/H68及成本後價格計畫仍須通過'}]
    for key in ['3日動能加速度百分點','成交額3日加速度%','TDCC千張大戶週變化pp']:
        count = sum(num(v) is not None for v in work.get(key,pd.Series(dtype=object)))
        health.append({'項目':key+'覆蓋','數值':f'{count}/{len(work)}'})
    return {'recommendations':recommended,'waiting':waiting,'audit':work[cols].reset_index(drop=True),'health':pd.DataFrame(health)}
