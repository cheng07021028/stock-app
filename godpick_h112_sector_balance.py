# -*- coding: utf-8 -*-
"""H112: Anti-monopoly and cross-sector audit radar.

This module is strictly RESEARCH-ONLY.  The audit evidence pool may furnish
observation candidates but it cannot create Formal, alter existing execution
rights, bypass H109 vetoes, or overwrite the legacy H101/H102 decision truth.
"""
from __future__ import annotations

from collections import Counter
from typing import Any
import pandas as pd

VERSION = "v191_h112_sector_balance_audit_radar_20261012"


def _number(value: Any) -> float | None:
    try:
        x = float(str(value).replace(',', '').strip())
        return x if pd.notna(x) and x != float('inf') and x != float('-inf') else None
    except (ValueError, TypeError):
        return None


def _code(value: Any) -> str:
    s = str(value or '').strip()
    if s.endswith('.0') and s[:-2].isdigit():
        s = s[:-2]
    return s.zfill(4) if s.isdigit() and len(s) < 4 else s


def _text(value: Any) -> str:
    if value is None:
        return ''
    try:
        if pd.isna(value):
            return ''
    except (ValueError, TypeError):
        pass
    x = str(value).strip()
    return '' if x.lower() in ('nan', 'none', 'null', 'nat') else x


def safe_audit_discovery(audit: pd.DataFrame | None) -> pd.DataFrame:
    """Surface only validated secondary RADAR from the full evidence pool.

    Never promote AUDIT directly into original Research or actionable.  Must
    have a future time-window and actual core setup, not just sector strength.
    """
    if not isinstance(audit, pd.DataFrame) or audit.empty:
        return pd.DataFrame()
    keep = []
    for row in audit.to_dict('records'):
        if _text(row.get('H109決策權威')) != 'RESEARCH_ELIGIBLE':
            continue
        if _text(row.get('H105今日強勢排除')) == '是':
            continue
        if _text(row.get('H110證據狀態')).startswith('CONFLICT'):
            continue
        if _text(row.get('H110主榜准入')).startswith('否'):
            continue
        if _text(row.get('市場別')) in ('興櫃', '創櫃'):
            continue
        if not _text(row.get('類別')) or not _code(row.get('股票代號')):
            continue
        if _number(row.get('H110最終研究優先分')) is None or _number(row.get('H110最終研究優先分')) < 63:
            continue
        if (_number(row.get('H105黑馬預發動分')) or 0) < 60:
            continue
        if (_number(row.get('H105核心證據完整度%')) or 0) < 75:
            continue
        if (_number(row.get('H105過熱追高風險分')) or 100) >= 62:
            continue
        stage = _text(row.get('H105發動階段'))
        tier = _text(row.get('H105黑馬層級'))
        window = _text(row.get('H105未來發動窗口'))
        if not (stage.startswith('BH1') or stage.startswith('BH2')):
            continue
        if not (tier.startswith('S1') or tier.startswith('S2')):
            continue
        if not window or window in ('未定', 'NO-TIMING-CONFIDENCE') or '已發動' in window:
            continue
        row['股票代號'] = _code(row['股票代號'])
        row['H112候選來源'] = 'AUDIT_SECONDARY_RADAR｜未進原Research，僅供研究觀察'
        row['H112研究權限'] = 'WATCH_ONLY｜不得當成正式買進'
        row['H105推薦性質'] = 'H112跨族群前兆｜稽核雷達觀察，非原Research'
        keep.append(row)
    return pd.DataFrame(keep) if keep else pd.DataFrame()


def balance_research(picked: pd.DataFrame, eligible: pd.DataFrame, target: int) -> pd.DataFrame:
    """Final check of Research exposure, with no audit-origin promotion.

    If the only Research-eligible candidates are in one sector and target has
    1-3 slots, the system must show fewer, not print 100% sector duplication.
    """
    if not isinstance(picked, pd.DataFrame) or picked.empty:
        return picked.copy()
    if not isinstance(eligible, pd.DataFrame) or eligible.empty:
        return picked.head(0).copy()
    data = eligible.loc[eligible['股票代號'].astype(str).isin(set(picked['股票代號'].astype(str)))].copy()
    if data.empty:
        return picked.head(0).copy()
    order_col = '__h108' if '__h108' in data.columns else 'H110最終研究優先分'
    data['__h112_base'] = pd.to_numeric(data[order_col], errors='coerce').fillna(-1000)
    data = data.sort_values(['__h112_base', '股票代號'], ascending=[False, True], kind='stable')
    seen = set(); cats = set(); indices = []
    n_categories = len(set(data['類別'].fillna('未分類').astype(str))) if '類別' in data else 1
    cap = 1 if target <= 3 or n_categories < 3 else 2
    for idx, row in data.iterrows():
        code = _code(row.get('股票代號'))
        cat = _text(row.get('類別')) or '__UNKNOWN__'
        if not code or code in seen or sum(1 for i in indices if (_text(data.at[i,'類別']) or '__UNKNOWN__') == cat) >= cap:
            continue
        seen.add(code); cats.add(cat); indices.append(idx)
        if len(indices) >= target:
            break
    out = data.loc[indices].copy().drop(columns=['__h112_base'],errors='ignore')
    return out.reset_index(drop=True)


def balance_blackhorse(frame: pd.DataFrame, limit: int) -> pd.DataFrame:
    """Select one leader per sector first, then secondary names with a real penalty.

    H110 score is retained as *evidence*; H112 final score is explicitly
    adjusted for AUDIT-only status, 14-day sector repeat, and same-list exposure.
    Stable deterministic ordering; short list is acceptable and honest.
    """
    if not isinstance(frame, pd.DataFrame) or frame.empty or limit <= 0:
        return frame.head(0).copy() if isinstance(frame, pd.DataFrame) else pd.DataFrame()
    work = frame.copy().reset_index(drop=True)
    work['股票代號'] = work['股票代號'].map(_code)
    work = work[work['股票代號'].ne('')].drop_duplicates('股票代號').reset_index(drop=True)
    if work.empty:
        return work
    if '類別' not in work:
        work['類別'] = '未分類'
    work['類別'] = work['類別'].fillna('未分類').astype(str)
    all_cats = work['類別'].nunique()
    # In a tiny 1-3 name shortlist, never let two from one sector crowd out the
    # only other opportunities.  Two same-sector names need >=3 sector choices.
    max_sector = 1 if min(limit, len(work)) < 4 or all_cats < 3 else 2
    counts: Counter[str] = Counter()
    chosen: list[tuple[int, float, float, str]] = []
    unused = set(work.index)
    while unused and len(chosen) < limit:
        # A diversified discovery shortlist must show at least one qualified name
        # per distinct sector before showing a second name from any sector.
        fresh_sector_available = any(counts[work.loc[k, "類別"]] == 0 for k in unused)
        candidates = []
        for i in unused:
            row = work.loc[i]
            cat = row['類別']
            if counts[cat] >= max_sector or (fresh_sector_available and counts[cat] > 0):
                continue
            base = _number(row.get('H110最終研究優先分'))
            if base is None:
                continue
            audit = 'AUDIT' in _text(row.get('H112候選來源'))
            days = max(0, int(_number(row.get('H107近期族群入選日數')) or 0))
            repeat_penalty = min(9., 2.0*max(0,days-1))
            audit_penalty = 7.0 if audit else 0.0
            same_sector_penalty = 22.0 * counts[cat]
            penalty = repeat_penalty + audit_penalty + same_sector_penalty
            value = max(0.0, base - penalty)
            candidates.append((value, base, -penalty, _code(row.get('股票代號')), i, penalty))
        if not candidates:
            break
        value, base, _negative_penalty, _ticker, index, penalty = max(candidates,key=lambda x:(x[0],x[1],x[2],-int(x[3]) if x[3].isdigit() else 0))
        row = work.loc[index]
        chosen.append((index, round(value,2), round(penalty,2),row['類別']))
        counts[row['類別']] += 1
        unused.remove(index)
    if not chosen:
        return work.head(0)
    out = work.loc[[i for i,_,_,_ in chosen]].copy().reset_index(drop=True)
    out['H112最終跨族群優先分'] = [s for _,s,_,_ in chosen]
    out['H112族群曝險扣分'] = [p for _,_,p,_ in chosen]
    out['H112候選來源'] = out.get('H112候選來源',pd.Series(index=out.index,dtype=str)).fillna('原Research／Waiting｜未來黑馬研究')
    out['H112研究權限'] = 'RESEARCH_ONLY｜不放寬Formal買進權限'
    src = out['H112候選來源'].astype(str).str.contains('AUDIT')
    out.loc[src,'H112研究權限'] = 'WATCH_ONLY｜原稽核池候選，尚未通過原Research'
    ordinal: Counter[str] = Counter()
    statuses = []
    for cat in out['類別']:
        ordinal[cat] += 1
        statuses.append(f'同族群第{ordinal[cat]}檔／共{counts[cat]}檔｜本輪上限{max_sector}檔')
    out['H112族群分散狀態'] = statuses
    return out


__all__ = ['VERSION','safe_audit_discovery','balance_research','balance_blackhorse']
