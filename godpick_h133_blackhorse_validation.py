# -*- coding: utf-8 -*-
"""V191-H133 | conservative, read-only blackhorse research out-of-sample audit.

DO NOT use this module to issue orders, authorize recommendations or update model
weights. Returns are signal close -> future closes, not obtainable fills. Historical
rows supplied by a user do not prove point-in-time (PIT) immutability.
"""
from __future__ import annotations

from datetime import date
from typing import Any
import math

import numpy as np
import pandas as pd

from godpick_h130_six_engine import _date, _code, _num, _text, _pick, normalize_prices

VERSION = 'V191-H133.0'
HORIZONS = (1, 3, 5)
MAX_ROWS = 20000
MAX_BARS = 250000
RISK_VETO = ('VETO', 'FADE', 'SHOCK', 'QUARANTINE', 'BLOCK', '禁止')
PROVEN_FLOW = ('PROVEN_FLOW', 'VERIFIED_FLOW')
PROVEN_HOLDER = ('VERIFIED_RISING', 'PROVEN_RISING')


def _empty_audit(code: str, msg: str) -> pd.DataFrame:
    return pd.DataFrame([{'資料狀態': code, '說明': msg}])


def _benchmark(prices: pd.DataFrame | None, *, asof: date) -> tuple[pd.DataFrame, str]:
    if not isinstance(prices, pd.DataFrame) or prices.empty:
        return pd.DataFrame(), 'NO_BENCHMARK'
    d = _pick(prices, '日期', 'Date', 'date')
    c = _pick(prices, '收盤價', '收盤', 'Close', 'close')
    if not d or not c or len(prices) > MAX_BARS:
        return pd.DataFrame(), 'BENCHMARK_SCHEMA_OR_SIZE'
    days = [_date(v) for v in prices[d]]
    values = [_num(v) for v in prices[c]]
    b = pd.DataFrame({'日期': days, '大盤收盤價': values})
    b = b.dropna(subset=['日期', '大盤收盤價'])
    b = b[(b['大盤收盤價'] > 0) & (b['日期'] <= asof)]
    if b.empty:
        return pd.DataFrame(), 'NO_VALID_BENCHMARK'
    # Refuse same-session contradictory close, rather than arbitrary keep-last.
    if b.groupby('日期')['大盤收盤價'].nunique().gt(1).any():
        return pd.DataFrame(), 'CONFLICTING_BENCHMARK_CLOSE'
    b = b.drop_duplicates('日期').sort_values('日期').reset_index(drop=True)
    if b['日期'].duplicated().any() or b['日期'].apply(lambda v: v.weekday() >= 5).any():
        return pd.DataFrame(), 'INVALID_MARKET_SESSION'
    if b['大盤收盤價'].pct_change().abs().gt(.40).any():
        return pd.DataFrame(), 'EXTREME_BENCHMARK_MOVE_RECHECK'
    return b, 'INPUT_NOT_OFFICIALLY_VERIFIED'


def _stock_prices(prices: pd.DataFrame | None, *, asof: date) -> tuple[pd.DataFrame, str]:
    if not isinstance(prices, pd.DataFrame) or prices.empty:
        return pd.DataFrame(), 'NO_STOCK_PRICES'
    if len(prices) > MAX_BARS:
        return pd.DataFrame(), 'PRICE_SIZE_LIMIT'
    raw_code = _pick(prices, '股票代號', '代號', 'code', 'Code')
    raw_day = _pick(prices, '日期', 'date', 'Date', 'datetime')
    raw_close = _pick(prices, '收盤價', '收盤', 'close', 'Close', 'Adj Close')
    if not all((raw_code, raw_day, raw_close)):
        return pd.DataFrame(), 'PRICE_SCHEMA_MISSING'
    frame = prices[[raw_code, raw_day, raw_close]].copy()
    frame.columns = ['code', 'day', 'close']
    frame['code'] = frame['code'].map(_code)
    frame['day'] = frame['day'].map(_date)
    frame['close'] = frame['close'].map(_num)
    frame = frame.dropna(subset=['day', 'close'])
    frame = frame[(frame.code != '') & (frame.close > 0) & (frame.day <= asof)]
    if frame.empty:
        return pd.DataFrame(), 'NO_VALID_STOCK_PRICES'
    conflict = frame.groupby(['code', 'day'])['close'].nunique().gt(1)
    if conflict.any():
        # Reject whole submission so overlapping sources cannot bias backtests.
        return pd.DataFrame(), 'CONFLICTING_STOCK_CLOSE'
    frame = frame.drop_duplicates(['code', 'day']).sort_values(['code', 'day'])
    if frame['day'].map(lambda d: d.weekday() >= 5).any():
        return pd.DataFrame(), 'WEEKEND_STOCK_PRICE_RECHECK'
    # Detect unadjusted splits/outliers: do not silently use giant jumps.
    jumps = frame.groupby('code')['close'].pct_change().abs()
    if jumps.gt(.40).any():
        return pd.DataFrame(), 'EXTREME_STOCK_MOVE_RECHECK'
    return frame.reset_index(drop=True), 'INPUT_NOT_OFFICIALLY_VERIFIED'


def _normalize_signals(signals: pd.DataFrame | None, *, asof: date) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not isinstance(signals, pd.DataFrame) or signals.empty:
        return pd.DataFrame(), _empty_audit('NO_SIGNALS', '請提供不同交易日的歷史H126研究快照；單日榜不能驗證長期策略。')
    if len(signals) > MAX_ROWS:
        return pd.DataFrame(), _empty_audit('SIGNALS_SIZE_LIMIT', f'歷史訊號最多 {MAX_ROWS} 筆。')
    ccol = _pick(signals, '股票代號', '代號')
    dcol = _pick(signals, '訊號研究日', 'H126研究資訊截止日', 'H110研究資訊截止日')
    layercol = _pick(signals, 'H126權威層別', '研究層別')
    if not all((ccol, dcol, layercol)):
        return pd.DataFrame(), _empty_audit('SIGNAL_SCHEMA_MISSING', '需要股票代號、訊號研究日、H126權威層別，不接受舊H102榜冒充。')
    rows, issues = [], []
    for idx, r in enumerate(signals.to_dict('records')):
        code = _code(r.get(ccol))
        day = _date(r.get(dcol))
        layer = _text(r.get(layercol))
        veto = _text(r.get('H109決策權威'))
        score = _num(r.get('H126最後研究分'))
        if score is None:
            score = _num(r.get('研究分數_當時'))
        if not code or day is None or day > asof or day.weekday() >= 5:
            issues.append({'資料狀態': 'INVALID_SIGNAL_DAY_OR_CODE', '股票代號': code, '資料行': idx + 2})
            continue
        if not layer.startswith(('A', 'B', 'Ａ', 'Ｂ')) or any(v in layer.upper() for v in RISK_VETO):
            issues.append({'資料狀態': 'NOT_ELIGIBLE_RESEARCH', '股票代號': code, '資料行': idx + 2})
            continue
        if any(v in veto.upper() for v in RISK_VETO):
            issues.append({'資料狀態': 'H109_HARD_VETO', '股票代號': code, '資料行': idx + 2})
            continue
        produced = r.get('訊號產生時間', r.get('快照建立時間'))
        provenance = 'PIT_UNVERIFIED'
        if _text(produced):
            try:
                stamp = pd.Timestamp(produced)
                if pd.isna(stamp):
                    raise ValueError('bad timestamp')
                if stamp.tzinfo is not None:
                    stamp = stamp.tz_convert('Asia/Taipei').tz_localize(None)
                if stamp.date() > day:
                    # Historical report exported next day cannot establish an as-of signal.
                    issues.append({'資料狀態': 'SNAPSHOT_CREATED_AFTER_SIGNAL_DAY', '股票代號': code, '資料行': idx + 2})
                    continue
                if stamp.date() < day:
                    issues.append({'資料狀態': 'SNAPSHOT_BEFORE_RESEARCH_DAY', '股票代號': code, '資料行': idx + 2})
                    continue
                provenance = 'TIMESTAMP_DECLARED_NOT_INDEPENDENTLY_VERIFIED'
            except (ValueError, TypeError, OverflowError):
                issues.append({'資料狀態': 'INVALID_SIGNAL_TIMESTAMP', '股票代號': code, '資料行': idx + 2})
                continue
        rows.append({'股票代號': code, '訊號研究日': day, 'H126權威層別': layer,
                     'H109決策權威': veto, '當時研究分數': score,
                     '法人證據宣稱': _text(r.get('H123法人實證狀態')),
                     '大戶證據宣稱': _text(r.get('H124大戶實證狀態')),
                     'PIT證明狀態': provenance})
    s = pd.DataFrame(rows)
    if s.empty:
        return s, pd.DataFrame(issues) if issues else _empty_audit('NO_ELIGIBLE', '沒有通過基本研究分層的歷史訊號。')
    ambiguous = s.groupby(['股票代號', '訊號研究日'], dropna=False).agg(
        n=('股票代號', 'size'), scores=('當時研究分數', lambda x: len(set(str(v) for v in x))),
        layers=('H126權威層別', 'nunique'), vetos=('H109決策權威', 'nunique'))
    conflicting = ambiguous[(ambiguous.scores > 1) | (ambiguous.layers > 1) | (ambiguous.vetos > 1)].index
    if len(conflicting):
        clashes = set(conflicting)
        s = s[[ (c, d) not in clashes for c, d in zip(s['股票代號'], s['訊號研究日'])]]
        for c, d in conflicting:
            issues.append({'資料狀態': 'CONFLICTING_SIGNAL_SNAPSHOT', '股票代號': c, '訊號研究日': str(d)})
    s = s.drop_duplicates(['股票代號', '訊號研究日']).sort_values(['訊號研究日', '股票代號']).reset_index(drop=True)
    audit = pd.DataFrame(issues) if issues else _empty_audit('SIGNALS_ACCEPTED_UNVERIFIED_PIT',
                          '僅檢查資料內部日期；上傳Excel不能證明訊號在當時已封存。')
    return s, audit


def _ci(cohort_alpha: np.ndarray) -> tuple[float | None, float | None]:
    if len(cohort_alpha) < 20:
        return None, None
    random = np.random.default_rng(133)
    # Moving-block bootstrap accounts partially for serially overlapping T+5 labels.
    block = min(5, len(cohort_alpha))
    starts = random.integers(0, len(cohort_alpha) - block + 1, size=(1000, (len(cohort_alpha) + block - 1) // block))
    draws = (starts[:, :, None] + np.arange(block)).reshape(1000, -1)[:, :len(cohort_alpha)]
    avg = cohort_alpha[draws].mean(axis=1)
    low, high = np.quantile(avg, [0.025, .975])
    return round(float(low), 3), round(float(high), 3)


def evaluate_blackhorse(
    signals: pd.DataFrame | None,
    stock_prices: pd.DataFrame | None,
    benchmark_prices: pd.DataFrame | None,
    *, asof: date,
    adjusted_confirmed: bool = False,
    top_k: int = 5,
    min_oos_days: int = 20,
) -> dict[str, Any]:
    """Safe history-aware research, *not* a verified executable backtest.

    Scores must have existed on the signal date. We cannot verify that through
    a spreadsheet; results are explicitly labelled retrospective unless an
    independent append-only timestamped authority is separately audited.
    """
    outputs: dict[str, Any] = {
        '狀態': 'NO_DATA', '說明': '', '訊號稽核': pd.DataFrame(),
        '逐筆觀測': pd.DataFrame(), '樣本外績效': pd.DataFrame(),
        '因子分組': pd.DataFrame(), '分割摘要': pd.DataFrame(),
    }
    if top_k < 1 or top_k > 20 or min_oos_days < 10:
        outputs['狀態'] = 'INVALID_SETTINGS'
        outputs['說明'] = '單日研究上限為1～20；樣本外至少10個獨立研究交易日。'
        return outputs
    sig, audit = _normalize_signals(signals, asof=asof)
    outputs['訊號稽核'] = audit
    if sig.empty:
        outputs['狀態'] = 'NO_VALID_SIGNALS'
        outputs['說明'] = '沒有可用的A/B研究候選；H109風險否決不可為了湊樣本放寬。'
        return outputs
    bar, bstatus = _benchmark(benchmark_prices, asof=asof)
    stock, pstatus = _stock_prices(stock_prices, asof=asof)
    if bar.empty or stock.empty:
        outputs['狀態'] = bstatus if bar.empty else pstatus
        outputs['說明'] = '股價或大盤日K缺失／矛盾，請上傳可核對的歷史行情並確認除權息調整。'
        return outputs
    days = list(bar['日期'])
    pos = {day: i for i, day in enumerate(days)}
    benchmark_closes = dict(zip(bar['日期'], bar['大盤收盤價']))
    stock_closes = {(code, day): close for code, day, close in stock[['code', 'day', 'close']].itertuples(index=False, name=None)}
    # Pick before computing future returns, using ONLY dated historical score.
    selected = sig.sort_values(['訊號研究日', '當時研究分數', '股票代號'],
                                ascending=[True, False, True], na_position='last')
    selected = selected.groupby('訊號研究日', sort=False).head(top_k).copy()
    observations, rejected = [], []
    expected = selected.groupby('訊號研究日').size().to_dict()
    for record in selected.to_dict('records'):
        day, code = record['訊號研究日'], record['股票代號']
        rec = {'股票代號': code, '訊號研究日': str(day), '研究權威': record['H126權威層別'],
               '當時研究分數': record['當時研究分數'],
               '法人證據宣稱': record['法人證據宣稱'], '大戶證據宣稱': record['大戶證據宣稱'],
               'PIT證明狀態': record['PIT證明狀態'], '訊號收盤價_非成交': None}
        p0 = stock_closes.get((code, day)) if day in pos else None
        if p0 is not None:
            rec['訊號收盤價_非成交'] = round(float(p0), 4)
        for h in HORIZONS:
            key_s, key_b, key_a, key_state = f'T+{h}個股%', f'T+{h}大盤%', f'T+{h}超額pp', f'T+{h}狀態'
            rec[key_s], rec[key_b], rec[key_a] = None, None, None
            if day not in pos:
                rec[key_state] = 'SIGNAL_DAY_NOT_MARKET_SESSION'
                continue
            if p0 is None:
                rec[key_state] = 'SIGNAL_DAY_STOCK_PRICE_MISSING'
                continue
            # Market session index, not nth observed ticker bar (handles suspension).
            target_index = pos[day] + h
            if target_index >= len(days):
                rec[key_state] = 'NOT_MATURED'
                continue
            target = days[target_index]
            px = stock_closes.get((code, target))
            if px is None:
                rec[key_state] = 'STOCK_BAR_MISSING_NO_FILL'
                continue
            stock_return = (px / p0 - 1) * 100
            bench_return = (benchmark_closes[target] / benchmark_closes[day] - 1) * 100
            if not (math.isfinite(stock_return) and math.isfinite(bench_return)) or abs(stock_return) > 35 or abs(bench_return) > 20:
                rec[key_state] = 'EXTREME_RETURN_QUARANTINE'
                continue
            rec[key_s] = round(stock_return, 4)
            rec[key_b] = round(bench_return, 4)
            rec[key_a] = round(stock_return - bench_return, 4)
            rec[key_state] = 'RESEARCH_ONLY_NO_FILL'
        observations.append(rec)
    observations = pd.DataFrame(observations)
    outputs['逐筆觀測'] = observations
    if observations.empty:
        outputs['狀態'] = 'NO_OBSERVABLE_PRICE'
        outputs['說明'] = '研究訊號日缺少大盤或個股K線，不進行績效推論。'
        return outputs
    unique_dates = sorted(str(v) for v in expected)
    # Single chronological split; no scoring/parameter selection on holdout.
    split_idx = max(1, int(len(unique_dates) * .7))
    test_start = unique_dates[split_idx] if split_idx < len(unique_dates) else None
    split_pos = pos[date.fromisoformat(test_start)] if test_start else None
    # Five market sessions embargo: excludes training signals whose T+5 extends into holdout.
    cutoff = days[max(0, split_pos - max(HORIZONS))] if split_pos is not None else None
    train_days = [d for d in unique_dates[:split_idx] if date.fromisoformat(d) < cutoff] if cutoff else []
    test_days = unique_dates[split_idx:]
    outputs['分割摘要'] = pd.DataFrame([{
        '訓練訊號交易日數_禁運後': len(train_days), '樣本外訊號交易日數': len(test_days),
        '樣本外起日': test_start or 'INSUFFICIENT_DATES',
        '禁運交易日': max(HORIZONS), '名次選取': f'訊號當日既有分數Top{top_k}',
        '資料型態': '研究收盤觀測；非成交損益',
        '大盤交易日曆來源': '由使用者大盤CSV推算；未向H117重新驗證交易日完整性',
        '報酬口徑': '個股調整收盤(需驗證)減大盤價格指數；非含息總報酬',
        'PIT來源真實性': '外部不可變封存尚未獨立驗證',
        '調整價確認': '使用者確認／仍需原始來源核驗' if adjusted_confirmed else '未確認；禁止有效性結論',
    }])
    summaries, factors = [], []
    for h in HORIZONS:
        alpha_key, stock_key, state_key = f'T+{h}超額pp', f'T+{h}個股%', f'T+{h}狀態'
        subset = observations[(observations['訊號研究日'].isin(test_days)) & (observations[state_key] == 'RESEARCH_ONLY_NO_FILL')]
        day_groups = subset.groupby('訊號研究日').agg(
            **{'超額pp': (alpha_key, 'mean'), '股票%': (stock_key, 'mean'),
               '每日股票數': ('股票代號', 'size')}).sort_index()
        # Complete-case daily cohorts ONLY. Partial survivorship must not inflate alpha.
        day_groups = day_groups[[int(row['每日股票數']) == expected.get(date.fromisoformat(idx), -1)
                                 for idx, row in day_groups.iterrows()]]
        daily_alpha = day_groups['超額pp'].to_numpy(dtype=float)
        lo, hi = _ci(daily_alpha)
        test_days_total = len(test_days)
        coverage = len(day_groups) / test_days_total if test_days_total else 0.
        classification = ('樣本不足／尚未成熟' if len(day_groups) < min_oos_days else
                          '調整價未確認' if not adjusted_confirmed else
                          '樣本日覆蓋不足' if coverage < .8 else
                          '僅歷史研究偏正向；PIT未獨立核實' if lo is not None and lo > 0 else
                          '未證明超額優勢')
        summaries.append({'期間': f'T+{h}', '樣本外研究日數': test_days_total,
                          '有效獨立交易日': len(day_groups), '涵蓋率%': round(coverage * 100, 2),
                          '有效觀測筆數': int(day_groups['每日股票數'].sum()) if len(day_groups) else 0,
                          '日等權個股變化%': round(float(day_groups['股票%'].mean()), 3) if len(day_groups) else None,
                          '日等權超額pp': round(float(daily_alpha.mean()), 3) if len(daily_alpha) else None,
                          '日勝過大盤比率%': round(float((daily_alpha > 0).mean() * 100), 2) if len(daily_alpha) else None,
                          'Bootstrap95%下界pp': lo, 'Bootstrap95%上界pp': hi,
                          '研究判斷': classification,
                          '證據權威': '不得修改H109/H126交易權限；來源PIT未驗證'})
        # Factor summaries are descriptive, never treat missing provenance as confirmed.
        valid_days = set(day_groups.index)
        factor_subset = subset[subset['訊號研究日'].isin(valid_days)]
        for col, whitelist, name in (
            ('法人證據宣稱', PROVEN_FLOW, '法人逐日證據'),
            ('大戶證據宣稱', PROVEN_HOLDER, 'TDCC持股證據')):
            values = factor_subset.assign(聲稱符合=factor_subset[col].isin(whitelist))
            for yes, sample in values.groupby('聲稱符合'):
                daily = sample.groupby('訊號研究日')[alpha_key].mean()
                factors.append({'期間': f'T+{h}', '因子': name,
                                '來源宣稱狀態': '宣稱已證明（未重查原始公告）' if yes else '未證明／未知',
                                '有效研究日數': len(daily), '平均日超額pp': round(float(daily.mean()), 3) if len(daily) >= 10 else None,
                                '判斷': '僅描述相關性，不作因果／調權證據' if len(daily) >= 10 else '樣本少於10日，不比較'})
    outputs['樣本外績效'] = pd.DataFrame(summaries)
    outputs['因子分組'] = pd.DataFrame(factors)
    outputs['狀態'] = 'RETROSPECTIVE_RESEARCH_ONLY' if adjusted_confirmed else 'ADJUSTMENT_UNVERIFIED'
    outputs['說明'] = ('不論回測結果如何，都沒有獨立不可變時點封存、完整交易日曆、總報酬企業行動驗證與實際成交成本；'
                       '不能證明未來獲利、不能自動調整模型權重或升級成正式交易授權。')
    return outputs
