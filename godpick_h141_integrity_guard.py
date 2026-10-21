# -*- coding: utf-8 -*-
"""H141 fail-closed remote-vs-local authority and manager Excel quality gates.

Pure checks; never creates missing evidence, trades, or remote writes.
"""
from __future__ import annotations
from datetime import date
from typing import Any
import pandas as pd

VERSION = 'V191-H141.0'


def _date(value: Any) -> str:
    try:
        d = pd.to_datetime(value, errors='coerce')
        return d.strftime('%Y-%m-%d') if pd.notna(d) else ''
    except Exception:
        return ''


def record_snapshot_guard(rows: list[dict], remote_state: dict, *, destructive: bool = False) -> tuple[bool, str]:
    """Reject a smaller/staler view of a newer remote authority before mutation.

    A deliberate deletion may lower count but may *never* remove a newer date
    simply because the UI was created from stale repository data.
    """
    if not isinstance(remote_state, dict) or not remote_state:
        return True, '尚無遠端權威摘要可對照'
    try:
        remote_count = int(remote_state.get('count') or 0)
    except (ValueError, TypeError):
        remote_count = 0
    remote_date = _date(remote_state.get('latest_recommendation_date'))
    local_dates = [_date(row.get('推薦日期') or row.get('date')) for row in rows if isinstance(row, dict)]
    latest = max(local_dates, default='')
    if remote_date and (not latest or latest < remote_date):
        return False, f'H141 歷史回退保護：畫面最新 {latest or "未知"}，遠端最新 {remote_date}；已阻擋舊資料寫入，請先從 runtime-data 完整還原。'
    if not destructive and remote_count and len(rows) < remote_count:
        return False, f'H141 歷史縮水保護：畫面 {len(rows)} 筆，遠端 {remote_count} 筆；必須先同步完整權威。'
    return True, '遠端日期與筆數防回退檢查通過'


def manager_quality_gate(authority: pd.DataFrame, audit: pd.DataFrame, risk: pd.DataFrame, legacy: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Supervisor-readable audit and safely redacted old research ranking."""
    authority = authority if isinstance(authority, pd.DataFrame) else pd.DataFrame()
    audit = audit if isinstance(audit, pd.DataFrame) else pd.DataFrame()
    risk = risk if isinstance(risk, pd.DataFrame) else pd.DataFrame()
    legacy = legacy.copy() if isinstance(legacy, pd.DataFrame) else pd.DataFrame()
    count = len(audit)
    h126_qualified = 0
    if 'H126能否買進' in authority.columns:
        h126_qualified = int(authority['H126能否買進'].astype(str).isin(('允許買進','YES','是')).sum())
    h109_veto = 0
    if 'H109決策權威' in audit.columns:
        h109_veto = int(audit['H109決策權威'].astype(str).str.contains('VETO|QUARANTINE', case=False, regex=True).sum())
    pending = 0
    for col in ('H123法人實證狀態','H124大戶實證狀態'):
        if col in audit.columns:
            pending += int(audit[col].astype(str).str.contains('PENDING|UNVERIFIED|MISSING', case=False, regex=True).sum())
    broad_veto = bool(count >= 20 and h109_veto / count >= 0.9)
    status = ('STOP｜全面風控否決比例異常，請先核對 H108/H109 市況與法人、TDCC 資料' if broad_veto else
              'NO_TRADE｜證據不足，維持研究用途' if h126_qualified == 0 else
              'REVIEW｜逐檔核對正式資格及交易日價格')
    rows = [
        ('H141匯出檢查版本', VERSION),
        ('本輪主管結論', status),
        ('H126授權進場檔數', h126_qualified),
        ('H113稽核候選檔數', count),
        ('H109風險否決檔數', h109_veto),
        ('H109風險否決比例', round(h109_veto/count*100,2) if count else None),
        ('H123/H124待驗證欄位數', pending),
        ('研究決策', '無證據即不得產生正式買進或價格計畫'),
        ('舊H102排序', '僅用於歷史研究，交易價格已隔離，不可下單'),
        ('進一步處置', '優先修復官方因子完整性、H108/H109族群判斷及上市櫃主流資金更新'),
    ]
    # Remove all legacy trading prices even if they came from H99 and look precise.
    price_markers = ('進場', '停損', '目標', 'RR', '目標交易日', '執行基準價', '價格計畫')
    for col in legacy.columns:
        if any(marker in str(col) for marker in price_markers):
            legacy[col] = None
    legacy.insert(0, 'H141研究授權警示', '歷史研究排序，不可買進；舊價格已隔離')
    return pd.DataFrame(rows, columns=['項目','數值']), legacy
