"""Offline challenger training, purged by label maturity, never auto-promoted.

Input: dated point-in-time records with explicit feature availability and label
maturity. Train on earlier dates only; evaluate ranked net excess returns on a
later date. Fixed feature allowlist prevents accidental use of outcome columns.
All coefficients and fold sizes are exported; no performance guarantee.
"""
from __future__ import annotations
import numpy as np
import pandas as pd

FEATURES = ['H47個股相對強度分','H53族群共振分','H53族群廣度分','當日量比',
            '今日漲幅%','近5日漲幅%','成交額3日加速度%','3日動能加速度百分點']
REQUIRED = FEATURES + ['股票代號','decision_at','features_available_at','label_mature_at',
                       'gross_return_pct','benchmark_return_pct','roundtrip_cost_pct']

def walk_forward(records, *, min_train=100, min_dates=20, top_k=5, ridge=10.):
    """Labels must use the same declared holding horizon and execution policy.

    A caller must supply the actual cost-adjusted realized return for its fill
    assumptions; signal returns and executable returns are different datasets.
    This function deliberately refuses mixed horizon/return-basis labels.
    """
    if min_train < 2 or min_dates < 2 or top_k < 1 or ridge <= 0:
        raise ValueError('invalid training policy')
    df = pd.DataFrame(records).copy()
    missing = [c for c in REQUIRED + ['horizon_sessions','return_basis'] if c not in df]
    if missing:
        return {'status':'INSUFFICIENT_SCHEMA','missing':missing,'activated':False,'folds':[]}
    if df['horizon_sessions'].nunique(dropna=False) != 1 or df['return_basis'].nunique(dropna=False) != 1:
        raise ValueError('mixed return basis/horizons')
    horizon = pd.to_numeric(df['horizon_sessions'], errors='coerce')
    if horizon.isna().any() or not (horizon > 0).all() or not df['return_basis'].isin(['signal_close_to_close','executable']).all():
        raise ValueError('invalid horizon/basis')
    for c in ['decision_at','features_available_at','label_mature_at']:
        df[c] = pd.to_datetime(df[c], utc=True, errors='coerce')
    numeric = FEATURES + ['gross_return_pct','benchmark_return_pct','roundtrip_cost_pct']
    for c in numeric:
        df[c] = pd.to_numeric(df[c],errors='coerce').replace([np.inf,-np.inf],np.nan)
    if df[REQUIRED].isna().any().any():
        raise ValueError('missing/invalid point-in-time training data')
    if df.duplicated(['股票代號','decision_at']).any():
        raise ValueError('duplicate training identity')
    if (df.features_available_at > df.decision_at).any():
        raise ValueError('future feature leakage')
    if (df.label_mature_at <= df.decision_at).any() or (df.roundtrip_cost_pct < 0).any():
        raise ValueError('invalid maturity/cost')
    df['_day'] = df.decision_at.dt.floor('D')
    df['_y'] = df.gross_return_pct - df.benchmark_return_pct - df.roundtrip_cost_pct
    folds = []
    for d in sorted(df['_day'].unique()):
        test = df[df['_day'].eq(d)].copy()
        cutoff = test.decision_at.min()
        train = df[(df['_day'] < d) & (df.label_mature_at < cutoff)].copy()
        if len(train) < min_train or train['_day'].nunique() < min_dates: continue
        x = train[FEATURES].to_numpy(float)
        median = np.median(x,axis=0)
        scale = np.std(x,axis=0); scale[scale < 1e-8] = 1
        x = (x - median) / scale
        y = train['_y'].to_numpy(float); center = y.mean()
        coef = np.linalg.solve(x.T @ x + ridge * np.eye(len(FEATURES)),x.T @ (y-center))
        test['_prediction'] = ((test[FEATURES].to_numpy(float)-median)/scale) @ coef + center
        test['_code'] = test['股票代號'].astype(str)
        picks = test.sort_values(['_prediction','_code'],ascending=[False,True]).head(top_k)
        baseline = float(test['_y'].mean())
        realized = float(picks['_y'].mean())
        folds.append({'test_date':str(d.date()),'train_rows':len(train),'test_rows':len(test),
                      'train_label_max':train.label_mature_at.max().isoformat(),
                      'test_decision_min':cutoff.isoformat(),'selected':picks['_code'].tolist(),
                      'net_excess_pct':realized,'pool_net_excess_pct':baseline,
                      'selection_lift_pct':realized-baseline,
                      'coefficients':dict(zip(FEATURES,coef.tolist())),
                      'train_median':median.tolist(),'train_scale':scale.tolist(),'intercept':float(center)})
    return {'status':'EVALUATED_SHADOW_ONLY' if folds else 'INSUFFICIENT_MATURE_HISTORY',
            'activated':False,'feature_allowlist':FEATURES,'folds':folds,
            'mean_net_excess_pct':float(np.mean([f['net_excess_pct'] for f in folds])) if folds else None,
            'mean_selection_lift_pct':float(np.mean([f['selection_lift_pct'] for f in folds])) if folds else None,
            'limitation':'僅離線挑戰模型；沒有自動上線。需獨立留出期、滑價/交易可行性與多重試驗檢查。'}
