"""
trainer.py
Trains an Isolation Forest on CLEAN datasets only.

Why Isolation Forest instead of supervised XGBoost?
- We have no ground-truth labels — only rule-generated ones that perfectly
  correlate with the features, making supervised ROC-AUC trivially 1.0.
- The real goal is anomaly detection: flag cells that look *unlike* clean data.
- IF learns the distribution of "normal" from clean data; at inference it scores
  how anomalous each cell is relative to that learned normality.
- No labels, no circular feature-label leakage, genuine generalisation.
"""

import io
import os
import sys
import json
import numpy as np
import pandas as pd
import joblib

from sklearn.ensemble import IsolationForest

from .feature_extractor import extract_dataset_features, FEATURE_NAMES, N_FEATURES

MODEL_PATH   = os.path.join(os.path.dirname(__file__), 'models', 'issue_detector.pkl')
METRICS_PATH = os.path.join(os.path.dirname(__file__), 'models', 'metrics.json')


# ---------------------------------------------------------------------------
# File loading
# ---------------------------------------------------------------------------
def _load_file(path: str) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    for enc in ('utf-8', 'latin-1'):
        try:
            if ext == '.csv':
                with open(path, 'r', encoding=enc, errors='replace') as f:
                    first = f.readline()
                counts = {s: first.count(s) for s in (',', ';', '\t', '|')}
                sep = max(counts, key=counts.get)
                return pd.read_csv(
                    path, sep=sep, encoding=enc,
                    low_memory=False, on_bad_lines='skip',
                )
            elif ext in ('.xlsx', '.xls'):
                df = pd.read_excel(path, engine='openpyxl')
                if df.shape[1] == 1 and ',' in str(df.columns[0]):
                    header = df.columns[0]
                    rows   = df.iloc[:, 0].astype(str).tolist()
                    text   = header + '\n' + '\n'.join(rows)
                    return pd.read_csv(
                        io.StringIO(text), low_memory=False, on_bad_lines='skip',
                    )
                return df
        except UnicodeDecodeError:
            continue
    raise ValueError(f'Cannot load: {path}')


def _iter_files(directory: str):
    exts = {'.csv', '.xlsx', '.xls'}
    for f in os.listdir(directory):
        fpath = os.path.join(directory, f)
        if os.path.isfile(fpath) and os.path.splitext(f)[1].lower() in exts:
            yield fpath


# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------
def train(
    clean_dir: str,
    dirty_dir: str = None,      # kept for API compatibility, unused
    max_rows_per_file: int = 3000,
    verbose: bool = True,
) -> dict:
    """
    Train an Isolation Forest on all clean datasets.
    dirty_dir is accepted but ignored (unsupervised approach needs no dirty data).
    """
    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)

    X_parts = []
    n_files = 0

    if verbose:
        print('[TRAIN] Loading clean datasets ...', flush=True)

    for fpath in sorted(_iter_files(clean_dir)):
        try:
            df = _load_file(fpath)
            X, _, _, _ = extract_dataset_features(df, max_rows=max_rows_per_file)
            if len(X) == 0:
                continue
            X_parts.append(X)
            n_files += 1
            if verbose:
                print(f'  + {os.path.basename(fpath)}: {len(X):,} cells', flush=True)
        except Exception as e:
            if verbose:
                print(f'  x {os.path.basename(fpath)}: {e}', flush=True)

    if not X_parts:
        raise RuntimeError('No clean data loaded — check clean_dir path.')

    X_clean = np.vstack(X_parts)

    if verbose:
        print(f'\n[TRAIN] Total clean cells: {len(X_clean):,}  |  Features: {N_FEATURES}', flush=True)
        print('[TRAIN] Fitting Isolation Forest ...', flush=True)

    model = IsolationForest(
        n_estimators=200,
        max_samples=min(2048, len(X_clean)),
        contamination=0.05,   # expect ~5% anomalous cells in new (unseen) data
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_clean)

    # Score distribution on training (clean) data
    train_scores = model.decision_function(X_clean)
    pct = np.percentile(train_scores, [5, 10, 25, 50, 75, 90, 95])

    if verbose:
        print(f'[TRAIN] Score distribution on clean data (higher = more normal):')
        for p, v in zip([5, 10, 25, 50, 75, 90, 95], pct):
            print(f'  p{p:2d}: {v:+.4f}')

    joblib.dump(model, MODEL_PATH)
    if verbose:
        print(f'\n[TRAIN] Model saved -> {MODEL_PATH}', flush=True)

    metrics = {
        'approach':        'isolation_forest',
        'n_train_cells':   int(len(X_clean)),
        'n_train_files':   n_files,
        'n_features':      N_FEATURES,
        'feature_names':   FEATURE_NAMES,
        'contamination':   0.05,
        'model_path':      MODEL_PATH,
        # score distribution (used by UI to contextualise anomaly scores)
        'score_p5':   float(pct[0]),
        'score_p10':  float(pct[1]),
        'score_p25':  float(pct[2]),
        'score_p50':  float(pct[3]),
        'score_p75':  float(pct[4]),
        'score_p90':  float(pct[5]),
        'score_p95':  float(pct[6]),
        # placeholders so UI panels that display roc_auc don't break
        'roc_auc':        None,
        'avg_precision':  None,
    }

    with open(METRICS_PATH, 'w') as f:
        json.dump(metrics, f, indent=2)

    return metrics
