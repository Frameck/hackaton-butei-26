"""
predictor.py
Loads the trained Isolation Forest and scores each cell for anomalousness.

Anomaly score → confidence mapping:
  decision_function() returns values roughly in [-0.5, +0.5]
    +0.5  → very normal (deep inside clean distribution)
     0.0  → boundary (as anomalous as the contamination threshold)
    -0.5  → very anomalous (far from clean distribution)

  confidence = clip(0.5 - score, 0, 1)
    → 0.0 for clearly normal cells
    → 0.5 for borderline cells
    → 1.0 for strongly anomalous cells
"""

import os
import json
import numpy as np
import pandas as pd
import joblib

from .feature_extractor import (
    extract_dataset_features, FEATURE_NAMES,
    has_corrupt_chars, MIXED_NUM_SPECIAL_RE, try_float, to_pattern,
)
from .label_generator import ISSUE_CLEAN

MODEL_PATH   = os.path.join(os.path.dirname(__file__), 'models', 'issue_detector.pkl')
METRICS_PATH = os.path.join(os.path.dirname(__file__), 'models', 'metrics.json')

MAX_PREDICT_ROWS = 15_000


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _to_confidence(decision_scores: np.ndarray, score_p50: float, score_p5: float) -> np.ndarray:
    """
    Convert IF decision_function scores to [0,1] anomaly confidence,
    calibrated on the training distribution so that:
      - cells at or above clean p50  → confidence ≈ 0  (normal)
      - cells at the clean p5 boundary → confidence = 0.5  (borderline)
      - cells clearly below p5       → confidence → 1.0  (anomalous)

    Without calibration, hardcoding 0.5 as the midpoint gives wrong results
    because IF scores are not centred on 0.5 — they depend on contamination
    and the data distribution.
    """
    width = max(score_p50 - score_p5, 1e-6)
    return np.clip((score_p50 - decision_scores) / (2.0 * width), 0.0, 1.0)


def _confidence_level(p: float) -> str:
    if p >= 0.70:
        return 'HIGH'
    if p >= 0.50:
        return 'MEDIUM'
    if p >= 0.35:
        return 'LOW'
    return 'NONE'


def _display_issue_type(value, profile) -> str:
    """
    Human-readable issue classification used only for display in the UI.
    More descriptive than the training labels — covers corrupt chars and
    pattern mismatches that the IF flags but training labels don't track.
    """
    if pd.isna(value):
        return 'null'
    str_val = str(value).strip()
    if not str_val:
        return 'empty'
    # Corrupt / mixed characters (e.g. "73.9@", "CASE-01#ß")
    if has_corrupt_chars(str_val) or bool(MIXED_NUM_SPECIAL_RE.search(str_val)):
        return 'corrupt_chars'
    # Type mismatch: non-parseable value in a numeric column
    if profile.inferred_type == 'numeric':
        fv = try_float(str_val)
        if fv is None:
            return 'type_mismatch'
        z = (fv - profile.num_mean) / profile.num_std
        if abs(z) > 4.5:
            return 'outlier'
    # Pattern mismatch: value doesn't match the dominant column pattern
    if profile.has_pattern and to_pattern(str_val) != profile.dominant_pattern:
        return 'pattern_mismatch'
    # IF flagged it but no specific rule applies → generic statistical anomaly
    return 'anomaly'


def _dominant_issue(types: list) -> str:
    non_clean = [t for t in types if t != ISSUE_CLEAN]
    if not non_clean:
        return ISSUE_CLEAN
    return max(set(non_clean), key=non_clean.count)


# ---------------------------------------------------------------------------
# Main prediction function
# ---------------------------------------------------------------------------
def predict_dataset(df: pd.DataFrame) -> dict:
    """
    Score every cell in df for anomalousness using the Isolation Forest.

    Returns a dict with:
      quality_score      – 0-100 (100 = no anomalies detected)
      overall_confidence – mean anomaly confidence across all cells
      overall_issue_rate – fraction of cells with confidence > 0.5
      total_cells        – cells analysed
      column_results     – {col: {mean_confidence, max_confidence, …}}
      top_issue_cells    – up to 50 most anomalous non-null cells
      feature_importances– empty dict (IF does not expose feature importances)
    """
    if not os.path.exists(MODEL_PATH):
        raise FileNotFoundError(
            'Model not trained yet. POST /api/ml/train first.'
        )

    model = joblib.load(MODEL_PATH)

    # Load score distribution from training — needed for calibrated confidence
    score_p50 = 0.1677   # sensible default matching typical IF output
    score_p5  = 0.0
    if os.path.exists(METRICS_PATH):
        with open(METRICS_PATH) as f:
            m = json.load(f)
        score_p50 = m.get('score_p50', score_p50)
        score_p5  = m.get('score_p5',  score_p5)

    # Sample large datasets
    if len(df) > MAX_PREDICT_ROWS:
        df_pred = df.sample(n=MAX_PREDICT_ROWS, random_state=42)
    else:
        df_pred = df.copy()

    # Extract features
    X, row_indices, col_names, profiles = extract_dataset_features(
        df_pred, max_rows=MAX_PREDICT_ROWS,
    )

    # IF anomaly scores → calibrated confidence
    decision_scores = model.decision_function(X)
    confidence      = _to_confidence(decision_scores, score_p50, score_p5)

    # Rule-based issue type per cell (for human-readable labels only)
    issue_types = []
    for ridx, col in zip(row_indices, col_names):
        val = df_pred.loc[ridx, col]
        itype = _display_issue_type(val, profiles[col])
        issue_types.append(itype)

    # ── Per-column aggregates ─────────────────────────────────────────────
    col_results = {}
    for col in df.columns:
        mask  = np.array([c == col for c in col_names])
        if not mask.any():
            continue
        cp    = confidence[mask]
        ctypes = [t for t, m in zip(issue_types, mask) if m]

        col_results[col] = {
            'mean_confidence':  float(cp.mean()),
            'max_confidence':   float(cp.max()),
            'p90_confidence':   float(np.percentile(cp, 90)),
            'issue_rate':       float((cp > 0.5).mean()),
            'issue_count':      int((cp > 0.5).sum()),
            'total_cells':      int(len(cp)),
            'dominant_issue':   _dominant_issue(ctypes),
            'confidence_level': _confidence_level(float(cp.mean())),
        }

    # ── Top suspicious cells (skip NULLs) ────────────────────────────────
    sorted_idx = np.argsort(confidence)[::-1]
    top_cells  = []
    seen       = set()
    for i in sorted_idx:
        if confidence[i] < 0.50 or len(top_cells) >= 50:
            break
        key = (row_indices[i], col_names[i])
        if key in seen:
            continue
        seen.add(key)
        val = df_pred.loc[row_indices[i], col_names[i]]
        if pd.isna(val):
            continue   # NULLs handled by the UI missing-values panel
        top_cells.append({
            'row_index':    int(row_indices[i]),
            'col':          col_names[i],
            'value':        str(val),
            'confidence':   float(confidence[i]),
            'level':        _confidence_level(float(confidence[i])),
            'issue_type':   issue_types[i],
        })

    # ── Dataset-level summary ─────────────────────────────────────────────
    overall_confidence = float(confidence.mean())
    overall_issue_rate = float((confidence > 0.5).mean())
    quality_score      = round(max(0.0, (1.0 - overall_confidence)) * 100, 1)

    return {
        'quality_score':        quality_score,
        'overall_confidence':   overall_confidence,
        'overall_issue_rate':   overall_issue_rate,
        'total_cells_analyzed': int(len(confidence)),
        'column_results':       col_results,
        'top_issue_cells':      top_cells,
        'feature_importances':  {},   # IF does not expose feature importances
    }


# ---------------------------------------------------------------------------
# Per-row anomaly grouping  (used by the Row Issues tab)
# ---------------------------------------------------------------------------
def get_suspicious_rows(
    df: pd.DataFrame,
    cell_threshold: float = 0.50,   # min confidence to include a cell as an issue
    row_threshold:  float = 0.65,   # min MAX-cell confidence to include a row at all
    max_rows: int = 500,
) -> dict:
    """
    Run the Isolation Forest on df and return anomalous cells grouped by row.

    A row is included only if its most-suspicious cell exceeds row_threshold.
    This prevents wide datasets (many columns) from flooding the view with
    rows that only have borderline LOW-confidence cells.

    Returns a dict compatible with the Row Issues tab:
      total_rows        – rows in the (possibly sampled) dataset
      total_issue_rows  – rows whose max cell confidence >= row_threshold
      shown_rows        – rows actually returned (capped at max_rows)
      columns           – display column list (first 30)
      issue_rows        – list of row dicts sorted by max_confidence desc
    """
    if not os.path.exists(MODEL_PATH):
        raise FileNotFoundError('Model not trained yet. POST /api/ml/train first.')

    model = joblib.load(MODEL_PATH)

    score_p50, score_p5 = 0.1677, 0.0
    if os.path.exists(METRICS_PATH):
        with open(METRICS_PATH) as f:
            m = json.load(f)
        score_p50 = m.get('score_p50', score_p50)
        score_p5  = m.get('score_p5',  score_p5)

    if len(df) > MAX_PREDICT_ROWS:
        df_pred = df.sample(n=MAX_PREDICT_ROWS, random_state=42)
    else:
        df_pred = df.copy()

    X, row_indices, col_names, profiles = extract_dataset_features(
        df_pred, max_rows=MAX_PREDICT_ROWS,
    )
    decision_scores = model.decision_function(X)
    confidence      = _to_confidence(decision_scores, score_p50, score_p5)

    # Group suspicious (non-null) cells by row (only cells >= cell_threshold)
    row_cells: dict = {}
    for i in range(len(confidence)):
        if confidence[i] < cell_threshold:
            continue
        ridx = row_indices[i]
        col  = col_names[i]
        val  = df_pred.loc[ridx, col]
        if pd.isna(val):
            continue
        itype = _display_issue_type(val, profiles[col])
        row_cells.setdefault(ridx, []).append({
            'col':        col,
            'confidence': float(confidence[i]),
            'level':      _confidence_level(float(confidence[i])),
            'issue_type': itype,
            'value':      str(val),
        })

    # Keep only rows whose best cell exceeds row_threshold, then sort by it
    sorted_rows = sorted(
        [(ridx, cells) for ridx, cells in row_cells.items()
         if max(c['confidence'] for c in cells) >= row_threshold],
        key=lambda x: max(c['confidence'] for c in x[1]),
        reverse=True,
    )

    display_cols = list(df_pred.columns[:30])
    issue_rows = []
    for ridx, issues in sorted_rows[:max_rows]:
        row = df_pred.loc[ridx]
        row_data = {}
        for col in display_cols:
            v = row[col]
            row_data[col] = None if pd.isna(v) else str(v)
        issue_rows.append({
            'row_index':      int(ridx),
            'data':           row_data,
            'issues':         sorted(issues, key=lambda c: -c['confidence']),
            'max_confidence': float(max(c['confidence'] for c in issues)),
            'issue_count':    len(issues),
        })

    return {
        'total_rows':       int(len(df_pred)),
        'total_issue_rows': len(sorted_rows),   # rows passing row_threshold
        'shown_rows':       len(issue_rows),
        'columns':          display_cols,
        'issue_rows':       issue_rows,
    }
