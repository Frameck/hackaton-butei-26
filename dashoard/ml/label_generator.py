"""
label_generator.py
Rule-based cell labelling used to create training targets.

For clean datasets  → label 0 (no issue) for every cell.
For dirty datasets  → label 1 (issue) when a rule fires, else 0.

The ML model then learns the combination of features that predicts these labels,
producing calibrated confidence scores (probabilities) rather than hard flags.
"""

import numpy as np
import pandas as pd

from .feature_extractor import (
    SENTINEL_SET,
    has_corrupt_chars,
    try_float,
    ColumnProfile,
)

# Issue type identifiers (used for human-readable output, not for training)
ISSUE_NULL       = 'null'
ISSUE_SENTINEL   = 'sentinel'
ISSUE_CORRUPT    = 'corrupt_chars'
ISSUE_TYPE_ERR   = 'type_mismatch'
ISSUE_OUTLIER    = 'outlier'
ISSUE_CLEAN      = 'clean'


# ---------------------------------------------------------------------------
# Single-cell labelling
# ---------------------------------------------------------------------------
def label_cell(value, profile: ColumnProfile) -> tuple:
    """
    Returns (label: int, issue_type: str)
      label 0 → clean
      label 1 → issue

    Only structural errors are labelled as issues:
      - type_mismatch : a non-null, non-sentinel value that cannot be parsed
                        as the expected type of the column
      - outlier       : a numeric value > 4.5 std-devs from the column mean

    NULL and sentinel values are deliberately NOT labelled as issues here.
    They are handled by deterministic UI rules and must not leak into ML
    training (is_null / is_sentinel were direct label sources → ROC-AUC=1).
    Corrupt-char flags are kept as features but removed from labels because
    their presence is highly context-dependent (e.g. email addresses).
    """
    # NULL / sentinel / empty → clean from ML perspective
    if pd.isna(value):
        return 0, ISSUE_CLEAN

    str_val = str(value).strip()

    if str_val.lower() in SENTINEL_SET or str_val == '':
        return 0, ISSUE_CLEAN

    # Type mismatch: value in a numeric column that cannot be parsed as number
    if profile.inferred_type == 'numeric':
        fv = try_float(str_val)
        if fv is None:
            return 1, ISSUE_TYPE_ERR
        # Statistical outlier (extreme z-score)
        z = (fv - profile.num_mean) / profile.num_std
        if abs(z) > 4.5:
            return 1, ISSUE_OUTLIER

    return 0, ISSUE_CLEAN


# ---------------------------------------------------------------------------
# Dataset-level labelling
# ---------------------------------------------------------------------------
def label_dataset(
    df: pd.DataFrame,
    is_dirty: bool,
    max_rows: int = 5000,
) -> tuple:
    """
    Assign a binary label to every cell (sampled to max_rows rows).

    Returns:
        labels      – np.ndarray int32, shape (n_cells,)
        issue_types – list of str, length n_cells
    """
    if len(df) > max_rows:
        df = df.sample(n=max_rows, random_state=42)

    labels      = []
    issue_types = []

    for col in df.columns:
        profile = ColumnProfile(df[col])
        for _, val in df[col].items():
            if is_dirty:
                lbl, itype = label_cell(val, profile)
            else:
                lbl, itype = 0, ISSUE_CLEAN
            labels.append(lbl)
            issue_types.append(itype)

    return np.array(labels, dtype=np.int32), issue_types
