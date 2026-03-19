"""
feature_extractor.py
Extracts a fixed-length feature vector for each cell in a dataframe.
Features are designed to be schema-agnostic: they capture quality signals
(null, sentinel, corrupt chars, type mismatch, statistical outlier, etc.)
using only the cell value and its column's own statistics.
"""

import re
import numpy as np
import pandas as pd
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SENTINEL_SET = {
    'missing', 'unknown', 'n/a', 'null', '?', '-', 'na', 'none', 'nan',
    '', 'undefined', 'n.a.', 'not available', 'nil', '#n/a', '#null!',
}

CORRUPT_RE = re.compile(r'[@#$%!*&\t]')
MIXED_NUM_SPECIAL_RE = re.compile(r'(?=.*\d)(?=.*[@#$%!*&])')
NON_ASCII_RE = re.compile(r'[^\x00-\x7F]')
DATE_RE = re.compile(
    r'^\d{4}-\d{2}-\d{2}|'
    r'^\d{2}[./]\d{2}[./]\d{4}|'
    r'^\d{2}-\d{2}-\d{4}'
)

FEATURE_NAMES = [
    # ── Cell-level raw signals ────────────────────────────────────────────
    # Removed from features:
    #   is_null / is_sentinel / is_empty_str  → trivially detectable, UI handles them
    #   col_null_rate / col_sentinel_rate / col_corrupt_rate → dataset-level leakage
    #   type_mismatch  → DIRECT encoding of the label (col_is_numeric & not parseable)
    #   is_outlier     → DIRECT encoding of the label (|z|>3.5, same as |z|>4.5 label)
    # Keeping only INDIRECT / continuous signals that the model must combine
    # to infer structural errors, avoiding trivial label memorisation.
    'has_corrupt_chars',        # contains @#$%!*& or non-printable
    'has_non_ascii',            # contains chars outside ASCII range
    'has_mixed_num_special',    # digits mixed with special chars (e.g. "73.9@")
    'is_numeric_parseable',     # can the cell be parsed as a float?
    'is_date_str',              # does it look like a date string?
    'str_length',               # raw character count
    'z_score',                  # continuous numeric z-score, clipped [-6, 6]
    'length_deviation',         # |len - modal column length|
    'length_z_score',           # (len - mean_len) / std_len, clipped [-5, 5]
    'matches_col_pattern',      # cell structure matches dominant column pattern
    # ── Column-level type context ─────────────────────────────────────────
    'col_numeric_rate',         # fraction of column parseable as numeric
    'col_unique_rate',          # cardinality ratio (low = categorical)
    'col_is_numeric',           # column inferred as numeric
    'col_is_date',              # column inferred as date
    'col_has_pattern',          # column has a dominant structural pattern
]

N_FEATURES = len(FEATURE_NAMES)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def try_float(val: Any) -> Optional[float]:
    try:
        return float(str(val).replace(',', '.'))
    except (ValueError, TypeError):
        return None


def is_date_str(val: str) -> bool:
    return bool(DATE_RE.match(val.strip()))


def has_corrupt_chars(val: str) -> bool:
    if CORRUPT_RE.search(val):
        return True
    if NON_ASCII_RE.search(val):
        return True
    return False


def to_pattern(s: str) -> str:
    """Reduce a string to a structural pattern (digits→#, letters→A)."""
    s = re.sub(r'\d+', '#', s)
    s = re.sub(r'[A-Za-z]+', 'A', s)
    return s


# ---------------------------------------------------------------------------
# Column profile
# ---------------------------------------------------------------------------
class ColumnProfile:
    """Lightweight statistics about a column, used as context for cell features."""

    def __init__(self, series: pd.Series):
        self.name = str(series.name)
        total = len(series)

        # Convert to plain object for uniform handling
        str_series = series.dropna().astype(str).str.strip()

        # Null / sentinel / corrupt rates
        self.null_rate    = float(series.isna().sum() / max(total, 1))
        self.sentinel_rate = float(str_series.str.lower().isin(SENTINEL_SET).sum() / max(total, 1))
        self.corrupt_rate  = float(str_series.apply(has_corrupt_chars).sum() / max(total, 1))

        # How many non-null values parse as numeric
        numeric_parsed = pd.to_numeric(str_series, errors='coerce')
        self.numeric_rate = float(numeric_parsed.notna().sum() / max(len(str_series), 1))

        # String length stats
        lengths = str_series.str.len()
        self.mean_length  = float(lengths.mean()) if len(lengths) > 0 else 0.0
        self.std_length   = float(lengths.std())  if len(lengths) > 1 else 1.0
        self.modal_length = int(lengths.mode().iloc[0]) if len(lengths) > 0 else 0
        if self.std_length < 0.1:
            self.std_length = 1.0

        # Numeric stats (for z-score)
        clean_num = numeric_parsed.dropna()
        self.num_mean = float(clean_num.mean()) if len(clean_num) > 0 else 0.0
        self.num_std  = float(clean_num.std())  if len(clean_num) > 1 else 1.0
        if self.num_std < 1e-6:
            self.num_std = 1.0

        # Inferred type
        if self.numeric_rate > 0.8:
            self.inferred_type = 'numeric'
        elif len(str_series) > 0 and str_series.apply(is_date_str).mean() > 0.5:
            self.inferred_type = 'date'
        elif total > 0 and series.nunique() / total < 0.05:
            self.inferred_type = 'categorical'
        else:
            self.inferred_type = 'string'

        # Unique rate
        self.unique_rate = float(series.nunique() / max(total, 1))

        # Dominant pattern (for ID-like columns)
        if len(str_series) >= 5:
            patterns = str_series.apply(to_pattern)
            top_count = patterns.value_counts().iloc[0]
            self.has_pattern = (top_count / max(len(str_series), 1)) > 0.65
            self.dominant_pattern = patterns.mode().iloc[0]
        else:
            self.has_pattern = False
            self.dominant_pattern = ''


# ---------------------------------------------------------------------------
# Cell feature extraction
# ---------------------------------------------------------------------------
def extract_cell_features(value: Any, profile: ColumnProfile) -> list:
    """Return a feature vector (list of floats) for a single cell."""

    is_null = int(pd.isna(value))

    if is_null:
        str_val = ''
        float_val = None
    else:
        str_val = str(value).strip()
        float_val = try_float(str_val)

    # -- Cell features --
    f_is_null            = is_null
    f_is_empty           = int(str_val == '' and not is_null)
    f_is_sentinel        = int(str_val.lower() in SENTINEL_SET)
    f_has_corrupt        = int(has_corrupt_chars(str_val)) if str_val else 0
    f_has_non_ascii      = int(bool(NON_ASCII_RE.search(str_val))) if str_val else 0
    f_has_mixed          = int(bool(MIXED_NUM_SPECIAL_RE.search(str_val))) if str_val else 0
    f_is_numeric         = int(float_val is not None and not is_null)
    f_is_date            = int(is_date_str(str_val)) if str_val else 0
    f_str_len            = len(str_val)

    f_type_mismatch = int(
        profile.inferred_type == 'numeric'
        and float_val is None
        and not is_null
        and not f_is_sentinel
    )

    # Z-score / outlier
    if float_val is not None and profile.inferred_type == 'numeric':
        z = (float_val - profile.num_mean) / profile.num_std
        f_z_score   = float(max(-6.0, min(6.0, z)))
        f_is_outlier = int(abs(z) > 3.5)
    else:
        f_z_score    = 0.0
        f_is_outlier = 0

    # String length deviation
    f_len_dev = float(abs(f_str_len - profile.modal_length))
    f_len_z   = float(max(-5.0, min(5.0,
        (f_str_len - profile.mean_length) / profile.std_length
    )))

    # Pattern match
    if profile.has_pattern and str_val and not is_null:
        f_pattern_match = int(to_pattern(str_val) == profile.dominant_pattern)
    else:
        f_pattern_match = 1  # unknown → assume ok

    # -- Column-level context --
    f_col_null_rate    = profile.null_rate
    f_col_sentinel_rate = profile.sentinel_rate
    f_col_corrupt_rate  = profile.corrupt_rate
    f_col_numeric_rate  = profile.numeric_rate
    f_col_unique_rate   = profile.unique_rate
    f_col_is_numeric    = int(profile.inferred_type == 'numeric')
    f_col_is_date       = int(profile.inferred_type == 'date')
    f_col_has_pattern   = int(profile.has_pattern)

    return [
        f_has_corrupt, f_has_non_ascii, f_has_mixed,
        f_is_numeric, f_is_date, f_str_len,
        f_z_score, f_len_dev, f_len_z, f_pattern_match,
        f_col_numeric_rate, f_col_unique_rate,
        f_col_is_numeric, f_col_is_date, f_col_has_pattern,
    ]


# ---------------------------------------------------------------------------
# Dataset-level extraction
# ---------------------------------------------------------------------------
def extract_dataset_features(
    df: pd.DataFrame,
    max_rows: int = 5000,
) -> tuple:
    """
    Extract features for all cells (sampled to max_rows).

    Returns:
        X            – np.ndarray shape (n_cells, N_FEATURES)
        row_indices  – list of original df index values, length n_cells
        col_names    – list of column names, length n_cells
        profiles     – dict {col: ColumnProfile}
    """
    if len(df) > max_rows:
        df = df.sample(n=max_rows, random_state=42)

    all_features   = []
    all_row_indices = []
    all_col_names   = []
    profiles        = {}

    for col in df.columns:
        profile = ColumnProfile(df[col])
        profiles[col] = profile
        for idx, val in df[col].items():
            all_features.append(extract_cell_features(val, profile))
            all_row_indices.append(idx)
            all_col_names.append(col)

    X = np.array(all_features, dtype=np.float32)
    return X, all_row_indices, all_col_names, profiles
