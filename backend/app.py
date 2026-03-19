"""
Data Quality Dashboard — Flask Backend
Analyzes all datasets in mock_databases/ and exposes a REST API.
"""

import os
import re
import sys
import logging
import time

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), "backend.log")),
    ],
)
log = logging.getLogger(__name__)

# Load .env if present
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.isfile(_env_path):
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                os.environ.setdefault(_k.strip(), _v.strip())
import json
import math
import threading
import numpy as np
import pandas as pd
import pdfplumber
from flask import Flask, Response, request
from flask_cors import CORS

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
MOCK_DB    = os.path.normpath(os.path.join(BASE_DIR, "..", "mock_databases"))
CLEAN_DIR  = os.path.normpath(os.path.join(BASE_DIR, "..", "train_datasets"))
ML_DIR     = os.path.normpath(os.path.join(BASE_DIR, "..", "ml"))

# Add ml/ to path so we can import the ml package
if ML_DIR not in sys.path:
    sys.path.insert(0, os.path.dirname(ML_DIR))

app = Flask(__name__)
CORS(app)

# ---------------------------------------------------------------------------
# Safe JSON serialisation – handles numpy scalars, nan, inf
# ---------------------------------------------------------------------------
def _safe_convert(obj):
    """Recursively convert numpy types and nan/inf to JSON-safe Python types."""
    if isinstance(obj, dict):
        return {k: _safe_convert(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_safe_convert(i) for i in obj]
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        f = float(obj)
        return None if (math.isnan(f) or math.isinf(f)) else f
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, np.ndarray):
        return [_safe_convert(i) for i in obj.tolist()]
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    return obj


def safe_jsonify(data):
    """Drop-in replacement for flask.jsonify that handles numpy / nan."""
    return Response(
        json.dumps(_safe_convert(data), ensure_ascii=False),
        mimetype="application/json",
    )

# ---------------------------------------------------------------------------
# PDF parsing (from pdf_to_db.py)
# ---------------------------------------------------------------------------
RE_DATE      = re.compile(r"Date:\s*(\S+)")
RE_SHIFT     = re.compile(r"Shift:\s*(.+)")
RE_PATIENT   = re.compile(r"Patient ID:\s*(\S+)")
RE_CASE      = re.compile(r"Case ID:\s*(\S+)")
RE_WARD      = re.compile(r"Ward:\s*(.+)")
RE_EVAL      = re.compile(r"Evaluation:\s*", re.IGNORECASE)
RE_SHIFT_LBL = re.compile(r"(Fr.hdienst|Sp.tdienst|Nachtdienst):\s*", re.IGNORECASE)
RE_INTERV    = re.compile(r"Interventions?:\s*", re.IGNORECASE)


def parse_pdf_page(page_text: str, page_num: int) -> dict:
    lines = [l.strip() for l in page_text.splitlines() if l.strip()]
    record = {
        "page": page_num, "date": None, "shift": None,
        "patient_id": None, "case_id": None, "ward": None,
        "observations": [], "interventions": [], "evaluation": None,
    }
    for line in lines:
        if not record["date"]:
            m = RE_DATE.search(line)
            if m: record["date"] = m.group(1)
        if not record["shift"]:
            m = RE_SHIFT.search(line)
            if m: record["shift"] = m.group(1).strip()
        if not record["patient_id"]:
            m = RE_PATIENT.search(line)
            if m: record["patient_id"] = m.group(1)
        if not record["case_id"]:
            m = RE_CASE.search(line)
            if m: record["case_id"] = m.group(1)
        if not record["ward"]:
            m = RE_WARD.search(line)
            if m: record["ward"] = m.group(1).strip()

    full_text  = " ".join(lines)
    body_match = RE_SHIFT_LBL.search(full_text)
    if body_match:
        body        = full_text[body_match.start():]
        body_no_lbl = RE_SHIFT_LBL.sub("", body, count=1)
        interv_m    = RE_INTERV.search(body_no_lbl)
        eval_m      = RE_EVAL.search(body_no_lbl)
        if interv_m and eval_m:
            obs_text    = body_no_lbl[:interv_m.start()].strip().rstrip(",")
            interv_text = body_no_lbl[interv_m.end():eval_m.start()].strip().rstrip(",")
            record["evaluation"] = body_no_lbl[eval_m.end():].strip()
        elif interv_m:
            obs_text    = body_no_lbl[:interv_m.start()].strip().rstrip(",")
            interv_text = body_no_lbl[interv_m.end():].strip().rstrip(",")
        elif eval_m:
            obs_text    = body_no_lbl[:eval_m.start()].strip().rstrip(",")
            interv_text = ""
            record["evaluation"] = body_no_lbl[eval_m.end():].strip()
        else:
            obs_text    = body_no_lbl.strip()
            interv_text = ""
        record["observations"]  = [o.strip() for o in obs_text.split(",")    if o.strip()]
        record["interventions"] = [i.strip() for i in interv_text.split(",") if i.strip()]
    return record


def load_pdf(filepath: str) -> pd.DataFrame:
    records = []
    with pdfplumber.open(filepath) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            if not text.strip():
                continue
            rec = parse_pdf_page(text, i)
            records.append({
                "page":          rec["page"],
                "date":          rec["date"],
                "shift":         rec["shift"],
                "patient_id":    rec["patient_id"],
                "case_id":       rec["case_id"],
                "ward":          rec["ward"],
                "observations":  "; ".join(rec["observations"]),
                "interventions": "; ".join(rec["interventions"]),
                "evaluation":    rec["evaluation"],
            })
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# CSV / XLSX loaders
# ---------------------------------------------------------------------------
SENTINEL_VALUES = {"missing", "unknown", "n/a", "null", "?", "-", "na", "none", "nan", ""}

def detect_separator(filepath: str) -> str:
    """Read the first line and pick the most likely delimiter."""
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        first = f.readline()
    counts = {sep: first.count(sep) for sep in [",", ";", "\t", "|"]}
    return max(counts, key=counts.get)


def load_csv(filepath: str) -> pd.DataFrame:
    sep = detect_separator(filepath)
    for enc in ("utf-8", "latin-1"):
        try:
            return pd.read_csv(filepath, sep=sep, encoding=enc,
                               low_memory=False, on_bad_lines="skip")
        except UnicodeDecodeError:
            continue
    # Last resort
    return pd.read_csv(filepath, sep=sep, encoding="latin-1",
                       low_memory=False, on_bad_lines="skip")


def _detect_file_format(filepath: str) -> str:
    """
    Inspect the first bytes to determine the real file format,
    ignoring the extension.
    XLSX (OpenXML) → ZIP archive   → b'PK\\x03\\x04'
    XLS  (BIFF)    → OLE2 compound → b'\\xD0\\xCF\\x11\\xE0'
    Anything else  → treat as text (CSV/TSV)
    """
    with open(filepath, "rb") as f:
        magic = f.read(8)
    if magic[:4] == b"PK\x03\x04":
        return "xlsx"
    if magic[:8] == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1":
        return "xls"
    return "text"


def load_xlsx(filepath: str) -> pd.DataFrame:
    fmt = _detect_file_format(filepath)
    if fmt == "text":
        # File has .xlsx/.xls extension but is actually plain-text (CSV/TSV)
        return load_csv(filepath)

    engine = "openpyxl" if fmt == "xlsx" else "xlrd"
    df = pd.read_excel(filepath, engine=engine)

    # Edge case: real XLSX but data was pasted as CSV in a single cell
    if df.shape[1] == 1 and "," in str(df.columns[0]):
        import io
        header = df.columns[0]
        rows   = df.iloc[:, 0].astype(str).tolist()
        text   = header + "\n" + "\n".join(rows)
        return pd.read_csv(io.StringIO(text), low_memory=False, on_bad_lines="skip")
    return df


def clean_sentinels(df: pd.DataFrame) -> pd.DataFrame:
    """Replace all sentinel placeholder strings with NaN (proper NULL)."""
    df = df.copy()
    for col in df.select_dtypes(include="object").columns:
        mask = df[col].astype(str).str.strip().str.lower().isin(SENTINEL_VALUES)
        df.loc[mask, col] = np.nan
    return df


def load_dataset(filepath: str) -> pd.DataFrame:
    ext = os.path.splitext(filepath)[1].lower()
    if ext == ".pdf":
        df = load_pdf(filepath)
    elif ext == ".csv":
        df = load_csv(filepath)
    elif ext in (".xlsx", ".xls"):
        df = load_xlsx(filepath)
    else:
        raise ValueError(f"Unsupported extension: {ext}")
    return clean_sentinels(df)


# ---------------------------------------------------------------------------
# Column analysis
# ---------------------------------------------------------------------------
RE_HAS_ALPHA_OR_SPECIAL = re.compile(r"[A-Za-z@#$%!*&]")
RE_HAS_DIGIT = re.compile(r"\d")

def is_sentinel(val) -> bool:
    if pd.isna(val):
        return False  # already null, counted separately
    return str(val).strip().lower() in SENTINEL_VALUES


def _sentinel_mask(series: pd.Series) -> pd.Series:
    if str(series.dtype) != "object":
        return pd.Series(False, index=series.index)
    return (
        series
        .fillna("")
        .astype(str)
        .str.strip()
        .str.lower()
        .isin(SENTINEL_VALUES)
        & series.notna()
    )


def _garbled_numeric_mask(series: pd.Series) -> pd.Series:
    if str(series.dtype) != "object":
        return pd.Series(False, index=series.index)
    text = series.fillna("").astype(str)
    return (
        text.str.contains(RE_HAS_ALPHA_OR_SPECIAL, regex=True)
        & text.str.contains(RE_HAS_DIGIT, regex=True)
        & series.notna()
    )


def infer_type(series: pd.Series, dtype) -> str:
    """Best-guess semantic type for a column."""
    dname = str(dtype)
    if "int"   in dname: return "integer"
    if "float" in dname: return "float"
    if "bool"  in dname: return "boolean"
    if "datetime" in dname: return "datetime"
    # object → try to guess
    sample = series.dropna().astype(str)
    if len(sample) == 0:
        return "unknown"
    numeric_ok = pd.to_numeric(sample, errors="coerce").notna().sum()
    ratio = numeric_ok / len(sample)
    if ratio > 0.8:
        return "numeric (as string)"
    if ratio > 0.3:
        return "mixed"
    # date heuristic
    if sample.str.match(r"\d{4}-\d{2}-\d{2}").mean() > 0.5:
        return "date string"
    return "string"


def detect_wrong_type(series: pd.Series, dtype) -> tuple[bool, str]:
    """
    Returns (wrong_type_bool, reason).
    wrong_type is True when an object column looks numeric or has garbled values.
    """
    if str(dtype) != "object":
        return False, ""
    sample = series.dropna().astype(str)
    if len(sample) == 0:
        return False, ""
    garbled = (
        sample.str.contains(RE_HAS_ALPHA_OR_SPECIAL, regex=True)
        & sample.str.contains(RE_HAS_DIGIT, regex=True)
    ).sum()
    garbled_ratio = garbled / len(sample)

    numeric_ok = pd.to_numeric(sample, errors="coerce").notna().sum()
    ratio = numeric_ok / len(sample)

    if garbled_ratio > 0.01:
        return True, "values contain non-numeric characters mixed with digits"
    if ratio > 0.8:
        return True, "numeric stored as string"
    if ratio > 0.3:
        return True, "mixed types"
    return False, ""


def analyze_column(series: pd.Series) -> dict:
    total      = len(series)
    null_count = int(series.isna().sum())
    non_null   = series.dropna()
    sentinel_c = int(non_null.apply(is_sentinel).sum())
    missing_total = null_count + sentinel_c
    missing_pct   = round(missing_total / total * 100, 2) if total > 0 else 0.0
    completeness  = round((total - missing_total) / total * 100, 2) if total > 0 else 0.0
    unique_count  = int(series.nunique(dropna=True))

    wrong_type, wt_reason = detect_wrong_type(series, series.dtype)
    inf_type = infer_type(series, series.dtype)

    return {
        "name":            series.name,
        "dtype":           str(series.dtype),
        "inferred_type":   inf_type,
        "missing_null":    null_count,
        "sentinel_count":  sentinel_c,
        "missing_total":   missing_total,
        "missing_pct":     missing_pct,
        "completeness_pct": completeness,
        "unique_count":    unique_count,
        "wrong_type":      wrong_type,
        "wrong_type_reason": wt_reason,
    }


# ---------------------------------------------------------------------------
# Dataset summary
# ---------------------------------------------------------------------------
def dataset_summary(name: str, df: pd.DataFrame) -> dict:
    rows, cols = df.shape
    total_cells  = rows * cols
    # null + sentinel per cell
    null_total = int(df.isna().sum().sum())
    sent_total = 0
    for col in df.columns:
        sent_total += int(df[col].dropna().apply(is_sentinel).sum())
    missing_total = null_total + sent_total
    completeness  = round((total_cells - missing_total) / total_cells * 100, 2) if total_cells > 0 else 0.0

    # wrong type columns
    wt_count = 0
    for col in df.columns:
        wt, _ = detect_wrong_type(df[col], df[col].dtype)
        if wt:
            wt_count += 1

    ext = os.path.splitext(name)[1].upper().lstrip(".")
    return {
        "name":            name,
        "type":            ext if ext else "UNKNOWN",
        "rows":            rows,
        "columns":         cols,
        "completeness":    completeness,
        "missing_total":   missing_total,
        "wrong_type_count": wt_count,
    }


def dataset_detail(name: str, df: pd.DataFrame) -> dict:
    summary = dataset_summary(name, df)
    col_details = [analyze_column(df[c]) for c in df.columns]

    # Preview: first 20 rows, max 30 columns
    preview_df = df.iloc[:20, :30].copy()

    # Build sentinel set for highlighting
    sentinel_cells = {}
    for col in preview_df.columns:
        for idx, val in preview_df[col].items():
            if pd.isna(val):
                sentinel_cells[f"{idx}_{col}"] = "null"
            elif is_sentinel(val):
                sentinel_cells[f"{idx}_{col}"] = "sentinel"

    # Wrong type cells
    wrong_type_cols = {c["name"] for c in col_details if c["wrong_type"]}

    # Convert preview to JSON-safe list of dicts
    preview_rows = []
    for _, row in preview_df.iterrows():
        r = {}
        for col in preview_df.columns:
            v = row[col]
            if pd.isna(v):
                r[col] = None
            elif isinstance(v, (np.integer,)):
                r[col] = int(v)
            elif isinstance(v, (np.floating,)):
                fv = float(v)
                r[col] = None if (math.isnan(fv) or math.isinf(fv)) else fv
            else:
                r[col] = str(v)
        preview_rows.append(r)

    preview_columns = list(preview_df.columns[:30])

    return {
        **summary,
        "column_details":  col_details,
        "preview_columns": preview_columns,
        "preview_rows":    preview_rows,
        "wrong_type_cols": list(wrong_type_cols),
    }


# ---------------------------------------------------------------------------
# Cache (lazy load)
# ---------------------------------------------------------------------------
_cache: dict = {}


def get_dataset(name: str) -> pd.DataFrame:
    if name not in _cache:
        path = os.path.join(MOCK_DB, name)
        if not os.path.isfile(path):
            return None
        _cache[name] = load_dataset(path)
    return _cache[name]


def list_dataset_files() -> list:
    if not os.path.isdir(MOCK_DB):
        return []
    return [f for f in os.listdir(MOCK_DB)
            if os.path.splitext(f)[1].lower() in {".csv", ".xlsx", ".xls", ".pdf"}]


def _coerce_json_cell(v):
    if pd.isna(v):
        return None
    if isinstance(v, np.integer):
        return int(v)
    if isinstance(v, np.floating):
        fv = float(v)
        return None if (math.isnan(fv) or math.isinf(fv)) else fv
    return str(v)


def _collect_issue_analysis(
    df: pd.DataFrame,
    row_limit: int = 500,
    display_col_limit: int = 30,
    issue_limit_per_row: int | None = None,
    prioritize: bool = False,
) -> dict:
    empty_mask = pd.Series(False, index=df.index)
    wt_cols = {col for col in df.columns if detect_wrong_type(df[col], df[col].dtype)[0]}
    sentinel_by_col = {
        col: _sentinel_mask(df[col])
        for col in df.select_dtypes(include="object").columns
    }
    garbled_by_col = {
        col: _garbled_numeric_mask(df[col])
        for col in wt_cols
        if str(df[col].dtype) == "object"
    }

    null_mask = df.isna().any(axis=1)

    sentinel_mask = empty_mask.copy()
    for mask in sentinel_by_col.values():
        sentinel_mask |= mask.reindex(df.index, fill_value=False)

    wt_cell_mask = empty_mask.copy()
    for mask in garbled_by_col.values():
        wt_cell_mask |= mask.reindex(df.index, fill_value=False)

    pid_col = find_patient_id_col(df)
    pid_stats = _analyze_patient_id(df, pid_col) if pid_col else {
        "invalid_indices": [],
        "invalid_count": 0,
        "dominant_pattern": "",
        "has_pattern": False,
        "reasons": {},
    }
    pid_invalid_set = set(pid_stats["invalid_indices"])
    pid_mask = pd.Series(df.index.isin(pid_invalid_set), index=df.index)

    has_issue = null_mask | sentinel_mask | wt_cell_mask | pid_mask
    issue_index = pd.Index(df.index[has_issue])
    total_issue_rows = int(len(issue_index))

    if prioritize and len(issue_index) > 0:
        score = (
            df.isna().sum(axis=1).astype(int)
            + pid_mask.astype(int) * 4
            + sentinel_mask.astype(int) * 3
            + wt_cell_mask.astype(int) * 5
        )
        ordered = score.loc[issue_index].sort_values(ascending=False, kind="stable")
        selected_indices = list(ordered.head(row_limit).index)
    else:
        selected_indices = list(issue_index[:row_limit])

    display_cols = list(df.columns[:display_col_limit])
    issue_rows = []
    for idx in selected_indices:
        row = df.loc[idx]
        issues = []
        for col in df.columns:
            if pd.isna(row[col]):
                issues.append({"col": col, "type": "null"})
            elif col in sentinel_by_col and bool(sentinel_by_col[col].get(idx, False)):
                issues.append({"col": col, "type": "sentinel", "value": str(row[col])})
            elif col in garbled_by_col and bool(garbled_by_col[col].get(idx, False)):
                issues.append({"col": col, "type": "wrong_type", "value": str(row[col])})
        if pid_col and idx in pid_invalid_set and not any(i["col"] == pid_col for i in issues):
            pid_val = row[pid_col]
            if pd.isna(pid_val):
                invalid_reason = "missing"
            else:
                pid_str = str(pid_val).strip()
                if not pid_str:
                    invalid_reason = "empty"
                elif _has_bad_chars(pid_str):
                    invalid_reason = "corrupt_chars"
                else:
                    invalid_reason = "pattern_mismatch"
            issues.append({
                "col": pid_col,
                "type": "invalid_patient_id",
                "value": None if pd.isna(pid_val) else str(pid_val),
                "reason": invalid_reason,
            })

        if not issues:
            continue

        if issue_limit_per_row is not None:
            issues = issues[:issue_limit_per_row]

        row_data = {col: _coerce_json_cell(row[col]) for col in display_cols}
        issue_rows.append({
            "row_index": int(idx),
            "data": row_data,
            "issues": issues,
            "issue_count": len(issues),
        })

    col_summary = {}
    for col in df.columns:
        null_count = int(df[col].isna().sum())
        sentinel_count = int(sentinel_by_col[col].sum()) if col in sentinel_by_col else 0
        wrong_type = col in wt_cols
        invalid_patient_ids = int(pid_col == col and pid_stats["invalid_count"])
        if null_count or sentinel_count or wrong_type or invalid_patient_ids:
            col_summary[col] = {
                "null_count": null_count,
                "sentinel_count": sentinel_count,
                "wrong_type": wrong_type,
            }
            if invalid_patient_ids:
                col_summary[col]["invalid_patient_ids"] = invalid_patient_ids

    return {
        "total_rows": int(len(df)),
        "total_issue_rows": total_issue_rows,
        "columns": display_cols,
        "issue_rows": issue_rows,
        "col_summary": col_summary,
        "patient_id_col": pid_col,
        "patient_id_stats": pid_stats,
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.route("/api/datasets", methods=["GET"])
def api_datasets():
    files = list_dataset_files()
    summaries = []
    for fname in sorted(files):
        try:
            df = get_dataset(fname)
            if df is None:
                continue
            summaries.append(dataset_summary(fname, df))
        except Exception as e:
            summaries.append({
                "name":    fname,
                "type":    os.path.splitext(fname)[1].upper().lstrip("."),
                "rows":    0,
                "columns": 0,
                "completeness":    0.0,
                "missing_total":   0,
                "wrong_type_count": 0,
                "error": str(e),
            })
    return safe_jsonify(summaries)


@app.route("/api/datasets/<path:name>", methods=["GET"])
def api_dataset_detail(name: str):
    try:
        df = get_dataset(name)
        if df is None:
            return safe_jsonify({"error": "Dataset not found"}), 404
        detail = dataset_detail(name, df)
        return safe_jsonify(detail)
    except Exception as e:
        return safe_jsonify({"error": str(e)}), 500


@app.route("/api/datasets/<path:name>/issues", methods=["GET"])
def api_dataset_issues(name: str):
    try:
        df = get_dataset(name)
        if df is None:
            return safe_jsonify({"error": "Dataset not found"}), 404
        analysis = _collect_issue_analysis(df, row_limit=500, display_col_limit=30)
        return safe_jsonify({
            "total_rows": analysis["total_rows"],
            "total_issue_rows": analysis["total_issue_rows"],
            "shown_rows": len(analysis["issue_rows"]),
            "columns": analysis["columns"],
            "issue_rows": analysis["issue_rows"],
            "col_summary": analysis["col_summary"],
        })
    except Exception as e:
        return safe_jsonify({"error": str(e)}), 500


@app.route("/api/datasets/<path:name>/missing-rows", methods=["GET"])
def api_missing_rows(name: str):
    try:
        df = get_dataset(name)
        if df is None:
            return safe_jsonify({"error": "Dataset not found"}), 404

        # After sentinel cleaning, missing = NaN only
        null_mask = df.isna().any(axis=1)
        total_missing_rows = int(null_mask.sum())

        display_cols = list(df.columns[:30])
        missing_df = df[null_mask].head(500)

        rows_out = []
        for idx in missing_df.index:
            row = df.loc[idx]
            missing_cols = [col for col in df.columns if pd.isna(row[col])]
            row_data = {}
            for col in display_cols:
                v = row[col]
                row_data[col] = None if pd.isna(v) else (
                    int(v) if isinstance(v, np.integer) else
                    (None if isinstance(v, np.floating) and (math.isnan(float(v)) or math.isinf(float(v))) else
                     float(v) if isinstance(v, np.floating) else str(v))
                )
            rows_out.append({
                "row_index": int(idx),
                "data": row_data,
                "missing_cols": missing_cols,
                "missing_count": len(missing_cols),
            })

        return safe_jsonify({
            "total_rows": int(len(df)),
            "total_missing_rows": total_missing_rows,
            "shown_rows": len(rows_out),
            "columns": display_cols,
            "rows": rows_out,
        })
    except Exception as e:
        return safe_jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# PatientID validation
# ---------------------------------------------------------------------------
PATIENT_ID_ALIASES = {"patientid", "patient_id", "patid", "pid"}

_TO_PATTERN_RE_DIGIT  = re.compile(r"\d+")
_TO_PATTERN_RE_LETTER = re.compile(r"[A-Za-z]+")
_BAD_CHARS_RE         = re.compile(r"[@#$%!*&\t]")
_NON_ASCII_RE         = re.compile(r"[^\x00-\x7F]")


def _to_pattern(s: str) -> str:
    s = _TO_PATTERN_RE_DIGIT.sub("#", s)
    s = _TO_PATTERN_RE_LETTER.sub("A", s)
    return s


def _has_bad_chars(s: str) -> bool:
    return bool(_BAD_CHARS_RE.search(s) or _NON_ASCII_RE.search(s))


def find_patient_id_col(df: pd.DataFrame):
    for col in df.columns:
        key = re.sub(r"[\s\-_]", "", col).lower()
        if key in PATIENT_ID_ALIASES:
            return col
    return None


def _analyze_patient_id(df: pd.DataFrame, col: str) -> dict:
    series = df[col]
    non_null = series.dropna().astype(str).str.strip()
    non_null = non_null[non_null != ""]

    # Dominant structural pattern
    dominant_pattern = ""
    has_pattern = False
    if len(non_null) >= 5:
        patterns = non_null.apply(_to_pattern)
        dominant_pattern = patterns.mode().iloc[0]
        has_pattern = (patterns == dominant_pattern).mean() > 0.5

    reasons = {}
    invalid_indices = []
    for idx, val in series.items():
        if pd.isna(val):
            reason = "missing"
        else:
            sv = str(val).strip()
            if not sv:
                reason = "empty"
            elif _has_bad_chars(sv):
                reason = "corrupt_chars"
            elif has_pattern and _to_pattern(sv) != dominant_pattern:
                reason = "pattern_mismatch"
            else:
                continue  # valid
        reasons[reason] = reasons.get(reason, 0) + 1
        invalid_indices.append(idx)

    return {
        "invalid_count":    len(invalid_indices),
        "dominant_pattern": dominant_pattern,
        "has_pattern":      has_pattern,
        "reasons":          reasons,
        "invalid_indices":  invalid_indices,
    }


@app.route("/api/datasets/<path:name>/patient-id", methods=["GET"])
def api_patient_id_stats(name: str):
    try:
        df = get_dataset(name)
        if df is None:
            return safe_jsonify({"error": "Dataset not found"}), 404
        col = find_patient_id_col(df)
        if not col:
            return safe_jsonify({"error": "No PatientID column"}), 404
        stats = _analyze_patient_id(df, col)
        total = int(len(df))
        return safe_jsonify({
            "column_name":      col,
            "total":            total,
            "valid_count":      total - stats["invalid_count"],
            "invalid_count":    stats["invalid_count"],
            "invalid_pct":      round(stats["invalid_count"] / max(total, 1) * 100, 2),
            "dominant_pattern": stats["dominant_pattern"],
            "reasons":          stats["reasons"],
        })
    except Exception as e:
        return safe_jsonify({"error": str(e)}), 500


@app.route("/api/datasets/<path:name>/patient-id/issues", methods=["GET"])
def api_patient_id_issues(name: str):
    try:
        df = get_dataset(name)
        if df is None:
            return safe_jsonify({"error": "Dataset not found"}), 404
        col = find_patient_id_col(df)
        if not col:
            return safe_jsonify({"error": "No PatientID column"}), 404

        stats = _analyze_patient_id(df, col)
        display_cols = [col] + [c for c in list(df.columns[:30]) if c != col]

        rows_out = []
        for idx in stats["invalid_indices"][:500]:
            row = df.loc[idx]
            val = row[col]
            if pd.isna(val):
                reason = "missing"
            else:
                sv = str(val).strip()
                if not sv:
                    reason = "empty"
                elif _has_bad_chars(sv):
                    reason = "corrupt_chars"
                else:
                    reason = "pattern_mismatch"
            row_data = {}
            for c in display_cols:
                v = row[c]
                row_data[c] = None if pd.isna(v) else str(v)
            rows_out.append({
                "row_index":        int(idx),
                "patient_id_value": None if pd.isna(val) else str(val),
                "invalid_reason":   reason,
                "data":             row_data,
            })

        return safe_jsonify({
            "column_name":   col,
            "total":         int(len(df)),
            "invalid_count": stats["invalid_count"],
            "columns":       display_cols,
            "rows":          rows_out,
        })
    except Exception as e:
        return safe_jsonify({"error": str(e)}), 500


@app.route("/api/health", methods=["GET"])
def health():
    return safe_jsonify({"status": "ok", "mock_db": MOCK_DB,
                         "datasets": list_dataset_files()})


# ---------------------------------------------------------------------------
# Lab anomaly detection
# Finds columns that follow the pattern <param>_value / <param>_ref_low / <param>_ref_high
# and flags rows where the value falls outside the reference range.
# Also respects existing <param>_flag columns.
# ---------------------------------------------------------------------------
def _find_lab_triplets(df: pd.DataFrame) -> list[dict]:
    """
    Return list of {param, value_col, low_col, high_col, flag_col} for every
    lab parameter that has at least a value column + one reference bound.
    """
    cols_lower = {c.lower(): c for c in df.columns}
    triplets = []
    seen = set()

    for col_lower, col in cols_lower.items():
        # accept suffixes: _value, _val, _result, _messwert
        for suffix in ("_value", "_val", "_result", "_messwert"):
            if col_lower.endswith(suffix):
                param = col_lower[: -len(suffix)]
                if param in seen:
                    continue
                low_col  = cols_lower.get(param + "_ref_low")  or cols_lower.get(param + "_low")
                high_col = cols_lower.get(param + "_ref_high") or cols_lower.get(param + "_high")
                flag_col = cols_lower.get(param + "_flag")
                if low_col or high_col:
                    seen.add(param)
                    triplets.append({
                        "param":      param,
                        "value_col":  col,
                        "low_col":    low_col,
                        "high_col":   high_col,
                        "flag_col":   flag_col,
                    })
    return triplets


@app.route("/api/datasets/<path:name>/lab-anomalies", methods=["GET"])
def api_lab_anomalies(name: str):
    """
    Detect lab values outside their reference range.
    Returns per-parameter summary + up to 500 flagged rows.
    """
    log.info("lab-anomalies called for: %s", name)
    try:
        df = get_dataset(name)
        if df is None:
            return safe_jsonify({"error": "Dataset not found"}), 404

        triplets = _find_lab_triplets(df)
        if not triplets:
            return safe_jsonify({"has_lab_data": False, "parameters": [], "anomaly_rows": []})

        anomaly_index: dict[int, dict] = {}   # row_idx -> {param -> info}
        param_summaries = []

        for t in triplets:
            val_s  = pd.to_numeric(df[t["value_col"]], errors="coerce")
            low_s  = pd.to_numeric(df[t["low_col"]],  errors="coerce") if t["low_col"]  else None
            high_s = pd.to_numeric(df[t["high_col"]], errors="coerce") if t["high_col"] else None

            below = (low_s  is not None) and (val_s < low_s)
            above = (high_s is not None) and (val_s > high_s)
            anom_mask = (below | above) & val_s.notna()

            count = int(anom_mask.sum())
            param_summaries.append({
                "param":       t["param"],
                "value_col":   t["value_col"],
                "low_col":     t["low_col"],
                "high_col":    t["high_col"],
                "flag_col":    t["flag_col"],
                "anomaly_count": count,
                "total_measured": int(val_s.notna().sum()),
                "anomaly_pct":  round(count / max(int(val_s.notna().sum()), 1) * 100, 1),
            })

            for idx in df[anom_mask].head(200).index:
                direction = "LOW" if (low_s is not None and val_s[idx] < low_s[idx]) else "HIGH"
                entry = {
                    "direction":  direction,
                    "value":      float(val_s[idx]),
                    "ref_low":    float(low_s[idx])  if low_s  is not None and pd.notna(low_s[idx])  else None,
                    "ref_high":   float(high_s[idx]) if high_s is not None and pd.notna(high_s[idx]) else None,
                    "flag":       str(df.loc[idx, t["flag_col"]]) if t["flag_col"] and pd.notna(df.loc[idx, t["flag_col"]]) else None,
                }
                if idx not in anomaly_index:
                    anomaly_index[idx] = {"row_index": int(idx), "anomalies": {}}
                anomaly_index[idx]["anomalies"][t["param"]] = entry

        # Attach row data (first 10 cols) to each anomaly row
        display_cols = list(df.columns[:10])
        anomaly_rows = []
        for idx, rec in list(anomaly_index.items())[:500]:
            row_data = {}
            for col in display_cols:
                v = df.loc[idx, col]
                row_data[col] = None if pd.isna(v) else str(v)
            anomaly_rows.append({**rec, "data": row_data})

        anomaly_rows.sort(key=lambda r: r["row_index"])

        log.info("lab-anomalies: %d params, %d anomaly rows", len(triplets), len(anomaly_rows))
        return safe_jsonify({
            "has_lab_data":    True,
            "parameters":      sorted(param_summaries, key=lambda p: -p["anomaly_count"]),
            "anomaly_rows":    anomaly_rows,
            "display_cols":    display_cols,
            "total_anomaly_rows": len(anomaly_index),
        })

    except Exception as e:
        log.exception("Error in lab-anomalies")
        return safe_jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# ML endpoints
# ---------------------------------------------------------------------------
_training_lock   = threading.Lock()
_training_status = {"state": "idle"}   # idle | running | done | error


@app.route("/api/ml/status", methods=["GET"])
def ml_status():
    from ml.predictor import MODEL_PATH
    import json as _json
    metrics = {}
    metrics_path = os.path.join(os.path.dirname(MODEL_PATH), "metrics.json")
    if os.path.exists(metrics_path):
        with open(metrics_path) as f:
            metrics = _json.load(f)
    return safe_jsonify({
        "trained":          os.path.exists(MODEL_PATH),
        "training_status":  _training_status,
        "metrics":          metrics,
    })


@app.route("/api/ml/train", methods=["POST"])
def ml_train():
    global _training_status

    if _training_status["state"] == "running":
        return safe_jsonify({"error": "Training already in progress"}), 409

    def _run():
        global _training_status
        _training_status = {"state": "running"}
        try:
            from ml.trainer import train as ml_train_fn
            result = ml_train_fn(CLEAN_DIR, MOCK_DB, max_rows_per_file=3000, verbose=True)
            _training_status = {"state": "done", "result": result}
        except Exception as e:
            _training_status = {"state": "error", "error": str(e)}

    with _training_lock:
        t = threading.Thread(target=_run, daemon=True)
        t.start()

    return safe_jsonify({"status": "started"})


@app.route("/api/ml/issues/<path:name>", methods=["GET"])
def ml_issues(name: str):
    try:
        from ml.predictor import get_suspicious_rows
        df = get_dataset(name)
        if df is None:
            return safe_jsonify({"error": "Dataset not found"}), 404
        result = get_suspicious_rows(df)
        return safe_jsonify(result)
    except FileNotFoundError as e:
        return safe_jsonify({"error": str(e), "needs_training": True}), 400
    except Exception as e:
        return safe_jsonify({"error": str(e)}), 500


@app.route("/api/ml/predict/<path:name>", methods=["GET"])
def ml_predict(name: str):
    try:
        from ml.predictor import predict_dataset
        df = get_dataset(name)
        if df is None:
            return safe_jsonify({"error": "Dataset not found"}), 404
        result = predict_dataset(df)
        return safe_jsonify(result)
    except FileNotFoundError as e:
        return safe_jsonify({"error": str(e), "needs_training": True}), 400
    except Exception as e:
        return safe_jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Claude AI Repair + SQLite Export
# ---------------------------------------------------------------------------
try:
    import anthropic as _anthropic
    _HAS_ANTHROPIC = True
except ImportError:
    _HAS_ANTHROPIC = False

import sqlite3

DB_DIR = os.path.normpath(os.path.join(BASE_DIR, "..", "databases"))
os.makedirs(DB_DIR, exist_ok=True)

# JSON Schema for Claude structured output
_CORRECTION_SCHEMA = {
    "type": "object",
    "properties": {
        "corrections": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "row_index":       {"type": "integer"},
                    "column":          {"type": "string"},
                    "original_value":  {"type": "string"},
                    "corrected_value": {"type": "string"},
                    "confidence":      {"type": "string"},
                    "reason":          {"type": "string"},
                },
                "required": ["row_index", "column", "original_value",
                             "corrected_value", "confidence", "reason"],
                "additionalProperties": False,
            },
        },
        "summary": {"type": "string"},
    },
    "required": ["corrections", "summary"],
    "additionalProperties": False,
}

_AI_REPAIR_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-6").strip() or "claude-sonnet-4-6"
_AI_REPAIR_MAX_ROWS = 18
_AI_REPAIR_MAX_CONTEXT_COLS = 8
_AI_REPAIR_MAX_ISSUES_PER_ROW = 4
_AI_REPAIR_REFERENCE_ROWS = 4
_AI_REPAIR_MAX_TOKENS = 1400
_AI_REPAIR_CONFIDENCE = {"high", "medium", "low"}
_AI_REPAIR_TOOL_NAME = "suggest_dataset_repairs"
_AI_REPAIR_SYSTEM_PROMPT = (
    "You are a medical data quality engineer. "
    "Suggest only precise, cell-level corrections for corrupted healthcare data. "
    "Use NULL only when a value cannot be inferred with confidence. "
    "Prefer conservative fixes over guesses."
)
_AI_REPAIR_TOOL = {
    "name": _AI_REPAIR_TOOL_NAME,
    "description": (
        "Return suggested corrections for corrupted dataset cells. "
        "Use exact row indices and column names from the payload. "
        "Only include meaningful fixes. "
        "original_value must exactly match the supplied value or be NULL for missing cells."
    ),
    "input_schema": _CORRECTION_SCHEMA,
    "strict": True,
}
_anthropic_client = None
_anthropic_client_key = None


def _get_anthropic_client(api_key: str):
    global _anthropic_client, _anthropic_client_key
    if _anthropic_client is None or _anthropic_client_key != api_key:
        _anthropic_client = _anthropic.Anthropic(api_key=api_key)
        _anthropic_client_key = api_key
    return _anthropic_client


def _column_profile(df: pd.DataFrame, col: str) -> dict:
    series = df[col]
    sample = series.dropna().astype(str).head(20)
    dominant_pattern = ""
    if len(sample) >= 3:
        patterns = sample.apply(_to_pattern)
        dominant_pattern = patterns.mode().iloc[0] if not patterns.empty else ""
    profile = {
        "name": col,
        "dtype": str(series.dtype),
    }
    if dominant_pattern:
        profile["dominant_pattern"] = dominant_pattern
    return profile


def _build_ai_context_columns(df: pd.DataFrame, issue_rows: list, patient_id_col: str | None) -> list[str]:
    ranked = []
    seen = set()
    if patient_id_col and patient_id_col in df.columns:
        ranked.append(patient_id_col)
        seen.add(patient_id_col)

    for row in issue_rows:
        for issue in row["issues"]:
            col = issue["col"]
            if col in df.columns and col not in seen:
                ranked.append(col)
                seen.add(col)
            if len(ranked) >= _AI_REPAIR_MAX_CONTEXT_COLS:
                return ranked

    for col in df.columns:
        if col not in seen:
            ranked.append(col)
            seen.add(col)
        if len(ranked) >= _AI_REPAIR_MAX_CONTEXT_COLS:
            break
    return ranked


def _build_repair_request(dataset_name: str, df: pd.DataFrame, issue_rows: list, patient_id_stats: dict, patient_id_col: str | None) -> tuple[dict, list[str]]:
    context_cols = _build_ai_context_columns(df, issue_rows, patient_id_col)
    issue_row_indices = {r["row_index"] for r in issue_rows}

    reference_rows = []
    valid_rows = df[~df.index.isin(issue_row_indices)].head(_AI_REPAIR_REFERENCE_ROWS)
    for _, row in valid_rows.iterrows():
        reference_rows.append({col: _coerce_json_cell(row[col]) for col in context_cols})

    compact_issue_rows = []
    for row in issue_rows:
        compact_issue_rows.append({
            "row_index": row["row_index"],
            "data": {col: row["data"].get(col) for col in context_cols},
            "issues": row["issues"],
        })

    payload = {
        "dataset_name": dataset_name,
        "context_columns": [_column_profile(df, col) for col in context_cols],
        "reference_rows": reference_rows,
        "corrupted_rows": compact_issue_rows,
        "rules": {
            "null_or_missing": 'Fill from context if obvious, otherwise use "NULL".',
            "wrong_type": 'Strip invalid characters and return the clean value as a string.',
            "invalid_patient_id": "Match the dominant patient ID pattern when clear.",
            "sentinel_values": 'Prefer "NULL" unless a better replacement is strongly inferable.',
            "confidence_levels": {
                "high": "obvious fix",
                "medium": "reasonable inference",
                "low": "uncertain but still useful",
            },
        },
    }
    if patient_id_col:
        payload["patient_id_context"] = {
            "column": patient_id_col,
            "dominant_pattern": patient_id_stats.get("dominant_pattern", ""),
            "has_pattern": bool(patient_id_stats.get("has_pattern")),
        }
    return payload, context_cols


def _block_attr(block, attr: str, default=None):
    if isinstance(block, dict):
        return block.get(attr, default)
    return getattr(block, attr, default)


def _extract_tool_payload(response) -> dict | None:
    for block in getattr(response, "content", []):
        if _block_attr(block, "type") == "tool_use" and _block_attr(block, "name") == _AI_REPAIR_TOOL_NAME:
            payload = _block_attr(block, "input", {})
            return payload if isinstance(payload, dict) else {}
    return None


def _extract_text_response(response) -> str:
    chunks = []
    for block in getattr(response, "content", []):
        if _block_attr(block, "type") == "text":
            text = _block_attr(block, "text", "")
            if text:
                chunks.append(text)
    return "\n".join(chunks).strip()


def _parse_legacy_claude_json(raw_text: str) -> dict:
    clean = raw_text.strip()
    if clean.startswith("```"):
        clean = re.sub(r"^```[a-z]*\n?", "", clean)
        clean = re.sub(r"\n?```$", "", clean.rstrip())
    return json.loads(clean)


def _normalize_ai_result(result: dict, df: pd.DataFrame) -> dict:
    corrections = []
    seen = set()
    for corr in result.get("corrections", []):
        try:
            row_index = int(corr["row_index"])
            column = str(corr["column"])
            if column not in df.columns or row_index not in df.index:
                continue

            key = (row_index, column)
            if key in seen:
                continue
            seen.add(key)

            current_value = df.at[row_index, column]
            original_value = "NULL" if pd.isna(current_value) else str(current_value)
            corrected_value = str(corr["corrected_value"]).strip()
            if not corrected_value:
                continue

            confidence = str(corr.get("confidence", "medium")).strip().lower()
            if confidence not in _AI_REPAIR_CONFIDENCE:
                confidence = "medium"

            normalized = {
                "row_index": row_index,
                "column": column,
                "original_value": original_value,
                "corrected_value": corrected_value,
                "confidence": confidence,
                "reason": str(corr.get("reason", "")).strip() or "AI suggested correction",
            }
            if normalized["corrected_value"] == normalized["original_value"]:
                continue
            corrections.append(normalized)
        except Exception:
            continue

    summary = str(result.get("summary", "")).strip()
    if not summary:
        summary = f"Generated {len(corrections)} AI repair suggestions."

    return {"corrections": corrections, "summary": summary}


def _call_claude_repair(client_ai, payload: dict) -> dict:
    request_args = {
        "model": _AI_REPAIR_MODEL,
        "max_tokens": _AI_REPAIR_MAX_TOKENS,
        "system": _AI_REPAIR_SYSTEM_PROMPT,
        "messages": [{"role": "user", "content": json.dumps(payload, ensure_ascii=False)}],
        "tools": [_AI_REPAIR_TOOL],
        "tool_choice": {"type": "tool", "name": _AI_REPAIR_TOOL_NAME},
        "disable_parallel_tool_use": True,
    }

    try:
        response = client_ai.messages.create(**request_args)
    except Exception as tool_exc:
        log.warning("Structured Claude repair call failed, falling back to text JSON: %s", tool_exc)
        fallback_prompt = (
            f"{_AI_REPAIR_SYSTEM_PROMPT}\n\n"
            "Return only JSON matching this schema:\n"
            f"{json.dumps(_CORRECTION_SCHEMA, ensure_ascii=False)}\n\n"
            f"Payload:\n{json.dumps(payload, ensure_ascii=False)}"
        )
        response = client_ai.messages.create(
            model=_AI_REPAIR_MODEL,
            max_tokens=_AI_REPAIR_MAX_TOKENS,
            messages=[{"role": "user", "content": fallback_prompt}],
        )
        raw_text = _extract_text_response(response)
        if not raw_text:
            raise RuntimeError("Claude returned an empty response")
        return _parse_legacy_claude_json(raw_text)

    tool_payload = _extract_tool_payload(response)
    if tool_payload is not None:
        return tool_payload

    raw_text = _extract_text_response(response)
    if not raw_text:
        raise RuntimeError("Claude returned neither tool output nor text output")
    return _parse_legacy_claude_json(raw_text)


@app.route("/api/db/test-connection", methods=["POST"])
def test_db_connection():
    """Test a SQL Server connection without writing any data."""
    try:
        from sqlalchemy import create_engine, text
        import urllib.parse
    except ImportError:
        return safe_jsonify({"error": "sqlalchemy/pyodbc not installed"}), 500

    body     = request.get_json(silent=True) or {}
    server   = body.get("server", "").strip()
    port     = int(body.get("port", 1433))
    database = body.get("database", "").strip()
    username = body.get("username", "").strip()
    password = body.get("password", "")
    driver   = body.get("driver", "SQL Server")
    auth     = body.get("auth", "sql")

    if not server or not database:
        return safe_jsonify({"ok": False, "error": "server and database are required"}), 400

    try:
        driver_enc = urllib.parse.quote_plus(driver)
        if auth == "windows":
            conn_str = (
                f"mssql+pyodbc://@{server},{port}/{database}"
                f"?driver={driver_enc}&trusted_connection=yes"
            )
        else:
            if not username:
                return safe_jsonify({"ok": False, "error": "username required for SQL auth"}), 400
            pw_enc   = urllib.parse.quote_plus(password)
            conn_str = (
                f"mssql+pyodbc://{username}:{pw_enc}@{server},{port}/{database}"
                f"?driver={driver_enc}"
            )

        engine = create_engine(conn_str, connect_args={"timeout": 8})
        with engine.connect() as con:
            con.execute(text("SELECT 1"))

        return safe_jsonify({"ok": True})
    except Exception as e:
        return safe_jsonify({"ok": False, "error": str(e)}), 200


@app.route("/api/datasets/<path:name>/ai-repair", methods=["POST"])
def ai_repair(name: str):
    """Use Claude AI to suggest corrections for corrupted data."""
    log.info("ai-repair called for dataset: %s", name)

    if not _HAS_ANTHROPIC:
        return safe_jsonify({"error": "anthropic package not installed. Run: pip install anthropic"}), 500

    try:
        df = get_dataset(name)
        if df is None:
            log.error("Dataset not found: %s", name)
            return safe_jsonify({"error": "Dataset not found"}), 404

        api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
        if not api_key:
            log.error("ANTHROPIC_API_KEY not set")
            return safe_jsonify({"error": "ANTHROPIC_API_KEY env var not set"}), 400

        analysis = _collect_issue_analysis(
            df,
            row_limit=_AI_REPAIR_MAX_ROWS,
            display_col_limit=_AI_REPAIR_MAX_CONTEXT_COLS,
            issue_limit_per_row=_AI_REPAIR_MAX_ISSUES_PER_ROW,
            prioritize=True,
        )
        issue_rows = analysis["issue_rows"]
        total_issue_rows = analysis["total_issue_rows"]

        if not issue_rows:
            return safe_jsonify({"corrections": [], "summary": "No issues found in dataset."})

        payload, context_cols = _build_repair_request(
            name,
            df,
            issue_rows,
            analysis["patient_id_stats"],
            analysis["patient_id_col"],
        )
        payload_size = len(json.dumps(payload, ensure_ascii=False))
        log.info(
            "ai-repair sending %d prioritized rows out of %d issue rows (%d chars)",
            len(issue_rows),
            total_issue_rows,
            payload_size,
        )

        start = time.perf_counter()
        client_ai = _get_anthropic_client(api_key)
        raw_result = _call_claude_repair(client_ai, payload)
        result = _normalize_ai_result(raw_result, df)
        elapsed_ms = round((time.perf_counter() - start) * 1000)

        result["meta"] = {
            "rows_analysed": len(issue_rows),
            "total_issue_rows": total_issue_rows,
            "context_columns": context_cols,
            "model": _AI_REPAIR_MODEL,
            "duration_ms": elapsed_ms,
            "note": (
                f"Optimized AI pass: prioritised {len(issue_rows)} high-signal rows "
                f"out of {total_issue_rows} issue rows."
            ),
        }

        log.info(
            "ai-repair done — %d corrections returned in %d ms",
            len(result.get("corrections", [])),
            elapsed_ms,
        )
        return safe_jsonify(result)

    except Exception as e:
        log.exception("Unhandled error in ai_repair")
        return safe_jsonify({"error": str(e)}), 500


def _coerce_corrected_value(series: pd.Series, value):
    if value == "NULL":
        return None

    dtype = series.dtype
    if pd.api.types.is_integer_dtype(dtype):
        parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        return None if pd.isna(parsed) else int(parsed)
    if pd.api.types.is_float_dtype(dtype):
        parsed = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
        return None if pd.isna(parsed) else float(parsed)
    if pd.api.types.is_bool_dtype(dtype):
        sval = str(value).strip().lower()
        if sval in {"true", "1", "yes"}:
            return True
        if sval in {"false", "0", "no"}:
            return False
        return value
    return value


@app.route("/api/datasets/<path:name>/export-db", methods=["POST"])
def export_db(name: str):
    """Export the dataset (with optional AI corrections) to SQL Server."""
    try:
        from sqlalchemy import create_engine, text
        import urllib.parse
    except ImportError:
        return safe_jsonify({"error": "sqlalchemy/pyodbc not installed"}), 500

    body        = request.get_json(silent=True) or {}
    corrections = body.get("corrections", [])
    conn_cfg    = body.get("connection", {})

    server   = conn_cfg.get("server", "").strip()
    port     = int(conn_cfg.get("port", 1433))
    database = conn_cfg.get("database", "").strip()
    username = conn_cfg.get("username", "").strip()
    password = conn_cfg.get("password", "")
    driver   = conn_cfg.get("driver", "SQL Server")
    auth     = conn_cfg.get("auth", "sql")  # "sql" | "windows"

    if not server or not database:
        return safe_jsonify({"error": "server and database are required"}), 400

    try:
        df = get_dataset(name)
        if df is None:
            return safe_jsonify({"error": "Dataset not found"}), 404

        # ── Apply corrections ───────────────────────────────────────────────
        corrected_df = df.copy()
        applied = 0
        for corr in corrections:
            try:
                row_idx = int(corr["row_index"])
                col     = corr["column"]
                val     = corr["corrected_value"]
                if col not in corrected_df.columns:
                    continue
                if row_idx not in corrected_df.index:
                    continue
                corrected_df.at[row_idx, col] = _coerce_corrected_value(corrected_df[col], val)
                applied += 1
            except Exception:
                continue

        # ── Derive table name ───────────────────────────────────────────────
        base       = os.path.splitext(name)[0]
        table_name = re.sub(r"[^\w]", "_", base).strip("_") or "dataset"
        # SQL Server max identifier length = 128 chars
        table_name = table_name[:128]

        # ── Build SQLAlchemy connection string ──────────────────────────────
        driver_enc = urllib.parse.quote_plus(driver)
        if auth == "windows":
            conn_str = (
                f"mssql+pyodbc://@{server},{port}/{database}"
                f"?driver={driver_enc}&trusted_connection=yes"
            )
        else:
            if not username:
                return safe_jsonify({"error": "username is required for SQL auth"}), 400
            pw_enc = urllib.parse.quote_plus(password)
            conn_str = (
                f"mssql+pyodbc://{username}:{pw_enc}@{server},{port}/{database}"
                f"?driver={driver_enc}"
            )

        engine = create_engine(conn_str, fast_executemany=True)

        # ── Test connection before writing ──────────────────────────────────
        with engine.connect() as con:
            con.execute(text("SELECT 1"))

        # ── Export to SQL Server ────────────────────────────────────────────
        corrected_df.to_sql(
            table_name, engine,
            if_exists="replace",
            index=True,
            chunksize=500,
        )

        return safe_jsonify({
            "success":             True,
            "server":              server,
            "database":            database,
            "table_name":          table_name,
            "rows_exported":       len(corrected_df),
            "corrections_applied": applied,
        })

    except Exception as e:
        return safe_jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print(f"[backend] Mock DB path: {MOCK_DB}")
    print(f"[backend] Datasets found: {list_dataset_files()}")
    app.run(debug=True, port=5000)
