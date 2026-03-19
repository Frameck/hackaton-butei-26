"""
pdf_to_db.py
Converts nursing PDF reports into a CSV file + terminal table.

Usage
-----
    python pdf_to_db.py [pdf_path] [csv_path]

Defaults:
    pdf_path = ../hack-butei-26/epaCC-START-Hack-2026-main/...clinic_4_nursing.pdf
    csv_path = nursing.csv
"""

import re
import sys
import textwrap
import pdfplumber
import pandas as pd

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
DEFAULT_PDF = (
    "../hack-butei-26/epaCC-START-Hack-2026-main/"
    "Endtestdaten_ohne_Fehler_ einheitliche ID/"
    "split_data_pat_case_altered/split_data_pat_case_altered/"
    "clinic_4_nursing.pdf"
)
DEFAULT_CSV = "nursing.csv"

# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------
RE_DATE      = re.compile(r"Date:\s*(\S+)")
RE_SHIFT     = re.compile(r"Shift:\s*(.+)")
RE_PATIENT   = re.compile(r"Patient ID:\s*(\S+)")
RE_CASE      = re.compile(r"Case ID:\s*(\S+)")
RE_WARD      = re.compile(r"Ward:\s*(.+)")
RE_EVAL      = re.compile(r"Evaluation:\s*", re.IGNORECASE)
RE_SHIFT_LBL = re.compile(r"(Fr.hdienst|Sp.tdienst|Nachtdienst):\s*", re.IGNORECASE)
RE_INTERV    = re.compile(r"Interventions?:\s*", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
def parse_page(page_text: str, page_num: int) -> dict:
    lines = [l.strip() for l in page_text.splitlines() if l.strip()]

    record = {
        "page":          page_num,
        "date":          None,
        "shift":         None,
        "patient_id":    None,
        "case_id":       None,
        "ward":          None,
        "observations":  [],
        "interventions": [],
        "evaluation":    None,
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

    full_text    = " ".join(lines)
    body_match   = RE_SHIFT_LBL.search(full_text)
    if body_match:
        body         = full_text[body_match.start():]
        body_no_lbl  = RE_SHIFT_LBL.sub("", body, count=1)
        interv_match = RE_INTERV.search(body_no_lbl)
        eval_match   = RE_EVAL.search(body_no_lbl)

        if interv_match and eval_match:
            obs_text    = body_no_lbl[:interv_match.start()].strip().rstrip(",")
            interv_text = body_no_lbl[interv_match.end():eval_match.start()].strip().rstrip(",")
            record["evaluation"] = body_no_lbl[eval_match.end():].strip()
        elif interv_match:
            obs_text    = body_no_lbl[:interv_match.start()].strip().rstrip(",")
            interv_text = body_no_lbl[interv_match.end():].strip().rstrip(",")
        elif eval_match:
            obs_text    = body_no_lbl[:eval_match.start()].strip().rstrip(",")
            interv_text = ""
            record["evaluation"] = body_no_lbl[eval_match.end():].strip()
        else:
            obs_text    = body_no_lbl.strip()
            interv_text = ""

        record["observations"]  = [o.strip()  for o  in obs_text.split(",")    if o.strip()]
        record["interventions"] = [iv.strip() for iv in interv_text.split(",") if iv.strip()]

    return record


# ---------------------------------------------------------------------------
# Terminal table (pretty-printed, wraps long cells)
# ---------------------------------------------------------------------------
WRAP = {
    "observations":  38,
    "interventions": 38,
    "evaluation":    28,
}

COLS = ["page", "date", "shift", "patient_id", "case_id",
        "ward", "observations", "interventions", "evaluation"]

COL_WIDTH = {
    "page":          4,
    "date":         10,
    "shift":        11,
    "patient_id":   10,
    "case_id":       8,
    "ward":         16,
    "observations":  38,
    "interventions": 38,
    "evaluation":    28,
}


def _cell_lines(col: str, value) -> list[str]:
    """Wrap a cell value into lines that fit COL_WIDTH[col]."""
    w = COL_WIDTH[col]
    if value is None:
        return [""]
    text = str(value)
    return textwrap.wrap(text, w) or [""]


def print_table(rows: list[dict]):
    sep = "+" + "+".join("-" * (COL_WIDTH[c] + 2) for c in COLS) + "+"
    header = "|" + "|".join(f" {c.upper():<{COL_WIDTH[c]}} " for c in COLS) + "|"

    print(sep)
    print(header)
    print(sep.replace("-", "="))

    for row in rows:
        # Flatten lists into newline-separated strings before wrapping
        display = {
            c: "; ".join(row[c]) if isinstance(row.get(c), list) else row.get(c)
            for c in COLS
        }
        cell_lines = {c: _cell_lines(c, display[c]) for c in COLS}
        n_lines = max(len(v) for v in cell_lines.values())

        for li in range(n_lines):
            line = "|"
            for c in COLS:
                part = cell_lines[c][li] if li < len(cell_lines[c]) else ""
                line += f" {part:<{COL_WIDTH[c]}} |"
            print(line)
        print(sep)


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
def print_summary(rows: list[dict], csv_path: str):
    total        = len(rows)
    dates        = sorted({r["date"] for r in rows if r["date"]})
    patients     = {r["patient_id"] for r in rows if r["patient_id"]}
    cases        = {r["case_id"]    for r in rows if r["case_id"]}
    wards        = {r["ward"]       for r in rows if r["ward"]}
    shifts       = {}
    for r in rows:
        s = r["shift"] or "unknown"
        shifts[s] = shifts.get(s, 0) + 1

    # --- Missing value analysis ---
    CRITICAL = ["date", "patient_id", "case_id", "shift"]
    IMPORTANT = ["ward", "observations", "interventions", "evaluation"]

    def is_missing(val):
        if val is None:
            return True
        if isinstance(val, list):
            return len(val) == 0
        return str(val).strip() == ""

    missing = {field: [] for field in CRITICAL + IMPORTANT}
    for r in rows:
        for field in CRITICAL + IMPORTANT:
            if is_missing(r.get(field)):
                missing[field].append(r.get("page", "?"))

    total_width = sum(COL_WIDTH[c] + 3 for c in COLS) - 1
    border  = "=" * total_width
    divider = "-" * total_width

    print(border)
    print(" SUMMARY")
    print(border)
    print(f"  Reports parsed    : {total}")
    print(f"  CSV saved to      : {csv_path}")
    if dates:
        print(f"  Date range        : {dates[0]}  ->  {dates[-1]}")
    print(f"  Unique patients   : {len(patients)}")
    print(f"  Unique cases      : {len(cases)}")
    print(f"  Unique wards      : {len(wards)}")
    if wards:
        print(f"  Wards             : {', '.join(sorted(wards))}")
    print(f"  Shifts breakdown  :")
    for shift, count in sorted(shifts.items()):
        print(f"      {shift:<30} {count} report(s)")

    # --- Missing values report ---
    print(divider)
    print(" MISSING VALUES REPORT")
    print(divider)

    has_issues = False

    for field in CRITICAL:
        pages = missing[field]
        if pages:
            has_issues = True
            pct = len(pages) / total * 100
            pages_str = ", ".join(str(p) for p in pages[:10])
            if len(pages) > 10:
                pages_str += f" ... (+{len(pages) - 10} more)"
            print(f"  [CRITICAL] {field:<18} missing in {len(pages):>3}/{total} reports ({pct:.0f}%)  ->  pages: {pages_str}")
        else:
            print(f"  [  OK    ] {field:<18} complete ({total}/{total})")

    print()

    for field in IMPORTANT:
        pages = missing[field]
        if pages:
            has_issues = True
            pct = len(pages) / total * 100
            pages_str = ", ".join(str(p) for p in pages[:10])
            if len(pages) > 10:
                pages_str += f" ... (+{len(pages) - 10} more)"
            print(f"  [WARNING ] {field:<18} missing in {len(pages):>3}/{total} reports ({pct:.0f}%)  ->  pages: {pages_str}")
        else:
            print(f"  [  OK    ] {field:<18} complete ({total}/{total})")

    print()
    if not has_issues:
        print("  All fields complete. No missing data detected.")
    else:
        crit_fields = [f for f in CRITICAL if missing[f]]
        if crit_fields:
            print(f"  !! ALERT: critical fields with missing data: {', '.join(crit_fields)}")
            print("     These records may be unusable. Check the source PDF pages listed above.")

    print(border)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    pdf_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_PDF
    csv_path = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_CSV

    print(f"Reading: {pdf_path}\n")
    records = []
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""
            if not text.strip():
                continue
            rec = parse_page(text, i)
            records.append(rec)

    # --- CSV export ---------------------------------------------------------
    flat_rows = []
    for r in records:
        flat_rows.append({
            "page":          r["page"],
            "date":          r["date"],
            "shift":         r["shift"],
            "patient_id":    r["patient_id"],
            "case_id":       r["case_id"],
            "ward":          r["ward"],
            "observations":  "; ".join(r["observations"]),
            "interventions": "; ".join(r["interventions"]),
            "evaluation":    r["evaluation"],
        })

    df = pd.DataFrame(flat_rows)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"CSV saved: {csv_path}  ({len(records)} reports)\n")

    # --- Terminal table -----------------------------------------------------
    print_table(flat_rows)

    # --- Summary ------------------------------------------------------------
    print_summary(flat_rows, csv_path)


if __name__ == "__main__":
    main()
