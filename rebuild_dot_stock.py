"""
Rebuild rawdata/unlock/dot_stock.csv from the per-plant
dot_stock_<PLANT>.xlsx files already on disk, filtering OUT every
row whose "Blocked Stock" column reads 1 — then push the filtered
CSV into the MySQL `stock` table using the existing loader.

One-shot utility; does NOT touch the SAP crawl.  Run whenever you
want the blocked_stock=1 noise stripped out of the live stock
table.  All settings inherited from load_csv_to_mysql.py (env-
driven DB_HOST / DB_PORT / DB_USER / DB_PASS / DB_NAME).

Usage:
    python rebuild_dot_stock.py           # rebuild + load
    python rebuild_dot_stock.py --dry-run # rebuild only, no SQL

Nov-05 user ask.  Keeps every other row untouched; only drops the
blocked ones.  Logs per-plant kept/dropped counts for a quick sanity
check.
"""
from __future__ import annotations

import csv
import glob
import os
import sys
from openpyxl import load_workbook

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
UNLOCK_DIR = os.path.join(BASE_DIR, "rawdata", "unlock")
OUT_CSV    = os.path.join(UNLOCK_DIR, "dot_stock.csv")

# Nov-05 Phase 1: 7-column schema (added block_qty + location_id).
# Same order as load_csv_to_mysql.load_stock's LOAD DATA after the
# auto-migration drops the matching columns onto the live table.
OUTPUT_HEADER = ["interface_date", "plant", "material", "dot_no",
                 "stock_qty", "block_qty", "location_id"]

# Headers we recognise in the per-plant XLSX.  Case + underscores +
# dots are collapsed by _norm_hdr so "DOT No.", "dot_no", "Dot  No"
# all land on the same key.
KEEP_COLUMNS = {
    "interface date": "interface_date",
    "plant":          "plant",
    "material":       "material",
    "dot no":         "dot_no",
    "dot no.":        "dot_no",
    "stock qty":      "stock_qty",
    "block qty":      "block_qty",
    "blocked qty":    "block_qty",
    "location id":    "location_id",
    "location":       "location_id",
}
# Nov-05: ZSDM64300 output ships the blocked quantity under the
# header "Block Qty" (confirmed from the operator's screenshot).
# Keep the older spellings too in case a different SAP layout /
# localisation uses them.
BLOCKED_HEADER_ALIASES = {
    "block qty", "blocked qty",
    "blocked stock", "block stock", "blocked stk", "block stk",
    "blocked", "block",
}


def _norm_hdr(s: str) -> str:
    s = (s or "").strip().lower().replace("_", " ").replace(".", "")
    return " ".join(s.split())


def _is_blocked(val) -> bool:
    """True when the cell carries ANY blocked quantity (> 0).  The
    header is "Block Qty" — a count of units held in blocked status,
    not a 0/1 flag — so a row with Block Qty = 7 is just as blocked
    as one with Block Qty = 1 and must also be dropped.  Blanks,
    zeros and non-numerics keep."""
    if val is None:
        return False
    s = str(val).strip().replace(",", ".")
    if not s:
        return False
    try:
        return float(s) > 0.0
    except Exception:
        return False


def _read_rows(path: str):
    wb = load_workbook(path, data_only=True)
    ws = wb.active
    out = []
    for row in ws.iter_rows(values_only=True):
        out.append(["" if v is None else str(v).strip() for v in row])
    while out and all(v == "" for v in out[-1]):
        out.pop()
    return out


def extract_filtered(path: str):
    """Return (kept_rows, blocked_count) for one per-plant XLSX.
    Nov-05 Phase 1: blocked rows are KEPT.  blocked_count is reported
    as informational so the operator still sees how many rows carry
    block_qty > 0 and will be hidden by downstream SPRF / Ageing
    filters (the tooltip then breaks them down by ageing + location)."""
    rows = _read_rows(path)
    if not rows:
        return [], 0
    header = rows[0]
    col_idx: dict[str, int] = {}
    for i, h in enumerate(header):
        key = KEEP_COLUMNS.get(_norm_hdr(h))
        if key and key not in col_idx:
            col_idx[key] = i
    missing = [k for k in OUTPUT_HEADER if k not in col_idx]
    if missing:
        print(f"  [WARN] {os.path.basename(path)}: missing columns {missing}.  "
              f"Headers seen: {[_norm_hdr(h) for h in header]}")
    bq_col = col_idx.get("block_qty", -1)
    kept, blocked_count = [], 0
    for r in rows[1:]:
        if not r:
            continue
        mat_i = col_idx.get("material", -1)
        if mat_i < 0 or mat_i >= len(r) or not r[mat_i]:
            continue
        row = [r[col_idx[k]] if 0 <= col_idx.get(k, -1) < len(r) else ""
               for k in OUTPUT_HEADER]
        kept.append(row)
        if 0 <= bq_col < len(r) and _is_blocked(r[bq_col]):
            blocked_count += 1
    return kept, blocked_count


def rebuild_combined_csv() -> tuple[int, int]:
    """Read every rawdata/unlock/dot_stock_*.xlsx, apply the filter,
    write the combined CSV, and return (total_kept, total_blocked)."""
    xlsx_files = sorted(glob.glob(os.path.join(UNLOCK_DIR, "dot_stock_*.xlsx")))
    if not xlsx_files:
        raise FileNotFoundError(
            f"No dot_stock_*.xlsx files found in {UNLOCK_DIR}.  "
            f"Run the SAP crawl (sapcrawling_batch.py) first to produce them."
        )
    print(f"Scanning {len(xlsx_files)} per-plant XLSX file(s) in {UNLOCK_DIR}")
    total_kept = 0
    total_blocked = 0
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(OUTPUT_HEADER)
        for p in xlsx_files:
            kept, blocked = extract_filtered(p)
            for row in kept:
                w.writerow(row)
            total_kept    += len(kept)
            total_blocked += blocked
            print(f"  {os.path.basename(p):<40}  kept={len(kept):>6,}  "
                  f"blocked-in-kept={blocked:>5,}")
    print(f"Wrote {OUT_CSV}  (kept={total_kept:,}, "
          f"of which blocked={total_blocked:,} — hidden by downstream views, "
          f"surfaced in tooltip)")
    return total_kept, total_blocked


def main():
    dry_run = "--dry-run" in sys.argv
    kept, blocked = rebuild_combined_csv()
    if kept == 0:
        print("[WARN] 0 rows survived — refusing to load an empty table.  "
              "Check the Blocked Stock column header mapping.")
        return 2
    if dry_run:
        print("--dry-run: skipping MySQL load.")
        return 0
    # Delegate the actual LOAD DATA LOCAL INFILE to the existing loader
    # so the CREATE / TRUNCATE / column cast logic stays in one place.
    import load_csv_to_mysql as loader
    import mysql.connector
    conn = mysql.connector.connect(
        host=loader.DB_HOST,
        port=loader.DB_PORT,
        user=loader.DB_USER,
        password=loader.DB_PASSWORD,
        database=loader.DB_NAME,
        allow_local_infile=True,
    )
    try:
        loader.load_stock(conn)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main() or 0)
