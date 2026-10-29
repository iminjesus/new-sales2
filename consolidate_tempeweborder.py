#!/usr/bin/env python3
"""
consolidate_tempeweborder.py - roll every Tempeweborder_YYYYMMDD_HHMM.csv
in a directory into one Excel workbook with a per-month column layout.

Each product (keyed on SIZE + brand + DESCRIPTION) becomes ONE row.
Per month (derived from the filename date), three columns are added:
    COST_YYYY-MM   -- dealer sell-in (COST col)
    PRICE_YYYY-MM  -- dealer retail  (PRICE col)
    STOCK_YYYY-MM  -- units on hand  (STOCK col, blank pre-Sep 2025)

When several CSVs share the same month, the LATEST file's values win
(the filename timestamp decides).  Older files on the same month are
still read for completeness but overwritten.

Usage:
    python consolidate_tempeweborder.py                     # defaults: cwd + ./tempeweborder_consolidated.xlsx
    python consolidate_tempeweborder.py --dir data/tempe
    python consolidate_tempeweborder.py --dir . --output rollup.xlsx
    python consolidate_tempeweborder.py --no-stock-blank    # keep raw STOCK vals even for pre-Sep months

Requires: openpyxl.
    pip install openpyxl
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import sys
from collections import defaultdict
from datetime import date

# Oct-29: Windows cp949/cp1252 chokes on any non-ASCII in print()
# or argparse's --help output.  Reconfigure stdout/stderr to utf-8
# with replace-fallback so progress text and status lines never
# trip the system codepage encoder.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# Filename pattern the crawler writes:  Tempeweborder_YYYYMMDD_HHMM.csv
FNAME_RX = re.compile(
    r"^tempeweborder_(?P<y>\d{4})(?P<m>\d{2})(?P<d>\d{2})_(?P<hh>\d{2})(?P<mm>\d{2})\.csv$",
    re.IGNORECASE,
)
# STOCK capture started mid-2025; pre-Sep months stay blank unless
# --no-stock-blank is set.  Change this threshold if your dataset
# started earlier.
STOCK_AVAILABLE_FROM = date(2025, 9, 1)


def _parse_filename(name: str):
    """Return (date, hhmm) tuple or None if the file isn't a crawl output."""
    m = FNAME_RX.match(os.path.basename(name))
    if not m:
        return None
    try:
        d  = date(int(m["y"]), int(m["m"]), int(m["d"]))
        hm = int(m["hh"]) * 100 + int(m["mm"])
        return d, hm
    except Exception:
        return None


def _month_key(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def _norm_cell(v) -> str:
    s = "" if v is None else str(v).strip()
    return s


def _to_number(s: str):
    """Return a float when s parses as a number (ignores $ , spaces,
    trailing %); else return the raw stripped string so a text value
    like 'CALL' survives into the Excel sheet."""
    if s is None:
        return ""
    t = str(s).replace(",", "").replace("$", "").replace("%", "").strip()
    if not t:
        return ""
    try:
        return float(t)
    except Exception:
        return s.strip()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=".",
                    help="Directory to scan for Tempeweborder_*.csv files "
                         "(default: current directory).")
    ap.add_argument("--output", default="tempeweborder_consolidated.xlsx",
                    help="Output .xlsx path (default: ./tempeweborder_consolidated.xlsx).")
    ap.add_argument("--no-stock-blank", action="store_true",
                    help="Keep STOCK values for pre-Sep-2025 months instead "
                         "of leaving the cell blank.  Default blanks them "
                         "because the crawler didn't record stock before "
                         "Sep 2025.")
    ap.add_argument("--include-recursive", action="store_true",
                    help="Also scan subdirectories under --dir.")
    args = ap.parse_args()

    # Walk directory and keep the LATEST file per month.
    source_dir = os.path.abspath(args.dir)
    if not os.path.isdir(source_dir):
        print(f"[fatal] not a directory: {source_dir}", file=sys.stderr)
        sys.exit(1)

    walker = os.walk(source_dir) if args.include_recursive \
             else [(source_dir, [], os.listdir(source_dir))]

    # month_key -> (date, hhmm, abs_path) with the latest timestamp wins
    latest_per_month: dict[str, tuple[date, int, str]] = {}
    all_files_seen = 0
    for root, _dirs, files in walker:
        for fn in files:
            parsed = _parse_filename(fn)
            if not parsed:
                continue
            all_files_seen += 1
            d, hhmm = parsed
            k = _month_key(d)
            prev = latest_per_month.get(k)
            # Prefer later day; on same day prefer later HHMM.
            if (prev is None
                    or d > prev[0]
                    or (d == prev[0] and hhmm > prev[1])):
                latest_per_month[k] = (d, hhmm, os.path.join(root, fn))

    if not latest_per_month:
        print(f"[fatal] no Tempeweborder_*.csv files found under {source_dir}",
              file=sys.stderr)
        sys.exit(2)

    print(f"[info] scanned {all_files_seen} CSV(s); keeping the latest "
          f"per month => {len(latest_per_month)} month(s):")
    months_sorted = sorted(latest_per_month.keys())
    for mk in months_sorted:
        d, hhmm, path = latest_per_month[mk]
        print(f"       {mk}  <-  {os.path.basename(path)}  "
              f"({d.isoformat()} {hhmm:04d})", file=sys.stdout)

    # Per-product row: key -> {col -> value}
    # key = (SIZE_UP, BRAND_UP, DESCRIPTION_RAW)
    rows: dict[tuple[str, str, str], dict[str, object]] = {}
    descriptions_seen: dict[tuple[str, str], str] = {}
    for mk in months_sorted:
        d, _hhmm, path = latest_per_month[mk]
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                print(f"[warn] {path} has no header - skipped", file=sys.stderr)
                continue
            # Field names are SIZE / brand / DESCRIPTION / COST / PRICE / STOCK
            # per the two tempeweborder_crawling*.py scripts.  Build a
            # case-insensitive lookup so a user's manual tweak that
            # switched case doesn't break us.
            lut = {h.lower(): h for h in reader.fieldnames}
            def _col(name_lower):
                return lut.get(name_lower)
            for r in reader:
                size = _norm_cell(r.get(_col("size") or "size"))
                brand = _norm_cell(r.get(_col("brand") or "brand"))
                desc = _norm_cell(r.get(_col("description") or "DESCRIPTION"))
                if not size and not brand and not desc:
                    continue
                key = (size.upper(), brand.upper(), desc)
                slot = rows.setdefault(key, {})
                cost = _norm_cell(r.get(_col("cost")  or "COST"))
                price = _norm_cell(r.get(_col("price") or "PRICE"))
                stock = _norm_cell(r.get(_col("stock") or "STOCK"))
                slot[f"COST_{mk}"]  = _to_number(cost)
                slot[f"PRICE_{mk}"] = _to_number(price)
                # Oct-29 user ask: stock column exists for every month
                # but values for pre-Sep-2025 months stay blank unless
                # --no-stock-blank is passed.
                if d >= STOCK_AVAILABLE_FROM or args.no_stock_blank:
                    slot[f"STOCK_{mk}"] = _to_number(stock) if stock != "" else ""
                else:
                    slot[f"STOCK_{mk}"] = ""
                # Keep the longest / latest description for display
                desc_key = (size.upper(), brand.upper())
                if desc and (desc_key not in descriptions_seen
                             or len(desc) > len(descriptions_seen[desc_key])):
                    descriptions_seen[desc_key] = desc

    print(f"[info] built {len(rows)} unique (SIZE, brand, DESCRIPTION) rows",
          file=sys.stderr)

    # Column order: SIZE, brand, DESCRIPTION, then per-month triplets
    # chronological left-to-right.
    headers = ["SIZE", "brand", "DESCRIPTION"]
    for mk in months_sorted:
        headers.extend([f"COST_{mk}", f"PRICE_{mk}", f"STOCK_{mk}"])

    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment
        from openpyxl.utils import get_column_letter
    except ImportError:
        print("[fatal] openpyxl is required.  Install with:\n"
              "    pip install openpyxl", file=sys.stderr)
        sys.exit(3)

    wb = Workbook()
    ws = wb.active
    ws.title = "Rollup"
    ws.append(headers)
    # Header styling: bold white on dark grey, with the three index
    # columns distinct from the per-month triplets.
    hdr_font = Font(bold=True, color="FFFFFF", size=11)
    idx_fill = PatternFill("solid", fgColor="4B5563")
    cost_fill = PatternFill("solid", fgColor="92400E")
    price_fill = PatternFill("solid", fgColor="7C2D12")
    stock_fill = PatternFill("solid", fgColor="166534")
    for col_idx, h in enumerate(headers, 1):
        c = ws.cell(row=1, column=col_idx)
        c.font = hdr_font
        c.alignment = Alignment(horizontal="center", vertical="center",
                                 wrap_text=True)
        if h in ("SIZE", "brand", "DESCRIPTION"):
            c.fill = idx_fill
        elif h.startswith("COST_"):
            c.fill = cost_fill
        elif h.startswith("PRICE_"):
            c.fill = price_fill
        elif h.startswith("STOCK_"):
            c.fill = stock_fill
    ws.row_dimensions[1].height = 32
    ws.freeze_panes = "D2"   # pin SIZE / brand / DESCRIPTION on scroll

    # Sort rows by SIZE then brand then description for a predictable
    # Excel layout.
    for key in sorted(rows.keys()):
        size_up, brand_up, desc = key
        display_desc = descriptions_seen.get((size_up, brand_up), desc)
        slot = rows[key]
        row_vals = [size_up, brand_up, display_desc]
        for mk in months_sorted:
            row_vals.append(slot.get(f"COST_{mk}", ""))
            row_vals.append(slot.get(f"PRICE_{mk}", ""))
            row_vals.append(slot.get(f"STOCK_{mk}", ""))
        ws.append(row_vals)

    # Simple column auto-sizing (take max cell width per column, cap
    # at 40 for readability).
    for col_idx, _ in enumerate(headers, 1):
        letter = get_column_letter(col_idx)
        maxlen = max(
            (len(str(ws.cell(row=r, column=col_idx).value or ""))
             for r in range(1, ws.max_row + 1)),
            default=10,
        )
        ws.column_dimensions[letter].width = min(max(maxlen + 2, 8), 40)

    out_abs = os.path.abspath(args.output)
    wb.save(out_abs)
    print(f"[done] wrote {ws.max_row - 1} rows x {len(headers)} cols -> {out_abs}")


if __name__ == "__main__":
    main()
