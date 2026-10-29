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
import shutil
import sys
from collections import defaultdict
from datetime import date

# Oct-29: shared helpers so this script, tempeweborder_crawling.py and
# tempeweborder_crawling_extend.py all use the same SIZE regex +
# description normalisation.  Replaces the duplicated inline logic that
# used to live here and would drift from the crawlers over time.
from tempeweborder_shared import (
    extract_size_from_desc,
    clean_description,
    norm_desc_for_dedup,
    make_sku_key,
    recover_size_if_blank,
)

# Oct-29: Windows cp949/cp1252 chokes on any non-ASCII in print()
# or argparse's --help output.  Reconfigure stdout/stderr to utf-8
# with replace-fallback so progress text and status lines never
# trip the system codepage encoder.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# Oct-29 user ask: BOTH crawler outputs are consumed by this script.
#   * Tempeweborder_YYYYMMDD_HHMM.csv          <- search-by-size crawl
#     (tempeweborder_crawling.py; 24 fixed size codes)
#   * Tempeweborder_extend_YYYYMMDD_HHMM.csv   <- brand-sweep crawl
#     (tempeweborder_crawling_extend.py; every SKU of ~10 brands)
# Same column schema (SIZE, brand, DESCRIPTION, COST, PRICE, STOCK), so
# rows from both sources merge cleanly through make_sku_key.  Keeping
# the two sources TRACKED separately (latest_per_source_per_month) lets
# both contribute to a single month's data pool instead of one silently
# overwriting the other.
FNAME_RX = re.compile(
    r"^tempeweborder_(?:(?P<src>extend)_)?"
    r"(?P<y>\d{4})(?P<m>\d{2})(?P<d>\d{2})_(?P<hh>\d{2})(?P<mm>\d{2})\.csv$",
    re.IGNORECASE,
)
# STOCK capture started mid-2025; pre-Sep months stay blank unless
# --no-stock-blank is set.  Change this threshold if your dataset
# started earlier.
STOCK_AVAILABLE_FROM = date(2025, 9, 1)


def _parse_filename(name: str):
    """Return (date, hhmm, source) tuple or None if the file isn't a
    crawl output.  `source` is "regular" for Tempeweborder_*.csv and
    "extend" for Tempeweborder_extend_*.csv - both are consumed."""
    m = FNAME_RX.match(os.path.basename(name))
    if not m:
        return None
    try:
        d  = date(int(m["y"]), int(m["m"]), int(m["d"]))
        hm = int(m["hh"]) * 100 + int(m["mm"])
        src = "extend" if (m["src"] or "").lower() == "extend" else "regular"
        return d, hm, src
    except Exception:
        return None


def _month_key(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def _norm_cell(v) -> str:
    s = "" if v is None else str(v).strip()
    return s


# Oct-29: SIZE recovery + description normalisation moved to the shared
# module (tempeweborder_shared) so the crawlers and this script always
# agree on what a valid tyre-size shape is.  Local aliases keep this
# file's existing call sites readable.
_size_from_desc = extract_size_from_desc
_norm_desc_for_key = norm_desc_for_dedup


def repair_csv_in_place(path: str) -> dict:
    """Oct-29 user ask: rewrite an existing Tempeweborder_*.csv file in
    place so that:

      (1) rows whose SIZE column is blank get a SIZE recovered from
          the DESCRIPTION text when a recognisable tyre-size token
          is present at the head (P-metric, LT-metric, flotation,
          NHS, 11R22.5, plain metric).
      (2) DESCRIPTION has its trailing '^^^' stripped and - when SIZE
          now carries the token - the leading SIZE prefix is removed
          so the two columns don't duplicate each other.
      (3) rows that collapse to the same (SIZE_UP, BRAND_UP,
          DESC_NORM) key within one file are deduped (first row wins).

    The original file is copied to <path>.bak on the first repair.
    Subsequent repairs do NOT clobber the backup - the first-ever
    version stays recoverable.  Returns a stats dict for the caller
    to print."""
    stats = {"rows_in": 0, "rows_out": 0, "size_recovered": 0,
             "duplicates_dropped": 0, "desc_cleaned": 0}
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)
    if not rows:
        return stats
    header = rows[0]
    body   = rows[1:]
    # Build a case-insensitive column lookup so a CSV whose header
    # uses "Size" vs "SIZE" vs "size" still works.
    hdr_lower = [h.strip().lower() for h in header]
    def _idx(name_lower, fallback_names=()):
        for cand in (name_lower,) + tuple(fallback_names):
            if cand in hdr_lower:
                return hdr_lower.index(cand)
        return -1
    i_size = _idx("size")
    i_brand = _idx("brand")
    i_desc = _idx("description")
    if i_size < 0 or i_brand < 0 or i_desc < 0:
        print(f"[repair:skip] {os.path.basename(path)} - missing SIZE / "
              f"brand / DESCRIPTION header column", file=sys.stderr)
        return stats
    seen_keys: set[tuple[str, str, str]] = set()
    kept_rows: list[list[str]] = []
    for r in body:
        if not r or all(not str(c or "").strip() for c in r):
            continue
        # Pad short rows so column indexing is safe.
        while len(r) < len(header):
            r.append("")
        stats["rows_in"] += 1
        size_raw  = str(r[i_size]  or "").strip()
        brand_raw = str(r[i_brand] or "").strip()
        desc_raw  = str(r[i_desc]  or "").strip()
        # (1) Recover size when blank.
        size, recovered = recover_size_if_blank(size_raw, desc_raw)
        if recovered:
            stats["size_recovered"] += 1
        # (2) Clean description.  We strip the leading SIZE prefix
        #     only when the row now has a non-empty SIZE - otherwise
        #     the SIZE token is the only identifier the DESCRIPTION
        #     carries and stripping it would lose information.
        new_desc = clean_description(desc_raw, strip_leading_size=bool(size))
        if new_desc != desc_raw:
            stats["desc_cleaned"] += 1
        # Fall back to the raw description if cleaning collapsed to
        # nothing (shouldn't happen, but defensive).
        if not new_desc:
            new_desc = desc_raw
        # (3) Dedup.
        key = make_sku_key(size, brand_raw, new_desc)
        if key in seen_keys:
            stats["duplicates_dropped"] += 1
            continue
        seen_keys.add(key)
        # Write back the cleaned columns in place; leave other
        # columns (COST / PRICE / STOCK / ...) untouched.
        r[i_size] = size
        r[i_desc] = new_desc
        kept_rows.append(r)
    stats["rows_out"] = len(kept_rows)
    bak = path + ".bak"
    if not os.path.exists(bak):
        shutil.copy2(path, bak)
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(kept_rows)
    return stats


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
    ap.add_argument("--no-stock-blank", action="store_true", default=True,
                    help="(default True) Keep whatever STOCK value the "
                         "source CSV carries, no matter what month the file "
                         "is from.  Pass --blank-pre-sep to force blanking "
                         "pre-Sep-2025 months instead.")
    ap.add_argument("--blank-pre-sep", action="store_true",
                    help="Blank out the STOCK column for months before "
                         f"{STOCK_AVAILABLE_FROM.isoformat()}.  Default is "
                         "to preserve whatever value the source file "
                         "carries (per Oct-29 user ask).")
    ap.add_argument("--include-recursive", action="store_true",
                    help="Also scan subdirectories under --dir.")
    ap.add_argument("--long", action="store_true",
                    help="Also write a long/unpivoted sibling workbook: one "
                         "row per (SKU, month) with columns SIZE, brand, "
                         "DESCRIPTION, year, month, cost, price, stock.  "
                         "Easier for pivot tables / line charts than the "
                         "wide per-month layout.")
    ap.add_argument("--long-only", action="store_true",
                    help="Write ONLY the long format; skip the wide workbook.")
    ap.add_argument("--repair", action="store_true",
                    help="BEFORE building the rollup, rewrite every "
                         "Tempeweborder_*.csv in --dir in place with: "
                         "(1) missing SIZE recovered from DESCRIPTION, "
                         "(2) trailing '^^^' and leading SIZE prefix "
                         "stripped from DESCRIPTION, (3) within-file "
                         "duplicates collapsed.  Each original file is "
                         "backed up to <name>.bak the first time it is "
                         "repaired - re-runs won't clobber the backup.")
    ap.add_argument("--repair-only", action="store_true",
                    help="Run the --repair pass and exit without "
                         "building the rollup workbook.")
    args = ap.parse_args()
    if args.repair_only:
        args.repair = True

    # Walk directory and keep the LATEST file per month.
    source_dir = os.path.abspath(args.dir)
    if not os.path.isdir(source_dir):
        print(f"[fatal] not a directory: {source_dir}", file=sys.stderr)
        sys.exit(1)

    walker = os.walk(source_dir) if args.include_recursive \
             else [(source_dir, [], os.listdir(source_dir))]

    # Oct-29 user ask: track latest file per (month, source) so BOTH
    # Tempeweborder_*.csv AND Tempeweborder_extend_*.csv contribute to
    # a single month's data pool.  Without the per-source split, an
    # extend file would silently overwrite the regular file for the
    # same month (or vice versa) and the user would lose half their
    # SKUs.  The downstream read loop iterates over months_sorted +
    # per-source entries, so the ordering is deterministic (regular
    # first, then extend) and dedupe keys handle any cross-source
    # overlap.
    #
    # Key: (month_str, source)  Value: (date, hhmm, abs_path)
    latest_per_mkey_src: dict[tuple[str, str],
                              tuple[date, int, str]] = {}
    all_files_seen = 0
    for root, _dirs, files in walker:
        for fn in files:
            parsed = _parse_filename(fn)
            if not parsed:
                continue
            all_files_seen += 1
            d, hhmm, src = parsed
            k = (_month_key(d), src)
            prev = latest_per_mkey_src.get(k)
            # Prefer later day; on same day prefer later HHMM.
            if (prev is None
                    or d > prev[0]
                    or (d == prev[0] and hhmm > prev[1])):
                latest_per_mkey_src[k] = (d, hhmm, os.path.join(root, fn))

    if not latest_per_mkey_src:
        print(f"[fatal] no Tempeweborder_*.csv or Tempeweborder_extend_*.csv "
              f"files found under {source_dir}", file=sys.stderr)
        sys.exit(2)

    # Oct-29 user ask: --repair rewrites EVERY Tempeweborder_*.csv in
    # the scan tree in place (not just the latest-per-month) so historic
    # files line up with the current normalisation rules before the
    # rollup reads them.  Each file is backed up to <name>.bak on its
    # first repair pass.
    if args.repair:
        all_csvs_abs: list[str] = []
        for root, _dirs, files in (os.walk(source_dir) if args.include_recursive
                                   else [(source_dir, [], os.listdir(source_dir))]):
            for fn in files:
                if _parse_filename(fn):
                    all_csvs_abs.append(os.path.join(root, fn))
        print(f"[repair] rewriting {len(all_csvs_abs)} CSV(s) in place "
              f"(backups: <file>.bak on first repair) ...", file=sys.stderr)
        tot = {"rows_in": 0, "rows_out": 0, "size_recovered": 0,
               "duplicates_dropped": 0, "desc_cleaned": 0}
        for path in sorted(all_csvs_abs):
            st = repair_csv_in_place(path)
            if (st["size_recovered"] or st["duplicates_dropped"]
                    or st["desc_cleaned"]):
                print(f"[repair] {os.path.basename(path):40}  "
                      f"in={st['rows_in']:5d}  out={st['rows_out']:5d}  "
                      f"size_recovered={st['size_recovered']:4d}  "
                      f"dup_dropped={st['duplicates_dropped']:4d}  "
                      f"desc_cleaned={st['desc_cleaned']:5d}",
                      file=sys.stderr)
            for k in tot: tot[k] += st[k]
        print(f"[repair] TOTAL: in={tot['rows_in']}  out={tot['rows_out']}  "
              f"size_recovered={tot['size_recovered']}  "
              f"dup_dropped={tot['duplicates_dropped']}  "
              f"desc_cleaned={tot['desc_cleaned']}",
              file=sys.stderr)
        if args.repair_only:
            print("[info] --repair-only set, exiting without rollup",
                  file=sys.stderr)
            return

    # Oct-29: iterate over (month_key, source) pairs - both the regular
    # crawl output AND the extend sweep contribute to the same month.
    # Readers downstream flatten this back to a single rows dict via
    # the (SIZE, brand, DESCRIPTION) key, so a SKU present in both
    # sources gets written exactly once.
    months_sorted = sorted({mk for (mk, _src) in latest_per_mkey_src.keys()})
    sources_sorted = ["regular", "extend"]   # deterministic read order
    print(f"[info] scanned {all_files_seen} CSV(s); keeping the latest "
          f"per (month, source) => {len(latest_per_mkey_src)} file(s) "
          f"across {len(months_sorted)} month(s):")
    for mk in months_sorted:
        for src in sources_sorted:
            entry = latest_per_mkey_src.get((mk, src))
            if entry is None:
                continue
            d, hhmm, path = entry
            print(f"       {mk} [{src:7s}] <-  {os.path.basename(path)}  "
                  f"({d.isoformat()} {hhmm:04d})", file=sys.stdout)

    # Per-product row: key -> slot dict with metadata + month data.
    # Oct-29: key is (SIZE_UP, BRAND_UP, DESC_NORM_UP) where DESC_NORM
    # is the whitespace-collapsed / ^^^-stripped / upper-cased
    # description.  This dedupes rows that differ ONLY in trailing
    # whitespace, stray '^^^' markers or case - which the source
    # occasionally emits as separate rows for the same SKU.  Each slot
    # stores its OWN original-casing description so the output always
    # shows the row's actual description text, not a longest-wins
    # shared one across a (size, brand) group.
    rows: dict[tuple[str, str, str], dict[str, object]] = {}
    size_recovered_total = 0
    dedup_collapsed_total = 0
    _blank_pre_sep = bool(args.blank_pre_sep)
    for mk in months_sorted:
      for src in sources_sorted:
        entry = latest_per_mkey_src.get((mk, src))
        if entry is None:
            continue
        d, _hhmm, path = entry
        rows_in_file = 0
        size_recovered = 0
        dedup_in_file = 0
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
            # Oct-29: track which keys we've already written in THIS
            # file so same-key duplicates inside a single CSV count as
            # "collapsed" rather than silently re-overwriting (so the
            # per-month info line below can report them to the user).
            seen_this_file: set[tuple[str, str, str]] = set()
            for r in reader:
                size = _norm_cell(r.get(_col("size") or "size"))
                brand = _norm_cell(r.get(_col("brand") or "brand"))
                desc = _norm_cell(r.get(_col("description") or "DESCRIPTION"))
                if not size and not brand and not desc:
                    continue
                # Oct-29 user ask: recover a missing SIZE from the
                # DESCRIPTION so flotation / P-metric / NHS SKUs with
                # the size baked into the description text don't ship
                # with a blank SIZE column.
                if not size:
                    recovered = _size_from_desc(desc)
                    if recovered:
                        size = recovered
                        size_recovered += 1
                rows_in_file += 1
                desc_norm = _norm_desc_for_key(desc)
                key = (size.upper(), brand.upper(), desc_norm)
                if key in seen_this_file:
                    dedup_in_file += 1
                else:
                    seen_this_file.add(key)
                slot = rows.get(key)
                if slot is None:
                    # First time we see this SKU - stamp its own
                    # display metadata on the slot.  Later rows for the
                    # same key (across months) only refresh the display
                    # fields when the current row carries a LONGER
                    # description (handles source rows that progres-
                    # sively flesh out the description text).
                    slot = {
                        "size":  size.upper(),
                        "brand": brand,            # keep original casing
                        "desc":  desc,             # keep original casing
                        "months": {},
                    }
                    rows[key] = slot
                elif desc and len(desc) > len(str(slot.get("desc") or "")):
                    slot["desc"] = desc
                cost = _norm_cell(r.get(_col("cost")  or "COST"))
                price = _norm_cell(r.get(_col("price") or "PRICE"))
                stock = _norm_cell(r.get(_col("stock") or "STOCK"))
                # Oct-29: STOCK column is preserved as-is by default.
                # Only --blank-pre-sep (opt-in) forces blanks for
                # months before STOCK_AVAILABLE_FROM.
                if _blank_pre_sep and d < STOCK_AVAILABLE_FROM:
                    stock_val = ""
                else:
                    stock_val = _to_number(stock) if stock != "" else ""
                slot["months"][mk] = {
                    "cost":  _to_number(cost),
                    "price": _to_number(price),
                    "stock": stock_val,
                }
        if size_recovered or dedup_in_file:
            print(f"[info] {mk} [{src}]: {rows_in_file} rows read "
                  f"(SIZE recovered from description: {size_recovered}, "
                  f"duplicate rows collapsed: {dedup_in_file})",
                  file=sys.stderr)
        size_recovered_total  += size_recovered
        dedup_collapsed_total += dedup_in_file

    print(f"[info] built {len(rows)} unique (SIZE, brand, DESCRIPTION) rows "
          f"(SIZE recovered: {size_recovered_total}; "
          f"duplicates collapsed: {dedup_collapsed_total})",
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
    # Excel layout.  Each slot carries its OWN display description so
    # rows that share (size, brand) but differ in description render
    # distinctly - no longest-wins collapse.
    for key in sorted(rows.keys()):
        slot = rows[key]
        row_vals = [slot["size"], slot["brand"], slot["desc"]]
        months_data = slot["months"]
        for mk in months_sorted:
            md = months_data.get(mk)
            if md is None:
                row_vals.extend(["", "", ""])
            else:
                row_vals.extend([md["cost"], md["price"], md["stock"]])
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
    if not args.long_only:
        wb.save(out_abs)
        print(f"[done] wide: wrote {ws.max_row - 1} rows x {len(headers)} cols -> {out_abs}")
    else:
        print(f"[info] --long-only set, skipping wide output")

    # Oct-29 user ask: optional long/unpivoted sibling workbook.  One
    # row per (SKU, month) with the four data columns laid flat so the
    # file is trivially pivot-friendly.
    if args.long or args.long_only:
        long_wb = Workbook()
        lws = long_wb.active
        lws.title = "Long"
        long_headers = ["SIZE", "brand", "DESCRIPTION",
                         "year", "month", "cost", "price", "stock"]
        lws.append(long_headers)
        for col_idx, h in enumerate(long_headers, 1):
            c = lws.cell(row=1, column=col_idx)
            c.font = hdr_font
            c.alignment = Alignment(horizontal="center", vertical="center")
            if h in ("SIZE", "brand", "DESCRIPTION"):
                c.fill = idx_fill
            elif h in ("year", "month"):
                c.fill = PatternFill("solid", fgColor="1E3A8A")
            elif h == "cost":
                c.fill = cost_fill
            elif h == "price":
                c.fill = price_fill
            elif h == "stock":
                c.fill = stock_fill
        lws.freeze_panes = "D2"
        for key in sorted(rows.keys()):
            slot = rows[key]
            months_data = slot["months"]
            for mk in months_sorted:
                yr, mo = mk.split("-")
                md = months_data.get(mk)
                if md is None:
                    # SKU wasn't in this month's source file at all -
                    # skip the (SKU, month) long row so the long file
                    # isn't bloated with placeholders for SKUs that
                    # never had any data in a given month.
                    continue
                cost  = md["cost"]
                price = md["price"]
                stock = md["stock"]
                lws.append([slot["size"], slot["brand"], slot["desc"],
                            int(yr), int(mo), cost, price, stock])
        for col_idx, _ in enumerate(long_headers, 1):
            letter = get_column_letter(col_idx)
            maxlen = max(
                (len(str(lws.cell(row=r, column=col_idx).value or ""))
                 for r in range(1, lws.max_row + 1)),
                default=10,
            )
            lws.column_dimensions[letter].width = min(max(maxlen + 2, 7), 40)
        # Default sibling path: <output>_long.xlsx (or the --output
        # value itself when --long-only).
        if args.long_only:
            long_out = out_abs
        else:
            base, ext = os.path.splitext(out_abs)
            long_out = base + "_long" + (ext or ".xlsx")
        long_wb.save(long_out)
        print(f"[done] long: wrote {lws.max_row - 1} rows x {len(long_headers)} cols -> {long_out}")


if __name__ == "__main__":
    main()
