#!/usr/bin/env python3
"""
consolidate_tempe_newbee_weborder.py - combine the MOST RECENT
Tempeweborder_extend_*.csv with the MOST RECENT NewbeeWeborder_*.csv
into a single Excel workbook so Tempe and Newbee prices can be
scanned side by side.

Output layout (one row per product per source):

    Source | SIZE | brand | DESCRIPTION | Shared_Description |
      COST | PRICE | STOCK

The "Source" column at the far left tags each row as "Tempe" or
"Newbee".  The "Shared_Description" column is populated only when a
product on one side has an "essentially the same" description as a
product on the other side for the same (SIZE, brand) -- then both
rows get the SAME shared-description value so matched pairs line up
visually in the sheet.  "Essentially the same" uses
tempeweborder_shared.norm_desc_for_dedup() which collapses
whitespace, strips trailing '^^^' noise and upper-cases, matching
how consolidate_tempeweborder.py already de-duplicates within a
single source.

The two outputs are stacked (all Tempe rows then all Newbee rows)
and sorted inside each block by (SIZE, brand, DESCRIPTION) so the
file reads like a diff would.

Usage:
    python consolidate_tempe_newbee_weborder.py
    python consolidate_tempe_newbee_weborder.py --dir .
    python consolidate_tempe_newbee_weborder.py --output rollup.xlsx

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

from tempeweborder_shared import (
    extract_size_from_desc,
    clean_description,
    norm_desc_for_dedup,
    make_sku_key,
)

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


# Only the extend crawl (the production sweep) is read on the Tempe
# side -- the user asked specifically for "Tempeweborder_extend*.csv
# 최근본".  Match the exact filename shape so a stray similarly-named
# file somewhere else in the directory does not sneak in.
_TEMPE_RX = re.compile(
    r"^tempeweborder_extend_"
    r"(?P<y>\d{4})(?P<m>\d{2})(?P<d>\d{2})_(?P<hh>\d{2})(?P<mm>\d{2})\.csv$",
    re.IGNORECASE,
)
_NEWB_RX = re.compile(
    r"^newbeeweborder_"
    r"(?P<y>\d{4})(?P<m>\d{2})(?P<d>\d{2})_(?P<hh>\d{2})(?P<mm>\d{2})\.csv$",
    re.IGNORECASE,
)


def _parse_dt(name: str, rx):
    m = rx.match(os.path.basename(name))
    if not m:
        return None
    try:
        return (
            date(int(m["y"]), int(m["m"]), int(m["d"])),
            int(m["hh"]) * 100 + int(m["mm"]),
        )
    except Exception:
        return None


def _latest_in(directory: str, rx):
    """Return (abs_path, date, hhmm) of the newest file in `directory`
    matching `rx`, or None if nothing matches."""
    best = None
    for fn in os.listdir(directory):
        pd = _parse_dt(fn, rx)
        if pd is None:
            continue
        d, hhmm = pd
        if best is None or d > best[1] or (d == best[1] and hhmm > best[2]):
            best = (os.path.join(directory, fn), d, hhmm)
    return best


def _norm_cell(v) -> str:
    return "" if v is None else str(v).strip()


def _to_number(s):
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
        return str(s).strip()


def _read_source_csv(path: str) -> list[dict]:
    """Read a Tempe-or-Newbee CSV and return a list of dicts with
    canonical keys: size, brand, desc, desc_norm, cost, price, stock.
    De-duplicates within the file (first row wins per (size, brand,
    desc_norm) key, same policy as consolidate_tempeweborder.py)."""
    out: list[dict] = []
    seen: set[tuple[str, str, str]] = set()
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return out
        lut = {h.lower(): h for h in reader.fieldnames}
        def _c(name_lower):
            return lut.get(name_lower)
        for r in reader:
            size  = _norm_cell(r.get(_c("size")  or "SIZE"))
            brand = _norm_cell(r.get(_c("brand") or "brand"))
            desc  = _norm_cell(r.get(_c("description") or "DESCRIPTION"))
            if not size and not brand and not desc:
                continue
            if not size:
                recovered = extract_size_from_desc(desc)
                if recovered:
                    size = recovered
            desc_clean = clean_description(desc, strip_leading_size=bool(size))
            if desc_clean:
                desc = desc_clean
            desc_norm = norm_desc_for_dedup(desc)
            key = (size.upper(), brand.upper(), desc_norm)
            if key in seen:
                continue
            seen.add(key)
            # Nov-02: per-warehouse stock columns.  Tempe files
            # carry STOCK_BRISBANE / STOCK_SYDNEY; Newbee files
            # carry STOCK_SYDNEY / STOCK_MELBOURNE.  Read whichever
            # pair is present (missing columns return "" via .get).
            _bri = _norm_cell(r.get(_c("stock_brisbane")  or "STOCK_BRISBANE"))
            _syd = _norm_cell(r.get(_c("stock_sydney")    or "STOCK_SYDNEY"))
            _mel = _norm_cell(r.get(_c("stock_melbourne") or "STOCK_MELBOURNE"))
            out.append({
                "size":        size.upper(),
                "brand":       brand,
                "desc":        desc,
                "desc_norm":   desc_norm,
                "cost":        _to_number(_norm_cell(r.get(_c("cost")  or "COST"))),
                "price":       _to_number(_norm_cell(r.get(_c("price") or "PRICE"))),
                "stock":       _to_number(_norm_cell(r.get(_c("stock") or "STOCK"))),
                "stock_bri":   _to_number(_bri) if _bri != "" else "",
                "stock_syd":   _to_number(_syd) if _syd != "" else "",
                "stock_mel":   _to_number(_mel) if _mel != "" else "",
            })
    return out


def _compute_shared_descriptions(tempe_rows, newb_rows) -> dict[tuple[str, str, str], str]:
    """Build a (size_up, brand_up, desc_norm) -> shared_description
    map.  A key gets a shared description when it appears in BOTH
    source lists (the exact normalised match).  The shared value is
    the LONGER of the two original-casing descriptions so the Excel
    cell carries whichever side had the more complete text."""
    by_key_tempe: dict[tuple[str, str, str], str] = {}
    by_key_newb: dict[tuple[str, str, str], str] = {}
    for r in tempe_rows:
        by_key_tempe.setdefault((r["size"], r["brand"].upper(), r["desc_norm"]),
                                r["desc"])
    for r in newb_rows:
        by_key_newb.setdefault((r["size"], r["brand"].upper(), r["desc_norm"]),
                               r["desc"])
    shared: dict[tuple[str, str, str], str] = {}
    for k, t_desc in by_key_tempe.items():
        if k in by_key_newb:
            n_desc = by_key_newb[k]
            shared[k] = t_desc if len(t_desc) >= len(n_desc) else n_desc
    return shared


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=".",
                    help="Directory to scan for the two source CSVs "
                         "(default: current directory).")
    ap.add_argument("--output",
                    default="tempe_newbee_weborder_consolidated.xlsx",
                    help="Output .xlsx path (default: "
                         "./tempe_newbee_weborder_consolidated.xlsx).")
    args = ap.parse_args()

    source_dir = os.path.abspath(args.dir)
    if not os.path.isdir(source_dir):
        print(f"[fatal] not a directory: {source_dir}", file=sys.stderr)
        sys.exit(1)

    latest_tempe = _latest_in(source_dir, _TEMPE_RX)
    latest_newb  = _latest_in(source_dir, _NEWB_RX)
    if not latest_tempe and not latest_newb:
        print(f"[fatal] no Tempeweborder_extend_*.csv or NewbeeWeborder_*.csv "
              f"found under {source_dir}", file=sys.stderr)
        sys.exit(2)
    if not latest_tempe:
        print(f"[warn] no Tempeweborder_extend_*.csv found; writing a "
              f"Newbee-only sheet", file=sys.stderr)
    if not latest_newb:
        print(f"[warn] no NewbeeWeborder_*.csv found; writing a "
              f"Tempe-only sheet", file=sys.stderr)
    if latest_tempe:
        print(f"[info] Tempe source : {os.path.basename(latest_tempe[0])}  "
              f"({latest_tempe[1].isoformat()} {latest_tempe[2]:04d})",
              file=sys.stderr)
    if latest_newb:
        print(f"[info] Newbee source: {os.path.basename(latest_newb[0])}  "
              f"({latest_newb[1].isoformat()} {latest_newb[2]:04d})",
              file=sys.stderr)

    tempe_rows = _read_source_csv(latest_tempe[0]) if latest_tempe else []
    newb_rows  = _read_source_csv(latest_newb[0])  if latest_newb  else []
    print(f"[info] rows: Tempe={len(tempe_rows)}  Newbee={len(newb_rows)}",
          file=sys.stderr)

    shared = _compute_shared_descriptions(tempe_rows, newb_rows)
    print(f"[info] shared product matches (same SIZE+brand+normalised DESC "
          f"on BOTH sides): {len(shared)}", file=sys.stderr)

    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment
        from openpyxl.utils import get_column_letter
    except ImportError:
        print("[fatal] openpyxl is required.  Install with:\n"
              "    pip install openpyxl", file=sys.stderr)
        sys.exit(3)

    # Nov-02: per-warehouse stock columns.  Both source files write
    # their branch-split qty into their own named columns; the Excel
    # sheet mirrors both so the rows read naturally per source:
    #   Tempe row -> STOCK_BRISBANE + STOCK_SYDNEY populated,
    #                STOCK_MELBOURNE blank.
    #   Newbee row -> STOCK_SYDNEY + STOCK_MELBOURNE populated,
    #                STOCK_BRISBANE blank.
    # STOCK stays as the legacy max-of-branches for backward compat.
    headers = [
        "Source", "SIZE", "brand", "DESCRIPTION", "Shared_Description",
        "COST", "PRICE", "STOCK",
        "STOCK_BRISBANE", "STOCK_SYDNEY", "STOCK_MELBOURNE",
    ]

    wb = Workbook()
    ws = wb.active
    ws.title = "Tempe+Newbee"
    ws.append(headers)

    hdr_font     = Font(bold=True, color="FFFFFF", size=11)
    src_fill     = PatternFill("solid", fgColor="4B5563")   # dark slate
    idx_fill     = PatternFill("solid", fgColor="4B5563")
    shared_fill  = PatternFill("solid", fgColor="1E3A8A")   # deep blue
    cost_fill    = PatternFill("solid", fgColor="92400E")
    price_fill   = PatternFill("solid", fgColor="7C2D12")
    stock_fill   = PatternFill("solid", fgColor="166534")
    tempe_row    = PatternFill("solid", fgColor="FFF4E6")   # pale orange
    newb_row     = PatternFill("solid", fgColor="E8EEFC")   # pale blue
    shared_cell  = PatternFill("solid", fgColor="DBEAFE")   # pale blue-100
    for col_idx, h in enumerate(headers, 1):
        c = ws.cell(row=1, column=col_idx)
        c.font = hdr_font
        c.alignment = Alignment(horizontal="center", vertical="center",
                                 wrap_text=True)
        if   h == "Source":             c.fill = src_fill
        elif h == "Shared_Description": c.fill = shared_fill
        elif h == "COST":               c.fill = cost_fill
        elif h == "PRICE":              c.fill = price_fill
        elif h.startswith("STOCK"):     c.fill = stock_fill
        else:                           c.fill = idx_fill
    ws.row_dimensions[1].height = 32
    ws.freeze_panes = "B2"   # pin the Source column on scroll

    def _emit_rows(rows, source_label, row_fill):
        for r in sorted(rows, key=lambda x: (x["size"], x["brand"].upper(),
                                              x["desc"].upper())):
            key = (r["size"], r["brand"].upper(), r["desc_norm"])
            sdesc = shared.get(key, "")
            row_vals = [
                source_label, r["size"], r["brand"], r["desc"], sdesc,
                r["cost"], r["price"], r["stock"],
                r.get("stock_bri", ""), r.get("stock_syd", ""),
                r.get("stock_mel", ""),
            ]
            ws.append(row_vals)
            r_idx = ws.max_row
            for col_idx in range(1, len(headers) + 1):
                cell = ws.cell(row=r_idx, column=col_idx)
                # Light row banding per source so Tempe and Newbee
                # blocks are visually distinct even without a filter.
                cell.fill = row_fill
                # Shared_Description cell gets its own tint when
                # populated so matched pairs pop even in a long sheet.
                if col_idx == 5 and sdesc:
                    cell.fill = shared_cell
                    cell.font = Font(bold=True)

    _emit_rows(tempe_rows, "Tempe",  tempe_row)
    _emit_rows(newb_rows,  "Newbee", newb_row)

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
    print(f"[done] wrote {ws.max_row - 1} rows x {len(headers)} cols -> "
          f"{out_abs}")
    print(f"       (Tempe={len(tempe_rows)}, Newbee={len(newb_rows)}, "
          f"matched_pairs={len(shared)})")


if __name__ == "__main__":
    main()
