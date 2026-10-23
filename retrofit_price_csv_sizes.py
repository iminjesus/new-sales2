"""
retrofit_price_csv_sizes.py — one-shot cleanup for historical price-crawl
CSVs that lost the decimal rim diameter on the SIZE column (265/70R19
should have been 265/70R19.5, 11R22 should have been 11R22.5, etc.).

The crawlers (jax / tempe / bobjane / tempeweborder) were updated on
2026-10-23 to accept decimal rims, but every CSV written before that
has the truncated value baked in.  This script walks a folder, finds
every CSV with a SIZE column + a DESCRIPTION / TITLE column, and
rewrites the SIZE in-place when the description holds the FULL,
decimal-rim version of the same size.

Why cross-reference against the description rather than just appending
".5" to every \\d{2} rim?  Because 265/70R19 and 265/70R19.5 are
DIFFERENT tyres.  We only patch rows where the SIZE is the integer
version AND the description independently contains the decimal form —
that's near-certain proof the integer was a crawler truncation, not a
real integer-rim size.

Usage (PowerShell on the operator PC):

    cd D:\\Data_Anal_website
    git pull
    # Dry run — tells you what WOULD change, no file touched:
    python retrofit_price_csv_sizes.py "D:\\Data_Anal_website"

    # Walk a specific folder:
    python retrofit_price_csv_sizes.py "E:\\01. work\\2025\\Data_Anal_Website"

    # Apply the changes (writes a .bak-<ts> next to each file first):
    python retrofit_price_csv_sizes.py "D:\\Data_Anal_website" --apply

CSV patterns scanned (case-insensitive, name-based so sub-folders /
timestamps in the filename don't matter):
    jax*, bobjane*, tempe*, tempeweborder*
plus their _extend / _yh_only / _hk_only / _inc_prod_name variants.

Report: per-file row count, change count, up to 5 before→after samples.
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import shutil
import sys
import time
from pathlib import Path

# Match passenger (265/70R19), truck (11R22), X-notation (33X12.5R15)
# and bare 3-digit (205R16) sizes WITHOUT a decimal rim.  Capture the
# whole size as group 1 so we can derive the decimal variant.
_INT_SIZE_RE = re.compile(
    r"^\s*("
    r"\d{2,3}X\d{1,2}(?:\.\d)?[A-Za-z]{1,2}\d{2}"
    r"|\d{3}/\d{2}[A-Za-z]{1,2}\d{2}"
    r"|\d{2,3}[A-Za-z]{1,2}\d{2}"   # covers 11R22, 205R16, 7.5R15, etc.
    r")\s*$"
)
# For every integer-rim size the row carries, build a regex that
# matches the SAME size with a .d suffix on the rim.  Pulled out
# per-row below.
def _decimal_variant_re(int_size: str) -> re.Pattern | None:
    """Given an integer-rim size string, return a regex that matches
    the same width/aspect/letter prefix but with ANY .d suffix on the
    rim diameter.  Returns None when the string doesn't parse."""
    m = re.match(
        r"^(\d{2,3})([XxX/])(\d{1,2}(?:\.\d)?)([A-Za-z]{1,2})(\d{2})$",
        int_size,
    )
    if m:
        w, sep, asp, letters, rim = m.groups()
        return re.compile(
            rf"\b{re.escape(w)}[{re.escape(sep)}]{re.escape(asp)}"
            rf"{re.escape(letters)}{re.escape(rim)}\.\d\b",
            re.IGNORECASE,
        )
    # 11R22 / 205R16 pattern (no slash, no X — truck sizes with 2- or
    # 3-digit widths).
    m = re.match(r"^(\d{2,3})([A-Za-z]{1,2})(\d{2})$", int_size)
    if m:
        w, letters, rim = m.groups()
        return re.compile(
            rf"\b{re.escape(w)}{re.escape(letters)}{re.escape(rim)}\.\d\b",
            re.IGNORECASE,
        )
    return None


def _pick_size_col(fieldnames: list[str]) -> str | None:
    """Case-insensitive match for the SIZE column."""
    for c in fieldnames:
        if c.strip().lower() == "size":
            return c
    return None


def _pick_desc_col(fieldnames: list[str]) -> str | None:
    """Return the column most likely to hold the original title /
    description text (jax / bobjane / tempe / tempeweborder all name
    it DESCRIPTION; a handful of legacy exports use TITLE)."""
    for cand in ("description", "title", "product", "desc", "name"):
        for c in fieldnames:
            if c.strip().lower() == cand:
                return c
    return None


_NAME_PATTERNS = (
    "jax", "bobjane", "bob_jane", "tempe", "tempeweborder",
    "tempe_weborder", "tempe-weborder",
)
def _looks_like_price_csv(path: Path) -> bool:
    low = path.name.lower()
    if not low.endswith(".csv"):
        return False
    return any(p in low for p in _NAME_PATTERNS)


def _fix_file(path: Path, apply: bool, max_examples: int = 5):
    """Scan one CSV; return (rows_total, rows_changed, examples)."""
    rows_total = 0
    changed_rows: list[tuple[int, str, str]] = []   # (lineno, before, after)
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        if not rdr.fieldnames:
            return (0, [], None, [])
        size_col = _pick_size_col(rdr.fieldnames)
        desc_col = _pick_desc_col(rdr.fieldnames)
        if not size_col:
            # No SIZE column — skip (not one of the crawler outputs).
            return (0, [], None, [])
        new_rows = []
        for i, row in enumerate(rdr, start=2):   # +2 = header + 1-indexed
            rows_total += 1
            size_val = (row.get(size_col) or "").strip()
            if not size_val:
                new_rows.append(row); continue
            m = _INT_SIZE_RE.match(size_val)
            if not m:
                new_rows.append(row); continue
            # Size is integer-rim.  Does the description carry the
            # decimal form of the SAME size?
            desc_val = (row.get(desc_col) or "") if desc_col else ""
            # Scan every column's text, not just description — tempe /
            # tempeweborder sometimes put the full spec in COST or an
            # extra column so a .d can hide there too.
            haystack = " ".join(str(v or "") for v in row.values())
            cand_re = _decimal_variant_re(m.group(1))
            if not cand_re:
                new_rows.append(row); continue
            hit = cand_re.search(haystack)
            if not hit:
                new_rows.append(row); continue
            # Found a decimal variant — replace the SIZE cell with it.
            new_size = hit.group(0).upper().replace("ZR", "R")
            if new_size == size_val:
                new_rows.append(row); continue
            new_row = dict(row)
            new_row[size_col] = new_size
            new_rows.append(new_row)
            if len(changed_rows) < max_examples:
                _snippet = desc_val[:60].replace("\n", " ")
                changed_rows.append((i, size_val, new_size + f"  ← desc: {_snippet!r}"))
            else:
                changed_rows.append((i, size_val, new_size))
        fieldnames = list(rdr.fieldnames)
    if not changed_rows:
        return (rows_total, [], size_col, [])
    if apply:
        ts = time.strftime("%Y%m%d_%H%M%S")
        bak = path.with_suffix(path.suffix + f".bak-{ts}")
        shutil.copy2(path, bak)
        with open(path, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(new_rows)
    return (rows_total, changed_rows, size_col, [bak] if apply else [])


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("root", help="Folder to walk (recursively).")
    ap.add_argument("--apply", action="store_true",
                    help="Write changes back (default: dry-run).")
    ap.add_argument("--pattern-only", action="store_true",
                    help="Only touch files whose name matches a known "
                         "crawler prefix (jax / bobjane / tempe / "
                         "tempeweborder).  Default: on.")
    args = ap.parse_args(argv)
    root = Path(args.root).resolve()
    if not root.exists():
        print(f"ERROR: {root} does not exist.")
        return 2
    print(f"root: {root}")
    print(f"mode: {'APPLY (writes files + .bak)' if args.apply else 'DRY-RUN (no files touched)'}")
    scanned = 0
    changed_files = 0
    total_rows_changed = 0
    for p in root.rglob("*.csv"):
        if not _looks_like_price_csv(p):
            continue
        scanned += 1
        try:
            rows_total, changes, size_col, baks = _fix_file(p, args.apply)
        except Exception as e:
            print(f"  SKIP {p} — {e}")
            continue
        if not changes:
            continue
        changed_files += 1
        total_rows_changed += len(changes)
        rel = p.relative_to(root) if root in p.parents else p
        print(f"\n=== {rel}")
        print(f"  rows total: {rows_total:>6}   rows fixed: {len(changes):>6}")
        for lineno, before, after in changes[:5]:
            print(f"  line {lineno:>6}: {before!r:>18}  ->  {after}")
        if len(changes) > 5:
            print(f"  … and {len(changes) - 5} more.")
        if baks:
            print(f"  backup: {baks[0].name}")
    print()
    print(f"files scanned: {scanned}")
    print(f"files changed: {changed_files}")
    print(f"rows changed : {total_rows_changed}")
    if not args.apply and total_rows_changed:
        print("\n(dry-run — re-run with --apply to write the changes back)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
