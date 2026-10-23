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

# Oct-23 user ask: a specific set of truck sizes NEVER exists in
# integer-rim form on the market — they only ship with a .5 rim.
# When the crawler's old regex dropped the ".5" from the SIZE cell,
# the integer form it left behind is a dead giveaway of truncation.
# For those, we can safely auto-upgrade WITHOUT needing the ".5"
# text to live in the description column too.
#
# Rule set — keyed by the integer form the crawler produced, value
# is the correct decimal form.  All entries are "size that does not
# exist as a passenger / LT tyre with the integer rim":
#
#   • Light-truck 17.5" / 19.5" radials
#   • TBR 22.5" / 24.5" radials
#   • Numeric-tread TBR (11R, 12R, 13R) — the industry only sells
#     those with a .5 rim.
#
# User can extend this map as more sizes surface.
KNOWN_DECIMAL_RIM_UPGRADES = {
    # 17.5" rim — light-truck
    "205/70R17":  "205/70R17.5",
    "205/75R17":  "205/75R17.5",
    "215/75R17":  "215/75R17.5",
    "225/70R17":  "225/70R17.5",
    "225/75R17":  "225/75R17.5",
    "235/75R17":  "235/75R17.5",
    "245/70R17":  "245/70R17.5",
    "265/70R17":  "265/70R17.5",
    # 19.5" rim — light-truck
    "215/75R19":  "215/75R19.5",
    "225/70R19":  "225/70R19.5",
    "245/70R19":  "245/70R19.5",
    "265/70R19":  "265/70R19.5",
    "285/70R19":  "285/70R19.5",
    "305/70R19":  "305/70R19.5",
    # 22.5" rim — TBR
    "275/70R22":  "275/70R22.5",
    "275/80R22":  "275/80R22.5",
    "285/75R22":  "285/75R22.5",
    "295/60R22":  "295/60R22.5",
    "295/75R22":  "295/75R22.5",
    "295/80R22":  "295/80R22.5",
    "305/70R22":  "305/70R22.5",
    "315/60R22":  "315/60R22.5",
    "315/70R22":  "315/70R22.5",
    "315/80R22":  "315/80R22.5",
    "385/55R22":  "385/55R22.5",
    "385/65R22":  "385/65R22.5",
    "425/65R22":  "425/65R22.5",
    "445/50R22":  "445/50R22.5",
    "445/65R22":  "445/65R22.5",
    # 24.5" rim — TBR
    "275/80R24":  "275/80R24.5",
    "285/75R24":  "285/75R24.5",
    "315/80R24":  "315/80R24.5",
    # Numeric-tread TBR (always .5 in the market)
    "11R22":      "11R22.5",
    "11R24":      "11R24.5",
    "12R22":      "12R22.5",
    "12R24":      "12R24.5",
    "13R22":      "13R22.5",
}
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


_ENCODING_FALLBACKS = ("utf-8-sig", "utf-8", "cp1252", "latin-1")

def _open_csv_tolerant(path: Path):
    """Open a CSV with the first encoding that decodes without error.
    cp1252 / latin-1 catch most legacy Windows exports; utf-8-sig
    strips a BOM when present.  Returns (file-handle, encoding)."""
    for enc in _ENCODING_FALLBACKS:
        try:
            fh = open(path, "r", encoding=enc, newline="")
            fh.read(16384)   # force decode of a chunk to validate
            fh.seek(0)
            return fh, enc
        except (UnicodeDecodeError, UnicodeError):
            try: fh.close()
            except Exception: pass
            continue
    # Last-resort: open with errors='replace' so a bad byte never
    # kills the whole retrofit.  The replacement char ('�') is
    # tolerable in a description cell and no size regex touches it.
    return open(path, "r", encoding="utf-8", errors="replace", newline=""), "utf-8+replace"


def _fix_file(path: Path, apply: bool, max_examples: int = 5):
    """Scan one CSV; return (rows_total, rows_changed, examples)."""
    rows_total = 0
    changed_rows: list[tuple[int, str, str]] = []   # (lineno, before, after)
    fh, _enc = _open_csv_tolerant(path)
    with fh as f:
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
            # ---- Pass 1 (Oct-23 user ask): known-truck table.  If
            # the integer-rim form on the SIZE cell is one of the
            # sizes that CANNOT exist as a passenger / LT tyre, the
            # ".5" was definitely dropped by the old crawler — upgrade
            # directly, without needing the description to carry the
            # full spelling.
            _size_key = m.group(1).upper().replace("ZR", "R")
            new_size = KNOWN_DECIMAL_RIM_UPGRADES.get(_size_key)
            # ---- Pass 2: cross-reference against description.  For
            # sizes NOT in the known-truck table, fall back to the
            # original rule — only upgrade when the decimal form of
            # this exact size appears somewhere else in the row.
            if not new_size:
                desc_val = (row.get(desc_col) or "") if desc_col else ""
                haystack = " ".join(str(v or "") for v in row.values())
                cand_re = _decimal_variant_re(m.group(1))
                if not cand_re:
                    new_rows.append(row); continue
                hit = cand_re.search(haystack)
                if not hit:
                    new_rows.append(row); continue
                new_size = hit.group(0).upper().replace("ZR", "R")
            if new_size == size_val:
                new_rows.append(row); continue
            new_row = dict(row)
            new_row[size_col] = new_size
            new_rows.append(new_row)
            if len(changed_rows) < max_examples:
                # desc_val may be undefined when the known-truck pass
                # fired (Pass 2 was skipped) — resolve defensively.
                _desc_val = (row.get(desc_col) or "") if desc_col else ""
                _snippet = _desc_val[:60].replace("\n", " ")
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


def _diagnose_file(path: Path, want_rows: int = 6):
    """Oct-23 user ask: when a dry-run reports 0 changes, we need to
    see WHY.  This dump shows:
      • the detected SIZE / DESCRIPTION column names
      • how many rows carry an INTEGER-rim SIZE
      • of those, how many have ANY "Rdd.d" decimal-rim token in the
        row's text (that's the pool a retrofit can touch)
      • up to want_rows sample rows (SIZE | row-text first 100 chars)
    Non-destructive; writes nothing."""
    fh, enc = _open_csv_tolerant(path)
    with fh as f:
        rdr = csv.DictReader(f)
        if not rdr.fieldnames:
            print(f"  (empty / no header)")
            return
        size_col = _pick_size_col(rdr.fieldnames)
        desc_col = _pick_desc_col(rdr.fieldnames)
        print(f"  encoding={enc!r}  size_col={size_col!r}  desc_col={desc_col!r}")
        if not size_col:
            print(f"  no SIZE column — nothing to retrofit here.")
            return
        rows_total = 0
        rows_int   = 0        # SIZE is integer-rim
        rows_fix_known  = 0   # fixable via KNOWN_DECIMAL_RIM_UPGRADES
        rows_fix_desc   = 0   # fixable via description cross-reference
        samples_int = []      # integer-rim rows that are NOT fixable
        samples_fix = []      # fixable rows (either pass)
        # No \b around R — size strings like "265/70R19.5" have a
        # digit immediately before the R, so a word boundary at that
        # position (digit→R) fails even though the match IS present.
        _any_decimal_in_file = re.compile(r"R\d{2}\.\d(?!\d)", re.IGNORECASE)
        _any_dec_hit = False
        for row in rdr:
            rows_total += 1
            size_val = (row.get(size_col) or "").strip()
            m = _INT_SIZE_RE.match(size_val) if size_val else None
            row_text = " ".join(str(v or "") for v in row.values())
            if _any_decimal_in_file.search(row_text):
                _any_dec_hit = True
            if m:
                rows_int += 1
                _key = m.group(1).upper().replace("ZR", "R")
                _upgrade = KNOWN_DECIMAL_RIM_UPGRADES.get(_key)
                if _upgrade:
                    rows_fix_known += 1
                    if len(samples_fix) < want_rows:
                        samples_fix.append(("known", size_val, _upgrade, row_text[:100]))
                    continue
                cand_re = _decimal_variant_re(m.group(1))
                hit = cand_re.search(row_text) if cand_re else None
                if hit:
                    rows_fix_desc += 1
                    if len(samples_fix) < want_rows:
                        samples_fix.append(("desc", size_val, hit.group(0), row_text[:100]))
                else:
                    if len(samples_int) < want_rows:
                        samples_int.append((size_val, row_text[:100]))
        rows_fixable = rows_fix_known + rows_fix_desc
        print(f"  rows total: {rows_total:>6}   int-rim SIZE: {rows_int:>6}   "
              f"fixable total: {rows_fixable:>6}  (known-truck table: "
              f"{rows_fix_known}, desc cross-ref: {rows_fix_desc})   "
              f"any R\\d\\d\\.d in file: {_any_dec_hit}")
        if samples_fix:
            print(f"  FIXABLE samples:")
            for src, s_before, s_after, snippet in samples_fix:
                print(f"    [{src:>5}]  {s_before!r:>14}  ->  {s_after!r:<14}  row: {snippet!r}")
        if samples_int and not samples_fix:
            print(f"  int-rim samples (NOT fixable — no .d variant in row):")
            for s_val, snippet in samples_int:
                print(f"    {s_val!r:>14}  row: {snippet!r}")


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("root", help="Folder to walk (recursively).")
    ap.add_argument("--apply", action="store_true",
                    help="Write changes back (default: dry-run).")
    ap.add_argument("--diagnose", action="store_true",
                    help="Non-destructive dump — print per-file samples "
                         "+ counts so you can see WHY a dry-run found no "
                         "fixable rows (usually: the crawler stripped "
                         "the decimal rim from the description too, so "
                         "the CSV alone can't recover it; re-crawl is "
                         "the only option in that case).")
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
        if args.diagnose:
            rel = p.relative_to(root) if root in p.parents else p
            print(f"\n--- {rel}")
            try:
                _diagnose_file(p)
            except Exception as e:
                print(f"  diagnose failed: {e!r}")
            continue
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
