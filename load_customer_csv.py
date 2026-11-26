r"""Bulk-reload the `customer` table from a geocode.py-produced
customer_<yyMM>.csv -- one command, no Workbench, no manual column list.

Why this exists
---------------
Operator was pasting a `LOAD DATA LOCAL INFILE` block into MySQL
Workbench once a month.  The CSV has three leading columns that are
NOT in the customer table (channel2, Parent CGR3, Delivery Plant) plus
a Payment Terms column that was inserted BEFORE Telephone / Mobile
Phone in the CSV but AFTER them in the DB (ALTER TABLE appended it).
A bare LOAD DATA (no column list) maps CSV columns to DB columns
positionally, so Payment Terms landed in `telephone`, Telephone landed
in `mobile_phone`, and Mobile Phone landed in `payment_terms`.

This script reads the CSV HEADER row and maps columns by NAME, so a
renamed or reordered CSV column doesn't quietly wreck the load.
Columns in the CSV that have no matching DB column are silently
skipped; columns the DB has but the CSV doesn't carry are left NULL.

Default path
------------
E:\01. work\2025\Data_Anal_Website\rawdata\unlock\customer_<yyMM>.csv
-- the newest matching file wins.  Override with --csv.

Usage
-----
  python load_customer_csv.py --dry-run
      preview parsed rows + per-state count; no DB change

  python load_customer_csv.py
      TRUNCATE + bulk-INSERT; reports loaded total + 5 sample rows

  python load_customer_csv.py --csv "D:\somewhere\customer_2612.csv"
      explicit source file

Env (reuses app.py / load_payment_terms.py's DB_* vars):
  DB_HOST / DB_PORT / DB_USER / DB_PASS / DB_NAME
"""
from __future__ import annotations

import argparse
import csv
import glob
import os
import re
import sys
from datetime import datetime

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


DEFAULT_CSV_DIR = r"E:\01. work\2025\Data_Anal_Website\rawdata\unlock"
TABLE_NAME = "customer"


# DB column order AS IT WILL BE WRITTEN by the INSERT.  Mirrors the
# DESCRIBE customer schema the operator confirmed on Nov-26: 16 cols,
# payment_terms at the end (added later via ALTER TABLE ADD COLUMN).
DB_COLS = [
    "sold_to",        "sold_to_name",
    "ship_to_state",  "ship_to",       "ship_to_name",
    "bde_state",      "salesman_name",
    "longitude",      "latitude",
    "postcode",       "address_1",     "city",         "email",
    "telephone",      "mobile_phone",
    "payment_terms",
]


# CSV header -> DB column name.  Multiple spellings handle variations
# the operator's Customer Master export picks up over time (same list
# geocode.CUSTOMER_COLUMN_ALIASES keeps in sync).  Headers are compared
# after _norm_hdr() so case / spaces / punctuation don't matter.
CSV_HEADER_MAP = {
    # (lhs = normalised CSV header; rhs = DB column)
    "soldto":           "sold_to",
    "sold_to":          "sold_to",
    "soldtoname":       "sold_to_name",
    "sold_to_name":     "sold_to_name",
    "shiptostate":      "ship_to_state",
    "ship_to_state":    "ship_to_state",
    "shipto":           "ship_to",
    "ship_to":          "ship_to",
    "shiptoname":       "ship_to_name",
    "ship_to_name":     "ship_to_name",
    "name":             "ship_to_name",   # geocode.py exports "Name"
    "bdestate":         "bde_state",
    "bde_state":        "bde_state",
    "salesmanname":     "salesman_name",
    "salesman_name":    "salesman_name",
    "salesman":         "salesman_name",
    "longitude":        "longitude",
    "latitude":         "latitude",
    "postalcode":       "postcode",
    "postcode":         "postcode",
    "address1":         "address_1",
    "address_1":        "address_1",
    "address":          "address_1",
    "city":             "city",
    "emailaddress":     "email",
    "email":            "email",
    "paymentterms":     "payment_terms",
    "termsofpayment":   "payment_terms",
    "payment_terms":    "payment_terms",
    "telephone":        "telephone",
    "phone":            "telephone",
    "mobilephone":      "mobile_phone",
    "mobile":           "mobile_phone",
    "mobilenumber":     "mobile_phone",
}


# Columns the DB declares NOT NULL; if the CSV cell is blank the loader
# writes "" instead of NULL so the INSERT doesn't fail.  payment_terms
# is NOT NULL on this schema; everything else is nullable.
NOT_NULL_COLS = {"payment_terms"}


# ----------------------------------------------------------------------
def _norm_hdr(h: str) -> str:
    """Normalise a CSV header for the CSV_HEADER_MAP lookup: drop every
    non-alphanumeric char and lowercase the rest.  'E-Mail Address' ->
    'emailaddress'; 'Postal Code' -> 'postalcode'."""
    return re.sub(r"[^A-Za-z0-9]+", "", (h or "")).lower()


def _connect():
    try:
        import mysql.connector
    except Exception as e:
        print(f"[fatal] mysql.connector import failed: {e}", file=sys.stderr)
        sys.exit(1)
    cfg = {
        "host": os.getenv("DB_HOST", "127.0.0.1"),
        "port": int(os.getenv("DB_PORT", "3306")),
        "user": os.getenv("DB_USER", "root"),
        "password": os.getenv("DB_PASS", ""),
        "database": os.getenv("DB_NAME", "my_new_database"),
        "autocommit": False,
    }
    return mysql.connector.connect(**cfg)


def _pick_default_csv():
    """Return the most-recent customer_*.csv under DEFAULT_CSV_DIR (or
    the current working directory as a fallback).  Returns None when
    no candidate exists."""
    cands = []
    for d in (DEFAULT_CSV_DIR, os.getcwd()):
        cands.extend(glob.glob(os.path.join(d, "customer_*.csv")))
    cands = [p for p in cands if os.path.isfile(p)]
    if not cands:
        return None
    cands.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return cands[0]


def _normalise_cell(v: str, db_col: str):
    """Trim + coerce one CSV cell into the value the INSERT will use."""
    s = (v or "").strip()
    if db_col in ("longitude", "latitude"):
        if s == "":
            return None
        # Strip stray spaces + commas (SAP sometimes exports "151.23,"
        # or " 151.23 ").  Keep the sign / decimal point untouched.
        s = s.replace(",", "").strip()
        try:
            return float(s)
        except ValueError:
            return None
    if s == "":
        return "" if db_col in NOT_NULL_COLS else None
    return s


def _build_col_index(header_row):
    """Given the CSV's header row, return {db_col: csv_col_idx} for
    every DB column that has a matching CSV header.  Columns absent
    from the CSV are left out (INSERT will supply NULL for them)."""
    out = {}
    for idx, raw in enumerate(header_row):
        db_col = CSV_HEADER_MAP.get(_norm_hdr(raw))
        if db_col and db_col not in out:
            out[db_col] = idx
    return out


def _parse_rows(path):
    """Read the CSV header, build the column index, then yield one
    tuple-per-row in DB_COLS order (None / "" for unmatched columns)."""
    with open(path, "r", newline="", encoding="utf-8-sig", errors="replace") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            return 0, [], {}
        idx_map = _build_col_index(header)

        if "sold_to" not in idx_map and "ship_to" not in idx_map:
            raise RuntimeError(
                f"[fatal] CSV header has neither Sold-to nor Ship-to columns -- "
                f"refusing to load.  Header seen: {header[:8]}...")

        rows_out = []
        for row in reader:
            if not row or all((c or "").strip() == "" for c in row):
                continue
            tup = []
            for db_col in DB_COLS:
                i = idx_map.get(db_col)
                raw = row[i] if (i is not None and i < len(row)) else ""
                tup.append(_normalise_cell(raw, db_col))
            # Skip rows that have no sold_to AND no ship_to -- subtotals
            # or stray trailing lines sometimes appear in the export.
            if not (tup[DB_COLS.index("sold_to")]
                    or tup[DB_COLS.index("ship_to")]):
                continue
            rows_out.append(tuple(tup))
        return len(rows_out), rows_out, idx_map


# ----------------------------------------------------------------------
def main() -> int:
    ap = argparse.ArgumentParser(
        description="Bulk-reload the `customer` table from a "
                    "geocode.py-produced customer_<yyMM>.csv.")
    ap.add_argument("--csv", default="",
                    help="Path to the CSV (default: newest "
                         "customer_*.csv under "
                         f"{DEFAULT_CSV_DIR!r}).")
    ap.add_argument("--dry-run", action="store_true",
                    help="Parse + show the first 5 rows and a per-"
                         "state count without touching MySQL.")
    args = ap.parse_args()

    csv_path = args.csv or _pick_default_csv()
    if not csv_path:
        print(f"[customer_csv] no customer_*.csv found in "
              f"{DEFAULT_CSV_DIR!r} or cwd -- pass --csv explicitly.")
        return 2
    if not os.path.exists(csv_path):
        print(f"[customer_csv] file not found: {csv_path}")
        return 2

    print(f"[customer_csv] source : {csv_path}")
    try:
        mtime = datetime.fromtimestamp(os.path.getmtime(csv_path))
        print(f"[customer_csv] mtime  : {mtime.strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception:
        pass

    try:
        n, rows, idx_map = _parse_rows(csv_path)
    except Exception as e:
        print(f"[customer_csv][fatal] parse failed: {e}")
        return 1

    mapped = ", ".join(f"{c}@{idx_map[c]}" for c in DB_COLS if c in idx_map)
    missing = [c for c in DB_COLS if c not in idx_map]
    print(f"[customer_csv] mapped : {mapped}")
    if missing:
        print(f"[customer_csv] unmapped (will be NULL/\"\"): {missing}")
    print(f"[customer_csv] parsed rows: {n:,}")

    if rows[:5]:
        print(f"[customer_csv] first 5 rows preview:")
        for r in rows[:5]:
            # Trim long cells so the preview fits on one line.
            short = tuple(
                (str(v)[:30] + "…") if isinstance(v, str) and len(v) > 30
                else v for v in r)
            print(f"   {short}")

    # Per-state tally so the operator can spot lopsided loads early.
    by_state = {}
    for r in rows:
        st = r[DB_COLS.index("ship_to_state")] or "(none)"
        by_state[st] = by_state.get(st, 0) + 1
    print(f"[customer_csv] per state  : {by_state}")

    if args.dry_run:
        print("[customer_csv] --dry-run: not touching MySQL.")
        return 0

    if not rows:
        print("[customer_csv] no data rows -- refusing to TRUNCATE the "
              "live table with an empty load.")
        return 1

    col_list = ", ".join(f"`{c}`" for c in DB_COLS)
    placeholders = ", ".join(["%s"] * len(DB_COLS))
    insert_sql = (f"INSERT INTO `{TABLE_NAME}` ({col_list}) "
                  f"VALUES ({placeholders})")

    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(f"TRUNCATE TABLE `{TABLE_NAME}`;")
        BATCH = 1000
        for i in range(0, len(rows), BATCH):
            cur.executemany(insert_sql, rows[i:i + BATCH])
        conn.commit()

        cur.execute(f"SELECT COUNT(*) FROM `{TABLE_NAME}`;")
        loaded = cur.fetchone()[0]
        print(f"[customer_csv] loaded rows: {loaded:,}")

        # Sanity sample: pull 5 rows where payment_terms was set and
        # print the three columns that used to be mis-aligned so the
        # operator sees them land in the right place.
        cur.execute(
            f"SELECT ship_to, telephone, mobile_phone, payment_terms "
            f"FROM `{TABLE_NAME}` "
            f"WHERE payment_terms IS NOT NULL AND payment_terms <> '' "
            f"LIMIT 5")
        sample = cur.fetchall()
        if sample:
            print(f"[customer_csv] alignment check (ship_to, telephone, mobile_phone, payment_terms):")
            for row in sample:
                print(f"   {row}")
    finally:
        cur.close()
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
