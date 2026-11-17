#!/usr/bin/env python3
"""
load_payment_terms.py -- bulk-update customer.payment_terms from
the latest customer_<yyMM>.csv that geocode.py wrote.

Why a dedicated loader: the full customer table is reloaded via an
established Workbench LOAD DATA path (managed outside this repo).
Adding a new column (payment_terms) to that reload would require
editing the operator's existing LOAD DATA script.  This script sits
beside it and only touches the one new column -- each CSV row
produces one UPDATE keyed on ship_to, so a repeat run is a no-op
unless the CSV has changed.

Policy:
    * Reads customer_<yyMM>.csv from OUTPUT_DIR (defaults to the same
      path geocode.py writes to -- override with --csv).
    * Walks the "Payment Terms" column, keyed on "Ship-to".
    * Issues one UPDATE per CSV row:
          UPDATE customer SET payment_terms = %s WHERE ship_to = %s
      (or, with --by sold_to, keys on sold_to instead).
    * Reports how many rows were updated vs. not-matched, and lists
      the first 20 misses so the operator can see which ship_tos the
      CSV carried that the customer table doesn't.
    * --dry-run prints the plan without hitting MySQL.

Usage:
    python load_payment_terms.py --dry-run
    python load_payment_terms.py --commit
    python load_payment_terms.py --commit --csv "E:/path/to/customer_2611.csv"
    python load_payment_terms.py --commit --by sold_to

Env (reuses app.py's DB_* vars):
    DB_HOST  / DB_PORT  / DB_USER  / DB_PASS  / DB_NAME
"""
from __future__ import annotations

import argparse
import csv
import glob
import os
import sys
from datetime import date

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


DEFAULT_CSV_DIR = r"E:\01. work\2025\Data_Anal_Website\rawdata\unlock"


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
        "use_pure": True,
    }
    return mysql.connector.connect(**cfg)


def _pick_default_csv():
    """Return the most-recent customer_<yyMM>.csv under DEFAULT_CSV_DIR
    (or the working directory as a fallback).  Returns None when no
    candidate exists."""
    candidates = []
    for d in (DEFAULT_CSV_DIR, os.getcwd()):
        candidates.extend(glob.glob(os.path.join(d, "customer_*.csv")))
    if not candidates:
        return None
    candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return candidates[0]


def _ensure_column(conn):
    """ALTER TABLE customer ADD COLUMN payment_terms if it doesn't
    exist.  Mirrors _ensure_customer_payment_terms_column() in app.py
    so the loader can run before the Flask app ever starts."""
    cur = conn.cursor()
    try:
        cur.execute("SHOW COLUMNS FROM customer LIKE %s", ("payment_terms",))
        if cur.fetchone():
            return False
        cur.execute("ALTER TABLE customer "
                    "ADD COLUMN `payment_terms` VARCHAR(10) "
                    "NOT NULL DEFAULT ''")
        conn.commit()
        return True
    finally:
        try: cur.close()
        except: pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=None,
                    help="Path to customer_<yyMM>.csv (default: newest "
                         f"under {DEFAULT_CSV_DIR} or cwd).")
    ap.add_argument("--by", default="ship_to",
                    choices=("ship_to", "sold_to"),
                    help="Which column to key the UPDATE on (default ship_to).")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true",
                   help="Read the CSV and show what would be UPDATEd; no DB writes.")
    g.add_argument("--commit", action="store_true",
                   help="Run the UPDATEs for real.")
    args = ap.parse_args()

    csv_path = args.csv or _pick_default_csv()
    if not csv_path:
        print("[fatal] no customer_*.csv found.  Pass --csv <path>.", file=sys.stderr)
        sys.exit(1)
    if not os.path.exists(csv_path):
        print(f"[fatal] csv not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    print(f"CSV: {csv_path}")
    print(f"Key: customer.{args.by}")

    # Parse the CSV: we need two columns -- the key (ship_to or sold_to)
    # and Payment Terms.  Header matching is case-insensitive and tolerant
    # of the hyphen variants geocode.py's FLAT_FILE_COLUMNS uses.
    key_header_aliases = {
        "ship_to": ("ship-to", "ship to", "shipto"),
        "sold_to": ("sold-to", "sold to", "soldto", "customer"),
    }
    pt_aliases = ("payment terms", "paymentterms", "payment_terms",
                  "terms of payment", "termsofpayment")

    def _norm(h):
        return (h or "").strip().lower()

    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, [])
        hmap = {_norm(h): i for i, h in enumerate(header)}
        key_idx = None
        for al in key_header_aliases[args.by]:
            if al in hmap:
                key_idx = hmap[al]; break
        pt_idx = None
        for al in pt_aliases:
            if al in hmap:
                pt_idx = hmap[al]; break
        if key_idx is None:
            print(f"[fatal] no {args.by} column in CSV header: {header}",
                  file=sys.stderr)
            sys.exit(1)
        if pt_idx is None:
            print(f"[fatal] no Payment Terms column in CSV header: {header}",
                  file=sys.stderr)
            print("Did you re-run geocode.py since the schema update?", file=sys.stderr)
            sys.exit(1)

        pairs = []  # (key_value, payment_terms)
        seen_key = set()
        for row in reader:
            if len(row) <= max(key_idx, pt_idx):
                continue
            k  = (row[key_idx] or "").strip()
            pt = (row[pt_idx]  or "").strip()
            if not k:
                continue
            # Skip duplicate keys (same ship_to repeated with same payment term)
            if k in seen_key:
                continue
            seen_key.add(k)
            pairs.append((k, pt))

    print(f"CSV distinct {args.by}s: {len(pairs)}")
    non_blank = sum(1 for _, p in pairs if p)
    print(f"  with a Payment Terms value: {non_blank}")
    print(f"  with blank Payment Terms  : {len(pairs) - non_blank}")

    if args.dry_run:
        print("\n--dry-run: no DB writes.  Sample of first 5:")
        for k, p in pairs[:5]:
            print(f"  {args.by}={k}  payment_terms={p!r}")
        return

    # Commit path.
    conn = _connect()
    try:
        added = _ensure_column(conn)
        if added:
            print("[customer] added column payment_terms VARCHAR(10) DEFAULT ''")
        cur = conn.cursor()
        try:
            updated = 0
            missed  = 0
            miss_examples = []
            sql = f"UPDATE customer SET payment_terms = %s WHERE {args.by} = %s"
            for k, p in pairs:
                cur.execute(sql, (p, k))
                if cur.rowcount == 0:
                    missed += 1
                    if len(miss_examples) < 20:
                        miss_examples.append(k)
                else:
                    updated += cur.rowcount
            conn.commit()
            print(f"UPDATE ok: {updated} row(s) touched, {missed} {args.by}(s) "
                  f"had no matching customer row.")
            if miss_examples:
                print(f"First {len(miss_examples)} unmatched {args.by}(s):")
                for k in miss_examples:
                    print(f"  {k}")
        finally:
            try: cur.close()
            except: pass
    finally:
        try: conn.close()
        except: pass


if __name__ == "__main__":
    main()
