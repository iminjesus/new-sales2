#!/usr/bin/env python3
"""
cleanup_reapproved_orders.py - one-shot repair script.

Nov-17 user ask: before commit 30b28c4, api_orders_approve wrote the
approve-side columns but left the reject-side columns untouched, so
any order that was Rejected first and then Approved ended up carrying
BOTH sets of state (approved_by_name='X' AND rejected_by_name='Y').
Every downstream gate -- backend DO# save gate, backend SAP flag gate,
SPRF detail-page unlock JS, orders_list "Rejected" chip, and the
"Approved / DO not entered" tile counter -- correctly reads such a
row as still rejected because that is what the database says, so DO#
entry is locked and the list page shows a red Rejected pill.

30b28c4 fixes the approve code path, but existing rows that already
drifted into the bad state don't self-heal -- they need either a
fresh Approve click or this one-off clean-up pass.

Policy (strict - a row must match ALL):
    - approved_by_name IS NOT NULL AND TRIM(approved_by_name) <> ''
    - rejected_by_name IS NOT NULL AND TRIM(rejected_by_name) <> ''
    - approved_at IS NOT NULL AND rejected_at IS NOT NULL
    - approved_at > rejected_at           (approve clearly came last)
    - deleted_at IS NULL                  (don't touch deleted rows)

What it writes to every matching row:
    - rejected_by_email = NULL
    - rejected_by_name  = NULL
    - rejected_at       = NULL
    - reject_note       = NULL
    - needs_mgmt_approval = 'Y'

Usage:
    python cleanup_reapproved_orders.py --dry-run     # see what would be touched
    python cleanup_reapproved_orders.py --commit      # actually apply the UPDATE

Env (reuses app.py's DB_* vars):
    DB_HOST  / DB_PORT  / DB_USER  / DB_PASS  / DB_NAME
"""
from __future__ import annotations

import argparse
import os
import sys


def _connect():
    # Deferred import so --help works in environments without
    # mysql-connector-python installed (e.g. a lint-only shell).
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


SELECT_SQL = """
SELECT id, order_no,
       submitted_at,
       submitted_by_bde,
       sold_to, sold_to_name,
       approved_by_name, approved_at,
       rejected_by_name, rejected_at,
       needs_mgmt_approval,
       sd_no, status_sap
  FROM submitted_orders
 WHERE approved_by_name IS NOT NULL AND TRIM(approved_by_name) <> ''
   AND rejected_by_name IS NOT NULL AND TRIM(rejected_by_name) <> ''
   AND approved_at IS NOT NULL
   AND rejected_at IS NOT NULL
   AND approved_at > rejected_at
   AND deleted_at IS NULL
 ORDER BY submitted_at, id
"""

UPDATE_SQL = """
UPDATE submitted_orders
   SET rejected_by_email   = NULL,
       rejected_by_name    = NULL,
       rejected_at         = NULL,
       reject_note         = NULL,
       needs_mgmt_approval = 'Y'
 WHERE id = %s
   AND approved_by_name IS NOT NULL AND TRIM(approved_by_name) <> ''
   AND rejected_by_name IS NOT NULL AND TRIM(rejected_by_name) <> ''
   AND approved_at IS NOT NULL
   AND rejected_at IS NOT NULL
   AND approved_at > rejected_at
   AND deleted_at IS NULL
"""

# Diagnostic query -- rows where rejected_at is newer than approved_at.
# That is NOT a bug (reject properly follows an earlier approved slot
# lifted later), but the operator should see them so nothing is
# touched by surprise.
INVERSE_SQL = """
SELECT id, order_no,
       approved_by_name, approved_at,
       rejected_by_name, rejected_at,
       needs_mgmt_approval
  FROM submitted_orders
 WHERE approved_by_name IS NOT NULL AND TRIM(approved_by_name) <> ''
   AND rejected_by_name IS NOT NULL AND TRIM(rejected_by_name) <> ''
   AND rejected_at >= approved_at
   AND deleted_at IS NULL
 ORDER BY submitted_at, id
"""


def _fmt(v):
    if v is None:
        return ""
    return str(v)


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true",
                   help="List the rows that WOULD be touched, no UPDATE sent.")
    g.add_argument("--commit", action="store_true",
                   help="Apply the UPDATE after showing the list.")
    args = ap.parse_args()

    conn = _connect()
    cur = conn.cursor()

    cur.execute(SELECT_SQL)
    rows = cur.fetchall()

    print(f"Scope: rows with BOTH approved_by_name AND rejected_by_name set,")
    print(f"       where approved_at > rejected_at (approve came last).")
    print(f"Matching rows: {len(rows)}\n")

    if rows:
        print(f"{'id':>6}  {'order_no':<16} {'BDE':<22} "
              f"{'sold_to':<10} {'rejected':<19} {'approved':<19} "
              f"{'nma':<4} {'sd_no':<16}")
        print("-" * 140)
        for r in rows:
            (rid, order_no, submitted_at, bde,
             sold_to, sold_to_name,
             app_by, app_at, rej_by, rej_at,
             nma, sd_no, status_sap) = r
            print(f"{rid:>6}  "
                  f"{_fmt(order_no):<16} "
                  f"{_fmt(bde)[:22]:<22} "
                  f"{_fmt(sold_to):<10} "
                  f"{_fmt(rej_at)[:19]:<19} "
                  f"{_fmt(app_at)[:19]:<19} "
                  f"{_fmt(nma):<4} "
                  f"{_fmt(sd_no)[:16]:<16}")

    # Show (but never touch) the inverse set so the operator notices
    # cases where the reject is the latest event -- those are legitimate
    # rejections that should NOT be cleaned up.
    cur.execute(INVERSE_SQL)
    inv = cur.fetchall()
    if inv:
        print(f"\nAlso present (NOT touched by this script -- reject is "
              f"the LATER event here): {len(inv)}")
        for (rid, order_no, app_by, app_at, rej_by, rej_at, nma) in inv:
            print(f"  id={rid:>6} order={order_no} approved={_fmt(app_at)} "
                  f"rejected={_fmt(rej_at)} nma={nma}")

    if args.dry_run:
        print("\n--dry-run: nothing committed.")
        cur.close()
        conn.close()
        return

    if not rows:
        print("\nNothing to do.")
        cur.close()
        conn.close()
        return

    print(f"\nApplying UPDATE to {len(rows)} row(s)...")
    touched = 0
    for r in rows:
        rid = r[0]
        cur.execute(UPDATE_SQL, (rid,))
        touched += cur.rowcount
    conn.commit()
    print(f"UPDATE ok: {touched} row(s) touched.")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
