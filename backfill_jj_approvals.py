#!/usr/bin/env python3
"""
backfill_jj_approvals.py - one-shot migration script.

Nov-03 user ask: historical orders already have a SAP DO number
(sd_no is set) and status_sap='Y', so they went through SAP long
ago.  The newly-added stock-out marker + the inter-state detection
retro-fit, however, re-opened some of them as "pending approval"
on the orders_list page even though no further sign-off is needed.
This script stamps every such order as approved by JJ at its
submission time, so the pending badge disappears and the Approver
column reads a clean "JJ".

Policy (strict - a row must match ALL):
    - sd_no IS NOT NULL AND TRIM(sd_no) <> ''  (DO already issued)
    - (approved_by_name IS NULL OR approved_by_name = '')
    - (rejected_by_name IS NULL OR rejected_by_name = '')
    - deleted_at IS NULL

What it writes:
    - approved_by_email = junjong.cho@hankooktyre.com.au
    - approved_by_name  = 'JJ'
    - approved_at       = submitted_at
    - approval_note     = 'Backfill: DO already issued (<yyyy-mm-dd>)'
    - approved_b        = 'Y'
    - approved_b_at     = submitted_at
    - needs_mgmt_approval = 'N'

Usage:
    python backfill_jj_approvals.py --dry-run     # see what would be touched
    python backfill_jj_approvals.py --commit      # actually apply the UPDATE

Env (reuses app.py's DB_* vars):
    DB_HOST  / DB_PORT  / DB_USER  / DB_PASS  / DB_NAME
"""
from __future__ import annotations

import argparse
import datetime
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


JJ_EMAIL = "junjong.cho@hankooktyre.com.au"
JJ_NAME  = "JJ"


def _scan(cur) -> list[dict]:
    cur.execute(
        """
        SELECT id, order_no, submitted_at, sold_to, sold_to_name,
               sd_no, status_sap, needs_mgmt_approval,
               approved_by_name, rejected_by_name
        FROM   submitted_orders
        WHERE  sd_no IS NOT NULL
          AND  TRIM(sd_no) <> ''
          AND  (approved_by_name IS NULL OR TRIM(approved_by_name) = '')
          AND  (rejected_by_name IS NULL OR TRIM(rejected_by_name) = '')
          AND  deleted_at IS NULL
        ORDER BY submitted_at ASC
        """
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _apply(cur, rows: list[dict]) -> int:
    """Apply JJ-approval to the given rows.  Returns the row count."""
    today = datetime.date.today().isoformat()
    note = f"Backfill: DO already issued ({today})"
    sql = """
        UPDATE submitted_orders
           SET approved_by_email    = %s,
               approved_by_name     = %s,
               approved_at          = submitted_at,
               approval_note        = %s,
               approved_b           = 'Y',
               approved_b_at        = submitted_at,
               needs_mgmt_approval  = 'N'
         WHERE id = %s
           AND (approved_by_name IS NULL OR TRIM(approved_by_name) = '')
           AND (rejected_by_name IS NULL OR TRIM(rejected_by_name) = '')
           AND sd_no IS NOT NULL
           AND TRIM(sd_no) <> ''
    """
    touched = 0
    for r in rows:
        cur.execute(sql, (JJ_EMAIL, JJ_NAME, note, r["id"]))
        touched += cur.rowcount or 0
    return touched


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--dry-run", action="store_true",
                   help="Print the rows that WOULD be updated; make no DB changes.")
    g.add_argument("--commit", action="store_true",
                   help="Actually apply the UPDATE.  Prints every id touched.")
    ap.add_argument("--limit", type=int, default=0,
                   help="Cap the scan to the N oldest rows (for staged rollouts).")
    args = ap.parse_args()

    conn = _connect()
    cur  = conn.cursor()
    try:
        rows = _scan(cur)
        if args.limit:
            rows = rows[:args.limit]
        print(f"[scan] {len(rows)} candidate row(s) (DO issued, no approver / rejector, not deleted).")
        if not rows:
            print("[info] nothing to do.")
            return
        print("")
        print(f"{'id':>8}  {'order_no':<14}  {'submitted_at':<19}  {'sd_no':<12}  "
              f"{'status_sap':<3}  {'need_appr':<3}  sold_to")
        for r in rows[:30]:
            print(f"{r['id']:>8}  {str(r.get('order_no') or ''):<14}  "
                  f"{str(r.get('submitted_at') or ''):<19}  "
                  f"{str(r.get('sd_no') or '')[:12]:<12}  "
                  f"{str(r.get('status_sap') or ''):<3}  "
                  f"{str(r.get('needs_mgmt_approval') or ''):<3}  "
                  f"{str(r.get('sold_to') or '')} / "
                  f"{str(r.get('sold_to_name') or '')}")
        if len(rows) > 30:
            print(f"       ...and {len(rows) - 30} more.")
        print("")

        if args.dry_run:
            print("[dry-run] no changes applied.  Re-run with --commit to apply.")
            return

        print(f"[commit] applying JJ-approval to {len(rows)} row(s)...")
        touched = _apply(cur, rows)
        conn.commit()
        print(f"[done] UPDATE touched {touched} row(s).  "
              f"approved_by_name = {JJ_NAME!r}, approved_at = submitted_at.")
    finally:
        try: cur.close()
        except Exception: pass
        try: conn.close()
        except Exception: pass


if __name__ == "__main__":
    main()
