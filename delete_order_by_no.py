"""
delete_order_by_no.py — safe draft deletion helper.

Usage (PowerShell OR cmd):
    python delete_order_by_no.py T261022_022N T261022_019N

Rules the script enforces:
  • Only rows with needs_mgmt_approval = 'temp' (drafts) can be
    hard-deleted.  Anything else (Y / N / R / blank) is refused.
    Submitted orders should be soft-deleted via the UI's Delete
    button so the audit trail is kept intact.
  • A dry-run SELECT is printed first; the actual DELETE then runs.
  • Credentials come from the same .env batch_orchestrator.py uses,
    so no secrets end up on your PowerShell history.

Exit code is 0 on success, 1 on any refusal or error.
"""
import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def _db_conn():
    import mysql.connector
    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(BASE_DIR, ".env"))
    except Exception:
        pass
    return mysql.connector.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "sales"),
    )

def main(argv):
    if not argv:
        print("Usage: python delete_order_by_no.py <order_no> [order_no ...]")
        return 1
    order_nos = [a.strip() for a in argv if a.strip()]
    if not order_nos:
        print("No order_no supplied.")
        return 1
    conn = _db_conn()
    cur  = conn.cursor(dictionary=True)
    try:
        ph = ",".join(["%s"] * len(order_nos))
        cur.execute(
            f"SELECT id, order_no, submitted_by_email, submitted_by_bde, "
            f"       sold_to_name, needs_mgmt_approval, submitted_at "
            f"FROM submitted_orders WHERE order_no IN ({ph})",
            tuple(order_nos),
        )
        rows = cur.fetchall() or []
        if not rows:
            print("No matching rows for:", ", ".join(order_nos))
            return 1
        print("Matched rows:")
        for r in rows:
            print(f"  id={r['id']:>6}  order_no={r['order_no']}  "
                  f"state={r['needs_mgmt_approval']!r:>6}  "
                  f"bde={r['submitted_by_bde']!r}  "
                  f"email={r['submitted_by_email']!r}  "
                  f"customer={r['sold_to_name']!r}  "
                  f"at={r['submitted_at']}")
        # Refuse anything that is not a draft.
        non_drafts = [r for r in rows
                      if (r.get("needs_mgmt_approval") or "").strip().lower() != "temp"]
        if non_drafts:
            print()
            print("REFUSED — the following rows are NOT drafts:")
            for r in non_drafts:
                print(f"  id={r['id']}  order_no={r['order_no']}  "
                      f"state={r['needs_mgmt_approval']!r}")
            print()
            print("Delete submitted orders through the Submitted Orders UI "
                  "so the deleted_by_* / deleted_at audit columns are set.")
            return 1
        # All drafts — hard delete.
        cur.execute(
            f"DELETE FROM submitted_orders WHERE order_no IN ({ph}) "
            f"AND needs_mgmt_approval = 'temp'",
            tuple(order_nos),
        )
        conn.commit()
        print()
        print(f"Deleted {cur.rowcount} draft row(s).")
        return 0
    finally:
        try: cur.close()
        except Exception: pass
        try: conn.close()
        except Exception: pass


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
