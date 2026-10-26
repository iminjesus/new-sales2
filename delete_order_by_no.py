"""
delete_order_by_no.py — safe hard-delete helper for submitted_orders.

Usage (PowerShell OR cmd):
    # Default: drafts only (safe)
    python delete_order_by_no.py T261022_022N T261022_019N

    # Hard-delete a SUBMITTED order (approved / rejected / standard).
    # Requires typing "DELETE" at the confirm prompt so a stray run
    # can't nuke live rows.
    python delete_order_by_no.py 261026_011N --force

Rules the script enforces:
  • Default mode (no --force): only rows with needs_mgmt_approval
    = 'temp' (drafts) are deleted; anything else is refused.
  • --force mode: deletes ANY matched row AFTER an explicit typed
    confirmation.  Use when the Submitted Orders UI's soft-delete
    isn't enough (e.g. the submitter can't be impersonated or you
    want the row completely gone, not just greyed out).
  • A dry-run SELECT is printed first so you can verify who / what
    before the DELETE runs.
  • Credentials come from the same .env batch_orchestrator.py uses,
    so no secrets end up on your PowerShell history.

Exit code is 0 on success, 1 on any refusal or error.
"""
import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def _db_conn():
    """Open MySQL connection using credentials from .env.  Resilient:
      • parses .env MANUALLY so python-dotenv doesn't have to be
        installed in the Python this script happens to run under
        (Oct-23: Python 3.13 at C:\\Users\\...\\Python313 doesn't carry
        it; the batch's own Python does — same box, two interpreters);
      • auto-discovers the database if DB_NAME isn't set, by asking
        information_schema which schema owns submitted_orders;
      • accepts DB_PASSWORD or DB_PASS (both names seen in the wild),
        and DB_NAME or DB_DATABASE."""
    import mysql.connector
    # Manual .env parse — no dotenv dependency.
    try:
        with open(os.path.join(BASE_DIR, ".env")) as _f:
            for _line in _f:
                _s = _line.strip()
                if not _s or _s.startswith("#") or "=" not in _s:
                    continue
                _k, _v = _s.split("=", 1)
                os.environ.setdefault(_k.strip(),
                                      _v.strip().strip('"').strip("'"))
    except Exception:
        pass
    _db = os.getenv("DB_NAME") or os.getenv("DB_DATABASE") or ""
    _kw = dict(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD") or os.getenv("DB_PASS", ""),
    )
    if _db:
        _kw["database"] = _db
        return mysql.connector.connect(**_kw)
    # DB_NAME not configured — connect without one, ask the server
    # which schema actually owns submitted_orders, then USE it.
    conn = mysql.connector.connect(**_kw)
    cur  = conn.cursor()
    cur.execute(
        "SELECT TABLE_SCHEMA FROM information_schema.TABLES "
        "WHERE TABLE_NAME = 'submitted_orders' LIMIT 1"
    )
    row = cur.fetchone()
    cur.close()
    if not row:
        raise RuntimeError(
            "No submitted_orders table on this server — set DB_NAME in "
            ".env or create the table first."
        )
    cur = conn.cursor()
    cur.execute(f"USE `{row[0]}`")
    cur.close()
    return conn

def main(argv):
    if not argv:
        print("Usage: python delete_order_by_no.py <order_no> [order_no ...] [--force]")
        return 1
    force = False
    cleaned = []
    for a in argv:
        a = a.strip()
        if not a: continue
        if a == "--force":
            force = True
            continue
        cleaned.append(a)
    order_nos = cleaned
    if not order_nos:
        print("No order_no supplied.")
        return 1
    conn = _db_conn()
    cur  = conn.cursor(dictionary=True)
    try:
        ph = ",".join(["%s"] * len(order_nos))
        cur.execute(
            f"SELECT id, order_no, submitted_by_email, submitted_by_bde, "
            f"       sold_to_name, needs_mgmt_approval, submitted_at, "
            f"       approved_by_name, rejected_by_name, status_sap "
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
                  f"approved_by={r.get('approved_by_name')!r}  "
                  f"rejected_by={r.get('rejected_by_name')!r}  "
                  f"sap={r.get('status_sap')!r}  "
                  f"bde={r['submitted_by_bde']!r}  "
                  f"email={r['submitted_by_email']!r}  "
                  f"customer={r['sold_to_name']!r}  "
                  f"at={r['submitted_at']}")
        non_drafts = [r for r in rows
                      if (r.get("needs_mgmt_approval") or "").strip().lower() != "temp"]
        if non_drafts and not force:
            print()
            print("REFUSED — the following rows are NOT drafts:")
            for r in non_drafts:
                print(f"  id={r['id']}  order_no={r['order_no']}  "
                      f"state={r['needs_mgmt_approval']!r}")
            print()
            print("Options:")
            print("  (a) Soft-delete via the Submitted Orders UI's Delete button")
            print("      — keeps the row with deleted_by_* / deleted_at stamps.")
            print("  (b) Re-run with --force to HARD-DELETE these rows (irreversible).")
            print("      e.g.: python delete_order_by_no.py", " ".join(order_nos), "--force")
            return 1
        if force and non_drafts:
            # Oct-26 user ask: safeguard against a stray --force run
            # by requiring an explicit typed confirmation that names
            # the row count.
            print()
            print(f"⚠  --force will HARD-DELETE {len(rows)} row(s), "
                  f"{len(non_drafts)} of which are NOT drafts.")
            print(f"⚠  This is IRREVERSIBLE — no audit trail is kept.")
            try:
                ans = input('    Type exactly "DELETE" to proceed: ')
            except Exception:
                ans = ""
            if ans.strip() != "DELETE":
                print("Confirmation did not match — nothing deleted.")
                return 1
            cur.execute(
                f"DELETE FROM submitted_orders WHERE order_no IN ({ph})",
                tuple(order_nos),
            )
            conn.commit()
            print()
            print(f"Hard-deleted {cur.rowcount} row(s).")
            return 0
        # All drafts path — safe hard delete gated by needs_mgmt_approval='temp'.
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
