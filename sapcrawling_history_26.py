"""
sapcrawling_history_26.py

Historical stock-snapshot backfill for 2026.  For every month from
January 2026 through the current calendar month, this script:

  1. Loops the four plants (PLANTS from sapcrawling.py)
  2. Sets ZSDM64300's Interface Date to the 1st of that month
     (DD.MM.YYYY — same format the live crawler uses)
  3. Exports the ALV grid, keeps the same five yellow columns
     (interface_date, plant, material, dot_no, stock_qty)
  4. Combines the four plants into one per-month CSV
  5. Loads the CSV into `stock_history` with a `snapshot_date`
     column stamped as the 1st of that month.

Companion to sapcrawling.py, which only grabs *yesterday*'s data
for the live `stock` table.  This script feeds the historical
line-graph feature so the /stock page can plot how stock evolved
month-by-month.

Default behaviour is idempotent: months already present in
`stock_history` are skipped.  Flags:

  --force            Re-crawl every month even if already loaded.
                     Existing rows for those months are DELETEd
                     before the new ones are inserted.
  --only YYYY-MM     Crawl only that one month (e.g. 2026-03).

Pre-reqs (Windows):
  - SAP GUI open AND logged in
  - pywin32, openpyxl, mysql-connector-python, python-dotenv
  - LOCAL INFILE enabled on the MySQL server + client
"""

import os
import sys
import time
import shutil
import csv
from datetime import datetime, date

import win32com.client
import mysql.connector
from dotenv import load_dotenv

# Reuse every SAP-driving helper from the live crawler so the two
# scripts don't drift on which grid IDs / export paths actually
# work for THIS SAP layout.
from sapcrawling import (
    PLANTS,
    SAP_EXPORT_DIR,
    UNLOCK_DIR,
    OUTPUT_HEADER,
    ZSDM_FIELDS,
    wait,
    exists,
    close_popups,
    start_transaction_fresh,
    newest_export_xlsx,
    trigger_export,
    go_back_to_selection,
    extract_keep_rows,
    dump_screen,
    _try_set_text,
    _try_set_radio,
)


# HIGH bound of the interface-date range.  Many SAP DATS-range
# selection fields silently interpret a lone LOW as "anywhere
# from LOW to today" — the first backfill run pulled the same
# ~8900-row cross-section 8 times because HIGH was left empty
# and SAP fell back to today's data every call.  Setting HIGH
# equal to LOW turns the range into a single-day filter.
INTERFACE_DATE_HIGH_IDS = [
    "wnd[0]/usr/ctxtS_DATE-HIGH",
    "wnd[0]/usr/ctxtS_INTFDT-HIGH",
]


# ================= CONFIG =================
load_dotenv()

# Where per-month XLSXs + combined CSVs land.  Kept out of the
# live `unlock/` root so the daily loader can't accidentally
# ingest a historical snapshot into the current-stock table.
HISTORY_DIR = os.path.join(UNLOCK_DIR, "history_26")

# Snapshot range: from (START_YEAR, START_MONTH) through the
# current calendar month, inclusive.
START_YEAR  = 2026
START_MONTH = 1

# DB — same defaults as load_csv_to_mysql.py.
DB_HOST     = os.getenv("DB_HOST", "100.127.139.79")
DB_PORT     = int(os.getenv("DB_PORT", "3306"))
DB_USER     = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASS", "")
DB_NAME     = os.getenv("DB_NAME", "my_new_database")

TABLE_NAME  = "stock_history"

# CSV row terminator LOAD DATA expects — matches what csv.writer
# emits on Windows.
LINES_TERMINATED_BY = r"\r\n"

MIN_CSV_SIZE = 200


# ================= HELPERS =================
def _ensure_dir(d):
    if d and not os.path.isdir(d):
        os.makedirs(d, exist_ok=True)


def _mysql_path(p: str) -> str:
    return p.replace("\\", "/")


def _first_existing(session, id_list):
    for wid in id_list:
        if exists(session, wid):
            return wid
    return None


def _set_and_read(session, wid, value):
    """Assign `value` to field `wid` and read back what SAP stored.
    Returns the round-tripped text (or None on failure).  Used to
    tell 'set silently succeeded' from 'value actually stuck'."""
    try:
        session.findById(wid).Text = value
        try:    return session.findById(wid).Text
        except: return None
    except Exception:
        return None


def set_selection_screen_history(session, plant, ddmmyyyy, dump_once=False):
    """Stricter replacement for sapcrawling.set_selection_screen.

    Sets BOTH the LOW and HIGH interface-date bounds to the
    target day so SAP treats it as a single-day filter (not
    'LOW..today').  Reads the LOW field back to confirm the
    value stuck and prints a loud warning if it didn't — the
    first backfill run silently swallowed every date and returned
    the same day 8 times.  If `dump_once` is True, dumps the
    entire selection screen post-set so we can see the real
    field IDs on THIS SAP layout."""
    _try_set_text(session, ZSDM_FIELDS["plant"], plant)

    low_id  = _first_existing(session, ZSDM_FIELDS["interface_date"])
    high_id = _first_existing(session, INTERFACE_DATE_HIGH_IDS)

    if low_id:
        got_low = _set_and_read(session, low_id, ddmmyyyy)
        if got_low and ddmmyyyy in got_low:
            pass  # confirmed
        else:
            print(f"    [WARN] LOW date field {low_id} set to "
                  f"{ddmmyyyy!r} but reads back as {got_low!r}")
    else:
        print(f"    [WARN] No known LOW date field exists on this "
              f"selection screen — SAP will fall back to its default "
              f"(usually today's data).")

    if high_id:
        got_high = _set_and_read(session, high_id, ddmmyyyy)
        if got_high and ddmmyyyy not in got_high:
            print(f"    [WARN] HIGH date field {high_id} set to "
                  f"{ddmmyyyy!r} but reads back as {got_high!r}")
    # HIGH missing is OK on single-date screens; only warn if LOW
    # was also missing (already warned above).

    _try_set_radio(session, ZSDM_FIELDS["display_interface"])

    if dump_once:
        print(f"\n─── SELECTION SCREEN DUMP (plant={plant}, date={ddmmyyyy}) ───")
        try:    dump_screen(session)
        except Exception as e:  print(f"    dump failed: {e}")
        print("─── END DUMP ───\n")

    session.findById("wnd[0]").sendVKey(0)
    wait(0.4)
    close_popups(session, 3)


def _normalise_ddmmyyyy(val):
    """openpyxl reads DATE cells as datetime → str() gives ISO
    'YYYY-MM-DD HH:MM:SS', so LOAD DATA's STR_TO_DATE(..,'%d.%m.%Y')
    NULLs every row.  Convert whatever we get into DD.MM.YYYY so
    the LOAD parses cleanly."""
    val = (val or "").strip()
    if not val:
        return ""
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
                "%Y-%m-%d",           "%d.%m.%Y",
                "%d/%m/%Y",           "%Y%m%d"):
        try:
            return datetime.strptime(val, fmt).strftime("%d.%m.%Y")
        except Exception:
            pass
    return val   # give up gracefully; LOAD will null it


def months_from_start():
    """Yield (year, month) from (START_YEAR, START_MONTH) up to
    and including the current calendar month."""
    y, m = START_YEAR, START_MONTH
    today = date.today()
    while (y, m) <= (today.year, today.month):
        yield y, m
        m += 1
        if m > 12:
            m = 1; y += 1


# ================= DB LAYER =================
def ensure_history_table(conn):
    """Create `stock_history` on first run.  Same shape as the
    live `stock` table plus a `snapshot_date` column at the front
    that tags each row with the 1st of the month it belongs to."""
    cur = conn.cursor()
    try:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` (
                snapshot_date  DATE NOT NULL,
                interface_date DATE,
                plant          VARCHAR(10),
                material       VARCHAR(20),
                dot_no         VARCHAR(6),
                stock_qty      INT,
                INDEX idx_snap             (snapshot_date),
                INDEX idx_snap_plant_mat   (snapshot_date, plant, material),
                INDEX idx_snap_dot         (snapshot_date, dot_no)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        conn.commit()
    finally:
        cur.close()


def existing_snapshots(conn):
    """Set of snapshot_date DATEs already in the table."""
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT TABLE_NAME FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s",
            (TABLE_NAME,))
        if not cur.fetchone():
            return set()
        cur.execute(f"SELECT DISTINCT snapshot_date FROM `{TABLE_NAME}`;")
        return {row[0] for row in cur.fetchall() if row[0]}
    finally:
        cur.close()


def delete_snapshot(conn, snap):
    """Wipe one month's rows so a re-crawl doesn't double up."""
    cur = conn.cursor()
    try:
        cur.execute(f"DELETE FROM `{TABLE_NAME}` WHERE snapshot_date = %s;",
                    (snap,))
        conn.commit()
    finally:
        cur.close()


def load_month_csv(conn, snap, csv_path):
    """DELETE any existing rows for `snap`, then LOAD the combined
    per-month CSV into `stock_history` with snapshot_date stamped
    on every inserted row."""
    delete_snapshot(conn, snap)

    load_sql = f"""
    LOAD DATA LOCAL INFILE '{_mysql_path(csv_path)}'
    INTO TABLE `{TABLE_NAME}`
    CHARACTER SET utf8mb4
    FIELDS TERMINATED BY ','
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY '{LINES_TERMINATED_BY}'
    IGNORE 1 LINES
    (@interface_date, @plant, @material, @dot_no, @stock_qty)
    SET
      snapshot_date  = '{snap.strftime('%Y-%m-%d')}',
      interface_date = STR_TO_DATE(TRIM(@interface_date), '%d.%m.%Y'),
      plant          = TRIM(@plant),
      material       = TRIM(@material),
      dot_no         = TRIM(@dot_no),
      stock_qty      = CAST(REPLACE(REPLACE(TRIM(@stock_qty), ',', ''), '"', '') AS SIGNED);
    """
    cur = conn.cursor()
    try:
        cur.execute(load_sql)
        conn.commit()
        cur.execute(f"SELECT COUNT(*) FROM `{TABLE_NAME}` "
                    f"WHERE snapshot_date = %s;", (snap,))
        n = cur.fetchone()[0]
        print(f"  [{snap}] Loaded {n:,} rows into {TABLE_NAME}")
    finally:
        cur.close()


# ================= COMBINE HELPERS =================
def combine_month(xlsx_paths, csv_path):
    """Same shape as sapcrawling.combine_to_csv but writes to a
    per-month CSV path so parallel months don't collide.  Also
    normalises the interface_date column into DD.MM.YYYY so the
    LOAD DATA's STR_TO_DATE actually parses (the first backfill
    left interface_date NULL because openpyxl handed us ISO)."""
    _ensure_dir(os.path.dirname(csv_path))
    idate_idx = OUTPUT_HEADER.index("interface_date")
    total = 0
    seen_idates = set()
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(OUTPUT_HEADER)
        for p in xlsx_paths:
            if not p or not os.path.exists(p):
                continue
            rows = extract_keep_rows(p)
            for r in rows:
                r[idate_idx] = _normalise_ddmmyyyy(r[idate_idx])
                if r[idate_idx]:
                    seen_idates.add(r[idate_idx])
                w.writerow(r)
            total += len(rows)
            print(f"    {os.path.basename(p)} → {len(rows):,} rows")
    print(f"  Combined CSV → {csv_path}  ({total:,} rows)")
    if seen_idates:
        print(f"  Interface dates in this month's CSV: {sorted(seen_idates)}")
    return total


# ================= SAP LOOP FOR ONE MONTH =================
_DUMPED_SELECTION = False   # print the selection-screen dump once per run

def run_one_month(session, snap):
    """Loop the plants for ONE snapshot month.  Returns list of
    per-plant XLSX file paths that we successfully collected."""
    global _DUMPED_SELECTION
    ddmmyyyy = snap.strftime("%d.%m.%Y")
    ym_tag   = snap.strftime("%Y%m%d")
    _ensure_dir(HISTORY_DIR)

    xlsx_files = []
    for plant in PLANTS:
        print(f"  ── {plant} @ {ddmmyyyy} ──")
        start_transaction_fresh(session)
        # Dump the selection screen on the very first call of the
        # run so we can see which date-field IDs actually exist on
        # this SAP layout (useful to diagnose 'same data 8 times'
        # failures).
        dump_now = not _DUMPED_SELECTION
        set_selection_screen_history(session, plant, ddmmyyyy,
                                     dump_once=dump_now)
        if dump_now:
            _DUMPED_SELECTION = True

        export_start_ts = time.time()
        session.findById("wnd[0]/tbar[1]/btn[8]").press()   # F8 execute
        wait(2.0); close_popups(session, 4)

        if not trigger_export(session):
            print(f"    [ERROR] {plant}: could not trigger export")
            continue

        # Wait for the SAP-side EXPORT_*.xlsx to finish writing
        # (poll size until it stops growing).
        xlsx_path = ""
        for _ in range(50):
            xlsx_path = newest_export_xlsx(export_start_ts)
            if xlsx_path and os.path.exists(xlsx_path):
                s1 = os.path.getsize(xlsx_path); wait(0.8)
                s2 = os.path.getsize(xlsx_path)
                if s2 >= s1 and s2 > 500:
                    break
            wait(0.6)
        if not xlsx_path:
            print(f"    [ERROR] {plant}: SAP export XLSX not found in {SAP_EXPORT_DIR}")
            go_back_to_selection(session); continue

        dest = os.path.join(HISTORY_DIR, f"dot_stock_{ym_tag}_{plant}.xlsx")
        shutil.copy2(xlsx_path, dest)
        print(f"    {plant} XLSX → {dest}")
        xlsx_files.append(dest)

        go_back_to_selection(session)
    return xlsx_files


# ================= ARG PARSING =================
def _parse_args():
    """Tiny arg parser — no argparse dependency needed for two
    flags."""
    force   = False
    only    = None
    args    = sys.argv[1:]
    i = 0
    while i < len(args):
        a = args[i]
        if a == "--force":
            force = True
        elif a == "--only" and i + 1 < len(args):
            try:
                only = datetime.strptime(args[i + 1], "%Y-%m").date().replace(day=1)
            except Exception:
                print(f"Invalid --only value {args[i+1]!r} (expected YYYY-MM)")
                sys.exit(2)
            i += 1
        else:
            print(f"Unknown arg: {a!r}"); sys.exit(2)
        i += 1
    return force, only


# ================= MAIN =================
def main():
    force, only = _parse_args()
    _ensure_dir(HISTORY_DIR)

    db = mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER,
        password=DB_PASSWORD, database=DB_NAME,
        allow_local_infile=True,
    )
    try:
        ensure_history_table(db)
        already = existing_snapshots(db)
        if already:
            print(f"Already in {TABLE_NAME}: "
                  f"{sorted(str(d) for d in already)}")

        # Pick which snapshot dates we're doing this run.
        if only is not None:
            snap_dates = [only]
        else:
            snap_dates = [date(y, m, 1) for (y, m) in months_from_start()]
            if not force:
                snap_dates = [d for d in snap_dates if d not in already]

        if not snap_dates:
            print("Nothing to do (all months already loaded; pass --force to re-crawl).")
            return
        print(f"Will process: {[str(d) for d in snap_dates]}")

        # Attach to SAP GUI once for the whole run.
        sap = win32com.client.GetObject("SAPGUI").GetScriptingEngine
        sap_conn = sap.Children(0)
        session  = sap_conn.Children(0)
        try:
            session.findById("wnd[0]").maximize()
        except Exception:
            pass

        for snap in snap_dates:
            print(f"\n═════ Snapshot {snap} ═════")
            xlsx_files = run_one_month(session, snap)
            if not xlsx_files:
                print(f"  [SKIP] {snap} — no XLSXs collected")
                continue

            csv_path = os.path.join(
                HISTORY_DIR, f"dot_stock_{snap.strftime('%Y%m%d')}.csv")
            rows = combine_month(xlsx_files, csv_path)
            if rows == 0 or (
                os.path.exists(csv_path) and
                os.path.getsize(csv_path) < MIN_CSV_SIZE):
                print(f"  [SKIP] {snap} — combined CSV too small")
                continue

            load_month_csv(db, snap, csv_path)

        print("\nAll done.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
