r"""Load a manually-downloaded SAP stock XLSX into `stock_rt`.

Why this exists
---------------
The hourly batch pipeline refreshes `stock_rt` by running
sapcrawling_stock_rt.py against SAP (/ZMMR40430), writing a dated CSV
at `rawdata/unlock/rt_stock_<YYYYMMDD>.csv`, and then handing that CSV
to `load_csv_to_mysql.load_stock_rt()`.

When that SAP export is too slow / hangs (SAP server load,
Factory/On-water column recomputation, etc.) the operator can still
pull the same report manually, save it as an XLSX, and drop it into
`E:\01. work\2025\Data_Anal_Website\stock.XLSX`.  This script
takes that XLSX and does the same `stock_rt` reload the CSV loader
would have done, so SPRF keeps showing fresh numbers until the
automated pipeline is working again.

XLSX layout (observed in the operator's file, Nov-23):

  A Plant          -> plant
  B Material       -> material
  C Description    -> description
  D Pattern        -> pattern
  E Load Index     -> load_index
  F HQ Group       -> hq_group
  G On Order       -> on_order
  H Stock          -> unrestricted    (**renamed in the XLSX**)
  I ATP Qt         -> atp_qt
  J List Price     -> list_price
  K At dock        -> at_dock
  L On water       -> on_water
  M Factory        -> factory
  N 3M Avg.        -> avg_3m

The XLSX header for the primary stock number is "Stock" rather than
"Unrestricted" (SAP label).  It is the same column -- the operator's
spreadsheet template just renamed it.

Table schema is identical to what load_csv_to_mysql.load_stock_rt
creates (CREATE IF NOT EXISTS so a pre-existing table with custom
indexes / extra columns survives).  Row set is replaced on every
load (TRUNCATE + bulk insert) so the result is a fresh snapshot.

batch_status stamps written on success (so the SPRF "Stock Info
updated" badge refreshes):

  stock_rt_loaded_mtime  = XLSX file mtime (epoch seconds)
  stock_rt_loaded_from   = XLSX basename
  stock_rt_csv_at        = XLSX mtime formatted "YYYY-MM-DD HH:MM:SS"

Usage
-----
  python load_stock_rt_xlsx.py
      - reads the default path above

  python load_stock_rt_xlsx.py --xlsx "C:\path\to\stock.XLSX"
      - override the source file

  python load_stock_rt_xlsx.py --sheet "Sheet1"
      - override the sheet name (default: first sheet)
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime

import mysql.connector
from dotenv import load_dotenv
from openpyxl import load_workbook

load_dotenv()

# --- same connection details as load_csv_to_mysql.py -----------------
DB_HOST     = os.getenv("DB_HOST", "100.127.139.79")
DB_PORT     = int(os.getenv("DB_PORT", "3306"))
DB_USER     = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASS", "")
DB_NAME     = os.getenv("DB_NAME", "my_new_database")

STOCK_RT_TABLE = "stock_rt"

# Default operator path -- the XLSX the user keeps dropping the manual
# SAP export into.  Overridable with --xlsx.
DEFAULT_XLSX = r"E:\01. work\2025\Data_Anal_Website\stock.XLSX"


CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS `{STOCK_RT_TABLE}` (
  plant         VARCHAR(10),
  material      VARCHAR(20),
  description   VARCHAR(255),
  pattern       VARCHAR(40),
  load_index    VARCHAR(40),
  hq_group      VARCHAR(40),
  on_order      INT         DEFAULT 0,
  unrestricted  INT         DEFAULT 0,
  atp_qt        INT         DEFAULT 0,
  list_price    DECIMAL(14,2) DEFAULT 0,
  at_dock       INT         DEFAULT 0,
  on_water      INT         DEFAULT 0,
  factory       INT         DEFAULT 0,
  avg_3m        DECIMAL(14,2) DEFAULT 0,
  snapshot_date DATE,
  INDEX idx_stock_rt_mat   (material),
  INDEX idx_stock_rt_plant (plant, material)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

INSERT_SQL = f"""
INSERT INTO `{STOCK_RT_TABLE}`
  (plant, material, description, pattern, load_index, hq_group,
   on_order, unrestricted, atp_qt, list_price,
   at_dock, on_water, factory, avg_3m, snapshot_date)
VALUES
  (%s, %s, %s, %s, %s, %s,
   %s, %s, %s, %s,
   %s, %s, %s, %s, %s)
"""


# ---------------- helpers ----------------
def _to_int(v) -> int:
    """XLSX cells come back as int / float / str / None.  Normalise to int.
    Empty / blank / non-numeric -> 0."""
    if v is None:
        return 0
    if isinstance(v, (int,)):
        return int(v)
    if isinstance(v, float):
        return int(round(v))
    s = str(v).strip().replace(",", "")
    if not s:
        return 0
    try:
        return int(round(float(s)))
    except ValueError:
        return 0


def _to_dec(v) -> float:
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "").replace("$", "")
    if not s:
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def _to_str(v) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    # Openpyxl sometimes returns floats for numeric-looking text cells
    # (e.g. material "2001535" -> 2001535.0).  Strip the trailing ".0".
    if isinstance(v, float) and s.endswith(".0"):
        s = s[:-2]
    return s


def _parse_rows(ws):
    """Yield one tuple per data row, skipping the header.

    Columns are positional (A..N) rather than header-name based so a
    renamed header ("Stock" vs "Unrestricted", "3M Avg." vs "3M Avg.
    Mthly Sale" etc.) does not break the loader.  If the sheet layout
    ever changes, this is the one place to update.
    """
    header_seen = False
    for row in ws.iter_rows(values_only=True):
        if not header_seen:
            # The first row is the header.  Don't try to be clever --
            # the operator's template is stable.
            header_seen = True
            continue
        # Drop blank rows (all cells None / empty).
        if row is None:
            continue
        if all(c is None or (isinstance(c, str) and not c.strip())
               for c in row):
            continue
        # Pad short rows to 14 columns so index access is safe.
        cells = list(row) + [None] * (14 - len(row)) if len(row) < 14 else list(row)
        yield cells[:14]


def _snapshot_date_for(xlsx_path: str) -> date:
    """Prefer the XLSX file's mtime date; fall back to today.
    This matches the dated-filename convention the CSV loader uses."""
    try:
        return date.fromtimestamp(os.path.getmtime(xlsx_path))
    except Exception:
        return date.today()


# ---------------- main ----------------
def main() -> int:
    ap = argparse.ArgumentParser(
        description="Bulk-load a manually-exported SAP stock XLSX into "
                    "the `stock_rt` MySQL table (same schema and "
                    "batch_status stamping as the automated CSV loader).")
    ap.add_argument("--xlsx", default=DEFAULT_XLSX,
                    help=f"Path to the stock XLSX (default: {DEFAULT_XLSX})")
    ap.add_argument("--sheet", default="",
                    help="Worksheet name (default: first sheet).")
    ap.add_argument("--dry-run", action="store_true",
                    help="Parse + print the first 5 rows and the row "
                         "count, but do NOT touch MySQL.")
    args = ap.parse_args()

    xlsx = args.xlsx
    if not os.path.exists(xlsx):
        print(f"[stock_rt_xlsx] file not found: {xlsx}")
        return 2

    print(f"[stock_rt_xlsx] source : {xlsx}")
    snap = _snapshot_date_for(xlsx)
    print(f"[stock_rt_xlsx] snapshot_date = {snap.isoformat()}  (file mtime date)")

    # openpyxl: read_only mode -- the file can be huge (~5k rows) and
    # we never write back.  data_only=True returns formula RESULTS
    # rather than the formula text.
    wb = load_workbook(filename=xlsx, read_only=True, data_only=True)
    try:
        if args.sheet:
            if args.sheet not in wb.sheetnames:
                print(f"[stock_rt_xlsx] sheet '{args.sheet}' not found. "
                      f"Available: {wb.sheetnames}")
                return 2
            ws = wb[args.sheet]
        else:
            ws = wb[wb.sheetnames[0]]
        print(f"[stock_rt_xlsx] sheet  : {ws.title}")

        batch: list[tuple] = []
        for cells in _parse_rows(ws):
            (c_plant, c_mat, c_desc, c_pat, c_li, c_hqg,
             c_oo, c_stock, c_atp, c_lp,
             c_dock, c_water, c_fac, c_3m) = cells

            # Skip rows without a material code -- subtotals / blank
            # separators sometimes appear at the bottom of the sheet.
            mat = _to_str(c_mat)
            if not mat:
                continue

            batch.append((
                _to_str(c_plant),
                mat,
                _to_str(c_desc),
                _to_str(c_pat),
                _to_str(c_li),
                _to_str(c_hqg),
                _to_int(c_oo),
                _to_int(c_stock),   # XLSX "Stock" -> DB "unrestricted"
                _to_int(c_atp),
                _to_dec(c_lp),
                _to_int(c_dock),
                _to_int(c_water),
                _to_int(c_fac),
                _to_dec(c_3m),
                snap,
            ))
    finally:
        wb.close()

    print(f"[stock_rt_xlsx] parsed rows: {len(batch):,}")
    if batch[:5]:
        print(f"[stock_rt_xlsx] first 5 rows preview:")
        for r in batch[:5]:
            print(f"   {r}")

    if args.dry_run:
        print("[stock_rt_xlsx] --dry-run: not touching MySQL")
        return 0

    if not batch:
        print("[stock_rt_xlsx] no data rows -- refusing to TRUNCATE "
              "the live table with an empty load.")
        return 1

    conn = mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER,
        password=DB_PASSWORD, database=DB_NAME,
    )
    cur = conn.cursor()
    try:
        cur.execute(CREATE_SQL)
        cur.execute(f"TRUNCATE TABLE `{STOCK_RT_TABLE}`;")
        # executemany is much faster than per-row inserts over the
        # Tailscale link to the home-PC MySQL (~5k rows in <2s vs ~45s).
        BATCH_SZ = 1000
        for i in range(0, len(batch), BATCH_SZ):
            cur.executemany(INSERT_SQL, batch[i:i + BATCH_SZ])
        conn.commit()

        cur.execute(f"SELECT COUNT(*) FROM `{STOCK_RT_TABLE}`;")
        total = cur.fetchone()[0]
        cur.execute(f"SELECT plant, COUNT(*) FROM `{STOCK_RT_TABLE}` "
                    f"GROUP BY plant ORDER BY plant;")
        print(f"[stock_rt_xlsx] loaded rows: {total:,}")
        for p, n in cur.fetchall():
            print(f"                {p or '(none)':<6} {n:>10,}")

        # Stamp batch_status so SPRF's "Stock Info updated: <ts>" label
        # refreshes.  Mirrors what load_csv_to_mysql.load_stock_rt does.
        try:
            xlsx_mtime = int(os.path.getmtime(xlsx))
            csv_at = datetime.fromtimestamp(xlsx_mtime).strftime(
                "%Y-%m-%d %H:%M:%S")
            cur.execute(
                "CREATE TABLE IF NOT EXISTS batch_status ("
                "  k VARCHAR(64) PRIMARY KEY,"
                "  v TEXT NULL,"
                "  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP "
                "    ON UPDATE CURRENT_TIMESTAMP"
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
            )
            cur.execute(
                "INSERT INTO batch_status (k, v) VALUES "
                "('stock_rt_loaded_mtime', %s), "
                "('stock_rt_loaded_from',  %s), "
                "('stock_rt_csv_at',       %s) "
                "ON DUPLICATE KEY UPDATE v = VALUES(v)",
                (str(xlsx_mtime), os.path.basename(xlsx), csv_at),
            )
            conn.commit()
            print(f"[stock_rt_xlsx] stamped stock_rt_csv_at = {csv_at}")
        except Exception as e:
            print(f"[stock_rt_xlsx] (couldn't stamp batch_status: {e})")
    finally:
        cur.close()
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
