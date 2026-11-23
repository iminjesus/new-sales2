import os
import re
import csv
from datetime import datetime
import mysql.connector
from dotenv import load_dotenv

load_dotenv()  # .env 파일에서 환경변수 읽기

# ---------------- CONFIG ----------------
# Resolve relative to this script so the same code runs on every checkout
# regardless of the absolute path the repo was cloned to.
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
# ZSDM64300 output — dot_stock.csv is written by sapcrawling.py after
# looping the four plants and keeping only the yellow columns.
CSV_PATH  = os.path.join(BASE_DIR, "rawdata", "unlock", "dot_stock.csv")

DB_HOST     = os.getenv("DB_HOST", "100.127.139.79")  # 집 PC Tailscale IP
DB_PORT     = int(os.getenv("DB_PORT", "3306"))
DB_USER     = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASS", "")
DB_NAME     = os.getenv("DB_NAME", "my_new_database")

TABLE_NAME = "stock"   # rebuilt from scratch every load
TRUNCATE_BEFORE_LOAD = True

# Windows CSV 줄바꿈은 보통 \r\n
LINES_TERMINATED_BY = r"\r\n"

# ---- sales_thismonth CSV ----
SALES_CSV_PATH = os.path.join(BASE_DIR, "rawdata", "unlock", "sales_thismonth.csv")
SALES_TABLE    = "sales_thismonth"

# ZSDR24030 CSV header (sanitized) → sales_thismonth DB column
# SAP export headers vary; add alternative names if needed
SALES_HEADER_MAP = {
    # Billing Date → day number (including SAP typo "Billng Date")
    "billing_date": "day",
    "billng_date":  "day",
    "fkdat":        "day",
    "billing_date_fkdat": "day",
    # S/O Type
    "s_o_type":     "so_type",
    "so_type":      "so_type",
    "auart":        "so_type",
    "order_type":   "so_type",
    # Sold-to
    "sold_to":          "sold_to",
    "sold_to_party":    "sold_to",
    "kunag":            "sold_to",
    # Ship-to
    "ship_to":          "ship_to",
    "ship_to_party":    "ship_to",
    "kunwe":            "ship_to",
    # Material
    "material":         "material",
    "matnr":            "material",
    # Brand
    "brand":            "brand",
    # Qty
    "bill_qty_in_sku":  "qty",
    "bill_qty":         "qty",
    "fkimg":            "qty",
    # Amount
    "net_value":        "amt",
    "netwr":            "amt",
    # State / Region
    "state":            "state",
    "sales_district":   "state",
    "bzirk":            "state",
    # BDE / Salesman
    "bde":              "bde",
    "salesperson":      "bde",
    "vkgrp":            "bde",
    # New columns
    "cogs":             "cogs",
    "dc_rate":          "dc_rate",
    "p_rate":           "p_rate",
    "p_rate_p_rate":    "p_rate",
}

# All DB columns we expect to populate (order matters for INSERT)
SALES_DB_COLS = ["day", "qty", "amt", "sold_to", "ship_to", "material", "brand",
                 "state", "bde", "so_type", "cogs", "dc_rate", "p_rate"]

# Business-effective "this month" rule — mirrors app.py's
# _business_effective_ym().  On or before the first business day
# (Mon-Fri) of a calendar month, "this month" is still the previous
# calendar month because the overnight batch for the new month
# hasn't landed yet.  Returned as (year, month) with month 1-12.
def _business_effective_ym(today=None):
    from datetime import date as _d, timedelta as _td
    today = today or _d.today()
    first = today.replace(day=1)
    while first.weekday() >= 5:   # 5=Sat, 6=Sun
        first += _td(days=1)
    if today <= first:
        if today.month == 1:
            return (today.year - 1, 12)
        return (today.year, today.month - 1)
    return (today.year, today.month)

# New columns to ADD to the table if missing
SALES_NEW_COLS = {
    "so_type":  "VARCHAR(10)",
    "brand":    "VARCHAR(8)",
    "cogs":     "DECIMAL(18,4)",
    "dc_rate":  "DECIMAL(10,4)",
    "p_rate":   "DECIMAL(10,4)",
}


# ---------------- HELPERS ----------------
def sanitize_col(name: str) -> str:
    s = (name or "").strip()
    # remove quotes
    s = s.strip('"').strip("'")
    s = s.lower()
    # replace non-alnum with underscore
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = s.strip("_")
    if not s:
        s = "col"
    # column cannot start with digit
    if re.match(r"^\d", s):
        s = "c_" + s
    return s

def read_header(csv_path: str):
    with open(csv_path, "r", encoding="utf-8-sig", errors="replace") as f:
        line = f.readline()
    parts = [p.strip().strip('"') for p in line.strip().split(",")]
    return parts

def ensure_unique(cols):
    seen = {}
    out = []
    for c in cols:
        base = c
        n = seen.get(base, 0)
        if n == 0:
            out.append(base)
        else:
            out.append(f"{base}_{n+1}")
        seen[base] = n + 1
    return out

def mysql_path(p: str) -> str:
    return p.replace("\\", "/")

def parse_billing_date_day(val: str) -> str:
    """
    Extract day number from various SAP/openpyxl date formats.
    openpyxl datetime → str() gives "2026-03-01 00:00:00"
    SAP screen input format: DD.MM.YYYY
    Returns day as integer string, e.g. "1", "15"
    """
    val = val.strip()
    if not val:
        return ""
    for fmt in (
        "%Y-%m-%d %H:%M:%S",   # openpyxl datetime str
        "%Y-%m-%d",            # ISO date
        "%d.%m.%Y",            # SAP screen format
        "%Y%m%d",              # compact
        "%d/%m/%Y",
        "%m/%d/%Y",
    ):
        try:
            d = datetime.strptime(val, fmt)
            return str(d.day)
        except:
            pass
    # Last resort: if val looks like a plain integer already
    try:
        day = int(float(val))
        if 1 <= day <= 31:
            return str(day)
    except:
        pass
    print(f"  [WARN] Could not parse billing date: {val!r}")
    return ""


# ---------------- STOCK LOAD ----------------
# Fixed 5-column schema for the ZSDM64300-derived stock table.
# Interface date arrives as DD.MM.YYYY (SAP screen format); the LOAD
# uses STR_TO_DATE to normalise it to a real DATE.  DOT No. is kept as
# VARCHAR — it's a WWYY code (e.g. "4022" = week 40 of 2022), and the
# leading zero on weeks 01-09 matters ("0326" ≠ "326").
def load_stock(conn):
    # Oct-21 user ask: prefer CREATE IF NOT EXISTS + TRUNCATE over
    # DROP + CREATE so manual tweaks (extra indexes, grants, additional
    # columns) that the operator may have added over time survive a
    # reload.  CREATE IF NOT EXISTS is a no-op on an existing table;
    # TRUNCATE empties the data in one atomic op and resets AUTO_INC.
    #
    # Nov-05 Phase 1: carry Block Qty + Location ID through so the
    # SPRF / Ageing views can hide blocked rows in the main count
    # AND surface them in a tooltip that breaks them down by ageing
    # bucket × location.  CREATE adds the two columns for a fresh
    # table; existing tables get them via the ALTER guards below.
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` (
      interface_date DATE,
      plant          VARCHAR(10),
      material       VARCHAR(20),
      dot_no         VARCHAR(6),
      stock_qty      INT,
      block_qty      INT        NOT NULL DEFAULT 0,
      location_id    VARCHAR(20) NOT NULL DEFAULT '',
      INDEX idx_stock_plant_mat (plant, material),
      INDEX idx_stock_dot       (dot_no),
      INDEX idx_stock_block     (block_qty)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    truncate_sql = f"TRUNCATE TABLE `{TABLE_NAME}`;"

    cur = conn.cursor()
    try:
        cur.execute(create_sql)
        # Nov-05: idempotent ADD COLUMN for existing deployments.  MySQL
        # pre-8.0 lacks "IF NOT EXISTS" on ADD COLUMN; guard with a
        # SHOW COLUMNS check so repeat runs are no-ops instead of errors.
        for col, ddl in (
            ("block_qty",   "INT NOT NULL DEFAULT 0"),
            ("location_id", "VARCHAR(20) NOT NULL DEFAULT ''"),
        ):
            cur.execute(f"SHOW COLUMNS FROM `{TABLE_NAME}` LIKE %s", (col,))
            if not cur.fetchone():
                cur.execute(f"ALTER TABLE `{TABLE_NAME}` "
                            f"ADD COLUMN `{col}` {ddl}")
                print(f"[stock] added column: {col}")
        # idx_stock_block too — only create if missing
        try:
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.statistics "
                "WHERE table_schema = DATABASE() "
                f" AND table_name = '{TABLE_NAME}' "
                "  AND index_name = 'idx_stock_block'"
            )
            if (cur.fetchone() or (0,))[0] == 0:
                cur.execute(f"CREATE INDEX idx_stock_block "
                            f"ON `{TABLE_NAME}` (block_qty)")
        except Exception as _e:
            print(f"[stock] idx_stock_block add skipped: {_e}")
        cur.execute(truncate_sql)
        # STR_TO_DATE handles the DD.MM.YYYY that comes out of SAP, and
        # the numeric qty is REPLACE'd to strip thousand separators
        # before the cast.  block_qty defaults to 0 when the CSV cell
        # is blank or non-numeric.  location_id is TRIM'd verbatim.
        load_sql = f"""
        LOAD DATA LOCAL INFILE '{mysql_path(CSV_PATH)}'
        INTO TABLE `{TABLE_NAME}`
        CHARACTER SET utf8mb4
        FIELDS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
        LINES TERMINATED BY '{LINES_TERMINATED_BY}'
        IGNORE 1 LINES
        (@interface_date, @plant, @material, @dot_no, @stock_qty,
         @block_qty, @location_id)
        SET
          interface_date = STR_TO_DATE(TRIM(@interface_date), '%d.%m.%Y'),
          plant          = TRIM(@plant),
          material       = TRIM(@material),
          dot_no         = TRIM(@dot_no),
          stock_qty      = CAST(REPLACE(REPLACE(TRIM(@stock_qty), ',', ''), '"', '') AS SIGNED),
          block_qty      = COALESCE(NULLIF(CAST(REPLACE(REPLACE(TRIM(@block_qty), ',', ''), '"', '') AS SIGNED), NULL), 0),
          location_id    = TRIM(@location_id);
        """
        cur.execute(load_sql)
        conn.commit()

        cur.execute(f"SELECT COUNT(*) FROM `{TABLE_NAME}`;")
        cnt = cur.fetchone()[0]
        cur.execute(f"SELECT COUNT(*) FROM `{TABLE_NAME}` WHERE block_qty > 0;")
        blocked_rows = (cur.fetchone() or (0,))[0]
        cur.execute(f"SELECT plant, COUNT(*) FROM `{TABLE_NAME}` GROUP BY plant ORDER BY plant;")
        by_plant = cur.fetchall()
        print(f"[stock] Loaded rows: {cnt}  (blocked: {blocked_rows})")
        for p, n in by_plant:
            print(f"        {p or '(none)':<6} {n:>10,}")
    finally:
        cur.close()


# ---------------- SALES LOAD ----------------
def load_sales(conn):
    if not os.path.exists(SALES_CSV_PATH):
        raise FileNotFoundError(SALES_CSV_PATH)

    # 1. Ensure new columns exist in the table.  The `month` column is
    # populated below from the business-effective month of each load.
    cur = conn.cursor()
    try:
        # month column — INT, first position when added by this
        # script.  AFTER-position only matters for brand-new tables;
        # existing tables with the ALTER already run keep whatever
        # position the operator added it in.
        try:
            cur.execute(
                f"ALTER TABLE `{SALES_TABLE}` ADD COLUMN `month` INT FIRST;"
            )
            print("  Added column: month")
        except mysql.connector.Error as e:
            if e.errno != 1060:  # 1060 = Duplicate column (already there)
                raise
        for col_name, col_type in SALES_NEW_COLS.items():
            try:
                cur.execute(
                    f"ALTER TABLE `{SALES_TABLE}` ADD COLUMN `{col_name}` {col_type};"
                )
                print(f"  Added column: {col_name}")
            except mysql.connector.Error as e:
                if e.errno == 1060:  # Duplicate column
                    pass
                else:
                    raise
        conn.commit()
    finally:
        cur.close()

    # Business-effective month this load is for.  Everything in the
    # CSV is tagged with this month on insert, and the DELETE pass
    # below is driven off it so that:
    #   • The month we're about to re-load (this_m) is cleared first.
    #   • Anything two months old (two_ago) is purged as housekeeping.
    #   • The previous month (prev_m) is left alone — if sales_2526
    #     hasn't swallowed its rows yet, the dashboard's monthly
    #     fallback keeps using them via sales_thismonth.
    eff_y, this_m   = _business_effective_ym()
    prev_m          = 12 if this_m == 1 else this_m - 1
    two_ago         = 12 if prev_m == 1 else prev_m - 1
    print(f"  Effective month: {this_m} (prev: {prev_m}, two-ago to purge: {two_ago})")

    # 2. Read CSV and map headers to DB columns
    with open(SALES_CSV_PATH, "r", encoding="utf-8-sig", errors="replace") as f:
        reader = csv.reader(f)
        raw_headers = next(reader)
        sanitized = [sanitize_col(h) for h in raw_headers]

        # Build (csv_col_index → db_col_name) mapping
        col_idx_map = {}  # db_col_name → csv index
        for i, s in enumerate(sanitized):
            db_col = SALES_HEADER_MAP.get(s)
            if db_col and db_col not in col_idx_map:
                col_idx_map[db_col] = i

        if not col_idx_map:
            print(f"  [WARN] No matching columns found in {SALES_CSV_PATH}")
            return

        # Columns we'll actually insert.  `month` is prepended so it
        # becomes the first value in every tuple and the first column
        # name in the INSERT list.
        insert_cols = ["month"] + [c for c in SALES_DB_COLS if c in col_idx_map]
        print(f"  Mapped columns: {insert_cols}")

        placeholders = ", ".join(["%s"] * len(insert_cols))
        col_names    = ", ".join([f"`{c}`" for c in insert_cols])
        insert_sql   = f"INSERT INTO `{SALES_TABLE}` ({col_names}) VALUES ({placeholders})"

        # 3. Scoped DELETE instead of TRUNCATE — see the comment above
        # the month resolution block for the rules.  Previous-month
        # rows survive this pass unless they get overwritten by rows
        # we're about to load (which is a no-op because this_m rows
        # never land in prev_m).
        cur = conn.cursor()
        try:
            cur.execute(
                f"DELETE FROM `{SALES_TABLE}` WHERE `month` = %s",
                (two_ago,),
            )
            print(f"  Purged month={two_ago}: {cur.rowcount} rows")
            cur.execute(
                f"DELETE FROM `{SALES_TABLE}` WHERE `month` = %s",
                (this_m,),
            )
            print(f"  Cleared month={this_m}: {cur.rowcount} rows (reload target)")

            batch = []
            for row in reader:
                if not any(v.strip() for v in row):
                    continue  # skip empty rows
                values = [this_m]   # month column, first in insert_cols
                broke = False
                for db_col in insert_cols[1:]:   # skip "month" — already added
                    idx = col_idx_map[db_col]
                    raw = row[idx].strip() if idx < len(row) else ""
                    if db_col == "day":
                        raw = parse_billing_date_day(raw)
                        if not raw:
                            broke = True
                            break  # 합계/소계 행 → 건너뜀
                    values.append(raw if raw != "" else None)
                if broke:
                    continue
                batch.append(tuple(values))

                if len(batch) >= 500:
                    cur.executemany(insert_sql, batch)
                    batch = []

            if batch:
                cur.executemany(insert_sql, batch)

            conn.commit()
            cur.execute(f"SELECT COUNT(*) FROM `{SALES_TABLE}`;")
            cnt = cur.fetchone()[0]
            cur.execute(
                f"SELECT `month`, COUNT(*) FROM `{SALES_TABLE}` GROUP BY `month`"
            )
            per_m = {int(r[0]) if r[0] is not None else None: int(r[1])
                     for r in cur.fetchall()}
            print(f"[sales] Loaded rows: {cnt}  per-month: {per_m}")
        finally:
            cur.close()


# ---------------- STOCK_RT LOAD (Oct-21 user ask) ----------------
# ZMMR40430 output — sapcrawling_stock_rt.py drops dated CSVs at
# rawdata/unlock/rt_stock_<YYYYMMDD>.csv.  We pick the newest (so
# a manual re-run of the crawler always wins over the hourly batch's
# earlier output) and reload the stock_rt table from it.
STOCK_RT_DIR     = os.path.join(BASE_DIR, "rawdata", "unlock")
STOCK_RT_GLOB    = "rt_stock_*.csv"
STOCK_RT_ALIAS   = os.path.join(STOCK_RT_DIR, "rt_stock.csv")
STOCK_RT_TABLE   = "stock_rt"

# Nov-23 user ask: the orchestrator now rotates through plants, writing
# rt_stock_<PLANT>_<YYYYMMDD_HHMM>.csv one plant per 10-min tick.  Keep
# the plant list in sync with batch_orchestrator._PLANT_ROTATION so the
# loader picks up every file the rotation produces.  42R3 is not used.
STOCK_RT_PLANTS  = ("42R0", "42R1", "42R2", "42R4")


def _pick_latest_rt_stock_csv():
    """Return the newest all-plants rt_stock_*.csv path, or the plain
    alias rt_stock.csv as a fallback.  Returns None when neither exists.
    PER-PLANT files (rt_stock_<PLANT>_<YYYYMMDD_HHMM>.csv) are deliberately
    SKIPPED here -- they're handled by _pick_latest_per_plant_csvs()
    and the per-plant branch of load_stock_rt()."""
    import glob as _g
    import re as _re
    files = _g.glob(os.path.join(STOCK_RT_DIR, STOCK_RT_GLOB))
    per_plant_re = _re.compile(
        r"^rt_stock_42R[0-9A-Z]_\d{8}_\d{4}\.csv$", _re.IGNORECASE)
    # Exclude the alias AND every per-plant file -- the dated all-plants
    # files follow pattern rt_stock_<YYYYMMDD>.csv (no plant code in
    # the filename), which per_plant_re does not match.
    dated = [p for p in files
             if os.path.basename(p) != "rt_stock.csv"
             and not per_plant_re.match(os.path.basename(p))]
    if dated:
        dated.sort(key=lambda p: os.path.getmtime(p), reverse=True)
        return dated[0]
    if os.path.exists(STOCK_RT_ALIAS):
        return STOCK_RT_ALIAS
    return None


def _pick_latest_per_plant_csvs():
    """Return {plant_code: newest_csv_path} for each plant that has at
    least one rt_stock_<PLANT>_<YYYYMMDD_HHMM>.csv file in STOCK_RT_DIR.
    Plants with no file are simply absent from the returned dict."""
    import glob as _g
    import re as _re
    per_plant_re = _re.compile(
        r"^rt_stock_(42R[0-9A-Z])_\d{8}_\d{4}\.csv$", _re.IGNORECASE)
    out = {}
    for p in _g.glob(os.path.join(STOCK_RT_DIR, "rt_stock_*.csv")):
        m = per_plant_re.match(os.path.basename(p))
        if not m:
            continue
        plant = m.group(1).upper()
        prev = out.get(plant)
        if prev is None or os.path.getmtime(p) > os.path.getmtime(prev):
            out[plant] = p
    return out


def load_stock_rt(conn):
    """Refresh the stock_rt table from the newest rt_stock_*.csv.

    Oct-21 user ask: do NOT drop the table on every load — if the
    operator adds a custom index or an extra column over time, those
    changes must survive a reload.  CREATE IF NOT EXISTS on first run
    only, then TRUNCATE + LOAD DATA every time after that so the row
    set is a fresh snapshot but the table structure is preserved.
    If the /ART_TH layout gains a new column later, the loader code
    (and this CREATE) get updated in one commit — not an auto-DROP.

    Oct-22 user ask: do NOT re-load an UNCHANGED CSV every hour.
    Previously a failed hourly RT crawl left no new CSV, the loader
    picked up yesterday's file again, and MySQL got reloaded with
    identical stale data on every slot — making it look like the
    pipeline was healthy when it wasn't.  Now:
      • Skip the load when the newest CSV's mtime matches the value
        stamped in batch_status.stock_rt_loaded_mtime (we already
        have this version in MySQL — no work to do).
      • Loud WARN when the newest CSV's filename date is OLDER
        than today, so the operator sees that RT crawl is failing
        instead of a silent 'Loaded rows: NNNN' repeat.
    """
    # Nov-23 user ask: the orchestrator now rotates through plants,
    # dropping rt_stock_<PLANT>_<YYYYMMDD_HHMM>.csv one plant per tick.
    # When those files exist, run the per-plant branch (DELETE WHERE
    # plant=X + LOAD, per-plant batch_status stamps).  Only fall back
    # to the old all-plants TRUNCATE path when no per-plant files are
    # present -- preserves back-compat with a manual all-plants run
    # of sapcrawling_stock_rt_batch.py (no --plant flag).
    per_plant = _pick_latest_per_plant_csvs()
    if per_plant:
        _load_stock_rt_per_plant(conn, per_plant)
        return

    csv_path = _pick_latest_rt_stock_csv()
    if not csv_path:
        print(f"[stock_rt] no rt_stock_*.csv in {STOCK_RT_DIR} — skipping")
        return
    _load_stock_rt_all_plants(conn, csv_path)


def _stock_rt_create_sql() -> str:
    return f"""
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


def _stock_rt_load_sql(csv_path: str, snap_iso: str) -> str:
    return f"""
    LOAD DATA LOCAL INFILE '{mysql_path(csv_path)}'
    INTO TABLE `{STOCK_RT_TABLE}`
    CHARACTER SET utf8mb4
    FIELDS TERMINATED BY ','
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY '{LINES_TERMINATED_BY}'
    IGNORE 1 LINES
    (@plant, @material, @description, @pattern, @load_index, @hq_group,
     @on_order, @unrestricted, @atp_qt, @list_price,
     @at_dock, @on_water, @factory, @avg_3m)
    SET
      plant         = TRIM(@plant),
      material      = TRIM(@material),
      description   = TRIM(@description),
      pattern       = TRIM(@pattern),
      load_index    = TRIM(@load_index),
      hq_group      = TRIM(@hq_group),
      on_order      = CAST(REPLACE(REPLACE(TRIM(@on_order),     ',', ''), '"', '') AS SIGNED),
      unrestricted  = CAST(REPLACE(REPLACE(TRIM(@unrestricted), ',', ''), '"', '') AS SIGNED),
      atp_qt        = CAST(REPLACE(REPLACE(TRIM(@atp_qt),       ',', ''), '"', '') AS SIGNED),
      list_price    = CAST(REPLACE(REPLACE(TRIM(@list_price),   ',', ''), '"', '') AS DECIMAL(14,2)),
      at_dock       = CAST(REPLACE(REPLACE(TRIM(@at_dock),      ',', ''), '"', '') AS SIGNED),
      on_water      = CAST(REPLACE(REPLACE(TRIM(@on_water),     ',', ''), '"', '') AS SIGNED),
      factory       = CAST(REPLACE(REPLACE(TRIM(@factory),      ',', ''), '"', '') AS SIGNED),
      avg_3m        = CAST(REPLACE(REPLACE(TRIM(@avg_3m),       ',', ''), '"', '') AS DECIMAL(14,2)),
      snapshot_date = '{snap_iso}';
    """


def _ensure_batch_status_table(cur):
    cur.execute(
        "CREATE TABLE IF NOT EXISTS batch_status ("
        "  k VARCHAR(64) PRIMARY KEY,"
        "  v TEXT NULL,"
        "  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP "
        "    ON UPDATE CURRENT_TIMESTAMP"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    )


def _stamp_batch_status_pairs(cur, pairs):
    """Upsert a list of (key, value) tuples into batch_status."""
    if not pairs:
        return
    sql = ("INSERT INTO batch_status (k, v) VALUES "
           + ", ".join(["(%s, %s)"] * len(pairs))
           + " ON DUPLICATE KEY UPDATE v = VALUES(v)")
    flat = [v for kv in pairs for v in kv]
    cur.execute(sql, flat)


def _refresh_global_stock_rt_csv_at(cur):
    """Recompute batch_status.stock_rt_csv_at as the MIN (oldest) of all
    per-plant stock_rt_csv_at_<PLANT> stamps so the SPRF global "Stock
    Info updated" label shows the worst-case freshness across plants.
    Noop when no per-plant stamps exist yet (keeps whatever prior value)."""
    cur.execute(
        "SELECT MIN(v) FROM batch_status WHERE k LIKE 'stock_rt_csv_at_42R%'"
    )
    row = cur.fetchone()
    worst = (row[0] if row else None) or ""
    if worst:
        _stamp_batch_status_pairs(cur, [("stock_rt_csv_at", worst)])


def _load_stock_rt_per_plant(conn, per_plant_dict):
    """Per-plant path: DELETE WHERE plant=X + LOAD DATA per plant.
    Each plant's rows are replaced atomically (one plant at a time);
    the other plants' rows stay put, so a failed plant leaves the
    other three fresh instead of wiping the whole snapshot."""
    import re as _re_dt
    from datetime import date as _d_cls
    from datetime import datetime as _dt_stamp

    cur = conn.cursor()
    try:
        cur.execute(_stock_rt_create_sql())
        _ensure_batch_status_table(cur)
        conn.commit()

        # Read every per-plant last-loaded mtime in one shot so the
        # "skip unchanged" check doesn't need a round-trip per plant.
        cur.execute(
            "SELECT k, v FROM batch_status WHERE k LIKE 'stock_rt_loaded_mtime_42R%'"
        )
        prior_mtimes = {}
        for k, v in (cur.fetchall() or []):
            try:
                prior_mtimes[k.split("_")[-1]] = int(v or 0)
            except Exception:
                pass

        plants_loaded = []
        for plant in sorted(per_plant_dict.keys()):
            csv_path = per_plant_dict[plant]
            try:
                csv_mtime = int(os.path.getmtime(csv_path))
            except Exception:
                csv_mtime = 0

            if csv_mtime and prior_mtimes.get(plant) == csv_mtime:
                print(f"[stock_rt][{plant}] {os.path.basename(csv_path)} "
                      f"unchanged since last load (mtime={csv_mtime}) "
                      f"— skipping.")
                continue

            # snapshot_date from filename (rt_stock_42R0_YYYYMMDD_HHMM.csv),
            # fall back to today when the pattern doesn't match.
            m = _re_dt.search(r"rt_stock_42R[0-9A-Z]_(\d{8})_\d{4}\.csv$",
                              csv_path, _re_dt.IGNORECASE)
            if m:
                try:
                    snap = _d_cls(int(m.group(1)[:4]),
                                  int(m.group(1)[4:6]),
                                  int(m.group(1)[6:8]))
                except Exception:
                    snap = _d_cls.today()
            else:
                snap = _d_cls.today()

            print(f"[stock_rt][{plant}] loading from {csv_path}")
            # DELETE WHERE plant=X then LOAD -- one plant at a time so
            # the other three plants' rows stay live throughout.
            cur.execute(
                f"DELETE FROM `{STOCK_RT_TABLE}` WHERE plant = %s", (plant,))
            cur.execute(_stock_rt_load_sql(csv_path, snap.isoformat()))

            cur.execute(
                f"SELECT COUNT(*) FROM `{STOCK_RT_TABLE}` WHERE plant = %s",
                (plant,))
            loaded_n = cur.fetchone()[0]
            print(f"[stock_rt][{plant}] rows now in table: {loaded_n:,}")

            csv_at = (_dt_stamp.fromtimestamp(csv_mtime)
                      .strftime("%Y-%m-%d %H:%M:%S") if csv_mtime else "")
            _stamp_batch_status_pairs(cur, [
                (f"stock_rt_loaded_mtime_{plant}", str(csv_mtime)),
                (f"stock_rt_loaded_from_{plant}",  os.path.basename(csv_path)),
                (f"stock_rt_csv_at_{plant}",       csv_at),
            ])
            plants_loaded.append(plant)

        # Refresh the global stock_rt_csv_at = MIN of per-plant stamps
        # whether or not THIS call loaded a new plant -- picks up any
        # fresh stamps written by past loads too.
        _refresh_global_stock_rt_csv_at(cur)
        conn.commit()

        if plants_loaded:
            print(f"[stock_rt] per-plant: loaded {plants_loaded}")
        else:
            print(f"[stock_rt] per-plant: no new files (all 4 plants in sync)")
    finally:
        cur.close()


def _load_stock_rt_all_plants(conn, csv_path):
    """Original all-plants path: TRUNCATE + LOAD DATA.  Used only for
    back-compat when no per-plant files have been written yet (e.g.
    the operator ran sapcrawling_stock_rt_batch.py without --plant)."""
    import re as _re_dt
    from datetime import date as _d_cls
    from datetime import datetime as _dt_stamp

    try:
        csv_mtime = int(os.path.getmtime(csv_path))
    except Exception:
        csv_mtime = 0
    try:
        _cur = conn.cursor()
        _cur.execute("SHOW TABLES LIKE 'batch_status'")
        if _cur.fetchone():
            _cur.execute(
                "SELECT v FROM batch_status WHERE k = 'stock_rt_loaded_mtime'"
            )
            _row = _cur.fetchone()
            prior_mtime = int(_row[0]) if _row and _row[0] else 0
            if csv_mtime and prior_mtime == csv_mtime:
                print(f"[stock_rt] {os.path.basename(csv_path)} is "
                      f"unchanged since the last load (mtime={csv_mtime}) "
                      f"— skipping reload.  MySQL already has this snapshot.")
                _cur.close()
                return
        _cur.close()
    except Exception:
        pass

    _m = _re_dt.search(r"rt_stock_(\d{8})\.csv$", csv_path)
    if _m:
        try:
            _file_d = _d_cls(int(_m.group(1)[:4]),
                             int(_m.group(1)[4:6]),
                             int(_m.group(1)[6:8]))
            if _file_d < _d_cls.today():
                print(f"[stock_rt] [WARN] newest rt_stock CSV is "
                      f"{_file_d.isoformat()} (not today).  The RT "
                      f"crawl has not produced a fresh file yet — "
                      f"check batch_orchestrator.log for ZMMR40430 "
                      f"errors.  Reloading the stale file anyway "
                      f"since this is the first load after a restart.")
        except Exception:
            pass

    print(f"[stock_rt] loading (all-plants) from {csv_path}")

    m = _re_dt.search(r"rt_stock_(\d{8})\.csv$", csv_path)
    if m:
        try:
            snap = _d_cls(int(m.group(1)[:4]),
                          int(m.group(1)[4:6]),
                          int(m.group(1)[6:8]))
        except Exception:
            snap = _d_cls.today()
    else:
        snap = _d_cls.today()

    cur = conn.cursor()
    try:
        cur.execute(_stock_rt_create_sql())
        cur.execute(f"TRUNCATE TABLE `{STOCK_RT_TABLE}`;")
        cur.execute(_stock_rt_load_sql(csv_path, snap.isoformat()))
        conn.commit()

        cur.execute(f"SELECT COUNT(*) FROM `{STOCK_RT_TABLE}`;")
        cnt = cur.fetchone()[0]
        cur.execute(f"SELECT plant, COUNT(*) FROM `{STOCK_RT_TABLE}` "
                    f"GROUP BY plant ORDER BY plant;")
        by_plant = cur.fetchall()
        print(f"[stock_rt] Loaded rows: {cnt}")
        for p, n in by_plant:
            print(f"           {p or '(none)':<6} {n:>10,}")

        try:
            csv_at = (_dt_stamp.fromtimestamp(csv_mtime)
                      .strftime("%Y-%m-%d %H:%M:%S") if csv_mtime else "")
            _ensure_batch_status_table(cur)
            _stamp_batch_status_pairs(cur, [
                ("stock_rt_loaded_mtime", str(csv_mtime)),
                ("stock_rt_loaded_from",  os.path.basename(csv_path)),
                ("stock_rt_csv_at",       csv_at),
            ])
            # An all-plants load resets every per-plant stamp to the
            # same value so SPRF's per-warehouse badges stay in sync.
            now_pairs = [(f"stock_rt_csv_at_{p}", csv_at)
                         for p in STOCK_RT_PLANTS]
            _stamp_batch_status_pairs(cur, now_pairs)
            conn.commit()
            print(f"[stock_rt] stamped stock_rt_csv_at = {csv_at}")
        except Exception as _e:
            print(f"[stock_rt] (couldn't stamp batch_status: {_e})")
    finally:
        cur.close()


# ---------------- MAIN ----------------
def main():
    conn = mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        allow_local_infile=True,
    )

    try:
        if not os.path.exists(CSV_PATH):
            raise FileNotFoundError(CSV_PATH)
        load_stock(conn)
        load_sales(conn)
        # Oct-21 user ask: load the RT stock feed if a rt_stock_*.csv
        # exists.  Missing CSV is a no-op (prints a skip line) so the
        # sales + stock loads still complete on hosts that haven't
        # run sapcrawling_stock_rt yet.
        try:
            load_stock_rt(conn)
        except Exception as _e:
            print(f"[stock_rt] load failed — skipping: {_e}")
        print("Table:", TABLE_NAME)
        print("Table:", SALES_TABLE)
        print("Table:", STOCK_RT_TABLE)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
