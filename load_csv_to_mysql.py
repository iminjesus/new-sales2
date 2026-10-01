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
    drop_sql   = f"DROP TABLE IF EXISTS `{TABLE_NAME}`;"
    create_sql = f"""
    CREATE TABLE `{TABLE_NAME}` (
      interface_date DATE,
      plant          VARCHAR(10),
      material       VARCHAR(20),
      dot_no         VARCHAR(6),
      stock_qty      INT,
      INDEX idx_stock_plant_mat (plant, material),
      INDEX idx_stock_dot       (dot_no)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    # STR_TO_DATE handles the DD.MM.YYYY that comes out of SAP, and the
    # numeric qty is REPLACE'd to strip thousand separators before the
    # cast — SAP sometimes writes "1,234" instead of "1234" depending on
    # user locale.  Any row with an unparseable date is skipped by the
    # NULLIF chain (NULL isn't allowed in interface_date after we cast
    # so we let it in as NULL; downstream code filters those out).
    load_sql = f"""
    LOAD DATA LOCAL INFILE '{mysql_path(CSV_PATH)}'
    INTO TABLE `{TABLE_NAME}`
    CHARACTER SET utf8mb4
    FIELDS TERMINATED BY ','
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY '{LINES_TERMINATED_BY}'
    IGNORE 1 LINES
    (@interface_date, @plant, @material, @dot_no, @stock_qty)
    SET
      interface_date = STR_TO_DATE(TRIM(@interface_date), '%d.%m.%Y'),
      plant          = TRIM(@plant),
      material       = TRIM(@material),
      dot_no         = TRIM(@dot_no),
      stock_qty      = CAST(REPLACE(REPLACE(TRIM(@stock_qty), ',', ''), '"', '') AS SIGNED);
    """

    cur = conn.cursor()
    try:
        cur.execute(drop_sql)
        cur.execute(create_sql)
        cur.execute(load_sql)
        conn.commit()

        cur.execute(f"SELECT COUNT(*) FROM `{TABLE_NAME}`;")
        cnt = cur.fetchone()[0]
        cur.execute(f"SELECT plant, COUNT(*) FROM `{TABLE_NAME}` GROUP BY plant ORDER BY plant;")
        by_plant = cur.fetchall()
        print(f"[stock] Loaded rows: {cnt}")
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
        print("Table:", TABLE_NAME)
        print("Table:", SALES_TABLE)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
