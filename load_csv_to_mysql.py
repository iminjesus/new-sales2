import os
import re
import csv
from datetime import datetime
import mysql.connector
from dotenv import load_dotenv

load_dotenv()  # .env 파일에서 환경변수 읽기

# ---------------- CONFIG ----------------
CSV_PATH = r"D:\Data-Anal website\rawdata\unlock\mb52_42.csv"

DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = int(os.getenv("DB_PORT", "3306"))
DB_USER     = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASS", "")
DB_NAME     = os.getenv("DB_NAME", "my_new_database")

TABLE_NAME = "stock"   # 원하면 바꿔도 됨
TRUNCATE_BEFORE_LOAD = True

# Windows CSV 줄바꿈은 보통 \r\n
LINES_TERMINATED_BY = r"\r\n"

# ---- sales_thismonth CSV ----
SALES_CSV_PATH = r"D:\Data-Anal website\rawdata\unlock\sales_thismonth.csv"
SALES_TABLE    = "sales_thismonth"

# ZSDR24030 CSV header (sanitized) → sales_thismonth DB column
# SAP export headers vary; add alternative names if needed
SALES_HEADER_MAP = {
    # Billing Date → day number
    "billing_date": "day",
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
SALES_DB_COLS = ["day", "qty", "amt", "sold_to", "ship_to", "material", "state", "bde",
                 "so_type", "cogs", "dc_rate", "p_rate"]

# New columns to ADD to the table if missing
SALES_NEW_COLS = {
    "so_type":  "VARCHAR(10)",
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
def load_stock(conn):
    raw_cols = read_header(CSV_PATH)
    cols = [sanitize_col(c) for c in raw_cols]
    cols = ensure_unique(cols)

    col_defs = ",\n  ".join([f"`{c}` TEXT" for c in cols])

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` (
      {col_defs}
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    truncate_sql = f"TRUNCATE TABLE `{TABLE_NAME}`;"

    load_sql = f"""
    LOAD DATA LOCAL INFILE '{mysql_path(CSV_PATH)}'
    INTO TABLE `{TABLE_NAME}`
    CHARACTER SET utf8mb4
    FIELDS TERMINATED BY ','
    OPTIONALLY ENCLOSED BY '"'
    LINES TERMINATED BY '{LINES_TERMINATED_BY}'
    IGNORE 1 LINES
    ({", ".join([f"`{c}`" for c in cols])});
    """

    cur = conn.cursor()
    try:
        cur.execute(create_sql)
        if TRUNCATE_BEFORE_LOAD:
            cur.execute(truncate_sql)
        cur.execute(load_sql)
        conn.commit()

        cur.execute(f"SELECT COUNT(*) FROM `{TABLE_NAME}`;")
        cnt = cur.fetchone()[0]
        print(f"[stock] Loaded rows: {cnt}")
    finally:
        cur.close()


# ---------------- SALES LOAD ----------------
def load_sales(conn):
    if not os.path.exists(SALES_CSV_PATH):
        raise FileNotFoundError(SALES_CSV_PATH)

    # 1. Ensure new columns exist in the table
    cur = conn.cursor()
    try:
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

    # 2. Read CSV and map headers to DB columns
    with open(SALES_CSV_PATH, "r", encoding="utf-8-sig", errors="replace") as f:
        reader = csv.reader(f)
        raw_headers = next(reader)
        sanitized = [sanitize_col(h) for h in raw_headers]

        print(f"  [DEBUG] Raw headers   : {raw_headers}")
        print(f"  [DEBUG] Sanitized     : {sanitized}")

        # Build (csv_col_index → db_col_name) mapping
        col_idx_map = {}  # db_col_name → csv index
        for i, s in enumerate(sanitized):
            db_col = SALES_HEADER_MAP.get(s)
            if db_col and db_col not in col_idx_map:
                col_idx_map[db_col] = i

        print(f"  [DEBUG] col_idx_map   : {col_idx_map}")

        if not col_idx_map:
            print(f"  [WARN] No matching columns found in {SALES_CSV_PATH}")
            return

        # Columns we'll actually insert (intersection of SALES_DB_COLS and what we found)
        insert_cols = [c for c in SALES_DB_COLS if c in col_idx_map]
        print(f"  Mapped columns: {insert_cols}")

        placeholders = ", ".join(["%s"] * len(insert_cols))
        col_names    = ", ".join([f"`{c}`" for c in insert_cols])
        insert_sql   = f"INSERT INTO `{SALES_TABLE}` ({col_names}) VALUES ({placeholders})"

        # 3. Truncate and insert
        cur = conn.cursor()
        try:
            cur.execute(f"TRUNCATE TABLE `{SALES_TABLE}`;")

            batch = []
            debug_shown = False
            for row in reader:
                if not any(v.strip() for v in row):
                    continue  # skip empty rows
                values = []
                for db_col in insert_cols:
                    idx = col_idx_map[db_col]
                    raw = row[idx].strip() if idx < len(row) else ""
                    if db_col == "day":
                        if not debug_shown:
                            print(f"  [DEBUG] First billing date raw value: {raw!r}")
                            debug_shown = True
                        raw = parse_billing_date_day(raw)
                    values.append(raw if raw != "" else None)
                batch.append(tuple(values))

                if len(batch) >= 500:
                    cur.executemany(insert_sql, batch)
                    batch = []

            if batch:
                cur.executemany(insert_sql, batch)

            conn.commit()
            cur.execute(f"SELECT COUNT(*) FROM `{SALES_TABLE}`;")
            cnt = cur.fetchone()[0]
            print(f"[sales] Loaded rows: {cnt}")
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
