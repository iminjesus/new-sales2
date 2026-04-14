import os
import re
import mysql.connector

# ---------------- CONFIG ----------------
CSV_PATH = r"E:\01. work\2025\Data_Anal_Website\rawdata\unlock\mb52_42.csv"

DB_HOST = "100.127.139.79"   # 집 PC Tailscale IP
DB_USER = "root"
DB_PASSWORD = ""          # 너 설정에 맞게
DB_NAME = "my_new_database"     # 너 DB명에 맞게

TABLE_NAME = "stock"   # 원하면 바꿔도 됨
TRUNCATE_BEFORE_LOAD = True

# Windows CSV 줄바꿈은 보통 \r\n
LINES_TERMINATED_BY = r"\r\n"

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
    # avoid reserved-ish generic names duplicates handled later
    return s

def read_header(csv_path: str):
    with open(csv_path, "r", encoding="utf-8-sig", errors="replace") as f:
        line = f.readline()
    # crude CSV split that works for most SAP exports
    # if your header contains commas inside quotes, tell me and I'll swap to csv.reader
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
    # MySQL LOAD DATA likes forward slashes
    return p.replace("\\", "/")

# ---------------- MAIN ----------------
def main():
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(CSV_PATH)

    raw_cols = read_header(CSV_PATH)
    cols = [sanitize_col(c) for c in raw_cols]
    cols = ensure_unique(cols)

    # Build CREATE TABLE with all TEXT (safe first step)
    # Later we can cast selected columns into DECIMAL/INT in a typed table.
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

    conn = mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        allow_local_infile=True,
    )

    cur = conn.cursor()
    try:
        cur.execute(create_sql)
        if TRUNCATE_BEFORE_LOAD:
            cur.execute(truncate_sql)
        cur.execute(load_sql)
        conn.commit()

        cur.execute(f"SELECT COUNT(*) FROM `{TABLE_NAME}`;")
        cnt = cur.fetchone()[0]
        print("Loaded rows:", cnt)
        print("Table:", TABLE_NAME)
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()