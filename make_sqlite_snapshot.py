"""
make_sqlite_snapshot.py
-----------------------
Run this LOCALLY (where MySQL is accessible) to regenerate snapshot.db.
The snapshot.db is then committed to the repo and used by the Render/Cloudflare
deployment (USE_SQLITE=1) as a read-only demo database.

Usage:
    python make_sqlite_snapshot.py

Env vars (optional, defaults match app.py):
    DB_HOST, DB_USER, DB_PASS, DB_NAME
"""

import os
import sqlite3
import mysql.connector
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "snapshot.db")

# ── MySQL connection ──────────────────────────────────────────────────────────
MYSQL_CFG = {
    "host":     os.getenv("DB_HOST", "127.0.0.1"),
    "port":     int(os.getenv("DB_PORT", "3306")),
    "user":     os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASS", ""),
    "database": os.getenv("DB_NAME", "my_new_database"),
}

# ── Tables to copy ────────────────────────────────────────────────────────────
# Format: (mysql_table, sqlite_table, optional_where_clause)
TABLES = [
    # Customer master
    ("customer",           "customer",           None),

    # Category filter lookup tables
    ("hm",                 "hm",                 None),
    ("iseg",               "iseg",               None),
    ("lowprofile",         "lowprofile",         None),
    ("strategic_commercial", "strategic_commercial", None),
    ("suv",                "suv",                None),

    # Product info (pattern / product_group)
    ("carrying_2602",      "carrying_2602",      None),

    # Sales fact tables (normalised)
    ("sales_21_25",        "sales_21_25",        None),
    ("sales_25_2602",      "sales_25_2602",      None),
    ("sales_thismonth",    "sales_thismonth",    None),

    # Target
    ("target_26",          "target_26",          None),

    # Profit
    ("profit_2501_10",     "profit_2501_10",     None),
]

CHUNK = 50_000   # rows per read chunk (avoids OOM for large tables)


def copy_table(mysql_cur, sqlite_conn, mysql_table, sqlite_table, where=None):
    sql = f"SELECT * FROM `{mysql_table}`"
    if where:
        sql += f" WHERE {where}"
    try:
        mysql_cur.execute(sql)
    except mysql.connector.Error as e:
        print(f"  [SKIP] {mysql_table}: {e}")
        return

    cols = [d[0] for d in mysql_cur.description]
    first = True
    total = 0
    while True:
        rows = mysql_cur.fetchmany(CHUNK)
        if not rows:
            break
        df = pd.DataFrame(rows, columns=cols)
        df.to_sql(sqlite_table, sqlite_conn,
                  if_exists="replace" if first else "append",
                  index=False)
        first = False
        total += len(df)
        print(f"  {total:,} rows…", end="\r")
    print(f"  done: {total:,} rows")


def main():
    print("Connecting to MySQL …")
    conn_m = mysql.connector.connect(**MYSQL_CFG)
    cur_m = conn_m.cursor()

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"Removed old {DB_PATH}")

    conn_s = sqlite3.connect(DB_PATH)

    for mysql_tbl, sqlite_tbl, where in TABLES:
        print(f"Copying  {mysql_tbl} → {sqlite_tbl} …")
        copy_table(cur_m, conn_s, mysql_tbl, sqlite_tbl, where)

    conn_s.commit()
    conn_s.close()
    cur_m.close()
    conn_m.close()

    size_mb = os.path.getsize(DB_PATH) / 1_048_576
    print(f"\nDone. snapshot.db = {size_mb:.1f} MB  →  {DB_PATH}")
    print("Commit snapshot.db to the repo and redeploy.")


if __name__ == "__main__":
    main()
