# make_sqlite_snapshot.py

import os
import sqlite3
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH   = os.path.join(BASE_DIR, "snapshot.db")
RAW_BASE  = os.path.join(BASE_DIR, "rawdata", "unlock")

def load_csv_to_table(conn, table_name, csv_path):
    print(f"Loading {table_name} from {csv_path}...")
    # Try UTF-8, fall back to cp949 for Korean Windows CSVs
    try:
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
    except UnicodeDecodeError:
        print("  UTF-8 decode failed, trying cp949...")
        df = pd.read_csv(csv_path, encoding="cp949")

    df.to_sql(table_name, conn, if_exists="replace", index=False)

def main():
    # remove old DB if exists
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    conn = sqlite3.connect(DB_PATH)

    # loop over ALL csv files in rawdata/unlock
    for fname in os.listdir(RAW_BASE):
        if not fname.lower().endswith(".csv"):
            continue

        csv_path   = os.path.join(RAW_BASE, fname)
        table_name = os.path.splitext(fname)[0]   # e.g. "sales_2501_11"

        load_csv_to_table(conn, table_name, csv_path)

    conn.close()
    print("Done. snapshot.db created from rawdata/unlock CSVs.")

if __name__ == "__main__":
    main()
