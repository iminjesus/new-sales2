import os
import sqlite3
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "snapshot.db")
RAW_BASE = os.path.join(BASE_DIR, "rawdata", "unlock")

CSV_TABLES = {
    "customer":              "customer.csv",
    "hm":                    "hm.csv",
    "iseg":                  "iseg.csv",
    "lowprofile":            "lowprofile.csv",
    "sales_21_2511":           "sales_21_2511.csv",
    "sales_2501_11":         "sales_2501_11.csv",
    "sales_2511":             "sales_2511.csv",
    "strategic_commercial":  "strategic_commercial.csv",
    "suv":                   "suv.csv",
    "target2025":            "target2025.csv",
    "profit_2501_10" :              "profit_2501_10.csv"
}

def load_csv_to_table(conn, table_name, csv_filename):
    csv_path = os.path.join(RAW_BASE, csv_filename)
    if not os.path.exists(csv_path):
        print(f"[WARN] CSV not found for {table_name}: {csv_path}")
        return

    print(f"Loading {table_name} from {csv_path}...")
    try:
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
    except UnicodeDecodeError:
        print("  UTF-8 decode failed, trying cp949...")
        df = pd.read_csv(csv_path, encoding="cp949")

    df.to_sql(table_name, conn, if_exists="replace", index=False)

def main():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    conn = sqlite3.connect(DB_PATH)

    for table_name, csv_file in CSV_TABLES.items():
        load_csv_to_table(conn, table_name, csv_file)

    conn.close()
    print("Done. snapshot.db created from rawdata/unlock CSVs.")

if __name__ == "__main__":
    main()
