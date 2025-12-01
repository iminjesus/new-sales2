# make_sqlite_snapshot.py
import os
import mysql.connector
import sqlite3
import pandas as pd

# 1) MySQL connection info (same as you use in app.py)
MYSQL_CONFIG = {
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASS", ""),
    "database": os.getenv("DB_NAME", "my_new_database"),
    "autocommit": True,
    }

# 2) Tables your Flask app uses
TABLES = [
    "sales20250111",
    "sales212511",
    "sales202511",
    "target2025",
    "customer",
    "hm",
    "iseg",
    "lowprofile",
    "strategic_commercial",
    "suv",
    "profit"
    # add any other tables that appear in your SQL in app.py
]

def main():
    mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
    sqlite_conn = sqlite3.connect("snapshot.db")  # created in project root

    for table in TABLES:
        print(f"Copying {table}...")
        df = pd.read_sql(f"SELECT * FROM {table}", mysql_conn)
        df.to_sql(table, sqlite_conn, index=False, if_exists="replace")

    sqlite_conn.close()
    mysql_conn.close()
    print("Done. snapshot.db created.")

if __name__ == "__main__":
    main()
