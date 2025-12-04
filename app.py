from flask import Flask, request, jsonify, send_from_directory
import sqlite3
import mysql.connector
from time import time  # cache timestamps
import os
from flask_cors import CORS

USE_SQLITE = os.environ.get("USE_SQLITE") == "1"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SQLITE_PATH = os.path.join(BASE_DIR, "snapshot.db")

import sqlite3  # make sure this is at the top of app.py

class SQLiteCursorWrapper:
    def __init__(self, cursor):
        self._cursor = cursor
        self._empty_result = False  # flag for demo mode

    def execute(self, sql, params=None):
        # Replace MySQL-style "%s" with SQLite "?" placeholders
        if "%s" in sql:
            sql = sql.replace("%s", "?")

        self._empty_result = False
        try:
            if params is None:
                return self._cursor.execute(sql)
            return self._cursor.execute(sql, params)
        except sqlite3.OperationalError as e:
            # DEMO MODE: if a table is missing, just pretend query returned nothing
            if "no such table" in str(e):
                print(f"[WARN] {e} -- returning empty result (demo mode)")
                self._empty_result = True
                # return self so caller can still call fetchall()/fetchone()
                return self
            # other SQLite errors still bubble up
            raise

    def executemany(self, sql, seq_of_params):
        if "%s" in sql:
            sql = sql.replace("%s", "?")
        self._empty_result = False
        try:
            return self._cursor.executemany(sql, seq_of_params)
        except sqlite3.OperationalError as e:
            if "no such table" in str(e):
                print(f"[WARN] {e} -- ignoring executemany (demo mode)")
                self._empty_result = True
                return self
            raise

    def fetchall(self):
        if self._empty_result:
            return []  # no data instead of error
        rows = self._cursor.fetchall()
        return [dict(r) for r in rows]

    def fetchone(self):
        if self._empty_result:
            return None
        r = self._cursor.fetchone()
        return dict(r) if r is not None else None

    def __iter__(self):
        if self._empty_result:
            return iter([])
        for r in self._cursor:
            yield dict(r)

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class SQLiteConnectionWrapper:
    def __init__(self, conn):
        self._conn = conn

    def cursor(self, *args, **kwargs):
        # ignore dictionary=True from mysql style
        kwargs.pop("dictionary", None)
        cur = self._conn.cursor(*args, **kwargs)
        return SQLiteCursorWrapper(cur)

    def __getattr__(self, name):
        return getattr(self._conn, name)


def parse_filters(req):
    """Uniform filter extraction."""
    return {
        "category":      (req.args.get("category") or "ALL").upper().strip(),
        "metric":        (req.args.get("metric") or "qty").lower().strip(),
        "region":        (req.args.get("region") or "ALL").strip(),
        "salesman":      (req.args.get("salesman") or "ALL").strip(),
        "sold_to_group": (req.args.get("sold_to_group") or "ALL").strip(),
        "sold_to":       (req.args.get("sold_to") or "ALL").strip(),
        "ship_to":       (req.args.get("ship_to") or "ALL").strip(),
        "product_group": (req.args.get("product_group") or "ALL").strip(),
        "pattern":       (req.args.get("pattern") or "ALL").strip(),
    }

def build_customer_filters(alias_fact: str, f, *, use_sold_to_name: bool=False):
    """
    Returns (joins, wheres, params) to apply Region/Salesman/Group/Sold_to on a fact table
    by joining customer once on equality:
        JOIN customer cus ON cus.ship_to = <fact>.ship_to
    If use_sold_to_name=True, 'sold_to' will match customer.Sold_to_Name instead of id.
    """
    joins = [f"left JOIN customer cus ON cus.ship_to = {alias_fact}.ship_to"]
    wh, p = [], []

    if f["region"] != "ALL":
        wh.append("cus.bde_state = %s"); p.append(f["region"])
    if f["salesman"] != "ALL":
        wh.append("UPPER(TRIM(cus.salesman_name)) = UPPER(TRIM(%s))"); p.append(f["salesman"])
    if f["sold_to_group"] != "ALL":
        wh.append("cus.sold_to_group = %s"); p.append(f["sold_to_group"])

    # sold_to: id (A.. / digits) or match by name via customer
    if f["sold_to"] != "ALL":
        sv = f["sold_to"]
        if not use_sold_to_name and (sv.isdigit() or sv.upper().startswith("A")):
            wh.append(f"{alias_fact}.ship_to = %s"); p.append(sv)
        else:
            wh.append("cus.sold_to_name = %s"); p.append(sv)

    # explicit ship_to id filter if given
    if f["ship_to"] != "ALL":
        wh.append(f"{alias_fact}.ship_to = %s"); p.append(f["ship_to"])

    return joins, wh, p


def category_filters(alias: str, category: str):
    """
    Return (joins, wheres) for monthly-schema facts (sales2025, profit).
    - alias: table alias for the fact (e.g., "s" for sales2025, "p" for profit)
    - All predicates are index-friendly (equality / LIKE prefix).
    - Add optional JOINs only when that category needs them.
    """
    joins, wh = [], []
    cat = (category or "ALL").upper()

    if cat == "ALL":
        return joins, wh

    elif cat == "PCLT":
        # material codes starting with 1 or 2
        wh.append(f"{alias}.line = 'PCLT'")

    elif cat == "TBR":
        # example logic: material codes starting with 3 (adjust to your real rule)
        wh.append(f"{alias}.line = 'TBR'")

     # NEW: 18+ Inch means PCLT & inch > 18
    elif cat == "18PLUS":
        wh.append(f"{alias}.line = 'PCLT'")
        # inch is often stored as text; cast to numeric for safety
        wh.append(f"CAST({alias}.inch AS DECIMAL(10,2)) >= 18.0")
        
    elif cat == "ISEG":
        # ISEG mapping by Material
        # Ensure an index on iseg(Material)
        joins.append(f"JOIN iseg i ON cast(trim(i.Material) as unsigned) = {alias}.material")

    elif cat == "SUV":
        # SUV by Pattern
        # Ensure an index on suv(Pattern)
        joins.append(f"JOIN suv suv ON suv.Pattern = {alias}.pattern")

    elif cat == "LOWPROFILE":
        # Low profile / strategic by Material
        joins.append(f"JOIN lowprofile lp ON cast(trim(lp.Material) as unsigned) = {alias}.material")

    elif cat == "HM":
        # HM by Sold-To (use your customer join for Ship_To ⇒ Sold_To; keep simple)
        # If your HM rule is customer-list based, prefer EXISTS against a keyed table.
        joins.append(f"JOIN HM hm ON hm.Sold_To = {alias}.sold_to")

    return joins, wh

def category_target_filters(alias: str, category: str):
    """
    Return (joins, wheres) for monthly-schema facts (sales2025, profit).
    - alias: table alias for the fact (e.g., "s" for sales2025, "p" for profit)
    - All predicates are index-friendly (equality / LIKE prefix).
    - Add optional JOINs only when that category needs them.
    """
    joins, wh = [], []
    cat = (category or "ALL").upper()

    if cat == "ALL":
        wh.append(f"{alias}.special =''")

    elif cat == "PCLT":
        # material codes starting with 1 or 2
        wh.append(f"{alias}.line = 'PCLT'")
        wh.append(f"{alias}.special =''")

    elif cat == "TBR":
        # example logic: material codes starting with 3 (adjust to your real rule)
        wh.append(f"{alias}.line = 'TBR'")
        wh.append(f"{alias}.special =''")

     # NEW: 18+ Inch means PCLT & inch > 18
    elif cat == "18PLUS":
        wh.append(f"{alias}.special = 'HighInch'")
       
        
    elif cat == "ISEG":
        wh.append(f"{alias}.special = 'iSeg'")

    elif cat == "SUV":
        
        wh.append(f"{alias}.special = 'SUV'")

    elif cat == "LOWPROFILE":
       wh.append(f"{alias}.special = 'Low Profile / Strategic TBR'")

    elif cat == "HM":
        # HM by Sold-To (use your customer join for Ship_To ⇒ Sold_To; keep simple)
        # If your HM rule is customer-list based, prefer EXISTS against a keyed table.
        wh.append(f"{alias}.special = 'HM'")

    return joins, wh

app = Flask(__name__, static_folder="static")
CORS(app, resources={r"/api/*": {"origins": "*"}})

def get_connection():
    # If USE_SQLITE=1 (on Render), use the local snapshot.db file
    if USE_SQLITE:
        conn = sqlite3.connect(SQLITE_PATH)
        conn.row_factory = sqlite3.Row  # rows behave like dicts
        return SQLiteConnectionWrapper(conn)

    # Otherwise use MySQL (your current local setup)
    cfg = {
        "host": os.getenv("DB_HOST", "127.0.0.1"),
        "port": int(os.getenv("DB_PORT", "3306")),
        "user": os.getenv("DB_USER", "root"),
        "password": os.getenv("DB_PASS", ""),
        "database": os.getenv("DB_NAME", "my_new_database"),  # change to your real db
        "autocommit": True,
    }
    try:
        return mysql.connector.connect(**cfg)
    except mysql.connector.Error as e:
        # temporary: don't kill the app, just log
        print("DB connection failed:", e)
        return None
@app.get("/api/ping")
def ping():
    return {"ok": True}

# ------------------------------------------------------------------------------

@app.route("/")
def index():
    return app.send_static_file("index.html")

@app.route("/map")
def map_page():
    return app.send_static_file("map.html")



# ----------------------------- Daily Sales ---------------------------------
@app.get("/api/daily_sales")
def daily_sales():
    f = parse_filters(request)
    value = "qty" if f["metric"] == "qty" else "amt"

    # 0 or missing = no top filter
    top_limit = int(request.args.get("top_limit", 0) or 0)

    joins, wh, params = build_customer_filters("s", f, use_sold_to_name=False)

    # category
    cat_joins, cat_where = category_filters("s", f["category"])
    joins += cat_joins
    wh    += cat_where

    # direct fields (indexable)
    if f["product_group"] != "ALL":
        wh.append("s.product_group = %s")
        params.append(f["product_group"])
    if f["pattern"] != "ALL":
        wh.append("s.pattern = %s")
        params.append(f["pattern"])

    base_where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        top_sold_to = None

        # 1) If top_limit > 0, get top N sold_to first
        if top_limit > 0:
            top_sql = f"""
              SELECT s.sold_to AS sold_to
                FROM sales_2511 s
                {' '.join(joins)}
                {base_where_sql}
               GROUP BY s.sold_to
               ORDER BY SUM(s.{value}) DESC
               LIMIT %s
            """
            cur.execute(top_sql, tuple(params) + (top_limit,))
            top_rows = cur.fetchall()
            top_sold_to = [r["sold_to"] for r in top_rows]

            if not top_sold_to:
                # no matching customers – all days = 0
                return jsonify([{"day": d, "value": 0} for d in range(1, 31)])

        # 2) Daily totals, optionally restricted to top N sold_to
        wh2 = list(wh)
        params2 = list(params)

        if top_sold_to:
            placeholders = ",".join(["%s"] * len(top_sold_to))
            wh2.append(f"s.sold_to IN ({placeholders})")
            params2.extend(top_sold_to)

        where_sql2 = ("WHERE " + " AND ".join(wh2)) if wh2 else ""

        daily_sql = f"""
          SELECT s.day AS day_num, SUM(s.{value}) AS daily_total
            FROM sales_2511 s
            {' '.join(joins)}
            {where_sql2}
           GROUP BY s.day
           ORDER BY s.day
        """
        cur.execute(daily_sql, tuple(params2))
        rows = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    day_map = {int(r["day_num"]): float(r["daily_total"] or 0) for r in rows}
    return jsonify([{"day": d, "value": day_map.get(d, 0)} for d in range(1, 31)])

#
# -------------------- Daily breakdown (stacked by group) -------------------
@app.get("/api/daily_breakdown")
def daily_breakdown():

    f = parse_filters(request)
    value = "qty" if f["metric"] == "qty" else "amt"
    # 0 or missing = no top filter
    top_limit = int(request.args.get("top_limit", 0) or 0)

    # Which dimension to group by?
    group_by = (request.args.get("group_by") or "region").strip()
    group_cols = {
        "product_group": "s.product_group",
        "region":        "cus.bde_state",
        "salesman":      "cus.salesman_name",
        "sold_to_group": "cus.sold_to_group",
        "sold_to":       "cus.sold_to_name",
        "pattern":       "s.pattern",
    }
    if group_by not in group_cols:
        return jsonify({"error": "invalid group_by"}), 400
    group_col = group_cols[group_by]

    # ---- Build base JOINs / WHEREs (same as daily_sales) ----
    joins, wh, params = build_customer_filters("s", f, use_sold_to_name=False)
    cat_joins, cat_where = category_filters("s", f["category"])
    joins += cat_joins
    wh    += cat_where

    # Direct, index-friendly filters that live on sales202511
    if f["product_group"] != "ALL":
        wh.append("s.product_group = %s"); params.append(f["product_group"])
    if f["pattern"] != "ALL":
        wh.append("s.pattern = %s");       params.append(f["pattern"])

    base_where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        top_sold_to = None

        # 1) If top_limit > 0, get top N sold_to first (same as daily_sales)
        if top_limit > 0:
            top_sql = f"""
              SELECT s.sold_to AS sold_to
                FROM sales_2511 s
                {' '.join(joins)}
                {base_where_sql}
               GROUP BY s.sold_to
               ORDER BY SUM(s.{value}) DESC
               LIMIT %s
            """
            cur.execute(top_sql, tuple(params) + (top_limit,))
            top_rows = cur.fetchall()
            top_sold_to = [r["sold_to"] for r in top_rows]

            # no matching customers – nothing to show
            if not top_sold_to:
                return jsonify([])

        # 2) Daily breakdown, restricted to those top customers,
        #    but stacked by group_col (region / salesman / etc.)
        wh2 = list(wh)
        params2 = list(params)

        if top_sold_to:
            placeholders = ",".join(["%s"] * len(top_sold_to))
            wh2.append(f"s.sold_to IN ({placeholders})")
            params2.extend(top_sold_to)

        where_sql2 = ("WHERE " + " AND ".join(wh2)) if wh2 else ""

        sql = f"""
          SELECT s.day AS day,
                 {group_col} AS group_label,
                 SUM(s.{value}) AS value
            FROM sales_2511 s
            {' '.join(joins)}
            {where_sql2}
           GROUP BY s.day, {group_col}
           ORDER BY s.day
        """
        cur.execute(sql, tuple(params2))
        rows = cur.fetchall()

    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass

    return jsonify(rows)

# ----------------------------- Daily Target (Oct) ---------------------------------
import calendar
@app.get("/api/daily_target")
def daily_target():
    f = parse_filters(request)
    value = "qty" if f["metric"] == "qty" else "amt"

    # which month? default to October (10) if nothing is passed
    month = int(request.args.get("month", 11))

    # 0 or missing = no top filter
    top_limit = int(request.args.get("top_limit", 0) or 0)

    joins, wh, params = build_customer_filters("t", f, use_sold_to_name=False)

    # category filters for target table
    cat_joins, cat_where = category_target_filters("t", f["category"])
    joins += cat_joins
    wh    += cat_where

    # restrict to the chosen month only
    wh.append("t.month = %s")
    params.append(month)

    base_where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

    conn = get_connection()
    cur  = conn.cursor(dictionary=True)
    try:
        top_sold_to = None

        # 1) If top_limit > 0, get top N sold_to from target2025
        if top_limit > 0:
            top_sql = f"""
              SELECT t.sold_to AS sold_to
                FROM target2025 t
                {' '.join(joins)}
                {base_where_sql}
               GROUP BY t.sold_to
               ORDER BY SUM(t.{value}) DESC
               LIMIT %s
            """
            cur.execute(top_sql, tuple(params) + (top_limit,))
            top_rows = cur.fetchall()
            top_sold_to = [r["sold_to"] for r in top_rows]

            # nothing matched -> all days = 0
            if not top_sold_to:
                days_in_month = calendar.monthrange(2025, month)[1]
                return jsonify([{"day": d, "value": 0} for d in range(1, days_in_month + 1)])

        # 2) Monthly target, optionally restricted to top N sold_to
        wh2      = list(wh)
        params2  = list(params)

        if top_sold_to:
            placeholders = ",".join(["%s"] * len(top_sold_to))
            wh2.append(f"t.sold_to IN ({placeholders})")
            params2.extend(top_sold_to)

        where_sql2 = ("WHERE " + " AND ".join(wh2)) if wh2 else ""

        sql = f"""
          SELECT t.month AS month_num, SUM(t.{value}) AS monthly_total
            FROM target2025 t
            {' '.join(joins)}
            {where_sql2}
           GROUP BY t.month
           ORDER BY t.month
        """

        cur.execute(sql, tuple(params2))
        row = cur.fetchone()

    finally:
        cur.close()
        conn.close()

    monthly_total = float(row["monthly_total"] or 0) if row else 0

    # how many days in that month? (2025 used as the year for target2025)
    days_in_month = calendar.monthrange(2025, month)[1]
    daily_value   = monthly_total / days_in_month if days_in_month else 0

    # return one entry per day: 1..N
    return jsonify([
        {"day": d, "value": daily_value}
        for d in range(1, days_in_month + 1)
    ])

# ----------------------------- Monthly Sales ---------------------------------
@app.get("/api/monthly_sales")
def monthly_sales():
    f = parse_filters(request)
    value = "qty" if f["metric"] == "qty" else "amt"

    # 0 or missing = no top filter, same behaviour as before
    top_limit = int(request.args.get("top_limit", 0) or 0)

    joins, wh, params = build_customer_filters("s", f, use_sold_to_name=False)

    # category
    cat_joins, cat_where = category_filters("s", f["category"])
    joins += cat_joins
    wh    += cat_where

    # direct fields (indexable)
    if f["product_group"] != "ALL":
        wh.append("s.product_group = %s")
        params.append(f["product_group"])
    if f["pattern"] != "ALL":
        wh.append("s.pattern = %s")
        params.append(f["pattern"])

    base_where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        top_sold_to = None

        # 1) If top_limit > 0, get top N sold_to first
        if top_limit > 0:
            top_sql = f"""
              SELECT s.sold_to AS sold_to
                FROM sales_2501_11 s
                {' '.join(joins)}
                {base_where_sql}
               GROUP BY s.sold_to
               ORDER BY SUM(s.{value}) DESC
               LIMIT %s
            """
            cur.execute(top_sql, tuple(params) + (top_limit,))
            top_rows = cur.fetchall()
            top_sold_to = [r["sold_to"] for r in top_rows]

            # If nothing found, just return zeros for all 12 months
            if not top_sold_to:
                return jsonify([{"month": m, "value": 0} for m in range(1, 12)])

        # 2) Monthly totals, optionally restricted to the top N sold_to
        wh2 = list(wh)
        params2 = list(params)

        if top_sold_to:
            placeholders = ",".join(["%s"] * len(top_sold_to))
            wh2.append(f"s.sold_to IN ({placeholders})")
            params2.extend(top_sold_to)

        where_sql2 = ("WHERE " + " AND ".join(wh2)) if wh2 else ""

        monthly_sql = f"""
          SELECT s.month AS month_num, SUM(s.{value}) AS monthly_total
            FROM sales_2501_11 s
            {' '.join(joins)}
            {where_sql2}
           GROUP BY s.month
           ORDER BY s.month
        """
        cur.execute(monthly_sql, tuple(params2))
        rows = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    month_map = {int(r["month_num"]): float(r["monthly_total"] or 0) for r in rows}
    return jsonify([{"month": m, "value": month_map.get(m, 0)} for m in range(1, 12)])

# -------------------- Monthly breakdown (stacked by group) -------------------
@app.get("/api/monthly_breakdown")
def monthly_breakdown():

    f = parse_filters(request)
    value = "qty" if f["metric"] == "qty" else "amt"

    # 0 or missing = no top filter
    top_limit = int(request.args.get("top_limit", 0) or 0)

    # Which dimension to group by?
    group_by = (request.args.get("group_by") or "region").strip()
    group_cols = {
        "product_group": "s.product_group",
        "region":        "cus.bde_state",
        "salesman":      "cus.salesman_name",
        "sold_to_group": "cus.sold_to_group",
        "sold_to":       "cus.sold_to_name",
        "pattern":       "s.pattern",
    }
    if group_by not in group_cols:
        return jsonify({"error": "invalid group_by"}), 400
    group_col = group_cols[group_by]

    # ---- Build base JOINs / WHEREs (same pattern as monthly_sales) ----
    joins, wh, params = build_customer_filters("s", f, use_sold_to_name=False)
    cat_joins, cat_where = category_filters("s", f["category"])
    joins += cat_joins
    wh    += cat_where

    # Direct, index-friendly filters that live on sales2025
    if f["product_group"] != "ALL":
        wh.append("s.product_group = %s"); params.append(f["product_group"])
    if f["pattern"] != "ALL":
        wh.append("s.pattern = %s");       params.append(f["pattern"])

    base_where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

    conn = get_connection()
    cur  = conn.cursor(dictionary=True)
    try:
        top_sold_to = None

        # 1) If top_limit > 0, get top N sold_to first (same as monthly_sales)
        if top_limit > 0:
            top_sql = f"""
              SELECT s.sold_to AS sold_to
                FROM sales_2501_11 s
                {' '.join(joins)}
                {base_where_sql}
               GROUP BY s.sold_to
               ORDER BY SUM(s.{value}) DESC
               LIMIT %s
            """
            cur.execute(top_sql, tuple(params) + (top_limit,))
            top_rows = cur.fetchall()
            top_sold_to = [r["sold_to"] for r in top_rows]

            # no matching customers – nothing to show
            if not top_sold_to:
                return jsonify([])

        # 2) Monthly breakdown, restricted to those top customers,
        #    but stacked by group_col (region / salesman / etc.)
        wh2     = list(wh)
        params2 = list(params)

        if top_sold_to:
            placeholders = ",".join(["%s"] * len(top_sold_to))
            wh2.append(f"s.sold_to IN ({placeholders})")
            params2.extend(top_sold_to)

        where_sql2 = ("WHERE " + " AND ".join(wh2)) if wh2 else ""

        sql = f"""
          SELECT s.Month AS month,
                 {group_col} AS group_label,
                 SUM(s.{value}) AS value
            FROM sales_2501_11 s
            {' '.join(joins)}
            {where_sql2}
           GROUP BY s.Month, {group_col}
           ORDER BY s.Month
        """
        cur.execute(sql, tuple(params2))
        rows = cur.fetchall()

    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass

    return jsonify(rows)


# ----------------------------- Monthly Target ---------------------------------
@app.get("/api/monthly_target")
def monthly_target():
    f = parse_filters(request)
    value = "qty" if f["metric"] == "qty" else "amt"

    # 0 or missing = no top filter
    top_limit = int(request.args.get("top_limit", 0) or 0)

    joins, wh, params = build_customer_filters("t", f, use_sold_to_name=False)

    # category filters for target table
    cat_joins, cat_where = category_target_filters("t", f["category"])
    joins += cat_joins
    wh    += cat_where

    # if target table also has product_group / pattern, keep these:
    if f.get("product_group") and f["product_group"] != "ALL":
        wh.append("t.product_group = %s")
        params.append(f["product_group"])
    if f.get("pattern") and f["pattern"] != "ALL":
        wh.append("t.pattern = %s")
        params.append(f["pattern"])

    base_where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

    conn = get_connection()
    cur  = conn.cursor(dictionary=True)
    try:
        top_sold_to = None

        # 1) If top_limit > 0, find top N sold_to in target2025
        if top_limit > 0:
            top_sql = f"""
              SELECT t.sold_to AS sold_to
                FROM target2025 t
                {' '.join(joins)}
                {base_where_sql}
               GROUP BY t.sold_to
               ORDER BY SUM(t.{value}) DESC
               LIMIT %s
            """
            cur.execute(top_sql, tuple(params) + (top_limit,))
            top_rows = cur.fetchall()
            top_sold_to = [r["sold_to"] for r in top_rows]

            # no matches -> all months zero
            if not top_sold_to:
                return jsonify([{"month": m, "value": 0} for m in range(1, 13)])

        # 2) Monthly target, optionally restricted to top N sold_to
        wh2     = list(wh)
        params2 = list(params)

        if top_sold_to:
            placeholders = ",".join(["%s"] * len(top_sold_to))
            wh2.append(f"t.sold_to IN ({placeholders})")
            params2.extend(top_sold_to)

        where_sql2 = ("WHERE " + " AND ".join(wh2)) if wh2 else ""

        monthly_sql = f"""
          SELECT t.month AS month_num, SUM(t.{value}) AS monthly_total
            FROM target2025 t
            {' '.join(joins)}
            {where_sql2}
            GROUP BY t.month
            ORDER BY t.month
        """
        cur.execute(monthly_sql, tuple(params2))
        rows = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    month_map = {int(r["month_num"]): float(r["monthly_total"] or 0) for r in rows}
    return jsonify([{"month": m, "value": month_map.get(m, 0)} for m in range(1, 13)])

# ----------------------------- Yearly Sales ---------------------------------
@app.get("/api/yearly_sales")
def yearly_sales():
    f = parse_filters(request)
    value = "qty" if f["metric"] == "qty" else "amt"

    # 0 or missing = no top filter
    top_limit = int(request.args.get("top_limit", 0) or 0)

    joins, wh, params = build_customer_filters("s", f, use_sold_to_name=False)

    # category
    cat_joins, cat_where = category_filters("s", f["category"])
    joins += cat_joins
    wh    += cat_where

    # direct fields (indexable)
    if f["product_group"] != "ALL":
        wh.append("s.product_group = %s")
        params.append(f["product_group"])
    if f["pattern"] != "ALL":
        wh.append("s.pattern = %s")
        params.append(f["pattern"])

    base_where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        top_sold_to = None

        # 1) If top_limit > 0, get top N sold_to first
        if top_limit > 0:
            top_sql = f"""
              SELECT s.sold_to AS sold_to
                FROM sales_21_2511 s
                {' '.join(joins)}
                {base_where_sql}
               GROUP BY s.sold_to
               ORDER BY SUM(s.{value}) DESC
               LIMIT %s
            """
            cur.execute(top_sql, tuple(params) + (top_limit,))
            top_rows = cur.fetchall()
            top_sold_to = [r["sold_to"] for r in top_rows]

            if not top_sold_to:
                # no data – return zeros for all years in range
                return jsonify([{"year": y, "value": 0} for y in range(2021, 2026)])

        # 2) Yearly totals, optionally restricted to those sold_to
        wh2 = list(wh)
        params2 = list(params)

        if top_sold_to:
            placeholders = ",".join(["%s"] * len(top_sold_to))
            wh2.append(f"s.sold_to IN ({placeholders})")
            params2.extend(top_sold_to)

        where_sql2 = ("WHERE " + " AND ".join(wh2)) if wh2 else ""

        yearly_sql = f"""
          SELECT s.year AS year_num, SUM(s.{value}) AS yearly_total
            FROM sales_21_2511 s
            {' '.join(joins)}
            {where_sql2}
           GROUP BY s.year
           ORDER BY s.year
        """
        cur.execute(yearly_sql, tuple(params2))
        rows = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    year_map = {int(r["year_num"]): float(r["yearly_total"] or 0) for r in rows}
    return jsonify([{"year": y, "value": year_map.get(y, 0)} for y in range(2021, 2026)])

# -------------------- Yearly breakdown (stacked by group) -------------------
@app.get("/api/yearly_breakdown")
def yearly_breakdown():

    f = parse_filters(request)
    value = "qty" if f["metric"] == "qty" else "amt"

    # 0 or missing = no top filter
    top_limit = int(request.args.get("top_limit", 0) or 0)

    # Which dimension to group by?
    group_by = (request.args.get("group_by") or "region").strip()
    group_cols = {
        "product_group": "s.product_group",
        "region":        "cus.bde_state",
        "salesman":      "cus.salesman_name",
        "sold_to_group": "cus.sold_to_group",
        "sold_to":       "cus.sold_to_name",
        "pattern":       "s.pattern",
    }
    if group_by not in group_cols:
        return jsonify({"error": "invalid group_by"}), 400
    group_col = group_cols[group_by]

    # ---- Build base JOINs / WHEREs (same pattern as yearly_sales) ----
    joins, wh, params = build_customer_filters("s", f, use_sold_to_name=False)
    cat_joins, cat_where = category_filters("s", f["category"])
    joins += cat_joins
    wh    += cat_where

    # Direct, index-friendly filters that live on sales212510
    if f["product_group"] != "ALL":
        wh.append("s.product_group = %s"); params.append(f["product_group"])
    if f["pattern"] != "ALL":
        wh.append("s.pattern = %s");       params.append(f["pattern"])

    base_where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

    conn = get_connection()
    cur  = conn.cursor(dictionary=True)
    try:
        top_sold_to = None

        # 1) If top_limit > 0, get top N sold_to first (same as yearly_sales)
        if top_limit > 0:
            top_sql = f"""
              SELECT s.sold_to AS sold_to
                FROM sales_21_2511 s
                {' '.join(joins)}
                {base_where_sql}
               GROUP BY s.sold_to
               ORDER BY SUM(s.{value}) DESC
               LIMIT %s
            """
            cur.execute(top_sql, tuple(params) + (top_limit,))
            top_rows = cur.fetchall()
            top_sold_to = [r["sold_to"] for r in top_rows]

            # no data – nothing to show
            if not top_sold_to:
                return jsonify([])

        # 2) Yearly breakdown, restricted to those top customers,
        #    but stacked by group_col
        wh2     = list(wh)
        params2 = list(params)

        if top_sold_to:
            placeholders = ",".join(["%s"] * len(top_sold_to))
            wh2.append(f"s.sold_to IN ({placeholders})")
            params2.extend(top_sold_to)

        where_sql2 = ("WHERE " + " AND ".join(wh2)) if wh2 else ""

        sql = f"""
          SELECT s.year AS year,
                 {group_col} AS group_label,
                 SUM(s.{value}) AS value
            FROM sales_21_2511 s
            {' '.join(joins)}
            {where_sql2}
           GROUP BY s.year, {group_col}
           ORDER BY s.year
        """
        cur.execute(sql, tuple(params2))
        rows = cur.fetchall()

    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass

    return jsonify(rows)

# ---------------------- lookups used by the UI (optional) --------------------
@app.get("/api/sold_to_groups")
def sold_to_groups():
    try:
        conn = get_connection(); cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT TRIM(sold_to_group)
            FROM customer
            WHERE sold_to_group IS NOT NULL AND TRIM(sold_to_group) <> ''
            ORDER BY TRIM(sold_to_group)
        """)
        groups = [r[0] for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify(groups)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/api/sold_to_names")
def sold_to_names():
    parent = request.args.get("sold_to_group", "ALL")
    top_limit = int(request.args.get("top_limit", 0) or 0)

    # ----------------- 1) No top_limit -> old behaviour -----------------
    if top_limit <= 0:
        try:
            conn = get_connection(); cur = conn.cursor()
            if parent != "ALL":
                cur.execute("""
                    SELECT DISTINCT TRIM(sold_to_name)
                    FROM customer
                    WHERE sold_to_group = %s
                      AND sold_to_name IS NOT NULL
                      AND TRIM(sold_to_name) <> ''
                    ORDER BY TRIM(sold_to_name)
                """, (parent,))
            else:
                cur.execute("""
                    SELECT DISTINCT TRIM(sold_to_name)
                    FROM customer
                    WHERE sold_to_name IS NOT NULL
                      AND TRIM(sold_to_name) <> ''
                    ORDER BY TRIM(sold_to_name)
                """)
            names = [r[0] for r in cur.fetchall()]
            cur.close(); conn.close()
            return jsonify(names)
        except Exception as e:
            import traceback; traceback.print_exc()
            return jsonify({"error": str(e)}), 500

    # --------------- 2) top_limit > 0 -> Top N by sales -----------------
    try:
        f = parse_filters(request)
        value = "qty" if f["metric"] == "qty" else "amt"

        joins, wh, params = build_customer_filters("s", f, use_sold_to_name=False)

        # category filters
        cat_joins, cat_where = category_filters("s", f["category"])
        joins += cat_joins
        wh    += cat_where

        # sold_to_group from parent – apply to *customer* table (cus)
        if parent != "ALL":
            wh.append("cus.sold_to_group = %s")
            params.append(parent)

        # direct fields on sales table
        if f["product_group"] != "ALL":
            wh.append("s.product_group = %s")
            params.append(f["product_group"])
        if f["pattern"] != "ALL":
            wh.append("s.pattern = %s")
            params.append(f["pattern"])

        where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

        sql = f"""
          SELECT
              TRIM(cus.sold_to_name) AS name,
              SUM(s.{value})         AS total_val
          FROM sales_2501_11 s
          
          {' '.join(joins)}
          {where_sql}
          GROUP BY cus.sold_to, TRIM(cus.sold_to_name)
          HAVING name IS NOT NULL AND name <> ''
          ORDER BY total_val DESC
          LIMIT %s
        """
        params2 = params + [top_limit]

        # plain cursor (works for MySQL and SQLite wrapper)
        conn = get_connection(); cur = conn.cursor()
        cur.execute(sql, tuple(params2))
        rows = cur.fetchall()
        cur.close(); conn.close()

        # first column is name
        names = [r[0] for r in rows]
        return jsonify(names)

    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.get("/api/ship_to_names")
def ship_to_names():
    # parent (big group)
    stg3    = (request.args.get("sold_to_group") or "ALL").strip()
    # child (sold-to name that user picked)
    sold_to = (request.args.get("sold_to") or "ALL").strip()

    try:
        conn = get_connection(); cur = conn.cursor()

        where = ["ship_to_name IS NOT NULL", "TRIM(ship_to_name) <> ''"]
        params = []

        # 1) if user picked a specific sold_to_name → use that
        if sold_to.upper() != "ALL":
            where.append("TRIM(sold_to_name) = %s")
            params.append(sold_to)
        # 2) otherwise, if user picked a group → use that
        elif stg3.upper() != "ALL":
            where.append("TRIM(sold_to_group) = %s")
            params.append(stg3)

        where_sql = "WHERE " + " AND ".join(where)

        cur.execute(f"""
            SELECT DISTINCT TRIM(ship_to_name)
            FROM customer
            {where_sql}
            ORDER BY TRIM(ship_to_name)
        """, tuple(params))

        names = [r[0] for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify(names)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/api/product_group")
def product_group():
    try:
        conn = get_connection(); cur = conn.cursor()
        cur.execute("SELECT DISTINCT product_group FROM sales_2501_11")
        groups = sorted(r[0] for r in cur.fetchall())
        cur.close(); conn.close()
        return jsonify(groups)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    
@app.get("/api/patterns")
def patterns():
    product_group = request.args.get("product_group", "ALL")
    try:
        conn = get_connection(); cur = conn.cursor()
        if product_group and product_group != "ALL":
            cur.execute("""
                SELECT DISTINCT TRIM(pattern)
                FROM sales_2501_11
                WHERE product_group = %s
                ORDER BY TRIM(pattern)
            """, (product_group,))
        else:
            cur.execute("""
                SELECT DISTINCT TRIM(pattern)
                FROM sales_2501_11
                ORDER BY TRIM(pattern)
            """)
        names = [r[0] for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify(names)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/api/profit_monthly")
def profit_monthly():
    import traceback

    try:
        f = parse_filters(request)

        # optional: ?top_limit=10 -> top 10 sold_to by profit
        top_limit = int(request.args.get("top_limit", 0) or 0)

        joins  = []
        wh     = []
        params = []

        # category filters (PCLT / TBR / 18PLUS etc.)
        # category_filters only needs the alias, and uses material etc.,
        # which exist in profit_2501_10
        cat_joins, cat_where = category_filters("p", f.get("category", "ALL"))
        joins += cat_joins
        wh    += cat_where

        # direct filters on profit_2501_10
        if f.get("product_group", "ALL") != "ALL":
            wh.append("p.product_group = %s")
            params.append(f["product_group"])

        if f.get("pattern", "ALL") != "ALL":
            wh.append("p.pattern = %s")
            params.append(f["pattern"])

        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        try:
            top_sold_to = None

            # 1) Top N sold_to by profit (optional)
            if top_limit > 0:
                where_top = ("WHERE " + " AND ".join(wh)) if wh else ""
                top_sql = f"""
                    SELECT p.sold_to,
                           SUM(p.profit) AS total_profit
                      FROM profit_2501_10 p
                      {' '.join(joins)}
                      {where_top}
                     GROUP BY p.sold_to
                     ORDER BY total_profit DESC
                     LIMIT %s
                """
                cur.execute(top_sql, tuple(params) + (top_limit,))
                top_rows = cur.fetchall()
                top_sold_to = [r["sold_to"] for r in top_rows]

                # nothing found -> all zeros
                if not top_sold_to:
                    return jsonify([
                        dict(month=m, gross=0, sd=0, cogs=0, op_cost=0)
                        for m in range(1, 13)
                    ])

            # 2) Monthly totals, optionally restricted to those top sold_to
            wh2     = list(wh)
            params2 = list(params)

            if top_sold_to:
                placeholders = ",".join(["%s"] * len(top_sold_to))
                wh2.append(f"p.sold_to IN ({placeholders})")
                params2.extend(top_sold_to)

            where_sql2 = ("WHERE " + " AND ".join(wh2)) if wh2 else ""
            monthly_sql = f"""
                SELECT CAST(p.month AS UNSIGNED) AS month,
                       SUM(p.gross)           AS gross,
                       SUM(p.sales_deduction) AS sd,
                       SUM(p.cogs)            AS cogs,
                       SUM(p.operating_cost)  AS op_cost
                  FROM profit_2501_10 p
                  {' '.join(joins)}
                  {where_sql2}
                 GROUP BY CAST(p.month AS UNSIGNED)
                 ORDER BY CAST(p.month AS UNSIGNED)
            """
            cur.execute(monthly_sql, tuple(params2))
            rows = cur.fetchall()
        finally:
            cur.close()
            conn.close()

        # Build output for months 1..12
        out = [dict(month=m, gross=0, sd=0, cogs=0, op_cost=0) for m in range(1, 13)]
        for r in rows:
            m = int(r["month"] or 0)
            if 1 <= m <= 12:
                out[m-1].update(
                    gross=float(r["gross"] or 0),
                    sd=float(r["sd"] or 0),
                    cogs=float(r["cogs"] or 0),
                    op_cost=float(r["op_cost"] or 0),
                )

        return jsonify(out)

    except Exception as e:
        # you’ll see the full traceback in the Flask terminal
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.get("/api/sales_map")
def sales_map():
    # 1) Same filter parsing as other APIs
    f = parse_filters(request)
    value = "Qty" if f["metric"] == "qty" else "amt"

    # 2) Customer / region / salesman / product filters
    joins, wh, params = build_customer_filters("s", f, use_sold_to_name=False)

    joins.append("""
    JOIN customer c
      ON c.ship_to = s.ship_to   -- CHANGE s.ship_to to your real sales key column
""")
    # 3) Category filters (PCLT/TBR/18+ etc.)
    cat_joins, cat_where = category_filters("s", f["category"])
    joins += cat_joins
    wh    += cat_where

    # 4) Direct fields
    if f["product_group"] != "ALL":
        wh.append("s.product_group = %s"); params.append(f["product_group"])
    if f["pattern"] != "ALL":
        wh.append("s.pattern = %s"); params.append(f["pattern"])

    # 5) Only customers that have coordinates
    #    (lat/lng are on the customer table that build_customer_filters joined, here assumed alias c)
    wh.append("c.latitude IS NOT NULL")
    wh.append("c.longitude IS NOT NULL")

    where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

    # 6) Aggregate by customer/location instead of by day
    sql = f"""
      SELECT
          c.ship_to       AS ship_to,
          c.ship_to_name  AS ship_to_name,
          c.latitude           AS latitude,
          c.longitude           AS longitude,
          MAX(c.bde_state)   AS region,      -- make sure this line exists
          MAX(c.salesman_name)      AS bde,         -- and this one
          SUM(s.{value})  AS total_value
        FROM sales_2501_11 s
        {' '.join(joins)}
        {where_sql}
       GROUP BY
          c.ship_to,
          c.ship_to_name,
          c.latitude,
          c.longitude
       ORDER BY total_value DESC
    """

    conn = get_connection(); cur = conn.cursor(dictionary=True)
    try:
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
    finally:
        cur.close(); conn.close()

    # 7) For the map we just return the rows directly (no day_map)
    return jsonify(rows)

# ------------------------------------------------------------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))   # Cloudtype probes 5000
    app.run(host="0.0.0.0", port=port, debug=False)