from flask import Flask, request, jsonify, send_file,send_from_directory
import sqlite3
import mysql.connector
from mysql.connector import pooling
import threading
from time import time  # cache timestamps
import os
from flask_cors import CORS
from typing import Dict, List, Tuple, Any
from io import BytesIO
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Font, Alignment
from datetime import datetime
import traceback
USE_SQLITE = os.environ.get("USE_SQLITE") == "1"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SQLITE_PATH = os.path.join(BASE_DIR, "snapshot.db")

# MySQL connection pool (reduces connect() overhead per request)
_MYSQL_POOL = None
_MYSQL_POOL_LOCK = threading.Lock()


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
        "year":          (req.args.get("year") or "").strip(),
        "month":         (req.args.get("month") or "").strip(),
        "region":        (req.args.get("region") or "ALL").strip(),
        "salesman":      (req.args.get("salesman") or "ALL").strip(),
        "sold_to_group": (req.args.get("sold_to_group") or "ALL").strip(),
        "sold_to":       (req.args.get("sold_to") or "ALL").strip(),
        "ship_to":       (req.args.get("ship_to") or "ALL").strip(),
        "product_group": (req.args.get("product_group") or "ALL").strip(),
        "pattern":       (req.args.get("pattern") or "ALL").strip(),
        "material":       (req.args.get("material") or "ALL").strip(),
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
    # explicit ship_to filter if given
    if f["ship_to"] != "ALL":
        st = f["ship_to"].strip()

        # If ship_to looks like an ID/code, filter by the fact table ship_to
        if st.isdigit() or st.upper().startswith("A"):
            wh.append(f"{alias_fact}.ship_to = %s")
            p.append(st)
        else:
            # Otherwise treat it as a ship_to_name coming from customer master
            wh.append("UPPER(TRIM(cus.ship_to_name)) = UPPER(TRIM(%s))")
            p.append(st)

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
        joins.append(f"JOIN hm hm ON hm.sold_to = {alias}.sold_to")

    elif cat == "443":
        wh.append(f"""
            EXISTS (
                SELECT 1
                FROM `443_25` p443
                WHERE p443.month         = {alias}.month
                AND p443.product_group = {alias}.product_group
                
            )
            """)
        
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

    

    if cat == "PCLT":
        # material codes starting with 1 or 2
        wh.append(f"{alias}.line = 'PCLT'")

    elif cat == "TBR":
        # example logic: material codes starting with 3 (adjust to your real rule)
        wh.append(f"{alias}.line = 'TBR'")
        
     # NEW: 18+ Inch means PCLT & inch > 18
    elif cat == "18PLUS":
        wh.append(f"{alias}.high_inch = 'High Inch'")
       
        
    elif cat == "ISEG":
        wh.append(f"{alias}.ev = 'EV'")

    elif cat == "SUV":
        
        wh.append(f"{alias}.suv_pup = 'SUV/PUP'")

    elif cat == "LOWPROFILE":
       wh.append(f"{alias}.low_profile = 'LowProfileTBR'")

    elif cat == "HM":
        # HM by Sold-To (use your customer join for Ship_To ⇒ Sold_To; keep simple)
        # If your HM rule is customer-list based, prefer EXISTS against a keyed table.
        wh.append(f"{alias}.hm = 'HM'")

    return joins, wh

app = Flask(__name__, static_folder="static")
CORS(app, resources={r"/api/*": {"origins": "*"}})

def category_filters_stock(alias: str, category: str):
    joins, wh = [], []
    cat = (category or "ALL").upper()

    if cat == "ALL":
        return joins, wh

    elif cat == "PCLT":
        wh.append(f"{alias}.line = 'PCLT'")  # stock 테이블에 line 있으면 OK

    elif cat == "TBR":
        wh.append(f"{alias}.line = 'TBR'")

    elif cat == "ISEG":
        joins.append("JOIN iseg i ON CAST(TRIM(i.Material) AS UNSIGNED) = s.material")

    elif cat == "SUV":
        # stock에는 pattern이 없을 수 있으니 carrying_2602로부터 pattern 가져와야 함
        joins.append("JOIN carrying_2602 c ON c.m_code = s.material")
        joins.append("JOIN suv suv ON suv.Pattern = c.pattern")

    elif cat == "LOWPROFILE":
        joins.append("JOIN lowprofile lp ON CAST(TRIM(lp.Material) AS UNSIGNED) = s.material")

    return joins, wh
def category_filters_orders(category: str):
    """
    Orders용 카테고리 필터.
    returns: (joins, wh, needs_carrying)
    - orders alias: o
    - carrying alias: c
    """
    joins, wh = [], []
    cat = (category or "ALL").strip().upper()
    needs_carrying = False

    if cat == "ALL":
        return joins, wh, needs_carrying

    if cat == "PCLT":
        wh.append("(CAST(o.material AS CHAR) LIKE '1%' OR CAST(o.material AS CHAR) LIKE '2%')")

    elif cat == "TBR":
        wh.append("CAST(o.material AS CHAR) LIKE '3%'")

    elif cat == "ISEG":
        joins.append("JOIN iseg i ON CAST(TRIM(i.Material) AS UNSIGNED) = o.material")

    elif cat == "LOWPROFILE":
        joins.append("JOIN lowprofile lp ON CAST(TRIM(lp.Material) AS UNSIGNED) = o.material")

    elif cat == "SUV":
        needs_carrying = True
        joins.append("JOIN suv suv ON suv.Pattern = c.pattern")

    return joins, wh, needs_carrying
# ------------------------------- v2 helpers -------------------------------
def _region_order_key(x: str) -> int:
    order = ["NSW", "QLD", "VIC", "WA", "SA", "NT", "TAS", "ACT", "COMMON"]
    try:
        return order.index((x or "").upper())
    except ValueError:
        return 999

def _to_cumulative(arr: List[float]) -> List[float]:
    out, run = [], 0.0
    for v in arr:
        run += float(v or 0)
        out.append(run)
    return out

def _stacks_from_rows(rows: List[Dict[str, Any]], idx_key: str, idx_count: int, *, group_sort: str="alpha"):
    """
    rows: [{idx_key: int, group_label: str, value: num}, ...]
    returns: (groups, value_by_group)
      value_by_group[group] = [len idx_count]
    """
    groups = sorted({(r.get("group_label") or "").strip() for r in (rows or []) if (r.get("group_label") or "").strip()})
    if group_sort == "region":
        groups = sorted(groups, key=lambda g: (_region_order_key(g), g))

    by_group: Dict[str, List[float]] = {g: [0.0] * idx_count for g in groups}
    for r in (rows or []):
        g = (r.get("group_label") or "").strip()
        if not g:
            continue
        i = int(r.get(idx_key) or 0) - 1
        if 0 <= i < idx_count:
            by_group[g][i] += float(r.get("value") or 0)

    return groups, by_group

def _pct_by_bucket(by_group: Dict[str, List[float]]) -> Dict[str, List[float]]:
    if not by_group:
        return {}
    keys = list(by_group.keys())
    n = len(by_group[keys[0]])
    totals = [0.0] * n
    for g in keys:
        for i, v in enumerate(by_group[g]):
            totals[i] += float(v or 0)
    out: Dict[str, List[float]] = {}
    for g in keys:
        out[g] = [ (0.0 if totals[i] <= 0 else round((float(by_group[g][i]) / totals[i]) * 100, 4)) for i in range(n) ]
    return out

# NOTE: simple process-local cache. Good enough for your current single-process setup.
# If you run multiple workers, each worker has its own cache (still fine for speed).
_V2_CACHE_DASH: Dict[str, Tuple[float, Any]] = {}
_V2_CACHE_DIMS: Dict[str, Tuple[float, Any]] = {}
_TOP_SOLD_TO_CACHE: Dict[str, Tuple[float, Any]] = {}
_API_CACHE: Dict[str, Tuple[float, Any]] = {}   # shared cache for old v1 endpoints

# ---- Fixed Top list computed once at startup (Top 10/20/30) ----
# Key: (top_limit, value) where value is 'qty' or 'amt'
_GLOBAL_TOP_FIXED: Dict[Tuple[int, str], List[str]] = {}
_GLOBAL_TOP_READY = False


def _cache_get(cache: Dict[str, Tuple[float, Any]], key: str):
    now = time()
    hit = cache.get(key)
    if not hit:
        return None
    exp, val = hit
    if exp < now:
        try:
            del cache[key]
        except Exception:
            pass
        return None
    return val

def _cache_set(cache: Dict[str, Tuple[float, Any]], key: str, val: Any, ttl_sec: int):
    cache[key] = (time() + ttl_sec, val)

def _norm(v: str) -> str:
    return (v or "").strip()

def _make_v2_key(prefix: str, req) -> str:
    # stable key: path + sorted query args
    items = sorted((k, _norm(v)) for k, v in req.args.items())
    return prefix + "|" + "&".join([f"{k}={v}" for k, v in items])


def _make_top_key(f: Dict[str, str], top_limit: int, value: str) -> str:
    # Top-N baseline is expensive; cache it a bit longer than chart payloads.
    parts = [
        f"top_limit={top_limit}",
        f"value={value}",
        f"year={_norm(f.get('year'))}",
        f"month={_norm(f.get('month'))}",
        f"category={_norm(f.get('category'))}",
        f"region={_norm(f.get('region'))}",
        f"salesman={_norm(f.get('salesman'))}",
        f"sold_to_group={_norm(f.get('sold_to_group'))}",
        f"sold_to={_norm(f.get('sold_to'))}",
        f"ship_to={_norm(f.get('ship_to'))}",
        f"product_group={_norm(f.get('product_group'))}",
        f"pattern={_norm(f.get('pattern'))}",
        f"material={_norm(f.get('material'))}",
    ]
    return "top|" + "&".join(parts)

def get_connection():
    """Get a DB connection.
    - SQLite: file-based (Render demo)
    - MySQL: pooled connections to avoid expensive TCP/auth handshake per request
    """
    # If USE_SQLITE=1 (on Render), use the local snapshot.db file
    if USE_SQLITE:
        conn = sqlite3.connect(SQLITE_PATH)
        conn.row_factory = sqlite3.Row  # rows behave like dicts
        return SQLiteConnectionWrapper(conn)

    # Otherwise use MySQL (your current local/remote setup)
    global _MYSQL_POOL
    cfg = {
        "host": os.getenv("DB_HOST", "127.0.0.1"),
        "port": int(os.getenv("DB_PORT", "3306")),
        "user": os.getenv("DB_USER", "root"),
        "password": os.getenv("DB_PASS", ""),
        "database": os.getenv("DB_NAME", "my_new_database"),
        "autocommit": True,
        "use_pure": True,
    }

    # Lazily create pool once (thread-safe).
    # NOTE: pool_size should be >= (gunicorn workers * threads) in production.
    pool_size = int(os.getenv("DB_POOL_SIZE", "8"))
    pool_name = os.getenv("DB_POOL_NAME", "hka_pool")

    try:
        if _MYSQL_POOL is None:
            with _MYSQL_POOL_LOCK:
                if _MYSQL_POOL is None:
                    _MYSQL_POOL = pooling.MySQLConnectionPool(
                        pool_name=pool_name,
                        pool_size=pool_size,
                        pool_reset_session=True,
                        **cfg,
                    )
        return _MYSQL_POOL.get_connection()
    except mysql.connector.Error as e:
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

@app.route("/stock")
def stock_page():
    return app.send_static_file("stock.html")

# ── Price comparison dashboard ─────────────────────────────────────────────────
from price_compare import price_dashboard
app.add_url_rule('/price', 'price_dashboard', price_dashboard)

# plant 좌표 매핑 (너가 줄 값으로 업데이트)
PLANT_GEO = {
    "42R0": {"lat": -27.8688, "lon": 153.2093},
    "42R1": {"lat": -33.86,   "lon": 150.20},
    "42R2": {"lat": -37.85,   "lon": 144.21},
    "42R4": {"lat": -31.83,   "lon": 116.23},
}
# category -> mapping table
CATEGORY_TABLE = {
    "ISEG": "iseg",
    "SUV": "suv",
    "LOWPROFILE": "lowprofile",
    # ALL/PCLT/TBR는 아래에서 별도 처리
}
@app.get("/api/stock")
def api_stock():
    # optional query params
    # ?metric=qty|unrestricted (기본 qty)
    # ?plants=42R0,42R1 (기본 42R0~42R4)
    # inputs
    category = (request.args.get("category") or "ALL").strip().upper()
    prod_group = (request.args.get("product_group") or "ALL").strip()
    pattern = (request.args.get("pattern") or "").strip()
    material = (request.args.get("material") or "").strip()
    metric_col = "unrestricted"

    plants_param = (request.args.get("plants") or "").strip()
    if plants_param:
        plants = [p.strip().upper() for p in plants_param.split(",") if p.strip()]
    else:
        plants = ["42R0", "42R1", "42R2", "42R4"]

    # 좌표 없는 plant는 제외
    plants = [p for p in plants if p in PLANT_GEO]

    if not plants:
        return jsonify({"rows": [], "meta": {"metric": metric_col}})

    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    try:
        joins = []
        wh = []
        params = []
        sql = f"""
            SELECT s.plant, SUM(s.{metric_col}) AS stock_value
            FROM stock s
        """

        # carrying join (for prod_group/pattern filters, and for PCLT/TBR if you implement via product_group)
        joins.append("JOIN carrying_2602 c ON c.m_code = s.material")

        # plant filter
        wh.append(f"s.plant IN ({','.join(['%s']*len(plants))})")
        params += plants

        # product_group dropdown
        if prod_group and prod_group != "ALL":
            wh.append("c.product_group = %s")
            params.append(prod_group)

        # pattern search
        if pattern:
            wh.append("c.pattern LIKE %s")
            params.append(f"%{pattern}%")

        # material search
        if material:
            # allow partial
            wh.append("c.m_desc LIKE %s")
            params.append(f"%{material}%")

        # category chip handling
        if category in CATEGORY_TABLE:
            t = CATEGORY_TABLE[category]
            joins.append(f"JOIN {t} cat ON cat.Material = s.material")  # column name 맞춰서 Material/m_code로 변경
        elif category == "PCLT":
            # 예시: carrying_2602.product_group 기준 (너 데이터에 맞게 조정)
            # wh.append("c.some_segment = 'PCLT'")
            pass
        elif category == "TBR":
            # 예시: carrying_2602.product_group 기준 (너 데이터에 맞게 조정)
            # wh.append("c.some_segment = 'TBR'")
            pass
        else:
            # ALL: no extra filter
            pass

        sql += "\n" + "\n".join(joins)
        if wh:
            sql += "\nWHERE " + "\n  AND ".join(wh)
        sql += "\nGROUP BY s.plant"

        cur.execute(sql, params)
        rows = cur.fetchall() or []
        rows_out = []
        for r in rows:
            p = r.get("plant")
            g = PLANT_GEO.get(p)
            if not g:
                continue
            rows_out.append({
                "plant": p,
                "stock_value": float(r.get("stock_value") or 0),
                "lat": g["lat"],
                "lon": g["lon"],
            })
        return jsonify({
            "rows": rows_out,
            "meta": {
                "metric": "unrestricted",
                "category": category,
                "product_group": prod_group,
                "pattern": pattern,
                "material": material,
                "plants": plants
            }
        })
    finally:
        cur.close()
        conn.close()
ORIGIN_GEO = {
    "CHN": {"name": "China", "lat": 33.8617, "lon": 104.1954},
    "KOR": {"name": "Korea", "lat": 33.5, "lon": 127.8},
    "HUN": {"name": "Hungary", "lat": 33.1625, "lon": 80.5033},
    "IDN": {"name": "Indonesia", "lat": -2.5489, "lon": 118.0149},
}        
@app.get("/api/orders")
def api_orders():
    # filters (same UI)
    category = (request.args.get("category") or "ALL").strip().upper()
    prod_group = (request.args.get("product_group") or "ALL").strip()
    pattern = (request.args.get("pattern") or "").strip()
    material = (request.args.get("material") or "").strip()

    # metric: po | confirm  (default po)
    metric = (request.args.get("metric") or "po").strip().lower()
    metric_col = "po_qty" if metric != "confirm" else "confirm_qty"

    # optional: plant filter if you want
    plants_param = (request.args.get("plants") or "").strip()
    plants = [p.strip().upper() for p in plants_param.split(",") if p.strip()] if plants_param else []

    # origin filter optional
    origins_param = (request.args.get("origins") or "").strip()
    origins = [x.strip().upper() for x in origins_param.split(",") if x.strip()] if origins_param else list(ORIGIN_GEO.keys())

    # keep only origins we can plot
    origins = [o for o in origins if o in ORIGIN_GEO]
    if not origins:
        return jsonify({"rows": [], "meta": {"metric": metric_col}})

    cat_joins, cat_wh, cat_needs_carrying = category_filters_orders(category)

    # carrying only when needed
    needs_carrying = cat_needs_carrying or (prod_group and prod_group != "ALL") or bool(pattern) or bool(material)

    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        joins = []
        wh = []
        params = []

        sql = f"""
            SELECT o.origin, SUM(o.{metric_col}) AS order_value
            FROM orders o
        """

        if needs_carrying:
            joins.append("JOIN carrying_2602 c ON c.m_code = o.material")

        joins += cat_joins

        # origin filter
        wh.append(f"o.origin IN ({','.join(['%s'] * len(origins))})")
        params += origins

        # optional plant filter
        if plants:
            wh.append(f"o.plant IN ({','.join(['%s'] * len(plants))})")
            params += plants

        # material filter
        # prod_group / pattern / material filters
        if needs_carrying:
            if material:
                wh.append("c.m_desc LIKE %s")
                params.append(f"%{material}%")
            if prod_group and prod_group != "ALL":
                wh.append("c.product_group = %s")
                params.append(prod_group)
            if pattern:
                wh.append("c.pattern LIKE %s")
                params.append(f"%{pattern}%")

        wh += cat_wh

        sql += "\n" + "\n".join(joins)
        if wh:
            sql += "\nWHERE " + "\n  AND ".join(wh)
        sql += "\nGROUP BY o.origin"

        cur.execute(sql, params)
        rows = cur.fetchall() or []

        by_origin = {r["origin"]: float(r.get("order_value") or 0) for r in rows}

        rows_out = []
        for og in origins:
            g = ORIGIN_GEO[og]
            rows_out.append({
                "origin": og,
                "origin_name": g.get("name", og),
                "order_value": by_origin.get(og, 0.0),
                "lat": g["lat"],
                "lon": g["lon"],
            })

        return jsonify({
            "rows": rows_out,
            "meta": {
                "metric": metric_col,
                "category": category,
                "product_group": prod_group,
                "pattern": pattern,
                "material": material,
                "origins": origins,
                "plants": plants,
                "needs_carrying": needs_carrying
            }
        })
    finally:
        cur.close()
        conn.close()
# origin name/code normalize
ORIGIN_CODE_MAP = {
    "CHN": "CHN", "CHINA": "CHN",
    "KOR": "KOR", "KOREA": "KOR",
    "HUN": "HUN", "HUNGARY": "HUN",
    "IDN": "IDN", "INDONESIA": "IDN",
}

def normalize_origin_code(x: str) -> str:
    s = (x or "").strip().upper()
    return ORIGIN_CODE_MAP.get(s, s)

def lerp(a: float, b: float, t: float) -> float:
    return a + (b - a) * t

from datetime import date, datetime

ETA_WINDOW_DAYS = 60  # 60일을 이동 구간으로 가정 (원하면 30/90으로)

def parse_date_ymd(x):
    if x is None:
        return None
    if isinstance(x, (date, datetime)):
        return x.date() if isinstance(x, datetime) else x
    s = str(x).strip()
    if not s or s.startswith("0000-00-00"):
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except:
        return None

def clamp01(v):
    return 0.0 if v < 0 else (1.0 if v > 1 else v)

def lerp(a, b, t):
    return a + (b - a) * t

@app.get("/api/incoming")
def api_incoming():
    category = (request.args.get("category") or "ALL").strip().upper()
    prod_group = (request.args.get("product_group") or "ALL").strip()
    pattern = (request.args.get("pattern") or "").strip()
    material = (request.args.get("material") or "").strip()

    metric = (request.args.get("metric") or "po").strip().lower()
    metric_col = "po_qty" if metric != "confirm" else "confirm_qty"

    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        joins = []
        wh = []
        params = []

        cat_joins, cat_wh, cat_needs_carrying = category_filters_orders(category)
        needs_carrying = cat_needs_carrying or (prod_group and prod_group != "ALL") or bool(pattern) or bool(material)

        # ✅ ETA별 위치를 바꾸려면 eta_date를 그룹에 포함해야 해서
        # origin/plant/eta_date 단위로 집계(같은 ETA끼리 묶임)
        sql = f"""
            SELECT o.plant, o.origin, o.eta_date, SUM(o.{metric_col}) AS incoming_value
            FROM incoming o
        """

        if needs_carrying:
            joins.append("JOIN carrying_2602 c ON c.m_code = o.material")

        joins += cat_joins

        if material:
            wh.append("c.m_desc LIKE %s")
            params.append(f"%{material}%")

        if needs_carrying:
            if prod_group and prod_group != "ALL":
                wh.append("c.product_group = %s")
                params.append(prod_group)
            if pattern:
                wh.append("c.pattern LIKE %s")
                params.append(f"%{pattern}%")

        wh += cat_wh

        sql += "\n" + "\n".join(joins)
        if wh:
            sql += "\nWHERE " + "\n  AND ".join(wh)
        sql += "\nGROUP BY o.plant, o.origin, o.eta_date"

        cur.execute(sql, params)
        rows = cur.fetchall() or []

        today = date.today()
        out = []

        for r in rows:
            plant = (r.get("plant") or "").strip().upper()
            origin_code = normalize_origin_code(r.get("origin"))

            gO = ORIGIN_GEO.get(origin_code)
            gP = PLANT_GEO.get(plant)
            if not gO or not gP:
                continue

            eta = parse_date_ymd(r.get("eta_date"))
            if eta:
                days_to_eta = (eta - today).days
                # progress 0(origin) -> 1(plant)
                progress = clamp01(1.0 - (days_to_eta / float(ETA_WINDOW_DAYS)))
            else:
                # ETA가 없으면 중간쯤(기존처럼)
                progress = 0.75

            lat = lerp(gO["lat"], gP["lat"], progress)
            lon = lerp(gO["lon"], gP["lon"], progress)

            out.append({
                "plant": plant,
                "origin": origin_code,
                "origin_name": gO.get("name", origin_code),
                "eta_date": eta.isoformat() if eta else None,
                "progress": progress,
                "incoming_value": float(r.get("incoming_value") or 0),
                "lat": lat,
                "lon": lon,
                # polyline points
                "line": [
                    {"lat": gO["lat"], "lon": gO["lon"]},
                    {"lat": lat, "lon": lon},
                    {"lat": gP["lat"], "lon": gP["lon"]},
                ]
            })

        return jsonify({
            "rows": out,
            "meta": {
                "metric": metric_col,
                "category": category,
                "product_group": prod_group,
                "pattern": pattern,
                "material": material,
                "eta_window_days": ETA_WINDOW_DAYS
            }
        })
    finally:
        cur.close()
        conn.close()       
def build_global_top_once():
    """Compute Top 10/20/30 once at server startup (no filters).
    This avoids expensive GROUP BY / ORDER BY SUM on every request.
    """
    global _GLOBAL_TOP_READY
    if _GLOBAL_TOP_READY:
        return
    print("[INIT] Building fixed Top 10/20/30 once...")

    conn = get_connection()
    if not conn:
        print("[INIT] DB not ready; skipping fixed Top build")
        return

    cur = conn.cursor(dictionary=True)
    try:
        for val in ("qty", "amt"):
            for limit in (10, 20, 30):
                sql = f"""
                    SELECT sold_to
                    FROM sales_25_2602
                    GROUP BY sold_to
                    ORDER BY SUM({val}) DESC
                    LIMIT {limit}
                """
                cur.execute(sql)
                rows = cur.fetchall() or []
                _GLOBAL_TOP_FIXED[(int(limit), str(val))] = [r.get("sold_to") for r in rows if r.get("sold_to") is not None]
        _GLOBAL_TOP_READY = True
        print("[INIT] Fixed Top ready:", {k: len(v) for k, v in _GLOBAL_TOP_FIXED.items()})
    except Exception as e:
        print("[INIT] Fixed Top build failed:", e)
    finally:
        try:
            cur.close(); conn.close()
        except Exception:
            pass

def get_top_sold_to_from_baseline(cur, f, top_limit, value):
    """Get Top N sold_to based on sales_25_2602 (baseline).
    This is a hot path when the UI uses top_limit; cache it briefly.
    """
    # Fast path: fixed Top 10/20/30 computed once at startup (no filters).
    # This completely removes baseline Top-N queries from the request path.
    try:
        tl = int(top_limit or 0)
    except Exception:
        tl = 0
    val_key = str(value or "qty")
    fixed = _GLOBAL_TOP_FIXED.get((tl, val_key))
    if fixed is not None:
        return fixed

    if not top_limit or top_limit <= 0:
        return None

    key = _make_top_key(f, int(top_limit), str(value))
    cached = _cache_get(_TOP_SOLD_TO_CACHE, key)
    if cached is not None:
        return cached

    joins, wh, params = build_customer_filters("sTop", f, use_sold_to_name=False)

    # category filter (same rule as sales)
    if f.get("category") != "443":
        cat_joins, cat_where = category_filters("sTop", f.get("category"))
        joins += cat_joins
        wh    += cat_where

    if f.get("product_group") != "ALL":
        wh.append("sTop.product_group = %s"); params.append(f["product_group"])
    if f.get("pattern") != "ALL":
        wh.append("sTop.pattern = %s"); params.append(f["pattern"])
    if f.get("material") != "ALL":
        wh.append("sTop.material = %s"); params.append(f["material"])

    where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

    sql = f"""
      SELECT sTop.sold_to
        FROM sales_25_2602 sTop
        {' '.join(joins)}
        {where_sql}
       GROUP BY sTop.sold_to
       ORDER BY SUM(sTop.{value}) DESC
       LIMIT %s
    """
    cur.execute(sql, tuple(params) + (int(top_limit),))
    rows = cur.fetchall()
    out = [r["sold_to"] for r in rows]

    # cache 60s (baseline doesn't change intra-minute for dashboards)
    _cache_set(_TOP_SOLD_TO_CACHE, key, out, ttl_sec=int(os.getenv("TOP_CACHE_TTL", "60")))
    return out


# ---------------------------------- v2: consolidated APIs ----------------------------------

@app.get("/api/v2/dimensions")
def v2_dimensions():
    """
    Consolidated lookups for the UI.
    Returns: sold_to_groups, sold_to_names, ship_to_names, product_groups, patterns, materials.
    Uses the same filtering logic as existing v1 endpoints.
    """
    key = _make_v2_key("dims", request)
    cached = _cache_get(_V2_CACHE_DIMS, key)
    if cached is not None:
        return jsonify(cached)

    f = parse_filters(request)
    value = "qty" if f.get("metric") == "qty" else "amt"
    top_limit = int(request.args.get("top_limit", 0) or 0)
    parent_group = (request.args.get("sold_to_group") or f.get("sold_to_group") or "ALL").strip()
    sold_to_name = (request.args.get("sold_to") or f.get("sold_to") or "ALL").strip()
    pg = (request.args.get("product_group") or f.get("product_group") or "ALL").strip()
    pat = (request.args.get("pattern") or f.get("pattern") or "ALL").strip()

    conn = get_connection(); cur = conn.cursor(dictionary=True)
    try:
        # Sold-to groups
        cur.execute("""
            SELECT DISTINCT TRIM(sold_to_group) AS v
            FROM customer
            WHERE sold_to_group IS NOT NULL AND TRIM(sold_to_group) <> ''
            ORDER BY TRIM(sold_to_group)
        """)
        sold_to_groups = [r["v"] for r in cur.fetchall()]

        # Sold-to names (optionally restricted by top_limit baseline)
        if top_limit <= 0:
            if parent_group != "ALL":
                cur.execute("""
                    SELECT DISTINCT TRIM(sold_to_name) AS v
                    FROM customer
                    WHERE sold_to_group = %s
                      AND sold_to_name IS NOT NULL AND TRIM(sold_to_name) <> ''
                    ORDER BY TRIM(sold_to_name)
                """, (parent_group,))
            else:
                cur.execute("""
                    SELECT DISTINCT TRIM(sold_to_name) AS v
                    FROM customer
                    WHERE sold_to_name IS NOT NULL AND TRIM(sold_to_name) <> ''
                    ORDER BY TRIM(sold_to_name)
                """)
            sold_to_names = [r["v"] for r in cur.fetchall()]
        else:
            top_sold_to = get_top_sold_to_from_baseline(cur, f, top_limit, value) or []
            if not top_sold_to:
                sold_to_names = []
            else:
                placeholders = ",".join(["%s"] * len(top_sold_to))
                params = list(top_sold_to)
                extra = ""
                if parent_group != "ALL":
                    extra = " AND c.sold_to_group = %s "
                    params.append(parent_group)
                cur.execute(f"""
                    SELECT DISTINCT TRIM(c.sold_to_name) AS v
                    FROM customer c
                    WHERE c.sold_to IN ({placeholders})
                      {extra}
                      AND c.sold_to_name IS NOT NULL AND TRIM(c.sold_to_name) <> ''
                    ORDER BY TRIM(c.sold_to_name)
                """, tuple(params))
                sold_to_names = [r["v"] for r in cur.fetchall()]

        # Ship-to names under selected sold-to or group
        where = ["ship_to_name IS NOT NULL", "TRIM(ship_to_name) <> ''"]
        params = []
        if sold_to_name.upper() != "ALL":
            where.append("TRIM(sold_to_name) = %s")
            params.append(sold_to_name)
        elif parent_group.upper() != "ALL":
            where.append("TRIM(sold_to_group) = %s")
            params.append(parent_group)
        where_sql = "WHERE " + " AND ".join(where)
        cur.execute(f"""
            SELECT DISTINCT TRIM(ship_to_name) AS v
            FROM customer
            {where_sql}
            ORDER BY TRIM(ship_to_name)
        """, tuple(params))
        ship_to_names = [r["v"] for r in cur.fetchall()]

        # Product groups
        cur.execute("SELECT DISTINCT TRIM(product_group) AS v FROM sales_2025 ORDER BY TRIM(product_group)")
        product_groups = [r["v"] for r in cur.fetchall() if r.get("v") is not None]

        # Patterns (filtered by product_group)
        if pg and pg != "ALL":
            cur.execute("""
                SELECT DISTINCT TRIM(pattern) AS v
                FROM sales_2025
                WHERE product_group = %s
                ORDER BY TRIM(pattern)
            """, (pg,))
        else:
            cur.execute("""
                SELECT DISTINCT TRIM(pattern) AS v
                FROM sales_2025
                ORDER BY TRIM(pattern)
            """)
        patterns = [r["v"] for r in cur.fetchall() if r.get("v") is not None]

        # Materials (filtered by product_group/pattern)
        w2, p2 = [], []
        if pg and pg != "ALL":
            w2.append("product_group = %s"); p2.append(pg)
        if pat and pat != "ALL":
            w2.append("pattern = %s"); p2.append(pat)
        w2_sql = ("WHERE " + " AND ".join(w2)) if w2 else ""
        cur.execute(f"""
            SELECT DISTINCT m_desc AS v
            FROM carrying_2602
            {w2_sql}
            ORDER BY m_desc
        """, tuple(p2))
        materials = [r["v"] for r in cur.fetchall() if r.get("v") is not None]

        payload = {
            "sold_to_groups": sold_to_groups,
            "sold_to_names": sold_to_names,
            "ship_to_names": ship_to_names,
            "product_groups": product_groups,
            "patterns": patterns,
            "materials": materials,
        }

        _cache_set(_V2_CACHE_DIMS, key, payload, ttl_sec=120)
        return jsonify(payload)

    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            cur.close(); conn.close()
        except Exception:
            pass


@app.get("/api/v2/dashboard")
def v2_dashboard():
    """
    Consolidated data endpoint to replace multiple v1 chart APIs.
    It returns daily/monthly/yearly totals + stacked breakdowns + cumulative + percent,
    using the same filter semantics as the existing v1 endpoints.
    """
    key = _make_v2_key("dash", request)
    cached = _cache_get(_V2_CACHE_DASH, key)
    if cached is not None:
        return jsonify(cached)

    f = parse_filters(request)
    value = "qty" if f.get("metric") == "qty" else "amt"
    top_limit = int(request.args.get("top_limit", 0) or 0)
    group_by = (request.args.get("group_by") or "region").strip()
    month = int(request.args.get("month", 1) or 1)  # daily page: month of 2026


    # Optional: limit work to only what the frontend needs.
    # sections=all (default) or comma list:
    #   daily,daily_stacked,monthly,monthly_stacked,monthly_target_stacked,yearly,yearly_stacked
    sections_raw = (request.args.get("sections") or "all").strip().lower()
    sections = {s.strip() for s in sections_raw.split(",") if s.strip()}
    if (not sections) or ("all" in sections):
        sections = {
            "daily",
            "daily_stacked",
            "monthly",
            "monthly_stacked",
            "monthly_target_stacked",
            "yearly",
            "yearly_stacked",
        }

    # group columns
    group_cols_sales = {
        "product_group": "s.product_group",
        "region":        "cus.bde_state",
        "salesman":      "cus.salesman_name",
        "sold_to_group": "cus.sold_to_group",
        "sold_to":       "cus.sold_to_name",
        "pattern":       "s.pattern",
    }
    group_cols_target = {
        "product_group": "t.product_group",
        "region":        "cus.bde_state",
        "salesman":      "cus.salesman_name",
        "sold_to_group": "cus.sold_to_group",
        "sold_to":       "cus.sold_to_name",
        "pattern":       "t.pattern",
    }
    if group_by not in group_cols_sales:
        return jsonify({"error": "invalid group_by"}), 400

    import calendar
    days_in_month = calendar.monthrange(2026, month)[1]
    daily_labels = list(range(1, days_in_month + 1))
    monthly_labels = list(range(1, 13))
    yearly_labels = list(range(2021, 2026))

    conn = get_connection(); cur = conn.cursor(dictionary=True)
    t0 = time()
    try:
        # ---------------- top-limit (baseline) ----------------
        top_sold_to = None
        if top_limit > 0:
            top_sold_to = get_top_sold_to_from_baseline(cur, f, top_limit, value) or []
            if not top_sold_to:
                # Nothing matched filters. Return empty but well-shaped payload.
                empty_daily = [0.0] * days_in_month
                empty_month = [0.0] * 12
                empty_year = [0.0] * len(yearly_labels)
                payload = {
                    "meta": {"filters": f, "group_by": group_by, "month": month, "top_limit": top_limit, "timing_ms": {"total": int((time()-t0)*1000)}},
                    "daily": {"labels": daily_labels, "total": empty_daily, "target": empty_daily, "cum_total": empty_daily, "cum_target": empty_daily, "achievement": [None]*days_in_month, "cum_achievement": [None]*days_in_month},
                    "daily_stacked": {"labels": daily_labels, "groups": [], "value": {}, "cum": {}, "pct": {}, "pct_cum": {}},
                    "monthly": {"labels": monthly_labels, "sales_2025": empty_month, "sales_2026": empty_month, "target_2026": empty_month, "cum_sales_2025": empty_month, "cum_sales_2026": empty_month, "cum_target_2026": empty_month, "ach_2026": [None]*12, "ach_cum_2026": [None]*12},
                    "monthly_stacked": {"labels": monthly_labels, "groups": [], "value_2025": {}, "value_2026": {}, "cum_2025": {}, "cum_2026": {}, "pct_2025": {}, "pct_2026": {}, "pct_cum_2025": {}, "pct_cum_2026": {}},
                    "monthly_target_stacked": {"labels": monthly_labels, "groups": [], "value": {}, "cum": {}, "pct": {}, "pct_cum": {}},
                    "yearly": {"labels": yearly_labels, "total": empty_year},
                    "yearly_stacked": {"labels": yearly_labels, "groups": [], "value": {}, "pct": {}},
                }
                _cache_set(_V2_CACHE_DASH, key, payload, ttl_sec=int(os.getenv("DASH_CACHE_TTL", "30")))
                return jsonify(payload)

        # ---------------- daily (sales_thismonth + target_26 month) ----------------
        joins_d, wh_d, params_d = build_customer_filters("s", f, use_sold_to_name=False)
        if f["category"] != "443":
            cj, cw = category_filters("s", f["category"])
            joins_d += cj; wh_d += cw
        if f["product_group"] != "ALL":
            wh_d.append("s.product_group = %s"); params_d.append(f["product_group"])
        if f["pattern"] != "ALL":
            wh_d.append("s.pattern = %s"); params_d.append(f["pattern"])
        if f["material"] != "ALL":
            wh_d.append("s.material = %s"); params_d.append(f["material"])
        if top_sold_to:
            placeholders = ",".join(["%s"] * len(top_sold_to))
            wh_d.append(f"s.sold_to IN ({placeholders})")
            params_d.extend(top_sold_to)
        where_d = ("WHERE " + " AND ".join(wh_d)) if wh_d else ""

        cur.execute(f"""
            SELECT s.day AS k, SUM(s.{value}) AS v
            FROM sales_thismonth s
            {' '.join(joins_d)}
            {where_d}
            GROUP BY s.day
            ORDER BY s.day
        """, tuple(params_d))
        daily_rows = cur.fetchall()
        daily_map = {int(r["k"]): float(r["v"] or 0) for r in daily_rows}
        daily_total = [daily_map.get(d, 0.0) for d in daily_labels]
        daily_cum = _to_cumulative(daily_total)

        # daily breakdown stacks
        group_col_d = group_cols_sales[group_by]
        cur.execute(f"""
            SELECT s.day AS day, {group_col_d} AS group_label, SUM(s.{value}) AS value
            FROM sales_thismonth s
            {' '.join(joins_d)}
            {where_d}
            GROUP BY s.day, {group_col_d}
            ORDER BY s.day
        """, tuple(params_d))
        d_break = cur.fetchall()
        d_groups, d_by = _stacks_from_rows(d_break, "day", days_in_month, group_sort=("region" if group_by=="region" else "alpha"))
        d_cum_by = {g: _to_cumulative(d_by[g]) for g in d_groups}
        d_pct_by = _pct_by_bucket(d_by)
        d_pct_cum_by = _pct_by_bucket(d_cum_by)

        # daily target for selected month (target_26)
        joins_t, wh_t, params_t = build_customer_filters("t", f, use_sold_to_name=False)
        tj, tw = category_target_filters("t", f["category"])
        joins_t += tj; wh_t += tw
        wh_t.append("t.month = %s"); params_t.append(month)
        if f["product_group"] != "ALL":
            wh_t.append("t.product_group = %s"); params_t.append(f["product_group"])
        if f["pattern"] != "ALL":
            wh_t.append("t.pattern = %s"); params_t.append(f["pattern"])
        if f["material"] != "ALL":
            wh_t.append("t.material = %s"); params_t.append(f["material"])
        if top_sold_to:
            placeholders = ",".join(["%s"] * len(top_sold_to))
            wh_t.append(f"t.sold_to IN ({placeholders})")
            params_t.extend(top_sold_to)
        where_t = ("WHERE " + " AND ".join(wh_t)) if wh_t else ""
        cur.execute(f"""
            SELECT SUM(t.{value}) AS monthly_total
            FROM target_26 t
            {' '.join(joins_t)}
            {where_t}
        """, tuple(params_t))
        row = cur.fetchone() or {}
        monthly_total_target = float(row.get("monthly_total") or 0)
        daily_target_val = (monthly_total_target / days_in_month) if days_in_month else 0.0
        daily_target = [daily_target_val for _ in daily_labels]
        daily_target_cum = _to_cumulative(daily_target)

        daily_ach = [None if daily_target[i] <= 0 else round((daily_total[i]/daily_target[i])*100.0, 2) for i in range(days_in_month)]
        daily_cum_ach = [None if daily_target_cum[i] <= 0 else round((daily_cum[i]/daily_target_cum[i])*100.0, 2) for i in range(days_in_month)]

        # ---------------- monthly (sales_25_2602 years 2025/2026 + target_26) ----------------
        joins_m, wh_m, params_m = build_customer_filters("s", f, use_sold_to_name=False)
        mj, mw = category_filters("s", f["category"])
        joins_m += mj; wh_m += mw
        if f["product_group"] != "ALL":
            wh_m.append("s.product_group = %s"); params_m.append(f["product_group"])
        if f["pattern"] != "ALL":
            wh_m.append("s.pattern = %s"); params_m.append(f["pattern"])
        if f["material"] != "ALL":
            wh_m.append("s.material = %s"); params_m.append(f["material"])
        wh_m.append("s.year IN (2025, 2026)")
        if top_sold_to:
            placeholders = ",".join(["%s"] * len(top_sold_to))
            wh_m.append(f"s.sold_to IN ({placeholders})")
            params_m.extend(top_sold_to)
        where_m = ("WHERE " + " AND ".join(wh_m)) if wh_m else ""

        cur.execute(f"""
            SELECT s.year AS year, s.month AS month, SUM(s.{value}) AS value
            FROM sales_25_2602 s
            {' '.join(joins_m)}
            {where_m}
            GROUP BY s.year, s.month
            ORDER BY s.year, s.month
        """, tuple(params_m))
        m_tot_rows = cur.fetchall()
        m25 = {int(r["month"]): float(r["value"] or 0) for r in m_tot_rows if int(r["year"]) == 2025}
        m26 = {int(r["month"]): float(r["value"] or 0) for r in m_tot_rows if int(r["year"]) == 2026}
        monthly_25 = [m25.get(m, 0.0) for m in monthly_labels]
        monthly_26 = [m26.get(m, 0.0) for m in monthly_labels]
        cum_25 = _to_cumulative(monthly_25)
        cum_26 = _to_cumulative(monthly_26)

        # monthly breakdown stacks (both years)
        group_col_m = group_cols_sales[group_by]
        cur.execute(f"""
            SELECT s.year AS year, s.month AS month, {group_col_m} AS group_label, SUM(s.{value}) AS value
            FROM sales_25_2602 s
            {' '.join(joins_m)}
            {where_m}
            GROUP BY s.year, s.month, {group_col_m}
            ORDER BY s.year, s.month
        """, tuple(params_m))
        m_break = cur.fetchall()

        m_break_25 = [r for r in m_break if int(r.get("year") or 0) == 2025]
        m_break_26 = [r for r in m_break if int(r.get("year") or 0) == 2026]
        m_groups_25, m_by_25 = _stacks_from_rows(m_break_25, "month", 12, group_sort=("region" if group_by=="region" else "alpha"))
        m_groups_26, m_by_26 = _stacks_from_rows(m_break_26, "month", 12, group_sort=("region" if group_by=="region" else "alpha"))
        # unified group list (so chart legend is stable)
        groups_union = _stacks_from_rows(m_break, "month", 12, group_sort=("region" if group_by=="region" else "alpha"))[0]
        # ensure missing groups are present as zeros
        for g in groups_union:
            m_by_25.setdefault(g, [0.0]*12)
            m_by_26.setdefault(g, [0.0]*12)
        m_cum_25 = {g: _to_cumulative(m_by_25[g]) for g in groups_union}
        m_cum_26 = {g: _to_cumulative(m_by_26[g]) for g in groups_union}
        m_pct_25 = _pct_by_bucket(m_by_25)
        m_pct_26 = _pct_by_bucket(m_by_26)
        m_pct_cum_25 = _pct_by_bucket(m_cum_25)
        m_pct_cum_26 = _pct_by_bucket(m_cum_26)

        # monthly target totals & breakdown (target_26)
        # Build fresh for all months (don't reuse params_t because it includes a specific month)
        joins_mt, wh_mt, params_mt = build_customer_filters("t", f, use_sold_to_name=False)
        tj2, tw2 = category_target_filters("t", f["category"])
        joins_mt += tj2; wh_mt += tw2
        if f["product_group"] != "ALL":
            wh_mt.append("t.product_group = %s"); params_mt.append(f["product_group"])
        if f["pattern"] != "ALL":
            wh_mt.append("t.pattern = %s"); params_mt.append(f["pattern"])
        if f["material"] != "ALL":
            wh_mt.append("t.material = %s"); params_mt.append(f["material"])
        if top_sold_to:
            placeholders = ",".join(["%s"] * len(top_sold_to))
            wh_mt.append(f"t.sold_to IN ({placeholders})")
            params_mt.extend(top_sold_to)
        where_mt = ("WHERE " + " AND ".join(wh_mt)) if wh_mt else ""
        cur.execute(f"""
            SELECT t.month AS month, SUM(t.{value}) AS value
            FROM target_26 t
            {' '.join(joins_mt)}
            {where_mt}
            GROUP BY t.month
            ORDER BY t.month
        """, tuple(params_mt))
        mt_rows = cur.fetchall()
        mt_map = {int(r["month"]): float(r["value"] or 0) for r in mt_rows}
        target_26 = [mt_map.get(m, 0.0) for m in monthly_labels]
        cum_target_26 = _to_cumulative(target_26)

        ach_26 = [None if target_26[i] <= 0 else round((monthly_26[i]/target_26[i])*100.0, 2) for i in range(12)]
        ach_cum_26 = [None if cum_target_26[i] <= 0 else round((cum_26[i]/cum_target_26[i])*100.0, 2) for i in range(12)]

        # target breakdown stacks
        group_col_mt = group_cols_target[group_by]
        cur.execute(f"""
            SELECT t.month AS month, {group_col_mt} AS group_label, SUM(t.{value}) AS value
            FROM target_26 t
            {' '.join(joins_mt)}
            {where_mt}
            GROUP BY t.month, {group_col_mt}
            ORDER BY t.month
        """, tuple(params_mt))
        mt_break = cur.fetchall()
        mt_groups, mt_by = _stacks_from_rows(mt_break, "month", 12, group_sort=("region" if group_by=="region" else "alpha"))
        mt_cum_by = {g: _to_cumulative(mt_by[g]) for g in mt_groups}
        mt_pct_by = _pct_by_bucket(mt_by)
        mt_pct_cum_by = _pct_by_bucket(mt_cum_by)

        # ---------------- yearly (sales_21_25) ----------------
        joins_y, wh_y, params_y = build_customer_filters("s", f, use_sold_to_name=False)
        if f["category"] != "443":
            yj, yw = category_filters("s", f["category"])
            joins_y += yj; wh_y += yw
        if f["product_group"] != "ALL":
            wh_y.append("s.product_group = %s"); params_y.append(f["product_group"])
        if f["pattern"] != "ALL":
            wh_y.append("s.pattern = %s"); params_y.append(f["pattern"])
        if f["material"] != "ALL":
            wh_y.append("s.material = %s"); params_y.append(f["material"])
        if top_sold_to:
            placeholders = ",".join(["%s"] * len(top_sold_to))
            wh_y.append(f"s.sold_to IN ({placeholders})")
            params_y.extend(top_sold_to)
        wh_y.append("s.year BETWEEN 2021 AND 2025")
        where_y = ("WHERE " + " AND ".join(wh_y)) if wh_y else ""
        cur.execute(f"""
            SELECT s.year AS year, SUM(s.{value}) AS value
            FROM sales_21_25 s
            {' '.join(joins_y)}
            {where_y}
            GROUP BY s.year
            ORDER BY s.year
        """, tuple(params_y))
        y_rows = cur.fetchall()
        y_map = {int(r["year"]): float(r["value"] or 0) for r in y_rows}
        y_total = [y_map.get(y, 0.0) for y in yearly_labels]

        group_col_y = group_cols_sales[group_by]
        cur.execute(f"""
            SELECT s.year AS year, {group_col_y} AS group_label, SUM(s.{value}) AS value
            FROM sales_21_25 s
            {' '.join(joins_y)}
            {where_y}
            GROUP BY s.year, {group_col_y}
            ORDER BY s.year
        """, tuple(params_y))
        y_break = cur.fetchall()
        # NOTE: years are not 1..N indexes; map manually
        y_groups = sorted({(r.get("group_label") or "").strip() for r in y_break if (r.get("group_label") or "").strip()}, key=lambda g: (_region_order_key(g), g) if group_by=="region" else (g.upper(), g))
        y_by2: Dict[str, List[float]] = {g: [0.0]*len(yearly_labels) for g in y_groups}
        year_idx = {y:i for i,y in enumerate(yearly_labels)}
        for r in y_break:
            g = (r.get("group_label") or "").strip()
            if not g:
                continue
            yy = int(r.get("year") or 0)
            if yy not in year_idx:
                continue
            y_by2[g][year_idx[yy]] += float(r.get("value") or 0)
        y_pct = _pct_by_bucket(y_by2)

        payload = {
            "meta": {
                "filters": f,
                "group_by": group_by,
                "month": month,
                "top_limit": top_limit,
                "timing_ms": {"total": int((time()-t0)*1000)},
            },
            "daily": {
                "labels": daily_labels,
                "total": daily_total,
                "target": daily_target,
                "cum_total": daily_cum,
                "cum_target": daily_target_cum,
                "achievement": daily_ach,
                "cum_achievement": daily_cum_ach,
            },
            "daily_stacked": {
                "labels": daily_labels,
                "groups": d_groups,
                "value": d_by,
                "cum": d_cum_by,
                "pct": d_pct_by,
                "pct_cum": d_pct_cum_by,
            },
            "monthly": {
                "labels": monthly_labels,
                "sales_2025": monthly_25,
                "sales_2026": monthly_26,
                "target_2026": target_26,
                "cum_sales_2025": cum_25,
                "cum_sales_2026": cum_26,
                "cum_target_2026": cum_target_26,
                "ach_2026": ach_26,
                "ach_cum_2026": ach_cum_26,
            },
            "monthly_stacked": {
                "labels": monthly_labels,
                "groups": groups_union,
                "value_2025": {g: m_by_25.get(g, [0.0]*12) for g in groups_union},
                "value_2026": {g: m_by_26.get(g, [0.0]*12) for g in groups_union},
                "cum_2025": {g: m_cum_25.get(g, [0.0]*12) for g in groups_union},
                "cum_2026": {g: m_cum_26.get(g, [0.0]*12) for g in groups_union},
                "pct_2025": m_pct_25,
                "pct_2026": m_pct_26,
                "pct_cum_2025": m_pct_cum_25,
                "pct_cum_2026": m_pct_cum_26,
            },
            "monthly_target_stacked": {
                "labels": monthly_labels,
                "groups": mt_groups,
                "value": mt_by,
                "cum": mt_cum_by,
                "pct": mt_pct_by,
                "pct_cum": mt_pct_cum_by,
            },
            "yearly": {
                "labels": yearly_labels,
                "total": y_total,
            },
            "yearly_stacked": {
                "labels": yearly_labels,
                "groups": y_groups,
                "value": y_by2,
                "pct": y_pct,
            },
        }

        _cache_set(_V2_CACHE_DASH, key, payload, ttl_sec=int(os.getenv("DASH_CACHE_TTL", "30")))
        return jsonify(payload)

    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            cur.close(); conn.close()
        except Exception:
            pass


# ----------------------------- Daily Sales ---------------------------------
@app.get("/api/daily_sales")
def daily_sales():
    _ck = _make_v2_key("daily_sales", request)
    _hit = _cache_get(_API_CACHE, _ck)
    if _hit is not None:
        return jsonify(_hit)
    f = parse_filters(request)
    value = "qty" if f["metric"] == "qty" else "amt"

    # 0 or missing = no top filter
    top_limit = int(request.args.get("top_limit", 0) or 0)

    joins, wh, params = build_customer_filters("s", f, use_sold_to_name=False)

    # category
    
    # Only apply category_filters for categories other than 443
    if f["category"] != "443":
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
    if f["material"] != "ALL":
        wh.append("s.material = %s")
        params.append(f["material"])
    base_where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        top_sold_to = None

        # 1) Get Top N sold_to from baseline table (sales_25_2602)
        if top_limit > 0:
            top_sold_to = get_top_sold_to_from_baseline(
                cur, f, top_limit, value
            )                                               # CHANGED

            if not top_sold_to:
                # no matching customers – all days = 0
                return jsonify([{"day": d, "value": 0} for d in range(1, 32)])

        # 2) Daily totals, optionally restricted to the baseline top sold_to set
        wh2 = list(wh)
        params2 = list(params)

        if top_sold_to:
            placeholders = ",".join(["%s"] * len(top_sold_to))
            wh2.append(f"s.sold_to IN ({placeholders})")
            params2.extend(top_sold_to)

        where_sql2 = ("WHERE " + " AND ".join(wh2)) if wh2 else ""

        daily_sql = f"""
        SELECT s.day AS day_num, SUM(s.{value}) AS daily_total
            FROM sales_thismonth s
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
    result = [{"day": d, "value": day_map.get(d, 0)} for d in range(1, 32)]
    _cache_set(_API_CACHE, _ck, result, ttl_sec=60)
    return jsonify(result)
     

#
# -------------------- Daily breakdown (stacked by group) -------------------
@app.get("/api/daily_breakdown")
def daily_breakdown():
    _ck = _make_v2_key("daily_breakdown", request)
    _hit = _cache_get(_API_CACHE, _ck)
    if _hit is not None:
        return jsonify(_hit)
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
    
    if f["category"] != "443":
        cat_joins, cat_where = category_filters("s", f["category"])
        joins += cat_joins
        wh    += cat_where

    # Direct, index-friendly filters that live on sales202601
    if f["product_group"] != "ALL":
        wh.append("s.product_group = %s"); params.append(f["product_group"])
    if f["pattern"] != "ALL":
        wh.append("s.pattern = %s");       params.append(f["pattern"])
    if f["material"] != "ALL":
        wh.append("s.material = %s")
        params.append(f["material"])
    base_where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        top_sold_to = None

        # 1) Get Top N sold_to from baseline table (sales_25_2602)
        if top_limit > 0:
            top_sold_to = get_top_sold_to_from_baseline(
                cur, f, top_limit, value
            )                                               # CHANGED

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
            FROM sales_thismonth s
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

    _cache_set(_API_CACHE, _ck, rows, ttl_sec=60)
    return jsonify(rows)


# ----------------------------- Daily Target (Oct) ---------------------------------
import calendar
@app.get("/api/daily_target")
def daily_target():
    _ck = _make_v2_key("daily_target", request)
    _hit = _cache_get(_API_CACHE, _ck)
    if _hit is not None:
        return jsonify(_hit)
    f = parse_filters(request)
    value = "qty" if f["metric"] == "qty" else "amt"

    # which month? default to October (10) if nothing is passed
    month = int(request.args.get("month", 2))

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

        # 1) Get Top N sold_to from baseline table (sales_25_2602)
        if top_limit > 0:
            top_sold_to = get_top_sold_to_from_baseline(
                cur, f, top_limit, value
            )                                               # CHANGED

            # nothing matched -> all days = 0
            if not top_sold_to:
                days_in_month = calendar.monthrange(2026, month)[1]   # CHANGED (2026)
                return jsonify([
                    {"day": d, "value": 0}
                    for d in range(1, days_in_month + 1)
                ])

        # 2) Monthly target, optionally restricted to baseline Top customers
        wh2     = list(wh)
        params2 = list(params)

        if top_sold_to:
            placeholders = ",".join(["%s"] * len(top_sold_to))
            wh2.append(f"t.sold_to IN ({placeholders})")
            params2.extend(top_sold_to)

        where_sql2 = ("WHERE " + " AND ".join(wh2)) if wh2 else ""

        sql = f"""
        SELECT t.month AS month_num, SUM(t.{value}) AS monthly_total
            FROM target_26 t
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

    # how many days in that month? (target_26 is for 2026)
    days_in_month = calendar.monthrange(2026, month)[1]     # CHANGED
    daily_value   = monthly_total / days_in_month if days_in_month else 0

    # return one entry per day: 1..N
    result = [{"day": d, "value": daily_value} for d in range(1, days_in_month + 1)]
    _cache_set(_API_CACHE, _ck, result, ttl_sec=60)
    return jsonify(result)

def autosize_columns(ws, max_width=60):
    """
    Auto adjust column width based on content length
    """
    for col in ws.columns:
        max_len = 0
        col_letter = get_column_letter(col[0].column)

        for cell in col:
            v = "" if cell.value is None else str(cell.value)
            if len(v) > max_len:
                max_len = len(v)

        ws.column_dimensions[col_letter].width = min(max_len + 2, max_width)


def build_excel(rows, sheet_name="Data", header_order=None, meta_lines=None):
    wb = Workbook()
    ws = wb.active
    ws.title = sheet_name[:31]

    # 1) 맨 위에 선택 조건 표시 (옵션)
    start_row = 1
    if meta_lines:
        for line in meta_lines:
            ws.append([line])
        ws.append([])  # blank line
        start_row = ws.max_row + 1

    if not rows:
        ws.append(["No data"])
        return wb

    headers = header_order or list(rows[0].keys())

    ws.append(headers)
    header_row = ws.max_row
    for c, h in enumerate(headers, start=1):
        cell = ws.cell(row=header_row, column=c)
        cell.font = Font(bold=True)
        cell.alignment = Alignment(horizontal="center", vertical="center")

    for r in rows:
        ws.append([r.get(h) for h in headers])

    # 헤더 다음 줄로 freeze
    ws.freeze_panes = f"A{header_row+1}"

    autosize_columns(ws)
    return wb
def fetch_table_rows(top_limit: int):
    f = parse_filters(request)
    metric = (request.args.get("metric") or f.get("metric") or "qty").lower()
    value_col = "qty" if metric == "qty" else "amt"

    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    # pivot columns: 1~31
    day_cols = ",\n".join([
        f"SUM(CASE WHEN s.`day` = {d} THEN s.{value_col} ELSE 0 END) AS `{d}`"
        for d in range(1, 32)
    ])

    # ---------- STEP 1: use GLOBAL TOP (prebuilt once) ----------
    top_pairs = None
    if top_limit in (10, 20, 30):
        sold_to_list = _GLOBAL_TOP_FIXED.get((int(top_limit), metric), [])
        if sold_to_list:
            top_pairs = [{"sold_to": s} for s in sold_to_list]

    # ---------- STEP 2: build filters (same as daily_sales) ----------
    joins, wh, params = build_customer_filters("s", f, use_sold_to_name=False)

    # category (same rule: skip 443)
    if f.get("category", "ALL") != "443":
        cat_joins, cat_where = category_filters("s", f.get("category", "ALL"))
        joins += cat_joins
        wh    += cat_where

    if f.get("product_group", "ALL") != "ALL":
        wh.append("s.product_group = %s"); params.append(f["product_group"])
    if f.get("pattern", "ALL") != "ALL":
        wh.append("s.pattern = %s"); params.append(f["pattern"])
    if f.get("material", "ALL") != "ALL":
        wh.append("s.material = %s"); params.append(f["material"])

    # top filter (sold_to 기준)
    if top_pairs:
        conds = []
        for p in top_pairs:
            conds.append("s.sold_to = %s")
            params.append(p["sold_to"])
        wh.append("(" + " OR ".join(conds) + ")")

    where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

    sql = f"""
        SELECT
            cus.bde_state AS region,
            cus.salesman_name AS bde,
            cus.sold_to_group AS sold_to_group,

            -- 코드 대신 이름 우선, 없으면 코드 fallback
            COALESCE(NULLIF(TRIM(cus.sold_to_name),''), s.sold_to) AS sold_to_name,
            COALESCE(NULLIF(TRIM(cus.ship_to_name),''), s.ship_to) AS ship_to_name,

            -- 추가 컬럼 (원하면 유지)
            s.sold_to AS sold_to_code,
            s.ship_to AS ship_to_code,

            {day_cols}
        FROM sales_thismonth s
        {' '.join(joins)}
        {where_sql}
        GROUP BY
            cus.bde_state, cus.salesman_name, cus.sold_to_group,
            sold_to_name, ship_to_name,
            s.sold_to, s.ship_to
        ORDER BY SUM(s.{value_col}) DESC
    """

    cur.execute(sql, params)
    rows = cur.fetchall()

    cur.close()
    conn.close()
    return rows
@app.get("/api/export_excel")
def export_excel():
    try:
        top_limit = int(request.args.get("top_limit", "0") or "0")
        f = parse_filters(request)
        metric = (request.args.get("metric") or f.get("metric") or "qty").lower()

        rows = fetch_table_rows(top_limit=top_limit)

        # 헤더 순서 고정 (원하는 컬럼 더 추가 가능)
        header_order = (
            ["region", "bde", "sold_to_group", "sold_to_name", "ship_to_name", "sold_to_code", "ship_to_code"]
            + [str(d) for d in range(1, 32)]
        )

        # 맨 위에 어떤 선택으로 이 데이터가 나왔는지 표시
        meta_lines = [
            f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"metric={metric}, top_limit={top_limit if top_limit else 'ALL'}",
            f"category={f.get('category','ALL')}, region={f.get('region','ALL')}, salesman={f.get('salesman','ALL')}, sold_to_group={f.get('sold_to_group','ALL')}",
            f"product_group={f.get('product_group','ALL')}, pattern={f.get('pattern','ALL')}, material={f.get('material','ALL')}",
            f"sold_to={f.get('sold_to','ALL')}, ship_to={f.get('ship_to','ALL')}",
        ]

        wb = build_excel(
            rows,
            sheet_name="sales_thismonth_by_day",
            header_order=header_order,
            meta_lines=meta_lines
        )

        bio = BytesIO()
        wb.save(bio)
        bio.seek(0)

        stamp = datetime.now().strftime("%Y%m%d_%H%M")
        filename = f"sales_thismonth_top{top_limit if top_limit else 'ALL'}_{metric}_{stamp}.xlsx"

        return send_file(
            bio,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        print("export_excel error:", repr(e))
        print(traceback.format_exc())
        return jsonify({"error": "Internal Server Error", "message": str(e)}), 500
        
        # ----------------------------- Monthly Sales ---------------------------------
@app.get("/api/monthly_sales")
def monthly_sales():
    _ck = _make_v2_key("monthly_sales", request)
    _hit = _cache_get(_API_CACHE, _ck)
    if _hit is not None:
        return jsonify(_hit)
    f = parse_filters(request)
    value = "qty" if f["metric"] == "qty" else "amt"

    # Year filter (default: current year if not provided)
    year = int(request.args.get("year", 2025) or 2025)

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
    if f["material"] != "ALL":
        wh.append("s.material = %s")
        params.append(f["material"])

    # year condition
    wh.append("s.year = %s")
    params.append(year)

    base_where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        top_sold_to = None

        # 1) Get Top N sold_to from baseline table (sales_25_2602)
        if top_limit > 0:
            top_sold_to = get_top_sold_to_from_baseline(
                cur, f, top_limit, value
            )                                               # CHANGED

            if not top_sold_to:
                return jsonify([{"month": m, "value": 0} for m in range(1, 13)])

        # 2) Monthly totals, optionally restricted to baseline Top customers
        wh2 = list(wh)
        params2 = list(params)

        if top_sold_to:
            placeholders = ",".join(["%s"] * len(top_sold_to))
            wh2.append(f"s.sold_to IN ({placeholders})")
            params2.extend(top_sold_to)

        where_sql2 = ("WHERE " + " AND ".join(wh2)) if wh2 else ""

        monthly_sql = f"""
        SELECT s.month AS month_num, SUM(s.{value}) AS monthly_total
            FROM sales_25_2602 s
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
    result = [{"month": m, "value": month_map.get(m, 0)} for m in range(1, 13)]
    _cache_set(_API_CACHE, _ck, result, ttl_sec=60)
    return jsonify(result)


# -------------------- Monthly breakdown (stacked by group) -------------------
@app.get("/api/monthly_breakdown")
def monthly_breakdown():
    _ck = _make_v2_key("monthly_breakdown", request)
    _hit = _cache_get(_API_CACHE, _ck)
    if _hit is not None:
        return jsonify(_hit)
    f = parse_filters(request)
    value = "qty" if f["metric"] == "qty" else "amt"

    # Year filter (default: 2025)
    year = int(request.args.get("year", 2025) or 2025)

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

    # Direct, index-friendly filters
    if f["product_group"] != "ALL":
        wh.append("s.product_group = %s"); params.append(f["product_group"])
    if f["pattern"] != "ALL":
        wh.append("s.pattern = %s");       params.append(f["pattern"])
    if f["material"] != "ALL":
        wh.append("s.material = %s");      params.append(f["material"])

    # Year condition
    wh.append("s.year = %s"); params.append(year)

    base_where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

    conn = get_connection()
    cur  = conn.cursor(dictionary=True)
    try:
        top_sold_to = None

        # 1) Get Top N sold_to from baseline table (sales_25_2602)
        if top_limit > 0:
            top_sold_to = get_top_sold_to_from_baseline(
                cur, f, top_limit, value
            )                                               # CHANGED

            if not top_sold_to:
                return jsonify([])

        # 2) Monthly breakdown
        wh2     = list(wh)
        params2 = list(params)

        if top_sold_to:
            placeholders = ",".join(["%s"] * len(top_sold_to))
            wh2.append(f"s.sold_to IN ({placeholders})")
            params2.extend(top_sold_to)

        where_sql2 = ("WHERE " + " AND ".join(wh2)) if wh2 else ""

        # NOTE: use s.month consistently (same as monthly_sales)
        sql = f"""
        SELECT s.month AS month,
                {group_col} AS group_label,
                SUM(s.{value}) AS value
            FROM sales_25_2602 s
            {' '.join(joins)}
            {where_sql2}
        GROUP BY s.month, {group_col}
        ORDER BY s.month
        """
        cur.execute(sql, tuple(params2))
        rows = cur.fetchall()

    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass

    _cache_set(_API_CACHE, _ck, rows, ttl_sec=60)
    return jsonify(rows)


# ----------------------------- Monthly Target ---------------------------------
@app.get("/api/monthly_target")
def monthly_target():
    _ck = _make_v2_key("monthly_target", request)
    _hit = _cache_get(_API_CACHE, _ck)
    if _hit is not None:
        return jsonify(_hit)
    f = parse_filters(request)
    value = "qty" if f["metric"] == "qty" else "amt"

    # 0 or missing = no top filter
    top_limit = int(request.args.get("top_limit", 0) or 0)

    joins, wh, params = build_customer_filters("t", f, use_sold_to_name=False)

    # category filters for target table
    cat_joins, cat_where = category_target_filters("t", f["category"])
    joins += cat_joins
    wh    += cat_where



    conn = get_connection()
    cur  = conn.cursor(dictionary=True)
    try:
        top_sold_to = None

        # 1) Get Top N sold_to from baseline table (sales_25_2602)
        if top_limit > 0:
            top_sold_to = get_top_sold_to_from_baseline(
                cur, f, top_limit, value
            )                                               # CHANGED

            # no matches -> all months zero
            if not top_sold_to:
                return jsonify([{"month": m, "value": 0} for m in range(1, 13)])

        # 2) Monthly target, optionally restricted to baseline Top customers
        wh2     = list(wh)
        params2 = list(params)

        if top_sold_to:
            placeholders = ",".join(["%s"] * len(top_sold_to))
            wh2.append(f"t.sold_to IN ({placeholders})")
            params2.extend(top_sold_to)

        where_sql2 = ("WHERE " + " AND ".join(wh2)) if wh2 else ""

        monthly_sql = f"""
        SELECT t.month AS month_num, SUM(t.{value}) AS monthly_total
            FROM target_26 t
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
    result = [{"month": m, "value": month_map.get(m, 0)} for m in range(1, 13)]
    _cache_set(_API_CACHE, _ck, result, ttl_sec=60)
    return jsonify(result)


# ------------------------- Monthly Target Breakdown --------------------------
from mysql.connector import Error as MySQLError

@app.get("/api/monthly_target_breakdown")
def monthly_target_breakdown():
    _ck = _make_v2_key("monthly_target_breakdown", request)
    _hit = _cache_get(_API_CACHE, _ck)
    if _hit is not None:
        return jsonify(_hit)
    f = parse_filters(request)
    value = "qty" if f["metric"] == "qty" else "amt"

    top_limit = int(request.args.get("top_limit", 0) or 0)

    group_by = (request.args.get("group_by") or "region").strip()
    group_cols = {
        "product_group": "t.product_group",
        "region":        "cus.bde_state",
        "salesman":      "cus.salesman_name",
        "sold_to_group": "cus.sold_to_group",
        "sold_to":       "cus.sold_to_name",
        "pattern":       "t.pattern",
    }
    if group_by not in group_cols:
        return jsonify({"error": "invalid group_by"}), 400
    group_col = group_cols[group_by]

    joins, wh, params = build_customer_filters("t", f, use_sold_to_name=False)

    cat_joins, cat_where = category_target_filters("t", f["category"])
    joins += cat_joins
    wh    += cat_where

    if f["product_group"] != "ALL":
        wh.append("t.product_group = %s"); params.append(f["product_group"])
    if f["pattern"] != "ALL":
        wh.append("t.pattern = %s");       params.append(f["pattern"])
    if f["material"] != "ALL":
        wh.append("t.material = %s");      params.append(f["material"])

    conn = get_connection()
    cur  = conn.cursor(dictionary=True)

    try:
        top_sold_to = None
        if top_limit > 0:
            top_sold_to = get_top_sold_to_from_baseline(cur, f, top_limit, value)
            if not top_sold_to:
                return jsonify([])

        wh2     = list(wh)
        params2 = list(params)

        if top_sold_to:
            placeholders = ",".join(["%s"] * len(top_sold_to))
            wh2.append(f"t.sold_to IN ({placeholders})")
            params2.extend(top_sold_to)

        where_sql2 = ("WHERE " + " AND ".join(wh2)) if wh2 else ""

        sql = f"""
            SELECT
                t.month AS month,
                {group_col} AS group_label,
                SUM(t.{value}) AS value
            FROM target_26 t
            {' '.join(joins)}
            {where_sql2}
            GROUP BY t.month, {group_col}
            ORDER BY t.month
        """

        cur.execute(sql, tuple(params2))
        rows = cur.fetchall()
        _cache_set(_API_CACHE, _ck, rows, ttl_sec=60)
        return jsonify(rows)

    except MySQLError as e:
        # 프론트에서 바로 원인 보이도록 내려줌 (운영이면 msg만 제거하고 로그로만 남기기)
        return jsonify({
            "error": "mysql_error",
            "msg": str(e),
            "group_by": group_by,
        }), 400

    except Exception as e:
        return jsonify({
            "error": "server_error",
            "msg": str(e),
            "group_by": group_by,
        }), 500

    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass
# ----------------------------- Yearly Sales ---------------------------------
@app.get("/api/yearly_sales")
def yearly_sales():
    _ck = _make_v2_key("yearly_sales", request)
    _hit = _cache_get(_API_CACHE, _ck)
    if _hit is not None:
        return jsonify(_hit)
    f = parse_filters(request)
    value = "qty" if f["metric"] == "qty" else "amt"

    # 0 or missing = no top filter
    top_limit = int(request.args.get("top_limit", 0) or 0)

    joins, wh, params = build_customer_filters("s", f, use_sold_to_name=False)

    if f["category"] != "443":
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
    if f["material"] != "ALL":
        wh.append("s.material = %s")
        params.append(f["material"])
    base_where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        top_sold_to = None

        # 1) If top_limit > 0, get top N sold_to from baseline (sales_25_2602)
        if top_limit > 0:
            top_sold_to = get_top_sold_to_from_baseline(
                cur, f, top_limit, value
            )  # CHANGED

            if not top_sold_to:
                # no data – return zeros for all years in range
                return jsonify([{"year": y, "value": 0} for y in range(2021, 2026)])  # CHANGED (range fix)

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
            FROM sales_21_25 s
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
    result = [{"year": y, "value": year_map.get(y, 0)} for y in range(2021, 2026)]
    _cache_set(_API_CACHE, _ck, result, ttl_sec=60)
    return jsonify(result)


# -------------------- Yearly breakdown (stacked by group) -------------------
@app.get("/api/yearly_breakdown")
def yearly_breakdown():
    _ck = _make_v2_key("yearly_breakdown", request)
    _hit = _cache_get(_API_CACHE, _ck)
    if _hit is not None:
        return jsonify(_hit)
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
    if f["category"] != "443":
        cat_joins, cat_where = category_filters("s", f["category"])
        joins += cat_joins
        wh    += cat_where

    # Direct, index-friendly filters that live on sales212510
    if f["product_group"] != "ALL":
        wh.append("s.product_group = %s"); params.append(f["product_group"])
    if f["pattern"] != "ALL":
        wh.append("s.pattern = %s");       params.append(f["pattern"])
    if f["material"] != "ALL":
        wh.append("s.material = %s")
        params.append(f["material"])
    base_where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

    conn = get_connection()
    cur  = conn.cursor(dictionary=True)
    try:
        top_sold_to = None

        # 1) If top_limit > 0, get top N sold_to from baseline (sales_25_2602)
        if top_limit > 0:
            top_sold_to = get_top_sold_to_from_baseline(
                cur, f, top_limit, value
            )  # CHANGED

            # no data – nothing to show
            if not top_sold_to:
                return jsonify([])  # same behavior

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
            FROM sales_21_25 s
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

    _cache_set(_API_CACHE, _ck, rows, ttl_sec=60)
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
    parent = (request.args.get("sold_to_group") or "ALL").strip()
    top_limit = int(request.args.get("top_limit", 0) or 0)

    # Use your existing filter parser (metric/category/region/salesman/... etc.)
    f = parse_filters(request)
    value = "qty" if f["metric"] == "qty" else "amt"

    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        # ----------------- 1) No top_limit -> old behavior -----------------
        if top_limit <= 0:
            if parent != "ALL":
                cur.execute("""
                    SELECT DISTINCT TRIM(sold_to_name) AS name
                      FROM customer
                     WHERE sold_to_group = %s
                       AND sold_to_name IS NOT NULL
                       AND TRIM(sold_to_name) <> ''
                     ORDER BY TRIM(sold_to_name)
                """, (parent,))
            else:
                cur.execute("""
                    SELECT DISTINCT TRIM(sold_to_name) AS name
                      FROM customer
                     WHERE sold_to_name IS NOT NULL
                       AND TRIM(sold_to_name) <> ''
                     ORDER BY TRIM(sold_to_name)
                """)
            rows = cur.fetchall()
            return jsonify([r["name"] for r in rows])

        # ----------------- 2) top_limit -> baseline top sold_to -> names -----------------
        # Get top sold_to list from baseline table (sales_25_2602)
        top_sold_to = get_top_sold_to_from_baseline(cur, f, top_limit, value)

        if not top_sold_to:
            return jsonify([])

        placeholders = ",".join(["%s"] * len(top_sold_to))

        # Filter names by sold_to_group if provided
        params = list(top_sold_to)
        where_parent = ""
        if parent != "ALL":
            where_parent = " AND c.sold_to_group = %s "
            params.append(parent)

        cur.execute(f"""
            SELECT DISTINCT TRIM(c.sold_to_name) AS name
              FROM customer c
             WHERE c.sold_to IN ({placeholders})
               {where_parent}
               AND c.sold_to_name IS NOT NULL
               AND TRIM(c.sold_to_name) <> ''
             ORDER BY TRIM(c.sold_to_name)
        """, tuple(params))

        rows = cur.fetchall()
        return jsonify([r["name"] for r in rows])

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass

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
        cur.execute("SELECT DISTINCT product_group FROM sales_2025")
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
                FROM sales_2025
                WHERE product_group = %s
                ORDER BY TRIM(pattern)
            """, (product_group,))
        else:
            cur.execute("""
                SELECT DISTINCT TRIM(pattern)
                FROM sales_2025
                ORDER BY TRIM(pattern)
            """)
        names = [r[0] for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify(names)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/api/materials")
def materials():
    """
    Return distinct Material list.
    - 필터: product_group, pattern (둘 다 'ALL' 이면 전체)
    """
    product_group = (request.args.get("product_group") or "ALL").strip()
    pattern       = (request.args.get("pattern")       or "ALL").strip()

    try:
        conn = get_connection(); cur = conn.cursor()

        where = []
        params = []

        if product_group and product_group != "ALL":
            where.append("product_group = %s")
            params.append(product_group)

        if pattern and pattern != "ALL":
            where.append("pattern = %s")
            params.append(pattern)

        where_sql = ""
        if where:
            where_sql = "WHERE " + " AND ".join(where)

        cur.execute(f"""
            SELECT DISTINCT m_desc
            FROM carrying_2602
            {where_sql}
            ORDER BY m_desc
        """, tuple(params))

        names = [r[0] for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify(names)

    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    
@app.get("/api/profit_monthly")
def profit_monthly():
    import traceback

    try:
        f = parse_filters(request)

        # same metric logic as daily_sales
        value = "qty" if f.get("metric") == "qty" else "amt"

        # optional: ?top_limit=10 -> top 10 sold_to by sales (from sales_2025)
        top_limit = int(request.args.get("top_limit", 0) or 0)

        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        try:
            top_sold_to = None

            # 1) Top-N sold_to should ALWAYS come from baseline sales table: sales_25_2602
            if top_limit > 0:
                # NOTE: assumes you already created this helper elsewhere:
                # def get_top_sold_to_from_baseline(cur, f, top_limit, value): ...
                top_sold_to = get_top_sold_to_from_baseline(cur, f, top_limit, value)

                if not top_sold_to:
                    return jsonify([
                        dict(month=m, gross=0, sd=0, cogs=0, op_cost=0)
                        for m in range(1, 13)
                    ])

            # 2) Monthly profit totals from profit,
            #    restricted to those top sold_to (if any)
            joins_p  = []
            wh_p     = []
            params_p = []

            # category filters on profit table
            cat_joins_p, cat_where_p = category_filters("p", f.get("category", "ALL"))
            joins_p += cat_joins_p
            wh_p    += cat_where_p

            if f.get("product_group", "ALL") != "ALL":
                wh_p.append("p.product_group = %s")
                params_p.append(f["product_group"])

            if f.get("pattern", "ALL") != "ALL":
                wh_p.append("p.pattern = %s")
                params_p.append(f["pattern"])
            if f["material"] != "ALL":
                wh_p.append("p.material = %s")
                params_p.append(f["material"])
            if top_sold_to:
                placeholders = ",".join(["%s"] * len(top_sold_to))
                wh_p.append(f"p.sold_to IN ({placeholders})")
                params_p.extend(top_sold_to)

            where_sql2 = ("WHERE " + " AND ".join(wh_p)) if wh_p else ""
            monthly_sql = f"""
                SELECT CAST(p.month AS UNSIGNED) AS month,
                       SUM(p.gross)           AS gross,
                       SUM(p.sales_deduction) AS sd,
                       SUM(p.cogs)            AS cogs,
                       SUM(p.operating_cost)  AS op_cost
                  FROM profit p
                  {' '.join(joins_p)}
                  {where_sql2}
                 GROUP BY CAST(p.month AS UNSIGNED)
                 ORDER BY CAST(p.month AS UNSIGNED)
            """
            cur.execute(monthly_sql, tuple(params_p))
            rows = cur.fetchall()

        finally:
            cur.close()
            conn.close()

        # Build output for months 1..12
        out = [dict(month=m, gross=0, sd=0, cogs=0, op_cost=0) for m in range(1, 13)]
        for r in rows:
            m = int(r["month"] or 0)
            if 1 <= m <= 12:
                out[m - 1].update(
                    gross=float(r["gross"] or 0),
                    sd=float(r["sd"] or 0),
                    cogs=float(r["cogs"] or 0),
                    op_cost=float(r["op_cost"] or 0),
                )

        return jsonify(out)

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.get("/api/sales_map")
def sales_map():
    f = parse_filters(request)
    value = "qty" if f["metric"] == "qty" else "amt"

    top_limit = int(request.args.get("top_limit", 0) or 0)

    # ---- same base filter pattern as daily_sales ----
    joins, wh, params = build_customer_filters("s", f, use_sold_to_name=False)

    # join customer for lat/lng
    joins.append("""
        JOIN customer c ON c.ship_to = s.ship_to
    """)

    # category
    if f["category"] != "443":
        cat_joins, cat_where = category_filters("s", f["category"])
        joins += cat_joins
        wh    += cat_where

    # direct filters (same as other APIs)
    if f["product_group"] != "ALL":
        wh.append("s.product_group = %s")
        params.append(f["product_group"])

    if f["pattern"] != "ALL":
        wh.append("s.pattern = %s")
        params.append(f["pattern"])

    if f["material"] != "ALL":
        wh.append("s.material = %s")
        params.append(f["material"])

    # only customers with coordinates
    wh.append("c.latitude IS NOT NULL")
    wh.append("c.longitude IS NOT NULL")

    # 2026 cumulative only
    wh.append("s.year = %s")
    params.append(2026)

    base_where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

    conn = get_connection()
    cur  = conn.cursor(dictionary=True)

    try:
        top_sold_to = None

        # ---- Top N logic (same baseline as other APIs) ----
        if top_limit > 0:
            top_sold_to = get_top_sold_to_from_baseline(cur, f, top_limit, value)
            if not top_sold_to:
                return jsonify([])

        # ---- apply top filter ----
        wh2     = list(wh)
        params2 = list(params)

        if top_sold_to:
            placeholders = ",".join(["%s"] * len(top_sold_to))
            wh2.append(f"s.sold_to IN ({placeholders})")
            params2.extend(top_sold_to)

        where_sql2 = ("WHERE " + " AND ".join(wh2)) if wh2 else ""

        # ---- Map totals ----
        map_sql = f"""
            SELECT
                c.ship_to,
                c.ship_to_name,
                c.latitude,
                c.longitude,
                MAX(c.bde_state)      AS region,
                MAX(c.salesman_name)  AS bde,
                SUM(s.{value})        AS total_value
            FROM sales_25_2602 s
            {' '.join(joins)}
            {where_sql2}
            GROUP BY
                c.ship_to,
                c.ship_to_name,
                c.latitude,
                c.longitude
            ORDER BY total_value DESC
        """

        cur.execute(map_sql, tuple(params2))
        rows = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    return jsonify(rows)

# ── GPS Visit count API ────────────────────────────────────────────────────────
@app.get("/api/monthly_visits")
def monthly_visits():
    """
    Count GPS visits within 500m of a given lat/lng per month.
    A 'visit' = at least one GPS record on a calendar day within the radius.
    Params: lat, lng, year
    Returns: [{"m": 1, "visits": 3}, ...]   (m = month number 1-12)
    """
    try:
        lat  = float(request.args.get("lat", ""))
        lng  = float(request.args.get("lng", ""))
    except (TypeError, ValueError):
        return jsonify([])

    year = int(request.args.get("year", 2026) or 2026)

    # bbox pre-filter (~500 m at ~30°S)
    LAT_D = 0.0045
    LNG_D = 0.0054

    conn = get_connection()
    cur  = conn.cursor(dictionary=True)
    try:
        if USE_SQLITE:
            # SQLite: bbox only (no trig functions)
            sql = """
                SELECT CAST(strftime('%m', local_date) AS INTEGER) AS m,
                       COUNT(DISTINCT local_date) AS visits
                FROM   gps
                WHERE  strftime('%Y', local_date) = ?
                  AND  latitude  BETWEEN ? AND ?
                  AND  longitude BETWEEN ? AND ?
                GROUP  BY m
                ORDER  BY m
            """
            cur.execute(sql, [str(year),
                               lat - LAT_D, lat + LAT_D,
                               lng - LNG_D, lng + LNG_D])
        else:
            # MySQL: bbox + precise Haversine ≤ 500 m
            sql = """
                SELECT MONTH(local_date)            AS m,
                       COUNT(DISTINCT local_date)   AS visits
                FROM   gps
                WHERE  YEAR(local_date) = %s
                  AND  latitude  BETWEEN %s AND %s
                  AND  longitude BETWEEN %s AND %s
                  AND  (6371000 * ACOS(LEAST(1.0,
                           COS(RADIANS(%s)) * COS(RADIANS(latitude))
                           * COS(RADIANS(longitude) - RADIANS(%s))
                           + SIN(RADIANS(%s)) * SIN(RADIANS(latitude))
                       ))) <= 500
                GROUP  BY MONTH(local_date)
                ORDER  BY m
            """
            cur.execute(sql, [year,
                               lat - LAT_D, lat + LAT_D,
                               lng - LNG_D, lng + LNG_D,
                               lat, lng, lat])

        rows = cur.fetchall() or []
    finally:
        cur.close()
        conn.close()

    return jsonify([{"m": r["m"], "visits": r["visits"]} for r in rows])

# ------------------------------------------------------------------------------
# Build fixed Top 10/20/30 once at startup (after functions are defined)
build_global_top_once()

if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))   # Cloudtype probes 5000
    app.run(host="0.0.0.0", port=port, debug=True)