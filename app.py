from flask import Flask, request, jsonify, send_file,send_from_directory
from werkzeug.middleware.proxy_fix import ProxyFix
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
from openpyxl.styles import Font, Alignment, PatternFill
from datetime import datetime
import traceback
USE_SQLITE = os.environ.get("USE_SQLITE") == "1"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Pick up SMTP / DB creds from .env (gitignored).  Silent no-op if
# python-dotenv isn't installed — the platform env vars still apply.
try:
    from dotenv import load_dotenv as _load_dotenv  # type: ignore
    _load_dotenv(os.path.join(BASE_DIR, ".env"))
except ImportError:
    pass
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
            # DEMO MODE: missing table or column ??pretend query returned nothing
            _e = str(e).lower()
            if "no such table" in _e or "no such column" in _e or "ambiguous column" in _e:
                print(f"[WARN] {e} -- returning empty result (demo mode)")
                self._empty_result = True
                return self
            raise

    def executemany(self, sql, seq_of_params):
        if "%s" in sql:
            sql = sql.replace("%s", "?")
        self._empty_result = False
        try:
            return self._cursor.executemany(sql, seq_of_params)
        except sqlite3.OperationalError as e:
            _e = str(e).lower()
            if "no such table" in _e or "no such column" in _e or "ambiguous column" in _e:
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

# States that belong to each sales region.
# SA/TAS are part of VIC territory; NT is part of WA; ACT is part of NSW.
REGION_STATES = {
    "NSW": ["NSW", "ACT"],
    "QLD": ["QLD"],
    "VIC": ["VIC", "SA", "TAS"],
    "WA":  ["WA",  "NT"],
}

def build_customer_filters(alias_fact: str, f, *, use_sold_to_name: bool=False):
    """
    Returns (joins, wheres, params) to apply Region/Salesman/Group/Sold_to on a fact table.
    Customer JOIN is added only when needed (name-based filters or customer-dimension filters).
    Region/Group filters use EXISTS on customer (ship_to only) to avoid JOIN inflation.
    Salesman filter uses EXISTS on target_26 (authoritative bde?뭩hip_to mapping),
    because customer.salesman_name is often incomplete for WA/NT accounts.
    If use_sold_to_name=True, 'sold_to' will match customer.Sold_to_Name instead of id.
    """
    joins = []
    wh, p = [], []
    needs_cus = False   # only for name-based sold_to/ship_to lookups

    # ?? region: EXISTS on customer (ship_to only) ??no JOIN inflation ??
    if f["region"] != "ALL":
        states = REGION_STATES.get(f["region"].upper(), [f["region"]])
        ph = ",".join(["%s"] * len(states))
        wh.append(
            f"EXISTS (SELECT 1 FROM customer _cr"
            f" WHERE _cr.ship_to = {alias_fact}.ship_to"
            f" AND _cr.bde_state IN ({ph}))"
        )
        p.extend(states)

    # ?? salesman: EXISTS on target_26 (bde is authoritative; customer.salesman_name
    #    may be missing or inconsistent for some regions like WA) ??
    if f["salesman"] != "ALL":
        wh.append(
            f"EXISTS (SELECT 1 FROM target_26 _t"
            f" WHERE _t.ship_to = {alias_fact}.ship_to"
            f" AND UPPER(TRIM(_t.bde)) = UPPER(TRIM(%s)))"
        )
        p.append(f["salesman"])

    # ?? sold_to_group: EXISTS on customer (ship_to only) ??
    if f["sold_to_group"] != "ALL":
        wh.append(
            f"EXISTS (SELECT 1 FROM customer _cr"
            f" WHERE _cr.ship_to = {alias_fact}.ship_to"
            f" AND _cr.sold_to_group = %s)"
        )
        p.append(f["sold_to_group"])

    # ?? sold_to: id ??direct filter on fact table; name ??subquery ??
    if f["sold_to"] != "ALL":
        sv = f["sold_to"]
        if not use_sold_to_name and (sv.isdigit() or sv.upper().startswith("A")):
            wh.append(f"{alias_fact}.sold_to = %s"); p.append(sv)
        else:
            wh.append(
                f"{alias_fact}.sold_to IN ("
                f"SELECT DISTINCT sold_to FROM customer WHERE sold_to_name = %s)"
            )
            p.append(sv)

    # ?? ship_to: code ??direct; name ??subquery (names now come as codes from frontend) ??
    if f["ship_to"] != "ALL":
        st = f["ship_to"].strip()
        if st.isdigit() or st.upper().startswith("A"):
            wh.append(f"{alias_fact}.ship_to = %s"); p.append(st)
        else:
            wh.append(
                f"{alias_fact}.ship_to IN ("
                f"SELECT DISTINCT ship_to FROM customer"
                f" WHERE UPPER(TRIM(ship_to_name)) = UPPER(TRIM(%s)))"
            )
            p.append(st)

    if needs_cus:
        joins.append(_customer_join(alias_fact))

    return joins, wh, p


def build_target_filters(alias: str, f):
    """
    Returns (joins, wheres, params) for target_26 table.
    target_26 has state, bde, ship_to, sold_to directly.
    When sold_to/ship_to is a name (not ID), joins customer table to resolve.
    """
    joins = []
    wh, p = [], []
    needs_cus = False

    if f["region"] != "ALL":
        states = REGION_STATES.get(f["region"].upper(), [f["region"]])
        if len(states) == 1:
            wh.append(f"{alias}.state = %s"); p.append(states[0])
        else:
            wh.append(f"{alias}.state IN ({','.join(['%s']*len(states))})"); p.extend(states)
    if f["salesman"] != "ALL":
        wh.append(f"UPPER(TRIM({alias}.bde)) = UPPER(TRIM(%s))"); p.append(f["salesman"])
    if f["sold_to_group"] != "ALL":
        needs_cus = True
        wh.append("cus.sold_to_group = %s"); p.append(f["sold_to_group"])
    if f["sold_to"] != "ALL":
        sv = f["sold_to"]
        if sv.isdigit() or sv.upper().startswith("A"):
            wh.append(f"{alias}.sold_to = %s"); p.append(sv)
        else:
            needs_cus = True
            wh.append("cus.sold_to_name = %s"); p.append(sv)
    if f["ship_to"] != "ALL":
        st = f["ship_to"].strip()
        if st.isdigit() or st.upper().startswith("A"):
            wh.append(f"{alias}.ship_to = %s"); p.append(st)
        else:
            needs_cus = True
            wh.append("UPPER(TRIM(cus.ship_to_name)) = UPPER(TRIM(%s))"); p.append(st)

    if needs_cus:
        joins.append(f"LEFT JOIN customer cus ON cus.ship_to = {alias}.ship_to")

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
        # HM by Sold-To (use your customer join for Ship_To ??Sold_To; keep simple)
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


# ?? Helpers for normalised sales tables (line/product_group/pattern/inch live in carrying_2602) ??

def _carrying_join(alias: str) -> str:
    """Returns the LEFT JOIN clause for carrying_2602 using alias 'mat'."""
    return f"LEFT JOIN carrying_2602 mat ON mat.m_code = {alias}.material"


def _ensure_carrying_join(alias: str, joins: list) -> None:
    """Adds the carrying_2602 join to 'joins' if it is not already present."""
    j = _carrying_join(alias)
    if j not in joins:
        joins.append(j)


def _customer_join(alias: str) -> str:
    """Returns a deduplicated LEFT JOIN for customer (one row per ship_to) to avoid
    inflating aggregates when a ship_to appears in multiple customer rows."""
    return (
        f"LEFT JOIN ("
        f"SELECT ship_to, MIN(bde_state) AS bde_state, MIN(salesman_name) AS salesman_name,"
        f" MIN(sold_to_group) AS sold_to_group, MIN(sold_to_name) AS sold_to_name,"
        f" MIN(ship_to_name) AS ship_to_name, MIN(sold_to) AS sold_to"
        f" FROM customer GROUP BY ship_to"
        f") cus ON cus.ship_to = {alias}.ship_to"
    )


def _ensure_customer_join(alias: str, joins: list) -> None:
    """Adds the customer join to 'joins' if it is not already present."""
    j = _customer_join(alias)
    if j not in joins:
        joins.append(j)


def category_filters_sales(alias: str, category: str, has_brand: bool = False):
    """
    Like category_filters() but for the normalised sales fact tables
    (sales_2601 / sales_2526 / sales_21_25) where line / inch / pattern
    have been removed and now live in carrying_2602 (alias: mat).

    Returns (joins, wheres) — same contract as category_filters().

    has_brand=True signals the fact table has its own `brand` column
    (currently only sales_thismonth).  For HK / LF that path filters
    directly on `{alias}.brand` so we don't drop rows where the
    material isn't in carrying_2602 (a real case — Rebate brand-tagged
    sales for materials still missing from the master).
    """
    joins, wh = [], []
    cat = (category or "ALL").upper()

    if cat == "ALL":
        return joins, wh

    elif cat == "PCLT":
        joins.append(_carrying_join(alias))
        wh.append("mat.line = 'PCLT'")

    elif cat == "TBR":
        joins.append(_carrying_join(alias))
        wh.append("mat.line = 'TBR'")

    elif cat == "18PLUS":
        joins.append(_carrying_join(alias))
        wh.append("mat.line = 'PCLT'")
        # Extract rim inch from size string e.g. "225/45R18" ??18
        wh.append("CAST(SUBSTRING_INDEX(mat.size, 'R', -1) AS DECIMAL(5,2)) >= 18.0")

    elif cat == "ISEG":
        joins.append(f"JOIN iseg i ON cast(trim(i.Material) as unsigned) = {alias}.material")

    elif cat == "SUV":
        joins.append(_carrying_join(alias))
        joins.append("JOIN suv suv ON suv.Pattern = mat.pattern")

    elif cat == "LOWPROFILE":
        joins.append(f"JOIN lowprofile lp ON cast(trim(lp.Material) as unsigned) = {alias}.material")

    elif cat == "HM":
        joins.append(f"JOIN hm hm ON hm.sold_to = {alias}.sold_to")

    elif cat in ("HK", "LF"):
        # Brand filter via carrying_2602.  sales_thismonth has its own
        # brand column but the other sales facts (monthly / yearly) don't,
        # so go through carrying for a single uniform path.
        if has_brand:
            wh.append(f"{alias}.brand = '{cat}'")
        else:
            joins.append(_carrying_join(alias))
            wh.append(f"mat.brand = '{cat}'")

    elif cat == "443":
        joins.append(_carrying_join(alias))
        wh.append(f"""
            EXISTS (
                SELECT 1
                FROM `443_25` p443
                WHERE p443.month         = {alias}.month
                AND p443.product_group = mat.product_group
            )
            """)

    return joins, wh


def category_target_filters(alias: str, category: str):
    """
    Returns (joins, wheres) for target_26 table.
    target_26 has a material column; line/inch/pattern attributes live in
    carrying_2602 so we JOIN that table (alias: mat) for category filtering,
    exactly like category_filters_sales() does for sales fact tables.
    """
    joins, wh = [], []
    cat = (category or "ALL").upper()

    if cat == "ALL":
        return joins, wh

    carrying_join = f"LEFT JOIN carrying_2602 mat ON mat.m_code = {alias}.material"

    if cat == "PCLT":
        joins.append(carrying_join)
        wh.append("mat.line = 'PCLT'")

    elif cat == "TBR":
        joins.append(carrying_join)
        wh.append("mat.line = 'TBR'")

    elif cat == "18PLUS":
        joins.append(carrying_join)
        wh.append("mat.line = 'PCLT'")
        wh.append("CAST(SUBSTRING_INDEX(mat.size, 'R', -1) AS DECIMAL(5,2)) >= 18.0")

    elif cat == "ISEG":
        joins.append(f"JOIN iseg i ON CAST(TRIM(i.Material) AS UNSIGNED) = {alias}.material")

    elif cat == "SUV":
        joins.append(carrying_join)
        joins.append("JOIN suv suv ON suv.Pattern = mat.pattern")

    elif cat == "LOWPROFILE":
        joins.append(f"JOIN lowprofile lp ON CAST(TRIM(lp.Material) AS UNSIGNED) = {alias}.material")

    elif cat == "HM":
        joins.append(f"JOIN hm hm ON hm.sold_to = {alias}.sold_to")

    elif cat in ("HK", "LF"):
        joins.append(carrying_join)
        wh.append(f"mat.brand = '{cat}'")

    elif cat == "443":
        # product_group lives in carrying_2602 (alias: mat) for target_26
        joins.append(f"LEFT JOIN carrying_2602 mat ON mat.m_code = {alias}.material")
        wh.append(f"""EXISTS (
            SELECT 1 FROM `443_25` p443
            WHERE p443.month = {alias}.month
            AND p443.product_group = mat.product_group
        )""")

    return joins, wh

app = Flask(__name__, static_folder="static")
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1)
CORS(app, resources={r"/api/*": {"origins": "*"}})

@app.errorhandler(500)
def handle_500(e):
    return jsonify({"error": "internal_server_error", "detail": str(e)}), 500

@app.errorhandler(Exception)
def handle_exception(e):
    import traceback as _tb
    _tb.print_exc()
    return jsonify({"error": type(e).__name__, "detail": str(e)}), 500

@app.after_request
def add_cache_headers(response):
    """Prevent Cloudflare / CDN from caching API responses."""
    if request.path.startswith('/api/'):
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        response.headers['Pragma'] = 'no-cache'
    else:
        # Static assets: prevent Cloudflare/CDN from caching
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        response.headers['Pragma'] = 'no-cache'
    return response

def category_filters_stock(alias: str, category: str):
    joins, wh = [], []
    cat = (category or "ALL").upper()

    if cat == "ALL":
        return joins, wh

    elif cat == "PCLT":
        wh.append(f"{alias}.line = 'PCLT'")  # stock ?뚯씠釉붿뿉 line ?덉쑝硫?OK

    elif cat == "TBR":
        wh.append(f"{alias}.line = 'TBR'")

    elif cat == "ISEG":
        joins.append("JOIN iseg i ON CAST(TRIM(i.Material) AS UNSIGNED) = s.material")

    elif cat == "SUV":
        # stock?먮뒗 pattern???놁쓣 ???덉쑝??carrying_2602濡쒕???pattern 媛?몄?????        joins.append("JOIN carrying_2602 c ON c.m_code = s.material")
        joins.append("JOIN suv suv ON suv.Pattern = c.pattern")

    elif cat == "LOWPROFILE":
        joins.append("JOIN lowprofile lp ON CAST(TRIM(lp.Material) AS UNSIGNED) = s.material")

    elif cat in ("HK", "LF"):
        joins.append("JOIN carrying_2602 c ON c.m_code = s.material")
        wh.append(f"c.brand = '{cat}'")

    return joins, wh
def category_filters_orders(category: str):
    """
    Orders??移댄뀒怨좊━ ?꾪꽣.
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

    elif cat in ("HK", "LF"):
        needs_carrying = True
        wh.append(f"c.brand = '{cat}'")

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
    groups = sorted({(r.get("group_label") or "").strip() or "COMMON" for r in (rows or [])})
    if group_sort == "region":
        groups = sorted(groups, key=lambda g: (_region_order_key(g), g))

    by_group: Dict[str, List[float]] = {g: [0.0] * idx_count for g in groups}
    for r in (rows or []):
        g = (r.get("group_label") or "").strip() or "COMMON"
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
# Graph-view aggregation cache — short-TTL bucket shared across the
# heavy chart endpoints.  A single page load fires the same query
# many times (per region / per year / per group_by), and identical
# query strings will hit cache on every repeat after the first.
_GRAPH_CACHE: Dict[str, Tuple[float, Any]] = {}

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


def cached_endpoint(ttl_sec: int = 30):
    """Wrap a Flask route so its JSON response is cached in _GRAPH_CACHE
    keyed by sorted query args.  Caches the decoded JSON body and
    re-jsonifies on cache hits, so each request gets a fresh Response
    object (Flask mutates response state during dispatch)."""
    from functools import wraps
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            key = _make_v2_key(fn.__name__, request)
            cached = _cache_get(_GRAPH_CACHE, key)
            if cached is not None:
                print(f"[cache HIT]  {fn.__name__}")
                return jsonify(cached)
            print(f"[cache MISS] {fn.__name__}")
            result = fn(*args, **kwargs)
            try:
                body = result.get_json(silent=True) if hasattr(result, "get_json") else None
            except Exception:
                body = None
            if body is not None:
                _cache_set(_GRAPH_CACHE, key, body, ttl_sec=ttl_sec)
            return result
        return wrapper
    return decorator


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

    # pool_size 32: mysql-connector-python caps the pool at 32.  With
    # the teardown_request safety net below making sure no connection
    # leaks past a single request, 32 is comfortable for a single
    # dashboard tab's 15-20 concurrent calls.
    pool_size = int(os.getenv("DB_POOL_SIZE", "32"))
    pool_name = os.getenv("DB_POOL_NAME", "hka_pool")

    if _MYSQL_POOL is None:
        with _MYSQL_POOL_LOCK:
            if _MYSQL_POOL is None:
                _MYSQL_POOL = pooling.MySQLConnectionPool(
                    pool_name=pool_name,
                    pool_size=pool_size,
                    pool_reset_session=True,
                    **cfg,
                )

    # Retry up to 5 times: pool may be momentarily exhausted under burst load
    last_err = None
    for attempt in range(5):
        try:
            conn = _MYSQL_POOL.get_connection()
            try:
                conn.ping(reconnect=True, attempts=2, delay=1)
            except Exception:
                pass
            return conn
        except mysql.connector.errors.PoolError as e:
            last_err = e
            import time as _time
            _time.sleep(0.2 * (attempt + 1))   # 0.2s, 0.4s, 0.6s, 0.8s, 1.0s
        except mysql.connector.Error as e:
            last_err = e
            break

    print("DB connection failed:", last_err)
    raise RuntimeError(f"DB connection unavailable: {last_err}") from last_err

@app.get("/api/ping")
def ping():
    try:
        conn = get_connection()
        conn.close()
        return {"ok": True}
    except Exception:
        return {"ok": False}, 503

@app.get("/api/debug_salesman")
def debug_salesman():
    """Temporary: diagnose salesman/ship_to data for a given bde_state."""
    state = request.args.get("state", "WA")
    states = REGION_STATES.get(state.upper(), [state])
    ph = ",".join(["%s"] * len(states))
    try:
        conn = get_connection(); cur = conn.cursor(dictionary=True)

        # 1. Distinct salesman names in customer for this state
        cur.execute(
            f"SELECT salesman_name, COUNT(*) AS cnt FROM customer"
            f" WHERE bde_state IN ({ph}) GROUP BY salesman_name ORDER BY cnt DESC",
            tuple(states))
        cus_salesmen = cur.fetchall()

        # 2. Distinct bde names in target_26 for this state
        cur.execute(
            f"SELECT bde, COUNT(DISTINCT ship_to) AS ships, COUNT(DISTINCT sold_to) AS soltos"
            f" FROM target_26 WHERE state IN ({ph})"
            f" GROUP BY bde ORDER BY ships DESC",
            tuple(states))
        tgt_bdes = cur.fetchall()

        # 3. Sample customer rows for state (shows ship_to, sold_to, salesman)
        cur.execute(
            f"SELECT ship_to, sold_to, salesman_name, bde_state FROM customer"
            f" WHERE bde_state IN ({ph}) LIMIT 20",
            tuple(states))
        cus_sample = cur.fetchall()

        # 4. Sample sales_2526 rows for a WA ship_to (check sold_to presence)
        cur.execute(
            f"SELECT s.ship_to, s.sold_to, s.year, SUM(s.amt) AS amt"
            f" FROM sales_2526 s"
            f" WHERE s.ship_to IN (SELECT DISTINCT ship_to FROM customer WHERE bde_state IN ({ph}))"
            f" AND s.year = 2025"
            f" GROUP BY s.ship_to, s.sold_to, s.year"
            f" LIMIT 20",
            tuple(states))
        sales_sample = cur.fetchall()

        # 5. Ship_to counts per salesman in target_26 for this state (shows split)
        cur.execute(
            f"SELECT t.bde, t.ship_to, t.sold_to, SUM(t.amt) AS tgt_amt"
            f" FROM target_26 t"
            f" WHERE t.state IN ({ph})"
            f" GROUP BY t.bde, t.ship_to, t.sold_to"
            f" LIMIT 30",
            tuple(states))
        tgt_sample = cur.fetchall()

        cur.close(); conn.close()
        return jsonify({
            "state_filter": states,
            "customer_salesmen": cus_salesmen,
            "target26_bdes": tgt_bdes,
            "customer_sample": cus_sample,
            "sales_sample": sales_sample,
            "target26_sample": tgt_sample,
        })
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500



@app.route("/")
def index():
    return app.send_static_file("index.html")

@app.route("/map")
def map_page():
    return app.send_static_file("map.html")

@app.route("/stock")
def stock_page():
    return app.send_static_file("stock.html")

# plant 醫뚰몴 留ㅽ븨 (?덇? 以?媛믪쑝濡??낅뜲?댄듃)
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
    # ALL/PCLT/TBR???꾨옒?먯꽌 蹂꾨룄 泥섎━
}
@app.get("/api/stock")
def api_stock():
    # optional query params
    # ?metric=qty|unrestricted (湲곕낯 qty)
    # ?plants=42R0,42R1 (湲곕낯 42R0~42R4)
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

    # 醫뚰몴 ?녿뒗 plant???쒖쇅
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
            wh.append("c.size LIKE %s")
            params.append(f"%{material}%")

        # category chip handling ??carrying_2602 c is already joined above
        if category == "PCLT":
            wh.append("c.line = 'PCLT'")
        elif category == "TBR":
            wh.append("c.line = 'TBR'")
        elif category == "18PLUS":
            wh.append("c.line = 'PCLT'")
            wh.append("CAST(SUBSTRING_INDEX(c.size, 'R', -1) AS DECIMAL(5,2)) >= 18.0")
        elif category == "ISEG":
            joins.append("JOIN iseg i ON CAST(TRIM(i.Material) AS UNSIGNED) = s.material")
        elif category == "SUV":
            joins.append("JOIN suv suv ON suv.Pattern = c.pattern")
        elif category == "LOWPROFILE":
            joins.append("JOIN lowprofile lp ON CAST(TRIM(lp.Material) AS UNSIGNED) = s.material")
        elif category == "HM":
            wh.append("""EXISTS (
                SELECT 1 FROM hm hm
                JOIN sales_2526 ss ON ss.sold_to = hm.sold_to
                WHERE ss.material = s.material
            )""")
        elif category == "443":
            wh.append("""EXISTS (
                SELECT 1 FROM `443_25` p443
                WHERE p443.product_group = c.product_group
            )""")
        # ALL: no extra filter

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

@app.route("/api/sales_stats")
def api_sales_stats():
    """Return 3M / 6M / 12M sales totals (qty) and their average (Base Sales)
    from sales_2526, filtered by the same category/product_group/pattern/material
    parameters used on the Stock page.
    Period boundaries are derived from the latest month present in the table.
    """
    category   = (request.args.get("category")      or "ALL").strip().upper()
    prod_group = (request.args.get("product_group") or "ALL").strip()
    pattern    = (request.args.get("pattern")       or "").strip()
    material   = (request.args.get("material")      or "").strip()

    conn = get_connection()
    cur  = conn.cursor(dictionary=True)
    try:
        # Determine the latest (year, month) present in sales_2526
        cur.execute("SELECT MAX(year*100 + month) AS ym FROM sales_2526")
        r = cur.fetchone()
        latest_ym = int((r or {}).get("ym") or 0)
        if not latest_ym:
            return jsonify({"qty_3m": 0, "qty_6m": 0, "qty_12m": 0, "base_sales": 0})

        latest_y = latest_ym // 100
        latest_m = latest_ym % 100

        def _months_back(n):
            """Return list of (year, month) tuples for the n months ending at latest."""
            result = []
            y, m = latest_y, latest_m
            for _ in range(n):
                result.append((y, m))
                m -= 1
                if m == 0:
                    m = 12
                    y -= 1
            return result

        periods_3  = _months_back(3)
        periods_6  = _months_back(6)
        periods_12 = _months_back(12)

        def _make_period_condition(periods, alias="s"):
            if not periods:
                return "1=0", []
            clauses = [f"({alias}.year=%s AND {alias}.month=%s)" for _ in periods]
            params  = [v for p in periods for v in p]
            return "(" + " OR ".join(clauses) + ")", params

        # Base joins / wheres shared across all three windows
        cat_joins, cat_wh = category_filters_sales("s", category)

        base_joins = list(cat_joins)
        base_wh    = list(cat_wh)
        base_params: list = []

        if prod_group and prod_group != "ALL":
            _ensure_carrying_join("s", base_joins)
            base_wh.append("mat.product_group = %s")
            base_params.append(prod_group)
        if pattern:
            _ensure_carrying_join("s", base_joins)
            base_wh.append("mat.pattern LIKE %s")
            base_params.append(f"%{pattern}%")
        if material:
            _ensure_carrying_join("s", base_joins)
            base_wh.append("mat.size LIKE %s")
            base_params.append(f"%{material}%")

        results = {}
        for label, periods in (("3m", periods_3), ("6m", periods_6), ("12m", periods_12)):
            period_cond, period_params = _make_period_condition(periods)
            wh_all    = base_wh + [period_cond]
            params_all = base_params + period_params
            join_sql   = "\n".join(base_joins)
            where_sql  = ("WHERE " + " AND ".join(wh_all)) if wh_all else ""
            cur.execute(f"""
                SELECT SUM(s.qty) AS qty
                FROM sales_2526 s
                {join_sql}
                {where_sql}
            """, params_all)
            row = cur.fetchone()
            total = float((row or {}).get("qty") or 0)
            n = {"3m": 3, "6m": 6, "12m": 12}[label]
            results[label] = round(total / n)

        base_sales = round((results["3m"] + results["6m"] + results["12m"]) / 3)
        return jsonify({
            "qty_3m":      results["3m"],
            "qty_6m":      results["6m"],
            "qty_12m":     results["12m"],
            "base_sales":  base_sales,
            "latest_year":  latest_y,
            "latest_month": latest_m,
        })
    finally:
        cur.close()
        conn.close()


@app.route("/api/sales_stats_by_state")
def api_sales_stats_by_state():
    """Return 3M / 6M / 12M / Base Sales + stock/water/factory qty per state."""
    # COMMON excluded intentionally
    STATE_ORDER = ["NSW", "QLD", "VIC", "WA"]
    # SA is part of VIC territory ??merge into VIC
    STATE_REMAP = {"SA": "VIC", "NT": "WA", "TAS": "VIC", "ACT": "NSW"}
    # plant -> state mapping derived from plant geographic locations
    PLANT_STATE = {"42R1": "NSW", "42R0": "QLD", "42R2": "VIC", "42R4": "WA"}
    ALL_PLANTS  = list(PLANT_STATE.keys())

    category   = (request.args.get("category")      or "ALL").strip().upper()
    prod_group = (request.args.get("product_group") or "ALL").strip()
    pattern    = (request.args.get("pattern")       or "").strip()
    material   = (request.args.get("material")      or "").strip()

    conn = get_connection()
    cur  = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT MAX(year*100 + month) AS ym FROM sales_2526")
        r = cur.fetchone()
        latest_ym = int((r or {}).get("ym") or 0)
        if not latest_ym:
            return jsonify({"rows": [], "latest_year": None, "latest_month": None})

        latest_y = latest_ym // 100
        latest_m = latest_ym % 100

        def _months_back(n):
            result = []
            y, m = latest_y, latest_m
            for _ in range(n):
                result.append((y, m))
                m -= 1
                if m == 0:
                    m = 12; y -= 1
            return result

        def _period_cond(periods, alias="s"):
            if not periods:
                return "1=0", []
            clauses = [f"({alias}.year=%s AND {alias}.month=%s)" for _ in periods]
            return "(" + " OR ".join(clauses) + ")", [v for p in periods for v in p]

        periods_3  = _months_back(3)
        periods_6  = _months_back(6)
        periods_12 = _months_back(12)

        # ?? category filter helpers ??????????????????????????????????
        def _cat_joins_wh_stock(tbl_alias):
            """Return (joins_list, wh_list, params_list) for stock/incoming/orders."""
            joins, wh, params = [], [], []
            needs_carrying = (
                (prod_group and prod_group != "ALL")
                or bool(pattern) or bool(material)
                or category in ("PCLT", "TBR", "18PLUS", "SUV", "443")
            )
            if needs_carrying:
                joins.append(f"JOIN carrying_2602 c ON c.m_code = {tbl_alias}.material")
            if prod_group and prod_group != "ALL":
                wh.append("c.product_group = %s"); params.append(prod_group)
            if pattern:
                wh.append("c.pattern LIKE %s"); params.append(f"%{pattern}%")
            if material:
                wh.append("c.size LIKE %s"); params.append(f"%{material}%")
            if category == "PCLT":
                wh.append("c.line = 'PCLT'")
            elif category == "TBR":
                wh.append("c.line = 'TBR'")
            elif category == "18PLUS":
                wh.append("c.line = 'PCLT'")
                wh.append("CAST(SUBSTRING_INDEX(c.size,'R',-1) AS DECIMAL(5,2)) >= 18.0")
            elif category == "ISEG":
                joins.append(f"JOIN iseg i ON CAST(TRIM(i.Material) AS UNSIGNED) = {tbl_alias}.material")
            elif category == "SUV":
                joins.append("JOIN suv suv ON suv.Pattern = c.pattern")
            elif category == "LOWPROFILE":
                joins.append(f"JOIN lowprofile lp ON CAST(TRIM(lp.Material) AS UNSIGNED) = {tbl_alias}.material")
            elif category == "HM":
                wh.append(f"""EXISTS (
                    SELECT 1 FROM hm hm
                    JOIN sales_2526 ss ON ss.sold_to = hm.sold_to
                    WHERE ss.material = {tbl_alias}.material
                )""")
            elif category == "443":
                wh.append("EXISTS (SELECT 1 FROM `443_25` p443 WHERE p443.product_group = c.product_group)")
            return joins, wh, params

        def _plant_totals(table, val_col, extra_wh=None):
            """Return {plant: qty} for given table and value column, grouped by plant."""
            j, wh, p = _cat_joins_wh_stock("t")
            if extra_wh:
                wh = wh + list(extra_wh)
            join_sql  = "\n".join(j)
            where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""
            cur.execute(f"""
                SELECT t.plant, SUM(t.{val_col}) AS val
                FROM {table} t
                {join_sql}
                {where_sql}
                GROUP BY t.plant
            """, p)
            return {row["plant"]: float(row["val"] or 0) for row in (cur.fetchall() or [])}

        # Exclude orders whose po_no already appears in the incoming table
        # (those shipments are already counted as incoming, not open orders)
        _orders_extra = [
            "t.po_no NOT IN (SELECT DISTINCT po_no FROM incoming"
            " WHERE po_no IS NOT NULL AND TRIM(po_no) <> '')"
        ]

        # ?? stock / water (incoming) / factory (orders) per plant ???
        stock_by_plant   = _plant_totals("stock",    "unrestricted")
        water_by_plant   = _plant_totals("incoming", "po_qty")
        factory_by_plant = _plant_totals("orders",   "po_qty", extra_wh=_orders_extra)

        # aggregate plant totals to state
        def _to_state(by_plant):
            d = {}
            for plant, val in by_plant.items():
                st = PLANT_STATE.get(plant)
                if st:
                    d[st] = d.get(st, 0) + val
            return d

        stock_by_state   = _to_state(stock_by_plant)
        water_by_state   = _to_state(water_by_plant)
        factory_by_state = _to_state(factory_by_plant)

        # ?? sales per state ?????????????????????????????????????????
        # Use a deduplicated subquery for the customer join so that ship_tos
        # with multiple rows in customer (different sold_tos) don't inflate sums.
        cat_joins_s, cat_wh_s = category_filters_sales("s", category)
        base_joins = [
            "JOIN ("
            "  SELECT ship_to, MIN(bde_state) AS bde_state"
            "  FROM customer"
            "  WHERE bde_state IS NOT NULL AND bde_state != 'COMMON'"
            "  GROUP BY ship_to"
            ") cus ON cus.ship_to = s.ship_to"
        ] + list(cat_joins_s)
        base_wh    = list(cat_wh_s)
        base_params: list = []

        if prod_group and prod_group != "ALL":
            _ensure_carrying_join("s", base_joins)
            base_wh.append("mat.product_group = %s"); base_params.append(prod_group)
        if pattern:
            _ensure_carrying_join("s", base_joins)
            base_wh.append("mat.pattern LIKE %s"); base_params.append(f"%{pattern}%")
        if material:
            _ensure_carrying_join("s", base_joins)
            base_wh.append("mat.size LIKE %s"); base_params.append(f"%{material}%")

        state_data = {}
        for label, periods in (("3m", periods_3), ("6m", periods_6), ("12m", periods_12)):
            pcond, pparams = _period_cond(periods)
            wh_all     = base_wh + [pcond]
            params_all = base_params + pparams
            join_sql   = "\n".join(base_joins)
            where_sql  = ("WHERE " + " AND ".join(wh_all)) if wh_all else ""
            cur.execute(f"""
                SELECT cus.bde_state AS state, SUM(s.qty) AS qty
                FROM sales_2526 s
                {join_sql}
                {where_sql}
                GROUP BY state
            """, params_all)
            n = {"3m": 3, "6m": 6, "12m": 12}[label]
            for row in (cur.fetchall() or []):
                st  = row["state"]
                if not st or st == "COMMON":
                    continue
                st = STATE_REMAP.get(st, st)  # SA?뭋IC, NT?뭌A, etc.
                val = round(float(row["qty"] or 0) / n)
                sd = state_data.setdefault(st, {})
                sd[label] = sd.get(label, 0) + val

        rows_out = []
        for st in STATE_ORDER:
            d = state_data.get(st, {})
            q3  = d.get("3m",  0)
            q6  = d.get("6m",  0)
            q12 = d.get("12m", 0)
            if q3 == 0 and q6 == 0 and q12 == 0:
                continue
            base = round((q3 + q6 + q12) / 3)
            rows_out.append({
                "state":       st,
                "qty_3m":      q3,
                "qty_6m":      q6,
                "qty_12m":     q12,
                "base_sales":  base,
                "stock_qty":   round(stock_by_state.get(st, 0)),
                "water_qty":   round(water_by_state.get(st, 0)),
                "factory_qty": round(factory_by_state.get(st, 0)),
            })

        return jsonify({"rows": rows_out, "latest_year": latest_y, "latest_month": latest_m})
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
                wh.append("c.size LIKE %s")
                params.append(f"%{material}%")
            if prod_group and prod_group != "ALL":
                wh.append("c.product_group = %s")
                params.append(prod_group)
            if pattern:
                wh.append("c.pattern LIKE %s")
                params.append(f"%{pattern}%")

        wh += cat_wh

        # Exclude orders whose po_no is already in the incoming table
        # (applies to both po_qty and confirmed_qty ??the row is already received)
        wh.append(
            "o.po_no NOT IN (SELECT DISTINCT po_no FROM incoming"
            " WHERE po_no IS NOT NULL AND TRIM(po_no) <> '')"
        )

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

ETA_WINDOW_DAYS = 60  # 60?쇱쓣 ?대룞 援ш컙?쇰줈 媛??(?먰븯硫?30/90?쇰줈)

def parse_date_ymd(x):
    if x is None:
        return None
    if isinstance(x, (date, datetime)):
        return x.date() if isinstance(x, datetime) else x
    s = str(x).strip()
    if not s or s.startswith("0000-00-00"):
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(s[:10], fmt).date()
        except:
            pass
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

        # ??ETA蹂??꾩튂瑜?諛붽씀?ㅻ㈃ eta_date瑜?洹몃９???ы븿?댁빞 ?댁꽌
        # origin/plant/eta_date ?⑥쐞濡?吏묎퀎(媛숈? ETA?쇰━ 臾띠엫)
        sql = f"""
            SELECT o.plant, o.origin, o.eta_date, SUM(o.{metric_col}) AS incoming_value
            FROM incoming o
        """

        if needs_carrying:
            joins.append("JOIN carrying_2602 c ON c.m_code = o.material")

        joins += cat_joins

        if material:
            wh.append("c.size LIKE %s")
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
                # ETA媛 ?놁쑝硫?以묎컙易?湲곗〈泥섎읆)
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
                    FROM sales_2526
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
    """Get Top N sold_to based on sales_2526 (baseline).
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

    # category filter (same rule as sales) ??use normalised version
    if f.get("category") != "443":
        cat_joins, cat_where = category_filters_sales("sTop", f.get("category"))
        joins += cat_joins
        wh    += cat_where

    # product_group / pattern / size all live in carrying_2602
    if (f.get("product_group") != "ALL" or f.get("pattern") != "ALL" or
        f.get("material") != "ALL"):
        _ensure_carrying_join("sTop", joins)
    if f.get("product_group") != "ALL":
        wh.append("mat.product_group = %s"); params.append(f["product_group"])
    if f.get("pattern") != "ALL":
        wh.append("mat.pattern = %s"); params.append(f["pattern"])
    if f.get("material") != "ALL":
        wh.append("mat.size = %s"); params.append(f["material"])

    where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

    sql = f"""
      SELECT sTop.sold_to
        FROM sales_2526 sTop
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

        # Product groups ??from material master (carrying_2602)
        cur.execute("""
            SELECT DISTINCT TRIM(product_group) AS v
            FROM carrying_2602
            WHERE product_group IS NOT NULL AND TRIM(product_group) <> ''
            ORDER BY TRIM(product_group)
        """)
        product_groups = [r["v"] for r in cur.fetchall() if r.get("v") is not None]

        # Patterns (filtered by product_group) ??from material master
        if pg and pg != "ALL":
            cur.execute("""
                SELECT DISTINCT TRIM(pattern) AS v
                FROM carrying_2602
                WHERE product_group = %s
                  AND pattern IS NOT NULL AND TRIM(pattern) <> ''
                ORDER BY TRIM(pattern)
            """, (pg,))
        else:
            cur.execute("""
                SELECT DISTINCT TRIM(pattern) AS v
                FROM carrying_2602
                WHERE pattern IS NOT NULL AND TRIM(pattern) <> ''
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
            SELECT DISTINCT size AS v
            FROM carrying_2602
            {w2_sql}
            ORDER BY size
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

    # group columns — for sold_to we GROUP BY the code (consistent across
    # sales and target tables) and SELECT the resolved name as label.
    group_cols_sales = {
        "product_group": "mat.product_group",
        "region":        "cus.bde_state",
        "salesman":      "cus.salesman_name",
        "sold_to_group": "cus.sold_to_group",
        "sold_to":       "s.sold_to",
        "pattern":       "mat.pattern",
    }
    group_cols_target = {
        "product_group": "mat.product_group",
        "region":        "t.state",
        "salesman":      "t.bde",
        "sold_to":       "t.sold_to",
        "pattern":       "mat.pattern",
    }
    if group_by not in group_cols_sales:
        return jsonify({"error": "invalid group_by"}), 400
    # scus / tcus = per-sold_to name resolvers — guarantee one label
    # per sold_to regardless of ship_to row joined.  See SOLD_TO_NAME_JOIN.
    label_col_sales  = ("MIN(COALESCE(scus.sold_to_name, s.sold_to))"
                       if group_by == "sold_to"
                       else f"COALESCE(NULLIF(TRIM({group_cols_sales[group_by]}),''), 'COMMON')")
    label_col_target = ("MIN(COALESCE(tcus.sold_to_name, t.sold_to))"
                       if group_by == "sold_to"
                       else f"COALESCE(NULLIF(TRIM({group_cols_target[group_by]}),''), 'COMMON')")

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
            cj, cw = category_filters_sales("s", f["category"], has_brand=True)
            joins_d += cj; wh_d += cw
        # Ensure carrying join when product_group / pattern / material (size)
        # filter or group_by needs it.  Size is stored on carrying_2602.size
        # (the dropdown value is the size string, not a material code), so
        # filtering by size requires the carrying join.
        if (group_by in ("product_group", "pattern") or
            f["product_group"] != "ALL" or f["pattern"] != "ALL" or
            f["material"] != "ALL"):
            _ensure_carrying_join("s", joins_d)
        if group_by in ("region", "salesman", "sold_to_group", "sold_to"):
            _ensure_customer_join("s", joins_d)
        if f["product_group"] != "ALL":
            wh_d.append("mat.product_group = %s"); params_d.append(f["product_group"])
        if f["pattern"] != "ALL":
            wh_d.append("mat.pattern = %s"); params_d.append(f["pattern"])
        if f["material"] != "ALL":
            wh_d.append("mat.size = %s"); params_d.append(f["material"])
        if top_sold_to:
            placeholders = ",".join(["%s"] * len(top_sold_to))
            wh_d.append(f"s.sold_to IN ({placeholders})")
            params_d.extend(top_sold_to)
        # Add per-sold_to name resolver only for breakdown stack (used below).
        SCUS_JOIN = (
            "LEFT JOIN ("
            "  SELECT sold_to, MIN(NULLIF(TRIM(sold_to_name),'')) AS sold_to_name "
            "  FROM customer GROUP BY sold_to"
            ") scus ON scus.sold_to = s.sold_to"
        )
        joins_d_break = list(joins_d) + ([SCUS_JOIN] if group_by == "sold_to" else [])
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
            SELECT s.day AS day, {label_col_sales} AS group_label, SUM(s.{value}) AS value
            FROM sales_thismonth s
            {' '.join(joins_d_break)}
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
        joins_t, wh_t, params_t = build_target_filters("t", f)
        tj, tw = category_target_filters("t", f["category"])
        joins_t += tj; wh_t += tw
        wh_t.append("t.month = %s"); params_t.append(month)
        carrying_join_t = "LEFT JOIN carrying_2602 mat ON mat.m_code = t.material"
        needs_carrying_t = False
        if f["product_group"] != "ALL":
            needs_carrying_t = True
            wh_t.append("mat.product_group = %s"); params_t.append(f["product_group"])
        if f["pattern"] != "ALL":
            needs_carrying_t = True
            wh_t.append("mat.pattern = %s"); params_t.append(f["pattern"])
        if f["material"] != "ALL":
            needs_carrying_t = True
            wh_t.append("mat.size = %s"); params_t.append(f["material"])
        if needs_carrying_t and carrying_join_t not in joins_t:
            joins_t.append(carrying_join_t)
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

        # ---------------- monthly (sales_2526 years 2025/2026 + target_26) ----------------
        joins_m, wh_m, params_m = build_customer_filters("s", f, use_sold_to_name=False)
        mj, mw = category_filters_sales("s", f["category"])
        joins_m += mj; wh_m += mw
        if (group_by in ("product_group", "pattern") or
            f["product_group"] != "ALL" or f["pattern"] != "ALL" or
            f["material"] != "ALL"):
            _ensure_carrying_join("s", joins_m)
        if group_by in ("region", "salesman", "sold_to_group", "sold_to"):
            _ensure_customer_join("s", joins_m)
        if f["product_group"] != "ALL":
            wh_m.append("mat.product_group = %s"); params_m.append(f["product_group"])
        if f["pattern"] != "ALL":
            wh_m.append("mat.pattern = %s"); params_m.append(f["pattern"])
        if f["material"] != "ALL":
            wh_m.append("mat.size = %s"); params_m.append(f["material"])
        wh_m.append("s.year IN (2025, 2026)")
        if top_sold_to:
            placeholders = ",".join(["%s"] * len(top_sold_to))
            wh_m.append(f"s.sold_to IN ({placeholders})")
            params_m.extend(top_sold_to)
        joins_m_break = list(joins_m) + ([SCUS_JOIN] if group_by == "sold_to" else [])
        where_m = ("WHERE " + " AND ".join(wh_m)) if wh_m else ""

        cur.execute(f"""
            SELECT s.year AS year, s.month AS month, SUM(s.{value}) AS value
            FROM sales_2526 s
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
            SELECT s.year AS year, s.month AS month, {label_col_sales} AS group_label, SUM(s.{value}) AS value
            FROM sales_2526 s
            {' '.join(joins_m_break)}
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
        joins_mt, wh_mt, params_mt = build_target_filters("t", f)
        tj2, tw2 = category_target_filters("t", f["category"])
        joins_mt += tj2; wh_mt += tw2
        # carrying_2602 needed for product_group/pattern (not stored in target_26 directly)
        carrying_join_mt = "LEFT JOIN carrying_2602 mat ON mat.m_code = t.material"
        needs_carrying_mt = group_by in ("product_group", "pattern")
        if f["product_group"] != "ALL":
            needs_carrying_mt = True
            wh_mt.append("mat.product_group = %s"); params_mt.append(f["product_group"])
        if f["pattern"] != "ALL":
            needs_carrying_mt = True
            wh_mt.append("mat.pattern = %s"); params_mt.append(f["pattern"])
        if f["material"] != "ALL":
            needs_carrying_mt = True
            wh_mt.append("mat.size = %s"); params_mt.append(f["material"])
        if needs_carrying_mt and carrying_join_mt not in joins_mt:
            joins_mt.append(carrying_join_mt)
        if top_sold_to:
            placeholders = ",".join(["%s"] * len(top_sold_to))
            wh_mt.append(f"t.sold_to IN ({placeholders})")
            params_mt.extend(top_sold_to)
        if group_by == "sold_to":
            joins_mt.append(
                "LEFT JOIN ("
                "  SELECT sold_to, MIN(NULLIF(TRIM(sold_to_name),'')) AS sold_to_name "
                "  FROM customer GROUP BY sold_to"
                ") tcus ON tcus.sold_to = t.sold_to"
            )
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
            SELECT t.month AS month, {label_col_target} AS group_label, SUM(t.{value}) AS value
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
            yj, yw = category_filters_sales("s", f["category"])
            joins_y += yj; wh_y += yw
        if (group_by in ("product_group", "pattern") or
            f["product_group"] != "ALL" or f["pattern"] != "ALL" or
            f["material"] != "ALL"):
            _ensure_carrying_join("s", joins_y)
        if group_by in ("region", "salesman", "sold_to_group", "sold_to"):
            _ensure_customer_join("s", joins_y)
        if f["product_group"] != "ALL":
            wh_y.append("mat.product_group = %s"); params_y.append(f["product_group"])
        if f["pattern"] != "ALL":
            wh_y.append("mat.pattern = %s"); params_y.append(f["pattern"])
        if f["material"] != "ALL":
            wh_y.append("mat.size = %s"); params_y.append(f["material"])
        if top_sold_to:
            placeholders = ",".join(["%s"] * len(top_sold_to))
            wh_y.append(f"s.sold_to IN ({placeholders})")
            params_y.extend(top_sold_to)
        wh_y.append("s.year BETWEEN 2021 AND 2025")
        joins_y_break = list(joins_y) + ([SCUS_JOIN] if group_by == "sold_to" else [])
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
            SELECT s.year AS year, {label_col_sales} AS group_label, SUM(s.{value}) AS value
            FROM sales_21_25 s
            {' '.join(joins_y_break)}
            {where_y}
            GROUP BY s.year, {group_col_y}
            ORDER BY s.year
        """, tuple(params_y))
        y_break = cur.fetchall()
        # NOTE: years are not 1..N indexes; map manually
        y_groups = sorted({(r.get("group_label") or "").strip() or "COMMON" for r in y_break}, key=lambda g: (_region_order_key(g), g) if group_by=="region" else (g.upper(), g))
        y_by2: Dict[str, List[float]] = {g: [0.0]*len(yearly_labels) for g in y_groups}
        year_idx = {y:i for i,y in enumerate(yearly_labels)}
        for r in y_break:
            g = (r.get("group_label") or "").strip() or "COMMON"
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
@cached_endpoint(60)
def daily_sales():
    f = parse_filters(request)
    value = "qty" if f["metric"] == "qty" else "amt"

    # 0 or missing = no top filter
    top_limit = int(request.args.get("top_limit", 0) or 0)

    joins, wh, params = build_customer_filters("s", f, use_sold_to_name=False)

    # category ??use normalised version for sales tables
    if f["category"] != "443":
        cat_joins, cat_where = category_filters_sales("s", f["category"], has_brand=True)
        joins += cat_joins
        wh    += cat_where

    # product_group / pattern / size all live in carrying_2602 (alias: mat)
    if f["product_group"] != "ALL" or f["pattern"] != "ALL" or f["material"] != "ALL":
        _ensure_carrying_join("s", joins)
    if f["product_group"] != "ALL":
        wh.append("mat.product_group = %s")
        params.append(f["product_group"])
    if f["pattern"] != "ALL":
        wh.append("mat.pattern = %s")
        params.append(f["pattern"])
    if f["material"] != "ALL":
        wh.append("mat.size = %s")
        params.append(f["material"])
    base_where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        top_sold_to = None

        # 1) Get Top N sold_to from baseline table (sales_2526)
        if top_limit > 0:
            top_sold_to = get_top_sold_to_from_baseline(
                cur, f, top_limit, value
            )

            if not top_sold_to:
                # no matching customers ??all days = 0
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
    return jsonify([{"day": d, "value": day_map.get(d, 0)} for d in range(1, 32)])
     

#
# -------------------- Daily breakdown (stacked by group) -------------------
@app.get("/api/daily_breakdown")
@cached_endpoint(60)
def daily_breakdown():

    f = parse_filters(request)
    value = "qty" if f["metric"] == "qty" else "amt"
    # 0 or missing = no top filter
    top_limit = int(request.args.get("top_limit", 0) or 0)

    # Which dimension to group by?
    group_by = (request.args.get("group_by") or "region").strip()
    group_cols = {
        "product_group": "mat.product_group",
        "region":        "cus.bde_state",
        "salesman":      "cus.salesman_name",
        "sold_to_group": "cus.sold_to_group",
        "sold_to":       "cus.sold_to_name",
        "pattern":       "mat.pattern",
    }
    if group_by not in group_cols:
        return jsonify({"error": "invalid group_by"}), 400
    group_col = group_cols[group_by]

    # ---- Build base JOINs / WHEREs (same as daily_sales) ----
    joins, wh, params = build_customer_filters("s", f, use_sold_to_name=False)

    if f["category"] != "443":
        cat_joins, cat_where = category_filters_sales("s", f["category"], has_brand=True)
        joins += cat_joins
        wh    += cat_where

    # Carrying/customer join needed for group_by or filter
    if (group_by in ("product_group", "pattern") or
        f["product_group"] != "ALL" or f["pattern"] != "ALL" or
        f["material"] != "ALL"):
        _ensure_carrying_join("s", joins)
    if group_by in ("region", "salesman", "sold_to_group", "sold_to"):
        _ensure_customer_join("s", joins)
    if f["product_group"] != "ALL":
        wh.append("mat.product_group = %s"); params.append(f["product_group"])
    if f["pattern"] != "ALL":
        wh.append("mat.pattern = %s");       params.append(f["pattern"])
    if f["material"] != "ALL":
        wh.append("mat.size = %s")
        params.append(f["material"])
    base_where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        top_sold_to = None

        # 1) Get Top N sold_to from baseline table (sales_2526)
        if top_limit > 0:
            top_sold_to = get_top_sold_to_from_baseline(
                cur, f, top_limit, value
            )

            # no matching customers ??nothing to show
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
                COALESCE(NULLIF(TRIM({group_col}),''), 'COMMON') AS group_label,
                SUM(s.{value}) AS value
            FROM sales_thismonth s
            {' '.join(joins)}
            {where_sql2}
        GROUP BY s.day, COALESCE(NULLIF(TRIM({group_col}),''), 'COMMON')
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
    month = int(request.args.get("month", 2))

    # 0 or missing = no top filter
    top_limit = int(request.args.get("top_limit", 0) or 0)

    joins, wh, params = build_target_filters("t", f)
    cat_joins, cat_where = category_target_filters("t", f["category"])
    joins += cat_joins
    wh    += cat_where

    # Apply product_group / pattern / size filters via carrying_2602 join.
    carrying_join_dt = "LEFT JOIN carrying_2602 mat ON mat.m_code = t.material"
    if (f["product_group"] != "ALL" or f["pattern"] != "ALL" or f["material"] != "ALL"):
        if carrying_join_dt not in joins:
            joins.append(carrying_join_dt)
    if f["product_group"] != "ALL":
        wh.append("mat.product_group = %s"); params.append(f["product_group"])
    if f["pattern"] != "ALL":
        wh.append("mat.pattern = %s"); params.append(f["pattern"])
    if f["material"] != "ALL":
        wh.append("mat.size = %s"); params.append(f["material"])

    # restrict to the chosen month only
    wh.append("t.month = %s")
    params.append(month)

    base_where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

    conn = get_connection()
    cur  = conn.cursor(dictionary=True)
    try:
        top_sold_to = None

        # 1) Get Top N sold_to from baseline table (sales_2526)
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
    days_in_month = calendar.monthrange(2026, month)[1]

    # ?? Determine working days ????????????????????????????????????????????????
    # Past days: working if company-wide total sales >= 10 (same threshold as frontend)
    # Future days: working if weekday (Mon?밊ri)
    conn2 = get_connection()
    cur2  = conn2.cursor(dictionary=True)
    try:
        cur2.execute("""
            SELECT day, SUM(qty) AS total_qty
            FROM sales_thismonth
            GROUP BY day
            ORDER BY day
        """)
        sales_by_day = {int(r["day"]): float(r["total_qty"] or 0) for r in cur2.fetchall()}
    finally:
        cur2.close()
        conn2.close()

    max_known_day = max(sales_by_day.keys()) if sales_by_day else 0

    from datetime import date as _date
    working_days = set()
    for d in range(1, days_in_month + 1):
        if d <= max_known_day:
            # past day: working if total company sales >= 10
            if sales_by_day.get(d, 0) >= 10:
                working_days.add(d)
        else:
            # future day: working if Mon?밊ri
            if _date(2026, month, d).weekday() < 5:
                working_days.add(d)

    total_working_days = len(working_days)
    daily_value = monthly_total / total_working_days if total_working_days else 0

    # return one entry per day: working days get daily_value, non-working days get 0
    return jsonify([
        {"day": d, "value": daily_value if d in working_days else 0}
        for d in range(1, days_in_month + 1)
    ])

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

    # 1) 留??꾩뿉 ?좏깮 議곌굔 ?쒖떆 (?듭뀡)
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

    # ?ㅻ뜑 ?ㅼ쓬 以꾨줈 freeze
    ws.freeze_panes = f"A{header_row+1}"

    autosize_columns(ws)
    # meta_lines in col A/B can be very long ??cap to keep sheet tidy
    if meta_lines:
        ws.column_dimensions['A'].width = min(ws.column_dimensions['A'].width, 10)
        ws.column_dimensions['B'].width = min(ws.column_dimensions['B'].width, 18)
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
    _ensure_customer_join("s", joins)  # always needed: SELECT uses cus.* columns

    # category (same rule: skip 443) ??use normalised version
    if f.get("category", "ALL") != "443":
        cat_joins, cat_where = category_filters_sales("s", f.get("category", "ALL"), has_brand=True)
        joins += cat_joins
        wh    += cat_where

    if (f.get("product_group", "ALL") != "ALL" or
        f.get("pattern", "ALL") != "ALL" or
        f.get("material", "ALL") != "ALL"):
        _ensure_carrying_join("s", joins)
    if f.get("product_group", "ALL") != "ALL":
        wh.append("mat.product_group = %s"); params.append(f["product_group"])
    if f.get("pattern", "ALL") != "ALL":
        wh.append("mat.pattern = %s"); params.append(f["pattern"])
    if f.get("material", "ALL") != "ALL":
        wh.append("mat.size = %s"); params.append(f["material"])

    # top filter (sold_to 湲곗?)
    if top_pairs:
        conds = []
        for p in top_pairs:
            conds.append("s.sold_to = %s")
            params.append(p["sold_to"])
        wh.append("(" + " OR ".join(conds) + ")")

    where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

    sql = f"""
        SELECT
            COALESCE(NULLIF(TRIM(cus.bde_state),''), 'COMMON') AS region,
            COALESCE(NULLIF(TRIM(cus.salesman_name),''), '') AS bde,
            COALESCE(NULLIF(TRIM(cus.sold_to_group),''), '') AS sold_to_group,
            COALESCE(NULLIF(TRIM(cus.sold_to_name),''), s.sold_to) AS sold_to_name,
            COALESCE(NULLIF(TRIM(cus.ship_to_name),''), s.ship_to) AS ship_to_name,
            s.sold_to AS sold_to_code,
            s.ship_to AS ship_to_code,
            {day_cols}
        FROM sales_thismonth s
        {' '.join(joins)}
        {where_sql}
        GROUP BY region, bde, sold_to_group, sold_to_name, ship_to_name, s.sold_to, s.ship_to
        ORDER BY region DESC, bde DESC
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
        value_col = "qty" if metric == "qty" else "amt"

        rows = fetch_table_rows(top_limit=top_limit)

        # Sort: NSW?뭂LD?뭋IC?뭌A, then BDE, then sold_to_name
        _REGION_ORDER = {"NSW": 0, "QLD": 1, "VIC": 2, "WA": 3}
        rows.sort(key=lambda r: (
            _REGION_ORDER.get((r.get("region") or "").upper(), 99),
            (r.get("bde") or "").lower(),
            (r.get("sold_to_name") or "").lower(),
        ))

        day_labels = [str(d) for d in range(1, 32)]
        for r in rows:
            r["Total"] = sum(float(r.get(c) or 0) for c in day_labels)

        # ?? Fetch monthly target ??ship_to level, fall back to sold_to ??
        conn2 = get_connection(); cur2 = conn2.cursor(dictionary=True)
        try:
            cur2.execute("SELECT MAX(year*100+month) AS ym FROM sales_thismonth")
            ym = int((cur2.fetchone() or {}).get("ym") or 0)
            cur_month = ym % 100 if ym else datetime.now().month
            # Fetch both ship_to and sold_to targets in one query
            cur2.execute(
                f"SELECT sold_to, ship_to, SUM({value_col}) AS tgt "
                f"FROM target_26 WHERE month=%s GROUP BY sold_to, ship_to",
                (cur_month,)
            )
            target_by_ship   = {}   # ship_to  -> target
            target_by_sold_to = {}  # sold_to  -> target
            for r2 in cur2.fetchall():
                st  = str(r2["ship_to"]  or "")
                so  = str(r2["sold_to"]  or "")
                tgt = float(r2["tgt"] or 0)
                if st:
                    target_by_ship[st]    = target_by_ship.get(st, 0) + tgt
                if so:
                    target_by_sold_to[so] = target_by_sold_to.get(so, 0) + tgt
        except Exception:
            target_by_ship = {}; target_by_sold_to = {}
        finally:
            cur2.close(); conn2.close()

        # Count ship_tos per sold_to (for proportional fallback)
        ship_count = {}
        for r in rows:
            sc = str(r.get("sold_to_code") or "")
            ship_count[sc] = ship_count.get(sc, 0) + 1

        for r in rows:
            ship = str(r.get("ship_to_code")  or "")
            sold = str(r.get("sold_to_code")  or "")
            # Use ship_to target if available; otherwise split sold_to target evenly
            tgt = target_by_ship.get(ship, 0)
            if tgt == 0 and sold in target_by_sold_to:
                cnt = ship_count.get(sold, 1)
                tgt = target_by_sold_to[sold] / cnt if cnt else 0
            r["Target"] = round(tgt, 1)
            r["Ach%"]   = round(r["Total"] / tgt * 100, 1) if tgt > 0 else None

        # ?? Pre-calculate state totals (sum sold_to targets once per sold_to) ??
        counted_sold_tos = set()
        state_totals = {}
        for r in rows:
            st = (r.get("region") or "").upper()
            sc = str(r.get("sold_to_code") or "")
            g  = state_totals.setdefault(st, {"Total": 0.0, "Target": 0.0})
            g["Total"] += r["Total"]
            # Add sold_to target only once per sold_to to avoid double-counting
            if sc not in counted_sold_tos:
                g["Target"] += r["Target"]
                counted_sold_tos.add(sc)

        # ?? Achievement color (matches KPI table) ????????????????????????
        def _ach_color(val):
            if val is None: return None
            if val >= 100:  return "16a34a"   # green
            if val >= 90:   return "f97316"   # orange
            return "dc2626"                    # red

        # ?? Build workbook ???????????????????????????????????????????????
        wb  = Workbook()
        ws  = wb.active
        ws.title = "sales_thismonth_by_day"

        # Meta info
        for line in [
            f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"metric={metric}, top_limit={top_limit if top_limit else 'ALL'}",
            f"category={f.get('category','ALL')}, region={f.get('region','ALL')}, salesman={f.get('salesman','ALL')}, sold_to_group={f.get('sold_to_group','ALL')}",
            f"product_group={f.get('product_group','ALL')}, pattern={f.get('pattern','ALL')}, material={f.get('material','ALL')}",
            f"sold_to={f.get('sold_to','ALL')}, ship_to={f.get('ship_to','ALL')}",
        ]:
            ws.append([line])
        ws.append([])

        # Header row: Total/Target/Ach% sit between ship_to_code and day columns
        HDR = (["region","bde","sold_to_group","sold_to_name","ship_to_name",
                "sold_to_code","ship_to_code","Total","Target","Ach%"]
               + day_labels)
        ws.append(HDR)
        hdr_row = ws.max_row
        for c, h in enumerate(HDR, 1):
            cell = ws.cell(row=hdr_row, column=c)
            cell.font      = Font(bold=True)
            cell.alignment = Alignment(horizontal="center", vertical="center")
        ws.freeze_panes = f"A{hdr_row+1}"

        ACH_COL = HDR.index("Ach%") + 1   # 1-based column index

        GREY_FILL = PatternFill("solid", fgColor="D9D9D9")

        def _append_state_summary(region):
            g    = state_totals.get(region, {})
            tot  = g.get("Total",  0.0)
            tgt  = g.get("Target", 0.0)
            ach  = round(tot / tgt * 100, 1) if tgt > 0 else None
            vals = ([region, "", "", f"?? {region} TOTAL ??", "", "",
                     "", round(tot,1), round(tgt,1),
                     (f"{ach:.1f}%" if ach is not None else "-")]
                    + [""] * 31)
            ws.append(vals)
            sr = ws.max_row
            for c in range(1, len(HDR)+1):
                ws.cell(row=sr, column=c).font = Font(bold=True)
                ws.cell(row=sr, column=c).fill = GREY_FILL
            if ach is not None:
                ws.cell(row=sr, column=ACH_COL).font = Font(bold=True, color=_ach_color(ach))

        current_region = None
        for r in rows:
            region = (r.get("region") or "").upper()
            if region != current_region:
                current_region = region
                _append_state_summary(region)

            # Build row values
            vals = []
            for h in HDR:
                if h == "Ach%":
                    v = r.get("Ach%")
                    vals.append(f"{v:.1f}%" if v is not None else "-")
                else:
                    vals.append(r.get(h))
            ws.append(vals)

            # Colour the Ach% cell
            ach_val = r.get("Ach%")
            if ach_val is not None:
                ws.cell(row=ws.max_row, column=ACH_COL).font = Font(color=_ach_color(ach_val))

        # Column widths
        autosize_columns(ws)
        ws.column_dimensions["A"].width = min(ws.column_dimensions["A"].width, 10)
        ws.column_dimensions["B"].width = min(ws.column_dimensions["B"].width, 18)

        bio = BytesIO()
        wb.save(bio); bio.seek(0)
        stamp    = datetime.now().strftime("%Y%m%d_%H%M")
        filename = f"sales_thismonth_top{top_limit if top_limit else 'ALL'}_{metric}_{stamp}.xlsx"
        return send_file(bio, as_attachment=True, download_name=filename,
                         mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    except Exception as e:
        print("export_excel error:", repr(e))
        print(traceback.format_exc())
        return jsonify({"error": "Internal Server Error", "message": str(e)}), 500


def _build_export_common_filters(f, joins, wh, params, alias="s"):
    """Apply category, product_group, pattern, material filters shared by all export endpoints."""
    if f.get("category", "ALL") != "ALL":
        cat_joins, cat_where = category_filters_sales(alias, f.get("category", "ALL"))
        joins += cat_joins
        wh    += cat_where

    if f.get("product_group", "ALL") != "ALL" or f.get("pattern", "ALL") != "ALL":
        _ensure_carrying_join(alias, joins)
    if f.get("product_group", "ALL") != "ALL":
        wh.append("mat.product_group = %s"); params.append(f["product_group"])
    if f.get("pattern", "ALL") != "ALL":
        wh.append("mat.pattern = %s"); params.append(f["pattern"])
    if f.get("material", "ALL") != "ALL":
        wh.append(f"{alias}.material = %s"); params.append(f["material"])


@app.get("/api/export_excel/sales2526")
def export_excel_sales2526():
    """Export 25/26 monthly sales pivoted by YYMM (2501..2512, 2601..2612)."""
    try:
        f = parse_filters(request)
        metric = f.get("metric", "qty")
        value_col = "qty" if metric == "qty" else "amt"

        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        joins, wh, params = build_customer_filters("s", f, use_sold_to_name=False)
        _ensure_customer_join("s", joins)
        _build_export_common_filters(f, joins, wh, params)

        # pivot columns: year*100+month ??label YYMM e.g. 2501
        pivot_cols = ",\n".join([
            f"SUM(CASE WHEN s.year={y} AND s.month={m} THEN s.{value_col} ELSE 0 END) AS `{y % 100:02d}{m:02d}`"
            for y in [2025, 2026]
            for m in range(1, 13)
        ])
        col_labels = [f"{y % 100:02d}{m:02d}" for y in [2025, 2026] for m in range(1, 13)]

        where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""
        sql = f"""
            SELECT
                COALESCE(NULLIF(TRIM(cus.bde_state),''), 'COMMON') AS region,
                COALESCE(NULLIF(TRIM(cus.salesman_name),''), '') AS bde,
                COALESCE(NULLIF(TRIM(cus.sold_to_group),''), '') AS sold_to_group,
                COALESCE(NULLIF(TRIM(cus.sold_to_name),''), s.sold_to) AS sold_to_name,
                COALESCE(NULLIF(TRIM(cus.ship_to_name),''), s.ship_to) AS ship_to_name,
                s.sold_to AS sold_to_code,
                s.ship_to AS ship_to_code,
                {pivot_cols}
            FROM sales_2526 s
            {' '.join(joins)}
            {where_sql}
            GROUP BY region, bde, sold_to_group, sold_to_name, ship_to_name, s.sold_to, s.ship_to
            ORDER BY region DESC, bde DESC
        """
        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close(); conn.close()

        # add Total column
        for r in rows:
            r["Total"] = sum(float(r.get(c) or 0) for c in col_labels)

        header_order = ["region", "bde", "sold_to_group", "sold_to_name", "ship_to_name",
                        "sold_to_code", "ship_to_code"] + col_labels + ["Total"]

        meta_lines = [
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"metric={metric}, category={f.get('category','ALL')}, region={f.get('region','ALL')}",
            f"salesman={f.get('salesman','ALL')}, sold_to_group={f.get('sold_to_group','ALL')}",
            f"product_group={f.get('product_group','ALL')}, pattern={f.get('pattern','ALL')}, material={f.get('material','ALL')}",
            f"sold_to={f.get('sold_to','ALL')}, ship_to={f.get('ship_to','ALL')}",
        ]

        wb = build_excel(rows, sheet_name="25_26_Sales", header_order=header_order, meta_lines=meta_lines)
        bio = BytesIO(); wb.save(bio); bio.seek(0)
        stamp = datetime.now().strftime("%Y%m%d_%H%M")
        return send_file(bio, as_attachment=True,
                         download_name=f"sales_25_26_{metric}_{stamp}.xlsx",
                         mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    except Exception as e:
        print("export_excel_sales2526 error:", repr(e)); print(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.get("/api/export_excel/yearly")
def export_excel_yearly():
    """Export yearly sales (2021-2025) pivoted by year."""
    try:
        f = parse_filters(request)
        metric = f.get("metric", "qty")
        value_col = "qty" if metric == "qty" else "amt"

        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        joins, wh, params = build_customer_filters("s", f, use_sold_to_name=False)
        _ensure_customer_join("s", joins)
        _build_export_common_filters(f, joins, wh, params)

        years = [2021, 2022, 2023, 2024, 2025]
        col_labels = [str(y % 100) for y in years]
        pivot_cols = ",\n".join([
            f"SUM(CASE WHEN s.year={y} THEN s.{value_col} ELSE 0 END) AS `{y % 100}`"
            for y in years
        ])

        where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""
        sql = f"""
            SELECT
                COALESCE(NULLIF(TRIM(cus.bde_state),''), 'COMMON') AS region,
                COALESCE(NULLIF(TRIM(cus.salesman_name),''), '') AS bde,
                COALESCE(NULLIF(TRIM(cus.sold_to_group),''), '') AS sold_to_group,
                COALESCE(NULLIF(TRIM(cus.sold_to_name),''), s.sold_to) AS sold_to_name,
                COALESCE(NULLIF(TRIM(cus.ship_to_name),''), s.ship_to) AS ship_to_name,
                s.sold_to AS sold_to_code,
                s.ship_to AS ship_to_code,
                {pivot_cols}
            FROM sales_21_25 s
            {' '.join(joins)}
            {where_sql}
            GROUP BY region, bde, sold_to_group, sold_to_name, ship_to_name, s.sold_to, s.ship_to
            ORDER BY region DESC, bde DESC
        """
        cur.execute(sql, params)
        rows = cur.fetchall()
        cur.close(); conn.close()

        for r in rows:
            r["Total"] = sum(float(r.get(c) or 0) for c in col_labels)

        header_order = ["region", "bde", "sold_to_group", "sold_to_name", "ship_to_name",
                        "sold_to_code", "ship_to_code"] + col_labels + ["Total"]

        meta_lines = [
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"metric={metric}, category={f.get('category','ALL')}, region={f.get('region','ALL')}",
            f"salesman={f.get('salesman','ALL')}, sold_to_group={f.get('sold_to_group','ALL')}",
            f"product_group={f.get('product_group','ALL')}, pattern={f.get('pattern','ALL')}, material={f.get('material','ALL')}",
            f"sold_to={f.get('sold_to','ALL')}, ship_to={f.get('ship_to','ALL')}",
        ]

        wb = build_excel(rows, sheet_name="Yearly_Sales", header_order=header_order, meta_lines=meta_lines)
        bio = BytesIO(); wb.save(bio); bio.seek(0)
        stamp = datetime.now().strftime("%Y%m%d_%H%M")
        return send_file(bio, as_attachment=True,
                         download_name=f"sales_yearly_{metric}_{stamp}.xlsx",
                         mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    except Exception as e:
        print("export_excel_yearly error:", repr(e)); print(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


        # ----------------------------- Monthly Sales ---------------------------------
@app.get("/api/monthly_sales")
@cached_endpoint(60)
def monthly_sales():
    f = parse_filters(request)
    value = "qty" if f["metric"] == "qty" else "amt"

    # Year filter (default: current year if not provided)
    year = int(request.args.get("year", 2025) or 2025)

    # 0 or missing = no top filter
    top_limit = int(request.args.get("top_limit", 0) or 0)

    joins, wh, params = build_customer_filters("s", f, use_sold_to_name=False)

    # category ??normalised version
    cat_joins, cat_where = category_filters_sales("s", f["category"])
    joins += cat_joins
    wh    += cat_where

    if f["product_group"] != "ALL" or f["pattern"] != "ALL" or f["material"] != "ALL":
        _ensure_carrying_join("s", joins)
    if f["product_group"] != "ALL":
        wh.append("mat.product_group = %s")
        params.append(f["product_group"])
    if f["pattern"] != "ALL":
        wh.append("mat.pattern = %s")
        params.append(f["pattern"])
    if f["material"] != "ALL":
        wh.append("mat.size = %s")
        params.append(f["material"])

    # year condition
    wh.append("s.year = %s")
    params.append(year)

    base_where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        top_sold_to = None

        # 1) Get Top N sold_to from baseline table (sales_2526)
        if top_limit > 0:
            top_sold_to = get_top_sold_to_from_baseline(
                cur, f, top_limit, value
            )

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
            FROM sales_2526 s
            {' '.join(joins)}
            {where_sql2}
        GROUP BY s.month
        ORDER BY s.month
        """
        cur.execute(monthly_sql, tuple(params2))
        rows = cur.fetchall()

    finally:
        try: cur.close()
        except: pass
        try: conn.close()
        except: pass

    month_map = {int(r["month_num"]): float(r["monthly_total"] or 0) for r in rows}
    return jsonify([{"month": m, "value": month_map.get(m, 0)} for m in range(1, 13)])


# -------------------- Monthly breakdown (stacked by group) -------------------
@app.get("/api/monthly_breakdown")
@cached_endpoint(60)
def monthly_breakdown():
    f = parse_filters(request)
    value = "qty" if f["metric"] == "qty" else "amt"

    # Year filter (default: 2025)
    year = int(request.args.get("year", 2025) or 2025)

    # 0 or missing = no top filter
    top_limit = int(request.args.get("top_limit", 0) or 0)

    # Which dimension to group by?
    group_by = (request.args.get("group_by") or "region").strip()
    # For sold_to: GROUP BY the code (s.sold_to) so sales and target rows
    # match by the same key, but SELECT the resolved name as the label
    # so the legend is readable.
    group_cols = {
        "product_group": "mat.product_group",
        "region":        "cus.bde_state",
        "salesman":      "cus.salesman_name",
        "sold_to_group": "cus.sold_to_group",
        "sold_to":       "s.sold_to",
        "pattern":       "mat.pattern",
    }
    if group_by not in group_cols:
        return jsonify({"error": "invalid group_by"}), 400
    group_col = group_cols[group_by]
    if group_by == "sold_to":
        # scus alias = customer aggregated per-sold_to. Lets us resolve a
        # consistent name regardless of which ship_to a row points at.
        label_col = "MIN(COALESCE(scus.sold_to_name, s.sold_to))"
    else:
        label_col = f"COALESCE(NULLIF(TRIM({group_col}),''), 'COMMON')"

    # ---- Build base JOINs / WHEREs (same pattern as monthly_sales) ----
    joins, wh, params = build_customer_filters("s", f, use_sold_to_name=False)
    cat_joins, cat_where = category_filters_sales("s", f["category"])
    joins += cat_joins
    wh    += cat_where

    if (group_by in ("product_group", "pattern") or
        f["product_group"] != "ALL" or f["pattern"] != "ALL" or
        f["material"] != "ALL"):
        _ensure_carrying_join("s", joins)
    if group_by in ("region", "salesman", "sold_to_group", "sold_to"):
        _ensure_customer_join("s", joins)
    # scus = per-sold_to name resolver (one row per sold_to). Used as
    # the label source when group_by == 'sold_to' so the resulting
    # legend doesn't have a mix of names and raw codes for the same
    # sold_to depending on which ship_to row was joined.
    if group_by == "sold_to":
        joins.append(
            "LEFT JOIN ("
            "  SELECT sold_to, MIN(NULLIF(TRIM(sold_to_name),'')) AS sold_to_name "
            "  FROM customer GROUP BY sold_to"
            ") scus ON scus.sold_to = s.sold_to"
        )
    if f["product_group"] != "ALL":
        wh.append("mat.product_group = %s"); params.append(f["product_group"])
    if f["pattern"] != "ALL":
        wh.append("mat.pattern = %s");       params.append(f["pattern"])
    if f["material"] != "ALL":
        wh.append("mat.size = %s");          params.append(f["material"])

    # Year condition
    wh.append("s.year = %s"); params.append(year)

    base_where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

    conn = get_connection()
    cur  = conn.cursor(dictionary=True)
    try:
        top_sold_to = None

        # 1) Get Top N sold_to from baseline table (sales_2526)
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
                {label_col} AS group_label,
                SUM(s.{value}) AS value
            FROM sales_2526 s
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

    return jsonify(rows)


# ----------------------------- Monthly Target ---------------------------------
@app.get("/api/monthly_target")
@cached_endpoint(60)
def monthly_target():
    f = parse_filters(request)
    value = "qty" if f["metric"] == "qty" else "amt"

    # 0 or missing = no top filter
    top_limit = int(request.args.get("top_limit", 0) or 0)

    joins, wh, params = build_target_filters("t", f)
    cat_joins, cat_where = category_target_filters("t", f["category"])
    joins += cat_joins
    wh    += cat_where

    # Apply product_group / pattern / size filters via carrying_2602 join.
    carrying_join_mt = "LEFT JOIN carrying_2602 mat ON mat.m_code = t.material"
    if (f["product_group"] != "ALL" or f["pattern"] != "ALL" or f["material"] != "ALL"):
        if carrying_join_mt not in joins:
            joins.append(carrying_join_mt)
    if f["product_group"] != "ALL":
        wh.append("mat.product_group = %s"); params.append(f["product_group"])
    if f["pattern"] != "ALL":
        wh.append("mat.pattern = %s"); params.append(f["pattern"])
    if f["material"] != "ALL":
        wh.append("mat.size = %s"); params.append(f["material"])

    conn = get_connection()
    cur  = conn.cursor(dictionary=True)
    try:
        top_sold_to = None

        # 1) Get Top N sold_to from baseline table (sales_2526)
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
    return jsonify([{"month": m, "value": month_map.get(m, 0)} for m in range(1, 13)])


# ------------------------- Monthly Target Breakdown --------------------------
from mysql.connector import Error as MySQLError

@app.get("/api/monthly_target_breakdown")
@cached_endpoint(60)
def monthly_target_breakdown():
    f = parse_filters(request)
    value = "qty" if f["metric"] == "qty" else "amt"

    top_limit = int(request.args.get("top_limit", 0) or 0)

    group_by = (request.args.get("group_by") or "region").strip()
    # For sold_to: GROUP BY the code (stable, matches sales) but SELECT
    # the name (readable in the legend).  Other dimensions group + label
    # on the same column.
    group_cols = {
        "product_group": "mat.product_group",
        "region":        "t.state",
        "salesman":      "t.bde",
        "sold_to":       "t.sold_to",
        "pattern":       "mat.pattern",
    }
    if group_by not in group_cols:
        return jsonify({"error": "invalid group_by"}), 400
    group_col = group_cols[group_by]
    # label_col: what shows up in the legend; for sold_to we resolve to name.
    if group_by == "sold_to":
        label_col = "MIN(COALESCE(tcus.sold_to_name, t.sold_to))"
    else:
        label_col = group_col

    joins, wh, params = build_target_filters("t", f)
    cat_joins, cat_where = category_target_filters("t", f["category"])
    joins += cat_joins
    wh    += cat_where

    if group_by == "sold_to":
        joins.append(
            "LEFT JOIN ("
            "  SELECT sold_to, MIN(NULLIF(TRIM(sold_to_name),'')) AS sold_to_name "
            "  FROM customer GROUP BY sold_to"
            ") tcus ON tcus.sold_to = t.sold_to"
        )

    # carrying_2602 join needed for group_by or filter on product_group/pattern
    carrying_join = "LEFT JOIN carrying_2602 mat ON mat.m_code = t.material"
    needs_carrying = group_by in ("product_group", "pattern")

    if f["product_group"] != "ALL":
        needs_carrying = True
        wh.append("mat.product_group = %s"); params.append(f["product_group"])
    if f["pattern"] != "ALL":
        needs_carrying = True
        wh.append("mat.pattern = %s");       params.append(f["pattern"])
    if f["material"] != "ALL":
        needs_carrying = True
        wh.append("mat.size = %s");          params.append(f["material"])

    if needs_carrying and carrying_join not in joins:
        joins.append(carrying_join)

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
                {label_col} AS group_label,
                SUM(t.{value}) AS value
            FROM target_26 t
            {' '.join(joins)}
            {where_sql2}
            GROUP BY t.month, {group_col}
            ORDER BY t.month
        """

        cur.execute(sql, tuple(params2))
        return jsonify(cur.fetchall())

    except MySQLError as e:
        # ?꾨줎?몄뿉??諛붾줈 ?먯씤 蹂댁씠?꾨줉 ?대젮以?(?댁쁺?대㈃ msg留??쒓굅?섍퀬 濡쒓렇濡쒕쭔 ?④린湲?
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
@cached_endpoint(60)
def yearly_sales():
    f = parse_filters(request)
    value = "qty" if f["metric"] == "qty" else "amt"

    # 0 or missing = no top filter
    top_limit = int(request.args.get("top_limit", 0) or 0)

    joins, wh, params = build_customer_filters("s", f, use_sold_to_name=False)

    if f["category"] != "443":
        cat_joins, cat_where = category_filters_sales("s", f["category"])
        joins += cat_joins
        wh    += cat_where

    if f["product_group"] != "ALL" or f["pattern"] != "ALL" or f["material"] != "ALL":
        _ensure_carrying_join("s", joins)
    if f["product_group"] != "ALL":
        wh.append("mat.product_group = %s")
        params.append(f["product_group"])
    if f["pattern"] != "ALL":
        wh.append("mat.pattern = %s")
        params.append(f["pattern"])
    if f["material"] != "ALL":
        wh.append("mat.size = %s")
        params.append(f["material"])
    base_where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        top_sold_to = None

        # 1) If top_limit > 0, get top N sold_to from baseline (sales_2526)
        if top_limit > 0:
            top_sold_to = get_top_sold_to_from_baseline(
                cur, f, top_limit, value
            )

            if not top_sold_to:
                # no data ??return zeros for all years in range
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
            FROM sales_21_25 s
            {' '.join(joins)}
            {where_sql2}
        GROUP BY s.year
        ORDER BY s.year
        """
        cur.execute(yearly_sql, tuple(params2))
        rows = cur.fetchall()

    finally:
        try: cur.close()
        except: pass
        try: conn.close()
        except: pass

    year_map = {int(r["year_num"]): float(r["yearly_total"] or 0) for r in rows}
    return jsonify([{"year": y, "value": year_map.get(y, 0)} for y in range(2021, 2026)])


# -------------------- Yearly breakdown (stacked by group) -------------------
@app.get("/api/yearly_breakdown")
@cached_endpoint(60)
def yearly_breakdown():

    f = parse_filters(request)
    value = "qty" if f["metric"] == "qty" else "amt"

    # 0 or missing = no top filter
    top_limit = int(request.args.get("top_limit", 0) or 0)

    # Which dimension to group by?
    group_by = (request.args.get("group_by") or "region").strip()
    # For sold_to: group by code, label as name (consistent with the
    # other breakdown endpoints).
    group_cols = {
        "product_group": "mat.product_group",
        "region":        "cus.bde_state",
        "salesman":      "cus.salesman_name",
        "sold_to_group": "cus.sold_to_group",
        "sold_to":       "s.sold_to",
        "pattern":       "mat.pattern",
    }
    if group_by not in group_cols:
        return jsonify({"error": "invalid group_by"}), 400
    group_col = group_cols[group_by]
    if group_by == "sold_to":
        label_col = "MIN(COALESCE(scus.sold_to_name, s.sold_to))"
    else:
        label_col = f"COALESCE(NULLIF(TRIM({group_col}),''), 'COMMON')"

    # ---- Build base JOINs / WHEREs (same pattern as yearly_sales) ----
    joins, wh, params = build_customer_filters("s", f, use_sold_to_name=False)
    if f["category"] != "443":
        cat_joins, cat_where = category_filters_sales("s", f["category"])
        joins += cat_joins
        wh    += cat_where
    if group_by == "sold_to":
        joins.append(
            "LEFT JOIN ("
            "  SELECT sold_to, MIN(NULLIF(TRIM(sold_to_name),'')) AS sold_to_name "
            "  FROM customer GROUP BY sold_to"
            ") scus ON scus.sold_to = s.sold_to"
        )

    if (group_by in ("product_group", "pattern") or
        f["product_group"] != "ALL" or f["pattern"] != "ALL" or
        f["material"] != "ALL"):
        _ensure_carrying_join("s", joins)
    if group_by in ("region", "salesman", "sold_to_group", "sold_to"):
        _ensure_customer_join("s", joins)
    if f["product_group"] != "ALL":
        wh.append("mat.product_group = %s"); params.append(f["product_group"])
    if f["pattern"] != "ALL":
        wh.append("mat.pattern = %s");       params.append(f["pattern"])
    if f["material"] != "ALL":
        wh.append("mat.size = %s")
        params.append(f["material"])
    base_where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

    conn = get_connection()
    cur  = conn.cursor(dictionary=True)
    try:
        top_sold_to = None

        # 1) If top_limit > 0, get top N sold_to from baseline (sales_2526)
        if top_limit > 0:
            top_sold_to = get_top_sold_to_from_baseline(
                cur, f, top_limit, value
            )

            # no data ??nothing to show
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
                {label_col} AS group_label,
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
        # ----------------- 1) No top_limit -----------------
        # Only show sold_tos that have actual sales (avoids showing every
        # inactive account in the customer master ??far too many entries).
        if top_limit <= 0:
            params_s: list = []
            extra_wh = (
                "AND c.sold_to_group = %s" if parent != "ALL" else ""
            )
            if parent != "ALL":
                params_s.append(parent)
            cur.execute(f"""
                SELECT DISTINCT TRIM(c.sold_to_name) AS name
                  FROM customer c
                 WHERE c.sold_to IN (
                       SELECT DISTINCT sold_to FROM sales_2526
                        WHERE sold_to IS NOT NULL
                 )
                   {extra_wh}
                   AND c.sold_to_name IS NOT NULL
                   AND TRIM(c.sold_to_name) <> ''
                 ORDER BY TRIM(c.sold_to_name)
            """, params_s)
            rows = cur.fetchall()
            return jsonify([r["name"] for r in rows])

        # ----------------- 2) top_limit -> baseline top sold_to -> names -----------------
        # Get top sold_to list from baseline table (sales_2526)
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
    # Top N filter (same baseline as the rest of the dashboard) — when set,
    # restricts the ship_to list to those belonging to the top sold_to set.
    top_limit = int(request.args.get("top_limit", 0) or 0)

    try:
        conn = get_connection(); cur = conn.cursor(dictionary=True)

        # Resolve top sold_to set first (if a top filter is active)
        top_sold_to = None
        if top_limit > 0:
            f = parse_filters(request)
            value = "qty" if f["metric"] == "qty" else "amt"
            top_sold_to = get_top_sold_to_from_baseline(cur, f, top_limit, value)
            if not top_sold_to:
                cur.close(); conn.close()
                return jsonify([])

        where = ["ship_to_name IS NOT NULL", "TRIM(ship_to_name) <> ''"]
        params = []

        if sold_to.upper() != "ALL":
            where.append("TRIM(sold_to_name) = %s")
            params.append(sold_to)
        elif stg3.upper() != "ALL":
            where.append("TRIM(sold_to_group) = %s")
            params.append(stg3)

        if top_sold_to:
            placeholders = ",".join(["%s"] * len(top_sold_to))
            where.append(f"sold_to IN ({placeholders})")
            params.extend(top_sold_to)

        where_sql = "WHERE " + " AND ".join(where)

        cur.execute(f"""
            SELECT DISTINCT ship_to, TRIM(ship_to_name) AS ship_to_name
            FROM customer
            {where_sql}
            ORDER BY TRIM(ship_to_name)
        """, tuple(params))

        rows_out = [{"code": str(r["ship_to"]), "name": r["ship_to_name"]} for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify(rows_out)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/api/product_group")
def product_group():
    try:
        conn = get_connection(); cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT TRIM(product_group)
            FROM carrying_2602
            WHERE product_group IS NOT NULL AND TRIM(product_group) <> ''
            ORDER BY TRIM(product_group)
        """)
        groups = [r[0] for r in cur.fetchall()]
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
                FROM carrying_2602
                WHERE product_group = %s
                  AND pattern IS NOT NULL AND TRIM(pattern) <> ''
                ORDER BY TRIM(pattern)
            """, (product_group,))
        else:
            cur.execute("""
                SELECT DISTINCT TRIM(pattern)
                FROM carrying_2602
                WHERE pattern IS NOT NULL AND TRIM(pattern) <> ''
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
    - ?꾪꽣: product_group, pattern (????'ALL' ?대㈃ ?꾩껜)
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
            SELECT DISTINCT size
            FROM carrying_2602
            {where_sql}
            ORDER BY size
        """, tuple(params))

        names = [r[0] for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify(names)

    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.get("/api/carrying_price")
def carrying_price():
    """Return avg list_price and purchase_price from carrying_2602 for current filter."""
    product_group = (request.args.get("product_group") or "ALL").strip()
    pattern       = (request.args.get("pattern")       or "").strip()
    material      = (request.args.get("material")      or "").strip()

    try:
        conn = get_connection()
        cur  = conn.cursor(dictionary=True)

        where  = []
        params = []

        if product_group and product_group != "ALL":
            where.append("product_group = %s")
            params.append(product_group)
        if pattern:
            where.append("pattern LIKE %s")
            params.append(f"%{pattern}%")
        if material:
            where.append("size LIKE %s")
            params.append(f"%{material}%")

        where_sql = ("WHERE " + " AND ".join(where)) if where else ""

        cur.execute(f"""
            SELECT AVG(NULLIF(list_price, 0)) AS list_price,
                   AVG(NULLIF(purchase_price, 0)) AS purchase_price
            FROM carrying_2602
            {where_sql}
        """, tuple(params))

        row = cur.fetchone()
        cur.close(); conn.close()

        return jsonify({
            "list_price":      float(row["list_price"])      if row and row["list_price"]      is not None else None,
            "purchase_price":  float(row["purchase_price"])  if row and row["purchase_price"]  is not None else None,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/api/profit_monthly")
def profit_monthly():
    import traceback

    try:
        f = parse_filters(request)
        value = "qty" if f.get("metric") == "qty" else "amt"
        top_limit = int(request.args.get("top_limit", 0) or 0)

        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        try:
            top_sold_to = None

            # 1) Top-N sold_to from baseline sales table
            if top_limit > 0:
                top_sold_to = get_top_sold_to_from_baseline(cur, f, top_limit, value)
                if not top_sold_to:
                    return jsonify([
                        dict(month=m, gross=0, sd=0, cogs=0, op_cost=0)
                        for m in range(1, 13)
                    ])

            # 2) Build filters for the new `profit` table.
            #    profit has: month, sold_to, material, gross, sales_deduction, cogs, operating_cost, profit
            #    NO ship_to column ??ship_to filter resolves to sold_to via customer subquery.
            joins_p  = []
            wh_p     = []
            params_p = []

            # ?? region filter (using EXISTS to avoid JOIN inflation) ??
            if f["region"] != "ALL":
                states = REGION_STATES.get(f["region"].upper(), [f["region"]])
                ph = ",".join(["%s"] * len(states))
                wh_p.append(
                    f"EXISTS (SELECT 1 FROM customer c2 WHERE c2.sold_to = p.sold_to"
                    f" AND c2.bde_state IN ({ph}))"
                )
                params_p.extend(states)

            # ?? salesman filter ??
            if f["salesman"] != "ALL":
                wh_p.append(
                    "EXISTS (SELECT 1 FROM customer c2 WHERE c2.sold_to = p.sold_to"
                    " AND UPPER(TRIM(c2.salesman_name)) = UPPER(TRIM(%s)))"
                )
                params_p.append(f["salesman"])

            # ?? sold_to_group filter ??
            if f["sold_to_group"] != "ALL":
                wh_p.append(
                    "EXISTS (SELECT 1 FROM customer c2 WHERE c2.sold_to = p.sold_to"
                    " AND c2.sold_to_group = %s)"
                )
                params_p.append(f["sold_to_group"])

            # ?? sold_to filter (directly on p.sold_to) ??
            if f["sold_to"] != "ALL":
                sv = f["sold_to"]
                if sv.isdigit() or sv.upper().startswith("A"):
                    wh_p.append("p.sold_to = %s"); params_p.append(sv)
                else:
                    wh_p.append(
                        "p.sold_to IN (SELECT DISTINCT sold_to FROM customer WHERE sold_to_name = %s)"
                    )
                    params_p.append(sv)

            # ?? ship_to filter: no ship_to in profit ??resolve to its sold_to ??
            if f["ship_to"] != "ALL":
                st = f["ship_to"].strip()
                if st.isdigit() or st.upper().startswith("A"):
                    wh_p.append(
                        "p.sold_to IN (SELECT DISTINCT sold_to FROM customer WHERE ship_to = %s)"
                    )
                    params_p.append(st)
                else:
                    wh_p.append(
                        "p.sold_to IN (SELECT DISTINCT sold_to FROM customer"
                        " WHERE UPPER(TRIM(ship_to_name)) = UPPER(TRIM(%s)))"
                    )
                    params_p.append(st)

            # ?? category filter (via carrying_2602, same as sales tables) ??
            cat_joins_p, cat_where_p = category_filters_sales("p", f.get("category", "ALL"))
            joins_p += cat_joins_p
            wh_p    += cat_where_p

            # ?? product_group / pattern filter (via carrying_2602) ??
            if f.get("product_group", "ALL") != "ALL" or f.get("pattern", "ALL") != "ALL" or f.get("material", "ALL") != "ALL":
                _ensure_carrying_join("p", joins_p)
            if f.get("product_group", "ALL") != "ALL":
                wh_p.append("mat.product_group = %s"); params_p.append(f["product_group"])
            if f.get("pattern", "ALL") != "ALL":
                wh_p.append("mat.pattern = %s"); params_p.append(f["pattern"])
            if f["material"] != "ALL":
                wh_p.append("mat.size = %s"); params_p.append(f["material"])

            # ?? top sold_to restriction ??
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

    # category ??normalised version
    if f["category"] != "443":
        cat_joins, cat_where = category_filters_sales("s", f["category"])
        joins += cat_joins
        wh    += cat_where

    if (f["product_group"] != "ALL" or f["pattern"] != "ALL" or
        f["material"] != "ALL"):
        _ensure_carrying_join("s", joins)
    if f["product_group"] != "ALL":
        wh.append("mat.product_group = %s")
        params.append(f["product_group"])

    if f["pattern"] != "ALL":
        wh.append("mat.pattern = %s")
        params.append(f["pattern"])

    if f["material"] != "ALL":
        wh.append("mat.size = %s")
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
                SUM(s.{value})        AS total_value,
                SUM(CASE WHEN s.year = 2026 THEN s.{value} ELSE 0 END) AS total_2026
            FROM sales_2526 s
            {' '.join(joins)}
            {where_sql2}
            GROUP BY
                c.ship_to,
                c.ship_to_name,
                c.latitude,
                c.longitude
            ORDER BY total_2026 DESC
        """

        cur.execute(map_sql, tuple(params2))
        rows = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    return jsonify(rows)

# ── GPS Visit count API ────────────────────────────────────────────────────────

# Candidate column names — the real schema is whatever the gps table actually
# uses; we pick the first one that exists so the endpoint isn't brittle.
_GPS_DATE_CANDIDATES = (
    "local_date", "date", "visit_date", "gps_date",
    "recorded_at", "created_at", "timestamp", "ts", "datetime",
)
_GPS_LAT_CANDIDATES = ("latitude", "lat", "y")
_GPS_LNG_CANDIDATES = ("longitude", "lng", "lon", "long", "x")
_GPS_SALESMAN_CANDIDATES = ("salesmen", "salesman", "salesman_name", "rep", "bde", "registration")

# BDE names to hide from the visit_summary by_bde table (e.g. test
# entries, old vehicles, names that aren't actual sales reps).
# Match is case/whitespace-insensitive.
BDE_EXCLUDE = frozenset({
    "JO TEDDY",
    "CHO JUNJONG",
})

def _resolve_gps_salesman_col(cur):
    """Optional column — returns the salesman/registration column name or
    None if the gps schema doesn't include one."""
    if USE_SQLITE:
        cur.execute("PRAGMA table_info(gps)")
        cols = [r["name"] if isinstance(r, dict) else r[1] for r in cur.fetchall()]
    else:
        cur.execute("SHOW COLUMNS FROM gps")
        cols = [r["Field"] for r in cur.fetchall()]
    cols_lc = {c.lower(): c for c in cols}
    for c in _GPS_SALESMAN_CANDIDATES:
        if c in cols_lc:
            return cols_lc[c]
    return None

# Australian national public holidays — used to exclude non-business days
# from visit counts.  State-specific holidays (Labour Day, Show Day, etc.)
# are intentionally not included to keep the rule consistent across
# territories. Add years here as needed.
AU_HOLIDAYS = frozenset({
    # 2025
    "2025-01-01", "2025-01-27", "2025-04-18", "2025-04-21",
    "2025-04-25", "2025-06-09", "2025-12-25", "2025-12-26",
    # 2026
    "2026-01-01", "2026-01-26", "2026-04-03", "2026-04-06",
    "2026-04-25", "2026-06-08", "2026-12-25", "2026-12-28",
    # 2027
    "2027-01-01", "2027-01-26", "2027-03-26", "2027-03-29",
    "2027-04-26", "2027-06-14", "2027-12-27", "2027-12-28",
})

def _business_day_filter_sql(date_col):
    """SQL fragment + params to keep only Mon–Fri non-holiday rows.
    MySQL DAYOFWEEK: 1=Sun, 7=Sat."""
    placeholders = ",".join(["%s"] * len(AU_HOLIDAYS))
    sql = (f"DAYOFWEEK({date_col}) NOT IN (1, 7) "
           f"AND DATE({date_col}) NOT IN ({placeholders})")
    return sql, list(AU_HOLIDAYS)

def _is_business_day(d):
    """d: date or datetime. True if Mon–Fri and not in AU_HOLIDAYS."""
    if d is None:
        return False
    if d.weekday() >= 5:  # 5=Sat, 6=Sun
        return False
    return d.strftime("%Y-%m-%d") not in AU_HOLIDAYS

def _resolve_gps_columns(cur):
    """Return (date_col, lat_col, lng_col) by inspecting the gps table.
    Raises RuntimeError if the table or required columns are missing."""
    if USE_SQLITE:
        cur.execute("PRAGMA table_info(gps)")
        cols = [r["name"] if isinstance(r, dict) else r[1] for r in cur.fetchall()]
    else:
        cur.execute("SHOW COLUMNS FROM gps")
        cols = [r["Field"] for r in cur.fetchall()]
    if not cols:
        raise RuntimeError("gps table has no columns or does not exist")
    cols_lc = {c.lower(): c for c in cols}
    def pick(cands, label):
        for c in cands:
            if c in cols_lc:
                return cols_lc[c]
        raise RuntimeError(f"no {label} column found in gps; tried {cands}; have {cols}")
    return pick(_GPS_DATE_CANDIDATES, "date"), pick(_GPS_LAT_CANDIDATES, "lat"), pick(_GPS_LNG_CANDIDATES, "lng")


@app.get("/api/monthly_visits")
def monthly_visits():
    """
    Count GPS visits within 300m of a given lat/lng per month.
    A 'visit' = at least one GPS record on a calendar day within the radius.
    Params: lat, lng, year, [radius=300]
    Returns: [{"m": 1, "visits": 3}, ...]   (m = month number 1-12)
    """
    try:
        lat = float(request.args.get("lat", ""))
        lng = float(request.args.get("lng", ""))
    except (TypeError, ValueError):
        return jsonify([])

    try:
        year = int(request.args.get("year", 2026) or 2026)
    except (TypeError, ValueError):
        year = 2026

    try:
        radius_m = float(request.args.get("radius", 300) or 300)
    except (TypeError, ValueError):
        radius_m = 300.0
    business_only = (request.args.get("business_days_only", "1").strip().lower()
                     not in ("0", "false", "no"))

    # bbox pre-filter sized from the radius (~111 km per degree lat,
    # ~96 km per degree lng at 30°S)
    LAT_D = (radius_m / 111000.0) * 1.2
    LNG_D = (radius_m / 96000.0)  * 1.2

    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
    except Exception as e:
        print("monthly_visits: DB connect failed:", e)
        return jsonify([])

    try:
        date_col, lat_col, lng_col = _resolve_gps_columns(cur)
        if USE_SQLITE:
            sql = f"""
                SELECT CAST(strftime('%m', {date_col}) AS INTEGER) AS m,
                       COUNT(DISTINCT date({date_col}))            AS visits
                FROM   gps
                WHERE  strftime('%Y', {date_col}) = ?
                  AND  {lat_col} BETWEEN ? AND ?
                  AND  {lng_col} BETWEEN ? AND ?
                GROUP  BY m
                ORDER  BY m
            """
            cur.execute(sql, [str(year),
                              lat - LAT_D, lat + LAT_D,
                              lng - LNG_D, lng + LNG_D])
        else:
            extra_sql = ""
            extra_params = []
            if business_only:
                bd_sql, bd_params = _business_day_filter_sql(date_col)
                extra_sql = f"  AND {bd_sql}\n"
                extra_params = bd_params
            sql = f"""
                SELECT MONTH({date_col})                  AS m,
                       COUNT(DISTINCT DATE({date_col}))   AS visits
                FROM   gps
                WHERE  YEAR({date_col}) = %s
                  AND  {lat_col} BETWEEN %s AND %s
                  AND  {lng_col} BETWEEN %s AND %s
                  AND  (6371000 * ACOS(LEAST(1.0,
                           COS(RADIANS(%s)) * COS(RADIANS({lat_col}))
                           * COS(RADIANS({lng_col}) - RADIANS(%s))
                           + SIN(RADIANS(%s)) * SIN(RADIANS({lat_col}))
                       ))) <= %s
                {extra_sql}
                GROUP  BY MONTH({date_col})
                ORDER  BY m
            """
            cur.execute(sql, [year,
                              lat - LAT_D, lat + LAT_D,
                              lng - LNG_D, lng + LNG_D,
                              lat, lng, lat, radius_m,
                              *extra_params])
        rows = cur.fetchall() or []
    except Exception as e:
        print("monthly_visits: query failed:", e)
        rows = []
    finally:
        try: cur.close()
        except Exception: pass
        try: conn.close()
        except Exception: pass

    return jsonify([{"m": r["m"], "visits": r["visits"]} for r in rows])


@app.get("/api/visit_for_shop")
def visit_for_shop():
    """Convenience: look up a shop's lat/lng by ship_to and return its
    monthly visit counts.  Saves you from copying coordinates manually.
    Params: ship_to (required), [year=2026], [radius=300]
    """
    ship_to = (request.args.get("ship_to") or "").strip()
    if not ship_to:
        return jsonify({"error": "ship_to required"})
    try:
        year = int(request.args.get("year", 2026) or 2026)
    except (TypeError, ValueError):
        year = 2026
    try:
        radius_m = float(request.args.get("radius", 300) or 300)
    except (TypeError, ValueError):
        radius_m = 300.0
    business_only = (request.args.get("business_days_only", "1").strip().lower()
                     not in ("0", "false", "no"))

    out = {"ship_to": ship_to, "year": year, "radius_m": radius_m,
           "business_days_only": business_only}
    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT ship_to_name, latitude, longitude FROM customer "
            "WHERE ship_to = %s LIMIT 1",
            [ship_to],
        )
        row = cur.fetchone()
        if not row:
            return jsonify({**out, "error": "ship_to not found in customer"})
        if row["latitude"] is None or row["longitude"] is None:
            return jsonify({**out, "ship_to_name": row["ship_to_name"],
                            "error": "shop has no lat/lng in customer table"})

        lat = float(row["latitude"])
        lng = float(row["longitude"])
        out["ship_to_name"] = row["ship_to_name"]
        out["shop_lat"] = lat
        out["shop_lng"] = lng

        date_col, lat_col, lng_col = _resolve_gps_columns(cur)
        LAT_D = (radius_m / 111000.0) * 1.2
        LNG_D = (radius_m / 96000.0)  * 1.2

        if USE_SQLITE:
            cur.execute(
                f"SELECT CAST(strftime('%m', {date_col}) AS INTEGER) AS m, "
                f"COUNT(DISTINCT date({date_col})) AS visits FROM gps "
                f"WHERE strftime('%Y', {date_col}) = ? "
                f"AND {lat_col} BETWEEN ? AND ? AND {lng_col} BETWEEN ? AND ? "
                f"GROUP BY m ORDER BY m",
                [str(year), lat-LAT_D, lat+LAT_D, lng-LNG_D, lng+LNG_D],
            )
        else:
            extra_sql = ""
            extra_params = []
            if business_only:
                bd_sql, bd_params = _business_day_filter_sql(date_col)
                extra_sql = f"AND {bd_sql} "
                extra_params = bd_params
            cur.execute(
                f"SELECT MONTH({date_col}) AS m, "
                f"COUNT(DISTINCT DATE({date_col})) AS visits, "
                f"GROUP_CONCAT(DISTINCT registration ORDER BY registration) AS regos "
                f"FROM gps WHERE YEAR({date_col}) = %s "
                f"AND {lat_col} BETWEEN %s AND %s AND {lng_col} BETWEEN %s AND %s "
                f"AND (6371000 * ACOS(LEAST(1.0, "
                f"COS(RADIANS(%s))*COS(RADIANS({lat_col}))"
                f"*COS(RADIANS({lng_col}) - RADIANS(%s)) "
                f"+ SIN(RADIANS(%s))*SIN(RADIANS({lat_col}))))) <= %s "
                f"{extra_sql}"
                f"GROUP BY MONTH({date_col}) ORDER BY m",
                [year, lat-LAT_D, lat+LAT_D, lng-LNG_D, lng+LNG_D,
                 lat, lng, lat, radius_m, *extra_params],
            )
        out["monthly_visits"] = cur.fetchall()
        out["total_visit_days"] = sum(r["visits"] for r in out["monthly_visits"])
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"
    finally:
        try: cur.close()
        except Exception: pass
        try: conn.close()
        except Exception: pass
    return jsonify(out)


@app.get("/api/visit_debug")
def visit_debug():
    """Diagnose why /api/monthly_visits returns nothing for a shop.
    Params: lat, lng, [year=2026]
    Returns table existence, resolved column names, sample rows, bbox/radius
    match counts at several radii, and per-month visit counts.
    """
    out = {"params": dict(request.args)}
    try:
        lat = float(request.args.get("lat", ""))
        lng = float(request.args.get("lng", ""))
    except (TypeError, ValueError):
        out["error"] = "lat/lng required as floats"
        return jsonify(out)
    try:
        year = int(request.args.get("year", 2026) or 2026)
    except (TypeError, ValueError):
        year = 2026

    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
    except Exception as e:
        return jsonify({**out, "error": f"db connect: {e}"})

    try:
        # Table existence + column list
        if USE_SQLITE:
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND lower(name)='gps'")
        else:
            cur.execute("SHOW TABLES LIKE 'gps'")
        out["gps_table_exists"] = bool(cur.fetchall())
        if not out["gps_table_exists"]:
            return jsonify(out)

        if USE_SQLITE:
            cur.execute("PRAGMA table_info(gps)")
            out["columns"] = [r["name"] if isinstance(r, dict) else r[1] for r in cur.fetchall()]
        else:
            cur.execute("SHOW COLUMNS FROM gps")
            out["columns"] = [{"name": r["Field"], "type": r["Type"]} for r in cur.fetchall()]

        try:
            date_col, lat_col, lng_col = _resolve_gps_columns(cur)
            out["resolved"] = {"date": date_col, "lat": lat_col, "lng": lng_col}
        except Exception as e:
            out["error"] = str(e)
            return jsonify(out)

        cur.execute(f"SELECT COUNT(*) AS n FROM gps")
        out["total_rows"] = cur.fetchone()["n"]

        cur.execute(f"SELECT * FROM gps LIMIT 3")
        out["sample"] = cur.fetchall()

        if USE_SQLITE:
            cur.execute(f"SELECT DISTINCT strftime('%Y', {date_col}) AS y FROM gps ORDER BY y")
        else:
            cur.execute(f"SELECT DISTINCT YEAR({date_col}) AS y FROM gps ORDER BY y")
        out["years_in_table"] = [r["y"] for r in cur.fetchall()]

        # Match counts at increasing radii
        match_counts = {}
        for r_m in (100, 250, 500, 1000, 2500, 5000):
            LAT_D = (r_m / 111000.0) * 1.2
            LNG_D = (r_m / 96000.0)  * 1.2
            if USE_SQLITE:
                cur.execute(
                    f"SELECT COUNT(*) AS n FROM gps "
                    f"WHERE strftime('%Y', {date_col}) = ? "
                    f"AND {lat_col} BETWEEN ? AND ? AND {lng_col} BETWEEN ? AND ?",
                    [str(year), lat-LAT_D, lat+LAT_D, lng-LNG_D, lng+LNG_D],
                )
            else:
                cur.execute(
                    f"SELECT COUNT(*) AS n FROM gps "
                    f"WHERE YEAR({date_col}) = %s "
                    f"AND {lat_col} BETWEEN %s AND %s AND {lng_col} BETWEEN %s AND %s "
                    f"AND (6371000 * ACOS(LEAST(1.0, "
                    f"COS(RADIANS(%s))*COS(RADIANS({lat_col}))"
                    f"*COS(RADIANS({lng_col}) - RADIANS(%s)) "
                    f"+ SIN(RADIANS(%s))*SIN(RADIANS({lat_col}))))) <= %s",
                    [year, lat-LAT_D, lat+LAT_D, lng-LNG_D, lng+LNG_D, lat, lng, lat, r_m],
                )
            match_counts[f"{r_m}m"] = cur.fetchone()["n"]
        out["match_counts_by_radius"] = match_counts

        # Geographic spread of the gps table — answers "does the data even
        # cover this region of Australia?"
        if not USE_SQLITE:
            cur.execute(
                f"SELECT MIN({lat_col}) AS lat_min, MAX({lat_col}) AS lat_max, "
                f"MIN({lng_col}) AS lng_min, MAX({lng_col}) AS lng_max FROM gps"
            )
            out["bbox_in_table"] = cur.fetchone()

            # Nearest 5 GPS points to the requested location, any year
            cur.execute(
                f"SELECT {date_col} AS d, {lat_col} AS lat, {lng_col} AS lng, "
                f"  registration, "
                f"  ROUND(6371000 * ACOS(LEAST(1.0, "
                f"    COS(RADIANS(%s))*COS(RADIANS({lat_col}))"
                f"    *COS(RADIANS({lng_col}) - RADIANS(%s)) "
                f"    + SIN(RADIANS(%s))*SIN(RADIANS({lat_col})) "
                f"  )), 0) AS dist_m "
                f"FROM gps "
                f"ORDER BY dist_m ASC LIMIT 5",
                [lat, lng, lat],
            )
            out["nearest_points_any_year"] = cur.fetchall()
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"
    finally:
        try: cur.close()
        except Exception: pass
        try: conn.close()
        except Exception: pass

    return jsonify(out)


@app.get("/api/visit_debug_topshops")
def visit_debug_topshops():
    """List the top customer ship-tos by GPS visit count for a year, so we can
    confirm the Visit line renders for at least one shop and identify which
    territories actually have GPS coverage.
    Params: year=2026, radius=300, limit=20
    """
    try:
        year = int(request.args.get("year", 2026) or 2026)
    except (TypeError, ValueError):
        year = 2026
    try:
        radius_m = float(request.args.get("radius", 300) or 300)
    except (TypeError, ValueError):
        radius_m = 300.0
    try:
        limit = int(request.args.get("limit", 20) or 20)
    except (TypeError, ValueError):
        limit = 20

    if USE_SQLITE:
        # Skip — need trig, only Haversine works on MySQL here
        return jsonify({"note": "available only on MySQL"})

    out = {"params": {"year": year, "radius_m": radius_m, "limit": limit}}
    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        date_col, lat_col, lng_col = _resolve_gps_columns(cur)

        # For each customer, count distinct visit-days where any GPS sample
        # lies within radius_m of the ship-to location.  Bbox prefilter on
        # both customer and gps cuts the join down dramatically.
        LAT_D = (radius_m / 111000.0) * 1.2
        LNG_D = (radius_m / 96000.0)  * 1.2
        sql = f"""
            SELECT c.ship_to,
                   c.ship_to_name,
                   c.latitude  AS shop_lat,
                   c.longitude AS shop_lng,
                   COUNT(DISTINCT DATE(g.{date_col})) AS visits
            FROM   customer c
            JOIN   gps g
              ON   g.{lat_col} BETWEEN c.latitude  - %s AND c.latitude  + %s
              AND  g.{lng_col} BETWEEN c.longitude - %s AND c.longitude + %s
              AND  YEAR(g.{date_col}) = %s
              AND  (6371000 * ACOS(LEAST(1.0,
                       COS(RADIANS(c.latitude))*COS(RADIANS(g.{lat_col}))
                       *COS(RADIANS(g.{lng_col}) - RADIANS(c.longitude))
                       + SIN(RADIANS(c.latitude))*SIN(RADIANS(g.{lat_col}))
                   ))) <= %s
            WHERE  c.latitude IS NOT NULL AND c.longitude IS NOT NULL
            GROUP  BY c.ship_to, c.ship_to_name, c.latitude, c.longitude
            ORDER  BY visits DESC
            LIMIT  %s
        """
        cur.execute(sql, [LAT_D, LAT_D, LNG_D, LNG_D, year, radius_m, limit])
        out["top_shops"] = cur.fetchall()
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"
    finally:
        try: cur.close()
        except Exception: pass
        try: conn.close()
        except Exception: pass
    return jsonify(out)


@app.get("/api/visit_debug_closest")
def visit_debug_closest():
    """Find the absolute closest customer-to-GPS distance — answers
    'is there a single customer that any salesperson got near at all?'.
    Also returns row counts per year so we can rule out the year filter,
    and the gps lat/lng range to spot coordinate-system issues.

    Strategy: load 30k gps points + a sample of customers into Python and
    do the nearest-neighbour scan in-memory.  A single SQL JOIN over
    customer × gps with bbox prefilter would time out without indexes
    on gps.latitude/longitude.
    Params: [sample=300] customers to scan, [limit=20]
    """
    if USE_SQLITE:
        return jsonify({"note": "available only on MySQL"})
    try:
        sample_n = int(request.args.get("sample", 300) or 300)
    except (TypeError, ValueError):
        sample_n = 300
    try:
        limit = int(request.args.get("limit", 20) or 20)
    except (TypeError, ValueError):
        limit = 20

    out = {"params": {"sample": sample_n, "limit": limit}}
    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        date_col, lat_col, lng_col = _resolve_gps_columns(cur)

        # Year distribution
        cur.execute(
            f"SELECT YEAR({date_col}) AS y, COUNT(*) AS n "
            f"FROM gps GROUP BY YEAR({date_col}) ORDER BY n DESC LIMIT 20"
        )
        out["rows_per_year_top20"] = cur.fetchall()

        # Bounding boxes side by side
        cur.execute(
            f"SELECT MIN({lat_col}) AS lat_min, MAX({lat_col}) AS lat_max, "
            f"MIN({lng_col}) AS lng_min, MAX({lng_col}) AS lng_max FROM gps"
        )
        out["gps_bbox"] = cur.fetchone()
        cur.execute(
            "SELECT MIN(latitude) AS lat_min, MAX(latitude) AS lat_max, "
            "MIN(longitude) AS lng_min, MAX(longitude) AS lng_max "
            "FROM customer WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
        )
        out["customer_bbox"] = cur.fetchone()

        # Pull all gps points (~30k floats) into Python — small enough.
        cur.execute(
            f"SELECT {lat_col} AS la, {lng_col} AS lo FROM gps "
            f"WHERE {lat_col} IS NOT NULL AND {lng_col} IS NOT NULL"
        )
        gps_pts = [(float(r["la"]), float(r["lo"])) for r in cur.fetchall()]
        out["gps_points_loaded"] = len(gps_pts)

        # Sample customers with coords
        cur.execute(
            "SELECT ship_to, ship_to_name, ship_to_state, latitude, longitude "
            "FROM customer "
            "WHERE latitude IS NOT NULL AND longitude IS NOT NULL "
            "ORDER BY RAND() LIMIT %s",
            [sample_n],
        )
        sample = cur.fetchall()
        out["customers_scanned"] = len(sample)

        # In-memory Haversine: O(sample × 30k) ≈ 9M ops, well under a second
        from math import radians, sin, cos, asin, sqrt
        R = 6371000.0
        results = []
        for c in sample:
            cla = radians(float(c["latitude"]))
            clo = radians(float(c["longitude"]))
            best = None
            for la, lo in gps_pts:
                dla = radians(la) - cla
                dlo = radians(lo) - clo
                a = sin(dla / 2) ** 2 + cos(cla) * cos(radians(la)) * sin(dlo / 2) ** 2
                d = 2 * R * asin(min(1.0, sqrt(a)))
                if best is None or d < best:
                    best = d
            results.append({
                "ship_to":       c["ship_to"],
                "ship_to_name":  c["ship_to_name"],
                "ship_to_state": c["ship_to_state"],
                "shop_lat":      float(c["latitude"]),
                "shop_lng":      float(c["longitude"]),
                "min_dist_m":    round(best, 0) if best is not None else None,
            })
        results.sort(key=lambda r: (r["min_dist_m"] is None, r["min_dist_m"] or 0))

        out["closest_customer_to_any_gps"] = results[:limit]
        # Distribution: how many customers are within X metres
        out["distance_buckets"] = {
            "<=500m":   sum(1 for r in results if r["min_dist_m"] is not None and r["min_dist_m"] <=    500),
            "<=1000m":  sum(1 for r in results if r["min_dist_m"] is not None and r["min_dist_m"] <=  1000),
            "<=2500m":  sum(1 for r in results if r["min_dist_m"] is not None and r["min_dist_m"] <=  2500),
            "<=5000m":  sum(1 for r in results if r["min_dist_m"] is not None and r["min_dist_m"] <=  5000),
            "<=20000m": sum(1 for r in results if r["min_dist_m"] is not None and r["min_dist_m"] <= 20000),
        }
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"
    finally:
        try: cur.close()
        except Exception: pass
        try: conn.close()
        except Exception: pass
    return jsonify(out)


@app.get("/api/visit_summary")
def visit_summary():
    """Total visit counts across every customer/ship-to.
    Params: [year=2026], [radius=300], [with_sales=1]
    Returns: total shops with ≥1 visit, total visit-days, monthly +
             per-state breakdown, and the top 20 shops.

    with_sales=1 (default) restricts the customer set to those that have
    at least one row in sales_thismonth — i.e. only count shops that are
    currently transacting, ignoring DNU / inactive accounts. Pass
    with_sales=0 to count every customer with coords regardless of sales.

    Strategy: load all gps points + all customers with coords into Python,
    bin gps into a coarse spatial grid, and for each customer scan only
    the nearby cells. Avoids the unindexed customer×gps SQL JOIN.
    """
    if USE_SQLITE:
        return jsonify({"note": "available only on MySQL"})
    try:
        year = int(request.args.get("year", 2026) or 2026)
    except (TypeError, ValueError):
        year = 2026
    try:
        radius_m = float(request.args.get("radius", 300) or 300)
    except (TypeError, ValueError):
        radius_m = 300.0
    with_sales = (request.args.get("with_sales", "0").strip().lower()
                  in ("1", "true", "yes"))
    business_only = (request.args.get("business_days_only", "1").strip().lower()
                     not in ("0", "false", "no"))

    out = {"params": {"year": year, "radius_m": radius_m,
                      "with_sales": with_sales,
                      "business_days_only": business_only}}
    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        date_col, lat_col, lng_col = _resolve_gps_columns(cur)
        salesman_col = _resolve_gps_salesman_col(cur)
        out["gps_salesman_column"] = salesman_col  # null if schema lacks it

        sm_select = f", {salesman_col} AS sm" if salesman_col else ""
        cur.execute(
            f"SELECT {lat_col} AS la, {lng_col} AS lo, {date_col} AS d{sm_select} "
            f"FROM gps WHERE YEAR({date_col}) = %s "
            f"AND {lat_col} IS NOT NULL AND {lng_col} IS NOT NULL",
            [year],
        )
        gps_rows = cur.fetchall()
        if business_only:
            gps_rows = [r for r in gps_rows if _is_business_day(r["d"])]
        out["gps_rows_in_year"] = len(gps_rows)

        # Always pull every customer — BDE assignment count must include
        # shops without sales and even shops without lat/lng.  The visit
        # check + with_sales filter is applied per-row inside the loop.
        cur.execute(
            "SELECT ship_to, ship_to_name, ship_to_state, "
            "       bde_state, salesman_name, latitude, longitude "
            "FROM customer"
        )
        customers = cur.fetchall()
        out["customers_total"] = len(customers)

        # When with_sales=1, only shops that appear in sales_thismonth count
        # toward the visit checking — but they still count toward total Shops.
        shops_with_sales = None
        if with_sales:
            cur.execute("SELECT DISTINCT ship_to FROM sales_thismonth")
            shops_with_sales = {r["ship_to"] for r in cur.fetchall()}
            out["customers_with_sales"] = len(shops_with_sales)

        # Spatial grid (cell ≈ 1.1 km).  Search 3×3 cells covers ≤ 1.5 km
        # which comfortably contains any radius up to ~1 km.  Bumps to 5×5
        # if the user passes a larger radius.
        from collections import defaultdict
        from math import radians, sin, cos, asin, sqrt
        GRID = 0.01
        # extend search ring so radius/cell_size_m is fully covered
        cell_size_m = 1100  # ~0.01° at 30°S
        ring = max(1, int((radius_m / cell_size_m) + 0.999))

        # Grid carries the salesman so we can attribute each GPS hit to the
        # actual BDE that recorded it.  Names normalised UPPER() / strip()
        # so customer.salesman_name and gps.salesmen match despite casing.
        def _norm_name(s):
            return (s or "").strip().upper()

        grid = defaultdict(list)
        for r in gps_rows:
            la, lo, d = float(r["la"]), float(r["lo"]), r["d"]
            sm = _norm_name(r.get("sm")) if salesman_col else ""
            grid[(int(la / GRID), int(lo / GRID))].append((la, lo, d, sm))

        # Active business days per BDE — denominator for No-Visit Days.
        # 'active' = a day where the BDE has any GPS recorded at all.
        active_days_by_bde = defaultdict(set)
        active_days_by_bde_by_month = defaultdict(lambda: defaultdict(set))  # bde -> month -> {date}
        if salesman_col:
            for r in gps_rows:
                sm = _norm_name(r.get("sm"))
                if sm and r["d"] is not None:
                    active_days_by_bde[sm].add(r["d"])
                    active_days_by_bde_by_month[sm][r["d"].month].add(r["d"])

        R = 6371000.0
        per_shop = []
        per_month = defaultdict(lambda: {"shops": set(), "visit_days": 0})
        per_state = defaultdict(lambda: {"shops": set(), "visit_days": 0})
        # per_bde keyed by NORMALIZED salesman name so customer.salesman_name
        # (territory) and gps.salesmen (visits) line up despite case/whitespace.
        per_bde = defaultdict(lambda: {
            "all_shops":    set(),                 # territory from customer.salesman_name
            "shops":        set(),                 # all shops their GPS hit (own + other)
            "shops_own":    set(),                 # shops in their own territory that they visited
            "shops_other":  set(),                 # shops outside their territory that they visited
            "visit_days":   0,
            "visited_dates": set(),
            "state_counts": defaultdict(int),
            "display_name": "",
            # Monthly sparkline data: each is keyed by calendar month (1-12).
            "shops_by_month":         defaultdict(set),  # month -> {ship_to}
            "visit_days_by_month":    defaultdict(int),
            "visited_dates_by_month": defaultdict(set),  # month -> {date}
            # Meeting-log-only tally (kept separate so the BDE table can
            # show a dedicated "Logs" metric next to the GPS-derived ones).
            "logs_total":             0,
            "logs_by_month":          defaultdict(int),
        })
        seen_ship_to = set()  # one ship_to can appear in multiple customer rows

        # Pre-seed display names from gps so a BDE who has no customer entries
        # (e.g. ex-territory) still shows up with their gps-side name.
        if salesman_col:
            for r in gps_rows:
                sm_raw = (r.get("sm") or "").strip()
                if sm_raw:
                    norm = sm_raw.upper()
                    if not per_bde[norm]["display_name"]:
                        per_bde[norm]["display_name"] = sm_raw

        for c in customers:
            if c["ship_to"] in seen_ship_to:
                continue
            seen_ship_to.add(c["ship_to"])

            # Territory: every assigned ship_to counts toward the BDE in
            # customer.salesman_name regardless of sales / coords / visits.
            cust_name_raw = (c["salesman_name"] or "").strip()
            cust_name_norm = cust_name_raw.upper() if cust_name_raw else "(UNASSIGNED)"
            per_bde[cust_name_norm]["all_shops"].add(c["ship_to"])
            # Customer master casing is authoritative for the display name.
            if cust_name_raw:
                per_bde[cust_name_norm]["display_name"] = cust_name_raw
            elif not per_bde[cust_name_norm]["display_name"]:
                per_bde[cust_name_norm]["display_name"] = "(unassigned)"
            if c["bde_state"]:
                per_bde[cust_name_norm]["state_counts"][c["bde_state"].strip()] += 1

            # Visit check needs coords; skip if missing.
            if c["latitude"] is None or c["longitude"] is None:
                continue
            # with_sales filter is applied ONLY to BDE attribution below.
            # Per-shop visit count (used by map popup + visits_by_ship_to)
            # should reflect every shop regardless of sales — otherwise the
            # popup says "0 visits" for a shop the chart clearly shows
            # had GPS visits.
            shop_has_sales = (shops_with_sales is None
                              or c["ship_to"] in shops_with_sales)

            try:
                cla_deg = float(c["latitude"]); clo_deg = float(c["longitude"])
            except (TypeError, ValueError):
                continue
            ci, cj = int(cla_deg / GRID), int(clo_deg / GRID)
            cla = radians(cla_deg); clo = radians(clo_deg)
            cos_cla = cos(cla)
            # Per-shop (any salesman) → drives map markers + popup totals.
            visit_dates_any = defaultdict(set)
            # Per-shop, per-gps-salesman → attribution for the BDE table.
            visit_dates_per_bde = defaultdict(lambda: defaultdict(set))  # bde_norm → month → dates

            for di in range(-ring, ring + 1):
                for dj in range(-ring, ring + 1):
                    for la, lo, d, gps_sm in grid.get((ci + di, cj + dj), ()):
                        dla = radians(la) - cla
                        dlo = radians(lo) - clo
                        h = sin(dla / 2) ** 2 + cos_cla * cos(radians(la)) * sin(dlo / 2) ** 2
                        dist = 2 * R * asin(min(1.0, sqrt(h)))
                        if dist <= radius_m:
                            visit_dates_any[d.month].add(d)
                            if salesman_col and gps_sm:
                                visit_dates_per_bde[gps_sm][d.month].add(d)

            # Per-shop (any salesman) — used by map / popup. Always populated.
            if visit_dates_any:
                total_days_any = sum(len(s) for s in visit_dates_any.values())
                # visited_by_norm = list of normalised BDE names who hit this
                # shop within radius (needed by the map BDE overlay to colour
                # "other-territory" visits when a BDE is filter-selected).
                visited_by_norm = sorted(visit_dates_per_bde.keys()) if salesman_col else []
                per_shop.append({
                    "ship_to":       c["ship_to"],
                    "ship_to_name":  c["ship_to_name"],
                    "ship_to_state": c["ship_to_state"],
                    "bde_state":     c["bde_state"],
                    "salesman_name": c["salesman_name"],
                    "latitude":      cla_deg,
                    "longitude":     clo_deg,
                    "visit_days":    total_days_any,
                    "visited_by":    visited_by_norm,
                    "by_month":      {m: len(s) for m, s in sorted(visit_dates_any.items())},
                })
                for m, s in visit_dates_any.items():
                    per_month[m]["shops"].add(c["ship_to"])
                    per_month[m]["visit_days"] += len(s)
                st = c["ship_to_state"] or "?"
                per_state[st]["shops"].add(c["ship_to"])
                per_state[st]["visit_days"] += total_days_any

            # Per-BDE — credit the visit to whoever's GPS came within radius.
            # Restricted to shops that have sales when with_sales=1 (default),
            # so the BDE table reflects effort on active accounts only.
            if shop_has_sales:
                for gps_norm, dates_by_month in visit_dates_per_bde.items():
                    total_days_this_bde = sum(len(s) for s in dates_by_month.values())
                    bde = per_bde[gps_norm]
                    bde["shops"].add(c["ship_to"])
                    bde["visit_days"] += total_days_this_bde
                    for m, s in dates_by_month.items():
                        bde["visited_dates"].update(s)
                        bde["visited_dates_by_month"][m].update(s)
                        bde["shops_by_month"][m].add(c["ship_to"])
                        bde["visit_days_by_month"][m] += len(s)

        per_shop.sort(key=lambda r: r["visit_days"], reverse=True)

        # ── meeting_log counts (separate "Logs" metric) ──────────────
        # Logged independently of GPS-derived visits — the BDE table
        # surfaces it as its own column so the user can compare effort
        # logged vs effort tracked.  Each row in meeting_log is one
        # tally point; no dedupe (a BDE who writes two memos for the
        # same call genuinely logged twice).
        logged_ship_tos = set()
        # Per-purpose ship-to lists so the map can offer a Purpose
        # filter row (All / Promotion / Product introduction / Claim
        # support / Rebate follow-up / Other).
        from collections import defaultdict as _dd
        logged_by_purpose = _dd(set)
        try:
            cur.execute(
                "SELECT bde_name, ship_to, visit_purpose, visit_date AS d "
                "FROM meeting_log WHERE YEAR(visit_date) = %s",
                [year],
            )
            for r in cur.fetchall():
                norm = (r.get("bde_name") or "").strip().upper()
                ship = (r.get("ship_to")  or "").strip()
                purp = (r.get("visit_purpose") or "").strip() or "Other"
                d    = r.get("d")
                if not norm or d is None:
                    continue
                if business_only and not _is_business_day(d):
                    continue
                bde = per_bde[norm]
                if not bde["display_name"]:
                    bde["display_name"] = r["bde_name"].strip()
                bde["logs_total"] += 1
                bde["logs_by_month"][d.month] += 1
                if ship:
                    logged_ship_tos.add(ship)
                    logged_by_purpose[purp].add(ship)
        except Exception as e:
            # meeting_log table may not exist on a brand-new install yet
            print(f"[visit_summary] meeting_log tally skipped: {e}")

        out["logged_ship_tos"] = sorted(logged_ship_tos)
        out["logged_ship_tos_by_purpose"] = {
            p: sorted(s) for p, s in logged_by_purpose.items()
        }
        out["total_shops_visited"] = len(per_shop)
        out["total_visit_days"]    = sum(r["visit_days"] for r in per_shop)
        out["by_month"] = [
            {"m": m, "shops_visited": len(per_month[m]["shops"]),
             "visit_days": per_month[m]["visit_days"]}
            for m in sorted(per_month)
        ]
        out["by_state"] = sorted(
            [{"state": s, "shops_visited": len(v["shops"]),
              "visit_days": v["visit_days"]} for s, v in per_state.items()],
            key=lambda r: r["visit_days"], reverse=True,
        )
        # Team-wide active business days (any salesman) — kept as a
        # reference number so the header line still shows it.
        active_business_days_team = {r["d"] for r in gps_rows
                                     if r["d"] is not None and _is_business_day(r["d"])}
        out["active_business_days"] = len(active_business_days_team)

        STATE_ORDER = ["NSW", "QLD", "VIC", "SA", "WA"]
        def _primary_state(counts):
            if not counts:
                return ""
            # Pick the most-common bde_state for this BDE; on ties, prefer
            # earlier in STATE_ORDER.
            return max(counts.items(),
                       key=lambda kv: (kv[1],
                                       -STATE_ORDER.index(kv[0]) if kv[0] in STATE_ORDER else -99))[0]
        def _state_sort_key(s):
            return STATE_ORDER.index(s) if s in STATE_ORDER else len(STATE_ORDER)

        bde_rows = []
        for norm, v in per_bde.items():
            if norm in BDE_EXCLUDE:
                continue
            # No-Visit Days denominator = days where THIS BDE had any GPS
            # recorded.  Subtract the days they actually visited a shop.
            bde_active = active_days_by_bde.get(norm, set())
            no_visit_days = max(0, len(bde_active) - len(v["visited_dates"]))
            display = v["display_name"] or norm
            # Monthly sparkline arrays (length 12, index 0 = January).
            active_by_month = active_days_by_bde_by_month.get(norm, {})
            shops_m  = [len(v["shops_by_month"].get(m, ()))      for m in range(1, 13)]
            visits_m = [int(v["visit_days_by_month"].get(m, 0))  for m in range(1, 13)]
            no_visit_m = [
                max(0, len(active_by_month.get(m, ())) -
                       len(v["visited_dates_by_month"].get(m, ())))
                for m in range(1, 13)
            ]
            logs_m   = [int(v["logs_by_month"].get(m, 0))        for m in range(1, 13)]
            bde_rows.append({
                "bde":           display,
                "state":         _primary_state(v["state_counts"]),
                "total_shops":   len(v["all_shops"]),
                "shops_visited": len(v["shops"]),
                "visit_days":    v["visit_days"],
                "active_days":   len(bde_active),
                "no_visit_days": no_visit_days,
                "logs_total":    v["logs_total"],
                "shops_by_month":         shops_m,
                "visit_days_by_month":    visits_m,
                "no_visit_days_by_month": no_visit_m,
                "logs_by_month":          logs_m,
            })
        out["by_bde"] = sorted(
            bde_rows,
            key=lambda r: (_state_sort_key(r["state"]), -r["visit_days"]),
        )
        out["top_shops"] = per_shop[:20]
        # Full set of visited ship_tos + per-shop visit counts so the map can
        # fade non-visited markers and show counts in popups without an extra
        # round-trip per shop.
        out["visited_ship_tos"] = [r["ship_to"] for r in per_shop]
        out["visits_by_ship_to"] = {r["ship_to"]: r["visit_days"] for r in per_shop}
        # Slim list of every visited shop with location + visited_by (norm
        # names) so the map can render "other-territory visits" when a single
        # BDE is filter-selected.  Trimmed fields keep payload small.
        out["visited_shops"] = [
            {
                "ship_to":       r["ship_to"],
                "ship_to_name":  r["ship_to_name"],
                "salesman_name": r["salesman_name"],
                "latitude":      r["latitude"],
                "longitude":     r["longitude"],
                "visit_days":    r["visit_days"],
                "visited_by":    r["visited_by"],
            }
            for r in per_shop
        ]
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"
    finally:
        try: cur.close()
        except Exception: pass
        try: conn.close()
        except Exception: pass
    return jsonify(out)


@app.get("/api/visit_debug_gps_locality")
def visit_debug_gps_locality():
    """For each GPS point, find the nearest customer and bucket by
    distance.  Answers 'is the GPS data even taken AT customer locations,
    or mostly on highways / depots / homes?'.

    Also breaks down GPS records by registration so we can see the
    sampling rate (rows per vehicle per day).
    Params: [year=2026], [with_sales=1]
    """
    if USE_SQLITE:
        return jsonify({"note": "available only on MySQL"})
    try:
        year = int(request.args.get("year", 2026) or 2026)
    except (TypeError, ValueError):
        year = 2026
    with_sales = (request.args.get("with_sales", "1").strip().lower()
                  not in ("0", "false", "no"))

    out = {"params": {"year": year, "with_sales": with_sales}}
    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        date_col, lat_col, lng_col = _resolve_gps_columns(cur)

        # GPS points for the year
        cur.execute(
            f"SELECT {lat_col} AS la, {lng_col} AS lo, {date_col} AS d, "
            f"       registration AS rego "
            f"FROM gps WHERE YEAR({date_col}) = %s "
            f"AND {lat_col} IS NOT NULL AND {lng_col} IS NOT NULL",
            [year],
        )
        gps_rows = cur.fetchall()
        out["gps_rows_in_year"] = len(gps_rows)

        # Customers (optionally filtered to those with sales)
        if with_sales:
            cur.execute(
                "SELECT c.ship_to, c.latitude, c.longitude FROM customer c "
                "WHERE c.latitude IS NOT NULL AND c.longitude IS NOT NULL "
                "AND EXISTS (SELECT 1 FROM sales_thismonth s "
                "            WHERE s.ship_to = c.ship_to)"
            )
        else:
            cur.execute(
                "SELECT ship_to, latitude, longitude FROM customer "
                "WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
            )
        customers = cur.fetchall()
        out["customers_considered"] = len(customers)

        # Build customer spatial grid for fast lookup
        from collections import defaultdict, Counter
        from math import radians, sin, cos, asin, sqrt
        GRID = 0.05  # ~5.5 km cells — search 3×3 covers ~15 km
        cust_grid = defaultdict(list)
        for c in customers:
            la, lo = float(c["latitude"]), float(c["longitude"])
            cust_grid[(int(la / GRID), int(lo / GRID))].append((la, lo, c["ship_to"]))

        R = 6371000.0
        buckets = Counter()
        BUCKET_EDGES = (100, 250, 500, 1000, 2500, 5000, 10000, 25000, 100000)
        no_match = 0  # GPS where there's no customer within the search ring

        for r in gps_rows:
            gla = float(r["la"]); glo = float(r["lo"])
            gi, gj = int(gla / GRID), int(glo / GRID)
            best = None
            for di in (-1, 0, 1):
                for dj in (-1, 0, 1):
                    for cla, clo, _ in cust_grid.get((gi + di, gj + dj), ()):
                        dla = radians(gla) - radians(cla)
                        dlo = radians(glo) - radians(clo)
                        h = sin(dla / 2) ** 2 + cos(radians(cla)) * cos(radians(gla)) * sin(dlo / 2) ** 2
                        d = 2 * R * asin(min(1.0, sqrt(h)))
                        if best is None or d < best:
                            best = d
            if best is None:
                no_match += 1
                continue
            # bucket: place into the smallest edge it fits under
            placed = False
            for edge in BUCKET_EDGES:
                if best <= edge:
                    buckets[f"<={edge}m"] += 1
                    placed = True
                    break
            if not placed:
                buckets[">100km"] += 1

        out["gps_no_match_within_15km"] = no_match
        out["gps_distance_to_nearest_customer"] = dict(buckets)
        out["gps_within_500m_of_some_customer_pct"] = round(
            100 * buckets.get("<=100m", 0) / max(1, len(gps_rows)) +
            100 * buckets.get("<=250m", 0) / max(1, len(gps_rows)) +
            100 * buckets.get("<=500m", 0) / max(1, len(gps_rows)),
            1,
        )

        # Per-registration sampling rate
        cur.execute(
            f"SELECT registration AS rego, COUNT(*) AS n, "
            f"COUNT(DISTINCT DATE({date_col})) AS days "
            f"FROM gps WHERE YEAR({date_col}) = %s "
            f"GROUP BY registration ORDER BY n DESC LIMIT 30",
            [year],
        )
        regs = cur.fetchall()
        for r in regs:
            r["rows_per_day"] = round(r["n"] / max(1, r["days"]), 2)
        out["top_registrations"] = regs
        cur.execute(
            f"SELECT COUNT(DISTINCT registration) AS n FROM gps "
            f"WHERE YEAR({date_col}) = %s",
            [year],
        )
        out["total_distinct_registrations"] = cur.fetchone()["n"]
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"
    finally:
        try: cur.close()
        except Exception: pass
        try: conn.close()
        except Exception: pass
    return jsonify(out)

# ==============================================================================
# REBATE CALCULATOR
# ==============================================================================

@app.get("/rebate")
def rebate_page():
    return send_from_directory("static", "rebate.html")

@app.get("/api/rebate_data")
def api_rebate_data():
    """
    Return rebate status per SHIP_TO 횞 territory ??server-side paginated.
    Query params:
      territory     (TTL | HK | LF | ALL, default ALL)
      sold_to_group (default ALL)
      search        free-text filter on name / code
      show          ALL | NEXT | MAX | ZERO
      sort          actual | est_rebate | needed | ship_to_name (default: actual)
      dir           desc | asc (default desc)
      page          0-indexed page of sold_to groups (default 0)
      page_size     groups per page (default 40)
    unit=A ??measure in $ amount
    unit=Q ??measure in qty
    brand=TTL ??sum HK + LF sales
    """
    brand_filter  = request.args.get("territory",     "ALL").upper()  # UI still sends 'territory'
    stg_filter    = request.args.get("sold_to_group", "ALL")
    region_filter = request.args.get("region",        "ALL").upper()
    search      = request.args.get("search",  "").strip().lower()
    show        = request.args.get("show",    "ALL").upper()
    sort_col    = request.args.get("sort",    "actual")
    sort_dir    = request.args.get("dir",     "desc")
    try:
        page      = int(request.args.get("page",      0))
        page_size = int(request.args.get("page_size", 40))
    except ValueError:
        page, page_size = 0, 40

    conn = get_connection()
    cur  = conn.cursor(dictionary=True)
    try:
        # ?? 1. Rebate-mapped customers (sold_to level) ???????????????????????
        cur.execute("""
            SELECT m.sold_to, m.brand, m.structure_name,
                   MIN(c.sold_to_name)  AS sold_to_name,
                   MIN(c.sold_to_group) AS sold_to_group
            FROM rebate_customer_map m
            LEFT JOIN customer c ON c.sold_to = m.sold_to
            WHERE (%s = 'ALL' OR m.brand = %s)
              AND (%s = 'ALL' OR c.sold_to_group = %s)
            GROUP BY m.sold_to, m.brand, m.structure_name
        """, (brand_filter, brand_filter, stg_filter, stg_filter))
        customers = cur.fetchall()
        if not customers:
            return jsonify([])

        # ?? 2. Sales from sales_thismonth by (sold_to, ship_to, brand, line) ??
        # Brand comes from sales_thismonth.brand directly (SAP loader fills it).
        # Line is derived from material prefix: 1xxx/2xxx → PCLT, 3xxx → TBR.
        # No carrying join needed.
        cur.execute("""
            SELECT s.sold_to, s.ship_to, s.brand AS brand,
                   CASE
                     WHEN LEFT(s.material,1) IN ('1','2') THEN 'PCLT'
                     WHEN LEFT(s.material,1) = '3'        THEN 'TBR'
                     ELSE ''
                   END AS line,
                   SUM(s.qty) AS qty, SUM(s.amt) AS amt
            FROM sales_thismonth s
            WHERE s.brand IN ('HK','LF')
              AND (s.so_type IS NULL OR s.so_type != 'ZWH2')
            GROUP BY s.sold_to, s.ship_to, s.brand,
                     CASE
                       WHEN LEFT(s.material,1) IN ('1','2') THEN 'PCLT'
                       WHEN LEFT(s.material,1) = '3'        THEN 'TBR'
                       ELSE ''
                     END
        """)
        ship_sales      = {}   # (sold_to, ship_to, brand) -> {qty, amt}  all lines
        ship_sales_line = {}   # (sold_to, ship_to, brand, line) -> {qty, amt}
        ship_idx        = {}   # (sold_to, brand) -> set{ship_to}
        ship_idx_line   = {}   # (sold_to, brand, line) -> set{ship_to}
        for r in cur.fetchall():
            st, sh, br, ln = str(r["sold_to"]), str(r["ship_to"]), r["brand"], r["line"]
            qty, amt = float(r["qty"] or 0), float(r["amt"] or 0)
            # aggregate all lines ??brand-level totals
            agg = ship_sales.setdefault((st, sh, br), {"qty": 0.0, "amt": 0.0})
            agg["qty"] += qty; agg["amt"] += amt
            # store by line
            ship_sales_line[(st, sh, br, ln)] = {"qty": qty, "amt": amt}
            ship_idx.setdefault((st, br), set()).add(sh)
            ship_idx_line.setdefault((st, br, ln), set()).add(sh)

        # ?? 3. Customer lookup (ship_to ??name, bde_state, salesman) ??????????
        cur.execute("SELECT ship_to, ship_to_name, bde_state, salesman_name FROM customer")
        ship_cust_map = {}   # ship_to str -> {name, state, bde}
        for r in cur.fetchall():
            sh = str(r["ship_to"])
            ship_cust_map[sh] = {
                "name":  r["ship_to_name"] or sh,
                "state": (r["bde_state"] or "").strip() or "-",
                "bde":   (r["salesman_name"] or "").strip() or "-",
            }
        name_map = {sh: v["name"] for sh, v in ship_cust_map.items()}

        # Build BDE ??region mapping: for each BDE, use the most common state among
        # their ship_tos in ship_cust_map (actual BDE region, not sold_to's bde_state)
        from collections import Counter
        _bde_state_counter = {}
        for v in ship_cust_map.values():
            bde = v.get("bde", "-")
            st  = v.get("state", "-")
            if bde and bde != "-" and st and st != "-":
                _bde_state_counter.setdefault(bde, Counter())[st] += 1
        bde_region_map = {bde: cnt.most_common(1)[0][0]
                          for bde, cnt in _bde_state_counter.items()}

        # ?? 4. Tier definitions (only meaningful tiers: tier_order <= top_order) ?
        cur.execute("""
            SELECT structure_name, unit, tier_order, top_order, threshold, rate
            FROM rebate_structure
            WHERE tier_order <= top_order
            ORDER BY structure_name, tier_order
        """)
        tiers_map = {}
        for r in cur.fetchall():
            sd = tiers_map.setdefault(r["structure_name"],
                                      {"unit": r["unit"], "top_order": int(r["top_order"]), "tiers": []})
            sd["tiers"].append({"tier":      int(r["tier_order"]),
                                 "threshold": float(r["threshold"]),
                                 "rate":      float(r["rate"])})

        # ?? 5. Build result ??one row per SHIP_TO ????????????????????????????
        def _calc_tier(actual, tiers, top_order):
            """
            Returns (curr_tier, next_tier).
            next_tier is None if actual has reached top_order tier.
            tiers list is already trimmed to top_order entries.
            """
            curr = {"tier": 0, "threshold": 0, "rate": 0}
            nxt  = None
            for t in tiers:
                if t["threshold"] <= actual and t["rate"] > 0:
                    curr = t
                elif t["threshold"] > actual and t["rate"] > 0 and nxt is None:
                    nxt = t
            # If curr tier has reached top_order, there is no next tier
            if curr["tier"] >= top_order:
                nxt = None
            return curr, nxt

        # AJT/ABJ/ATP/APP/ACD: calculate per ship_to; others: aggregate to sold_to level
        # Determined by structure name (not sold_to_group DB field)
        SHIP_TO_STRUCT_KEYS = {'AJT', 'ABJ', 'ATP', 'APP', 'ACD'}

        def _atp_variants(struct_name, brand):
            """Return list of (atp_brand, atp_line, brand_key, badges) to process separately.
            AL_ATP: two entries ??PCLT and TBR (each HK+LF combined).
            HK_ATP: one entry ??HK brand, TBR line.
            Others: one entry ??normal brand/unit (resolved later).
            """
            if "ATP" not in struct_name:
                return [(None, None, brand, None)]   # badges resolved later
            if struct_name.startswith("AL_ATP"):
                return [
                    (None,  "PCLT", "PCLT", ["PCLT", "Q"]),   # HK+LF combined
                    ("HK",  "TBR",  "TBR",  ["TBR",  "Q"]),   # HK only
                ]
            if struct_name.startswith("HK_ATP"):
                return [("HK", "TBR", "HK_TBR", ["HK", "TBR", "Q"])]
            return [(None, None, brand, None)]

        rows = []
        for c in customers:
            struct        = c["structure_name"]
            brand         = c["brand"]
            sold_to       = str(c["sold_to"])
            sold_to_group = c["sold_to_group"] or "-"
            sd            = tiers_map.get(struct)
            if not sd:
                continue
            unit      = sd["unit"]       # A=Amount, Q=Qty
            tiers     = sd["tiers"]
            top_order = sd["top_order"]

            for atp_brand, atp_line, brand_key, badges_override in _atp_variants(struct, brand):

                # Collect ship_tos for this variant
                if atp_line:
                    if atp_brand:   # HK_ATP: HK brand, specific line
                        ship_set = ship_idx_line.get((sold_to, atp_brand, atp_line), set()).copy()
                    else:           # AL_ATP: HK+LF brands, specific line (PCLT or TBR)
                        ship_set = (ship_idx_line.get((sold_to, "HK", atp_line), set()) |
                                    ship_idx_line.get((sold_to, "LF", atp_line), set()))
                elif brand_key == "TTL":
                    ship_set = (ship_idx.get((sold_to, "HK"), set()) |
                                ship_idx.get((sold_to, "LF"), set()))
                else:
                    ship_set = ship_idx.get((sold_to, brand_key), set()).copy()

                # Only keep ship_tos that are known in the customer table
                ship_set = {sh for sh in ship_set if sh in ship_cust_map}

                if not ship_set:
                    ship_set.add(sold_to)   # show zero row so sold_to is visible

                # Determine sold_to's canonical BDE and region.
                # Start from the self-referencing record (ship_to == sold_to code),
                # then fall back to individual ship_tos if BDE/region is still missing.
                st_info = ship_cust_map.get(sold_to, {})
                sold_to_bde    = (st_info.get("bde",   "-") or "-") if st_info else "-"
                sold_to_region = (st_info.get("state", "-") or "-") if st_info else "-"

                # If sold_to's own record is missing BDE or region, infer from
                # the actual ship_tos (customer table joined via ship_to).
                if sold_to_bde == "-" or sold_to_region == "-":
                    real_ships = [sh for sh in ship_set if sh != sold_to]
                    state_cnt = Counter(
                        ship_cust_map[sh].get("state", "") for sh in real_ships
                        if ship_cust_map.get(sh, {}).get("state", "")
                        and ship_cust_map[sh]["state"] != "-"
                    )
                    bde_cnt = Counter(
                        ship_cust_map[sh].get("bde", "") for sh in real_ships
                        if ship_cust_map.get(sh, {}).get("bde", "")
                        and ship_cust_map[sh]["bde"] != "-"
                    )
                    if sold_to_bde == "-":
                        sold_to_bde    = bde_cnt.most_common(1)[0][0]   if bde_cnt   else "-"
                    if sold_to_region == "-":
                        sold_to_region = state_cnt.most_common(1)[0][0] if state_cnt else "-"

                # Badge labels for UI
                if badges_override is not None:
                    badges = badges_override
                else:
                    badges = [brand_key, unit]

                def _get_sales(sh, _atp_brand=atp_brand, _atp_line=atp_line, _brand_key=brand_key):
                    """Return (qty, amt) for this ship_to for this variant."""
                    if _atp_line:
                        if _atp_brand:
                            d = ship_sales_line.get((sold_to, sh, _atp_brand, _atp_line), {"qty": 0.0, "amt": 0.0})
                            return d["qty"], d["amt"]
                        else:   # AL_ATP: HK+LF brands, single line (PCLT or TBR)
                            hk = ship_sales_line.get((sold_to, sh, "HK", _atp_line), {"qty": 0.0, "amt": 0.0})
                            lf = ship_sales_line.get((sold_to, sh, "LF", _atp_line), {"qty": 0.0, "amt": 0.0})
                            return hk["qty"] + lf["qty"], hk["amt"] + lf["amt"]
                    elif _brand_key == "TTL":
                        hk = ship_sales.get((sold_to, sh, "HK"), {"qty": 0.0, "amt": 0.0})
                        lf = ship_sales.get((sold_to, sh, "LF"), {"qty": 0.0, "amt": 0.0})
                        return hk["qty"] + lf["qty"], hk["amt"] + lf["amt"]
                    else:
                        d = ship_sales.get((sold_to, sh, _brand_key), {"qty": 0.0, "amt": 0.0})
                        return d["qty"], d["amt"]

                if any(k in struct for k in SHIP_TO_STRUCT_KEYS):
                    # One row per ship_to ??BDE comes from each ship_to's own customer record
                    calc_items = []
                    for sh in sorted(ship_set):
                        q, a = _get_sales(sh)
                        sh_info_local = ship_cust_map.get(sh, {})
                        calc_items.append({
                            "sh":           sh,
                            "qty":          q,
                            "amt":          a,
                            "bde":          sh_info_local.get("bde",   "-") or sold_to_bde,
                            "region":       sh_info_local.get("state", "-") or sold_to_region,
                            "sold_to_basis": False,
                            "ship_details": [],
                        })
                else:
                    # Group ship_tos by their BDE ??one row per BDE per sold_to.
                    # This ensures every salesman sees their own portion of shared accounts
                    # like JAXQUICKFIT whose 94 ship_tos span multiple BDEs.
                    bde_groups = {}  # bde_name -> {region, qty, amt, ship_details}
                    for sh in sorted(ship_set):
                        q, a = _get_sales(sh)
                        sh_info_local = ship_cust_map.get(sh, {})
                        sh_bde   = sh_info_local.get("bde",   "-") or sold_to_bde
                        sh_state = sh_info_local.get("state", "-") or sold_to_region
                        grp = bde_groups.setdefault(sh_bde, {
                            "bde":          sh_bde,
                            "region":       sh_state,
                            "qty":          0.0,
                            "amt":          0.0,
                            "ship_details": [],
                        })
                        grp["qty"] += q
                        grp["amt"] += a
                        grp["ship_details"].append({
                            "ship_to":      sh,
                            "ship_to_name": sh_info_local.get("name") or sh,
                            "actual_qty":   round(q, 2),
                            "actual_amt":   round(a, 2),
                        })

                    calc_items = [
                        {
                            "sh":           sold_to,
                            "qty":          grp["qty"],
                            "amt":          grp["amt"],
                            "bde":          grp["bde"],
                            "region":       grp["region"],
                            "sold_to_basis": True,
                            "ship_details": grp["ship_details"],
                        }
                        for grp in sorted(bde_groups.values(), key=lambda x: x["bde"])
                    ]
                    if not calc_items:
                        calc_items = [{
                            "sh":           sold_to,
                            "qty":          0.0,
                            "amt":          0.0,
                            "bde":          sold_to_bde,
                            "region":       sold_to_region,
                            "sold_to_basis": True,
                            "ship_details": [],
                        }]

                for item in calc_items:
                    sh          = item["sh"]
                    actual_qty  = item["qty"]
                    actual_amt  = item["amt"]
                    row_bde     = item["bde"]
                    row_region  = item["region"]
                    row_stbasis = item["sold_to_basis"]
                    row_details = item["ship_details"]

                    actual = actual_qty if unit == "Q" else actual_amt

                    curr_tier, next_tier = _calc_tier(actual, tiers, top_order)
                    curr_rebate = round(actual_amt * curr_tier["rate"] / 100, 2)
                    est_rebate  = round(next_tier["threshold"] * next_tier["rate"] / 100, 2) if next_tier else None
                    needed_qty = round(next_tier["threshold"] - actual_qty, 2) if next_tier and unit == "Q" else None
                    needed_amt = round(next_tier["threshold"] - actual_amt, 2) if next_tier and unit == "A" else None

                    sh_info = ship_cust_map.get(sh, {})
                    rows.append({
                        "sold_to":        sold_to,
                        "sold_to_name":   c["sold_to_name"] or st_info.get("name") or sold_to,
                        "sold_to_group":  sold_to_group,
                        "region":         row_region,
                        "bde":            row_bde,
                        "ship_to":        sh,
                        "ship_to_name":   sh_info.get("name") or (c["sold_to_name"] or sh),
                        "brand":          brand_key,
                        "badges":         badges,
                        "structure_name": struct,
                        "sold_to_basis":  row_stbasis,
                        "ship_details":   row_details,
                        "unit":           unit,
                        "actual_qty":     round(actual_qty, 2),
                        "actual_amt":     round(actual_amt, 2),
                        "actual":         round(actual, 2),
                        "curr_rate":      curr_tier["rate"],
                        "curr_threshold": curr_tier["threshold"],
                        "next_threshold": next_tier["threshold"] if next_tier else None,
                        "next_rate":      next_tier["rate"]      if next_tier else None,
                        "needed_qty":     needed_qty,
                        "needed_amt":     needed_amt,
                        "curr_rebate":    curr_rebate,
                        "est_rebate":     est_rebate,
                        "tiers":          tiers,
                    })

        # ?? 6. Client-side-style filters applied server-side ?????????????????
        if search:
            rows = [r for r in rows if
                    search in r["sold_to_name"].lower() or
                    search in str(r["sold_to"]) or
                    search in r["ship_to_name"].lower() or
                    search in str(r["ship_to"])]
        if show == "NEXT":
            rows = [r for r in rows if r["next_rate"] is not None and r["actual"] > 0]
        elif show == "MAX":
            rows = [r for r in rows if r["next_rate"] is None and r["curr_rate"] > 0]
        elif show == "ZERO":
            rows = [r for r in rows if r["actual"] == 0]

        # ?? 7. Summary stats (over all filtered rows) ?????????????????????????
        REGION_KEYS = ["NSW", "QLD", "VIC", "WA"]
        region_totals = {rk: {"rebate": 0.0, "qty": 0.0, "amt": 0.0} for rk in REGION_KEYS}
        for r in rows:
            rk = (r["region"] or "").strip().upper()
            if rk in region_totals:
                region_totals[rk]["rebate"] += r["curr_rebate"]
                region_totals[rk]["qty"]    += r["actual_qty"]
                region_totals[rk]["amt"]    += r["actual_amt"]
        for rk in region_totals:
            region_totals[rk] = {k: round(v, 2) for k, v in region_totals[rk].items()}

        summary = {
            "total_ship_to": len(rows),
            "has_next":  sum(1 for r in rows if r["next_rate"] is not None and r["actual"] > 0),
            "max_tier":  sum(1 for r in rows if r["next_rate"] is None and r["curr_rate"] > 0),
            "zero_sales": sum(1 for r in rows if r["actual"] == 0),
            "est_total":  round(sum(r["curr_rebate"] for r in rows), 2),
            "region_totals": region_totals,
        }

        # Apply region filter to rows (after computing region_totals)
        if region_filter != "ALL":
            rows = [r for r in rows if (r["region"] or "").strip().upper() == region_filter]

        # ?? 8. Group by (region, sold_to) with brand sub-groups ??????????????
        grp_map = {}
        brand_order = {"HK": 0, "LF": 1, "TTL": 2}
        for r in rows:
            key = r["region"] + "|" + r["sold_to"]
            if key not in grp_map:
                grp_map[key] = {
                    "key": key,
                    "sold_to": r["sold_to"], "sold_to_name": r["sold_to_name"],
                    "sold_to_group": r["sold_to_group"], "region": r["region"],
                    "bde": r["bde"],
                    "grp_actual": 0.0, "grp_actual_qty": 0.0, "grp_actual_amt": 0.0,
                    "grp_curr_rebate": 0.0, "grp_est": 0.0,
                    "brands": {},
                }
            g = grp_map[key]
            g["grp_actual"]      += r["actual"]
            g["grp_actual_qty"]  += r["actual_qty"]
            g["grp_actual_amt"]  += r["actual_amt"]
            g["grp_curr_rebate"] += r["curr_rebate"]
            g["grp_est"]         += r["est_rebate"] if r["est_rebate"] else 0.0
            bkey = r["brand"]
            if bkey not in g["brands"]:
                g["brands"][bkey] = {
                    "brand": r["brand"], "unit": r["unit"],
                    "badges": r["badges"],
                    "structure_name": r["structure_name"],
                    "grp_actual": 0.0, "grp_actual_qty": 0.0, "grp_actual_amt": 0.0,
                    "grp_curr_rebate": 0.0, "grp_est": 0.0, "items": [],
                }
            b = g["brands"][bkey]
            b["grp_actual"]      += r["actual"]
            b["grp_actual_qty"]  += r["actual_qty"]
            b["grp_actual_amt"]  += r["actual_amt"]
            b["grp_curr_rebate"] += r["curr_rebate"]
            b["grp_est"]         += r["est_rebate"] if r["est_rebate"] else 0.0
            b["items"].append(r)

        # Convert brands dict to sorted list (HK ??LF ??TTL)
        for g in grp_map.values():
            g["brands"] = sorted(g["brands"].values(), key=lambda b: brand_order.get(b["brand"], 99))

        groups = list(grp_map.values())
        summary["total_groups"] = len(groups)

        # ?? 9. Sort groups ????????????????????????????????????????????????????
        rev = (sort_dir != "asc")
        if sort_col == "est_rebate":
            groups.sort(key=lambda g: g["grp_est"], reverse=rev)
        elif sort_col == "actual":
            groups.sort(key=lambda g: g["grp_actual"], reverse=rev)
        elif sort_col == "sold_to_name":
            groups.sort(key=lambda g: g["sold_to_name"].lower(), reverse=rev)
        else:
            groups.sort(key=lambda g: (g["region"].lower(), g["bde"].lower(), g["sold_to_name"].lower()))

        # ?? 10. Sort items within each brand sub-group by actual desc ?????????
        for g in groups:
            for b in g["brands"]:
                b["items"].sort(key=lambda r: r["actual"], reverse=True)

        # ?? 11. Paginate ??????????????????????????????????????????????????????
        total_pages = max(1, -(-len(groups) // page_size))  # ceil div
        page = max(0, min(page, total_pages - 1))
        page_groups = groups[page * page_size : (page + 1) * page_size]

        stg_list = sorted({r["sold_to_group"] for r in rows if r["sold_to_group"] != "-"})
        return jsonify({
            "summary":     summary,
            "page":        page,
            "page_size":   page_size,
            "total_pages": total_pages,
            "groups":      page_groups,
            "stg_list":    stg_list,
        })

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close()
        conn.close()

@app.get("/api/rebate_export")
def api_rebate_export():
    """Stream a CSV of all rows matching current filters (no pagination)."""
    import csv, io
    brand_filter = request.args.get("territory",     "ALL").upper()
    stg_filter   = request.args.get("sold_to_group", "ALL")
    search       = request.args.get("search",  "").strip().lower()
    show         = request.args.get("show",    "ALL").upper()
    sort_dir     = request.args.get("dir",     "desc")

    conn = get_connection()
    cur  = conn.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT m.sold_to, m.brand, m.structure_name,
                   c.sold_to_name, c.sold_to_group, c.bde_state AS region
            FROM rebate_customer_map m
            LEFT JOIN customer c ON c.sold_to = m.sold_to
            WHERE (%s='ALL' OR m.brand=%s)
              AND (%s='ALL' OR c.sold_to_group=%s)
        """, (brand_filter, brand_filter, stg_filter, stg_filter))
        customers = cur.fetchall()

        # Brand comes from sales_thismonth.brand (no carrying join needed
        # since this query doesn't read line/product_group/pattern).
        cur.execute("""
            SELECT s.sold_to, s.ship_to, s.brand AS brand,
                   SUM(s.qty) AS qty, SUM(s.amt) AS amt
            FROM sales_thismonth s
            WHERE s.brand IN ('HK','LF')
              AND (s.so_type IS NULL OR s.so_type != 'ZWH2')
            GROUP BY s.sold_to, s.ship_to, s.brand
        """)
        ship_sales = {}; ship_idx = {}
        for r in cur.fetchall():
            st,sh,br = str(r["sold_to"]),str(r["ship_to"]),r["brand"]
            ship_sales[(st,sh,br)] = {"qty":float(r["qty"] or 0),"amt":float(r["amt"] or 0)}
            ship_idx.setdefault((st,br),set()).add(sh)

        cur.execute("SELECT ship_to, ship_to_name, bde_state, salesman_name FROM customer")
        ship_cust_map_ex = {}
        for r in cur.fetchall():
            sh = str(r["ship_to"])
            ship_cust_map_ex[sh] = {
                "name":  r["ship_to_name"] or sh,
                "state": (r["bde_state"]    or "").strip() or "-",
                "bde":   (r["salesman_name"] or "").strip() or "-",
            }
        name_map = {sh: v["name"] for sh, v in ship_cust_map_ex.items()}

        cur.execute("""
            SELECT structure_name, unit, tier_order, top_order, threshold, rate
            FROM rebate_structure
            WHERE tier_order <= top_order
            ORDER BY structure_name, tier_order
        """)
        tiers_map = {}
        for r in cur.fetchall():
            sd = tiers_map.setdefault(r["structure_name"],{"unit":r["unit"],"top_order":int(r["top_order"]),"tiers":[]})
            sd["tiers"].append({"tier":int(r["tier_order"]),"threshold":float(r["threshold"]),"rate":float(r["rate"])})

        def _calc(actual, tiers, top_order):
            curr={"tier":0,"threshold":0,"rate":0}; nxt=None
            for t in tiers:
                if t["threshold"]<=actual and t["rate"]>0: curr=t
                elif t["threshold"]>actual and t["rate"]>0 and nxt is None: nxt=t
            if curr["tier"] >= top_order: nxt=None
            return curr, nxt

        SHIP_TO_STRUCT_KEYS = {'AJT', 'ABJ', 'ATP', 'APP', 'ACD'}
        rows=[]
        for c in customers:
            struct=c["structure_name"]; brand=c["brand"]; sold_to=str(c["sold_to"])
            sold_to_group = c["sold_to_group"] or "-"
            sd=tiers_map.get(struct)
            if not sd: continue
            unit=sd["unit"]; tiers=sd["tiers"]; top_order=sd["top_order"]
            if brand=="TTL":
                ship_set=(ship_idx.get((sold_to,"HK"),set())|ship_idx.get((sold_to,"LF"),set()))
            else:
                ship_set=ship_idx.get((sold_to,brand),set()).copy()
            ship_set = {sh for sh in ship_set if sh in ship_cust_map_ex}
            if not ship_set: ship_set.add(sold_to)

            # Resolve region and BDE from sold_to's own record, falling back to ship_tos
            st_info_ex = ship_cust_map_ex.get(sold_to, {})
            sold_to_bde_ex    = (st_info_ex.get("bde",   "-") or "-") if st_info_ex else "-"
            sold_to_region_ex = (st_info_ex.get("state", "-") or "-") if st_info_ex else "-"
            if sold_to_bde_ex == "-" or sold_to_region_ex == "-":
                real_ships = [sh for sh in ship_set if sh != sold_to]
                if real_ships:
                    from collections import Counter as _Counter
                    sc = _Counter(ship_cust_map_ex[sh].get("state","") for sh in real_ships
                                  if ship_cust_map_ex.get(sh,{}).get("state","") and ship_cust_map_ex[sh]["state"]!="-")
                    bc = _Counter(ship_cust_map_ex[sh].get("bde","") for sh in real_ships
                                  if ship_cust_map_ex.get(sh,{}).get("bde","") and ship_cust_map_ex[sh]["bde"]!="-")
                    if sold_to_bde_ex == "-":
                        sold_to_bde_ex    = bc.most_common(1)[0][0] if bc else "-"
                    if sold_to_region_ex == "-":
                        sold_to_region_ex = sc.most_common(1)[0][0] if sc else "-"

            if any(k in struct for k in SHIP_TO_STRUCT_KEYS):
                # One row per ship_to
                calc_items=[]
                for sh in sorted(ship_set):
                    if brand=="TTL":
                        hk=ship_sales.get((sold_to,sh,"HK"),{"qty":0,"amt":0})
                        lf=ship_sales.get((sold_to,sh,"LF"),{"qty":0,"amt":0})
                        calc_items.append((sh,(hk["qty"]+lf["qty"]) if unit=="Q" else (hk["amt"]+lf["amt"])))
                    else:
                        d=ship_sales.get((sold_to,sh,brand),{"qty":0,"amt":0})
                        calc_items.append((sh,d["qty"] if unit=="Q" else d["amt"]))
            else:
                # Aggregate all ship_tos ??one row per sold_to
                total=0.0
                for sh in ship_set:
                    if brand=="TTL":
                        hk=ship_sales.get((sold_to,sh,"HK"),{"qty":0,"amt":0})
                        lf=ship_sales.get((sold_to,sh,"LF"),{"qty":0,"amt":0})
                        total+=(hk["qty"]+lf["qty"]) if unit=="Q" else (hk["amt"]+lf["amt"])
                    else:
                        d=ship_sales.get((sold_to,sh,brand),{"qty":0,"amt":0})
                        total+=d["qty"] if unit=="Q" else d["amt"]
                calc_items=[(sold_to,total)]

            for sh,actual in calc_items:
                sh_info = ship_cust_map_ex.get(sh, {})
                curr_tier,next_tier=_calc(actual,tiers,top_order)
                rows.append({
                    "sold_to":sold_to,"sold_to_name":c["sold_to_name"] or sold_to,
                    "sold_to_group":sold_to_group,
                    "region": sold_to_region_ex,
                    "bde":    sold_to_bde_ex,
                    "ship_to":sh,"ship_to_name":sh_info.get("name") or (c["sold_to_name"] or sh),
                    "brand":brand,"structure_name":struct,"unit":unit,
                    "actual":round(actual,2),"curr_rate":curr_tier["rate"],
                    "next_rate":next_tier["rate"] if next_tier else None,
                    "needed":round(next_tier["threshold"]-actual,2) if next_tier else None,
                    "est_rebate":round(actual*curr_tier["rate"]/100,2),
                })

        # filters
        if search:
            rows=[r for r in rows if search in r["sold_to_name"].lower() or search in str(r["sold_to"]) or search in r["ship_to_name"].lower() or search in str(r["ship_to"])]
        if show=="NEXT": rows=[r for r in rows if r["next_rate"] is not None and r["actual"]>0]
        elif show=="MAX": rows=[r for r in rows if r["next_rate"] is None and r["curr_rate"]>0]
        elif show=="ZERO": rows=[r for r in rows if r["actual"]==0]

        rows.sort(key=lambda r:(r["sold_to_group"],r["sold_to_name"],r["brand"],r["ship_to"]))

        # build CSV
        out=io.StringIO()
        w=csv.writer(out)
        w.writerow(["Sold-To","Sold-To Name","Group","Region","BDE","Ship-To","Ship-To Name","Brand","Type","Actual","Curr Rate%","Next Rate%","Need to Reach","Est Rebate","Structure"])
        for r in rows:
            w.writerow([r["sold_to"],r["sold_to_name"],r["sold_to_group"],r["region"],r["bde"],r["ship_to"],r["ship_to_name"],r["brand"],"Annual $" if r["unit"]=="A" else "QTR Qty",r["actual"],r["curr_rate"],r["next_rate"] if r["next_rate"] is not None else "",r["needed"] if r["needed"] is not None else "",r["est_rebate"],r["structure_name"]])

        from flask import Response
        from datetime import date
        fname=f"rebate_{date.today().isoformat()}.csv"
        return Response(out.getvalue(), mimetype="text/csv",
                        headers={"Content-Disposition":f"attachment;filename={fname}"})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error":str(e)}),500
    finally:
        cur.close(); conn.close()

# ------------------------------------------------------------------------------
# Build fixed Top 10/20/30 once at startup (after functions are defined)
build_global_top_once()

# ------------------------------------------------------------------------------
# BDE Visit / Meeting Log — page at /meeting, backed by meeting_log table.
# ------------------------------------------------------------------------------

MEETING_PHOTO_DIR = os.path.join(BASE_DIR, "static", "meeting_photos")

# ── Mail config ────────────────────────────────────────────────────
# All settings overridable via .env so SMTP / Graph credentials never
# live in the repo.  Two delivery paths are supported, in order of
# preference: Microsoft Graph API (modern auth, recommended) →
# Exchange SMTP (legacy, blocked by Security Defaults).
MAIL_FROM      = os.getenv("MAIL_FROM",      "dashboard@hankooktyre.com.au")
SMTP_HOST      = os.getenv("SMTP_HOST",      "smtp.office365.com")
SMTP_PORT      = int(os.getenv("SMTP_PORT",  "587"))
SMTP_USER      = os.getenv("SMTP_USER",      MAIL_FROM)
SMTP_PASSWORD  = os.getenv("SMTP_PASSWORD",  "")
SMTP_USE_TLS   = (os.getenv("SMTP_USE_TLS",  "1") == "1")
GRAPH_TENANT_ID     = os.getenv("GRAPH_TENANT_ID",     "")
GRAPH_CLIENT_ID     = os.getenv("GRAPH_CLIENT_ID",     "")
GRAPH_CLIENT_SECRET = os.getenv("GRAPH_CLIENT_SECRET", "")
DASHBOARD_URL  = os.getenv("DASHBOARD_URL",  "https://sales.hkaudashboard.com")
MAIL_DEBUG     = (os.getenv("MAIL_DEBUG",    "0") == "1")

# ── BDE / state-manager directory ──────────────────────────────────
# (name, email, state, role).
#   role = "BDE"  → locked to their own salesman name (own territory
#                   + amber overlay markers on the map for shops they
#                   visited outside that territory).
#         "SM"   → locked to their state's region filter.
#         "ALL"  → no scope restriction (leadership / dashboard ops).
_BDE_DIRECTORY = [
    # Name MUST match the customer.salesman_name format used by the
    # database / REGION_SALESMEN constant in app.js — generally
    # "Lastname Firstname" with the case the DB stores, otherwise the
    # lock_salesman value won't match any rows.
    # NSW
    ("Robinson Peter",   "peter.robinson@hankooktyre.com.au",   "NSW", "BDE"),
    ("Buckley Paul",     "paul.buckley@hankooktyre.com.au",     "NSW", "SM"),
    ("Borghese Alessio", "alessio.borghese@hankooktyre.com.au", "NSW", "BDE"),
    # QLD
    ("Marsh Aaron",      "aaron.marsh@hankooktyre.com.au",      "QLD", "SM"),
    ("Spires Steven",    "steven.spires@hankooktyre.com.au",    "QLD", "BDE"),
    ("Maclure Adam",     "adam.maclure@hankooktyre.com.au",     "QLD", "BDE"),
    # VIC / SA / TAS (one SM covers all three; the region filter
    # groups SA/TAS shops under VIC)
    ("Hobkirk Calvin",   "calvin.hobkirk@hankooktyre.com.au",   "VIC", "SM"),
    ("Bilston Kelley",   "kelley.bilston@hankooktyre.com.au",   "VIC", "BDE"),
    ("Bellotto Nicola",  "nicola.bellotto@hankooktyre.com.au",  "VIC", "BDE"),
    ("Gultjaeff Jason",  "jason.gultjaeff@hankooktyre.com.au",  "SA",  "BDE"),
    # WA
    ("Asim Qureshi",     "asim.qureshi@hankooktyre.com.au",     "WA",  "SM"),
    ("DAIS Jim",         "jim.dais@hankooktyre.com.au",         "WA",  "BDE"),
    # Leadership / dashboard ops — full scope
    ("Begbie Hayden",    "hayden.begbie@hankooktyre.com.au",    "NSW", "ALL"),
    ("Cho JunJong",      "junjong.cho@hankooktyre.com.au",      "NSW", "ALL"),
    ("Bhang Jayden",     "jayden.bhang@hankooktyre.com.au",     "NSW", "ALL"),
]
STATE_MANAGER_EMAIL = {
    "NSW": "paul.buckley@hankooktyre.com.au",
    "QLD": "aaron.marsh@hankooktyre.com.au",
    "VIC": "calvin.hobkirk@hankooktyre.com.au",
    "SA":  "calvin.hobkirk@hankooktyre.com.au",
    "TAS": "calvin.hobkirk@hankooktyre.com.au",
    "WA":  "asim.qureshi@hankooktyre.com.au",
}
ALWAYS_TO = ["hayden.begbie@hankooktyre.com.au",
             "junjong.cho@hankooktyre.com.au",
             "jayden.bhang@hankooktyre.com.au"]

def _bde_name_keys(full_name):
    """Generate uppercase lookup keys for both 'First Last' and 'Last First'."""
    if not full_name: return set()
    parts = full_name.strip().upper().split()
    if len(parts) < 2: return {full_name.strip().upper()}
    return {" ".join(parts), " ".join(reversed(parts))}

_BDE_EMAIL_MAP, _BDE_STATE_MAP, _BDE_ROLE_MAP = {}, {}, {}
_EMAIL_TO_DIR = {}     # email → (canonical_name, state, role)
for _entry in _BDE_DIRECTORY:
    _n, _e, _s, _r = _entry
    _EMAIL_TO_DIR.setdefault(_e.lower(), (_n, _s, _r))
    for _k in _bde_name_keys(_n):
        _BDE_EMAIL_MAP.setdefault(_k, _e)
        _BDE_STATE_MAP.setdefault(_k, _s)
        _BDE_ROLE_MAP.setdefault(_k, _r)

def _lookup_bde_email(name):
    return _BDE_EMAIL_MAP.get((name or "").strip().upper())

def _lookup_bde_state(name):
    return _BDE_STATE_MAP.get((name or "").strip().upper())

def _lookup_bde_role(name):
    return _BDE_ROLE_MAP.get((name or "").strip().upper())

# ── Microsoft Graph token cache ────────────────────────────────────
# Tokens live ~1h; we cache and refresh ~60s before expiry.
_GRAPH_TOKEN = {"access_token": None, "expires_at": 0.0}

def _get_graph_token():
    import time as _time, requests as _rq
    now = _time.time()
    cached = _GRAPH_TOKEN["access_token"]
    if cached and _GRAPH_TOKEN["expires_at"] > now + 60:
        return cached
    if not (GRAPH_TENANT_ID and GRAPH_CLIENT_ID and GRAPH_CLIENT_SECRET):
        raise RuntimeError("Graph creds not set: GRAPH_TENANT_ID / "
                           "GRAPH_CLIENT_ID / GRAPH_CLIENT_SECRET")
    r = _rq.post(
        f"https://login.microsoftonline.com/{GRAPH_TENANT_ID}/oauth2/v2.0/token",
        data={
            "client_id":     GRAPH_CLIENT_ID,
            "client_secret": GRAPH_CLIENT_SECRET,
            "scope":         "https://graph.microsoft.com/.default",
            "grant_type":    "client_credentials",
        },
        timeout=15,
    )
    if r.status_code != 200:
        raise RuntimeError(f"token endpoint {r.status_code}: {r.text[:300]}")
    j = r.json()
    _GRAPH_TOKEN["access_token"] = j["access_token"]
    _GRAPH_TOKEN["expires_at"]   = now + int(j.get("expires_in", 3600))
    return _GRAPH_TOKEN["access_token"]

def _graph_send(to_list, cc_list, subject, html_body):
    """Send via Microsoft Graph /sendMail.  Raises on failure."""
    import requests as _rq
    token = _get_graph_token()
    payload = {
        "message": {
            "subject": subject,
            "body":    {"contentType": "HTML", "content": html_body},
            "from":    {"emailAddress": {"address": MAIL_FROM}},
            "toRecipients": [{"emailAddress": {"address": x}} for x in (to_list or [])],
            "ccRecipients": [{"emailAddress": {"address": x}} for x in (cc_list or [])],
        },
        "saveToSentItems": "true",
    }
    r = _rq.post(
        f"https://graph.microsoft.com/v1.0/users/{MAIL_FROM}/sendMail",
        headers={"Authorization": f"Bearer {token}",
                 "Content-Type":  "application/json"},
        json=payload, timeout=20,
    )
    if r.status_code not in (200, 202):
        raise RuntimeError(f"graph sendMail {r.status_code}: {r.text[:400]}")

def _smtp_send(to_list, cc_list, subject, html_body):
    """Legacy SMTP send.  Raises on failure."""
    import smtplib, re as _re
    from email.message import EmailMessage
    if not SMTP_PASSWORD:
        raise RuntimeError("SMTP_PASSWORD not set")
    msg = EmailMessage()
    msg["From"] = MAIL_FROM
    msg["To"]   = ", ".join(to_list or [])
    if cc_list: msg["Cc"] = ", ".join(cc_list)
    msg["Subject"] = subject
    txt = _re.sub(r"<[^>]+>", "", html_body)
    txt = txt.replace("&nbsp;", " ").replace("&amp;", "&")
    msg.set_content(txt)
    msg.add_alternative(html_body, subtype="html")
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as s:
        if SMTP_USE_TLS: s.starttls()
        if SMTP_USER and SMTP_PASSWORD:
            s.login(SMTP_USER, SMTP_PASSWORD)
        s.send_message(msg, from_addr=MAIL_FROM,
                       to_addrs=(to_list or []) + (cc_list or []))

def _send_mail_async(to_list, cc_list, subject, html_body):
    """Pick Graph if creds present, else SMTP, else just log.  Runs the
    actual delivery on a background thread so the API request returns
    immediately."""
    import threading
    if not (to_list or cc_list):
        return
    if GRAPH_CLIENT_ID and GRAPH_CLIENT_SECRET and GRAPH_TENANT_ID:
        path, sender = "graph", _graph_send
    elif SMTP_PASSWORD:
        path, sender = "smtp",  _smtp_send
    else:
        if MAIL_DEBUG:
            print(f"[mail] would send to {to_list} cc {cc_list}: {subject}")
        else:
            print(f"[mail] no Graph or SMTP creds set — skipping "
                  f"(to {to_list}, subject={subject!r})")
        return

    def _run():
        try:
            sender(to_list, cc_list, subject, html_body)
            if MAIL_DEBUG:
                print(f"[mail/{path}] sent: {subject} → {to_list} cc {cc_list}")
        except Exception as e:
            traceback.print_exc()
            print(f"[mail/{path}] send failed: {e}")
    threading.Thread(target=_run, daemon=True).start()

def _esc_html(s):
    return (str(s or "")
            .replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
            .replace("\n","<br>"))

def _email_log_html(rid, bde_name, sold_to_name, ship_to, ship_to_name,
                    visit_date, met_person, notes, next_action, feedback,
                    visit_purpose=""):
    rows = []
    def _row(lbl, val):
        return (f'<tr><td style="padding:4px 10px;color:#6b7280;font-size:11px;'
                f'text-transform:uppercase;letter-spacing:.4px;width:120px;'
                f'vertical-align:top;">{_esc_html(lbl)}</td>'
                f'<td style="padding:4px 10px;color:#111827;font-size:13px;'
                f'vertical-align:top;">{val}</td></tr>')
    rows.append(_row("BDE",         _esc_html(bde_name)))
    rows.append(_row("Visit date",  _esc_html(visit_date)))
    rows.append(_row("Sold-to",     _esc_html(sold_to_name)))
    rows.append(_row("Ship-to",     f"{_esc_html(ship_to)} — {_esc_html(ship_to_name)}"))
    if visit_purpose:
        rows.append(_row("Purpose",  _esc_html(visit_purpose)))
    if met_person:
        rows.append(_row("Met",      _esc_html(met_person)))
    rows.append(_row("Notes",       f'<div style="white-space:pre-wrap;">{_esc_html(notes)}</div>'))
    if next_action:
        rows.append(_row("Next step", _esc_html(next_action)))
    if feedback:
        rows.append(_row("Feedback",
                        f'<div style="background:#fef3c7;border-left:3px solid #f59e0b;'
                        f'padding:6px 8px;color:#78350f;white-space:pre-wrap;">'
                        f'{_esc_html(feedback)}</div>'))
    link        = f"{DASHBOARD_URL}/meeting"
    comment_url = f"{DASHBOARD_URL}/meeting#fb-{rid}"
    return (
        f'<div style="font-family:-apple-system,Segoe UI,sans-serif;font-size:13px;color:#111827;">'
        f'<div style="background:#1e3a5f;color:#fff;padding:10px 14px;border-radius:6px 6px 0 0;'
        f'font-weight:700;">BDE Visit Log #{rid}</div>'
        f'<table style="border-collapse:collapse;background:#fff;width:100%;'
        f'border:1px solid #e5e7eb;border-top:none;border-radius:0 0 6px 6px;">'
        f'{"".join(rows)}</table>'
        f'<div style="margin-top:12px;text-align:center;">'
        f'<a href="{comment_url}" '
        f'style="display:inline-block;background:#2563eb;color:#fff;'
        f'padding:9px 22px;border-radius:6px;text-decoration:none;'
        f'font-weight:600;font-size:13px;">💬 Make a comment</a>'
        f'</div>'
        f'<div style="margin-top:10px;font-size:11px;color:#6b7280;">'
        f'Opens the feedback box for this visit on the dashboard. '
        f'Full log list: <a href="{link}">{link}</a></div>'
        f'</div>'
    )

def _resolve_bde_state(bde_name):
    """Try the hardcoded directory first; if the BDE isn't there (new
    hire, name format drift, etc.) fall back to the customer master
    via salesman_name → bde_state.  Empty string when nothing matches."""
    s = _lookup_bde_state(bde_name)
    if s: return s
    if not bde_name: return ""
    try:
        conn = get_connection(); cur = conn.cursor()
        cur.execute(
            "SELECT bde_state FROM customer "
            "WHERE UPPER(TRIM(salesman_name)) = %s "
            "AND bde_state IS NOT NULL AND TRIM(bde_state) <> '' "
            "ORDER BY (CASE WHEN bde_state IN ('NSW','QLD','VIC','SA','WA') "
            "          THEN 0 ELSE 1 END) "
            "LIMIT 1",
            (bde_name.strip().upper(),),
        )
        row = cur.fetchone()
        cur.close(); conn.close()
        return (row[0] or "").strip() if row else ""
    except Exception as e:
        print(f"[mail] _resolve_bde_state DB fallback failed: {e}")
        return ""

def _notify_new_log(rid, bde_name, sold_to_name, ship_to, ship_to_name,
                    visit_date, met_person, notes, next_action,
                    visit_purpose=""):
    """Email Hayden + JJ + Jayden + the State Manager of the BDE's
    state.  CC the BDE author so they have a record of what was sent
    on their behalf."""
    state    = _resolve_bde_state(bde_name)
    sm_email = STATE_MANAGER_EMAIL.get(state) if state else None
    author   = _lookup_bde_email(bde_name)
    to_list  = list(ALWAYS_TO)
    if sm_email and sm_email.lower() not in [x.lower() for x in to_list]:
        to_list.append(sm_email)
    cc_list  = [author] if author and author.lower() not in [x.lower() for x in to_list] else []
    subject  = (f"[BDE Visit] {bde_name} → {ship_to} "
                f"{ship_to_name or ''} ({visit_date})")
    # Surface the resolution so any "SM not getting mail" issue is
    # diagnosable from the console log without code changes.
    print(f"[mail] new_log #{rid} bde={bde_name!r} state={state!r} "
          f"sm={sm_email!r} → to={to_list} cc={cc_list}")
    html = _email_log_html(rid, bde_name, sold_to_name, ship_to, ship_to_name,
                           visit_date, met_person, notes, next_action, None,
                           visit_purpose=visit_purpose)
    _send_mail_async(to_list, cc_list, subject, html)

def _notify_feedback_thread(rid, log_row, thread, current_author_email=""):
    """Email everyone in the feedback thread when a new comment lands.

    Recipients = original BDE + every prior commenter's email +
    Hayden/JJ + State Manager — minus the person who just typed
    (no point mailing yourself back).  Each thread participant sees
    the entire conversation chronologically in the message body.
    """
    if not log_row or not thread:
        return
    me = (current_author_email or "").strip().lower()

    # Gather participants
    participants = set()
    bde_email = (log_row.get("bde_email") or "").strip().lower() \
                or (_lookup_bde_email(log_row.get("bde_name") or "") or "").lower()
    if bde_email: participants.add(bde_email)
    for t in thread:
        em = (t.get("author_email") or "").strip().lower()
        if em: participants.add(em)
        # If we only have the name, resolve via the directory.
        if not em:
            resolved = _lookup_bde_email(t.get("author_name") or "")
            if resolved: participants.add(resolved.lower())

    # Always-notify recipients
    for em in ALWAYS_TO:
        participants.add(em.lower())
    state    = _lookup_bde_state(log_row.get("bde_name") or "")
    sm_email = STATE_MANAGER_EMAIL.get(state) if state else None
    if sm_email: participants.add(sm_email.lower())

    # Don't mail the person who just commented.
    if me: participants.discard(me)
    if not participants:
        return

    last  = thread[-1]
    actor = (last.get("author_name") or last.get("author_email") or "Someone")
    subj  = (f"[BDE Visit] {actor} commented on {log_row.get('bde_name','')}'s "
             f"{log_row.get('visit_date','')} visit — {log_row.get('ship_to','')}")

    # Build the thread block in HTML so the reader gets the full
    # conversation, latest at the bottom (chronological reading order).
    thread_html_parts = []
    for t in thread:
        meta = (f"<div style='font-size:11px;color:#6b7280;margin-bottom:2px;'>"
                f"<b>{_esc_html(t.get('author_name') or t.get('author_email') or '—')}</b> "
                f"· {_esc_html(t.get('created_at') or '')}</div>")
        body_div = (f"<div style='white-space:pre-wrap;font-size:13px;color:#111827;"
                    f"padding:6px 10px;border-left:3px solid #f59e0b;"
                    f"background:#fef3c7;border-radius:4px;margin-bottom:6px;'>"
                    f"{_esc_html(t.get('text') or '')}</div>")
        thread_html_parts.append(meta + body_div)
    thread_html = "".join(thread_html_parts)

    # Log card + thread
    card = _email_log_html(
        rid,
        log_row.get("bde_name") or "",
        log_row.get("sold_to_name") or log_row.get("sold_to") or "",
        log_row.get("ship_to") or "",
        log_row.get("ship_to_name") or "",
        log_row.get("visit_date") or "",
        log_row.get("met_person") or "",
        log_row.get("notes") or "",
        log_row.get("next_action") or "",
        None,           # feedback is shown as a thread below instead
    )
    link        = f"{DASHBOARD_URL}/meeting"
    comment_url = f"{DASHBOARD_URL}/meeting#fb-{rid}"
    full = (
        f"{card}"
        f"<div style='margin-top:14px;font-family:-apple-system,Segoe UI,sans-serif;"
        f"font-size:13px;color:#111827;'>"
        f"<div style='font-weight:700;color:#1e3a5f;margin-bottom:6px;'>Feedback thread</div>"
        f"{thread_html}"
        f"<div style='margin-top:12px;text-align:center;'>"
        f"<a href='{comment_url}' "
        f"style='display:inline-block;background:#2563eb;color:#fff;"
        f"padding:9px 22px;border-radius:6px;text-decoration:none;"
        f"font-weight:600;font-size:13px;'>💬 Make a comment</a>"
        f"</div>"
        f"<div style='margin-top:10px;font-size:11px;color:#6b7280;'>"
        f"Reply opens the feedback box for this visit on the dashboard. "
        f"<a href='{link}'>{link}</a></div>"
        f"</div>"
    )
    _send_mail_async(list(participants), [], subj, full)

def _ensure_meeting_log_table():
    """Create the meeting_log table on startup if it doesn't exist."""
    try:
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS meeting_log (
                id           INT AUTO_INCREMENT PRIMARY KEY,
                created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                visit_date   DATE NOT NULL,
                bde_email    VARCHAR(120) NOT NULL DEFAULT '',
                bde_name     VARCHAR(120) NOT NULL DEFAULT '',
                sold_to      VARCHAR(64)  NOT NULL DEFAULT '',
                sold_to_name VARCHAR(160) NOT NULL DEFAULT '',
                ship_to      VARCHAR(64)  NOT NULL DEFAULT '',
                met_person   VARCHAR(120) NOT NULL DEFAULT '',
                notes        TEXT,
                next_action  TEXT,
                photo_paths  TEXT,
                INDEX  idx_meeting_ship_to (ship_to),
                INDEX  idx_meeting_visit_date (visit_date),
                INDEX  idx_meeting_bde     (bde_name)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        # Threaded feedback — each row = one comment in the conversation
        # on a meeting_log entry.  Replaces the single TEXT column on
        # meeting_log (which is kept around as a denormalised "latest"
        # cache for older code paths).
        cur.execute("""
            CREATE TABLE IF NOT EXISTS meeting_feedback (
                id           INT AUTO_INCREMENT PRIMARY KEY,
                meeting_id   INT NOT NULL,
                created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                author_email VARCHAR(120) NOT NULL DEFAULT '',
                author_name  VARCHAR(120) NOT NULL DEFAULT '',
                text         TEXT NOT NULL,
                INDEX idx_fb_meeting (meeting_id),
                INDEX idx_fb_created (created_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        # Idempotent migration for older meeting_log tables.
        migrations = [
            "ALTER TABLE meeting_log MODIFY COLUMN sold_to VARCHAR(64) NOT NULL DEFAULT ''",
            "ALTER TABLE meeting_log MODIFY COLUMN ship_to VARCHAR(64) NOT NULL DEFAULT ''",
        ]
        for sql in migrations:
            try: cur.execute(sql)
            except Exception: pass
        # Add columns that may not exist on legacy tables.
        for col, defn, after in (
            ("sold_to_name",  "VARCHAR(160) NOT NULL DEFAULT ''", "sold_to"),
            ("visit_date",    "DATE NOT NULL DEFAULT '1970-01-01'", "created_at"),
            ("feedback",      "TEXT",                              "next_action"),
            ("visit_purpose", "VARCHAR(40) NOT NULL DEFAULT ''",   "ship_to"),
        ):
            try:
                cur.execute(f"ALTER TABLE meeting_log "
                            f"ADD COLUMN {col} {defn} AFTER {after}")
            except Exception:
                pass
        # One-time migration: if meeting_feedback is empty but some
        # meeting_log rows still carry single-string feedback, lift
        # those into the thread as a "(legacy)" first comment.
        try:
            cur.execute("SELECT COUNT(*) FROM meeting_feedback")
            (n,) = cur.fetchone()
            if n == 0:
                cur.execute("""
                    INSERT INTO meeting_feedback
                        (meeting_id, created_at, author_email, author_name, text)
                    SELECT id, created_at, '', '(legacy)', feedback
                    FROM meeting_log
                    WHERE feedback IS NOT NULL AND TRIM(feedback) <> ''
                """)
        except Exception as e:
            print(f"[meeting_feedback] legacy backfill skipped: {e}")
        # Backfill visit_date from created_at where it's still the sentinel.
        try:
            cur.execute("UPDATE meeting_log SET visit_date = DATE(created_at) "
                        "WHERE visit_date = '1970-01-01'")
        except Exception:
            pass
        try:
            cur.execute("ALTER TABLE meeting_log "
                        "ADD INDEX idx_meeting_visit_date (visit_date)")
        except Exception:
            pass
        conn.commit()
        cur.close(); conn.close()
        os.makedirs(MEETING_PHOTO_DIR, exist_ok=True)
    except Exception as e:
        print(f"[meeting_log] schema init failed: {e}")

_ensure_meeting_log_table()

def _ensure_meeting_plan_table():
    """One-row-per-planned-visit table backing the drag-and-drop
    calendar on /meeting.  Independent from meeting_log: a plan is
    intent (\"visit this shop next Tuesday\"), a log is the memo
    written after the actual visit."""
    try:
        conn = get_connection(); cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS meeting_plan (
                id           INT AUTO_INCREMENT PRIMARY KEY,
                plan_date    DATE NOT NULL,
                ship_to      VARCHAR(64) NOT NULL,
                sold_to      VARCHAR(64) NOT NULL DEFAULT '',
                ship_to_name VARCHAR(160) NOT NULL DEFAULT '',
                sold_to_name VARCHAR(160) NOT NULL DEFAULT '',
                bde_email    VARCHAR(120) NOT NULL DEFAULT '',
                bde_name     VARCHAR(120) NOT NULL DEFAULT '',
                created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_plan_date    (plan_date),
                INDEX idx_plan_ship_to (ship_to),
                INDEX idx_plan_bde     (bde_email),
                INDEX idx_plan_bde_nm  (bde_name)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        conn.commit(); cur.close(); conn.close()
    except Exception as e:
        print(f"[meeting_plan] schema init failed: {e}")

_ensure_meeting_plan_table()

def _bde_from_request():
    """Best-effort 'who is logged in'.  Cloudflare Access (if it's in
    front of the app) injects Cf-Access-Authenticated-User-Email; behind
    Tailscale / local dev that header is absent, so the form also sends
    bde_name as a fallback (manual pick)."""
    email = (request.headers.get("Cf-Access-Authenticated-User-Email")
             or request.headers.get("cf-access-authenticated-user-email")
             or "").strip().lower()
    return email

@app.get("/meeting")
def meeting_page():
    return send_from_directory("static", "meeting.html")

@app.get("/api/whoami")
def whoami():
    """Identify the current user from Cloudflare Access and return
    the scope locks the frontend should apply.

    BDE  → lock_salesman = own name (own territory + amber overlay
           markers for shops visited outside it on the map).
    SM   → lock_region   = own state.
    ALL  → no locks (leadership / Hayden / JJ / dashboard ops).

    No CF header (Tailscale / local dev) or unknown email → no locks
    so internal access from the office network keeps working."""
    email = _bde_from_request()
    out = {
        "email":          email,
        "name":           "",
        "role":           "ALL",
        "state":          "",
        "lock_salesman":  None,
        "lock_region":    None,
        "logged_in":      bool(email),
    }
    if not email:
        return jsonify(out)
    found = _EMAIL_TO_DIR.get(email.lower())
    if not found:
        # Recognised by CF but not in our directory — let them in with
        # full scope so we don't accidentally lock out a new hire.
        return jsonify(out)
    name, state, role = found
    out["name"]  = name
    out["state"] = state
    out["role"]  = role
    if role == "BDE":
        out["lock_salesman"] = name
    elif role == "SM":
        out["lock_region"]   = state
    return jsonify(out)

@app.get("/api/mail_test")
def mail_test():
    """Diagnostic: try sending one test email synchronously so any error
    surfaces in the HTTP response.  Picks Graph if creds are set, else
    SMTP.  Use ?to=you@example.com."""
    to = (request.args.get("to") or "").strip()
    if not to:
        return jsonify({"error": "missing ?to=email"}), 400

    report = {
        "mail_from":          MAIL_FROM,
        "graph_tenant_set":   bool(GRAPH_TENANT_ID),
        "graph_client_set":   bool(GRAPH_CLIENT_ID),
        "graph_secret_set":   bool(GRAPH_CLIENT_SECRET),
        "smtp_host":          SMTP_HOST,
        "smtp_port":          SMTP_PORT,
        "smtp_user":          SMTP_USER,
        "smtp_password_set":  bool(SMTP_PASSWORD),
        "to":                 to,
    }
    subject = "[BDE Visit] Mail-path test from the dashboard"
    body    = ("<p>This is a test message from the dashboard.</p>"
               "<p>If you got it, mail delivery is wired up correctly.</p>")

    if GRAPH_TENANT_ID and GRAPH_CLIENT_ID and GRAPH_CLIENT_SECRET:
        report["path"] = "graph"
        try:
            _graph_send([to], [], subject, body)
            report["result"] = "SENT via Microsoft Graph — check inbox (and spam)"
            return jsonify(report)
        except Exception as e:
            report["result"] = f"FAILED — {type(e).__name__}: {e}"
            return jsonify(report), 500

    if SMTP_PASSWORD:
        report["path"] = "smtp"
        try:
            _smtp_send([to], [], subject, body)
            report["result"] = "SENT via SMTP — check inbox (and spam)"
            return jsonify(report)
        except Exception as e:
            report["result"] = f"FAILED — {type(e).__name__}: {e}"
            return jsonify(report), 500

    report["path"]   = "none"
    report["result"] = ("SKIPPED — set GRAPH_TENANT_ID / GRAPH_CLIENT_ID / "
                        "GRAPH_CLIENT_SECRET (recommended) or SMTP_PASSWORD in .env")
    return jsonify(report)

@app.get("/api/people")
def people_directory():
    """Names that can leave feedback — BDEs + State Managers + Hayden + JJ.
    Sourced from the hardcoded _BDE_DIRECTORY so it works without DB."""
    out = []
    seen = set()
    for nm, em, st, _role in _BDE_DIRECTORY:
        key = nm.upper()
        if key in seen: continue
        seen.add(key)
        out.append({"name": nm, "email": em, "state": st})
    return jsonify(out)

@app.get("/api/bdes_active")
def bdes_active():
    """Distinct salesman names from the customer master, dropdown source
    for the meeting form."""
    try:
        conn = get_connection(); cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT DISTINCT TRIM(salesman_name) AS name
            FROM customer
            WHERE salesman_name IS NOT NULL
              AND TRIM(salesman_name) <> ''
              AND TRIM(salesman_name) <> '#N/A'
            ORDER BY name
        """)
        out = [r["name"] for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify(out)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.post("/api/meeting")
def meeting_post():
    """Insert one visit-log entry.  Accepts multipart/form-data so the
    optional photo uploads (up to 5 files) can come in the same request."""
    try:
        bde_email = _bde_from_request()
        bde_name  = (request.form.get("bde_name") or "").strip()
        sold_in   = (request.form.get("sold_to")  or "").strip()
        ship_to   = (request.form.get("ship_to")  or "").strip()
        met       = (request.form.get("met_person") or "").strip()
        notes     = (request.form.get("notes") or "").strip()
        next_act  = (request.form.get("next_action") or "").strip()
        feedback  = (request.form.get("feedback") or "").strip()
        visit_in  = (request.form.get("visit_date") or "").strip()
        purpose   = (request.form.get("visit_purpose") or "").strip()

        if not ship_to:
            return jsonify({"error": "ship_to is required"}), 400
        if not notes:
            return jsonify({"error": "notes is required"}), 400
        if not purpose:
            return jsonify({"error": "visit_purpose is required"}), 400
        if purpose not in ("Promotion", "Product introduction",
                           "Claim support", "Rebate follow-up", "Other"):
            return jsonify({"error": "invalid visit_purpose"}), 400

        # Visit date: YYYY-MM-DD; default to today if blank/invalid.
        # Capped at today so BDE can backfill past visits but can't
        # invent future ones.
        from datetime import date as _date_cls
        visit_date_obj = _date_cls.today()
        if visit_in:
            try:
                visit_date_obj = datetime.strptime(visit_in, "%Y-%m-%d").date()
            except ValueError:
                pass
        if visit_date_obj > _date_cls.today():
            visit_date_obj = _date_cls.today()

        # The Sold-to picker submits the NAME (because /api/sold_to_names
        # only exposes names).  We resolve it to a sold-to CODE via the
        # customer master so meeting_log lines up with the rest of the
        # database.  If lookup fails, fall back to storing the raw input
        # truncated to fit the column.
        sold_to, sold_to_name = "", sold_in
        if sold_in:
            try:
                _conn = get_connection()
                _cur  = _conn.cursor(dictionary=True)
                # If ship_to is known, prefer the sold_to that actually
                # owns this ship_to (handles the rare case of two sold_tos
                # sharing a sold_to_name).
                if ship_to:
                    _cur.execute(
                        "SELECT sold_to, sold_to_name FROM customer "
                        "WHERE ship_to = %s LIMIT 1", (ship_to,))
                    r = _cur.fetchone()
                    if r and r["sold_to"]:
                        sold_to      = str(r["sold_to"])
                        sold_to_name = (r["sold_to_name"] or sold_in).strip()
                if not sold_to:
                    _cur.execute(
                        "SELECT sold_to FROM customer "
                        "WHERE TRIM(sold_to_name) = %s "
                        "ORDER BY sold_to LIMIT 1", (sold_in,))
                    r = _cur.fetchone()
                    if r and r["sold_to"]:
                        sold_to = str(r["sold_to"])
                _cur.close(); _conn.close()
            except Exception as e:
                print(f"[meeting] sold_to resolve failed: {e}")
        # Belt-and-braces in case lookup failed and the raw input is huge:
        sold_to      = (sold_to or sold_in)[:64]
        sold_to_name = sold_to_name[:160]
        ship_to      = ship_to[:64]

        # Save uploaded photos under static/meeting_photos/YYYY/MM/...
        # using <timestamp>_<ship_to>_<idx>.<ext>.  Up to 5 files honoured;
        # everything else discarded.
        saved = []
        files = request.files.getlist("photos") or []
        if files:
            now    = datetime.now()
            subdir = os.path.join(MEETING_PHOTO_DIR,
                                  f"{now.year:04d}", f"{now.month:02d}")
            os.makedirs(subdir, exist_ok=True)
            ts = now.strftime("%Y%m%d_%H%M%S")
            for i, f in enumerate(files[:5]):
                if not f or not f.filename:
                    continue
                ext = os.path.splitext(f.filename)[1].lower()
                if ext not in (".jpg", ".jpeg", ".png", ".webp", ".heic"):
                    continue
                safe_ship = "".join(c for c in ship_to if c.isalnum()) or "X"
                fname = f"{ts}_{safe_ship}_{i}{ext}"
                path  = os.path.join(subdir, fname)
                f.save(path)
                # Store the URL relative to /static so the frontend can
                # link straight to it.
                saved.append(f"/static/meeting_photos/{now.year:04d}/{now.month:02d}/{fname}")

        conn = get_connection(); cur = conn.cursor()
        cur.execute("""
            INSERT INTO meeting_log
                (visit_date, bde_email, bde_name, sold_to, sold_to_name,
                 ship_to, visit_purpose, met_person, notes, next_action,
                 feedback, photo_paths)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (visit_date_obj, bde_email, bde_name, sold_to, sold_to_name,
              ship_to, purpose, met, notes, next_act,
              feedback if feedback else None,
              ",".join(saved) if saved else None))
        new_id = cur.lastrowid
        # Pull the resolved ship_to_name back out so the notification
        # email can show a friendly label without an extra round trip.
        ship_nm = ""
        try:
            cur.execute("SELECT MIN(NULLIF(TRIM(ship_to_name),'')) FROM customer "
                        "WHERE ship_to = %s", (ship_to,))
            r = cur.fetchone()
            if r and r[0]: ship_nm = r[0]
        except Exception:
            pass
        conn.commit()
        cur.close(); conn.close()

        # Fire-and-forget notification: Hayden + JJ + State Manager,
        # CC the BDE author.  Runs in a background thread so the form
        # POST returns immediately.
        try:
            _notify_new_log(new_id, bde_name, sold_to_name or sold_to,
                            ship_to, ship_nm,
                            visit_date_obj.strftime("%Y-%m-%d"),
                            met, notes, next_act,
                            visit_purpose=purpose)
        except Exception as e:
            print(f"[meeting] notify_new_log failed: {e}")

        return jsonify({"ok": True, "id": new_id, "photos": saved})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.post("/api/meeting/feedback")
def meeting_feedback():
    """Append a comment to a meeting_log's feedback thread.

    Body (JSON or form):  id, text, author_name
      • id           – meeting_log.id to comment on
      • text         – the comment body
      • author_name  – display name (resolved to email via the BDE
                       directory; falls back to the CF Access header
                       if running behind Cloudflare Access).

    The same payload key still accepts 'feedback' for backward compat
    with the older single-string call shape.
    """
    try:
        body  = request.get_json(silent=True) or {}
        rid   = body.get("id") or request.form.get("id")
        text  = (body.get("text") or body.get("feedback")
                 or request.form.get("text") or request.form.get("feedback")
                 or "").strip()
        a_nm  = (body.get("author_name") or request.form.get("author_name") or "").strip()
        try:
            rid = int(rid)
        except (TypeError, ValueError):
            return jsonify({"error": "invalid id"}), 400
        if not text:
            return jsonify({"error": "text is required"}), 400

        # Resolve author email: prefer CF Access header → fall back to
        # directory lookup by the picked name.
        a_email = _bde_from_request() or _lookup_bde_email(a_nm) or ""

        conn = get_connection(); cur = conn.cursor(dictionary=True)
        cur.execute("""
            INSERT INTO meeting_feedback
                (meeting_id, author_email, author_name, text)
            VALUES (%s, %s, %s, %s)
        """, (rid, a_email, a_nm, text))
        new_fb_id = cur.lastrowid

        # Keep meeting_log.feedback as a denormalised "latest comment"
        # cache so existing list queries that only read that column
        # still surface something useful.
        cur.execute("UPDATE meeting_log SET feedback=%s WHERE id=%s",
                    (text, rid))

        # Re-read the meeting_log row + everyone who has already
        # commented on this thread → those are the email recipients.
        cur.execute("""
            SELECT m.id, m.visit_date, m.bde_name, m.bde_email,
                   m.sold_to, m.sold_to_name, m.ship_to,
                   m.met_person, m.notes, m.next_action,
                   COALESCE(NULLIF(TRIM(c.ship_to_name),''), m.ship_to) AS ship_to_name
            FROM meeting_log m
            LEFT JOIN (
                SELECT ship_to, MIN(NULLIF(TRIM(ship_to_name),'')) AS ship_to_name
                FROM customer GROUP BY ship_to
            ) c ON c.ship_to = m.ship_to
            WHERE m.id = %s
        """, (rid,))
        log_row = cur.fetchone()
        if log_row and log_row.get("visit_date"):
            log_row["visit_date"] = log_row["visit_date"].strftime("%Y-%m-%d")

        cur.execute("""
            SELECT id, created_at, author_email, author_name, text
            FROM meeting_feedback
            WHERE meeting_id = %s
            ORDER BY created_at ASC, id ASC
        """, (rid,))
        thread = cur.fetchall()
        for t in thread:
            if t.get("created_at"):
                t["created_at"] = t["created_at"].strftime("%Y-%m-%d %H:%M")

        conn.commit()
        cur.close(); conn.close()

        # Notify everyone touching this thread (original BDE +
        # everyone who has commented before + Hayden/JJ + State Manager)
        # except the person who just typed this comment.
        try:
            _notify_feedback_thread(rid, log_row, thread,
                                    current_author_email=a_email)
        except Exception as e:
            print(f"[meeting] notify_feedback_thread failed: {e}")

        return jsonify({"ok": True, "id": rid, "feedback_id": new_fb_id,
                        "thread": thread})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# ── /meeting calendar planner ──────────────────────────────────────
@app.get("/api/meeting_plan")
def meeting_plan_list():
    """Return scheduled visits for a month (Y/M).  Scope filter mirrors
    the rest of the dashboard:
      • BDE → only their own plans.
      • SM  → plans by any BDE who belongs to their state.
      • ALL → everyone (Hayden / JJ / Jayden / unknown emails).
    """
    try:
        year  = int(request.args.get("year")  or datetime.now().year)
        month = int(request.args.get("month") or datetime.now().month)
    except Exception:
        return jsonify({"error": "year/month must be integers"}), 400

    email = _bde_from_request()
    me    = _EMAIL_TO_DIR.get(email.lower()) if email else None
    role  = me[2] if me else "ALL"
    bde   = me[0] if me else None
    state = me[1] if me else None

    wh = ["YEAR(plan_date) = %s", "MONTH(plan_date) = %s"]
    params = [year, month]
    if role == "BDE" and bde:
        wh.append("UPPER(bde_name) = %s")
        params.append(bde.upper())
    elif role == "SM" and state:
        # All BDEs whose home state matches this SM
        state_bdes = [n for (n, _e, s, _r) in _BDE_DIRECTORY if s == state]
        if state_bdes:
            placeholders = ",".join(["%s"] * len(state_bdes))
            wh.append(f"UPPER(bde_name) IN ({placeholders})")
            params.extend([n.upper() for n in state_bdes])

    try:
        conn = get_connection(); cur = conn.cursor(dictionary=True)
        cur.execute(f"""
            SELECT p.id, p.plan_date, p.ship_to, p.sold_to,
                   p.ship_to_name, p.sold_to_name,
                   p.bde_email, p.bde_name,
                   EXISTS(
                     SELECT 1 FROM meeting_log m
                     WHERE m.ship_to = p.ship_to
                       AND YEAR(m.visit_date)  = YEAR(p.plan_date)
                       AND MONTH(m.visit_date) = MONTH(p.plan_date)
                   ) AS logged
            FROM meeting_plan p
            WHERE {' AND '.join(wh)}
            ORDER BY p.plan_date, p.id
        """, tuple(params))
        rows = cur.fetchall()
        for r in rows:
            if r.get("plan_date"):
                r["plan_date"] = r["plan_date"].strftime("%Y-%m-%d")
            r["logged"] = bool(r.get("logged"))
        cur.close(); conn.close()
        return jsonify(rows)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.post("/api/meeting_plan")
def meeting_plan_create():
    """Schedule one ship_to on a date.  Body (JSON or form):
    {date: YYYY-MM-DD, ship_to: <code>, bde_name: <name> }
    bde_name is sent by the client (selected in the form) — for BDE
    role we override with their own name to prevent scheduling on
    behalf of someone else."""
    try:
        body  = request.get_json(silent=True) or {}
        date  = (body.get("date")     or request.form.get("date")     or "").strip()
        ship  = (body.get("ship_to")  or request.form.get("ship_to")  or "").strip()
        bdenm = (body.get("bde_name") or request.form.get("bde_name") or "").strip()
        if not date or not ship:
            return jsonify({"error": "date and ship_to are required"}), 400
        try:
            plan_d = datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError:
            return jsonify({"error": "date must be YYYY-MM-DD"}), 400

        # BDE-scope: force the planner to be the signed-in BDE.
        email = _bde_from_request()
        found = _EMAIL_TO_DIR.get(email.lower()) if email else None
        if found and found[2] == "BDE":
            bdenm = found[0]
            bde_email = email
        else:
            bde_email = email or ""

        # Resolve sold_to + names from customer master so the calendar
        # chip can show a friendly label without a separate lookup.
        sold_to, sold_to_name, ship_to_name = "", "", ""
        try:
            conn = get_connection(); cur = conn.cursor(dictionary=True)
            cur.execute("""
                SELECT MIN(sold_to)      AS sold_to,
                       MIN(NULLIF(TRIM(sold_to_name),'')) AS sold_to_name,
                       MIN(NULLIF(TRIM(ship_to_name),'')) AS ship_to_name
                FROM customer
                WHERE ship_to = %s
            """, (ship,))
            r = cur.fetchone()
            if r:
                sold_to      = str(r.get("sold_to") or "")
                sold_to_name = r.get("sold_to_name") or ""
                ship_to_name = r.get("ship_to_name") or ""
            cur.close(); conn.close()
        except Exception:
            pass

        conn = get_connection(); cur = conn.cursor()
        cur.execute("""
            INSERT INTO meeting_plan
                (plan_date, ship_to, sold_to, ship_to_name, sold_to_name,
                 bde_email, bde_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (plan_d, ship[:64], sold_to[:64], ship_to_name[:160], sold_to_name[:160],
              bde_email[:120], bdenm[:120]))
        new_id = cur.lastrowid
        conn.commit(); cur.close(); conn.close()
        return jsonify({"ok": True, "id": new_id,
                        "plan_date":   plan_d.strftime("%Y-%m-%d"),
                        "ship_to":     ship,
                        "ship_to_name": ship_to_name,
                        "sold_to":     sold_to,
                        "sold_to_name": sold_to_name,
                        "bde_name":    bdenm,
                        "bde_email":   bde_email})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.delete("/api/meeting_plan/<int:plan_id>")
def meeting_plan_delete(plan_id):
    try:
        # If signed-in user is a BDE, restrict delete to their own rows.
        email = _bde_from_request()
        found = _EMAIL_TO_DIR.get(email.lower()) if email else None
        bde_scope = found[0] if (found and found[2] == "BDE") else None

        conn = get_connection(); cur = conn.cursor()
        if bde_scope:
            cur.execute("DELETE FROM meeting_plan "
                        "WHERE id = %s AND UPPER(bde_name) = %s",
                        (plan_id, bde_scope.upper()))
        else:
            cur.execute("DELETE FROM meeting_plan WHERE id = %s", (plan_id,))
        n = cur.rowcount
        conn.commit(); cur.close(); conn.close()
        return jsonify({"ok": True, "deleted": n})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.get("/api/meeting_plan/shop_to_options")
def meeting_plan_shop_options():
    """Slim ship_to list for the drag-and-drop palette.  Returns the
    full customer master (capped at 3000) so a BDE can plan visits
    outside their assigned territory too — matches the form-side
    behaviour."""
    try:
        conn = get_connection(); cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT DISTINCT ship_to,
                   MIN(NULLIF(TRIM(ship_to_name),'')) AS ship_to_name,
                   MIN(sold_to)                       AS sold_to,
                   MIN(NULLIF(TRIM(sold_to_name),'')) AS sold_to_name
            FROM customer
            WHERE ship_to IS NOT NULL AND TRIM(ship_to_name) <> ''
            GROUP BY ship_to
            ORDER BY MIN(TRIM(ship_to_name))
            LIMIT 3000
        """)
        rows = cur.fetchall()
        cur.close(); conn.close()
        return jsonify(rows)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.get("/api/meeting/list")
def meeting_list():
    """Recent entries.  Optional ?ship_to=… / ?bde=… / ?sold_to=… /
    ?limit=N filters."""
    try:
        ship_to = (request.args.get("ship_to") or "").strip()
        bde     = (request.args.get("bde")     or "").strip()
        sold_to = (request.args.get("sold_to") or "").strip()
        limit   = max(1, min(500, int(request.args.get("limit", 100) or 100)))

        wh, params = [], []
        if ship_to:
            # Partial match against either the ship_to code OR the
            # shop name (from customer master via the LEFT JOIN below).
            like = f"%{ship_to}%"
            wh.append("(m.ship_to LIKE %s "
                      "  OR EXISTS (SELECT 1 FROM customer cc "
                      "             WHERE cc.ship_to = m.ship_to "
                      "             AND TRIM(cc.ship_to_name) LIKE %s))")
            params.extend([like, like])
        if bde:
            wh.append("m.bde_name = %s"); params.append(bde)
        if sold_to:
            # Partial match against any of: sold_to code, the
            # denormalised sold_to_name stored on the log row, and
            # the customer master's sold_to_name for the ship_to.
            like = f"%{sold_to}%"
            wh.append("(m.sold_to LIKE %s "
                      "  OR TRIM(m.sold_to_name) LIKE %s "
                      "  OR EXISTS (SELECT 1 FROM customer cc "
                      "             WHERE cc.ship_to = m.ship_to "
                      "             AND TRIM(cc.sold_to_name) LIKE %s))")
            params.extend([like, like, like])
        where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

        conn = get_connection(); cur = conn.cursor(dictionary=True)
        cur.execute(f"""
            SELECT m.id, m.created_at, m.visit_date,
                   m.bde_email, m.bde_name,
                   m.sold_to, m.ship_to, m.visit_purpose, m.met_person,
                   m.notes, m.next_action, m.feedback, m.photo_paths,
                   COALESCE(NULLIF(TRIM(c.ship_to_name),''), m.ship_to) AS ship_to_name,
                   COALESCE(NULLIF(TRIM(c.sold_to_name),''),
                            NULLIF(TRIM(m.sold_to_name),''),
                            m.sold_to) AS sold_to_name
            FROM meeting_log m
            LEFT JOIN (
                SELECT ship_to,
                       MIN(NULLIF(TRIM(ship_to_name),'')) AS ship_to_name,
                       MIN(NULLIF(TRIM(sold_to_name),'')) AS sold_to_name
                FROM customer GROUP BY ship_to
            ) c ON c.ship_to = m.ship_to
            {where_sql}
            ORDER BY m.bde_name ASC, m.ship_to ASC,
                     m.visit_purpose ASC, m.visit_date DESC, m.created_at DESC
            LIMIT {limit}
        """, tuple(params))
        rows = cur.fetchall()
        ids = [r["id"] for r in rows] if rows else []
        thread_by_meeting = {}
        if ids:
            placeholders = ",".join(["%s"] * len(ids))
            cur.execute(f"""
                SELECT id, meeting_id, created_at, author_email, author_name, text
                FROM meeting_feedback
                WHERE meeting_id IN ({placeholders})
                ORDER BY created_at ASC, id ASC
            """, tuple(ids))
            for t in cur.fetchall():
                if t.get("created_at"):
                    t["created_at"] = t["created_at"].strftime("%Y-%m-%d %H:%M")
                thread_by_meeting.setdefault(t["meeting_id"], []).append(t)
        for r in rows:
            r["created_at"] = r["created_at"].strftime("%Y-%m-%d %H:%M") if r["created_at"] else ""
            r["visit_date"] = r["visit_date"].strftime("%Y-%m-%d") if r["visit_date"] else ""
            r["photo_paths"] = r["photo_paths"].split(",") if r["photo_paths"] else []
            r["thread"]      = thread_by_meeting.get(r["id"], [])
        cur.close(); conn.close()
        return jsonify(rows)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))   # Cloudtype probes 5000
    from price_compare import price_dashboard, load_all_months, build_data
    app.add_url_rule('/price', 'price_dashboard', price_dashboard)

    @app.get("/api/price_debug")
    def price_debug():
        from flask import jsonify
        monthly = load_all_months()
        if not monthly:
            return jsonify({"error": "no monthly data"})
        data = build_data(monthly)
        return jsonify({
            "months": data["months"],
            "store_avg_tempe": data["store_avg"]["tempe"],
            "brand_size_data_keys": list(data["brand_size_data"].keys()),
            "sample": {abbr: list(sizes.keys())[:3] for abbr, sizes in list(data["brand_size_data"].items())[:3]},
        })

    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
