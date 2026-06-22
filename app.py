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

# SA/TAS roll up to VIC, NT to WA, ACT to NSW.  Used both inside
# api_stock and by the claim portal's notification routing.
STATE_REMAP = {"SA": "VIC", "NT": "WA", "TAS": "VIC", "ACT": "NSW"}

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


# ?? Helpers for normalised sales tables (line/product_group/pattern/inch live in carrying_26) ??

def _carrying_join(alias: str) -> str:
    """Returns the LEFT JOIN clause for carrying_26 using alias 'mat'."""
    return f"LEFT JOIN carrying_26 mat ON mat.m_code = {alias}.material"


def _ensure_carrying_join(alias: str, joins: list) -> None:
    """Adds the carrying_26 join to 'joins' if it is not already present."""
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
    have been removed and now live in carrying_26 (alias: mat).

    Returns (joins, wheres) — same contract as category_filters().

    has_brand=True signals the fact table has its own `brand` column
    (currently only sales_thismonth).  For HK / LF that path filters
    directly on `{alias}.brand` so we don't drop rows where the
    material isn't in carrying_26 (a real case — Rebate brand-tagged
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
        # Brand filter via carrying_26.  sales_thismonth has its own
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
    carrying_26 so we JOIN that table (alias: mat) for category filtering,
    exactly like category_filters_sales() does for sales fact tables.
    """
    joins, wh = [], []
    cat = (category or "ALL").upper()

    if cat == "ALL":
        return joins, wh

    carrying_join = f"LEFT JOIN carrying_26 mat ON mat.m_code = {alias}.material"

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
        # product_group lives in carrying_26 (alias: mat) for target_26
        joins.append(f"LEFT JOIN carrying_26 mat ON mat.m_code = {alias}.material")
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

# ─── Per-request usage logging ───────────────────────────────────────
# Lightweight middleware that writes one row per HTTP request into
# request_log so we can answer questions Cloudflare Access logs can't:
# "which BDE opened /sales/rebate yesterday", "how many times did Asim
# open the meeting view this week", "which paths get the most traffic".
# Skips static + favicon to keep the table from filling with noise.
import time as _time

def _ensure_request_log_table():
    try:
        conn = get_connection(); cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS request_log (
                id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                user_email  VARCHAR(255) NOT NULL DEFAULT '',
                path        VARCHAR(255) NOT NULL,
                method      VARCHAR(10)  NOT NULL DEFAULT 'GET',
                status      INT          NOT NULL DEFAULT 0,
                duration_ms INT          NOT NULL DEFAULT 0,
                ip          VARCHAR(64)  NOT NULL DEFAULT '',
                ua          VARCHAR(255) NOT NULL DEFAULT '',
                created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_rl_user_date (user_email, created_at),
                INDEX idx_rl_path_date (path, created_at),
                INDEX idx_rl_date      (created_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        # Add country column if missing (Cloudflare CF-IPCountry, 2-letter
        # ISO code).  Older deployments created the table before this
        # column existed — keep adding it idempotently here so we don't
        # need a separate migration step.
        cur.execute("SHOW COLUMNS FROM request_log LIKE 'country'")
        if not cur.fetchone():
            cur.execute("ALTER TABLE request_log "
                        "ADD COLUMN country VARCHAR(8) NOT NULL DEFAULT ''")
        conn.commit(); cur.close(); conn.close()
    except Exception as e:
        print(f"[request_log] schema init failed: {e}")

# Initial table creation is deferred to after get_connection() is defined
# (further down in the file).  The schema-init call happens alongside
# _ensure_meeting_plan_table() down there.

# Paths we never log — static assets, favicons, and the health-check-ish
# polls that would otherwise dominate the table and crowd out the
# user-meaningful navigation we actually care about.
_LOG_SKIP_PREFIXES = ("/static/", "/favicon")
_LOG_SKIP_EXACT    = {"/api/ai_status", "/api/whoami"}

@app.before_request
def _request_log_start():
    request._rl_t0 = _time.monotonic()

@app.after_request
def _request_log_save(response):
    try:
        path = request.path or ""
        if request.method == "OPTIONS":
            return response
        if any(path.startswith(p) for p in _LOG_SKIP_PREFIXES):
            return response
        if path in _LOG_SKIP_EXACT:
            return response
        email = (_bde_from_request() or "").strip().lower()
        dur = int((_time.monotonic() - getattr(request, "_rl_t0", _time.monotonic())) * 1000)
        ip = request.headers.get("CF-Connecting-IP") or request.remote_addr or ""
        ua = (request.user_agent.string or "")[:255]
        # Cloudflare injects CF-IPCountry on every request (2-letter ISO);
        # behind Tailscale / local dev it's absent so we just store "".
        country = (request.headers.get("CF-IPCountry") or "")[:8]
        conn = get_connection(); cur = conn.cursor()
        cur.execute(
            "INSERT INTO request_log "
            "(user_email, path, method, status, duration_ms, ip, ua, country) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (email[:255], path[:255], request.method[:10],
             int(response.status_code or 0), dur, ip[:64], ua, country)
        )
        conn.commit(); cur.close(); conn.close()
    except Exception:
        # Logging failure must never break the response — swallow quietly.
        pass
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
        # stock?먮뒗 pattern???놁쓣 ???덉쑝??carrying_26濡쒕???pattern 媛?몄?????        joins.append("JOIN carrying_26 c ON c.m_code = s.material")
        joins.append("JOIN suv suv ON suv.Pattern = c.pattern")

    elif cat == "LOWPROFILE":
        joins.append("JOIN lowprofile lp ON CAST(TRIM(lp.Material) AS UNSIGNED) = s.material")

    elif cat in ("HK", "LF"):
        joins.append("JOIN carrying_26 c ON c.m_code = s.material")
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



def _hostname(url):
    from urllib.parse import urlparse
    try:
        return (urlparse(url or "").hostname or "").lower()
    except Exception:
        return ""

def _claim_portal_host():
    """Hostname of the customer-facing portal so the root route can
    decide whether to serve the Sales Dashboard or the claim form."""
    return _hostname(CLAIM_PORTAL_URL)

@app.route("/")
def index():
    portal_host = _claim_portal_host()
    dash_host   = _hostname(DASHBOARD_URL)
    # Only swap to the claim form when the portal subdomain is genuinely
    # distinct from the admin dashboard's hostname.  When both env vars
    # point at the same domain (e.g. customers reach the same
    # sales.hkaudashboard.com via a Cloudflare Access bypass on /claim/*),
    # the host check can't tell them apart and would otherwise shadow
    # the Sales Dashboard at /.
    if portal_host and portal_host != dash_host \
       and request.host.split(":")[0].lower() == portal_host:
        return send_from_directory("static", "claim.html")
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
        joins.append("JOIN carrying_26 c ON c.m_code = s.material")

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

        # category chip handling ??carrying_26 c is already joined above
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
                joins.append(f"JOIN carrying_26 c ON c.m_code = {tbl_alias}.material")
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
        # (those shipments are already counted as incoming, not open orders).
        # NOTE: this powers the "+ Factory Qty" column (Orders PO total)
        # and is intentionally NOT filtered to '42'-prefixed POs — that
        # filter only belongs on the Ready-to-Ship metric (confirm_qty),
        # applied in /api/orders below.
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
            stk = stock_by_state.get(st, 0)
            wtr = water_by_state.get(st, 0)
            fac = factory_by_state.get(st, 0)
            # Skip only when EVERY column would be zero — a state with
            # no sales for the current filter but plant inventory still
            # in transit (or sitting in stock) should stay visible so
            # the map dot matches a table row.
            if (q3 == 0 and q6 == 0 and q12 == 0
                and stk == 0 and wtr == 0 and fac == 0):
                continue
            base = round((q3 + q6 + q12) / 3)
            rows_out.append({
                "state":       st,
                "qty_3m":      q3,
                "qty_6m":      q6,
                "qty_12m":     q12,
                "base_sales":  base,
                "stock_qty":   round(stk),
                "water_qty":   round(wtr),
                "factory_qty": round(fac),
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
            joins.append("JOIN carrying_26 c ON c.m_code = o.material")

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
        # (applies to both po_qty and confirmed_qty — the row is already
        # received).
        wh.append(
            "o.po_no NOT IN (SELECT DISTINCT po_no FROM incoming"
            " WHERE po_no IS NOT NULL AND TRIM(po_no) <> '')"
        )
        # Ready-to-Ship (confirm metric) only: real factory RTS POs all
        # start with '42'.  Other PO prefixes on the confirm side are
        # samples / internal moves that inflate the number.  Orders PO
        # (po metric) does NOT apply this filter — it still shows every
        # PO regardless of prefix.
        if metric == "confirm":
            wh.append("o.po_no LIKE '42%'")

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
            joins.append("JOIN carrying_26 c ON c.m_code = o.material")

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


def get_top_sold_to_from_baseline(cur, f, top_limit, value):
    """Top N sold_to based on 2026 sales (sales_2526 filtered to year=2026).
    Dynamic per filter: when region/salesman/category/etc are set, ranking
    runs within that slice — so clicking NSW + Top 10 gives the NSW top 10,
    not the country-wide top 10."""
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

    # product_group / pattern / size all live in carrying_26
    if (f.get("product_group") != "ALL" or f.get("pattern") != "ALL" or
        f.get("material") != "ALL"):
        _ensure_carrying_join("sTop", joins)
    if f.get("product_group") != "ALL":
        wh.append("mat.product_group = %s"); params.append(f["product_group"])
    if f.get("pattern") != "ALL":
        wh.append("mat.pattern = %s"); params.append(f["pattern"])
    if f.get("material") != "ALL":
        wh.append("mat.size = %s"); params.append(f["material"])

    # Always restrict to 2026 — the ranking is "this year's top sold_tos
    # within the selected slice", not last year's.
    wh.append("sTop.year = 2026")
    where_sql = "WHERE " + " AND ".join(wh)

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

        # Product groups ??from material master (carrying_26)
        cur.execute("""
            SELECT DISTINCT TRIM(product_group) AS v
            FROM carrying_26
            WHERE product_group IS NOT NULL AND TRIM(product_group) <> ''
            ORDER BY TRIM(product_group)
        """)
        product_groups = [r["v"] for r in cur.fetchall() if r.get("v") is not None]

        # Patterns (filtered by product_group) ??from material master
        if pg and pg != "ALL":
            cur.execute("""
                SELECT DISTINCT TRIM(pattern) AS v
                FROM carrying_26
                WHERE product_group = %s
                  AND pattern IS NOT NULL AND TRIM(pattern) <> ''
                ORDER BY TRIM(pattern)
            """, (pg,))
        else:
            cur.execute("""
                SELECT DISTINCT TRIM(pattern) AS v
                FROM carrying_26
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
            FROM carrying_26
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
        # filter or group_by needs it.  Size is stored on carrying_26.size
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
        carrying_join_t = "LEFT JOIN carrying_26 mat ON mat.m_code = t.material"
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
        # carrying_26 needed for product_group/pattern (not stored in target_26 directly)
        carrying_join_mt = "LEFT JOIN carrying_26 mat ON mat.m_code = t.material"
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

    # product_group / pattern / size all live in carrying_26 (alias: mat)
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

    # Apply product_group / pattern / size filters via carrying_26 join.
    carrying_join_dt = "LEFT JOIN carrying_26 mat ON mat.m_code = t.material"
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

    # ---------- STEP 1: Top-N sold_to (dynamic per current filter slice) ----------
    top_pairs = None
    if top_limit in (10, 20, 30):
        sold_to_list = get_top_sold_to_from_baseline(cur, f, int(top_limit), metric)
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

    # Selected sub-promos from the new 443 / iON / TrueBlue buttons.
    # Promo filtering is only meaningful within the 2026 calendar year
    # (the promo_customer / promo_plan tables don't cover 2025 data),
    # so we silently ignore the param when year != 2026.
    promos = request.args.getlist("promo")
    if year != 2026:
        promos = []
    use_2026_union = (year == 2026)

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

    if promos:
        # Promo filter needs carrying_26 (mat) for line/product_group +
        # customer (cus) for the sold_to_group fallback in the EXISTS.
        _ensure_carrying_join("s", joins)
        _ensure_customer_join("s", joins)
        promo_wh, promo_p = _promo_filter_clauses(promos)
        wh.extend(promo_wh)
        params.extend(promo_p)

    # year condition — only when staying on sales_2526.  When we switch
    # to the 2026 union below, the table set itself bounds the year.
    if not use_2026_union:
        wh.append("s.year = %s")
        params.append(year)

    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        # Pick the FROM source: 2026 union for the current calendar
        # year (so the monthly chart goes through the current month),
        # sales_2526 for any other year.
        if use_2026_union:
            from_sql = "FROM " + _sales_2026_union(cur, alias="s")
        else:
            from_sql = "FROM sales_2526 s"

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
            {from_sql}
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

    # Apply product_group / pattern / size filters via carrying_26 join.
    carrying_join_mt = "LEFT JOIN carrying_26 mat ON mat.m_code = t.material"
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

    # carrying_26 join needed for group_by or filter on product_group/pattern
    carrying_join = "LEFT JOIN carrying_26 mat ON mat.m_code = t.material"
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
            FROM carrying_26
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
                FROM carrying_26
                WHERE product_group = %s
                  AND pattern IS NOT NULL AND TRIM(pattern) <> ''
                ORDER BY TRIM(pattern)
            """, (product_group,))
        else:
            cur.execute("""
                SELECT DISTINCT TRIM(pattern)
                FROM carrying_26
                WHERE pattern IS NOT NULL AND TRIM(pattern) <> ''
                ORDER BY TRIM(pattern)
            """)
        names = [r[0] for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify(names)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/api/carrying_matrix")
def carrying_matrix():
    """Distinct (product_group, pattern, size) tuples from carrying_26.
    Used by the claim form so three independent dropdowns can cascade
    in any direction client-side without a round-trip per change."""
    return _carrying_matrix_response()

@app.get("/api/claim/product_matrix")
def claim_product_matrix():
    """Public mirror of /api/carrying_matrix that lives under /api/claim/*
    so it falls inside the Cloudflare Access bypass for the customer
    claim form (which has no Cf-Access-Authenticated-User-Email header)."""
    return _carrying_matrix_response()

def _carrying_matrix_response():
    try:
        conn = get_connection(); cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT
                   TRIM(product_group) AS pg,
                   TRIM(pattern)       AS pat,
                   TRIM(size)          AS sz
            FROM carrying_26
            WHERE product_group IS NOT NULL AND TRIM(product_group) <> ''
              AND pattern       IS NOT NULL AND TRIM(pattern)       <> ''
              AND size          IS NOT NULL AND TRIM(size)          <> ''
            ORDER BY pg, pat, sz
        """)
        rows = [{"pg": r[0], "pattern": r[1], "size": r[2]} for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify(rows)
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
            FROM carrying_26
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
    """Return avg list_price and purchase_price from carrying_26 for current filter."""
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
            FROM carrying_26
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

            # ?? category filter (via carrying_26, same as sales tables) ??
            cat_joins_p, cat_where_p = category_filters_sales("p", f.get("category", "ALL"))
            joins_p += cat_joins_p
            wh_p    += cat_where_p

            # ?? product_group / pattern filter (via carrying_26) ??
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
        # support / Rebate follow-up / Stock / Other).
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

@app.get("/api/rebate_assignment_check")
def api_rebate_assignment_check():
    """Diagnostic for 'why isn't BDE X showing shop Y on the rebate page?'.

    Query params:
      bde   – salesman_name to look up (e.g. 'Borghese Alessio')
      sold  – optional, restrict to ship_tos under a sold_to_name
              containing this string (e.g. 'JAX' or 'JAXQUICKFIT')

    Returns:
      total_ship_tos              – ship_to count assigned to that BDE
      ship_to_breakdown_by_sold   – sold_to_name → count of ship_tos
      filtered_sold_ship_tos      – matching ship_tos for ?sold=…
      jax_assignment_summary      – every BDE that owns any JAX ship_to
                                    (case insensitive sold_to_name LIKE %JAX%)
    """
    bde   = (request.args.get("bde")  or "").strip()
    sold  = (request.args.get("sold") or "").strip()
    out   = {}
    try:
        conn = get_connection(); cur = conn.cursor(dictionary=True)

        # BDE-specific stats
        if bde:
            cur.execute(
                "SELECT COUNT(*) AS n FROM customer "
                "WHERE UPPER(TRIM(salesman_name)) = UPPER(TRIM(%s))",
                (bde,))
            out["total_ship_tos"] = (cur.fetchone() or {}).get("n", 0)

            cur.execute(
                "SELECT sold_to_name, COUNT(*) AS n FROM customer "
                "WHERE UPPER(TRIM(salesman_name)) = UPPER(TRIM(%s)) "
                "GROUP BY sold_to_name "
                "ORDER BY n DESC, sold_to_name "
                "LIMIT 80",
                (bde,))
            out["ship_to_breakdown_by_sold"] = cur.fetchall()

            if sold:
                like = f"%{sold}%"
                cur.execute(
                    "SELECT sold_to, sold_to_name, ship_to, ship_to_name, bde_state "
                    "FROM customer "
                    "WHERE UPPER(TRIM(salesman_name)) = UPPER(TRIM(%s)) "
                    "  AND UPPER(sold_to_name) LIKE UPPER(%s) "
                    "ORDER BY ship_to_name "
                    "LIMIT 200",
                    (bde, like))
                out["filtered_sold_ship_tos"] = cur.fetchall()

        # Cross-cutting summary: who owns JAX ship_tos right now?
        cur.execute(
            "SELECT TRIM(salesman_name) AS bde, bde_state, COUNT(*) AS n "
            "FROM customer "
            "WHERE UPPER(sold_to_name) LIKE '%JAX%' "
            "   OR UPPER(ship_to_name) LIKE 'JAX %' "
            "GROUP BY TRIM(salesman_name), bde_state "
            "ORDER BY n DESC")
        out["jax_assignment_summary"] = cur.fetchall()

        # All distinct salesman_name spellings — to catch case/format drift
        # (e.g. 'Borghese Alessio' vs 'BORGHESE Alessio' vs 'Alessio Borghese').
        cur.execute(
            "SELECT salesman_name, COUNT(*) AS n FROM customer "
            "GROUP BY salesman_name ORDER BY n DESC")
        out["all_salesman_names"] = cur.fetchall()

        cur.close(); conn.close()
        return jsonify(out)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


# ─── Rebate calc-basis helpers ───────────────────────────────────────────────
# The trailing token of a structure name designates how it is calculated:
#   _SR                       -> per SHIP_TO  (JAX/AJT, BJ/ABJ, TP/ATP, ACD, APP,
#                                              ATW, ABZ, Mobis, AVW …)
#   _HQ / _VR / _IT / _WTY    -> per SOLD_TO, on the full sold_to total
#   (no suffix)               -> per SOLD_TO  (base structure)
# This replaces the old hardcoded substring list {AJT,ABJ,ATP,APP,ACD}, which
# mis-classified HQ/VR variants (e.g. 731942's HK_ALL_AJT_Q_HQ) as per-ship_to
# and missed the SR variants of ATW/ABZ/Mobis/AVW.
_REBATE_SUFFIXES = ("SR", "HQ", "VR", "IT", "WTY")

def _rebate_suffix(struct):
    last = (struct or "").rsplit("_", 1)[-1].upper()
    return last if last in _REBATE_SUFFIXES else ""

def _rebate_is_ship_to(struct):
    return _rebate_suffix(struct) == "SR"

def _rebate_scope(struct):
    """Display/grouping scope: SR | HQ | VR | IT | WTY | BASE."""
    suf = _rebate_suffix(struct)
    return "SR" if suf == "SR" else (suf or "BASE")


# Sales order types (sales_thismonth.so_type) that count toward rebates.
# Only these are included in every rebate query / diagnostic below.
REBATE_SO_TYPES = ("ZWH1", "ZCR1", "ZDR1", "ZDF1", "ZRE1", "ZREN")
_REBATE_SO_TYPES_IN = "(" + ",".join("'%s'" % t for t in REBATE_SO_TYPES) + ")"

# ─── 2026 monthly sales tables — auto-detected at request time ─────────
# Promo features pull only from 2026 calendar-year sales (sales_2526
# carries no promo metadata) and the set of monthly tables grows by one
# every month — sales_2607, sales_2608, …  Discover them via
# INFORMATION_SCHEMA so a fresh month doesn't require a code change.
_SALES_2026_TABLES_CACHE = {"ts": 0, "names": []}

def _sales_2026_tables(cur):
    """Return the ordered list of sales tables that hold 2026 data:
    every sales_26?? plus sales_thismonth.  Cached for 60s so the schema
    introspection doesn't repeat per request."""
    import time as _t
    now = _t.monotonic()
    if (now - _SALES_2026_TABLES_CACHE["ts"]) < 60 and _SALES_2026_TABLES_CACHE["names"]:
        return list(_SALES_2026_TABLES_CACHE["names"])
    try:
        cur.execute("""
            SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = DATABASE()
              AND (TABLE_NAME REGEXP '^sales_26[0-9]{2}$'
                   OR TABLE_NAME = 'sales_thismonth')
            ORDER BY TABLE_NAME
        """)
        names = [r[0] if not isinstance(r, dict) else r["TABLE_NAME"]
                 for r in cur.fetchall()]
        # Sort by (year, month) so sales_thismonth lands at the end.
        def _key(n):
            if n == "sales_thismonth":
                return (9999, 99)
            try:
                yy = int(n[6:8]); mm = int(n[8:10])
                return (2000 + yy, mm)
            except Exception:
                return (9999, 99)
        names.sort(key=_key)
        _SALES_2026_TABLES_CACHE["ts"]    = now
        _SALES_2026_TABLES_CACHE["names"] = names
        return list(names)
    except Exception as e:
        print(f"[2026 sales] table discovery failed: {e}")
        # Fall back to the known set so the feature still works in an
        # environment that lacks INFORMATION_SCHEMA permissions.
        return ["sales_2601", "sales_2602", "sales_2603",
                "sales_2604", "sales_2605", "sales_thismonth"]

def _sales_2026_union(cur, alias="s", cols=None):
    """Build a `(SELECT … UNION ALL SELECT … …) AS <alias>` SQL fragment
    that exposes every 2026 monthly sales table as a single virtual
    source.  Use in place of `FROM sales_2526` for any 2026-only query.

    `SELECT *` would work if every monthly table had identical columns,
    but in practice sales_thismonth has been seen to differ from
    sales_26?? by a column or two — and any mismatch makes MySQL fail
    the entire UNION silently (or surface as 'unknown column'
    downstream).  So we ask each table for its columns via SHOW COLUMNS
    (more universally supported than INFORMATION_SCHEMA), compare in
    lowercase, and UNION the intersection.  If for some reason the
    intersection misses a column we know is required, fall back to a
    hand-curated list — that way the SQL we emit still has the
    minimum columns every analytics query depends on."""
    tables = _sales_2026_tables(cur)
    if not tables:
        return "(SELECT * FROM sales_thismonth WHERE 1=0) AS " + alias

    if cols is None:
        # Tracked separately so we can pick canonical column names from
        # the first table where each name first appears (preserving
        # original case in SQL).
        seen_in_all = None
        canonical   = {}     # lowercase name → original-case name
        for t in tables:
            cur.execute(f"SHOW COLUMNS FROM {t}")
            got_lc = set()
            for r in cur.fetchall():
                name = r[0] if not isinstance(r, dict) else r.get("Field") or r.get("field")
                if not name:
                    continue
                lc = name.lower()
                got_lc.add(lc)
                canonical.setdefault(lc, name)
            seen_in_all = got_lc if seen_in_all is None else (seen_in_all & got_lc)

        # Minimum set every sales-analytics query needs.  If
        # introspection somehow drops one of these (case mismatch,
        # connector returning empty rows for SHOW COLUMNS, etc.),
        # include it anyway so the outer query at least produces a
        # clear "unknown column" error pointing at the missing table.
        required = ["year", "month", "qty", "amt",
                    "sold_to", "ship_to", "brand", "material"]
        for lc in required:
            seen_in_all.add(lc) if seen_in_all is not None else None
            canonical.setdefault(lc, lc)
        if not seen_in_all:
            seen_in_all = set(required)

        cols = ", ".join(canonical[lc] for lc in sorted(seen_in_all))

    parts = [f"SELECT {cols} FROM {t}" for t in tables]
    return "(\n  " + "\n  UNION ALL ".join(parts) + f"\n) AS {alias}"

def _ensure_promo_customer_category():
    """Earlier iterations added a `category` column to promo_customer;
    the actual setup has a separate `promo_category` table with the
    mapping, so drop the now-unused column if it lingers from a prior
    deploy.  Safe to leave the column too — keeping the migration
    idempotent in both directions."""
    try:
        conn = get_connection(); cur = conn.cursor()
        cur.execute("SHOW TABLES LIKE 'promo_customer'")
        if not cur.fetchone():
            cur.close(); conn.close()
            return
        cur.execute("SHOW COLUMNS FROM promo_customer LIKE 'category'")
        if cur.fetchone():
            try:
                cur.execute("DROP INDEX idx_promo_customer_cat ON promo_customer")
            except Exception:
                pass
            try:
                cur.execute("ALTER TABLE promo_customer DROP COLUMN category")
            except Exception as e:
                # Non-fatal — column stays in place, queries below ignore it.
                print(f"[promo_customer] could not drop legacy category col: {e}")
        conn.commit(); cur.close(); conn.close()
    except Exception as e:
        print(f"[promo_customer] category cleanup skipped: {e}")

_ensure_promo_customer_category()

def _promo_category_of(promo_name):
    """Auto-derive the top-level button name from a sub-promo string by
    splitting on the first underscore.  No mapping table required.
        '443'          → '443'
        '443_30%'      → '443'
        'iON_70%'      → 'iON'
        'TrueBlue_12%' → 'TrueBlue'
    A bare promo with no underscore acts as both category and its sole
    sub-button (e.g. '443' renders as a category with a single sub '443')."""
    s = (promo_name or "").strip()
    if not s:
        return ""
    return s.split("_", 1)[0]

def _promo_filter_clauses(promos, sales_alias="s",
                          carrying_alias="mat", customer_alias="cus"):
    """Build the WHERE clauses + params needed to constrain a sales query
    to rows that qualify for any of the selected sub-promos.

    Caller responsibility:
      • Ensure the `mat` (carrying_26) join is present — promos require
        product_group + line columns.
      • Ensure the `cus` (customer) join is present — sold_to_group is
        used for customer_group fallback when promo_customer.sold_to is
        empty.
      • Sales table aliased as `s` (or pass sales_alias).

    Returns: (wh_list, params_list).  Empty when promos is empty.
    """
    if not promos:
        return [], []
    placeholders = ",".join(["%s"] * len(promos))
    s = sales_alias; c = carrying_alias; cu = customer_alias
    wh = [
        # PCLT is always the umbrella for promos.
        f"{c}.line = 'PCLT'",
        # Sale qualifies for at least one selected sub-promo.
        f"""EXISTS (
            SELECT 1
            FROM promo_customer pc
            LEFT JOIN promo_plan pp ON pp.promo = pc.promo
            WHERE pc.promo IN ({placeholders})
              AND {s}.qty     >= pc.min_qty
              AND {s}.dc_rate  = pc.dc_rate
              AND {s}.brand    = pc.brand
              AND (
                   (pc.sold_to <> '' AND pc.sold_to = {s}.sold_to)
                OR (pc.sold_to = '' AND pc.customer_group = {cu}.sold_to_group)
              )
              AND (
                pp.promo IS NULL
                OR (
                  {c}.product_group = pp.product_group
                  AND ({s}.year * 100 + {s}.month) BETWEEN
                       (YEAR(pp.start_date) * 100 + MONTH(pp.start_date)) AND
                       (YEAR(pp.end_date)   * 100 + MONTH(pp.end_date))
                  AND (pp.material = '' OR pp.material = {s}.material)
                )
              )
        )"""
    ]
    return wh, list(promos)

@app.get("/api/promo/buttons")
def api_promo_buttons():
    """Return the top-level promo categories + the sub-promos under each,
    derived live from DISTINCT promo_customer.promo.  No mapping table
    needed: category = part before the first underscore.

    Shape:
      { "categories": [
          { "name": "443",      "subs": ["443","443_30%"] },
          { "name": "iON",      "subs": ["iON_70%"] },
          { "name": "TrueBlue", "subs": ["TrueBlue_12%","TrueBlue_18%"] },
        ] }
    """
    try:
        conn = get_connection(); cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT DISTINCT promo
            FROM promo_customer
            WHERE promo IS NOT NULL AND TRIM(promo) <> ''
            ORDER BY promo
        """)
        rows = cur.fetchall()
        cur.close(); conn.close()
        out = {}
        for r in rows:
            pr  = r["promo"]
            cat = _promo_category_of(pr)
            out.setdefault(cat, [])
            if pr not in out[cat]:
                out[cat].append(pr)
        # Stable ordering — numeric categories first ('443'), then
        # alphabetic.  Keeps the button row predictable across deploys.
        cats = sorted(out.keys(),
                      key=lambda k: (0 if k and k[:1].isdigit() else 1, k))
        return jsonify({
            "categories": [{"name": k, "subs": out[k]} for k in cats]
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# Sales source table per ?month=... button on the rebate page.  A whitelist —
# the resolved value is the only thing interpolated into the FROM clause, so an
# unknown month can never inject a table name.
REBATE_SALES_TABLES = {
    "thismonth": "sales_thismonth",
    "2601": "sales_2601",
    "2602": "sales_2602",
    "2603": "sales_2603",
    "2604": "sales_2604",
    "2605": "sales_2605",
}

def _rebate_sales_table(month_arg):
    return REBATE_SALES_TABLES.get((month_arg or "thismonth").strip().lower(),
                                   "sales_thismonth")


_REBATE_REGION_MAP = {"SA": "VIC", "TAS": "VIC", "NT": "VIC", "ACT": "NSW"}

def _rebate_region(state):
    """Region used on the rebate view.  The 4 region cards are NSW/QLD/VIC/WA;
    the smaller states/territories roll into one of them (SA/TAS/NT -> VIC,
    ACT -> NSW) so their sales/rebate don't fall outside every card."""
    s = (state or "").strip()
    return _REBATE_REGION_MAP.get(s.upper(), s)

def _or_default(val, dflt):
    """val unless it's blank or the '-' placeholder, in which case dflt.  Used
    so ship_tos missing from the customer master (no region/BDE) fall back to
    the sold_to's region/BDE instead of becoming an orphan '-' that drops out
    of the 4 region cards."""
    v = (val or "").strip()
    return v if v and v != "-" else dflt


@app.get("/api/rebate_structure_check")
def api_rebate_structure_check():
    """Diagnostic: what rebate structure is a sold_to mapped to, and how
    does the rebate calc bucket its ship_tos by BDE?  Hit like:
      /api/rebate_structure_check?sold_to=731942
    Returns:
      structures           – every (brand, structure_name) row from
                              rebate_customer_map for this sold_to
      structure_path       – 'PER_SHIP_TO' (AJT/ABJ/ATP/APP/ACD) or
                              'BDE_GROUPED' (everything else)
      bde_breakdown        – per-BDE counts:
                              owned_in_customer  – customer.salesman_name
                                                    rows for this sold_to
                              with_sales_thismonth – of those, how many
                                                    have a sales_thismonth
                                                    row this period
    """
    sold = (request.args.get("sold_to") or "").strip()
    if not sold:
        return jsonify({"error": "sold_to is required"}), 400
    sales_tbl = _rebate_sales_table(request.args.get("month"))
    try:
        conn = get_connection(); cur = conn.cursor(dictionary=True)

        cur.execute(
            "SELECT brand, structure_name FROM rebate_customer_map "
            "WHERE sold_to = %s", (sold,))
        structures = cur.fetchall()

        # Per-ship_to if ANY mapped structure carries the _SR suffix.
        path = "BDE_GROUPED"
        for s in structures:
            if _rebate_is_ship_to(s.get("structure_name") or ""):
                path = "PER_SHIP_TO"; break
        # Annotate each structure with its own calc basis (mixed SR + HQ/VR
        # accounts like 731942 carry both).
        for s in structures:
            s["calc_basis"] = ("SHIP_TO" if _rebate_is_ship_to(s.get("structure_name") or "")
                               else "SOLD_TO")
            s["scope"] = _rebate_scope(s.get("structure_name") or "")

        cur.execute(
            "SELECT TRIM(c.salesman_name) AS bde, "
            "       COUNT(DISTINCT c.ship_to) AS owned_in_customer, "
            "       COUNT(DISTINCT CASE WHEN EXISTS("
            "           SELECT 1 FROM " + sales_tbl + " s "
            "           WHERE s.ship_to = c.ship_to "
            "             AND s.brand IN ('HK','LF') "
            "             AND s.so_type IN " + _REBATE_SO_TYPES_IN + " "
            "       ) THEN c.ship_to END) AS with_sales_thismonth "
            "FROM customer c "
            "WHERE c.sold_to = %s "
            "GROUP BY TRIM(c.salesman_name) "
            "ORDER BY owned_in_customer DESC",
            (sold,))
        breakdown = cur.fetchall()

        # Cross-check from the sales-side: which ship_tos in
        # sales_thismonth carry this sold_to, and who do they map back to
        # via customer.salesman_name (this is exactly what the rebate
        # BDE-grouping loop sees).
        cur.execute(
            "SELECT TRIM(c.salesman_name) AS bde, "
            "       COUNT(DISTINCT s.ship_to) AS sales_ship_tos "
            "FROM " + sales_tbl + " s "
            "LEFT JOIN customer c ON c.ship_to = s.ship_to "
            "WHERE s.sold_to = %s "
            "  AND s.brand IN ('HK','LF') "
            "  AND s.so_type IN " + _REBATE_SO_TYPES_IN + " "
            "GROUP BY TRIM(c.salesman_name) "
            "ORDER BY sales_ship_tos DESC",
            (sold,))
        sales_side = cur.fetchall()

        cur.close(); conn.close()
        return jsonify({
            "sold_to":           sold,
            "structures":        structures,
            "structure_path":    path,
            "bde_breakdown":     breakdown,
            "sales_side_bde":    sales_side,
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

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
    sales_tbl     = _rebate_sales_table(request.args.get("month"))  # source table per month button
    export_fmt    = (request.args.get("export") or "").strip().lower()  # 'xlsx' -> download
    # BDE filter — matches the role-scope lock used by graph/map views.
    # Comparison is case-insensitive on salesman_name so name casing in
    # the customer master doesn't make a BDE invisible to themselves.
    bde_filter    = (request.args.get("bde") or "").strip()
    line_filter   = (request.args.get("line") or "ALL").upper()        # PCLT | TBR | ALL
    sold_to_filter = (request.args.get("sold_to") or "").strip()       # exact name or code
    ship_to_filter = (request.args.get("ship_to") or "").strip()
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
              AND (%s = 'ALL' OR m.line = %s)
              AND (%s = 'ALL' OR c.sold_to_group = %s)
            GROUP BY m.sold_to, m.brand, m.structure_name
        """, (brand_filter, brand_filter, line_filter, line_filter, stg_filter, stg_filter))
        customers = cur.fetchall()
        if not customers:
            return jsonify([])

        # Ship_to -> Group map (for _SR group summing).  The map lists
        # individual ship_tos with a manual Group number for SR structures.
        # ship_tos sharing a Group are summed and the tier is applied to the
        # group total; every member then displays that same group figure
        # (even when members sit under different BDEs).
        ship_grp = {}   # (sold_to, structure_name, ship_to) -> group label
        cur.execute("""
            SELECT sold_to, structure_name, ship_to, grp
            FROM rebate_customer_map
            WHERE grp IS NOT NULL AND grp <> '' AND ship_to IS NOT NULL AND ship_to <> ''
        """)
        for r in cur.fetchall():
            ship_grp[(str(r["sold_to"]), r["structure_name"], str(r["ship_to"]))] = str(r["grp"])

        # Sold_to-level grouping: rows with grp filled and ship_to EMPTY bundle
        # multiple sold_tos together for tier qualification on non-_SR
        # structures (e.g. HK_ALL_DIA_A_VR with 3 sold_tos sharing group "1").
        # tier basis = sum of all group members; each sold_to/store still earns
        # on its own amount x the resulting rate, so totals are sum-correct.
        sold_to_grp  = {}   # (structure_name, sold_to) -> grp label
        grp_to_solds = {}   # (structure_name, grp)     -> set of sold_tos
        cur.execute("""
            SELECT sold_to, structure_name, grp
            FROM rebate_customer_map
            WHERE grp IS NOT NULL AND grp <> ''
              AND (ship_to IS NULL OR ship_to = '')
        """)
        for r in cur.fetchall():
            k = (r["structure_name"], str(r["sold_to"]))
            sold_to_grp[k] = str(r["grp"])
            grp_to_solds.setdefault((r["structure_name"], str(r["grp"])), set()).add(str(r["sold_to"]))

        # sold_tos that carry a Store rebate (_SR) structure.  For these, the
        # secondary HQ/VR/IT/WTY rebates are pulled out of the per-BDE/State
        # table and combined into the top summary box instead.  sold_tos with
        # NO _SR structure keep their HQ/VR in the table (BDE/State as before).
        sr_sold_tos = {str(c["sold_to"]) for c in customers
                       if _rebate_is_ship_to(c["structure_name"])}
        hq_box = {}   # sold_to -> {sold_to, sold_to_name, sold_to_group, rebate, parts:[...]}
        hq_by_region = {}   # region -> HQ/VR rebate (each store's amount x rate, by its region)

        # Pre-load the set of "Promo" ship_tos so the rebate calc can
        # skip them in every aggregation downstream — the customer
        # master flags promotional stores by including "Promo" in the
        # ship_to_name (case-insensitive substring), and those don't
        # count toward rebate qualification or earning.
        cur.execute(
            "SELECT ship_to FROM customer "
            "WHERE UPPER(ship_to_name) LIKE '%PROMO%'"
        )
        promo_ship_tos = {str(r["ship_to"]) for r in cur.fetchall()}

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
            FROM """ + sales_tbl + """ s
            WHERE s.brand IN ('HK','LF')
              AND s.so_type IN """ + _REBATE_SO_TYPES_IN + """
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
        sold_brand_tot      = {}   # (sold_to, brand) -> {qty, amt}  per sold_to
        sold_brand_line_tot = {}   # (sold_to, brand, line) -> {qty, amt}
        for r in cur.fetchall():
            st, sh, br, ln = str(r["sold_to"]), str(r["ship_to"]), r["brand"], r["line"]
            if sh in promo_ship_tos:
                continue   # Promo ship_tos are excluded from rebate calc.
            qty, amt = float(r["qty"] or 0), float(r["amt"] or 0)
            # aggregate all lines ??brand-level totals
            agg = ship_sales.setdefault((st, sh, br), {"qty": 0.0, "amt": 0.0})
            agg["qty"] += qty; agg["amt"] += amt
            # store by line
            ship_sales_line[(st, sh, br, ln)] = {"qty": qty, "amt": amt}
            ship_idx.setdefault((st, br), set()).add(sh)
            ship_idx_line.setdefault((st, br, ln), set()).add(sh)
            # per-sold_to roll-ups (for sold_to-group tier basis)
            agg2 = sold_brand_tot.setdefault((st, br), {"qty": 0.0, "amt": 0.0})
            agg2["qty"] += qty; agg2["amt"] += amt
            agg3 = sold_brand_line_tot.setdefault((st, br, ln), {"qty": 0.0, "amt": 0.0})
            agg3["qty"] += qty; agg3["amt"] += amt

        # ?? 3. Customer lookup (ship_to ??name, bde_state, salesman) ??????????
        cur.execute("SELECT ship_to, ship_to_name, bde_state, salesman_name, sold_to FROM customer")
        ship_cust_map  = {}   # ship_to str -> {name, state, bde}
        sold_to_ships  = {}   # sold_to str -> set(ship_to) from customer master.
                              # Lets the rebate calc surface a BDE row even when
                              # all of that BDE's ship_tos for a sold_to have
                              # zero sales this month (otherwise they're hidden
                              # because ship_idx is sales-based).
        for r in cur.fetchall():
            sh = str(r["ship_to"])
            ship_cust_map[sh] = {
                "name":  r["ship_to_name"] or sh,
                "state": _rebate_region(r["bde_state"]) or "-",
                "bde":   (r["salesman_name"] or "").strip() or "-",
            }
            if sh in promo_ship_tos:
                continue   # Don't surface Promo ships as zero-sales BDE rows.
            stk = str(r["sold_to"] or "")
            if stk:
                sold_to_ships.setdefault(stk, set()).add(sh)
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

        # Calc basis is driven by the structure-name suffix (see _rebate_*
        # helpers above): _SR -> per ship_to; _HQ/_VR/_IT/_WTY -> per sold_to
        # on the full sold_to total; no suffix -> per sold_to (BDE-grouped).

        def _rebate_brand_line(struct_name):
            """From the structure name tokens decide which sales to count:
              token0 brand : HK | LF | ALL(=HK+LF)
              token1 line  : PCLT | TBR | ALL(=all lines)
            A 'NOLFT' token anywhere in the rest of the name flags an
            exclusion ('all selected sales MINUS LF/TBR') for the rare
            case where the basis should ignore that one (brand, line)
            cell.  Other tokens stay free-form (used by scope suffixes).
            Returns (brands_list, line_filter_or_None, brand_key_for_display, excludes).
            """
            tk = struct_name.split("_")
            bt = (tk[0] if len(tk) > 0 else "ALL").upper()
            lt = (tk[1] if len(tk) > 1 else "ALL").upper()
            brands    = ["HK", "LF"] if bt == "ALL" else [bt]
            line_filt = lt if lt in ("PCLT", "TBR") else None
            brand_key = "TTL" if bt == "ALL" else bt
            excludes  = set()
            if "NOLFT" in [t.upper() for t in tk[2:]]:
                excludes.add(("LF", "TBR"))
            return brands, line_filt, brand_key, excludes

        def _sold_to_qa(st_id, brand_list, line, excludes=()):
            """(qty, amt) for one sold_to summed over the given brands/line,
            subtracting any (brand, line) cells flagged as excluded."""
            q = a = 0.0
            for br in brand_list:
                if line:
                    d = sold_brand_line_tot.get((st_id, br, line), {"qty": 0.0, "amt": 0.0})
                else:
                    d = sold_brand_tot.get((st_id, br), {"qty": 0.0, "amt": 0.0})
                q += d["qty"]; a += d["amt"]
                for (eb, el) in excludes:
                    if eb != br: continue
                    if line == el:
                        # Whole bucket already matches the excluded cell.
                        q -= d["qty"]; a -= d["amt"]
                    elif line is None:
                        de = sold_brand_line_tot.get((st_id, eb, el), {"qty": 0.0, "amt": 0.0})
                        q -= de["qty"]; a -= de["amt"]
            return q, a


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
            scope     = _rebate_scope(struct)            # SR | HQ | VR | IT | WTY | BASE
            is_ship_to_struct = (scope == "SR")
            is_secondary      = scope in ("HQ", "VR", "IT", "WTY")

            for brands, line_filt, brand_key, excludes in [_rebate_brand_line(struct)]:

                # ship_tos with sales for this structure's brand(s) and line.
                # line_filt PCLT/TBR -> only that line (material prefix 1,2=PCLT
                # 3=TBR); None -> all lines (brand-level totals).
                ship_set = set()
                for _br in brands:
                    if line_filt:
                        ship_set |= ship_idx_line.get((sold_to, _br, line_filt), set())
                    else:
                        ship_set |= ship_idx.get((sold_to, _br), set())

                # Keep every ship_to that has sales for this sold_to, even if it
                # isn't in the customer master (e.g. A018377 — qty 0 but an
                # amount, or credits/returns) — those still count toward the
                # rebate.  Region/BDE fall back to the sold_to's when unknown.

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

                # Badge labels for UI: brand, then line (PCLT/TBR) if filtered,
                # then unit; secondary scopes (HQ/VR/IT/WTY) get a scope tag.
                badges = [brand_key] + ([line_filt] if line_filt else []) + [unit]
                if is_secondary:
                    badges = badges + [scope]

                def _get_sales(sh, _brands=brands, _line=line_filt, _excl=excludes):
                    """(qty, amt) summed over this structure's brand(s)/line,
                    minus any (brand, line) cells flagged as excluded."""
                    q = a = 0.0
                    for _br in _brands:
                        if _line:
                            d = ship_sales_line.get((sold_to, sh, _br, _line), {"qty": 0.0, "amt": 0.0})
                        else:
                            d = ship_sales.get((sold_to, sh, _br), {"qty": 0.0, "amt": 0.0})
                        q += d["qty"]; a += d["amt"]
                        for (eb, el) in _excl:
                            if eb != _br: continue
                            if _line == el:
                                q -= d["qty"]; a -= d["amt"]
                            elif _line is None:
                                de = ship_sales_line.get((sold_to, sh, eb, el), {"qty": 0.0, "amt": 0.0})
                                q -= de["qty"]; a -= de["amt"]
                    return q, a

                if is_ship_to_struct:
                    # _SR → per ship_to, but ship_tos sharing a map Group are
                    # summed and the tier applied to the group total.  Every
                    # member then displays that same group figure (qty/amt/
                    # rate/rebate) while staying under its own BDE.  A ship_to
                    # with no Group is its own singleton.
                    grp_totals = {}   # group_key -> {qty, amt, members:[...]}
                    for sh in sorted(ship_set):
                        q, a = _get_sales(sh)
                        sh_info_local = ship_cust_map.get(sh, {})
                        gk = ship_grp.get((sold_to, struct, sh))
                        group_key = ("G", gk) if gk else ("S", sh)
                        gt = grp_totals.setdefault(group_key, {"qty": 0.0, "amt": 0.0, "members": []})
                        gt["qty"] += q
                        gt["amt"] += a
                        gt["members"].append({
                            "sh":     sh,
                            "bde":    _or_default(sh_info_local.get("bde"),   sold_to_bde),
                            "region": _or_default(sh_info_local.get("state"), sold_to_region),
                        })
                    calc_items = []
                    for group_key, gt in grp_totals.items():
                        members = gt["members"]
                        # Count the group's rebate ONCE toward region/BDE totals,
                        # attributed to the BDE that owns the most members; the
                        # other members show the same figure but are display-only
                        # so a cross-BDE group isn't summed twice.
                        dom_bde = Counter(m["bde"] for m in members).most_common(1)[0][0]
                        primary_taken = False
                        for m in members:
                            is_primary = (not primary_taken) and (m["bde"] == dom_bde)
                            if is_primary:
                                primary_taken = True
                            calc_items.append({
                                "sh":            m["sh"],
                                "qty":           gt["qty"],   # group total → same number for all members
                                "amt":           gt["amt"],
                                "bde":           m["bde"],
                                "region":        m["region"],
                                "sold_to_basis": False,
                                "ship_details":  [],
                                "rollup":        is_primary,
                            })
                elif is_secondary and sold_to in sr_sold_tos:
                    # _HQ/_VR/_IT/_WTY for an account that ALSO has a Store
                    # rebate (_SR): compute one rebate on the FULL sold_to total
                    # and fold it into the top summary box (combined across HQ/
                    # VR/etc).  No per-BDE/State table rows for these.
                    full_ship_set = ship_set | sold_to_ships.get(sold_to, set())
                    tot_q = tot_a = 0.0
                    store_amts = []   # (region, amt) per store, to split the HQ/VR by region
                    for sh in full_ship_set:
                        q, a = _get_sales(sh)
                        tot_q += q; tot_a += a
                        reg = _or_default(ship_cust_map.get(sh, {}).get("state"), sold_to_region)
                        store_amts.append((reg, a))
                    # Tier qualifies on the sold_to GROUP total when this
                    # sold_to is bundled with others via the map's Group column;
                    # otherwise on this sold_to's own total.  Each sold_to still
                    # earns on its own amount x the resulting rate.
                    grp_label = sold_to_grp.get((struct, sold_to))
                    if grp_label:
                        bq = ba = 0.0
                        for _gs in grp_to_solds.get((struct, grp_label), {sold_to}):
                            _q, _a = _sold_to_qa(_gs, brands, line_filt, excludes)
                            bq += _q; ba += _a
                    else:
                        bq, ba = tot_q, tot_a
                    hq_actual = bq if unit == "Q" else ba
                    hq_curr, _ = _calc_tier(hq_actual, tiers, top_order)
                    hq_rebate = round(tot_a * hq_curr["rate"] / 100, 2)
                    for reg, a in store_amts:   # split this HQ/VR across regions by store sales
                        hq_by_region[reg] = hq_by_region.get(reg, 0.0) + a * hq_curr["rate"] / 100
                    box = hq_box.setdefault(sold_to, {
                        "sold_to":       sold_to,
                        "sold_to_name":  c["sold_to_name"] or st_info.get("name") or sold_to,
                        "sold_to_group": sold_to_group,
                        "rebate":        0.0,
                        "parts":         [],
                    })
                    box["rebate"] += hq_rebate
                    box["parts"].append({
                        "scope":          scope,
                        "brand":          brand_key,
                        "structure_name": struct,
                        "rate":           hq_curr["rate"],
                        "actual_qty":     round(tot_q, 2),
                        "actual_amt":     round(tot_a, 2),
                        "rebate":         hq_rebate,
                    })
                    calc_items = []   # nothing in the per-BDE/State table
                else:
                    # sold_to-level rebate (base, or HQ/VR of a non-Store-rebate
                    # account).  The tier QUALIFIES on the full sold_to total, but
                    # each Store earns the rebate on its OWN sales at that rate —
                    # so every store shows its own qty/amt/rebate (not one shared
                    # group number).  No rollup gating needed: each store is
                    # counted once with its own amount, summing to the correct
                    # sold_to total.
                    full_ship_set = ship_set | sold_to_ships.get(sold_to, set())
                    full_q = full_a = 0.0
                    per_store = []
                    for sh in sorted(full_ship_set):
                        q, a = _get_sales(sh)
                        full_q += q; full_a += a
                        info = ship_cust_map.get(sh, {})
                        per_store.append({
                            "sh":     sh,
                            "qty":    q,
                            "amt":    a,
                            "bde":    _or_default(info.get("bde"),   sold_to_bde),
                            "region": _or_default(info.get("state"), sold_to_region),
                        })
                    # tier basis = sold_to GROUP total if grouped, else own total
                    grp_label = sold_to_grp.get((struct, sold_to))
                    if grp_label:
                        bq = ba = 0.0
                        for _gs in grp_to_solds.get((struct, grp_label), {sold_to}):
                            _q, _a = _sold_to_qa(_gs, brands, line_filt, excludes)
                            bq += _q; ba += _a
                    else:
                        bq, ba = full_q, full_a
                    calc_items = [{
                        "sh":            st["sh"],
                        "qty":           st["qty"],
                        "amt":           st["amt"],
                        "bde":           st["bde"],
                        "region":        st["region"],
                        "sold_to_basis": False,
                        "ship_details":  [],
                        "tier_basis_q":  bq,   # tier qualifies on the (group) total
                        "tier_basis_a":  ba,
                        "rollup":        True,
                    } for st in per_store]
                    if not calc_items:
                        calc_items = [{
                            "sh":           sold_to,
                            "qty":          0.0,
                            "amt":          0.0,
                            "bde":          sold_to_bde,
                            "region":       sold_to_region,
                            "sold_to_basis": False,
                            "ship_details": [],
                            "tier_basis_q": 0.0,
                            "tier_basis_a": 0.0,
                            "rollup":       True,
                        }]

                for item in calc_items:
                    sh          = item["sh"]
                    actual_qty  = item["qty"]
                    actual_amt  = item["amt"]
                    row_bde     = item["bde"]
                    row_region  = item["region"]
                    row_stbasis = item["sold_to_basis"]
                    row_details = item["ship_details"]
                    row_rollup  = item.get("rollup", True)

                    actual = actual_qty if unit == "Q" else actual_amt

                    # The tier (rate / next / needed) qualifies on the basis —
                    # the group/sold_to total for sold_to-level rebates — while
                    # the rebate is earned on THIS row's own amount.  Rows that
                    # don't set a basis (SR group members, box) use their own
                    # value, so the rate matches what they display.
                    basis_qty = item.get("tier_basis_q", actual_qty)
                    basis_amt = item.get("tier_basis_a", actual_amt)
                    basis = basis_qty if unit == "Q" else basis_amt

                    curr_tier, next_tier = _calc_tier(basis, tiers, top_order)
                    curr_rebate = round(actual_amt * curr_tier["rate"] / 100, 2)
                    est_rebate  = round(next_tier["threshold"] * next_tier["rate"] / 100, 2) if next_tier else None
                    needed_qty = round(next_tier["threshold"] - basis_qty, 2) if next_tier and unit == "Q" else None
                    needed_amt = round(next_tier["threshold"] - basis_amt, 2) if next_tier and unit == "A" else None

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
                        "scope":          scope,
                        "rollup":         row_rollup,
                        "sold_to_basis":  row_stbasis,
                        "ship_details":   row_details,
                        "unit":           unit,
                        "actual_qty":     round(actual_qty, 2),
                        "actual_amt":     round(actual_amt, 2),
                        "actual":         round(actual, 2),
                        "tier_actual":    round(basis, 2),
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
        # Role-scope BDE filter applied first so the summary cards (region
        # totals etc.) only reflect what this BDE actually owns.  Region
        # buttons still let them switch between regions, but they only
        # ever see their own slice.
        if bde_filter:
            bf = bde_filter.strip().upper()
            rows = [r for r in rows if (r["bde"] or "").strip().upper() == bf]
        if sold_to_filter and sold_to_filter.upper() != "ALL":
            sf = sold_to_filter.upper()
            rows = [r for r in rows if (r["sold_to_name"] or "").strip().upper() == sf
                    or str(r["sold_to"]).upper() == sf]
        if ship_to_filter and ship_to_filter.upper() != "ALL":
            shf = ship_to_filter.upper()
            rows = [r for r in rows if (r["ship_to_name"] or "").strip().upper() == shf
                    or str(r["ship_to"]).upper() == shf]
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

        # Top-box HQ/VR rebate (Store-rebate accounts only).  Respects the
        # search filter (sold_to name/code) but NOT region/BDE — HQ ignores
        # State/BDE by design.
        hq_items = sorted(hq_box.values(), key=lambda x: x["rebate"], reverse=True)
        if search:
            hq_items = [h for h in hq_items
                        if search in (h["sold_to_name"] or "").lower()
                        or search in str(h["sold_to"])]
        hq_box_out = {
            "total": round(sum(h["rebate"] for h in hq_items), 2),
            "items": [{**h, "rebate": round(h["rebate"], 2)} for h in hq_items],
        }

        # ?? 7. Summary stats (over all filtered rows) ?????????????????????????
        REGION_KEYS = ["NSW", "QLD", "VIC", "WA"]
        region_totals = {rk: {"rebate": 0.0, "qty": 0.0, "amt": 0.0} for rk in REGION_KEYS}
        for r in rows:
            if not r.get("rollup", True):
                continue   # display-only _SR group duplicate
            rk = (r["region"] or "").strip().upper()
            if rk in region_totals:
                region_totals[rk]["rebate"] += r["curr_rebate"]
                region_totals[rk]["qty"]    += r["actual_qty"]
                region_totals[rk]["amt"]    += r["actual_amt"]
        for rk in region_totals:
            region_totals[rk] = {k: round(v, 2) for k, v in region_totals[rk].items()}

        # Store-rebate (table) total, HQ/VR total, and the grand total split by
        # region: region card = Store/base rebate in that region + the HQ/VR
        # allocated to it (each store's amount x rate).  The 4 region cards sum
        # to the grand total.
        store_total = round(sum(rt["rebate"] for rt in region_totals.values()), 2)
        hq_total    = hq_box_out["total"]
        grand_total = round(store_total + hq_total, 2)
        region_grand = {rk: round(region_totals[rk]["rebate"] + hq_by_region.get(rk, 0.0), 2)
                        for rk in REGION_KEYS}

        summary = {
            "total_ship_to": len(rows),
            "has_next":  sum(1 for r in rows if r["next_rate"] is not None and r["actual"] > 0),
            "max_tier":  sum(1 for r in rows if r["next_rate"] is None and r["curr_rate"] > 0),
            "zero_sales": sum(1 for r in rows if r["actual"] == 0),
            "est_total":  round(sum(r["curr_rebate"] for r in rows if r.get("rollup", True)), 2),
            "region_totals": region_totals,
            "region_grand":  region_grand,
            "hq_by_region":  {rk: round(hq_by_region.get(rk, 0.0), 2) for rk in REGION_KEYS},
            "store_total":   store_total,
            "hq_total":      hq_total,
            "grand_total":   grand_total,
            "hq_box": hq_box_out,
        }

        # Apply region filter to rows (after computing region_totals)
        if region_filter != "ALL":
            rows = [r for r in rows if (r["region"] or "").strip().upper() == region_filter]

        # Excel download of exactly what the current filters produced (reuses
        # the same computed rows + HQ/VR box, so it matches the screen).
        if export_fmt == "xlsx":
            from io import BytesIO
            from datetime import date
            from flask import Response
            wb = Workbook()
            ws = wb.active; ws.title = "Rebate"
            hdr = ["Region", "BDE", "Sold-To", "Sold-To Name", "Ship-To",
                   "Ship-To Name", "Brand", "Type", "Structure", "Qty",
                   "Amount", "Rate %", "Next %", "Need Qty", "Need Amt",
                   "Rebate"]
            ws.append(hdr)
            for cell in ws[1]:
                cell.font = Font(bold=True, color="FFFFFF")
                cell.fill = PatternFill("solid", fgColor="1E3A5F")
            for r in sorted(rows, key=lambda x: (x["region"] or "", x["bde"] or "",
                                                 x["sold_to_name"] or "", -x["actual"])):
                ws.append([
                    r["region"], r["bde"], r["sold_to"], r["sold_to_name"],
                    r["ship_to"], r["ship_to_name"], r["brand"],
                    "Store" if r["scope"] == "SR" else r["scope"],
                    r["structure_name"], r["actual_qty"], r["actual_amt"],
                    r["curr_rate"],
                    r["next_rate"]   if r["next_rate"]   is not None else "",
                    r["needed_qty"]  if r["needed_qty"]  is not None else "",
                    r["needed_amt"]  if r["needed_amt"]  is not None else "",
                    r["curr_rebate"],
                ])
            # Sum-safe TOTAL: matches the screen's Grand Total exactly by
            # applying the same two rules used there:
            #   1. rollup=False rows are display-only duplicates of an _SR
            #      group, so they're excluded entirely.
            #   2. Secondary HQ/VR/IT/WTY rows cover the same underlying
            #      sales as the primary (BASE/SR) rows on the same group,
            #      so their qty/amt is excluded when a primary row exists.
            # Rebate stays a straight sum — each rebate is earned independently.
            SECONDARY_SCOPES = {"HQ", "VR", "IT", "WTY"}
            scopes_by_group = {}
            for r in rows:
                if not r.get("rollup", True): continue
                k = (r.get("region") or "", r.get("bde") or "", r.get("sold_to") or "")
                scopes_by_group.setdefault(k, set()).add(r.get("scope") or "BASE")
            tot_qty = tot_amt = tot_reb = 0.0
            for r in rows:
                if not r.get("rollup", True): continue
                k = (r.get("region") or "", r.get("bde") or "", r.get("sold_to") or "")
                has_primary = any(s not in SECONDARY_SCOPES for s in scopes_by_group.get(k, ()))
                is_secondary = (r.get("scope") or "BASE") in SECONDARY_SCOPES
                if not (has_primary and is_secondary):
                    tot_qty += r["actual_qty"]
                    tot_amt += r["actual_amt"]
                tot_reb += r["curr_rebate"]
            tot_qty = round(tot_qty, 2)
            tot_amt = round(tot_amt, 2)
            store_reb = round(tot_reb, 2)
            hq_reb    = round(hq_box_out["total"], 2)
            grand_reb = round(store_reb + hq_reb, 2)
            ws.append([])
            # Grand Total row: matches the screen's top-right GRAND TOTAL box
            # exactly (Store rebate + HQ/VR rebate).  Qty and Amount sum the
            # underlying sales the rebates were earned on.
            ws.append(["TOTAL", "", "", "", "", "", "", "", "",
                       tot_qty, tot_amt, "", "", "", "", grand_reb])
            for cell in ws[ws.max_row]:
                cell.font = Font(bold=True)
                cell.fill = PatternFill("solid", fgColor="DBEAFE")
            for label, val in (("  · Store rebate (per-store program)", store_reb),
                               ("  · HQ / VR rebate (account-level program)", hq_reb)):
                ws.append([label, "", "", "", "", "", "", "", "",
                           "", "", "", "", "", "", val])
                for cell in ws[ws.max_row]:
                    cell.font = Font(italic=True, color="64748B")

            # HQ / VR rebate (the top box) on its own sheet
            ws2 = wb.create_sheet("HQ_VR Rebate")
            ws2.append(["Sold-To", "Sold-To Name", "Group", "Rebate", "Detail"])
            for c in ws2[1]:
                c.font = Font(bold=True, color="FFFFFF")
                c.fill = PatternFill("solid", fgColor="BE123C")
            for h in hq_box_out["items"]:
                detail = "; ".join(f'{p["structure_name"]} {p["rate"]}% = {p["rebate"]}'
                                   for p in h.get("parts", []))
                ws2.append([h["sold_to"], h["sold_to_name"], h.get("sold_to_group", ""),
                            h["rebate"], detail])
            ws2.append([])
            ws2.append(["TOTAL", "", "", hq_box_out["total"], ""])
            bio = BytesIO(); wb.save(bio); bio.seek(0)
            mon = (request.args.get("month") or "thismonth")
            fname = f"rebate_{mon}_{date.today().isoformat()}.xlsx"
            return Response(
                bio.getvalue(),
                mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f"attachment;filename={fname}"})

        # ?? 8. Group by (region, sold_to) with brand sub-groups ??????????????
        grp_map = {}
        brand_order = {"HK": 0, "LF": 1, "TTL": 2}
        for r in rows:
            # Include BDE in the group key.  For PER_SHIP_TO structures
            # (AJT etc.) each ship_to row already carries its own BDE; the
            # frontend keys its display tree by region+bde.  Without BDE
            # here, every NSW row for JAX 731942 collapses into a single
            # group whose label is whichever row landed first — so
            # Makris's 26 ship_tos and Alessio's 21 show up under one
            # BDE only.
            key = r["region"] + "|" + r["sold_to"] + "|" + (r["bde"] or "-")
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
            # rollup=False rows are display-only duplicates: members of an _SR
            # Group that aren't the group's primary BDE row.  They still render
            # (so every ship_to shows the shared group figure) but must not be
            # summed into the totals, else a multi-member group would multiply.
            # (HQ/VR of _SR accounts never reach here — they go to the top box.)
            do_rollup = r.get("rollup", True)
            if do_rollup:
                g["grp_curr_rebate"] += r["curr_rebate"]
                g["grp_est"]         += r["est_rebate"] if r["est_rebate"] else 0.0
                g["grp_actual"]      += r["actual"]
                g["grp_actual_qty"]  += r["actual_qty"]
                g["grp_actual_amt"]  += r["actual_amt"]
            # One display block per structure, so different structures that
            # share a brand_key (e.g. two TTL/HK_TBR/… or an _SR vs _HQ of the
            # same brand) never merge into one block.
            bkey = r["structure_name"]
            if bkey not in g["brands"]:
                g["brands"][bkey] = {
                    "brand": r["brand"], "unit": r["unit"],
                    "scope": r["scope"],
                    "badges": r["badges"],
                    "structure_name": r["structure_name"],
                    "grp_actual": 0.0, "grp_actual_qty": 0.0, "grp_actual_amt": 0.0,
                    "grp_curr_rebate": 0.0, "grp_est": 0.0, "items": [],
                }
            b = g["brands"][bkey]
            if do_rollup:
                b["grp_actual"]      += r["actual"]
                b["grp_actual_qty"]  += r["actual_qty"]
                b["grp_actual_amt"]  += r["actual_amt"]
                b["grp_curr_rebate"] += r["curr_rebate"]
                b["grp_est"]         += r["est_rebate"] if r["est_rebate"] else 0.0
            b["items"].append(r)

        # Convert brands dict to sorted list (HK → LF → TTL; within a brand,
        # the SR/base block first, then the HQ/VR/IT/WTY programs).
        scope_order = {"SR": 0, "BASE": 0, "HQ": 1, "VR": 2, "IT": 3, "WTY": 4}
        for g in grp_map.values():
            g["brands"] = sorted(
                g["brands"].values(),
                key=lambda b: (brand_order.get(b["brand"], 99),
                               scope_order.get(b.get("scope", "BASE"), 9)))
            # When a sold_to has both a Store/BASE rebate AND a covering
            # HQ/VR/IT/WTY rebate, the secondary structure's qty/amt is
            # the same underlying sales already counted in the base.
            # Subtract the secondary contributions so the sold_to summary
            # row shows the actual sales once (not doubled).
            secondary_scopes = {"HQ", "VR", "IT", "WTY"}
            has_primary = any(b.get("scope", "BASE") not in secondary_scopes
                              for b in g["brands"])
            if has_primary:
                for b in g["brands"]:
                    if b.get("scope", "BASE") in secondary_scopes:
                        g["grp_actual_qty"] -= b["grp_actual_qty"]
                        g["grp_actual_amt"] -= b["grp_actual_amt"]
                        g["grp_actual"]     -= b["grp_actual"]

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
    sales_tbl    = _rebate_sales_table(request.args.get("month"))

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
            FROM """ + sales_tbl + """ s
            WHERE s.brand IN ('HK','LF')
              AND s.so_type IN """ + _REBATE_SO_TYPES_IN + """
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
                "state": _rebate_region(r["bde_state"]) or "-",
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

            if _rebate_is_ship_to(struct):
                # _SR → one row per ship_to
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
# Customer-facing portal (claim form + QR codes). Falls back to
# DASHBOARD_URL so existing setups keep working until a dedicated
# subdomain (e.g. https://claim.hkaudashboard.com.au) is provisioned.
CLAIM_PORTAL_URL = os.getenv("CLAIM_PORTAL_URL", DASHBOARD_URL)
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
    ("Makris George",    "george.makris@hankooktyre.com.au",    "NSW", "BDE"),
    # QLD
    ("Marsh Aaron",      "aaron.marsh@hankooktyre.com.au",      "QLD", "SM"),
    ("Spires Steven",    "steven.spires@hankooktyre.com.au",    "QLD", "BDE"),
    ("Maclure Adam",     "adam.maclure@hankooktyre.com.au",     "QLD", "BDE"),
    ("Bovey Craig",      "craig.bovey@hankooktyre.com.au",      "QLD", "BDE"),
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
                    visit_purpose="", prep_notes="", prep_history=None):
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
    if prep_notes:
        rows.append(_row("Prep",
                        f'<div style="background:#eff6ff;border-left:3px solid #2563eb;'
                        f'padding:6px 8px;color:#1e3a5f;white-space:pre-wrap;">'
                        f'{_esc_html(prep_notes)}</div>'))
    # Older prep notes for this same ship_to (from prior submissions
    # — typically the "Meeting Preparation" entries written before
    # the visit happened).  Shown chronologically so the reader has
    # the full prep context in one place.
    if prep_history:
        history_html = "".join(
            f'<div style="margin-top:4px;padding:6px 8px;background:#f8fafc;'
            f'border-left:3px solid #94a3b8;border-radius:3px;font-size:12px;'
            f'color:#334155;">'
            f'<div style="font-size:10px;color:#6b7280;margin-bottom:2px;">'
            f'<b>{_esc_html(h.get("bde_name") or "")}</b> · '
            f'{_esc_html(h.get("visit_date") or "")}</div>'
            f'<div style="white-space:pre-wrap;">{_esc_html(h.get("prep_notes") or "")}</div>'
            f'</div>'
            for h in prep_history
        )
        rows.append(_row("Prep history", history_html))
    if met_person:
        rows.append(_row("Met",      _esc_html(met_person)))
    if notes:
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

def _fetch_prep_history(ship_to, exclude_id=None):
    """Return every prior prep_notes entry for this ship_to so an
    actual-log notification can carry forward the prep context (the
    BDE writes the visit log into a fresh textarea on purpose; this
    keeps the email recipients seeing what was prepped beforehand)."""
    if not ship_to:
        return []
    try:
        conn = get_connection(); cur = conn.cursor(dictionary=True)
        sql = (
            "SELECT id, visit_date, bde_name, prep_notes "
            "FROM meeting_log "
            "WHERE ship_to = %s "
            "  AND prep_notes IS NOT NULL AND TRIM(prep_notes) <> '' "
        )
        params = [ship_to]
        if exclude_id is not None:
            sql += "AND id <> %s "
            params.append(exclude_id)
        sql += "ORDER BY visit_date ASC, id ASC"
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
        for r in rows:
            if r.get("visit_date"):
                r["visit_date"] = r["visit_date"].strftime("%Y-%m-%d")
        cur.close(); conn.close()
        return rows
    except Exception as e:
        print(f"[mail] _fetch_prep_history failed: {e}")
        return []

def _notify_new_log(rid, bde_name, sold_to_name, ship_to, ship_to_name,
                    visit_date, met_person, notes, next_action,
                    visit_purpose="", plan_bde_email="", prep_notes=""):
    """Email Hayden + JJ + Jayden + the State Manager of the BDE's
    state.  CC the BDE author so they have a record of what was sent
    on their behalf.  Also CC the original scheduling BDE (plan_bde_email)
    if the log was created from someone else's calendar plan."""
    state    = _resolve_bde_state(bde_name)
    sm_email = STATE_MANAGER_EMAIL.get(state) if state else None
    author   = _lookup_bde_email(bde_name)
    to_list  = list(ALWAYS_TO)
    if sm_email and sm_email.lower() not in [x.lower() for x in to_list]:
        to_list.append(sm_email)
    cc_list  = [author] if author and author.lower() not in [x.lower() for x in to_list] else []
    if plan_bde_email and plan_bde_email.lower() != (author or "").lower() \
       and plan_bde_email.lower() not in [x.lower() for x in to_list] \
       and plan_bde_email.lower() not in [x.lower() for x in cc_list]:
        cc_list.append(plan_bde_email)
    subject  = (f"[BDE Visit] {bde_name} → {ship_to} "
                f"{ship_to_name or ''} ({visit_date})")
    # Surface the resolution so any "SM not getting mail" issue is
    # diagnosable from the console log without code changes.
    print(f"[mail] new_log #{rid} bde={bde_name!r} state={state!r} "
          f"sm={sm_email!r} plan_bde={plan_bde_email!r} → to={to_list} cc={cc_list}")
    prep_history = _fetch_prep_history(ship_to, exclude_id=rid)
    html = _email_log_html(rid, bde_name, sold_to_name, ship_to, ship_to_name,
                           visit_date, met_person, notes, next_action, None,
                           visit_purpose=visit_purpose,
                           prep_notes=prep_notes,
                           prep_history=prep_history)
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
    # Original scheduling BDE — if the log was created from a calendar
    # chip planned by someone else (e.g. a manager logged a visit on
    # behalf of the BDE who scheduled it), make sure that BDE is in the
    # loop on every comment.
    plan_em = (log_row.get("plan_bde_email") or "").strip().lower() \
              or (_lookup_bde_email(log_row.get("plan_bde_name") or "") or "").lower()
    if plan_em: participants.add(plan_em)

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

    # Log card + thread.  prep_history pulls every prior prep entry
    # for this ship_to so the reader of a feedback notification still
    # sees the full context, not just whatever was on this one row.
    prep_history = _fetch_prep_history(log_row.get("ship_to") or "",
                                       exclude_id=rid)
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
        visit_purpose=log_row.get("visit_purpose") or "",
        prep_notes=log_row.get("prep_notes") or "",
        prep_history=prep_history,
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
            ("sold_to_name",   "VARCHAR(160) NOT NULL DEFAULT ''", "sold_to"),
            ("visit_date",     "DATE NOT NULL DEFAULT '1970-01-01'", "created_at"),
            ("feedback",       "TEXT",                              "next_action"),
            ("visit_purpose",  "VARCHAR(40) NOT NULL DEFAULT ''",   "ship_to"),
            # Meeting Preparation note + the BDE who originally scheduled
            # the visit on the calendar (so feedback emails CC them).
            ("prep_notes",         "TEXT",                              "feedback"),
            ("plan_bde_email",     "VARCHAR(120) NOT NULL DEFAULT ''",  "bde_email"),
            ("plan_bde_name",      "VARCHAR(120) NOT NULL DEFAULT ''",  "plan_bde_email"),
            ("met_person_contact", "VARCHAR(80) NOT NULL DEFAULT ''",   "met_person"),
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
_ensure_request_log_table()

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
        # BDEs see only their own data: lock the salesman dropdown to
        # their name AND the region buttons to their state, so they
        # can't accidentally widen the view by clicking another State.
        out["lock_salesman"] = name
        out["lock_region"]   = state
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


def _meeting_owner_or_403(cur, mid):
    """Return (ok, owner_email).  ok is False with the row missing/forbidden.
    Legacy rows may have a blank bde_email — match by bde_name in that case."""
    me_email = (_bde_from_request() or "").strip().lower()
    if not me_email:
        return False, None
    me_name = (_EMAIL_TO_DIR.get(me_email) or ("", "", ""))[0].strip().lower()
    cur.execute("SELECT bde_email, bde_name FROM meeting_log WHERE id=%s", (mid,))
    row = cur.fetchone()
    if not row:
        return False, None
    o_email = (row.get("bde_email") or "").strip().lower()
    o_name  = (row.get("bde_name")  or "").strip().lower()
    ok = (o_email and o_email == me_email) or (me_name and o_name == me_name)
    return bool(ok), o_email

@app.patch("/api/meeting/<int:mid>")
def meeting_patch(mid):
    """Edit one's own meeting_log row.  Only the original author (matched by
    bde_email) can update notes / next_action / prep_notes / met_person /
    visit_purpose / visit_date."""
    body = request.get_json(silent=True) or {}
    allowed = ("notes", "next_action", "prep_notes",
               "met_person", "met_person_contact",
               "visit_purpose", "visit_date")
    updates = {k: (body[k] or "").strip() if isinstance(body[k], str) else body[k]
               for k in allowed if k in body}
    if "visit_purpose" in updates and updates["visit_purpose"] and \
       updates["visit_purpose"] not in ("Promotion", "Product introduction",
                                        "Claim support", "Rebate follow-up",
                                        "Stock", "Other"):
        return jsonify({"error": "invalid visit_purpose"}), 400
    if not updates:
        return jsonify({"error": "no fields"}), 400
    conn = get_connection(); cur = conn.cursor(dictionary=True)
    try:
        ok, _ = _meeting_owner_or_403(cur, mid)
        if not ok:
            return jsonify({"error": "forbidden"}), 403
        sets   = ", ".join(f"{k}=%s" for k in updates)
        params = list(updates.values()) + [mid]
        cur.execute(f"UPDATE meeting_log SET {sets} WHERE id=%s", params)
        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close(); conn.close()

@app.delete("/api/meeting/<int:mid>")
def meeting_delete(mid):
    """Delete one's own meeting_log row (and its feedback thread)."""
    conn = get_connection(); cur = conn.cursor(dictionary=True)
    try:
        ok, _ = _meeting_owner_or_403(cur, mid)
        if not ok:
            return jsonify({"error": "forbidden"}), 403
        cur.execute("DELETE FROM meeting_feedback WHERE meeting_id=%s", (mid,))
        cur.execute("DELETE FROM meeting_log WHERE id=%s", (mid,))
        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close(); conn.close()

@app.patch("/api/meeting/feedback/<int:fid>")
def meeting_feedback_patch(fid):
    """Edit one's own feedback comment."""
    body = request.get_json(silent=True) or {}
    text = (body.get("text") or "").strip()
    if not text:
        return jsonify({"error": "text is required"}), 400
    me_email = (_bde_from_request() or "").strip().lower()
    if not me_email:
        return jsonify({"error": "auth required"}), 401
    me_name = (_EMAIL_TO_DIR.get(me_email) or ("", "", ""))[0].strip().lower()
    conn = get_connection(); cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT author_email, author_name, meeting_id FROM meeting_feedback WHERE id=%s", (fid,))
        row = cur.fetchone()
        if not row:
            return jsonify({"error": "not found"}), 404
        o_email = (row.get("author_email") or "").strip().lower()
        o_name  = (row.get("author_name")  or "").strip().lower()
        if not ((o_email and o_email == me_email) or (me_name and o_name == me_name)):
            return jsonify({"error": "forbidden"}), 403
        cur.execute("UPDATE meeting_feedback SET text=%s WHERE id=%s", (text, fid))
        # Refresh the denormalised latest-feedback cache on meeting_log
        cur.execute("""
            SELECT text FROM meeting_feedback
            WHERE meeting_id=%s
            ORDER BY created_at DESC, id DESC LIMIT 1
        """, (row["meeting_id"],))
        last = cur.fetchone()
        cur.execute("UPDATE meeting_log SET feedback=%s WHERE id=%s",
                    (last["text"] if last else None, row["meeting_id"]))
        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close(); conn.close()

@app.delete("/api/meeting/feedback/<int:fid>")
def meeting_feedback_delete(fid):
    """Delete one's own feedback comment."""
    me_email = (_bde_from_request() or "").strip().lower()
    if not me_email:
        return jsonify({"error": "auth required"}), 401
    me_name = (_EMAIL_TO_DIR.get(me_email) or ("", "", ""))[0].strip().lower()
    conn = get_connection(); cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT author_email, author_name, meeting_id FROM meeting_feedback WHERE id=%s", (fid,))
        row = cur.fetchone()
        if not row:
            return jsonify({"error": "not found"}), 404
        o_email = (row.get("author_email") or "").strip().lower()
        o_name  = (row.get("author_name")  or "").strip().lower()
        if not ((o_email and o_email == me_email) or (me_name and o_name == me_name)):
            return jsonify({"error": "forbidden"}), 403
        mid = row["meeting_id"]
        cur.execute("DELETE FROM meeting_feedback WHERE id=%s", (fid,))
        cur.execute("""
            SELECT text FROM meeting_feedback
            WHERE meeting_id=%s
            ORDER BY created_at DESC, id DESC LIMIT 1
        """, (mid,))
        last = cur.fetchone()
        cur.execute("UPDATE meeting_log SET feedback=%s WHERE id=%s",
                    (last["text"] if last else None, mid))
        conn.commit()
        return jsonify({"ok": True})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
    finally:
        cur.close(); conn.close()


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
        met_contact = (request.form.get("met_person_contact") or "").strip()[:80]
        notes     = (request.form.get("notes") or "").strip()
        next_act  = (request.form.get("next_action") or "").strip()
        feedback  = (request.form.get("feedback") or "").strip()
        visit_in  = (request.form.get("visit_date") or "").strip()
        purpose   = (request.form.get("visit_purpose") or "").strip()
        prep      = (request.form.get("prep_notes") or "").strip()
        # If the user landed on the form by clicking a calendar chip,
        # the frontend passes the original scheduling BDE's name so we
        # can email them when feedback is later added on this log.
        plan_bde_name  = (request.form.get("plan_bde_name") or "").strip()
        plan_bde_email = _lookup_bde_email(plan_bde_name) or "" if plan_bde_name else ""

        if not ship_to:
            return jsonify({"error": "ship_to is required"}), 400
        # Prep-only submissions are allowed: a row that only carries
        # Meeting Preparation (pre-visit) is valid.  A row with notes
        # (a real visit log) still requires purpose.
        if not notes and not prep:
            return jsonify({"error": "notes or prep_notes is required"}), 400
        if notes and not purpose:
            return jsonify({"error": "visit_purpose is required when notes is provided"}), 400
        if purpose and purpose not in ("Promotion", "Product introduction",
                                       "Claim support", "Rebate follow-up",
                                       "Stock", "Other"):
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
        # Potential customers (POT-<id>) own their own sold_to_name in
        # the potential_customer row — use that directly so the log
        # carries the same labels the BDE originally typed.
        pot_id = _parse_potential_id(ship_to)
        if pot_id is not None:
            try:
                _conn = get_connection(); _cur = _conn.cursor(dictionary=True)
                _cur.execute("SELECT sold_to_name FROM potential_customer "
                             "WHERE id = %s", (pot_id,))
                r = _cur.fetchone()
                _cur.close(); _conn.close()
                if r and r.get("sold_to_name"):
                    sold_to_name = r["sold_to_name"]
            except Exception as e:
                print(f"[meeting] potential sold_to resolve failed: {e}")
        elif sold_in:
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
                (visit_date, bde_email, bde_name,
                 plan_bde_email, plan_bde_name,
                 sold_to, sold_to_name,
                 ship_to, visit_purpose, met_person, met_person_contact,
                 notes, next_action,
                 feedback, prep_notes, photo_paths)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (visit_date_obj, bde_email, bde_name,
              plan_bde_email, plan_bde_name,
              sold_to, sold_to_name,
              ship_to, purpose, met, met_contact,
              notes, next_act,
              feedback if feedback else None,
              prep if prep else None,
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
                            visit_purpose=purpose,
                            plan_bde_email=plan_bde_email,
                            prep_notes=prep)
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
                   m.plan_bde_name, m.plan_bde_email,
                   m.sold_to, m.sold_to_name, m.ship_to,
                   m.visit_purpose, m.prep_notes,
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
                       AND m.notes IS NOT NULL AND TRIM(m.notes) <> ''
                   ) AS logged,
                   (SELECT GROUP_CONCAT(DISTINCT m.visit_purpose
                                        ORDER BY m.visit_purpose SEPARATOR ',')
                    FROM meeting_log m
                    WHERE m.ship_to = p.ship_to
                      AND YEAR(m.visit_date)  = YEAR(p.plan_date)
                      AND MONTH(m.visit_date) = MONTH(p.plan_date)
                      AND m.visit_purpose IS NOT NULL
                      AND TRIM(m.visit_purpose) <> ''
                   ) AS purposes
            FROM meeting_plan p
            WHERE {' AND '.join(wh)}
            ORDER BY p.plan_date, p.id
        """, tuple(params))
        rows = cur.fetchall()
        for r in rows:
            if r.get("plan_date"):
                r["plan_date"] = r["plan_date"].strftime("%Y-%m-%d")
            r["logged"] = bool(r.get("logged"))
            r["purposes"] = (r.get("purposes") or "")
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
        # Without a BDE name the chip would be impossible to attribute
        # and the BDE-role calendar filter would hide it.  Return a
        # clear error instead of silently inserting an orphan row —
        # makes mis-configured directory entries obvious instead of
        # producing 'drag-drop does nothing' bug reports.
        if not bdenm.strip():
            return jsonify({"error":
                "Cannot find your BDE name.  "
                "Pick a BDE in the form first, or ask the admin to add "
                "your email to the directory."}), 400

        # Resolve sold_to + names from customer master so the calendar
        # chip can show a friendly label without a separate lookup.
        # Potential customers (POT-<id>) come from a different table.
        sold_to, sold_to_name, ship_to_name = "", "", ""
        try:
            conn = get_connection(); cur = conn.cursor(dictionary=True)
            pid = _parse_potential_id(ship)
            if pid is not None:
                cur.execute("SELECT name, sold_to_name FROM potential_customer "
                            "WHERE id = %s", (pid,))
                r = cur.fetchone()
                if r:
                    ship_to_name = r.get("name") or ""
                    sold_to_name = r.get("sold_to_name") or ""
            else:
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
    """Slim ship_to list for the drag-and-drop palette.  Returns one
    row per (ship_to, sold_to_name) combination so a ship_to that
    sits under multiple parents in the customer master shows up under
    each parent's Sold-to filter.  The frontend dedupes by ship_to
    for the unfiltered view."""
    try:
        conn = get_connection(); cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT ship_to,
                   MIN(NULLIF(TRIM(ship_to_name),'')) AS ship_to_name,
                   sold_to,
                   NULLIF(TRIM(sold_to_name),'')      AS sold_to_name
            FROM customer
            WHERE ship_to IS NOT NULL AND TRIM(ship_to_name) <> ''
            GROUP BY ship_to, sold_to, sold_to_name
            ORDER BY ship_to_name, sold_to_name
            LIMIT 8000
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
                   m.plan_bde_name, m.plan_bde_email,
                   m.sold_to, m.ship_to, m.visit_purpose,
                   m.met_person, m.met_person_contact,
                   m.notes, m.next_action, m.feedback, m.prep_notes, m.photo_paths,
                   COALESCE(NULLIF(TRIM(c.ship_to_name),''), m.ship_to) AS ship_to_name,
                   COALESCE(NULLIF(TRIM(c.sold_to_name),''),
                            NULLIF(TRIM(m.sold_to_name),''),
                            m.sold_to) AS sold_to_name,
                   c.contact_person AS contact_person,
                   c.phone          AS contact_phone,
                   -- Closest plan_date within the same month, for the same
                   -- ship_to, so the panel can show 'Planned vs Visited'.
                   (SELECT p.plan_date
                    FROM meeting_plan p
                    WHERE p.ship_to = m.ship_to
                      AND YEAR(p.plan_date)  = YEAR(m.visit_date)
                      AND MONTH(p.plan_date) = MONTH(m.visit_date)
                    ORDER BY ABS(DATEDIFF(p.plan_date, m.visit_date)), p.plan_date
                    LIMIT 1) AS plan_date
            FROM meeting_log m
            LEFT JOIN (
                SELECT ship_to,
                       MIN(NULLIF(TRIM(ship_to_name),'')) AS ship_to_name,
                       MIN(NULLIF(TRIM(sold_to_name),'')) AS sold_to_name,
                       CAST(NULL AS CHAR) AS contact_person,
                       CAST(NULL AS CHAR) AS phone
                FROM customer GROUP BY ship_to
                UNION ALL
                -- Potential customers come through as POT-<id> in ship_to;
                -- expose both names AND the contact info BDEs typed when
                -- creating the prospect so the panel can show how to
                -- re-contact them without a separate lookup.
                SELECT CONCAT('POT-', id) AS ship_to,
                       name              AS ship_to_name,
                       sold_to_name      AS sold_to_name,
                       contact_person    AS contact_person,
                       phone             AS phone
                FROM potential_customer
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
            if r.get("plan_date"):
                r["plan_date"] = r["plan_date"].strftime("%Y-%m-%d")
            r["photo_paths"] = r["photo_paths"].split(",") if r["photo_paths"] else []
            r["thread"]      = thread_by_meeting.get(r["id"], [])
            # Frontend State filter uses this — group SA/TAS under VIC
            # (same as the State Manager mapping) so the 4 main state
            # buttons cover every BDE.
            st = _resolve_bde_state(r.get("bde_name") or "") or ""
            r["bde_state"] = "VIC" if st in ("SA", "TAS") else st
        cur.close(); conn.close()
        return jsonify(rows)
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────────────────────────────
# AI feature — Summary / Strategy generator for one meeting_log row.
# Self-contained block; remove everything between the START / END
# markers (and the matching frontend section in static/meeting.html) to
# fully revert.  When ANTHROPIC_API_KEY is unset, the feature stays
# dormant: GET /api/ai_status returns enabled=false and the frontend
# hides the button.
# ─────────────────────────────────────────────────────────────────────
# === AI FEATURE START ===
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
AI_MODEL          = os.getenv("AI_MODEL", "claude-haiku-4-5")
# Cap on rows the digest endpoint will accept in one call so token
# spend per click stays predictable.
AI_DIGEST_MAX_ROWS = int(os.getenv("AI_DIGEST_MAX_ROWS", "30"))

def _ai_fmt_row(r):
    bits = []
    if r.get("visit_date"):    bits.append(f"[{r['visit_date']}]")
    if r.get("bde_name"):      bits.append(f"BDE: {r['bde_name']}")
    if r.get("visit_purpose"): bits.append(f"Purpose: {r['visit_purpose']}")
    if r.get("met_person"):    bits.append(f"Met: {r['met_person']}")
    if r.get("ship_to"):       bits.append(f"Shop: {r['ship_to']}")
    if r.get("sold_to_name") or r.get("sold_to"):
        bits.append(f"Customer: {r.get('sold_to_name') or r.get('sold_to')}")
    lines = [" | ".join(bits)] if bits else []
    if r.get("prep_notes"):  lines.append(f"  Prep:  {r['prep_notes']}")
    if r.get("notes"):       lines.append(f"  Notes: {r['notes']}")
    if r.get("next_action"): lines.append(f"  Next:  {r['next_action']}")
    return "\n".join(lines)

def _ai_call_claude(prompt):
    if not ANTHROPIC_API_KEY:
        return None, "AI disabled — ANTHROPIC_API_KEY not configured on the server"
    import requests as _rq
    try:
        r = _rq.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": AI_MODEL,
                "max_tokens": 1200,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=60,
        )
        if r.status_code != 200:
            return None, f"Anthropic API {r.status_code}: {r.text[:300]}"
        data = r.json()
        text = "".join(b.get("text", "") for b in (data.get("content") or []))
        return text.strip(), None
    except Exception as e:
        return None, f"AI call error: {e}"

def _ai_parse(text):
    if not text: return "", ""
    import re as _re
    parts = _re.split(r"(?im)^\s*STRATEGY\s*:?\s*", text, maxsplit=1)
    if len(parts) == 2:
        summary = _re.sub(r"(?im)^\s*SUMMARY\s*:?\s*", "", parts[0]).strip()
        strategy = parts[1].strip()
    else:
        summary = _re.sub(r"(?im)^\s*SUMMARY\s*:?\s*", "", text).strip()
        strategy = ""
    return summary, strategy

@app.get("/api/ai_status")
def ai_status():
    return jsonify({"enabled": bool(ANTHROPIC_API_KEY), "model": AI_MODEL})

# ─── Visit Impact ────────────────────────────────────────────────────
# "Did this BDE visit actually move the needle on sales?" — answered
# by comparing the shop's sales in the month BEFORE a visit vs the
# month AFTER.  Strict pre/post, no in-month overlap, so causality is
# at least defensible (the model: "BDE visited mid-April; did May
# sales tick up vs March?").  We don't claim causation in the UI —
# this is correlation surfaced to the BDE/manager to spot patterns.
def _sales_table_for_ym(year, month):
    """Return the sales-table name that holds a given (year, month),
    or None if we don't have that month on disk."""
    try:
        now = datetime.now()
        if int(year) == now.year and int(month) == now.month:
            return "sales_thismonth"
        yymm = f"{int(year) % 100:02d}{int(month):02d}"
        table = f"sales_{yymm}"
        if table in REBATE_SALES_TABLES.values():
            return table
    except Exception:
        pass
    return None

def _sales_for_month(cur, ship_to, year, month):
    """SUM(qty), SUM(amt) for ship_to in (year, month).  Returns None
    when the table for that month doesn't exist on this server."""
    table = _sales_table_for_ym(year, month)
    if not table:
        return None
    try:
        cur.execute(
            "SELECT COALESCE(SUM(qty),0) AS qty, COALESCE(SUM(amt),0) AS amt "
            "FROM " + table + " "
            "WHERE ship_to = %s AND brand IN ('HK','LF')",
            (ship_to,)
        )
        r = cur.fetchone() or {}
        return {"qty": float(r.get("qty") or 0), "amt": float(r.get("amt") or 0)}
    except Exception:
        return None

@app.post("/api/meeting/impact")
def meeting_impact():
    """Batch pre/post sales delta for a list of meeting_log IDs.
    Body: { "ids": [int, ...] }
    Returns: { "impacts": { "<id>": {pre_year_month, post_year_month,
                                     pre, post, delta_qty, delta_amt,
                                     delta_qty_pct, delta_amt_pct,
                                     status}, ... } }
    status ∈ {ok, post_pending, pre_unavailable}.
    """
    body = request.get_json(silent=True) or {}
    raw_ids = body.get("ids") or []
    ids = []
    for x in raw_ids:
        try:
            ids.append(int(x))
        except Exception:
            pass
    if not ids:
        return jsonify({"impacts": {}})
    ids = ids[:200]   # cap so a chatty client can't tie up the DB

    try:
        conn = get_connection(); cur = conn.cursor(dictionary=True)
        placeholders = ",".join(["%s"] * len(ids))
        cur.execute(
            f"SELECT id, ship_to, visit_date FROM meeting_log "
            f"WHERE id IN ({placeholders}) AND visit_date IS NOT NULL "
            f"  AND ship_to IS NOT NULL AND ship_to <> ''",
            tuple(ids)
        )
        visits = cur.fetchall()

        impacts = {}
        for v in visits:
            vd = v.get("visit_date")
            ship_to = v.get("ship_to")
            if not vd or not ship_to:
                continue
            year, month = vd.year, vd.month
            # Previous calendar month.
            py = year - 1 if month == 1 else year
            pm = 12 if month == 1 else month - 1
            # Next calendar month.
            ny = year + 1 if month == 12 else year
            nm = 1 if month == 12 else month + 1

            pre  = _sales_for_month(cur, ship_to, py, pm)
            post = _sales_for_month(cur, ship_to, ny, nm)

            entry = {
                "pre_year_month":  f"{py}-{pm:02d}",
                "post_year_month": f"{ny}-{nm:02d}",
            }
            if pre is None:
                entry["status"] = "pre_unavailable"
                impacts[str(v["id"])] = entry
                continue
            entry["pre"] = pre
            if post is None:
                entry["status"] = "post_pending"
                impacts[str(v["id"])] = entry
                continue
            entry["post"] = post
            entry["delta_qty"] = round(post["qty"] - pre["qty"], 2)
            entry["delta_amt"] = round(post["amt"] - pre["amt"], 2)
            entry["delta_qty_pct"] = round(((post["qty"] - pre["qty"]) / pre["qty"] * 100), 1) if pre["qty"] > 0 else None
            entry["delta_amt_pct"] = round(((post["amt"] - pre["amt"]) / pre["amt"] * 100), 1) if pre["amt"] > 0 else None
            entry["status"] = "ok"
            impacts[str(v["id"])] = entry

        cur.close(); conn.close()
        return jsonify({"impacts": impacts})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


# ─── Shop Briefing Card ───────────────────────────────────────────────
# Per-shop summary view a BDE pulls up on the phone right before walking
# into a shop (or before calling its owner): last visit recap, recent
# contacts, 6-month sales trend, and a one-tap call/SMS/email row.  The
# rebate next-tier figures live on /api/rebate_data already and are
# fetched separately from the frontend so we don't duplicate that calc
# (and so the briefing renders even if rebate calc is slow).
@app.get("/shop/<ship_to>")
def shop_briefing_page(ship_to):
    return send_from_directory("static", "shop_briefing.html")

@app.get("/api/shop_briefing/<ship_to>")
def shop_briefing_data(ship_to):
    if not ship_to:
        return jsonify({"error": "ship_to required"}), 400
    try:
        conn = get_connection(); cur = conn.cursor(dictionary=True)

        # 1. Shop master ----------------------------------------------------
        cur.execute(
            "SELECT ship_to, ship_to_name, sold_to, sold_to_name, "
            "       bde_state, salesman_name "
            "FROM customer WHERE ship_to = %s LIMIT 1",
            (ship_to,),
        )
        shop = cur.fetchone() or {"ship_to": ship_to}

        # 2. Last meeting log -----------------------------------------------
        cur.execute(
            "SELECT id, visit_date, bde_name, bde_email, visit_purpose, "
            "       met_person, met_person_contact, prep_notes, notes, "
            "       next_action, created_at "
            "FROM meeting_log "
            "WHERE ship_to = %s "
            "  AND notes IS NOT NULL AND TRIM(notes) <> '' "
            "ORDER BY visit_date DESC, id DESC "
            "LIMIT 1",
            (ship_to,),
        )
        last_meeting = cur.fetchone()
        if last_meeting:
            for k in ("visit_date", "created_at"):
                if last_meeting.get(k):
                    last_meeting[k] = last_meeting[k].strftime("%Y-%m-%d")

        # 3. Distinct met person + contact pairs (history, newest first) -----
        # Many BDEs talk to multiple people at the same shop over time —
        # the briefing surfaces all of them so a one-tap dial picks any.
        cur.execute(
            "SELECT met_person, met_person_contact, MAX(visit_date) AS last_seen "
            "FROM meeting_log "
            "WHERE ship_to = %s "
            "  AND ((met_person IS NOT NULL AND TRIM(met_person) <> '') "
            "    OR (met_person_contact IS NOT NULL AND TRIM(met_person_contact) <> '')) "
            "GROUP BY met_person, met_person_contact "
            "ORDER BY last_seen DESC "
            "LIMIT 10",
            (ship_to,),
        )
        contacts = []
        for r in cur.fetchall():
            person  = (r.get("met_person") or "").strip()
            contact = (r.get("met_person_contact") or "").strip()
            if not person and not contact:
                continue
            contacts.append({
                "name":      person,
                "contact":   contact,
                "is_email":  "@" in contact,
                "last_seen": r["last_seen"].strftime("%Y-%m-%d") if r.get("last_seen") else "",
            })

        # 4. 6-month sales trend --------------------------------------------
        # Loop through the per-month REBATE_SALES_TABLES + sales_thismonth
        # so the trend always reflects whatever ETL has produced so far —
        # don't hand-code month names.  Tables that don't exist yet (e.g.
        # before this fiscal-year refresh) silently contribute zero.
        trend_specs = [
            ("Jan", "sales_2601"),
            ("Feb", "sales_2602"),
            ("Mar", "sales_2603"),
            ("Apr", "sales_2604"),
            ("May", "sales_2605"),
            ("Jun", "sales_thismonth"),
        ]
        sales_trend = []
        for label, tbl in trend_specs:
            try:
                cur.execute(
                    "SELECT COALESCE(SUM(qty),0) AS qty, "
                    "       COALESCE(SUM(amt),0) AS amt "
                    "FROM " + tbl + " "
                    "WHERE ship_to = %s "
                    "  AND brand IN ('HK','LF')",
                    (ship_to,)
                )
                r = cur.fetchone() or {"qty": 0, "amt": 0}
                sales_trend.append({
                    "month": label,
                    "table": tbl,
                    "qty":   float(r.get("qty") or 0),
                    "amt":   float(r.get("amt") or 0),
                })
            except Exception:
                # Table missing for that month — show zero rather than 500.
                sales_trend.append({"month": label, "table": tbl, "qty": 0, "amt": 0})

        # 5. Meeting count (past 12 months) ----------------------------------
        cur.execute(
            "SELECT COUNT(*) AS n FROM meeting_log "
            "WHERE ship_to = %s AND visit_date >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)",
            (ship_to,)
        )
        rcnt = cur.fetchone() or {"n": 0}
        meeting_count_12m = int(rcnt.get("n") or 0)

        cur.close(); conn.close()

        return jsonify({
            "shop":              shop,
            "last_meeting":      last_meeting,
            "contacts":          contacts,
            "sales_trend":       sales_trend,
            "meeting_count_12m": meeting_count_12m,
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


# ─── Admin: dashboard usage analytics ────────────────────────────────
# Read-only view onto request_log so we can quantify who's using the
# dashboard and which views matter.  Restricted to ALL-role users (the
# small admin set in _BDE_DIRECTORY — Hayden / JJ / Jayden).
def _is_admin_request():
    email = (_bde_from_request() or "").strip().lower()
    if not email:
        return False
    me = _EMAIL_TO_DIR.get(email)
    # role is the third tuple element; "ALL" = unscoped admin
    return bool(me and me[2] == "ALL")

@app.get("/admin/usage")
def admin_usage_page():
    if not _is_admin_request():
        return ("Forbidden — admin only.", 403)
    return send_from_directory("static", "admin_usage.html")

@app.get("/api/admin/usage")
def admin_usage_data():
    """Aggregate request_log over the requested window (default 30 days).
    Returns top paths, top users, daily totals, and per-user activity
    counts so the admin_usage.html page can render without further
    server round-trips."""
    if not _is_admin_request():
        return jsonify({"error": "admin only"}), 403
    try:
        days = int(request.args.get("days") or 30)
    except Exception:
        days = 30
    days = max(1, min(days, 365))

    try:
        conn = get_connection(); cur = conn.cursor(dictionary=True)

        cur.execute(
            "SELECT COUNT(*) AS total, "
            "COUNT(DISTINCT user_email) AS uniq_users "
            "FROM request_log "
            "WHERE created_at >= NOW() - INTERVAL %s DAY",
            (days,)
        )
        summary = cur.fetchone() or {"total": 0, "uniq_users": 0}

        # Top paths — strip the cache-busting query string by indexing on
        # `path` only (already query-less since request.path).  Group / and
        # trailing-slash variants together by trimming a trailing slash.
        cur.execute(
            "SELECT path, COUNT(*) AS hits, "
            "COUNT(DISTINCT user_email) AS uniq_users "
            "FROM request_log "
            "WHERE created_at >= NOW() - INTERVAL %s DAY "
            "GROUP BY path "
            "ORDER BY hits DESC "
            "LIMIT 30",
            (days,)
        )
        top_paths = cur.fetchall()

        # Top users — include anonymous rows (empty email) too so the
        # unique-users KPI and this list always reconcile.  Subquery
        # picks the most common (country, IP) per user so the admin can
        # see "where they came from" without scanning every row.
        cur.execute(
            "SELECT r.user_email, COUNT(*) AS hits, "
            "       COUNT(DISTINCT DATE(r.created_at)) AS active_days, "
            "       MAX(r.created_at) AS last_seen, "
            "       (SELECT country FROM request_log r2 "
            "         WHERE r2.user_email = r.user_email "
            "           AND r2.created_at >= NOW() - INTERVAL %s DAY "
            "         GROUP BY country ORDER BY COUNT(*) DESC LIMIT 1) AS country, "
            "       (SELECT ip FROM request_log r3 "
            "         WHERE r3.user_email = r.user_email "
            "           AND r3.created_at >= NOW() - INTERVAL %s DAY "
            "         GROUP BY ip ORDER BY COUNT(*) DESC LIMIT 1) AS ip "
            "FROM request_log r "
            "WHERE r.created_at >= NOW() - INTERVAL %s DAY "
            "GROUP BY r.user_email "
            "ORDER BY hits DESC "
            "LIMIT 50",
            (days, days, days)
        )
        top_users = cur.fetchall()
        for r in top_users:
            if r.get("last_seen"):
                r["last_seen"] = r["last_seen"].strftime("%Y-%m-%d %H:%M")
            if not r.get("user_email"):
                # Empty-email rows = requests that didn't carry a
                # Cf-Access-Authenticated-User-Email header.  Most
                # commonly: the public /claim/* customer portal, or
                # health-check hits from Cloudflare itself.
                r["user_email"] = "(no auth — public /claim or health check)"
            r["country"] = r.get("country") or ""
            r["ip"] = r.get("ip") or ""

        cur.execute(
            "SELECT DATE(created_at) AS d, COUNT(*) AS hits, "
            "COUNT(DISTINCT user_email) AS uniq_users "
            "FROM request_log "
            "WHERE created_at >= NOW() - INTERVAL %s DAY "
            "GROUP BY DATE(created_at) "
            "ORDER BY d",
            (days,)
        )
        daily = cur.fetchall()
        for r in daily:
            if r.get("d"):
                r["d"] = r["d"].strftime("%Y-%m-%d")

        cur.close(); conn.close()
        return jsonify({
            "days": days,
            "total": int(summary.get("total") or 0),
            "uniq_users": int(summary.get("uniq_users") or 0),
            "top_paths": top_paths,
            "top_users": top_users,
            "daily": daily,
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.get("/api/admin/usage/user")
def admin_usage_user_detail():
    """Per-user drill-down: which paths did this user hit, when, and
    from where.  Drives the per-user activity panel on /admin/usage."""
    if not _is_admin_request():
        return jsonify({"error": "admin only"}), 403
    email = (request.args.get("email") or "").strip().lower()
    try:
        days = int(request.args.get("days") or 30)
    except Exception:
        days = 30
    days = max(1, min(days, 365))
    # The Top Users list shows "(no auth …)" for empty-email rows;
    # the frontend sends that label back verbatim — translate it into
    # the SQL-friendly empty string.
    if email.startswith("(no auth"):
        email = ""

    try:
        conn = get_connection(); cur = conn.cursor(dictionary=True)

        cur.execute(
            "SELECT path, COUNT(*) AS hits, MAX(created_at) AS last_seen "
            "FROM request_log "
            "WHERE user_email = %s "
            "  AND created_at >= NOW() - INTERVAL %s DAY "
            "GROUP BY path "
            "ORDER BY hits DESC "
            "LIMIT 30",
            (email, days)
        )
        paths = cur.fetchall()
        for r in paths:
            if r.get("last_seen"):
                r["last_seen"] = r["last_seen"].strftime("%Y-%m-%d %H:%M")

        cur.execute(
            "SELECT DATE(created_at) AS d, COUNT(*) AS hits "
            "FROM request_log "
            "WHERE user_email = %s "
            "  AND created_at >= NOW() - INTERVAL %s DAY "
            "GROUP BY DATE(created_at) ORDER BY d",
            (email, days)
        )
        daily = cur.fetchall()
        for r in daily:
            if r.get("d"):
                r["d"] = r["d"].strftime("%Y-%m-%d")

        cur.execute(
            "SELECT country, ip, COUNT(*) AS hits, MAX(created_at) AS last_seen "
            "FROM request_log "
            "WHERE user_email = %s "
            "  AND created_at >= NOW() - INTERVAL %s DAY "
            "GROUP BY country, ip "
            "ORDER BY hits DESC "
            "LIMIT 10",
            (email, days)
        )
        locations = cur.fetchall()
        for r in locations:
            if r.get("last_seen"):
                r["last_seen"] = r["last_seen"].strftime("%Y-%m-%d %H:%M")

        cur.close(); conn.close()
        return jsonify({
            "email": email or "(no auth)",
            "days": days,
            "paths": paths,
            "daily": daily,
            "locations": locations,
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.post("/api/meeting/voice_extract")
def meeting_voice_extract():
    """Convert a free-form voice transcript into structured meeting_log fields.
    Body: { "transcript": "<user's spoken recap>" }
    Returns: {
        notes, met_person, met_person_contact, next_action,
        visit_purpose ("" | "Promotion" | "Product introduction"
                       | "Claim support" | "Rebate follow-up" | "Stock" | "Other"),
        model
    }
    Each field is a string — empty when the model couldn't infer it.  The
    frontend only writes into empty form fields so user-typed values are
    never overwritten.
    """
    if not ANTHROPIC_API_KEY:
        return jsonify({"error": "AI feature disabled — ANTHROPIC_API_KEY is not set on the server"}), 503
    body = request.get_json(silent=True) or {}
    transcript = (body.get("transcript") or "").strip()
    if not transcript:
        return jsonify({"error": "transcript is empty"}), 400
    if len(transcript) > 4000:
        transcript = transcript[:4000]

    prompt = (
        "You extract structured fields from a BDE's spoken recap of a tyre-shop visit.\n"
        "Return ONLY a JSON object with these keys (all strings, empty when unknown):\n"
        '  - "notes": a clean written summary of what happened (1-3 sentences, in English).\n'
        '  - "met_person": the name or role of the person met (e.g. "John", "owner", "store manager"). Empty if not mentioned.\n'
        '  - "met_person_contact": phone or email if mentioned. Empty if not.\n'
        '  - "next_action": follow-up needed, if mentioned. Empty if not.\n'
        '  - "visit_purpose": MUST be exactly one of "Promotion", "Product introduction", '
        '"Claim support", "Rebate follow-up", "Stock", "Other", or empty string.\n'
        "    Pick the closest match based on what was discussed. Empty only if truly unclear.\n"
        "\n"
        "Transcript:\n"
        f"\"\"\"\n{transcript}\n\"\"\"\n"
        "\n"
        "Respond with ONLY the JSON object, no markdown, no commentary."
    )
    text, err = _ai_call_claude(prompt)
    if err:
        return jsonify({"error": err}), 502

    import json as _json, re as _re
    raw = (text or "").strip()
    m = _re.search(r"\{.*\}", raw, _re.DOTALL)
    if not m:
        return jsonify({"error": "AI returned no JSON", "raw": raw[:300]}), 502
    try:
        parsed = _json.loads(m.group(0))
    except Exception as e:
        return jsonify({"error": f"AI JSON parse failed: {e}", "raw": raw[:300]}), 502

    allowed_purposes = {"Promotion", "Product introduction", "Claim support",
                        "Rebate follow-up", "Stock", "Other", ""}
    out = {
        "notes":              str(parsed.get("notes") or "").strip(),
        "met_person":         str(parsed.get("met_person") or "").strip()[:120],
        "met_person_contact": str(parsed.get("met_person_contact") or "").strip()[:80],
        "next_action":        str(parsed.get("next_action") or "").strip(),
        "visit_purpose":      str(parsed.get("visit_purpose") or "").strip(),
        "model":              AI_MODEL,
    }
    if out["visit_purpose"] not in allowed_purposes:
        out["visit_purpose"] = ""
    return jsonify(out)

@app.post("/api/meeting/ai_digest")
def meeting_ai_digest():
    """Summarise an arbitrary set of meeting_log rows (the rows currently
    visible after the user's State/BDE/Purpose/Shop filters).  Body:
        { "ids": [int, int, ...] }
    Returns { summary, strategy, model, count }.
    """
    if not ANTHROPIC_API_KEY:
        return jsonify({"error": "AI feature disabled — ANTHROPIC_API_KEY is not set on the server"}), 503
    body = request.get_json(silent=True) or {}
    raw_ids = body.get("ids") or []
    try:
        ids = [int(x) for x in raw_ids if str(x).strip()]
    except (TypeError, ValueError):
        return jsonify({"error": "ids must be a list of integers"}), 400
    if not ids:
        return jsonify({"error": "No entries selected"}), 400
    if len(ids) > AI_DIGEST_MAX_ROWS:
        ids = ids[:AI_DIGEST_MAX_ROWS]
    try:
        conn = get_connection(); cur = conn.cursor(dictionary=True)
        placeholders = ",".join(["%s"] * len(ids))
        cur.execute(f"""
            SELECT id, visit_date, bde_name,
                   sold_to, sold_to_name, ship_to, visit_purpose,
                   met_person, notes, next_action, prep_notes
            FROM meeting_log
            WHERE id IN ({placeholders})
            ORDER BY bde_name ASC, ship_to ASC,
                     visit_date DESC, created_at DESC
        """, tuple(ids))
        rows = cur.fetchall() or []
        cur.close(); conn.close()
        if not rows:
            return jsonify({"error": "No matching entries found"}), 404
        body_parts = [
            f"# {len(rows)} visit log entries to analyse "
            f"(the user has already narrowed the list with their filters)",
            "",
        ]
        for r in rows:
            body_parts.append(_ai_fmt_row(r))
            body_parts.append("")
        instructions = (
            "You are a sales-operations analyst for a tyre distributor in Australia.\n"
            "Treat the entries below as one cohort the user wants understood as a whole.\n"
            "Look for patterns across BDEs, customers, products, complaints, and outstanding next-actions.\n\n"
            "Reply in English with EXACTLY this format:\n\n"
            "SUMMARY:\n"
            "- 4 to 6 short bullets covering the cohort's main themes: what was discussed, "
            "recurring concerns, which customers/shops stand out, and any unresolved items.\n\n"
            "STRATEGY:\n"
            "- 4 to 6 concrete, prioritised next-step recommendations the team should take based on "
            "this cohort. Reference specific shops, BDEs, or product groups when relevant. "
            "Avoid generic advice.\n\n"
            "Keep each bullet under 30 words. Do not invent facts not present in the data."
        )
        prompt = instructions + "\n\n" + "\n".join(body_parts)
        text, err = _ai_call_claude(prompt)
        if err:
            return jsonify({"error": err}), 502
        summary, strategy = _ai_parse(text)
        return jsonify({
            "model":    AI_MODEL,
            "count":    len(rows),
            "summary":  summary,
            "strategy": strategy,
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
# === AI FEATURE END ===


# ─────────────────────────────────────────────────────────────────────
# CLAIM portal — public form at /claim/<ship_to_code> + threaded back
# and forth at /claims for internal team.  Wrapped in START/END markers
# for easy revert.  No login on the customer side: the URL itself
# identifies the shop, supplemented by a honeypot field for bot spam.
# ─────────────────────────────────────────────────────────────────────
# === CLAIM FEATURE START ===
CLAIM_PHOTO_DIR = os.path.join(BASE_DIR, "static", "claim_photos")
CLAIM_TYPES = ("Road Hazard Warranty",)
CLAIM_STATUSES = ("New", "Reviewing", "Awaiting customer",
                  "Completed", "Resolved", "Rejected")

def _ensure_claim_tables():
    try:
        os.makedirs(CLAIM_PHOTO_DIR, exist_ok=True)
        conn = get_connection(); cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS claim_submission (
                id            INT AUTO_INCREMENT PRIMARY KEY,
                ship_to       VARCHAR(64)  NOT NULL,
                sold_to       VARCHAR(64)  NOT NULL DEFAULT '',
                ship_to_name  VARCHAR(160) NOT NULL DEFAULT '',
                sold_to_name  VARCHAR(160) NOT NULL DEFAULT '',
                contact_name  VARCHAR(120) NOT NULL DEFAULT '',
                contact_phone VARCHAR(40)  NOT NULL DEFAULT '',
                contact_email VARCHAR(160) NOT NULL DEFAULT '',
                claim_type    VARCHAR(40)  NOT NULL DEFAULT '',
                product_size  VARCHAR(80)  NOT NULL DEFAULT '',
                description   TEXT,
                photo_paths   TEXT,
                status        VARCHAR(40)  NOT NULL DEFAULT 'New',
                assigned_to   VARCHAR(120) NOT NULL DEFAULT '',
                created_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
                resolved_at   TIMESTAMP    NULL,
                INDEX idx_ship    (ship_to),
                INDEX idx_status  (status),
                INDEX idx_created (created_at)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS claim_message (
                id           INT AUTO_INCREMENT PRIMARY KEY,
                claim_id     INT NOT NULL,
                author_type  VARCHAR(20)  NOT NULL DEFAULT 'customer',
                author_name  VARCHAR(120) NOT NULL DEFAULT '',
                author_email VARCHAR(160) NOT NULL DEFAULT '',
                category     VARCHAR(40)  NOT NULL DEFAULT '',
                text         TEXT,
                photo_paths  TEXT,
                created_at   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_claim   (claim_id),
                INDEX idx_created (created_at)
            )
        """)
        # Idempotent migration for tables that pre-date the category column.
        try:
            cur.execute("ALTER TABLE claim_message "
                        "ADD COLUMN category VARCHAR(40) NOT NULL DEFAULT '' "
                        "AFTER author_email")
        except Exception:
            pass
        cur.close(); conn.close()
    except Exception as e:
        print(f"[claim] schema init failed: {e}")
_ensure_claim_tables()

def _claim_lookup_shop(ship_to):
    """Resolve ship_to → (ship_to_name, sold_to, sold_to_name, bde_state, salesman_name)."""
    try:
        conn = get_connection(); cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT ship_to, ship_to_name, sold_to, sold_to_name, "
            "       bde_state, salesman_name "
            "FROM customer WHERE ship_to = %s LIMIT 1",
            (ship_to,),
        )
        row = cur.fetchone()
        cur.close(); conn.close()
        return row
    except Exception as e:
        print(f"[claim] _claim_lookup_shop failed: {e}")
        return None

def _claim_save_photos(files, prefix, categories=None):
    """Save uploaded photo files under static/claim_photos/YYYY/MM/.
    Returns a list of public /static URLs.  When categories is given
    (parallel list of strings the size of files), the category is
    embedded in the filename so the viewer can label each photo
    without an extra DB column."""
    saved = []
    if not files: return saved
    now = datetime.now()
    subdir = os.path.join(CLAIM_PHOTO_DIR, f"{now.year:04d}", f"{now.month:02d}")
    os.makedirs(subdir, exist_ok=True)
    ts = now.strftime("%Y%m%d_%H%M%S")
    safe = "".join(c for c in (prefix or "X") if c.isalnum()) or "X"
    cats = list(categories or [])
    for i, f in enumerate(files[:10]):
        if not f or not f.filename:
            continue
        ext = os.path.splitext(f.filename)[1].lower()
        if ext not in (".jpg", ".jpeg", ".png", ".webp", ".heic", ".pdf"):
            continue
        cat = (cats[i] if i < len(cats) else "") or ""
        safe_cat = "".join(c for c in cat if c.isalnum() or c in "-_")[:24]
        suffix   = f"_{safe_cat}" if safe_cat else ""
        fname = f"{ts}_{safe}_{i}{suffix}{ext}"
        path  = os.path.join(subdir, fname)
        f.save(path)
        saved.append(f"/static/claim_photos/{now.year:04d}/{now.month:02d}/{fname}")
    return saved

def _claim_notify_new(claim_id, shop, contact_name, claim_type, description):
    """Email BDE/SM/leadership when a new claim is submitted."""
    state = shop.get("bde_state") if shop else None
    state = STATE_REMAP.get(state, state) if state else None
    sm_email = STATE_MANAGER_EMAIL.get(state) if state else None
    bde_email = _lookup_bde_email(shop.get("salesman_name") if shop else "")
    to_list = list(ALWAYS_TO)
    if sm_email and sm_email.lower() not in [x.lower() for x in to_list]:
        to_list.append(sm_email)
    if bde_email and bde_email.lower() not in [x.lower() for x in to_list]:
        to_list.append(bde_email)
    subject = f"[Claim #{claim_id}] {shop.get('ship_to_name') if shop else ''} — {claim_type or 'New claim'}"
    link = f"{DASHBOARD_URL.rstrip('/')}/claims#{claim_id}"
    safe_desc = _esc_html((description or "")[:600])
    html = (
        f"<p>A new claim was submitted.</p>"
        f"<table style='border-collapse:collapse;font-family:Arial,sans-serif;font-size:13px;'>"
        f"<tr><td><b>Shop</b></td><td>{_esc_html(shop.get('ship_to_name') if shop else '')} "
        f"({_esc_html(shop.get('ship_to') if shop else '')})</td></tr>"
        f"<tr><td><b>Customer</b></td><td>{_esc_html(shop.get('sold_to_name') if shop else '')}</td></tr>"
        f"<tr><td><b>Contact</b></td><td>{_esc_html(contact_name or '')}</td></tr>"
        f"<tr><td><b>Type</b></td><td>{_esc_html(claim_type or '')}</td></tr>"
        f"<tr><td><b>Description</b></td><td><pre style='white-space:pre-wrap;'>{safe_desc}</pre></td></tr>"
        f"</table>"
        f"<p><a href='{link}'>Open claim #{claim_id} in the dashboard →</a></p>"
    )
    _send_mail_async(to_list, [], subject, html)

def _claim_notify_reply(claim_id, author_type, author_name, text):
    """Notify the other side when a new message lands on a thread."""
    try:
        conn = get_connection(); cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT ship_to, ship_to_name, sold_to_name, contact_email, "
            "       contact_name, assigned_to "
            "FROM claim_submission WHERE id = %s", (claim_id,))
        row = cur.fetchone()
        cur.close(); conn.close()
        if not row: return
        link = f"{DASHBOARD_URL.rstrip('/')}/claims#{claim_id}"
        cust_link = f"{CLAIM_PORTAL_URL.rstrip('/')}/claim/{row['ship_to']}"
        safe_text = _esc_html((text or "")[:600])
        if author_type == "customer":
            shop = _claim_lookup_shop(row["ship_to"])
            state = shop.get("bde_state") if shop else None
            state = STATE_REMAP.get(state, state) if state else None
            sm_email  = STATE_MANAGER_EMAIL.get(state) if state else None
            bde_email = _lookup_bde_email(shop.get("salesman_name") if shop else "")
            to_list = list(ALWAYS_TO)
            if sm_email and sm_email.lower() not in [x.lower() for x in to_list]:
                to_list.append(sm_email)
            if bde_email and bde_email.lower() not in [x.lower() for x in to_list]:
                to_list.append(bde_email)
            subject = f"[Claim #{claim_id}] Customer replied — {row['ship_to_name']}"
            html = (f"<p><b>{_esc_html(author_name or row['contact_name'] or 'Customer')}</b> "
                    f"replied on claim #{claim_id} ({_esc_html(row['ship_to_name'])}):</p>"
                    f"<pre style='white-space:pre-wrap;font-family:Arial,sans-serif;'>{safe_text}</pre>"
                    f"<p><a href='{link}'>Open claim →</a></p>")
            _send_mail_async(to_list, [], subject, html)
        else:
            if row.get("contact_email"):
                subject = f"[Hankook Claim #{claim_id}] Reply from our team"
                html = (f"<p>Our team replied to your claim:</p>"
                        f"<pre style='white-space:pre-wrap;font-family:Arial,sans-serif;'>{safe_text}</pre>"
                        f"<p><a href='{cust_link}'>View and respond →</a></p>")
                _send_mail_async([row["contact_email"]], [], subject, html)
    except Exception as e:
        print(f"[claim] _claim_notify_reply failed: {e}")

# ── Customer-facing routes ───────────────────────────────────────────
@app.get("/claim/<ship_to>")
def claim_page(ship_to):
    return send_from_directory("static", "claim.html")

# Short-URL alias so QR codes (and the printable cards) can show
# claim.hankooktyre.com.au/735486 instead of …/claim/735486.  Restricted
# to ship-to-shaped paths so unrelated future routes (/meeting, /map,
# etc.) keep priority and random typos return 404 instead of leaking
# the claim page.
import re as _re
_SHIP_TO_SHORT_RE = _re.compile(r'^[A-Za-z0-9_-]{4,32}$')
_SHIP_TO_RESERVED = {
    "api", "static", "claim", "claims", "meeting", "map", "stock",
    "rebate", "price", "potentials", "favicon.ico", "robots.txt",
    "sitemap.xml", "health", "ping", "index.html",
}
@app.get("/<ship_to>")
def claim_short_url(ship_to):
    s = (ship_to or "").strip()
    if s.lower() in _SHIP_TO_RESERVED or not _SHIP_TO_SHORT_RE.match(s):
        from flask import abort
        abort(404)
    return send_from_directory("static", "claim.html")

@app.get("/api/claim/shop/<ship_to>")
def claim_shop_info(ship_to):
    shop = _claim_lookup_shop(ship_to)
    if not shop:
        return jsonify({"error": "Shop code not recognised", "ship_to": ship_to}), 404
    return jsonify({
        "ship_to":      shop["ship_to"],
        "ship_to_name": shop.get("ship_to_name") or "",
        "sold_to":      shop.get("sold_to") or "",
        "sold_to_name": shop.get("sold_to_name") or "",
        # Customer-facing base URL so the printable QR card embeds the
        # configured portal domain instead of whatever host happened to
        # serve the admin /claims page.
        "portal_url":   CLAIM_PORTAL_URL.rstrip("/"),
    })

@app.get("/api/claim/by_ship/<ship_to>")
def claim_list_for_shop(ship_to):
    """Customer-side: list this shop's prior claims + threads so they
    can check on status and continue any open conversation."""
    try:
        conn = get_connection(); cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT id, claim_type, status, created_at, resolved_at,
                   contact_name, description
            FROM claim_submission
            WHERE ship_to = %s
            ORDER BY created_at DESC
            LIMIT 50
        """, (ship_to,))
        claims = cur.fetchall()
        ids = [c["id"] for c in claims]
        threads = {}
        if ids:
            ph = ",".join(["%s"] * len(ids))
            cur.execute(f"""
                SELECT id, claim_id, author_type, author_name, category,
                       text, photo_paths, created_at
                FROM claim_message
                WHERE claim_id IN ({ph})
                ORDER BY created_at ASC, id ASC
            """, tuple(ids))
            for m in cur.fetchall():
                if m.get("created_at"):
                    m["created_at"] = m["created_at"].strftime("%Y-%m-%d %H:%M")
                m["photo_paths"] = m["photo_paths"].split(",") if m["photo_paths"] else []
                threads.setdefault(m["claim_id"], []).append(m)
        for c in claims:
            if c.get("created_at"):
                c["created_at"] = c["created_at"].strftime("%Y-%m-%d %H:%M")
            if c.get("resolved_at"):
                c["resolved_at"] = c["resolved_at"].strftime("%Y-%m-%d %H:%M")
            c["thread"] = threads.get(c["id"], [])
        cur.close(); conn.close()
        return jsonify({"claims": claims})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.post("/api/claim/by_ship/<ship_to>")
def claim_create(ship_to):
    # Honeypot — bots fill every field, real users don't see this one.
    if (request.form.get("website") or "").strip():
        return jsonify({"error": "spam"}), 400
    shop = _claim_lookup_shop(ship_to)
    if not shop:
        return jsonify({"error": "Shop code not recognised"}), 404
    contact_name  = (request.form.get("contact_name")  or "").strip()[:120]
    contact_phone = (request.form.get("contact_phone") or "").strip()[:40]
    contact_email = (request.form.get("contact_email") or "").strip()[:160]
    claim_type    = (request.form.get("claim_type")    or "").strip()[:40]
    product_size  = (request.form.get("product_size")  or "").strip()[:80]
    description   = (request.form.get("description")   or "").strip()
    if not contact_name or not contact_phone:
        return jsonify({"error": "Name and phone are required"}), 400
    if not description:
        return jsonify({"error": "Please describe the issue"}), 400
    # Only one claim type is currently exposed, so coerce any other
    # value to the default (frontend already restricts the dropdown).
    if claim_type not in CLAIM_TYPES:
        claim_type = CLAIM_TYPES[0]
    # Categories come as a comma-joined string in parallel to the
    # ordered photos files.  Frontend pushes one category per file
    # (multi-file slot like tread_measurement contributes N entries),
    # plus an empty-entry placeholder for empty slots, so the list
    # stays index-aligned with raw_files.
    cats = [c.strip() for c in (request.form.get("photo_categories") or "").split(",")]
    raw_files = request.files.getlist("photos") or []
    # Per-category minimums.  Six single-photo slots, tread measurement
    # requires at least 3 photos (one per groove), and proof of purchase
    # (invoice) is also required.  'others' is intentionally absent —
    # the slot exists for extras but doesn't block submission.
    REQUIRED_MIN = {
        "whole_tyre": 1, "tread": 1, "damaged": 1,
        "dot_code":   1, "dot_cut": 1, "serial_barcode": 1,
        "tread_measurement": 3,
        "invoice": 1,
    }
    got_by_cat = {}
    for i, f in enumerate(raw_files):
        if not (f and f.filename):
            continue
        cat = cats[i] if i < len(cats) else ""
        if cat in REQUIRED_MIN:
            got_by_cat[cat] = got_by_cat.get(cat, 0) + 1
    short = [c for c, need in REQUIRED_MIN.items() if got_by_cat.get(c, 0) < need]
    if short:
        return jsonify({"error":
            "Please attach all required product photos — missing: "
            + ", ".join(c.replace("_", " ") for c in short)}), 400
    photos = _claim_save_photos(raw_files, ship_to, categories=cats)
    total_required = sum(REQUIRED_MIN.values())
    if len(photos) < total_required:
        return jsonify({"error":
            f"Could not save all {total_required} required photos — "
            "please check file types (JPG / PNG / HEIC / PDF) and try again."}), 400
    try:
        conn = get_connection(); cur = conn.cursor()
        cur.execute("""
            INSERT INTO claim_submission
                (ship_to, sold_to, ship_to_name, sold_to_name,
                 contact_name, contact_phone, contact_email,
                 claim_type, product_size, description, photo_paths,
                 status)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'New')
        """, (
            ship_to, shop.get("sold_to") or "",
            shop.get("ship_to_name") or "", shop.get("sold_to_name") or "",
            contact_name, contact_phone, contact_email,
            claim_type, product_size, description,
            ",".join(photos) if photos else None,
        ))
        cid = cur.lastrowid
        # First message: mirror the description so the thread reads as
        # a normal conversation from message #1.
        cur.execute("""
            INSERT INTO claim_message
                (claim_id, author_type, author_name, author_email, text, photo_paths)
            VALUES (%s, 'customer', %s, %s, %s, %s)
        """, (cid, contact_name, contact_email, description,
              ",".join(photos) if photos else None))
        cur.close(); conn.close()
        _claim_notify_new(cid, shop, contact_name, claim_type, description)
        return jsonify({"id": cid, "status": "New"})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.post("/api/claim/<int:cid>/customer_reply")
def claim_customer_reply(cid):
    """Customer-side reply.  Requires the same ship_to in the body so a
    leaked claim id alone can't be used to talk on someone else's thread."""
    if (request.form.get("website") or "").strip():
        return jsonify({"error": "spam"}), 400
    ship_to = (request.form.get("ship_to") or "").strip()
    text    = (request.form.get("text") or "").strip()
    name    = (request.form.get("contact_name") or "").strip()[:120]
    email   = (request.form.get("contact_email") or "").strip()[:160]
    if not ship_to or not text:
        return jsonify({"error": "ship_to and text are required"}), 400
    try:
        conn = get_connection(); cur = conn.cursor(dictionary=True)
        cur.execute("SELECT ship_to FROM claim_submission WHERE id=%s", (cid,))
        row = cur.fetchone()
        if not row or row["ship_to"] != ship_to:
            cur.close(); conn.close()
            return jsonify({"error": "Claim not found for this shop"}), 404
        photos = _claim_save_photos(request.files.getlist("photos"), ship_to)
        cur.execute("""
            INSERT INTO claim_message
                (claim_id, author_type, author_name, author_email, text, photo_paths)
            VALUES (%s, 'customer', %s, %s, %s, %s)
        """, (cid, name, email, text, ",".join(photos) if photos else None))
        cur.close(); conn.close()
        _claim_notify_reply(cid, "customer", name, text)
        return jsonify({"ok": True})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# ── Internal routes ──────────────────────────────────────────────────
@app.get("/claims")
def claims_page():
    return send_from_directory("static", "claims.html")

@app.get("/api/claims")
def claims_list():
    """Internal: list claims with optional filters (status, state, BDE, search)."""
    status = (request.args.get("status") or "").strip()
    state  = (request.args.get("state")  or "").strip().upper()
    bde    = (request.args.get("bde")    or "").strip()
    q      = (request.args.get("q")      or "").strip()
    wh = []
    params = []
    if status:
        wh.append("c.status = %s"); params.append(status)
    if q:
        wh.append("(c.ship_to LIKE %s OR c.ship_to_name LIKE %s "
                  " OR c.sold_to_name LIKE %s OR c.contact_name LIKE %s)")
        like = f"%{q}%"
        params += [like, like, like, like]
    where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""
    try:
        conn = get_connection(); cur = conn.cursor(dictionary=True)
        cur.execute(f"""
            SELECT c.id, c.ship_to, c.ship_to_name, c.sold_to_name,
                   c.contact_name, c.contact_phone, c.contact_email,
                   c.claim_type, c.status, c.assigned_to,
                   c.created_at, c.resolved_at,
                   c.description, c.product_size, c.photo_paths,
                   (SELECT COUNT(*) FROM claim_message m WHERE m.claim_id = c.id) AS msg_count
            FROM claim_submission c
            {where_sql}
            ORDER BY c.created_at DESC
            LIMIT 500
        """, tuple(params))
        rows = cur.fetchall()
        # State + BDE filters apply via the customer master so a single
        # join covers both — narrows the visible claims to those whose
        # shop matches the chosen scope.
        if (state and state in ("NSW","QLD","VIC","WA","SA","TAS")) or bde:
            ships = [r["ship_to"] for r in rows]
            keep = set()
            if ships:
                ph = ",".join(["%s"] * len(ships))
                cur.execute(f"""
                    SELECT DISTINCT ship_to, bde_state, salesman_name
                    FROM customer
                    WHERE ship_to IN ({ph})
                """, tuple(ships))
                cust_rows = cur.fetchall()
                for s in cust_rows:
                    if state:
                        st = STATE_REMAP.get(s.get("bde_state"), s.get("bde_state"))
                        if st != state: continue
                    if bde and (s.get("salesman_name") or "").strip().upper() != bde.upper():
                        continue
                    keep.add(s["ship_to"])
            rows = [r for r in rows if r["ship_to"] in keep]
        for r in rows:
            if r.get("created_at"):
                r["created_at"] = r["created_at"].strftime("%Y-%m-%d %H:%M")
            if r.get("resolved_at"):
                r["resolved_at"] = r["resolved_at"].strftime("%Y-%m-%d %H:%M")
            r["photo_paths"] = r["photo_paths"].split(",") if r["photo_paths"] else []
        # BDE list for the filter dropdown — narrowed by state when set,
        # so the picker can't include BDEs outside the current scope.
        bde_wh = ["salesman_name IS NOT NULL", "TRIM(salesman_name) <> ''"]
        bde_params = []
        if state and state in ("NSW","QLD","VIC","WA","SA","TAS"):
            bde_wh.append("bde_state = %s"); bde_params.append(state)
        cur.execute(f"""
            SELECT DISTINCT TRIM(salesman_name) AS name
            FROM customer
            WHERE {' AND '.join(bde_wh)}
            ORDER BY name
        """, tuple(bde_params))
        bdes = [r["name"] for r in cur.fetchall() if r.get("name")]
        cur.close(); conn.close()
        return jsonify({
            "claims":   rows,
            "statuses": list(CLAIM_STATUSES),
            "bdes":     bdes,
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.get("/api/claim/<int:cid>")
def claim_detail(cid):
    try:
        conn = get_connection(); cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM claim_submission WHERE id=%s", (cid,))
        row = cur.fetchone()
        if not row:
            cur.close(); conn.close()
            return jsonify({"error": "Claim not found"}), 404
        cur.execute("""
            SELECT id, claim_id, author_type, author_name, author_email,
                   category, text, photo_paths, created_at
            FROM claim_message
            WHERE claim_id = %s
            ORDER BY created_at ASC, id ASC
        """, (cid,))
        thread = cur.fetchall()
        for m in thread:
            if m.get("created_at"):
                m["created_at"] = m["created_at"].strftime("%Y-%m-%d %H:%M")
            m["photo_paths"] = m["photo_paths"].split(",") if m["photo_paths"] else []
        if row.get("created_at"):
            row["created_at"] = row["created_at"].strftime("%Y-%m-%d %H:%M")
        if row.get("resolved_at"):
            row["resolved_at"] = row["resolved_at"].strftime("%Y-%m-%d %H:%M")
        row["photo_paths"] = row["photo_paths"].split(",") if row["photo_paths"] else []
        cur.close(); conn.close()
        return jsonify({"claim": row, "thread": thread})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.patch("/api/claim/<int:cid>")
def claim_update(cid):
    body = request.get_json(silent=True) or {}
    fields = {}
    if "status" in body:
        s = (body["status"] or "").strip()
        if s and s not in CLAIM_STATUSES:
            return jsonify({"error": "invalid status"}), 400
        fields["status"] = s
        if s in ("Completed", "Resolved"):
            fields["resolved_at"] = datetime.now()
        else:
            fields["resolved_at"] = None
    if "assigned_to" in body:
        fields["assigned_to"] = (body["assigned_to"] or "").strip()[:120]
    if not fields:
        return jsonify({"error": "Nothing to update"}), 400
    try:
        conn = get_connection(); cur = conn.cursor()
        sets = ", ".join([f"{k} = %s" for k in fields])
        params = list(fields.values()) + [cid]
        cur.execute(f"UPDATE claim_submission SET {sets} WHERE id = %s", params)
        cur.close(); conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.post("/api/claim/<int:cid>/internal_reply")
def claim_internal_reply(cid):
    email = _bde_from_request()
    me    = _EMAIL_TO_DIR.get(email.lower()) if email else None
    name  = (me[0] if me else "") or "Hankook team"
    text  = ""
    category = ""
    is_json = request.is_json
    if is_json:
        body = request.get_json(silent=True) or {}
        text = (body.get("text") or "").strip()
        category = (body.get("category") or "").strip()[:40]
    else:
        text = (request.form.get("text") or "").strip()
        category = (request.form.get("category") or "").strip()[:40]
    if not text:
        return jsonify({"error": "text is required"}), 400
    photos = _claim_save_photos(request.files.getlist("photos"), f"int{cid}") if not is_json else []
    try:
        conn = get_connection(); cur = conn.cursor()
        cur.execute("""
            INSERT INTO claim_message
                (claim_id, author_type, author_name, author_email,
                 category, text, photo_paths)
            VALUES (%s, 'internal', %s, %s, %s, %s, %s)
        """, (cid, name, email or "", category, text,
              ",".join(photos) if photos else None))
        # Auto-transition: a claim sitting in 'New' jumps to 'Reviewing'
        # the moment someone on our side replies, so the workflow
        # statuses match what's actually happening without the manager
        # having to flip the dropdown manually.
        cur.execute(
            "UPDATE claim_submission SET status = 'Reviewing' "
            "WHERE id = %s AND status = 'New'", (cid,))
        cur.close(); conn.close()
        _claim_notify_reply(cid, "internal", name, text)
        return jsonify({"ok": True})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
# ── QR code generation (sticker per shop + bulk grid) ──────────────
@app.get("/api/claim/qr/<ship_to>")
def claim_qr_png(ship_to):
    """Render a PNG QR code that encodes /claim/<ship_to>.  The image
    is what the printable card/grid pages embed via <img src>."""
    try:
        import qrcode  # type: ignore
    except ImportError:
        return ("qrcode package not installed — pip install qrcode[pil]", 500)
    from io import BytesIO
    from flask import send_file
    url = f"{CLAIM_PORTAL_URL.rstrip('/')}/claim/{ship_to}"
    qr  = qrcode.QRCode(
        version=None,
        error_correction=qrcode.constants.ERROR_CORRECT_M,
        box_size=10, border=2,
    )
    qr.add_data(url); qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buf = BytesIO(); img.save(buf, format="PNG"); buf.seek(0)
    resp = send_file(buf, mimetype="image/png",
                     download_name=f"claim_qr_{ship_to}.png")
    resp.headers["Cache-Control"] = "public, max-age=3600"
    return resp

@app.get("/claims/qr/<ship_to>")
def claim_qr_card(ship_to):
    """Single-shop printable A6 card."""
    return send_from_directory("static", "claim_qr_card.html")

@app.get("/claims/qr_bulk")
def claim_qr_bulk_page():
    """Bulk A4 grid of QR cards.  Frontend reads ?ships=A,B,C&state=NSW
    and renders one card per shop on the page."""
    return send_from_directory("static", "claim_qr_bulk.html")

@app.get("/api/claim/qr_shops")
def claim_qr_shops():
    """Helper for the bulk page — returns ship_tos matching the filters.
    Defaults to active shops (any in customer master)."""
    state = (request.args.get("state") or "").strip().upper()
    bde   = (request.args.get("bde")   or "").strip()
    limit = min(int(request.args.get("limit") or "300"), 1000)
    wh = ["ship_to IS NOT NULL", "TRIM(ship_to) <> ''"]
    params = []
    if state:
        # Map our region buttons back to the underlying bde_state values
        # so SA/TAS/ACT shops still print under the manager who serves them.
        wh.append("bde_state = %s")
        params.append(state)
    if bde:
        wh.append("UPPER(TRIM(salesman_name)) = UPPER(TRIM(%s))")
        params.append(bde)
    where_sql = "WHERE " + " AND ".join(wh)
    try:
        conn = get_connection(); cur = conn.cursor(dictionary=True)
        cur.execute(f"""
            SELECT ship_to,
                   MIN(NULLIF(TRIM(ship_to_name),'')) AS ship_to_name,
                   MIN(NULLIF(TRIM(sold_to_name),'')) AS sold_to_name,
                   MIN(NULLIF(TRIM(bde_state),''))   AS bde_state,
                   MIN(NULLIF(TRIM(salesman_name),'')) AS salesman_name
            FROM customer
            {where_sql}
            GROUP BY ship_to
            ORDER BY ship_to_name, ship_to
            LIMIT {limit}
        """, tuple(params))
        rows = cur.fetchall()
        # Also surface the available BDE list (for the bulk page dropdown)
        # narrowed by state if one is selected — keeps the picker scoped.
        bde_wh = ["salesman_name IS NOT NULL", "TRIM(salesman_name) <> ''"]
        bde_params = []
        if state:
            bde_wh.append("bde_state = %s"); bde_params.append(state)
        cur.execute(f"""
            SELECT DISTINCT TRIM(salesman_name) AS name
            FROM customer
            WHERE {' AND '.join(bde_wh)}
            ORDER BY name
        """, tuple(bde_params))
        bdes = [r["name"] for r in cur.fetchall() if r.get("name")]
        cur.close(); conn.close()
        return jsonify({
            "shops":    rows,
            "bdes":     bdes,
            "base_url": CLAIM_PORTAL_URL.rstrip("/"),
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
# === CLAIM FEATURE END ===


# ─────────────────────────────────────────────────────────────────────
# POTENTIAL CUSTOMER feature — BDEs add prospect shops they're chasing
# so the planner / map can track excavation work alongside the real
# customer master.  Wrapped in START/END markers for easy revert.
# ─────────────────────────────────────────────────────────────────────
# === POTENTIAL CUSTOMER FEATURE START ===
POTENTIAL_STATUSES = ("Lead", "Visited", "Negotiating", "Converted", "Lost")

def _ensure_potential_table():
    try:
        conn = get_connection(); cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS potential_customer (
                id             INT AUTO_INCREMENT PRIMARY KEY,
                name           VARCHAR(160) NOT NULL,
                sold_to_name   VARCHAR(160) NOT NULL DEFAULT '',
                address        VARCHAR(255) NOT NULL DEFAULT '',
                phone          VARCHAR(40)  NOT NULL DEFAULT '',
                contact_person VARCHAR(120) NOT NULL DEFAULT '',
                state          VARCHAR(8)   NOT NULL DEFAULT '',
                bde_name       VARCHAR(120) NOT NULL DEFAULT '',
                bde_email      VARCHAR(160) NOT NULL DEFAULT '',
                lat            DECIMAL(10,6) NULL,
                lon            DECIMAL(11,6) NULL,
                status         VARCHAR(40)  NOT NULL DEFAULT 'Lead',
                notes          TEXT,
                created_at     TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at     TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
                               ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_state (state),
                INDEX idx_bde   (bde_name)
            )
        """)
        # Idempotent migration for rows created before sold_to_name existed.
        try:
            cur.execute("ALTER TABLE potential_customer "
                        "ADD COLUMN sold_to_name VARCHAR(160) NOT NULL DEFAULT '' "
                        "AFTER name")
        except Exception:
            pass
        cur.close(); conn.close()
    except Exception as e:
        print(f"[potential] schema init failed: {e}")
_ensure_potential_table()

def _potential_to_pseudo_ship(pid):
    """Pseudo ship_to code used in meeting_plan / meeting_log so the
    rest of the system can carry potential customers without a real
    customer-master row.  Format chosen to be obviously non-numeric."""
    return f"POT-{int(pid)}"

def _parse_potential_id(ship_to):
    """Reverse of the above.  Returns int id or None for real shops."""
    s = (ship_to or "").strip().upper()
    if not s.startswith("POT-"): return None
    try: return int(s[4:])
    except ValueError: return None

@app.get("/api/potential_customers")
def potential_customers_list():
    """List potential customers, optionally filtered by state / bde."""
    state = (request.args.get("state") or "").strip().upper()
    bde   = (request.args.get("bde")   or "").strip()
    wh, params = [], []
    if state and state in ("NSW","QLD","VIC","WA","SA","TAS"):
        wh.append("state = %s"); params.append(state)
    if bde:
        wh.append("UPPER(TRIM(bde_name)) = %s")
        params.append(bde.upper())
    where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""
    try:
        conn = get_connection(); cur = conn.cursor(dictionary=True)
        cur.execute(f"""
            SELECT id, name, sold_to_name,
                   address, phone, contact_person,
                   state, bde_name, bde_email, lat, lon, status, notes,
                   created_at, updated_at
            FROM potential_customer
            {where_sql}
            ORDER BY name ASC, id ASC
        """, tuple(params))
        rows = cur.fetchall()
        for r in rows:
            for k in ("created_at", "updated_at"):
                if r.get(k):
                    r[k] = r[k].strftime("%Y-%m-%d %H:%M")
            r["lat"] = float(r["lat"]) if r["lat"] is not None else None
            r["lon"] = float(r["lon"]) if r["lon"] is not None else None
            r["pseudo_ship_to"] = _potential_to_pseudo_ship(r["id"])
        cur.close(); conn.close()
        return jsonify({"potentials": rows, "statuses": list(POTENTIAL_STATUSES)})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.post("/api/potential_customer")
def potential_customer_create():
    body = request.get_json(silent=True) or request.form
    name = (body.get("name") or "").strip()[:160]
    if not name:
        return jsonify({"error": "ship-to name is required"}), 400
    email = _bde_from_request()
    me    = _EMAIL_TO_DIR.get(email.lower()) if email else None
    sold_to_name = (body.get("sold_to_name") or "").strip()[:160]
    bde_name  = (body.get("bde_name") or (me[0] if me else "")).strip()[:120]
    state     = (body.get("state")    or (me[1] if me else "")).strip().upper()[:8]
    address   = (body.get("address")  or "").strip()[:255]
    phone     = (body.get("phone")    or "").strip()[:40]
    contact   = (body.get("contact_person") or "").strip()[:120]
    notes     = (body.get("notes")    or "").strip()
    status    = (body.get("status")   or "Lead").strip()
    if status not in POTENTIAL_STATUSES: status = "Lead"
    def _flt(v):
        try: return float(v)
        except (TypeError, ValueError): return None
    lat = _flt(body.get("lat"))
    lon = _flt(body.get("lon"))
    try:
        conn = get_connection(); cur = conn.cursor()
        cur.execute("""
            INSERT INTO potential_customer
                (name, sold_to_name, address, phone, contact_person,
                 state, bde_name, bde_email, lat, lon, status, notes)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (name, sold_to_name, address, phone, contact,
              state, bde_name, email or "",
              lat, lon, status, notes))
        pid = cur.lastrowid
        cur.close(); conn.close()
        return jsonify({"id": pid, "pseudo_ship_to": _potential_to_pseudo_ship(pid)})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.patch("/api/potential_customer/<int:pid>")
def potential_customer_update(pid):
    body = request.get_json(silent=True) or {}
    allowed = ("name", "sold_to_name", "address", "phone", "contact_person",
               "state", "bde_name", "status", "notes", "lat", "lon")
    sets, params = [], []
    for k in allowed:
        if k in body:
            v = body[k]
            if k in ("lat", "lon"):
                try: v = float(v) if v not in (None, "") else None
                except (TypeError, ValueError): v = None
            elif k == "status":
                v = (v or "").strip()
                if v not in POTENTIAL_STATUSES:
                    return jsonify({"error": "invalid status"}), 400
            elif k == "state":
                v = (v or "").strip().upper()[:8]
            else:
                v = (v or "").strip()
                if k == "name" and not v:
                    return jsonify({"error": "name cannot be empty"}), 400
            sets.append(f"{k} = %s"); params.append(v)
    if not sets:
        return jsonify({"error": "nothing to update"}), 400
    params.append(pid)
    try:
        conn = get_connection(); cur = conn.cursor()
        cur.execute(f"UPDATE potential_customer SET {', '.join(sets)} "
                    f"WHERE id = %s", params)
        cur.close(); conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.delete("/api/potential_customer/<int:pid>")
def potential_customer_delete(pid):
    try:
        conn = get_connection(); cur = conn.cursor()
        cur.execute("DELETE FROM potential_customer WHERE id = %s", (pid,))
        cur.close(); conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.get("/api/potential_customers/stats")
def potential_customers_stats():
    """Counts grouped by State and by Status for the management page header."""
    try:
        conn = get_connection(); cur = conn.cursor(dictionary=True)
        cur.execute("SELECT COUNT(*) AS n FROM potential_customer")
        total = (cur.fetchone() or {}).get("n", 0)
        cur.execute("""
            SELECT state, COUNT(*) AS n
            FROM potential_customer
            GROUP BY state
            ORDER BY state
        """)
        by_state = {r["state"] or "—": r["n"] for r in cur.fetchall()}
        cur.execute("""
            SELECT status, COUNT(*) AS n
            FROM potential_customer
            GROUP BY status
            ORDER BY status
        """)
        by_status = {r["status"] or "Lead": r["n"] for r in cur.fetchall()}
        cur.execute("""
            SELECT bde_name, COUNT(*) AS n
            FROM potential_customer
            GROUP BY bde_name
            ORDER BY n DESC, bde_name ASC
            LIMIT 50
        """)
        by_bde = [{"bde_name": r["bde_name"] or "—", "n": r["n"]} for r in cur.fetchall()]
        cur.close(); conn.close()
        return jsonify({
            "total":    total,
            "by_state": by_state,
            "by_status": by_status,
            "by_bde":   by_bde,
            "statuses": list(POTENTIAL_STATUSES),
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
# === POTENTIAL CUSTOMER FEATURE END ===


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
