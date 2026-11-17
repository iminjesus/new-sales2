#!/usr/bin/env python3
"""
rma_experimental.py -- standalone Flask app for the RMA (Return
Material Authorization) proof of concept.

Why a separate app: CST team spends 10-15 minutes per case filling out
the Hankook RMA form by hand.  Most of the data (customer name/address,
tyre size/pattern, original DO, billing doc) is already known once the
billing document or DO number is provided.  We want to show CST a
prototype of an auto-fill flow before touching the live H-Force app
(app.py).  Keeping it separate means:
  * live SPRF users are never disturbed
  * breakage here is isolated from production
  * once CST signs off, selected pieces move into app.py

Contract with the ops environment:
  * Shares MySQL with app.py (reads customer, writes new *_test tables)
  * Reads DB creds from the same env vars (DB_HOST / DB_PORT /
    DB_USER / DB_PASS / DB_NAME)
  * Runs on a different port so it can live alongside app.py.
    Defaults to 5555; override with RMA_PORT env var.

Launch:
  python rma_experimental.py
  # open http://localhost:5555/rma  in a browser

Endpoints (all under /rma/*, no collision with app.py):
  GET  /rma                      -- the single-page UI
  POST /rma/api/upload_billing   -- accept a sales-CSV upload, insert into
                                    so_do_billing_test (dedup on
                                    billing_doc+material+billing_date)
  GET  /rma/api/lookup           -- ?billing_doc=... OR ?do_no=...
                                    returns an auto-fill payload
  POST /rma/api/save             -- persist an RMA request (header + lines)
  GET  /rma/api/list             -- recent RMA requests
  GET  /rma/api/get/<id>         -- one RMA with its lines, for the
                                    print view
"""
from __future__ import annotations

import csv
import io
import json
import os
import re
import sys
from datetime import date, datetime

try:
    import mysql.connector
except Exception as e:
    print(f"[fatal] mysql.connector import failed: {e}", file=sys.stderr)
    sys.exit(1)

try:
    from flask import Flask, jsonify, request, Response
except Exception as e:
    print(f"[fatal] flask import failed: {e}", file=sys.stderr)
    sys.exit(1)


# ---------- config ----------
DB_CFG = {
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASS", ""),
    "database": os.getenv("DB_NAME", "my_new_database"),
    "autocommit": False,
    "use_pure": True,
}
PORT = int(os.getenv("RMA_PORT", "5555"))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


# ---------- db ----------
def get_conn():
    return mysql.connector.connect(**DB_CFG)


def ensure_schema():
    """Create the three experimental tables if they don't exist.
    Idempotent -- safe to call on every app start."""
    ddl = [
        # Per-line billing history -- same shape as a ZSDR24030 row,
        # enriched with full billing date + doc + DO.  Keyed on
        # (billing_doc, material) so repeat loads of the same month
        # overwrite instead of duplicate.
        """
        CREATE TABLE IF NOT EXISTS so_do_billing_test (
            id            BIGINT AUTO_INCREMENT PRIMARY KEY,
            billing_date  DATE,
            billing_doc   VARCHAR(20)  NOT NULL DEFAULT '',
            do_no         VARCHAR(20)  NOT NULL DEFAULT '',
            so_type       VARCHAR(10)  NOT NULL DEFAULT '',
            sold_to       VARCHAR(20)  NOT NULL DEFAULT '',
            ship_to       VARCHAR(20)  NOT NULL DEFAULT '',
            material      VARCHAR(20)  NOT NULL DEFAULT '',
            qty           DECIMAL(12,2) NOT NULL DEFAULT 0,
            amt           DECIMAL(18,4) NOT NULL DEFAULT 0,
            brand         VARCHAR(10)  NOT NULL DEFAULT '',
            state         VARCHAR(10)  NOT NULL DEFAULT '',
            bde           VARCHAR(50)  NOT NULL DEFAULT '',
            cogs          DECIMAL(18,4) NOT NULL DEFAULT 0,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uk_bill_mat (billing_doc, material),
            INDEX idx_bill_date (billing_date),
            INDEX idx_do        (do_no),
            INDEX idx_soldto    (sold_to),
            INDEX idx_shipto    (ship_to),
            INDEX idx_material  (material)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        # RMA request header -- one row per form.
        """
        CREATE TABLE IF NOT EXISTS rma_request_test (
            id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
            rma_number          VARCHAR(20)  NOT NULL DEFAULT '',
            request_date        DATE,
            rma_type            VARCHAR(40)  NOT NULL DEFAULT '',
            requestor_name      VARCHAR(100) NOT NULL DEFAULT '',
            requestor_email     VARCHAR(100) NOT NULL DEFAULT '',
            customer_name       VARCHAR(200) NOT NULL DEFAULT '',
            sold_to             VARCHAR(20)  NOT NULL DEFAULT '',
            ship_to             VARCHAR(20)  NOT NULL DEFAULT '',
            customer_address    VARCHAR(500) NOT NULL DEFAULT '',
            customer_state      VARCHAR(10)  NOT NULL DEFAULT '',
            customer_postcode   VARCHAR(10)  NOT NULL DEFAULT '',
            reason_code         VARCHAR(50)  NOT NULL DEFAULT '',
            condition_new       TINYINT      NOT NULL DEFAULT 0,
            condition_fitted    TINYINT      NOT NULL DEFAULT 0,
            condition_dirty     TINYINT      NOT NULL DEFAULT 0,
            condition_label_on  TINYINT      NOT NULL DEFAULT 1,
            approval_route      VARCHAR(80)  NOT NULL DEFAULT '',
            special_instructions TEXT,
            status              VARCHAR(20)  NOT NULL DEFAULT 'draft',
            created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_rma_number (rma_number),
            INDEX idx_soldto     (sold_to),
            INDEX idx_shipto     (ship_to),
            INDEX idx_status     (status)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
        # Per-line material details on an RMA.
        """
        CREATE TABLE IF NOT EXISTS rma_request_line_test (
            id            BIGINT AUTO_INCREMENT PRIMARY KEY,
            rma_id        BIGINT NOT NULL,
            billing_doc   VARCHAR(20)  NOT NULL DEFAULT '',
            do_no         VARCHAR(20)  NOT NULL DEFAULT '',
            material      VARCHAR(20)  NOT NULL DEFAULT '',
            description   VARCHAR(255) NOT NULL DEFAULT '',
            tyre_size     VARCHAR(50)  NOT NULL DEFAULT '',
            pattern       VARCHAR(50)  NOT NULL DEFAULT '',
            qty           DECIMAL(12,2) NOT NULL DEFAULT 0,
            billing_date  DATE,
            amount        DECIMAL(18,4) NOT NULL DEFAULT 0,
            INDEX idx_rma (rma_id),
            CONSTRAINT fk_rma FOREIGN KEY (rma_id)
                REFERENCES rma_request_test(id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """,
    ]
    conn = get_conn()
    cur = conn.cursor()
    try:
        for stmt in ddl:
            cur.execute(stmt)
        conn.commit()
    finally:
        cur.close(); conn.close()


# ---------- helpers ----------
def _norm_key(s):
    """Column-header normaliser matching load_csv_to_mysql.sanitize_col."""
    s = (s or "").strip().strip('"').strip("'").lower()
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s


# Expected billing CSV columns (sanitized header -> our table column).
# Mirrors load_csv_to_mysql.SALES_HEADER_MAP plus two extra columns we
# need (billing_doc, do_no, full billing_date).
BILLING_HEADER_MAP = {
    # Full billing date
    "billing_date": "billing_date",
    "billng_date":  "billing_date",
    "fkdat":        "billing_date",
    "billing_date_fkdat": "billing_date",
    # Billing doc / invoice number
    "billing_doc":  "billing_doc",
    "billing_document": "billing_doc",
    "invoice":      "billing_doc",
    "invoice_no":   "billing_doc",
    "vbeln":        "billing_doc",
    # Original delivery note / DO
    "do":           "do_no",
    "do_no":        "do_no",
    "delivery":     "do_no",
    "delivery_no":  "do_no",
    "vgbel":        "do_no",
    # the rest -- same as load_csv_to_mysql
    "s_o_type":     "so_type",
    "so_type":      "so_type",
    "auart":        "so_type",
    "order_type":   "so_type",
    "sold_to":          "sold_to",
    "sold_to_party":    "sold_to",
    "kunag":            "sold_to",
    "ship_to":          "ship_to",
    "ship_to_party":    "ship_to",
    "kunwe":            "ship_to",
    "material":         "material",
    "matnr":            "material",
    "brand":            "brand",
    "bill_qty_in_sku":  "qty",
    "bill_qty":         "qty",
    "fkimg":            "qty",
    "net_value":        "amt",
    "netwr":            "amt",
    "state":            "state",
    "sales_district":   "state",
    "bzirk":            "state",
    "bde":              "bde",
    "salesperson":      "bde",
    "vkgrp":            "bde",
    "cogs":             "cogs",
}


def _parse_billing_date(val):
    """Accept DD.MM.YYYY, YYYY-MM-DD, DD/MM/YYYY, plus openpyxl-exported
    'YYYY-MM-DD HH:MM:SS'.  Returns None on any parse failure."""
    s = (val or "").strip()
    if not s:
        return None
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S",
                "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s[:19] if " " in s else s, fmt).date()
        except Exception:
            continue
    return None


def _parse_num(val):
    s = (val or "").strip().replace(",", "").replace('"', "")
    if not s:
        return 0
    # SAP export uses trailing minus; also parse negatives.
    neg = False
    if s.endswith("-"):
        neg = True
        s = s[:-1].strip()
    try:
        n = float(s)
        return -n if neg else n
    except Exception:
        return 0


# ---------- app ----------
app = Flask(__name__)


@app.get("/rma")
@app.get("/rma/")
def page():
    html_path = os.path.join(BASE_DIR, "static", "rma_experimental.html")
    try:
        with open(html_path, "r", encoding="utf-8") as f:
            return Response(f.read(), mimetype="text/html")
    except Exception as e:
        return Response(
            f"<h1>rma_experimental: missing UI file</h1><pre>{e}</pre>",
            status=500, mimetype="text/html")


@app.post("/rma/api/upload_billing")
def upload_billing():
    """Accept a CSV upload, dedup rows into so_do_billing_test.
    Returns counts: inserted / updated / skipped / total."""
    f = request.files.get("file")
    if not f:
        return jsonify({"error": "no file posted (field name must be 'file')"}), 400
    try:
        raw = f.read().decode("utf-8-sig", errors="replace")
    except Exception as e:
        return jsonify({"error": f"could not read file: {e}"}), 400
    reader = csv.reader(io.StringIO(raw))
    try:
        header = next(reader)
    except StopIteration:
        return jsonify({"error": "empty file"}), 400
    norm_header = [_norm_key(h) for h in header]
    # Map CSV column index -> our DB column name.
    col_map = {}
    for i, h in enumerate(norm_header):
        tgt = BILLING_HEADER_MAP.get(h)
        if tgt and tgt not in col_map.values():
            col_map[i] = tgt

    missing = {"material", "billing_doc"} - set(col_map.values())
    if missing:
        return jsonify({
            "error": f"missing required column(s): {sorted(missing)}",
            "header_seen": norm_header,
            "hint": "Expect ZSDR24030 output with a Billing Doc / Vbeln column. "
                    "Add an alias to BILLING_HEADER_MAP if your variant uses "
                    "a different header.",
        }), 400

    rows = []
    for raw_row in reader:
        if not raw_row or all((c or "").strip() == "" for c in raw_row):
            continue
        row = {"billing_date": None, "billing_doc": "", "do_no": "",
               "so_type": "", "sold_to": "", "ship_to": "",
               "material": "", "qty": 0, "amt": 0, "brand": "",
               "state": "", "bde": "", "cogs": 0}
        for i, tgt in col_map.items():
            if i >= len(raw_row):
                continue
            v = raw_row[i]
            if tgt == "billing_date":
                row[tgt] = _parse_billing_date(v)
            elif tgt in ("qty", "amt", "cogs"):
                row[tgt] = _parse_num(v)
            else:
                row[tgt] = (v or "").strip()
        if not row["billing_doc"] or not row["material"]:
            continue
        rows.append(row)

    if not rows:
        return jsonify({"error": "no usable rows after parsing",
                        "header_seen": norm_header}), 400

    conn = get_conn()
    cur = conn.cursor()
    inserted = updated = 0
    try:
        sql = ("INSERT INTO so_do_billing_test "
               "(billing_date, billing_doc, do_no, so_type, sold_to, ship_to, "
               " material, qty, amt, brand, state, bde, cogs) "
               "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
               "ON DUPLICATE KEY UPDATE "
               "  billing_date = VALUES(billing_date), "
               "  do_no        = VALUES(do_no), "
               "  so_type      = VALUES(so_type), "
               "  sold_to      = VALUES(sold_to), "
               "  ship_to      = VALUES(ship_to), "
               "  qty          = VALUES(qty), "
               "  amt          = VALUES(amt), "
               "  brand        = VALUES(brand), "
               "  state        = VALUES(state), "
               "  bde          = VALUES(bde), "
               "  cogs         = VALUES(cogs)")
        for r in rows:
            cur.execute(sql, (
                r["billing_date"], r["billing_doc"], r["do_no"], r["so_type"],
                r["sold_to"], r["ship_to"], r["material"],
                r["qty"], r["amt"], r["brand"], r["state"], r["bde"], r["cogs"],
            ))
            # cursor.rowcount: 1 = inserted, 2 = updated (MySQL quirk)
            if cur.rowcount == 1:
                inserted += 1
            elif cur.rowcount == 2:
                updated += 1
        conn.commit()
    finally:
        cur.close(); conn.close()

    return jsonify({
        "ok": True,
        "total_rows_in_file": sum(1 for _ in [None]) + len(rows) + 0,  # just the parsed count
        "parsed_rows": len(rows),
        "inserted": inserted,
        "updated": updated,
    })


@app.get("/rma/api/lookup")
def lookup():
    """Return the auto-fill payload for a given billing doc OR DO number.
    Response:
      {header: {... customer fields ...},
       lines : [ {material, description, qty, ...}, ... ]}
    description / tyre_size / pattern are parsed from material tables
    when available (falls back to blank)."""
    billing_doc = (request.args.get("billing_doc") or "").strip()
    do_no       = (request.args.get("do_no") or "").strip()
    if not billing_doc and not do_no:
        return jsonify({"error": "pass billing_doc or do_no"}), 400

    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    try:
        if billing_doc:
            cur.execute(
                "SELECT * FROM so_do_billing_test WHERE billing_doc=%s "
                "ORDER BY material",
                (billing_doc,))
        else:
            cur.execute(
                "SELECT * FROM so_do_billing_test WHERE do_no=%s "
                "ORDER BY material",
                (do_no,))
        billing_rows = cur.fetchall()

        if not billing_rows:
            return jsonify({"error": "no billing rows found",
                            "billing_doc": billing_doc, "do_no": do_no}), 404

        # Customer side -- reuse the shared `customer` table (read-only).
        cust = {}
        try:
            ship = billing_rows[0].get("ship_to") or ""
            sold = billing_rows[0].get("sold_to") or ""
            cols = _customer_columns(cur)
            # Try ship_to first (delivery party); fall back to sold_to.
            if ship:
                cur.execute(
                    f"SELECT {cols} FROM customer WHERE ship_to=%s LIMIT 1",
                    (ship,))
                cust = cur.fetchone() or {}
            if not cust and sold:
                cur.execute(
                    f"SELECT {cols} FROM customer WHERE sold_to=%s LIMIT 1",
                    (sold,))
                cust = cur.fetchone() or {}
        except Exception:
            pass

        # Material description lookup -- try a product / material table
        # if present; otherwise leave blank.
        desc_by_mat = {}
        try:
            cur.execute("SHOW TABLES LIKE 'material_master'")
            if cur.fetchone():
                mats = tuple({r["material"] for r in billing_rows if r.get("material")})
                if mats:
                    ph = ",".join(["%s"] * len(mats))
                    cur.execute(
                        f"SELECT material, description FROM material_master "
                        f"WHERE material IN ({ph})", mats)
                    for r in cur.fetchall():
                        desc_by_mat[r["material"]] = r["description"]
        except Exception:
            pass

        def _size_from_desc(d):
            m = re.search(r"\b(\d{3}/\d{2}[A-Z]?R?\d{2}(?:\.\d+)?)\b", d or "")
            return m.group(1) if m else ""
        def _pattern_from_desc(d):
            # ZSDR24030 description convention is comma-separated, e.g.
            # "195R14C,08,RA58C,L,B,C-,HK" -- 3rd token is the pattern.
            parts = (d or "").split(",")
            return parts[2].strip() if len(parts) >= 3 else ""

        lines = []
        for br in billing_rows:
            desc = desc_by_mat.get(br.get("material"), "")
            lines.append({
                "billing_doc": br.get("billing_doc") or "",
                "do_no":       br.get("do_no") or "",
                "material":    br.get("material") or "",
                "description": desc,
                "tyre_size":   _size_from_desc(desc),
                "pattern":     _pattern_from_desc(desc),
                "qty":         float(br.get("qty") or 0),
                "billing_date": (br["billing_date"].isoformat()
                                 if br.get("billing_date") else ""),
                "amount":      float(br.get("amt") or 0),
            })

        first = billing_rows[0]
        header = {
            "billing_doc":   first.get("billing_doc") or "",
            "do_no":         first.get("do_no") or "",
            "billing_date":  (first["billing_date"].isoformat()
                              if first.get("billing_date") else ""),
            "sold_to":       first.get("sold_to") or "",
            "ship_to":       first.get("ship_to") or "",
            "customer_name": (cust.get("ship_to_name")
                              or cust.get("sold_to_name") or ""),
            "customer_address": _join_address(cust),
            "customer_state":   (cust.get("ship_to_state")
                                 or cust.get("state") or ""),
            "customer_postcode": (cust.get("postcode")
                                  or cust.get("postal_code") or ""),
            "bde_name":      cust.get("salesman_name") or first.get("bde") or "",
        }
        return jsonify({"ok": True, "header": header, "lines": lines})
    finally:
        cur.close(); conn.close()


def _customer_columns(cur):
    """Return a safe SELECT-column list for the shared customer table,
    checking SHOW COLUMNS so this works against deployments with
    slightly different schemas."""
    cur.execute("SHOW COLUMNS FROM customer")
    cols = {r["Field"] for r in cur.fetchall()}
    wanted = {"ship_to", "ship_to_name", "ship_to_state", "sold_to",
              "sold_to_name", "state", "postcode", "postal_code",
              "address_1", "city", "salesman_name"}
    picks = [c for c in wanted if c in cols]
    return ", ".join(picks) if picks else "*"


def _join_address(cust):
    if not cust:
        return ""
    parts = [cust.get("address_1"), cust.get("city")]
    return ", ".join([p for p in parts if p])


@app.post("/rma/api/save")
def save_rma():
    """Persist an RMA request (header + lines).  Body: JSON with
    {header: {...}, lines: [{...}]}.  Returns {ok, id, rma_number}."""
    try:
        payload = request.get_json(force=True) or {}
    except Exception as e:
        return jsonify({"error": f"invalid JSON: {e}"}), 400
    h = payload.get("header") or {}
    lines = payload.get("lines") or []
    if not lines:
        return jsonify({"error": "at least one line is required"}), 400

    # Auto-assign an RMA number if none supplied: RMA-YYMM-<seq>
    rma_number = (h.get("rma_number") or "").strip()
    conn = get_conn()
    cur = conn.cursor()
    try:
        if not rma_number:
            today = date.today()
            prefix = f"RMA-{today.strftime('%y%m')}-"
            cur.execute(
                "SELECT COUNT(*) FROM rma_request_test WHERE rma_number LIKE %s",
                (prefix + "%",))
            n = (cur.fetchone() or (0,))[0]
            rma_number = f"{prefix}{n + 1:04d}"
        req_date = h.get("request_date") or date.today().isoformat()
        cur.execute(
            "INSERT INTO rma_request_test "
            "(rma_number, request_date, rma_type, requestor_name, requestor_email, "
            " customer_name, sold_to, ship_to, customer_address, customer_state, "
            " customer_postcode, reason_code, condition_new, condition_fitted, "
            " condition_dirty, condition_label_on, approval_route, "
            " special_instructions, status) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                rma_number, req_date,
                h.get("rma_type") or "",
                h.get("requestor_name") or "",
                h.get("requestor_email") or "",
                h.get("customer_name") or "",
                h.get("sold_to") or "",
                h.get("ship_to") or "",
                h.get("customer_address") or "",
                h.get("customer_state") or "",
                h.get("customer_postcode") or "",
                h.get("reason_code") or "",
                1 if h.get("condition_new")      else 0,
                1 if h.get("condition_fitted")   else 0,
                1 if h.get("condition_dirty")    else 0,
                1 if h.get("condition_label_on") else 0,
                h.get("approval_route") or "",
                h.get("special_instructions") or "",
                h.get("status") or "submitted",
            ))
        rma_id = cur.lastrowid
        for ln in lines:
            cur.execute(
                "INSERT INTO rma_request_line_test "
                "(rma_id, billing_doc, do_no, material, description, "
                " tyre_size, pattern, qty, billing_date, amount) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    rma_id,
                    ln.get("billing_doc") or "",
                    ln.get("do_no") or "",
                    ln.get("material") or "",
                    ln.get("description") or "",
                    ln.get("tyre_size") or "",
                    ln.get("pattern") or "",
                    float(ln.get("qty") or 0),
                    ln.get("billing_date") or None,
                    float(ln.get("amount") or 0),
                ))
        conn.commit()
    finally:
        cur.close(); conn.close()

    return jsonify({"ok": True, "id": rma_id, "rma_number": rma_number})


@app.get("/rma/api/list")
def list_rma():
    """Recent RMAs, newest first.  Supports ?limit=N (default 50)."""
    limit = min(int(request.args.get("limit", "50") or 50), 200)
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            "SELECT id, rma_number, request_date, customer_name, "
            "       sold_to, ship_to, rma_type, reason_code, status, "
            "       created_at "
            "FROM rma_request_test ORDER BY id DESC LIMIT %s", (limit,))
        rows = cur.fetchall()
        for r in rows:
            for k in ("request_date", "created_at"):
                v = r.get(k)
                if v is not None:
                    r[k] = v.isoformat() if hasattr(v, "isoformat") else str(v)
        return jsonify({"ok": True, "rows": rows})
    finally:
        cur.close(); conn.close()


@app.get("/rma/api/get/<int:rid>")
def get_rma(rid):
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT * FROM rma_request_test WHERE id=%s", (rid,))
        header = cur.fetchone()
        if not header:
            return jsonify({"error": "not found"}), 404
        cur.execute("SELECT * FROM rma_request_line_test WHERE rma_id=%s "
                    "ORDER BY id", (rid,))
        lines = cur.fetchall()
        for row in (header, *lines):
            for k, v in list(row.items()):
                if v is not None and hasattr(v, "isoformat"):
                    row[k] = v.isoformat()
                elif isinstance(v, (bytes, bytearray)):
                    row[k] = v.decode("utf-8", errors="replace")
        return jsonify({"ok": True, "header": header, "lines": lines})
    finally:
        cur.close(); conn.close()


# ---------- health ----------
@app.get("/rma/api/health")
def health():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close(); conn.close()
        return jsonify({"ok": True, "db": "ok", "port": PORT})
    except Exception as e:
        return jsonify({"ok": False, "db": str(e), "port": PORT}), 500


if __name__ == "__main__":
    ensure_schema()
    print(f"[rma] starting experimental RMA app on http://0.0.0.0:{PORT}/rma")
    print(f"[rma] DB: {DB_CFG['host']}:{DB_CFG['port']}/{DB_CFG['database']}")
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)
