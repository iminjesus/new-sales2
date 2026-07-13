#!/usr/bin/env python3
"""
postcode_penetration.py — Market penetration matrix by comparing BITRE
fleet-based rim demand with Hankook's actual sales.

Pipeline:
  BITRE     postcode_rim_demand.csv         (state, postcode, rim_family, fleet)
     +
  Hankook   sales_2526 × customer × carrying_26
                → (state, postcode?, rim_family, sold_units)
     ↓ join
  Penetration matrix: fleet_units, hk_sold_units, penetration_pct,
                       opportunity_units per (state, postcode?, rim_family)

Two granularities:
  --level state       (default; robust — uses customer.bde_state which
                       is always populated).  Aggregates BITRE to state
                       level, joins by (state, rim_family).
  --level postcode    Tries to extract a 4-digit postcode from
                       customer.address_1.  Requires address_1 to
                       actually contain the postcode; falls back to
                       state for rows where postcode extraction fails.

Data source for Hankook sales:
  --db                Query MySQL directly (default).  Needs env vars
                       DB_HOST, DB_USER, DB_PASS, DB_NAME (same as the
                       load_csv_to_mysql.py convention) OR .env file.
  --sales-csv PATH    Skip the DB, read a pre-exported CSV with
                       columns: bde_state, address_1, size, qty.

Output: postcode_penetration.csv
  state, postcode?, rim_family, fleet_units, hk_sold_units,
  penetration_pct, opportunity_units

Requirements:
  pip install mysql-connector-python python-dotenv
"""
from __future__ import annotations
import argparse
import csv
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BASE_DIR   = Path(__file__).resolve().parent
BITRE_CSV  = BASE_DIR / "out" / "rego" / "postcode_rim_demand.csv"
OUTPUT_CSV = BASE_DIR / "out" / "rego" / "postcode_penetration.csv"

# ─── extractors ───────────────────────────────────────────────────────
_POSTCODE_RX = re.compile(r"\b(\d{4})\b")

def extract_postcode(address_1: str) -> str:
    """Pull the LAST 4-digit run from an address string — Australian
    postal format puts the postcode at the end after the state code
    ("... NSW 2000"), so 'last match wins' is the safe pick."""
    if not address_1:
        return ""
    matches = _POSTCODE_RX.findall(str(address_1))
    return matches[-1] if matches else ""


# Rim-diameter parse from carrying_26.size strings.  Handles:
#   215/70R14       → 14
#   185/60R15       → 15
#   195R15C         → 15
#   11R22.5         → 22
#   275/70R22.5     → 22
_RIM_RX = re.compile(r"R\s*(\d{2}(?:\.\d)?)", re.IGNORECASE)

def rim_from_size(size_str: str) -> int | None:
    if not size_str:
        return None
    m = _RIM_RX.search(str(size_str))
    if not m:
        return None
    val = m.group(1)
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def rim_family(inches: int | None) -> str:
    if inches is None:
        return "UNKNOWN"
    if inches <= 13: return "R13"
    if inches == 14: return "R14"
    if inches == 15: return "R15"
    if inches == 16: return "R16"
    if inches == 17: return "R17"
    if inches == 18: return "R18"
    if inches == 19: return "R19"
    if inches == 20: return "R20"
    if inches == 21: return "R21"
    if inches >= 22: return "R22+ (truck)"
    return f"R{inches}"


# ─── BITRE side loader ────────────────────────────────────────────────
def load_bitre(path: Path, level: str) -> dict:
    """Aggregate postcode_rim_demand.csv to the requested granularity.
    Skips MOTORCYCLE and UNKNOWN rim families — comparing tyre demand
    against those makes no sense."""
    bucket: dict = defaultdict(int)
    total = 0
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            state = (r.get("state") or "").strip().upper()
            pc    = (r.get("postcode") or "").strip()
            fam   = (r.get("rim_family") or "").strip()
            try:
                units = int(r.get("fleet_units") or 0)
            except ValueError:
                units = 0
            if units <= 0:
                continue
            if fam in ("UNKNOWN", "MOTORCYCLE"):
                continue
            if level == "state":
                bucket[(state, fam)] += units
            else:
                bucket[(state, pc, fam)] += units
            total += units
    return bucket, total


# ─── Hankook side loader (DB) ─────────────────────────────────────────
def fetch_hankook_sales_from_db(year: int) -> list[dict]:
    import mysql.connector
    cfg = {
        "host":     os.getenv("DB_HOST", "127.0.0.1"),
        "port":     int(os.getenv("DB_PORT", "3306")),
        "user":     os.getenv("DB_USER", "root"),
        "password": os.getenv("DB_PASS", ""),
        "database": os.getenv("DB_NAME", "my_new_database"),
    }
    conn = mysql.connector.connect(**cfg)
    cur  = conn.cursor(dictionary=True)
    # Group at ship_to level so we don't return one row per line-item.
    # Postcode extraction happens in Python off address_1 because the
    # customer table doesn't carry a dedicated postcode column.
    sql = f"""
        SELECT
            c.ship_to,
            c.bde_state,
            c.address_1,
            cr.size,
            SUM(s.qty) AS qty
        FROM sales_2526 s
        JOIN customer c    ON c.ship_to = s.ship_to
        JOIN carrying_26 cr ON cr.m_code = s.material
        WHERE s.billing_date >= '{year}-01-01'
          AND s.billing_date <  '{year + 1}-01-01'
          AND s.qty > 0
        GROUP BY c.ship_to, c.bde_state, c.address_1, cr.size
    """
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


# ─── Hankook side aggregator ──────────────────────────────────────────
def aggregate_hankook(rows: list[dict], level: str) -> tuple[dict, dict]:
    """Roll raw sales rows to the requested granularity.  Returns the
    aggregated dict AND diagnostic counters so the run can report how
    much data was lost to missing fields."""
    agg: dict = defaultdict(int)
    diag = {"total_qty": 0, "no_state": 0, "no_postcode": 0,
            "no_rim": 0, "kept_qty": 0}
    for r in rows:
        state = (r.get("bde_state") or "").strip().upper()
        pc    = extract_postcode(r.get("address_1") or "")
        rim   = rim_from_size(r.get("size") or "")
        try:
            qty = int(float(r.get("qty") or 0))
        except (ValueError, TypeError):
            qty = 0
        diag["total_qty"] += qty
        if qty <= 0:
            continue
        if not state:
            diag["no_state"] += qty
            continue
        if rim is None:
            diag["no_rim"] += qty
            continue
        fam = rim_family(rim)
        if level == "state":
            agg[(state, fam)] += qty
        else:
            if not pc:
                diag["no_postcode"] += qty
                continue
            agg[(state, pc, fam)] += qty
        diag["kept_qty"] += qty
    return agg, diag


# ─── Output ───────────────────────────────────────────────────────────
def write_penetration(out_path: Path, level: str,
                      bitre: dict, sales: dict) -> int:
    all_keys = sorted(set(bitre.keys()) | set(sales.keys()))
    header = (["state", "rim_family"] if level == "state"
              else ["state", "postcode", "rim_family"])
    header += ["fleet_units", "hk_sold_units",
               "penetration_pct", "opportunity_units"]

    out_dir = os.path.dirname(os.path.abspath(out_path))
    if out_dir and not os.path.isdir(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    try:
        f = open(out_path, "w", newline="", encoding="utf-8")
    except PermissionError:
        from datetime import datetime as _dt
        stamp = _dt.now().strftime("%Y%m%d_%H%M%S")
        alt = Path(out_path).with_stem(Path(out_path).stem + "_" + stamp)
        print(f"[warn] {out_path.name} is locked (open in Notepad/Excel?). "
              f"Writing to {alt.name} instead.", file=sys.stderr)
        out_path = alt
        f = open(out_path, "w", newline="", encoding="utf-8")

    n = 0
    with f:
        w = csv.writer(f)
        w.writerow(header)
        for key in all_keys:
            fleet = bitre.get(key, 0)
            sold  = sales.get(key, 0)
            pen   = (sold / fleet * 100.0) if fleet > 0 else (float('inf') if sold > 0 else 0.0)
            opp   = max(fleet - sold, 0)
            pen_out = "" if pen == float('inf') else f"{pen:.2f}"
            row = list(key) + [fleet, sold, pen_out, opp]
            w.writerow(row)
            n += 1
    print(f"[done] wrote {out_path} ({n:,} rows)")
    return n


def print_diagnostics(bitre_total: int, sales_diag: dict, level: str,
                      bitre: dict, sales: dict):
    kept  = sales_diag["kept_qty"]
    total = sales_diag["total_qty"]
    lost  = total - kept
    print(f"\n[hankook diagnostics]")
    print(f"    total sales qty:      {total:>12,}")
    print(f"    matched into bucket:  {kept:>12,}  ({kept/max(total,1)*100:.1f} %)")
    print(f"    dropped no state:     {sales_diag['no_state']:>12,}")
    if level != "state":
        print(f"    dropped no postcode:  {sales_diag['no_postcode']:>12,}")
    print(f"    dropped no rim:       {sales_diag['no_rim']:>12,}")
    print(f"\n[coverage]")
    print(f"    BITRE fleet total:    {bitre_total:>12,}")
    print(f"    Hankook sold total:   {kept:>12,}")
    if bitre_total:
        print(f"    National penetration: {kept/bitre_total*100:>12.2f} %")

    # Top-N biggest untapped buckets by absolute gap
    gaps = []
    for k in bitre:
        gap = bitre[k] - sales.get(k, 0)
        if gap > 0:
            gaps.append((gap, k, sales.get(k, 0)))
    gaps.sort(reverse=True)
    print(f"\n[top-15 biggest opportunity gaps (fleet − sold)]")
    hdr = ("state pc rim" if level != "state" else "state rim")
    print(f"    {hdr:20}  fleet         sold        gap")
    for gap, k, sold in gaps[:15]:
        label = " ".join(k)
        fleet = bitre[k]
        print(f"    {label:20}  {fleet:>10,}   {sold:>10,}   {gap:>10,}")


# ─── main ─────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bitre",   default=str(BITRE_CSV),
                    help="postcode_rim_demand.csv path (default: %(default)s)")
    ap.add_argument("--output",  default=str(OUTPUT_CSV),
                    help="Output CSV path (default: %(default)s)")
    ap.add_argument("--level",   choices=["state", "postcode"], default="state",
                    help="Granularity of the join (default: state).  "
                         "Postcode requires customer.address_1 to contain "
                         "the 4-digit postcode.")
    ap.add_argument("--year",    type=int, default=2026,
                    help="Hankook sales year (billing_date; default 2026).")
    ap.add_argument("--sales-csv", default=None,
                    help="Skip DB — read pre-exported sales CSV with columns "
                         "(bde_state, address_1, size, qty).")
    args = ap.parse_args()

    bitre_path = Path(args.bitre)
    if not bitre_path.exists():
        print(f"[fatal] BITRE input not found: {bitre_path}")
        print(f"        Run postcode_rim_demand.py first.")
        sys.exit(1)

    print(f"[load] BITRE fleet: {bitre_path}  (level={args.level})")
    bitre, bitre_total = load_bitre(bitre_path, args.level)
    print(f"       {len(bitre):,} buckets, {bitre_total:,} total fleet units")

    if args.sales_csv:
        print(f"[load] Hankook sales CSV: {args.sales_csv}")
        rows: list[dict] = []
        with open(args.sales_csv, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                rows.append(r)
    else:
        print(f"[load] Hankook sales: MySQL sales_2526 × customer × carrying_26 "
              f"(year={args.year})")
        try:
            rows = fetch_hankook_sales_from_db(args.year)
        except ImportError:
            print("[error] mysql-connector-python not installed.  "
                  "Install: pip install mysql-connector-python", file=sys.stderr)
            print("        Or export a CSV manually and pass --sales-csv.",
                  file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"[error] DB query failed: {e}", file=sys.stderr)
            print("        Check DB_HOST / DB_USER / DB_PASS env vars, "
                  "or use --sales-csv.", file=sys.stderr)
            sys.exit(1)
    print(f"       {len(rows):,} raw (ship_to × size) rows")

    sales, diag = aggregate_hankook(rows, args.level)
    print(f"       {len(sales):,} aggregated buckets")

    write_penetration(Path(args.output), args.level, bitre, sales)
    print_diagnostics(bitre_total, diag, args.level, bitre, sales)


if __name__ == "__main__":
    main()
