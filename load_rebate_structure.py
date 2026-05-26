#!/usr/bin/env python3
"""Rebuild the rebate_structure + rebate_customer_map tables from the two
source CSVs kept in rawdata/unlock/.

The loader picks the most recently modified file matching each name pattern,
so the workflow is: drop the updated CSV(s) into rawdata/unlock/ and re-run
this script.  Patterns:
    structure : rebate_structure*.csv
    map       : rebate_category*.csv  (or rebate_customer_map*.csv)
Override with env vars REBATE_RAW_DIR / REBATE_STRUCT_CSV / REBATE_MAP_CSV.

Usage:
    python load_rebate_structure.py --dry-run        # parse + validate only
    python load_rebate_structure.py                  # write to MySQL

Category code convention (underscore separated):
    HK_ALL_APP_A_SR
     1   2   3  4  5
  1 brand  : HK | LF | ALL          (ALL = HK + LF combined)
  2 line   : PCLT | TBR | ALL
  3 code   : structure mnemonic (APP, GLD, AJT, …) — a label only
  4 unit   : A (amount $) | Q (qty)
  5 suffix : SR        -> calculate per SHIP_TO
             (absent)  -> calculate per SOLD_TO
             HQ/VR/IT/WTY/…           -> calculate per SOLD_TO

DB connection reuses the same env vars / defaults as app.py's get_connection
(DB_HOST=127.0.0.1, DB_PORT=3306, DB_USER=root, DB_PASS='', DB_NAME=my_new_database).
"""
import os, csv, sys, argparse, glob

RAW_DIR = os.getenv("REBATE_RAW_DIR", os.path.join("rawdata", "unlock"))

def _newest(*patterns):
    """Newest (by mtime, then name) file in RAW_DIR matching any pattern."""
    cands = []
    for p in patterns:
        cands += glob.glob(os.path.join(RAW_DIR, p))
    if not cands:
        return None
    return max(cands, key=lambda f: (os.path.getmtime(f), f))

STRUCT_CSV = os.getenv("REBATE_STRUCT_CSV") or _newest("rebate_structure*.csv")
MAP_CSV    = os.getenv("REBATE_MAP_CSV") or _newest(
                 "rebate_category*.csv", "rebate_customer_map*.csv")


def clean_num(s):
    s = (s or "").strip().replace("$", "").replace(",", "").replace("%", "").replace('"', "").strip()
    if s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def normalize_name(name):
    """Collapse accidental double underscores (e.g. 'HK__ALL_RRS_A')."""
    parts = [p for p in name.split("_")]
    # remove empty tokens caused by '__'
    cleaned = "_".join(p for p in parts if p != "")
    return cleaned


def parse_tokens(name):
    """Return (brand, line, code, unit, suffix, calc_basis)."""
    tk = name.split("_")
    brand  = tk[0] if len(tk) > 0 else ""
    line   = tk[1] if len(tk) > 1 else ""
    code   = tk[2] if len(tk) > 2 else ""
    unit   = tk[3] if len(tk) > 3 else ""
    suffix = tk[4] if len(tk) > 4 else ""
    calc_basis = "SHIP_TO" if suffix.upper() == "SR" else "SOLD_TO"
    return brand, line, code, unit, suffix, calc_basis


def parse_structures(path):
    """Return dict name -> {brand,line,code,unit,suffix,calc_basis,tiers:[(threshold,rate)]}."""
    with open(path, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.reader(f))
    out, dupes, issues = {}, [], []
    i = 1  # skip header
    while i < len(rows):
        raw = (rows[i][0] or "").strip()
        if not raw:
            i += 1
            continue
        name = normalize_name(raw)
        thresholds = [clean_num(x) for x in rows[i][1:]]
        rates = []
        if i + 1 < len(rows) and (rows[i + 1][0] or "").strip() == "":
            rates = [clean_num(x) for x in rows[i + 1][1:]]
            i += 2
        else:
            i += 1
        tiers = []
        for t, rt in zip(thresholds, rates):
            if t is None and rt is None:
                continue
            tiers.append((t or 0.0, rt or 0.0))
        # data smell: non-monotonic thresholds
        nz = [t for t, _ in tiers if t > 0]
        if nz != sorted(nz):
            issues.append(f"NON-MONOTONIC thresholds in {name}: {[t for t,_ in tiers]}")
        if name in out:
            dupes.append(name)
        brand, line, code, unit, suffix, basis = parse_tokens(name)
        out[name] = {
            "brand": brand, "line": line, "code": code,
            "unit": unit, "suffix": suffix, "calc_basis": basis,
            "tiers": tiers,
        }
    return out, dupes, issues


def parse_map(path):
    """Return list of dicts {sold_to, grp, ship_to, structure_name}."""
    rows_out = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        rd = csv.reader(f)
        next(rd, None)  # header
        for r in rd:
            if not any((c or "").strip() for c in r):
                continue
            sold = (r[0] or "").strip()
            grp  = (r[1] or "").strip() if len(r) > 1 else ""
            ship = (r[2] or "").strip() if len(r) > 2 else ""
            code = (r[3] or "").strip() if len(r) > 3 else ""
            if not sold or not code:
                continue
            rows_out.append({
                "sold_to": sold, "grp": grp, "ship_to": ship,
                "structure_name": normalize_name(code),
            })
    return rows_out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="parse + validate, print summary, do not touch the DB")
    args = ap.parse_args()

    missing = []
    if not STRUCT_CSV or not os.path.exists(STRUCT_CSV):
        missing.append("structure (rebate_structure*.csv)")
    if not MAP_CSV or not os.path.exists(MAP_CSV):
        missing.append("map (rebate_category*.csv / rebate_customer_map*.csv)")
    if missing:
        sys.exit(f"[error] source CSV not found in {RAW_DIR!r}: {', '.join(missing)}. "
                 f"Save the file(s) there or set REBATE_STRUCT_CSV / REBATE_MAP_CSV.")
    print(f"[src] structure : {STRUCT_CSV}")
    print(f"[src] map       : {MAP_CSV}")

    structs, dupes, issues = parse_structures(STRUCT_CSV)
    mappings = parse_map(MAP_CSV)

    # Validation report
    used_codes = {m["structure_name"] for m in mappings}
    missing = sorted(c for c in used_codes if c not in structs)

    print(f"[parse] structures            : {len(structs)}")
    print(f"[parse] customer-map rows      : {len(mappings)}")
    print(f"[parse] distinct codes in map  : {len(used_codes)}")
    if dupes:
        print(f"[WARN]  duplicate structure names (last wins): {sorted(set(dupes))}")
    for s in issues:
        print(f"[WARN]  {s}")
    if missing:
        print(f"[WARN]  map codes with NO structure definition (these sold_tos "
              f"won't get a rebate calc): {missing}")

    if args.dry_run:
        print("\n[dry-run] no DB writes.")
        return

    import mysql.connector
    conn = mysql.connector.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASS", ""),
        database=os.getenv("DB_NAME", "my_new_database"),
        use_pure=True,
    )
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS rebate_structure")
    cur.execute("""
        CREATE TABLE rebate_structure (
            structure_name VARCHAR(64)  NOT NULL,
            brand          VARCHAR(8)   NOT NULL,
            line           VARCHAR(8)   NOT NULL,
            code           VARCHAR(16)  NOT NULL,
            unit           CHAR(1)      NOT NULL,
            suffix         VARCHAR(8)   NOT NULL DEFAULT '',
            calc_basis     VARCHAR(8)   NOT NULL,
            tier_order     INT          NOT NULL,
            top_order      INT          NOT NULL,
            threshold      DECIMAL(14,2) NOT NULL,
            rate           DECIMAL(6,3)  NOT NULL,
            PRIMARY KEY (structure_name, tier_order)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    for name, sd in structs.items():
        top_order = len(sd["tiers"]) - 1
        for idx, (th, rt) in enumerate(sd["tiers"]):
            cur.execute(
                "INSERT INTO rebate_structure "
                "(structure_name, brand, line, code, unit, suffix, calc_basis, "
                " tier_order, top_order, threshold, rate) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (name, sd["brand"], sd["line"], sd["code"], sd["unit"],
                 sd["suffix"], sd["calc_basis"], idx, top_order, th, rt),
            )

    cur.execute("DROP TABLE IF EXISTS rebate_customer_map")
    cur.execute("""
        CREATE TABLE rebate_customer_map (
            sold_to        VARCHAR(64)  NOT NULL,
            grp            VARCHAR(16)  NOT NULL DEFAULT '',
            ship_to        VARCHAR(64)  NOT NULL DEFAULT '',
            structure_name VARCHAR(64)  NOT NULL,
            brand          VARCHAR(8)   NOT NULL DEFAULT '',
            line           VARCHAR(8)   NOT NULL DEFAULT '',
            unit           CHAR(1)      NOT NULL DEFAULT 'A',
            calc_basis     VARCHAR(8)   NOT NULL DEFAULT 'SOLD_TO',
            INDEX idx_rcm_sold (sold_to),
            INDEX idx_rcm_struct (structure_name)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)
    for m in mappings:
        sd = structs.get(m["structure_name"])
        brand = sd["brand"] if sd else ""
        line  = sd["line"]  if sd else ""
        unit  = sd["unit"]  if sd else "A"
        basis = sd["calc_basis"] if sd else "SOLD_TO"
        cur.execute(
            "INSERT INTO rebate_customer_map "
            "(sold_to, grp, ship_to, structure_name, brand, line, unit, calc_basis) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (m["sold_to"], m["grp"], m["ship_to"], m["structure_name"],
             brand, line, unit, basis),
        )

    conn.commit()
    cur.close()
    conn.close()
    print(f"\n[done] loaded {len(structs)} structures and {len(mappings)} map rows.")


if __name__ == "__main__":
    main()
