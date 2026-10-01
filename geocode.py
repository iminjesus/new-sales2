"""
geocode.py — Customer Master → Template → Geocode → Flat CSV pipeline

Two file pickers run at launch:

  1. "Please locate most recent customer master file" — the raw
     Customer Master export (unlocked workbook from SAP / BW).
     Only columns from A through "Channel2" are read.

  2. "Please locate the Geolocation template file for the customer
     master file" — the Pamela template workbook that holds the
     Customer Master Excel Table + the Location Master sheet.  Its
     Customer Master table has three formula columns at the right
     end that depend on the Channel2-and-earlier data copied in
     from (1); those formulas are preserved and re-applied across
     the new row count by this script, so the Table auto-extends
     in Excel on next open regardless of how many rows (1) grew by.

Then the normal pipeline:

  A. Pass 1 — compare the composite address key (Address 1, City,
     Region, Postal Code, Country) on both sheets of the TEMPLATE.
     Every Customer Master key that isn't already a Location
     Master row is appended at the bottom of Location Master with
     blank lat / lon.
  B. Pass 2 — every Location Master row that still has blank F or
     G is sent to the Google Geocoding API.  Progress is cached
     in geocode_add_cache.csv keyed by the composite address so a
     Ctrl-C restart doesn't re-charge the API for solved rows.
  C. The template workbook is saved back in place (falls back to
     <name>_updated.xlsx when it's open in Excel).
  D. A flat CSV is written to
        E:\\01. work\\2025\\Data_Anal_Website\\rawdata\\unlock\\customer_YYMM.csv
     where YY / MM are today's two-digit year / month.  Columns
     are the Customer Master fields the dashboard loader expects,
     with Longitude / Latitude joined in from Location Master.

Dependencies: pip install openpyxl requests
"""

import csv
import os
import sys
import time
from datetime import date

import requests
from openpyxl import load_workbook

# ──────────────────────────────────────────────────────────────────────
#  CONFIG
# ──────────────────────────────────────────────────────────────────────

# Google Geocoding API key.  Set an env var or paste directly.
API_KEY = os.environ.get(
    "GOOGLE_GEOCODING_KEY",
    "AIzaSyCXBVpgrLEc_iCheZxAQ4TXu_kzYoAotmw",
)

# Progress cache so a re-run doesn't re-charge for solved addresses.
# Keyed by the composite address string.  CSV so it's inspectable.
CACHE_FILE = "geocode_add_cache.csv"

# Sheet names inside the workbook.
CUSTOMER_SHEET = "Customer Master"
LOCATION_SHEET = "Location Master"

# Address columns — same in both sheets, header-matched case-insensitively.
# Order matters: this is the composite-key order and the Location Master
# column order (A → E).
ADDRESS_COLUMNS = ["Address 1", "City", "Region", "Postal Code", "Country"]

# Columns the final flat file needs, in order.  Values come from the
# Customer Master row, EXCEPT "Longitude" / "Latitude" which are joined
# in from Location Master.  If a column header on the Customer Master
# sheet is different, add an alias to CUSTOMER_COLUMN_ALIASES below.
FLAT_FILE_COLUMNS = [
    "channel2",
    "Parent CGR3",
    "Sold-to",
    "Sold-to Name",
    "Ship-to State",
    "Ship-to",
    "Name",
    "BDE State",
    "Salesman Name",
    "Longitude",       # from Location Master
    "Latitude",        # from Location Master
    "Postal Code",
    "Address 1",
    "City",
    "E-Mail Address",
    "Telephone",
    "Mobile Phone",
]

# Different Customer Master versions use slightly different headers.
# Map FLAT_FILE_COLUMNS entry → list of header spellings this script
# should accept.  Case / whitespace are ignored by the lookup.
CUSTOMER_COLUMN_ALIASES = {
    "channel2":        ["channel2", "Channel2", "Channel 2"],
    "Parent CGR3":     ["Parent CGR3", "ParentCGR3", "Parent CGR 3"],
    "Sold-to":         ["Sold-to", "Sold to", "SoldTo"],
    "Sold-to Name":    ["Sold-to Name", "Sold to Name", "SoldToName"],
    "Ship-to":         ["Ship-to", "Ship to", "ShipTo", "Customer"],
    "Ship-to State":   ["Ship-to State", "Ship to State", "ShipToState", "State"],
    "Name":            ["Name", "Ship-to Name", "Ship to Name"],
    "BDE State":       ["BDE State", "BDE_State", "BDE state"],
    "Salesman Name":   ["Salesman Name", "Salesman", "SalesmanName"],
    "E-Mail Address":  ["E-Mail Address", "Email Address", "E-mail", "Email"],
    "Mobile Phone":    ["Mobile Phone", "Mobile", "Mobile Number"],
    "Telephone":       ["Telephone", "Phone", "Phone Number"],
    "Postal Code":     ["Postal Code", "PostalCode", "Postcode", "Post Code"],
    "Address 1":       ["Address 1", "Address1", "Street"],
    "City":            ["City", "Town"],
    "Region":          ["Region", "State"],
    "Country":         ["Country"],
}

# Where the flat file is written on the operator's workstation.
# Switch to a POSIX path when running the script on Linux for testing.
OUTPUT_DIR = r"E:\01. work\2025\Data_Anal_Website\rawdata\unlock"

# Country code → full name (for the geocoder input only).
COUNTRY_NAME_MAP = {
    "AU": "Australia",
    "NZ": "New Zealand",
    "PG": "Papua New Guinea",
    "CO": "Colombia",
}

# Polite delay between Geocoding API calls (seconds).  Google will
# throttle well before this matters, but keeping it round the one-per-
# second mark means a long batch stays well under the free-tier
# per-day budget.
GEOCODE_DELAY_SEC = 0.1


# ──────────────────────────────────────────────────────────────────────
#  Helpers
# ──────────────────────────────────────────────────────────────────────

def pick_file(title):
    """Open a native file dialog with the given title.  Falls back
    to a stdin prompt when tkinter isn't available (headless server,
    SSH-forwarded run)."""
    try:
        import tkinter as tk
        from tkinter import filedialog
    except Exception:
        p = input(f"{title}: ").strip().strip('"')
        return p
    root = tk.Tk()
    root.withdraw()
    root.update()
    p = filedialog.askopenfilename(
        title=title,
        filetypes=[("Excel workbook", "*.xlsx *.xlsm"), ("All files", "*.*")],
    )
    root.destroy()
    return p


def copy_source_through_channel2(src_path, tpl_path):
    """Copy columns A..Channel2 (inclusive, every row) from the source
    Customer Master workbook into the TEMPLATE's Customer Master
    sheet, in place.  The template is where everything downstream
    (Location Master geocoding + flat-file export) runs — the source
    file is read-only input.

    Preserves the template's Excel Table structure:
      • Captures the formulas on the LAST THREE columns of the
        template's Customer Master Table from the first data row.
      • Clears every existing data row (columns A..Channel2) so
        yesterday's rows don't leak into today's.
      • Writes source rows into the template.
      • Replays the captured formulas onto every new row, shifting
        row references via openpyxl.formula.translate.Translator.
      • Updates the Excel Table's `ref` so Excel treats the new rows
        as part of the Table on next open.

    Raises on IO errors; caller catches."""
    from openpyxl.utils import range_boundaries, get_column_letter
    from openpyxl.formula.translate import Translator

    print(f"\nStage 1 — copy source → template")
    print(f"  Source  : {src_path}")
    print(f"  Template: {tpl_path}")

    # Read source as values only — the template owns the formulas.
    src_wb = load_workbook(src_path, data_only=True)
    tpl_wb = load_workbook(tpl_path)

    src_ws = src_wb[CUSTOMER_SHEET] if CUSTOMER_SHEET in src_wb.sheetnames else src_wb.active
    if CUSTOMER_SHEET not in tpl_wb.sheetnames:
        raise RuntimeError(
            f"Template is missing the '{CUSTOMER_SHEET}' sheet. "
            f"Available sheets: {tpl_wb.sheetnames}"
        )
    tpl_ws = tpl_wb[CUSTOMER_SHEET]
    if src_ws.title != CUSTOMER_SHEET:
        print(f"  (source: '{CUSTOMER_SHEET}' not found; using active sheet "
              f"'{src_ws.title}')")

    # Find the Channel2 column in the source so we know where to stop
    # the copy.  Column-letter match tolerates spelling drift
    # ("channel2", "Channel 2", "CHANNEL2").
    src_headers = [c.value for c in src_ws[1]]
    def _nrm(s): return "" if s is None else " ".join(str(s).split()).strip().lower()
    ch2_idx_1b = None   # 1-based column number
    for i, h in enumerate(src_headers, start=1):
        if _nrm(h).replace(" ", "") == "channel2":
            ch2_idx_1b = i
            break
    if ch2_idx_1b is None:
        print("  WARNING: 'Channel2' header not found in source; "
              "copying every source column.")
        ch2_idx_1b = len(src_headers)
    print(f"  Source Channel2 is column {get_column_letter(ch2_idx_1b)} "
          f"({ch2_idx_1b}); copying columns A..{get_column_letter(ch2_idx_1b)}")

    src_data_rows = max(0, src_ws.max_row - 1)
    print(f"  Source has {src_data_rows} data rows")

    # Find the Table in the template Customer Master sheet so we can
    # preserve + extend it.  First table wins; a template normally
    # only carries one on this sheet.
    tbl = None
    try:
        tables = tpl_ws.tables   # openpyxl ≥ 3.0 — dict-like
        for t in list(tables.values()):
            tbl = t
            break
    except Exception:
        tbl = None

    if tbl is not None:
        min_col, min_row, max_col, max_row = range_boundaries(tbl.ref)
        print(f"  Template table '{tbl.name}' at {tbl.ref} "
              f"(cols {get_column_letter(min_col)}..{get_column_letter(max_col)}, "
              f"rows {min_row}..{max_row})")
        header_row  = min_row
        first_data  = min_row + 1
    else:
        print("  No Excel Table found on template Customer Master sheet; "
              "treating row 1 as headers.")
        min_col, max_col = 1, max(tpl_ws.max_column, ch2_idx_1b + 3)
        header_row = 1
        first_data = 2
        max_row    = tpl_ws.max_row

    # Capture formula templates from the first data row of the LAST
    # THREE columns of the table — the ones the user said carry the
    # formulas we need to re-apply across the new row count.
    formula_cols = []
    for col in range(max(ch2_idx_1b + 1, max_col - 2), max_col + 1):
        cell = tpl_ws.cell(row=first_data, column=col)
        v = cell.value
        if isinstance(v, str) and v.startswith("="):
            formula_cols.append((col, v))
    if formula_cols:
        print("  Formula columns captured: " +
              ", ".join(f"{get_column_letter(c)}={f}" for c, f in formula_cols))
    else:
        print("  No formula columns detected in the last three table columns.")

    # Wipe existing data in columns A..Channel2 across every existing
    # row — covers the "today's file is smaller than yesterday's" case.
    for row in range(first_data, max_row + 1):
        for col in range(min_col, ch2_idx_1b + 1):
            tpl_ws.cell(row=row, column=col, value=None)

    # Also wipe formula columns BEYOND the new row count so stale
    # formulas don't hang off the end of the table.
    new_last = first_data + src_data_rows - 1
    if new_last < max_row:
        for row in range(new_last + 1, max_row + 1):
            for col in range(ch2_idx_1b + 1, max_col + 1):
                tpl_ws.cell(row=row, column=col, value=None)

    # Paste every source data row into the template, columns A..Channel2.
    for src_row in range(2, src_ws.max_row + 1):
        tpl_row = first_data + (src_row - 2)
        for col in range(1, ch2_idx_1b + 1):
            tpl_ws.cell(row=tpl_row, column=col,
                        value=src_ws.cell(row=src_row, column=col).value)

    # Re-apply formulas on every new row by row-shifting the first-row
    # formula via openpyxl's Translator.
    for row in range(first_data, new_last + 1):
        for col, formula_src in formula_cols:
            origin = f"{get_column_letter(col)}{first_data}"
            target = f"{get_column_letter(col)}{row}"
            try:
                shifted = Translator(formula_src, origin=origin).translate_formula(target)
            except Exception:
                shifted = formula_src   # fallback: paste as-is
            tpl_ws.cell(row=row, column=col, value=shifted)

    # Extend the Excel Table range so Excel sees every new row as part
    # of the table on next open (and auto-applies the calculated-column
    # formula to any row we might have missed).
    if tbl is not None and new_last > 0:
        new_ref = (f"{get_column_letter(min_col)}{min_row}:"
                   f"{get_column_letter(max_col)}{new_last}")
        tbl.ref = new_ref
        print(f"  Extended table '{tbl.name}' → {new_ref}")

    # Save template back in place (fall back to <name>_updated.xlsx when
    # the user left the file open in Excel).
    try:
        tpl_wb.save(tpl_path)
        print(f"  Saved template: {tpl_path}")
        return tpl_path
    except PermissionError:
        alt = os.path.splitext(tpl_path)[0] + "_updated.xlsx"
        tpl_wb.save(alt)
        print(f"  Source template was locked — saved copy to: {alt}")
        return alt


def _norm_key(parts):
    """Normalise an address tuple for comparison / cache lookup.
    Strips whitespace, uppercases, and collapses runs of spaces so
    'MACQUARIE  PARK' and 'macquarie park' match."""
    out = []
    for p in parts:
        s = "" if p is None else str(p)
        s = " ".join(s.strip().split()).upper()
        out.append(s)
    return tuple(out)


def _lookup_header(header_row, aliases):
    """Return the first column INDEX whose header matches any of the
    accepted aliases (case / whitespace insensitive).  None if nothing
    matches."""
    def _nrm(s):
        return "" if s is None else " ".join(str(s).strip().split()).lower()
    normalised = {_nrm(h): i for i, h in enumerate(header_row)}
    for alias in aliases:
        if _nrm(alias) in normalised:
            return normalised[_nrm(alias)]
    return None


def read_sheet(ws):
    """Return (headers, rows).  Headers come from row 1; each row is a
    list of raw cell values in the same positional order so caller can
    look up by column index."""
    rows_iter = ws.iter_rows(values_only=True)
    try:
        headers = list(next(rows_iter))
    except StopIteration:
        return [], []
    rows = [list(r) for r in rows_iter]
    return headers, rows


# ──────────────────────────────────────────────────────────────────────
#  Google Geocoding
# ──────────────────────────────────────────────────────────────────────

def build_address_string(parts_dict):
    """Compose the single-line address the Geocoding API likes best."""
    street   = (parts_dict.get("Address 1") or "").strip()
    city     = (parts_dict.get("City") or "").strip()
    region   = (parts_dict.get("Region") or "").strip()
    postcode = (parts_dict.get("Postal Code") or "").strip()
    country_code = (parts_dict.get("Country") or "").strip().upper()
    country = COUNTRY_NAME_MAP.get(country_code, country_code or "")
    parts = []
    if street:
        parts.append(street)
    tail = " ".join(p for p in [region, postcode] if p)
    second_line_parts = [city] if city else []
    if tail:
        second_line_parts.append(tail)
    if second_line_parts:
        parts.append(", ".join(second_line_parts))
    if country:
        parts.append(country)
    return ", ".join(parts)


def geocode(address, session):
    """Call Google Geocoding; return (lat, lon) as floats or
    (None, None).  Blank input returns (None, None) without an API hit."""
    address = (address or "").strip()
    if not address:
        return None, None
    try:
        resp = session.get(
            "https://maps.googleapis.com/maps/api/geocode/json",
            params={"address": address, "key": API_KEY},
            timeout=15,
        )
        resp.raise_for_status()
    except Exception as e:
        print(f"  HTTP error: {e}")
        return None, None
    data = resp.json()
    if data.get("status") != "OK" or not data.get("results"):
        print(f"  no result ({data.get('status')}): {address}")
        return None, None
    loc = data["results"][0]["geometry"]["location"]
    return float(loc["lat"]), float(loc["lng"])


# ──────────────────────────────────────────────────────────────────────
#  Cache
# ──────────────────────────────────────────────────────────────────────

def load_cache():
    """Return {composite_key_str: (lat, lon)} from the on-disk cache."""
    cache = {}
    if not os.path.exists(CACHE_FILE):
        return cache
    with open(CACHE_FILE, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (row.get("key") or "").strip()
            if not key:
                continue
            lat = row.get("lat"); lon = row.get("lon")
            try:
                lat = float(lat) if lat not in (None, "", "None") else None
                lon = float(lon) if lon not in (None, "", "None") else None
            except ValueError:
                lat = lon = None
            cache[key] = (lat, lon)
    return cache


def append_cache(key, address_str, lat, lon):
    new_file = not os.path.exists(CACHE_FILE) or os.path.getsize(CACHE_FILE) == 0
    with open(CACHE_FILE, "a", newline="", encoding="utf-8", errors="replace") as f:
        w = csv.DictWriter(f, fieldnames=["key", "address", "lat", "lon"])
        if new_file:
            w.writeheader()
        w.writerow({
            "key":     key,
            "address": address_str,
            "lat":     "" if lat is None else lat,
            "lon":     "" if lon is None else lon,
        })


# ──────────────────────────────────────────────────────────────────────
#  Main pipeline
# ──────────────────────────────────────────────────────────────────────

def main():
    # ── Stage 0: pick the two workbooks ───────────────────────────
    src_path = pick_file("Please locate most recent customer master file")
    if not src_path:
        print("No source Customer Master file selected — exiting.")
        return
    if not os.path.exists(src_path):
        print(f"Source file not found: {src_path}")
        return

    tpl_path = pick_file("Please locate the Geolocation template file for the customer master file")
    if not tpl_path:
        print("No template file selected — exiting.")
        return
    if not os.path.exists(tpl_path):
        print(f"Template file not found: {tpl_path}")
        return

    # ── Stage 1: copy source (through Channel2) into the template ──
    # Everything downstream — the Pass-1 missing-address check, the
    # Pass-2 geocoder, and the flat-file export — runs against the
    # TEMPLATE workbook, which now carries the fresh Customer Master
    # data plus its own Location Master history and formula columns.
    try:
        xlsx_path = copy_source_through_channel2(src_path, tpl_path)
    except Exception as e:
        print(f"\nCould not copy source into template: {e}")
        import traceback; traceback.print_exc()
        return

    print(f"\nStage 2+: running geocode + flat-file pipeline on template")
    print(f"  Working file: {xlsx_path}")

    wb = load_workbook(xlsx_path)
    if LOCATION_SHEET not in wb.sheetnames:
        print(f"Sheet '{LOCATION_SHEET}' not found. Available: {wb.sheetnames}")
        return
    if CUSTOMER_SHEET not in wb.sheetnames:
        print(f"Sheet '{CUSTOMER_SHEET}' not found. Available: {wb.sheetnames}")
        return

    cm_ws = wb[CUSTOMER_SHEET]
    lm_ws = wb[LOCATION_SHEET]

    cm_headers, cm_rows = read_sheet(cm_ws)
    lm_headers, lm_rows = read_sheet(lm_ws)

    # Column index maps for both sheets.  Fail loudly if an address
    # column is missing — the whole pipeline hinges on them.
    cm_addr_idx = {col: _lookup_header(cm_headers, CUSTOMER_COLUMN_ALIASES.get(col, [col]))
                   for col in ADDRESS_COLUMNS}
    lm_addr_idx = {col: _lookup_header(lm_headers, CUSTOMER_COLUMN_ALIASES.get(col, [col]))
                   for col in ADDRESS_COLUMNS}
    for col, idx in cm_addr_idx.items():
        if idx is None:
            print(f"[Customer Master] missing address column: {col}")
            return
    for col, idx in lm_addr_idx.items():
        if idx is None:
            print(f"[Location Master] missing address column: {col}")
            return

    # Latitude / Longitude columns on Location Master.  Default F/G
    # (indexes 5 / 6) if the headers aren't recognisable.
    lat_idx = _lookup_header(lm_headers, ["Latitude", "lat"])
    lon_idx = _lookup_header(lm_headers, ["Longitude", "lon", "lng"])
    if lat_idx is None:
        lat_idx = 5   # column F
    if lon_idx is None:
        lon_idx = 6   # column G
    # Make sure Location Master header row carries labels for F and G
    # so future human edits read cleanly.
    while len(lm_headers) <= max(lat_idx, lon_idx):
        lm_headers.append(None)
    if lm_headers[lat_idx] is None:
        lm_headers[lat_idx] = "Latitude"
        lm_ws.cell(row=1, column=lat_idx + 1, value="Latitude")
    if lm_headers[lon_idx] is None:
        lm_headers[lon_idx] = "Longitude"
        lm_ws.cell(row=1, column=lon_idx + 1, value="Longitude")

    # ── Pass 1: append missing addresses from Customer Master ──────
    def _row_key(row, idx_map):
        return _norm_key([row[idx_map[c]] if idx_map[c] < len(row) else "" for c in ADDRESS_COLUMNS])

    lm_keys = {_row_key(r, lm_addr_idx) for r in lm_rows}
    missing = []
    for r in cm_rows:
        if all((r[cm_addr_idx[c]] if cm_addr_idx[c] < len(r) else None) in (None, "", " ")
               for c in ADDRESS_COLUMNS):
            continue  # skip blank rows
        k = _row_key(r, cm_addr_idx)
        if k not in lm_keys:
            lm_keys.add(k)
            missing.append(k)

    if missing:
        start_row = lm_ws.max_row + 1
        for i, k in enumerate(missing):
            for col_i, addr_col in enumerate(ADDRESS_COLUMNS):
                lm_ws.cell(row=start_row + i,
                           column=lm_addr_idx[addr_col] + 1,
                           value=k[col_i])
            # Rebuild the in-memory rows list so pass 2 sees the new rows.
            new_row = [None] * max(len(lm_headers), lon_idx + 1)
            for col_i, addr_col in enumerate(ADDRESS_COLUMNS):
                new_row[lm_addr_idx[addr_col]] = k[col_i]
            lm_rows.append(new_row)
        print(f"[Location Master] appended {len(missing)} missing addresses from Customer Master")
    else:
        print("[Location Master] no missing addresses from Customer Master")

    # ── Pass 2: geocode every row with blank lat or lon ────────────
    cache = load_cache()
    print(f"Cache: {len(cache)} entries loaded")

    session = requests.Session()
    pending = []
    for i, r in enumerate(lm_rows):
        lat = r[lat_idx] if lat_idx < len(r) else None
        lon = r[lon_idx] if lon_idx < len(r) else None
        if lat not in (None, "", " ") and lon not in (None, "", " "):
            continue
        parts = {c: (r[lm_addr_idx[c]] if lm_addr_idx[c] < len(r) else "") for c in ADDRESS_COLUMNS}
        key_tuple = _norm_key([parts[c] for c in ADDRESS_COLUMNS])
        pending.append((i, parts, key_tuple))

    print(f"To geocode: {len(pending)}")

    try:
        for n, (row_i, parts, key_tuple) in enumerate(pending, start=1):
            key = "|".join(key_tuple)
            addr_str = build_address_string(parts)
            if key in cache:
                lat, lon = cache[key]
                src = "cache"
            else:
                print(f"[{n}/{len(pending)}] {addr_str}")
                lat, lon = geocode(addr_str, session)
                append_cache(key, addr_str, lat, lon)
                time.sleep(GEOCODE_DELAY_SEC)
                src = "api"
            # Write back into the worksheet (1-based rows; +1 for header row).
            if lat is not None:
                lm_ws.cell(row=row_i + 2, column=lat_idx + 1, value=lat)
            if lon is not None:
                lm_ws.cell(row=row_i + 2, column=lon_idx + 1, value=lon)
            # Keep in-memory copy in sync for the flat-file join below.
            while len(lm_rows[row_i]) <= max(lat_idx, lon_idx):
                lm_rows[row_i].append(None)
            lm_rows[row_i][lat_idx] = lat
            lm_rows[row_i][lon_idx] = lon
            if src == "api":
                print(f"  -> lat={lat}, lon={lon}")
    except KeyboardInterrupt:
        print("\nInterrupted — saving what we have.")

    # ── Save the workbook back in place ────────────────────────────
    try:
        wb.save(xlsx_path)
        print(f"Workbook saved: {xlsx_path}")
    except PermissionError:
        alt = os.path.splitext(xlsx_path)[0] + "_updated.xlsx"
        wb.save(alt)
        print(f"Source file was locked — saved copy to: {alt}")

    # ── Flat-file export ───────────────────────────────────────────
    today = date.today()
    fname = f"customer_{today.strftime('%y%m')}.csv"
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        out_path = os.path.join(OUTPUT_DIR, fname)
    except Exception as e:
        print(f"[flat] can't write to {OUTPUT_DIR}: {e}")
        out_path = os.path.join(os.getcwd(), fname)
        print(f"[flat] falling back to {out_path}")

    # Build lat/lon lookup from the (now-fresh) Location Master
    # so each Customer Master row carries the coords for its address.
    loc_by_key = {}
    for r in lm_rows:
        key = _row_key(r, lm_addr_idx)
        lat = r[lat_idx] if lat_idx < len(r) else None
        lon = r[lon_idx] if lon_idx < len(r) else None
        loc_by_key[key] = (lat, lon)

    # Column lookup from Customer Master for every FLAT_FILE_COLUMNS
    # entry.  Longitude / Latitude are joined in separately.
    cm_col_idx = {}
    for col in FLAT_FILE_COLUMNS:
        if col in ("Longitude", "Latitude"):
            continue
        idx = _lookup_header(cm_headers, CUSTOMER_COLUMN_ALIASES.get(col, [col]))
        cm_col_idx[col] = idx

    missing_cm_cols = [c for c, i in cm_col_idx.items() if i is None]
    if missing_cm_cols:
        print(f"[flat] WARNING — Customer Master is missing: {missing_cm_cols}")
        print("       Those columns in the output will be blank; add header")
        print("       aliases to CUSTOMER_COLUMN_ALIASES if your workbook uses")
        print("       different names.")

    written = 0
    with open(out_path, "w", newline="", encoding="utf-8") as f_out:
        w = csv.writer(f_out)
        w.writerow(FLAT_FILE_COLUMNS)
        for r in cm_rows:
            key = _row_key(r, cm_addr_idx)
            # Skip rows with no address at all — they're usually
            # "Delete" markers or empty footers.
            if all(k == "" for k in key):
                continue
            lat, lon = loc_by_key.get(key, (None, None))
            out_row = []
            for col in FLAT_FILE_COLUMNS:
                if col == "Latitude":
                    out_row.append("" if lat is None else lat)
                elif col == "Longitude":
                    out_row.append("" if lon is None else lon)
                else:
                    i = cm_col_idx.get(col)
                    v = (r[i] if (i is not None and i < len(r)) else "")
                    out_row.append("" if v is None else v)
            w.writerow(out_row)
            written += 1
    print(f"[flat] wrote {written} rows → {out_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nFATAL: {e}")
        import traceback; traceback.print_exc()
        sys.exit(1)
