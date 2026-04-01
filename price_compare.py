"""
price_compare.py
----------------
Reads the latest Tempe_*.csv and creates a brand price comparison Excel file.

Brand abbreviations (from comparison table):
  MC=Michelin, BS=Bridgestone, CT=Continental, GY=Goodyear,
  KH=Kumho, FK=Falken, HK=Hankook, LF=Laufenn
"""
import csv
import glob
import os
from openpyxl import Workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from openpyxl.formatting.rule import ColorScaleRule

# ── Brand mapping ─────────────────────────────────────────────────────────────
BRANDS = {
    "MC": "Michelin",
    "BS": "Bridgestone",
    "CT": "Continental",
    "GY": "Goodyear",
    "KH": "Kumho",
    "FK": "Falken",
    "HK": "Hankook",
    "LF": "Laufenn",
    "DL": "Dunlop",
    "YO": "Yokohama",
}

# Header colours per brand
BRAND_COLOURS = {
    "MC":  "C8102E",  # Michelin red
    "BS":  "E31837",  # Bridgestone red
    "CT":  "FFA500",  # Continental orange
    "GY":  "003087",  # Goodyear blue
    "KH":  "00539B",  # Kumho blue
    "FK":  "E8001A",  # Falken red
    "HK":  "FF6600",  # Hankook orange
    "LF":  "FF6600",  # Laufenn (Hankook sub-brand)
    "DL":  "E4002B",  # Dunlop red
    "YO":  "003DA5",  # Yokohama blue
}

ROW_FILLS = {
    "MC":  "FADADD", "BS":  "FFE0E0", "CT":  "FFF3CD",
    "GY":  "D6EAF8", "KH":  "D4E6F1", "FK":  "FCE4D6",
    "HK":  "FFF0E0", "LF":  "FDEBD0", "DL":  "FADBD8",
    "YO":  "D4E6F1",
}

# ── Helpers ───────────────────────────────────────────────────────────────────
def thin():
    s = Side(style="thin", color="CCCCCC")
    return Border(left=s, right=s, top=s, bottom=s)

def hdr_cell(ws, row, col, value, bg="1F4E79", fg="FFFFFF", bold=True, align="center"):
    c = ws.cell(row=row, column=col, value=value)
    c.fill   = PatternFill("solid", fgColor=bg)
    c.font   = Font(color=fg, bold=bold, size=11)
    c.alignment = Alignment(horizontal=align, vertical="center", wrap_text=True)
    c.border = thin()
    return c

def data_cell(ws, row, col, value, bg="FFFFFF", align="left", num_fmt=None):
    c = ws.cell(row=row, column=col, value=value)
    c.fill      = PatternFill("solid", fgColor=bg)
    c.font      = Font(size=10)
    c.alignment = Alignment(horizontal=align, vertical="center")
    c.border    = thin()
    if num_fmt:
        c.number_format = num_fmt
    return c

def abbr_for(brand_name):
    for abbr, name in BRANDS.items():
        if name.lower() == brand_name.strip().lower():
            return abbr
    return None

def autofit(ws):
    for col in ws.columns:
        w = max((len(str(c.value)) for c in col if c.value), default=8)
        ws.column_dimensions[get_column_letter(col[0].column)].width = min(w + 3, 55)

# ── Load CSV ──────────────────────────────────────────────────────────────────
def load_csv(path):
    rows = []
    with open(path, encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            rows.append(r)
    return rows

# ── Sheet 1: Comparison summary (one column per brand) ───────────────────────
def sheet_summary(wb, all_rows):
    ws = wb.create_sheet("Summary")
    ws.freeze_panes = "C3"

    brand_abbrs = list(BRANDS.keys())

    # Row 1: title
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=2 + len(brand_abbrs) * 2)
    t = ws.cell(row=1, column=1, value="175/65R14 — Brand Price Comparison  (COST / PRICE)")
    t.font      = Font(bold=True, size=13, color="FFFFFF")
    t.fill      = PatternFill("solid", fgColor="1F4E79")
    t.alignment = Alignment(horizontal="center", vertical="center")
    ws.row_dimensions[1].height = 28

    # Row 2: column headers
    hdr_cell(ws, 2, 1, "Size",        bg="2E4057")
    hdr_cell(ws, 2, 2, "Description", bg="2E4057")
    col = 3
    for abbr in brand_abbrs:
        ws.merge_cells(start_row=2, start_column=col, end_row=2, end_column=col+1)
        hdr_cell(ws, 2, col,   f"{abbr}\n{BRANDS[abbr]}",
                 bg=BRAND_COLOURS.get(abbr, "555555"))
        ws.cell(row=2, column=col+1).fill = PatternFill("solid",
                 fgColor=BRAND_COLOURS.get(abbr, "555555"))
        col += 2

    # Row 3: sub-headers
    hdr_cell(ws, 3, 1, "Size",        bg="334155", fg="CCCCCC", bold=False)
    hdr_cell(ws, 3, 2, "Description", bg="334155", fg="CCCCCC", bold=False)
    col = 3
    for abbr in brand_abbrs:
        hdr_cell(ws, 3, col,   "Cost",  bg="334155", fg="AAAAAA", bold=False)
        hdr_cell(ws, 3, col+1, "Price", bg="334155", fg="AAAAAA", bold=False)
        col += 2
    ws.row_dimensions[3].height = 18

    # Group rows by (size, description) — keep unique descriptions
    # For summary: one row per description, show cost/price in each brand column
    # Build lookup: (brand_abbr, description) → (cost, price)
    lookup = {}
    sizes_seen = []
    for r in all_rows:
        brand  = r.get("brand", "").strip()
        desc   = r.get("DESCRIPTION", "").strip()
        cost   = r.get("COST",  "").strip()
        price  = r.get("PRICE", "").strip()
        abbr   = abbr_for(brand)
        if not abbr:
            continue
        size_part = desc.split(" ")[0] if desc else ""
        key = (size_part, desc)
        if key not in lookup:
            lookup[key] = {}
            sizes_seen.append(key)
        lookup[key][abbr] = (cost, price)

    row_num = 4
    for (size_part, desc) in sizes_seen:
        bg = "F7F9FC" if row_num % 2 == 0 else "FFFFFF"
        data_cell(ws, row_num, 1, size_part, bg=bg, align="center")
        data_cell(ws, row_num, 2, desc,      bg=bg)
        col = 3
        for abbr in brand_abbrs:
            brand_bg = ROW_FILLS.get(abbr, "FFFFFF")
            entry = lookup.get((size_part, desc), {}).get(abbr)
            if entry:
                cost_val  = float(entry[0]) if entry[0] else None
                price_val = float(entry[1]) if entry[1] else None
                data_cell(ws, row_num, col,   cost_val,  bg=brand_bg, align="right", num_fmt="$#,##0.00")
                data_cell(ws, row_num, col+1, price_val, bg=brand_bg, align="right", num_fmt="$#,##0.00")
            else:
                data_cell(ws, row_num, col,   "-", bg="F0F0F0", align="center")
                data_cell(ws, row_num, col+1, "-", bg="F0F0F0", align="center")
            col += 2
        row_num += 1

    autofit(ws)
    ws.column_dimensions["B"].width = 45


# ── Sheet 2: Detail (all brands, sorted by price) ────────────────────────────
def sheet_detail(wb, all_rows):
    ws = wb.create_sheet("All Products")
    ws.freeze_panes = "A3"

    hdr_cell(ws, 1, 1, "Brand Abbr", bg="2E4057")
    hdr_cell(ws, 1, 2, "Brand",       bg="2E4057")
    hdr_cell(ws, 1, 3, "Description", bg="2E4057")
    hdr_cell(ws, 1, 4, "Cost ($)",    bg="2E4057")
    hdr_cell(ws, 1, 5, "Price ($)",   bg="2E4057")
    hdr_cell(ws, 1, 6, "Margin ($)",  bg="2E4057")
    hdr_cell(ws, 1, 7, "Margin %",    bg="2E4057")

    known_rows = []
    other_rows = []
    for r in all_rows:
        brand = r.get("brand", "").strip()
        abbr  = abbr_for(brand)
        if abbr:
            known_rows.append((abbr, r))
        else:
            other_rows.append(("--", r))

    known_rows.sort(key=lambda x: (x[0], float(x[1].get("PRICE","0") or 0)))

    all_out = known_rows + other_rows
    for i, (abbr, r) in enumerate(all_out):
        rn     = i + 2
        brand  = r.get("brand", "").strip()
        desc   = r.get("DESCRIPTION", "").strip()
        cost   = r.get("COST",  "").strip()
        price  = r.get("PRICE", "").strip()
        bg     = ROW_FILLS.get(abbr, "F9F9F9") if abbr != "--" else "F9F9F9"

        cost_f  = float(cost)  if cost  else None
        price_f = float(price) if price else None
        margin  = round(price_f - cost_f, 2)           if (cost_f and price_f) else None
        margin_pct = round(margin / price_f * 100, 1)  if (margin and price_f) else None

        data_cell(ws, rn, 1, abbr if abbr != "--" else "other", bg=bg, align="center")
        data_cell(ws, rn, 2, brand, bg=bg)
        data_cell(ws, rn, 3, desc,  bg=bg)
        data_cell(ws, rn, 4, cost_f,      bg=bg, align="right", num_fmt="$#,##0.00")
        data_cell(ws, rn, 5, price_f,     bg=bg, align="right", num_fmt="$#,##0.00")
        data_cell(ws, rn, 6, margin,      bg=bg, align="right", num_fmt="$#,##0.00")
        data_cell(ws, rn, 7, margin_pct,  bg=bg, align="right", num_fmt='0.0"%"')

    # Conditional colour scale on Price column
    last = len(all_out) + 1
    ws.conditional_formatting.add(
        f"E2:E{last}",
        ColorScaleRule(start_type="min", start_color="63BE7B",
                       mid_type="percentile", mid_value=50, mid_color="FFEB84",
                       end_type="max", end_color="F8696B")
    )
    autofit(ws)
    ws.column_dimensions["C"].width = 50


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    csv_files = sorted(glob.glob("Tempe_*.csv"), reverse=True)
    if not csv_files:
        print("ERROR: No Tempe_*.csv file found in current directory.")
        return

    csv_path = csv_files[0]
    print(f"Reading: {csv_path}")
    rows = load_csv(csv_path)
    print(f"  {len(rows)} rows loaded")

    wb = Workbook()
    wb.remove(wb.active)          # remove default sheet
    sheet_summary(wb, rows)
    sheet_detail(wb, rows)

    out = csv_path.replace(".csv", "_comparison.xlsx")
    wb.save(out)
    print(f"Saved:  {out}")
    os.startfile(out)             # auto-open in Excel (Windows)


if __name__ == "__main__":
    main()
