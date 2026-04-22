"""
convert_tempe_txt.py
--------------------
Converts tempe_*.txt pipe-delimited files to Tempe_YYYYMM01_0000.csv
for use with price_compare.py.

Expected input (pipe-delimited):
  Month | Tempe | Brand | FullPattern | ShortPattern | ... | SKU | Size | Price | ... | LoadSpeed | Promo

Expected filename formats:
  tempe_jan.txt  |  tempe_feb.txt  |  tempe_mar.txt  |  Tempe_Jan_2026.txt

Output: Tempe_20260301_0000.csv  (one file per input, same directory)

Usage:
    python convert_tempe_txt.py
"""
import csv, os, re
from datetime import datetime

_BASE = os.path.dirname(os.path.abspath(__file__))

SIZE_RE   = re.compile(r'(\d{3}/\d{2}[A-Za-z]\d{2}|\d{3}R\d{2})', re.IGNORECASE)
IS_NUM_RE = re.compile(r'^\d+(\.\d+)?$')

BRAND_MAP = {
    'MICHELIN':    'Michelin',
    'BRIDGESTONE': 'Bridgestone',
    'CONTINENTAL': 'Continental',
    'GOODYEAR':    'Goodyear',
    'FALKEN':      'Falken',
    'HANKOOK':     'Hankook',
    'LAUFENN':     'Laufenn',
    'DUNLOP':      'Dunlop',
    'KUMHO':       'Kumho',
    'YOKOHAMA':    'Yokohama',
}

MONTH_MAP = {
    'JAN':'01','JANUARY':'01',
    'FEB':'02','FEBRUARY':'02',
    'MAR':'03','MARCH':'03',
    'APR':'04','APRIL':'04',
    'MAY':'05',
    'JUN':'06','JUNE':'06',
    'JUL':'07','JULY':'07',
    'AUG':'08','AUGUST':'08',
    'SEP':'09','SEPTEMBER':'09',
    'OCT':'10','OCTOBER':'10',
    'NOV':'11','NOVEMBER':'11',
    'DEC':'12','DECEMBER':'12',
}


def get_year_month(filepath, first_line=None):
    """Get (year, month_num) from filename, falling back to first data row."""
    name = os.path.splitext(os.path.basename(filepath))[0].upper()

    ym = re.search(r'(\d{4})', name)
    year = ym.group(1) if ym else str(datetime.now().year)

    month_num = None
    best_len = 0
    for key, num in MONTH_MAP.items():
        if key in name and len(key) > best_len:
            month_num = num
            best_len = len(key)

    # Month not in filename — read from first column of first data row
    if not month_num and first_line:
        col0 = first_line.split('|')[0].strip().upper()
        for key, num in MONTH_MAP.items():
            if col0 == key and len(key) > best_len:
                month_num = num
                best_len = len(key)

    return year, month_num or '01'


def parse_line(line):
    cols = line.rstrip('\n').split('|')
    if len(cols) < 4:
        return None

    # Find SIZE column
    size_col = next((i for i, c in enumerate(cols) if SIZE_RE.match(c.strip())), -1)
    if size_col == -1:
        return None
    size = SIZE_RE.match(cols[size_col].strip()).group(1).upper()

    # Find BRAND column (scan left of size)
    brand = None
    brand_col = -1
    for i in range(size_col):
        key = cols[i].strip().upper()
        if key in BRAND_MAP:
            brand = BRAND_MAP[key]
            brand_col = i
            break
    if not brand:
        return None

    # DESCRIPTION: column immediately after brand (full pattern name)
    desc = ''
    if brand_col + 1 < size_col:
        desc = cols[brand_col + 1].strip()
    if not desc or len(desc) < 3:
        candidates = [(len(c.strip()), c.strip())
                      for c in cols[brand_col + 1:size_col]
                      if c.strip() and not IS_NUM_RE.match(c.strip())]
        if candidates:
            desc = max(candidates)[1]

    # PRICE: first positive numeric after size
    price = ''
    for i in range(size_col + 1, len(cols)):
        c = cols[i].strip()
        if IS_NUM_RE.match(c) and float(c) > 0:
            price = f"{float(c):.2f}"
            break

    if not price:
        return None

    # PROMO: last column — blank out "No Promotion"
    promo = ''
    last = cols[-1].strip()
    if last and not re.match(r'no\s*promotion', last, re.IGNORECASE):
        promo = last

    return {'size': size, 'brand': brand, 'desc': desc, 'price': price, 'promo': promo}


def convert(txt_path):
    with open(txt_path, encoding='utf-8-sig', errors='replace') as f:
        lines = [l.strip() for l in f if l.strip()]

    if not lines:
        print(f"  {os.path.basename(txt_path):30s} — empty, skipped")
        return 0

    year, month = get_year_month(txt_path, lines[0])
    out_name = f"Tempe_{year}{month}01_0000.csv"
    out_path = os.path.join(_BASE, out_name)

    rows, skipped = [], 0
    for line in lines:
        r = parse_line(line)
        if r:
            rows.append(r)
        else:
            skipped += 1

    with open(out_path, 'w', newline='', encoding='utf-8-sig') as f:
        w = csv.writer(f)
        w.writerow(['SIZE', 'brand', 'DESCRIPTION', 'PRICE', 'DISC_PRICE', 'PROMO'])
        for r in rows:
            w.writerow([r['size'], r['brand'], r['desc'], r['price'], '', r['promo']])

    print(f"  {os.path.basename(txt_path):30s} → {out_name}  "
          f"({len(rows)} rows, {skipped} skipped)")
    return len(rows)


def main():
    txt_files = sorted(
        f for f in os.listdir(_BASE)
        if re.match(r'tempe_', f, re.IGNORECASE) and f.lower().endswith('.txt')
    )
    if not txt_files:
        print("No tempe_*.txt files found in", _BASE)
        return

    print(f"\nConverting {len(txt_files)} file(s)...\n")
    total = 0
    for fname in txt_files:
        total += convert(os.path.join(_BASE, fname))
    print(f"\nDone. {total} total rows converted.")
    print("Refresh http://127.0.0.1:5000/price to see updated data.\n")


if __name__ == '__main__':
    main()
