"""
convert_jax_txt.py
------------------
Converts JAX_*.txt pipe-delimited files to JAX_YYYYMMDD_0000.csv format
for use with price_compare.py / price_compare_jax.py.

Column positions are AUTO-DETECTED per file (size pattern locates the anchor,
then brand/description/price/disc/promo are found relative to it).

Expected filename formats:
  JAX_Mar_2026.txt  |  JAX_March_2026.txt  |  JAX_Feb.txt  |  JAX_Jan_2026.txt

Output: JAX_20260301_0000.csv  (one file per input, in same directory)

Usage:
    python convert_jax_txt.py
"""
import csv, os, re, sys
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

PROMO_KEYWORDS = ['buy', 'get', 'free', 'off', 'save', '%', 'deal', 'tyre free']


def get_year_month(filepath):
    """Extract (year, month_num) from filename like JAX_Mar_2026.txt."""
    name = os.path.splitext(os.path.basename(filepath))[0].upper()

    # Year: first 4-digit number in filename
    ym = re.search(r'(\d{4})', name)
    year = ym.group(1) if ym else str(datetime.now().year)

    # Month: longest matching month name/abbr
    month_num = '01'
    best_len = 0
    for key, num in MONTH_MAP.items():
        if key in name and len(key) > best_len:
            month_num = num
            best_len = len(key)

    return year, month_num


def parse_line(line):
    """Parse one pipe-delimited line. Returns dict or None."""
    cols = line.rstrip('\n').split('|')
    if len(cols) < 4:
        return None

    # ── Find SIZE column ──────────────────────────────────
    size_col = next((i for i, c in enumerate(cols) if SIZE_RE.match(c.strip())), -1)
    if size_col == -1:
        return None
    size = SIZE_RE.match(cols[size_col].strip()).group(1).upper()

    # ── Find BRAND column (scan left of size) ─────────────
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

    # ── DESCRIPTION: column right after brand (full product name) ──
    desc = ''
    if brand_col + 1 < size_col:
        desc = cols[brand_col + 1].strip()
    if not desc or len(desc) < 3:
        # fallback: longest non-numeric column between brand and size
        candidates = [(len(c.strip()), c.strip())
                      for c in cols[brand_col+1:size_col]
                      if c.strip() and not IS_NUM_RE.match(c.strip())]
        if candidates:
            desc = max(candidates)[1]

    # ── PRICE / DISC_PRICE: first two numeric values after size ──
    numerics = []
    for i in range(size_col + 1, len(cols)):
        c = cols[i].strip()
        if IS_NUM_RE.match(c) and float(c) > 0:
            numerics.append(float(c))
        if len(numerics) == 2:
            break

    price = f"{numerics[0]:.2f}" if numerics else ''
    disc  = f"{numerics[1]:.2f}" if len(numerics) >= 2 else ''

    # ── PROMO: last column containing promo keywords ──────
    promo = ''
    for c in reversed(cols[size_col:]):
        c = c.strip()
        if any(kw in c.lower() for kw in PROMO_KEYWORDS):
            promo = c
            break

    # If "BUY 4 & GET 4TH FREE" but disc is missing, compute as 75% of price
    if promo and 'get 4th' in promo.lower() and price and not disc:
        disc = f"{float(price) * 0.75:.2f}"

    if not price:
        return None

    return {'size': size, 'brand': brand, 'desc': desc,
            'price': price, 'disc': disc, 'promo': promo}


def convert(txt_path):
    year, month = get_year_month(txt_path)
    out_name = f"JAX_{year}{month}01_0000.csv"
    out_path = os.path.join(_BASE, out_name)

    rows = []
    skipped = 0
    with open(txt_path, encoding='utf-8-sig', errors='replace') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            r = parse_line(line)
            if r:
                rows.append(r)
            else:
                skipped += 1

    with open(out_path, 'w', newline='', encoding='utf-8-sig') as f:
        w = csv.writer(f)
        w.writerow(['SIZE', 'brand', 'DESCRIPTION', 'PRICE', 'DISC_PRICE', 'PROMO'])
        for r in rows:
            w.writerow([r['size'], r['brand'], r['desc'],
                        r['price'], r['disc'], r['promo']])

    print(f"  {os.path.basename(txt_path):30s} → {out_name}  "
          f"({len(rows)} rows, {skipped} skipped)")
    return len(rows)


def main():
    txt_files = sorted(
        f for f in os.listdir(_BASE)
        if re.match(r'JAX_', f, re.IGNORECASE) and f.lower().endswith('.txt')
    )
    if not txt_files:
        print("No JAX_*.txt files found in", _BASE)
        return

    print(f"\nConverting {len(txt_files)} file(s)...\n")
    total = 0
    for fname in txt_files:
        total += convert(os.path.join(_BASE, fname))
    print(f"\nDone. {total} total rows converted.")
    print("Refresh http://127.0.0.1:5000/price to see updated data.\n")


if __name__ == '__main__':
    main()
