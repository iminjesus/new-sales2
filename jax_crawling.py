"""
jax_crawling.py
---------------
Scrapes all tyre prices from https://www.jaxtyres.com.au
for the brands in the Tempe price-comparison list.

Strategy: URL-based pagination, pagesize=45 (maximum per page).
Page URL template:
  https://www.jaxtyres.com.au/tyres/{brand}
    ?searchqrytype=Brand&isrunflat=False&searchtype=All
    &sorttype=PriceAsc&pagesize=45&pagenumber={N}

Output: JAX_YYYYMMDD_HHMM.csv
Columns: SIZE | brand | DESCRIPTION | PRICE | DISC_PRICE | PROMO

  PRICE      — regular per-tyre price
  DISC_PRICE — "OR BUY 4 FOR $X" per-tyre price (blank if no bulk deal)

Usage:
    python jax_crawling.py
"""
import re
import csv
import math
import time
from datetime import datetime
from selenium import webdriver

PAGE_URL = (
    "https://www.jaxtyres.com.au/tyres/{brand}"
    "?searchqrytype=Brand&isrunflat=False&searchtype=All"
    "&sorttype=PriceAsc&pagesize=45&pagenumber={page}"
)
OUTPUT_FILE = datetime.now().strftime("JAX_%Y%m%d_%H%M.csv")
PAGE_SIZE   = 45

# abbr → (display name, JAX URL slug)
# Verify slugs at: https://www.jaxtyres.com.au/tyres/<slug>
BRANDS = {
    "MC":  ("Michelin",     "michelin"),
    "BS":  ("Bridgestone",  "bridgestone"),
    "CT":  ("Continental",  "continental"),
    "GY":  ("Goodyear",     "goodyear"),
    "KH":  ("Kumho",        "kumho"),
    "FK":  ("Falken",       "falken"),
    "HK":  ("Hankook",      "hankook"),
    "LF":  ("Laufenn",      "laufenn"),
    "DL":  ("Dunlop",       "dunlop"),
    "YO":  ("Yokohama",     "yokohama"),
}


def init_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--window-size=1400,900")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    driver = webdriver.Chrome(options=options)
    driver.set_script_timeout(60)
    return driver


# ── JS snippets ───────────────────────────────────────────────────────────────

_JS_TOTAL = r"""
var t = (document.body.innerText || document.body.textContent || '');
var m = t.match(/\d[\d,]*\s*[-\u2013]\s*\d[\d,]*\s+of\s+([\d,]+)\s+items/i);
if (m) return parseInt(m[1].replace(/,/g,''));
var m2 = t.match(/of\s+([\d,]+)\s+items/i);
if (m2) return parseInt(m2[1].replace(/,/g,''));
return -1;
"""

# Extracts all product cards from the current page.
# Returns array of {name, spec, size, price, disc_price, promo}.
_JS_EXTRACT = r"""
return (function() {
    var results = [];
    var seen = {};

    /* ── Find product cards ────────────────────────────────────────────────
       Try selectors from most specific to least; stop on first hit with ≥3. */
    var SELECTORS = [
        'div.product-listing-item',
        'div[class*="ProductItem"]',
        'div[class*="product-item"]',
        'li[class*="product"]',
        'article[class*="product"]',
        'div[class*="tyre-card"]',
        'div[class*="TyreCard"]',
    ];
    var cards = null;
    for (var si = 0; si < SELECTORS.length; si++) {
        var found = document.querySelectorAll(SELECTORS[si]);
        if (found && found.length >= 3) { cards = found; break; }
    }
    if (!cards || !cards.length) return results;

    for (var ci = 0; ci < cards.length; ci++) {
        var card = cards[ci];
        var text = (card.textContent || '').replace(/\s+/g, ' ').trim();
        if (!text) continue;

        /* ── Product name ────────────────────────────────────────────────── */
        var name = '';
        var hEl = card.querySelector('h2,h3,h4');
        if (hEl) {
            name = (hEl.textContent || '').trim();
        }
        if (!name) {
            var links = card.querySelectorAll('a');
            for (var li = 0; li < links.length; li++) {
                var lt = (links[li].textContent || '').trim();
                if (lt.length > 5 && !/add|booking|detail|view/i.test(lt)) {
                    name = lt; break;
                }
            }
        }

        /* ── Tyre spec: "175/65R14 82H XL" ─────────────────────────────── */
        var spec = '';
        var size = '';

        /* Try to find spec in a child <p> or <span> near the size pattern  */
        var specEl = card.querySelector('p,span');
        if (specEl) {
            var st = (specEl.textContent || '').trim();
            if (/\d{3}\/\d{2}[A-Za-z]\d{2}/.test(st)) spec = st;
        }
        /* Fallback: extract from full card text */
        if (!spec) {
            var sm = text.match(/(\d{3}\/\d{2}[A-Za-z]\d{2}[A-Za-z0-9 ]*?)(?=\s*\$|\s+[Aa]dd|\s+[Vv]iew|\s+[Qq]ty|$)/);
            if (sm) spec = sm[1].trim();
        }
        /* Old-format size: "185R14" */
        if (!spec) {
            var om = text.match(/(\d{3}R\d{2}[A-Za-z0-9 ]*?)(?=\s*\$|\s+[Aa]dd|\s+[Vv]iew|$)/);
            if (om) spec = om[1].trim();
        }
        /* Extract clean size from spec */
        var szm = spec.match(/(\d{3}\/\d{2}[A-Za-z]\d{2})/);
        if (szm) {
            size = szm[1].toUpperCase();
        } else {
            var szm2 = spec.match(/(\d{3}R\d{2})/i);
            if (szm2) size = szm2[1].toUpperCase();
        }

        /* ── Regular price: first $ amount before any "OR BUY" ──────────── */
        var price = '';
        var buyPos = text.toUpperCase().indexOf('OR BUY');
        var priceSearchIn = buyPos > 0 ? text.substring(0, buyPos) : text;
        var pm = priceSearchIn.match(/\$([\d,]+(?:\.\d+)?)/);
        if (pm) price = pm[1].replace(/,/g, '');

        /* ── Bulk / promo price ──────────────────────────────────────────── */
        var disc_price = '';
        var promo = '';

        /* "OR BUY 4 FOR $122.50ea" */
        var b4m = text.match(/OR\s+BUY\s+4\s+FOR\s+\$([\d,]+(?:\.\d+)?)/i);
        if (b4m) {
            disc_price = b4m[1].replace(/,/g, '');
            promo = 'OR BUY 4 FOR $' + b4m[1];
        }
        /* "BUY 4 & GET 4TH TYRE FREE" */
        if (!promo && /BUY\s+4\s*[&+]\s*GET\s+4TH\s+TYRE\s+FREE/i.test(text)) {
            promo = 'Buy 4 Get 4th Free';
            /* effective per-tyre = price × 3/4 */
            if (price) disc_price = (Math.round(parseFloat(price) * 0.75 * 100) / 100).toFixed(2);
        }

        if (!name && !price) continue;

        var key = (name + '|' + spec).toLowerCase();
        if (seen[key]) continue;
        seen[key] = 1;

        results.push({
            name: name, spec: spec, size: size,
            price: price, disc_price: disc_price, promo: promo
        });
    }
    return results;
}());
"""


# ── Python helpers ────────────────────────────────────────────────────────────

def get_total(driver):
    try:
        n = driver.execute_script(_JS_TOTAL)
        return int(n) if n and n > 0 else -1
    except Exception:
        return -1


def extract_page(driver):
    try:
        raw = driver.execute_script(_JS_EXTRACT)
        return raw or []
    except Exception as e:
        print(f"  [JS error] {e}")
        return []


def process_raw(raw_items, brand_name):
    rows = []
    for item in raw_items:
        name      = (item.get("name")       or "").strip()
        spec      = (item.get("spec")       or "").strip()
        size      = (item.get("size")       or "").strip().upper()
        price_s   = (item.get("price")      or "").strip()
        disc_s    = (item.get("disc_price") or "").strip()
        promo     = (item.get("promo")      or "").strip()

        # Build description: "Advantage Touring 82T XL" (name + load-spec)
        desc_extra = re.sub(re.escape(size), "", spec, flags=re.IGNORECASE).strip(" -–") if size else spec
        desc = f"{name} {desc_extra}".strip()

        price = f"{float(price_s):.2f}" if price_s else ""
        disc  = f"{float(disc_s):.2f}"  if disc_s  else ""

        if not size or not desc:
            continue

        rows.append({
            "size": size, "brand": brand_name, "desc": desc,
            "price": price, "disc": disc, "promo": promo,
        })
    return rows


def write_rows(writer, rows):
    for r in rows:
        writer.writerow([r["size"], r["brand"], r["desc"],
                         r["price"], r["disc"], r["promo"]])
        line = f"  {r['brand']:<14} | {r['size']:<12} | {r['desc'][:38]:<38} | ${r['price']}"
        if r["disc"]:
            line += f"  → ${r['disc']}"
        print(line)


def scrape_brand(driver, abbr, brand_name, slug, writer, f_out):
    print(f"\n{'='*65}")
    print(f"  {abbr}  {brand_name}  (slug: {slug})")
    print(f"{'='*65}")

    url1 = PAGE_URL.format(brand=slug, page=1)
    driver.get(url1)
    time.sleep(4)

    total = get_total(driver)
    if total <= 0:
        print(f"  WARNING: could not read item count (total={total}). Assuming 1 page.")
        total = PAGE_SIZE
    total_pages = math.ceil(total / PAGE_SIZE)
    print(f"  {total} items → {total_pages} page(s)\n")

    brand_total = 0
    for page in range(1, total_pages + 1):
        if page > 1:
            driver.get(PAGE_URL.format(brand=slug, page=page))
            time.sleep(3)

        raw  = extract_page(driver)
        rows = process_raw(raw, brand_name)
        write_rows(writer, rows)
        f_out.flush()
        brand_total += len(rows)
        print(f"  ── page {page}/{total_pages}: {len(rows)} products  (subtotal: {brand_total})")

    return brand_total


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    driver = init_driver()
    print(f"\nOutput: {OUTPUT_FILE}")
    print("JAX Tyres crawler — brand-by-brand, URL pagination (pagesize=45)\n")

    grand_total = 0

    try:
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(["SIZE", "brand", "DESCRIPTION",
                             "PRICE", "DISC_PRICE", "PROMO"])

            for abbr, (brand_name, slug) in BRANDS.items():
                try:
                    count = scrape_brand(driver, abbr, brand_name, slug, writer, f)
                    grand_total += count
                    print(f"\n  ✓ {brand_name}: {count} products saved")
                except Exception as e:
                    print(f"\n  ✗ {brand_name}: ERROR — {e}")

        print(f"\n{'='*65}")
        print(f"Done. {grand_total} total products → {OUTPUT_FILE}")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
