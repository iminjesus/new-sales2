"""
tempe_crawling.py
-----------------
Scrapes all tyre prices from https://www.tempetyres.com.au
for the brands used in the Tempe vs BJ vs JAX price comparison.

URL pattern:
  https://www.tempetyres.com.au/tyres?Brand={brand}&page={N}

Output: Tempe_YYYYMMDD_HHMM.csv
Columns: SIZE | brand | DESCRIPTION | COST | PRICE

  SIZE        — tyre size  (e.g. 225/45R17)
  brand       — brand name (e.g. Michelin)
  DESCRIPTION — product description
  COST        — blank (retail site, no cost data)
  PRICE       — retail price per tyre

Usage:
    python tempe_crawling.py
"""
import re
import csv
import time
from datetime import datetime
from selenium import webdriver

BASE_URL    = "https://www.tempetyres.com.au/tyres?Brand={brand}&page={page}"
OUTPUT_FILE = datetime.now().strftime("Tempe_%Y%m%d_%H%M.csv")
PAGE_SIZE   = 24   # estimated; adjusted dynamically from first page count

# abbr → (display name, URL brand slug)
BRANDS = {
    "MC": ("Michelin",    "Michelin"),
    "BS": ("Bridgestone", "Bridgestone"),
    "CT": ("Continental", "Continental"),
    "GY": ("Goodyear",    "Goodyear"),
    "FK": ("Falken",      "Falken"),
    "HK": ("Hankook",     "Hankook"),
    "LF": ("Laufenn",     "Laufenn"),
    "DL": ("Dunlop",      "Dunlop"),
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


# ── JS: read total item count from page text ──────────────────────────────────
_JS_TOTAL = r"""
var t = document.body.innerText || document.body.textContent || '';
// "Showing 1-24 of 87 results"  or  "1 to 24 of 87"  or  "87 products"
var m = t.match(/\d[\d,]*\s*[-–to]+\s*\d[\d,]*\s+of\s+([\d,]+)/i);
if (m) return parseInt(m[1].replace(/,/g,''));
var m2 = t.match(/of\s+([\d,]+)\s*(results|products|items)/i);
if (m2) return parseInt(m2[1].replace(/,/g,''));
var m3 = t.match(/([\d,]+)\s*(results|products|items)/i);
if (m3) return parseInt(m3[1].replace(/,/g,''));
return -1;
"""

# ── JS: DOM diagnostic when 0 products found ──────────────────────────────────
_JS_DIAGNOSE = r"""
return (function() {
    var info = { url: location.href, title: document.title };

    var body = document.body.textContent || '';
    var pm = body.match(/\$\d{2,4}/g);
    info.price_count = pm ? pm.length : 0;
    var sm = body.match(/\d{3}\/\d{2}[A-Za-z]\d{2}/g);
    info.size_count = sm ? sm.length : 0;

    var divs = document.querySelectorAll('div');
    var seenCls = {};
    info.div_classes = [];
    for (var i = 0; i < Math.min(divs.length, 400); i++) {
        var cls = divs[i].className;
        if (cls && typeof cls === 'string' && !seenCls[cls] &&
                cls.length > 3 && cls.length < 120) {
            seenCls[cls] = 1;
            info.div_classes.push(cls);
            if (info.div_classes.length >= 40) break;
        }
    }

    // Dump first element containing both a size pattern and a price
    info.first_match_text = '';
    for (var di = 0; di < Math.min(divs.length, 500); di++) {
        var t = divs[di].textContent || '';
        if (/\d{3}\/\d{2}[A-Za-z]\d{2}/.test(t) && /\$\d{2,4}/.test(t) && t.length < 400) {
            info.first_match_text = t.replace(/\s+/g,' ').substring(0, 300);
            info.first_match_class = divs[di].className || '(no class)';
            break;
        }
    }
    return info;
}());
"""

# ── JS: extract product cards ──────────────────────────────────────────────────
# Strategy:
#   1. Walk up from any element containing both a size pattern AND a price
#      until we find a minimal ancestor containing both.
#   2. Fallback: common CSS selectors for product listing cards.
_JS_EXTRACT = r"""
return (function() {
    var results = [];
    var seen = {};
    var BRAND_RE = /^(?:michelin|bridgestone|continental|goodyear|falken|hankook|laufenn|dunlop|kumho|yokohama)\s*/i;

    function extractCard(card) {
        var text = (card.textContent || '').replace(/\s+/g, ' ').trim();

        // Size
        var size = '';
        var szm = text.match(/(\d{3}\/\d{2}[A-Za-z]\d{2,3}(?:C)?(?:\s+XL)?)/i);
        if (!szm) szm = text.match(/(\d{3}R\d{2,3}(?:C)?)/i);
        if (szm) size = szm[1].replace(/\s+/g,'').toUpperCase();

        // Spec (size + load/speed)
        var spec = size;
        if (size) {
            var esc = size.replace(/[.*+?^${}()|[\]\\]/g,'\\$&');
            var lm  = text.match(new RegExp(esc + '\\s+(\\d{2,3}(?:\\/\\d{2,3})?[A-Za-z]{1,2}(?:\\s+XL)?)', 'i'));
            if (lm) spec = size + ' ' + lm[1].trim();
        }

        // Description: product model name — text AFTER size+spec, before "IN STOCK" / price
        var name = '';
        if (spec) {
            var specIdx = text.indexOf(spec);
            if (specIdx >= 0) {
                var afterSpec = text.substring(specIdx + spec.length).trim();
                var stopM = afterSpec.match(/\d+\+?\s*in\s*stock|\bIN\s*STOCK\b|\$\d{2,4}|price\s+includes/i);
                if (stopM && stopM.index > 0) {
                    name = afterSpec.substring(0, stopM.index).replace(BRAND_RE, '').trim();
                }
            }
        }
        // Fallback: PATTERN row from the detail table
        if (!name) {
            var pm2 = text.match(/\bPATTERN\s+([A-Z0-9][^\n$]{2,60}?)(?=\s+(?:Price|SIZE|LOAD|SPEED|\$|$))/i);
            if (pm2) name = pm2[1].trim();
        }

        // Price
        var price = '';
        var wasPosM = text.match(/was\s+\$[\d,.]+/i);
        var searchIn = wasPosM ? text.substring(wasPosM.index + wasPosM[0].length) : text;
        var buyPos = searchIn.toUpperCase().indexOf('OR BUY');
        if (buyPos > 0) searchIn = searchIn.substring(0, buyPos);
        var pm = searchIn.match(/\$([\d,]+(?:\.\d+)?)/);
        if (pm) price = pm[1].replace(/,/g,'');

        // SAVE_TEXT: detect SALE badge on the card
        var saveText = '';
        var saleBadge = card.querySelector('[class*="sale"],[class*="Sale"],[class*="badge"],[class*="tag"]');
        if (saleBadge) {
            var bt = (saleBadge.textContent || '').trim();
            if (bt.length > 0 && bt.length < 30 && /sale|save|\d+%/i.test(bt)) saveText = bt;
        }
        if (!saveText && /^\s*SALE\b/i.test(text)) saveText = 'SALE';

        return { name: name, size: size, price: price, saveText: saveText };
    }

    function addCard(card) {
        var key = (card.textContent || '').replace(/\s+/g,' ').substring(0, 120);
        if (seen[key]) return;
        seen[key] = 1;
        var item = extractCard(card);
        if (item.size && item.price) results.push(item);
    }

    // Strategy 1: find minimal div/li that contains both a size pattern and a price
    var all = document.querySelectorAll('div,li,article');
    var candidates = [];
    for (var i = 0; i < all.length; i++) {
        var t = all[i].textContent || '';
        if (/\d{3}\/\d{2}[A-Za-z]\d{2}/.test(t) && /\$\d{2,4}/.test(t)) {
            candidates.push(all[i]);
        }
    }
    // Keep only the smallest elements containing both size+price (true leaf cards):
    // an element is a leaf if it doesn't contain any other candidate.
    for (var ci = 0; ci < candidates.length; ci++) {
        var el = candidates[ci];
        var isLeaf = true;
        for (var cj = 0; cj < candidates.length; cj++) {
            if (cj !== ci && el.contains(candidates[cj])) {
                isLeaf = false;
                break;
            }
        }
        if (isLeaf) addCard(el);
    }
    if (results.length >= 3) return results;

    // Strategy 2: CSS class selectors
    var SELS = [
        'div[class*="product-item"]', 'div[class*="ProductItem"]',
        'div[class*="tyre-card"]',    'div[class*="TyreCard"]',
        'div[class*="listing-item"]', 'div[class*="product-card"]',
        'div[class*="ProductCard"]',  'li[class*="product"]',
        'article[class*="product"]',  'div[class*="item-card"]',
    ];
    for (var si = 0; si < SELS.length; si++) {
        var cards = document.querySelectorAll(SELS[si]);
        if (cards && cards.length >= 3) {
            for (var j = 0; j < cards.length; j++) addCard(cards[j]);
            if (results.length >= 3) break;
        }
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


def diagnose(driver):
    try:
        info = driver.execute_script(_JS_DIAGNOSE)
        if not info:
            print("  [DIAG] No info returned")
            return
        print(f"  [DIAG] URL:         {info.get('url','')}")
        print(f"  [DIAG] Title:       {info.get('title','')}")
        print(f"  [DIAG] price_count: {info.get('price_count',0)}")
        print(f"  [DIAG] size_count:  {info.get('size_count',0)}")
        fc = info.get('first_match_class','')
        ft = info.get('first_match_text','')
        if fc or ft:
            print(f"  [DIAG] first card class: {fc}")
            print(f"  [DIAG] first card text:  {ft}")
        print(f"  [DIAG] div classes (first 40):")
        for cls in (info.get('div_classes') or []):
            print(f"           {cls}")
    except Exception as e:
        print(f"  [DIAG error] {e}")


def extract_page(driver, run_diag=False):
    try:
        raw = driver.execute_script(_JS_EXTRACT)
        if not raw and run_diag:
            print("  [0 products — running DOM diagnostic]")
            diagnose(driver)
        return raw or []
    except Exception as e:
        print(f"  [JS error] {e}")
        return []


def process_raw(raw_items, brand_name):
    rows = []
    for item in raw_items:
        size      = (item.get("size")     or "").strip().upper()
        name      = (item.get("name")     or "").strip()
        price     = (item.get("price")    or "").strip()
        save_text = (item.get("saveText") or "").strip()

        if not size or not price:
            continue

        price_fmt = f"{float(price):.2f}" if price else ""

        rows.append({
            "size":      size,
            "brand":     brand_name,
            "desc":      name,
            "price":     price_fmt,
            "save_text": save_text,
        })
    return rows


def write_rows(writer, rows):
    for r in rows:
        # SIZE | brand | DESCRIPTION | price | disc_price | PROMO | SAVE_TEXT
        writer.writerow([r["size"], r["brand"], r["desc"],
                         r["price"], "", "", r["save_text"]])
        line = f"  {r['brand']:<14} | {r['size']:<13} | {r['desc'][:40]:<40} | ${r['price']}"
        if r["save_text"]:
            line += f"  [{r['save_text']}]"
        print(line)


def scrape_brand(driver, abbr, brand_name, url_slug, writer, f_out):
    print(f"\n{'='*68}")
    print(f"  {abbr}  {brand_name}  (URL slug: {url_slug})")
    print(f"{'='*68}")

    url1 = BASE_URL.format(brand=url_slug, page=1)
    driver.get(url1)
    time.sleep(4)

    total = get_total(driver)
    page1_raw = extract_page(driver, run_diag=(total <= 0))
    page1_rows = process_raw(page1_raw, brand_name)

    # Estimate pages from first page count + total
    first_page_count = len(page1_rows)
    if total <= 0 and first_page_count > 0:
        # Can't read total — try page 2; if empty, only 1 page
        total = first_page_count   # will recalculate below
    elif total <= 0:
        print(f"  WARNING: 0 products on page 1 and could not read total.")
        return 0

    effective_page_size = first_page_count if first_page_count > 0 else PAGE_SIZE
    if total > 0 and first_page_count > 0:
        import math
        total_pages = math.ceil(total / effective_page_size)
    else:
        total_pages = 1

    print(f"  {total} items  ·  {first_page_count} on page 1  →  {total_pages} page(s)\n")

    write_rows(writer, page1_rows)
    f_out.flush()
    brand_total = first_page_count
    print(f"  ── page 1/{total_pages}: {first_page_count} products  (subtotal: {brand_total})")

    for page in range(2, total_pages + 1):
        driver.get(BASE_URL.format(brand=url_slug, page=page))
        time.sleep(3)

        raw  = extract_page(driver)
        rows = process_raw(raw, brand_name)

        if not rows:
            print(f"  ── page {page}/{total_pages}: 0 products — stopping early")
            break

        write_rows(writer, rows)
        f_out.flush()
        brand_total += len(rows)
        print(f"  ── page {page}/{total_pages}: {len(rows)} products  (subtotal: {brand_total})")

    return brand_total


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    driver = init_driver()
    print(f"\nOutput: {OUTPUT_FILE}")
    print("Tempe Tyres retail crawler — brand-by-brand, URL pagination\n")

    grand_total = 0

    try:
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(["SIZE", "brand", "DESCRIPTION", "price", "disc_price", "PROMO", "SAVE_TEXT"])

            for abbr, (brand_name, url_slug) in BRANDS.items():
                try:
                    count = scrape_brand(driver, abbr, brand_name, url_slug, writer, f)
                    grand_total += count
                    print(f"\n  ✓ {brand_name}: {count} products saved")
                except Exception as e:
                    print(f"\n  ✗ {brand_name}: ERROR — {e}")
                    import traceback; traceback.print_exc()

        print(f"\n{'='*68}")
        print(f"Done. {grand_total} total products → {OUTPUT_FILE}")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
