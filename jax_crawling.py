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
    "FK":  ("Falken",       "falken"),
    "HK":  ("Hankook",      "hankook"),
    "LF":  ("Laufenn",      "laufenn"),
    "DL":  ("Dunlop",       "dunlop"),
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

# Diagnostic: run when 0 products found to identify correct selectors.
_JS_DIAGNOSE = r"""
return (function() {
    var info = { url: location.href, title: document.title };

    /* Count "View Product Details" links (reliable card marker) */
    var detail_count = 0;
    var links = document.querySelectorAll('a');
    for (var i = 0; i < links.length; i++) {
        if (/view\s+product\s+details/i.test(links[i].textContent || '')) detail_count++;
    }
    info.detail_links = detail_count;

    /* Count price / size patterns in body text */
    var body = document.body.textContent || '';
    var pm = body.match(/\$\d{2,4}/g);
    info.price_count = pm ? pm.length : 0;
    var sm = body.match(/\d{3}\/\d{2}[A-Za-z]\d{2}/g);
    info.size_count  = sm ? sm.length : 0;

    /* Collect unique div class names (first 300 divs) */
    var divs = document.querySelectorAll('div');
    var seenCls = {};
    info.div_classes = [];
    for (var i = 0; i < Math.min(divs.length, 300); i++) {
        var cls = divs[i].className;
        if (cls && typeof cls === 'string' && !seenCls[cls] &&
                cls.length > 3 && cls.length < 120) {
            seenCls[cls] = 1;
            info.div_classes.push(cls);
            if (info.div_classes.length >= 30) break;
        }
    }

    /* Dump first card's raw textContent so we can see structure */
    info.first_card_text = '';
    for (var li = 0; li < links.length; li++) {
        if (/view\s+product\s+details/i.test(links[li].textContent || '')) {
            var el = links[li];
            for (var lvl = 0; lvl < 10; lvl++) {
                el = el.parentElement;
                if (!el || el === document.body) break;
                var t = el.textContent || '';
                if (/\d{3}\/\d{2}[A-Za-z]\d{2}/.test(t) && /\$\d{2,4}/.test(t)) {
                    info.first_card_text = t.replace(/\s+/g,' ').substring(0, 300);
                    break;
                }
            }
            if (info.first_card_text) break;
        }
    }
    return info;
}());
"""

# Extracts all product cards.  Strategy:
#   1. Walk up from each "View Product Details" link until we find an ancestor
#      containing both a tyre-size pattern and a price → that's the card.
#   2. Fallback to common CSS class selectors if strategy 1 yields < 3 results.
#
# Name extraction: the card text is structured as:
#   "MICHELIN Energy XM2+ 175/50R15 79H XL $145 ..."
#   → regex captures the text between brand name and size pattern.
_JS_EXTRACT = r"""
return (function() {
    var results = [];
    var seen = {};

    var JUNK_RE = /\d+\s*day\s+satisfaction\s+guarantee|compare\s+fuel\s+saving|\bfuel\s+saving\b|\bcompare\b|run\s*flat|EV\s+tyre|select\s+a\s+store|add\s+to\s+booking|view\s+product|qty/gi;
    var BRAND_NAMES_RE = /^(?:michelin|bridgestone|continental|goodyear|falken|hankook|laufenn|dunlop|kumho|yokohama)\s*/i;

    /* ── Shared card-extraction helper ─────────────────────────────────── */
    function extractCard(card) {
        var text = (card.textContent || '').replace(/\s+/g, ' ').trim();

        /* Size: extract from text */
        var size = '';
        var szm = text.match(/(\d{3}\/\d{2}[A-Za-z]\d{2})/);
        if (szm) {
            size = szm[1].toUpperCase();
        } else {
            var szm2 = text.match(/(\d{3}R\d{2})/i);
            if (szm2) size = szm2[1].toUpperCase();
        }

        /* Spec: size + load/speed rating + optional XL only — stop before words */
        var spec = size;
        if (size) {
            var escaped = size.replace('/', '\\/');
            var loadM = text.match(new RegExp(escaped + '\\s+(\\d{2,3}(?:\\/\\d{2,3})?[A-Za-z]{1,2}(?:\\s+XL)?)', 'i'));
            if (loadM) spec = size + ' ' + loadM[1].trim();
        }

        /* Name: text before the tyre-size pattern, after stripping noise */
        var name = '';
        var sizePos = szm ? szm.index : (text.search(/\d{3}R\d{2}/i));
        if (sizePos > 0) {
            var pre = text.substring(0, sizePos)
                         .replace(JUNK_RE, ' ')
                         .replace(BRAND_NAMES_RE, '')
                         .replace(/\s+/g, ' ').trim();
            /* pre is now something like "Energy XM2+" or "" */
            if (pre.length > 2 && pre.length < 80) name = pre;
        }
        /* Fallback 1: h2/h3/h4 */
        if (!name) {
            var hEl = card.querySelector('h2,h3,h4');
            if (hEl) name = (hEl.textContent || '').trim();
        }
        /* Fallback 2: first p/span that looks like a product name */
        if (!name) {
            var elems = card.querySelectorAll('p,span');
            for (var ei = 0; ei < elems.length; ei++) {
                var et = (elems[ei].textContent || '').trim();
                if (et.length > 3 && et.length < 60 &&
                        !/\$|\d{3}\/|\bqty\b|booking|detail|view|store/i.test(et) &&
                        !/^\d+/.test(et)) {
                    name = et; break;
                }
            }
        }
        /* Fallback 3: first meaningful link */
        if (!name) {
            var ls = card.querySelectorAll('a');
            for (var li = 0; li < ls.length; li++) {
                var lt = (ls[li].textContent || '').trim();
                if (lt.length > 5 && !/add|booking|detail|view|cart/i.test(lt)) {
                    name = lt; break;
                }
            }
        }

        /* Regular price (before "OR BUY") */
        var price = '';
        var buyPos = text.toUpperCase().indexOf('OR BUY');
        var priceIn = buyPos > 0 ? text.substring(0, buyPos) : text;
        var pm = priceIn.match(/\$([\d,]+(?:\.\d+)?)/);
        if (pm) price = pm[1].replace(/,/g, '');

        /* Bulk / promo */
        var disc = '', promo = '';
        var b4m = text.match(/OR\s+BUY\s+4\s+FOR\s+\$([\d,]+(?:\.\d+)?)/i);
        if (b4m) {
            disc  = b4m[1].replace(/,/g, '');
            promo = 'OR BUY 4 FOR $' + b4m[1];
        }
        if (!promo && /BUY\s+4\s*[&+]\s*GET\s+4TH\s+TYRE\s+FREE/i.test(text)) {
            promo = 'Buy 4 Get 4th Free';
            if (price) disc = (Math.round(parseFloat(price) * 0.75 * 100) / 100).toFixed(2);
        }

        return { name: name, spec: spec, size: size,
                 price: price, disc_price: disc, promo: promo };
    }

    function addCard(card) {
        var key = (card.textContent || '').replace(/\s+/g,' ').substring(0, 100);
        if (seen[key]) return;
        seen[key] = 1;
        var item = extractCard(card);
        if (item.name || item.price) results.push(item);
    }

    /* ── Strategy 1: anchor on "View Product Details" links ─────────────
       Walk up from each link until we hit an element containing both a
       tyre-size pattern AND a price — that ancestor is the product card. */
    var allLinks = document.querySelectorAll('a');
    var anchorLinks = [];
    for (var li = 0; li < allLinks.length; li++) {
        if (/view\s+product\s+details/i.test(allLinks[li].textContent || ''))
            anchorLinks.push(allLinks[li]);
    }

    for (var ai = 0; ai < anchorLinks.length; ai++) {
        var el = anchorLinks[ai];
        for (var level = 0; level < 10; level++) {
            el = el.parentElement;
            if (!el || el === document.body) break;
            var t = (el.textContent || '');
            if (/\d{3}\/\d{2}[A-Za-z]\d{2}/.test(t) && /\$\d{2,4}/.test(t)) {
                addCard(el);
                break;
            }
        }
    }
    if (results.length >= 3) return results;

    /* ── Strategy 2: CSS class selectors (fallback) ─────────────────── */
    var SELS = [
        'div.product-listing-item', 'div[class*="ProductItem"]',
        'div[class*="product-item"]', 'li[class*="product"]',
        'article[class*="product"]', 'div[class*="tyre-card"]',
        'div[class*="TyreCard"]',    'div[class*="listing-item"]',
    ];
    for (var si = 0; si < SELS.length; si++) {
        var cards = document.querySelectorAll(SELS[si]);
        if (cards && cards.length >= 3) {
            for (var ci = 0; ci < cards.length; ci++) addCard(cards[ci]);
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
    """Print DOM debug info to help identify the correct card selector."""
    try:
        info = driver.execute_script(_JS_DIAGNOSE)
        if not info:
            print("  [DIAG] No info returned")
            return
        print(f"  [DIAG] URL:          {info.get('url','')}")
        print(f"  [DIAG] Title:        {info.get('title','')}")
        print(f"  [DIAG] detail_links: {info.get('detail_links',0)}")
        print(f"  [DIAG] price_count:  {info.get('price_count',0)}")
        print(f"  [DIAG] size_count:   {info.get('size_count',0)}")
        print(f"  [DIAG] div classes (first 30):")
        for cls in (info.get('div_classes') or []):
            print(f"           {cls}")
        if info.get('first_card_text'):
            print(f"  [DIAG] first card text: {info['first_card_text']}")
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

        # Run diagnostic only on page 1 of the first brand (page=1, brand_total=0)
        raw  = extract_page(driver, run_diag=(page == 1 and brand_total == 0))
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
