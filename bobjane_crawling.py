"""
bobjane_crawling.py
-------------------
Scrapes ALL tyre prices from https://www.bobjane.com.au/collections/tyres
No login, no size filter — just clicks Load More until all products are loaded,
then saves SIZE (from title), brand, DESCRIPTION, PRICE, DISC_PRICE, PROMO, SAVE_TEXT.

Usage:
    python bobjane_crawling.py
"""
import re
import csv
import time
from datetime import datetime
from selenium import webdriver

BASE_URL    = "https://www.bobjane.com.au/collections/tyres"
OUTPUT_FILE = datetime.now().strftime("BobJane_%Y%m%d_%H%M.csv")
MAX_LOAD_MORE = 300   # safety cap for the full catalogue


def init_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--window-size=1400,900")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    driver = webdriver.Chrome(options=options)
    driver.set_script_timeout(120)   # large page needs time
    return driver


# ── JS: find and click Load More (specific selectors only — no mass scan) ─────
# Use raw string to avoid Python escape warnings for \s etc.
_JS_CLICK_LOAD_MORE = r"""
/* 1. Try class/attribute selectors first — O(1) lookup */
var btn = document.querySelector(
    '[data-load-more], [class*="load-more"], [class*="LoadMore"]'
);
/* 2. Fallback: scan only <button> elements (far fewer than all <a> tags) */
if (!btn) {
    var buttons = document.querySelectorAll('button');
    for (var i = 0; i < buttons.length; i++) {
        if (/load\s*more/i.test(buttons[i].textContent)) {
            btn = buttons[i];
            break;
        }
    }
}
/* 3. Last resort: scan <a> elements with href="#" or no href (pagination links) */
if (!btn) {
    var anchors = document.querySelectorAll('a[href="#"], a[href="javascript:;"], a[href=""]');
    for (var j = 0; j < anchors.length; j++) {
        if (/load\s*more/i.test(anchors[j].textContent)) {
            btn = anchors[j];
            break;
        }
    }
}
if (btn && btn.offsetParent !== null) {
    btn.scrollIntoView({block: 'center'});
    btn.click();
    return true;
}
return false;
"""

# ── JS: extract all product data in ONE call ──────────────────────────────────
# Use textContent (no layout reflow) for bulk text; innerText only where needed.
_JS_EXTRACT = r"""
(function() {
    var CARD_SELECTORS = [
        'div.product-card', 'li.product-card', 'article.product-card',
        'div.product-item', 'div.grid-product', 'div.card-wrapper',
        'div[class*="ProductCard"]', 'div[class*="product-block"]',
        'li[class*="grid__item"]'
    ];

    var cards = [];
    for (var si = 0; si < CARD_SELECTORS.length; si++) {
        var els = document.querySelectorAll(CARD_SELECTORS[si]);
        if (els.length > 2) { cards = Array.from(els); break; }
    }

    if (cards.length === 0) {
        /* Fallback: unique parent containers of /products/ links that have a price */
        var seen = new Set();
        var links = document.querySelectorAll('a[href*="/products/"]');
        for (var li = 0; li < links.length; li++) {
            var el = links[li];
            for (var up = 0; up < 6; up++) {
                el = el.parentElement;
                if (!el) break;
                var tc = el.textContent || '';
                if (/\$\d/.test(tc) && tc.length < 1000 && !seen.has(el)) {
                    seen.add(el);
                    cards.push(el);
                    break;
                }
            }
        }
    }

    var results = [];
    var dedupKeys = new Set();

    for (var ci = 0; ci < cards.length; ci++) {
        var card = cards[ci];
        /* textContent is fast (no layout reflow); use for regex matching */
        var text = (card.textContent || '').trim();
        if (!text) continue;

        /* Title: prefer heading/link elements */
        var titleEl = card.querySelector(
            'h2, h3, h4, [class*="title"] a, a[href*="/products/"]'
        );
        var title = '';
        if (titleEl) {
            title = (titleEl.textContent || '').trim();
        }
        if (!title) {
            /* Fall back to first non-price line in card text */
            var lines = text.split('\n');
            for (var ln = 0; ln < lines.length; ln++) {
                var line = lines[ln].trim();
                if (line.length > 8 && line[0] !== '$') { title = line; break; }
            }
        }

        /* Price: "$N per tyre" or first "$N" before "SAVE" */
        var price = '';
        var pm = text.match(/\$([\d,]+(?:\.\d+)?)\s*per\s*tyre/i);
        if (pm) {
            price = pm[1].replace(/,/g, '');
        } else {
            var saveIdx = text.toUpperCase().indexOf('SAVE');
            var searchIn = saveIdx > 0 ? text.substring(0, saveIdx) : text;
            var sm = searchIn.match(/\$([\d,]+(?:\.\d+)?)/);
            if (sm) price = sm[1].replace(/,/g, '');
        }

        /* Save text: "SAVE $N ON A SET OF N" */
        var saveText = '';
        var stm = text.match(/(SAVE\s+\$[\d,]+\s+ON\s+A\s+SET\s+OF\s+\d+)/i);
        if (stm) saveText = stm[1].trim();

        /* Promo badge — use innerText on just this one small element */
        var promo = '';
        var promoEl = card.querySelector(
            '[class*="badge"], [class*="banner"], [class*="promo"],' +
            ' [class*="ribbon"], [class*="label"], [class*="tag"]'
        );
        if (promoEl) {
            var pt = (promoEl.innerText || promoEl.textContent || '').trim();
            if (pt && pt.length < 60 && pt[0] !== '$') promo = pt;
        }

        if (!title && !price) continue;

        var key = title + '|' + price;
        if (dedupKeys.has(key)) continue;
        dedupKeys.add(key);

        results.push({ title: title, price: price, saveText: saveText, promo: promo });
    }
    return results;
})();
"""


def extract_size_from_title(title):
    m = re.search(r'(\d{3}/\d{2}[A-Za-z]\d{2})', title)
    if m:
        return m.group(1).upper()
    m = re.search(r'(\d{3}[A-Za-z]\d{2})', title)
    if m:
        return m.group(1).upper()
    return ""


KNOWN_BRANDS = [
    "Michelin", "Bridgestone", "Continental", "Goodyear", "Kumho",
    "Falken", "Hankook", "Laufenn", "Dunlop", "Yokohama", "Pirelli",
    "Toyo", "Nexen", "Nitto", "Cooper", "BFGoodrich", "General",
    "Maxxis", "Sailun", "Accelera", "Achilles", "Landsail", "Winrun",
    "Roadstone", "Haida", "Minerva", "Centara", "Hifly", "Triangle",
    "Davanti", "Firemax", "Boto", "Comforser", "Radar", "Arivo",
    "Westlake", "Linglong", "GT Radial", "Goodride", "Ironman",
    "Federal", "Thunderer", "Grenlander", "Trazano", "Tracmax",
]


def parse_title(title):
    size  = extract_size_from_title(title)
    brand = ""
    rest  = title.strip()

    for b in KNOWN_BRANDS:
        if rest.lower().startswith(b.lower()):
            brand = b
            rest  = rest[len(b):].strip()
            break

    if size:
        rest = re.sub(re.escape(size), "", rest, flags=re.IGNORECASE).strip()
    rest = re.sub(r'\d{3}/\d{2}[A-Za-z]\d{2}[A-Za-z\d]*', "", rest).strip()
    rest = re.sub(r'\d{3}[A-Za-z]\d{2}[A-Za-z\d]*',       "", rest).strip()
    rest = re.sub(r'^[\s\-–/]+', '', rest).strip()

    return size, brand or title.split()[0], rest or title


def load_all_results(driver):
    """Click Load More via JS until gone or cap hit."""
    clicked = 0
    while clicked < MAX_LOAD_MORE:
        try:
            found = driver.execute_script(_JS_CLICK_LOAD_MORE)
        except Exception as e:
            print(f"\n  Load More JS error: {e}")
            break
        if not found:
            print(f"  No more Load More — done after {clicked} clicks")
            break
        clicked += 1
        print(f"  Load More clicked ({clicked})", end="\r")
        time.sleep(1.5)   # wait for AJAX batch to render

    if clicked >= MAX_LOAD_MORE:
        print(f"\n  [WARN] Hit cap ({MAX_LOAD_MORE} clicks)")
    else:
        print()


def scrape_all_products(driver):
    """Single JS call extracts all product data; Python only does title parsing."""
    print("  Running JS extraction...")
    try:
        raw = driver.execute_script(_JS_EXTRACT)
    except Exception as e:
        print(f"  [ERROR] JS extraction failed: {e}")
        return []

    if not raw:
        print("  [WARN] No products found by JS extractor")
        return []

    print(f"  JS returned {len(raw)} raw items — parsing titles...")
    results = []
    seen = set()

    for item in raw:
        title     = (item.get("title")    or "").strip()
        price_s   = (item.get("price")    or "").strip()
        save_text = (item.get("saveText") or "").strip()
        promo     = (item.get("promo")    or "").strip()

        price = f"{float(price_s):.2f}" if price_s else ""

        disc_price  = ""
        save_amount = None
        m = re.search(r'SAVE\s+\$([\d,]+)\s+ON\s+A\s+SET\s+OF\s+(\d+)',
                      save_text, re.IGNORECASE)
        if m:
            try:
                save_amount = float(m.group(1).replace(',', '')) / int(m.group(2))
            except Exception:
                pass

        if price:
            if save_amount is not None:
                disc_price = f"{round(float(price) - save_amount, 2):.2f}"
            elif re.search(r'buy\s*3\s*get\s*1\s*free', promo, re.IGNORECASE):
                disc_price = f"{round(float(price) * 3 / 4, 2):.2f}"

        size, brand, desc = parse_title(title)

        key = (size, brand, desc)
        if key in seen:
            continue
        seen.add(key)

        results.append({
            "size":        size,
            "brand":       brand,
            "description": desc,
            "price":       price,
            "disc_price":  disc_price,
            "promo":       promo,
            "save_text":   save_text,
        })
        line_out = f"  {brand:<15} | {size:<12} | {desc[:35]:<35} | ${price}"
        if disc_price:
            line_out += f" → ${disc_price}"
        print(line_out)

    return results


def main():
    driver = init_driver()

    print(f"\nOutput: {OUTPUT_FILE}")
    print("Navigating to Bob Jane catalogue — do NOT close the browser.\n")

    try:
        driver.get(BASE_URL)
        time.sleep(3)

        print("Clicking Load More to load all products...")
        load_all_results(driver)

        print("\nScraping product cards...")
        rows = scrape_all_products(driver)
        print(f"\nTotal products found: {len(rows)}")

        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(["SIZE", "brand", "DESCRIPTION",
                             "PRICE", "DISC_PRICE", "PROMO", "SAVE_TEXT"])
            for r in rows:
                writer.writerow([r["size"],  r["brand"],  r["description"],
                                 r["price"], r["disc_price"],
                                 r["promo"], r["save_text"]])

        print(f"Saved: {OUTPUT_FILE}")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
