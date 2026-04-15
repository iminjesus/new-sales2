"""
bobjane_crawling.py
-------------------
Scrapes ALL tyre prices from https://www.bobjane.com.au/collections/tyres
Uses URL pagination (?page=N) instead of Load More — keeps each page small
so the browser stays fast throughout. Saves to CSV after every page.

Usage:
    python bobjane_crawling.py
"""
import re
import csv
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

BASE_URL    = "https://www.bobjane.com.au/collections/tyres"
OUTPUT_FILE = datetime.now().strftime("BobJane_%Y%m%d_%H%M.csv")


def init_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--window-size=1400,900")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    driver = webdriver.Chrome(options=options)
    driver.set_script_timeout(60)
    return driver


# ── JS: extract all product data from current page in ONE call ────────────────
# Bob Jane DOM: div.productCard > div.productCardInner > div.productInfo
_JS_EXTRACT = r"""
(function() {
    var cards = Array.from(document.querySelectorAll('div.productCard'));

    /* Fallback: walk up from product links to price container */
    if (cards.length === 0) {
        var seen = new Set();
        var links = document.querySelectorAll('a[href*="/products/"]');
        for (var li = 0; li < links.length; li++) {
            var el = links[li].parentElement;
            for (var up = 0; up < 8; up++) {
                if (!el) break;
                var tc = el.textContent || '';
                if (/\$\d/.test(tc) && tc.length < 2000 && !seen.has(el)) {
                    seen.add(el); cards.push(el); break;
                }
                el = el.parentElement;
            }
        }
    }

    var results = [];
    var dedupKeys = new Set();

    for (var ci = 0; ci < cards.length; ci++) {
        var card = cards[ci];
        var text = (card.textContent || '').replace(/\s+/g, ' ').trim();
        if (!text) continue;

        /* Title: product info link (not image link) */
        var title = '';
        var infoEl = card.querySelector('div.productInfo, .productInfo');
        if (infoEl) {
            var infoLink = infoEl.querySelector('a[href*="/products/"]');
            if (infoLink) title = (infoLink.textContent || '').trim();
        }
        if (!title) {
            var allLinks = card.querySelectorAll('a[href*="/products/"]');
            for (var tl = 0; tl < allLinks.length; tl++) {
                var lt = (allLinks[tl].textContent || '').trim();
                if (lt.length > 6) { title = lt; break; }
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

        /* Save text */
        var saveText = '';
        var stm = text.match(/(SAVE \$[\d,]+ ON A SET OF \d+)/i);
        if (stm) saveText = stm[1];

        /* Promo badge */
        var promo = '';
        var promoEl = card.querySelector(
            '[class*="badge"],[class*="promo"],[class*="ribbon"],' +
            '[class*="label"],[class*="tag"],[class*="banner"]'
        );
        if (promoEl) {
            var pt = (promoEl.textContent || '').trim();
            if (pt && pt.length < 60 && pt[0] !== '$') promo = pt;
        }
        if (!promo) {
            var buyM = text.match(/^((?:buy|get)\s+\d+\s+\w+\s+\d+\s+\w+)/i);
            if (buyM) promo = buyM[1];
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


def scrape_page(driver):
    """Extract products from current page via single JS call."""
    try:
        raw = driver.execute_script(_JS_EXTRACT)
    except Exception as e:
        print(f"  [JS error] {e}")
        return []

    if not raw:
        return []

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

    return results


def main():
    driver = init_driver()

    print(f"\nOutput: {OUTPUT_FILE}")
    print("Crawling page by page — saves after every page, browser stays fast.\n")

    try:
        all_seen  = set()   # global dedup across all pages
        page_num  = 1
        total     = 0

        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(["SIZE", "brand", "DESCRIPTION",
                             "PRICE", "DISC_PRICE", "PROMO", "SAVE_TEXT"])

            while True:
                # Page 1: use bare URL (site JS ignores ?page=1)
                # Page 2+: use ?page=N for Shopify pagination
                url = BASE_URL if page_num == 1 else f"{BASE_URL}?page={page_num}"
                driver.get(url)
                time.sleep(5)   # wait for AJAX product loading (plain sleep — reliable)

                # Debug: show card count and page title if empty
                try:
                    card_count = driver.execute_script(
                        "return document.querySelectorAll('div.productCard').length;")
                    page_title = driver.title
                    print(f"  [debug] title='{page_title[:60]}' cards={card_count}")
                except Exception:
                    pass

                rows = scrape_page(driver)

                if not rows:
                    print(f"Page {page_num:>3}: no products — finished")
                    break

                # Shopify returns page 1 when page number exceeds catalogue
                new_rows = [r for r in rows
                            if (r['size'], r['brand'], r['description']) not in all_seen]

                if not new_rows:
                    print(f"Page {page_num:>3}: all duplicates — finished")
                    break

                for r in new_rows:
                    all_seen.add((r['size'], r['brand'], r['description']))
                    writer.writerow([r["size"],  r["brand"],  r["description"],
                                     r["price"], r["disc_price"],
                                     r["promo"], r["save_text"]])
                    line = f"  {r['brand']:<15} | {r['size']:<12} | {r['description'][:30]:<30} | ${r['price']}"
                    if r['disc_price']:
                        line += f" → ${r['disc_price']}"
                    print(line)

                f.flush()
                total    += len(new_rows)
                page_num += 1
                print(f"  ── page {page_num-1} done: {len(new_rows)} products  (running total: {total})")

        print(f"\nFinished. {total} products saved → {OUTPUT_FILE}")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
