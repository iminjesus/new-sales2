"""
bobjane_crawling.py
-------------------
Scrapes ALL tyre prices from https://www.bobjane.com.au/collections/tyres
Uses Load More (AJAX) but extracts only NEW cards after each click and
saves to CSV immediately — keeps JS work tiny, survives browser crashes.

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
MAX_CLICKS  = 300


def init_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--window-size=1400,900")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    driver = webdriver.Chrome(options=options)
    driver.set_script_timeout(120)
    return driver


# ── JS snippets ───────────────────────────────────────────────────────────────

_JS_COUNT = r"""
return document.querySelectorAll('div.productCard').length;
"""

_JS_CLICK = r"""
var btn = document.querySelector('button.load-more-btn');
if (!btn) btn = document.querySelector('[class*="load-more"],[class*="LoadMore"],[data-load-more]');
if (!btn) {
    var btns = document.querySelectorAll('button');
    for (var i = 0; i < btns.length; i++) {
        if (/load\s*more/i.test(btns[i].textContent)) { btn = btns[i]; break; }
    }
}
if (!btn) return false;
btn.click();
return true;
"""

# Pass startIdx as arguments[0] — only processes cards from that index onward
_JS_EXTRACT = r"""
return (function(startIdx) {
    var allCards = document.querySelectorAll('div.productCard');
    var cards = Array.from(allCards).slice(startIdx || 0);

    var results = [];
    var dedupKeys = new Set();

    for (var ci = 0; ci < cards.length; ci++) {
        var card = cards[ci];
        var text = (card.textContent || '').replace(/\s+/g, ' ').trim();
        if (!text) continue;

        /* Title: product info link (not the image link) */
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

        /* Save text: "SAVE $N ON A SET OF N" */
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
    /* Remove the cards we processed so the page stays lightweight even after
       dozens of Load More clicks (the extraction JS slows down dramatically
       as the DOM grows past a couple thousand cards). */
    for (var ri = 0; ri < cards.length; ri++) {
        try { cards[ri].remove(); } catch (e) {}
    }
    return results;
})(arguments[0]);
"""


# ── Python helpers ────────────────────────────────────────────────────────────

def count_cards(driver):
    try:
        return driver.execute_script(_JS_COUNT) or 0
    except Exception:
        return 0


def click_load_more(driver):
    try:
        return driver.execute_script(_JS_CLICK)
    except Exception:
        return False


def extract_from(driver, start_idx):
    """Extract only cards from start_idx onward. Returns list of row dicts."""
    try:
        raw = driver.execute_script(_JS_EXTRACT, start_idx)
    except Exception as e:
        print(f"  [JS error] {e}")
        return []
    if not raw:
        return []
    return raw


KNOWN_BRANDS = [
    "Michelin", "Bridgestone", "Continental", "Goodyear", "Kumho",
    "Falken", "Hankook", "Laufenn", "Dunlop", "Yokohama", "Pirelli",
    "Toyo", "Nexen", "Nitto", "Cooper", "BFGoodrich", "General",
    "Maxxis", "Sailun", "Accelera", "Achilles", "Landsail", "Winrun",
    "Roadstone", "Haida", "Minerva", "Centara", "Hifly", "Triangle",
    "Davanti", "Firemax", "Boto", "Comforser", "Radar", "Arivo",
    "Westlake", "Linglong", "GT Radial", "Goodride", "Ironman",
    "Federal", "Thunderer", "Grenlander", "Trazano", "Tracmax",
    "Bob Jane", "RoadX", "Mazzini", "Aplus", "Vitour", "Wanli",
]


def extract_size_from_title(title):
    # Accept one OR two letters between the aspect-ratio and rim-diameter:
    # R (standard), ZR (Z-rated speed) — Michelin/Pirelli/Tracmax write
    # high-speed tyres as "225/40ZR18" so the old single-letter regex left
    # the SIZE blank.  Normalise ZR -> R so the size matches the comparison
    # dropdown which only carries R sizes.
    m = re.search(r'(\d{3}/\d{2}[A-Za-z]{1,2}\d{2})', title)
    if m:
        return m.group(1).upper().replace("ZR", "R")
    m = re.search(r'(\d{3}[A-Za-z]{1,2}\d{2})', title)
    if m:
        return m.group(1).upper().replace("ZR", "R")
    return ""


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


def process_raw(raw_items):
    """Convert raw JS items to final row dicts with parsed title."""
    results = []
    for item in raw_items:
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
        results.append({
            "size": size, "brand": brand, "description": desc,
            "price": price, "disc_price": disc_price,
            "promo": promo, "save_text": save_text,
        })
    return results


def write_rows(writer, rows):
    for r in rows:
        writer.writerow([r["size"], r["brand"], r["description"],
                         r["price"], r["disc_price"], r["promo"], r["save_text"]])
        line = f"  {r['brand']:<15} | {r['size']:<12} | {r['description'][:30]:<30} | ${r['price']}"
        if r["disc_price"]:
            line += f" → ${r['disc_price']}"
        print(line)


def main():
    driver = init_driver()

    print(f"\nOutput: {OUTPUT_FILE}")
    print("Loading Bob Jane catalogue — extracts + saves after every Load More click.\n")

    try:
        driver.get(BASE_URL)
        time.sleep(5)   # wait for AJAX initial load

        total         = 0
        stale         = 0   # consecutive "no new cards appeared"
        extract_fail  = 0   # consecutive "cards appeared but extraction returned 0"
        click_num     = 0

        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(["SIZE", "brand", "DESCRIPTION",
                             "PRICE", "DISC_PRICE", "PROMO", "SAVE_TEXT"])

            # ── Extract initial page ──────────────────────────────────────────
            # The extractor now removes the cards it processed, so we always
            # extract "from 0" — whatever is currently on the page is new.
            initial_count = count_cards(driver)
            print(f"Initial load: {initial_count} cards")
            raw  = extract_from(driver, 0)
            rows = process_raw(raw)
            write_rows(writer, rows)
            f.flush()
            total         += len(rows)
            print(f"  ── saved {len(rows)}  (total: {total})\n")

            # ── Load More loop ────────────────────────────────────────────────
            # Each iteration: click → wait for cards to appear → extract → the
            # extractor removes them from the DOM, so the page stays small and
            # the JS never runs out of time.
            while click_num < MAX_CLICKS:
                found = click_load_more(driver)
                if not found:
                    print(f"Load More button gone — finished")
                    break

                click_num += 1

                # Adaptive wait: poll until at least one card is on the page
                # (after our previous removal the count was 0, so any card
                # means the click actually loaded new ones).
                deadline = time.time() + 6.0
                after = 0
                while time.time() < deadline:
                    time.sleep(0.3)
                    after = count_cards(driver)
                    if after > 0:
                        break

                if after == 0:
                    stale += 1
                    print(f"  Click {click_num:>3}: +0  [stale {stale}/5]")
                    if stale >= 5:
                        print("  No new products — finished")
                        break
                    continue

                stale = 0

                # Extract everything currently on the page (the removal step
                # inside _JS_EXTRACT will clean up after itself).
                raw  = extract_from(driver, 0)
                rows = process_raw(raw)
                if rows:
                    extract_fail = 0
                else:
                    extract_fail += 1
                write_rows(writer, rows)
                f.flush()
                total += len(rows)
                tag = "" if rows else f"  [extract failed {extract_fail}/5]"
                print(f"  Click {click_num:>3}: +{after:>3} cards → {len(rows):>3} products  (total: {total}){tag}")
                if extract_fail >= 5:
                    print("  Too many extraction failures in a row — stopping")
                    break

        print(f"\nFinished. {total} products saved → {OUTPUT_FILE}")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
