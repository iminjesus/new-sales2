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
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

BASE_URL    = "https://www.bobjane.com.au/collections/tyres"
OUTPUT_FILE = datetime.now().strftime("BobJane_%Y%m%d_%H%M.csv")
MAX_LOAD_MORE = 300   # safety cap for the full catalogue


def clean(text):
    return " ".join(text.strip().split()) if text else ""


def init_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--window-size=1400,900")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    driver = webdriver.Chrome(options=options)
    driver.set_script_timeout(60)   # allow longer JS execution on big pages
    return driver


def find_load_more_btn(driver):
    """Find the Load More button using fast, simple selectors only."""
    # Fast: button/a containing 'load' (case-insensitive via lower-case XPath)
    for xpath in [
        "//button[contains(translate(text(),'LOADMRE ','loadmre '),'load more')]",
        "//a[contains(translate(text(),'LOADMRE ','loadmre '),'load more')]",
        "//button[contains(@class,'load')]",
        "//button[contains(@class,'Load')]",
        "//*[@data-load-more]",
        "//*[contains(@class,'load-more')]",
    ]:
        try:
            els = driver.find_elements(By.XPATH, xpath)
            visible = [e for e in els if e.is_displayed()]
            if visible:
                return visible[0]
        except Exception:
            pass
    return None


def load_all_results(driver):
    """Click Load More until the button disappears or the cap is hit."""
    clicked = 0
    while clicked < MAX_LOAD_MORE:
        btn = find_load_more_btn(driver)
        if not btn:
            print(f"  No more Load More — done after {clicked} clicks")
            break
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            time.sleep(0.6)
            driver.execute_script("arguments[0].click();", btn)
            clicked += 1
            print(f"  Load More clicked ({clicked})", end="\r")
            time.sleep(2.5)
        except Exception as e:
            print(f"\n  Load More error: {e}")
            break

    if clicked >= MAX_LOAD_MORE:
        print(f"\n  [WARN] Hit cap ({MAX_LOAD_MORE} clicks)")
    else:
        print()


def extract_price_number(text):
    """'$159 per tyre' → '159.00'"""
    m = re.search(r'\$([\d,]+(?:\.\d+)?)', text.replace(',', ''))
    return f"{float(m.group(1)):.2f}" if m else ""


def extract_size_from_title(title):
    """
    Pull tyre size from product title.
    e.g. 'Hankook Kinergy eco2 (K435) 205/55R16 91V' → '205/55R16'
         'Bridgestone Duravis R660 195R15C 106S'      → '195R15'
    """
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
    """
    Split product title into (size, brand, description).
    Title format: 'Hankook Kinergy eco2 (K435) 205/55R16 91V'
    """
    size  = extract_size_from_title(title)
    brand = ""
    rest  = title.strip()

    for b in KNOWN_BRANDS:
        if rest.lower().startswith(b.lower()):
            brand = b
            rest  = rest[len(b):].strip()
            break

    # Remove size from rest
    if size:
        rest = re.sub(re.escape(size), "", rest, flags=re.IGNORECASE).strip()
    # Generic size pattern cleanup
    rest = re.sub(r'\d{3}/\d{2}[A-Za-z]\d{2}[A-Za-z\d]*', "", rest).strip()
    rest = re.sub(r'\d{3}[A-Za-z]\d{2}[A-Za-z\d]*', "", rest).strip()
    rest = re.sub(r'^[\s\-–/]+', '', rest).strip()

    return size, brand or title.split()[0], rest or title


def get_product_cards(driver):
    """
    Find product card containers using multiple strategies.
    Returns a list of WebElements.
    """
    # Strategy 1: common CSS class selectors
    for sel in [
        "div.product-card",
        "li.product-card",
        "article.product-card",
        "div.product-item",
        "div.grid-product",
        "div.card-wrapper",
        "div[class*='ProductCard']",
        "div[class*='product-block']",
        "li[class*='grid__item']",
    ]:
        cards = driver.find_elements(By.CSS_SELECTOR, sel)
        if len(cards) > 2:
            print(f"  Card selector matched: '{sel}' → {len(cards)} cards")
            return cards

    # Strategy 2: JavaScript — find unique parent containers of product links
    try:
        cards = driver.execute_script("""
            const links = Array.from(
                document.querySelectorAll('a[href*="/products/"]')
            );
            const containers = links.map(a => {
                let el = a;
                // Walk up until we find a container with a price
                for (let i = 0; i < 6; i++) {
                    el = el.parentElement;
                    if (!el) break;
                    const text = el.innerText || '';
                    if (text.match(/\\$\\d+/) && text.length < 600) return el;
                }
                return a.parentElement;
            }).filter(Boolean);
            // Deduplicate
            return [...new Set(containers)];
        """)
        if cards and len(cards) > 2:
            print(f"  JS strategy: {len(cards)} cards found")
            return cards
    except Exception as e:
        print(f"  JS strategy error: {e}")

    # Strategy 3: fallback — find all elements with price text
    try:
        all_els = driver.find_elements(
            By.XPATH,
            "//*[contains(text(),'per tyre') or contains(text(),'Per Tyre')]"
        )
        containers = []
        for el in all_els:
            try:
                parent = driver.execute_script(
                    "return arguments[0].parentElement.parentElement;", el)
                if parent and parent not in containers:
                    containers.append(parent)
            except Exception:
                pass
        if containers:
            print(f"  'per tyre' strategy: {len(containers)} cards")
            return containers
    except Exception as e:
        print(f"  'per tyre' strategy error: {e}")

    return []


def scrape_all_products(driver):
    """Extract every product card currently in the DOM."""
    cards = get_product_cards(driver)
    if not cards:
        page_snippet = clean(driver.find_element(By.TAG_NAME, "body").text)[:200]
        print(f"  [WARN] No cards found. Page: {page_snippet!r}")
        return []

    results = []
    for card in cards:
        try:
            card_text = clean(card.text if hasattr(card, 'text') else
                              driver.execute_script("return arguments[0].innerText;", card))
            if not card_text:
                continue

            # ── Title ─────────────────────────────────────────────────────────
            title = ""
            for sel in ["h2", "h3", "h4",
                        ".product-card__title", ".product-title",
                        "[class*='title']", "a[href*='/products/']"]:
                try:
                    el = card.find_element(By.CSS_SELECTOR, sel)
                    t  = clean(el.text or
                               driver.execute_script("return arguments[0].innerText;", el))
                    if t and len(t) > 5:
                        title = t
                        break
                except Exception:
                    pass
            if not title:
                # Grab first non-empty line from card text
                for line in card_text.split("\n"):
                    line = line.strip()
                    if len(line) > 8 and not line.startswith("$"):
                        title = line
                        break

            # ── Promo banner ("Buy 3 Get 1 Free") ─────────────────────────────
            promo = ""
            for sel in ["[class*='badge']", "[class*='banner']",
                        "[class*='promo']", "[class*='label']",
                        "[class*='tag']", "[class*='ribbon']"]:
                try:
                    raw = clean(card.find_element(By.CSS_SELECTOR, sel).text)
                    if raw and len(raw) < 60 and not raw.startswith("$"):
                        promo = raw
                        break
                except Exception:
                    pass

            # ── Price ("$159 per tyre") ────────────────────────────────────────
            price = ""
            for line in card_text.split("\n"):
                line = line.strip()
                if re.match(r'^\$\d', line) and "SAVE" not in line.upper():
                    price = extract_price_number(line)
                    if price:
                        break

            # ── Save text ("SAVE $134 ON A SET OF 4") ─────────────────────────
            save_text = ""
            m = re.search(r'(SAVE\s+\$[\d,]+[^\n]*)', card_text, re.IGNORECASE)
            if m:
                save_text = m.group(1).strip()

            # ── Calculated discount per-tyre price ────────────────────────────
            disc_price = ""
            if save_text and price:
                m2 = re.search(r'SAVE\s+\$([\d,]+)\s+ON\s+A\s+SET\s+OF\s+(\d+)',
                               save_text, re.IGNORECASE)
                if m2:
                    try:
                        save_total = float(m2.group(1).replace(',', ''))
                        set_of     = int(m2.group(2))
                        disc = round(float(price) - save_total / set_of, 2)
                        disc_price = f"{disc:.2f}"
                    except Exception:
                        pass
            if not disc_price and promo and re.search(
                    r'buy\s*3\s*get\s*1\s*free', promo, re.IGNORECASE):
                try:
                    disc_price = f"{round(float(price) * 3 / 4, 2):.2f}"
                except Exception:
                    pass

            if not title and not price:
                continue

            size, brand, desc = parse_title(title)
            results.append({
                "size":       size,
                "brand":      brand,
                "description": desc,
                "price":      price,
                "disc_price": disc_price,
                "promo":      promo,
                "save_text":  save_text,
            })
            line_out = f"  {brand:<15} | {size:<12} | {desc[:35]:<35} | ${price}"
            if disc_price:
                line_out += f" → ${disc_price}"
            print(line_out)

        except Exception as e:
            print(f"  [WARN] {e}")

    return results


def main():
    driver = init_driver()

    print(f"\nOutput: {OUTPUT_FILE}")
    print("Navigating to Bob Jane catalogue — do NOT close the browser.\n")

    try:
        driver.get(BASE_URL)
        time.sleep(4)

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
