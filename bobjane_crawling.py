"""
bobjane_crawling.py
-------------------
Scrapes tyre prices from https://www.bobjane.com.au/collections/tyres
for each target size, clicks "Load More" until all results are visible,
then saves SIZE, brand, DESCRIPTION, PRICE to a CSV file.

No login required. Run from new-sales2 folder:
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
from selenium.common.exceptions import TimeoutException, NoSuchElementException

BASE_URL = "https://www.bobjane.com.au/collections/tyres"

# Same 24 sizes as Tempe — (search_query, canonical_size)
# Bob Jane URL filter params are tried in order; falls back to text search
SEARCH_SIZES = [
    ("175", "65", "14", "175/65R14"),
    ("195", "65", "15", "195/65R15"),
    ("205", "65", "16", "205/65R16"),
    ("225", "45", "17", "225/45R17"),
    ("215", "45", "17", "215/45R17"),
    ("235", "45", "17", "235/45R17"),
    ("225", "40", "18", "225/40R18"),
    ("235", "45", "19", "235/45R19"),
    ("245", "35", "20", "245/35R20"),
    ("225", "65", "17", "225/65R17"),
    ("225", "55", "18", "225/55R18"),
    ("225", "60", "18", "225/60R18"),
    ("225", "55", "19", "225/55R19"),
    ("225", "45", "19", "225/45R19"),
    ("255", "45", "20", "255/45R20"),
    ("265", "40", "21", "265/40R21"),
    ("245", "70", "16", "245/70R16"),
    ("265", "70", "16", "265/70R16"),
    ("265", "60", "18", "265/60R18"),
    ("265", "65", "17", "265/65R17"),
    ("265", "75", "16", "265/75R16"),
    ("185",  "",  "14", "185R14"),
    ("195",  "",  "15", "195R15"),
    ("235", "65", "16", "235/65R16"),
]

OUTPUT_FILE = datetime.now().strftime("BobJane_%Y%m%d_%H%M.csv")


def clean(text):
    return text.strip() if text else ""


def init_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--window-size=1400,900")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    return webdriver.Chrome(options=options)


def build_url(width, profile, rim):
    """Try common Shopify filter URL patterns."""
    if profile:
        return (
            f"{BASE_URL}?width={width}&profile={profile}&rim={rim}"
        )
    else:
        # Run-flat / no-profile tyres (185R14 etc.)
        return f"{BASE_URL}?width={width}&rim={rim}"


def load_all_results(driver, wait):
    """Keep clicking 'Load More' until the button disappears."""
    loaded = 0
    while True:
        # Common selectors for Load More button
        btn = None
        for sel in [
            "//button[contains(translate(text(),'abcdefghijklmnopqrstuvwxyz','ABCDEFGHIJKLMNOPQRSTUVWXYZ'),'LOAD MORE')]",
            "//a[contains(translate(text(),'abcdefghijklmnopqrstuvwxyz','ABCDEFGHIJKLMNOPQRSTUVWXYZ'),'LOAD MORE')]",
            "//span[contains(translate(text(),'abcdefghijklmnopqrstuvwxyz','ABCDEFGHIJKLMNOPQRSTUVWXYZ'),'LOAD MORE')]",
        ]:
            els = driver.find_elements(By.XPATH, sel)
            visible = [e for e in els if e.is_displayed()]
            if visible:
                btn = visible[0]
                break

        if not btn:
            break

        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            time.sleep(0.5)
            driver.execute_script("arguments[0].click();", btn)
            loaded += 1
            print(f"    Load More clicked ({loaded})")
            time.sleep(2.5)   # wait for AJAX to append products
        except Exception as e:
            print(f"    Load More error: {e}")
            break

    print(f"    Total Load More clicks: {loaded}")


def extract_price(text):
    """Extract numeric price from strings like '$315 per tyre' or '$315.00'."""
    m = re.search(r'\$?([\d,]+\.?\d*)', text.replace(',', ''))
    return m.group(1) if m else ""


def parse_title(title, size):
    """
    Split 'Michelin Pilot Sport 4 175/65R14 82T' into (brand, description).
    Returns (brand, description_without_brand_and_size).
    """
    # Known brands to detect from the start of the title
    known_brands = [
        "Michelin", "Bridgestone", "Continental", "Goodyear", "Kumho",
        "Falken", "Hankook", "Laufenn", "Dunlop", "Yokohama", "Pirelli",
        "Toyo", "Nexen", "Nitto", "Cooper", "BFGoodrich", "General",
        "Maxxis", "Sailun", "Accelera", "Achilles", "Landsail", "Winrun",
        "Roadstone", "Haida", "Minerva", "Centara", "Hifly", "Triangle",
        "Davanti", "Firemax", "Boto", "Comforser", "Invovic", "Radar",
        "Arivo", "Westlake", "Linglong", "GT Radial", "Goodride",
        "Starfire", "Thunderer", "Ironman", "Federal", "Grenlander",
    ]
    brand = ""
    rest  = title.strip()
    for b in known_brands:
        if rest.lower().startswith(b.lower()):
            brand = b
            rest  = rest[len(b):].strip()
            break

    # Remove size string from rest
    for pattern in [
        re.escape(size),                             # exact "175/65R14"
        r'\d{3}/\d{2}[A-Za-z]\d{2}[A-Za-z]?\d?',   # generic metric size
        r'\d{3}[A-Za-z]\d{2}',                       # 185R14
    ]:
        rest = re.sub(pattern, '', rest, flags=re.IGNORECASE).strip()

    # Clean up leading/trailing punctuation
    rest = re.sub(r'^[\s\-–/]+', '', rest).strip()
    return brand or title.split()[0], rest or title


def scrape_products(driver, size):
    """Extract all product cards currently in the DOM."""
    results = []

    # Try multiple product card selectors (Shopify themes vary)
    card_selectors = [
        "div.product-card",
        "div.product-item",
        "div.grid__item",
        "li.product-card",
        "article.product-card",
        "div[class*='ProductCard']",
        "div[class*='product_card']",
        "div.card-wrapper",
    ]

    cards = []
    for sel in card_selectors:
        cards = driver.find_elements(By.CSS_SELECTOR, sel)
        if cards:
            print(f"    Found {len(cards)} cards using: {sel}")
            break

    if not cards:
        # Debug: show what's on the page
        body_text = driver.find_element(By.TAG_NAME, "body").text[:300]
        print(f"    [WARN] No product cards found. Page start: {body_text!r}")
        return results

    for card in cards:
        try:
            # ── Title ────────────────────────────────────────────────────────
            title = ""
            for sel in ["h2", "h3", ".product-card__title", ".product-title",
                        "[class*='title']", "a[href*='/products/']"]:
                try:
                    title = clean(card.find_element(By.CSS_SELECTOR, sel).text)
                    if title:
                        break
                except Exception:
                    pass

            # ── Price ─────────────────────────────────────────────────────────
            price = ""
            for sel in [".price", ".product-price", "[class*='price']",
                        "span.money", ".price__regular", ".price-item"]:
                try:
                    raw = clean(card.find_element(By.CSS_SELECTOR, sel).text)
                    # Skip "SAVE $..." lines
                    if raw and not raw.upper().startswith("SAVE"):
                        price = extract_price(raw)
                        if price:
                            break
                except Exception:
                    pass

            if not title and not price:
                continue

            brand, desc = parse_title(title, size)
            results.append({
                "size":        size,
                "brand":       brand,
                "description": desc,
                "price":       price,
            })
            print(f"    {brand:<15} | {desc[:45]} | price={price}")

        except Exception as e:
            print(f"    [WARN] card parse error: {e}")

    return results


def scrape_size(driver, wait, width, profile, rim, size):
    """Navigate to the size-filtered page and scrape all products."""
    url = build_url(width, profile, rim)
    print(f"  Navigating: {url}")
    driver.get(url)
    time.sleep(3)

    # Check if any products loaded; if 0, try alternate URL patterns
    alt_urls = [
        f"{BASE_URL}?q={size.replace('/', '%2F')}",
        f"{BASE_URL}?filter.p.m.custom.width={width}&filter.p.m.custom.profile={profile}&filter.p.m.custom.rim={rim}",
        f"https://www.bobjane.com.au/search?type=product&q={width}+{profile}+{rim}",
    ]

    # Quick check for product cards
    found = any(
        driver.find_elements(By.CSS_SELECTOR, sel)
        for sel in ["div.product-card", "div.product-item", "div.grid__item",
                    "li.product-card", "article.product-card", "div.card-wrapper"]
    )

    if not found:
        for alt in alt_urls:
            print(f"  Trying alternate: {alt}")
            driver.get(alt)
            time.sleep(3)
            found = any(
                driver.find_elements(By.CSS_SELECTOR, sel)
                for sel in ["div.product-card", "div.product-item",
                            "div.grid__item", "li.product-card",
                            "article.product-card", "div.card-wrapper"]
            )
            if found:
                break

    # Click Load More until all products are visible
    load_all_results(driver, wait)

    # Scrape all visible product cards
    return scrape_products(driver, size)


def main():
    driver = init_driver()
    wait   = WebDriverWait(driver, 20)

    print(f"\nOutput: {OUTPUT_FILE}")
    print("Browser will open — do NOT close it during scraping.\n")

    try:
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(["SIZE", "brand", "DESCRIPTION", "PRICE"])

            for width, profile, rim, size in SEARCH_SIZES:
                print(f"\n=== {size} ===")
                try:
                    rows = scrape_size(driver, wait, width, profile, rim, size)
                    for r in rows:
                        writer.writerow([r["size"], r["brand"],
                                         r["description"], r["price"]])
                    f.flush()
                    print(f"  {len(rows)} rows written")
                except Exception as e:
                    print(f"  [ERROR] {e} — skipping {size}")

        print(f"\nDone. Saved to: {OUTPUT_FILE}")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
