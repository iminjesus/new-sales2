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


def select_option(driver, select_el, value):
    """Select an option from a <select> element by value or visible text."""
    from selenium.webdriver.support.ui import Select
    sel = Select(select_el)
    try:
        sel.select_by_value(str(value))
        return True
    except Exception:
        pass
    # Try partial text match
    for option in sel.options:
        if str(value) in option.text:
            sel.select_by_visible_text(option.text)
            return True
    return False


def apply_size_filter(driver, wait, width, profile, rim, size):
    """
    Use the Bob Jane on-page size filter dropdowns:
      Width → Profile/Aspect Ratio → Rim/Diameter → click Filter By Size
    Returns True if filter was successfully applied.
    """
    try:
        # Wait for filter section to be present
        wait.until(EC.presence_of_element_located(
            (By.XPATH, "//*[contains(text(),'Filter By Size') or "
                       "contains(text(),'By Size') or "
                       "contains(text(),'By Width')]")))
        time.sleep(1)
    except TimeoutException:
        return False

    selects = driver.find_elements(By.CSS_SELECTOR, "select")
    if len(selects) < 2:
        return False

    # Bob Jane filter has 3 selects: Width, Profile, Rim
    try:
        select_option(driver, selects[0], width)
        time.sleep(0.8)
        # Refresh selects as DOM may update after width selection
        selects = driver.find_elements(By.CSS_SELECTOR, "select")
        if profile and len(selects) > 1:
            select_option(driver, selects[1], profile)
            time.sleep(0.8)
        selects = driver.find_elements(By.CSS_SELECTOR, "select")
        if len(selects) > 2:
            select_option(driver, selects[2], rim)
            time.sleep(0.8)
    except Exception as e:
        print(f"  [WARN] dropdown select failed: {e}")
        return False

    # Click Filter By Size button
    for xpath in [
        "//button[contains(text(),'Filter By Size')]",
        "//button[contains(text(),'Filter')]",
        "//input[@value='Filter By Size']",
        "//a[contains(text(),'Filter By Size')]",
    ]:
        btns = driver.find_elements(By.XPATH, xpath)
        visible = [b for b in btns if b.is_displayed()]
        if visible:
            driver.execute_script("arguments[0].click();", visible[0])
            print(f"  Filter applied: {size}")
            time.sleep(3)
            return True

    return False


MAX_LOAD_MORE = 15  # safety cap — if filter works, 1 size should need far fewer


def load_all_results(driver, wait, size=""):
    """Keep clicking 'Load More' until button disappears or cap is reached."""
    loaded = 0
    while loaded < MAX_LOAD_MORE:
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
            time.sleep(2.5)
        except Exception as e:
            print(f"    Load More error: {e}")
            break

    if loaded >= MAX_LOAD_MORE:
        print(f"    [WARN] Hit Load More cap ({MAX_LOAD_MORE}) — "
              f"URL filter may not be working for {size}")
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
            # ── Promo banner (e.g. "Buy 3 Get 1 Free") ───────────────────────
            promo = ""
            for sel in [
                "[class*='badge']", "[class*='Banner']", "[class*='banner']",
                "[class*='promo']", "[class*='tag']", "[class*='label']",
                "[class*='ribbon']", "[class*='sticker']",
            ]:
                try:
                    raw = clean(card.find_element(By.CSS_SELECTOR, sel).text)
                    if raw and len(raw) < 60:
                        promo = raw
                        break
                except Exception:
                    pass

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

            # ── Price (normal per tyre) ───────────────────────────────────────
            price = ""
            for sel in [".price", ".product-price", "[class*='price']",
                        "span.money", ".price__regular", ".price-item"]:
                try:
                    els = card.find_elements(By.CSS_SELECTOR, sel)
                    for el in els:
                        raw = clean(el.text)
                        if raw and not raw.upper().startswith("SAVE"):
                            p = extract_price(raw)
                            if p:
                                price = p
                                break
                    if price:
                        break
                except Exception:
                    pass

            # ── Save text (e.g. "SAVE $134 ON A SET OF 4") ───────────────────
            save_text = ""
            full_text = clean(card.text)
            m = re.search(r'(SAVE\s+\$[\d,]+[^\n]*)', full_text, re.IGNORECASE)
            if m:
                save_text = m.group(1).strip()

            # ── Calculate discounted per-tyre price ───────────────────────────
            disc_price = ""
            if save_text and price:
                # "SAVE $134 ON A SET OF 4" → save 134/4 = 33.50 per tyre
                m_save = re.search(r'SAVE\s+\$([\d,]+)\s+ON\s+A\s+SET\s+OF\s+(\d+)',
                                   save_text, re.IGNORECASE)
                if m_save:
                    save_total = float(m_save.group(1).replace(',', ''))
                    set_of     = int(m_save.group(2))
                    try:
                        disc = round(float(price) - save_total / set_of, 2)
                        disc_price = f"{disc:.2f}"
                    except Exception:
                        pass
                # "Buy 3 Get 1 Free" → effective price = 3/4 of normal
                elif promo and re.search(r'buy\s*3\s*get\s*1\s*free', promo, re.IGNORECASE):
                    try:
                        disc = round(float(price) * 3 / 4, 2)
                        disc_price = f"{disc:.2f}"
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
                "disc_price":  disc_price,
                "promo":       promo,
                "save_text":   save_text,
            })
            print(f"    {brand:<15} | {desc[:40]} | ${price}"
                  + (f" → ${disc_price} ({save_text})" if disc_price else ""))

        except Exception as e:
            print(f"    [WARN] card parse error: {e}")

    return results


def scrape_size(driver, wait, width, profile, rim, size):
    """Navigate to the base page, apply the size filter UI, then scrape."""
    # Always start from the base collections page (filter UI lives there)
    if BASE_URL not in driver.current_url:
        driver.get(BASE_URL)
        time.sleep(3)

    # Try the on-page filter dropdowns first (most reliable)
    filter_ok = apply_size_filter(driver, wait, width, profile, rim, size)

    if not filter_ok:
        # Fallback: try URL-based filter parameters
        print(f"  Filter UI failed, trying URL params...")
        driver.get(BASE_URL)
        time.sleep(2)
        driver.get(f"{BASE_URL}?width={width}&profile={profile}&rim={rim}")
        time.sleep(3)
        filter_ok = apply_size_filter(driver, wait, width, profile, rim, size)

    # Click Load More until all products are visible
    load_all_results(driver, wait, size)

    # Scrape all visible product cards, filtering to matching size only
    all_products = scrape_products(driver, size)

    # If URL filter didn't work, the page may have returned all sizes —
    # keep only rows whose title or description contains the target size
    size_pattern = re.sub(r'[/R]', r'.?', size)   # "175/65R14" → "175.?65.?14"
    matched = [r for r in all_products
               if re.search(size_pattern, r["description"] + r["brand"], re.IGNORECASE)
               or re.search(size_pattern, r["size"], re.IGNORECASE)]
    if len(matched) < len(all_products):
        print(f"  Filtered {len(all_products)} → {len(matched)} products matching {size}")
    return matched if matched else all_products


def main():
    driver = init_driver()
    wait   = WebDriverWait(driver, 20)

    print(f"\nOutput: {OUTPUT_FILE}")
    print("Browser will open — do NOT close it during scraping.\n")

    try:
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(["SIZE", "brand", "DESCRIPTION",
                             "PRICE", "DISC_PRICE", "PROMO", "SAVE_TEXT"])

            for width, profile, rim, size in SEARCH_SIZES:
                print(f"\n=== {size} ===")
                try:
                    rows = scrape_size(driver, wait, width, profile, rim, size)
                    for r in rows:
                        writer.writerow([r["size"], r["brand"], r["description"],
                                         r["price"], r["disc_price"],
                                         r["promo"], r["save_text"]])
                    f.flush()
                    print(f"  {len(rows)} rows written")
                except Exception as e:
                    print(f"  [ERROR] {e} — skipping {size}")

        print(f"\nDone. Saved to: {OUTPUT_FILE}")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
