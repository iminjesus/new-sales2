import re
import csv
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

SEARCH_URL = "https://orders.tempetyreswholesale.com.au/WebOrder/Product/Search"

BRAND = "hankook"

# All sizes from the Passenger / LT "Common Searches" panel.
# Format: (digits-only-size, display-size)
SIZES = [
    ("1756514", "175/65R14"),
    ("1856015", "185/60R15"),
    ("1856514", "185/65R14"),
    ("1856515", "185/65R15"),
    ("1956015", "195/60R15"),
    ("1956515", "195/65R15"),
    ("2055017", "205/50R17"),
    ("2055516", "205/55R16"),
    ("2056016", "205/60R16"),
    ("2056515", "205/65R15"),
    ("2056516", "205/65R16"),
    ("2057515", "205/75R15"),
    ("2154517", "215/45R17"),
    ("2155516", "215/55R16"),
    ("2155517", "215/55R17"),
    ("2156016", "215/60R16"),
    ("2156017", "215/60R17"),
    ("2156516", "215/65R16"),
    ("2156517", "215/65R17"),
    ("2157015", "215/70R15"),
    ("2157016", "215/70R16"),
    ("2254517", "225/45R17"),
    ("2255017", "225/50R17"),
    ("2255517", "225/55R17"),
    ("2256016", "225/60R16"),
    ("2256017", "225/60R17"),
    ("2256516", "225/65R16"),
    ("2256517", "225/65R17"),
    ("2257016", "225/70R16"),
    ("2257515", "225/75R15"),
    ("2257516", "225/75R16"),
    ("2355517", "235/55R17"),
    ("2355518", "235/55R18"),
    ("2356017", "235/60R17"),
    ("2356018", "235/60R18"),
    ("2356516", "235/65R16"),
    ("2356517", "235/65R17"),
    ("2357016", "235/70R16"),
    ("2357515", "235/75R15"),
    ("2358516", "235/85R16"),
    ("2456018", "245/60R18"),
    ("2456517", "245/65R17"),
    ("2457017", "245/70R17"),
    ("2457516", "245/75R16"),
    ("2657017", "265/70R17"),
    ("2657516", "265/75R16"),
    ("2755520", "275/55R20"),
    ("2756020", "275/60R20"),
    ("2756518", "275/65R18"),
    ("2757018", "275/70R18"),
]

# Build queries like "hankook 1756514"
SEARCH_QUERIES = [(f"{BRAND} {digits}", display) for digits, display in SIZES]

OUTPUT_FILE = datetime.now().strftime("Tempe_HK_%Y%m%d_%H%M.csv")


def clean(text):
    return text.strip() if text else ""


def init_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--window-size=1280,900")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    return webdriver.Chrome(options=options)


def wait_for_manual_login(driver):
    driver.get("https://orders.tempetyreswholesale.com.au/weborder")
    print("\n" + "=" * 60)
    print("Chrome 브라우저가 열렸습니다.")
    print("1. 로그인 하세요")
    print("2. Search Tyres 탭으로 이동하세요")
    print("3. 준비되면 여기서 Enter를 누르세요")
    print("=" * 60)
    input(">>> Enter를 누르면 크롤링을 시작합니다: ")


def search_tyres(driver, wait, query):
    if "Product/Search" not in driver.current_url:
        driver.get(SEARCH_URL)
        time.sleep(2)

    # Click Clear to reset previous search results
    try:
        clear_btn = driver.find_element(
            By.XPATH,
            "//input[@value='Clear'] | //button[normalize-space()='Clear']",
        )
        driver.execute_script("arguments[0].click();", clear_btn)
        time.sleep(0.8)
    except Exception:
        pass

    search_input = wait.until(
        EC.presence_of_element_located((By.ID, "simpleSearchText"))
    )
    search_input.clear()
    search_input.send_keys(query)
    time.sleep(1)

    # Click the red Search button inside the dropdown
    try:
        search_btn = wait.until(
            EC.element_to_be_clickable(
                (
                    By.XPATH,
                    "//input[@value='Search'] | //button[normalize-space()='Search'] | //a[normalize-space()='Search']",
                )
            )
        )
        driver.execute_script("arguments[0].click();", search_btn)
        print("Clicked Search button")
    except Exception:
        search_input.send_keys(Keys.RETURN)
        print("Pressed Enter")

    time.sleep(2)

    # Close the dropdown by pressing Escape
    driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
    time.sleep(1)

    # Wait for results — detect by "Get Cost" text in any element.
    # If no results at all, we may not see "Get Cost"; bail out gracefully.
    try:
        wait.until(
            EC.presence_of_element_located(
                (By.XPATH, "//*[contains(text(),'Get Cost')]")
            )
        )
    except Exception:
        print("  No 'Get Cost' element found — likely no results.")
    time.sleep(1)
    print("Results loaded.")


def click_all_get_costs(driver):
    """Click every visible 'Get Cost' element. Filters out hidden elements to avoid infinite loop."""
    short_wait = WebDriverWait(driver, 8)
    clicked = 0
    skipped = 0

    while True:
        all_els = driver.find_elements(By.XPATH, "//*[contains(text(),'Get Cost')]")
        els = [e for e in all_els if e.is_displayed()]
        if not els:
            break

        el = els[0]
        prev_total = len(all_els)

        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            time.sleep(0.25)
            driver.execute_script("arguments[0].click();", el)
            short_wait.until(
                lambda d: len(
                    d.find_elements(By.XPATH, "//*[contains(text(),'Get Cost')]")
                )
                < prev_total
            )
            clicked += 1
        except Exception:
            skipped += 1
            try:
                driver.execute_script("arguments[0].textContent = 'N/A';", el)
            except Exception:
                pass
        time.sleep(0.2)

    print(f"  'Get Cost' clicked={clicked}, skipped={skipped}")


def extract_on_hand(row):
    """Extract ON HAND stock value (e.g. '8+', '1', '6') from a product row.

    The value sits in the ON HAND column as a green badge between PRICE and QTY.
    Try a series of selectors, then fall back to a regex over the row text that
    finds a standalone integer (optionally followed by '+') that is NOT a $price,
    NOT a tyre size, and NOT inside an input field.
    """
    selector_candidates = [
        "[class*='on-hand'] *",
        "[class*='onhand'] *",
        "[class*='OnHand'] *",
        "[class*='stock'] span",
        "[class*='Stock'] span",
        "[class*='inventory'] span",
        "[class*='available'] span",
        "[class*='qoh'] span",
        "span[class*='badge']",
        "div[class*='badge']",
        "span.label",
        "[ng-bind*='OnHand']",
        "[ng-bind*='onHand']",
        "[ng-bind*='Stock']",
        "[ng-bind*='Inventory']",
        "[ng-bind*='Available']",
    ]
    for sel in selector_candidates:
        try:
            for el in row.find_elements(By.CSS_SELECTOR, sel):
                txt = clean(el.text)
                if re.fullmatch(r"\d{1,3}\+?", txt):
                    return txt
        except Exception:
            continue

    # Fallback: scan all visible text nodes inside the row, skipping inputs.
    try:
        candidates = row.find_elements(
            By.XPATH,
            ".//*[not(self::input) and not(self::textarea) and not(self::script) and not(self::style)]",
        )
        for el in candidates:
            try:
                txt = clean(el.text)
            except Exception:
                continue
            if not txt or "\n" in txt:
                continue
            if re.fullmatch(r"\d{1,3}\+?", txt):
                # Heuristic: ON HAND values are short. Skip pure "0" since QTY
                # input may be empty-rendered. Accept everything else.
                return txt
    except Exception:
        pass

    return ""


def scrape_rows(driver):
    """Click all Get Cost links first, then collect all row data."""
    click_all_get_costs(driver)

    driver.execute_script("window.scrollTo(0, 0);")
    time.sleep(0.5)

    rows = driver.find_elements(By.CSS_SELECTOR, "div.product-data-list")
    print(f"  Collecting {len(rows)} rows")

    results = []
    for row in rows:
        try:
            # Brand name
            brand = ""
            for sel in [
                "div[class*='brand'] span",
                "span[class*='brand']",
                "div.brand span",
                "div[ng-bind*='brand']",
                "span[ng-bind*='brand']",
            ]:
                try:
                    brand = clean(row.find_element(By.CSS_SELECTOR, sel).text)
                    if brand:
                        break
                except Exception:
                    pass
            if not brand:
                all_spans = row.find_elements(By.CSS_SELECTOR, "span.ng-binding")
                texts = [clean(el.text) for el in all_spans]
                for t in texts:
                    if t and not re.match(r"^\$", t) and not re.match(r"^\d{3}", t):
                        brand = t
                        break

            all_spans = row.find_elements(By.CSS_SELECTOR, "span.ng-binding")
            texts = [clean(el.text) for el in all_spans]

            dollar_texts = [t for t in texts if re.match(r"^\$[\d.]", t)]
            other_texts = [t for t in texts if t and not re.match(r"^\$", t)]

            # Cost
            cost = ""
            try:
                cost_el = row.find_element(
                    By.CSS_SELECTOR,
                    "div[class*='col-xs-7'] span.clickable, div[class*='col-xs-7'] span.ng-binding",
                )
                val = clean(cost_el.text)
                if re.match(r"^\$[\d.]", val):
                    cost = re.sub(r"[^\d.]", "", val)
            except Exception:
                cost = re.sub(r"[^\d.]", "", dollar_texts[0]) if dollar_texts else ""

            # Price
            price = ""
            try:
                price_el = row.find_element(
                    By.CSS_SELECTOR, "div[class*='col-xs-9'] span.ng-binding"
                )
                val = clean(price_el.text)
                if re.match(r"^\$[\d.]", val):
                    price = re.sub(r"[^\d.]", "", val)
            except Exception:
                price = (
                    re.sub(r"[^\d.]", "", dollar_texts[1])
                    if len(dollar_texts) >= 2
                    else ""
                )

            # ON HAND (green badge in ON HAND column, e.g. "8+", "1", "6")
            on_hand = extract_on_hand(row)

            full_desc = (
                other_texts[1]
                if len(other_texts) > 1
                else other_texts[0]
                if other_texts
                else ""
            )
            desc = full_desc
            if brand and desc.lower().startswith(brand.lower()):
                desc = desc[len(brand):].strip()
            desc = re.sub(r"^\d{3}/\d{2}[A-Za-z]\d{2}\s*", "", desc).strip()
            desc = re.sub(r"^\d{3}[A-Za-z]\d{2}\s*", "", desc).strip()

            if not desc:
                desc = full_desc

            if not desc:
                continue

            # Filter: only keep Hankook results
            if brand and BRAND.lower() not in brand.lower():
                continue

            # SKU — appears as "SKU: 1010819" somewhere in the row
            sku = ""
            try:
                row_text = row.text or ""
                m = re.search(r"SKU\s*[:#]?\s*([A-Za-z0-9\-]+)", row_text)
                if m:
                    sku = m.group(1)
            except Exception:
                pass

            results.append(
                {
                    "brand": brand,
                    "description": desc,
                    "cost": cost,
                    "price": price,
                    "sku": sku,
                    "on_hand": on_hand,
                }
            )
            print(
                f"  {brand:<15} | SKU={sku:<12} | {desc[:50]} | "
                f"cost={cost} | price={price} | on_hand={on_hand}"
            )

        except Exception as e:
            print(f"  [WARN] {e}")

    return results


def has_next_page(driver):
    try:
        btn = driver.find_element(
            By.CSS_SELECTOR,
            "a[rel='next'], li.next:not(.disabled) a, .pagination .next:not(.disabled) a",
        )
        return btn.is_displayed()
    except Exception:
        return False


def go_next_page(driver, wait):
    btn = driver.find_element(
        By.CSS_SELECTOR,
        "a[rel='next'], li.next:not(.disabled) a, .pagination .next:not(.disabled) a",
    )
    driver.execute_script("arguments[0].click();", btn)
    wait.until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "div.product-data-list"))
    )
    time.sleep(1.5)


def main():
    driver = init_driver()
    wait = WebDriverWait(driver, 25)

    try:
        wait_for_manual_login(driver)

        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(
                ["SIZE", "brand", "SKU", "DESCRIPTION", "COST", "PRICE", "ON_HAND"]
            )

            for query, size in SEARCH_QUERIES:
                print(f"\n=== Searching: {query}  ({size}) ===")
                try:
                    search_tyres(driver, wait, query)
                except Exception as e:
                    print(f"  [ERROR] search failed: {e} — skipping")
                    continue

                page = 1
                while True:
                    print(f"  -- Page {page} --")
                    rows = scrape_rows(driver)
                    for r in rows:
                        writer.writerow(
                            [
                                size,
                                r["brand"],
                                r["sku"],
                                r["description"],
                                r["cost"],
                                r["price"],
                                r["on_hand"],
                            ]
                        )
                    f.flush()
                    print(f"  {len(rows)} rows written")

                    if has_next_page(driver):
                        go_next_page(driver, wait)
                        page += 1
                    else:
                        break

        print(f"\nDone. Saved to: {OUTPUT_FILE}")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
