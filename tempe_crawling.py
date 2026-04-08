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

# Format: width + aspect + rim (digits only), e.g. 175/65R14 → 1756514
# For no-aspect tyres: 185R14 → 18514
SEARCH_QUERIES = [
    ("1756514", "175/65R14"),
    ("1956515", "195/65R15"),
    ("2056516", "205/65R16"),
    ("2254517", "225/45R17"),
    ("2154517", "215/45R17"),
    ("2354517", "235/45R17"),
    ("2254018", "225/40R18"),
    ("2354519", "235/45R19"),
    ("2453520", "245/35R20"),
    ("2256517", "225/65R17"),
    ("2255518", "225/55R18"),
    ("2256018", "225/60R18"),
    ("2255519", "225/55R19"),
    ("2254519", "225/45R19"),
    ("2554520", "255/45R20"),
    ("2654021", "265/40R21"),
    ("2457016", "245/70R16"),
    ("2657016", "265/70R16"),
    ("2656018", "265/60R18"),
    ("2656517", "265/65R17"),
    ("2657516", "265/75R16"),
    ("18514",   "185R14"),
    ("19515",   "195R15"),
    ("2356516", "235/65R16"),
]

current_month = datetime.now().strftime('%b')
current_year  = datetime.now().strftime('%Y')
current_time  = datetime.now().strftime('%H%M')
OUTPUT_FILE   = f"Tempe_{current_month}_{current_year}_{current_time}.csv"


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
    print("\n" + "="*60)
    print("Chrome 브라우저가 열렸습니다.")
    print("1. 로그인 하세요")
    print("2. Search Tyres 탭으로 이동하세요")
    print("3. 준비되면 여기서 Enter를 누르세요")
    print("="*60)
    input(">>> Enter를 누르면 크롤링을 시작합니다: ")


def search_tyres(driver, wait, query):
    if "Product/Search" not in driver.current_url:
        driver.get(SEARCH_URL)
        time.sleep(2)

    # Click Clear to reset previous search results
    try:
        clear_btn = driver.find_element(By.XPATH,
            "//input[@value='Clear'] | //button[normalize-space()='Clear']")
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
            EC.element_to_be_clickable((By.XPATH,
                "//input[@value='Search'] | //button[normalize-space()='Search'] | //a[normalize-space()='Search']"
            ))
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

    # Wait for results — detect by "Get Cost" text in any element
    wait.until(EC.presence_of_element_located(
        (By.XPATH, "//*[contains(text(),'Get Cost')]")
    ))
    time.sleep(1)
    print("Results loaded.")


def click_all_get_costs(driver):
    """Click every visible 'Get Cost' element. Filters out hidden elements to avoid infinite loop."""
    short_wait = WebDriverWait(driver, 8)
    clicked = 0
    skipped = 0

    while True:
        # Only process VISIBLE elements — hidden ones (display:none) are excluded
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
            # Wait for any "Get Cost" element to disappear (total count drops)
            short_wait.until(lambda d: len(d.find_elements(
                By.XPATH, "//*[contains(text(),'Get Cost')]")) < prev_total)
            clicked += 1
        except Exception:
            # Mark this element as processed so we don't retry it
            skipped += 1
            try:
                driver.execute_script(
                    "arguments[0].textContent = 'N/A';", el)
            except Exception:
                pass
        time.sleep(0.2)

    print(f"  'Get Cost' clicked={clicked}, skipped={skipped}")


def scrape_rows(driver):
    """Click all Get Cost links first, then collect all row data."""
    click_all_get_costs(driver)

    # Scroll back to top so all rows are accessible
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
            # Fallback: first non-dollar non-size text that looks like a brand word
            if not brand:
                all_spans = row.find_elements(By.CSS_SELECTOR, "span.ng-binding")
                texts = [clean(el.text) for el in all_spans]
                for t in texts:
                    if t and not re.match(r'^\$', t) and not re.match(r'^\d{3}', t):
                        brand = t
                        break

            # All ng-binding span texts in DOM order
            all_spans = row.find_elements(By.CSS_SELECTOR, "span.ng-binding")
            texts = [clean(el.text) for el in all_spans]

            dollar_texts = [t for t in texts if re.match(r'^\$[\d.]', t)]
            other_texts  = [t for t in texts if t and not re.match(r'^\$', t)]

            # Cost: span.clickable inside col-xs-7
            cost = ""
            try:
                cost_el = row.find_element(By.CSS_SELECTOR,
                    "div[class*='col-xs-7'] span.clickable, div[class*='col-xs-7'] span.ng-binding")
                val = clean(cost_el.text)
                if re.match(r'^\$[\d.]', val):
                    cost = re.sub(r"[^\d.]", "", val)
            except Exception:
                cost = re.sub(r"[^\d.]", "", dollar_texts[0]) if dollar_texts else ""

            # Price: span inside col-xs-9
            price = ""
            try:
                price_el = row.find_element(By.CSS_SELECTOR,
                    "div[class*='col-xs-9'] span.ng-binding")
                val = clean(price_el.text)
                if re.match(r'^\$[\d.]', val):
                    price = re.sub(r"[^\d.]", "", val)
            except Exception:
                price = re.sub(r"[^\d.]", "", dollar_texts[1]) if len(dollar_texts) >= 2 else ""

            # DESCRIPTION: first non-$ ng-binding text (contains size + model)
            description = other_texts[0] if other_texts else ""

            if not description:
                continue

            results.append({"brand": brand, "description": description,
                            "cost": cost, "price": price})
            print(f"  {brand:<15} | {description[:50]} | cost={cost} | price={price}")

        except Exception as e:
            print(f"  [WARN] {e}")

    return results


def has_next_page(driver):
    try:
        btn = driver.find_element(
            By.CSS_SELECTOR,
            "a[rel='next'], li.next:not(.disabled) a, .pagination .next:not(.disabled) a"
        )
        return btn.is_displayed()
    except Exception:
        return False


def go_next_page(driver, wait):
    btn = driver.find_element(
        By.CSS_SELECTOR,
        "a[rel='next'], li.next:not(.disabled) a, .pagination .next:not(.disabled) a"
    )
    driver.execute_script("arguments[0].click();", btn)
    # Wait for Angular product rows to load (not table — Angular uses divs)
    wait.until(EC.presence_of_element_located(
        (By.CSS_SELECTOR, "div.product-data-list")))
    time.sleep(1.5)


def main():
    driver = init_driver()
    wait   = WebDriverWait(driver, 25)

    try:
        wait_for_manual_login(driver)

        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(["SIZE", "brand", "DESCRIPTION", "COST", "PRICE"])

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
                        writer.writerow([size, r["brand"], r["description"],
                                         r["cost"], r["price"]])
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
