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

OUTPUT_FILE = datetime.now().strftime("Tempeweborder_%Y%m%d_%H%M.csv")


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
    print("1. 로그인만 완료하세요 (Search Tyres 탭 이동은 자동으로 처리합니다)")
    print("2. 준비되면 여기서 Enter를 누르세요")
    print("="*60)
    input(">>> Enter를 누르면 Search 페이지로 자동 이동 + 크롤링을 시작합니다: ")

    # Auto-navigate to the Search Tyres page after login so the user
    # does NOT need to click the tab manually.  Previously the user
    # had to open the Search Tyres tab themselves and the first
    # search_tyres() call would silently time out on simpleSearchText
    # if they forgot — now we drive the browser there and WAIT for
    # the search input to actually appear before returning control
    # to the main crawl loop.
    print("→ Search Tyres 페이지로 자동 이동 중…")
    driver.get(SEARCH_URL)
    # Give the Angular / server-rendered search page up to 60 s to
    # finish booting.  The first search in a fresh session often
    # needs more than the normal 25 s because auth cookies, product
    # inventory and pricing are all warming up at once.
    try:
        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.ID, "simpleSearchText"))
        )
        print("→ Search 페이지 로드 완료 — 크롤링을 시작합니다.")
    except Exception:
        # Don't abort here — search_tyres() will try its own fallback
        # locators next and print a clearer diagnostic if the input
        # really is missing.
        print(f"→ [WARN] Search 입력창(simpleSearchText)이 60초 안에 나타나지 않음")
        print(f"→        URL  = {driver.current_url!r}")
        print(f"→        Title= {driver.title!r}")
        print(f"→        그래도 크롤링을 시도합니다 (fallback locators 존재).")


def search_tyres(driver, wait, query):
    # Force the search page every call.  The old "if Product/Search not
    # in current_url" guard assumed the user stayed on the Search tab;
    # if the first search times out and we re-enter here, the browser
    # may have landed on an error page where the Clear / search-input
    # elements no longer exist.  Reloading is cheap and keeps the DOM
    # in a known state.
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

    # Try multiple selectors for the search input — the site may have
    # renamed the id, or shown a different control depending on which
    # tab the user landed on after login.  Fall back on <input> whose
    # placeholder / name hints at search.
    try:
        search_input = wait.until(
            EC.presence_of_element_located((By.ID, "simpleSearchText"))
        )
    except Exception as e:
        # Diagnostic so the user can see WHY the wait timed out
        # (empty "Message:" by itself was useless).
        print(f"  [DIAG] simpleSearchText not found on {driver.current_url!r}")
        print(f"  [DIAG] page title: {driver.title!r}")
        # Fallback locators — any of these should work if the search
        # tab is open under a different control id.
        alt_locators = [
            (By.CSS_SELECTOR, "input[placeholder*='Search' i]"),
            (By.CSS_SELECTOR, "input[name*='search' i][type='text']"),
            (By.CSS_SELECTOR, "input[type='search']"),
        ]
        search_input = None
        for loc in alt_locators:
            try:
                search_input = driver.find_element(*loc)
                print(f"  [DIAG] fell back to {loc}")
                break
            except Exception:
                continue
        if search_input is None:
            # Re-raise with a meaningful message so the top-level
            # try/except in main() prints something useful instead
            # of an empty Selenium Message.
            raise RuntimeError(
                f"Search input not found on {driver.current_url}. "
                f"Make sure you are on the 'Search Tyres' tab before "
                f"pressing Enter."
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

    # Wait for results — detect by "Get Cost" text in any element.
    # Longer timeout here (45 s) because the results table can be slow
    # on the first search of a session when auth / inventory warm up.
    long_wait = WebDriverWait(driver, 45)
    try:
        long_wait.until(EC.presence_of_element_located(
            (By.XPATH, "//*[contains(text(),'Get Cost')]")
        ))
    except Exception:
        # No "Get Cost" after 45 s likely means the search returned
        # zero results for this size.  Not an error — just no rows.
        print(f"  [INFO] no 'Get Cost' seen for {query} within 45s "
              f"(URL={driver.current_url}) — treating as 0 results")
        return
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

            # Oct-23 user clarification: stock is the COLUMN right
            # after sell-in and sell-out prices.  So the DOM is a row
            # of bootstrap col-xs-* divs, cost sits in one, price in
            # the next, stock in the one after that.
            #
            # Capture strategy — three paths tried in order, first hit
            # wins so a theme change on either direction still works:
            #   1. XPath walk — find the price's col-xs ancestor and
            #      grab the NEXT col-xs sibling.  This matches the
            #      user's description exactly.
            #   2. Enumerate every col-xs div in the row, record their
            #      text in DOM order, pick the first integer-looking
            #      text that comes AFTER the one carrying the price.
            #   3. Explicit "stock" / "availability" class selectors
            #      as a last resort.
            stock = ""
            try:
                _nx = row.find_element(
                    By.XPATH,
                    ".//div[contains(@class,'col-xs-9')]"
                    "/following-sibling::div[contains(@class,'col-xs')][1]")
                _s = clean(_nx.text or "")
                _m = re.search(r"(\d+)", _s)
                if _m: stock = _m.group(1)
            except Exception:
                pass
            if not stock:
                try:
                    _cols = row.find_elements(
                        By.CSS_SELECTOR, "div[class*='col-xs']")
                    _texts = [clean(c.text or "") for c in _cols]
                    _price_idx = -1
                    for i, t in enumerate(_texts):
                        if price and price in t:
                            _price_idx = i; break
                    if _price_idx != -1:
                        for t in _texts[_price_idx + 1:]:
                            _m = re.fullmatch(r"\$?\s*(\d{1,5})\+?", (t or "").strip())
                            if _m:
                                stock = _m.group(1); break
                except Exception:
                    pass
            if not stock:
                try:
                    _st = row.find_element(By.CSS_SELECTOR,
                        "[class*='stock'] span, span[class*='stock'], "
                        "[class*='availability'] span, span[class*='availability']")
                    _s = clean(_st.text or "")
                    _m = re.search(r"(\d+)", _s)
                    if _m: stock = _m.group(1)
                except Exception:
                    pass

            # Full description is other_texts[1] (other_texts[0] is the brand name)
            # Strip leading brand and size to keep only load rating + model name
            full_desc = other_texts[1] if len(other_texts) > 1 else other_texts[0] if other_texts else ""
            desc = full_desc
            # Remove brand prefix (case-insensitive)
            if brand and desc.lower().startswith(brand.lower()):
                desc = desc[len(brand):].strip()
            # Remove size prefix e.g. "175/65R14" or "185R14" — Oct-23:
            # allow decimal rim diameters (265/70R19.5, 11R22.5, …) so
            # the .5 isn't left dangling on the description.
            desc = re.sub(r'^\d{3}/\d{2}[A-Za-z]\d{2}(?:\.\d)?\s*', '', desc).strip()
            desc = re.sub(r'^\d{3}[A-Za-z]\d{2}(?:\.\d)?\s*', '', desc).strip()

            if not desc:
                desc = full_desc  # fallback to full text if stripping removed everything

            if not desc:
                continue

            results.append({"brand": brand, "description": desc,
                            "cost": cost, "price": price, "stock": stock})
            print(f"  {brand:<15} | {desc[:50]} | cost={cost} | price={price} | stock={stock or '—'}")

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
            # Oct-23 user ask: capture STOCK column (right of PRICE)
            # so the Price dashboard can show it on hover later.
            writer.writerow(["SIZE", "brand", "DESCRIPTION", "COST", "PRICE", "STOCK"])

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
                                         r["cost"], r["price"],
                                         r.get("stock", "")])
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
