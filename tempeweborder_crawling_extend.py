"""
tempeweborder_crawling_extend.py
──────────────────────────────────────────────────────────────────────
Extended Tempe-Weborder crawler — scrapes EVERY on-sale tyre for a
curated list of brands across BOTH "Passenger / LT" and "Medium Truck"
categories, instead of the digit-only size queries the original
tempeweborder_crawling.py script walks.

Workflow after login:
  1. Open Search page.
  2. Tick "On Sale Only".
  3. For each category (Passenger / LT, Medium Truck):
       for each brand in BRANDS:
         • Clear previous filter.
         • Open Manufacturer modal.
         • Switch to the category tab.
         • Click the brand logo / name.
         • Close modal.
         • Click the main Search button.
         • Walk every result page, scraping brand / description /
           cost / price / size (size is extracted from DESCRIPTION
           because this crawl no longer filters by size).

Output CSV columns match the original crawler so the dashboard's
build_tw_lookup() picks it up without changes:
     SIZE, brand, DESCRIPTION, COST, PRICE
plus an extra CATEGORY column at the end for debugging.

Run:   python tempeweborder_crawling_extend.py
"""
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

# ── 14 brands the user highlighted in the Oct-12 Manufacturer screenshot
#    (red rectangles).  Scrape each one under both Passenger/LT and
#    Medium Truck tabs. ------------------------------------------------
BRANDS = [
    "BF GOODRICH",
    "BRIDGESTONE",
    "CONTINENTAL",
    "DUNLOP",
    "FALKEN",
    "GOODYEAR",
    "HANKOOK",
    "KUMHO",
    "MAXXIS",
    "MICHELIN",
    "NEXEN",
    "PIRELLI",
    "TOYO",
    "YOKOHAMA",
]

# Tab labels in the Manufacturer modal (visible as pills across the top).
CATEGORIES = ["Passenger / LT", "Medium Truck"]

OUTPUT_FILE = datetime.now().strftime("Tempeweborder_extend_%Y%m%d_%H%M.csv")

# Size regex extracted from the DESCRIPTION column, since this
# extended crawl doesn't search by size any more.  Supports the
# usual "<width>/<aspect>R<rim>" shape plus commercial "<w>R<rim>"
# vans / "<w>/<asp>R<rim>C" C-rated specs.
SIZE_RE = re.compile(r'\b(\d{3}(?:/\d{2})?R\d{2}C?)\b', re.IGNORECASE)


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
    print("1. 로그인만 완료하세요 (Search Tyres 탭 이동은 자동으로 처리합니다)")
    print("2. 준비되면 여기서 Enter를 누르세요")
    print("=" * 60)
    input(">>> Enter를 누르면 Search 페이지로 자동 이동 + 크롤링을 시작합니다: ")

    print("→ Search Tyres 페이지로 자동 이동 중…")
    driver.get(SEARCH_URL)
    try:
        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.ID, "simpleSearchText"))
        )
        print("→ Search 페이지 로드 완료.")
    except Exception:
        print(f"→ [WARN] simpleSearchText가 60초 안에 나타나지 않음 (URL={driver.current_url!r})")


# ── Filter helpers ──────────────────────────────────────────────────────

def check_on_sale_only(driver):
    """Tick the 'On Sale Only' checkbox once at the top of the run."""
    try:
        cb = driver.find_element(
            By.XPATH,
            "//input[@type='checkbox' and "
            "(following-sibling::*[contains(.,'On Sale Only')] or "
            " preceding-sibling::*[contains(.,'On Sale Only')] or "
            " ../*[contains(.,'On Sale Only')])]"
        )
        if not cb.is_selected():
            driver.execute_script("arguments[0].click();", cb)
            print("  → 'On Sale Only' checked")
            time.sleep(0.4)
        else:
            print("  → 'On Sale Only' already checked")
    except Exception as e:
        print(f"  [WARN] 'On Sale Only' checkbox 못 찾음: {e}")


def click_clear(driver):
    """Reset the search so a fresh brand filter can be applied."""
    try:
        btn = driver.find_element(
            By.XPATH,
            "//input[@value='Clear'] | //button[normalize-space()='Clear']"
        )
        driver.execute_script("arguments[0].click();", btn)
        time.sleep(0.6)
    except Exception:
        pass


def open_manufacturer_modal(driver, wait):
    """Open the Manufacturer modal (grey 'Manufacturer' pill)."""
    btn = wait.until(EC.element_to_be_clickable((
        By.XPATH,
        "//input[@value='Manufacturer'] | //button[normalize-space()='Manufacturer'] "
        "| //*[self::a or self::div][normalize-space(text())='Manufacturer']"
    )))
    driver.execute_script("arguments[0].click();", btn)
    time.sleep(1.0)


def select_category_tab(driver, category):
    """Switch between 'Passenger / LT' and 'Medium Truck' tabs inside
    the Manufacturer modal.  Tab labels are matched exactly (case-sensitive)
    so stray whitespace or re-casing on the site WILL miss — adjust
    CATEGORIES above if the DOM changes. """
    try:
        tab = driver.find_element(
            By.XPATH,
            f"//*[self::a or self::button or self::div or self::li]"
            f"[normalize-space()={category!r}]"
        )
        driver.execute_script("arguments[0].click();", tab)
        time.sleep(0.5)
    except Exception as e:
        print(f"  [WARN] category tab {category!r} 못 찾음: {e}")


def select_brand(driver, brand):
    """Click a brand logo / name in the Manufacturer modal.  Brand
    names in the modal use mixed case ("Hankook", "BF Goodrich") so
    the XPath normalises to upper-case for an exact match against the
    all-caps BRANDS list. """
    uppercase = ("translate(normalize-space(.),"
                 "'abcdefghijklmnopqrstuvwxyz',"
                 "'ABCDEFGHIJKLMNOPQRSTUVWXYZ')")
    # Try text match on label-like elements first.
    try:
        el = driver.find_element(
            By.XPATH,
            f"//*[self::a or self::div or self::span or self::li or self::button]"
            f"[{uppercase}={brand.upper()!r}]"
        )
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        time.sleep(0.2)
        driver.execute_script("arguments[0].click();", el)
        time.sleep(0.4)
        return True
    except Exception as e:
        print(f"  [WARN] brand {brand!r} 못 찾음: {e}")
        return False


def close_modal(driver):
    """Close the Manufacturer modal by its ✕ button, or Escape."""
    for sel in [
        (By.XPATH, "//*[self::button or self::a or self::span][contains(@class,'close')]"),
        (By.XPATH, "//*[@aria-label='Close']"),
        (By.XPATH, "//button[normalize-space()='×']"),
        (By.XPATH, "//span[normalize-space()='×']"),
    ]:
        try:
            el = driver.find_element(*sel)
            if el.is_displayed():
                driver.execute_script("arguments[0].click();", el)
                time.sleep(0.4)
                return
        except Exception:
            continue
    # Fallback: send ESC
    try:
        driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
        time.sleep(0.4)
    except Exception:
        pass


def click_search(driver, wait):
    """Click the main red Search button to run the filtered query."""
    try:
        btn = wait.until(EC.element_to_be_clickable((
            By.XPATH,
            "//input[@value='Search'] | //button[normalize-space()='Search'] "
            "| //a[normalize-space()='Search']"
        )))
        driver.execute_script("arguments[0].click();", btn)
    except Exception:
        # Fallback: press Enter in the search input
        try:
            driver.find_element(By.ID, "simpleSearchText").send_keys(Keys.RETURN)
        except Exception:
            pass
    time.sleep(1.5)
    # Dismiss any lingering suggestion dropdown
    try:
        driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
    except Exception:
        pass


def wait_results(driver):
    """Wait up to 60 s for at least one result row to appear.  Returns
    True when results loaded, False when the brand/category combo had
    zero matches (common for commercial brands with no on-sale stock). """
    try:
        WebDriverWait(driver, 60).until(EC.any_of(
            EC.presence_of_element_located((By.XPATH, "//*[contains(text(),'Get Cost')]")),
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.product-data-list")),
        ))
    except Exception:
        print(f"  [INFO] no results in 60s (URL={driver.current_url}) — zero rows")
        return False
    time.sleep(1)
    return True


# ── Click + scrape helpers (reused from tempeweborder_crawling.py) ──────

def click_all_get_costs(driver):
    """Click every visible 'Get Cost' element so the COST column populates."""
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
            time.sleep(0.2)
            driver.execute_script("arguments[0].click();", el)
            short_wait.until(lambda d: len(d.find_elements(
                By.XPATH, "//*[contains(text(),'Get Cost')]")) < prev_total)
            clicked += 1
        except Exception:
            skipped += 1
            try:
                driver.execute_script("arguments[0].textContent = 'N/A';", el)
            except Exception:
                pass
        time.sleep(0.15)
    print(f"    'Get Cost' clicked={clicked}, skipped={skipped}")


def scrape_rows(driver):
    """Click all Get Cost links, then collect {brand, description, cost,
    price, size} dicts for every row on the current page."""
    click_all_get_costs(driver)
    driver.execute_script("window.scrollTo(0, 0);")
    time.sleep(0.4)
    rows = driver.find_elements(By.CSS_SELECTOR, "div.product-data-list")
    print(f"    Collecting {len(rows)} rows")
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
                    if t and not re.match(r'^\$', t) and not re.match(r'^\d{3}', t):
                        brand = t
                        break

            all_spans = row.find_elements(By.CSS_SELECTOR, "span.ng-binding")
            texts = [clean(el.text) for el in all_spans]
            dollar_texts = [t for t in texts if re.match(r'^\$[\d.]', t)]
            other_texts  = [t for t in texts if t and not re.match(r'^\$', t)]

            cost = ""
            try:
                cost_el = row.find_element(
                    By.CSS_SELECTOR,
                    "div[class*='col-xs-7'] span.clickable, div[class*='col-xs-7'] span.ng-binding"
                )
                val = clean(cost_el.text)
                if re.match(r'^\$[\d.]', val):
                    cost = re.sub(r"[^\d.]", "", val)
            except Exception:
                cost = re.sub(r"[^\d.]", "", dollar_texts[0]) if dollar_texts else ""

            price = ""
            try:
                price_el = row.find_element(
                    By.CSS_SELECTOR,
                    "div[class*='col-xs-9'] span.ng-binding"
                )
                val = clean(price_el.text)
                if re.match(r'^\$[\d.]', val):
                    price = re.sub(r"[^\d.]", "", val)
            except Exception:
                price = re.sub(r"[^\d.]", "", dollar_texts[1]) if len(dollar_texts) >= 2 else ""

            full_desc = other_texts[1] if len(other_texts) > 1 else other_texts[0] if other_texts else ""

            # SIZE: extract from the full description.  This replaces the
            # hard-coded SEARCH_QUERIES tag from the original crawler
            # because this run doesn't filter by size.
            size = ""
            m = SIZE_RE.search(full_desc)
            if m:
                size = m.group(1).upper()

            # Strip brand + size prefix from the description so the
            # saved value matches the compact pattern the original
            # crawler produced (load rating + model name).
            desc = full_desc
            if brand and desc.lower().startswith(brand.lower()):
                desc = desc[len(brand):].strip()
            desc = re.sub(r'^\d{3}(?:/\d{2})?R\d{2}C?\s*', '', desc, flags=re.IGNORECASE).strip()
            if not desc:
                desc = full_desc
            if not desc:
                continue

            results.append({
                "size": size,
                "brand": brand,
                "description": desc,
                "cost": cost,
                "price": price,
            })
            print(f"    {size:<12} | {brand:<12} | {desc[:50]} | ${cost}/${price}")
        except Exception as e:
            print(f"    [WARN] {e}")
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
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.product-data-list")))
    time.sleep(1.5)


# ── Main orchestrator ─────────────────────────────────────────────────────

def main():
    driver = init_driver()
    wait = WebDriverWait(driver, 25)
    try:
        wait_for_manual_login(driver)
        check_on_sale_only(driver)

        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(["SIZE", "brand", "DESCRIPTION", "COST", "PRICE", "CATEGORY"])

            for category in CATEGORIES:
                for brand in BRANDS:
                    print(f"\n=== {category}  |  {brand} ===")
                    click_clear(driver)
                    try:
                        open_manufacturer_modal(driver, wait)
                        select_category_tab(driver, category)
                        if not select_brand(driver, brand):
                            close_modal(driver)
                            continue
                        close_modal(driver)
                        click_search(driver, wait)
                    except Exception as e:
                        print(f"  [ERROR] filter 설정 실패: {e} — skip")
                        continue

                    if not wait_results(driver):
                        continue

                    page = 1
                    while True:
                        print(f"  -- Page {page} --")
                        rows = scrape_rows(driver)
                        for r in rows:
                            writer.writerow([
                                r["size"], r["brand"], r["description"],
                                r["cost"], r["price"], category,
                            ])
                        f.flush()
                        if not has_next_page(driver):
                            break
                        try:
                            go_next_page(driver, wait)
                        except Exception as e:
                            print(f"  [WARN] next page 실패: {e}")
                            break
                        page += 1

        print(f"\n✓ 저장 완료: {OUTPUT_FILE}")
    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    main()
