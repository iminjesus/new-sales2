"""
tempeweborder_crawling_extend.py
──────────────────────────────────────────────────────────────────────
Simplified brand-sweep crawler — tempeweborder_crawling.py loops
24 hard-coded SIZE codes; this one loops BRAND NAMES instead so every
on-sale size for each brand lands in one CSV without having to open
the Manufacturer modal.

Workflow after login:
  1. Auto-navigate to Search Tyres page (with retry).
  2. Tick "On Sale Only".
  3. For each brand in BRANDS:
       • Clear the search panel.
       • Type the brand name into #simpleSearchText (same text input
         the user types a size code into — e.g. "HANKOOK" works the
         same way the digit codes did).
       • Click the red Search button.
       • Wait for div.product-data-list to populate (polled every
         5 s up to 120 s so slow warm-up queries aren't skipped).
       • Click every "Get Cost" link, scrape rows, follow Next.
  4. Save Tempeweborder_extend_<YYYYMMDD>_<HHMM>.csv.

Output columns match the original crawler so the dashboard's
build_tw_lookup() reads it unchanged:
     SIZE, brand, DESCRIPTION, COST, PRICE
SIZE is extracted from the DESCRIPTION field via SIZE_RE because
this crawl no longer filters by size.

No Manufacturer modal, no Medium Truck tab, no category pass —
Oct-13 user confirmed those weren't needed.

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

LOGIN_URL  = "https://orders.tempetyreswholesale.com.au/weborder"
SEARCH_URL = "https://orders.tempetyreswholesale.com.au/WebOrder/Product/Search"

# 14 brands to sweep — the red-boxed logos in the Oct-12 Manufacturer
# screenshot.  Typed verbatim into the search input the same way the
# original crawler typed size codes.
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

OUTPUT_FILE = datetime.now().strftime("Tempeweborder_extend_%Y%m%d_%H%M.csv")

# Size regex extracted from the DESCRIPTION column (we no longer
# filter by size via the input).  Covers the usual passenger
# <width>/<aspect>R<rim> plus commercial <w>R<rim> / C-rated.
# Oct-23 user ask: accept decimal rim diameters (265/70R19.5, 11R22.5,
# 295/80R22.5, …) so truck sizes aren't truncated to the integer.
SIZE_RE = re.compile(r'\b(\d{3}(?:/\d{2})?R\d{2}(?:\.\d)?C?)\b', re.IGNORECASE)


def clean(text):
    return text.strip() if text else ""


def init_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--window-size=1280,900")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    return webdriver.Chrome(options=options)


def _nav_to_search_page(driver, label=""):
    """Navigate to the Search Tyres page and wait for #simpleSearchText.
    Retries up to 3 times — the first attempt right after login often
    hits a redirect while the session cookie settles. """
    for attempt in range(1, 4):
        print(f"→ Search Tyres 페이지로 이동 (시도 {attempt}/3) …")
        driver.get(SEARCH_URL)
        try:
            WebDriverWait(driver, 60).until(
                EC.presence_of_element_located((By.ID, "simpleSearchText"))
            )
            print(f"→ Search 페이지 로드 완료.  URL={driver.current_url}")
            return True
        except Exception:
            print(f"→ [WARN] simpleSearchText 60초 안에 못 봄 "
                  f"(URL={driver.current_url}, title={driver.title!r})")
            # If we got bounced to the login page, pause and let the
            # user re-login if needed.
            if "login" in driver.current_url.lower() or "signin" in driver.current_url.lower():
                print(f"→ [WARN] 로그인 페이지로 redirect됨 — 다시 로그인 후 Enter")
                input(">>> 로그인 완료 후 Enter: ")
            time.sleep(2)
    print(f"→ [ERROR] Search 페이지 3회 재시도 실패")
    return False


def wait_for_manual_login(driver):
    driver.get(LOGIN_URL)
    print("\n" + "=" * 60)
    print("Chrome 브라우저가 열렸습니다.")
    print("1. 로그인만 완료하세요")
    print("2. 준비되면 여기서 Enter를 누르세요")
    print("   (Search Tyres 페이지 이동은 자동으로 처리합니다)")
    print("=" * 60)
    input(">>> Enter: ")
    _nav_to_search_page(driver)


# ── Filter helpers ──────────────────────────────────────────────────────

def check_on_sale_only(driver):
    """Tick the 'On Sale Only' checkbox once at the top of the run.
    Multiple XPath variants because the checkbox / label markup
    varies across page states."""
    for xpath in [
        "//input[@type='checkbox' and @id and ../label[contains(.,'On Sale')]]",
        "//label[contains(.,'On Sale')]/preceding-sibling::input[@type='checkbox']",
        "//label[contains(.,'On Sale')]/following-sibling::input[@type='checkbox']",
        "//*[contains(text(),'On Sale Only')]/preceding::input[@type='checkbox'][1]",
        "//*[contains(text(),'On Sale Only')]/following::input[@type='checkbox'][1]",
    ]:
        try:
            cb = driver.find_element(By.XPATH, xpath)
            if not cb.is_selected():
                driver.execute_script("arguments[0].click();", cb)
                print("  → 'On Sale Only' checked")
                time.sleep(0.4)
            else:
                print("  → 'On Sale Only' already checked")
            return
        except Exception:
            continue
    print("  [WARN] 'On Sale Only' checkbox 못 찾음 — 확인 필요")


def click_clear(driver):
    """Reset previous search so a fresh brand query starts clean."""
    try:
        btn = driver.find_element(
            By.XPATH,
            "//input[@value='Clear'] | //button[normalize-space()='Clear']"
        )
        driver.execute_script("arguments[0].click();", btn)
        time.sleep(0.6)
    except Exception:
        pass


def search_brand(driver, wait, brand):
    """Type the brand name into #simpleSearchText and click Search —
    mirrors the original crawler's search_tyres() but with a brand
    word instead of a digit size code."""
    # Make sure the search panel is clean before each brand.
    click_clear(driver)

    # Resolve the search input; fall back to common alternates if the
    # id was renamed on the site.
    search_input = None
    try:
        search_input = wait.until(
            EC.presence_of_element_located((By.ID, "simpleSearchText"))
        )
    except Exception:
        print(f"  [DIAG] simpleSearchText 없음 — URL={driver.current_url}")
        for loc in [
            (By.CSS_SELECTOR, "input[placeholder*='Search' i]"),
            (By.CSS_SELECTOR, "input[name*='search' i][type='text']"),
            (By.CSS_SELECTOR, "input[type='search']"),
        ]:
            try:
                search_input = driver.find_element(*loc)
                print(f"  [DIAG] fallback locator로 입력창 찾음: {loc}")
                break
            except Exception:
                continue
    if search_input is None:
        raise RuntimeError(f"Search input not found on {driver.current_url}")

    search_input.clear()
    search_input.send_keys(brand)
    time.sleep(1)

    # Click the main red Search button.
    try:
        btn = wait.until(EC.element_to_be_clickable((
            By.XPATH,
            "//input[@value='Search'] | //button[normalize-space()='Search']"
        )))
        driver.execute_script("arguments[0].click();", btn)
        print(f"  → Search clicked for {brand}")
    except Exception:
        search_input.send_keys(Keys.RETURN)
        print(f"  → Enter pressed (fallback) for {brand}")

    time.sleep(1.5)
    # Dismiss any lingering suggestion dropdown
    try:
        driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
    except Exception:
        pass


def wait_results(driver, max_total_s=120):
    """Poll div.product-data-list every 5 s up to max_total_s.
    Returns True once rows appear, False after the full timeout.
    Prints progress every chunk so the retry is visible."""
    waited = 0
    chunk  = 5
    while waited < max_total_s:
        rows = driver.find_elements(By.CSS_SELECTOR, "div.product-data-list")
        if rows:
            # Let Angular bindings settle so ng-binding text is populated.
            time.sleep(0.5)
            print(f"  → {len(rows)} rows loaded after {waited}s")
            return True
        waited += chunk
        print(f"  … 0 rows — 다시 {chunk}s 대기 (누적 {waited}s)")
        time.sleep(chunk)
    print(f"  [INFO] {max_total_s}s 대기 후에도 0 rows — "
          f"브랜드에 재고 없음으로 간주 (URL={driver.current_url})")
    return False


# ── Click + scrape helpers (reused from the size-sweep crawler) ─────

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
    """Click all Get Cost links, then collect dicts for every row on
    the current page.  SIZE comes from DESCRIPTION via SIZE_RE."""
    click_all_get_costs(driver)
    driver.execute_script("window.scrollTo(0, 0);")
    time.sleep(0.4)
    rows = driver.find_elements(By.CSS_SELECTOR, "div.product-data-list")
    print(f"    Collecting {len(rows)} rows")
    results = []
    for row in rows:
        try:
            # Brand name — try class-based selectors first, then fall
            # back to the first non-dollar / non-size ng-binding span.
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

            # SIZE from full description (not from a search input any more).
            size = ""
            m = SIZE_RE.search(full_desc)
            if m:
                size = m.group(1).upper()

            # Strip brand + size prefix from the saved description so
            # the pattern-grouping on the dashboard side still works.
            desc = full_desc
            if brand and desc.lower().startswith(brand.lower()):
                desc = desc[len(brand):].strip()
            desc = re.sub(r'^\d{3}(?:/\d{2})?R\d{2}(?:\.\d)?C?\s*', '', desc, flags=re.IGNORECASE).strip()
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
            writer.writerow(["SIZE", "brand", "DESCRIPTION", "COST", "PRICE"])

            for brand in BRANDS:
                print(f"\n=== {brand} ===")
                try:
                    search_brand(driver, wait, brand)
                except Exception as e:
                    print(f"  [ERROR] search 설정 실패: {e} — skip")
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
                            r["cost"], r["price"],
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
