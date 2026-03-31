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
SEARCH_QUERIES = ["1756514"]

current_month = datetime.now().strftime('%b')
current_year  = datetime.now().strftime('%Y')
OUTPUT_FILE   = f"Tempe_{current_month}_{current_year}.csv"


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

    print(f"Search page: {driver.current_url}")

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

    # Wait for results — detect by "Get Cost" links appearing
    wait.until(EC.presence_of_element_located(
        (By.XPATH, "//*[contains(text(),'Get Cost') or contains(text(),'get-cost')]")
    ))
    time.sleep(1)
    print("Results loaded.")


def get_cost(driver, wait, row):
    """Click 'Get Cost' if present, then return the cost value."""
    try:
        link = row.find_element(By.XPATH, ".//*[contains(text(),'Get Cost')]")
        driver.execute_script("arguments[0].scrollIntoView(true);", link)
        driver.execute_script("arguments[0].click();", link)
        parent = link.find_element(By.XPATH, "..")
        wait.until(lambda d: "Get Cost" not in parent.text)
        return re.sub(r"[^\d.]", "", clean(parent.text))
    except Exception:
        pass

    # Cost may already be visible (e.g. $33.00)
    try:
        cost_cell = row.find_element(By.XPATH,
            ".//td[contains(@class,'cost')] | .//td[a[contains(@class,'cost')]] | .//a[contains(@href,'GetCost')]/..")
        val = re.sub(r"[^\d.]", "", clean(cost_cell.text))
        if val:
            return val
    except Exception:
        pass

    # Fallback: look for any cell that starts with $
    cells = row.find_elements(By.TAG_NAME, "td")
    dollar_cells = [c for c in cells if clean(c.text).startswith("$")]
    if len(dollar_cells) >= 2:
        return re.sub(r"[^\d.]", "", clean(dollar_cells[0].text))
    return ""


def scrape_rows(driver, wait):
    # Try multiple row selectors for Angular/table structures
    rows = driver.find_elements(By.CSS_SELECTOR,
        "table tbody tr, tr[ng-repeat], tr[data-ng-repeat]")
    if not rows:
        rows = driver.find_elements(By.XPATH,
            "//tr[.//a[contains(text(),'Get Cost')] or .//td[contains(text(),'Get Cost')]]"
            " | //tr[.//td[contains(@class,'cost')]]")
    print(f"  Found {len(rows)} rows")
    results = []

    for row in rows:
        cells = row.find_elements(By.TAG_NAME, "td")
        if len(cells) < 3:
            continue
        try:
            # SIZE — first cell (e.g. "175/65R14 82T\nSKU: 15740RSC")
            size = clean(cells[0].text).splitlines()[0]

            # DESCRIPTION — second cell (first line only)
            description = clean(cells[1].text).splitlines()[0] if len(cells) > 1 else ""

            # COST — click "Get Cost" or read existing value
            cost = get_cost(driver, wait, row)

            # PRICE — find cell starting with $, skip COST cell
            price = ""
            dollar_cells = [c for c in cells if clean(c.text).startswith("$")]
            if len(dollar_cells) >= 2:
                price = re.sub(r"[^\d.]", "", clean(dollar_cells[1].text))
            elif len(dollar_cells) == 1:
                price = re.sub(r"[^\d.]", "", clean(dollar_cells[0].text))

            results.append({
                "size": size,
                "description": description,
                "cost": cost,
                "price": price,
            })
            print(f"  {size} | {description} | cost={cost} | price={price}")

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
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr")))
    time.sleep(1)


def main():
    driver = init_driver()
    wait   = WebDriverWait(driver, 20)

    try:
        wait_for_manual_login(driver)

        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(["SIZE", "DESCRIPTION", "COST", "PRICE"])

            for query in SEARCH_QUERIES:
                print(f"\n=== Searching: {query} ===")
                search_tyres(driver, wait, query)

                page = 1
                while True:
                    print(f"  -- Page {page} --")
                    rows = scrape_rows(driver, wait)
                    for r in rows:
                        writer.writerow([r["size"], r["description"], r["cost"], r["price"]])
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
