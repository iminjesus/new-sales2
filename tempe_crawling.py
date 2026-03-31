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


def scrape_rows(driver, wait):
    """Find all 'Get Cost' links, click each one, then extract row data."""
    results = []

    # Screenshot to see what's actually on screen
    driver.save_screenshot("scrape_debug.png")
    print("  Saved scrape_debug.png")

    # Print all frames/iframes present
    iframes = driver.find_elements(By.TAG_NAME, "iframe")
    print(f"  iframes found: {len(iframes)}")

    # Print ALL anchor tags on page
    all_links = driver.find_elements(By.TAG_NAME, "a")
    print(f"  Total <a> tags: {len(all_links)}")
    for lnk in all_links[:30]:
        print(f"    href={lnk.get_attribute('href')!r} text={lnk.text!r}")

    # Find every "Get Cost" link on the page
    get_cost_links = driver.find_elements(
        By.XPATH, "//a[contains(text(),'Get Cost')]"
    )
    print(f"  Found {len(get_cost_links)} 'Get Cost' links")

    for i, link in enumerate(get_cost_links):
        try:
            # Scroll into view and click "Get Cost"
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", link)
            time.sleep(0.3)
            driver.execute_script("arguments[0].click();", link)

            # Wait until the link's parent cell no longer says "Get Cost"
            cost_cell = link.find_element(By.XPATH, "..")
            wait.until(lambda d, c=cost_cell: "Get Cost" not in c.text)
            cost = re.sub(r"[^\d.]", "", clean(cost_cell.text))

            # Walk up to find the row (<tr>)
            row = link.find_element(By.XPATH, "ancestor::tr[1]")
            cells = row.find_elements(By.TAG_NAME, "td")

            # SIZE — first td, first line
            size = clean(cells[0].text).splitlines()[0] if cells else ""

            # DESCRIPTION — second td, first line
            description = clean(cells[1].text).splitlines()[0] if len(cells) > 1 else ""

            # PRICE — first td whose text starts with "$" (excluding cost cell)
            price = ""
            for cell in cells:
                txt = clean(cell.text)
                if txt.startswith("$"):
                    price = re.sub(r"[^\d.]", "", txt)
                    break

            results.append({
                "size": size,
                "description": description,
                "cost": cost,
                "price": price,
            })
            print(f"  [{i+1}] {size} | {description} | cost={cost} | price={price}")

        except Exception as e:
            print(f"  [{i+1}] [WARN] {e}")

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
