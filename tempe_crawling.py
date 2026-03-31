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

    # Close the dropdown by pressing Escape
    driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
    time.sleep(1)

    # Wait for results — detect by "Get Cost" text in any element
    wait.until(EC.presence_of_element_located(
        (By.XPATH, "//*[contains(text(),'Get Cost')]")
    ))
    time.sleep(1)
    print("Results loaded.")


def click_all_get_costs(driver, wait):
    """Click every 'Get Cost' element one by one, always re-finding fresh elements."""
    clicked = 0
    max_iter = 500
    for _ in range(max_iter):
        els = driver.find_elements(By.XPATH, "//*[contains(text(),'Get Cost')]")
        if not els:
            break
        el = els[0]
        prev_count = len(els)
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            time.sleep(0.2)
            driver.execute_script("arguments[0].click();", el)
            # Wait until this element disappears (replaced by cost or "Call For Availability")
            wait.until(lambda d: len(d.find_elements(
                By.XPATH, "//*[contains(text(),'Get Cost')]")) < prev_count)
        except Exception:
            # If click failed or timed out, skip by scrolling past
            try:
                driver.execute_script("arguments[0].style.display='none';", el)
            except Exception:
                break
        time.sleep(0.2)
        clicked += 1
    print(f"  Clicked {clicked} 'Get Cost' elements")


def scrape_rows(driver, wait):
    """Click all Get Cost links first, then collect all row data."""
    click_all_get_costs(driver, wait)

    # Collect all product rows — rows that have a $ price cell
    rows = driver.find_elements(By.XPATH,
        "//tr[.//td[starts-with(normalize-space(.),'$')]]")
    print(f"  Collecting {len(rows)} rows")

    results = []
    for row in rows:
        try:
            cells = row.find_elements(By.TAG_NAME, "td")
            if len(cells) < 2:
                continue

            size = clean(cells[0].text).splitlines()[0]
            description = clean(cells[1].text).splitlines()[0] if len(cells) > 1 else ""

            # Find $ cells: first = cost, second = price
            dollar_cells = [c for c in cells if re.match(r'^\$\d', clean(c.text))]
            cost  = re.sub(r"[^\d.]", "", clean(dollar_cells[0].text)) if len(dollar_cells) >= 1 else ""
            price = re.sub(r"[^\d.]", "", clean(dollar_cells[1].text)) if len(dollar_cells) >= 2 else ""

            if not size:
                continue

            results.append({"size": size, "description": description, "cost": cost, "price": price})
            print(f"  {size} | {description[:45]} | cost={cost} | price={price}")

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
