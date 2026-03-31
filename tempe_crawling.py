import re
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
current_year = datetime.now().strftime('%Y')
OUTPUT_FILE = f"Tempe_{current_month}_{current_year}.txt"


def clean_text(text):
    return text.strip() if text else ""


def extract_brand(description):
    parts = description.strip().split()
    return parts[0] if parts else ""


def init_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--window-size=1280,900")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    driver = webdriver.Chrome(options=options)
    return driver


def wait_for_manual_login(driver):
    driver.get(SEARCH_URL)
    print("\n" + "="*60)
    print("Chrome 브라우저가 열렸습니다.")
    print("1. 로그인 하세요")
    print("2. Search Tyres 탭으로 이동하세요")
    print("3. 준비되면 여기서 Enter를 누르세요")
    print("="*60)
    input(">>> Enter를 누르면 크롤링을 시작합니다: ")
    print(f"현재 URL: {driver.current_url}")


def search_tyres(driver, wait, query):
    driver.get(SEARCH_URL)
    time.sleep(2)
    print(f"Search page: {driver.current_url}")

    search_container = wait.until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, ".select2-selection"))
    )
    search_container.click()
    time.sleep(0.8)

    search_input = wait.until(
        EC.presence_of_element_located((By.CSS_SELECTOR, ".select2-search__field"))
    )
    search_input.send_keys(query)
    time.sleep(1.5)

    try:
        first_option = wait.until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, ".select2-results__option:not(.select2-results__option--group)")
            )
        )
        print(f"Selecting: {first_option.text}")
        first_option.click()
    except Exception:
        search_input.send_keys(Keys.ESCAPE)

    time.sleep(0.5)

    search_btn = wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "//input[@value='Search'] | //button[normalize-space()='Search']")
        )
    )
    search_btn.click()

    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr")))
    time.sleep(1)
    print(f"Results loaded for: {query}")


def get_cost_for_row(driver, wait, row):
    try:
        get_cost_link = row.find_element(By.XPATH, ".//a[contains(text(),'Get Cost')]")
        driver.execute_script("arguments[0].scrollIntoView(true);", get_cost_link)
        driver.execute_script("arguments[0].click();", get_cost_link)
        cost_cell = get_cost_link.find_element(By.XPATH, "..")
        wait.until(lambda d: "Get Cost" not in cost_cell.text)
        return re.sub(r"[^\d.]", "", clean_text(cost_cell.text))
    except Exception:
        return ""


def scrape_rows(driver, wait):
    rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
    results = []

    for row in rows:
        cells = row.find_elements(By.TAG_NAME, "td")
        if len(cells) < 4:
            continue
        try:
            size_lines = clean_text(cells[0].text).splitlines()
            size = size_lines[0] if size_lines else ""
            sku = ""
            for line in size_lines[1:]:
                if "SKU" in line.upper():
                    sku = line.replace("SKU:", "").replace("SKU", "").strip()
                    break

            desc_lines = clean_text(cells[1].text).splitlines() if len(cells) > 1 else [""]
            description = desc_lines[0]
            brand = extract_brand(description)

            cost = get_cost_for_row(driver, wait, row)

            price_text = ""
            for i in range(2, len(cells)):
                txt = clean_text(cells[i].text)
                if txt.startswith("$"):
                    price_text = re.sub(r"[^\d.]", "", txt)
                    break

            on_hand = ""
            try:
                on_hand_elem = row.find_element(By.CSS_SELECTOR, ".btn-success, .badge-success")
                on_hand = clean_text(on_hand_elem.text)
            except Exception:
                for i in range(len(cells) - 1, max(len(cells) - 4, 0), -1):
                    txt = clean_text(cells[i].text)
                    if re.match(r"^\d+\+?$", txt):
                        on_hand = txt
                        break

            results.append({
                "size": size, "sku": sku, "description": description,
                "brand": brand, "cost": cost, "price": price_text, "on_hand": on_hand,
            })
        except Exception as e:
            print(f"[WARN] Row parse error: {e}")

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
    wait = WebDriverWait(driver, 20)

    try:
        wait_for_manual_login(driver)

        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            for query in SEARCH_QUERIES:
                print(f"\n=== Searching: {query} ===")
                search_tyres(driver, wait, query)

                page = 1
                while True:
                    print(f"  Scraping page {page}...")
                    rows = scrape_rows(driver, wait)

                    for r in rows:
                        line = (
                            f"{current_month}_{current_year}|TEMPE|{r['brand']}|"
                            f"{r['description']}|{r['sku']}|{r['size']}|"
                            f"{r['cost']}|{r['price']}|{r['on_hand']}\n"
                        )
                        f.write(line)

                    print(f"  -> {len(rows)} rows written from page {page}")

                    if has_next_page(driver):
                        go_next_page(driver, wait)
                        page += 1
                    else:
                        break

        print(f"\nDone. Output saved to: {OUTPUT_FILE}")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()
