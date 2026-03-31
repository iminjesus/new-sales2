import re
import os
import time
from datetime import datetime
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

load_dotenv(encoding="utf-8-sig")  # utf-8-sig handles both UTF-8 and UTF-16 BOM (Windows PowerShell)

TEMPE_USERNAME = os.getenv("Tempe_username")
TEMPE_PASSWORD = os.getenv("Tempe_password")

BASE_URL = "https://orders.tempetyreswholesale.com.au/weborder"
SEARCH_URL = "https://orders.tempetyreswholesale.com.au/WebOrder/Product/Search"

# Tyre size queries to search (format: WWWAALLRR -> e.g. 1756514 = 175/65R14)
SEARCH_QUERIES = ["1756514"]

current_month = datetime.now().strftime('%b')
current_year = datetime.now().strftime('%Y')
OUTPUT_FILE = f"Tempe_{current_month}_{current_year}.txt"


def clean_text(text):
    return text.strip() if text else ""


def extract_brand(description):
    """Extract the brand (first word) from the product description."""
    parts = description.strip().split()
    return parts[0] if parts else ""


def init_driver(headless=False):
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    # Avoid bot detection
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    return webdriver.Chrome(options=options)


def login(driver, wait):
    # Navigate to root — site will redirect to its own login page
    driver.get(BASE_URL)
    time.sleep(2)
    print(f"Login page loaded: {driver.current_url}")

    # Try multiple possible selectors for the username field
    username_selectors = [
        (By.ID, "UserName"),
        (By.NAME, "UserName"),
        (By.NAME, "username"),
        (By.CSS_SELECTOR, "input[type='email']"),
        (By.CSS_SELECTOR, "input[name*='ser']"),
        (By.CSS_SELECTOR, "input[id*='ser']"),
    ]
    username_field = None
    for by, selector in username_selectors:
        try:
            username_field = wait.until(EC.presence_of_element_located((by, selector)))
            print(f"Found username field with: {by}={selector}")
            break
        except Exception:
            continue

    if not username_field:
        driver.save_screenshot("login_debug.png")
        print("Saved login_debug.png — check the screenshot to see what the login page looks like.")
        raise RuntimeError("Could not find username field. Check login_debug.png.")

    password_selectors = [
        (By.ID, "Password"),
        (By.NAME, "Password"),
        (By.NAME, "password"),
        (By.CSS_SELECTOR, "input[type='password']"),
    ]
    password_field = None
    for by, selector in password_selectors:
        try:
            password_field = driver.find_element(by, selector)
            print(f"Found password field with: {by}={selector}")
            break
        except Exception:
            continue

    if not password_field:
        driver.save_screenshot("login_debug.png")
        raise RuntimeError("Could not find password field. Check login_debug.png.")

    username_field.clear()
    username_field.send_keys(TEMPE_USERNAME)
    password_field.clear()
    password_field.send_keys(TEMPE_PASSWORD)

    # Click the Login button
    try:
        login_btn = driver.find_element(By.CSS_SELECTOR, "input[value='Login'], button[type='submit'], input[type='submit']")
        login_btn.click()
    except Exception:
        password_field.send_keys(Keys.RETURN)

    try:
        wait.until(EC.url_contains("/WebOrder/Product"))
    except Exception:
        pass

    print(f"After login URL: {driver.current_url}")
    if "login" in driver.current_url.lower() or "account" in driver.current_url.lower():
        driver.save_screenshot("login_failed.png")
        raise RuntimeError("Login failed — still on login page. Check login_failed.png.")

    print("Login successful.")


def search_tyres(driver, wait, query):
    driver.get(SEARCH_URL)

    # Wait for the Select2 container to be present on the page
    search_container = wait.until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, ".select2-selection, .select2-container"))
    )
    search_container.click()
    time.sleep(0.8)

    # After clicking, the search input appears inside the dropdown
    search_input = wait.until(
        EC.presence_of_element_located((By.CSS_SELECTOR, ".select2-search__field"))
    )
    search_input.send_keys(query)
    time.sleep(1.5)  # wait for autocomplete options to appear

    # Click the first matching result from the dropdown list
    try:
        first_option = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, ".select2-results__option:not(.select2-results__option--group)"))
        )
        first_option.click()
        print(f"Selected option: {first_option.text}")
    except Exception:
        # No dropdown option — just close and proceed with Search button
        search_input.send_keys(Keys.ESCAPE)

    time.sleep(0.5)

    # Click the red Search button
    search_btn = wait.until(
        EC.element_to_be_clickable((By.XPATH, "//input[@value='Search'] | //button[normalize-space()='Search']"))
    )
    search_btn.click()

    # Wait for results table to appear
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr")))
    time.sleep(1)
    print(f"Search results loaded for: {query}")


def get_cost_for_row(driver, wait, row):
    """Click 'Get Cost' link in the row and return the revealed cost."""
    try:
        get_cost_link = row.find_element(By.XPATH, ".//a[contains(text(),'Get Cost')]")
        driver.execute_script("arguments[0].scrollIntoView(true);", get_cost_link)
        driver.execute_script("arguments[0].click();", get_cost_link)

        # Wait for the cost value to replace the link in the same cell
        cost_cell = get_cost_link.find_element(By.XPATH, "..")
        wait.until(lambda d: "Get Cost" not in cost_cell.text)
        cost_text = clean_text(cost_cell.text)
        return re.sub(r"[^\d.]", "", cost_text)
    except Exception:
        return ""


def scrape_rows(driver, wait):
    """Scrape all rows on the current results page."""
    rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
    results = []

    for row in rows:
        cells = row.find_elements(By.TAG_NAME, "td")
        if len(cells) < 4:
            continue

        try:
            # SIZE cell — first column, may contain size + SKU
            size_cell = clean_text(cells[0].text)
            size_lines = size_cell.splitlines()
            size = size_lines[0] if size_lines else ""
            sku = ""
            for line in size_lines[1:]:
                if "SKU" in line.upper():
                    sku = line.replace("SKU:", "").replace("SKU", "").strip()
                    break

            # DESCRIPTION cell
            description = clean_text(cells[1].text) if len(cells) > 1 else ""
            # Remove duplicate load/speed rating sub-line that sometimes appears
            description_lines = description.splitlines()
            description = description_lines[0] if description_lines else description

            brand = extract_brand(description)

            # COST cell — click "Get Cost"
            cost = get_cost_for_row(driver, wait, row)

            # PRICE cell
            price_text = ""
            for i in range(2, len(cells)):
                txt = clean_text(cells[i].text)
                if txt.startswith("$"):
                    price_text = re.sub(r"[^\d.]", "", txt)
                    break

            # ON HAND cell (green badge)
            on_hand = ""
            try:
                on_hand_elem = row.find_element(By.CSS_SELECTOR, ".btn-success, .badge-success, td.on-hand, .stock-qty")
                on_hand = clean_text(on_hand_elem.text)
            except Exception:
                # Fallback: look for numeric-ish cell near the end
                for i in range(len(cells) - 1, max(len(cells) - 4, 0), -1):
                    txt = clean_text(cells[i].text)
                    if re.match(r"^\d+\+?$", txt):
                        on_hand = txt
                        break

            results.append({
                "size": size,
                "sku": sku,
                "description": description,
                "brand": brand,
                "cost": cost,
                "price": price_text,
                "on_hand": on_hand,
            })

        except Exception as e:
            print(f"[WARN] Failed to parse row: {e}")
            continue

    return results


def has_next_page(driver):
    """Return True if a 'next page' control exists and is enabled."""
    try:
        next_btn = driver.find_element(
            By.CSS_SELECTOR,
            "a[rel='next'], li.next:not(.disabled) a, .pagination .next:not(.disabled) a"
        )
        return next_btn.is_displayed()
    except Exception:
        return False


def go_next_page(driver, wait):
    next_btn = driver.find_element(
        By.CSS_SELECTOR,
        "a[rel='next'], li.next:not(.disabled) a, .pagination .next:not(.disabled) a"
    )
    driver.execute_script("arguments[0].click();", next_btn)
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody tr")))
    time.sleep(1)


def main():
    if not TEMPE_USERNAME or not TEMPE_PASSWORD:
        raise RuntimeError(
            "Missing credentials. Set TEMPE_USERNAME and TEMPE_PASSWORD in your .env file."
        )

    driver = init_driver(headless=False)  # Set to True after confirming login works
    wait = WebDriverWait(driver, 20)

    try:
        login(driver, wait)

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

                    print(f"  -> {len(rows)} products written from page {page}")

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
