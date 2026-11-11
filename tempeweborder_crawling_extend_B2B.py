"""
tempeweborder_crawling_extend_B2B.py
──────────────────────────────────────────────────────────────────────
Nov-11 user ask.  The legacy crawler
(tempeweborder_crawling_extend.py) hits
https://orders.tempetyreswholesale.com.au/weborder which stopped
accepting the automation's login.  Tempe publishes the SAME catalogue
through a parallel B2B portal at
https://www.tempetyreswholesale.com.au/tempe-b2b/ that logs in fine.

This script mirrors tempeweborder_crawling_extend's SIZE-sweep
workflow against the B2B portal and writes the result in the SAME
CSV layout so downstream scripts (tempe_price_lookup,
price_compare.py, build_tw_lookup, etc.) read it unchanged.

Usage:
  python tempeweborder_crawling_extend_B2B.py                       # full sweep
  python tempeweborder_crawling_extend_B2B.py --test-size 2156515   # one-size dry run
  python tempeweborder_crawling_extend_B2B.py --test-size 2156515 --debug
  python tempeweborder_crawling_extend_B2B.py --headless

Output:   Tempeweborder_extend_<YYYYMMDD>_<HHMM>.csv
"""
import argparse
import csv
import os
import re
import sys
import time
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    StaleElementReferenceException,
)

from tempeweborder_shared import (
    TYRE_SIZE_RX as SIZE_RE,
    extract_size_from_desc,
    clean_description,
    make_sku_key,
)

B2B_BASE   = "https://www.tempetyreswholesale.com.au/tempe-b2b"
LOGIN_URL  = f"{B2B_BASE}/"
SEARCH_URL = f"{B2B_BASE}/search-tyres/"

OUTPUT_FILE = datetime.now().strftime("Tempeweborder_extend_%Y%m%d_%H%M.csv")

# The results table on /search-tyres/ carries these column headers.
# We resolve indices on the fly by matching <th> text against these
# keys so a re-ordering never breaks extraction.
HEADER_TO_FIELD = {
    "sku":          "sku",
    "brand":        "brand",
    "size":         "size",
    "model":        "model",
    "description":  "description",
    "stock":        "stock_cell",
    "retail price": "retail_price",
    "retail":       "retail_price",
    "buy price":    "buy_price",
    "buy":          "buy_price",
    "qty":          "qty",
    "add":          "add",
}


def _clean(text):
    return (text or "").strip()


def init_driver(headless: bool):
    options = webdriver.ChromeOptions()
    options.add_argument("--window-size=1500,950")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    if headless:
        options.add_argument("--headless=new")
    return webdriver.Chrome(options=options)


def wait_for_manual_login(driver):
    driver.get(LOGIN_URL)
    print("\n" + "=" * 60)
    print(f"Chrome 브라우저가 열렸습니다: {LOGIN_URL}")
    print("1. Login 폼에 Username / Password 입력 후 Sign In")
    print("2. 상단에 'Hello, <YOUR DEALER>' 가 뜨면 완료")
    print("3. 여기서 Enter를 눌러 계속")
    print("=" * 60)
    input(">>> Enter: ")


def nav_to_search(driver, wait):
    for attempt in range(1, 4):
        try:
            print(f"→ Search Tyres 페이지 이동 (시도 {attempt}/3) …")
            driver.get(SEARCH_URL)
            wait.until(EC.presence_of_element_located(
                (By.XPATH,
                 "//input[contains(@placeholder, 'sku') or contains(@placeholder, 'size')"
                 " or contains(@placeholder, 'profile')]")))
            print(f"→ Search 페이지 로드 완료.  URL={driver.current_url}")
            return True
        except Exception as e:
            print(f"→ [WARN] Search 입력창 안 뜸 ({e!r})")
            time.sleep(2)
    return False


def _find_search_input(driver):
    for loc in [
        (By.XPATH, "//input[contains(@placeholder, 'sku')"
                   " or contains(@placeholder, 'size')"
                   " or contains(@placeholder, 'profile')]"),
        (By.CSS_SELECTOR, "input[type='search']"),
        (By.CSS_SELECTOR, "input[name*='search' i][type='text']"),
    ]:
        try:
            for el in driver.find_elements(*loc):
                if el.is_displayed():
                    return el
        except Exception:
            continue
    return None


def _find_search_button(driver):
    """Return a visible 'Search' button near the search input."""
    for loc in [
        (By.XPATH, "//button[contains(@class,'btn') and normalize-space(.)='Search']"),
        (By.XPATH, "//button[.//i[contains(@class,'fa-search')]]"),
        (By.XPATH, "//button[.//text()[contains(., 'Search')]][not(ancestor::nav)]"),
        (By.CSS_SELECTOR, "button[type='submit']"),
    ]:
        try:
            for el in driver.find_elements(*loc):
                if el.is_displayed():
                    return el
        except Exception:
            continue
    return None


def _dismiss_overlays(driver):
    """Best-effort kill of cookie / consent / notification banners that
    sit in front of the Search button and block its click."""
    # Common selectors for cookie banners / toasts / promo bubbles.
    for sel in [
        "button[aria-label*='close' i]",
        "button[aria-label*='dismiss' i]",
        "button.cookie-close",
        ".modal-backdrop",
        "div[class*='cookie'] button",
        "div[class*='consent'] button",
        "div[class*='notification'] button[class*='close']",
    ]:
        try:
            for el in driver.find_elements(By.CSS_SELECTOR, sel):
                if el.is_displayed():
                    try:
                        driver.execute_script("arguments[0].click();", el)
                    except Exception:
                        pass
        except Exception:
            pass
    # Press Escape in case a lightbox is up.
    try:
        driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
    except Exception:
        pass


def submit_size(driver, wait, query):
    """Type the query into the search box and click Search.  Robust
    against click-intercept by falling back to JS click + Enter key."""
    _dismiss_overlays(driver)
    inp = _find_search_input(driver)
    if inp is None:
        print("  [warn] search input missing — re-nav")
        nav_to_search(driver, wait)
        inp = _find_search_input(driver)
    if inp is None:
        raise RuntimeError("search input not found")
    # Scroll input into view, then clear + type.
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", inp)
    time.sleep(0.2)
    try:
        inp.click()
    except Exception:
        driver.execute_script("arguments[0].focus();", inp)
    try:
        inp.send_keys(Keys.CONTROL, "a")
        inp.send_keys(Keys.DELETE)
    except Exception:
        driver.execute_script("arguments[0].value = '';", inp)
    time.sleep(0.15)
    inp.send_keys(query)
    time.sleep(0.3)
    # Try the Search button first — JS click directly on the element,
    # which bypasses any overlay interception.
    submitted = False
    btn = _find_search_button(driver)
    if btn is not None:
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            time.sleep(0.15)
            driver.execute_script("arguments[0].click();", btn)
            submitted = True
        except Exception as e:
            print(f"  [warn] JS click failed: {e!r}")
    if not submitted:
        # Fallback: press Enter in the input.
        try:
            inp.send_keys(Keys.RETURN)
            submitted = True
        except Exception:
            pass
    print(f"  → submitted '{query}' ({'JS click' if btn else 'Enter'})")
    time.sleep(1.0)


def _pick_results_table(driver):
    """Walk every <table> on the page and return the one whose <thead>
    contains BOTH 'SKU' AND 'STOCK' cells (or the three-of-four
    'SKU/BRAND/SIZE/DESCRIPTION').  Returns None when no such table
    exists on the current page (e.g. no results yet)."""
    needed = {"sku", "stock"}
    nice_to_have = {"sku", "brand", "size", "description"}
    tables = driver.find_elements(By.TAG_NAME, "table")
    best = None
    best_score = 0
    for t in tables:
        try:
            ths = t.find_elements(By.CSS_SELECTOR, "thead th")
            if not ths:
                # Some tables render headers in the first <tr> of <tbody>.
                ths = t.find_elements(By.CSS_SELECTOR, "tr:first-child th")
            headers = {" ".join((_clean(th.text) or "").lower().split())
                       for th in ths}
            if not headers:
                continue
            if not needed.issubset(headers):
                continue
            score = len(nice_to_have.intersection(headers))
            if score > best_score:
                best_score = score
                best = t
        except Exception:
            continue
    return best


def _resolve_columns_for(table):
    out = {}
    try:
        ths = table.find_elements(By.CSS_SELECTOR, "thead th")
        if not ths:
            ths = table.find_elements(By.CSS_SELECTOR, "tr:first-child th")
        for i, th in enumerate(ths):
            key = " ".join((_clean(th.text) or "").lower().split())
            field = HEADER_TO_FIELD.get(key)
            if field and field not in out:
                out[field] = i
    except Exception:
        pass
    return out


def wait_for_results_table(driver, max_total_s=25):
    """Poll until a table matching the results-table signature appears
    AND has at least one data row.  Returns (table_element, rows) or
    (None, []) on timeout."""
    waited = 0
    chunk = 2
    while waited < max_total_s:
        t = _pick_results_table(driver)
        if t is not None:
            try:
                rows = t.find_elements(By.CSS_SELECTOR, "tbody tr")
                if not rows:
                    rows = [r for r in t.find_elements(By.CSS_SELECTOR, "tr")
                            if r.find_elements(By.TAG_NAME, "td")]
                if rows:
                    time.sleep(0.5)
                    return t, rows
            except StaleElementReferenceException:
                pass
        time.sleep(chunk)
        waited += chunk
    return None, []


def _cell_text(row, idx):
    if idx is None or idx < 0:
        return ""
    try:
        tds = row.find_elements(By.TAG_NAME, "td")
        if 0 <= idx < len(tds):
            return _clean(tds[idx].text)
    except Exception:
        pass
    return ""


def _parse_stock_badge(txt):
    if not txt:
        return ""
    m = re.match(r"\s*(\d+\+?|\d+)", txt)
    return m.group(1) if m else ""


def _extract_tooltip_branches(driver, row, stock_col_idx):
    """Hover the Stock cell, parse the tooltip for per-branch qty.
    Returns (bri, syd, mel)."""
    if stock_col_idx is None or stock_col_idx < 0:
        return "", "", ""
    try:
        tds = row.find_elements(By.TAG_NAME, "td")
        if stock_col_idx >= len(tds):
            return "", "", ""
        cell = tds[stock_col_idx]
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", cell)
        time.sleep(0.15)
        ActionChains(driver).move_to_element(cell).perform()
        time.sleep(0.7)
        tip_text = ""
        for sel in [
            ".tooltip:not([style*='display: none']) .tooltip-inner",
            ".tippy-box .tippy-content",
            "[role='tooltip']",
            ".popover.show .popover-body",
            ".popover-body",
        ]:
            try:
                for t in driver.find_elements(By.CSS_SELECTOR, sel):
                    if t.is_displayed():
                        tip_text = _clean(t.text)
                        if tip_text:
                            break
                if tip_text:
                    break
            except Exception:
                continue
        if not tip_text:
            for attr in ("title", "aria-label", "data-original-title"):
                v = cell.get_attribute(attr) or ""
                if v and any(k in v.lower() for k in ("sydney", "melbourne", "brisbane")):
                    tip_text = v
                    break
        if not tip_text:
            return "", "", ""
        def _grab(name):
            m = re.search(rf"{name}\s*[:\-]?\s*(\d+\+?)", tip_text, re.IGNORECASE)
            return m.group(1) if m else ""
        return _grab("Brisbane"), _grab("Sydney"), _grab("Melbourne")
    except Exception:
        return "", "", ""


def scrape_rows(driver, table, rows, debug=False):
    cols = _resolve_columns_for(table)
    if debug:
        print(f"    [DIAG] resolved columns: {cols}")
        if rows:
            try:
                print(f"    [DIAG] row[0] outerHTML (first 500 chars): "
                      f"{rows[0].get_attribute('outerHTML')[:500]}")
            except Exception:
                pass
    results = []
    # Re-query rows once more RIGHT before iteration — the earlier
    # list may be stale after `_resolve_columns_for` churn.
    try:
        rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
        if not rows:
            rows = [r for r in table.find_elements(By.CSS_SELECTOR, "tr")
                    if r.find_elements(By.TAG_NAME, "td")]
    except StaleElementReferenceException:
        return []
    for ri, row in enumerate(rows):
        try:
            sku    = _cell_text(row, cols.get("sku"))
            brand  = _cell_text(row, cols.get("brand"))
            size   = _cell_text(row, cols.get("size"))
            model  = _cell_text(row, cols.get("model"))
            desc   = _cell_text(row, cols.get("description")) or model
            stock_txt = _cell_text(row, cols.get("stock_cell"))
            retail = _cell_text(row, cols.get("retail_price"))
            buy    = _cell_text(row, cols.get("buy_price"))
            if debug and ri == 0:
                print(f"    [DIAG] row 0: sku={sku!r} brand={brand!r} "
                      f"size={size!r} model={model!r} desc={desc!r} "
                      f"stock={stock_txt!r} retail={retail!r} buy={buy!r}")
            # Skip empty rows (e.g. a "no results" placeholder <tr>).
            if not sku and not brand:
                continue
            size_norm = extract_size_from_desc(size) or extract_size_from_desc(desc) or size
            parts = []
            for p in (brand, model, desc):
                p = _clean(p)
                if p and p.lower() not in (x.lower() for x in parts):
                    parts.append(p)
            full_desc = " ".join(parts)
            full_desc = clean_description(full_desc)
            if brand and full_desc.lower().startswith(brand.lower()):
                full_desc = full_desc[len(brand):].strip()
            if not full_desc:
                full_desc = desc or model
            def _money(s):
                s = _clean(s).replace("$", "").replace(",", "")
                m = re.search(r"\d+(?:\.\d+)?", s)
                return m.group(0) if m else ""
            cost  = _money(buy)
            price = _money(retail)
            stock = _parse_stock_badge(stock_txt)
            bri, syd, mel = _extract_tooltip_branches(
                driver, row, cols.get("stock_cell"))
            results.append({
                "sku":         sku,
                "size":        size_norm,
                "brand":       brand,
                "description": full_desc,
                "cost":        cost,
                "price":       price,
                "stock":       stock,
                "stock_bri":   bri,
                "stock_syd":   syd,
                "stock_mel":   mel,
            })
            _bri = bri or "-"; _syd = syd or "-"; _mel = mel or "-"
            print(f"  {size_norm:<12} | {brand:<12} | "
                  f"{full_desc[:48]:<48} | cost={cost or '-':>7} "
                  f"price={price or '-':>7} | "
                  f"stock={stock or '-'} (B:{_bri}/S:{_syd}/M:{_mel})")
        except StaleElementReferenceException:
            print(f"    [WARN] row {ri} went stale — skipping remainder")
            break
        except Exception as e:
            print(f"    [WARN] row {ri}: {e!r}")
    return results


def _merged_size_queries():
    try:
        from tempeweborder_crawling import _merged_search_queries
        return _merged_search_queries()
    except Exception as e:
        print(f"[merge-sizes] legacy loader failed: {e!r}")
        from tempeweborder_crawling import SEARCH_QUERIES
        return list(SEARCH_QUERIES)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test-size", default="",
                        help="Run only these digit-code queries and exit.  "
                             "Comma-separate for several, e.g. 2156515,2254517.")
    parser.add_argument("--limit", type=int, default=0,
                        help="Stop after this many sizes from the full merged list "
                             "(ignored when --test-size is set).")
    parser.add_argument("--headless", action="store_true",
                        help="Run Chrome headless.")
    parser.add_argument("--debug", action="store_true",
                        help="Dump column indices + first-row HTML on each size.")
    args = parser.parse_args()

    if args.test_size:
        # Comma-separated → list of (digits, label) with label = digits.
        _codes = [c.strip() for c in args.test_size.split(",") if c.strip()]
        queries = [(c, c) for c in _codes]
        print(f"[sweep] TEST mode — {len(queries)} size(s): {_codes}")
    else:
        queries = _merged_size_queries()
        if args.limit > 0:
            queries = queries[:args.limit]
            print(f"[sweep] LIMITED mode — first {len(queries)} of merged list")
        else:
            print(f"[sweep] full mode — {len(queries)} sizes")

    driver = init_driver(headless=args.headless)
    wait = WebDriverWait(driver, 25)
    try:
        wait_for_manual_login(driver)
        if not nav_to_search(driver, wait):
            print("[fatal] search page unreachable — aborting")
            return

        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.writer(f)
            w.writerow([
                "SIZE", "brand", "DESCRIPTION", "COST", "PRICE",
                "STOCK", "STOCK_BRISBANE", "STOCK_SYDNEY", "STOCK_MELBOURNE",
            ])
            seen: set[tuple[str, str, str]] = set()
            dup = 0
            for i, (digits, label) in enumerate(queries, 1):
                print(f"\n=== [{i}/{len(queries)}] SIZE {label} (query={digits}) ===")
                # Nav back to the search page BETWEEN each size — the
                # B2B portal keeps the last result up, and the Search
                # button can get occluded by the previous page's chrome
                # (what ElementClickInterceptedException was about).
                # Cheap round-trip ensures a clean search input each time.
                if i == 1:
                    pass  # already on the page from nav_to_search above
                else:
                    try:
                        driver.get(SEARCH_URL)
                        wait.until(EC.presence_of_element_located(
                            (By.XPATH, "//input[contains(@placeholder, 'sku')"
                                       " or contains(@placeholder, 'size')"
                                       " or contains(@placeholder, 'profile')]")))
                    except Exception as e:
                        print(f"  [warn] re-nav to search failed: {e!r}")
                try:
                    submit_size(driver, wait, digits)
                except ElementClickInterceptedException:
                    print("  [retry] click intercepted — scrolling top + dismissing overlays, retry once")
                    driver.execute_script("window.scrollTo(0, 0);")
                    _dismiss_overlays(driver)
                    time.sleep(0.5)
                    try:
                        submit_size(driver, wait, digits)
                    except Exception as e2:
                        print(f"  [ERROR] retry failed: {e2!r}")
                        continue
                except Exception as e:
                    print(f"  [ERROR] submit failed: {e!r}")
                    continue
                table, rows = wait_for_results_table(driver)
                if not table or not rows:
                    print(f"  [INFO] no results table for {label}")
                    continue
                dump_first = args.debug or (i == 1)
                scraped = scrape_rows(driver, table, rows, debug=dump_first)
                for r in scraped:
                    key = make_sku_key(r["size"], r["brand"], r["description"])
                    if key in seen:
                        dup += 1
                        continue
                    seen.add(key)
                    w.writerow([
                        r["size"], r["brand"], r["description"],
                        r["cost"], r["price"],
                        r.get("stock", ""),
                        r.get("stock_bri", ""),
                        r.get("stock_syd", ""),
                        r.get("stock_mel", ""),
                    ])
                f.flush()
        if dup:
            print(f"\n[info] in-file dedup dropped {dup} duplicate row(s)")
        print(f"\n✓ 저장 완료: {OUTPUT_FILE}")
    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    main()
