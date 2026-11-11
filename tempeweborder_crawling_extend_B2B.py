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

Workflow after login:
  1. Wait for human login, then Enter to continue.
  2. Navigate to /tempe-b2b/search-tyres/ .
  3. For each query in the same merged size list the legacy crawler
     uses (base 24 sizes + every historical size ever operated on):
       • Type the digit code into the search input (e.g. 2156515).
       • Click the blue Search button.
       • For every row in the results table:
           - SKU / BRAND / SIZE / MODEL / DESCRIPTION  → cells
           - BUY PRICE (SI) → COST column
           - RETAIL PRICE (DR) → PRICE column
           - STOCK badge   → legacy STOCK column
           - Hover Stock cell for per-branch tooltip
             (Melbourne / Sydney / Brisbane) → STOCK_BRISBANE
             + STOCK_SYDNEY (and STOCK_MELBOURNE for the new site).

Usage:
  python tempeweborder_crawling_extend_B2B.py                  # full sweep
  python tempeweborder_crawling_extend_B2B.py --test-size 2156515   # one-size dry run
  python tempeweborder_crawling_extend_B2B.py --headless      # no visible window (after login done once in headed mode)

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

# Shared helpers — same module the legacy crawler imports so SIZE /
# DESCRIPTION normalisation stays identical across the two sites.
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


def _clean(text):
    return (text or "").strip()


def init_driver(headless: bool):
    options = webdriver.ChromeOptions()
    options.add_argument("--window-size=1400,950")
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
    """Open the Search Tyres page and wait for the main size input."""
    for attempt in range(1, 4):
        try:
            print(f"→ Search Tyres 페이지 이동 (시도 {attempt}/3) …")
            driver.get(SEARCH_URL)
            # The input looks like: <input placeholder="Enter sku, size, or profile e.g. Pirelli 2256018">
            wait.until(EC.presence_of_element_located(
                (By.XPATH,
                 "//input[contains(@placeholder, 'sku') or contains(@placeholder, 'size')"
                 " or contains(@placeholder, 'profile')]")))
            print(f"→ Search 페이지 로드 완료.  URL={driver.current_url}")
            return True
        except Exception as e:
            print(f"→ [WARN] Search 입력창 안 뜸 ({e!r}) — retry")
            time.sleep(2)
    print(f"→ [ERROR] Search 페이지 3회 재시도 실패")
    return False


def _find_search_input(driver):
    """Locate the main size/SKU search input on /search-tyres/."""
    for loc in [
        (By.XPATH, "//input[contains(@placeholder, 'sku')"
                   " or contains(@placeholder, 'size')"
                   " or contains(@placeholder, 'profile')]"),
        (By.CSS_SELECTOR, "input[type='search']"),
        (By.CSS_SELECTOR, "input[name*='search' i]"),
        (By.CSS_SELECTOR, "input[placeholder*='search' i]"),
    ]:
        try:
            el = driver.find_element(*loc)
            if el.is_displayed():
                return el
        except Exception:
            continue
    return None


def _find_search_button(driver):
    for loc in [
        (By.XPATH, "//button[.//text()[contains(., 'Search')]]"),
        (By.XPATH, "//button[contains(@class, 'btn') and contains(., 'Search')]"),
        (By.CSS_SELECTOR, "button[type='submit']"),
    ]:
        try:
            el = driver.find_element(*loc)
            if el.is_displayed():
                return el
        except Exception:
            continue
    return None


def submit_size(driver, wait, query):
    """Type the digit-code query into the search box and click Search."""
    inp = _find_search_input(driver)
    if inp is None:
        print(f"  [WARN] search input missing — re-nav to search page")
        nav_to_search(driver, wait)
        inp = _find_search_input(driver)
    if inp is None:
        raise RuntimeError(f"search input not found on {driver.current_url}")
    # Clear + type + submit.
    inp.click()
    inp.send_keys(Keys.CONTROL, "a")
    inp.send_keys(Keys.DELETE)
    time.sleep(0.2)
    inp.send_keys(query)
    time.sleep(0.4)
    btn = _find_search_button(driver)
    if btn is not None:
        try:
            driver.execute_script("arguments[0].click();", btn)
        except Exception:
            inp.send_keys(Keys.RETURN)
    else:
        inp.send_keys(Keys.RETURN)
    print(f"  → submitted '{query}'")
    time.sleep(1.0)


def wait_results(driver, max_total_s=20):
    """Poll for the results table + at least one data row.  Returns the
    list of <tr> row elements (empty when the search found nothing)."""
    waited = 0
    chunk = 2
    while waited < max_total_s:
        rows = driver.find_elements(By.CSS_SELECTOR, "table tr")
        # Filter: data rows typically carry a SKU cell with digits.
        # The header row has <th>, data rows have <td>.
        data_rows = [r for r in rows
                     if r.find_elements(By.TAG_NAME, "td")]
        if data_rows:
            # Let any async stock/price bindings settle.
            time.sleep(0.6)
            return data_rows
        time.sleep(chunk)
        waited += chunk
    return []


# Column names we expect in the B2B results table, mapped to the
# normalised internal field.  The order in the first screenshot is
# SKU | BRAND | SIZE | MODEL | DESCRIPTION | STOCK | RETAIL PRICE |
# BUY PRICE | QTY | ADD, but the header rendering can wrap, so we
# resolve column indices from the <thead> on the fly.
HEADER_TO_FIELD = {
    "sku":              "sku",
    "brand":            "brand",
    "size":             "size",
    "model":            "model",
    "description":      "description",
    "stock":            "stock_cell",
    "retail price":     "retail_price",
    "retail":           "retail_price",
    "buy price":        "buy_price",
    "buy":              "buy_price",
    "qty":              "qty",
    "add":              "add",
}


def _resolve_columns(driver):
    """Walk the results table's <thead> and return {field: col_index}.
    Returns {} when no <thead> is found."""
    out = {}
    try:
        thead = driver.find_element(By.CSS_SELECTOR, "table thead")
        ths = thead.find_elements(By.TAG_NAME, "th")
        for i, th in enumerate(ths):
            raw = _clean(th.text).lower()
            # Collapse whitespace so "RETAIL\nPRICE" also matches.
            key = " ".join(raw.split())
            field = HEADER_TO_FIELD.get(key)
            if field and field not in out:
                out[field] = i
    except Exception as _e:
        pass
    return out


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
    """The Stock badge reads like "2 More Available", "8+ More Available",
    "1 At Other Locations", "8+ At Other Locations".  Return the leading
    quantity ("2", "8+", "1", "8+") — same spelling the legacy STOCK
    column uses."""
    if not txt:
        return ""
    m = re.match(r"\s*(\d+\+?|\d+)", txt)
    return m.group(1) if m else ""


def _extract_tooltip_branches(driver, row, stock_col_idx):
    """Hover the Stock cell and parse the tooltip for per-branch qty.
    Returns (bri, syd, mel).  All strings; empty means "unknown"."""
    if stock_col_idx is None or stock_col_idx < 0:
        return "", "", ""
    try:
        tds = row.find_elements(By.TAG_NAME, "td")
        if stock_col_idx >= len(tds):
            return "", "", ""
        cell = tds[stock_col_idx]
        # Scroll into view so hover actually reaches the badge.
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", cell)
        time.sleep(0.15)
        ActionChains(driver).move_to_element(cell).perform()
        time.sleep(0.6)   # let tooltip render
        # The tooltip usually appears as a floating tippy / bootstrap
        # popover.  Try a handful of common selectors.
        tip_text = ""
        for sel in [
            ".tooltip:not([style*='display: none']) .tooltip-inner",
            ".tippy-box .tippy-content",
            "[role='tooltip']",
            ".popover.show .popover-body",
            ".popover-body",
        ]:
            try:
                tips = driver.find_elements(By.CSS_SELECTOR, sel)
                for t in tips:
                    if t.is_displayed():
                        tip_text = _clean(t.text)
                        if tip_text:
                            break
                if tip_text:
                    break
            except Exception:
                continue
        # Last-ditch: the Stock cell's own title / aria-label often
        # carries the branch breakdown too (plain-HTML tooltips).
        if not tip_text:
            for attr in ("title", "aria-label", "data-original-title"):
                v = cell.get_attribute(attr) or ""
                if v and any(k in v.lower() for k in ("sydney", "melbourne", "brisbane")):
                    tip_text = v
                    break
        if not tip_text:
            return "", "", ""
        # Parse "Sydney: 2  Melbourne: 0  Brisbane: 8+" and variants.
        def _grab(name):
            m = re.search(rf"{name}\s*[:\-]?\s*(\d+\+?)", tip_text, re.IGNORECASE)
            return m.group(1) if m else ""
        return _grab("Brisbane"), _grab("Sydney"), _grab("Melbourne")
    except Exception as _e:
        return "", "", ""


def scrape_rows(driver, dump_first=False):
    """Collect dicts for every row in the current results table."""
    cols = _resolve_columns(driver)
    if dump_first:
        print(f"    [DIAG] resolved columns: {cols}")
    rows = driver.find_elements(By.CSS_SELECTOR, "table tbody tr")
    if not rows:
        # Fallback — some tables omit tbody.
        rows = [r for r in driver.find_elements(By.CSS_SELECTOR, "table tr")
                if r.find_elements(By.TAG_NAME, "td")]
    results = []
    for ri, row in enumerate(rows):
        try:
            sku   = _cell_text(row, cols.get("sku"))
            brand = _cell_text(row, cols.get("brand"))
            size  = _cell_text(row, cols.get("size"))
            model = _cell_text(row, cols.get("model"))
            desc  = _cell_text(row, cols.get("description")) or model
            stock_txt = _cell_text(row, cols.get("stock_cell"))
            retail = _cell_text(row, cols.get("retail_price"))
            buy    = _cell_text(row, cols.get("buy_price"))
            # Diagnostic dump for the first row of the first size so
            # we can see the raw markup if any extraction is off.
            if dump_first and ri == 0:
                print(f"    [DIAG] row 0 raw: sku={sku!r} brand={brand!r} "
                      f"size={size!r} model={model!r} desc={desc!r} "
                      f"stock={stock_txt!r} retail={retail!r} buy={buy!r}")
            # Normalise SIZE via the shared regex (same behaviour as
            # the legacy crawler so downstream dedupe keys stay stable).
            size_norm = extract_size_from_desc(size) or extract_size_from_desc(desc) or size
            # Build a DESCRIPTION string that mirrors the legacy
            # format: "<brand> <model> <description>" collapsed +
            # cleaned.  Drop empty parts.
            parts = []
            for p in (brand, model, desc):
                p = _clean(p)
                if p and p.lower() not in (x.lower() for x in parts):
                    parts.append(p)
            full_desc = " ".join(parts)
            full_desc = clean_description(full_desc)
            # Strip leading brand if the shared cleaner missed it.
            if brand and full_desc.lower().startswith(brand.lower()):
                full_desc = full_desc[len(brand):].strip()
            if not full_desc:
                full_desc = desc or model
            # Price strings — strip "$" / commas, keep digits + dot.
            def _money(s):
                s = _clean(s).replace("$", "").replace(",", "")
                m = re.search(r"\d+(?:\.\d+)?", s)
                return m.group(0) if m else ""
            cost  = _money(buy)      # BUY PRICE = SI
            price = _money(retail)   # RETAIL PRICE = DR
            # Stock badge + branch split via hover tooltip.
            stock = _parse_stock_badge(stock_txt)
            bri, syd, mel = _extract_tooltip_branches(
                driver, row, cols.get("stock_cell"))
            results.append({
                "sku":            sku,
                "size":           size_norm,
                "brand":          brand,
                "description":    full_desc,
                "cost":           cost,
                "price":          price,
                "stock":          stock,
                "stock_bri":      bri,
                "stock_syd":      syd,
                "stock_mel":      mel,
            })
            _bri = bri or "-"; _syd = syd or "-"; _mel = mel or "-"
            print(f"  {size_norm:<12} | {brand:<12} | "
                  f"{full_desc[:48]:<48} | cost={cost or '-':>7} "
                  f"price={price or '-':>7} | "
                  f"stock={stock or '-'} (B:{_bri}/S:{_syd}/M:{_mel})")
        except Exception as e:
            print(f"    [WARN] row {ri}: {e!r}")
    return results


def _merged_size_queries():
    """Reuse the legacy crawler's merged (base-24 + historical) list
    so the two crawlers scan an identical universe of sizes."""
    try:
        from tempeweborder_crawling import _merged_search_queries
        return _merged_search_queries()
    except Exception as _e:
        print(f"[merge-sizes] fallback — legacy loader failed: {_e!r}")
        # Minimal fallback: just the 24 base queries.
        from tempeweborder_crawling import SEARCH_QUERIES
        return list(SEARCH_QUERIES)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--test-size", default="",
                        help="Run one size only (digit code, e.g. 2156515) and exit.")
    parser.add_argument("--headless", action="store_true",
                        help="Run Chrome headless — only use after a cookie has been saved.")
    args = parser.parse_args()

    # Build the sweep list.
    if args.test_size:
        queries = [(args.test_size, args.test_size)]
        print(f"[sweep] TEST mode — one size: {args.test_size}")
    else:
        queries = _merged_size_queries()
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
            # Same header order as the legacy crawler + optional
            # STOCK_MELBOURNE tacked onto the end (B2B portal exposes
            # Melbourne qty on the tooltip, which the legacy site
            # didn't).  Downstream readers ignore unknown trailing
            # columns so this stays back-compat.
            w.writerow([
                "SIZE", "brand", "DESCRIPTION", "COST", "PRICE",
                "STOCK", "STOCK_BRISBANE", "STOCK_SYDNEY", "STOCK_MELBOURNE",
            ])
            seen: set[tuple[str, str, str]] = set()
            dup = 0
            for i, (digits, label) in enumerate(queries, 1):
                print(f"\n=== [{i}/{len(queries)}] SIZE {label} (query={digits}) ===")
                try:
                    submit_size(driver, wait, digits)
                except Exception as e:
                    print(f"  [ERROR] submit failed: {e!r}")
                    continue
                data_rows = wait_results(driver)
                if not data_rows:
                    print(f"  [INFO] no rows for {label}")
                    continue
                dump_first = (i == 1)
                rows = scrape_rows(driver, dump_first=dump_first)
                for r in rows:
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
