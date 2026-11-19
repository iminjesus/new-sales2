#!/usr/bin/env python3
"""
yhi_price_crawler.py -- crawl tyre prices + stock from yhi.com.au
as a specific logged-in customer.

Current scope (Nov-19 user ask):
  * Only YHI NZ (SAP #0000107051) -- a single customer
  * All 24 sizes from tempeweborder_crawling.SEARCH_QUERIES
  * Full stock breakdown per warehouse + On-Order ETA

Workflow (manual passthrough, same shape as yhi_customer_crawler.py):
  1. Open a visible Chrome window pointed at the customer-search page.
  2. Operator logs in to YHI, finds the YHI NZ row, clicks its LOGIN
     button (that logs the session INTO YHI NZ's account and opens
     the tyres catalogue for that store).
  3. Operator presses ENTER in the terminal.
  4. For each size in SEARCH_QUERIES, the script:
       - navigates to https://www.yhi.com.au/tyres
       - focuses the size search box
       - types the digits (e.g. '1956515' for 195/65R15)
       - waits for the SKU list to render
       - scrapes brand / pattern / size / load-speed / price / stock
       - clicks each row's "N Local" link to expand the per-warehouse
         breakdown and captures Sydney / Melbourne / Brisbane /
         Adelaide / Perth / Launceston + On-Order ETA rows

Output:
  YHI_prices_<store>_<YYYYMMDD_HHMM>.csv
  columns:
    store_code, store_name,
    query_digits, query_size,
    brand, pattern, size_spec, load_speed,
    price,
    stock_local, stock_sydney, stock_melbourne, stock_brisbane,
    stock_adelaide, stock_perth, stock_launceston,
    stock_on_order,
    eta_syd, eta_mel, eta_bri, eta_per,
    crawled_at

Usage:
  python yhi_price_crawler.py                          # all 24 base sizes
  python yhi_price_crawler.py --sizes 1956515,2254517  # only those
  python yhi_price_crawler.py --limit 2                # first 2 sizes only
  python yhi_price_crawler.py --debug                  # dump the first
                                                        # tyres page HTML
                                                        # for selector tuning
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import time
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains


BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
CUSTOMER_URL = "https://www.yhi.com.au/customer-search"
TYRES_URL    = "https://www.yhi.com.au/tyres"

STORE_CODE   = "107051"
STORE_NAME   = "YHI NZ"

TYRE_SEARCH_INPUT_SELECTORS = (
    "input[placeholder*='Search']",
    "input[placeholder*='size']",
    "input[placeholder*='Size']",
    ".search-bar input",
    "input[type='search']",
    "input[type='text']",
)
RESULT_ROW_SELECTORS = (
    ".component-tyre-search-result-item",
    ".component-product-item",
    ".component-product-row",
    "[class*='tyre-search-result']",
    "[class*='product-item']",
    "[class*='product-row']",
    "[class*='search-result-item']",
    # Last-resort generic patterns: a list/table row that carries a
    # price AND a "Local"/"Interstate" stock indicator is almost
    # certainly a result row.
    ".result-item",
    ".result-row",
    "tbody tr",
)
STOCK_EXPAND_PATTERNS = (
    # Link text that opens the per-warehouse breakdown.
    "a[href*='Local']",
    "a.stock-link",
    "span.stock-link",
    "[class*='stock-detail']",
    "[class*='stock-popup']",
)

CITY_KEYS = {
    "sydney":    "stock_sydney",
    "melbourne": "stock_melbourne",
    "brisbane":  "stock_brisbane",
    "adelaide":  "stock_adelaide",
    "perth":     "stock_perth",
    "launceston":"stock_launceston",
    # NZ warehouses might show up differently; capture generically later
}


def _ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M")


def _log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# ------------------------------------------------------------------
def _load_size_list():
    """Import the Tempe master size list.  SEARCH_QUERIES is a list of
    (digits, display) tuples."""
    try:
        from tempeweborder_crawling import SEARCH_QUERIES
        return SEARCH_QUERIES[:]
    except Exception as e:
        _log(f"[fatal] could not import SEARCH_QUERIES: {e}")
        sys.exit(1)


def make_driver(headless: bool = False) -> webdriver.Chrome:
    opts = ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1500,1050")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--lang=en-AU")
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"
    )
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    drv = webdriver.Chrome(options=opts)
    drv.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"},
    )
    return drv


def _text(el) -> str:
    try:
        return (el.text or "").strip()
    except Exception:
        return ""


def _find_first(driver, selectors):
    for sel in selectors:
        try:
            els = driver.find_elements(By.CSS_SELECTOR, sel)
        except Exception:
            continue
        for e in els:
            try:
                if e.is_displayed():
                    return e, sel
            except Exception:
                continue
    return None, None


def _find_all(driver, selectors):
    """Return the biggest list of elements across the candidate
    selectors (so we don't have to know the real class name)."""
    best = []
    best_sel = ""
    for sel in selectors:
        try:
            els = driver.find_elements(By.CSS_SELECTOR, sel)
        except Exception:
            continue
        if len(els) > len(best):
            best = els
            best_sel = sel
    return best, best_sel


# ------------------------------------------------------------------
_RE_SIZE_SPEC = re.compile(
    r"\b(\d{3})\s*/\s*(\d{2})\s*R\s*(\d{2}(?:\.\d+)?)\s*([0-9]{2,3}[A-Z]?)?",
    re.IGNORECASE,
)
_RE_PRICE  = re.compile(r"\$\s*([0-9]{1,3}(?:,\d{3})*(?:\.\d{1,2})?)")
_RE_LOCAL  = re.compile(r"(\d{1,5})\s+Local", re.IGNORECASE)
_RE_INTER  = re.compile(r"(\d{1,5})\s+Interstate", re.IGNORECASE)
_RE_CITY_N = re.compile(r"^\s*(\d{1,5})\s+([A-Za-z]+)\s*$")
_RE_ETA    = re.compile(r"ETA\s+([A-Z]{3})\s+(\d{1,2}/\d{1,2}/\d{4})", re.IGNORECASE)


def _parse_row(row_text: str) -> dict:
    """Parse one tyre-result-row's visible text into header fields.
    Structure observed in the screenshot:
        <brand>
        <pattern>
        [NEW] [SALE]
        195/65 R15 91V
        161 Local
        $31.54+
        [ADD TO CART]
    """
    lines = [ln.strip() for ln in (row_text or "").splitlines() if ln.strip()]
    # Strip the button + tags that are not substantive.
    filt = [ln for ln in lines
            if ln.upper() not in ("ADD TO CART", "SALE", "NEW",
                                  "MADE IN THAILAND", "MADE IN CHINA",
                                  "MADE IN KOREA", "MADE IN VIETNAM")]

    brand   = ""
    pattern = ""
    size_spec = ""
    load_speed = ""
    price = ""
    stock_local = ""
    stock_inter = ""

    # Brand + pattern: first 2 non-tag lines.
    if filt:
        brand = filt[0]
    if len(filt) > 1:
        pattern = filt[1]

    # Size + load/speed: match the size regex against any line.
    for ln in filt:
        m = _RE_SIZE_SPEC.search(ln)
        if m:
            w, a, r = m.group(1), m.group(2), m.group(3)
            size_spec = f"{w}/{a}R{r}"
            load_speed = (m.group(4) or "").upper()
            break

    # Local / Interstate stock figure (both reported shortly after size).
    for ln in filt:
        m = _RE_LOCAL.search(ln)
        if m:
            stock_local = m.group(1); break
    for ln in filt:
        m = _RE_INTER.search(ln)
        if m:
            stock_inter = m.group(1); break

    # Price: first $ figure in the row text.
    for ln in filt:
        m = _RE_PRICE.search(ln)
        if m:
            price = m.group(1).replace(",", "")
            break

    return {
        "brand":       brand,
        "pattern":     pattern,
        "size_spec":   size_spec,
        "load_speed":  load_speed,
        "price":       price,
        "stock_local": stock_local,
        "stock_interstate": stock_inter,
    }


def _parse_stock_popup(txt: str) -> dict:
    """Lines look like:
        161 Sydney
        271 Melbourne
        121 Brisbane
        30 Adelaide
        0 Perth
        107 Launceston
        1370 On Order
          ETA SYD 12/12/2026
          ETA MEL 12/12/2026
          ETA BRI 10/12/2026
          ETA PER 12/12/2026
    """
    out = {
        "stock_sydney": "", "stock_melbourne": "", "stock_brisbane": "",
        "stock_adelaide": "", "stock_perth": "", "stock_launceston": "",
        "stock_on_order": "", "eta_syd": "", "eta_mel": "",
        "eta_bri": "", "eta_per": "",
    }
    for raw in (txt or "").splitlines():
        ln = raw.strip()
        if not ln:
            continue
        m = _RE_CITY_N.match(ln)
        if m:
            qty, city = m.group(1), m.group(2).lower()
            key = CITY_KEYS.get(city)
            if key:
                out[key] = qty
            else:
                # Unknown city -- stash under stock_<city_lower>
                out[f"stock_{city}"] = qty
            continue
        if "on order" in ln.lower():
            m2 = re.search(r"(\d{1,6})", ln)
            if m2:
                out["stock_on_order"] = m2.group(1)
            continue
        em = _RE_ETA.search(ln)
        if em:
            code, dt = em.group(1).upper(), em.group(2)
            mapping = {"SYD": "eta_syd", "MEL": "eta_mel",
                       "BRI": "eta_bri", "PER": "eta_per"}
            if code in mapping:
                out[mapping[code]] = dt
    return out


# ------------------------------------------------------------------
_RE_ZERO_RESULTS = re.compile(
    r"\b0\s+RESULTS?\s+FOUND\b|\bNO\s+RESULTS?\s+FOUND\b|"
    r"\bPLEASE\s+CHANGE\s+YOUR\s+SEARCH\b",
    re.IGNORECASE,
)


def _empty_state_visible(driver) -> bool:
    """Return True when the /tyres search results panel is currently
    showing an explicit 'no results' message -- we should short-
    circuit the result wait in that case so the next size starts
    immediately instead of blocking the full timeout."""
    try:
        body_txt = (driver.find_element(By.TAG_NAME, "body").text or "")
    except Exception:
        return False
    return bool(_RE_ZERO_RESULTS.search(body_txt))


def search_size(driver, digits: str) -> bool:
    """Type the size digits into the search box and wait for either
    (a) at least one result row, or (b) an explicit '0 results found'
    message -- whichever comes first."""
    inp, sel = _find_first(driver, TYRE_SEARCH_INPUT_SELECTORS)
    if inp is None:
        _log(f"[search] could not find a search input on /tyres")
        return False
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", inp)
        inp.click()
        # Clear anything that might be in there.
        inp.send_keys(Keys.CONTROL, "a")
        inp.send_keys(Keys.DELETE)
        time.sleep(0.2)
        inp.send_keys(digits)
        time.sleep(0.5)
        inp.send_keys(Keys.ENTER)
    except Exception as e:
        _log(f"[search] typing failed: {e}")
        return False

    # Wait up to 20s but also peek for a "0 results" message so we
    # can bail out in under a second when YHI NZ doesn't carry that
    # size.
    end = time.time() + 20
    while time.time() < end:
        rows, _ = _find_all(driver, RESULT_ROW_SELECTORS)
        if rows:
            return True
        if _empty_state_visible(driver):
            _log(f"[search] 0 results for digits='{digits}' -- skipping")
            return False
        time.sleep(0.3)
    _log(f"[search] no result rows after 20s for digits='{digits}'")
    return False


def expand_stock(driver, row) -> dict:
    """Hover/click the 'N Local' label inside `row` to open the per-
    warehouse breakdown, scrape it, close the popup.  Returns a dict
    of parsed stock fields (blanks when the expand failed)."""
    blank = {
        "stock_sydney": "", "stock_melbourne": "", "stock_brisbane": "",
        "stock_adelaide": "", "stock_perth": "", "stock_launceston": "",
        "stock_on_order": "", "eta_syd": "", "eta_mel": "",
        "eta_bri": "", "eta_per": "",
    }
    # Find the clickable local-stock element inside this row.
    anchor = None
    for sel in STOCK_EXPAND_PATTERNS:
        try:
            els = row.find_elements(By.CSS_SELECTOR, sel)
            for e in els:
                try:
                    t = _text(e).lower()
                    if t and ("local" in t or "interstate" in t):
                        anchor = e; break
                except Exception:
                    continue
            if anchor is not None:
                break
        except Exception:
            continue
    # Fallback: any element inside the row whose visible text matches "N Local".
    if anchor is None:
        try:
            all_in = row.find_elements(By.XPATH, ".//*")
            for e in all_in:
                t = _text(e)
                if _RE_LOCAL.search(t) or _RE_INTER.search(t):
                    # Prefer leaf-ish nodes (shorter text) so we click
                    # the actual pill, not the whole row.
                    if 0 < len(t) < 25:
                        anchor = e; break
        except Exception:
            pass
    if anchor is None:
        return blank

    # Hover + click (the breakdown is usually on hover OR click).
    try:
        ActionChains(driver).move_to_element(anchor).perform()
        time.sleep(0.5)
        try:
            anchor.click()
        except Exception:
            driver.execute_script("arguments[0].click();", anchor)
        time.sleep(0.6)
    except Exception:
        pass

    # Collect text from any now-visible tooltip / popup near the anchor.
    popup_text = ""
    for sel in (".tooltip", ".popup", "[class*='tooltip']",
                "[class*='popup']", "[class*='stock-breakdown']",
                "[class*='stock-popup']"):
        try:
            cand = driver.find_elements(By.CSS_SELECTOR, sel)
            for c in cand:
                if not c.is_displayed():
                    continue
                t = _text(c)
                if t and ("sydney" in t.lower() or "melbourne" in t.lower()
                          or "brisbane" in t.lower()
                          or "launceston" in t.lower()):
                    popup_text = t; break
            if popup_text:
                break
        except Exception:
            continue

    # Close popup (click outside or Escape).
    try:
        ActionChains(driver).send_keys(Keys.ESCAPE).perform()
    except Exception:
        pass

    if not popup_text:
        return blank
    return _parse_stock_popup(popup_text)


_DEBUG_DUMPED = [False]      # one-shot flag so we only dump once


def crawl_size(driver, digits: str, size_label: str, sink,
               verbose: bool = False) -> int:
    """Run one size through the search + extraction + stock-expand
    pipeline and append rows to `sink`.  Returns the row count."""
    _log(f"Searching for digits='{digits}' ({size_label})...")
    # Always navigate to /tyres first so the search state is clean.
    try:
        driver.get(TYRES_URL)
    except Exception as e:
        _log(f"[nav] get(/tyres) failed: {e}")
        return 0
    time.sleep(2)
    if not search_size(driver, digits):
        return 0

    rows, used_sel = _find_all(driver, RESULT_ROW_SELECTORS)
    _log(f"  selector='{used_sel}' -> {len(rows)} row(s)")

    # Nov-19: first-row introspection -- on the very first search that
    # returns rows, dump the first row's raw text + outerHTML snippet
    # to disk so the operator can see what the parser has to work with.
    # Also lets me (the author) pin precise sub-selectors when the
    # adaptive wrapper selection lands on a container that misses the
    # price / size / stock.
    if rows and (verbose or not _DEBUG_DUMPED[0]):
        _DEBUG_DUMPED[0] = True
        try:
            first = rows[0]
            t = _text(first)
            html = first.get_attribute("outerHTML") or ""
            print("-" * 60)
            print(f"[introspect] first row .text (selector '{used_sel}'):")
            for ln in t.splitlines():
                print(f"    | {ln}")
            print(f"[introspect] first row outerHTML (first 1200 chars):")
            print("    " + html[:1200].replace("\n", " "))
            # Also save to a file -- easier to upload back for tuning.
            dump = os.path.join(BASE_DIR, "yhi_tyres_first_row.html")
            with open(dump, "w", encoding="utf-8") as f:
                f.write(html)
            print(f"[introspect] wrote full outerHTML to: {dump}")
            print("-" * 60)
        except Exception as e:
            print(f"[introspect] could not read first row: {e}")

    out_rows = []
    for row in rows:
        try:
            txt = _text(row)
        except Exception:
            continue
        parsed = _parse_row(txt)
        # Don't try to expand if there's no local stock to click.
        if parsed.get("stock_local") or parsed.get("stock_interstate"):
            try:
                stock_detail = expand_stock(driver, row)
            except Exception:
                stock_detail = {}
        else:
            stock_detail = {}
        rec = {
            "store_code":  STORE_CODE,
            "store_name":  STORE_NAME,
            "query_digits": digits,
            "query_size":   size_label,
            **parsed,
            **stock_detail,
            "crawled_at":   datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        out_rows.append(rec)

    sink.write_many(out_rows)
    return len(out_rows)


# ------------------------------------------------------------------
class CsvSink:
    HEAD = ["store_code", "store_name",
            "query_digits", "query_size",
            "brand", "pattern", "size_spec", "load_speed",
            "price",
            "stock_local", "stock_interstate",
            "stock_sydney", "stock_melbourne", "stock_brisbane",
            "stock_adelaide", "stock_perth", "stock_launceston",
            "stock_on_order",
            "eta_syd", "eta_mel", "eta_bri", "eta_per",
            "crawled_at"]

    def __init__(self, path: str):
        self.path = path
        self._fh = open(path, "w", newline="", encoding="utf-8")
        self._w = csv.DictWriter(self._fh, fieldnames=self.HEAD,
                                 extrasaction="ignore")
        self._w.writeheader()
        self._fh.flush()
        self.count = 0

    def write_many(self, rows):
        for r in rows:
            self._w.writerow(r)
        self.count += len(rows)
        self._fh.flush()

    def close(self):
        try: self._fh.close()
        except Exception: pass


# ------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", default="",
                    help="Comma-separated digit codes to crawl (overrides "
                         "the default 24-size list).  e.g. 1956515,2254517")
    ap.add_argument("--limit", type=int, default=0,
                    help="Only crawl the first N sizes (0 = all).")
    ap.add_argument("--debug", action="store_true",
                    help="Dump the /tyres page's HTML after login so "
                         "selectors can be tuned if extraction fails.")
    args = ap.parse_args()

    if args.sizes:
        want = [s.strip() for s in args.sizes.split(",") if s.strip()]
        size_list = [(d, d) for d in want]
    else:
        size_list = _load_size_list()
    if args.limit:
        size_list = size_list[:args.limit]

    out_path = os.path.join(BASE_DIR,
                            f"YHI_prices_{STORE_CODE}_{_ts()}.csv")
    _log(f"Store : {STORE_CODE} ({STORE_NAME})")
    _log(f"Sizes : {len(size_list)}")
    _log(f"Output: {out_path}")

    sink = CsvSink(out_path)
    drv  = make_driver(headless=False)
    try:
        _log(f"Opening {CUSTOMER_URL}")
        drv.get(CUSTOMER_URL)

        print("\n" + "=" * 72)
        print("  MANUAL PASSTHROUGH MODE  --  login AS YHI NZ")
        print("=" * 72)
        print("  In the Chrome window that just opened:")
        print()
        print("   1. Click 'Login' (top-right) and sign in to yhi.com.au.")
        print(f"   2. In the customer-search list, find '{STORE_NAME}'")
        print(f"      (SAP #0000{STORE_CODE}) and click its LOGIN button.")
        print("      That logs the session INTO that store.")
        print("   3. You'll land on the YHI homepage as YHI NZ -- stop.")
        print("      Do NOT navigate to /tyres yourself; the script will.")
        print()
        print("   (Do NOT close the browser.  The script needs it open.)")
        print("=" * 72)
        try:
            input("  Done 1+2+3?  Press ENTER to start the price crawl...  ")
        except EOFError:
            pass

        if args.debug:
            # Navigate to /tyres once so the debug dump is useful.
            try:
                drv.get(TYRES_URL)
                time.sleep(2)
                p = os.path.join(BASE_DIR, "yhi_tyres_page.html")
                with open(p, "w", encoding="utf-8") as f:
                    f.write(drv.page_source)
                _log(f"[debug] wrote {p}")
            except Exception as e:
                _log(f"[debug] dump failed: {e}")

        for i, (digits, label) in enumerate(size_list, 1):
            try:
                n = crawl_size(drv, digits, label, sink)
                _log(f"[{i}/{len(size_list)}] {label} -> {n} row(s) "
                     f"(total {sink.count})")
            except Exception as e:
                _log(f"[{i}/{len(size_list)}] {label} FAILED: {e}")
            time.sleep(1)

        _log(f"Done.  Wrote {sink.count} row(s) to {out_path}")
    except KeyboardInterrupt:
        _log(f"Interrupted.  Partial CSV at {out_path} "
             f"({sink.count} rows so far).")
    finally:
        sink.close()
        _log("Browser left open for inspection.  Close it manually.")


if __name__ == "__main__":
    main()
