#!/usr/bin/env python3
"""
yhi_customer_crawler.py -- crawl the YHI retailer directory at
https://www.yhi.com.au/customer-search and save every store's name,
owner and address into a timestamped CSV.

Why this exists: the public retailer directory lists ~4,376 stores
across ~365 pages, and the H-Force team needs the full roster for
cross-referencing against our customer master.  Manual copy-paste
is not viable.

Output:
    YHI_customers_YYYYMMDD_HHMM.csv  (next to this script)
    Columns: page, sap_code, store_name, owner, address

Pre-reqs (same as the other crawlers in this repo):
    * Chrome / Chromium installed
    * pip install selenium
    * Internet reachable to https://www.yhi.com.au/

Usage:
    python yhi_customer_crawler.py                 # all pages
    python yhi_customer_crawler.py --limit 5       # first 5 pages
    python yhi_customer_crawler.py --debug         # dump the first
                                                     page's HTML so you
                                                     can send it back
                                                     for selector tuning
    python yhi_customer_crawler.py --head          # visible browser
                                                     (headless by default)

Design notes:
    * I built this without being able to inspect yhi.com.au directly
      (sandbox network does not allow it), so the extractor is
      ADAPTIVE: it tries several selector strategies and uses the
      one that lands the most records.  If the site structure is
      different from the common patterns tried below, run with
      --debug and share back the first page's dump so I can tune
      the selectors precisely.
    * Saves CSV every FLUSH_EVERY pages so a Ctrl-C mid-run keeps
      what has been collected so far.
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
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    StaleElementReferenceException,
)


BASE_URL = "https://www.yhi.com.au/customer-search"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FLUSH_EVERY = 10              # write CSV to disk every N pages
PAGE_WAIT_SEC = 25            # was 12; some SPA sites need longer first paint
INTER_PAGE_DELAY = 0.8        # courtesy delay between page navigations

# Nov-19 user ask: known CSS class names extracted from the YHI
# customer-search JS chunk.  Using the exact names gets us a
# deterministic extractor instead of the adaptive guesswork.
CARD_SELECTOR        = ".component-customer-search-result-item"
NEXT_PAGE_SELECTOR   = ".pagination-item-control"    # prev / next arrows
PAGE_NUMBER_SELECTOR = ".pagination-item"            # numbered pages


def _ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M")


def _log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# ------------------------------------------------------------------
# Browser
# ------------------------------------------------------------------
def make_driver(headless: bool = True) -> webdriver.Chrome:
    opts = ChromeOptions()
    if headless:
        # The newer headless mode renders flex/grid layouts the same
        # way as a real window, so JS-rendered sites that look weird
        # in old-headless work normally here.
        opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1400,1000")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--lang=en-AU")
    # Pretend to be a current Chrome -- some sites 403 on default UA.
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"
    )
    # Suppress Chrome's "automation" banner that some sites sniff for.
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    drv = webdriver.Chrome(options=opts)
    drv.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"},
    )
    return drv


# ------------------------------------------------------------------
# Page detection + waiting
# ------------------------------------------------------------------
def wait_for_results(driver, timeout: int = PAGE_WAIT_SEC) -> bool:
    """Return True once at least 2 '.component-customer-search-result-item'
    cards are on the page.  Scrolls to the bottom + back once during the
    wait to trigger IntersectionObserver / lazy-load patterns that gate
    the next fetch on the list entering the viewport."""
    end = time.time() + timeout
    _scrolled_once = False
    while time.time() < end:
        try:
            cards = driver.find_elements(By.CSS_SELECTOR, CARD_SELECTOR)
            if len(cards) >= 2:
                return True
        except Exception:
            pass
        try:
            rs = driver.execute_script("return document.readyState")
        except Exception:
            rs = ""
        if rs == "complete" and not _scrolled_once:
            try:
                driver.execute_script(
                    "window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(0.6)
                driver.execute_script("window.scrollTo(0, 0);")
                _scrolled_once = True
            except Exception:
                pass
        time.sleep(0.5)
    return False


# ------------------------------------------------------------------
# Card extraction (adaptive)
# ------------------------------------------------------------------
_RE_SAP      = re.compile(r"#\s*0*\d{6,}")
_RE_POSTCODE = re.compile(r"\b(?:ACT|NSW|NT|QLD|SA|TAS|VIC|WA)\s+\d{4}\b",
                          re.IGNORECASE)


def _text(el) -> str:
    try:
        return (el.text or "").strip()
    except Exception:
        return ""


def extract_cards_text(driver) -> list[str]:
    """Return the raw .text of every .component-customer-search-result-item
    element on the current page."""
    try:
        els = driver.find_elements(By.CSS_SELECTOR, CARD_SELECTOR)
    except Exception:
        return []
    out = []
    for el in els:
        txt = _text(el)
        if txt:
            out.append(txt)
    return out


def parse_card(txt: str) -> dict:
    """Pull sap_code / store_name / owner / address out of a card's
    raw text.  Card layout observed in screenshots:
        #0000104333
        BENALLA AUTO
        HARLEN BLACK
        35 LOWANS LANE BENALLA VIC 3672
    The exact line ordering is tolerant: SAP code is matched by
    pattern, address is matched by STATE + postcode.  Everything
    between is treated as (store_name, owner)."""
    lines = [ln.strip() for ln in (txt or "").splitlines() if ln.strip()]
    # strip a trailing 'LOGIN' button-text line if present
    lines = [ln for ln in lines if ln.upper() != "LOGIN"]

    sap_code = ""
    address  = ""
    for ln in lines:
        m = _RE_SAP.search(ln)
        if m and not sap_code:
            sap_code = m.group(0).replace("#", "").strip()
            sap_code = sap_code.lstrip("0") or "0"
        if _RE_POSTCODE.search(ln) and not address:
            address = ln

    # Meta lines (sap_code line, address line) -> remove so the rest
    # is name + owner.
    meta = []
    for ln in lines:
        if _RE_SAP.search(ln) or _RE_POSTCODE.search(ln):
            meta.append(ln)
    middle = [ln for ln in lines if ln not in meta]

    store_name = middle[0] if len(middle) >= 1 else ""
    owner      = middle[1] if len(middle) >= 2 else ""

    return {
        "sap_code":   sap_code,
        "store_name": store_name,
        "owner":      owner,
        "address":    address,
    }


# ------------------------------------------------------------------
# Pagination
# ------------------------------------------------------------------
def total_pages(driver) -> int:
    """Read the max page number from the '.pagination-item' strip."""
    try:
        items = driver.find_elements(By.CSS_SELECTOR, PAGE_NUMBER_SELECTOR)
    except Exception:
        items = []
    nums = []
    for it in items:
        t = _text(it)
        if t.isdigit() and 1 <= len(t) <= 4:
            nums.append(int(t))
    return max(nums) if nums else 0


def click_next(driver, current_page: int) -> bool:
    """Click the Next arrow in the pagination bar and wait for the new
    cards to render.  Confirms success by waiting until the FIRST card
    on the page changes (new page means new first store code)."""
    # Record the current first-card text to detect the swap.
    before = ""
    try:
        first = driver.find_element(By.CSS_SELECTOR, CARD_SELECTOR)
        before = _text(first)
    except Exception:
        pass

    # The pagination strip has prev-arrow, [1][2][3]...[365], next-arrow.
    # NEXT is the LAST .pagination-item-control.
    try:
        ctrls = driver.find_elements(By.CSS_SELECTOR, NEXT_PAGE_SELECTOR)
    except Exception:
        ctrls = []
    if not ctrls:
        return False
    nxt = ctrls[-1]
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", nxt)
        nxt.click()
    except Exception:
        return False

    # Wait until the first-card text actually changes (strong signal
    # the new page has rendered).  Plain wait_for_results isn't enough
    # because the previous page's cards are still in the DOM for a
    # moment during the swap.
    end = time.time() + 15
    while time.time() < end:
        try:
            first = driver.find_element(By.CSS_SELECTOR, CARD_SELECTOR)
            if _text(first) and _text(first) != before:
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False


# ------------------------------------------------------------------
# CSV writer
# ------------------------------------------------------------------
class CsvSink:
    def __init__(self, path: str):
        self.path = path
        self._fh = open(path, "w", newline="", encoding="utf-8")
        self._w  = csv.DictWriter(self._fh,
            fieldnames=["page", "sap_code", "store_name", "owner", "address"])
        self._w.writeheader()
        self._fh.flush()
        self._count = 0

    def write_many(self, rows: list[dict]) -> None:
        for r in rows:
            self._w.writerow(r)
        self._count += len(rows)
        self._fh.flush()

    def close(self) -> None:
        try:
            self._fh.close()
        except Exception:
            pass

    @property
    def count(self) -> int:
        return self._count


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0,
                    help="Only crawl the first N pages (0 = all).")
    ap.add_argument("--debug", action="store_true",
                    help="Dump the first page's HTML to yhi_page1.html "
                         "for selector tuning.")
    ap.add_argument("--head", action="store_true",
                    help="Show the browser window (non-headless).  "
                         "Implied by --manual.")
    ap.add_argument("--manual", action="store_true",
                    help="Manual-passthrough mode: open a visible Chrome "
                         "window, let the OPERATOR click through any "
                         "reCAPTCHA / 'verify you are human' challenge, "
                         "then press Enter in the terminal to start "
                         "scraping.  Use this when headless is being "
                         "blocked by bot detection.")
    args = ap.parse_args()

    if args.manual:
        args.head = True   # manual mode must be visible

    out_path = os.path.join(BASE_DIR, f"YHI_customers_{_ts()}.csv")
    _log(f"Output: {out_path}")

    sink = CsvSink(out_path)
    drv = make_driver(headless=not args.head)
    try:
        _log(f"Opening {BASE_URL}")
        drv.get(BASE_URL)

        if args.manual:
            print("\n" + "=" * 72)
            print("  MANUAL PASSTHROUGH MODE  --  login required")
            print("=" * 72)
            print("  The browser window is now open.  yhi.com.au serves the")
            print("  customer-search list ONLY TO LOGGED-IN USERS, so:")
            print()
            print("   1. In the Chrome window that just opened, click 'Login'")
            print("      (top-right) or go to  https://www.yhi.com.au/login")
            print("   2. Enter your YHI credentials and sign in.")
            print("   3. Once you are at ANY logged-in page, come back here")
            print("      and press ENTER.  The script will then navigate the")
            print("      browser to /customer-search itself and start scraping.")
            print()
            print("   (Do NOT close the browser.  The script needs it open.)")
            print("=" * 72)
            try:
                input("  Logged in?  Press ENTER to start scraping...  ")
            except EOFError:
                pass
            # Re-navigate to the search page using the now-logged-in
            # session cookies.  Operator may be on /home, /profile or
            # wherever after login; forcing the URL here means they do
            # not have to click through to the search page themselves.
            _log(f"Navigating logged-in session to {BASE_URL}")
            try:
                drv.get(BASE_URL)
            except Exception as e:
                _log(f"[warn] could not navigate: {e}")
            if not wait_for_results(drv, timeout=PAGE_WAIT_SEC):
                _log("[warn] store list still did not render.  If the "
                     "browser shows a login prompt, log in again and "
                     "re-run with --manual.")
        else:
            if not wait_for_results(drv, timeout=PAGE_WAIT_SEC):
                _log("[warn] store list did not appear within timeout -- "
                     "trying to continue anyway.  If 0 rows are collected, "
                     "re-run with --manual to pass any human-check by hand.")

        if args.debug:
            dump_path = os.path.join(BASE_DIR, "yhi_page1.html")
            with open(dump_path, "w", encoding="utf-8") as f:
                f.write(drv.page_source)
            _log(f"[debug] wrote page 1 HTML to {dump_path}")

        # Quick sanity-check: how many cards are visible right now?
        try:
            _initial = len(drv.find_elements(By.CSS_SELECTOR, CARD_SELECTOR))
            _log(f"[init] {_initial} card(s) visible on page 1")
        except Exception:
            pass

        last = total_pages(drv) or 0
        if last:
            _log(f"[init] pagination reports {last} pages total")
        else:
            _log("[init] could not read total page count -- will crawl "
                 "until Next-click fails")

        if args.limit:
            last = min(last, args.limit) if last else args.limit
            _log(f"[opts] limit active -- stopping after page {last}")

        page = 1
        while True:
            if last and page > last:
                break
            cards = extract_cards_text(drv)
            rows = []
            for txt in cards:
                row = parse_card(txt)
                row["page"] = page
                rows.append(row)
            sink.write_many(rows)
            _log(f"Page {page}/{last or '?'}: {len(rows)} rows "
                 f"(total {sink.count})")

            if not rows:
                _log("[warn] 0 rows on this page -- waiting 5s and retrying.")
                time.sleep(5)
                cards = extract_cards_text(drv)
                if cards:
                    rows = [dict(**parse_card(t), page=page) for t in cards]
                    sink.write_many(rows)
                    _log(f"Page {page} retry OK: {len(rows)} rows "
                         f"(total {sink.count})")
                else:
                    _log(f"[fatal] still 0 rows on page {page} after retry. "
                         f"Stopping.")
                    break

            # Advance to next page.
            page += 1
            if last and page > last:
                break
            time.sleep(INTER_PAGE_DELAY)
            if not click_next(drv, page - 1):
                _log(f"[fatal] could not click Next after page {page - 1}. "
                     f"Stopping.")
                break

        _log(f"Done.  Wrote {sink.count} row(s) to {out_path}")

    except KeyboardInterrupt:
        _log(f"Interrupted by user.  Partial CSV at {out_path} "
             f"({sink.count} rows so far).")
    finally:
        sink.close()
        try:
            if not args.manual:
                drv.quit()
            else:
                _log("Browser left open for your review (manual mode).  "
                     "Close it manually when you're done.")
        except Exception:
            pass


if __name__ == "__main__":
    main()
