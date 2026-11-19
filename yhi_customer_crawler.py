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
# Nov-19: the list only fetches after the user touches the search
# input -- the page is coded to wait for a keystroke event instead of
# auto-loading.  We need to focus this input and send a dummy char
# + backspace to trigger the onChange handler.  Operator confirmed
# this manually: typing 'Y' and deleting it made the full list appear.
SEARCH_INPUT_SELECTORS = (
    ".component-customer-search-bar input",
    ".search-bar input",
    "input[placeholder*='Customer Search']",
    "input[placeholder*='Search']",
    "input[type='text']",
    "input[type='search']",
)


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


def trigger_search_list(driver) -> bool:
    """Focus the search input and send a dummy keystroke + backspace so
    the page's onChange handler fires and the full list fetches.  The
    operator confirmed manually that typing 'Y' and immediately
    deleting it reveals the whole 4,376-store list; this reproduces
    that interaction programmatically."""
    inp = None
    for sel in SEARCH_INPUT_SELECTORS:
        try:
            cand = driver.find_elements(By.CSS_SELECTOR, sel)
        except Exception:
            cand = []
        for c in cand:
            try:
                if c.is_displayed() and c.is_enabled():
                    inp = c
                    _log(f"[trigger] search input found via '{sel}'")
                    break
            except Exception:
                continue
        if inp is not None:
            break
    if inp is None:
        _log("[trigger] could not find a search input to poke")
        return False

    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", inp)
        inp.click()
        time.sleep(0.2)
        inp.send_keys("y")
        time.sleep(0.3)
        inp.send_keys(Keys.BACK_SPACE)
        time.sleep(0.3)
        return True
    except Exception as e:
        _log(f"[trigger] keystroke failed: {e}")
        return False


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


def current_page_from_dom(driver) -> int:
    """Return the page number the pagination strip currently highlights
    as active, or 0 if no active marker is detected.  Looks for any
    .pagination-item with an 'active', 'current', 'selected' or
    'is-active' modifier class; falls back to the first numbered item
    whose aria-current attribute is set.  Nov-19: added because
    --start was just a CSV label -- scraper has to know the REAL
    page the browser is on, not whatever counter we incremented."""
    try:
        items = driver.find_elements(By.CSS_SELECTOR, PAGE_NUMBER_SELECTOR)
    except Exception:
        items = []
    for it in items:
        try:
            cls = (it.get_attribute("class") or "").lower()
            ac  = (it.get_attribute("aria-current") or "").lower()
        except Exception:
            cls = ac = ""
        if ("active" in cls or "current" in cls or "selected" in cls
                or ac in ("page", "true")):
            t = _text(it)
            if t.isdigit():
                return int(t)
    return 0


def _wait_new_first_card(driver, before: str, timeout: float = 15) -> bool:
    """Block until the FIRST card's text differs from `before` -- a
    strong signal the new page has rendered."""
    end = time.time() + timeout
    while time.time() < end:
        try:
            first = driver.find_element(By.CSS_SELECTOR, CARD_SELECTOR)
            txt = _text(first)
            if txt and txt != before:
                return True
        except Exception:
            pass
        time.sleep(0.3)
    return False


def click_next(driver, current_page: int) -> bool:
    """Advance to the next page.  Strategy order:
       1. Click the specific page-number button (page N+1) if visible.
       2. Click the NEXT arrow (last .pagination-item-control).
    Confirms success by waiting until the first-card text changes.
    Nov-19: URL-based pagination is NOT supported by the YHI page
    (confirmed by observation: ?page=2 is ignored, the list depends
    on in-page state), so we rely on DOM clicks here."""
    before = ""
    try:
        first = driver.find_element(By.CSS_SELECTOR, CARD_SELECTOR)
        before = _text(first)
    except Exception:
        pass
    target_page = current_page + 1

    # Strategy 1: click the specific page-number button.  More
    # reliable mid-crawl because the pagination strip reshapes around
    # the current page (PREV 1 ... 27 28 [29] 30 ... 365 NEXT), and
    # clicking "30" always advances by exactly one page.
    try:
        items = driver.find_elements(By.CSS_SELECTOR, PAGE_NUMBER_SELECTOR)
    except Exception:
        items = []
    for it in items:
        t = _text(it)
        if t == str(target_page):
            try:
                driver.execute_script(
                    "arguments[0].scrollIntoView({block:'center'});", it)
                it.click()
                if _wait_new_first_card(driver, before):
                    return True
            except Exception:
                pass
            break   # found the right page number but click failed -- fall through

    # Strategy 2: NEXT arrow (last .pagination-item-control).
    try:
        ctrls = driver.find_elements(By.CSS_SELECTOR, NEXT_PAGE_SELECTOR)
    except Exception:
        ctrls = []
    if not ctrls:
        _log(f"[next] no .pagination-item-control elements on page {current_page}")
        return False
    nxt = ctrls[-1]
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", nxt)
        try:
            nxt.click()
        except Exception:
            # Overlay or sticky header covers it -- fall back to a JS click
            driver.execute_script("arguments[0].click();", nxt)
        return _wait_new_first_card(driver, before)
    except Exception as e:
        _log(f"[next] click failed on page {current_page}: {e}")
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
    ap.add_argument("--start", type=int, default=1,
                    help="Start from this page number (default 1).  Use "
                         "together with --manual: before pressing ENTER, "
                         "manually navigate the browser to that page.")
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
            print("  yhi.com.au only returns the store list AFTER you have:")
            print("     (a) logged in, AND")
            print("     (b) interacted with the search box on the page.")
            print()
            print("  In the browser window that just opened, do these 3 steps:")
            print()
            print("   1. Click 'Login' (top-right) and sign in with YHI creds.")
            print("   2. Navigate to  https://www.yhi.com.au/customer-search")
            print("      (type it in the address bar, or click the menu).")
            print("   3. STOP there -- do NOT touch the search box.  Come back")
            print("      here and press ENTER.  The script will poke the search")
            print("      box automatically (sends 'y' + BACKSPACE), which makes")
            print("      the full list of ~4,376 stores appear.")
            print()
            print("   (Do NOT close the browser.  The script needs it open.)")
            print("=" * 72)
            try:
                input("  Done 1+2?  Press ENTER to start scraping...  ")
            except EOFError:
                pass

            # Check we are on /customer-search; if not, navigate once.
            # Then trigger the search so the list fetches.
            try:
                cur = (drv.current_url or "").lower()
            except Exception:
                cur = ""
            if "customer-search" not in cur:
                _log(f"Browser is at '{cur}' -- navigating to {BASE_URL}")
                try:
                    drv.get(BASE_URL)
                    time.sleep(2)
                except Exception as e:
                    _log(f"[warn] navigate failed: {e}")

            # Trigger the search-box onChange so the list hydrates.
            _log("Triggering search box to force list fetch...")
            if not trigger_search_list(drv):
                _log("[warn] could not drive the search box.  If the page is "
                     "still empty, click it yourself, type a character and "
                     "delete it, then re-run the script.")
            # Now wait for cards.
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

        # Read the ACTUAL active page from the pagination strip rather
        # than trusting a counter.  --start N is accepted but if the
        # browser is not actually on page N (operator forgot to click
        # through), we refuse to run so the CSV never gets mislabelled
        # rows.
        dom_page = current_page_from_dom(drv) or 1
        _log(f"[init] browser is currently on page {dom_page}")
        if args.start > 1 and dom_page != args.start:
            _log(f"[fatal] --start {args.start} but the browser is on "
                 f"page {dom_page}.  Click page {args.start} in the "
                 f"browser's pagination strip, then rerun with --manual "
                 f"--start {args.start}.")
            return
        page = dom_page

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
            next_target = page + 1
            if last and next_target > last:
                break
            time.sleep(INTER_PAGE_DELAY)
            if not click_next(drv, page):
                _log(f"[fatal] could not click Next after page {page}. "
                     f"Stopping.")
                break
            # Re-read the active page from the DOM so we never drift
            # from what the browser actually shows.
            new_page = current_page_from_dom(drv) or (page + 1)
            if new_page == page:
                _log(f"[warn] active page didn't advance from {page} "
                     f"after Next -- treating as end of list.")
                break
            page = new_page

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
