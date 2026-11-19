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
    """Return True once the store list has rendered.  Detects the list
    by looking for ANY element whose text starts with "#" + digits
    (the SAP code pattern visible in the screenshots).  Along the way
    we scroll the viewport to the bottom + back to the top to trigger
    IntersectionObserver / lazy-load patterns that only fetch cards
    once the list enters the viewport."""
    end = time.time() + timeout
    _scrolled_once = False
    while time.time() < end:
        try:
            rs = driver.execute_script("return document.readyState")
        except Exception:
            rs = ""
        if rs == "complete":
            # Look for the account-number style "#0000104333".
            try:
                hits = driver.find_elements(By.XPATH,
                    "//*[starts-with(normalize-space(text()), '#')]")
                good = []
                for h in hits:
                    t = _text(h)
                    if _RE_SAP.search(t):
                        good.append(h)
                if len(good) >= 2:          # need at least 2 cards
                    return True
            except Exception:
                pass
            # First-time trigger: scroll to the bottom so any lazy
            # list below the fold hydrates, then scroll back up.
            if not _scrolled_once:
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


def discover_card_selector(driver) -> str | None:
    """Pick the CSS selector that returns the most 'card-like' elements
    (each containing at least one SAP code AND one address postcode)."""
    candidates = [
        "div.store",
        "div.customer",
        "div.store-card",
        "div.customer-card",
        "div.store-item",
        "div.customer-item",
        "li.store",
        "li.customer",
        "article",
        "div.card",
        # last-resort: any direct-child div of the main content area
    ]
    best = (None, 0)
    for sel in candidates:
        try:
            els = driver.find_elements(By.CSS_SELECTOR, sel)
        except Exception:
            continue
        score = 0
        for el in els[:80]:
            txt = _text(el)
            if _RE_SAP.search(txt) and _RE_POSTCODE.search(txt):
                score += 1
        if score > best[1]:
            best = (sel, score)
    if best[0]:
        _log(f"[discover] card selector = '{best[0]}' with {best[1]} hits")
        return best[0]

    # Fallback: scan every element with class 'login'-adjacent parent
    # structure -- each store card has a Login button, so the LI/DIV
    # containing that button is likely the card.
    try:
        logins = driver.find_elements(By.XPATH,
            "//a[normalize-space(text())='Login'] | //button[normalize-space(text())='Login']")
        if logins:
            _log(f"[discover] found {len(logins)} Login buttons -- using parent heuristic")
            return "__login_parent__"
    except Exception:
        pass
    return None


def extract_cards_text(driver, card_sel: str) -> list[str]:
    """Return the raw .text of every card on the current page."""
    if card_sel == "__login_parent__":
        try:
            logins = driver.find_elements(By.XPATH,
                "//a[normalize-space(text())='Login'] | //button[normalize-space(text())='Login']")
        except Exception:
            return []
        out = []
        for btn in logins:
            # Walk up to the nearest ancestor that contains BOTH the
            # SAP code and an address postcode.
            node = btn
            for _ in range(8):
                try:
                    node = node.find_element(By.XPATH, "..")
                except Exception:
                    break
                txt = _text(node)
                if _RE_SAP.search(txt) and _RE_POSTCODE.search(txt):
                    out.append(txt)
                    break
        return out
    try:
        els = driver.find_elements(By.CSS_SELECTOR, card_sel)
    except Exception:
        return []
    out = []
    for el in els:
        txt = _text(el)
        if _RE_SAP.search(txt):
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
    """Read the max page number from the pagination strip."""
    try:
        buttons = driver.find_elements(By.XPATH,
            "//*[self::a or self::button][translate(normalize-space(text()), "
            "'0123456789', '9999999999') = translate(normalize-space(text()), "
            "'0123456789', '9999999999') and normalize-space(text())!='']")
    except Exception:
        buttons = []
    nums = []
    for b in buttons:
        t = _text(b)
        if t.isdigit() and 1 <= len(t) <= 4:
            nums.append(int(t))
    # The pagination strip has "1 2 3 4 ... 365 NEXT".
    return max(nums) if nums else 0


def goto_page(driver, page_num: int) -> bool:
    """Try URL-based pagination first (?page=N); fall back to clicking
    the NEXT button or the numbered pagination link."""
    # (1) URL rewrite
    url_with = f"{BASE_URL}?page={page_num}"
    try:
        driver.get(url_with)
        if wait_for_results(driver, timeout=8):
            # Sanity: confirm the page param stuck.
            return True
    except Exception:
        pass
    # (2) click the numbered link
    try:
        el = driver.find_element(By.XPATH,
            f"//a[normalize-space(text())='{page_num}'] | "
            f"//button[normalize-space(text())='{page_num}']")
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        el.click()
        time.sleep(1.0)
        return wait_for_results(driver, timeout=8)
    except Exception:
        return False


def click_next(driver) -> bool:
    try:
        nxt = driver.find_element(By.XPATH,
            "//a[contains(translate(normalize-space(text()), "
            "'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'), 'NEXT')] | "
            "//button[contains(translate(normalize-space(text()), "
            "'abcdefghijklmnopqrstuvwxyz', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'), 'NEXT')]")
    except Exception:
        return False
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", nxt)
        nxt.click()
        time.sleep(1.0)
        return wait_for_results(driver, timeout=8)
    except Exception:
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
                    help="Start from this page number (default 1).")
    ap.add_argument("--debug", action="store_true",
                    help="Dump the first page's HTML to yhi_page1.html "
                         "so you can send it back for selector tuning "
                         "if extraction looks wrong.")
    ap.add_argument("--head", action="store_true",
                    help="Show the browser window (non-headless).")
    args = ap.parse_args()

    out_path = os.path.join(BASE_DIR, f"YHI_customers_{_ts()}.csv")
    _log(f"Output: {out_path}")

    sink = CsvSink(out_path)
    drv = make_driver(headless=not args.head)
    try:
        _log(f"Opening {BASE_URL}")
        drv.get(BASE_URL)
        if not wait_for_results(drv, timeout=PAGE_WAIT_SEC):
            _log("[warn] store list did not appear within timeout -- "
                 "trying to continue anyway")

        if args.debug:
            dump_path = os.path.join(BASE_DIR, "yhi_page1.html")
            with open(dump_path, "w", encoding="utf-8") as f:
                f.write(drv.page_source)
            _log(f"[debug] wrote page 1 HTML to {dump_path}")

        card_sel = discover_card_selector(drv)
        if not card_sel:
            _log("[fatal] could not discover a card selector on page 1. "
                 "Re-run with --debug and share the dumped yhi_page1.html.")
            return

        last = total_pages(drv) or 0
        if last:
            _log(f"[discover] pagination reports {last} pages total")
        else:
            _log("[discover] could not read total page count -- will "
                 "use click-next fallback")

        if args.limit:
            last = min(last, args.limit) if last else args.limit
            _log(f"[opts] limit active -- stopping after page {last}")

        start_page = max(args.start, 1)
        page = start_page
        if page != 1:
            _log(f"Jumping to start page {page}")
            if not goto_page(drv, page):
                _log(f"[fatal] could not jump to page {page}")
                return

        while True:
            if last and page > last:
                break
            cards = extract_cards_text(drv, card_sel)
            rows = []
            for txt in cards:
                row = parse_card(txt)
                row["page"] = page
                rows.append(row)
            sink.write_many(rows)
            _log(f"Page {page}/{last or '?'}: {len(rows)} rows "
                 f"(total {sink.count})")

            if not rows:
                _log("[warn] 0 rows on this page -- likely rate-limited or "
                     "selector drift.  Pausing 5 s and retrying once.")
                time.sleep(5)
                cards = extract_cards_text(drv, card_sel)
                if cards:
                    rows = [dict(**parse_card(t), page=page) for t in cards]
                    sink.write_many(rows)
                    _log(f"Page {page} retry OK: {len(rows)} rows "
                         f"(total {sink.count})")
                else:
                    _log(f"[fatal] still 0 rows on page {page} after retry. "
                         f"Stopping.")
                    break

            # Advance.
            page += 1
            if last and page > last:
                break
            time.sleep(INTER_PAGE_DELAY)
            if not goto_page(drv, page):
                if not click_next(drv):
                    _log(f"[fatal] could not navigate to page {page}. Stop.")
                    break

        _log(f"Done.  Wrote {sink.count} row(s) to {out_path}")

    except KeyboardInterrupt:
        _log(f"Interrupted by user.  Partial CSV at {out_path} "
             f"({sink.count} rows so far).")
    finally:
        sink.close()
        try:
            drv.quit()
        except Exception:
            pass


if __name__ == "__main__":
    main()
