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
    ".component-product-row",    # confirmed from live DOM Nov-19
    # Fall-backs in case YHI renames the component later.
    "[class*='product-row']",
    "[class*='tyre-search-result']",
)
STOCK_EXPAND_PATTERNS = (
    # Link text that opens the per-warehouse breakdown.
    "a[href*='Local']",
    "a.stock-link",
    "span.stock-link",
    "[class*='stock-detail']",
    "[class*='stock-popup']",
)

KNOWN_CITIES = ("sydney", "melbourne", "brisbane", "adelaide",
                "perth", "launceston")
# Operator asked to drop the 'stock_' prefix.  Columns now land as
# 'sydney', 'melbourne', ... directly.
CITY_KEYS = {c: c for c in KNOWN_CITIES}


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
# Nov-19: YHI renders prices as e.g. "$ 41 .61 +" -- spaces around the
# decimal point.  Accept both that form and the normal "$41.61" form.
_RE_PRICE  = re.compile(
    r"\$\s*([0-9]{1,3}(?:,?\d{3})*)\s*(?:[.,]\s*(\d{1,2}))?"
)
_RE_LOCAL  = re.compile(r"\b(\d{1,5})\s+Local\b", re.IGNORECASE)
_RE_INTER  = re.compile(r"\b(\d{1,5})\s+Interstate\b", re.IGNORECASE)
_RE_SUPPLIER = re.compile(r"\b(\d{1,5})\s+Supplier\s+Stock\b", re.IGNORECASE)
_RE_CITY_N = re.compile(r"^\s*(\d{1,5})\s+([A-Za-z]+)\s*$")
_RE_ETA    = re.compile(r"ETA\s+([A-Z]{3})\s+(\d{1,2}/\d{1,2}/\d{4})", re.IGNORECASE)
# Speed index + load rating pattern inside a size phrase: digits then
# a single letter (e.g. 91V, 94W, 105H).  Appears right after the
# rim diameter in "195/65 R15 91V".
_RE_SI_LR = re.compile(r"\bR\s*\d{2}(?:\.\d+)?\s+([0-9]{2,3}[A-Z])\b",
                       re.IGNORECASE)


def _text_of(root, selector, default=""):
    """Helper: return .text of the first element matching `selector`
    under `root`, or `default` when the selector doesn't match."""
    try:
        return (root.find_element(By.CSS_SELECTOR, selector).text or "").strip()
    except Exception:
        return default


def _extract_row_dom(row) -> dict:
    """Extract fields from the live DOM of a .component-product-row
    element.  Structure confirmed Nov-19 from the operator's live
    yhi_tyres_first_row.html dump:

        <div class="component-product-row">
          <div class="product-status"> ... </div>
          <div class="product-image">   ...  </div>
          <div class="product-details">
            <p class="h6 brand">ATLAS</p>
            <p class="h1 name">AS380</p>
            <p>  (attribute line, e.g. 'RUNFLAT - BMW', or empty)  </p>
          </div>
          <div class="front-rear-specs">
            <div class="front-details">
              <div class="product-specs">
                <p class="h5">
                  <span class="medium">175/65</span>
                  <span class="medium">R14</span>
                  <span>82T</span>           <- SI/LR
                </p>
              </div>
              <div class="stock-levels">
                <div class="stock-color" ...></div>
                <div class="tooltip-wrapper">
                  <div class="stock-level-status">
                    <span class="h5 medium"> 63 </span>
                    <span class="h5">Local</span>    <- or 'Interstate' / 'Supplier Stock'
                  </div>
                </div>
              </div>
              <div class="price">
                <p class="">
                  <span>$</span>
                  <span>24</span>
                  <span>.84</span>
                  <span>+</span>
                </p>
              </div>
              <div class="add-to-cart"> ... </div>
            </div>
          </div>
        </div>

    Querying the sub-elements directly instead of parsing the row's
    .text means we never misread a stray page-level number as this
    row's stock, and we can pull the price out of the broken-up span
    structure cleanly."""
    import re as _re

    # Status pill (NEW / SALE / etc.) -- sits in .product-status.
    # When a row carries no pill the extractor returns "".
    status_tag = ""
    try:
        pills = row.find_elements(By.CSS_SELECTOR, ".product-status .status-pill")
        parts = []
        for p in pills:
            t = (p.text or "").strip()
            if t:
                parts.append(t.upper())
        status_tag = " / ".join(parts)
    except Exception:
        pass

    # brand + pattern
    brand   = _text_of(row, ".product-details .brand")
    pattern = _text_of(row, ".product-details .name")

    # attribute line: any <p> inside .product-details that is NOT
    # .brand / .name AND does NOT carry size / price characters.
    # (Belt-and-braces: a few early rows ended up with attribute_desc
    # of the shape "175/65 R14 82T $ 24 .84 +", suggesting a sibling
    # <p> occasionally contains the full-row readout.  Reject anything
    # that has a size pattern, '$', or more than 3 digits in a row --
    # a real attribute line is text like "RUNFLAT - BMW" or
    # "MADE IN THAILAND".)
    attribute_desc = ""
    try:
        details_ps = row.find_elements(By.CSS_SELECTOR, ".product-details p")
        for p in details_ps:
            cls = (p.get_attribute("class") or "").lower()
            if "brand" in cls or "name" in cls:
                continue
            txt = (p.text or "").strip(" -\t·•*|")
            if not txt:
                continue
            if _RE_SIZE_SPEC.search(txt):
                continue
            if "$" in txt:
                continue
            if _re.search(r"\d{4,}", txt):
                continue
            attribute_desc = txt
            break
    except Exception:
        pass

    attrs = []
    if attribute_desc:
        pieces = _re.split(r"\s*[·•*|]\s*", attribute_desc)
        for p in pieces:
            p = p.strip(" -\t·•*|")
            if p:
                attrs.append(p)

    # SI/LR -- the LAST <span> inside .product-specs is '82T' / '91V'.
    si_lr = ""
    try:
        spec_spans = row.find_elements(
            By.CSS_SELECTOR, ".product-specs span")
        if spec_spans:
            last = (spec_spans[-1].text or "").strip().upper()
            if _re.match(r"^\d{2,3}[A-Z]$", last):
                si_lr = last
    except Exception:
        pass

    # Price: assemble from the per-span rendering "$ / 24 / .84 / +".
    price = ""
    try:
        price_spans = row.find_elements(By.CSS_SELECTOR, ".price span")
        parts = []
        for s in price_spans:
            t = (s.text or "").strip()
            if t and t not in ("$", "+"):
                parts.append(t)
        joined = "".join(parts).replace(",", "")
        if joined:
            m = _re.match(r"^(\d+)(\.\d{1,2})?$", joined)
            if m:
                price = m.group(1) + (m.group(2) or "")
    except Exception:
        pass

    # Stock badge: '63 Local' / 'N Interstate' / 'N Supplier Stock'
    stock_local = ""
    stock_inter = ""
    stock_supp  = ""
    try:
        status = row.find_element(
            By.CSS_SELECTOR, ".stock-level-status")
        status_txt = (status.text or "").strip()
        m = _RE_LOCAL.search(status_txt)
        if m: stock_local = m.group(1)
        m = _RE_INTER.search(status_txt)
        if m: stock_inter = m.group(1)
        m = _RE_SUPPLIER.search(status_txt)
        if m: stock_supp  = m.group(1)
    except Exception:
        pass

    return {
        "status_tag":     status_tag,
        "brand":          brand,
        "pattern":        pattern,
        "attribute_desc": attribute_desc,
        "attr_1": attrs[0] if len(attrs) > 0 else "",
        "attr_2": attrs[1] if len(attrs) > 1 else "",
        "attr_3": attrs[2] if len(attrs) > 2 else "",
        "si_lr":          si_lr,
        "price":          price,
        "stock_local":      stock_local,
        "stock_interstate": stock_inter,
        "stock_supplier":   stock_supp,
    }


def _parse_row(row_text: str) -> dict:
    """Parse one tyre-result-row's visible text into fields.

    Columns captured per row:
      * brand            e.g. 'PIRELLI'
      * pattern          e.g. 'P7 CINTURATO (*)'
      * attribute_desc   raw attribute line ONLY when one exists -- e.g.
                         'MADE IN THAILAND' or 'RUNFLAT - BMW'; never the
                         size line and never the price line.
      * attr_1, attr_2, attr_3: attribute_desc split on the YHI bullet.
      * si_lr            the speed-index + load-rating pill, e.g. '91V'
                         or '94W' (extracted from the row's size phrase
                         but the size itself is NOT stored, since
                         query_size already carries it).
      * price            numeric string with dot decimal only, no '$',
                         no trailing '+', no thousands commas -- e.g.
                         '41.61'.
      * stock_local, stock_interstate, stock_supplier -- integer
                         strings pulled from one of 'N Local' /
                         'N Interstate' / 'N Supplier Stock' badges.

    Rows render like one of:
        PIRELLI
        P7 CINTURATO (*)
        RUNFLAT - BMW        <- attribute line (U+00B7 bullet)
        225/45 R17 91Y       <- size + SI/LR
        7 Local              <- stock badge
        $143.97+             <- price pill

    YHI occasionally emits the size + price on a SINGLE visual line
    with weird spacing, e.g. '225/45 R17 94W   $ 41 .61 +'.  The
    parser therefore looks for each pattern independently per line
    AND once across the whole row text, picking whichever extraction
    yields the SI/LR and the price.
    """
    import re as _re
    raw   = row_text or ""
    lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
    # 'ADD TO CART', 'SALE' and 'NEW' are button / badge texts with
    # no data content -- drop them before classifying.
    filt = [ln for ln in lines
            if ln.upper() not in ("ADD TO CART", "SALE", "NEW")]

    si_lr = ""
    price = ""
    stock_local    = ""
    stock_inter    = ""
    stock_supplier = ""

    # SI/LR: extracted from the first line that carries a size phrase.
    # The size string itself is NOT kept -- query_size has it.
    joined = " ".join(filt)
    m = _RE_SI_LR.search(joined)
    if m:
        si_lr = m.group(1).upper()

    # Price: accept both '$41.61' and '$ 41 .61'.
    m = _RE_PRICE.search(joined)
    if m:
        whole, frac = m.group(1), m.group(2)
        whole = whole.replace(",", "")
        if frac:
            price = f"{whole}.{frac}"
        else:
            price = whole

    # Stock badges -- one of these three (per-row, not the global search
    # count; we match the badges on isolated text to avoid stray big
    # numbers elsewhere on the page).
    for ln in filt:
        if not stock_local:
            m = _RE_LOCAL.search(ln)
            if m: stock_local = m.group(1)
        if not stock_inter:
            m = _RE_INTER.search(ln)
            if m: stock_inter = m.group(1)
        if not stock_supplier:
            m = _RE_SUPPLIER.search(ln)
            if m: stock_supplier = m.group(1)

    # Figure out which lines were 'consumed' (size / stock / price) so
    # the remaining lines can be split into brand + pattern + attributes.
    def _consumed(ln):
        if _RE_SIZE_SPEC.search(ln):     return True
        if _RE_PRICE.search(ln):         return True
        if _RE_LOCAL.search(ln):         return True
        if _RE_INTER.search(ln):         return True
        if _RE_SUPPLIER.search(ln):      return True
        return False

    carrier_lines = [ln for ln in filt if not _consumed(ln)]
    brand   = carrier_lines[0] if len(carrier_lines) >= 1 else ""
    pattern = carrier_lines[1] if len(carrier_lines) >= 2 else ""
    attribute_lines = carrier_lines[2:] if len(carrier_lines) >= 3 else []
    attribute_desc  = " ".join(attribute_lines).strip()

    # Split attributes on the YHI bullet (U+00B7) or an ASCII fallback.
    attrs = []
    if attribute_desc:
        pieces = _re.split(r"\s*[·•*|]\s*", attribute_desc)
        for p in pieces:
            p = p.strip(" -\t·•*|")
            if p:
                attrs.append(p)

    return {
        "brand":          brand,
        "pattern":        pattern,
        "attribute_desc": attribute_desc,
        "attr_1": attrs[0] if len(attrs) > 0 else "",
        "attr_2": attrs[1] if len(attrs) > 1 else "",
        "attr_3": attrs[2] if len(attrs) > 2 else "",
        "si_lr":          si_lr,
        "price":          price,
        "stock_local":      stock_local,
        "stock_interstate": stock_inter,
        "stock_supplier":   stock_supplier,
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

    Dynamic: whatever cities appear are captured, keyed by
    lower-case city name.  'On Order' has its own slot, and ETA
    lines are matched back to their state code.

    If the operator hover-stacked tooltips so the text contains the
    same city twice, the LAST occurrence wins (so the latest row's
    tooltip dominates).  The expand_stock() path also now dismisses
    the tooltip explicitly so this stacking should not happen going
    forward."""
    out = {c: "" for c in KNOWN_CITIES}
    out.update({"on_order": "", "eta_syd": "", "eta_mel": "",
                "eta_bri": "", "eta_per": ""})

    # YHI's tooltip is rendered with each number and each label in
    # separate <span> elements, so Selenium's .text returns them on
    # ALTERNATING lines:
    #     63
    #     Sydney
    #     31
    #     Melbourne
    #     ...
    #     50
    #     On Order
    #     ETA BRI 12/12/2026
    # We therefore walk line-pairs: if line N is just a number and
    # line N+1 is a label we recognise, that is one (qty, label)
    # entry.  Still handle the single-line "63 Sydney" form too for
    # robustness (some pages may render it that way).
    lines = [raw.strip() for raw in (txt or "").splitlines() if raw.strip()]

    KNOWN_LABELS = set(KNOWN_CITIES)   # lower-cased

    def _stash(qty, label):
        lab = (label or "").strip().lower()
        if not qty or not lab:
            return
        if "order" in lab:
            out["on_order"] = qty
            return
        # Any city word -- known or novel -- is stored.  A novel city
        # drops under its lower-cased name without an existing column,
        # which the DictWriter's extrasaction='ignore' quietly drops.
        out[lab] = qty

    i = 0
    while i < len(lines):
        ln = lines[i]

        # ETA line (anywhere it appears, on its own line in practice).
        em = _RE_ETA.search(ln)
        if em:
            code, dt = em.group(1).upper(), em.group(2)
            mapping = {"SYD": "eta_syd", "MEL": "eta_mel",
                       "BRI": "eta_bri", "PER": "eta_per"}
            if code in mapping:
                out[mapping[code]] = dt
            i += 1
            continue

        # Single-line "63 Sydney" / "50 On Order"
        m = _RE_CITY_N.match(ln)
        if m:
            _stash(m.group(1), m.group(2))
            i += 1
            continue

        # Two-line form: line N is a pure number, line N+1 is the label.
        if ln.isdigit() and i + 1 < len(lines):
            nxt = lines[i + 1]
            # Label line must be a word / a few words (letters + spaces).
            if re.match(r"^[A-Za-z][A-Za-z ]{1,24}$", nxt):
                _stash(ln, nxt)
                i += 2
                continue

        i += 1

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


_TOOLTIP_DUMPED = [False]      # one-shot tooltip HTML dump
_LAST_ANCHOR = [None]          # pointer to the previously-hovered pill

# JS payload: thoroughly dismiss the LAST hovered pill by firing
# pointerleave + mouseleave + mouseout on it AND on every ancestor
# up to document.body.  Then hover a neutral invisible target so the
# browser's :hover CSS state drops off the pill entirely.  Called at
# the START of each expand_stock() iteration instead of at the end
# of the previous one -- that way a wholesome mouseleave is the last
# event before the next mouseenter, which is what the YHI component
# listens for.
_JS_DISMISS_LAST = """
const prev = arguments[0];
if (!prev) { return; }
const opts = {bubbles: true, cancelable: true, view: window};
['pointerout', 'pointerleave', 'mouseout', 'mouseleave'].forEach(evt => {
  prev.dispatchEvent(new MouseEvent(evt, opts));
});
let p = prev.parentElement;
let hops = 0;
while (p && hops < 12 && p !== document.body) {
  ['pointerleave', 'mouseleave', 'mouseout'].forEach(evt => {
    p.dispatchEvent(new MouseEvent(evt, opts));
  });
  p = p.parentElement;
  hops += 1;
}
document.body.dispatchEvent(new MouseEvent('mouseover',
  {bubbles: true, cancelable: true, clientX: 2, clientY: 2}));
"""

# JS payload: dispatches a sequence of mouseover + mouseenter events
# on the anchor so React/Vue hover handlers mount the tooltip reliably
# (ActionChains hover was intermittently not firing against the YHI
# component's listener).
_JS_HOVER = """
const el = arguments[0];
const bbox = el.getBoundingClientRect();
const x = bbox.left + bbox.width / 2;
const y = bbox.top  + bbox.height / 2;
const opts = {bubbles: true, cancelable: true,
              clientX: x, clientY: y, view: window};
['pointerover', 'mouseover', 'mouseenter', 'mousemove'].forEach(evt => {
  el.dispatchEvent(new MouseEvent(evt, opts));
});
"""

# Nov-19: the YHI tooltip component does NOT dismiss on
# ActionChains.move_by_offset; previous rows' tooltips stacked
# visually.  We now explicitly fire pointerleave + mouseleave +
# mouseout on both the anchor AND its tooltip-wrapper parent so the
# component's hover state resets.  The sequence of bubbles=false
# events reaches the same listener chain the browser would use on a
# real mouse-out.
_JS_UNHOVER = """
const el = arguments[0];
const bbox = el.getBoundingClientRect();
const opts = {bubbles: true, cancelable: true,
              clientX: bbox.left + bbox.width + 400,
              clientY: bbox.top  + bbox.height + 400,
              view: window};
['pointerout', 'pointerleave', 'mouseout', 'mouseleave'].forEach(evt => {
  el.dispatchEvent(new MouseEvent(evt, opts));
});
let p = el.parentElement;
let hops = 0;
while (p && hops < 4) {
  ['pointerleave', 'mouseleave', 'mouseout'].forEach(evt => {
    p.dispatchEvent(new MouseEvent(evt, opts));
  });
  p = p.parentElement;
  hops += 1;
}
"""


def _scan_for_tooltip(driver, row, wrapper):
    """Find the stock-breakdown tooltip wherever it was mounted.
    Returns (popup_text, popup_outerHTML) on hit, or ('', '').

    A real tooltip body contains at least TWO of the six warehouse
    names -- single-city text elsewhere on the page (promotional
    banners etc.) won't match."""
    CITIES_RE = re.compile(r"\b(sydney|melbourne|brisbane|adelaide|perth|launceston)\b",
                           re.IGNORECASE)

    def _check(c):
        try:
            if not c.is_displayed():
                return None
        except Exception:
            pass
        try:
            t = (c.text or "").strip()
        except Exception:
            return None
        if not t:
            return None
        cities = set(m.group(1).lower() for m in CITIES_RE.finditer(t))
        if len(cities) < 2:
            return None
        try:
            html = c.get_attribute("outerHTML") or ""
        except Exception:
            html = ""
        return (t, html)

    # 1. Scoped to the row's tooltip-wrapper first (fast path when
    #    the tooltip renders as a sibling of .stock-level-status).
    scope_sel = []
    if wrapper is not None:
        scope_sel.append((wrapper, "div"))
    scope_sel += [
        (row, ".tooltip"),
        (row, "[role='tooltip']"),
        (row, "[class*='tooltip']"),
        (row, "[class*='popover']"),
        (row, "[class*='popper']"),
    ]
    for scope, sel in scope_sel:
        try:
            cands = scope.find_elements(By.CSS_SELECTOR, sel)
        except Exception:
            cands = []
        for c in cands:
            r = _check(c)
            if r is not None:
                return r

    # 2. Document-wide fallback.  Scan by selector first, then do an
    #    XPath contains() sweep as the last-resort catchall.
    for sel in ("[role='tooltip']",
                "[class*='tooltip-content']",
                "[class*='stock-breakdown']",
                "[class*='stock-popup']",
                ".tooltip",
                ".popover",
                ".popper",
                "div[class*='popup']"):
        try:
            cands = driver.find_elements(By.CSS_SELECTOR, sel)
        except Exception:
            cands = []
        for c in cands:
            r = _check(c)
            if r is not None:
                return r

    # 3. XPath catchall: any element containing BOTH 'Sydney' and
    #    'Melbourne' as substrings in its text.  Expensive but only
    #    runs when nothing above matched.
    try:
        cands = driver.find_elements(
            By.XPATH,
            "//*[contains(., 'Sydney') and contains(., 'Melbourne') "
            "and contains(., 'Brisbane')]")
        # Prefer the SMALLEST such element (most-specific container).
        cands.sort(key=lambda e: len((e.text or "")))
        for c in cands:
            r = _check(c)
            if r is not None:
                return r
    except Exception:
        pass

    return ("", "")


def expand_stock(driver, row) -> dict:
    """Trigger a hover on the '.stock-level-status' pill inside `row`
    and read the resulting tooltip.  Returns per-warehouse fields
    plus a 'stock_total' sum.  First call that lands on an empty
    tooltip dumps the whole .tooltip-wrapper outerHTML to
    yhi_tyres_stock_popup.html so we can diagnose any remaining
    mismatch."""
    blank = {c: "" for c in KNOWN_CITIES}
    blank.update({
        "on_order": "", "total_stock": "",
        "eta_syd": "", "eta_mel": "", "eta_bri": "", "eta_per": "",
    })

    # Anchor: the stock pill itself.
    try:
        anchor = row.find_element(By.CSS_SELECTOR, ".stock-level-status")
    except Exception:
        return blank
    try:
        wrapper = row.find_element(By.CSS_SELECTOR, ".tooltip-wrapper")
    except Exception:
        wrapper = None

    # STEP 1: fully dismiss the PREVIOUS row's tooltip before hovering
    # the new pill.  Doing it here (rather than at the end of the
    # previous call) means the last event the YHI component sees is
    # the dismiss, immediately before our new mouseenter arrives.
    try:
        if _LAST_ANCHOR[0] is not None:
            driver.execute_script(_JS_DISMISS_LAST, _LAST_ANCHOR[0])
            # Give the component's teardown handler time to unmount
            # the stale tooltip before the new hover fires.
            time.sleep(0.5)
    except Exception:
        pass

    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});",
                              anchor)
    except Exception:
        pass

    # Nudge the cursor to a far-off neutral spot FIRST so we start
    # from a known no-hover state, then do the real hover.
    try:
        body = driver.find_element(By.TAG_NAME, "body")
        ActionChains(driver).move_to_element_with_offset(body, 5, 5).perform()
        time.sleep(0.15)
    except Exception:
        pass

    # Fire a synthetic mouseover sequence via JS (the YHI hover
    # listeners sometimes miss Selenium-generated native events).
    try:
        driver.execute_script(_JS_HOVER, anchor)
    except Exception:
        pass
    # Also follow up with a physical hover for engines where JS events
    # alone don't flip React's hover state.
    try:
        ActionChains(driver).move_to_element(anchor).perform()
    except Exception:
        pass
    # Record this anchor so the NEXT call can dismiss its tooltip.
    _LAST_ANCHOR[0] = anchor

    # Poll for the tooltip for up to 3s.  Vue / React tooltip
    # components usually mount+render within ~200ms, but the FIRST
    # hover on a freshly-loaded search result sometimes takes a bit
    # longer because the component is lazy-loaded.  We watch the
    # whole document for any element whose text contains BOTH a
    # known city AND the word 'Launceston' or 'Perth' or 'Melbourne'
    # (two cities = guaranteed to be the warehouse tooltip, not some
    # unrelated 'Sydney' text elsewhere on the page).
    popup_text = ""
    popup_html = ""
    deadline = time.time() + 3.0
    while time.time() < deadline:
        popup_text, popup_html = _scan_for_tooltip(driver, row, wrapper)
        if popup_text:
            break
        time.sleep(0.2)

    # Diagnostic -- always runs on the FIRST row.  Writes a dump
    # containing: (a) the row's outerHTML, (b) the row's
    # .tooltip-wrapper outerHTML, (c) ALL potential tooltip
    # candidates document-wide at the moment of the hover, (d) the
    # text our scanner actually captured (if any), and (e) any
    # element on the page whose text contains at least 3 city names.
    # Enough signal to pin the extraction in one more commit.
    if not _TOOLTIP_DUMPED[0]:
        _TOOLTIP_DUMPED[0] = True
        try:
            dump_path = os.path.join(BASE_DIR, "yhi_tyres_stock_popup.html")
            parts = []
            parts.append(f"<!-- scanner returned popup_text (len={len(popup_text)}): -->")
            parts.append(f"<!--\n{popup_text[:1500]}\n-->")
            try:
                parts.append("\n<!-- row outerHTML (first 3000 chars) -->")
                row_html = row.get_attribute("outerHTML") or ""
                parts.append(row_html[:3000])
            except Exception:
                pass
            if wrapper is not None:
                try:
                    parts.append("\n<!-- row.tooltip-wrapper outerHTML -->")
                    parts.append(wrapper.get_attribute("outerHTML") or "")
                except Exception:
                    pass
            # Document-wide candidates.
            try:
                body = driver.find_element(By.TAG_NAME, "body")
                for sel in ("[role='tooltip']", "[class*='tooltip']",
                            "[class*='popover']", "[class*='popper']",
                            "[class*='stock-popup']",
                            "[class*='stock-breakdown']"):
                    for el in body.find_elements(By.CSS_SELECTOR, sel):
                        try:
                            parts.append(f"\n<!-- body overlay sel='{sel}' -->")
                            parts.append(el.get_attribute("outerHTML") or "")
                        except Exception:
                            continue
            except Exception:
                pass
            # XPath-based "contains 3 cities" probe so we see which
            # containers ACTUALLY carry the breakdown text, if any.
            try:
                xp = ("//*[contains(., 'Sydney') and contains(., 'Melbourne') "
                      "and contains(., 'Brisbane')]")
                xcands = driver.find_elements(By.XPATH, xp)
                parts.append(f"\n<!-- XPath 3-city candidates: {len(xcands)} -->")
                for i, el in enumerate(xcands[:5]):
                    parts.append(f"\n<!-- xcand #{i} tag={el.tag_name} "
                                 f"class={el.get_attribute('class')!r} "
                                 f"text={(el.text or '')[:200]!r} -->")
                    try:
                        parts.append(el.get_attribute("outerHTML")[:1500] or "")
                    except Exception:
                        pass
            except Exception as _e:
                parts.append(f"\n<!-- XPath probe error: {_e} -->")
            with open(dump_path, "w", encoding="utf-8") as f:
                f.write("\n".join(parts))
            if popup_text:
                _log(f"[tooltip-debug] captured tooltip (len={len(popup_text)}); "
                     f"dumped to {dump_path}")
            else:
                _log(f"[tooltip-debug] NO breakdown found; dumped candidates "
                     f"to {dump_path}")
        except Exception as e:
            _log(f"[tooltip-debug] dump failed: {e}")

    # Dismiss the tooltip so the NEXT row's hover does not stack on
    # top.  Fires explicit mouseout + mouseleave events on the anchor
    # and bubbles them up to the tooltip-wrapper, which is what the
    # YHI component listens on.  Belt-and-braces: also push the
    # cursor to screen (0,0) via ActionChains so a race on the JS
    # path doesn't leave the pill in a lingering hover state.
    try:
        driver.execute_script(_JS_UNHOVER, anchor)
    except Exception:
        pass
    try:
        # Click a neutral spot (the body) to flush any stuck hover.
        body = driver.find_element(By.TAG_NAME, "body")
        ActionChains(driver).move_to_element_with_offset(body, 5, 5).perform()
    except Exception:
        pass
    time.sleep(0.3)

    if not popup_text:
        return blank
    parsed = _parse_stock_popup(popup_text)
    # Compute total stock as the sum of ALL numeric warehouse figures
    # (whichever cities were listed), excluding On Order (which is
    # incoming, not current).
    try:
        total = 0
        for k, v in list(parsed.items()):
            if k in ("on_order",) or k.startswith("eta_"):
                continue
            if isinstance(v, str) and v.isdigit():
                total += int(v)
        parsed["total_stock"] = str(total) if total else ""
    except Exception:
        parsed["total_stock"] = ""
    # Include other blank keys to match CsvSink.HEAD.
    for k, v in blank.items():
        parsed.setdefault(k, v)
    return parsed


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
            parsed = _extract_row_dom(row)
        except Exception as e:
            _log(f"[row] DOM extract failed: {e}")
            continue
        # Expand the stock tooltip only when there IS a local or
        # interstate figure to drill into.  'Supplier Stock' rows
        # have no per-warehouse popup to expand.
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
            "status_tag",                            # NEW / SALE / ...
            "brand", "pattern",
            "attribute_desc", "attr_1", "attr_2", "attr_3",
            "si_lr",
            "price",
            "stock_local", "stock_interstate", "stock_supplier",
            "sydney", "melbourne", "brisbane",
            "adelaide", "perth", "launceston",
            "total_stock",                           # sum of per-city stock
            "on_order",
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
