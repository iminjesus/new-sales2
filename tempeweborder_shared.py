"""
tempeweborder_shared.py - canonical helpers for SIZE / DESCRIPTION
normalisation used by every Tempeweborder script in the repo.

Three scripts need the same logic:
  - tempeweborder_crawling.py        (search-by-size crawl)
  - tempeweborder_crawling_extend.py (brand-sweep crawl, extracts SIZE
    from description text)
  - consolidate_tempeweborder.py     (rollup + repair of existing CSVs)

Keeping the regex + helpers in ONE module avoids drift where one script
accepts a shape (e.g. 35x12.50R17LT) and another doesn't.  Any time a
new tyre-size shape shows up in the source feed, add it here once.

Patterns covered (upper-case the match before storing):
  * Metric:         235/60R15, 315/70R17, 275/55R20
  * Metric + load:  11R22.5, 11R22.5C
  * P-metric:       P235/60R15 (passenger)
  * LT-metric:      LT285/75R16 (light truck)
  * Flotation:      35x12.50R17LT, 31X10.50R15, 35x12.5R17
  * NHS flotation:  28X10.00R14 NHS (NHS stays on the description;
    the SIZE is the "28X10.00R14" head)
"""
from __future__ import annotations

import re
from typing import Optional

# Order of alternations matters: the P / LT prefix variants come first
# so a leading "LT285" doesn't match the plain-metric branch and drop
# the LT marker; same for flotation (NxN.NRN) before plain metric.
TYRE_SIZE_RX = re.compile(
    r"\b("
    r"(?:LT|P)?\d{2,3}/\d{2}R\d{1,2}(?:\.\d)?C?(?:LT)?"      # P235/60R15 LT285/75R16 265/70R19.5
    r"|\d{2,3}[Xx]\d{1,2}(?:\.\d{1,2})?R\d{1,2}(?:LT)?"      # 35x12.50R17LT  31X10.50R15
    r"|\d{2,3}R\d{1,2}(?:\.\d)?C?"                            # 185R14  11R22.5  11R22.5C
    r")\b",
    re.IGNORECASE,
)


def extract_size_from_desc(desc: str) -> str:
    """Return the first tyre-size token found in the description text
    (upper-cased), or "" if no recognised shape is present.  Earliest
    match wins so a description that happens to carry a trailing
    numeric token (e.g. "121Q MUD TERRAIN") doesn't beat the real
    size at the head."""
    m = TYRE_SIZE_RX.search(str(desc or ""))
    return m.group(1).upper() if m else ""


# Trailing "^^^" marker is a known source artifact - strip it off the
# DISPLAY description during repair so the output file reads cleanly.
_TRAILING_HATS_RX = re.compile(r"\^+\s*$")
# Leading brand prefix stripper is handled in the crawler scripts
# themselves - this module only touches size + whitespace cleanup, so
# a brand name embedded in the description doesn't surprise the
# downstream consumers that already strip it.
_LEADING_SIZE_RX = re.compile(
    r"^\s*(?:LT|P)?\d{2,3}(?:/\d{2}R\d{1,2}(?:\.\d)?C?(?:LT)?"
    r"|[Xx]\d{1,2}(?:\.\d{1,2})?R\d{1,2}(?:LT)?"
    r"|R\d{1,2}(?:\.\d)?C?)\s*",
    re.IGNORECASE,
)


def strip_size_prefix(desc: str) -> str:
    """Strip a leading tyre-size token from the description so a row
    that stored SIZE separately doesn't carry the same size at the
    head of the DESCRIPTION cell.  Also handles the standalone-flotation
    case where SIZE_RE skipped the leading two-digit width."""
    s = str(desc or "")
    # Full-pattern head strip (handles 35x12.50R17LT, 28X10.00R14,
    # P235/60R15, LT285/75R16, 235/60R15, 11R22.5).
    m = TYRE_SIZE_RX.match(s.strip())
    if m and (s.strip().startswith(m.group(1))
              or s.strip().upper().startswith(m.group(1).upper())):
        s = s.strip()[len(m.group(1)):].strip()
    return s


def clean_description(desc: str, strip_leading_size: bool = True) -> str:
    """Normalise a DESCRIPTION cell for the OUTPUT (display) column:
    collapse runs of whitespace, strip trailing '^^^' markers, and
    optionally strip a leading SIZE token if the SIZE column carries
    it separately."""
    s = str(desc or "").strip()
    s = _TRAILING_HATS_RX.sub("", s).strip()
    if strip_leading_size:
        s = strip_size_prefix(s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def norm_desc_for_dedup(desc: str) -> str:
    """Normalise a DESCRIPTION cell for the DEDUP KEY: whitespace-
    collapsed, trailing '^^^' stripped, upper-cased.  Used to key
    the per-SKU dict so two rows that differ only in whitespace /
    case / stray markers collapse to one slot."""
    s = str(desc or "").strip()
    s = _TRAILING_HATS_RX.sub("", s).strip()
    s = re.sub(r"\s+", " ", s)
    return s.upper()


def make_sku_key(size: str, brand: str, desc: str) -> tuple[str, str, str]:
    """Canonical (SIZE_UP, BRAND_UP, DESC_NORM) tuple.  Every writer /
    reader that needs to dedupe rows must key through this helper so
    the exact-same normalisation applies everywhere."""
    return (
        str(size  or "").strip().upper(),
        str(brand or "").strip().upper(),
        norm_desc_for_dedup(desc),
    )


def recover_size_if_blank(size: str, desc: str) -> tuple[str, bool]:
    """If `size` is empty, try to pull one from `desc`.  Returns the
    final (size, recovered?) pair - the second element is True when
    the size was recovered from the description, False otherwise.
    Callers can tally the True count for a progress line."""
    s = str(size or "").strip()
    if s:
        return s.upper(), False
    recovered = extract_size_from_desc(desc)
    if recovered:
        return recovered, True
    return "", False


# ─── Historical size collector (Oct-30) ──────────────────────────────────
def size_to_search_digits(size: str) -> str:
    """Convert a canonical tyre-size string into the digits-only form
    the Tempe-style WebOrder search box accepts.

    Examples:
      '175/65R14'  -> '1756514'
      '225/50R17'  -> '2255017'
      '185R14'     -> '18514'      (no aspect)
      '11R22.5'    -> '11225'      (dot stripped; truck sizes often
                                     search as digits-only)
      'LT245/70R16'-> '2457016'    (LT prefix stripped so the digit
                                     string matches the normal-metric
                                     search box format)
      '35x12.50R17LT' -> '351250'  (flotation shapes - best-effort;
                                     some sites don't support them)
    """
    import re as _re
    s = str(size or "").upper()
    # Strip "LT"/"P" prefix (they're tyre-type markers, not part of the
    # dimensional query the search box expects).
    s = _re.sub(r"^(?:LT|P)", "", s.strip())
    # Keep only digits.
    digits = _re.sub(r"\D", "", s)
    return digits


def collect_historical_sizes(directory: str,
                              days_back: int = 365,
                              fname_rx: Optional[str] = None,
                              min_count: int = 1) -> list[tuple[str, int]]:
    """Scan a directory for Tempeweborder-style CSVs modified in the
    last `days_back` days, aggregate every SIZE value seen, and return
    a list of (size, occurrence_count) tuples sorted by count DESC
    then size ASC.

    Args:
      directory  : path to scan (non-recursive by default)
      days_back  : only consider files whose mtime is within this many
                   days of now.  0 or negative -> no time cutoff.
      fname_rx   : optional regex to filter which CSV filenames count
                   (default: Tempeweborder_*.csv AND NewbeeWeborder_*.csv).
      min_count  : drop sizes seen fewer than this many times across
                   the scanned files (default 1 -> keep everything).

    The caller usually feeds the returned sizes into a crawler's search
    loop so no size we've ever operated on gets missed on the next
    pass."""
    import csv as _csv
    import os as _os
    import re as _re
    import time as _time
    cutoff = 0.0
    if days_back and days_back > 0:
        cutoff = _time.time() - days_back * 86400
    if fname_rx is None:
        # Default: all Tempeweborder_* and NewbeeWeborder_* csvs,
        # whether they live in project root or any subfolder the
        # caller walked.  Case-insensitive.
        name_rx = _re.compile(
            r"^(?:tempeweborder|newbeeweborder)(?:_extend)?_"
            r"\d{8}_\d{4}\.csv$",
            _re.IGNORECASE,
        )
    else:
        name_rx = _re.compile(fname_rx, _re.IGNORECASE)
    counts: dict[str, int] = {}
    files_seen = 0
    try:
        entries = _os.listdir(directory)
    except OSError:
        return []
    for name in entries:
        if not name_rx.match(name):
            continue
        full = _os.path.join(directory, name)
        try:
            mt = _os.path.getmtime(full)
        except OSError:
            continue
        if cutoff and mt < cutoff:
            continue
        files_seen += 1
        try:
            with open(full, "r", encoding="utf-8-sig", newline="") as f:
                r = _csv.DictReader(f)
                # Case-insensitive SIZE column lookup.
                size_col = None
                for h in (r.fieldnames or []):
                    if (h or "").strip().lower() == "size":
                        size_col = h; break
                if not size_col:
                    continue
                for row in r:
                    sz = str(row.get(size_col) or "").strip().upper()
                    # Recover size from description when SIZE blank.
                    if not sz:
                        _desc = str(row.get("DESCRIPTION")
                                    or row.get("description") or "")
                        sz = extract_size_from_desc(_desc)
                    if sz:
                        counts[sz] = counts.get(sz, 0) + 1
        except Exception:
            continue
    if min_count > 1:
        counts = {k: v for k, v in counts.items() if v >= min_count}
    # Sort by count DESC, then size ASC so the most common sizes
    # crawl first (useful when the budget is capped).
    return sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))


# ──────────────────────────────────────────────────────────────────────
# Nov-02 user ask: per-warehouse stock extraction.
#
# Tempe (Brisbane + Sydney) and Newbee (Sydney + Melbourne) use the
# same Angular layout with four stock-badge variants:
#
#   "N+ More Available"      (green pill)   -> local + other BOTH have
#                                              stock; a click opens a
#                                              modal with per-warehouse
#                                              qty, which we scrape and
#                                              return verbatim.
#   "N+ At Other Locations"  (yellow pill)  -> ONLY the sister branch
#                                              has stock, labelled qty
#                                              N; local = 0.
#   plain number ("8+" / "2") (dark green)  -> ONLY the local branch
#                                              has stock, qty N;
#                                              sister = 0.
#   "Call for Availability"  (red pill)     -> both branches 0.
#
# The caller passes (wh_local, wh_other) names so the same helper
# works for Tempe ("Brisbane", "Sydney") and Newbee ("Sydney",
# "Melbourne").  Returns (qty_local, qty_other) as strings - "N+"
# suffix preserved verbatim so the dashboard can render "exactly N"
# vs "N or more".
# ──────────────────────────────────────────────────────────────────────


def _clean(s):
    return (s or "").strip()


def _close_tempe_modal(driver, pause=0.3):
    """Best-effort close of the per-product details modal.  Try the
    close button first; fall back to the ESC key.  Short pause after
    so the next row interaction sees a settled DOM."""
    import time
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.common.action_chains import ActionChains
    for xp in [
        "//button[contains(@class,'close') and (normalize-space()='x'"
        " or normalize-space()='X' or normalize-space()='×'"
        " or @aria-label='Close')]",
        "//button[@aria-label='Close']",
        "//*[self::button or self::a][contains(@class,'close')]"
        "[not(ancestor::*[contains(@style,'display: none')])]",
        "//i[contains(@class,'glyphicon-remove') or contains(@class,'fa-times')]",
        "//button[normalize-space()='×']",
    ]:
        try:
            btns = driver.find_elements(By.XPATH, xp)
            for b in btns:
                try:
                    if b.is_displayed():
                        try:
                            driver.execute_script("arguments[0].click();", b)
                        except Exception:
                            b.click()
                        time.sleep(pause)
                        return True
                except Exception:
                    continue
        except Exception:
            continue
    # Fallback: send ESC on the page body
    try:
        ActionChains(driver).send_keys(Keys.ESCAPE).perform()
        time.sleep(pause)
        return True
    except Exception:
        pass
    return False


def _read_modal_warehouse_qty(driver, wh_names, timeout=5.0):
    """Wait for the per-product details modal to render, then read qty
    for each name in `wh_names`.  Qty is kept verbatim with the "+"
    suffix.  "Call" (seen when one branch is 0 but the other has
    stock) maps to "0".  Returns a dict {name: qty_str}; missing
    names map to "".

    Scoped to text matches that start with "Tempe Tyres <name>:" (the
    Angular modal's wording) so the digit buttons in the modal's
    number pad do NOT get mistaken for warehouse qty.
    """
    import re
    import time
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    out = {w: "" for w in wh_names}
    deadline = time.monotonic() + float(timeout)
    first_name = wh_names[0]
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((
                By.XPATH,
                "//*[contains(normalize-space(.),'Location Availability')"
                f" or contains(normalize-space(.),'{first_name}')]"
            ))
        )
    except Exception:
        return out
    # Walk rows anchored on "<name>:" so we skip the modal's digit
    # keypad (0-9 buttons) and left-side On Hand / Ordered boxes.
    for w in wh_names:
        qty = ""
        for xp in [
            # Table-row layout: <tr> contains "<wh>" and a <td> qty
            f"//tr[contains(normalize-space(.),'{w}:') or contains("
            f"normalize-space(.),'{w} ')]/td[last()]",
            # Flex-row layout: find anything containing "<wh>:", get its
            # following sibling with qty-like text
            f"//*[contains(normalize-space(.),'{w}:')]/following-sibling::*[1]",
            # Grandparent → nearest text child that is qty-like
            f"//*[contains(normalize-space(.),'{w}:')]/..",
        ]:
            if time.monotonic() > deadline:
                break
            try:
                els = driver.find_elements(By.XPATH, xp)
            except Exception:
                continue
            for el in els:
                try:
                    t = _clean(el.text or "")
                except Exception:
                    continue
                if not t:
                    continue
                # Pure qty cell
                m = re.fullmatch(r"\d{1,5}\+?", t)
                if m:
                    qty = t
                    break
                if re.fullmatch(r"call", t, re.IGNORECASE):
                    qty = "0"
                    break
                # "<wh>: N+" or "<wh>: Call" blob
                m2 = re.search(
                    rf"{re.escape(w)}\s*:?\s*(\d{{1,5}}\+?|call)\b",
                    t, re.IGNORECASE,
                )
                if m2:
                    g = m2.group(1)
                    qty = "0" if g.lower() == "call" else g
                    break
            if qty:
                break
        out[w] = qty
    return out


def extract_warehouse_stock(driver, row, wh_local, wh_other, allow_modal=True):
    """Return (qty_local, qty_other) for a single search-result `row`.

    The caller passes the warehouse names in site-local order:
      Tempe  -> wh_local = "Brisbane", wh_other = "Sydney"
      Newbee -> wh_local = "Sydney",   wh_other = "Melbourne"

    Button-to-mapping rules (user ask Nov-02):
      "Call for Availability"       -> ("0",  "0")
      "N+ At Other Locations"       -> ("0",  N )
      "N+ More Available"           -> click modal -> read both
      plain "N+" / "N" badge        -> ( N ,  "0")
      no badge found                -> ("0",  "0")

    `allow_modal=False` disables the click fallback -- useful for a
    fast dry-run; "More Available" rows then record qty as the label
    N on BOTH warehouses (best guess without the modal).
    """
    import re
    from selenium.webdriver.common.by import By
    try:
        full = _clean(row.text or "")
    except Exception:
        full = ""
    full_lower = full.lower()

    # 1. OOS -> both 0
    if re.search(r"\bcall\s+for\s+avail(?:ability)?\b", full_lower):
        return ("0", "0")

    # 2. At Other Locations -> local = 0, other = N (from label)
    m_other = re.search(
        r"(\d{1,5}\+?)\s+(?:at\s+)?other\s+location", full, re.IGNORECASE,
    )
    if m_other:
        return ("0", m_other.group(1))

    # 3. More Available -> click modal if allowed
    m_more = re.search(
        r"(\d{1,5}\+?)\s+more\s+avail(?:able)?", full, re.IGNORECASE,
    )
    if m_more:
        label_n = m_more.group(1)
        if not allow_modal:
            # Fast-path fallback when modal is disabled: assume both
            # branches have `label_n` (which is what the button says
            # anyway).
            return (label_n, label_n)
        # Find the clickable badge element.  Prefer the deepest
        # element whose text contains "More Available" so we click
        # the actual pill not a wrapper div.
        btn = None
        for xp in [
            ".//*[contains(normalize-space(.),'More Available')]"
            "[not(descendant::*[contains(normalize-space(.),'More Available')])]",
            ".//button[contains(normalize-space(.),'More Available')]",
            ".//a[contains(normalize-space(.),'More Available')]",
        ]:
            try:
                cands = row.find_elements(By.XPATH, xp)
                for c in cands:
                    try:
                        if c.is_displayed():
                            btn = c
                            break
                    except Exception:
                        continue
                if btn is not None:
                    break
            except Exception:
                continue
        if btn is None:
            # Click target not found; fall back to label-only.
            return (label_n, label_n)
        try:
            driver.execute_script("arguments[0].click();", btn)
        except Exception:
            try:
                btn.click()
            except Exception:
                return (label_n, label_n)
        qmap = _read_modal_warehouse_qty(driver, [wh_local, wh_other])
        _close_tempe_modal(driver)
        q_local = qmap.get(wh_local) or ""
        q_other = qmap.get(wh_other) or ""
        # Normalise "" to "0" so the CSV is unambiguous.
        return (q_local or "0", q_other or "0")

    # 4. Plain number badge (local only) — look inside the row for a
    #    SHORT badge/pill element whose text is pure "N" or "N+".
    for xp in [
        ".//*[self::span or self::div or self::button]"
        "[normalize-space()!=''][string-length(normalize-space())<=4]"
        "[contains(translate(@class,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'badge')"
        " or contains(translate(@class,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'pill')"
        " or contains(translate(@class,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'stock')"
        " or contains(translate(@class,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'onhand')"
        " or contains(translate(@class,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'on-hand')"
        " or contains(translate(@class,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'green')"
        " or contains(translate(@class,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'success')"
        "]",
    ]:
        try:
            els = row.find_elements(By.XPATH, xp)
        except Exception:
            continue
        for el in els:
            try:
                t = _clean(el.text or "")
            except Exception:
                continue
            m = re.fullmatch(r"\d{1,5}\+?", t)
            if m:
                return (t, "0")

    # 5. Nothing matched — treat as unknown / 0.
    return ("0", "0")
