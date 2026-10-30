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
