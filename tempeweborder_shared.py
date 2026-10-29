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
