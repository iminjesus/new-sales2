"""
tempe_price_lookup.py — look up Tempe dealer / consumer prices for an SPRF
material line, with a size → size+pattern → size+pattern+load/speed
match hierarchy so the Remark cell on the Orders form can show a
"what competitors charge" reference.

Reads the LATEST file per prefix in _BASE (same files the Price
dashboard reads):
  • Tempeweborder_YYYYMMDD_HHMM.csv  →  DEALER supply price + stock
  • Tempe_YYYYMMDD_HHMM.csv          →  Tempe RETAIL / consumer price

Public:
  lookup_tempe_prices(size, pattern="", load_speed="")
        → {"dealer":   <match> | None,
           "consumer": <match> | None,
           "sources":  {"dealer_file": str, "consumer_file": str}}

Where <match> is a dict:
  {
    "price":           "620.00",
    "stock":           "12"   (dealer only — Tempeweborder new STOCK col),
    "description":    "SmartFlex AH35",
    "brand":          "HK",
    "size_seen":      "11R22.5",
    "load_speed_seen":"148L",
    "match":          "exact" | "same_pattern" | "size_only",
    "mismatch":       ""  | "load/speed: 148L (yours: 150K)" | "pattern: H502 (yours: AH35)",
    "source":         "dealer" | "consumer",
    "source_file":    "Tempeweborder_20261013_1105.csv",
  }

Match hierarchy (picks the FIRST that fires):
  1. SIZE + PATTERN + LOAD/SPEED all match       →  match="exact"
  2. SIZE + PATTERN match, load/speed differs    →  match="same_pattern",
                                                    mismatch spells out
                                                    the load/speed we saw
  3. SIZE only                                   →  match="size_only",
                                                    mismatch spells out
                                                    the pattern + load/speed
                                                    we saw

Cached per-file for 5 minutes so repeated material picks on the same
customer don't re-read a 2 MB CSV every time.
"""
from __future__ import annotations
import csv
import os
import re
import time

_BASE = os.path.dirname(os.path.abspath(__file__))

_CACHE: dict = {}        # {path: (mtime, loaded_at, rows)}
_CACHE_TTL_SEC = 300


def _latest_file(prefix: str) -> str | None:
    """Case-insensitive scan of _BASE for the newest <prefix>_*.csv."""
    pl = prefix.lower()
    hits = []
    try:
        for name in os.listdir(_BASE):
            nl = name.lower()
            if nl.startswith(pl + "_") and nl.endswith(".csv"):
                hits.append(os.path.join(_BASE, name))
    except OSError:
        return None
    if not hits:
        return None
    return max(hits, key=os.path.getmtime)


def _load(path: str):
    """Open the CSV with the same encoding fallback as retrofit_price_
    csv_sizes.py — a mix of UTF-8 and cp1252 lives on disk."""
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            with open(path, "r", encoding=enc, newline="") as f:
                return list(csv.DictReader(f))
        except (UnicodeDecodeError, UnicodeError):
            continue
    try:
        with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []


def _rows_for(prefix: str):
    f = _latest_file(prefix)
    if not f:
        return [], None
    try:
        mtime = os.path.getmtime(f)
    except OSError:
        mtime = 0
    now = time.time()
    cached = _CACHE.get(f)
    if cached and cached[0] == mtime and (now - cached[1]) < _CACHE_TTL_SEC:
        return cached[2], f
    rows = _load(f)
    _CACHE[f] = (mtime, now, rows)
    return rows, f


_NOISE_RE = re.compile(r"[\s/,.\-'&+()]+")
def _nrm(s: str) -> str:
    return _NOISE_RE.sub("", (s or "").upper()).replace("ZR", "R")


_LOAD_SPEED_RE = re.compile(
    r"\b((?:[6-9]\d|1\d{2})(?:/\d{2,3})?(?:ZR|[BCDEFGHJKLMNPQRSTUVWY]))\b",
    re.IGNORECASE,
)
def _extract_load_speed(text: str) -> str:
    m = _LOAD_SPEED_RE.search((text or "").upper())
    return m.group(1).upper() if m else ""


def _pick_col(row: dict, *names: str) -> str:
    for n in names:
        if n in row and row[n] is not None:
            v = str(row[n]).strip()
            if v: return v
        # case-insensitive fallback
        for k in row.keys():
            if k.strip().lower() == n.lower():
                v = str(row[k] or "").strip()
                if v: return v
    return ""


def _best_match(rows, ns_size: str, np_pattern: str, ul_load: str, *,
                price_cols: tuple, stock_col: str | None,
                source_label: str, source_file: str | None):
    """Walk `rows` once, picking the FIRST row that satisfies each
    tier of the match hierarchy.  Returns the best-tier hit as a
    serialisable dict, or None when no row carries the same size."""
    exact = same_pattern = size_only = None
    for r in rows:
        size_raw = _pick_col(r, "SIZE", "size")
        if _nrm(size_raw) != ns_size:
            continue
        desc = _pick_col(r, "DESCRIPTION", "description", "title")
        brand = _pick_col(r, "brand", "BRAND")
        rl = _extract_load_speed(desc)
        rpat_norm = _nrm(desc)
        price = ""
        for pc in price_cols:
            price = _pick_col(r, pc)
            if price: break
        stock = _pick_col(r, stock_col) if stock_col else ""
        has_pattern = bool(np_pattern) and np_pattern in rpat_norm
        has_load    = bool(ul_load) and rl.upper() == ul_load.upper()
        entry = {
            "price":            price,
            "stock":            stock,
            "description":      desc,
            "brand":            brand,
            "size_seen":        size_raw,
            "load_speed_seen":  rl,
            "source":           source_label,
            "source_file":      os.path.basename(source_file or ""),
        }
        if has_pattern and has_load and not exact:
            entry["match"] = "exact"
            entry["mismatch"] = ""
            exact = entry
        elif has_pattern and not same_pattern:
            entry["match"] = "same_pattern"
            entry["mismatch"] = (
                f"load/speed: {rl or '—'} (yours: {ul_load or '—'})"
            ) if ul_load or rl else ""
            same_pattern = entry
        elif (not np_pattern or not same_pattern) and not size_only:
            # Record the size-only hit if we haven't got anything
            # better yet.  Don't overwrite a same-pattern hit found
            # earlier.
            entry["match"] = "size_only"
            _bits = []
            if np_pattern and not has_pattern:
                _bits.append(f"pattern mismatch")
            if ul_load and not has_load:
                _bits.append(f"load/speed: {rl or '—'} (yours: {ul_load})")
            entry["mismatch"] = " · ".join(_bits)
            size_only = entry
    return exact or same_pattern or size_only


def lookup_tempe_prices(size: str, pattern: str = "", load_speed: str = ""):
    """Primary entry.  Returns {dealer, consumer, sources}."""
    ns = _nrm(size)
    if not ns:
        return {"dealer": None, "consumer": None,
                "sources": {"dealer_file": "", "consumer_file": ""}}
    np = _nrm(pattern) if pattern else ""
    ul = (load_speed or "").upper().replace(" ", "")
    dealer_rows, dealer_file = _rows_for("Tempeweborder")
    consumer_rows, consumer_file = _rows_for("Tempe")
    dealer = _best_match(
        dealer_rows, ns, np, ul,
        price_cols=("PRICE", "price", "COST", "cost"),
        stock_col="STOCK",
        source_label="dealer",
        source_file=dealer_file,
    )
    consumer = _best_match(
        consumer_rows, ns, np, ul,
        price_cols=("price", "disc_price", "PRICE", "DISC_PRICE"),
        stock_col=None,
        source_label="consumer",
        source_file=consumer_file,
    )
    return {
        "dealer":   dealer,
        "consumer": consumer,
        "sources": {
            "dealer_file":   os.path.basename(dealer_file or ""),
            "consumer_file": os.path.basename(consumer_file or ""),
        },
    }
