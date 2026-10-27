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


# Oct-23 user ask: when the current-month CSV doesn't carry the size
# the SPRF asked about, walk backwards month-by-month until a hit is
# found; chip label spells out the fallback month ("'26.9" style).
_MONTH_ABBR = {
    'JAN':'01','JANUARY':'01','FEB':'02','FEBRUARY':'02','MAR':'03','MARCH':'03',
    'APR':'04','APRIL':'04','MAY':'05','JUN':'06','JUNE':'06',
    'JUL':'07','JULY':'07','AUG':'08','AUGUST':'08','SEP':'09','SEPTEMBER':'09',
    'OCT':'10','OCTOBER':'10','NOV':'11','NOVEMBER':'11','DEC':'12','DECEMBER':'12',
}
def _month_key_from_name(name: str) -> str:
    """Return YYYYMM extracted from a crawler-output filename.  Covers
    the two shapes in production: timestamped (Tempe_20260401_1439.csv)
    and month-named (Tempe_Apr_2026.csv)."""
    up = name.upper()
    m = re.search(r"(\d{4})(\d{2})\d{2}", up)
    if m:
        return f"{m.group(1)}{m.group(2)}"
    yr = re.search(r"(\d{4})", up)
    if yr:
        yyyy = yr.group(1)
        best_len, best = 0, None
        for key, num in _MONTH_ABBR.items():
            if key in up and len(key) > best_len:
                best_len, best = len(key), num
        if best: return f"{yyyy}{best}"
    return "000000"


def month_label(yyyymm: str) -> str:
    """YYYYMM → "'26.9" compact label (same style the price dashboard
    uses).  Blank for the "000000" sentinel."""
    s = str(yyyymm or "")
    if len(s) != 6 or not s.isdigit():
        return ""
    yy = int(s[2:4])
    mm = int(s[4:6])
    return f"'{yy:02d}.{mm}"


def _files_by_month(prefix: str):
    """Return [(yyyymm, path), ...] sorted newest-month-first.  When
    multiple files share a month-key, keep the one with the newest
    mtime (crawler re-runs within the same month)."""
    pl = prefix.lower()
    by_month: dict = {}
    try:
        for name in os.listdir(_BASE):
            nl = name.lower()
            if not (nl.startswith(pl + "_") and nl.endswith(".csv")):
                continue
            fp = os.path.join(_BASE, name)
            mk = _month_key_from_name(name)
            if mk not in by_month or os.path.getmtime(fp) > os.path.getmtime(by_month[mk]):
                by_month[mk] = fp
    except OSError:
        return []
    return sorted(by_month.items(), key=lambda t: t[0], reverse=True)


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


# Oct-23 user ask: "195/70R14T" (trailing speed symbol), "195/70ZR14"
# (ZR high-speed form), and "195/70R14 82T" (load-index + speed
# symbol tail) all mean the SAME tyre size for comparison purposes.
# Extract just the canonical "<width>[/X]<aspect?><letter(s)><rim[.d]>C?"
# core so comparisons line up.
_SIZE_CORE_RE = re.compile(
    r"("
    r"\d{2,3}[/X]\d{1,2}(?:\.\d)?[A-Z]{0,2}\d{2}(?:\.\d)?C?"   # 195/70R14, 33X12.5R15
    r"|\d{2,3}[A-Z]{1,2}\d{2}(?:\.\d)?C?"                        # 11R22.5, 205R16
    r")"
)
def _canon_size(raw: str) -> str:
    """Extract the canonical size core — strips a trailing load / speed
    tail ("195/70R14T" → "195/70R14") and folds ZR to R.  When no
    canonical core is found, returns the raw string so an unexpected
    shape still gets a chance to match verbatim."""
    _up = (raw or "").upper().replace("ZR", "R").replace(" ", "")
    m = _SIZE_CORE_RE.search(_up)
    return m.group(1) if m else _up


# Oct-23 user ask: when the SPRF side holds a truncated size
# ("225/70R17" because carrying_26 lost its ".5"), the Tempe CSV
# still has the correct "225/70R17.5" — so the lookup must try the
# decimal variant too, otherwise an obvious match is missed.
# Mirror of KNOWN_DECIMAL_RIM_UPGRADES in retrofit_price_csv_sizes.py
# (kept in sync by hand — small list, rare to change).
_KNOWN_DECIMAL_UPGRADES = {
    "205/70R17", "205/75R17", "215/75R17", "225/70R17", "225/75R17",
    "235/75R17", "245/70R17", "265/70R17",
    "215/75R19", "225/70R19", "245/70R19", "265/70R19", "285/70R19", "305/70R19",
    "275/70R22", "275/80R22", "285/75R22", "295/60R22", "295/75R22",
    "295/80R22", "305/70R22", "315/60R22", "315/70R22", "315/80R22",
    "385/55R22", "385/65R22", "425/65R22", "445/50R22", "445/65R22",
    "275/80R24", "285/75R24", "315/80R24",
    "11R22", "11R24", "12R22", "12R24", "13R22",
}

def _size_variants(raw: str):
    """Return the set of normalised size strings worth trying.  Steps:
      1. Canonicalise the raw string — strips trailing load / speed
         ("195/70R14T" → "195/70R14") and folds ZR to R.
      2. Normalise that canonical form (strip punct, upper).
      3. If the canonical form is one of the known-truncated truck
         sizes (carrying_26 may still carry the integer-rim version
         while Tempe has the ".5"), also add the ".5" variant.
      4. Also keep the raw-normalised form in the set — a seller may
         list the size with a load/speed suffix that happens to match
         the SPRF side literally."""
    _canon = _canon_size(raw)
    out = {_nrm(_canon), _nrm(raw)}
    if _canon in _KNOWN_DECIMAL_UPGRADES:
        out.add(_nrm(_canon + ".5"))
    return {v for v in out if v}


def _brand_match(row_brand: str, caller_brand: str) -> bool:
    """Oct-23 user ask: scope Tempe matching to HK / LF when the SPRF
    material is HK / LF (user's own words: 'Hankook Brand 또는 Laufenn
    Brand만 확인하여 Mapping하면 되기에').  Returns True when the row
    carries the same manufacturer, with the common aliases:
      HK  ⟷ "Hankook"
      LF  ⟷ "Laufenn"
    `caller_brand` empty → everything matches (used for the fallback
    pass when the brand-scoped pass finds nothing)."""
    if not caller_brand:
        return True
    rb = (row_brand or "").strip().upper()
    cb = caller_brand.strip().upper()
    aliases = {
        "HK": {"HK", "HANKOOK"},
        "LF": {"LF", "LAUFENN"},
    }
    for k, al in aliases.items():
        if cb in al and rb in al:
            return True
    return rb == cb


_LOAD_SPEED_RE = re.compile(
    r"\b((?:[6-9]\d|1\d{2})(?:/\d{2,3})?(?:ZR|[BCDEFGHJKLMNPQRSTUVWY]))\b",
    re.IGNORECASE,
)
def _extract_load_speed(text: str) -> str:
    m = _LOAD_SPEED_RE.search((text or "").upper())
    return m.group(1).upper() if m else ""


_MONEY_RE = re.compile(r"^\$?\d+(?:,\d{3})*(?:\.\d+)?$")
def _is_money(s: str) -> bool:
    """True when `s` parses as a dollar amount (optional $ / thousands
    separators / decimals).  Used to reject stray non-numeric cells
    that landed in the COST column of the extend-variant CSV
    ("K435" is a pattern code, not a price)."""
    return bool(_MONEY_RE.match((s or "").strip()))


def _pick_col(row: dict, *names: str) -> str:
    for n in names:
        if n in row and row[n] is not None:
            v = str(row[n]).strip()
            if v: return v
        # case-insensitive fallback — skip the None key csv.DictReader
        # uses for overflow fields (row with more fields than header).
        for k in row.keys():
            if k is None: continue
            if k.strip().lower() == n.lower():
                v = str(row[k] or "").strip()
                if v: return v
    return ""


def _row_cells(row: dict):
    """Flatten every value in `row` to a list of trimmed strings —
    including the overflow list csv.DictReader stashes at row[None]
    when a row carries MORE fields than the header declares.  The
    Tempeweborder_extend_*.csv file is exactly that case: 6-field
    rows under a 5-field header, so the real PRICE (6th field) sits
    inside row[None] and would otherwise be invisible to the numeric
    fallback that picks the dealer sell-in / retail amounts."""
    cells = []
    for k, v in row.items():
        if v is None:
            continue
        if isinstance(v, list):
            cells.extend(str(x or "").strip() for x in v if x is not None)
        else:
            s = str(v).strip()
            if s: cells.append(s)
    return cells


def _best_match(rows, size_variants: set, np_pattern: str, ul_load: str,
                caller_brand: str, *,
                price_cols: tuple, cost_cols: tuple = (),
                stock_col: str | None,
                source_label: str, source_file: str | None):
    """Walk `rows` once, picking the FIRST row that satisfies each
    tier of the match hierarchy.  Returns the best-tier hit as a
    serialisable dict, or None when no row carries an acceptable size.

    size_variants : set of normalised size strings — any one of them
                    matching is enough (lets carrying_26's truncated
                    "225/70R17" still find Tempe's "225/70R17.5").
    caller_brand  : "" / "HK" / "LF" — restricts to Hankook / Laufenn
                    rows when non-empty.
    price_cols    : primary dollar field (Tempe = retail; Tempeweborder
                    = PRICE which is the dealer's recommended retail
                    quote through the web-order portal).
    cost_cols     : (Oct-23 user ask) secondary dollar field —
                    Tempeweborder's COST column, which is the dealer
                    SELL-IN price (what Tempe charges the dealer).
                    Returned alongside `price` so the Remark chip can
                    show BOTH figures.
    """
    exact = same_pattern = size_only = None
    for r in rows:
        size_raw = _pick_col(r, "SIZE", "size")
        # Oct-23: canonical size compare — covers "195/70R14T",
        # "195/70ZR14", "195/70R14 82T" etc. all mapping to the same
        # normalised form as "195/70R14".
        _row_size_variants = _size_variants(size_raw)
        if size_variants.isdisjoint(_row_size_variants):
            continue
        if not _brand_match(_pick_col(r, "brand", "BRAND"), caller_brand):
            continue
        desc = _pick_col(r, "DESCRIPTION", "description", "title")
        brand = _pick_col(r, "brand", "BRAND")
        # Oct-23 user ask: when the extend crawler misaligned the
        # columns (K435 landed in the COST slot instead of its own
        # PATTERN column), scanning only DESCRIPTION missed the
        # pattern code and misread K435 as a dollar amount.  Build
        # pattern-search text from ALL cell values + run the numeric
        # fallback across every cell too — robust against any column
        # drift in the extend / future crawler variants.
        _all_cells = _row_cells(r)
        _all_text  = " ".join(_all_cells)
        rl = _extract_load_speed(_all_text) or _extract_load_speed(desc)
        rpat_norm = _nrm(_all_text)
        price = ""
        for pc in price_cols:
            v = _pick_col(r, pc)
            if v and _is_money(v):
                price = v; break
        cost = ""
        for pc in cost_cols:
            v = _pick_col(r, pc)
            if v and _is_money(v):
                cost = v; break
        if not price or not cost:
            # Smart fallback — scan every cell for dollar-looking
            # values.  Smaller = COST (dealer sell-in), larger = PRICE
            # (dealer retail quote).  Covers extend-variant rows where
            # the pattern code landed in the COST slot and pushed the
            # real dollars one column over.
            _nums = []
            for v in _all_cells:
                if _is_money(v):
                    try: _nums.append((float(v.replace(",", "").replace("$", "")), v))
                    except Exception: pass
            if len(_nums) >= 2:
                _nums.sort(key=lambda t: t[0])
                # When the named COST column was empty (its slot held
                # a non-money value like a pattern code), the named
                # PRICE is probably in the wrong column too — the real
                # dollars just shifted one position over.  Trust the
                # positional sort for BOTH in that case.
                if not cost:
                    cost  = _nums[0][1]
                    price = _nums[-1][1]
                elif not price:
                    price = _nums[-1][1]
            elif len(_nums) == 1 and not price:
                price = _nums[0][1]
        stock = _pick_col(r, stock_col) if stock_col else ""
        has_pattern = bool(np_pattern) and np_pattern in rpat_norm
        has_load    = bool(ul_load) and rl.upper() == ul_load.upper()
        entry = {
            "price":            price,
            "cost":             cost,
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


def lookup_tempe_prices(size: str, pattern: str = "",
                        load_speed: str = "", brand: str = ""):
    """Primary entry.  Returns {dealer, consumer, sources}.

    Oct-23 (two user asks landed here):
      • `brand` scopes the Tempe match to Hankook / Laufenn when the
        SPRF material is HK / LF — avoids a Falken row with the same
        size winning on first-match-wins.  Non-HK/LF brands skip the
        brand gate (match any row).
      • Size variants: carrying_26 may still carry the truncated form
        ("225/70R17") while the Tempe CSV holds the correct
        "225/70R17.5" — _size_variants() expands the input to both so
        the match lands either way.
      • Two-pass strategy: run brand-scoped first; if nothing hits in
        BOTH dealer and consumer, run a second pass unscoped so the
        reader still gets a sensible fallback.  Preserves user intent
        (prefer own brand) without going silent when no own-brand
        row exists.
    """
    ns_set = _size_variants(size) if size else set()
    if not ns_set:
        return {"dealer": None, "consumer": None,
                "sources": {"dealer_file": "", "consumer_file": ""}}
    np = _nrm(pattern) if pattern else ""
    ul = (load_speed or "").upper().replace(" ", "")
    _brand_up = (brand or "").strip().upper()
    _scope    = _brand_up if _brand_up in ("HK", "LF") else ""

    # Oct-23 user ask: when the newest-month CSV has no match for
    # this size, walk back through older months until a hit lands.
    # Chip label then shows the fallback month ("'26.9") so the
    # operator knows the data isn't current.
    dealer_months   = _files_by_month("Tempeweborder")
    consumer_months = _files_by_month("Tempe")
    def _match_in_months(months, scope_brand, *, is_dealer):
        """Walk months newest-first; return (entry, yyyymm, path) when
        a row matches, else (None, "", "")."""
        for yyyymm, fp in months:
            rows, _ = _rows_for_path(fp)
            if is_dealer:
                e = _best_match(
                    rows, ns_set, np, ul, scope_brand,
                    price_cols=("PRICE", "price"),
                    cost_cols=("COST", "cost"),
                    stock_col="STOCK",
                    source_label="dealer",
                    source_file=fp,
                )
            else:
                # Oct-27: the consumer (retail-site) CSV now carries a
                # STOCK column too — "8+" / "3" / "0" / "" — so the
                # Remark chip can show "~OOS" or "~8+" on the EC
                # side as well, not just the dealer side.
                e = _best_match(
                    rows, ns_set, np, ul, scope_brand,
                    price_cols=("price", "disc_price", "PRICE", "DISC_PRICE"),
                    cost_cols=(),
                    stock_col="STOCK",
                    source_label="consumer",
                    source_file=fp,
                )
            if e is not None:
                return e, yyyymm, fp
        return None, "", ""

    def _run_scoped(scope_brand):
        d, dm, df = _match_in_months(dealer_months,   scope_brand, is_dealer=True)
        c, cm, cf = _match_in_months(consumer_months, scope_brand, is_dealer=False)
        return d, dm, df, c, cm, cf
    dealer, dm, df, consumer, cm, cf = _run_scoped(_scope)
    if _scope and not dealer and not consumer:
        # Brand-scoped walk missed everywhere — any-brand fallback.
        dealer, dm, df, consumer, cm, cf = _run_scoped("")

    # Oct-23 user ask: "current" is TODAY's calendar month (not
    # whatever the latest file on disk happens to be).  So if today
    # is Oct 2026 and the hit came from Sep 2026, the chip prefixes
    # "'26.9".  If the hit is Oct 2026 (same calendar month), no
    # prefix — the data is fresh.
    from datetime import datetime as _dt
    _today = _dt.now()
    _cur_month = f"{_today.year:04d}{_today.month:02d}"
    latest_dealer_month   = dealer_months[0][0]   if dealer_months   else ""
    latest_consumer_month = consumer_months[0][0] if consumer_months else ""
    if dealer:
        dealer["month"]       = dm
        dealer["is_current"]  = (dm == _cur_month)
        dealer["month_label"] = month_label(dm)
    if consumer:
        consumer["month"]       = cm
        consumer["is_current"]  = (cm == _cur_month)
        consumer["month_label"] = month_label(cm)
    return {
        "dealer":   dealer,
        "consumer": consumer,
        "sources": {
            "dealer_file":           os.path.basename(df or ""),
            "consumer_file":         os.path.basename(cf or ""),
            "latest_dealer_month":   latest_dealer_month,
            "latest_consumer_month": latest_consumer_month,
        },
    }


def _rows_for_path(path: str):
    """Variant of _rows_for() that takes an explicit path instead of a
    prefix — used by the month walker in lookup_tempe_prices."""
    if not path:
        return [], None
    try:
        mtime = os.path.getmtime(path)
    except OSError:
        mtime = 0
    now = time.time()
    cached = _CACHE.get(path)
    if cached and cached[0] == mtime and (now - cached[1]) < _CACHE_TTL_SEC:
        return cached[2], path
    rows = _load(path)
    _CACHE[path] = (mtime, now, rows)
    return rows, path
