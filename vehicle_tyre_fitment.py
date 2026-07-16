#!/usr/bin/env python3
"""
vehicle_tyre_fitment.py — crawl wheel-size.com for a (make, model[, year])
→ tyre size mapping, keyed off the make/model list you already got from
vehicle_rego.py.

Why wheel-size.com:
  - Clean, predictable URL scheme: /size/<make>/<model>/<year>/
  - Fitment tables render in the initial HTML (no JS execution needed)
  - Widely used as a fitment reference; matches the Australian market
    reasonably well for mainstream models.

Pipeline:
  1. Read vehicle_postcode_make_model.csv (or any CSV with make + model)
  2. Collapse to unique (make, model) pairs
  3. For each, GET https://www.wheel-size.com/size/<make>/<model>/
     (the page lists all model-year links)
  4. Optionally drill each year link for trim-level sizes
  5. Write vehicle_tyre_fitment.csv incrementally so a crash / rate-limit
     doesn't lose work.  Rerun with --resume to pick up where it stopped.

Politeness:
  1 request per DEFAULT_DELAY seconds, backs off exponentially on any
  429/503.  Skips (make, model) rows already present in the output CSV.

Usage:
    python vehicle_tyre_fitment.py                # default settings
    python vehicle_tyre_fitment.py --input my.csv --output fit.csv
    python vehicle_tyre_fitment.py --years-only   # skip year drill-down
    python vehicle_tyre_fitment.py --resume

Requirements:
    pip install requests beautifulsoup4 lxml pandas
"""
from __future__ import annotations
import argparse
import csv
import os
import re
import sys
import time
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

# ─────────────────────────── config ────────────────────────────────────
BASE_DIR       = os.path.dirname(os.path.abspath(__file__))
# vehicle_rego.py writes its normalized CSVs under out/rego/ next to the
# script, so default the fitment crawler to read + write there too.
# --input / --output CLI flags still override this if the layout differs.
REGO_DIR       = os.path.join(BASE_DIR, "out", "rego")
DEFAULT_INPUT  = os.path.join(REGO_DIR, "vehicle_postcode_make_model.csv")
DEFAULT_OUTPUT = os.path.join(REGO_DIR, "vehicle_tyre_fitment.csv")
BASE_URL       = "https://www.wheel-size.com"
DEFAULT_DELAY  = 4.0     # seconds between requests — wheel-size.com
                         # trips its Cloudflare challenge after ~20
                         # rapid requests, so we go slower up-front to
                         # avoid getting flagged in the first place.
MAX_RETRIES    = 4
# When Cloudflare is actively challenging us, per-URL retries just
# burn wall-clock without recovering — 4-min-per-model × thousands
# of models is untenable.  Bot-block (tiny/405/403) uses a smaller
# retry budget with a shorter cap so we fall through to FETCH_BLOCKED
# quickly and let the outer long-cooldown handle the WAF flag instead.
BOTBLOCK_MAX_RETRIES = 2
BOTBLOCK_MAX_WAIT    = 30.0   # cap per-attempt backoff (seconds)
# Rotate a small pool of realistic desktop User-Agents so a session
# doesn't hammer wheel-size.com with a single fingerprint (a common
# trivial bot-detection signal).
UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0 Safari/537.36",
]

def _headers(idx: int) -> dict:
    # Send a full desktop-browser header set — Cloudflare Bot Fight
    # Mode checks header presence + order.  Sec-Fetch-* alone kicks a
    # meaningful share of naive requests to a 405 challenge.
    return {
        "User-Agent":       UA_POOL[idx % len(UA_POOL)],
        "Accept":           "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language":  "en-AU,en;q=0.9",
        "Accept-Encoding":  "gzip, deflate, br",
        "Referer":          BASE_URL + "/",
        "DNT":              "1",
        "Connection":       "keep-alive",
        "Upgrade-Insecure-Requests":  "1",
        "Sec-Fetch-Dest":   "document",
        "Sec-Fetch-Mode":   "navigate",
        "Sec-Fetch-Site":   "same-origin",
        "Sec-Fetch-User":   "?1",
        "Cache-Control":    "max-age=0",
    }

# Anything shorter than this is treated as a bot-challenge page, not a
# real fitment page.  Real wheel-size.com model pages are 40-200 KB.
TINY_BODY_BYTES = 5000
# When we hit N consecutive bot-blocked responses, sleep for a much
# longer stretch to let Cloudflare's rate limiter cool down.  Trigger
# on the 2nd consecutive block so we don't waste 3× per-URL retries
# before recognising the WAF is active.
TINY_STREAK_LIMIT = 2
LONG_COOLDOWN_SECS = 180  # 3 min for the first cooldown

# Some BITRE names use spellings wheel-size.com doesn't.  Map here once
# and the crawler transparently rewrites the URL slug.
MAKE_ALIASES = {
    "MERCEDES-BENZ": "mercedes-benz",
    "MERCEDES BENZ": "mercedes-benz",
    "ROLLS ROYCE":   "rolls-royce",
    "LAND ROVER":    "land-rover",
    "ALFA ROMEO":    "alfa-romeo",
    "GREAT WALL":    "great-wall",
    "TATA MOTORS":   "tata",
    "CHERY AUTOMOBILE": "chery",
    "GENESIS MOTOR": "genesis",
    "ISUZU-UTE":     "isuzu",
    "GMSV":          "gmsv",
    "HSV":           "hsv",
    "FORD PERFORMANCE VEHICLES": "ford",
}

MODEL_ALIASES = {
    # Common Australian → wheel-size renames.  Add on discovery.
    # ("MAKE", "BITRE MODEL"): "wheel-size slug"
    ("MAZDA", "MAZDA 3"):  "3",
    ("MAZDA", "MAZDA 2"):  "2",
    ("MAZDA", "MAZDA 6"):  "6",
    ("MAZDA", "CX-3"):     "cx-3",
    ("MAZDA", "CX-5"):     "cx-5",
    ("MAZDA", "CX-8"):     "cx-8",
    ("MAZDA", "CX-9"):     "cx-9",
    ("MAZDA", "BT-50"):    "bt-50",
    ("HYUNDAI", "I30"):    "i30",
    ("HYUNDAI", "I20"):    "i20",
    ("HYUNDAI", "IX35"):   "ix35",
    ("BMW", "3 SERIES"):   "3-series",
    ("BMW", "5 SERIES"):   "5-series",
    ("BMW", "X3"):         "x3",
    ("BMW", "X5"):         "x5",
    ("KIA", "CERATO"):     "cerato",
    ("MERCEDES-BENZ", "C-CLASS"): "c-class",
    ("MERCEDES-BENZ", "E-CLASS"): "e-class",
    ("MERCEDES-BENZ", "GLC-CLASS"): "glc",
    ("TOYOTA", "COROLLA"): "corolla",
    ("TOYOTA", "HILUX"):   "hilux",
    ("TOYOTA", "PRADO"):   "land-cruiser-prado",
    ("TOYOTA", "LANDCRUISER"): "land-cruiser",
    ("TOYOTA", "LAND CRUISER"): "land-cruiser",
    ("MITSUBISHI", "OUTLANDER"): "outlander",
    ("MITSUBISHI", "TRITON"): "triton",
    ("MITSUBISHI", "PAJERO"): "pajero",
    ("MITSUBISHI", "PAJERO SPORT"): "pajero-sport",
    ("MITSUBISHI", "ASX"):  "asx",
    ("FORD", "RANGER"):    "ranger",
    ("FORD", "EVEREST"):   "everest",
    ("FORD", "FOCUS"):     "focus",
    ("HOLDEN", "COMMODORE"): "commodore",
    ("HOLDEN", "COLORADO"): "colorado",
    ("SUBARU", "FORESTER"): "forester",
    ("SUBARU", "OUTBACK"):  "outback",
    ("SUBARU", "IMPREZA"):  "impreza",
    ("SUBARU", "XV"):       "xv",
    ("SUBARU", "WRX"):      "impreza-wrx",
    ("NISSAN", "X-TRAIL"):  "x-trail",
    ("NISSAN", "NAVARA"):   "navara",
    ("NISSAN", "PATROL"):   "patrol",
    ("VOLKSWAGEN", "GOLF"): "golf",
    ("VOLKSWAGEN", "AMAROK"): "amarok",
    ("VOLKSWAGEN", "TIGUAN"): "tiguan",
}

FIELD_NAMES = ["make", "model", "gen_start_year", "size"]


def _slug_start_year(slug: str) -> int | None:
    """Pull the earliest 4-digit year token out of a wheel-size gen slug
    (e.g. 'pd-2020-2025' → 2020, 'c519-2022-now' → 2022, '2024' → 2024).
    That's the START year of the generation, which is the natural key
    for ranking 'which gen is the latest'."""
    years = [int(m.group(1))
             for m in re.finditer(r"(?<!\d)(19\d{2}|20\d{2})(?!\d)", slug)]
    return min(years) if years else None


def _pick_latest_gen(slugs: list[str]) -> str | None:
    """From every generation slug the model landing offered, pick the
    one that represents the most-recent generation.  Sort key:
      1) highest start year (latest gen wins)
      2) prefer generation-style slugs ('pd-2024-now', 'c519-2022-now')
         over bare year-only slugs ('2024', '2025') on ties — the gen
         page carries every trim's fitment; the year page is a subset.
      3) prefer slugs whose end marker is 'now' (i.e. still in production)."""
    if not slugs:
        return None
    ranked = []
    for s in slugs:
        yr = _slug_start_year(s) or 0
        has_letters   = bool(re.search(r"[a-z]", s.lower()))
        ends_with_now = s.lower().endswith("-now") or s.lower() == "now"
        # Sort ascending so lower is better; negate year so "higher"
        # actually wins the sort.
        ranked.append((
            -yr,
            0 if ends_with_now else 1,
            0 if has_letters   else 1,
            s,
        ))
    ranked.sort()
    return ranked[0][3]


# ─────────────────────────── helpers ───────────────────────────────────
def slugify(name: str) -> str:
    """Best-effort slug for the wheel-size.com URL segment.  Lowercases,
    strips non-alphanumeric, joins with dashes so " Corolla Cross " →
    'corolla-cross'."""
    if not name:
        return ""
    s = re.sub(r"[^A-Za-z0-9]+", "-", name.strip().upper()).strip("-").lower()
    return s


def make_slug(make: str) -> str:
    up = (make or "").strip().upper()
    return MAKE_ALIASES.get(up, slugify(up))


def model_slug(make: str, model: str) -> str:
    key = ((make or "").strip().upper(), (model or "").strip().upper())
    if key in MODEL_ALIASES:
        return MODEL_ALIASES[key]
    return slugify(model)


# Sentinels so the caller can distinguish "confirmed absent" (404) from
# "we got bot-blocked, DON'T record as done — retry on next --resume".
FETCH_BLOCKED = object()

def fetch(session: requests.Session, url: str, delay: float,
          ua_idx: int = 0) -> str | None | object:
    """GET with polite delay + exponential backoff.

    Return values:
      * str      — real page body (parse it)
      * None     — 404 or hard failure (record model as empty; don't retry)
      * FETCH_BLOCKED — tiny/challenge response after all retries
                        (do NOT record a row so --resume picks it back up)

    Tiny 200 responses (< TINY_BODY_BYTES) are treated as bot-challenge
    pages and trigger the same exponential backoff as 429 / 503.  A real
    wheel-size fitment page is 40-200 KB; a Cloudflare challenge body
    is a few hundred bytes."""
    botblock_attempts = 0
    attempt = 0
    while attempt < MAX_RETRIES:
        hdr = _headers(ua_idx + attempt)   # rotate UA on each retry
        try:
            r = session.get(url, headers=hdr, timeout=30)
        except requests.RequestException as e:
            print(f"    [warn] {url}: {e}", file=sys.stderr)
            time.sleep(delay * (2 ** attempt))
            attempt += 1
            continue
        sz = len(r.text or "")
        if r.status_code == 200 and sz >= TINY_BODY_BYTES:
            print(f"    [http] 200 · {sz:>6} bytes", file=sys.stderr)
            time.sleep(delay)
            return r.text
        is_botblock = (
            (r.status_code == 200 and sz < TINY_BODY_BYTES)
            or (r.status_code in (403, 405) and sz < TINY_BODY_BYTES)
            or r.status_code in (429, 503)
        )
        if is_botblock:
            botblock_attempts += 1
            if botblock_attempts > BOTBLOCK_MAX_RETRIES:
                # Give up fast — the outer main loop's long cooldown
                # is the right place to recover, not per-URL retries.
                print(f"    [http] {r.status_code} · {sz} bytes (bot-block, "
                      f"giving up after {botblock_attempts} tries)",
                      file=sys.stderr)
                break
            wait = min(BOTBLOCK_MAX_WAIT, delay * (2 ** (botblock_attempts + 1)))
            print(f"    [http] {r.status_code} · {sz} bytes (bot-block) — "
                  f"backing off {wait:.1f}s", file=sys.stderr)
            time.sleep(wait)
            attempt += 1
            continue
        if r.status_code == 404:
            print(f"    [http] 404 · not on wheel-size.com", file=sys.stderr)
            time.sleep(delay)
            return None
        print(f"    [http] {r.status_code} · {sz} bytes", file=sys.stderr)
        time.sleep(delay)
        return None
    # All retries exhausted — signal "blocked, retry later".
    return FETCH_BLOCKED


TYRE_RX = re.compile(r"(\d{3})/(\d{2})\s*[RZ]\s*(\d{2})", re.IGNORECASE)

# Generation / year links off a wheel-size model page.  The site's
# schema is /size/<make>/<model>/<year>[-suffix]/ — e.g.
# /size/hyundai/i30/2024/, /2020-mk3/, /2015/, …  Only /size/… paths
# under the same (make, model) are considered so we don't wander off
# into other brands via cross-links.
def _gen_link_rx(m_slug: str, d_slug: str) -> re.Pattern:
    return re.compile(
        rf'/size/{re.escape(m_slug)}/{re.escape(d_slug)}/([^/"\s#?]+)/',
        re.IGNORECASE,
    )


def _norm_size(raw: str) -> str:
    """Normalise scraped sizes to "215/70R14" form so the same tyre from
    different pages doesn't dedup as separate rows."""
    m = TYRE_RX.search(raw or "")
    if not m:
        return ""
    return f"{m.group(1)}/{m.group(2)}R{m.group(3)}"


def parse_generation_links(html: str, m_slug: str, d_slug: str) -> list[str]:
    """Return the unique generation / year slugs that link out of the
    model landing page.  Slugs look like '2024', '2020-mk3', '2015',
    '2000-alfa-tempo', etc. — anything past the /model/ segment."""
    if not html:
        return []
    seen: dict[str, None] = {}
    for m in _gen_link_rx(m_slug, d_slug).finditer(html):
        slug = m.group(1).strip("/")
        if not slug or slug.lower() in {"specifications", "wheels", "tires", "tyres"}:
            continue
        if slug not in seen:
            seen[slug] = None
    return list(seen.keys())


def parse_model_page(html: str) -> list[dict]:
    """Extract (year, trim, size) triples from a wheel-size.com model /
    year page.

    Strategy (in order):
      1) Try structured tables — most precise (year + trim + size).
      2) Scan the RAW HTML body with the tyre regex.  This catches
         sizes stashed inside JSON blobs / data attributes / script
         tags that wheel-size.com uses for its JS-rendered widgets,
         which soup.get_text() would otherwise skip.

    Step 2 is coarse (no year / trim) but reliable — every model
    landing page carries at least a handful of size tokens in the
    served HTML text or JSON."""
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")
    rows = []
    seen = set()

    # 1) Tables — most reliable source: each row has trim + size cells.
    for table in soup.select("table"):
        headers = [th.get_text(strip=True).lower()
                   for th in table.select("thead th")]
        # Fitment tables typically carry "Wheel", "Tire", "Fitment", etc.
        if not any(k in " ".join(headers) for k in ("tire", "tyre", "wheel", "fitment")):
            continue
        # Best-effort column indexing.
        idx_year = idx_trim = idx_size = -1
        for i, h in enumerate(headers):
            if idx_year == -1 and "year" in h:  idx_year = i
            if idx_trim == -1 and any(k in h for k in ("trim", "generation", "modification")):
                idx_trim = i
            if idx_size == -1 and any(k in h for k in ("tire", "tyre", "fitment")):
                idx_size = i
        if idx_size == -1:
            continue
        for tr in table.select("tbody tr"):
            cells = tr.find_all(["td", "th"])
            if not cells or idx_size >= len(cells):
                continue
            size_txt = cells[idx_size].get_text(" ", strip=True)
            for m in TYRE_RX.finditer(size_txt):
                sz = _norm_size(m.group(0))
                if not sz:
                    continue
                year = cells[idx_year].get_text(strip=True) if 0 <= idx_year < len(cells) else ""
                trim = cells[idx_trim].get_text(strip=True) if 0 <= idx_trim < len(cells) else ""
                key = (year, trim, sz)
                if key in seen:
                    continue
                seen.add(key)
                rows.append({"year": year, "trim": trim, "size": sz})

    # 2) Fallback — scan the RAW served HTML for size-shaped tokens.
    # wheel-size.com renders most of its fitment via JS off JSON in
    # script tags, so soup.get_text() skips the sizes; the regex has
    # to walk the original bytes.  Coarse (no year/trim) but reliable.
    if not rows:
        seen_sz = set()
        for m in TYRE_RX.finditer(html):
            sz = _norm_size(m.group(0))
            if sz and sz not in seen_sz:
                seen_sz.add(sz)
                rows.append({"year": "", "trim": "", "size": sz})

    return rows


# ────────────────────── input / output plumbing ────────────────────────
def load_unique_make_model(input_csv: str) -> list[tuple[str, str]]:
    """Read make + model columns off the BITRE CSV and dedup, PRESERVING
    the CSV row order.  BITRE is sorted by state → postcode, so the
    first (make, model) pairs to appear are the popular Aussie
    mainstreams (HYUNDAI I30, FORD FOCUS, TOYOTA HILUX…), not the
    alphabetically-first obscure brands (AC Cobra, ACE trailers…).
    Using a dict as an ordered set preserves first-appearance order."""
    seen: dict[tuple[str, str], None] = {}
    with open(input_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "make" not in reader.fieldnames or "model" not in reader.fieldnames:
            raise ValueError(
                f"{input_csv} needs `make` and `model` columns "
                f"(got {reader.fieldnames})."
            )
        for r in reader:
            mk = (r.get("make") or "").strip()
            md = (r.get("model") or "").strip()
            if not mk or not md:
                continue
            up_mk, up_md = mk.upper(), md.upper()
            # Filter obvious placeholders — BITRE stamps these when the
            # per-postcode counts are too small to disclose or the model
            # field couldn't be resolved.
            if up_mk in {"-", "TOTAL"} or up_md in {"-", "TOTAL", "UNKNOWN", "OTHER"}:
                continue
            key = (up_mk, up_md)
            if key not in seen:
                seen[key] = None
    return list(seen.keys())


def load_already_done(output_csv: str, retry_empty: bool = False) -> set[tuple[str, str]]:
    """(make, model) pairs already present in the output — used to skip
    on --resume so the crawler picks up where it stopped.

    When retry_empty=True, rows whose size is blank AND gen_start_year
    is blank are treated as NOT done — probably bot-blocked pages from a
    previous run that recorded empty rows before the block-detection
    logic existed.  Retrying them costs another crawl but recovers
    coverage."""
    done = set()
    if not os.path.exists(output_csv):
        return done
    # First pass — group rows per (make, model) so we can decide whether
    # ANY size was ever recovered for the pair before deciding to skip.
    per_pair: dict[tuple[str, str], dict] = {}
    with open(output_csv, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            key = ((r.get("make") or "").upper(), (r.get("model") or "").upper())
            sz  = (r.get("size") or "").strip()
            gy  = (r.get("gen_start_year") or "").strip()
            slot = per_pair.setdefault(key, {"any_size": False, "any_gen": False})
            if sz:
                slot["any_size"] = True
            if gy:
                slot["any_gen"] = True
    for key, slot in per_pair.items():
        # With --retry-empty, ANY pair whose recorded rows never captured
        # a size is retried.  This catches both fully-blocked landings
        # (no gen, no size) and gen-page-blocks (gen captured on the
        # landing page, but the gen-page fetch bounced so no sizes).
        # A model wheel-size.com genuinely lists 0 fitments for is very
        # rare — better to burn one extra fetch than to leave the row
        # permanently empty.
        if retry_empty and not slot["any_size"]:
            continue
        done.add(key)
    return done


# ─────────────────────────── main ──────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input",  default=DEFAULT_INPUT,
                    help="Input CSV with make/model columns.")
    ap.add_argument("--output", default=DEFAULT_OUTPUT,
                    help="Output CSV (append mode).")
    ap.add_argument("--delay",  type=float, default=DEFAULT_DELAY,
                    help="Seconds between requests (default: %(default)s).")
    ap.add_argument("--resume", action="store_true",
                    help="Skip (make, model) rows already in the output.")
    ap.add_argument("--retry-empty", action="store_true",
                    help="With --resume, also retry pairs whose recorded rows "
                         "have no size AND no gen_start_year (likely bot-blocked "
                         "on the earlier run).")
    ap.add_argument("--limit",  type=int, default=0,
                    help="Stop after N pairs (0 = no limit — useful for a smoke test).")
    ap.add_argument("--dump-html", action="store_true",
                    help="Also save each fetched HTML as debug/<make>_<model>.html so "
                         "the parser can be inspected against the real page structure.")
    args = ap.parse_args()

    if not os.path.exists(args.input):
        print(f"[fatal] input CSV not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    pairs = load_unique_make_model(args.input)
    done  = (load_already_done(args.output, retry_empty=args.retry_empty)
             if args.resume else set())
    todo  = [(mk, md) for (mk, md) in pairs if (mk, md) not in done]
    print(f"[info] unique pairs: {len(pairs)}  · already done: {len(done)}  · todo: {len(todo)}")
    if args.limit:
        todo = todo[:args.limit]
        print(f"[info] limit={args.limit} — will stop after {len(todo)} pairs")

    # Open output in append mode; write header on first create only.
    # Make sure the directory exists so a fresh --output path works
    # without the user having to mkdir it first.
    out_dir = os.path.dirname(os.path.abspath(args.output))
    if out_dir and not os.path.isdir(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    # Startup cleanup — always dedup the output CSV, drop legacy
    # columns from older schemas (7-col: year/trim/source_url/... or
    # 3-col: no gen_start_year), and migrate to the current 4-col
    # (make, model, gen_start_year, size) shape.  Append-mode writes +
    # crash/rerun without --resume can double-write the prefix; the
    # dedup here keeps the file honest regardless of invocation history.
    if os.path.exists(args.output) and os.path.getsize(args.output) > 0:
        with open(args.output, newline="", encoding="utf-8") as f_in:
            probe = csv.DictReader(f_in)
            existing_cols = probe.fieldnames or []
            extras = [c for c in existing_cols if c not in FIELD_NAMES]
            missing = [c for c in FIELD_NAMES if c not in existing_cols]
            seen_rows: set[tuple[str, str, str]] = set()
            kept: list[dict] = []
            # First pass — collect every row per pair so we can decide
            # which pairs are "empty" (no size recovered) and drop those
            # entirely when --retry-empty is set.
            rows_by_pair: dict[tuple[str, str], list[dict]] = {}
            any_size_by_pair: dict[tuple[str, str], bool] = {}
            for r in probe:
                mk = (r.get("make")  or "").strip()
                md = (r.get("model") or "").strip()
                sz = (r.get("size")  or "").strip()
                gy = (r.get("gen_start_year") or "").strip()
                pair = (mk, md)
                rows_by_pair.setdefault(pair, []).append(
                    {"make": mk, "model": md, "gen_start_year": gy, "size": sz}
                )
                if sz:
                    any_size_by_pair[pair] = True
            for pair, rows in rows_by_pair.items():
                # Drop empty-only pairs on --retry-empty so the next
                # crawl actually re-fetches them.
                if args.retry_empty and not any_size_by_pair.get(pair):
                    continue
                for r in rows:
                    key = (r["make"], r["model"], r["size"])
                    if key in seen_rows:
                        continue
                    seen_rows.add(key)
                    kept.append(r)
            with open(args.output, newline="", encoding="utf-8") as f_count:
                dropped = sum(1 for _ in csv.DictReader(f_count)) - len(kept)
            if extras or missing or dropped > 0:
                if extras:
                    print(f"[info] dropping legacy columns: {extras}", file=sys.stderr)
                if missing:
                    print(f"[info] migrating to new schema (adding: {missing})",
                          file=sys.stderr)
                if dropped > 0:
                    print(f"[info] deduping {dropped:,} duplicate rows "
                          "(append-mode restart residue)", file=sys.stderr)
                with open(args.output, "w", newline="", encoding="utf-8") as f_out:
                    w = csv.DictWriter(f_out, fieldnames=FIELD_NAMES)
                    w.writeheader()
                    w.writerows(kept)
                print(f"[info] kept {len(kept):,} unique rows", file=sys.stderr)

    write_header = not os.path.exists(args.output) or os.path.getsize(args.output) == 0
    fh = open(args.output, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(fh, fieldnames=FIELD_NAMES)
    if write_header:
        writer.writeheader()

    session = requests.Session()
    ok_pairs = 0
    empty_pairs = 0
    blocked_pairs = 0
    tiny_streak = 0
    for i, (mk, md) in enumerate(todo, 1):
        m_slug = make_slug(mk)
        d_slug = model_slug(mk, md)
        if not m_slug or not d_slug:
            print(f"  [skip] {mk} · {md} (no slug)")
            continue
        model_url = f"{BASE_URL}/size/{quote(m_slug)}/{quote(d_slug)}/"
        print(f"[{i}/{len(todo)}] {mk} · {md}  →  {model_url}")
        model_html = fetch(session, model_url, args.delay, ua_idx=i)
        # Long cooldown after too many consecutive blocks so Cloudflare's
        # rate limiter has a chance to reset before we keep hammering.
        if model_html is FETCH_BLOCKED:
            tiny_streak += 1
            blocked_pairs += 1
            print(f"  [blocked] {mk} · {md} — not recorded, will retry with --resume",
                  file=sys.stderr)
            if tiny_streak >= TINY_STREAK_LIMIT:
                print(f"  [cooldown] {TINY_STREAK_LIMIT} consecutive blocks — "
                      f"sleeping {LONG_COOLDOWN_SECS}s", file=sys.stderr)
                time.sleep(LONG_COOLDOWN_SECS)
                session = requests.Session()   # fresh cookies / TLS session
                tiny_streak = 0
            continue
        # Real response (or None) — reset the streak.
        tiny_streak = 0
        if args.dump_html and model_html:
            dump_dir = os.path.join(os.path.dirname(os.path.abspath(args.output)), "debug")
            os.makedirs(dump_dir, exist_ok=True)
            slug = f"{make_slug(mk)}__{model_slug(mk, md)}".replace("/", "-")[:120] or "page"
            with open(os.path.join(dump_dir, slug + ".html"), "w", encoding="utf-8") as df:
                df.write(model_html)
            print(f"    [dump] debug/{slug}.html", file=sys.stderr)

        # Simplified two-level crawl.  Model landing shows the
        # generation timeline; pick THE latest generation and only
        # fetch that one page — we don't need historic fitments here,
        # just the currently-produced sizes.  Output shrinks to
        # (make, model, size) with duplicates collapsed.
        sizes: set[str] = set()
        # A tiny "quick facts" block on the model landing sometimes
        # already carries the current-gen size(s).
        for r in (parse_model_page(model_html) if model_html else []):
            if r["size"]:
                sizes.add(r["size"])

        gen_slugs = parse_generation_links(model_html, m_slug, d_slug) if model_html else []
        picked   = _pick_latest_gen(gen_slugs) if gen_slugs else None
        gen_year = _slug_start_year(picked) if picked else None
        if picked:
            gen_url = f"{BASE_URL}/size/{quote(m_slug)}/{quote(d_slug)}/{quote(picked)}/"
            print(f"    [latest] {picked}  →  {gen_url}", file=sys.stderr)
            gen_html = fetch(session, gen_url, args.delay, ua_idx=i + 1)
            if gen_html is FETCH_BLOCKED:
                # Landing succeeded but gen page bounced — record what
                # we already have from the landing rather than lose the
                # whole model to bot-block.
                gen_html = None
            for r in (parse_model_page(gen_html) if gen_html else []):
                if r["size"]:
                    sizes.add(r["size"])

        gy_out = str(gen_year) if gen_year else ""
        if not sizes:
            empty_pairs += 1
            writer.writerow({"make": mk, "model": md,
                             "gen_start_year": gy_out, "size": ""})
        else:
            ok_pairs += 1
            for sz in sorted(sizes):
                writer.writerow({"make": mk, "model": md,
                                 "gen_start_year": gy_out, "size": sz})
        fh.flush()
    fh.close()
    print(f"[done] {ok_pairs} with fitment · {empty_pairs} empty · "
          f"{blocked_pairs} bot-blocked (not recorded — rerun with --resume) · "
          f"output: {args.output}")


if __name__ == "__main__":
    main()
