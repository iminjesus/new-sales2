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
from datetime import datetime
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
DEFAULT_DELAY  = 2.0     # seconds between requests
MAX_RETRIES    = 4
USER_AGENT     = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)
HEADERS = {"User-Agent": USER_AGENT, "Accept-Language": "en-AU,en;q=0.9"}

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

FIELD_NAMES = ["make", "model", "year", "trim", "size", "source_url", "fetched_at"]


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


def fetch(session: requests.Session, url: str, delay: float) -> str | None:
    """GET with a polite delay + exponential backoff on 429 / 503.  Logs
    HTTP status + response length so we can tell a genuine 404 from a
    bot-block (which typically returns 200 with a tiny challenge body)
    without the caller having to re-run with verbose."""
    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(url, headers=HEADERS, timeout=30)
        except requests.RequestException as e:
            print(f"    [warn] {url}: {e}", file=sys.stderr)
            time.sleep(delay * (2 ** attempt))
            continue
        sz = len(r.text or "")
        if r.status_code == 200:
            # A real fitment page is 40-200 KB.  Anything under ~2 KB
            # is almost always a bot-challenge / redirect body, so
            # flag it for the caller.
            tag = " (tiny — check for bot-block)" if sz < 2000 else ""
            print(f"    [http] 200 · {sz:>6} bytes{tag}", file=sys.stderr)
            time.sleep(delay)
            return r.text
        if r.status_code == 404:
            print(f"    [http] 404 · not on wheel-size.com", file=sys.stderr)
            time.sleep(delay)
            return None
        if r.status_code in (429, 503):
            wait = delay * (2 ** (attempt + 2))
            print(f"    [http] {r.status_code} — backing off {wait:.1f}s", file=sys.stderr)
            time.sleep(wait)
            continue
        print(f"    [http] {r.status_code} · {sz} bytes", file=sys.stderr)
        time.sleep(delay)
        return None
    return None


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


def load_already_done(output_csv: str) -> set[tuple[str, str]]:
    """(make, model) pairs already present in the output — used to skip
    on --resume so the crawler picks up where it stopped."""
    done = set()
    if not os.path.exists(output_csv):
        return done
    with open(output_csv, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            done.add(((r.get("make") or "").upper(), (r.get("model") or "").upper()))
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
    ap.add_argument("--limit",  type=int, default=0,
                    help="Stop after N pairs (0 = no limit — useful for a smoke test).")
    ap.add_argument("--max-gens", type=int, default=6,
                    help="Per model, visit at most this many generation/year sub-pages "
                         "(default: %(default)s).  wheel-size.com lists each i30 "
                         "generation/year separately; drilling all of them can add "
                         "10-15 requests per model.")
    ap.add_argument("--dump-html", action="store_true",
                    help="Also save each fetched HTML as debug/<make>_<model>.html so "
                         "the parser can be inspected against the real page structure.")
    args = ap.parse_args()

    if not os.path.exists(args.input):
        print(f"[fatal] input CSV not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    pairs = load_unique_make_model(args.input)
    done  = load_already_done(args.output) if args.resume else set()
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
    write_header = not os.path.exists(args.output) or os.path.getsize(args.output) == 0
    fh = open(args.output, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(fh, fieldnames=FIELD_NAMES)
    if write_header:
        writer.writeheader()

    session = requests.Session()
    ok_pairs = 0
    empty_pairs = 0
    for i, (mk, md) in enumerate(todo, 1):
        m_slug = make_slug(mk)
        d_slug = model_slug(mk, md)
        if not m_slug or not d_slug:
            print(f"  [skip] {mk} · {md} (no slug)")
            continue
        model_url = f"{BASE_URL}/size/{quote(m_slug)}/{quote(d_slug)}/"
        print(f"[{i}/{len(todo)}] {mk} · {md}  →  {model_url}")
        model_html = fetch(session, model_url, args.delay)
        if args.dump_html and model_html:
            dump_dir = os.path.join(os.path.dirname(os.path.abspath(args.output)), "debug")
            os.makedirs(dump_dir, exist_ok=True)
            slug = f"{make_slug(mk)}__{model_slug(mk, md)}".replace("/", "-")[:120] or "page"
            with open(os.path.join(dump_dir, slug + ".html"), "w", encoding="utf-8") as df:
                df.write(model_html)
            print(f"    [dump] debug/{slug}.html", file=sys.stderr)

        # Two-level crawl: the model landing lists generation/year
        # links, and the fitment sizes live on THOSE pages (the model
        # landing itself just shows a timeline).  Grab the generation
        # slugs, visit up to --max-gens of them, and pool the sizes.
        rows: list[dict] = []
        seen_triples: set[tuple[str, str, str]] = set()
        # Also pick up any sizes exposed on the model landing itself —
        # some pages carry a "quick facts" block with the current-gen
        # size.
        for r in (parse_model_page(model_html) if model_html else []):
            key = (r["year"], r["trim"], r["size"])
            if r["size"] and key not in seen_triples:
                seen_triples.add(key); rows.append(r)

        gen_slugs = parse_generation_links(model_html, m_slug, d_slug) if model_html else []
        if gen_slugs:
            print(f"    [gens] {len(gen_slugs)} generations found; "
                  f"visiting {min(len(gen_slugs), args.max_gens)}", file=sys.stderr)
        for gen_slug in gen_slugs[:args.max_gens]:
            gen_url = f"{BASE_URL}/size/{quote(m_slug)}/{quote(d_slug)}/{quote(gen_slug)}/"
            print(f"    [gen] {gen_slug}  →  {gen_url}", file=sys.stderr)
            gen_html = fetch(session, gen_url, args.delay)
            for r in (parse_model_page(gen_html) if gen_html else []):
                if not r["size"]:
                    continue
                # Prefer the gen slug as the year label when the row
                # itself doesn't carry a year — makes the CSV a bit
                # easier to slice later.
                if not r["year"]:
                    r["year"] = gen_slug
                key = (r["year"], r["trim"], r["size"])
                if key in seen_triples:
                    continue
                seen_triples.add(key)
                # Track which URL surfaced the size so a later re-run
                # can retrace the origin.
                r["source_url"] = gen_url
                rows.append(r)

        if not rows:
            empty_pairs += 1
            writer.writerow({
                "make": mk, "model": md, "year": "", "trim": "", "size": "",
                "source_url": model_url,
                "fetched_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            })
        else:
            ok_pairs += 1
            for r in rows:
                writer.writerow({
                    "make": mk, "model": md,
                    "year": r["year"], "trim": r["trim"], "size": r["size"],
                    "source_url": r.get("source_url") or model_url,
                    "fetched_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                })
        fh.flush()
    fh.close()
    print(f"[done] {ok_pairs} with fitment · {empty_pairs} empty · output: {args.output}")


if __name__ == "__main__":
    main()
